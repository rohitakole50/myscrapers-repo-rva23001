import io, datetime as dt
import pandas as pd
from google.cloud import storage
from .dwml_parse import fetch_dwml, flatten_dwml, fetch_energy, flatten_energy
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Pipeline:
    def __init__(self, *, project_id: str, bucket_name: str,
                 raw_prefix: str, csv_prefix: str,
                 lat: float, lon: float, fcst_url: str):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.raw_prefix = raw_prefix
        self.csv_prefix = csv_prefix
        self.lat = lat
        self.lon = lon
        self.fcst_url = fcst_url
        self.storage = storage.Client(project=project_id)
        self.bucket = self.storage.bucket(bucket_name)

    def run_once(self) -> dict:
        stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

        # 1) fetch and save raw
        resp = fetch_dwml(self.fcst_url)
        raw_name = f"{self.raw_prefix}dwml_{stamp}.xml"
        self.bucket.blob(raw_name).upload_from_string(resp.text, content_type="application/xml")

        # 2) flatten
        df = flatten_dwml(resp.content, stamp, self.lat, self.lon)

        # 3) write per-run CSV
        per_run = f"{self.csv_prefix}flat_{stamp}.csv"
        buf = io.StringIO(); df.to_csv(buf, index=False)
        self.bucket.blob(per_run).upload_from_string(buf.getvalue(), content_type="text/csv")

         # We no longer maintain a rolling master.csv or push to BigQuery.
        return {
            "raw_xml": f"gs://{self.bucket_name}/{raw_name}",
            "per_run_csv": f"gs://{self.bucket_name}/{per_run}",
            "rows_this_run": len(df),
        }

    def run_energy_range(self, start_date: str, location: str, url_base: str,
                         raw_prefix: str, csv_prefix: str, iso_user: str = None, iso_pass: str = None) -> dict:
        """Fetch per-day energy data from `start_date` (YYYYMMDD) through today.

        For each day the method calls: {url_base}/day/{YYYYMMDD}/location/{location}
        and uploads raw JSON and a flattened CSV to the provided prefixes.
        Returns a summary dict with per-day results.
        """
        try:
            cur = dt.datetime.strptime(start_date, "%Y%m%d").date()
        except Exception as e:
            return {"error": f"bad_start_date: {e}", "start_date": start_date}

        # Use yesterday as the end date — current-day payloads may not be available yet
        end_dt = (dt.datetime.utcnow().date() - dt.timedelta(days=1))
        day_results = {}
        total_rows = 0
        total_days = 0

        while cur <= end_dt:
            day_str = cur.strftime("%Y%m%d")
            stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            day_url = f"{url_base}/day/{day_str}/location/{location}"

            # fetch with Basic Auth if provided
            try:
                logger.info("Fetching energy URL: %s (date=%s)", day_url, day_str)
                resp = fetch_energy(day_url, iso_user=iso_user, iso_pass=iso_pass)
                logger.info("Fetched energy for %s: status=%s, len=%d", day_str, getattr(resp, 'status_code', None), len(resp.text or ""))
            except Exception as e:
                day_results[day_str] = {"error": f"fetch_failed: {e}", "url": day_url}
                cur = cur + dt.timedelta(days=1)
                total_days += 1
                continue

            raw_name = f"{raw_prefix}energy_{day_str}.json"
            raw_blob = self.bucket.blob(raw_name)
            raw_gs = f"gs://{self.bucket_name}/{raw_name}"
            try:
                if raw_blob.exists():
                    logger.info("Raw energy JSON already exists at %s; skipping upload", raw_gs)
                else:
                    raw_blob.upload_from_string(resp.text, content_type="application/json")
                    logger.info("Uploaded raw energy JSON to %s", raw_gs)
            except Exception as e:
                day_results[day_str] = {"error": f"upload_raw_failed: {e}", "raw_name": raw_name}
                cur = cur + dt.timedelta(days=1)
                total_days += 1
                continue

            # flatten
            try:
                # pass raw text to flatten_energy; let it handle empty/invalid JSON
                df = flatten_energy(resp.text, stamp, location)
                logger.info("Flattened energy for %s: rows=%d", day_str, len(df))
            except Exception as e:
                logger.exception("flatten_failed for %s", day_str)
                day_results[day_str] = {"error": f"flatten_failed: {e}", "day": day_str}
                cur = cur + dt.timedelta(days=1)
                total_days += 1
                continue

            # Append new rows into a single master CSV instead of per-day CSVs.
            master_name = f"{csv_prefix}energy_master.csv"
            master_blob = self.bucket.blob(master_name)
            try:
                if master_blob.exists():
                    txt = master_blob.download_as_text()
                    try:
                        master_df = pd.read_csv(io.StringIO(txt), parse_dates=["begin_date", "scrape_time_utc"]) 
                    except Exception:
                        # fallback: read without parsing, then coerce
                        master_df = pd.read_csv(io.StringIO(txt))
                        if "begin_date" in master_df.columns:
                            master_df["begin_date"] = pd.to_datetime(master_df["begin_date"], utc=True, errors="coerce")
                        if "scrape_time_utc" in master_df.columns:
                            master_df["scrape_time_utc"] = pd.to_datetime(master_df["scrape_time_utc"], utc=True, errors="coerce")
                    logger.info("Loaded existing master CSV with %d rows", len(master_df))
                else:
                    master_df = pd.DataFrame(columns=["scrape_time_utc", "begin_date", "location", "location_id", "load"])
                    logger.info("No existing master CSV found; will create new one")
                    # Ensure an empty master CSV exists so users see the flattened path in GCS
                    try:
                        buf_init = io.StringIO(); master_df.to_csv(buf_init, index=False)
                        self.bucket.blob(master_name).upload_from_string(buf_init.getvalue(), content_type="text/csv")
                        logger.info("Initialized empty master CSV at gs://%s/%s", self.bucket_name, master_name)
                    except Exception:
                        logger.exception("failed to write initial empty master CSV")

                # normalize key columns for dedupe
                master_keys = set()
                if not master_df.empty:
                    master_df["begin_date"] = pd.to_datetime(master_df["begin_date"], utc=True, errors="coerce")
                    master_df["location_id"] = master_df["location_id"].astype(str)
                    master_keys = set(zip(master_df["begin_date"].astype(str), master_df["location_id"]))

                # prepare incoming rows
                df["begin_date"] = pd.to_datetime(df["begin_date"], utc=True, errors="coerce")
                df["location_id"] = df["location_id"].astype(str)
                incoming_keys = list(zip(df["begin_date"].astype(str), df["location_id"]))
                mask_new = [k not in master_keys for k in incoming_keys]
                new_rows = df[mask_new]

                if new_rows.empty:
                    logger.info("No new rows to append for %s", day_str)
                    day_results[day_str] = {"raw_json": raw_gs, "rows_this_run": 0, "skipped": "no_new_rows"}
                else:
                    # append and write back
                    updated = pd.concat([master_df, new_rows], ignore_index=True)
                    # optional: sort by begin_date
                    if "begin_date" in updated.columns:
                        updated["begin_date"] = pd.to_datetime(updated["begin_date"], utc=True, errors="coerce")
                        updated.sort_values("begin_date", inplace=True)
                    buf2 = io.StringIO(); updated.to_csv(buf2, index=False)
                    self.bucket.blob(master_name).upload_from_string(buf2.getvalue(), content_type="text/csv")
                    master_gs = f"gs://{self.bucket_name}/{master_name}"
                    logger.info("Appended %d new rows to master CSV %s (total %d)", len(new_rows), master_gs, len(updated))
                    day_results[day_str] = {"raw_json": raw_gs, "master_csv": master_gs, "rows_this_run": len(new_rows)}
            except Exception as e:
                logger.exception("master_update_failed for %s", day_str)
                day_results[day_str] = {"error": f"master_update_failed: {e}", "day": day_str}
                cur = cur + dt.timedelta(days=1)
                total_days += 1
                continue

            total_rows += len(df)
            total_days += 1
            cur = cur + dt.timedelta(days=1)

        return {
            "start_date": start_date,
            "end_date": end_dt.strftime("%Y%m%d"),
            "location": location,
            "days_requested": total_days,
            "total_rows": total_rows,
            "per_day": day_results,

        }






