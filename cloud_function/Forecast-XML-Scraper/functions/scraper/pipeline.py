import io, datetime as dt
import pandas as pd
from google.cloud import storage
from .dwml_parse import fetch_dwml, flatten_dwml, fetch_energy, flatten_energy

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

        end_dt = dt.datetime.utcnow().date()
        day_results = {}
        total_rows = 0
        total_days = 0

        while cur <= end_dt:
            day_str = cur.strftime("%Y%m%d")
            stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            day_url = f"{url_base}/day/{day_str}/location/{location}"

            # fetch with Basic Auth if provided
            try:
                resp = fetch_energy(day_url, iso_user=iso_user, iso_pass=iso_pass)
            except Exception as e:
                day_results[day_str] = {"error": f"fetch_failed: {e}", "url": day_url}
                cur = cur + dt.timedelta(days=1)
                total_days += 1
                continue

            raw_name = f"{raw_prefix}energy_{day_str}_{stamp}.json"
            try:
                self.bucket.blob(raw_name).upload_from_string(resp.text, content_type="application/json")
                raw_gs = f"gs://{self.bucket_name}/{raw_name}"
            except Exception as e:
                day_results[day_str] = {"error": f"upload_raw_failed: {e}", "raw_name": raw_name}
                cur = cur + dt.timedelta(days=1)
                total_days += 1
                continue

            # flatten
            try:
                df = flatten_energy(resp.json(), stamp, location)
            except Exception as e:
                day_results[day_str] = {"error": f"flatten_failed: {e}", "day": day_str}
                cur = cur + dt.timedelta(days=1)
                total_days += 1
                continue

            per_run = f"{csv_prefix}energy_flat_{day_str}_{stamp}.csv"
            buf = io.StringIO(); df.to_csv(buf, index=False)
            try:
                self.bucket.blob(per_run).upload_from_string(buf.getvalue(), content_type="text/csv")
                csv_gs = f"gs://{self.bucket_name}/{per_run}"
            except Exception as e:
                day_results[day_str] = {"error": f"upload_csv_failed: {e}", "per_run": per_run}
                cur = cur + dt.timedelta(days=1)
                total_days += 1
                continue

            day_results[day_str] = {"raw_json": raw_gs, "per_run_csv": csv_gs, "rows_this_run": len(df)}
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
