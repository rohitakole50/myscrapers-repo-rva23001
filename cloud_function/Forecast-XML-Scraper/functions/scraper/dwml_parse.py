import io
import pandas as pd
import requests
from lxml import etree
from dateutil import parser as dtparser
import json
from typing import Any, List
import logging
from json import JSONDecodeError

logger = logging.getLogger(__name__)

def fetch_dwml(url: str, timeout: int = 60) -> requests.Response:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r

def _time_map(root) -> dict:
    m = {}
    for tl in root.xpath(".//time-layout"):
        key = tl.xpath("./layout-key/text()")[0]
        times = [pd.Timestamp(dtparser.parse(t)).tz_convert("UTC")
                 for t in tl.xpath("./start-valid-time/text()")]
        m[key] = times
    return m

def flatten_dwml(resp_bytes: bytes, stamp_utc: str, lat: float, lon: float) -> pd.DataFrame:
    """
    Returns a wide DataFrame with:
      scrape_time_utc, location_lat, location_lon, forecast_time_utc, and many feature columns.
    One row per forecast timestamp across ALL series in the XML.
    """
    root = etree.fromstring(resp_bytes)
    tmap = _time_map(root)

    # collect all numeric series from all <parameters>
    series = []
    for params in root.xpath(".//parameters"):
        for node in params:
            if not isinstance(node.tag, str):
                continue
            tag = node.tag
            if tag in {"weather", "conditions-icon"}:
                continue
            if not node.xpath("./value"):
                continue
            tl = node.get("time-layout")
            if not tl or tl not in tmap:
                continue
            vals = [v.text for v in node.xpath("./value")]
            times = tmap[tl]
            n = min(len(vals), len(times))
            if n == 0:
                continue
            name = f"{tag}_{node.get('type')}" if node.get("type") else tag
            s = pd.Series(vals[:n], index=pd.to_datetime(times[:n], utc=True), dtype="object")
            s = s.replace({"": pd.NA, "NA": pd.NA, None: pd.NA})
            s = pd.to_numeric(s, errors="coerce")
            series.append((name, s))

    # weather flags (rain/thunder/fog)
    rain = pd.Series(dtype="boolean"); thunder = pd.Series(dtype="boolean"); fog = pd.Series(dtype="boolean")
    wnode = root.xpath(".//parameters/weather")
    if wnode:
        node = wnode[0]
        tl = node.get("time-layout")
        times = tmap.get(tl, [])
        conds = node.xpath("./weather-conditions")
        m = min(len(times), len(conds))
        if m:
            idx = pd.to_datetime(times[:m], utc=True)
            def flags(wc):
                txt = " ".join([
                    " ".join(wc.xpath(".//@weather-type")),
                    " ".join(wc.xpath(".//@intensity")),
                    " ".join(wc.xpath(".//@coverage")),
                    " ".join(wc.xpath(".//@additive")),
                ]).lower()
                return (
                    any(k in txt for k in ["rain","shower","drizzle"]),
                    any(k in txt for k in ["thunder","tstm","tstorm"]),
                    any(k in txt for k in ["fog","mist"])
                )
            fl = [flags(conds[i]) for i in range(m)]
            rain = pd.Series([x[0] for x in fl], index=idx, dtype="boolean")
            thunder = pd.Series([x[1] for x in fl], index=idx, dtype="boolean")
            fog = pd.Series([x[2] for x in fl], index=idx, dtype="boolean")

    # master index (union of all timestamps)
    all_idx = [s.index for _, s in series]
    for extra in (rain.index, thunder.index, fog.index):
        if len(extra): all_idx.append(extra)
    if not all_idx:
        raise RuntimeError("No timestamps found in DWML")
    master = pd.Index([]);  [master := master.union(ix) for ix in all_idx]
    master = master.sort_values()

    # wide frame
    df = pd.DataFrame(index=master)
    for name, s in series:
        df[name] = s.reindex(df.index)
    if len(rain):    df["weather_rain"] = rain.reindex(df.index)
    if len(thunder): df["weather_thunder"] = thunder.reindex(df.index)
    if len(fog):     df["weather_fog"] = fog.reindex(df.index)

    df.reset_index(inplace=True)
    df.rename(columns={"index":"forecast_time_utc"}, inplace=True)
    df.insert(0, "scrape_time_utc", pd.Timestamp(stamp_utc))
    df.insert(1, "location_lat", lat)
    df.insert(2, "location_lon", lon)

    # friendly names
    rename = {
        "temperature_hourly": "temp_F",
        "temperature_apparent": "heat_index_F",
        "dewpoint_hourly": "dewpoint_F",
        "wind-speed_sustained": "wind_speed_mph",
        "wind-speed_gust": "wind_gust_mph",
        "direction": "wind_dir_deg",
        "probability-of-precipitation": "pop_pct",
        "cloud-amount": "sky_cover_pct",
        "humidity_relative": "rh_pct",
        "pressure_sea-level": "pressure_hPa",
        "visibility": "visibility_mi",
        "cig": "ceiling_ft",
    }
    df.rename(columns={k:v for k,v in rename.items() if k in df.columns}, inplace=True)
    df.sort_values("forecast_time_utc", inplace=True)
    return df


def fetch_energy(url: str, iso_user: str = None, iso_pass: str = None, timeout: int = 60) -> requests.Response:
    """Fetch a per-day energy JSON from ISO-NE using HTTP Basic Auth when provided."""
    auth = (iso_user, iso_pass) if iso_user and iso_pass else None
    r = requests.get(url, auth=auth, timeout=timeout)
    r.raise_for_status()
    return r


def _find_record_list(obj: Any) -> List[dict]:
    """Given a JSON payload, heuristically find the list of records (dicts).

    Prefer a top-level list, then a top-level key named like `HourlyRtDemand`,
    otherwise any first list-valued field whose items look like dicts.
    """
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        # common field name
        if "HourlyRtDemand" in obj and isinstance(obj["HourlyRtDemand"], list):
            return obj["HourlyRtDemand"]
        # otherwise search for the first list of dicts
        for v in obj.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
    return []


def flatten_energy(resp_json: Any, stamp_utc: str, location_id: str) -> pd.DataFrame:
    """Flatten ISO-NE per-day JSON into a narrow DataFrame.

    Expected per-record shape (example):
      {"BeginDate": "2025-08-25T03:00:00.000-04:00",
       "Location": {"LocId": "4004", "Name": ".Z.CONNECTICUT"},
       "Load": 2552.328}

    This function produces columns: `scrape_time_utc`, `begin_date`,
    `location`, `location_id`, `load`.
    """
    # parse JSON if necessary. Be tolerant of empty or invalid payloads.
    if isinstance(resp_json, (bytes, str)):
        text = resp_json.decode() if isinstance(resp_json, bytes) else resp_json
        if not text or not text.strip():
            logger.warning("flatten_energy: empty response for location %s at %s", location_id, stamp_utc)
            return pd.DataFrame(columns=["scrape_time_utc", "begin_date", "location", "location_id", "load"])
        try:
            j = json.loads(text)
        except (JSONDecodeError, ValueError) as e:
            # Not JSON — try parsing as XML (ISO-NE sometimes returns XML payloads)
            try:
                root = etree.fromstring(text.encode("utf-8"))
                # find all HourlyRtDemand elements regardless of namespace
                nodes = root.xpath('.//*[local-name()="HourlyRtDemand"]')
                rows = []
                for node in nodes:
                    bd = node.xpath('.//*[local-name()="BeginDate"]/text()')
                    begin = bd[0] if bd else None
                    loc_node = node.xpath('.//*[local-name()="Location"]')
                    if loc_node:
                        loc_elem = loc_node[0]
                        loc_id = loc_elem.get('LocId') or loc_elem.get('LocID') or location_id
                        loc_name = (loc_elem.text or "").strip()
                    else:
                        loc_id = location_id
                        loc_name = ""
                    load_val = node.xpath('.//*[local-name()="Load"]/text()')
                    load = load_val[0] if load_val else None
                    rows.append({
                        "scrape_time_utc": pd.Timestamp(stamp_utc),
                        "begin_date": pd.to_datetime(begin, utc=True) if begin else pd.NaT,
                        "location": loc_name,
                        "location_id": loc_id,
                        "load": pd.to_numeric(load, errors="coerce"),
                    })
                if not rows:
                    logger.warning("flatten_energy: invalid JSON payload and no XML HourlyRtDemand nodes for location %s at %s: %s", location_id, stamp_utc, e)
                    return pd.DataFrame(columns=["scrape_time_utc", "begin_date", "location", "location_id", "load"])
                df_xml = pd.DataFrame(rows)
                df_xml.sort_values("begin_date", inplace=True)
                logger.info("flatten_energy: parsed XML payload with %d rows for location %s at %s", len(df_xml), location_id, stamp_utc)
                return df_xml
            except Exception:
                logger.warning("flatten_energy: invalid JSON payload for location %s at %s: %s", location_id, stamp_utc, e)
                return pd.DataFrame(columns=["scrape_time_utc", "begin_date", "location", "location_id", "load"])
    else:
        j = resp_json

    records = _find_record_list(j)
    rows = []
    for rec in records:
        # BeginDate may be under various keys
        begin = rec.get("BeginDate") or rec.get("Begin") or rec.get("beginDate")
        # Location may be dict or simple value
        loc = rec.get("Location")
        if isinstance(loc, dict):
            loc_id = loc.get("LocId") or loc.get("Id") or location_id
            # prefer name fields
            loc_name = loc.get("Name") or loc.get("Location") or ""
        else:
            loc_name = loc or ""
            loc_id = location_id
        # load value
        load = rec.get("Load") or rec.get("Value") or rec.get("load")
        rows.append({
            "scrape_time_utc": pd.Timestamp(stamp_utc),
            "begin_date": pd.to_datetime(begin, utc=True) if begin else pd.NaT,
            "location": loc_name,
            "location_id": loc_id,
            "load": pd.to_numeric(load, errors="coerce"),
        })

    if not rows:
        return pd.DataFrame(columns=["scrape_time_utc", "begin_date", "location", "location_id", "load"])
    df = pd.DataFrame(rows)
    df.sort_values("begin_date", inplace=True)

    return df

