import json
import os
import functions_framework
from flask import Request
from textminer.products import scrape_missing_versions

# Cloud Function entry point
@functions_framework.http
def scrape_nws_text(request: Request):
    # Env
    project_id = os.getenv("PROJECT_ID", "")
    bucket = os.getenv("GCS_BUCKET", "")
    lat = float(os.getenv("LAT", "42.3601"))
    lon = float(os.getenv("LON", "-71.0589"))
    default_hours = int(os.getenv("BACKFILL_HOURS", "24"))

    # Defaults
    hours = default_hours
    all_types = True
    products = None  # e.g. ["AFD","HWO"]

    # Accept JSON body (Scheduler can send "{}"), or query params for ad-hoc tests
    try:
        if request.is_json:
            body = request.get_json(silent=True) or {}
            hours = int(body.get("hours", default_hours))
            all_types = bool(body.get("all_types", True))
            p = body.get("products")
            if isinstance(p, list):
                products = [str(x).strip().upper() for x in p if str(x).strip()]
        else:
            if request.args.get("hours"):
                hours = int(request.args.get("hours"))
            if request.args.get("all_types"):
                all_types = request.args.get("all_types").lower() in ("1","true","yes","y")
            if request.args.get("products"):
                products = [x.strip().upper() for x in request.args.get("products").split(",") if x.strip()]
    except Exception:
        # fall back to defaults if input is messy
        hours = default_hours

    try:
        result = scrape_missing_versions(
            project_id=project_id,
            bucket_name=bucket,
            lat=lat,
            lon=lon,
            products=products,
            backfill_hours=hours,
            all_types=all_types,
        )
        return (json.dumps(result), 200, {"Content-Type": "application/json"})
    except Exception as e:
        # Make failures visible in logs/Scheduler
        err = {"error": str(e)}

        return (json.dumps(err), 500, {"Content-Type": "application/json"})
