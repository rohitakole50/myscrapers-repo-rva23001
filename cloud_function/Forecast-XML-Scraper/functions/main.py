import os
import functions_framework
from scraper.pipeline import Pipeline

# Energy configuration (minimal, non-intrusive)
ENERGY_RAW_PREFIX = os.getenv("ENERGY_RAW_PREFIX", "nws_energy_raw/")
ENERGY_CSV_PREFIX = os.getenv("ENERGY_CSV_PREFIX", "nws_energy_flat/")
ENERGY_URL_BASE = os.getenv("ENERGY_URL_BASE", "https://webservices.iso-ne.com/api/v1.1/realtimehourlydemand")
ENERGY_START_DATE = os.getenv("ENERGY_START_DATE", "")
ENERGY_LOCATION = os.getenv("ENERGY_LOCATION", "4004")
# ISO-NE credentials for Basic Auth
ISO_NE_USER = os.getenv("ISO_NE_USER", "")
ISO_NE_PASS = os.getenv("ISO_NE_PASS", "")

# ---- Env config (override in deploy flags) ----
PROJECT_ID = os.getenv("PROJECT_ID", "")
GCS_BUCKET = os.getenv("GCS_BUCKET", "")
RAW_PREFIX = os.getenv("RAW_PREFIX", "nws_raw/")
CSV_PREFIX = os.getenv("CSV_PREFIX", "nws_flat/")

LAT = float(os.getenv("LAT", "41.94"))
LON = float(os.getenv("LON", "-72.685"))
FCST_TYPE = os.getenv("FCST_TYPE", "digitalDWML")

DWML_URL = (
    f"https://forecast.weather.gov/MapClick.php?"
    f"lat={LAT}&lon={LON}&unit=0&lg=english&FcstType={FCST_TYPE}"
)

pipe = Pipeline(
    project_id=PROJECT_ID,
    bucket_name=GCS_BUCKET,
    raw_prefix=RAW_PREFIX,
    csv_prefix=CSV_PREFIX,
    lat=LAT, lon=LON,
    fcst_url=DWML_URL
)

@functions_framework.http
def scrape_dwml(request):
    result = pipe.run_once()
    # run the energy fetch range in addition to DWML (non-destructive addition)
    try:
        if GCS_BUCKET:
            # pass creds (if provided) to pipeline runner
            energy_res = pipe.run_energy_range(
                start_date=ENERGY_START_DATE,
                location=ENERGY_LOCATION,
                url_base=ENERGY_URL_BASE,
                raw_prefix=ENERGY_RAW_PREFIX,
                csv_prefix=ENERGY_CSV_PREFIX,
                iso_user=ISO_NE_USER or None,
                iso_pass=ISO_NE_PASS or None,
            )
        else:
            energy_res = {"skipped": "no GCS_BUCKET configured"}
    except Exception as e:
        energy_res = {"error": str(e)}


    return {"dwml": result, "energy": energy_res}
