from typing import Iterable, Dict, Any, List, Tuple, Optional
from datetime import datetime, timedelta, timezone
import os
import requests, pytz, logging
from google.cloud import storage

# Allow override of the per-request timeout via environment variable for Cloud Run tuning
DEFAULT_TIMEOUT = int(os.getenv("NWS_REQUEST_TIMEOUT", "30"))
EASTERN = pytz.timezone("US/Eastern")
logging.getLogger(__name__).setLevel(logging.INFO)

# Fetch all product codes from /products/types
def _all_product_codes(sess: requests.Session) -> List[str]:
    """Return list of all product codes from /products/types."""
    url = "https://api.weather.gov/products/types"
    r = sess.get(url, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    codes = []
    for item in r.json().get("@graph", []):
        if "productCode" in item and item["productCode"]:
            codes.append(item["productCode"])
        elif "@id" in item:
            codes.append(item["@id"].split("/")[-1])
    # de-dupe & sort for stable order
    return sorted(set(codes))

# Iterate through all products for one type & office, following pagination
def _iter_products_for_type(
    sess: requests.Session,
    headers: Dict[str, str],
    product_code: str,
    office: str,
) -> Iterable[Dict[str, Any]]:
    """
    Yield items for one product type & office.
    NOTE: Do NOT add ?limit=... (NWS returns 400); follow pagination.next instead.
    """
    url = f"https://api.weather.gov/products/types/{product_code}/locations/{office}"
    while url:
        r = sess.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
        if r.status_code == 400:
            # Many type/office combos are invalid; let caller decide to skip
            r.raise_for_status()
        r.raise_for_status()
        data = r.json()
        for item in data.get("@graph", []):
            yield item
        url = (data.get("pagination") or {}).get("next")

# Convert issuanceTime (ISO 8601 UTC) to "YYYY-MM-DD_HH-MM-SS" in US/Eastern
def _stamp_from_issuance(iso_dt: str) -> str:
    utc = datetime.fromisoformat(iso_dt.replace("Z", "+00:00"))
    edt = utc.astimezone(EASTERN)
    return edt.strftime("%Y-%m-%d_%H-%M-%S")

# List existing files in GCS bucket for this product type
def _existing_stamps(bucket: storage.Bucket, product: str) -> set:
    prefix = f"nws_text/{product}/"
    stamps = set()
    for b in bucket.list_blobs(prefix=prefix):
        name = b.name.rsplit("/", 1)[-1]
        if name.endswith(".txt"):
            stamps.add(name[:-4])
    return stamps

# For one product type & office, save missing files to GCS bucket
def _save_missing_for_type(
    project_id: str,
    bucket_name: str,
    office: str,
    product: str,
    sess: requests.Session,
    headers: Dict[str, str],
    since_utc: Optional[datetime] = None,
) -> Tuple[int, List[str]]:
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    existing = _existing_stamps(bucket, product)

    saved = 0
    errors: List[str] = []

    for item in _iter_products_for_type(sess, headers, product, office):
        try:
            issued_iso = item["issuanceTime"]
            issued_utc = datetime.fromisoformat(issued_iso.replace("Z", "+00:00"))
            if since_utc and issued_utc < since_utc:
                # Listings are newest-first; once older than window, stop early
                break

            stamp = _stamp_from_issuance(issued_iso)
            if stamp in existing:
                continue

            detail = sess.get(item["@id"], headers=headers, timeout=DEFAULT_TIMEOUT)
            detail.raise_for_status()
            text = detail.json().get("productText", "")
            if not text:
                continue

            path = f"nws_text/{product}/{stamp}.txt"
            bucket.blob(path).upload_from_string(text, content_type="text/plain")
            existing.add(stamp)
            saved += 1
        except Exception as ex:
            errors.append(f"{product}: {ex}")

    return saved, errors

# Resolve lat/lon to NWS office code (e.g. "BOX")
def _resolve_office(sess: requests.Session, headers: Dict[str, str], lat: float, lon: float) -> str:
    r = sess.get(f"https://api.weather.gov/points/{lat},{lon}", headers=headers, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return r.json()["properties"]["cwa"]  # e.g. "BOX"

# Main function to scrape missing versions for given products or all types
def scrape_missing_versions(
    *,
    project_id: str,
    bucket_name: str,
    lat: float,
    lon: float,
    user_agent: str,
    products: Optional[List[str]] = None,
    backfill_hours: int = 24,
    all_types: bool = False,
) -> Dict[str, Any]:
    """
    Save only missing files.
    - If all_types=True, it scans EVERY code from /products/types.
    - Else, provide `products` (e.g. ["AFD","HWO",...]).
    - backfill_hours controls how far back to look.
    """
    sess = requests.Session()
    headers = {"User-Agent": user_agent}
    office = _resolve_office(sess, headers, lat, lon)

    # Decide product list: only fetch ALL codes when explicitly requested.
    if all_types:
        logging.info("all_types=True, fetching all product codes from NWS API")
        product_list = _all_product_codes(sess)
    else:
        # Prefer caller-specified list; if none provided, use a small, safe default
        if products:
            product_list = [p.strip().upper() for p in products if str(p).strip()]
        else:
            # Safe default set of common product types to limit runtime
            product_list = [
                "AFD",
                "HWO",
                "MTR",
            ]
            logging.info("Using safe default product list to limit runtime: %s", product_list)

    since_utc = datetime.now(timezone.utc) - timedelta(hours=backfill_hours)

    total_saved = 0
    skipped: List[str] = []
    all_errors: List[str] = []

    for code in product_list:
        try:
            s, errs = _save_missing_for_type(
                project_id, bucket_name, office, code, sess, headers, since_utc=since_utc
            )
            total_saved += s
            all_errors.extend(errs)
        except requests.HTTPError as he:
            # 400 means that type isn�t valid for this office � record & move on
            skipped.append(f"{code}: {he}")
        except Exception as ex:
            all_errors.append(f"{code}: {ex}")

    return {
        "office": office,
        "since_utc": since_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "checked_types": len(product_list),
        "saved_files": total_saved,
        "skipped_types": skipped,   # typically lots of 400s
        "errors": all_errors,
    }
