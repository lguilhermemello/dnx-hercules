import os
import time
import logging
from typing import Optional, Dict, Any
import azure.functions as func
from twilio.twiml.messaging_response import MessagingResponse
import pandas as pd
import requests
from geopy.geocoders import Nominatim

# =========================================================
# CONFIGURATION
# =========================================================

CSV_FILE_PATH = "/tmp/burn_ban_data.csv"  # IMPORTANTE no Azure
COUNTY_COLUMN_EXACT = "County"
STATUS_COLUMN_EXACT = "Burn Ban"

CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", str(60 * 60)))  # 1 hour

BURNBAN_MAPSERVER_URL = (
    "https://tfsgis.tfs.tamu.edu/arcgis/rest/services/BurnBan/BurnBan/"
    "MapServer/0/query"
    "?where=1%3D1"
    "&outFields=County%2CStartDate%2CBurnBan%2CCountyID"
    "&f=json"
)

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("burnban")

_cache: Dict[str, Any] = {
    "last_download": 0
}

# =========================================================
# DOWNLOAD OFFICIAL BURN BAN TABLE
# =========================================================

def baixar_tabela_oficial(force: bool = False) -> None:
    now = time.time()
    last = _cache.get("last_download", 0)

    if not force and (now - last) < CACHE_TTL and os.path.exists(CSV_FILE_PATH):
        logger.info("Using cached burn ban table.")
        return

    logger.info("Downloading official Texas burn ban data...")

    r = requests.get(BURNBAN_MAPSERVER_URL, timeout=20)
    r.raise_for_status()

    data = r.json()
    features = data.get("features", [])

    if not features:
        raise RuntimeError("MapServer API returned no data.")

    records = []

    for f in features:
        attrs = f.get("attributes", {})
        start_date_raw = attrs.get("StartDate")

        if start_date_raw:
            try:
                start_date = pd.to_datetime(
                    start_date_raw, unit="ms"
                ).strftime("%m/%d/%Y")
            except Exception:
                start_date = ""
        else:
            start_date = ""

        records.append({
            "County": str(attrs.get("County", "")).strip(),
            "Burn Ban": str(attrs.get("BurnBan", "")).strip(),
            "Date": start_date,
            "CountyID": attrs.get("CountyID", "")
        })

    df = pd.DataFrame(records)
    df.to_csv(CSV_FILE_PATH, sep=";", index=False, encoding="utf-8")

    _cache["last_download"] = now
    logger.info("Burn ban table saved successfully.")

# =========================================================
# ZIP → COUNTY
# =========================================================

_geolocator: Optional[Nominatim] = None

def get_geolocator() -> Nominatim:
    global _geolocator
    if _geolocator is None:
        _geolocator = Nominatim(user_agent="burnban_app")
    return _geolocator

def zip_to_county(zip_code: str) -> Optional[str]:
    try:
        geolocator = get_geolocator()

        location = geolocator.geocode(
            {"postalcode": zip_code, "country": "USA"},
            timeout=10
        )

        if not location:
            return None

        lat = location.latitude
        lon = location.longitude

        fcc_url = (
            "https://geo.fcc.gov/api/census/block/find"
            f"?format=json&latitude={lat}&longitude={lon}"
        )

        fcc_data = requests.get(fcc_url, timeout=10).json()
        county = fcc_data.get("County", {}).get("name")

        if county:
            return county.replace("County", "").strip()

        return None

    except Exception as e:
        logger.exception("ZIP to county error: %s", e)
        return None

# =========================================================
# BURN BAN LOGIC
# =========================================================

def verificar_condado_burnban(target_county: str) -> Dict[str, Any]:
    try:
        baixar_tabela_oficial()
    except Exception as e:
        return {"can_burn": None, "start_date": "", "error": str(e)}

    df = pd.read_csv(CSV_FILE_PATH, sep=";")
    df["Normalized_County"] = df[COUNTY_COLUMN_EXACT].str.upper().str.strip()

    row = df[df["Normalized_County"] == target_county.upper()]

    if row.empty:
        return {"can_burn": None, "start_date": "", "error": "County not found"}

    status = row[STATUS_COLUMN_EXACT].iloc[0].upper()
    start_date = row["Date"].iloc[0]

    if status == "YES":
        return {"can_burn": False, "start_date": start_date, "error": ""}
    if status == "NO":
        return {"can_burn": True, "start_date": "", "error": ""}

    return {"can_burn": None, "start_date": "", "error": "Unexpected status"}

# =========================================================
# POWER AUTOMATE
# =========================================================

def enviar_para_power_automate(payload: Dict[str, Any]) -> None:
    try:
        url = os.getenv("POWER_AUTOMATE_URL")
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.exception("Power Automate error: %s", e)

# =========================================================
# AZURE FUNCTION ENTRYPOINT (TWILIO)
# =========================================================

def main(req: func.HttpRequest) -> func.HttpResponse:
    user_msg = req.form.get("Body", "").strip()
    from_number = req.form.get("From", "")

    resp = MessagingResponse()

    zipcode = "".join(filter(str.isdigit, user_msg))

    if len(zipcode) != 5:
        resp.message("Please send a valid U.S. ZIP Code (example: 78701).")
        return func.HttpResponse(str(resp), mimetype="application/xml")

    county = zip_to_county(zipcode)

    if not county:
        resp.message("Could not determine the county for this ZIP Code.")
        return func.HttpResponse(str(resp), mimetype="application/xml")

    result = verificar_condado_burnban(county)

    enviar_para_power_automate({
        "phone_number": from_number,
        "zipcode": zipcode,
        "county": county,
        "burn_ban": result["can_burn"],
        "startDateBurn": result["start_date"],
        "timestamp": time.time(),
        "source": "twilio_sms"
    })

    if result["can_burn"] is True:
        text = f"County: {county}\nThis location is not under a burn ban."
    elif result["can_burn"] is False:
        text = (
            f"County: {county}\n"
            f"This location is under a burn ban.\n"
            f"Ban started on {result['start_date']}."
        )
    else:
        text = f"County: {county}\nUnable to determine burn status."

    resp.message(text)
    return func.HttpResponse(str(resp), mimetype="application/xml")
