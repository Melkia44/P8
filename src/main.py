import os
import json
import re
import hashlib
import logging
from datetime import datetime, timezone

import boto3
from pymongo import MongoClient, ASCENDING
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

S3_BUCKET = os.getenv("S3_BUCKET", "oc-meteo-staging-data")
S3_PREFIX = os.getenv("S3_PREFIX_RAW", "raw/dataset_meteo/")
SOURCE_TAG = os.getenv("SOURCE_TAG", "UNKNOWN")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "meteo")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "observations")

def _clean_nb(s: str) -> str:
    # retire espaces (y compris insécables) et garde chiffres, point, signe
    return re.sub(r"[^\d\.\-]", "", s.replace("\u00a0", " ").strip())

def parse_percent(v):
    if v is None: return None
    s = _clean_nb(str(v))
    return int(float(s)) if s else None

def parse_float(v):
    if v is None: return None
    s = _clean_nb(str(v))
    return float(s) if s else None

def f_to_c(f):
    return (f - 32.0) * 5.0 / 9.0

def inhg_to_hpa(inhg):
    return inhg * 33.8638866667

def mph_to_kmh(mph):
    return mph * 1.609344

def inch_to_mm(inch):
    return inch * 25.4

def obs_datetime_from(extracted_at_ms: int, hhmmss: str):
    # date = extracted_at (UTC), heure = champ "Time" (00:04:00)
    base = datetime.fromtimestamp(extracted_at_ms / 1000.0, tz=timezone.utc)
    if not hhmmss:
        return base
    try:
        t = datetime.strptime(hhmmss.strip(), "%H:%M:%S").time()
        return datetime(base.year, base.month, base.day, t.hour, t.minute, t.second, tzinfo=timezone.utc)
    except ValueError:
        return base

def make_hash(source: str, obs_dt: datetime, payload: dict) -> str:
    # hash stable pour dédoublonner
    key = f"{source}|{obs_dt.isoformat()}|{payload.get('temperature_c')}|{payload.get('humidity_pct')}|{payload.get('pressure_hpa')}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

def transform_airbyte_record(rec: dict) -> dict | None:
    data = rec.get("_airbyte_data")
    if not isinstance(data, dict):
        return None

    extracted_at = rec.get("_airbyte_extracted_at")
    if extracted_at is None:
        return None

    obs_dt = obs_datetime_from(int(extracted_at), data.get("Time"))

    # parsing unités
    humidity = parse_percent(data.get("Humidity"))                      # "87 %"
    temp_f = parse_float(data.get("Temperature"))                       # "56.8 °F"
    dew_f = parse_float(data.get("Dew Point"))                          # "53.1 °F"
    press_in = parse_float(data.get("Pressure"))                        # "29.48 in"
    speed_mph = parse_float(data.get("Speed"))                          # "8.2 mph"
    gust_mph = parse_float(data.get("Gust"))                            # "10.4 mph"
    pr_in = parse_float(data.get("Precip. Rate."))                      # "0.00 in"
    pa_in = parse_float(data.get("Precip. Accum."))                     # "0.00 in"
    solar = parse_float(data.get("Solar"))                              # "0 w/m²"
    uv = data.get("UV")

    doc = {
        "obs_datetime": obs_dt,
        "humidity_pct": humidity,
        "temperature_c": f_to_c(temp_f) if temp_f is not None else None,
        "dew_point_c": f_to_c(dew_f) if dew_f is not None else None,
        "pressure_hpa": inhg_to_hpa(press_in) if press_in is not None else None,
        "wind_dir": data.get("Wind"),
        "wind_speed_kmh": mph_to_kmh(speed_mph) if speed_mph is not None else None,
        "wind_gust_kmh": mph_to_kmh(gust_mph) if gust_mph is not None else None,
        "precip_rate_mm": inch_to_mm(pr_in) if pr_in is not None else None,
        "precip_accum_mm": inch_to_mm(pa_in) if pa_in is not None else None,
        "solar_wm2": int(solar) if solar is not None else None,
        "uv_index": int(uv) if uv is not None and str(uv).strip() != "" else None,
        "source": SOURCE_TAG,
        "sync_id": rec.get("_airbyte_meta", {}).get("sync_id"),
        "ingestion_ts": datetime.now(timezone.utc),
    }

    doc["record_hash"] = make_hash(SOURCE_TAG, obs_dt, doc)
    return doc

def iter_s3_jsonl(bucket: str, prefix: str):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".jsonl"):
                continue
            logging.info(f"Reading S3 object: s3://{bucket}/{key}")
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].iter_lines()
            for line in body:
                if not line:
                    continue
                yield key, line.decode("utf-8", errors="replace")

def main():
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]

    # index utiles jury
    col.create_index([("obs_datetime", ASCENDING)])
    col.create_index([("record_hash", ASCENDING)], unique=True)

    stats = {
        "lines_read": 0,
        "json_valid": 0,
        "rejected": 0,
        "inserted": 0,
        "duplicates": 0,
    }

    for key, line in iter_s3_jsonl(S3_BUCKET, S3_PREFIX):
        stats["lines_read"] += 1
        try:
            rec = json.loads(line)
            stats["json_valid"] += 1
        except json.JSONDecodeError:
            stats["rejected"] += 1
            continue

        doc = transform_airbyte_record(rec)
        if doc is None:
            stats["rejected"] += 1
            continue

        try:
            col.insert_one(doc)
            stats["inserted"] += 1
        except Exception as e:
            # doublon record_hash => OK (industrialisation minimale)
            if "duplicate key error" in str(e):
                stats["duplicates"] += 1
            else:
                stats["rejected"] += 1

    logging.info(f"STATS: {stats}")

if __name__ == "__main__":
    main()
