import os
import json
import re
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Iterable, Tuple

import boto3
from pymongo import MongoClient, ASCENDING, errors as pymongo_errors
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

S3_BUCKET = os.getenv("S3_BUCKET", "oc-meteo-staging-data")
S3_PREFIX = os.getenv("S3_PREFIX_RAW", "raw/dataset_meteo/")
SOURCE_TAG = os.getenv("SOURCE_TAG", "UNKNOWN")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "meteo")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "observations")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

NULL_LIKE = {"", "none", "null", "nan", "na", "n/a", "-", "—"}


def clean_nb(v: Any) -> str:
    s = "" if v is None else str(v)
    s = s.replace("\u00a0", " ").strip().lower()
    if s in NULL_LIKE:
        return ""
    return re.sub(r"[^\d\.\-]", "", s)

def to_int(v: Any) -> Optional[int]:
    s = clean_nb(v)
    if not s: return None
    try: return int(float(s))
    except ValueError: return None

def to_float(v: Any) -> Optional[float]:
    s = clean_nb(v)
    if not s: return None
    try: return float(s)
    except ValueError: return None

def f_to_c(f: float) -> float: return (f - 32.0) * 5.0 / 9.0
def inhg_to_hpa(x: float) -> float: return x * 33.8638866667
def mph_to_kmh(x: float) -> float: return x * 1.609344
def inch_to_mm(x: float) -> float: return x * 25.4


def obs_dt_from_extracted(extracted_ms: int, hhmmss: Optional[str]) -> datetime:
    base = datetime.fromtimestamp(extracted_ms / 1000.0, tz=timezone.utc)
    if not hhmmss:
        return base
    try:
        t = datetime.strptime(str(hhmmss).strip(), "%H:%M:%S").time()
        return datetime(base.year, base.month, base.day, t.hour, t.minute, t.second, tzinfo=timezone.utc)
    except ValueError:
        return base

def parse_dh_utc(v: Any) -> Optional[datetime]:
    s = ("" if v is None else str(v)).strip()
    if not s or s.lower() in NULL_LIKE:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


def record_hash(source: str, station_id: str, obs_dt: datetime, d: Dict[str, Any]) -> str:
    key = f"{source}|{station_id}|{obs_dt.isoformat()}|{d.get('temperature_c')}|{d.get('humidity_pct')}|{d.get('pressure_hpa')}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def transform(rec: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = rec.get("_airbyte_data")
    if not isinstance(data, dict):
        return []

    # 1) WU "flat"
    if "Time" in data and "Temperature" in data:
        extracted = rec.get("_airbyte_extracted_at")
        if extracted is None:
            return []
        obs_dt = obs_dt_from_extracted(int(extracted), data.get("Time"))

        temp_f = to_float(data.get("Temperature"))
        dew_f = to_float(data.get("Dew Point"))
        press_in = to_float(data.get("Pressure"))
        speed_mph = to_float(data.get("Speed"))
        gust_mph = to_float(data.get("Gust"))
        pr_in = to_float(data.get("Precip. Rate."))
        pa_in = to_float(data.get("Precip. Accum."))

        doc = {
            "obs_datetime": obs_dt,
            "station_id": SOURCE_TAG,
            "station_provider": "WU",
            "humidity_pct": to_int(data.get("Humidity")),
            "temperature_c": f_to_c(temp_f) if temp_f is not None else None,
            "dew_point_c": f_to_c(dew_f) if dew_f is not None else None,
            "pressure_hpa": inhg_to_hpa(press_in) if press_in is not None else None,
            "wind_dir": data.get("Wind"),
            "wind_speed_kmh": mph_to_kmh(speed_mph) if speed_mph is not None else None,
            "wind_gust_kmh": mph_to_kmh(gust_mph) if gust_mph is not None else None,
            "precip_rate_mm": inch_to_mm(pr_in) if pr_in is not None else None,
            "precip_accum_mm": inch_to_mm(pa_in) if pa_in is not None else None,
            "source": SOURCE_TAG,
            "sync_id": rec.get("_airbyte_meta", {}).get("sync_id"),
            "ingestion_ts": datetime.now(timezone.utc),
        }
        doc["record_hash"] = record_hash(doc["source"], doc["station_id"], obs_dt, doc)
        return [doc]

    # 2) Bundle "hourly" multi-stations
    hourly = data.get("hourly")
    if isinstance(hourly, dict):
        out: List[Dict[str, Any]] = []
        for station_id, rows in hourly.items():
            if station_id == "_params" or not isinstance(rows, list):
                continue
            for r in rows:
                if not isinstance(r, dict):
                    continue
                obs_dt = parse_dh_utc(r.get("dh_utc"))
                if obs_dt is None:
                    continue
                doc = {
                    "obs_datetime": obs_dt,
                    "station_id": station_id,
                    "station_provider": "API_HOURLY",
                    "temperature_c": to_float(r.get("temperature")),
                    "pressure_hpa": to_float(r.get("pression")),
                    "humidity_pct": to_int(r.get("humidite")),
                    "dew_point_c": to_float(r.get("point_de_rosee")),
                    "wind_speed_kmh": to_float(r.get("vent_moyen")),
                    "wind_gust_kmh": to_float(r.get("vent_rafales")),
                    "wind_dir": to_int(r.get("vent_direction")),
                    "precip_1h_mm": to_float(r.get("pluie_1h")),
                    "precip_3h_mm": to_float(r.get("pluie_3h")),
                    "source": SOURCE_TAG,
                    "sync_id": rec.get("_airbyte_meta", {}).get("sync_id"),
                    "ingestion_ts": datetime.now(timezone.utc),
                }
                doc["record_hash"] = record_hash(doc["source"], doc["station_id"], obs_dt, doc)
                out.append(doc)
        return out

    return []


def iter_s3_jsonl(bucket: str, prefix: str) -> Iterable[Tuple[str, str]]:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".jsonl"):
                continue
            logging.info(f"Reading s3://{bucket}/{key}")
            for line in s3.get_object(Bucket=bucket, Key=key)["Body"].iter_lines():
                if line:
                    yield key, line.decode("utf-8", errors="replace")


def flush(col, buf: List[Dict[str, Any]], stats: Dict[str, int]):
    if not buf:
        return
    try:
        res = col.insert_many(buf, ordered=False)
        stats["inserted"] += len(res.inserted_ids)
    except pymongo_errors.BulkWriteError as bwe:
        errs = bwe.details.get("writeErrors", [])
        dup = sum(1 for e in errs if e.get("code") == 11000)
        stats["duplicates"] += dup
        stats["rejected"] += (len(errs) - dup)
        stats["inserted"] += bwe.details.get("nInserted", 0)
    finally:
        buf.clear()


def main():
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]

    col.create_index([("obs_datetime", ASCENDING)])
    col.create_index([("station_id", ASCENDING), ("obs_datetime", ASCENDING)])
    col.create_index([("record_hash", ASCENDING)], unique=True)

    stats = {"lines": 0, "json_ok": 0, "docs": 0, "inserted": 0, "duplicates": 0, "rejected": 0}
    buf: List[Dict[str, Any]] = []

    for key, line in iter_s3_jsonl(S3_BUCKET, S3_PREFIX):
        stats["lines"] += 1
        try:
            rec = json.loads(line)
            stats["json_ok"] += 1
        except json.JSONDecodeError:
            stats["rejected"] += 1
            continue

        docs = transform(rec)
        if not docs:
            stats["rejected"] += 1
            continue

        stats["docs"] += len(docs)
        buf.extend(docs)

        if len(buf) >= BATCH_SIZE:
            flush(col, buf, stats)

    flush(col, buf, stats)
    logging.info(f"STATS: {stats}")


if __name__ == "__main__":
    main()
