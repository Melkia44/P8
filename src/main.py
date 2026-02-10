import os
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from utils import iter_s3_jsonl, s3_put_json, s3_put_jsonl, to_int, to_float
from stations import STATIONS, SOURCE_TAG_TO_STATION_ID

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX_RAW = os.getenv("S3_PREFIX_RAW", "raw/dataset_meteo/")
S3_PREFIX_OUT = os.getenv("S3_PREFIX_OUT", "processed/dataset_meteo/")
SOURCE_TAG = os.getenv("SOURCE_TAG", "UNKNOWN")

OUT_DIR = os.getenv("OUT_DIR", "output")
os.makedirs(OUT_DIR, exist_ok=True)

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
    s = "" if v is None else str(v).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None

def record_hash(source: str, station_id: str, obs_dt: datetime, d: Dict[str, Any]) -> str:
    key = f"{source}|{station_id}|{obs_dt.isoformat()}|{d.get('temperature_c')}|{d.get('humidity_pct')}|{d.get('pressure_hpa')}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

def transform_record_to_docs(rec: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = rec.get("_airbyte_data")
    if not isinstance(data, dict):
        return []

    docs: List[Dict[str, Any]] = []

    # A) WU flat (issu Excel)
    if "Time" in data and "Temperature" in data:
        extracted = rec.get("_airbyte_extracted_at")
        if extracted is None:
            return []

        station_id = SOURCE_TAG_TO_STATION_ID.get(SOURCE_TAG, SOURCE_TAG)
        obs_dt = obs_dt_from_extracted(int(extracted), data.get("Time"))

        temp_f = to_float(data.get("Temperature"))
        dew_f = to_float(data.get("Dew Point"))
        press_in = to_float(data.get("Pressure"))
        speed_mph = to_float(data.get("Speed"))
        gust_mph = to_float(data.get("Gust"))
        pr_in = to_float(data.get("Precip. Rate."))
        pa_in = to_float(data.get("Precip. Accum."))

        d = {
            "obs_datetime": obs_dt.isoformat(),
            "station_id": station_id,
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
            "ingestion_ts": datetime.now(timezone.utc).isoformat(),
        }
        d["record_hash"] = record_hash(d["source"], d["station_id"], obs_dt, d)
        docs.append(d)
        return docs

    # B) hourly bundle (JSON)
    hourly = data.get("hourly")
    if isinstance(hourly, dict):
        for station_id, rows in hourly.items():
            if station_id == "_params" or not isinstance(rows, list):
                continue
            for r in rows:
                if not isinstance(r, dict):
                    continue
                obs_dt = parse_dh_utc(r.get("dh_utc"))
                if obs_dt is None:
                    continue
                d = {
                    "obs_datetime": obs_dt.isoformat(),
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
                    "ingestion_ts": datetime.now(timezone.utc).isoformat(),
                }
                d["record_hash"] = record_hash(d["source"], d["station_id"], obs_dt, d)
                docs.append(d)

    return docs

def main():
    obs_out_path = os.path.join(OUT_DIR, "observations.jsonl")
    stations_out_path = os.path.join(OUT_DIR, "stations.json")

    stats = {"lines": 0, "json_ok": 0, "docs": 0, "rejected": 0}

    with open(obs_out_path, "w", encoding="utf-8") as f_out:
        for key, line in iter_s3_jsonl(S3_BUCKET, S3_PREFIX_RAW):
            stats["lines"] += 1
            try:
                rec = json.loads(line)
                stats["json_ok"] += 1
            except json.JSONDecodeError:
                stats["rejected"] += 1
                continue

            docs = transform_record_to_docs(rec)
            if not docs:
                stats["rejected"] += 1
                continue

            for d in docs:
                f_out.write(json.dumps(d, ensure_ascii=False) + "\n")
            stats["docs"] += len(docs)

    # export stations (fixe)
    with open(stations_out_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(STATIONS, ensure_ascii=False, indent=2))

    # upload vers S3 (Step 1)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    obs_s3_key = f"{S3_PREFIX_OUT}{SOURCE_TAG}/observations_{ts}.jsonl"
    stations_s3_key = f"{S3_PREFIX_OUT}{SOURCE_TAG}/stations.json"

    with open(obs_out_path, "r", encoding="utf-8") as f:
        s3_put_jsonl(S3_BUCKET, obs_s3_key, (l.rstrip("\n") for l in f))

    s3_put_json(S3_BUCKET, stations_s3_key, STATIONS)

    logging.info(f"STATS: {stats}")
    logging.info(f"OUTPUT local: {obs_out_path} | {stations_out_path}")
    logging.info(f"OUTPUT s3: s3://{S3_BUCKET}/{obs_s3_key}")
    logging.info(f"OUTPUT s3: s3://{S3_BUCKET}/{stations_s3_key}")

if __name__ == "__main__":
    main()
