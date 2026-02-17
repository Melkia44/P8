from __future__ import annotations

import argparse
import json
import logging
import os
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from dotenv import load_dotenv

import pymongo
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, PyMongoError


# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("load_mongodb")


# ============================================================
# PATHS + .env
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent  # racine P8

# Charge .env avant lecture env vars
load_dotenv(PROJECT_DIR / ".env")


# ============================================================
# DEFAULTS (repo)
# ============================================================
DEFAULT_INPUT = PROJECT_DIR / "data" / "airbyte" / "weather_data.jsonl"
DEFAULT_REPORT = SCRIPT_DIR / "mongodb_report.json"

DEFAULT_DB_NAME = "weather_db"
DEFAULT_COLLECTION_NAME = "weather_data"
DEFAULT_BATCH_SIZE = 500
DEFAULT_RESET_COLLECTION = True


# ============================================================
# SCHEMA VALIDATOR
# ============================================================
SCHEMA_VALIDATOR: Dict[str, Any] = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["source", "station_id", "station_name", "latitude", "longitude", "timestamp"],
        "properties": {
            "source": {"bsonType": "string", "enum": ["infoclimat", "weather_underground"]},
            "station_id": {"bsonType": "string"},
            "station_name": {"bsonType": "string"},
            "latitude": {"bsonType": "double", "minimum": -90, "maximum": 90},
            "longitude": {"bsonType": "double", "minimum": -180, "maximum": 180},
            "elevation": {"bsonType": ["double", "int", "null"]},
            "station_type": {"bsonType": ["string", "null"]},
            "timestamp": {"bsonType": "date"},
            "temperature_c": {"bsonType": ["double", "null"], "minimum": -60, "maximum": 60},
            "dew_point_c": {"bsonType": ["double", "null"]},
            "humidity_pct": {"bsonType": ["double", "null"], "minimum": 0, "maximum": 100},
            "wind_direction_deg": {"bsonType": ["double", "null"], "minimum": 0, "maximum": 360},
            "wind_speed_kmh": {"bsonType": ["double", "null"], "minimum": 0},
            "wind_gust_kmh": {"bsonType": ["double", "null"], "minimum": 0},
            "pressure_hpa": {"bsonType": ["double", "null"], "minimum": 870, "maximum": 1084},
            "precip_rate_mm": {"bsonType": ["double", "null"], "minimum": 0},
            "precip_accum_mm": {"bsonType": ["double", "null"], "minimum": 0},
            "visibility_m": {"bsonType": ["double", "null"], "minimum": 0},
            "cloud_cover_octas": {"bsonType": ["double", "int", "null"], "minimum": 0, "maximum": 8},
            "snow_depth_cm": {"bsonType": ["double", "null"], "minimum": 0},
            "weather_code": {"bsonType": ["string", "null"]},
            "uv_index": {"bsonType": ["double", "int", "null"], "minimum": 0},
            "solar_radiation_wm2": {"bsonType": ["double", "null"], "minimum": 0},
        },
    }
}


# ============================================================
# HELPERS
# ============================================================
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except ValueError:
            return None
    return None


def safe_float(x: Any) -> Any:
    return float(x) if isinstance(x, int) else x


def classify_error(err: Dict[str, Any]) -> str:
    code = err.get("code")
    msg = (err.get("errmsg") or "").lower()
    if code == 11000 or "duplicate key" in msg:
        return "duplicate_key"
    if code == 121 or "failed validation" in msg or "document failed validation" in msg:
        return "schema_validation"
    return "other"


def redact_mongo_uri(uri: str) -> str:
    """
    Evite d'afficher user:password dans les logs.
    """
    try:
        p = urlparse(uri)
        if p.username or p.password:
            netloc = p.hostname or ""
            if p.port:
                netloc += f":{p.port}"
            if p.username:
                netloc = f"{p.username}:***@{netloc}"
            return urlunparse((p.scheme, netloc, p.path, p.params, p.query, p.fragment))
        return uri
    except Exception:
        return uri


def ensure_direct_connection_if_needed(uri: str, force: bool) -> str:
    """
    Cas classique: replica set configuré avec hostname interne Docker (ex: mongodb:27017).
    Si le loader tourne sur l'host, on force directConnection pour éviter la découverte RS.
    """
    if not force:
        return uri

    p = urlparse(uri)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q["directConnection"] = "true"
    # On laisse replicaSet si présent, directConnection prendra le dessus côté client.
    new_query = urlencode(q)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))


@dataclass
class ImportStats:
    total_lines: int = 0
    total_parsed: int = 0
    total_submitted: int = 0
    total_inserted: int = 0
    total_errors: int = 0
    insertion_time_s: float = 0.0
    error_types: Counter = None
    errors_sample: List[Dict[str, Any]] = None
    parse_errors_sample: List[Dict[str, Any]] = None

    def __post_init__(self):
        self.error_types = Counter()
        self.errors_sample = []
        self.parse_errors_sample = []


# ============================================================
# JSONL LOADER (streaming)
# ============================================================
def iter_jsonl(filepath: Path) -> Iterable[Tuple[int, str]]:
    with filepath.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if raw:
                yield line_no, raw


def normalize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    rec["timestamp"] = parse_timestamp(rec.get("timestamp"))

    for key in [
        "latitude",
        "longitude",
        "elevation",
        "temperature_c",
        "dew_point_c",
        "humidity_pct",
        "wind_direction_deg",
        "wind_speed_kmh",
        "wind_gust_kmh",
        "pressure_hpa",
        "precip_rate_mm",
        "precip_accum_mm",
        "visibility_m",
        "snow_depth_cm",
        "solar_radiation_wm2",
    ]:
        if key in rec and rec[key] is not None:
            rec[key] = safe_float(rec[key])

    return rec


def load_batches(filepath: Path, batch_size: int, stats: ImportStats) -> Iterable[List[Dict[str, Any]]]:
    batch: List[Dict[str, Any]] = []

    for line_no, raw in iter_jsonl(filepath):
        stats.total_lines += 1
        try:
            rec = json.loads(raw)
        except json.JSONDecodeError as e:
            if len(stats.parse_errors_sample) < 10:
                stats.parse_errors_sample.append({"line": line_no, "error": str(e), "raw": raw[:200]})
            continue

        stats.total_parsed += 1
        batch.append(normalize_record(rec))

        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


# ============================================================
# MONGO OPS
# ============================================================
def connect_mongo(uri: str) -> MongoClient:
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client


def setup_collection(db: pymongo.database.Database, collection_name: str, reset: bool) -> pymongo.collection.Collection:
    if reset and collection_name in db.list_collection_names():
        db.drop_collection(collection_name)
        logger.info("Collection '%s' supprimée (reset)", collection_name)

    if collection_name not in db.list_collection_names():
        db.create_collection(
            collection_name,
            validator=SCHEMA_VALIDATOR,
            validationLevel="strict",
            validationAction="error",
        )
        logger.info("Collection '%s' créée avec validation strict", collection_name)
    else:
        logger.info("Collection '%s' existe déjà (pas de reset)", collection_name)

    return db[collection_name]


def create_indexes(collection: pymongo.collection.Collection) -> None:
    collection.create_index(
        [("station_id", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)],
        unique=True,
        name="idx_station_timestamp",
    )
    collection.create_index([("source", pymongo.ASCENDING)], name="idx_source")
    collection.create_index([("timestamp", pymongo.ASCENDING)], name="idx_timestamp")
    logger.info("Index OK: idx_station_timestamp (unique), idx_source, idx_timestamp")


def import_documents(
    collection: pymongo.collection.Collection, filepath: Path, batch_size: int, stats: ImportStats
) -> Dict[str, Any]:
    t0 = time.time()

    for batch_no, batch in enumerate(load_batches(filepath, batch_size, stats), start=1):
        stats.total_submitted += len(batch)

        try:
            res = collection.insert_many(batch, ordered=False)
            stats.total_inserted += len(res.inserted_ids)
        except BulkWriteError as bwe:
            inserted = bwe.details.get("nInserted", 0)
            stats.total_inserted += inserted

            write_errors = bwe.details.get("writeErrors", [])
            stats.total_errors += len(write_errors)

            for err in write_errors:
                etype = classify_error(err)
                stats.error_types[etype] += 1
                if len(stats.errors_sample) < 15:
                    stats.errors_sample.append(
                        {
                            "batch": batch_no,
                            "index": err.get("index"),
                            "code": err.get("code"),
                            "type": etype,
                            "message": (err.get("errmsg", "") or "")[:250],
                        }
                    )

            logger.warning("Batch %s: %s erreurs, %s insérés", batch_no, len(write_errors), inserted)
        except PyMongoError as e:
            stats.total_errors += len(batch)
            stats.error_types["mongo_error"] += len(batch)
            logger.error("Batch %s: erreur MongoDB (%s). Batch compté en erreur.", batch_no, str(e)[:200])

    stats.insertion_time_s = round(time.time() - t0, 2)

    return {
        "batch_size": batch_size,
        "total_lines": stats.total_lines,
        "total_parsed": stats.total_parsed,
        "total_submitted": stats.total_submitted,
        "total_inserted": stats.total_inserted,
        "total_errors": stats.total_errors,
        "error_types": dict(stats.error_types),
        "errors_sample": stats.errors_sample,
        "parse_errors_sample": stats.parse_errors_sample,
        "insertion_time_s": stats.insertion_time_s,
    }


def validate_quality(collection: pymongo.collection.Collection) -> Dict[str, Any]:
    total = collection.count_documents({})

    required_fields = ["source", "station_id", "station_name", "latitude", "longitude", "timestamp"]
    missing_required: Dict[str, int] = {}
    for field in required_fields:
        count = collection.count_documents({"$or": [{field: {"$exists": False}}, {field: None}]})
        if count:
            missing_required[field] = count

    measure_fields = [
        "temperature_c",
        "dew_point_c",
        "humidity_pct",
        "wind_direction_deg",
        "wind_speed_kmh",
        "wind_gust_kmh",
        "pressure_hpa",
        "precip_rate_mm",
        "precip_accum_mm",
        "visibility_m",
        "cloud_cover_octas",
        "snow_depth_cm",
        "weather_code",
        "uv_index",
        "solar_radiation_wm2",
    ]

    null_rates: Dict[str, float] = {}
    for field in measure_fields:
        null_count = collection.count_documents({"$or": [{field: {"$exists": False}}, {field: None}]})
        null_rates[field] = round((null_count / total) * 100, 2) if total else 0.0

    by_source = {
        r["_id"]: r["count"]
        for r in collection.aggregate([{"$group": {"_id": "$source", "count": {"$sum": 1}}}])
    }

    date_range = list(
        collection.aggregate([{"$group": {"_id": None, "min": {"$min": "$timestamp"}, "max": {"$max": "$timestamp"}}}])
    )

    invalid_required = sum(missing_required.values())
    conformity = round((total - invalid_required) / total * 100, 2) if total else 0.0

    return {
        "total_documents": total,
        "schema_conformity_rate_pct": conformity,
        "missing_required_fields": missing_required,
        "null_rates_pct": null_rates,
        "by_source": by_source,
        "date_range": {
            "min": str(date_range[0]["min"]) if date_range else None,
            "max": str(date_range[0]["max"]) if date_range else None,
        },
    }


def measure_access_times(collection: pymongo.collection.Collection) -> Dict[str, Any]:
    queries = {
        "count_total": ("Compter tous les documents", lambda: collection.count_documents({})),
        "latest_10_records": (
            "10 derniers enregistrements station WU (IICHTE19)",
            lambda: list(collection.find({"station_id": "IICHTE19"}).sort("timestamp", -1).limit(10)),
        ),
    }

    results: Dict[str, Any] = {}
    for name, (desc, fn) in queries.items():
        t0 = time.time()
        out = fn()
        t_ms = round((time.time() - t0) * 1000, 2)
        count = out if isinstance(out, int) else len(out)
        results[name] = {"description": desc, "time_ms": t_ms, "result_count": count}
        logger.info("%s: %sms (%s résultats)", desc, t_ms, count)

    return results


def test_replication(client: MongoClient) -> Dict[str, Any]:
    try:
        rs_status = client.admin.command("replSetGetStatus")
        members = [{"name": m.get("name"), "state": m.get("stateStr")} for m in rs_status.get("members", [])]
    except Exception as e:
        return {"status": "no_replica_set", "error": str(e)[:300]}

    return {"status": "ok", "replica_set_name": rs_status.get("set"), "members": members}


# ============================================================
# CLI / CONFIG
# ============================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load weather JSONL into MongoDB with schema validation.")
    parser.add_argument("--input", dest="input_path", default=os.getenv("INPUT_PATH", str(DEFAULT_INPUT)))
    parser.add_argument("--report", dest="report_path", default=os.getenv("REPORT_PATH", str(DEFAULT_REPORT)))
    parser.add_argument("--mongo-uri", dest="mongo_uri", default=os.getenv("MONGO_URI"))
    parser.add_argument("--db", dest="db_name", default=os.getenv("DB_NAME", DEFAULT_DB_NAME))
    parser.add_argument("--collection", dest="collection_name", default=os.getenv("COLLECTION_NAME", DEFAULT_COLLECTION_NAME))
    parser.add_argument("--batch-size", dest="batch_size", type=int, default=int(os.getenv("BATCH_SIZE", str(DEFAULT_BATCH_SIZE))))
    parser.add_argument("--reset", dest="reset_collection", action="store_true", default=os.getenv("RESET_COLLECTION", str(DEFAULT_RESET_COLLECTION)).lower() in {"1","true","yes","y"})
    parser.add_argument("--no-reset", dest="reset_collection", action="store_false")
    parser.add_argument(
        "--force-direct",
        dest="force_direct",
        action="store_true",
        default=os.getenv("FORCE_DIRECT_CONNECTION", "false").lower() in {"1","true","yes","y"},
        help="Force directConnection=true (utile quand le RS annonce un hostname interne Docker).",
    )
    return parser.parse_args()


def validate_config(cfg: argparse.Namespace) -> Tuple[Path, Path, str]:
    input_path = Path(cfg.input_path).expanduser().resolve()
    report_path = Path(cfg.report_path).expanduser().resolve()

    if not cfg.mongo_uri:
        raise SystemExit(
            "MONGO_URI manquant. Exemple host->docker: "
            "MONGO_URI='mongodb://localhost:27018/?directConnection=true'"
        )

    if not input_path.exists():
        raise FileNotFoundError(f"Input introuvable: {input_path}")

    return input_path, report_path, cfg.mongo_uri


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    cfg = parse_args()
    input_path, report_path, mongo_uri = validate_config(cfg)

    mongo_uri = ensure_direct_connection_if_needed(mongo_uri, force=cfg.force_direct)

    logger.info("=" * 80)
    logger.info("LOAD MONGODB - P8 Weather")
    logger.info("=" * 80)
    logger.info("Input: %s", input_path)
    logger.info("Mongo: %s", redact_mongo_uri(mongo_uri))
    logger.info("DB/Collection: %s.%s", cfg.db_name, cfg.collection_name)
    logger.info("Batch size: %s | Reset: %s | force_direct=%s", cfg.batch_size, cfg.reset_collection, cfg.force_direct)

    report: Dict[str, Any] = {
        "run_timestamp_utc": now_utc_iso(),
        "config": {
            "mongo_uri": redact_mongo_uri(mongo_uri),
            "db_name": cfg.db_name,
            "collection_name": cfg.collection_name,
            "input_path": str(input_path),
            "report_path": str(report_path),
            "batch_size": cfg.batch_size,
            "reset_collection": cfg.reset_collection,
            "force_direct_connection": cfg.force_direct,
        },
    }

    client = connect_mongo(mongo_uri)
    try:
        db = client[cfg.db_name]
        collection = setup_collection(db, cfg.collection_name, cfg.reset_collection)

        stats = ImportStats()
        report["import"] = import_documents(collection, input_path, cfg.batch_size, stats)

        create_indexes(collection)
        report["quality"] = validate_quality(collection)
        report["access_times"] = measure_access_times(collection)
        report["replication"] = test_replication(client)

        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str, ensure_ascii=False)

        logger.info("Rapport écrit: %s", report_path)
        logger.info("OK")
    finally:
        client.close()


if __name__ == "__main__":
    main()
