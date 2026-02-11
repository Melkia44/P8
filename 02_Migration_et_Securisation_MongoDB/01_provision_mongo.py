from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, OperationFailure

# -------------------------------------------------------------------
# PROJECT ROOT & PATHS (STEP 2)
# -------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_IN_DIR = PROJECT_ROOT / "output" / "01_local_processing"
DEFAULT_OUT_DIR = PROJECT_ROOT / "output" / "02_local_processing"

IN_DIR = Path(os.getenv("IN_DIR", str(DEFAULT_IN_DIR)))
OUT_DIR = Path(os.getenv("OUT_DIR", str(DEFAULT_OUT_DIR)))

STATIONS_PATH = Path(os.getenv("STATIONS_PATH", str(IN_DIR / "stations.json")))
OBS_PATH = Path(os.getenv("OBS_PATH", str(IN_DIR / "observations.jsonl")))
QUALITY_OUT = Path(os.getenv("QUALITY_OUT", str(OUT_DIR / "quality_post_mongo.json")))

# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------
def parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        v = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(v)
        except ValueError:
            return None
    return None

def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

# -------------------------------------------------------------------
# MONGO SETUP (INDEXES)
# -------------------------------------------------------------------
def ensure_indexes(db) -> None:
    # stations: station_id unique
    db.stations.create_index([("station_id", 1)], unique=True, name="ux_station_id")

    # observations: record_hash unique + main index for (station, date)
    db.observations.create_index([("record_hash", 1)], unique=True, name="ux_record_hash")
    db.observations.create_index([("station_id", 1), ("obs_datetime", 1)], name="ix_station_datetime")

# -------------------------------------------------------------------
# MIGRATION LOGIC
# -------------------------------------------------------------------
def normalize_station(d: dict[str, Any]) -> dict[str, Any]:
    d = dict(d)
    d["created_at"] = parse_dt(d.get("created_at")) or d.get("created_at")
    d["updated_at"] = parse_dt(d.get("updated_at")) or d.get("updated_at")
    return d

def migrate_stations(db, stations: list[dict[str, Any]]) -> dict[str, int]:
    ops = [
        UpdateOne({"station_id": s["station_id"]}, {"$setOnInsert": normalize_station(s)}, upsert=True)
        for s in stations
        if isinstance(s, dict) and s.get("station_id")
    ]

    inserted = duplicates = errors = 0
    if ops:
        try:
            res = db.stations.bulk_write(ops, ordered=False)
            inserted = res.upserted_count or 0
            duplicates = res.matched_count or 0
        except BulkWriteError as e:
            errors = len(e.details.get("writeErrors", []))

    return {"inserted": inserted, "duplicates": duplicates, "errors": errors}

def normalize_observation(d: dict[str, Any]) -> dict[str, Any]:
    d = dict(d)
    d["obs_datetime"] = parse_dt(d.get("obs_datetime"))
    d["ingestion_ts"] = parse_dt(d.get("ingestion_ts"))
    return d

def migrate_observations(db, obs_path: Path) -> dict[str, int]:
    ops: list[UpdateOne] = []
    total = 0

    for d in read_jsonl(obs_path):
        total += 1
        d = normalize_observation(d)
        ops.append(
            UpdateOne(
                {"record_hash": d["record_hash"]},
                {"$setOnInsert": d},
                upsert=True,
            )
        )

    inserted = duplicates = errors = 0
    if ops:
        try:
            res = db.observations.bulk_write(ops, ordered=False)
            inserted = res.upserted_count or 0
            duplicates = res.matched_count or 0
        except BulkWriteError as e:
            errors = len(e.details.get("writeErrors", []))

    return {"total": total, "inserted": inserted, "duplicates": duplicates, "errors": errors}

# -------------------------------------------------------------------
# CRUD PROOF (JURY FRIENDLY)
# -------------------------------------------------------------------
def crud_proof(db) -> dict[str, Any]:
    test = {
        "station_id": "TEST:TEST01",
        "name": "TestStation",
        "lat": 0.0,
        "lon": 0.0,
        "city": "TestCity",
        "provider": "TEST",
        "source": "CRUD_PROOF",
    }

    db.stations.update_one({"station_id": test["station_id"]}, {"$setOnInsert": test}, upsert=True)
    read_back = db.stations.find_one({"station_id": test["station_id"]}, {"_id": 0})

    db.stations.update_one({"station_id": test["station_id"]}, {"$set": {"hardware": "virtual"}})
    updated = db.stations.find_one({"station_id": test["station_id"]}, {"_id": 0})

    db.stations.delete_one({"station_id": test["station_id"]})
    deleted_exists = db.stations.find_one({"station_id": test["station_id"]}) is not None

    return {"read": read_back, "updated": updated, "deleted_exists": deleted_exists}

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
def main() -> int:
    load_dotenv()

    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB", "meteo")

    if not mongo_uri:
        raise SystemExit("MONGO_URI manquant")

    print(f"[MIGRATE] Mongo URI: {mongo_uri}")
    print(f"[MIGRATE] Database: {db_name}")
    print(f"[MIGRATE] Input stations: {STATIONS_PATH}")
    print(f"[MIGRATE] Input observations: {OBS_PATH}")
    print(f"[MIGRATE] Output report: {QUALITY_OUT}")

    client = MongoClient(mongo_uri, appname="p8-de-migrate")
    db = client[db_name]

    try:
        db.command("ping")
    except OperationFailure as e:
        raise SystemExit(f"Mongo ping KO: {e}") from e

    # indexes = perf + import efficient
    ensure_indexes(db)

    before_count = db.observations.count_documents({})
    print(f"[MIGRATE] Observations BEFORE: {before_count}")

    stations = read_json(STATIONS_PATH)
    if not isinstance(stations, list):
        stations = [stations]

    st = migrate_stations(db, stations)
    ob = migrate_observations(db, OBS_PATH)

    after_count = db.observations.count_documents({})
    print(f"[MIGRATE] Observations AFTER: {after_count}")

    write_errors = st["errors"] + ob["errors"]
    error_rate = (write_errors / ob["total"]) if ob["total"] else 0.0

    report = {
        "stations": st,
        "observations": ob,
        "mongo_observations_before": before_count,
        "mongo_observations_after": after_count,
        "write_errors": write_errors,
        "error_rate": error_rate,
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with QUALITY_OUT.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("[MIGRATE] OK - Migration terminée")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print("[MIGRATE] CRUD proof:", json.dumps(crud_proof(db), ensure_ascii=False, indent=2))

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
