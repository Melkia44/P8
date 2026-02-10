from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Iterable

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, OperationFailure


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


def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def read_jsonl(path: str) -> Iterable[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def migrate_stations(db, stations: list[dict[str, Any]]) -> dict[str, int]:
    ops = [
        UpdateOne({"station_id": s["station_id"]}, {"$setOnInsert": s}, upsert=True)
        for s in stations
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


def migrate_observations(db, obs_path: str) -> dict[str, int]:
    ops: list[UpdateOne] = []
    total = 0

    for d in read_jsonl(obs_path):
        total += 1
        d = normalize_observation(d)

        # Upsert by record_hash (clé d’unicité)
        ops.append(UpdateOne({"record_hash": d["record_hash"]}, {"$setOnInsert": d}, upsert=True))

    inserted = duplicates = errors = 0
    if ops:
        try:
            res = db.observations.bulk_write(ops, ordered=False)
            inserted = res.upserted_count or 0
            duplicates = res.matched_count or 0
        except BulkWriteError as e:
            errors = len(e.details.get("writeErrors", []))

    return {"total": total, "inserted": inserted, "duplicates": duplicates, "errors": errors}


def crud_proof(db) -> dict[str, Any]:
    # CREATE
    test = {
        "station_id": "TEST01",
        "name": "TestStation",
        "lat": 0.0,
        "lon": 0.0,
        "city": "TestCity",
        "provider": "TEST",
    }
    db.stations.update_one({"station_id": "TEST01"}, {"$setOnInsert": test}, upsert=True)

    # READ
    read_back = db.stations.find_one({"station_id": "TEST01"}, {"_id": 0})

    # UPDATE
    db.stations.update_one({"station_id": "TEST01"}, {"$set": {"hardware": "virtual"}})
    updated = db.stations.find_one({"station_id": "TEST01"}, {"_id": 0})

    # DELETE
    db.stations.delete_one({"station_id": "TEST01"})
    deleted_exists = db.stations.find_one({"station_id": "TEST01"}) is not None

    return {"read": read_back, "updated": updated, "deleted_exists": deleted_exists}


def main() -> int:
    load_dotenv()

    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB", "meteo")

    stations_path = os.getenv("STATIONS_PATH", "./output/stations.json")
    obs_path = os.getenv("OBS_PATH", "./output/observations.jsonl")
    quality_out = os.getenv("QUALITY_OUT", "./output/quality_post_mongo.json")

    if not mongo_uri:
        raise SystemExit("MONGO_URI manquant dans .env")

    print(f"[MIGRATE] MongoDB URI: {mongo_uri}")
    print(f"[MIGRATE] Database: {db_name}")
    print(f"[MIGRATE] Input stations: {stations_path}")
    print(f"[MIGRATE] Input observations: {obs_path}")
    print(f"[MIGRATE] Output report: {quality_out}")

    client = MongoClient(mongo_uri, appname="p8-de-migrate")
    db = client[db_name]

    try:
        db.command("ping")
    except OperationFailure as e:
        raise SystemExit(f"Mongo ping KO: {e}") from e

    before_count = db.observations.count_documents({})
    print(f"[MIGRATE] Observations count BEFORE: {before_count}")

    stations = read_json(stations_path)
    if not isinstance(stations, list):
        stations = [stations]

    st = migrate_stations(db, stations)
    ob = migrate_observations(db, obs_path)

    after_count = db.observations.count_documents({})
    print(f"[MIGRATE] Observations count AFTER: {after_count}")

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

    ensure_dir(os.path.dirname(quality_out) or ".")
    with open(quality_out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("[MIGRATE] OK - Migration terminée")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print("[MIGRATE] CRUD proof:", json.dumps(crud_proof(db), ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
