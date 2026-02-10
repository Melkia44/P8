from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import OperationFailure


VALIDATORS: dict[str, dict[str, Any]] = {
    "stations": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["station_id", "name", "lat", "lon", "city", "provider"],
            "properties": {
                "station_id": {"bsonType": "string"},
                "name": {"bsonType": "string"},
                "lat": {"bsonType": ["double", "int", "decimal"]},
                "lon": {"bsonType": ["double", "int", "decimal"]},
                "elevation_m": {"bsonType": ["double", "int", "decimal", "null"]},
                "city": {"bsonType": "string"},
                "state": {"bsonType": ["string", "null"]},
                "hardware": {"bsonType": ["string", "null"]},
                "software": {"bsonType": ["string", "null"]},
                "provider": {"bsonType": "string"},
            },
        }
    },
    "observations": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "obs_datetime",
                "station_id",
                "station_provider",
                "source",
                "ingestion_ts",
                "record_hash",
            ],
            "properties": {
                "obs_datetime": {"bsonType": "date"},
                "station_id": {"bsonType": "string"},
                "station_provider": {"bsonType": "string"},
                "humidity_pct": {"bsonType": ["int", "double", "decimal", "null"], "minimum": 0, "maximum": 100},
                "temperature_c": {"bsonType": ["double", "int", "decimal", "null"]},
                "dew_point_c": {"bsonType": ["double", "int", "decimal", "null"]},
                "pressure_hpa": {"bsonType": ["double", "int", "decimal", "null"], "minimum": 800, "maximum": 1100},
                "wind_dir": {"bsonType": ["string", "null"]},
                "wind_speed_kmh": {"bsonType": ["double", "int", "decimal", "null"], "minimum": 0},
                "wind_gust_kmh": {"bsonType": ["double", "int", "decimal", "null"], "minimum": 0},
                "precip_rate_mm": {"bsonType": ["double", "int", "decimal", "null"], "minimum": 0},
                "precip_accum_mm": {"bsonType": ["double", "int", "decimal", "null"], "minimum": 0},
                "precip_1h_mm": {"bsonType": ["double", "int", "decimal", "null"], "minimum": 0},
                "precip_3h_mm": {"bsonType": ["double", "int", "decimal", "null"], "minimum": 0},
                "sync_id": {"bsonType": ["int", "long", "null"]},
                "ingestion_ts": {"bsonType": "date"},
                "source": {"bsonType": "string"},
                "record_hash": {"bsonType": "string"},
            },
        }
    },
}


def ensure_collection(db, name: str, validator: dict[str, Any]) -> None:
    # Create if missing
    if name not in db.list_collection_names():
        db.create_collection(name)

    # Apply / update validator
    db.command(
        "collMod",
        name,
        validator={"$jsonSchema": validator["$jsonSchema"]},
        validationLevel="moderate",
    )


def ensure_indexes(db) -> None:
    # stations
    db.stations.create_index([("station_id", 1)], unique=True, name="ux_station_id")
    db.stations.create_index([("provider", 1), ("city", 1)], name="ix_provider_city")

    # observations
    db.observations.create_index([("record_hash", 1)], unique=True, name="ux_record_hash")
    db.observations.create_index([("station_id", 1), ("obs_datetime", 1)], name="ix_station_datetime")
    db.observations.create_index([("obs_datetime", 1)], name="ix_obs_datetime")


def main() -> int:
    load_dotenv()

    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB", "meteo")

    if not mongo_uri:
        raise SystemExit("MONGO_URI manquant dans .env")

    print(f"[PROVISION] MongoDB URI: {mongo_uri}")
    print(f"[PROVISION] Database: {db_name}")

    client = MongoClient(mongo_uri, appname="p8-de-provision")
    db = client[db_name]

    try:
        # Lightweight ping
        db.command("ping")
    except OperationFailure as e:
        raise SystemExit(f"Mongo ping KO: {e}") from e

    ensure_collection(db, "stations", VALIDATORS["stations"])
    ensure_collection(db, "observations", VALIDATORS["observations"])
    ensure_indexes(db)

    print("[PROVISION] OK - Provisioning terminé")
    print("[PROVISION] Collections:", db.list_collection_names())
    print("[PROVISION] Stations indexes:", [i["name"] for i in db.stations.list_indexes()])
    print("[PROVISION] Observations indexes:", [i["name"] for i in db.observations.list_indexes()])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
