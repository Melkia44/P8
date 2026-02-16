"""
load_mongodb.py - Import des données transformées dans MongoDB

Prérequis :
    - MongoDB accessible (local ou Docker)
    - Fichier weather_data.json produit par transform.py

Usage :
    python load_mongodb.py

Structure attendue :
    P8/
    ├── 01_Recuperation_et_Transformation_Donnees/
    │   └── weather_data.json          ← produit par transform.py
    └── 02_Migration_MongoDB/
        └── load_mongodb.py            ← ce script

Ce script :
    1. Crée la collection avec validation de schéma JSON Schema
    2. Importe les documents par batch
    3. Crée les index
    4. Mesure la qualité post-migration (taux de conformité)
    5. Mesure le temps d'accessibilité aux données
    6. Teste la réplication
    7. Génère un rapport complet
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path

import pymongo
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# ============================================================
# CONFIG & LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Chemins relatifs basés sur la structure du projet
SCRIPT_DIR = Path(__file__).resolve().parent          # 02_Migration_MongoDB/
PROJECT_DIR = SCRIPT_DIR.parent                        # P8/
INPUT_FILE = PROJECT_DIR / "01_Recuperation_et_Transformation_Donnees" / "weather_data.json"
REPORT_FILE = SCRIPT_DIR / "mongodb_report.json"

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "weather_db"
COLLECTION_NAME = "weather_data"

# ============================================================
# SCHÉMA DE VALIDATION MONGODB (JSON Schema)
# ============================================================
SCHEMA_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "source", "station_id", "station_name",
            "latitude", "longitude", "timestamp",
        ],
        "properties": {
            "source": {
                "bsonType": "string",
                "enum": ["infoclimat", "weather_underground"],
                "description": "Origine des données",
            },
            "station_id": {
                "bsonType": "string",
                "description": "Identifiant unique de la station",
            },
            "station_name": {
                "bsonType": "string",
                "description": "Nom lisible de la station",
            },
            "latitude": {
                "bsonType": "double",
                "minimum": -90,
                "maximum": 90,
            },
            "longitude": {
                "bsonType": "double",
                "minimum": -180,
                "maximum": 180,
            },
            "elevation": {
                "bsonType": ["double", "int", "null"],
            },
            "station_type": {
                "bsonType": ["string", "null"],
            },
            "timestamp": {
                "bsonType": "date",
                "description": "Horodatage UTC de la mesure",
            },
            "temperature_c": {
                "bsonType": ["double", "null"],
                "minimum": -60,
                "maximum": 60,
            },
            "dew_point_c": {
                "bsonType": ["double", "null"],
            },
            "humidity_pct": {
                "bsonType": ["double", "null"],
                "minimum": 0,
                "maximum": 100,
            },
            "wind_direction_deg": {
                "bsonType": ["double", "null"],
                "minimum": 0,
                "maximum": 360,
            },
            "wind_speed_kmh": {
                "bsonType": ["double", "null"],
                "minimum": 0,
            },
            "wind_gust_kmh": {
                "bsonType": ["double", "null"],
                "minimum": 0,
            },
            "pressure_hpa": {
                "bsonType": ["double", "null"],
                "minimum": 870,
                "maximum": 1084,
            },
            "precip_rate_mm": {
                "bsonType": ["double", "null"],
                "minimum": 0,
            },
            "precip_accum_mm": {
                "bsonType": ["double", "null"],
                "minimum": 0,
            },
            "visibility_m": {
                "bsonType": ["double", "null"],
                "minimum": 0,
            },
            "cloud_cover_octas": {
                "bsonType": ["double", "int", "null"],
                "minimum": 0,
                "maximum": 8,
            },
            "snow_depth_cm": {
                "bsonType": ["double", "null"],
                "minimum": 0,
            },
            "weather_code": {
                "bsonType": ["string", "null"],
            },
            "uv_index": {
                "bsonType": ["double", "int", "null"],
                "minimum": 0,
            },
            "solar_radiation_wm2": {
                "bsonType": ["double", "null"],
                "minimum": 0,
            },
        },
    }
}


# ============================================================
# 1. CHARGEMENT DU JSON
# ============================================================
def load_json(filepath: str) -> list[dict]:
    """Charge le JSON et convertit les timestamps en datetime."""
    logger.info(f"Chargement de {filepath}...")
    with open(filepath, "r", encoding="utf-8") as f:
        records = json.load(f)

    # Convertir les timestamps string → datetime pour MongoDB
    for rec in records:
        if rec.get("timestamp"):
            rec["timestamp"] = datetime.fromisoformat(rec["timestamp"])
        # Convertir les int en float pour les champs numériques
        # MongoDB JSON Schema attend "double", pas "int"
        for key in ["latitude", "longitude", "elevation",
                     "temperature_c", "dew_point_c", "humidity_pct",
                     "wind_direction_deg", "wind_speed_kmh", "wind_gust_kmh",
                     "pressure_hpa", "precip_rate_mm", "precip_accum_mm",
                     "visibility_m", "snow_depth_cm",
                     "solar_radiation_wm2"]:
            if rec.get(key) is not None and isinstance(rec[key], int):
                rec[key] = float(rec[key])

    logger.info(f"  → {len(records)} documents chargés")
    return records


# ============================================================
# 2. CRÉATION DE LA COLLECTION AVEC VALIDATION
# ============================================================
def setup_collection(db) -> pymongo.collection.Collection:
    """Crée (ou recrée) la collection avec le schéma de validation."""
    if COLLECTION_NAME in db.list_collection_names():
        db.drop_collection(COLLECTION_NAME)
        logger.info(f"Collection '{COLLECTION_NAME}' supprimée (reset)")

    db.create_collection(
        COLLECTION_NAME,
        validator=SCHEMA_VALIDATOR,
        validationLevel="strict",
        validationAction="error",
    )
    logger.info(f"Collection '{COLLECTION_NAME}' créée avec validation strict")
    return db[COLLECTION_NAME]


# ============================================================
# 3. IMPORT PAR BATCH
# ============================================================
def import_documents(collection, records: list[dict]) -> dict:
    """Insère les documents par batch. Retourne les stats d'insertion."""
    BATCH_SIZE = 500
    total_inserted = 0
    total_errors = 0
    errors_detail = []

    t_start = time.time()

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        try:
            result = collection.insert_many(batch, ordered=False)
            total_inserted += len(result.inserted_ids)
        except BulkWriteError as bwe:
            n_inserted = bwe.details.get("nInserted", 0)
            total_inserted += n_inserted
            write_errors = bwe.details.get("writeErrors", [])
            total_errors += len(write_errors)
            # Garder un échantillon des erreurs pour le rapport
            for err in write_errors[:3]:
                errors_detail.append({
                    "batch": batch_num,
                    "index": err.get("index"),
                    "code": err.get("code"),
                    "message": err.get("errmsg", "")[:200],
                })
            logger.warning(
                f"  Batch {batch_num}: {len(write_errors)} erreurs, "
                f"{n_inserted} insérés"
            )

    t_elapsed = time.time() - t_start

    logger.info(
        f"Import terminé : {total_inserted}/{len(records)} documents "
        f"en {t_elapsed:.2f}s ({total_errors} erreurs)"
    )

    return {
        "total_submitted": len(records),
        "total_inserted": total_inserted,
        "total_errors": total_errors,
        "insertion_time_s": round(t_elapsed, 2),
        "errors_sample": errors_detail,
    }


# ============================================================
# 4. CRÉATION DES INDEX
# ============================================================
def create_indexes(collection):
    """Crée les index pour les requêtes typiques des Data Scientists."""
    collection.create_index(
        [("station_id", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)],
        unique=True,
        name="idx_station_timestamp",
    )
    collection.create_index(
        [("source", pymongo.ASCENDING)],
        name="idx_source",
    )
    collection.create_index(
        [("timestamp", pymongo.ASCENDING)],
        name="idx_timestamp",
    )
    logger.info("Index créés : idx_station_timestamp (unique), idx_source, idx_timestamp")


# ============================================================
# 5. VALIDATION QUALITÉ POST-MIGRATION
# ============================================================
def validate_quality(collection) -> dict:
    """Mesure la qualité des données en base."""
    logger.info("--- Validation qualité post-migration ---")

    total = collection.count_documents({})

    # Champs requis : compter ceux qui sont null
    required_fields = ["source", "station_id", "station_name",
                       "latitude", "longitude", "timestamp"]
    missing = {}
    for field in required_fields:
        count = collection.count_documents(
            {"$or": [{field: {"$exists": False}}, {field: None}]}
        )
        if count > 0:
            missing[field] = count

    # Taux de null par champ de mesure
    measure_fields = [
        "temperature_c", "dew_point_c", "humidity_pct",
        "wind_direction_deg", "wind_speed_kmh", "wind_gust_kmh",
        "pressure_hpa", "precip_rate_mm", "precip_accum_mm",
        "visibility_m", "cloud_cover_octas", "snow_depth_cm",
        "weather_code", "uv_index", "solar_radiation_wm2",
    ]
    null_rates = {}
    for field in measure_fields:
        null_count = collection.count_documents(
            {"$or": [{field: {"$exists": False}}, {field: None}]}
        )
        null_rates[field] = round(null_count / total * 100, 2) if total > 0 else 0

    # Stats aggregées
    stats_pipeline = [
        {"$group": {
            "_id": None,
            "temp_min": {"$min": "$temperature_c"},
            "temp_max": {"$max": "$temperature_c"},
            "temp_avg": {"$avg": "$temperature_c"},
            "hum_min": {"$min": "$humidity_pct"},
            "hum_max": {"$max": "$humidity_pct"},
            "pres_min": {"$min": "$pressure_hpa"},
            "pres_max": {"$max": "$pressure_hpa"},
        }}
    ]
    stats = list(collection.aggregate(stats_pipeline))

    # Par source et par station
    by_source = {
        r["_id"]: r["count"]
        for r in collection.aggregate([
            {"$group": {"_id": "$source", "count": {"$sum": 1}}}
        ])
    }
    by_station = {
        r["_id"]: r["count"]
        for r in collection.aggregate([
            {"$group": {"_id": "$station_id", "count": {"$sum": 1}}}
        ])
    }

    # Plage de dates
    date_range = list(collection.aggregate([
        {"$group": {
            "_id": None,
            "min": {"$min": "$timestamp"},
            "max": {"$max": "$timestamp"},
        }}
    ]))

    invalid_required = sum(missing.values())
    conformity = round((total - invalid_required) / total * 100, 2) if total > 0 else 0

    report = {
        "total_documents": total,
        "schema_conformity_rate_pct": conformity,
        "missing_required_fields": missing,
        "null_rates_pct": null_rates,
        "by_source": by_source,
        "by_station": by_station,
        "date_range": {
            "min": str(date_range[0]["min"]) if date_range else None,
            "max": str(date_range[0]["max"]) if date_range else None,
        },
        "data_stats": {
            "temperature_c": {
                "min": stats[0]["temp_min"],
                "max": stats[0]["temp_max"],
                "avg": round(stats[0]["temp_avg"], 2),
            },
            "humidity_pct": {
                "min": stats[0]["hum_min"],
                "max": stats[0]["hum_max"],
            },
            "pressure_hpa": {
                "min": stats[0]["pres_min"],
                "max": stats[0]["pres_max"],
            },
        } if stats else {},
    }

    logger.info(f"  Documents en base : {total}")
    logger.info(f"  Conformité schéma : {conformity}%")
    logger.info(f"  Par source : {by_source}")
    if missing:
        logger.warning(f"  Champs requis manquants : {missing}")

    return report


# ============================================================
# 6. MESURE TEMPS D'ACCESSIBILITÉ
# ============================================================
def measure_access_times(collection) -> dict:
    """Chronomètre des requêtes typiques Data Science."""
    logger.info("--- Mesure temps d'accessibilité ---")

    queries = {
        "count_total": {
            "description": "Compter tous les documents",
            "fn": lambda: collection.count_documents({}),
        },
        "find_one_station_one_day": {
            "description": "Données d'une station sur un jour",
            "fn": lambda: list(collection.find({
                "station_id": "07015",
                "timestamp": {
                    "$gte": datetime(2024, 10, 5),
                    "$lt": datetime(2024, 10, 6),
                },
            })),
        },
        "find_all_stations_one_day": {
            "description": "Toutes les stations sur un jour",
            "fn": lambda: list(collection.find({
                "timestamp": {
                    "$gte": datetime(2024, 10, 5),
                    "$lt": datetime(2024, 10, 6),
                },
            })),
        },
        "avg_temp_by_station": {
            "description": "Température moyenne par station",
            "fn": lambda: list(collection.aggregate([
                {"$group": {
                    "_id": "$station_id",
                    "avg_temp": {"$avg": "$temperature_c"},
                }}
            ])),
        },
        "latest_10_records": {
            "description": "10 derniers enregistrements d'une station WU",
            "fn": lambda: list(
                collection.find({"station_id": "IICHTE19"})
                .sort("timestamp", -1)
                .limit(10)
            ),
        },
    }

    results = {}
    for name, q in queries.items():
        t_start = time.time()
        result = q["fn"]()
        t_ms = round((time.time() - t_start) * 1000, 2)
        count = result if isinstance(result, int) else len(result)

        results[name] = {
            "description": q["description"],
            "time_ms": t_ms,
            "result_count": count,
        }
        logger.info(f"  {q['description']} : {t_ms}ms ({count} résultats)")

    return results


# ============================================================
# 7. TEST DE RÉPLICATION
# ============================================================
def test_replication(client) -> dict:
    """Vérifie le replica set et teste un cycle write/read/delete."""
    logger.info("--- Test de réplication ---")

    admin_db = client.admin
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Statut du RS
    try:
        rs_status = admin_db.command("replSetGetStatus")
        members = [
            {"name": m["name"], "state": m["stateStr"]}
            for m in rs_status.get("members", [])
        ]
        logger.info(f"  Replica Set '{rs_status['set']}' actif : {len(members)} membre(s)")
        for m in members:
            logger.info(f"    {m['name']} → {m['state']}")
    except Exception as e:
        logger.error(f"  Pas de replica set détecté : {e}")
        return {"status": "no_replica_set", "error": str(e)}

    # Test write → read → delete
    test_doc = {
        "source": "infoclimat",
        "station_id": "TEST_REPLICATION",
        "station_name": "Test",
        "latitude": 0.0,
        "longitude": 0.0,
        "timestamp": datetime.utcnow(),
        "temperature_c": None,
    }

    try:
        # Écriture avec write concern w=1
        res = collection.with_options(
            write_concern=pymongo.WriteConcern(w=1, wtimeout=5000)
        ).insert_one(test_doc)
        logger.info(f"  Write OK : {res.inserted_id}")

        # Relecture
        found = collection.find_one({"station_id": "TEST_REPLICATION"})
        read_ok = found is not None
        logger.info(f"  Read OK : {read_ok}")

        # Suppression
        collection.delete_many({"station_id": "TEST_REPLICATION"})
        logger.info("  Delete OK : document de test supprimé")

        return {
            "status": "ok",
            "replica_set_name": rs_status["set"],
            "members": members,
            "write_read_delete_test": "passed",
        }
    except Exception as e:
        logger.error(f"  Erreur test réplication : {e}")
        return {"status": "error", "error": str(e)}


# ============================================================
# MAIN
# ============================================================
def main():
    logger.info("=" * 60)
    logger.info("IMPORT MONGODB — Projet Forecast 2.0")
    logger.info("=" * 60)
    logger.info(f"  Input  : {INPUT_FILE}")
    logger.info(f"  MongoDB: {MONGO_URI}")

    # Vérifier que le fichier source existe
    if not INPUT_FILE.exists():
        logger.error(f"Fichier introuvable : {INPUT_FILE}")
        logger.error("As-tu lancé transform.py d'abord ?")
        return

    report = {"run_timestamp": datetime.utcnow().isoformat()}

    # 1. Charger le JSON
    records = load_json(str(INPUT_FILE))

    # 2. Connexion MongoDB
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        logger.info("  Connexion MongoDB OK")
    except Exception as e:
        logger.error(f"  Impossible de se connecter à MongoDB : {e}")
        return

    db = client[DB_NAME]

    # 3. Créer la collection avec validation
    collection = setup_collection(db)

    # 4. Import
    import_stats = import_documents(collection, records)
    report["import"] = import_stats

    # 5. Index
    create_indexes(collection)

    # 6. Validation qualité
    quality = validate_quality(collection)
    report["quality"] = quality

    # 7. Temps d'accessibilité
    access = measure_access_times(collection)
    report["access_times"] = access

    # 8. Test réplication
    replication = test_replication(client)
    report["replication"] = replication

    # --- Sauvegarde du rapport ---
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info("=" * 60)
    logger.info(f"✅ Terminé — Rapport : {REPORT_FILE}")
    logger.info("=" * 60)

    client.close()


if __name__ == "__main__":
    main()
