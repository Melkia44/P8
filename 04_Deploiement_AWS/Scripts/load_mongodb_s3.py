#!/usr/bin/env python3
"""
load_mongodb_s3.py - Charge MongoDB depuis S3 Transform/

Lit weather_data.jsonl depuis S3 Transform/ et charge dans MongoDB AWS
"""

import os, sys, json, logging, time
from datetime import datetime
from collections import Counter
from typing import List, Dict

import boto3
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError, PyMongoError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger("load_mongodb_s3")

# CONFIG
BUCKET = os.getenv("BUCKET_NAME", "oc-meteo-staging-data")
REGION = os.getenv("AWS_REGION", "eu-west-3")
INPUT_FILE = os.getenv("INPUT_FILE", "Transform/weather_data.jsonl")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:ForecastSecure2024!@51.44.220.64:27017/")
DB_NAME = os.getenv("DB_NAME", "weather_db")
COLLECTION = os.getenv("COLLECTION_NAME", "weather_data")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
RESET = os.getenv("RESET_COLLECTION", "false").lower() in {"true", "1", "yes"}

# SCHEMA VALIDATOR
SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["source", "station_id", "timestamp"],
        "properties": {
            "source": {"bsonType": "string"},
            "station_id": {"bsonType": "string"},
            "timestamp": {"bsonType": "date"},
            "temperature_c": {"bsonType": ["double", "null"], "minimum": -60, "maximum": 60},
            "humidity_pct": {"bsonType": ["double", "null"], "minimum": 0, "maximum": 100},
            "pressure_hpa": {"bsonType": ["double", "null"], "minimum": 870, "maximum": 1084},
        }
    }
}

def s3_client():
    return boto3.client('s3', region_name=REGION)

def download_jsonl(s3, bucket, key):
    logger.info(f"📥 Download: s3://{bucket}/{key}")
    resp = s3.get_object(Bucket=bucket, Key=key)
    content = resp['Body'].read().decode('utf-8')
    
    recs = []
    for line_no, line in enumerate(content.strip().split('\n'), 1):
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
            recs.append(rec)
        except json.JSONDecodeError as e:
            logger.warning(f"  ⚠️  Ligne {line_no} invalide: {e}")
    
    logger.info(f"  ✅ {len(recs)} records parsés")
    return recs

def parse_timestamp(value):
    """Parse timestamp ISO vers datetime"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return dt
        except:
            return None
    return None

def normalize_record(rec):
    """Normalise un record pour MongoDB"""
    rec["timestamp"] = parse_timestamp(rec.get("timestamp"))
    
    # Convertir int en float
    numeric = ["latitude", "longitude", "elevation", "temperature_c", "dew_point_c",
               "humidity_pct", "wind_direction_deg", "wind_speed_kmh", "wind_gust_kmh",
               "pressure_hpa", "precip_rate_mm", "precip_accum_mm"]
    
    for field in numeric:
        if field in rec and isinstance(rec[field], int):
            rec[field] = float(rec[field])
    
    return rec

def connect_mongo(uri):
    logger.info("🔌 Connexion MongoDB...")
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    logger.info("  ✅ Connecté")
    return client

def setup_collection(db, coll_name, reset):
    if reset and coll_name in db.list_collection_names():
        db.drop_collection(coll_name)
        logger.info(f"🗑️  Collection '{coll_name}' supprimée")
    
    if coll_name not in db.list_collection_names():
        db.create_collection(coll_name, validator=SCHEMA, validationLevel="moderate", validationAction="warn")
        logger.info(f"✅ Collection '{coll_name}' créée")
    else:
        logger.info(f"ℹ️  Collection '{coll_name}' existe")
    
    return db[coll_name]

def create_indexes(coll):
    try:
        coll.create_index([("station_id", ASCENDING), ("timestamp", ASCENDING)], unique=True, name="idx_station_ts")
        coll.create_index([("source", ASCENDING)], name="idx_source")
        coll.create_index([("timestamp", ASCENDING)], name="idx_timestamp")
        logger.info("✅ Index créés")
    except Exception as e:
        logger.warning(f"⚠️  Index: {e}")

def bulk_insert(coll, recs, stats):
    if not recs:
        return
    
    normalized = [normalize_record(r.copy()) for r in recs]
    stats["total_submitted"] += len(normalized)
    
    try:
        result = coll.insert_many(normalized, ordered=False)
        stats["total_inserted"] += len(result.inserted_ids)
        logger.info(f"  ✅ {len(result.inserted_ids)} insérés")
    except BulkWriteError as bwe:
        inserted = bwe.details.get("nInserted", 0)
        stats["total_inserted"] += inserted
        
        errors = bwe.details.get("writeErrors", [])
        stats["total_errors"] += len(errors)
        
        for err in errors:
            code = err.get("code")
            if code == 11000:
                stats["error_types"]["duplicate"] += 1
            elif code == 121:
                stats["error_types"]["validation"] += 1
            else:
                stats["error_types"]["other"] += 1
        
        logger.warning(f"  ⚠️  {inserted} insérés, {len(errors)} erreurs")
    except PyMongoError as e:
        stats["total_errors"] += len(normalized)
        stats["error_types"]["mongo_error"] += len(normalized)
        logger.error(f"  ❌ Erreur: {str(e)[:200]}")

def validate_quality(coll):
    total = coll.count_documents({})
    
    by_source = {r["_id"]: r["count"] for r in coll.aggregate([
        {"$group": {"_id": "$source", "count": {"$sum": 1}}}
    ])}
    
    by_station = {r["_id"]: r["count"] for r in coll.aggregate([
        {"$group": {"_id": "$station_id", "count": {"$sum": 1}}}
    ])}
    
    date_range = list(coll.aggregate([
        {"$group": {"_id": None, "min": {"$min": "$timestamp"}, "max": {"$max": "$timestamp"}}}
    ]))
    
    return {
        "total_documents": total,
        "by_source": by_source,
        "by_station": by_station,
        "date_range": {
            "min": str(date_range[0]["min"]) if date_range else None,
            "max": str(date_range[0]["max"]) if date_range else None
        }
    }

def main():
    logger.info("="*80)
    logger.info("LOAD MONGODB FROM S3 - Forecast 2.0")
    logger.info("="*80)
    
    if not BUCKET:
        raise SystemExit("❌ BUCKET_NAME requis")
    
    logger.info(f"📦 Bucket: {BUCKET}")
    logger.info(f"📥 Input: {INPUT_FILE}")
    logger.info(f"🗄️  MongoDB: {DB_NAME}.{COLLECTION}")
    logger.info(f"📊 Batch: {BATCH_SIZE}")
    logger.info(f"🔄 Reset: {RESET}")
    
    stats = {
        "total_submitted": 0,
        "total_inserted": 0,
        "total_errors": 0,
        "error_types": Counter(),
        "start": time.time()
    }
    
    try:
        # S3
        s3 = s3_client()
        records = download_jsonl(s3, BUCKET, INPUT_FILE)
        
        if not records:
            raise SystemExit("❌ Aucun record")
        
        # MongoDB
        client = connect_mongo(MONGO_URI)
        db = client[DB_NAME]
        coll = setup_collection(db, COLLECTION, RESET)
        create_indexes(coll)
        
        # Insert par batch
        logger.info(f"\n📥 CHARGEMENT DE {len(records)} RECORDS")
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i+BATCH_SIZE]
            bulk_insert(coll, batch, stats)
        
        stats["duration"] = round(time.time() - stats["start"], 2)
        
        logger.info("\n" + "="*80)
        logger.info("📊 STATISTIQUES")
        logger.info("="*80)
        logger.info(f"Soumis: {stats['total_submitted']}")
        logger.info(f"Insérés: {stats['total_inserted']}")
        logger.info(f"Erreurs: {stats['total_errors']}")
        logger.info(f"Durée: {stats['duration']}s")
        
        if stats["error_types"]:
            logger.info("\nTypes d'erreurs:")
            for etype, count in stats["error_types"].items():
                logger.info(f"  - {etype}: {count}")
        
        # Quality
        quality = validate_quality(coll)
        logger.info(f"\n✅ Total MongoDB: {quality['total_documents']}")
        logger.info(f"Par source: {quality['by_source']}")
        logger.info(f"Par station: {quality['by_station']}")
        logger.info(f"Période: {quality['date_range']['min']} → {quality['date_range']['max']}")
        
        client.close()
        logger.info("\n✅ CHARGEMENT TERMINÉ")
    
    except Exception as e:
        logger.error(f"\n❌ ERREUR: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
