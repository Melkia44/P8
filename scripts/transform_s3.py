#!/usr/bin/env python3
"""
transform_s3.py - Transformation données météo depuis S3 (format Airbyte)
Gère le wrapper Airbyte _airbyte_data
"""

import os, sys, json, math, logging
from datetime import datetime
from typing import List, Dict
import boto3, pandas as pd, numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger("transform_s3")

BUCKET = os.getenv("BUCKET_NAME", "oc-meteo-staging-data")
REGION = os.getenv("AWS_REGION", "eu-west-3")
IN_PREFIX = os.getenv("INPUT_PREFIX", "raw/")
OUT_PREFIX = os.getenv("OUTPUT_PREFIX", "Transform/")

OUT_FILE = f"{OUT_PREFIX}weather_data.jsonl"
QUAL_FILE = f"{OUT_PREFIX}weather_data.quality.json"

COLS = ["source","station_id","station_name","latitude","longitude","elevation","station_type","timestamp","temperature_c","dew_point_c","humidity_pct","wind_direction_deg","wind_speed_kmh","wind_gust_kmh","pressure_hpa","precip_rate_mm","precip_accum_mm","visibility_m","cloud_cover_octas","snow_depth_cm","weather_code","uv_index","solar_radiation_wm2"]

WIND = {"North":0,"NNE":22.5,"NE":45,"ENE":67.5,"East":90,"ESE":112.5,"SE":135,"SSE":157.5,"South":180,"SSW":202.5,"SW":225,"WSW":247.5,"West":270,"WNW":292.5,"NW":315,"NNW":337.5}

WU_META = {
    "IICHTE19": {"station_name":"WeerstationBS","latitude":51.092,"longitude":2.999,"elevation":15,"station_type":"weather_underground"},
    "ILAMAD25": {"station_name":"La Madeleine","latitude":50.659,"longitude":3.07,"elevation":23,"station_type":"weather_underground"}
}

def s3_client():
    return boto3.client('s3', region_name=REGION)

def list_jsonl(s3, bucket, prefix):
    logger.info(f"📋 Liste fichiers: s3://{bucket}/{prefix}")
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith(('.jsonl','.json')):
                    files.append(obj['Key'])
                    logger.info(f"  ✓ {obj['Key']}")
    logger.info(f"📊 Total: {len(files)} fichiers")
    return files

def download_jsonl(s3, bucket, key):
    logger.info(f"📥 Download: {key}")
    resp = s3.get_object(Bucket=bucket, Key=key)
    content = resp['Body'].read().decode('utf-8')
    recs = []
    for line in content.strip().split('\n'):
        if line.strip():
            try:
                recs.append(json.loads(line))
            except:
                pass
    logger.info(f"  ✅ {len(recs)} records")
    return recs

def upload_s3(s3, bucket, key, data, ctype):
    logger.info(f"📤 Upload: s3://{bucket}/{key}")
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=ctype)

def extract_airbyte(rec):
    """Extrait données du wrapper Airbyte"""
    return rec.get("_airbyte_data", rec)

def f2c(f):
    return round((f-32)*5/9,2) if pd.notna(f) else np.nan

def mph2kmh(m):
    return round(m*1.60934,2) if pd.notna(m) else np.nan

def inhg2hpa(i):
    return round(i*33.8639,2) if pd.notna(i) else np.nan

def in2mm(i):
    return round(i*25.4,2) if pd.notna(i) else np.nan

def parse_wu_val(raw):
    if raw is None or (isinstance(raw,float) and math.isnan(raw)):
        return np.nan
    if isinstance(raw,(int,float)):
        return float(raw)
    s = str(raw).replace("\xa0"," ").strip()
    if s in {"","-","--","N/A"}:
        return np.nan
    token = (s.split()[0] if s.split() else s).replace(",",".").lstrip("<").strip()
    try:
        return float(token)
    except:
        return np.nan

def wind2deg(txt):
    return WIND.get(txt.strip(), np.nan) if txt and isinstance(txt,str) else np.nan

def sanitize(obj):
    if obj is None:
        return None
    if isinstance(obj,np.integer):
        return int(obj)
    if isinstance(obj,np.floating):
        x=float(obj)
        return None if math.isnan(x) or math.isinf(x) else x
    if isinstance(obj,float):
        return None if math.isnan(obj) or math.isinf(obj) else obj
    if isinstance(obj,dict):
        return {k:sanitize(v) for k,v in obj.items()}
    if isinstance(obj,list):
        return [sanitize(v) for v in obj]
    return obj

def detect_source(recs):
    if not recs:
        return "unknown"
    sample = extract_airbyte(recs[0])
    if "id_station" in sample and "dh_utc" in sample:
        return "infoclimat"
    if "Temperature" in sample or "Dew Point" in sample or "Time" in sample:
        return "weather_underground"
    return "unknown"

def parse_infoclimat(recs):
    logger.info("🔧 Parse InfoClimat")
    unified = []
    for r in recs:
        d = extract_airbyte(r)
        unified.append({
            "source":"infoclimat","station_id":str(d.get("id_station","")),"station_name":None,
            "latitude":None,"longitude":None,"elevation":None,"station_type":"infoclimat_api",
            "timestamp":d.get("dh_utc"),"temperature_c":d.get("temperature"),"dew_point_c":d.get("point_de_rosee"),
            "humidity_pct":d.get("humidite"),"wind_direction_deg":d.get("vent_direction"),
            "wind_speed_kmh":d.get("vent_moyen"),"wind_gust_kmh":d.get("vent_rafales"),
            "pressure_hpa":d.get("pression"),"precip_rate_mm":d.get("pluie_1h"),
            "precip_accum_mm":d.get("pluie_3h"),"visibility_m":d.get("visibilite"),
            "cloud_cover_octas":d.get("nebulosite"),"snow_depth_cm":d.get("neige_au_sol"),
            "weather_code":d.get("temps_omm"),"uv_index":None,"solar_radiation_wm2":None
        })
    df = pd.DataFrame(unified)
    for c in ["latitude","longitude","elevation","temperature_c","dew_point_c","humidity_pct","wind_direction_deg","wind_speed_kmh","wind_gust_kmh","pressure_hpa","precip_rate_mm","precip_accum_mm","visibility_m","snow_depth_cm","cloud_cover_octas"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    logger.info(f"  ✅ {len(df)} records")
    return df

def extract_date_from_path(s3_path):
    """Extrait la date depuis le chemin S3
    Exemple: raw/BE/011024/ -> 2024-10-01
    Format: MMDDYY
    """
    import re
    match = re.search(r'/(\d{6})/', s3_path)
    if not match:
        return None
    
    date_str = match.group(1)  # Ex: "011024"
    try:
        month = int(date_str[0:2])
        day = int(date_str[2:4])
        year = 2000 + int(date_str[4:6])
        return datetime(year, month, day).date()
    except:
        return None


def parse_wu(recs, sid, s3_path):
    logger.info(f"🔧 Parse WU ({sid})")
    meta = WU_META.get(sid, {"station_name":f"Unknown_{sid}","latitude":None,"longitude":None,"elevation":None,"station_type":"weather_underground"})
    
    # Extraire date depuis chemin S3
    base_date = extract_date_from_path(s3_path)
    if base_date:
        logger.info(f"  📅 Date extraite: {base_date}")
    else:
        logger.warning(f"  ⚠️  Impossible d'extraire date depuis {s3_path}")
    
    unified = []
    for r in recs:
        d = extract_airbyte(r)
        time_str = d.get("Time")
        
        # Reconstruire timestamp complet
        if base_date and time_str:
            try:
                # Parse heure depuis format "12:04 AM"
                time_obj = datetime.strptime(time_str, "%I:%M %p").time()
                full_timestamp = datetime.combine(base_date, time_obj)
            except:
                full_timestamp = None
        else:
            full_timestamp = None
        
        unified.append({
            "source":"weather_underground","station_id":sid,"station_name":meta["station_name"],
            "latitude":meta["latitude"],"longitude":meta["longitude"],"elevation":meta["elevation"],
            "station_type":meta["station_type"],"timestamp":full_timestamp,
            "temperature_c":f2c(parse_wu_val(d.get("Temperature"))),
            "dew_point_c":f2c(parse_wu_val(d.get("Dew Point"))),
            "humidity_pct":parse_wu_val(d.get("Humidity")),
            "wind_direction_deg":wind2deg(d.get("Wind")),
            "wind_speed_kmh":mph2kmh(parse_wu_val(d.get("Speed"))),
            "wind_gust_kmh":mph2kmh(parse_wu_val(d.get("Gust"))),
            "pressure_hpa":inhg2hpa(parse_wu_val(d.get("Pressure"))),
            "precip_rate_mm":in2mm(parse_wu_val(d.get("Precip. Rate"))),
            "precip_accum_mm":in2mm(parse_wu_val(d.get("Precip. Accum."))),
            "uv_index":parse_wu_val(d.get("UV")),
            "solar_radiation_wm2":parse_wu_val(d.get("Solar")),
            "visibility_m":None,"cloud_cover_octas":None,"snow_depth_cm":None,"weather_code":None
        })
    df = pd.DataFrame(unified)
    logger.info(f"  ✅ {len(df)} records")
    return df

def infer_station(path):
    if "BE" in path or "ichtegem" in path.lower():
        return "IICHTE19"
    if "FR" in path or "madeleine" in path.lower():
        return "ILAMAD25"
    return "UNKNOWN"

def validate(df):
    total = len(df)
    if total == 0:
        return {"total_records":0,"anomalies":["Empty"]}
    m = {
        "total_records":total,
        "records_per_source":df["source"].value_counts().to_dict(),
        "records_per_station":df["station_id"].value_counts().to_dict(),
        "date_range":{"min":str(df["timestamp"].min()),"max":str(df["timestamp"].max())},
        "null_rates":{c:round(df[c].isna().sum()/total*100,2) for c in COLS},
        "duplicates":int(df.duplicated(subset=["station_id","timestamp"]).sum()),
        "anomalies":[]
    }
    if pd.notna(df["temperature_c"].min()) and df["temperature_c"].min()<-50:
        m["anomalies"].append(f"Temp min: {df['temperature_c'].min()}°C")
    if pd.notna(df["temperature_c"].max()) and df["temperature_c"].max()>60:
        m["anomalies"].append(f"Temp max: {df['temperature_c'].max()}°C")
    return m

def df2jsonl(df):
    do = df.copy()
    do["timestamp"] = pd.to_datetime(do["timestamp"], errors="coerce")
    do["timestamp"] = do["timestamp"].apply(lambda x: x.to_pydatetime().isoformat() if pd.notna(x) and hasattr(x,"to_pydatetime") else None)
    do = do.replace([np.nan,np.inf,-np.inf], None)
    lines = [json.dumps(sanitize(r), ensure_ascii=False, allow_nan=False) for r in do.to_dict(orient="records")]
    return "\n".join(lines).encode('utf-8')

def main():
    logger.info("="*80)
    logger.info("TRANSFORM S3 - Forecast 2.0")
    logger.info("="*80)
    if not BUCKET:
        raise SystemExit("❌ BUCKET_NAME requis")
    logger.info(f"📦 Bucket: {BUCKET}")
    logger.info(f"📥 Input: {IN_PREFIX}")
    logger.info(f"📤 Output: {OUT_PREFIX}")
    
    try:
        s3 = s3_client()
        files = list_jsonl(s3, BUCKET, IN_PREFIX)
        if not files:
            raise SystemExit("❌ Aucun fichier")
        
        all_dfs = []
        for fkey in files:
            recs = download_jsonl(s3, BUCKET, fkey)
            if not recs:
                continue
            
            src_type = detect_source(recs)
            logger.info(f"  Type: {src_type}")
            
            if src_type == "infoclimat":
                df = parse_infoclimat(recs)
            elif src_type == "weather_underground":
                sid = infer_station(fkey)
                df = parse_wu(recs, sid, fkey)  # Passer le chemin S3
            else:
                logger.warning(f"  ⚠️  Type inconnu, skip")
                continue
            
            all_dfs.append(df)
        
        if not all_dfs:
            raise SystemExit("❌ Aucune donnée parsée")
        
        df_all = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"\n✅ Total avant dédup: {len(df_all)} records")
        
        # DÉDUPLICATION sur (station_id, timestamp)
        df_all = df_all.drop_duplicates(subset=["station_id", "timestamp"], keep="first")
        logger.info(f"✅ Total après dédup: {len(df_all)} records")
        
        logger.info("\n📊 Validation...")
        metrics = validate(df_all)
        logger.info(f"  Total: {metrics['total_records']}")
        logger.info(f"  Par source: {metrics['records_per_source']}")
        logger.info(f"  Doublons: {metrics['duplicates']}")
        
        for a in metrics.get("anomalies",[]):
            logger.warning(f"  ⚠️  {a}")
        
        logger.info("\n📤 Export S3...")
        jsonl_bytes = df2jsonl(df_all)
        upload_s3(s3, BUCKET, OUT_FILE, jsonl_bytes, "application/x-ndjson")
        
        qual_bytes = json.dumps(metrics, indent=2, default=str).encode('utf-8')
        upload_s3(s3, BUCKET, QUAL_FILE, qual_bytes, "application/json")
        
        logger.info(f"\n✅ TERMINÉ")
        logger.info(f"📄 Données: s3://{BUCKET}/{OUT_FILE}")
        logger.info(f"📊 Qualité: s3://{BUCKET}/{QUAL_FILE}")
    
    except Exception as e:
        logger.error(f"\n❌ ERREUR: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
