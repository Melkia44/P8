#!/usr/bin/env python3
"""
transform.py - ETL de transformation des données météo multi-sources
vers un schéma unifié (métrique) exportable en JSON / JSONL.

Objectif:
- Exécutable depuis n'importe quel dossier.
- Lit automatiquement les fichiers présents sur ta machine dans:
    /home/melkia/P8/data
  (surcharge possible via --data-root ou env DATA_ROOT)

Entrées attendues (dans data-root):
- Data_Source1_011024-071024.json
- Ichtegem_BE.xlsx
- La_Madeleine_FR.xlsx

Sorties (par défaut):
- <data-root>/airbyte/weather_data.jsonl
- <data-root>/airbyte/weather_data.quality.json

Usage:
  python transform.py
  python transform.py --data-root /chemin/vers/data
  DATA_ROOT=/chemin/vers/data python transform.py
  python transform.py --output /tmp/weather.jsonl
"""

import os
import json
import math
import logging
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np


# ============================================================
# CONFIG & LOGGING
# ============================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("transform")

DEFAULT_DATA_ROOT = Path("/home/melkia/P8/data")

DEFAULT_INPUTS = {
    "infoclimat": "Data_Source1_011024-071024.json",
    "wu_ichtegem": "Ichtegem_BE.xlsx",
    "wu_lamadeleine": "La_Madeleine_FR.xlsx",
}

TARGET_COLUMNS = [
    "source", "station_id", "station_name",
    "latitude", "longitude", "elevation", "station_type",
    "timestamp",
    "temperature_c", "dew_point_c", "humidity_pct",
    "wind_direction_deg", "wind_speed_kmh", "wind_gust_kmh",
    "pressure_hpa", "precip_rate_mm", "precip_accum_mm",
    "visibility_m", "cloud_cover_octas", "snow_depth_cm", "weather_code",
    "uv_index", "solar_radiation_wm2",
]

WIND_DIR_MAP = {
    "North": 0, "NNE": 22.5, "NE": 45, "ENE": 67.5,
    "East": 90, "ESE": 112.5, "SE": 135, "SSE": 157.5,
    "South": 180, "SSW": 202.5, "SW": 225, "WSW": 247.5,
    "West": 270, "WNW": 292.5, "NW": 315, "NNW": 337.5,
}

# Métadonnées WU (énoncé)
WU_STATIONS = {
    "IICHTE19": {
        "station_name": "WeerstationBS",
        "latitude": 51.092,
        "longitude": 2.999,
        "elevation": 15,
        "station_type": "weather_underground",
    },
    "ILAMAD25": {
        "station_name": "La Madeleine",
        "latitude": 50.659,
        "longitude": 3.07,
        "elevation": 23,
        "station_type": "weather_underground",
    },
}


# ============================================================
# PATHS (portable)
# ============================================================
def resolve_paths(data_root_arg: str | None, output_arg: str | None) -> dict:
    """
    Résout les chemins de façon portable :
    - Tu peux exécuter le script depuis n'importe où.
    - Les entrées sont cherchées dans data-root.
    - data-root = --data-root OR env DATA_ROOT OR /home/melkia/P8/data
    - output:
        - si chemin relatif -> <data-root>/airbyte/<output>
        - si absent -> <data-root>/airbyte/weather_data.jsonl
    """
    data_root = Path(data_root_arg or os.getenv("DATA_ROOT") or str(DEFAULT_DATA_ROOT)).expanduser().resolve()

    infoclimat = (data_root / DEFAULT_INPUTS["infoclimat"]).resolve()
    wu_ichtegem = (data_root / DEFAULT_INPUTS["wu_ichtegem"]).resolve()
    wu_lamadeleine = (data_root / DEFAULT_INPUTS["wu_lamadeleine"]).resolve()

    if output_arg:
        out = Path(output_arg).expanduser()
        if out.is_absolute():
            output = out.resolve()
        else:
            output = (data_root / "airbyte" / out).resolve()
    else:
        output = (data_root / "airbyte" / "weather_data.jsonl").resolve()

    missing = [p for p in [infoclimat, wu_ichtegem, wu_lamadeleine] if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Fichiers d'entrée introuvables:\n" + "\n".join(f"- {p}" for p in missing)
        )

    output.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Paths resolved:")
    logger.info(f"  data_root      : {data_root}")
    logger.info(f"  infoclimat     : {infoclimat}")
    logger.info(f"  wu_ichtegem    : {wu_ichtegem}")
    logger.info(f"  wu_lamadeleine : {wu_lamadeleine}")
    logger.info(f"  output         : {output}")

    return {
        "data_root": data_root,
        "infoclimat": infoclimat,
        "wu_ichtegem": wu_ichtegem,
        "wu_lamadeleine": wu_lamadeleine,
        "output": output,
    }


# ============================================================
# CONVERSIONS
# ============================================================
def fahrenheit_to_celsius(f: float) -> float:
    return round((f - 32) * 5 / 9, 2)


def mph_to_kmh(mph: float) -> float:
    return round(mph * 1.60934, 2)


def inhg_to_hpa(inhg: float) -> float:
    return round(inhg * 33.8639, 2)


def inches_to_mm(inches: float) -> float:
    return round(inches * 25.4, 2)


def parse_wu_value(raw) -> float:
    """
    Parse une valeur WU type '57.7\xa0°F' -> 57.7
    Gère aussi virgule décimale.
    Retourne NaN si non parsable.
    """
    if raw is None:
        return np.nan
    if isinstance(raw, float) and math.isnan(raw):
        return np.nan
    if isinstance(raw, (int, float)):
        return float(raw)

    s = str(raw).replace("\xa0", " ").strip()
    if s in {"", "-", "--", "N/A"}:
        return np.nan

    # Ex: "0 w/m²" -> "0"
    parts = s.split()
    token = parts[0] if parts else s
    token = token.replace(",", ".")
    # Ex: "< 0.1" -> "0.1"
    token = token.lstrip("<").strip()

    try:
        return float(token)
    except ValueError:
        return np.nan


def wind_text_to_degrees(text) -> float:
    if text is None or not isinstance(text, str) or text.strip() == "":
        return np.nan
    return WIND_DIR_MAP.get(text.strip(), np.nan)


# ============================================================
# PARSEUR SOURCE 1 : INFOCLIMAT (JSON)
# ============================================================
def parse_infoclimat(filepath: str | Path) -> pd.DataFrame:
    filepath = Path(filepath)
    logger.info(f"Parsing InfoClimat: {filepath}")

    with filepath.open("r", encoding="utf-8") as f:
        data = json.load(f)

    station_meta = {}
    for s in data.get("stations", []):
        station_meta[s["id"]] = {
            "station_name": s.get("name"),
            "latitude": s.get("latitude"),
            "longitude": s.get("longitude"),
            "elevation": s.get("elevation"),
            "station_type": s.get("type"),
        }

    all_records = []
    for station_id, records in data.get("hourly", {}).items():
        if str(station_id).startswith("_"):
            continue

        meta = station_meta.get(station_id, {})

        for rec in records:
            all_records.append({
                "source": "infoclimat",
                "station_id": station_id,
                "station_name": meta.get("station_name"),
                "latitude": meta.get("latitude"),
                "longitude": meta.get("longitude"),
                "elevation": meta.get("elevation"),
                "station_type": meta.get("station_type"),
                "timestamp": rec.get("dh_utc"),
                "temperature_c": rec.get("temperature"),
                "dew_point_c": rec.get("point_de_rosee"),
                "humidity_pct": rec.get("humidite"),
                "wind_direction_deg": rec.get("vent_direction"),
                "wind_speed_kmh": rec.get("vent_moyen"),
                "wind_gust_kmh": rec.get("vent_rafales"),
                "pressure_hpa": rec.get("pression"),
                "precip_rate_mm": rec.get("pluie_1h"),
                "precip_accum_mm": rec.get("pluie_3h"),
                "visibility_m": rec.get("visibilite"),
                "cloud_cover_octas": rec.get("nebulosite"),
                "snow_depth_cm": rec.get("neige_au_sol"),
                "weather_code": rec.get("temps_omm"),
                "uv_index": None,
                "solar_radiation_wm2": None,
            })

    df = pd.DataFrame(all_records)

    numeric_cols = [
        "temperature_c", "dew_point_c", "humidity_pct",
        "wind_direction_deg", "wind_speed_kmh", "wind_gust_kmh",
        "pressure_hpa", "precip_rate_mm", "precip_accum_mm",
        "visibility_m", "snow_depth_cm", "cloud_cover_octas",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    logger.info(f"  → {len(df)} enregistrements, {df['station_id'].nunique()} stations")
    return df


# ============================================================
# PARSEUR SOURCES 2 & 3 : WEATHER UNDERGROUND (XLSX)
# ============================================================
def parse_weather_underground(filepath: str | Path, station_id: str) -> pd.DataFrame:
    filepath = Path(filepath)
    logger.info(f"Parsing Weather Underground: {filepath} (station: {station_id})")

    meta = WU_STATIONS[station_id]
    xl = pd.ExcelFile(filepath)
    all_dfs = []

    for sheet_name in xl.sheet_names:
        # date depuis le nom d'onglet (ex "011024")
        try:
            day = int(sheet_name[:2])
            month = int(sheet_name[2:4])
            year = 2000 + int(sheet_name[4:6])
            sheet_date = datetime(year, month, day).date()
        except (ValueError, IndexError):
            logger.warning(f"  Sheet '{sheet_name}' : nom non parsable, skip")
            continue

        df_sheet = pd.read_excel(filepath, sheet_name=sheet_name, header=0)
        df_sheet = df_sheet.dropna(how="all").reset_index(drop=True)

        if df_sheet.empty:
            logger.warning(f"  Sheet '{sheet_name}' vide, skip")
            continue

        # Timestamp : tolérant (time / datetime / string)
        def to_ts(t):
            if pd.isna(t):
                return pd.NaT
            if isinstance(t, datetime):
                return datetime.combine(sheet_date, t.time())
            if hasattr(t, "hour") and hasattr(t, "minute"):
                return datetime.combine(sheet_date, t)
            parsed = pd.to_datetime(str(t), errors="coerce")
            if pd.isna(parsed):
                return pd.NaT
            return datetime.combine(sheet_date, parsed.to_pydatetime().time())

        if "Time" not in df_sheet.columns:
            logger.warning(f"  Sheet '{sheet_name}' sans colonne 'Time', skip")
            continue

        df_sheet["timestamp"] = df_sheet["Time"].apply(to_ts)

        # Conversions WU -> métrique
        df_sheet["temperature_c"] = df_sheet["Temperature"].apply(parse_wu_value).apply(
            lambda x: fahrenheit_to_celsius(x) if pd.notna(x) else np.nan
        )
        df_sheet["dew_point_c"] = df_sheet["Dew Point"].apply(parse_wu_value).apply(
            lambda x: fahrenheit_to_celsius(x) if pd.notna(x) else np.nan
        )
        df_sheet["humidity_pct"] = df_sheet["Humidity"].apply(parse_wu_value)
        df_sheet["wind_direction_deg"] = df_sheet["Wind"].apply(wind_text_to_degrees)
        df_sheet["wind_speed_kmh"] = df_sheet["Speed"].apply(parse_wu_value).apply(
            lambda x: mph_to_kmh(x) if pd.notna(x) else np.nan
        )
        df_sheet["wind_gust_kmh"] = df_sheet["Gust"].apply(parse_wu_value).apply(
            lambda x: mph_to_kmh(x) if pd.notna(x) else np.nan
        )
        df_sheet["pressure_hpa"] = df_sheet["Pressure"].apply(parse_wu_value).apply(
            lambda x: inhg_to_hpa(x) if pd.notna(x) else np.nan
        )
        df_sheet["precip_rate_mm"] = df_sheet["Precip. Rate."].apply(parse_wu_value).apply(
            lambda x: inches_to_mm(x) if pd.notna(x) else np.nan
        )
        df_sheet["precip_accum_mm"] = df_sheet["Precip. Accum."].apply(parse_wu_value).apply(
            lambda x: inches_to_mm(x) if pd.notna(x) else np.nan
        )
        df_sheet["uv_index"] = pd.to_numeric(df_sheet["UV"], errors="coerce")
        df_sheet["solar_radiation_wm2"] = df_sheet["Solar"].apply(parse_wu_value)

        # Métadonnées station
        df_sheet["source"] = "weather_underground"
        df_sheet["station_id"] = station_id
        df_sheet["station_name"] = meta["station_name"]
        df_sheet["latitude"] = meta["latitude"]
        df_sheet["longitude"] = meta["longitude"]
        df_sheet["elevation"] = meta["elevation"]
        df_sheet["station_type"] = meta["station_type"]

        # Champs non dispo WU
        df_sheet["visibility_m"] = None
        df_sheet["cloud_cover_octas"] = None
        df_sheet["snow_depth_cm"] = None
        df_sheet["weather_code"] = None

        # Sélection finale
        missing_cols = [c for c in TARGET_COLUMNS if c not in df_sheet.columns]
        if missing_cols:
            raise KeyError(f"Colonnes manquantes dans WU ({sheet_name}): {missing_cols}")

        all_dfs.append(df_sheet[TARGET_COLUMNS])

    if not all_dfs:
        logger.warning(f"Aucune donnée exploitable dans {filepath}")
        return pd.DataFrame(columns=TARGET_COLUMNS)

    df = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"  → {len(df)} enregistrements")
    return df


# ============================================================
# VALIDATION QUALITÉ
# ============================================================
def validate_dataframe(df: pd.DataFrame) -> dict:
    total = len(df)
    if total == 0:
        return {
            "total_records": 0,
            "records_per_source": {},
            "records_per_station": {},
            "date_range": {"min": None, "max": None},
            "null_rates": {},
            "duplicates": 0,
            "anomalies": ["DataFrame vide"],
        }

    metrics = {
        "total_records": total,
        "records_per_source": df["source"].value_counts(dropna=False).to_dict(),
        "records_per_station": df["station_id"].value_counts(dropna=False).to_dict(),
        "date_range": {
            "min": str(df["timestamp"].min()),
            "max": str(df["timestamp"].max()),
        },
        "null_rates": {},
        "duplicates": 0,
        "invalid_timestamp": int(df["timestamp"].isna().sum()),
        "missing_station_id": int(df["station_id"].isna().sum()),
    }

    for col in TARGET_COLUMNS:
        null_count = df[col].isna().sum()
        metrics["null_rates"][col] = round(null_count / total * 100, 2)

    duplicates = df.duplicated(subset=["station_id", "timestamp"], keep=False)
    metrics["duplicates"] = int(duplicates.sum())

    anomalies = []
    if pd.notna(df["temperature_c"].min()) and df["temperature_c"].min() < -50:
        anomalies.append(f"Température min suspecte: {df['temperature_c'].min()}°C")
    if pd.notna(df["temperature_c"].max()) and df["temperature_c"].max() > 60:
        anomalies.append(f"Température max suspecte: {df['temperature_c'].max()}°C")
    if pd.notna(df["humidity_pct"].max()) and df["humidity_pct"].max() > 100:
        anomalies.append(f"Humidité > 100%: {df['humidity_pct'].max()}")
    if pd.notna(df["pressure_hpa"].min()) and df["pressure_hpa"].min() < 870:
        anomalies.append(f"Pression min suspecte: {df['pressure_hpa'].min()} hPa")

    metrics["anomalies"] = anomalies
    return metrics


# ============================================================
# EXPORT JSONL (recommandé ingestion)
# ============================================================
def export_jsonl(df: pd.DataFrame, output_path: str | Path) -> None:
    """
    Exporte 1 document par ligne (JSONL).
    Convertit NaN -> null, timestamp -> ISO string.
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    df_out = df.copy()
    df_out = df_out.where(pd.notna(df_out), None)

    def ts_to_iso(x):
        if x is None or pd.isna(x):
            return None
        if isinstance(x, str):
            return x
        try:
            return x.isoformat()
        except Exception:
            return str(x)

    df_out["timestamp"] = df_out["timestamp"].apply(ts_to_iso)

    with out.open("w", encoding="utf-8") as f:
        for rec in df_out.to_dict(orient="records"):
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    logger.info(f"Export JSONL: {len(df_out)} documents → {out}")


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="ETL météo multi-sources → JSONL unifié")
    parser.add_argument(
        "--data-root",
        default=None,
        help="Dossier contenant les fichiers d'entrée (default: env DATA_ROOT puis /home/melkia/P8/data)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Fichier de sortie. Si relatif: écrit dans <data-root>/airbyte/<output>",
    )
    args = parser.parse_args()

    paths = resolve_paths(args.data_root, args.output)

    df_infoclimat = parse_infoclimat(paths["infoclimat"])
    df_ichtegem = parse_weather_underground(paths["wu_ichtegem"], "IICHTE19")
    df_lamadeleine = parse_weather_underground(paths["wu_lamadeleine"], "ILAMAD25")

    df_all = pd.concat([df_infoclimat, df_ichtegem, df_lamadeleine], ignore_index=True)
    logger.info(f"Total après concat: {len(df_all)} enregistrements")

    metrics = validate_dataframe(df_all)
    logger.info("=== RAPPORT QUALITÉ ===")
    logger.info(f"  Total: {metrics['total_records']} records")
    logger.info(f"  Par source: {metrics['records_per_source']}")
    logger.info(f"  Par station: {metrics['records_per_station']}")
    logger.info(f"  Plage dates: {metrics['date_range']}")
    logger.info(f"  Doublons: {metrics['duplicates']}")
    logger.info(f"  Invalid timestamp: {metrics.get('invalid_timestamp')}")
    logger.info(f"  Missing station_id: {metrics.get('missing_station_id')}")
    for a in metrics.get("anomalies", []):
        logger.warning(f"  ANOMALIE: {a}")

    for col, rate in metrics.get("null_rates", {}).items():
        if rate > 50:
            logger.info(f"  Null rate élevé: {col} = {rate}%")

    export_jsonl(df_all, paths["output"])

    report_path = Path(paths["output"]).with_suffix(".quality.json")
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, default=str, ensure_ascii=False)
    logger.info(f"Rapport qualité → {report_path}")


if __name__ == "__main__":
    main()
