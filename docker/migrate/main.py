import os
import json
import math
from datetime import datetime, date, time
from pathlib import Path

import pandas as pd
from pymongo import MongoClient

EXCEL_DIR = Path(os.getenv("EXCEL_DIR", "/data/excel"))
JSON_DIR = Path(os.getenv("JSON_DIR", "/data/json"))
OUT_DIR = Path(os.getenv("OUT_DIR", "/data/out"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "meteo")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "observations")


def to_jsonable(obj):
    """Convertit récursivement en types JSON sérialisables."""
    if obj is None:
        return None

    # NaN / NaT
    try:
        if isinstance(obj, float) and math.isnan(obj):
            return None
    except Exception:
        pass

    # pandas Timestamp / datetime / date / time
    if isinstance(obj, pd.Timestamp):
        if pd.isna(obj):
            return None
        return obj.to_pydatetime().isoformat()
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, time):
        return obj.isoformat()

    # Containers
    if isinstance(obj, dict):
        return {str(k): to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]

    return obj


def normalize_record(rec: dict) -> dict:
    out = dict(rec)

    # Exemple de normalisation dh_utc si format texte
    if "dh_utc" in out and out["dh_utc"]:
        try:
            dt = datetime.strptime(str(out["dh_utc"]), "%Y-%m-%d %H:%M:%S")
            out["dh_utc"] = dt.isoformat() + "Z"
        except Exception:
            pass

    # Cast numériques
    for k in [
        "temperature", "pression", "humidite", "point_de_rosee",
        "vent_moyen", "vent_rafales", "vent_direction",
        "pluie_3h", "pluie_1h", "visibilite"
    ]:
        if k in out and out[k] not in (None, ""):
            try:
                out[k] = float(out[k])
            except Exception:
                pass

    out["_ingested_at"] = datetime.utcnow().isoformat() + "Z"
    return out


def read_json(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)

    records = []
    if isinstance(obj, list):
        records = obj
    elif isinstance(obj, dict):
        # cas 1: {"data": [...]}
        if "data" in obj and isinstance(obj["data"], list):
            records = obj["data"]
        # cas 2: {"data": {"xxx": [...], "yyy":[...]}}
        elif "data" in obj and isinstance(obj["data"], dict):
            for v in obj["data"].values():
                if isinstance(v, list):
                    records.extend(v)
        else:
            for v in obj.values():
                if isinstance(v, list):
                    records.extend(v)

    return [r for r in records if isinstance(r, dict)]


def read_excel(path: Path) -> list[dict]:
    df = pd.read_excel(path)

    # Nettoyage NaN/NaT + types bizarres
    df = df.where(pd.notnull(df), None)

    # Important: éviter les objets non sérialisables en dict
    records = df.to_dict(orient="records")
    return records


def list_files_safe(folder: Path, exts: tuple[str, ...]) -> list[Path]:
    if not folder.exists():
        print(f"[WARN] Dossier introuvable: {folder}")
        return []
    return [p for p in folder.rglob("*") if p.is_file() and p.suffix.lower() in exts]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    json_files = list_files_safe(JSON_DIR, (".json",))
    excel_files = list_files_safe(EXCEL_DIR, (".xlsx", ".xls"))

    if not json_files and not excel_files:
        print(f"[ERROR] Aucun fichier .json dans {JSON_DIR} et aucun .xlsx/.xls dans {EXCEL_DIR}")
        return

    all_records: list[dict] = []

    for p in json_files:
        recs = read_json(p)
        recs_norm = [normalize_record(r) for r in recs]
        print(f"[OK] JSON {p.name}: {len(recs_norm)} enregistrements")
        all_records.extend(recs_norm)

    for p in excel_files:
        recs = read_excel(p)
        recs_norm = [normalize_record(r) for r in recs]
        print(f"[OK] EXCEL {p.name}: {len(recs_norm)} enregistrements")
        all_records.extend(recs_norm)

    if not all_records:
        print("[ERROR] 0 record après lecture/normalisation")
        return

    # JSON-safe
    all_records = [to_jsonable(r) for r in all_records]

    # Export JSONL (preuve de transformation)
    out_jsonl = OUT_DIR / "observations_normalized.jsonl"
    with out_jsonl.open("w", encoding="utf-8") as f:
        for r in all_records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Insert Mongo (preuve de migration)
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]

    col.create_index([("id_station", 1), ("dh_utc", 1)], unique=False)

    res = col.insert_many(all_records)
    print(f"[MONGO] Inserted: {len(res.inserted_ids)} docs into {MONGO_DB}.{MONGO_COLLECTION}")
    print(f"[OUT] Export: {out_jsonl}")


if __name__ == "__main__":
    main()
