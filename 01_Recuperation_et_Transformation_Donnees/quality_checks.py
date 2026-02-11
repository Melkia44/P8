import json
import os
from collections import Counter
from typing import Any, Dict

from pathlib import Path

# -------------------------------------------------------------------
# PROJECT ROOT & PATHS (STEP 1 ONLY)
# -------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_DIR = PROJECT_ROOT / "output" / "01_local_processing"

OUT_DIR = Path(os.getenv("OUT_DIR", str(DEFAULT_OUT_DIR)))
OBS_PATH = OUT_DIR / "observations.jsonl"
STATIONS_PATH = OUT_DIR / "stations.json"
REPORT_PATH = OUT_DIR / "quality_report.json"

EXPECTED_FIELDS = [
    "obs_datetime", "station_id", "station_provider", "source", "record_hash"
]

NUMERIC_FIELDS = [
    "temperature_c", "dew_point_c", "humidity_pct", "pressure_hpa",
    "wind_speed_kmh", "wind_gust_kmh",
    "precip_rate_mm", "precip_accum_mm", "precip_1h_mm", "precip_3h_mm"
]

def is_null(v: Any) -> bool:
    return v is None or v == ""

def main() -> None:
    if not OBS_PATH.exists():
        raise FileNotFoundError(
            f"Observations file not found: {OBS_PATH}\n"
            f"Run Step 1 first: python 01_Recuperation_et_Transformation_Donnees/main.py\n"
            f"Or override OUT_DIR to the correct folder."
        )

    if not STATIONS_PATH.exists():
        raise FileNotFoundError(
            f"Stations file not found: {STATIONS_PATH}\n"
            f"Run Step 1 first: python 01_Recuperation_et_Transformation_Donnees/main.py\n"
            f"Or override OUT_DIR to the correct folder."
        )

    # Load stations list to validate FK integrity
    with STATIONS_PATH.open("r", encoding="utf-8") as f:
        stations = json.load(f)

    station_ids = set()
    for s in stations:
        if isinstance(s, dict) and s.get("station_id"):
            station_ids.add(s["station_id"])

    total = 0
    missing = Counter()
    nulls = Counter()
    types_mismatch = Counter()
    duplicates = 0
    hashes = set()

    mins: Dict[str, float] = {}
    maxs: Dict[str, float] = {}

    provider_count = Counter()
    station_count = Counter()

    # New checks
    unknown_station_id = 0
    fk_missing_station = 0
    bad_datetime = 0

    with OBS_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            total += 1
            d = json.loads(line)

            provider_count[d.get("station_provider")] += 1
            station_count[d.get("station_id")] += 1

            # champs attendus
            for k in EXPECTED_FIELDS:
                if k not in d:
                    missing[k] += 1
                elif is_null(d.get(k)):
                    nulls[k] += 1

            # station_id integrity
            sid = d.get("station_id")
            if sid in (None, "", "UNKNOWN", "WU:UNKNOWN", "SRC1:UNKNOWN"):
                unknown_station_id += 1
            elif sid not in station_ids:
                fk_missing_station += 1

            # obs_datetime basic sanity check (ISO-like string)
            odt = d.get("obs_datetime")
            if not isinstance(odt, str) or "T" not in odt:
                bad_datetime += 1

            # doublons
            h = d.get("record_hash")
            if h in hashes:
                duplicates += 1
            else:
                hashes.add(h)

            # numeric checks
            for k in NUMERIC_FIELDS:
                if k not in d:
                    continue
                v = d.get(k)
                if v is None:
                    nulls[k] += 1
                    continue
                if not isinstance(v, (int, float)):
                    types_mismatch[k] += 1
                    continue
                mins[k] = v if k not in mins else min(mins[k], v)
                maxs[k] = v if k not in maxs else max(maxs[k], v)

    error_components = duplicates + sum(missing.values()) + sum(types_mismatch.values()) + fk_missing_station + unknown_station_id + bad_datetime

    report: Dict[str, Any] = {
        "rows_total": total,
        "providers": dict(provider_count),
        "stations_in_observations": dict(station_count),
        "stations_reference_count": len(station_ids),
        "missing_fields": dict(missing),
        "nulls": dict(nulls),
        "type_mismatches": dict(types_mismatch),
        "duplicate_hashes": duplicates,
        "fk_missing_station": fk_missing_station,
        "unknown_station_id": unknown_station_id,
        "bad_datetime": bad_datetime,
        "min": mins,
        "max": maxs,
        "error_rate_estimate": error_components / max(total, 1),
        "paths": {
            "out_dir": str(OUT_DIR),
            "observations": str(OBS_PATH),
            "stations": str(STATIONS_PATH),
            "report": str(REPORT_PATH),
        },
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with REPORT_PATH.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"Quality report written: {REPORT_PATH}")

if __name__ == "__main__":
    main()
