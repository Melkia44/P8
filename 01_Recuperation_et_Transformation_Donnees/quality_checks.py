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

    report: Dict[str, Any] = {
        "rows_total": total,
        "providers": dict(provider_count),
        "stations": dict(station_count),
        "missing_fields": dict(missing),
        "nulls": dict(nulls),
        "type_mismatches": dict(types_mismatch),
        "duplicate_hashes": duplicates,
        "min": mins,
        "max": maxs,
        "error_rate_estimate": (duplicates + sum(missing.values()) + sum(types_mismatch.values())) / max(total, 1),
        "paths": {
            "out_dir": str(OUT_DIR),
            "observations": str(OBS_PATH),
            "report": str(REPORT_PATH),
        },
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with REPORT_PATH.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"Quality report written: {REPORT_PATH}")

if __name__ == "__main__":
    main()
