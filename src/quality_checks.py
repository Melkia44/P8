import json
import os
from collections import Counter, defaultdict
from typing import Any, Dict, List

OUT_DIR = os.getenv("OUT_DIR", "output")
OBS_PATH = os.path.join(OUT_DIR, "observations.jsonl")
REPORT_PATH = os.path.join(OUT_DIR, "quality_report.json")

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

def main():
    total = 0
    missing = Counter()
    nulls = Counter()
    types_mismatch = Counter()
    duplicates = 0
    hashes = set()

    mins = {}
    maxs = {}

    provider_count = Counter()
    station_count = Counter()

    with open(OBS_PATH, "r", encoding="utf-8") as f:
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
    }

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"Quality report written: {REPORT_PATH}")

if __name__ == "__main__":
    main()
