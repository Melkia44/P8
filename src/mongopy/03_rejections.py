from __future__ import annotations

import os
import json
from collections import Counter
from datetime import datetime
from dotenv import load_dotenv


def parse_dt(value):
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


def iter_jsonl(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def main():
    load_dotenv()

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    default_path = os.path.join(base_dir, "output", "observations.jsonl")
    env_path = os.getenv("OBS_PATH")

    if env_path:
        if os.path.isabs(env_path):
            path = env_path
        else:
            path = os.path.join(base_dir, env_path.lstrip("./"))
    else:
        path = default_path

    print("OBS_PATH used:", path)

    reasons = Counter()
    total = 0
    rejectable = 0

    required = [
        "obs_datetime",
        "station_id",
        "station_provider",
        "source",
        "ingestion_ts",
        "record_hash",
    ]

    for d in iter_jsonl(path):
        total += 1
        r = []

        # Champs requis
        for k in required:
            if k not in d or d[k] in (None, ""):
                r.append(f"missing_{k}")

        # Dates
        if parse_dt(d.get("obs_datetime")) is None:
            r.append("bad_obs_datetime")
        if parse_dt(d.get("ingestion_ts")) is None:
            r.append("bad_ingestion_ts")

        # Bornes
        h = d.get("humidity_pct")
        if h is not None and not (0 <= h <= 100):
            r.append("humidity_out_of_range")

        p = d.get("pressure_hpa")
        if p is not None and not (800 <= p <= 1100):
            r.append("pressure_out_of_range")

        if r:
            rejectable += 1
            reasons.update(set(r))

    print("TOTAL:", total)
    print("REJECTABLE (>=1 reason):", rejectable)
    if total:
        print("REJECTABLE RATE:", rejectable / total)

    print("Top reasons:")
    for k, v in reasons.most_common(20):
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
