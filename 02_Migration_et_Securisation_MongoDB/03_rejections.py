from __future__ import annotations

import os
import json
from collections import Counter
from datetime import datetime
from typing import Any, Set

from dotenv import load_dotenv


def parse_dt(value: Any):
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


def load_station_ids(stations_path: str) -> Set[str]:
    with open(stations_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        data = [data]
    out = set()
    for s in data:
        if isinstance(s, dict) and s.get("station_id"):
            out.add(s["station_id"])
    return out


def main():
    load_dotenv()

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

    default_obs_path = os.path.join(base_dir, "output", "01_local_processing", "observations.jsonl")
    default_stations_path = os.path.join(base_dir, "output", "01_local_processing", "stations.json")

    env_obs_path = os.getenv("OBS_PATH")
    env_stations_path = os.getenv("STATIONS_PATH")

    # Resolve OBS path
    if env_obs_path:
        obs_path = env_obs_path if os.path.isabs(env_obs_path) else os.path.join(base_dir, env_obs_path.lstrip("./"))
    else:
        obs_path = default_obs_path

    # Resolve stations path (for FK check)
    if env_stations_path:
        stations_path = env_stations_path if os.path.isabs(env_stations_path) else os.path.join(base_dir, env_stations_path.lstrip("./"))
    else:
        stations_path = default_stations_path

    print("OBS_PATH used:", obs_path)
    print("STATIONS_PATH used:", stations_path)

    station_ids = set()
    fk_check_enabled = False
    if os.path.exists(stations_path):
        station_ids = load_station_ids(stations_path)
        fk_check_enabled = True
        print("FK check enabled. stations loaded:", len(station_ids))
    else:
        print("FK check disabled (stations.json not found).")

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

    unknown_values = {"UNKNOWN", "WU:UNKNOWN", "SRC1:UNKNOWN"}

    for d in iter_jsonl(obs_path):
        total += 1
        r = []

        # Champs requis
        for k in required:
            if k not in d or d[k] in (None, ""):
                r.append(f"missing_{k}")

        # station_id
        sid = d.get("station_id")
        if sid in (None, ""):
            r.append("missing_station_id")
        elif sid in unknown_values:
            r.append("unknown_station_id")
        elif fk_check_enabled and sid not in station_ids:
            r.append("fk_missing_station")

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
    for k, v in reasons.most_common(30):
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
