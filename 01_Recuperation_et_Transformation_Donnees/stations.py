from datetime import datetime, timezone

# -------------------------------------------------------------------
# Stations WU (Excel)
# - On garde les IDs "bruts" ici (ILAMAD25 / IICHTE19)
# - Le préfixage WU:... est fait dans main.py (normalize_station_id)
# -------------------------------------------------------------------
_NOW = datetime.now(timezone.utc).isoformat()

STATIONS = [
    {
        "station_id": "ILAMAD25",
        "name": "La Madeleine",
        "lat": 50.659,
        "lon": 3.07,
        "elevation_m": 23,
        "city": "La Madeleine",
        "state": "-/-",
        "hardware": "other",
        "software": "EasyWeatherPro_V5.1.6",
        "provider": "WU",
        "source": "WU_EXCEL",
        "created_at": _NOW,
        "updated_at": _NOW,
    },
    {
        "station_id": "IICHTE19",
        "name": "WeerstationBS",
        "lat": 51.092,
        "lon": 2.999,
        "elevation_m": 15,
        "city": "Ichtegem",
        "state": "-/-",
        "hardware": "other",
        "software": "EasyWeatherV1.6.6",
        "provider": "WU",
        "source": "WU_EXCEL",
        "created_at": _NOW,
        "updated_at": _NOW,
    },
]

# -------------------------------------------------------------------
# Mapping pour relier un run WU (SOURCE_TAG) à une station
# -------------------------------------------------------------------
SOURCE_TAG_TO_STATION_ID = {
    "WU_LA_MADELEINE": "ILAMAD25",
    "WU_ICHTEGEM": "IICHTE19",
}
