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
    },
]

# mapping pour relier un run WU à une station
SOURCE_TAG_TO_STATION_ID = {
    "WU_LA_MADELEINE": "ILAMAD25",
    "WU_ICHTEGEM": "IICHTE19",
}
