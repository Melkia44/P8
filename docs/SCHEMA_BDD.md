# 🗄️ SCHÉMA DE BASE DE DONNÉES - MongoDB

**Base :** `weather_db`  
**Collection :** `weather_data`  
**Type :** Document NoSQL (MongoDB 7)  
**Documents totaux :** 3807

---

## 📊 Structure de la collection

```
Collection: weather_data
│
├── _id : ObjectId
│   └── Clé primaire auto-générée par MongoDB
│
├── MÉTADONNÉES STATION (6 champs)
│   ├── source : String (required)
│   │   └── Valeurs: "infoclimat" | "weather_underground"
│   ├── station_id : String (required)
│   │   └── Ex: "IICHTE19", "ILAMAD25", "07015", "00052"
│   ├── station_name : String | null
│   │   └── Nom convivial de la station
│   ├── latitude : Double | null
│   │   └── Coordonnée GPS (-90 à 90°)
│   ├── longitude : Double | null
│   │   └── Coordonnée GPS (-180 à 180°)
│   ├── elevation : Double | null
│   │   └── Altitude en mètres
│   └── station_type : String | null
│       └── Ex: "weather_underground", "infoclimat_api", "synop"
│
├── HORODATAGE (1 champ)
│   └── timestamp : Date (required)
│       └── Format: ISO 8601 / ISODate
│       └── Ex: ISODate("2024-10-01T00:04:00Z")
│
├── TEMPÉRATURE ET HUMIDITÉ (3 champs)
│   ├── temperature_c : Double | null
│   │   └── Température en Celsius
│   │   └── Range: -60°C à +60°C
│   ├── dew_point_c : Double | null
│   │   └── Point de rosée en Celsius
│   └── humidity_pct : Double | null
│       └── Humidité relative en %
│       └── Range: 0 à 100%
│
├── VENT (3 champs)
│   ├── wind_direction_deg : Double | null
│   │   └── Direction du vent en degrés
│   │   └── Range: 0° à 360° (0° = Nord)
│   ├── wind_speed_kmh : Double | null
│   │   └── Vitesse moyenne du vent en km/h
│   └── wind_gust_kmh : Double | null
│       └── Rafales de vent en km/h
│
├── PRESSION ET PRÉCIPITATIONS (3 champs)
│   ├── pressure_hpa : Double | null
│   │   └── Pression atmosphérique en hPa
│   │   └── Range: 870 à 1084 hPa
│   ├── precip_rate_mm : Double | null
│   │   └── Taux de précipitation en mm/h
│   └── precip_accum_mm : Double | null
│       └── Cumul de précipitation en mm
│
├── VISIBILITÉ ET NÉBULOSITÉ (3 champs)
│   ├── visibility_m : Double | null
│   │   └── Visibilité horizontale en mètres
│   ├── cloud_cover_octas : Double | null
│   │   └── Couverture nuageuse en octas
│   │   └── Range: 0 (ciel dégagé) à 8 (ciel couvert)
│   └── snow_depth_cm : Double | null
│       └── Épaisseur de neige au sol en cm
│
├── CODES MÉTÉO (1 champ)
│   └── weather_code : String | null
│       └── Code OMM du temps présent
│       └── Ex: "10", "50", "80"
│
└── UV ET RADIATION SOLAIRE (2 champs)
    ├── uv_index : Double | null
    │   └── Indice UV
    │   └── Range: 0 (nuit) à 11+ (extrême)
    └── solar_radiation_wm2 : Double | null
        └── Radiation solaire en W/m²
```

---

## 🔑 Index MongoDB

### Index 1 : Index unique (station + timestamp)
```javascript
{
  "station_id": 1,
  "timestamp": 1
}
```
- **Type :** Unique
- **Nom :** `idx_station_ts`
- **Objectif :** Empêcher les doublons temporels par station
- **Performance :** O(log n) pour recherche exacte

### Index 2 : Index source
```javascript
{
  "source": 1
}
```
- **Nom :** `idx_source`
- **Objectif :** Filtrage rapide par source de données
- **Usage :** Requêtes analytiques par provenance

### Index 3 : Index timestamp
```javascript
{
  "timestamp": 1
}
```
- **Nom :** `idx_timestamp`
- **Objectif :** Recherche par plage temporelle
- **Usage :** Time-series queries, agrégations temporelles

---

## ✅ Validation du schéma (JSON Schema)

```javascript
{
  "$jsonSchema": {
    "bsonType": "object",
    "required": ["source", "station_id", "timestamp"],
    "properties": {
      "source": {
        "bsonType": "string",
        "description": "Source de données (required)"
      },
      "station_id": {
        "bsonType": "string",
        "description": "Identifiant unique station (required)"
      },
      "timestamp": {
        "bsonType": "date",
        "description": "Horodatage de la mesure (required)"
      },
      "temperature_c": {
        "bsonType": ["double", "null"],
        "minimum": -60,
        "maximum": 60,
        "description": "Température en Celsius"
      },
      "humidity_pct": {
        "bsonType": ["double", "null"],
        "minimum": 0,
        "maximum": 100,
        "description": "Humidité relative en %"
      },
      "pressure_hpa": {
        "bsonType": ["double", "null"],
        "minimum": 870,
        "maximum": 1084,
        "description": "Pression atmosphérique"
      }
    }
  }
}
```

**Niveau de validation :** `moderate`  
**Action en cas d'échec :** `warn` (log + insertion quand même)

---

## 📋 Exemple de document complet

```json
{
  "_id": ObjectId("6997153294afd5135c4486ad"),
  
  "source": "weather_underground",
  "station_id": "IICHTE19",
  "station_name": "WeerstationBS",
  "latitude": 51.092,
  "longitude": 2.999,
  "elevation": 15.0,
  "station_type": "weather_underground",
  
  "timestamp": ISODate("2024-10-01T00:04:00Z"),
  
  "temperature_c": 14.2,
  "dew_point_c": 12.8,
  "humidity_pct": 87.0,
  
  "wind_direction_deg": 225.0,
  "wind_speed_kmh": 13.0,
  "wind_gust_kmh": 16.4,
  
  "pressure_hpa": 1002.1,
  "precip_rate_mm": 0.0,
  "precip_accum_mm": 0.0,
  
  "visibility_m": null,
  "cloud_cover_octas": null,
  "snow_depth_cm": null,
  
  "weather_code": null,
  
  "uv_index": 0.0,
  "solar_radiation_wm2": 0.0
}
```

---

## 📊 Statistiques de la collection

| Métrique | Valeur |
|----------|--------|
| Documents totaux | 3807 |
| Taille moyenne document | ~500 bytes |
| Taille collection | ~2 MB |
| Index count | 3 |
| Index size | ~150 KB |

### Répartition par source
- `weather_underground` : 3807 documents (100%)
- `infoclimat` : 0 documents (données non chargées dans ce run)

### Répartition par station
- `IICHTE19` (Belgique) : 1899 documents (49.9%)
- `ILAMAD25` (France) : 1908 documents (50.1%)

### Période couverte
- **Début :** 2024-01-01 00:04:00
- **Fin :** 2024-07-10 14:59:00
- **Durée :** 191 jours (~6.4 mois)

---

## 🎯 Justification du schéma

### Pourquoi NoSQL (MongoDB) ?

✅ **Schéma flexible**
- Facilite l'ajout de nouvelles sources avec champs différents
- Pas de migration complexe pour ajouter colonnes

✅ **Performance lecture**
- Index optimisés pour time-series queries
- Agrégations rapides pour analytics

✅ **Scalabilité horizontale**
- Sharding possible sur `station_id` ou plages temporelles
- Replica sets pour haute disponibilité

### Pourquoi ce schéma unifié ?

✅ **Multi-sources dans même collection**
- Simplifie les requêtes cross-sources
- Facilite les analyses comparatives

✅ **Normalisation métrique**
- Toutes les unités harmonisées (système métrique)
- Pas de conversion côté client

✅ **Champs null vs absent**
- `null` indique "mesure non disponible"
- Absence totale du champ = "non applicable à cette source"

---

## 🔍 Requêtes MongoDB typiques

### Température moyenne par station
```javascript
db.weather_data.aggregate([
  {
    $group: {
      _id: "$station_id",
      avg_temp: { $avg: "$temperature_c" },
      min_temp: { $min: "$temperature_c" },
      max_temp: { $max: "$temperature_c" },
      count: { $sum: 1 }
    }
  },
  { $sort: { avg_temp: -1 } }
])
```

### Données d'une journée spécifique
```javascript
db.weather_data.find({
  timestamp: {
    $gte: ISODate("2024-10-01T00:00:00Z"),
    $lt: ISODate("2024-10-02T00:00:00Z")
  }
}).sort({ timestamp: 1 })
```

### Données avec vent fort (>50 km/h)
```javascript
db.weather_data.find({
  wind_speed_kmh: { $gt: 50 }
}).sort({ wind_speed_kmh: -1 })
```

---

**Version :** 1.0  
**Auteur :** Mathieu Melkia  
**Date :** 21 février 2026