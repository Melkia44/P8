
Projet P8 – Step 1

Collecte, transformation et contrôle qualité de données météo

Objectif

Mettre en place un pipeline simple et robuste pour :

collecter des données météo depuis plusieurs sources via Airbyte ;

transformer ces données dans un format propre et cohérent, compatible avec MongoDB ;

intégrer les métadonnées des stations météo amateurs ;

contrôler automatiquement la qualité des données avant toute migration en base NoSQL.

L’idée est de préparer des données faciles à analyser pour un data scientist, sans encore les stocker dans MongoDB.

Sources de données
Données météo (observations)

Weather Underground (WU)
Données issues de fichiers Excel, converties par Airbyte en JSONL.

API météo “hourly”
Données JSON contenant des observations horaires pour plusieurs stations.

Les données brutes sont stockées dans un bucket S3 :

s3://oc-meteo-staging-data/raw/dataset_meteo/

Métadonnées des stations (référentiel)

Deux stations amateurs sont intégrées explicitement, comme demandé dans l’énoncé :

ILAMAD25 – La Madeleine (France)

IICHTE19 – WeerstationBS (Ichtegem, Belgique)

Ces métadonnées sont fixes (localisation, matériel, logiciel…) et séparées des observations.

Principe de transformation (explication simple)

On peut résumer le travail comme ceci :

Les données brutes sont “jolies à lire” pour un humain,
mais pas pratiques pour un ordinateur.
Le script enlève tout ce qui est décoratif (unités, symboles, formats variables)
et garde uniquement des valeurs numériques propres et comparables.

Exemples

"56.8 °F" → temperature_c = 13.78

"29.48 in" → pressure_hpa = 998.3

"8.2 mph" → wind_speed_kmh = 13.2

Toutes les données sont :

en unités métriques ;

en UTC pour les dates ;

typées (int, float, string).

Structure des données produites
1. Observations météo

Fichier généré :

output/observations.jsonl


Chaque ligne correspond à une observation météo :

{
  "obs_datetime": "2026-02-09T00:04:00+00:00",
  "station_id": "ILAMAD25",
  "station_provider": "WU",
  "temperature_c": 13.78,
  "humidity_pct": 87,
  "pressure_hpa": 998.3,
  "wind_speed_kmh": 13.2,
  "wind_gust_kmh": 16.7,
  "precip_rate_mm": 0.0,
  "source": "WU_LA_MADELEINE",
  "record_hash": "..."
}


Ce format est directement compatible avec MongoDB (une ligne = un document).

2. Métadonnées des stations

Fichier généré :

output/stations.json

{
  "station_id": "ILAMAD25",
  "name": "La Madeleine",
  "lat": 50.659,
  "lon": 3.07,
  "elevation_m": 23,
  "city": "La Madeleine",
  "hardware": "other",
  "software": "EasyWeatherPro_V5.1.6"
}


Ces données servent de référentiel et seront utilisées plus tard lors de l’import MongoDB (Step 2).

Contrôles qualité automatisés

Un script dédié analyse les données transformées et produit un rapport :

output/quality_report.json

Contrôles réalisés

présence des champs obligatoires ;

typage correct des valeurs ;

détection des doublons (via un hash stable) ;

comptage des valeurs nulles par champ ;

vérification des plages min / max (température, pression, pluie, vent).

Exemple de résultats

4 950 observations analysées

0 champ manquant

0 doublon

valeurs cohérentes et réalistes

taux d’erreur estimé : 0 %

Les valeurs nulles observées concernent uniquement des champs optionnels ou dépendants de la source (ex : pluie sur certaines stations).

Exécution du pipeline (Step 1)
Prérequis

Python 3.10+

Accès AWS configuré (~/.aws/credentials)

Environnement virtuel Python

Commandes
source .venv/bin/activate
python src/main.py
python src/quality_checks.py

Sorties

fichiers locaux dans output/

fichiers uploadés dans S3 :

s3://oc-meteo-staging-data/processed/dataset_meteo/<SOURCE_TAG>/

Choix d’architecture (justification)

Séparation observations / stations pour éviter la redondance.

Transformation avant stockage pour simplifier l’analyse.

Tests qualité automatisés pour sécuriser la migration future.

Pas de MongoDB à cette étape :
la base NoSQL est introduite uniquement au Step 2, conformément à l’énoncé.

Conclusion

À l’issue de ce Step 1 :

les données sont propres, cohérentes et documentées ;

les stations météo sont intégrées ;

la qualité est mesurée et tracée ;

le jeu de données est prêt à être importé dans MongoDB.

Le pipeline est reproductible, lisible et aligné avec les bonnes pratiques Data Engineering niveau Master 2.