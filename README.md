# Forecast 2.0 - Pipeline ETL Meteo Cloud-Native

**Auteur :** Mathieu Lowagie
**Formation :** Master 2 Data Engineering - OpenClassrooms
**Projet :** P8 - Construisez et testez une infrastructure de donnees
**Date :** Fevrier 2026

---

## Objectif

Construire un pipeline ETL cloud-native pour collecter, transformer et stocker des donnees meteorologiques multi-sources destinees a alimenter des modeles de prevision de demande energetique (Forecast 2.0 - GreenAndCoop).

## Resultats obtenus

| Metrique | Valeur |
|----------|--------|
| Records charges | **4950** (3807 WU + 1143 InfoClimat) |
| Stations | **6** (2 WU + 4 InfoClimat) |
| Erreurs d'insertion | **0** |
| Latence d'acces | **13 ms** |
| Conformite schema | **100%** |
| Cout infrastructure | **~21 EUR/mois** |

---

## Architecture

[Sources locales]
    |
    | Airbyte (3 connecteurs)
    v
[S3 raw/] ------> 15 fichiers JSONL (format Airbyte)
    |
    | transform_s3.py (Python)
    v
[S3 Transform/] -> weather_data.jsonl (schema unifie 23 colonnes)
    |
    | load_mongodb_s3.py (Python)
    v
[MongoDB AWS ECS] -> weather_db.weather_data (4950 documents)

### Composants AWS

| Composant | Configuration | Etat |
|-----------|--------------|------|
| MongoDB ECS | Fargate 0.5vCPU, 1GB | RUNNING |
| EFS Storage | 6GB General Purpose | Persistant |
| S3 Bucket | oc-meteo-staging-data | Actif |
| Security Group | mongodb-forecast-sg | Configure |
| CloudWatch Logs | /ecs/mongodb-forecast | Actif |

---

## Structure du repo

P8/
├── README.md
├── .env.example
├── .gitignore
├── requirements.txt
│
├── 01_Recuperation_et_Transformation_Donnees/
│   └── transform.py
│
├── 02_Chargement_DB/
│   ├── load_mongodb.py
│   └── mongodb_report.json
│
├── 03_Docker/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── requirements.txt
│   ├── data/
│   ├── output/
│   └── scripts/
│       ├── transform.py
│       ├── load_mongodb.py
│       └── run_pipeline.py
│
├── 04_Deploiement_AWS/
│   └── Scripts/
│       ├── transform_s3.py
│       ├── load_mongodb_s3.py
│       └── test_mongodb_aws.py
│
├── 05_tests/
│   ├── __init__.py
│   └── test_transform.py
│
├── scripts/
│   ├── transform_s3.py
│   ├── load_mongodb_s3.py
│   └── requirements.txt
│
├── data/
│   ├── Data_Source1_011024-071024.json
│   ├── Ichtegem_BE.xlsx
│   └── La_Madeleine_FR.xlsx
│
└── docs/
    ├── SCHEMA_BDD.md
    ├── LOGIGRAMME.md
    └── ARCHITECTURE_AWS.md

---

## Sources de donnees

| Source | Type | Stations | Periode | Records |
|--------|------|----------|---------|---------|
| InfoClimat | JSON (API) | 4 (Hauts-de-France) | 01-07 Oct 2024 | 1143 |
| WU Belgique | Excel (XLSX) | IICHTE19 (Ichtegem) | Jan-Jul 2024 | 1899 |
| WU France | Excel (XLSX) | ILAMAD25 (La Madeleine) | Jan-Jul 2024 | 1908 |

## Transformations

| Mesure | Source (WU) | Cible | Formule |
|--------|-------------|-------|---------|
| Temperature | degF | degC | (F-32)*5/9 |
| Vent | mph | km/h | mph*1.60934 |
| Pression | inHg | hPa | inHg*33.8639 |
| Precip. | inches | mm | in*25.4 |
| Direction vent | Texte | Degres | Mapping cardinal -> 0-360 |

---

## Instructions d'execution

### Prerequis

python3 --version   # Python 3.11+
aws configure       # AWS CLI configure
pip install -r requirements.txt --break-system-packages

### Configuration

cp .env.example .env
# Editer .env avec vos credentials MongoDB et AWS

### Execution locale (Docker)

cd 03_Docker
docker compose up --build

### Execution AWS

source .env
python3 scripts/transform_s3.py
python3 scripts/load_mongodb_s3.py

### Tests

pytest 05_tests/ -v
export MONGO_URI='mongodb://admin:<password>@<ECS_IP>:27017/'
python3 04_Deploiement_AWS/Scripts/test_mongodb_aws.py

---

## Securite

- Credentials via variables d'environnement (.env non commite)
- Security Groups restrictifs (port 27017 uniquement depuis IP autorisee)
- EFS chiffre at rest + S3 SSE-S3 + TLS sur EFS en transit
- IAM Task Execution Role pour ECS

---

## Justifications techniques

### MongoDB (NoSQL)
- Schema flexible pour sources heterogenes
- Index optimises pour requetes time-series
- Scalabilite horizontale via sharding

### ECS Fargate (vs EC2)
- Serverless : pas de gestion serveurs
- Pay-per-use : ~21 EUR/mois pour le POC
- Auto-restart sur failure

### S3 comme zone de staging
- Decouplage Extract/Transform/Load
- Tracabilite : donnees brutes conservees
- Reprise sur erreur

---

## Documentation detaillee

| Document | Contenu |
|----------|---------|
| [SCHEMA_BDD.md](docs/SCHEMA_BDD.md) | Schema MongoDB 23 champs, index, validation |
| [LOGIGRAMME.md](docs/LOGIGRAMME.md) | Flowchart ETL complet |
| [ARCHITECTURE_AWS.md](docs/ARCHITECTURE_AWS.md) | Infrastructure AWS, securite, couts |

---


**Version :** 2.0
**Date de livraison :** 22 fevrier 2026
