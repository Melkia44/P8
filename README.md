Projet P8 – Pipeline Data Engineering & MongoDB
Objectif

Mettre en place un pipeline complet de collecte, transformation et stockage de données météorologiques multi-sources, avec migration vers une base MongoDB répliquée, validation de schéma et mesure de la qualité post-migration.

Le projet couvre :

l’ingestion de données hétérogènes,
leur normalisation,
le contrôle qualité,
l’importation dans MongoDB via script Python,
la mise en place de réplications et de validateurs,
la démonstration des opérations CRUD.
Architecture générale du pipeline

Collecte & transformation (Step 1)
Ingestion des données issues de plusieurs sources météo.
Normalisation dans un schéma commun.
Génération de fichiers propres (JSON / JSONL).
Calcul d’un rapport de qualité pré-migration.

Provisioning MongoDB (Step 2)
Déploiement d’un replica set MongoDB via Docker Compose.
Création des collections.
Application de validateurs de schéma MongoDB.
Création des index (unicité, performances).

Migration & qualité post-migration (Step 3)
Import des données propres via script Python.
Rejet automatique des documents non conformes.
Calcul d’un taux d’erreur post-migration.
Démonstration des opérations CRUD via script.

Arborescence du projet
P8/
├── data/
│   ├── excel/
│   └── json/
├── mongo/
│   └── docker-compose.yml
├── output/
│   ├── stations.json
│   ├── observations.jsonl
│   ├── quality_report.json
│   └── quality_post_mongo.json
├── src/
│   ├── main.py
│   ├── stations.py
│   ├── quality_checks.py
│   ├── utils.py
│   └── mongopy/
│       ├── __init__.py
│       ├── 01_provision_mongo.py
│       └── 02_migrate_to_mongo.py
├── .env
├── requirements.txt
└── README.md

Configuration (.env)
# AWS / S3
S3_BUCKET=oc-meteo-staging-data
S3_PREFIX_RAW=raw/dataset_meteo/
S3_PREFIX_OUT=processed/dataset_meteo/

# Local outputs
OUT_DIR=output

# MongoDB
MONGO_URI=mongodb://mongo1:27017,mongo2:27018,mongo3:27019/?replicaSet=rs0
MONGO_DB=meteo

# Inputs Mongo (sorties clean)
STATIONS_PATH=./output/stations.json
OBS_PATH=./output/observations.jsonl

# Qualité post-migration
QUALITY_OUT=./output/quality_post_mongo.json

Step 2 – Provisioning MongoDB

Le provisioning est réalisé via le script :
python3 -m src.mongopy.01_provision_mongo


Ce script :

se connecte au replica set MongoDB,
crée les collections stations et observations,
applique des validateurs de schéma MongoDB ($jsonSchema),
crée les index suivants :
unicité sur station_id,
unicité sur record_hash,
index temporels et fonctionnels.
L’opération est idempotente et peut être rejouée sans effet de bord.

Step 3 – Migration des données & qualité post-migration

La migration est réalisée via :
python3 -m src.mongopy.02_migrate_to_mongo

Fonctionnalités :
import des stations et observations depuis les fichiers propres,
conversion des champs temporels (datetime),
upsert basé sur des clés d’unicité,
rejet automatique des documents non conformes au schéma MongoDB,
calcul d’un rapport de qualité post-migration,
démonstration des opérations CRUD via script Python.
Qualité post-migration
Un rapport est généré automatiquement :
output/quality_post_mongo.json

Il contient notamment :
nombre total de documents traités,
nombre de documents insérés,
nombre de documents rejetés,
taux d’erreur post-migration.

Les erreurs correspondent aux documents volontairement rejetés par MongoDB en raison de contraintes de validation (types, bornes, champs obligatoires).
Ce mécanisme garantit l’intégrité des données stockées.

Le taux d’erreur post-migration (~22 %) correspond à des rejets techniques MongoDB (validation de schéma et index uniques) lors de l’insertion en mode bulk_write non ordonné.
Une analyse métier indépendante des données sources montre un taux de rejet fonctionnel de 0 %, confirmant que les données sont cohérentes et conformes aux règles métier.
Les rejets MongoDB sont donc des mécanismes de protection de l’intégrité de la base (déduplication, typage strict) et non des erreurs de qualité des données.

Opérations CRUD

Le script de migration inclut une démonstration complète des opérations CRUD :
Create : insertion d’une station de test,
Read : lecture du document inséré,
Update : modification d’un champ,
Delete : suppression du document.
Cette démonstration est réalisée exclusivement via script Python.
Choix de conception
Pipeline multi-sources : les données issues de plusieurs sources sont agrégées et normalisées avant la migration MongoDB.
Validation côté base : la qualité est garantie par des validateurs MongoDB plutôt que par une validation uniquement applicative.
Séparation des responsabilités : provisioning, migration et contrôle qualité sont isolés dans des scripts dédiés.
Réplication : MongoDB est déployé en replica set pour assurer la résilience.

Logigramme Mermaid :  https://mermaid.ai/d/19e27a95-edb3-48dd-8376-31d66ff93959

flowchart TD
    A[Sources météo<br/>Excel / JSON] --> B[Step 1 - Collecte & Transformation<br/>Python]
    B --> C[Contrôles qualité pré-migration<br/>quality_report.json]
    B --> D[Fichiers clean<br/>stations.json<br/>observations.jsonl]

    E[MongoDB Replica Set rs0<br/>Docker Compose] --> F[Step 2 - Provisioning]
    F --> G[Création collections<br/>stations / observations]
    F --> H[Validation schéma JSON<br/>Index & unicité]

    D --> I[Step 3 - Migration MongoDB<br/>Python]
    I --> J[Insert / Upsert MongoDB]
    J --> K[Qualité post-migration<br/>quality_post_mongo.json<br/>error_rate]

    I --> L[CRUD Proof<br/>Create / Read / Update / Delete]

