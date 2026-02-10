Projet P8 – Pipeline Data Engineering & MongoDB
Objectif

Concevoir et mettre en œuvre un pipeline complet de traitement de données météorologiques multi-sources, depuis l’ingestion jusqu’au stockage sécurisé dans une base MongoDB conteneurisée, avec normalisation, contrôles qualité et validation de schéma côté base.

Le projet couvre :

ingestion de données hétérogènes (Excel, JSON),
normalisation dans un schéma commun,
contrôles qualité pré-migration,
migration sécurisée vers MongoDB,
validation de schéma et intégrité des données,
mesure de la qualité post-migration,
démonstration des opérations CRUD.
Architecture du pipeline

Step 1 – Récupération & transformation des données

Collecte de données issues de plusieurs sources météo.
Normalisation dans un modèle cible commun.
Génération de fichiers propres (JSON, JSONL).
Calcul d’un rapport de qualité pré-migration.

Step 2 – Migration & sécurisation MongoDB

Déploiement d’une instance MongoDB via Docker Compose.
Création des collections et index.
Application de validateurs de schéma MongoDB ($jsonSchema).
Migration contrôlée des données via scripts Python.

Step 3 – Conteneurisation

Exécution du pipeline dans un environnement Docker.
Séparation claire entre exécution locale et conteneurisée.

Step 4 – Déploiement cloud

Utilisation d’AWS S3 comme zone de staging des données issues d’Airbyte.

Rôle d’Airbyte

Airbyte est utilisé pour l’extraction et le chargement des données sources vers une zone de staging (AWS S3).
Les étapes de transformation, de contrôle qualité et de migration MongoDB sont volontairement réalisées hors Airbyte afin de :
maîtriser les règles métier,
centraliser la logique de qualité des données,
garantir la cohérence des données avant insertion en base.

Arborescence du projet
P8/
├── .venv/
│
├── 01_Recuperation_et_Transformation_Donnees/
│   ├── main.py                     # Orchestration collecte + normalisation
│   ├── stations.py                # Traitement des stations météo
│   ├── quality_checks.py           # Contrôles qualité pré-migration
│   ├── utils.py                    # Fonctions utilitaires
│
├── 02_Migration_et_Securisation_MongoDB/
│   ├── __init__.py
│   ├── 01_provision_mongo.py       # Création collections, schémas, index
│   ├── 02_migrate_to_mongo.py      # Migration + qualité post-migration + CRUD
│   └── 03_rejections.py            # Analyse des documents rejetés
│
├── 03_Containerisation_Docker/
│   ├── docker-compose.yml          # MongoDB
│   ├── Dockerfile                  # Image Python pipeline
│   ├── main.py                     # Exécution pipeline conteneurisée
│   └── requirements.txt
│
├── 04_Deploiement_AWS/
│
├── data/
│   ├── airbyte/
│   │   └── docker-compose.yaml     # Stack Airbyte locale
│   ├── excel/
│   │   ├── Weather_Ichtegem_BE.xlsx
│   │   └── Weather_La_Madeleine_FR.xlsx
│   └── json/
│       └── Data_Source1_011024-071024.json
│
├── output/
│   ├── 01_local_processing/        # Données clean pré-migration
│   └── 02_local_processing/        # Données post-migration & rapports qualité
│
├── .env
├── .gitignore
├── requirements.txt
└── README.md

Prérequis

Python 3.10+
Docker & Docker Compose
Accès AWS S3
Ports MongoDB disponibles localement


Configuration (.env)
# AWS / S3
S3_BUCKET=oc-meteo-staging-data
S3_PREFIX_RAW=raw/dataset_meteo/
S3_PREFIX_OUT=processed/dataset_meteo/

# MongoDB
MONGO_URI=mongodb://localhost:27017
MONGO_DB=meteo

Démarrage rapide
# Démarrage MongoDB
docker compose -f 03_Containerisation_Docker/docker-compose.yml up -d

# Transformation & qualité pré-migration
python3 01_Recuperation_et_Transformation_Donnees/main.py

# Provisioning MongoDB
python3 02_Migration_et_Securisation_MongoDB/01_provision_mongo.py

# Migration + qualité post-migration + CRUD
python3 02_Migration_et_Securisation_MongoDB/02_migrate_to_mongo.py


Le pipeline est idempotent et peut être relancé sans effet de bord.

Provisioning MongoDB

Le script 01_provision_mongo.py :

crée les collections stations et observations,
applique des validateurs de schéma MongoDB ($jsonSchema),
crée les index suivants :
unicité sur station_id,
unicité sur record_hash,
index temporels et fonctionnels.

Les contraintes sont appliquées côté base afin de garantir l’intégrité des données.

Migration & qualité post-migration

Le script 02_migrate_to_mongo.py :

importe les données propres depuis les fichiers générés,
convertit les champs temporels (datetime),
applique des upserts basés sur des clés d’unicité,
rejette automatiquement les documents non conformes,
génère un rapport de qualité post-migration,
démontre les opérations CRUD via script Python.
Qualité des données
Qualité pré-migration
vérification des types,
champs obligatoires,
doublons,
valeurs manquantes.

Un rapport est généré dans output/01_local_processing.

Qualité post-migration

Un rapport est généré automatiquement :

output/02_local_processing/quality_post_mongo.json

Il contient :

nombre total de documents traités,
documents insérés,
documents rejetés,
taux d’erreur post-migration.

Les rejets correspondent à des mécanismes de validation MongoDB et non à des incohérences métier.

Opérations CRUD

Le script de migration inclut une démonstration complète :
Create
Read
Update
Delete

Toutes les opérations sont réalisées via script Python.

Choix de conception

Pipeline multi-sources avec normalisation centralisée.
Validation de la qualité côté base MongoDB.
Séparation claire des responsabilités.
Pipeline idempotent et relançable.
Déploiement MongoDB conteneurisé pour reproductibilité.
Architecture compatible industrialisation.

Logigramme du pipeline

Diagramme Mermaid :
https://mermaid.ai/d/19e27a95-edb3-48dd-8376-31d66ff93959

flowchart TD
    A[Sources météo<br/>Excel / JSON] --> B[Transformation Python]
    B --> C[Qualité pré-migration]
    B --> D[Fichiers clean]

    E[MongoDB Docker] --> F[Provisioning]
    F --> G[Schémas & index]

    D --> H[Migration MongoDB]
    H --> I[Qualité post-migration]
    H --> J[CRUD]
