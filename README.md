# 🌦️ Forecast 2.0 - Livrables Projet P8

**Auteur :** Mathieu Lowagie  
**Formation :** Master 2 Data Engineering - OpenClassrooms  
**Projet :** Construisez et testez une infrastructure de données  
**Date :** Février 2026

---

## 📦 Contenu des livrables

```
livrables_p8/
├── README.md (ce fichier)
├── docs/
│   ├── SCHEMA_BDD.md           # Schéma MongoDB détaillé
│   ├── LOGIGRAMME.md           # Processus ETL complet
│   └── ARCHITECTURE_AWS.md      # Infrastructure AWS déployée
└── scripts/
    ├── transform_s3_corrected.py   # Script transformation S3
    ├── load_mongodb_s3_final.py     # Script chargement MongoDB
    └── requirements.txt             # Dépendances Python
```

---

## 🎯 Résumé du projet

### Objectif

Construire un pipeline ETL cloud-native pour collecter, transformer et stocker des données météorologiques multi-sources destinées à alimenter des modèles de prévision de demande énergétique.

### Résultats obtenus

✅ **3807 records** météorologiques chargés  
✅ **2 stations** Weather Underground (BE + FR)  
✅ **7 mois** de données (Jan-Jul 2024)  
✅ **0 erreur** d'insertion  
✅ **13ms** de latence d'accès  
✅ **100%** de qualité des données

---

## 🏗️ Architecture déployée

### Composants AWS

| Composant | Configuration | État |
|-----------|--------------|------|
| **MongoDB ECS** | Fargate 0.5vCPU, 1GB | ✅ RUNNING |
| **EFS Storage** | 6GB General Purpose | ✅ Persistant |
| **S3 Bucket** | oc-meteo-staging-data | ✅ Actif |
| **Security Group** | mongodb-forecast-sg | ✅ Configuré |
| **CloudWatch Logs** | /ecs/mongodb-forecast | ✅ Actif |

### Pipeline ETL

```
[Sources locales]
    ↓ Airbyte
[S3 raw/] (15 fichiers JSONL)
    ↓ transform_s3.py
[S3 Transform/] (weather_data.jsonl)
    ↓ load_mongodb_s3.py
[MongoDB AWS ECS] (3807 documents)
```

---

## 📋 Livrables OpenClassrooms

### 1. Schéma de la base de données ✅

**Fichier :** `docs/SCHEMA_BDD.md`

**Contenu :**
- Structure complète de la collection MongoDB
- 23 champs détaillés
- 3 index (dont 1 unique)
- JSON Schema validation
- Exemples de documents

### 2. Logigramme du processus ✅

**Fichier :** `docs/LOGIGRAMME.md`

**Contenu :**
- Flow chart complet du pipeline ETL
- 4 phases : Extraction → Transformation → Chargement → Tests
- Points de décision
- Gestion des erreurs
- Temps d'exécution

### 3. Architecture de la base de données ✅

**Fichier :** `docs/ARCHITECTURE_AWS.md`

**Contenu :**
- Diagramme d'infrastructure AWS
- VPC, Security Groups, ECS, EFS, S3
- Configuration détaillée de chaque composant
- Sécurité et haute disponibilité
- Coûts estimés

### 4. Installation fonctionnelle d'Airbyte ✅

**Réalisé :**
- Airbyte local déployé (Docker Compose)
- 3 connexions configurées :
  - InfoClimat JSON → S3
  - WU Belgique XLSX → S3
  - WU France XLSX → S3
- 15 fichiers JSONL générés

### 5. Scripts de transformation ✅

**Fichier :** `scripts/transform_s3.py`

**Fonctionnalités :**
- Lit raw/ depuis S3
- Détecte type de source (IC/WU)
- Unifie formats
- Convertit unités (F°→C°, mph→km/h, etc.)
- Reconstruit timestamps
- Déduplique
- Valide qualité
- Écrit Transform/ sur S3

### 6. Script de chargement MongoDB ✅

**Fichier :** `scripts/load_mongodb_s3.py`

**Fonctionnalités :**
- Lit Transform/ depuis S3
- Configure collection + validation
- Crée index
- Bulk insert (batch 500)
- Gère doublons
- Rapport qualité

### 7. Reporting qualité des données ✅

**Métriques mesurées :**

| Métrique | Valeur |
|----------|--------|
| Temps d'accessibilité | 13.24 ms |
| Taux d'erreurs | 0% (0/3807) |
| Taux de documents valides | 100% |
| Doublons | 0 |

**Rapport généré :** `Transform/weather_data.quality.json`

### 8. Tests d'infrastructure ✅

**Tests réalisés :**
- ✅ Test connexion MongoDB
- ✅ Test CRUD complet
- ✅ Test performance (latence)
- ✅ Test persistance EFS
- ✅ Validation schéma

**Résultats :** 100% de réussite

### 9. Monitoring ✅

**CloudWatch configuré :**
- Log group : `/ecs/mongodb-forecast`
- Container Insights activé
- Métriques : CPU, Memory, Network

---

## 🔧 Transformations de données

### Conversions d'unités

| Mesure | Source (WU) | Cible | Formule |
|--------|-------------|-------|---------|
| Température | °F | °C | `(F-32)×5/9` |
| Vent | mph | km/h | `mph×1.60934` |
| Pression | inHg | hPa | `inHg×33.8639` |
| Précip. | inches | mm | `in×25.4` |
| Direction vent | Texte | Degrés | Mapping |

### Reconstruction timestamps

**Problème :** Excel contient `"12:04 AM"` sans date  
**Solution :** Extraction date depuis chemin S3

```
raw/BE/011024/ → Date: 2024-10-01
Time: "12:04 AM" → Heure: 00:04
Résultat: 2024-10-01T00:04:00
```

### Schéma unifié

**23 colonnes standardisées :**
- Métadonnées station (6 champs)
- Horodatage (1 champ)
- Température/Humidité (3 champs)
- Vent (3 champs)
- Pression/Précip (3 champs)
- Visibilité/Nébulosité (3 champs)
- Codes météo (1 champ)
- UV/Radiation (2 champs)

---

## ⚡ Performance

### Infrastructure

| Ressource | Spécification | Performance |
|-----------|--------------|-------------|
| MongoDB | 0.5 vCPU, 1GB | CPU: 15-20%, RAM: 29% |
| EFS | General Purpose | Latence: <1ms |
| S3 | Standard | Transfer: ~100KB/s |
| Réseau | VPC eu-west-3 | 13ms latence |

### Pipeline ETL

| Phase | Durée | Records/sec |
|-------|-------|-------------|
| Transformation | 2-3s | ~1500 |
| Chargement | 1.1s | ~3461 |
| **Total** | **~5s** | **~760** |

---

## 🚀 Instructions d'exécution

### Prérequis

```bash
# Python 3.11+
python3 --version

# AWS CLI configuré
aws configure

# Install dépendances
pip install -r scripts/requirements.txt --break-system-packages
```

### Transformation

```bash
export BUCKET_NAME=oc-meteo-staging-data
export AWS_REGION=eu-west-3
python3 scripts/transform_s3_corrected.py
```

### Chargement

```bash
export MONGO_URI=mongodb://admin:***@51.44.220.64:27017/
python3 scripts/load_mongodb_s3_final.py
```

---

## 📊 Justifications techniques

### Choix MongoDB (NoSQL)

✅ **Schéma flexible** - Ajout facile de nouvelles sources  
✅ **Performance lecture** - Index optimisés time-series  
✅ **Scalabilité horizontale** - Sharding possible

### Choix ECS Fargate (vs EC2)

✅ **Serverless** - Pas de gestion serveurs  
✅ **Auto-scaling** - S'adapte à la charge  
✅ **Économique** - Pay-per-use (~21€/mois)

### Choix S3 staging

✅ **Découplage** - Extract/Transform/Load séparés  
✅ **Traçabilité** - Données brutes conservées  
✅ **Reprise** - Rejouer transformation si erreur

---

## 🔐 Sécurité

✅ **Network** - Security Groups restrictifs  
✅ **Data at rest** - EFS + S3 chiffrés  
✅ **Authentication** - MongoDB avec credentials  
✅ **IAM Roles** - Pas de credentials hardcodés

---

## 📚 Documentation complète

### Fichiers détaillés

1. **SCHEMA_BDD.md** - Structure MongoDB complète
2. **LOGIGRAMME.md** - Processus ETL détaillé
3. **ARCHITECTURE_AWS.md** - Infrastructure déployée

### Scripts Python

1. **transform_s3_corrected.py** - ETL transformation
2. **load_mongodb_s3_final.py** - Chargement MongoDB

---

**Version finale :** 1.0  
**Date de livraison :** 21 février 2026