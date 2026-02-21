# ☁️ ARCHITECTURE AWS - Infrastructure Déployée

**Projet :** Forecast 2.0  
**Région :** eu-west-3 (Paris)  
**Type :** Cloud-native serverless + containers

---

## 🏗️ Vue d'ensemble de l'infrastructure

```
┌──────────────────────────────────────────────────────────────────────┐
│                        AWS CLOUD (eu-west-3)                          │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    VPC vpc-071be79041d1d6dd                  │   │
│  │                       (Default VPC)                           │   │
│  │                                                               │   │
│  │  ┌──────────────────────────────────────────────────────┐   │   │
│  │  │          Security Group: mongodb-forecast-sg         │   │   │
│  │  │          ID: sg-088e6a8c692b12b23                     │   │   │
│  │  │                                                        │   │   │
│  │  │  Inbound Rules:                                       │   │   │
│  │  │  • Port 27017/TCP (MongoDB) from IP locale           │   │   │
│  │  │  • Port 2049/TCP (NFS/EFS) from sg-self              │   │   │
│  │  │                                                        │   │   │
│  │  │  Outbound Rules:                                      │   │   │
│  │  │  • All traffic (0.0.0.0/0)                           │   │   │
│  │  └──────────────────────────────────────────────────────┘   │   │
│  │                           │                                   │   │
│  │                           ▼                                   │   │
│  │  ┌──────────────────────────────────────────────────────┐   │   │
│  │  │             ECS Cluster: forecast-cluster            │   │   │
│  │  │                 Launch type: Fargate                  │   │   │
│  │  │                                                        │   │   │
│  │  │   ┌──────────────────────────────────────────────┐  │   │   │
│  │  │   │   Task: mongodb-task (RUNNING)               │  │   │   │
│  │  │   │   ID: 3605e59869bf43db94db8926a046e271        │  │   │   │
│  │  │   │                                                │  │   │   │
│  │  │   │   Compute:                                    │  │   │   │
│  │  │   │   ├─ CPU: 0.5 vCPU (512 units)               │  │   │   │
│  │  │   │   └─ Memory: 1 GB (1024 MB)                  │  │   │   │
│  │  │   │                                                │  │   │   │
│  │  │   │   Network:                                    │  │   │   │
│  │  │   │   ├─ Public IP: 51.44.220.64                 │  │   │   │
│  │  │   │   ├─ Private IP: 172.31.x.x                  │  │   │   │
│  │  │   │   └─ Security Group: mongodb-forecast-sg     │  │   │   │
│  │  │   │                                                │  │   │   │
│  │  │   │   ┌────────────────────────────────────────┐ │  │   │   │
│  │  │   │   │  Container: mongodb                    │ │  │   │   │
│  │  │   │   │  Image: mongo:7                        │ │  │   │   │
│  │  │   │   │  Port: 27017                           │ │  │   │   │
│  │  │   │   │                                         │ │  │   │   │
│  │  │   │   │  Environment:                          │ │  │   │   │
│  │  │   │   │  • MONGO_INITDB_ROOT_USERNAME=admin   │ │  │   │   │
│  │  │   │   │  • MONGO_INITDB_ROOT_PASSWORD=***     │ │  │   │   │
│  │  │   │   │                                         │ │  │   │   │
│  │  │   │   │  HealthCheck:                          │ │  │   │   │
│  │  │   │   │  • Command: mongosh ping              │ │  │   │   │
│  │  │   │   │  • Interval: 30s                       │ │  │   │   │
│  │  │   │   │  • Timeout: 5s                         │ │  │   │   │
│  │  │   │   │  • Retries: 3                          │ │  │   │   │
│  │  │   │   │  • Start period: 60s                   │ │  │   │   │
│  │  │   │   └────────────────────────────────────────┘ │  │   │   │
│  │  │   │                │                               │  │   │   │
│  │  │   │                │ Mount                         │  │   │   │
│  │  │   │                ▼                               │  │   │   │
│  │  │   │   ┌────────────────────────────────────────┐ │  │   │   │
│  │  │   │   │  Volume EFS                            │ │  │   │   │
│  │  │   │   │  Mount point: /data/db                 │ │  │   │   │
│  │  │   │   │  Encryption in transit: Enabled        │ │  │   │   │
│  │  │   │   └────────────────────────────────────────┘ │  │   │   │
│  │  │   └──────────────────────────────────────────────┘  │   │   │
│  │  └──────────────────────────────────────────────────────┘   │   │
│  │                           │                                   │   │
│  │                           │                                   │   │
│  │                           ▼                                   │   │
│  │  ┌──────────────────────────────────────────────────────┐   │   │
│  │  │         EFS: mongodb-data-efs                         │   │   │
│  │  │         ID: fs-07c9820df66d398d0                       │   │   │
│  │  │                                                        │   │   │
│  │  │  Performance: General Purpose                         │   │   │
│  │  │  Throughput: Bursting                                │   │   │
│  │  │  Encryption at rest: Enabled                         │   │   │
│  │  │                                                        │   │   │
│  │  │  Mount Targets (3 AZ):                               │   │   │
│  │  │  ├─ eu-west-3a: 172.31.x.x                           │   │   │
│  │  │  ├─ eu-west-3b: 172.31.x.x                           │   │   │
│  │  │  └─ eu-west-3c: 172.31.x.x                           │   │   │
│  │  │                                                        │   │   │
│  │  │  Stored Data:                                         │   │   │
│  │  │  • /data/db/                                          │   │   │
│  │  │    ├─ MongoDB system files                           │   │   │
│  │  │    ├─ weather_db/                                     │   │   │
│  │  │    │  └─ weather_data.bson                           │   │   │
│  │  │    └─ Indices                                         │   │   │
│  │  └──────────────────────────────────────────────────────┘   │   │
│  └───────────────────────────────────────────────────────────┘   │
│                                                                   │
├───────────────────────────────────────────────────────────────────┤
│                        S3 STORAGE                                  │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │         Bucket: oc-meteo-staging-data                     │   │
│  │         Region: eu-west-3                                  │   │
│  │         Versioning: Disabled                               │   │
│  │         Encryption: Server-side (SSE-S3)                   │   │
│  │                                                            │   │
│  │   Structure:                                               │   │
│  │   ├── raw/                                                │   │
│  │   │   ├── BE/ (7 fichiers JSONL, ~1.8 MB)                │   │
│  │   │   │   ├── 011024/2026_02_20_xxx.jsonl (288 records)  │   │
│  │   │   │   ├── 021024/... (285 records)                   │   │
│  │   │   │   ├── 031024/... (284 records)                   │   │
│  │   │   │   ├── 041024/... (288 records)                   │   │
│  │   │   │   ├── 051024/... (288 records)                   │   │
│  │   │   │   ├── 061024/... (288 records)                   │   │
│  │   │   │   └── 071024/... (178 records)                   │   │
│  │   │   │                                                    │   │
│  │   │   ├── FR/ (7 fichiers JSONL, ~1.9 MB)                │   │
│  │   │   │   ├── 011024/... (288 records)                   │   │
│  │   │   │   ├── 021024/... (288 records)                   │   │
│  │   │   │   ├── 031024/... (288 records)                   │   │
│  │   │   │   ├── 041024/... (288 records)                   │   │
│  │   │   │   ├── 051024/... (288 records)                   │   │
│  │   │   │   ├── 061024/... (288 records)                   │   │
│  │   │   │   └── 071024/... (180 records)                   │   │
│  │   │   │                                                    │   │
│  │   │   └── s3_meteo_staging/ (1 fichier JSONL)            │   │
│  │   │       └── 2026_02_20_xxx.jsonl (InfoClimat)          │   │
│  │   │                                                        │   │
│  │   └── Transform/                                           │   │
│  │       ├── weather_data.jsonl (3807 records, ~1.5 MB)     │   │
│  │       └── weather_data.quality.json (2 KB)               │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                   │
├───────────────────────────────────────────────────────────────────┤
│                     CLOUDWATCH MONITORING                          │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │         Log Group: /ecs/mongodb-forecast                  │   │
│  │         Retention: Unlimited                               │   │
│  │                                                            │   │
│  │   Log Streams:                                             │   │
│  │   ├─ mongodb/mongodb-task/xxx (Container logs)            │   │
│  │   │   ├─ MongoDB startup                                  │   │
│  │   │   ├─ Connection logs                                  │   │
│  │   │   ├─ Query logs                                       │   │
│  │   │   └─ Health checks                                    │   │
│  │   │                                                        │   │
│  │   └─ ecs-agent/... (Fargate agent logs)                  │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │         Container Insights                                 │   │
│  │                                                            │   │
│  │   Metrics:                                                 │   │
│  │   ├─ CPUUtilization (~15-20%)                            │   │
│  │   ├─ MemoryUtilization (~300 MB / 1024 MB)               │   │
│  │   ├─ NetworkRx/Tx                                         │   │
│  │   └─ Task Count (1 RUNNING)                              │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

---

## 📊 Composants détaillés

### 1. MongoDB ECS Task

**Task Definition :** `mongodb-task:1`

| Paramètre | Valeur |
|-----------|--------|
| **Launch type** | AWS Fargate (serverless) |
| **CPU** | 0.5 vCPU (512 units) |
| **Memory** | 1 GB (1024 MB) |
| **Network mode** | awsvpc |
| **Platform version** | LATEST |

**Container :**
- Image : `mongo:7` (Docker Hub officiel)
- Port mapping : `27017:27017/tcp`
- Essential : `true`

**Environment variables :**
```bash
MONGO_INITDB_ROOT_USERNAME=admin
MONGO_INITDB_ROOT_PASSWORD=ForecastSecure2024!
```

**Health check :**
```json
{
  "command": ["CMD-SHELL", "mongosh --eval \"db.adminCommand('ping')\" || exit 1"],
  "interval": 30,
  "timeout": 5,
  "retries": 3,
  "startPeriod": 60
}
```

**Volume mount :**
- Type : EFS
- Source : `fs-07c9820df66d398d0`
- Container path : `/data/db`
- Encryption in transit : Enabled

### 2. EFS (Elastic File System)

**ID :** `fs-07c9820df66d398d0`  
**Name :** `mongodb-data-efs`

| Paramètre | Valeur |
|-----------|--------|
| **Performance mode** | General Purpose |
| **Throughput mode** | Bursting |
| **Encryption at rest** | Enabled (AWS managed key) |
| **Lifecycle policy** | None |
| **Size** | ~6 GB (utilisé) |

**Mount targets :** 3 (haute disponibilité)
- eu-west-3a : Subnet subnet-062d169b70e3dbe06
- eu-west-3b : Subnet subnet-0d617e41345f36598
- eu-west-3c : Subnet subnet-0bb0897167393c8bd

**Security :** Security Group `mongodb-forecast-sg`

### 3. Security Group

**ID :** `sg-088e6a8c692b12b23`  
**Name :** `mongodb-forecast-sg`  
**VPC :** `vpc-071be79041d1d6dd`

**Inbound rules :**

| Type | Protocol | Port | Source | Description |
|------|----------|------|--------|-------------|
| Custom TCP | TCP | 27017 | Mon IP publique | MongoDB access |
| NFS | TCP | 2049 | sg-088e6a8c692b12b23 | EFS mount |

**Outbound rules :**
- All traffic vers 0.0.0.0/0 (Internet access)

### 4. S3 Bucket

**Name :** `oc-meteo-staging-data`  
**Region :** `eu-west-3`

| Paramètre | Configuration |
|-----------|---------------|
| **Versioning** | Disabled |
| **Encryption** | Server-side (SSE-S3) |
| **Public access** | Blocked |
| **Object lock** | Disabled |

**Taille totale :** ~6 MB
- raw/ : ~3.7 MB (15 fichiers)
- Transform/ : ~1.5 MB (2 fichiers)

### 5. CloudWatch Logs

**Log group :** `/ecs/mongodb-forecast`  
**Retention :** Never expire

**Logs capturés :**
- MongoDB startup et shutdown
- Connexions clients
- Requêtes (si verbose mode)
- Erreurs et warnings
- Health check results

---

## 🔐 Sécurité

### Network Security

✅ **VPC isolé**
- Traffic interne uniquement via Security Groups
- Pas d'exposition directe Internet (sauf MongoDB sur IP autorisée)

✅ **Security Groups**
- Règles strictes inbound
- MongoDB accessible uniquement depuis IP de dev
- NFS limité au SG lui-même

### Data Security

✅ **Encryption at rest**
- EFS : AWS managed keys
- S3 : Server-side encryption (SSE-S3)

✅ **Encryption in transit**
- EFS mount : TLS enabled
- MongoDB : Connexion non chiffrée (à améliorer en prod)

### Access Control

✅ **MongoDB authentication**
- Username/password requis
- Database-level permissions

✅ **IAM Roles**
- ECS Task execution role pour pull image
- Pas de credentials hardcodés

---

## 💰 Coûts estimés

### Coûts mensuels (24/7)

| Service | Configuration | Coût/mois |
|---------|---------------|-----------|
| **ECS Fargate** | 0.5 vCPU, 1GB, 24/7 | ~20€ |
| **EFS** | 6 GB General Purpose | ~1.20€ |
| **S3** | 6 MB Standard | <0.01€ |
| **CloudWatch Logs** | ~100 MB/mois | ~0.05€ |
| **Data Transfer** | Minimal | ~0.20€ |
| **TOTAL** | | **~21.50€/mois** |

### Optimisations possibles

💡 **Arrêter MongoDB hors usage :**
- Coût réduit à ~3€/mois (EFS + S3 seulement)
- Redémarrage en <2 minutes

💡 **Reserved pricing :**
- Compute Savings Plan pourrait réduire 30-40%

💡 **S3 Intelligent-Tiering :**
- Négligeable à cette échelle

---

## 📈 Performance

### Latence mesurée

| Opération | Latence moyenne | Notes |
|-----------|-----------------|-------|
| **Connexion initiale** | 88ms | Depuis local vers AWS |
| **INSERT simple** | 21ms | Avec index |
| **Temps d'accès global** | 13ms | Moyenne toutes ops |
| **Bulk insert (500)** | ~50ms | Batch optimisé |

### Utilisation ressources

| Ressource | Utilisé | Alloué | % |
|-----------|---------|--------|---|
| **CPU** | ~80-100 units | 512 units | 15-20% |
| **Memory** | ~300 MB | 1024 MB | 29% |
| **EFS** | 6 GB | Unlimited | - |
| **Network** | <1 Mbps | Unlimited | - |

---

## 🔄 Haute disponibilité

### Actuellement (Standalone)

⚠️ **Single point of failure**
- 1 seule Task ECS
- Si crash → Redémarrage auto (ECS)
- Données persistées (EFS)

### Recommandations production

✅ **MongoDB Replica Set (3 nodes)**
```
Primary (51.44.220.64) ──┐
Secondary (IP2)          ├─ Replica Set
Secondary (IP3)          ┘
```

✅ **ECS Service avec Auto Scaling**
- Desired count : 1
- Min : 1, Max : 3
- Auto-restart sur failure

✅ **EFS avec backup automatique**
- AWS Backup policy
- Retention 30 jours

---

## 🚀 Déploiement et gestion

### Déployer MongoDB

```bash
# Créer cluster (si n'existe pas)
aws ecs create-cluster --cluster-name forecast-cluster --region eu-west-3

# Lancer task
aws ecs run-task \
  --cluster forecast-cluster \
  --task-definition mongodb-task:1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-062d169b70e3dbe06],securityGroups=[sg-088e6a8c692b12b23],assignPublicIp=ENABLED}"
```

### Arrêter MongoDB

```bash
# Lister tasks
aws ecs list-tasks --cluster forecast-cluster

# Stopper task
aws ecs stop-task --cluster forecast-cluster --task <TASK_ID>
```

### Consulter logs

```bash
# Via CLI
aws logs tail /ecs/mongodb-forecast --follow

# Via Console
CloudWatch > Log groups > /ecs/mongodb-forecast
```

---

## 🔧 Améliorations futures

### Court terme (semaines)

- [ ] EventBridge rule pour démarrage automatique
- [ ] SNS alertes sur task stopped
- [ ] CloudWatch Dashboard personnalisé

### Moyen terme (mois)

- [ ] MongoDB Replica Set (3 nodes)
- [ ] ALB devant MongoDB pour load balancing
- [ ] AWS Backup automatique EFS

### Long terme (trimestre)

- [ ] Migration vers DocumentDB (managed)
- [ ] VPC Peering pour accès sécurisé
- [ ] Secrets Manager pour credentials

---

**Auteur :** Mathieu Melkia  
**Version :** 1.0  
**Date :** 21 février 2026