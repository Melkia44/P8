#!/bin/bash
# =============================================================
# run_pipeline.sh - Pipeline Forecast 2.0
# Récupère l'IP MongoDB depuis ECS puis lance le chargement
# =============================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# --- Charger le .env ---
ENV_FILE="$SCRIPT_DIR/../.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "ERREUR : Fichier .env introuvable ($ENV_FILE)"
    exit 1
fi
set -a
source "$ENV_FILE"
set +a

# --- Config ECS ---
ECS_CLUSTER="forecast-cluster"
AWS_PROFILE="oc-ecs"
REGION="eu-west-3"

echo "============================================================"
echo "  FORECAST 2.0 - Pipeline de chargement MongoDB"
echo "============================================================"
echo ""

# --- Etape 0 : Récupérer l'IP MongoDB automatiquement ---
echo "[0/3] Recuperation de l'IP MongoDB depuis ECS..."

TASK_ARN=$(aws ecs list-tasks \
  --cluster "$ECS_CLUSTER" \
  --region "$REGION" \
  --profile "$AWS_PROFILE" \
  --query 'taskArns[0]' \
  --output text 2>/dev/null)

if [ -z "$TASK_ARN" ] || [ "$TASK_ARN" = "None" ]; then
    echo "  ERREUR : Aucune task MongoDB trouvee dans le cluster $ECS_CLUSTER"
    echo "  Verifiez que la task ECS est demarree"
    exit 1
fi

# Chercher l'IP publique en priorité
MONGO_IP=$(aws ecs describe-tasks \
  --cluster "$ECS_CLUSTER" \
  --tasks "$TASK_ARN" \
  --region "$REGION" \
  --profile "$AWS_PROFILE" \
  --query 'tasks[0].attachments[0].details[?name==`networkInterfaceId`].value | [0]' \
  --output text 2>/dev/null)

# Récupérer l'IP publique via l'ENI
if [ -n "$MONGO_IP" ] && [ "$MONGO_IP" != "None" ]; then
    ENI_ID="$MONGO_IP"
    MONGO_IP=$(aws ec2 describe-network-interfaces \
      --network-interface-ids "$ENI_ID" \
      --region "$REGION" \
      --profile "$AWS_PROFILE" \
      --query 'NetworkInterfaces[0].Association.PublicIp' \
      --output text 2>/dev/null)
fi

# Fallback : saisie manuelle
if [ -z "$MONGO_IP" ] || [ "$MONGO_IP" = "None" ]; then
    echo "  Auto-detection echouee."
    read -p "  Entrez l'IP MongoDB manuellement : " MONGO_IP
fi

echo "  MongoDB detecte : $MONGO_IP:27017"
echo ""

# --- Construire MONGO_URI avec l'IP detectee ---
export MONGO_URI="mongodb://${MONGO_USER}:${MONGO_PASS}@${MONGO_IP}:27017/"

echo "  Configuration :"
echo "    Bucket   : $BUCKET_NAME"
echo "    Input    : $INPUT_FILE"
echo "    MongoDB  : $MONGO_IP:27017"
echo "    Database : $DB_NAME.$COLLECTION_NAME"
echo ""

# --- Etape 1 : Test de connectivité ---
echo "[1/3] Test de connexion MongoDB..."
nc -z -w 3 "$MONGO_IP" 27017 2>/dev/null
if [ $? -ne 0 ]; then
    echo "  ERREUR : MongoDB injoignable sur $MONGO_IP:27017"
    echo "  Verifiez le Security Group (port 27017 ouvert)"
    exit 1
fi
echo "  OK - MongoDB joignable"
echo ""

# --- Etape 2 : Chargement ---
echo "[2/3] Chargement des donnees dans MongoDB..."
echo ""
python3 load_mongodb_s3.py
LOAD_STATUS=$?
echo ""

if [ $LOAD_STATUS -ne 0 ]; then
    echo "  ERREUR : Le chargement a echoue (code $LOAD_STATUS)"
    exit 1
fi

# --- Etape 3 : Tests de validation ---
echo "[3/3] Tests de validation..."
echo ""
python3 "$SCRIPT_DIR/../04_Deploiement_AWS/Scripts/test_mongodb_aws.py"
echo ""

echo "============================================================"
echo "  Pipeline termine avec succes"
echo "============================================================"
