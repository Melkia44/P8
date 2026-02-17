#!/usr/bin/env python3
"""
run_pipeline.py — Orchestrateur Docker pour le pipeline ETL complet.

Chaîne automatiquement :
  1. transform.py  (données brutes → JSONL unifié)
  2. load_mongodb.py (JSONL → MongoDB avec validation)

Ce script est conçu pour tourner dans le conteneur Docker.
Il réutilise les scripts standalone sans les modifier :
  - transform.py est appelé via subprocess (comme en CLI)
  - load_mongodb.py est appelé via subprocess (comme en CLI)

Variables d'environnement attendues (via docker-compose) :
  MONGO_URI          - URI MongoDB (ex: mongodb://mongodb:27017/?replicaSet=rs0)
  DATA_ROOT          - Dossier des fichiers sources (monté via volume)

Optionnelles :
  DB_NAME            - Nom de la base (défaut: weather_db)
  COLLECTION_NAME    - Nom de la collection (défaut: weather_data)
  FORCE_DIRECT_CONNECTION - true/false (défaut: false)
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("run_pipeline")

SCRIPTS_DIR = Path(__file__).resolve().parent


def run_step(name: str, cmd: list[str]) -> None:
    """Exécute une commande et lève une erreur si elle échoue."""
    logger.info("=" * 60)
    logger.info(f"ÉTAPE : {name}")
    logger.info(f"CMD   : {' '.join(cmd)}")
    logger.info("=" * 60)

    result = subprocess.run(cmd, cwd=str(SCRIPTS_DIR))

    if result.returncode != 0:
        logger.error(f"ÉCHEC : {name} (code {result.returncode})")
        sys.exit(result.returncode)

    logger.info(f"OK : {name}")


def main():
    logger.info("🚀 Pipeline ETL Docker — Forecast 2.0")

    data_root = os.getenv("DATA_ROOT", "/app/data")
    output_dir = os.getenv("OUTPUT_DIR", "/app/output")
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017/?replicaSet=rs0")

    jsonl_path = f"{output_dir}/weather_data.jsonl"
    report_path = f"{output_dir}/mongodb_report.json"

    # Vérifier que les fichiers sources sont présents
    data_path = Path(data_root)
    if not data_path.exists():
        logger.error(f"DATA_ROOT introuvable : {data_root}")
        logger.error("Vérifiez le montage du volume Docker.")
        sys.exit(1)

    # ---- ÉTAPE 1 : TRANSFORMATION ----
    run_step(
        "Transformation des données",
        [
            sys.executable,
            str(SCRIPTS_DIR / "transform.py"),
            "--data-root", data_root,
            "--output", jsonl_path,
        ],
    )

    # ---- ÉTAPE 2 : IMPORT MONGODB ----
    # On passe la config via les variables d'environnement
    # que load_mongodb.py lit déjà (MONGO_URI, DB_NAME, etc.)
    env = os.environ.copy()
    env["MONGO_URI"] = mongo_uri
    env["INPUT_PATH"] = jsonl_path
    env["REPORT_PATH"] = report_path
    env["RESET_COLLECTION"] = "true"

    logger.info("=" * 60)
    logger.info("ÉTAPE : Import MongoDB")
    logger.info(f"CMD   : {sys.executable} load_mongodb.py")
    logger.info("=" * 60)

    result = subprocess.run(
        [sys.executable, str(SCRIPTS_DIR / "load_mongodb.py")],
        cwd=str(SCRIPTS_DIR),
        env=env,
    )

    if result.returncode != 0:
        logger.error(f"ÉCHEC : Import MongoDB (code {result.returncode})")
        sys.exit(result.returncode)

    logger.info("=" * 60)
    logger.info(f"✅ Pipeline terminé — Rapport : {report_path}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
