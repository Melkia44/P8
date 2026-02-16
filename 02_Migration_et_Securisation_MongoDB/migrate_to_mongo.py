"""
ÉTAPE 2 - MIGRATION ET SÉCURISATION MONGODB
Import de la collection unique déjà préparée

Pipeline simplifié :
1. Création de la collection avec schéma
2. Création des index
3. Import des données JSONL
4. Validation et reporting
5. Démonstration CRUD

Auteur: Projet P8 - Data Engineering
Date: 2026-02-16
"""

import json
import os
from datetime import datetime
from typing import Dict, List
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, BulkWriteError
from dotenv import load_dotenv
import logging

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Chargement des variables d'environnement
load_dotenv()

# Configuration MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "meteo")
COLLECTION_NAME = "observations_enrichies"


class MongoDBMigration:
    """Classe pour gérer la migration vers MongoDB"""
    
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[COLLECTION_NAME]
        
        self.stats = {
            "documents_processed": 0,
            "documents_inserted": 0,
            "duplicates": 0,
            "errors": 0
        }
    
    def drop_collection_if_exists(self):
        """Supprime la collection si elle existe"""
        if COLLECTION_NAME in self.db.list_collection_names():
            logger.info(f"⚠️  Suppression de la collection '{COLLECTION_NAME}'...")
            self.collection.drop()
            logger.info("✅ Collection supprimée")
    
    def create_collection_with_schema(self):
        """Crée la collection avec validation de schéma"""
        
        schema = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["record_hash", "obs_datetime", "station", "measurements"],
                "properties": {
                    "record_hash": {
                        "bsonType": "string",
                        "description": "Hash SHA256 unique"
                    },
                    "obs_datetime": {
                        "bsonType": "string",
                        "description": "Timestamp ISO8601"
                    },
                    "date": {"bsonType": "string"},
                    "hour": {"bsonType": "int"},
                    "day_of_week": {"bsonType": "int"},
                    "month": {"bsonType": "int"},
                    "year": {"bsonType": "int"},
                    "station": {
                        "bsonType": "object",
                        "required": ["station_id", "name"],
                        "properties": {
                            "station_id": {"bsonType": "string"},
                            "name": {"bsonType": "string"},
                            "latitude": {"bsonType": ["double", "null"]},
                            "longitude": {"bsonType": ["double", "null"]},
                            "elevation_m": {"bsonType": ["int", "null"]},
                            "provider": {"bsonType": ["string", "null"]},
                            "source": {"bsonType": ["string", "null"]}
                        }
                    },
                    "measurements": {"bsonType": "object"},
                    "data_quality": {"bsonType": "object"}
                }
            }
        }
        
        try:
            self.db.create_collection(
                COLLECTION_NAME,
                validator=schema,
                validationLevel="moderate",
                validationAction="error"
            )
            logger.info(f"✅ Collection '{COLLECTION_NAME}' créée avec validation")
        except Exception as e:
            logger.warning(f"⚠️  {e}")
    
    def create_indexes(self):
        """Crée les index optimisés"""
        
        indexes = [
            {
                "keys": [("record_hash", ASCENDING)],
                "options": {"unique": True, "name": "idx_record_hash_unique"}
            },
            {
                "keys": [("station.station_id", ASCENDING), ("obs_datetime", DESCENDING)],
                "options": {"name": "idx_station_datetime"}
            },
            {
                "keys": [("obs_datetime", DESCENDING)],
                "options": {"name": "idx_datetime"}
            },
            {
                "keys": [("year", ASCENDING), ("month", ASCENDING)],
                "options": {"name": "idx_year_month"}
            },
            {
                "keys": [("measurements.temperature_c", ASCENDING)],
                "options": {"name": "idx_temperature", "sparse": True}
            }
        ]
        
        logger.info("\n🔧 Création des index...")
        for idx in indexes:
            try:
                self.collection.create_index(idx["keys"], **idx["options"])
                logger.info(f"  ✅ {idx['options']['name']}")
            except Exception as e:
                logger.warning(f"  ⚠️  {idx['options']['name']}: {e}")
    
    def import_jsonl(self, jsonl_file: str):
        """Importe les données depuis le fichier JSONL"""
        
        logger.info(f"\n📂 Import depuis {jsonl_file}")
        
        documents = []
        batch_size = 1000
        
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    doc = json.loads(line.strip())
                    
                    # Convertir obs_datetime en datetime Python pour MongoDB
                    if isinstance(doc.get("obs_datetime"), str):
                        doc["obs_datetime"] = datetime.fromisoformat(doc["obs_datetime"].replace('Z', '+00:00'))
                    
                    if isinstance(doc.get("ingestion_ts"), str):
                        doc["ingestion_ts"] = datetime.fromisoformat(doc["ingestion_ts"].replace('Z', '+00:00'))
                    
                    documents.append(doc)
                    self.stats["documents_processed"] += 1
                    
                    # Insertion par batch
                    if len(documents) >= batch_size:
                        self._insert_batch(documents)
                        documents = []
                        
                        if line_num % 5000 == 0:
                            logger.info(f"  ⏳ {line_num} lignes traitées...")
                
                except json.JSONDecodeError as e:
                    logger.error(f"  ❌ Ligne {line_num}: JSON invalide - {e}")
                    self.stats["errors"] += 1
                except Exception as e:
                    logger.error(f"  ❌ Ligne {line_num}: {e}")
                    self.stats["errors"] += 1
        
        # Insérer le dernier batch
        if documents:
            self._insert_batch(documents)
        
        logger.info(f"✅ Import terminé : {self.stats['documents_processed']} documents traités")
    
    def _insert_batch(self, documents: List[Dict]):
        """Insère un batch avec gestion des doublons"""
        try:
            result = self.collection.insert_many(documents, ordered=False)
            self.stats["documents_inserted"] += len(result.inserted_ids)
        
        except BulkWriteError as bwe:
            inserted = bwe.details.get('nInserted', 0)
            duplicates = len(documents) - inserted
            
            self.stats["documents_inserted"] += inserted
            self.stats["duplicates"] += duplicates
            
            if duplicates > 0:
                logger.warning(f"  ⚠️  {duplicates} doublons ignorés")
        
        except Exception as e:
            logger.error(f"  ❌ Erreur d'insertion : {e}")
            self.stats["errors"] += len(documents)
    
    def generate_post_migration_report(self) -> Dict:
        """Génère un rapport post-migration"""
        
        # Statistiques de la collection
        total_docs = self.collection.count_documents({})
        
        # Statistiques par station
        pipeline_stations = [
            {"$group": {"_id": "$station.station_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        stations_stats = list(self.collection.aggregate(pipeline_stations))
        
        # Qualité moyenne
        pipeline_quality = [
            {"$group": {
                "_id": None,
                "avg_completeness": {"$avg": "$data_quality.completeness_score"},
                "docs_with_issues": {
                    "$sum": {"$cond": [{"$gt": [{"$size": {"$ifNull": ["$data_quality.issues", []]}}, 0]}, 1, 0]}
                }
            }}
        ]
        quality_stats = list(self.collection.aggregate(pipeline_quality))
        
        report = {
            "migration_execution": {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "status": "completed"
            },
            "import_stats": {
                "documents_processed": self.stats["documents_processed"],
                "documents_inserted": self.stats["documents_inserted"],
                "duplicates": self.stats["duplicates"],
                "errors": self.stats["errors"],
                "success_rate": round(
                    (self.stats["documents_inserted"] / self.stats["documents_processed"] * 100)
                    if self.stats["documents_processed"] > 0 else 0, 2
                )
            },
            "collection_stats": {
                "total_documents": total_docs,
                "stations_count": len(stations_stats),
                "documents_by_station": stations_stats
            },
            "data_quality": quality_stats[0] if quality_stats else {}
        }
        
        return report
    
    def save_post_migration_report(self, report: Dict, output_dir: str):
        """Sauvegarde le rapport post-migration"""
        output_file = f"{output_dir}/quality_report_post_migration.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📊 Rapport post-migration : {output_file}")
    
    def demonstrate_crud(self):
        """Démonstration des opérations CRUD"""
        
        logger.info("\n" + "="*70)
        logger.info("🔧 DÉMONSTRATION CRUD")
        logger.info("="*70)
        
        # READ
        logger.info("\n1️⃣  READ - Lecture d'un document")
        sample = self.collection.find_one({})
        if sample:
            logger.info(f"   Station: {sample['station']['name']}")
            logger.info(f"   Date: {sample['obs_datetime']}")
            logger.info(f"   Température: {sample['measurements'].get('temperature_c')}°C")
        
        # CREATE
        logger.info("\n2️⃣  CREATE - Insertion d'un document test")
        test_doc = {
            "record_hash": "test_hash_crud_demo",
            "obs_datetime": datetime.utcnow(),
            "date": datetime.utcnow().strftime("%Y-%m-%d"),
            "hour": datetime.utcnow().hour,
            "day_of_week": datetime.utcnow().weekday(),
            "month": datetime.utcnow().month,
            "year": datetime.utcnow().year,
            "station": {
                "station_id": "TEST_STATION",
                "name": "Station de Test",
                "latitude": 50.0,
                "longitude": 3.0
            },
            "measurements": {
                "temperature_c": 20.0,
                "humidity_pct": 75
            },
            "data_quality": {
                "completeness_score": 1.0,
                "has_nulls": False,
                "validated": True,
                "issues": []
            }
        }
        
        try:
            result = self.collection.insert_one(test_doc)
            logger.info(f"   ✅ Document inséré avec _id: {result.inserted_id}")
            test_id = result.inserted_id
        except Exception as e:
            logger.error(f"   ❌ Erreur : {e}")
            test_id = None
        
        # UPDATE
        if test_id:
            logger.info("\n3️⃣  UPDATE - Modification du document test")
            try:
                result = self.collection.update_one(
                    {"_id": test_id},
                    {"$set": {"measurements.temperature_c": 25.0}}
                )
                logger.info(f"   ✅ {result.modified_count} document modifié")
            except Exception as e:
                logger.error(f"   ❌ Erreur : {e}")
        
        # DELETE
        if test_id:
            logger.info("\n4️⃣  DELETE - Suppression du document test")
            try:
                result = self.collection.delete_one({"_id": test_id})
                logger.info(f"   ✅ {result.deleted_count} document supprimé")
            except Exception as e:
                logger.error(f"   ❌ Erreur : {e}")
        
        logger.info("\n✅ Démonstration CRUD terminée")
        logger.info("="*70)
    
    def print_summary(self):
        """Affiche un résumé"""
        print("\n" + "="*70)
        print("📊 RÉSUMÉ DE L'ÉTAPE 2 - MIGRATION MONGODB")
        print("="*70)
        print(f"Documents traités  : {self.stats['documents_processed']}")
        print(f"Documents insérés  : {self.stats['documents_inserted']}")
        print(f"Doublons ignorés   : {self.stats['duplicates']}")
        print(f"Erreurs            : {self.stats['errors']}")
        print(f"\nTaux de succès     : {(self.stats['documents_inserted'] / self.stats['documents_processed'] * 100) if self.stats['documents_processed'] > 0 else 0:.2f}%")
        print("="*70 + "\n")
    
    def close(self):
        """Ferme la connexion"""
        self.client.close()
        logger.info("👋 Connexion MongoDB fermée")


def main():
    """Fonction principale - Pipeline Étape 2"""
    
    print("\n" + "="*70)
    print("🚀 ÉTAPE 2 - MIGRATION ET SÉCURISATION MONGODB")
    print("="*70 + "\n")
    
    # Fichier d'entrée (sortie de l'étape 1)
    input_file = "/home/claude/output_etape1/observations_enrichies.jsonl"
    output_dir = "/home/claude/output_etape2"
    
    # Créer le dossier de sortie
    os.makedirs(output_dir, exist_ok=True)
    
    # Vérifier que le fichier existe
    if not os.path.exists(input_file):
        logger.error(f"❌ Fichier non trouvé : {input_file}")
        logger.error("   Veuillez exécuter l'étape 1 d'abord !")
        return
    
    # Initialisation
    migration = MongoDBMigration(MONGO_URI, MONGO_DB)
    
    try:
        # 1. Suppression de l'ancienne collection
        migration.drop_collection_if_exists()
        
        # 2. Création de la collection avec schéma
        migration.create_collection_with_schema()
        
        # 3. Création des index
        migration.create_indexes()
        
        # 4. Import des données
        migration.import_jsonl(input_file)
        
        # 5. Génération du rapport post-migration
        report = migration.generate_post_migration_report()
        migration.save_post_migration_report(report, output_dir)
        
        # 6. Démonstration CRUD
        migration.demonstrate_crud()
        
        # 7. Résumé
        migration.print_summary()
        
        logger.info("\n✅ ÉTAPE 2 TERMINÉE AVEC SUCCÈS !")
        
    except Exception as e:
        logger.error(f"\n❌ ERREUR CRITIQUE: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        migration.close()


if __name__ == "__main__":
    main()
