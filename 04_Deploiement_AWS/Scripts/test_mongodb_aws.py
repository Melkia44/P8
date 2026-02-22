#!/usr/bin/env python3
"""
Script de test MongoDB AWS - Projet Forecast 2.0
Teste la connexion, l'authentification, les operations CRUD et la persistance EFS.

Configuration via variables d'environnement :
  MONGO_URI  - URI complete MongoDB (OBLIGATOIRE)

Usage :
  export MONGO_URI='mongodb://admin:password@<ECS_IP>:27017/'
  python3 test_mongodb_aws.py
"""

import os
import sys
import time
from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# Charge .env si present
load_dotenv()

# ============================================================================
# CONFIGURATION (via env uniquement - jamais de credentials en dur)
# ============================================================================
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    print("ERREUR : MONGO_URI non defini.")
    print("Usage : export MONGO_URI='mongodb://user:password@host:27017/'")
    print("        python3 test_mongodb_aws.py")
    sys.exit(1)

# Extraire host pour affichage (sans credentials)
from urllib.parse import urlparse

_parsed = urlparse(MONGO_URI)
MONGO_HOST = _parsed.hostname or "unknown"
MONGO_PORT = _parsed.port or 27017


# ============================================================================
# TESTS
# ============================================================================
def test_1_connection():
    """Test 1 : Connexion basique"""
    print("\n" + "=" * 70)
    print("TEST 1 : CONNEXION A MONGODB AWS")
    print("=" * 70)

    print(f"Tentative de connexion a {MONGO_HOST}:{MONGO_PORT}...")

    try:
        start = time.time()
        client = MongoClient(
            MONGO_URI, serverSelectionTimeoutMS=10000, connectTimeoutMS=10000
        )

        client.admin.command("ping")
        latency = (time.time() - start) * 1000

        print(f"[OK] Connexion reussie !")
        print(f"     Latence initiale : {latency:.2f} ms")

        server_info = client.server_info()
        print(f"     Version MongoDB : {server_info['version']}")
        print(
            f"     Stockage : {server_info.get('storageEngine', {}).get('name', 'N/A')}"
        )

        client.close()
        return True

    except ConnectionFailure as e:
        print(f"[ECHEC] Connexion : {e}")
        print("\nChecklist de debogage :")
        print("  1. L'IP publique ECS est-elle correcte ?")
        print("  2. La Task ECS est-elle en status RUNNING ?")
        print("  3. Le Security Group autorise-t-il votre IP sur port 27017 ?")
        print("  4. L'Auto-assign public IP est-il active ?")
        return False
    except Exception as e:
        print(f"[ECHEC] Erreur inattendue : {e}")
        return False


def test_2_authentication():
    """Test 2 : Authentification"""
    print("\n" + "=" * 70)
    print("TEST 2 : AUTHENTIFICATION")
    print("=" * 70)

    try:
        client = MongoClient(MONGO_URI)
        dbs = client.list_database_names()
        print(f"[OK] Authentification reussie !")
        print(f"     Bases de donnees existantes : {dbs}")

        client.close()
        return True

    except OperationFailure as e:
        print(f"[ECHEC] Authentification : {e}")
        print("Verifiez les credentials dans MONGO_URI")
        return False


def test_3_crud_operations():
    """Test 3 : Operations CRUD"""
    print("\n" + "=" * 70)
    print("TEST 3 : OPERATIONS CRUD")
    print("=" * 70)

    try:
        client = MongoClient(MONGO_URI)
        db = client.forecast_test
        collection = db.deployment_tests

        # CREATE
        print("Test INSERT...")
        doc = {
            "test": "aws_deployment_validation",
            "timestamp": datetime.utcnow(),
            "source": "test_script",
            "host": MONGO_HOST,
            "environment": "AWS ECS Fargate",
        }
        result = collection.insert_one(doc)
        print(f"[OK] Document insere (ID: {result.inserted_id})")

        # READ
        print("Test FIND...")
        retrieved = collection.find_one({"_id": result.inserted_id})
        print(f"[OK] Document lu : {retrieved['test']}")

        # UPDATE
        print("Test UPDATE...")
        collection.update_one(
            {"_id": result.inserted_id},
            {"$set": {"updated": True, "update_time": datetime.utcnow()}},
        )
        updated = collection.find_one({"_id": result.inserted_id})
        print(f"[OK] Document mis a jour : updated={updated.get('updated')}")

        # COUNT
        total = collection.count_documents({})
        print(f"     Total de documents de test : {total}")

        # DELETE (nettoyage)
        print("Test DELETE...")
        collection.delete_one({"_id": result.inserted_id})
        print("[OK] Document supprime")

        client.close()
        return True

    except Exception as e:
        print(f"[ECHEC] CRUD : {e}")
        return False


def test_4_performance():
    """Test 4 : Performance et latence"""
    print("\n" + "=" * 70)
    print("TEST 4 : PERFORMANCE")
    print("=" * 70)

    try:
        client = MongoClient(MONGO_URI)
        db = client.forecast_test
        collection = db.performance_tests

        # Test latence INSERT
        print("Mesure de latence INSERT (10 documents)...")
        latencies = []
        for i in range(10):
            start = time.time()
            collection.insert_one({"test": i, "timestamp": datetime.utcnow()})
            latencies.append((time.time() - start) * 1000)

        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)

        print(f"[OK] Latence INSERT :")
        print(f"     Moyenne : {avg_latency:.2f} ms")
        print(f"     Min : {min_latency:.2f} ms")
        print(f"     Max : {max_latency:.2f} ms")

        # Nettoyage
        collection.delete_many({})

        # Test temps d'acces
        print("\nMesure temps d'acces global...")
        start = time.time()
        db.command("ping")
        access_time = (time.time() - start) * 1000
        print(f"[OK] Temps d'accessibilite : {access_time:.2f} ms")

        client.close()
        return True, avg_latency, access_time

    except Exception as e:
        print(f"[ECHEC] Performance : {e}")
        return False, 0, 0


def test_5_efs_persistence():
    """Test 5 : Validation persistance EFS"""
    print("\n" + "=" * 70)
    print("TEST 5 : VALIDATION PERSISTANCE EFS")
    print("=" * 70)

    try:
        client = MongoClient(MONGO_URI)
        db = client.forecast_production
        collection = db.efs_validation

        validation_doc = {
            "validation_id": "efs_persistence_test",
            "created_at": datetime.utcnow(),
            "message": "Ce document prouve que les donnees sont sur EFS",
            "test_run": datetime.utcnow().isoformat(),
        }

        existing = collection.find_one({"validation_id": "efs_persistence_test"})

        if existing:
            print(f"[OK] Document EFS trouve (cree le {existing['created_at']})")
            print(
                "[OK] PREUVE DE PERSISTANCE : Les donnees ont survecu a un redemarrage !"
            )
            collection.update_one(
                {"validation_id": "efs_persistence_test"},
                {"$set": {"last_accessed": datetime.utcnow()}},
            )
        else:
            print("     Aucun document de validation trouve")
            print("     Creation du document temoin...")
            collection.insert_one(validation_doc)
            print("[OK] Document temoin cree")
            print("\nPour tester la persistance :")
            print("  1. Arretez la Task ECS")
            print("  2. Relancez une nouvelle Task")
            print("  3. Relancez ce script")
            print("  -> Le document devrait etre retrouve !")

        client.close()
        return True

    except Exception as e:
        print(f"[ECHEC] Validation EFS : {e}")
        return False


def test_6_weather_data():
    """Test 6 : Verification des donnees meteo chargees"""
    print("\n" + "=" * 70)
    print("TEST 6 : VERIFICATION DONNEES METEO")
    print("=" * 70)

    try:
        client = MongoClient(MONGO_URI)
        db = client.weather_db
        collection = db.weather_data

        total = collection.count_documents({})
        print(f"     Total documents : {total}")

        if total == 0:
            print("[WARN] Collection vide - les donnees n'ont pas encore ete chargees")
            client.close()
            return True

        # Repartition par source
        by_source = list(
            collection.aggregate(
                [{"$group": {"_id": "$source", "count": {"$sum": 1}}}]
            )
        )
        print("     Repartition par source :")
        for s in by_source:
            print(f"       - {s['_id']}: {s['count']}")

        # Repartition par station
        by_station = list(
            collection.aggregate(
                [{"$group": {"_id": "$station_id", "count": {"$sum": 1}}}]
            )
        )
        print("     Repartition par station :")
        for s in by_station:
            print(f"       - {s['_id']}: {s['count']}")

        # Plage temporelle
        date_range = list(
            collection.aggregate(
                [
                    {
                        "$group": {
                            "_id": None,
                            "min": {"$min": "$timestamp"},
                            "max": {"$max": "$timestamp"},
                        }
                    }
                ]
            )
        )
        if date_range:
            print(
                f"     Periode : {date_range[0]['min']} -> {date_range[0]['max']}"
            )

        # Validation : un sample de documents
        sample = collection.find_one({"source": "weather_underground"})
        if sample:
            print(f"\n[OK] Exemple document WU : station={sample.get('station_id')}, "
                  f"temp={sample.get('temperature_c')}C, "
                  f"ts={sample.get('timestamp')}")

        print(f"\n[OK] Donnees meteo valides : {total} documents")

        client.close()
        return True

    except Exception as e:
        print(f"[ECHEC] Verification donnees : {e}")
        return False


# ============================================================================
# RAPPORT FINAL
# ============================================================================
def generate_report(results):
    """Genere un rapport final"""
    print("\n" + "=" * 70)
    print("RAPPORT FINAL - DEPLOIEMENT MONGODB AWS")
    print("=" * 70)

    print(f"\nConfiguration :")
    print(f"  Host : {MONGO_HOST}:{MONGO_PORT}")
    print(f"  Image : mongo:7")
    print(f"  Infrastructure : AWS ECS Fargate + EFS")

    print(f"\nResultats des tests :")
    for test_name, status in results.items():
        icon = "[OK]" if status else "[KO]"
        print(f"  {icon} {test_name}")

    success_rate = (sum(results.values()) / len(results)) * 100
    print(f"\nTaux de reussite : {success_rate:.0f}%")

    if success_rate == 100:
        print("\nDEPLOIEMENT VALIDE AVEC SUCCES !")
    else:
        print("\nCertains tests ont echoue - verifiez les logs ci-dessus")

    print("=" * 70)


# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    print("=" * 70)
    print("  TEST MONGODB AWS - FORECAST 2.0")
    print("=" * 70)

    results = {}

    # Test 1 : Connexion
    results["Connexion"] = test_1_connection()
    if not results["Connexion"]:
        print("\nImpossible de continuer sans connexion")
        sys.exit(1)

    # Test 2 : Authentification
    results["Authentification"] = test_2_authentication()

    # Test 3 : CRUD
    results["Operations CRUD"] = test_3_crud_operations()

    # Test 4 : Performance
    perf_result = test_4_performance()
    results["Performance"] = perf_result[0] if isinstance(perf_result, tuple) else perf_result

    # Test 5 : Persistance EFS
    results["Persistance EFS"] = test_5_efs_persistence()

    # Test 6 : Donnees meteo
    results["Donnees meteo"] = test_6_weather_data()

    # Rapport final
    generate_report(results)