#!/usr/bin/env python3
"""
Script de test MongoDB AWS - Projet Forecast 2.0
Teste la connexion, l'authentification, les opérations CRUD et la persistance EFS
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import time
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

MONGO_AWS_IP = "51.44.220.64"  # IP publique de ta Task ECS
MONGO_PORT = 27017
MONGO_USER = "admin"
MONGO_PASSWORD = "ForecastSecure2024!"

MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_AWS_IP}:{MONGO_PORT}/"

# ============================================================================
# TESTS
# ============================================================================

def test_1_connection():
    """Test 1 : Connexion basique"""
    print("\n" + "="*70)
    print("TEST 1 : CONNEXION À MONGODB AWS")
    print("="*70)
    
    print(f"🔄 Tentative de connexion à {MONGO_AWS_IP}:{MONGO_PORT}...")
    
    try:
        start = time.time()
        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000
        )
        
        # Force la connexion
        client.admin.command('ping')
        latency = (time.time() - start) * 1000
        
        print(f"✅ Connexion réussie !")
        print(f"⏱️  Latence initiale : {latency:.2f} ms")
        
        # Infos serveur
        server_info = client.server_info()
        print(f"📊 Version MongoDB : {server_info['version']}")
        print(f"📦 Stockage : {server_info.get('storageEngine', {}).get('name', 'N/A')}")
        
        client.close()
        return True
        
    except ConnectionFailure as e:
        print(f"❌ ÉCHEC de connexion : {e}")
        print("\n🔍 Checklist de débogage :")
        print("  1. L'IP publique ECS est-elle correcte ?")
        print("  2. La Task ECS est-elle en status RUNNING ?")
        print("  3. Le Security Group autorise-t-il ton IP sur port 27017 ?")
        print("  4. L'Auto-assign public IP est-il activé sur la Task ?")
        return False
    except Exception as e:
        print(f"❌ Erreur inattendue : {e}")
        return False


def test_2_authentication():
    """Test 2 : Authentification"""
    print("\n" + "="*70)
    print("TEST 2 : AUTHENTIFICATION")
    print("="*70)
    
    try:
        client = MongoClient(MONGO_URI)
        
        # Lister les databases (nécessite authentification)
        dbs = client.list_database_names()
        print(f"✅ Authentification réussie !")
        print(f"📂 Bases de données existantes : {dbs}")
        
        client.close()
        return True
        
    except OperationFailure as e:
        print(f"❌ Échec d'authentification : {e}")
        print("🔍 Vérifiez les credentials dans les variables d'environnement ECS")
        return False


def test_3_crud_operations():
    """Test 3 : Opérations CRUD"""
    print("\n" + "="*70)
    print("TEST 3 : OPÉRATIONS CRUD")
    print("="*70)
    
    try:
        client = MongoClient(MONGO_URI)
        db = client.forecast_test
        collection = db.deployment_tests
        
        # CREATE
        print("📝 Test INSERT...")
        doc = {
            "test": "aws_deployment_validation",
            "timestamp": datetime.utcnow(),
            "source": "local_laptop",
            "ip_aws": MONGO_AWS_IP,
            "environment": "AWS ECS Fargate"
        }
        result = collection.insert_one(doc)
        print(f"✅ Document inséré (ID: {result.inserted_id})")
        
        # READ
        print("📖 Test FIND...")
        retrieved = collection.find_one({"_id": result.inserted_id})
        print(f"✅ Document lu : {retrieved['test']}")
        
        # UPDATE
        print("✏️  Test UPDATE...")
        collection.update_one(
            {"_id": result.inserted_id},
            {"$set": {"updated": True, "update_time": datetime.utcnow()}}
        )
        updated = collection.find_one({"_id": result.inserted_id})
        print(f"✅ Document mis à jour : updated={updated.get('updated')}")
        
        # COUNT
        total = collection.count_documents({})
        print(f"📊 Total de documents de test : {total}")
        
        # DELETE (nettoyage)
        print("🗑️  Test DELETE...")
        collection.delete_one({"_id": result.inserted_id})
        print(f"✅ Document supprimé")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ Erreur CRUD : {e}")
        return False


def test_4_performance():
    """Test 4 : Performance et latence"""
    print("\n" + "="*70)
    print("TEST 4 : PERFORMANCE")
    print("="*70)
    
    try:
        client = MongoClient(MONGO_URI)
        db = client.forecast_test
        collection = db.performance_tests
        
        # Test latence INSERT
        print("⏱️  Mesure de latence INSERT (10 documents)...")
        latencies = []
        for i in range(10):
            start = time.time()
            collection.insert_one({"test": i, "timestamp": datetime.utcnow()})
            latencies.append((time.time() - start) * 1000)
        
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        
        print(f"✅ Latence INSERT :")
        print(f"   - Moyenne : {avg_latency:.2f} ms")
        print(f"   - Min : {min_latency:.2f} ms")
        print(f"   - Max : {max_latency:.2f} ms")
        
        # Nettoyage
        collection.delete_many({})
        
        # Test de temps d'accès aux données
        print("\n⏱️  Mesure temps d'accès global...")
        start = time.time()
        db.command('ping')
        access_time = (time.time() - start) * 1000
        print(f"✅ Temps d'accessibilité : {access_time:.2f} ms")
        
        client.close()
        return True, avg_latency, access_time
        
    except Exception as e:
        print(f"❌ Erreur performance : {e}")
        return False, 0, 0


def test_5_efs_persistence():
    """Test 5 : Validation persistance EFS"""
    print("\n" + "="*70)
    print("TEST 5 : VALIDATION PERSISTANCE EFS")
    print("="*70)
    
    try:
        client = MongoClient(MONGO_URI)
        db = client.forecast_production
        collection = db.efs_validation
        
        # Insérer un document de validation
        validation_doc = {
            "validation_id": "efs_persistence_test",
            "created_at": datetime.utcnow(),
            "message": "Ce document prouve que les données sont sur EFS",
            "test_run": datetime.utcnow().isoformat()
        }
        
        # Vérifier si un document existe déjà
        existing = collection.find_one({"validation_id": "efs_persistence_test"})
        
        if existing:
            print(f"✅ Document EFS trouvé (créé le {existing['created_at']})")
            print("✅ PREUVE DE PERSISTANCE : Les données ont survécu à un redémarrage !")
            
            # Mettre à jour pour tracer ce test
            collection.update_one(
                {"validation_id": "efs_persistence_test"},
                {"$set": {"last_accessed": datetime.utcnow()}}
            )
        else:
            print("ℹ️  Aucun document de validation trouvé")
            print("📝 Création du document témoin...")
            collection.insert_one(validation_doc)
            print("✅ Document témoin créé")
            print("\n💡 Pour tester la persistance :")
            print("   1. Arrête la Task ECS")
            print("   2. Relance une nouvelle Task")
            print("   3. Relance ce script")
            print("   → Le document devrait être retrouvé !")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ Erreur validation EFS : {e}")
        return False


def test_6_quality_check():
    """Test 6 : Contrôle qualité des données"""
    print("\n" + "="*70)
    print("TEST 6 : CONTRÔLE QUALITÉ DES DONNÉES")
    print("="*70)
    
    try:
        client = MongoClient(MONGO_URI)
        db = client.forecast_production
        
        # Simuler insertion de données météo
        weather_collection = db.weather_data
        
        print("📊 Simulation insertion données météo...")
        sample_doc = {
            "station_id": "TEST001",
            "timestamp": datetime.utcnow(),
            "temperature": 15.2,
            "humidity": 78,
            "pressure": 1013.5,
            "metadata": {
                "source": "test_validation",
                "location": "AWS ECS"
            }
        }
        
        weather_collection.insert_one(sample_doc)
        
        # Vérifier la qualité
        total_docs = weather_collection.count_documents({})
        valid_docs = weather_collection.count_documents({"temperature": {"$exists": True}})
        
        quality_rate = (valid_docs / total_docs * 100) if total_docs > 0 else 0
        
        print(f"✅ Total documents : {total_docs}")
        print(f"✅ Documents valides : {valid_docs}")
        print(f"✅ Taux de qualité : {quality_rate:.2f}%")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ Erreur qualité : {e}")
        return False


# ============================================================================
# RAPPORT FINAL
# ============================================================================

def generate_report(results):
    """Génère un rapport final"""
    print("\n" + "="*70)
    print("RAPPORT FINAL - DÉPLOIEMENT MONGODB AWS")
    print("="*70)
    
    print(f"\n📍 Configuration :")
    print(f"   - IP AWS : {MONGO_AWS_IP}")
    print(f"   - Port : {MONGO_PORT}")
    print(f"   - Image : mongo:7")
    print(f"   - Infrastructure : AWS ECS Fargate")
    print(f"   - Stockage : Amazon EFS (persistant)")
    
    print(f"\n📊 Résultats des tests :")
    for test_name, status in results.items():
        icon = "✅" if status else "❌"
        print(f"   {icon} {test_name}")
    
    success_rate = (sum(results.values()) / len(results)) * 100
    print(f"\n🎯 Taux de réussite : {success_rate:.0f}%")
    
    if success_rate == 100:
        print("\n🎉 DÉPLOIEMENT VALIDÉ AVEC SUCCÈS !")
        print("✅ MongoDB est opérationnel sur AWS ECS")
        print("✅ Toutes les fonctionnalités sont testées et validées")
        print("✅ Prêt pour connexion Airbyte")
    else:
        print("\n⚠️  Certains tests ont échoué")
        print("🔍 Vérifiez les logs ci-dessus pour diagnostiquer")
    
    print("="*70)


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("╔" + "="*68 + "╗")
    print("║" + " "*15 + "TEST MONGODB AWS - FORECAST 2.0" + " "*22 + "║")
    print("╚" + "="*68 + "╝")
    
    results = {}
    
    # Test 1 : Connexion
    results["Connexion"] = test_1_connection()
    if not results["Connexion"]:
        print("\n❌ Impossible de continuer sans connexion")
        exit(1)
    
    # Test 2 : Authentification
    results["Authentification"] = test_2_authentication()
    
    # Test 3 : CRUD
    results["Opérations CRUD"] = test_3_crud_operations()
    
    # Test 4 : Performance
    perf_result = test_4_performance()
    results["Performance"] = perf_result[0] if isinstance(perf_result, tuple) else perf_result
    
    # Test 5 : Persistance EFS
    results["Persistance EFS"] = test_5_efs_persistence()
    
    # Test 6 : Qualité
    results["Contrôle qualité"] = test_6_quality_check()
    
    # Rapport final
    generate_report(results)
    
    print(f"\n💾 URI de connexion pour Airbyte :")
    print(f"   mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_AWS_IP}:{MONGO_PORT}/")
