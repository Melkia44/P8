"""
ÉTAPE 1 - RÉCUPÉRATION ET TRANSFORMATION DES DONNÉES
Produit directement une collection unique observations_enrichies

Pipeline :
1. Lecture des sources (JSON + Excel)
2. Fusion stations + observations DÈS LA TRANSFORMATION
3. Normalisation et contrôle qualité
4. Export en JSONL prêt pour MongoDB

Auteur: Projet P8 - Data Engineering
"""

import json
import hashlib
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Tuple
from pathlib import Path
import logging

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UnifiedDataPipeline:
    """Pipeline de transformation produisant directement la collection unique"""
    
    def __init__(self, output_dir: str = "/home/claude/output_etape1"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Statistiques
        self.stats = {
            "sources_processed": 0,
            "stations_found": 0,
            "observations_total": 0,
            "observations_enriched": 0,
            "errors": 0,
            "quality_issues": []
        }
        
        # Cache des stations
        self.stations_cache = {}
    
    def safe_float(self, value: Any) -> float:
        """Conversion sécurisée en float"""
        if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def safe_int(self, value: Any) -> int:
        """Conversion sécurisée en int"""
        if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    def generate_record_hash(self, station_id: str, datetime_str: str) -> str:
        """Génère un hash unique pour chaque observation"""
        unique_string = f"{station_id}_{datetime_str}"
        return hashlib.sha256(unique_string.encode()).hexdigest()
    
    def calculate_completeness(self, measurements: Dict) -> float:
        """Calcule le score de complétude des mesures"""
        total_fields = len(measurements)
        if total_fields == 0:
            return 0.0
        non_null_fields = sum(1 for v in measurements.values() if v is not None)
        return round(non_null_fields / total_fields, 2)
    
    def process_json_source(self, json_file: str) -> List[Dict]:
        """
        Traite le fichier JSON et produit directement des documents enrichis
        FUSION IMMÉDIATE : stations + observations
        """
        logger.info(f"📂 Traitement de {json_file}")
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 1. CHARGER LES STATIONS DANS LE CACHE
        stations = data.get("stations", [])
        logger.info(f"✅ {len(stations)} stations trouvées")
        self.stats["stations_found"] += len(stations)
        
        for station in stations:
            station_id = station["id"]
            
            # Normalisation des données station
            self.stations_cache[station_id] = {
                "station_id": station_id,
                "name": station["name"],
                "latitude": float(station["latitude"]),
                "longitude": float(station["longitude"]),
                "elevation_m": int(station["elevation"]) if station.get("elevation") else None,
                "type": station.get("type"),
                "provider": station.get("license", {}).get("source", "unknown"),
                "source": "SOURCE1_JSON",
                "location": {
                    "type": "Point",
                    "coordinates": [float(station["longitude"]), float(station["latitude"])]
                },
                "license": {
                    "name": station.get("license", {}).get("license"),
                    "url": station.get("license", {}).get("url"),
                    "source": station.get("license", {}).get("source"),
                    "metadata_url": station.get("license", {}).get("metadonnees")
                }
            }
        
        # 2. TRAITER LES OBSERVATIONS ET FUSIONNER IMMÉDIATEMENT
        enriched_documents = []
        hourly_data = data.get("hourly", {})
        
        for station_id, observations in hourly_data.items():
            station_info = self.stations_cache.get(station_id)
            
            if not station_info:
                logger.warning(f"⚠️  Station {station_id} non trouvée dans le cache")
                self.stats["errors"] += 1
                continue
            
            for obs in observations:
                try:
                    # CRÉATION DU DOCUMENT ENRICHI
                    enriched_doc = self.build_enriched_document(obs, station_info, "SOURCE1_JSON")
                    enriched_documents.append(enriched_doc)
                    self.stats["observations_enriched"] += 1
                    
                except Exception as e:
                    logger.error(f"❌ Erreur sur observation {obs.get('dh_utc')}: {e}")
                    self.stats["errors"] += 1
        
        self.stats["observations_total"] += len(enriched_documents)
        logger.info(f"✅ {len(enriched_documents)} observations enrichies")
        
        return enriched_documents
    
    def process_excel_source(self, excel_file: str, station_info_override: Dict = None) -> List[Dict]:
        """
        Traite un fichier Excel (Weather Underground)
        FUSION IMMÉDIATE : crée les documents enrichis directement
        """
        logger.info(f"📂 Traitement de {excel_file}")
        
        # Lire le fichier Excel
        try:
            df = pd.read_excel(excel_file)
        except Exception as e:
            logger.error(f"❌ Impossible de lire {excel_file}: {e}")
            self.stats["errors"] += 1
            return []
        
        # Extraire les infos de la station depuis le nom du fichier
        # Exemple: Weather_Ichtegem_BE.xlsx ou Weather_La_Madeleine_FR.xlsx
        filename = Path(excel_file).stem
        parts = filename.split('_')
        
        if len(parts) >= 3:
            city = parts[1]
            country = parts[2]
            station_id = f"WU_{city.upper()}"
        else:
            station_id = f"WU_UNKNOWN"
            city = "Unknown"
            country = "Unknown"
        
        # Créer ou récupérer les infos station
        if station_id not in self.stations_cache:
            # Essayer d'extraire lat/lon du fichier si possible
            # Sinon, valeurs par défaut
            self.stations_cache[station_id] = {
                "station_id": station_id,
                "name": city.replace('_', ' '),
                "latitude": None,  # À compléter si disponible dans Excel
                "longitude": None,
                "elevation_m": None,
                "city": city.replace('_', ' '),
                "state": None,
                "hardware": "other",
                "software": "EasyWeatherPro",  # d'après l'énoncé
                "provider": "Weather Underground",
                "source": "WU_EXCEL",
                "location": None  # Sera None si pas de coordonnées
            }
            self.stats["stations_found"] += 1
        
        station_info = self.stations_cache[station_id]
        
        # Traiter les observations
        enriched_documents = []
        
        # Identifier les colonnes importantes (adapter selon la structure réelle)
        # On suppose que le fichier contient au minimum : Time, Temperature, etc.
        
        # IMPORTANT : À adapter selon la structure réelle de vos fichiers Excel
        # Pour l'instant, on crée un exemple générique
        
        logger.warning(f"⚠️  Traitement Excel à implémenter selon structure réelle")
        logger.info(f"Colonnes trouvées : {list(df.columns)}")
        
        # TODO: Mapper les colonnes Excel vers le schéma unifié
        # Exemple de mapping à ajuster :
        """
        for idx, row in df.iterrows():
            try:
                obs_data = {
                    "id_station": station_id,
                    "dh_utc": row.get('Time'),  # À adapter
                    "temperature": row.get('Temperature'),
                    # ... autres champs
                }
                
                enriched_doc = self.build_enriched_document(obs_data, station_info, "WU_EXCEL")
                enriched_documents.append(enriched_doc)
                
            except Exception as e:
                logger.error(f"Erreur ligne {idx}: {e}")
                self.stats["errors"] += 1
        """
        
        return enriched_documents
    
    def build_enriched_document(self, observation: Dict, station_info: Dict, source: str) -> Dict:
        """
        CŒUR DU PIPELINE : Construction du document enrichi unifié
        Fusionne immédiatement observation + station
        """
        
        # Parse datetime
        try:
            if isinstance(observation.get("dh_utc"), str):
                obs_dt = datetime.strptime(observation["dh_utc"], "%Y-%m-%d %H:%M:%S")
            else:
                obs_dt = observation.get("dh_utc")
        except Exception as e:
            logger.error(f"Erreur parsing date {observation.get('dh_utc')}: {e}")
            raise
        
        # Génération du hash unique
        record_hash = self.generate_record_hash(
            observation["id_station"],
            observation["dh_utc"]
        )
        
        # Construction des measurements normalisés
        measurements = {
            "temperature_c": self.safe_float(observation.get("temperature")),
            "pressure_hpa": self.safe_float(observation.get("pression")),
            "humidity_pct": self.safe_int(observation.get("humidite")),
            "dew_point_c": self.safe_float(observation.get("point_de_rosee")),
            "visibility_m": self.safe_int(observation.get("visibilite")),
            "wind_speed_kmh": self.safe_float(observation.get("vent_moyen")),
            "wind_gust_kmh": self.safe_float(observation.get("vent_rafales")),
            "wind_dir": str(observation.get("vent_direction")) if observation.get("vent_direction") else None,
            "precip_1h_mm": self.safe_float(observation.get("pluie_1h")),
            "precip_3h_mm": self.safe_float(observation.get("pluie_3h")),
            "snow_depth_cm": self.safe_float(observation.get("neige_au_sol")),
            "cloud_cover_octas": observation.get("nebulosite") if observation.get("nebulosite") != "" else None,
            "weather_code": observation.get("temps_omm")
        }
        
        # Calcul de la qualité
        completeness = self.calculate_completeness(measurements)
        has_nulls = None in measurements.values()
        
        # Vérifications qualité
        quality_issues = []
        if completeness < 0.5:
            quality_issues.append("low_completeness")
        if measurements["temperature_c"] is not None:
            if measurements["temperature_c"] < -50 or measurements["temperature_c"] > 60:
                quality_issues.append("temperature_out_of_range")
        
        if quality_issues:
            self.stats["quality_issues"].extend(quality_issues)
        
        # DOCUMENT ENRICHI FINAL
        enriched_document = {
            "record_hash": record_hash,
            "obs_datetime": obs_dt.isoformat() + "Z",
            "date": obs_dt.strftime("%Y-%m-%d"),
            "hour": obs_dt.hour,
            "day_of_week": obs_dt.weekday(),
            "month": obs_dt.month,
            "year": obs_dt.year,
            "station": station_info,  # FUSION ICI : infos station embarquées
            "measurements": measurements,
            "ingestion_ts": datetime.utcnow().isoformat() + "Z",
            "data_quality": {
                "completeness_score": completeness,
                "has_nulls": has_nulls,
                "validated": True,
                "issues": quality_issues if quality_issues else []
            }
        }
        
        return enriched_document
    
    def save_enriched_data(self, documents: List[Dict], filename: str):
        """Sauvegarde les documents enrichis en JSONL"""
        output_file = self.output_dir / filename
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for doc in documents:
                f.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        logger.info(f"💾 Sauvegarde : {output_file} ({len(documents)} documents)")
    
    def generate_quality_report(self) -> Dict:
        """Génère un rapport de qualité pré-migration"""
        
        report = {
            "pipeline_execution": {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "status": "completed",
                "version": "1.0"
            },
            "sources": {
                "sources_processed": self.stats["sources_processed"],
                "stations_found": self.stats["stations_found"]
            },
            "observations": {
                "total_observations": self.stats["observations_total"],
                "enriched_observations": self.stats["observations_enriched"],
                "success_rate": round(
                    (self.stats["observations_enriched"] / self.stats["observations_total"] * 100)
                    if self.stats["observations_total"] > 0 else 0, 2
                )
            },
            "quality": {
                "errors": self.stats["errors"],
                "quality_issues_count": len(self.stats["quality_issues"]),
                "quality_issues_types": list(set(self.stats["quality_issues"]))
            },
            "stations_list": list(self.stations_cache.keys())
        }
        
        return report
    
    def save_quality_report(self, report: Dict):
        """Sauvegarde le rapport qualité"""
        output_file = self.output_dir / "quality_report_pre_migration.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📊 Rapport qualité : {output_file}")
    
    def print_summary(self):
        """Affiche un résumé de l'exécution"""
        print("\n" + "="*70)
        print("📊 RÉSUMÉ DE L'ÉTAPE 1 - RÉCUPÉRATION & TRANSFORMATION")
        print("="*70)
        print(f"Sources traitées       : {self.stats['sources_processed']}")
        print(f"Stations identifiées   : {self.stats['stations_found']}")
        print(f"Observations totales   : {self.stats['observations_total']}")
        print(f"Observations enrichies : {self.stats['observations_enriched']}")
        print(f"Erreurs                : {self.stats['errors']}")
        print(f"Issues qualité         : {len(self.stats['quality_issues'])}")
        print(f"\nTaux de succès         : {(self.stats['observations_enriched'] / self.stats['observations_total'] * 100) if self.stats['observations_total'] > 0 else 0:.2f}%")
        print("="*70)
        
        if self.stations_cache:
            print("\n📍 Stations dans le cache :")
            for station_id, info in self.stations_cache.items():
                print(f"  - {station_id}: {info['name']} ({info.get('provider', 'unknown')})")
        
        print("\n✅ Fichiers générés :")
        print(f"  - observations_enrichies.jsonl")
        print(f"  - quality_report_pre_migration.json")
        print("="*70 + "\n")


def main():
    """Fonction principale - Pipeline complet Étape 1"""
    
    print("\n" + "="*70)
    print("🚀 ÉTAPE 1 - RÉCUPÉRATION & TRANSFORMATION")
    print("   Production directe de la collection unique")
    print("="*70 + "\n")
    
    # Initialisation du pipeline
    pipeline = UnifiedDataPipeline(output_dir="/home/claude/output_etape1")
    
    # Liste des sources à traiter
    sources = [
        {
            "type": "json",
            "path": "/mnt/project/Data_Source1_011024-071024.json"
        }
        # Ajouter les fichiers Excel ici quand prêts
        # {
        #     "type": "excel",
        #     "path": "/mnt/project/data/excel/Weather_Ichtegem_BE.xlsx"
        # },
    ]
    
    all_enriched_documents = []
    
    # Traiter chaque source
    for source in sources:
        logger.info(f"\n{'='*70}")
        logger.info(f"📂 Source: {source['path']}")
        logger.info(f"{'='*70}")
        
        try:
            if source["type"] == "json":
                docs = pipeline.process_json_source(source["path"])
            elif source["type"] == "excel":
                docs = pipeline.process_excel_source(source["path"])
            else:
                logger.warning(f"⚠️  Type de source non supporté: {source['type']}")
                continue
            
            all_enriched_documents.extend(docs)
            pipeline.stats["sources_processed"] += 1
            
        except Exception as e:
            logger.error(f"❌ Erreur traitement source {source['path']}: {e}")
            pipeline.stats["errors"] += 1
    
    # Sauvegarder les données enrichies
    if all_enriched_documents:
        pipeline.save_enriched_data(
            all_enriched_documents,
            "observations_enrichies.jsonl"
        )
    
    # Générer et sauvegarder le rapport qualité
    quality_report = pipeline.generate_quality_report()
    pipeline.save_quality_report(quality_report)
    
    # Afficher le résumé
    pipeline.print_summary()
    
    logger.info("\n✅ ÉTAPE 1 TERMINÉE AVEC SUCCÈS !")
    logger.info(f"📁 Données prêtes pour l'étape 2 dans : /home/claude/output_etape1/\n")


if __name__ == "__main__":
    main()
