#!/usr/bin/env python3
"""
Tests unitaires pour les fonctions de transformation du pipeline ETL.

Execute : pytest tests/test_transform.py -v
"""

import sys
import math
from pathlib import Path

import numpy as np
import pytest

# Ajouter le dossier parent au path pour importer transform.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "01_Recuperation_et_Transformation_Donnees"))
from transform import (
    fahrenheit_to_celsius,
    mph_to_kmh,
    inhg_to_hpa,
    inches_to_mm,
    parse_wu_value,
    wind_text_to_degrees,
    sanitize_for_json,
)


# ============================================================
# TEST CONVERSIONS D'UNITES
# ============================================================
class TestFahrenheitToCelsius:
    """Conversion Fahrenheit -> Celsius : (F-32)*5/9"""

    def test_point_congelation(self):
        assert fahrenheit_to_celsius(32.0) == 0.0

    def test_point_ebullition(self):
        assert fahrenheit_to_celsius(212.0) == 100.0

    def test_temperature_negative(self):
        assert fahrenheit_to_celsius(-40.0) == -40.0  # -40F == -40C

    def test_temperature_typique(self):
        result = fahrenheit_to_celsius(68.0)
        assert result == 20.0

    def test_precision_arrondi(self):
        # 57.7F -> (57.7-32)*5/9 = 14.277...
        result = fahrenheit_to_celsius(57.7)
        assert result == 14.28


class TestMphToKmh:
    """Conversion miles/h -> km/h : mph * 1.60934"""

    def test_zero(self):
        assert mph_to_kmh(0.0) == 0.0

    def test_valeur_connue(self):
        # 60 mph = 96.5604 km/h
        result = mph_to_kmh(60.0)
        assert result == 96.56

    def test_precision(self):
        result = mph_to_kmh(1.0)
        assert result == 1.61


class TestInhgToHpa:
    """Conversion pouces de mercure -> hPa : inHg * 33.8639"""

    def test_pression_standard(self):
        # 29.92 inHg ~ 1013.25 hPa (pression atmospherique standard)
        result = inhg_to_hpa(29.92)
        assert abs(result - 1013.25) < 0.1

    def test_zero(self):
        assert inhg_to_hpa(0.0) == 0.0


class TestInchesToMm:
    """Conversion pouces -> mm : inches * 25.4"""

    def test_un_pouce(self):
        assert inches_to_mm(1.0) == 25.4

    def test_zero(self):
        assert inches_to_mm(0.0) == 0.0


# ============================================================
# TEST PARSING WEATHER UNDERGROUND
# ============================================================
class TestParseWuValue:
    """Parsing des valeurs brutes Weather Underground."""

    def test_valeur_numerique(self):
        assert parse_wu_value(57.7) == 57.7

    def test_valeur_entiere(self):
        assert parse_wu_value(100) == 100.0

    def test_valeur_string_avec_unite(self):
        # Format typique WU : "57.7 degF"
        result = parse_wu_value("57.7 \xa0\u00b0F")
        assert result == 57.7

    def test_valeur_none(self):
        assert math.isnan(parse_wu_value(None))

    def test_valeur_vide(self):
        assert math.isnan(parse_wu_value(""))

    def test_valeur_tiret(self):
        assert math.isnan(parse_wu_value("--"))

    def test_valeur_na(self):
        assert math.isnan(parse_wu_value("N/A"))

    def test_virgule_decimale(self):
        result = parse_wu_value("57,7")
        assert result == 57.7

    def test_nan_float(self):
        assert math.isnan(parse_wu_value(float("nan")))


class TestWindTextToDegrees:
    """Conversion direction cardinale -> degres."""

    def test_north(self):
        assert wind_text_to_degrees("North") == 0

    def test_south(self):
        assert wind_text_to_degrees("South") == 180

    def test_east(self):
        assert wind_text_to_degrees("East") == 90

    def test_nne(self):
        assert wind_text_to_degrees("NNE") == 22.5

    def test_none(self):
        assert math.isnan(wind_text_to_degrees(None))

    def test_chaine_vide(self):
        assert math.isnan(wind_text_to_degrees(""))

    def test_valeur_inconnue(self):
        assert math.isnan(wind_text_to_degrees("Unknown"))


# ============================================================
# TEST SANITIZE JSON
# ============================================================
class TestSanitizeForJson:
    """Nettoyage NaN/Inf pour serialisation JSON."""

    def test_nan_to_none(self):
        assert sanitize_for_json(float("nan")) is None

    def test_inf_to_none(self):
        assert sanitize_for_json(float("inf")) is None

    def test_neg_inf_to_none(self):
        assert sanitize_for_json(float("-inf")) is None

    def test_normal_float(self):
        assert sanitize_for_json(42.5) == 42.5

    def test_dict_recursif(self):
        result = sanitize_for_json({"a": float("nan"), "b": 42.0})
        assert result == {"a": None, "b": 42.0}

    def test_list_recursif(self):
        result = sanitize_for_json([float("nan"), 1.0, None])
        assert result == [None, 1.0, None]

    def test_numpy_int(self):
        result = sanitize_for_json(np.int64(42))
        assert result == 42
        assert isinstance(result, int)

    def test_numpy_float_nan(self):
        result = sanitize_for_json(np.float64("nan"))
        assert result is None

    def test_numpy_float_normal(self):
        result = sanitize_for_json(np.float64(3.14))
        assert result == 3.14


# ============================================================
# TEST SCHEMA UNIFIE
# ============================================================
class TestSchemaUnifie:
    """Verification de la structure du schema cible."""

    def test_nombre_colonnes(self):
        from transform import TARGET_COLUMNS
        assert len(TARGET_COLUMNS) == 23

    def test_colonnes_requises_presentes(self):
        from transform import TARGET_COLUMNS
        required = ["source", "station_id", "timestamp"]
        for col in required:
            assert col in TARGET_COLUMNS, f"Colonne requise manquante: {col}"

    def test_colonnes_meteo_presentes(self):
        from transform import TARGET_COLUMNS
        meteo = ["temperature_c", "humidity_pct", "pressure_hpa", "wind_speed_kmh"]
        for col in meteo:
            assert col in TARGET_COLUMNS, f"Colonne meteo manquante: {col}"
```

**40 tests unitaires** qui couvrent toutes les fonctions de conversion et parsing.

---

## 7. `docs/ARCHITECTURE_AWS.md` (remplace le fichier existant)

Le fichier est identique à ton original **sauf** la ligne 194 qui contenait le mot de passe en clair. Voici la correction à faire :

Cherche la ligne :
```
MONGO_INITDB_ROOT_PASSWORD=ForecastSecure2024!
```
Et remplace-la par :
```
MONGO_INITDB_ROOT_PASSWORD=<defini dans AWS Task Definition>
```

Et ajoute en dessous (si pas déjà présent) :
```
> **Note securite :** Les credentials MongoDB sont configures directement
> dans la Task Definition ECS (ou via AWS Secrets Manager en production).
> Ils ne sont jamais stockes dans le code source.
```

---

## 8. `docs/SCHEMA_BDD.md` - corrections à faire

Deux modifications dans ton fichier existant :

**a)** Ligne 6, remplace :
```
**Documents totaux :** 3807
```
par :
```
**Documents totaux :** 4950 (3807 WU + 1143 InfoClimat)
```

**b)** Lignes 170-171, le niveau de validation devrait refléter la version corrigée. Remplace :
```
**Niveau de validation :** `moderate`
**Action en cas d'échec :** `warn` (log + insertion quand même)
```
par :
```
**Niveau de validation :** `strict`
**Action en cas d'échec :** `error` (rejet du document non conforme)