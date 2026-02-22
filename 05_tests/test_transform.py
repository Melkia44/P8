#!/usr/bin/env python3
"""
Tests unitaires pour les fonctions de transformation du pipeline ETL.

Execute : pytest 05_tests/test_transform.py -v
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
    def test_point_congelation(self):
        assert fahrenheit_to_celsius(32.0) == 0.0

    def test_point_ebullition(self):
        assert fahrenheit_to_celsius(212.0) == 100.0

    def test_temperature_negative(self):
        assert fahrenheit_to_celsius(-40.0) == -40.0

    def test_temperature_typique(self):
        assert fahrenheit_to_celsius(68.0) == 20.0

    def test_precision_arrondi(self):
        assert fahrenheit_to_celsius(57.7) == 14.28


class TestMphToKmh:
    def test_zero(self):
        assert mph_to_kmh(0.0) == 0.0

    def test_valeur_connue(self):
        assert mph_to_kmh(60.0) == 96.56

    def test_precision(self):
        assert mph_to_kmh(1.0) == 1.61


class TestInhgToHpa:
    def test_pression_standard(self):
        assert abs(inhg_to_hpa(29.92) - 1013.25) < 0.1

    def test_zero(self):
        assert inhg_to_hpa(0.0) == 0.0


class TestInchesToMm:
    def test_un_pouce(self):
        assert inches_to_mm(1.0) == 25.4

    def test_zero(self):
        assert inches_to_mm(0.0) == 0.0


# ============================================================
# TEST PARSING WEATHER UNDERGROUND
# ============================================================
class TestParseWuValue:
    def test_valeur_numerique(self):
        assert parse_wu_value(57.7) == 57.7

    def test_valeur_entiere(self):
        assert parse_wu_value(100) == 100.0

    def test_valeur_string_avec_unite(self):
        assert parse_wu_value("57.7 \xa0\u00b0F") == 57.7

    def test_valeur_none(self):
        assert math.isnan(parse_wu_value(None))

    def test_valeur_vide(self):
        assert math.isnan(parse_wu_value(""))

    def test_valeur_tiret(self):
        assert math.isnan(parse_wu_value("--"))

    def test_valeur_na(self):
        assert math.isnan(parse_wu_value("N/A"))

    def test_virgule_decimale(self):
        assert parse_wu_value("57,7") == 57.7

    def test_nan_float(self):
        assert math.isnan(parse_wu_value(float("nan")))


class TestWindTextToDegrees:
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
    def test_nan_to_none(self):
        assert sanitize_for_json(float("nan")) is None

    def test_inf_to_none(self):
        assert sanitize_for_json(float("inf")) is None

    def test_neg_inf_to_none(self):
        assert sanitize_for_json(float("-inf")) is None

    def test_normal_float(self):
        assert sanitize_for_json(42.5) == 42.5

    def test_dict_recursif(self):
        assert sanitize_for_json({"a": float("nan"), "b": 42.0}) == {"a": None, "b": 42.0}

    def test_list_recursif(self):
        assert sanitize_for_json([float("nan"), 1.0, None]) == [None, 1.0, None]

    def test_numpy_int(self):
        result = sanitize_for_json(np.int64(42))
        assert result == 42
        assert isinstance(result, int)

    def test_numpy_float_nan(self):
        assert sanitize_for_json(np.float64("nan")) is None

    def test_numpy_float_normal(self):
        assert sanitize_for_json(np.float64(3.14)) == 3.14


# ============================================================
# TEST SCHEMA UNIFIE
# ============================================================
class TestSchemaUnifie:
    def test_nombre_colonnes(self):
        from transform import TARGET_COLUMNS
        assert len(TARGET_COLUMNS) == 23

    def test_colonnes_requises_presentes(self):
        from transform import TARGET_COLUMNS
        for col in ["source", "station_id", "timestamp"]:
            assert col in TARGET_COLUMNS

    def test_colonnes_meteo_presentes(self):
        from transform import TARGET_COLUMNS
        for col in ["temperature_c", "humidity_pct", "pressure_hpa", "wind_speed_kmh"]:
            assert col in TARGET_COLUMNS