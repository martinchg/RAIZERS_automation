"""
Tests unitaires pour patrimoine_tables_native.py et patrimoine_sheet.py.
Lance avec : cd src && python -m pytest ../tests/test_patrimoine_native.py -v
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest
from patrimoine_tables_native import (
    _build_patrimoine_rows,
    _find_header_row_idx,
    _map_columns,
    _normalize_header,
    _parse_number,
    _score_table,
    pick_patrimoine_table,
    render_patrimoine_context,
)
from sheets.patrimoine_sheet import _resolve_table_value, _PATRIMOINE_COL_MAP


# ---------------------------------------------------------------------------
# Fixtures : tables réalistes issues du template Raizers
# ---------------------------------------------------------------------------

HEADER_ROW = [
    "Type de bien (1)",
    "Adresse et destination (2)",
    "Surface habitable",
    "Type de détention (3)",
    "% de détention",
    "Valeur d'acquisition",
    "Estimation actuelle",
    "Capital restant dû",
    "Garanties données (4)",
    "Revenus locatifs",
]

DATA_ROW_1 = [
    "APT",
    "51 BLD DU SABLIER 13008 MARSEILLE (RP)",
    "92",
    "INDIVISIO",
    "50",
    "530000",
    "630000",
    "525000",
    "CP",
    "0",
]

DATA_ROW_2 = [
    "MAISO",
    "43 RUE JEAN BART 59200 TOURCOING (RL)",
    "120",
    "SCI",
    "50",
    "194000",
    "382000",
    "170000",
    "HP",
    "3000",
]

SAMPLE_TABLE = [HEADER_ROW, DATA_ROW_1, DATA_ROW_2]


# ---------------------------------------------------------------------------
# _normalize_header
# ---------------------------------------------------------------------------

class TestNormalizeHeader:
    def test_strips_footnote_numbers(self):
        assert _normalize_header("Type de bien (1)") == _normalize_header("Type de bien")

    def test_canonical_accent(self):
        assert _normalize_header("Type de détention (3)") == "typededetention"

    def test_pct(self):
        assert _normalize_header("% de détention") == "dedetention"

    def test_estimation(self):
        assert _normalize_header("Estimation actuelle") == "estimationactuelle"

    def test_capital(self):
        assert _normalize_header("Capital restant dû") == "capitalrestantdu"


# ---------------------------------------------------------------------------
# _parse_number
# ---------------------------------------------------------------------------

class TestParseNumber:
    def test_integer(self):
        assert _parse_number("530000") == 530000.0

    def test_with_spaces(self):
        assert _parse_number("530 000") == 530000.0

    def test_with_euro(self):
        assert _parse_number("530 000 €") == 530000.0

    def test_zero(self):
        assert _parse_number("0") == 0.0

    def test_negative_parentheses(self):
        assert _parse_number("(12000)") == -12000.0

    def test_non_numeric(self):
        assert _parse_number("APT") is None

    def test_empty(self):
        assert _parse_number("") is None


# ---------------------------------------------------------------------------
# _score_table / pick_patrimoine_table
# ---------------------------------------------------------------------------

class TestPickTable:
    def test_score_valid_table(self):
        score = _score_table(SAMPLE_TABLE)
        assert score >= 3

    def test_score_empty(self):
        assert _score_table([]) == 0

    def test_score_random_table(self):
        random_table = [["Nom", "Prénom", "Âge"], ["Alice", "Martin", "30"]]
        assert _score_table(random_table) == 0

    def test_pick_returns_best(self):
        random_table = [["Nom", "Prénom"], ["Alice", "Martin"]]
        result = pick_patrimoine_table([random_table, SAMPLE_TABLE])
        assert result is SAMPLE_TABLE

    def test_pick_none_when_no_match(self):
        random_table = [["Nom", "Prénom"], ["Alice", "Martin"]]
        assert pick_patrimoine_table([random_table]) is None

    def test_pick_none_on_empty(self):
        assert pick_patrimoine_table([]) is None


# ---------------------------------------------------------------------------
# _find_header_row_idx / _map_columns / _build_patrimoine_rows
# ---------------------------------------------------------------------------

class TestBuildRows:
    def test_finds_header_row(self):
        assert _find_header_row_idx(SAMPLE_TABLE) == 0

    def test_map_columns_completeness(self):
        col_map = _map_columns(HEADER_ROW)
        assert "type_bien" in col_map.values()
        assert "adresse" in col_map.values()
        assert "surface" in col_map.values()
        assert "type_de_detention" in col_map.values()
        assert "pct_detention" in col_map.values()
        assert "valeur_acquisition" in col_map.values()
        assert "valeur_bien" in col_map.values()
        assert "valeur_banque" in col_map.values()
        assert "garanties_donnees" in col_map.values()
        assert "revenus_locatifs" in col_map.values()

    def test_build_rows_count(self):
        rows = _build_patrimoine_rows(SAMPLE_TABLE)
        assert len(rows) == 2

    def test_build_rows_first_entry(self):
        rows = _build_patrimoine_rows(SAMPLE_TABLE)
        r = rows[0]
        assert r["type_bien"] == "APT"
        assert r["surface"] == 92.0
        assert r["type_de_detention"] == "INDIVISIO"
        assert r["pct_detention"] == 50.0
        assert r["valeur_acquisition"] == 530000.0
        assert r["valeur_bien"] == 630000.0
        assert r["valeur_banque"] == 525000.0
        assert r["garanties_donnees"] == "CP"
        assert r["revenus_locatifs"] == 0.0

    def test_build_rows_second_entry(self):
        rows = _build_patrimoine_rows(SAMPLE_TABLE)
        r = rows[1]
        assert r["type_bien"] == "MAISO"
        assert r["valeur_bien"] == 382000.0
        assert r["revenus_locatifs"] == 3000.0

    def test_empty_rows_skipped(self):
        table_with_empty = SAMPLE_TABLE + [["", "", "", "", "", "", "", "", "", ""]]
        rows = _build_patrimoine_rows(table_with_empty)
        assert len(rows) == 2

    def test_no_header_returns_empty(self):
        bad_table = [["Nom", "Prénom"], ["Alice", "30"]]
        assert _build_patrimoine_rows(bad_table) == []


# ---------------------------------------------------------------------------
# render_patrimoine_context
# ---------------------------------------------------------------------------

class TestRenderContext:
    def test_empty_when_not_available(self):
        assert render_patrimoine_context({"_native_available": False}) == ""

    def test_empty_when_no_rows(self):
        assert render_patrimoine_context({"_native_available": True, "patrimoine_immobilier_table": []}) == ""

    def test_contains_json(self):
        data = {
            "_native_available": True,
            "_native_source": "pymupdf_find_tables_patrimoine",
            "pages": {"patrimoine_immobilier": 2},
            "patrimoine_immobilier_table": [{"type_bien": "APT", "valeur_bien": 630000}],
        }
        ctx = render_patrimoine_context(data)
        assert "patrimoine_immobilier_table" in ctx
        assert "APT" in ctx
        assert "630000" in ctx


# ---------------------------------------------------------------------------
# _resolve_table_value (patrimoine_sheet)
# ---------------------------------------------------------------------------

class TestResolveTableValue:
    def test_canonical_key_direct(self):
        row = {"type_bien": "APT"}
        assert _resolve_table_value(row, "type_bien") == "APT"

    def test_alias_estimation_actuelle(self):
        row = {"estimation_actuelle": 630000}
        assert _resolve_table_value(row, "valeur_bien") == 630000

    def test_alias_capital_restant_du(self):
        row = {"capital_restant_du": 525000}
        assert _resolve_table_value(row, "valeur_banque") == 525000

    def test_old_type_detention_maps_to_type_bien(self):
        # rétrocompatibilité : ancien champ type_detention → type_bien
        row = {"type_detention": "APT - 92 m²"}
        assert _resolve_table_value(row, "type_bien") == "APT - 92 m²"

    def test_valeur_nette_calculated(self):
        row = {"valeur_bien": 630000, "valeur_banque": 525000}
        result = _resolve_table_value(row, "valeur_nette_detenue")
        assert result == 105000

    def test_valeur_nette_missing_inputs(self):
        row = {"valeur_bien": 630000}
        assert _resolve_table_value(row, "valeur_nette_detenue") is None

    def test_unknown_key_returns_none(self):
        row = {"type_bien": "APT"}
        assert _resolve_table_value(row, "champ_inexistant") is None

    def test_normalized_key_match(self):
        # LLM retourne "TypeDeBien" au lieu de "type_bien"
        row = {"TypeDeBien": "MAISO"}
        assert _resolve_table_value(row, "type_bien") == "MAISO"

    def test_col_map_coverage(self):
        # Vérifie que toutes les clés canoniques utiles sont dans la map
        expected_keys = {
            "type_bien", "adresse", "surface", "type_de_detention",
            "pct_detention", "valeur_acquisition", "valeur_bien",
            "valeur_banque", "garanties_donnees", "revenus_locatifs",
        }
        assert expected_keys.issubset(set(_PATRIMOINE_COL_MAP.keys()))
