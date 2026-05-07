from __future__ import annotations

import sys
from io import BytesIO
from pathlib import Path

from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from scraping_excel import build_scraping_excel_export, normalize_scraping_results


def test_normalize_scraping_results_supports_target_scrapers():
    rows = normalize_scraping_results(
        [
            {
                "source": "consortium_immobilier",
                "prix": [{"type_bien": "maison", "prix_m2_min": 8200, "prix_m2_moyen": 9100, "prix_m2_max": 10200}],
            },
            {
                "source": "lesclesdumidi",
                "prix": [{"type_bien": "maison", "prix_m2_min": 8000, "prix_m2_moyen": 9000, "prix_m2_max": 9800}],
            },
            {
                "source": "terrain_construction",
                "prix_m2_moyen": 3500,
            },
            {
                "source": "meilleursagents",
                "tranche_basse": 8700,
                "prix_m2_moyen": 9400,
                "tranche_haute": 10100,
            },
        ],
        property_type="maison",
    )

    assert rows[0]["prix_bas_m2"] == 8200
    assert rows[0]["prix_moyen_m2"] == 9100
    assert rows[0]["prix_haut_m2"] == 10200

    assert rows[1]["prix_bas_m2"] == 8000
    assert rows[1]["prix_moyen_m2"] == 9000
    assert rows[1]["prix_haut_m2"] == 9800

    assert rows[2]["prix_bas_m2"] is None
    assert rows[2]["prix_moyen_m2"] == 3500
    assert rows[2]["prix_haut_m2"] is None

    assert rows[3]["prix_bas_m2"] == 8700
    assert rows[3]["prix_moyen_m2"] == 9400
    assert rows[3]["prix_haut_m2"] == 10100


def test_build_scraping_excel_export_adds_global_average_row():
    excel_bytes = build_scraping_excel_export(
        [
            {
                "source": "consortium_immobilier",
                "prix": [{"type_bien": "maison", "prix_m2_min": 8000, "prix_m2_moyen": 9000, "prix_m2_max": 10000}],
            },
            {
                "source": "meilleursagents",
                "tranche_basse": 8200,
                "prix_m2_moyen": 9400,
                "tranche_haute": 10600,
            },
        ],
        property_type="maison",
    )

    workbook = load_workbook(BytesIO(excel_bytes))
    sheet = workbook["Scrapers"]

    assert sheet["A5"].value == "consortium_immobilier"
    assert sheet["A6"].value == "meilleursagents"
    assert sheet["A7"].value == "Moyenne globale"
    assert sheet["D7"].value == 8100
    assert sheet["E7"].value == 9200
    assert sheet["F7"].value == 10300
    assert len(sheet._charts) == 1
