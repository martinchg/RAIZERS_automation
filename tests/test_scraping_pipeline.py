from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import scraping_pipeline


class FakeGeocoder:
    def geocode(self, address: str):
        return {
            "normalized_address": address,
            "street_name": "Rue de Test",
            "city": "Paris",
            "postcode": "75002",
            "longitude": 2.34,
            "latitude": 48.87,
        }


def test_derive_department_code():
    assert scraping_pipeline.derive_department_code("75002") == "75"
    assert scraping_pipeline.derive_department_code("97400") == "974"
    assert scraping_pipeline.derive_department_code("20137") == "2A"
    assert scraping_pipeline.derive_department_code("20200") == "2B"


def test_run_scraping_pipeline_skips_terrain_for_apartment():
    result = scraping_pipeline.run_scraping_pipeline(
        {
            "address": "12 rue de Rivoli",
            "property_type": "appartement",
            "living_area_sqm": 42,
            "rooms": 2,
            "scrapers": ["terrain_construction"],
        },
        geocoder=FakeGeocoder(),
    )

    assert result["subject"]["department_code"] == "75"
    assert result["results"][0]["source"] == "terrain_construction"
    assert result["results"][0]["status"] == "skipped"
    assert result["statistics"]["sources_skipped"] == 1


def test_run_scraping_pipeline_aggregates_results(monkeypatch):
    monkeypatch.setattr(
        scraping_pipeline,
        "_run_consortium",
        lambda subject: {
            "source": "consortium_immobilier",
            "property_type": subject["property_type"],
            "prix": [{"type_bien": "maison", "prix_m2_min": 8000, "prix_m2_moyen": 9000, "prix_m2_max": 10000}],
        },
    )
    monkeypatch.setattr(
        scraping_pipeline,
        "_run_meilleursagents",
        lambda subject, payload: {
            "source": "meilleursagents",
            "property_type": subject["property_type"],
            "tranche_basse": 8200,
            "prix_m2_moyen": 9400,
            "tranche_haute": 10600,
        },
    )

    result = scraping_pipeline.run_scraping_pipeline(
        {
            "address": "12 rue de Rivoli",
            "property_type": "maison",
            "living_area_sqm": 120,
            "rooms": 5,
            "land_area_sqm": 400,
            "scrapers": ["consortium_immobilier", "meilleursagents"],
        },
        geocoder=FakeGeocoder(),
    )

    assert len(result["results"]) == 2
    assert result["results"][0]["prix_moyen_m2"] == 9000
    assert result["results"][1]["prix_moyen_m2"] == 9400
    assert result["statistics"]["sources_succeeded"] == 2
    assert result["statistics"]["average_price_per_sqm_eur"] == 9200
