import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import tab_immo
from immo_scoring import ComparableOutlierFilter, ComparableScorer
from tab_immo import ComparablePipeline, ComparableRequest


class FakeGeocoderClient:
    def geocode(self, address: str):
        return {
            "normalized_address": address,
            "street_name": "Rue de la Paix",
            "city": "Paris",
            "postcode": "75002",
            "longitude": 2.331,
            "latitude": 48.868,
        }


class FakeDVFClient:
    def __init__(self, records):
        self.records = records

    def search_transactions(self, **kwargs):
        return list(self.records)


def _record(
    *,
    address_number: str,
    street_name: str,
    total_price: float,
    living_area: float = 50.0,
    rooms: int = 2,
    sale_date: str = "2025-01-15",
    longitude: float,
    latitude: float,
):
    return {
        "properties": {
            "libtypbien": "Appartement",
            "valeurfonc": total_price,
            "sbati": living_area,
            "nbpprinc": rooms,
            "datemut": sale_date,
            "l_num": address_number,
            "l_typvoie": "Rue",
            "l_nomv": street_name,
        },
        "geometry": {
            "type": "Point",
            "coordinates": [longitude, latitude],
        },
    }


def test_pipeline_keeps_all_comparables_and_marks_exclusions(monkeypatch):
    monkeypatch.setattr(tab_immo, "reverse_geocode", lambda longitude, latitude: {})

    records = [
        _record(address_number="1", street_name="de la Paix", total_price=250000, longitude=2.3312, latitude=48.8681),
        _record(address_number="2", street_name="de la Paix", total_price=255000, longitude=2.3314, latitude=48.8682),
        _record(address_number="3", street_name="de la Paix", total_price=260000, longitude=2.3316, latitude=48.8683),
        _record(address_number="4", street_name="de la Paix", total_price=252500, longitude=2.3318, latitude=48.8684),
        _record(address_number="5", street_name="de la Paix", total_price=75000, longitude=2.3320, latitude=48.8685),
        _record(address_number="1", street_name="de la Paix", total_price=251000, sale_date="2024-09-20", longitude=2.3312, latitude=48.8681),
    ]

    pipeline = ComparablePipeline(
        geocoder=FakeGeocoderClient(),
        dvf_client=FakeDVFClient(records),
        scorer=ComparableScorer(),
        outlier_filter=ComparableOutlierFilter(),
    )

    result = pipeline.run(
        ComparableRequest(
            address="1 Rue de la Paix, Paris",
            property_type="appartement",
            living_area_sqm=50,
            rooms=2,
            search_radius_m=500,
            api_min_year=2024,
        )
    )

    comparables = result["comparables"]
    statistics = result["statistics"]

    assert len(comparables) == 6
    assert statistics["comparables_found"] == 6
    assert statistics["comparables_retained"] == 4
    assert statistics["comparables_excluded"] == 2
    assert statistics["average_price_per_sqm_eur"] == 5087.5
    assert statistics["median_price_per_sqm_eur"] == 5075.0
    assert statistics["min_price_per_sqm_eur"] == 5000.0
    assert statistics["max_price_per_sqm_eur"] == 5200.0

    reasons = {row["Raison"] for row in comparables if row["Retenu"] == "Non"}
    assert "price_outlier_low" in reasons
    assert "duplicate_recent_sale" in reasons

    retained_rows = [row for row in comparables if row["Retenu"] == "Oui"]
    assert retained_rows
    assert all(row["Raison"] is None for row in retained_rows)
