from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import scraping_cache_service


def test_save_and_load_scraping_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(scraping_cache_service, "CACHE_DIR", tmp_path / "scraping")

    payload = {
        "subject": {"address": "12 rue de Rivoli"},
        "results": [{"source": "consortium_immobilier", "prix_moyen_m2": 9000}],
        "statistics": {"sources_succeeded": 1},
    }

    latest_path = scraping_cache_service.save_scraping_cache("project-123", payload, address="12 rue de Rivoli")
    assert latest_path.exists()

    latest_payload = json.loads(latest_path.read_text(encoding="utf-8"))
    assert latest_payload["project_id"] == "project-123"
    assert latest_payload["results"][0]["source"] == "consortium_immobilier"

    loaded = scraping_cache_service.load_scraping_cache("project-123")
    assert loaded["statistics"]["sources_succeeded"] == 1
