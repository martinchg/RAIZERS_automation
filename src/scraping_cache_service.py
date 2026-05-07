from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

ROOT_DIR = Path(__file__).parent.parent.resolve()
CACHE_DIR = ROOT_DIR / "cache" / "scraping"


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", (value or "").strip()).strip("-").lower()
    return cleaned or "ad-hoc"


def _cache_folder(project_id: Optional[str], address: Optional[str]) -> Path:
    key = project_id or _slugify(address or "")
    return CACHE_DIR / key


def load_scraping_cache(project_id: Optional[str], *, address: Optional[str] = None) -> dict[str, Any]:
    folder = _cache_folder(project_id, address)
    path = folder / "latest.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_scraping_cache(
    project_id: Optional[str],
    payload: dict[str, Any],
    *,
    address: Optional[str] = None,
) -> Path:
    folder = _cache_folder(project_id, address)
    folder.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    enriched_payload = {
        **payload,
        "cached_at": timestamp,
        "project_id": project_id,
    }

    latest_path = folder / "latest.json"
    history_path = folder / f"{timestamp}.json"

    latest_path.write_text(json.dumps(enriched_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    history_path.write_text(json.dumps(enriched_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return latest_path
