from __future__ import annotations

import re
from typing import Any, Iterable, Optional

from scraping_excel import build_scraping_excel_export


def build_scraping_report_filename(
    property_type: Optional[str] = None,
    *,
    prefix: str = "scraping_immobilier",
) -> str:
    if not property_type:
        return f"{prefix}.xlsx"
    cleaned = re.sub(r"[^a-z0-9]+", "_", str(property_type).strip().lower()).strip("_")
    return f"{prefix}_{cleaned or 'resultats'}.xlsx"


def generate_scraping_excel_report(
    results: Iterable[dict[str, Any]],
    *,
    property_type: Optional[str] = None,
) -> bytes:
    return build_scraping_excel_export(results, property_type=property_type)
