from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from core.normalization import canonical_name
from project_catalog_service import build_project_catalog

ROOT_DIR = Path(__file__).parent.parent.resolve()
OUTPUT_DIR = ROOT_DIR / "output"


def _load_json_if_exists(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _split_display_name(value: str) -> tuple[str, str]:
    parts = [part for part in value.strip().split() if part]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def build_patrimoine_payload(project_id: str) -> dict[str, Any]:
    catalog = build_project_catalog(project_id)
    mandats_data = _load_json_if_exists(OUTPUT_DIR / project_id / "mandats_results.json") or {}

    people = catalog.get("people") or []
    summary_cards: list[dict[str, str]] = []
    details: list[dict[str, Any]] = []

    societes_by_person = mandats_data.get("societes_par_personne") or {}
    for folder_name, people_map in societes_by_person.items():
        for person_name, companies in people_map.items():
            summary_cards.append(
                {
                    "label": person_name,
                    "value": f"{len(companies)} société(s) trouvée(s)",
                }
            )
            details.append(
                {
                    "folder_name": folder_name,
                    "person_name": person_name,
                    "companies": companies,
                }
            )

    return {
        "project_id": project_id,
        "people": people,
        "summary_cards": summary_cards,
        "details": details,
        "summary": {
            "people_detected": len(people),
            "people_enriched": len(summary_cards),
            "companies_found": sum(len(item.get("companies", [])) for item in details),
        },
    }


def build_people_by_folder_from_selection(selection: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    people_by_folder: dict[str, list[dict[str, Any]]] = {}

    for item in selection:
        display_name = (item.get("display_name") or "").strip()
        if not display_name:
            continue

        raw_nom = (item.get("nom") or "").strip()
        raw_prenoms = (item.get("prenoms") or "").strip()

        if raw_nom:
            nom = canonical_name(raw_nom).upper()
            prenoms = raw_prenoms
        else:
            first_name, remainder = _split_display_name(display_name)
            nom = canonical_name(remainder).upper() if remainder else canonical_name(first_name).upper()
            prenoms = first_name if remainder else ""

        folder_name = (item.get("folder_name") or display_name or "Ajout manuel").strip()

        people_by_folder.setdefault(folder_name, []).append(
            {
                "nom": nom,
                "prenoms": prenoms,
                "date_naissance": item.get("birth_date"),
            }
        )

    return people_by_folder
