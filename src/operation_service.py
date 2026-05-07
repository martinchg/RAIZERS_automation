from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from extraction.question_config import filter_fields_for_excel_tabs, load_questions_config

ROOT_DIR = Path(__file__).parent.parent.resolve()
OUTPUT_DIR = ROOT_DIR / "output"

SUMMARY_FIELD_IDS = [
    "raison_sociale_operation",
    "siren_operation",
    "dirigeants_operation",
    "raison_sociale_emprunt",
    "siren_emprunt",
    "dirigeants_emprunt",
    "localisation_projet",
    "montant_collecte",
    "duree",
    "ltv",
]

SECTION_TITLES = {
    "emprunt": "Société emprunteuse",
    "operation": "Société opération",
    "__other": "Opération",
}


def _has_value(value: Any) -> bool:
    return value not in (None, "", [], {})


def _resolve_section(field_id: str) -> str:
    if field_id.endswith("_emprunt"):
        return "emprunt"
    if field_id.endswith("_operation"):
        return "operation"
    return "__other"


def _load_results_payload(project_id: str) -> dict[str, Any]:
    results_path = OUTPUT_DIR / project_id / "extraction_results.json"
    if not results_path.exists():
        raise FileNotFoundError(f"Résultats d'extraction introuvables pour {project_id}")
    return json.loads(results_path.read_text(encoding="utf-8"))


def build_operation_payload(project_id: str) -> dict[str, Any]:
    payload = _load_results_payload(project_id)
    results = payload.get("results", {})
    summary = payload.get("summary", {})

    all_fields = load_questions_config(ROOT_DIR / "config")["fields"]
    operation_fields = filter_fields_for_excel_tabs(
        all_fields,
        include_operation=True,
        include_patrimoine=False,
        include_bilan=False,
        include_compte_resultat=False,
        include_lots=False,
    )
    scalar_fields = [
        field for field in operation_fields
        if field.get("type") != "table" and field.get("excel_sheet") not in ("{person_name}", "{company_name}")
    ]

    fields_by_id = {
        str(field["field_id"]): field
        for field in scalar_fields
        if field.get("field_id")
    }

    summary_cards = []
    for field_id in SUMMARY_FIELD_IDS:
        value = results.get(field_id)
        field = fields_by_id.get(field_id)
        if field and _has_value(value):
            summary_cards.append(
                {
                    "field_id": field_id,
                    "label": field.get("label", field_id),
                    "value": str(value),
                }
            )

    sections_map: dict[str, list[dict[str, str]]] = {
        "emprunt": [],
        "operation": [],
        "__other": [],
    }
    filled_count = 0

    for field in scalar_fields:
        field_id = str(field["field_id"])
        value = results.get(field_id)
        if not _has_value(value):
            continue
        filled_count += 1
        sections_map[_resolve_section(field_id)].append(
            {
                "field_id": field_id,
                "label": field.get("label", field_id),
                "value": str(value),
            }
        )

    sections = [
        {
            "id": section_id,
            "title": SECTION_TITLES[section_id],
            "items": items,
        }
        for section_id, items in sections_map.items()
        if items
    ]

    return {
        "project_id": project_id,
        "summary": {
            "answered": summary.get("answered"),
            "total": summary.get("total"),
            "answered_global_fields": summary.get("answered_global_fields"),
            "asked_global_fields": summary.get("asked_global_fields"),
            "persons": summary.get("persons"),
            "companies": summary.get("companies"),
            "filled_operation_fields": filled_count,
        },
        "summary_cards": summary_cards,
        "sections": sections,
    }
