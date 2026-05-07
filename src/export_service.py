from __future__ import annotations

import json
import re
from io import BytesIO
from pathlib import Path
from typing import Any

from extraction.question_config import load_questions_config
from scraping_cache_service import load_scraping_cache
from scraping_excel import build_scraping_excel_export
from sheets.excel_filler import fill_excel
from tab_consolide import build_consolidated_excel
from tab_immo import build_immo_excel_export

ROOT_DIR = Path(__file__).parent.parent.resolve()
OUTPUT_DIR = ROOT_DIR / "output"


def _load_json_if_exists(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _display_project_name(project_path: str | None, project_id: str) -> str:
    if not project_path:
        return project_id
    name = project_path.rstrip("/").rsplit("/", 1)[-1].strip()
    return re.sub(r"^\d+\.\s*", "", name) or project_id


def _sanitize_filename_part(value: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', " ", value).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def build_report_filename(project_id: str) -> str:
    manifest = _load_json_if_exists(OUTPUT_DIR / project_id / "manifest.json") or {}
    project_name = _sanitize_filename_part(
        _display_project_name(manifest.get("project_path"), project_id)
    )
    audit_folder = _sanitize_filename_part(manifest.get("selected_audit_folder") or "")
    if audit_folder:
        return f"{project_name} - {audit_folder}.xlsx"
    return f"{project_name}.xlsx"


def get_export_status(project_id: str) -> dict[str, Any]:
    project_dir = OUTPUT_DIR / project_id
    report_path = project_dir / "rapport.xlsx"
    report_filename = build_report_filename(project_id)
    extraction_results = _load_json_if_exists(project_dir / "extraction_results.json") or {}
    mandats_results = _load_json_if_exists(project_dir / "mandats_results.json") or {}
    bilan_results = _load_json_if_exists(project_dir / "bilan_results.json") or []

    scraping_data = load_scraping_cache(project_id)
    scraping_results = scraping_data.get("results") or []

    tabs = {
        "operation": bool(extraction_results.get("results")),
        "patrimoine": bool(mandats_results.get("societes_par_personne")) or bool(extraction_results.get("person_folders")),
        "financier": bool(bilan_results) or bool(extraction_results.get("company_names")),
        "immo": False,
        "scraping": bool(scraping_results),
    }

    return {
        "project_id": project_id,
        "report_exists": report_path.exists(),
        "report_filename": report_filename,
        "report_download_url": f"/api/audit/projects/{project_id}/export/report",
        "tabs": tabs,
        "summaries": {
            "operation": {
                "answered": (extraction_results.get("summary") or {}).get("answered_global_fields"),
                "asked": (extraction_results.get("summary") or {}).get("asked_global_fields"),
            },
            "patrimoine": {
                "people_detected": (extraction_results.get("summary") or {}).get("persons"),
                "people_enriched": (mandats_results.get("summary") or {}).get("persons"),
                "companies_found": (mandats_results.get("summary") or {}).get("societes"),
            },
            "financier": {
                "companies_detected": len(extraction_results.get("company_names") or {}),
                "bilan_exports_ready": len(bilan_results),
            },
            "scraping": {
                "sources_total": len(scraping_results),
                "sources_ok": len([r for r in scraping_results if r.get("status") == "ok"]),
            },
        },
    }


def _write_bytes_to_workbook(path: Path, payload: bytes) -> Path:
    path.write_bytes(payload)
    return path


def _build_scraping_bytes(project_id: str) -> bytes | None:
    scraping_data = load_scraping_cache(project_id)
    results = scraping_data.get("results") or []
    if not results:
        return None
    property_type = (scraping_data.get("subject") or {}).get("property_type")
    return build_scraping_excel_export(results, property_type=property_type)


def generate_excel_report(
    project_id: str,
    selected_tabs: list[str] | None = None,
    immo_result: dict[str, Any] | None = None,
) -> Path:
    project_dir = OUTPUT_DIR / project_id
    results_path = project_dir / "extraction_results.json"
    report_path = project_dir / "rapport.xlsx"
    selected = set(selected_tabs) if selected_tabs else {"operation", "patrimoine", "financier"}
    include_immo = "immo" in selected and immo_result is not None
    include_scraping = "scraping" in selected
    include_audit = bool(selected & {"operation", "patrimoine", "financier"})

    scraping_bytes = _build_scraping_bytes(project_id) if include_scraping else None

    if not include_audit:
        parts: list[bytes] = []
        if include_immo:
            parts.append(build_immo_excel_export(immo_result))
        if scraping_bytes:
            parts.append(scraping_bytes)
        if not parts:
            raise ValueError("Aucun onglet sélectionné avec des données disponibles.")
        merged = parts[0]
        for part in parts[1:]:
            merged = build_consolidated_excel(merged, part)
        return _write_bytes_to_workbook(report_path, merged)

    if not results_path.exists():
        raise FileNotFoundError(f"extraction_results.json introuvable pour {project_id}")

    extraction_data = json.loads(results_path.read_text(encoding="utf-8"))
    questions_data = load_questions_config(ROOT_DIR / "config")
    fields = [field for field in questions_data["fields"] if isinstance(field, dict) and field.get("field_id")]

    mandats_data = _load_json_if_exists(project_dir / "mandats_results.json") or {}
    bilan_results = _load_json_if_exists(project_dir / "bilan_results.json")
    audit_report_path = fill_excel(
        results=extraction_data["results"],
        fields=fields,
        output_dir=project_dir,
        person_folder_map=extraction_data.get("person_folders"),
        pappers_mandats=mandats_data.get("societes_par_personne"),
        bilan_results=bilan_results,
        include_operation="operation" in selected,
        include_patrimoine="patrimoine" in selected,
        include_bilan="financier" in selected,
        include_compte_resultat="financier" in selected,
        include_lots=False,
    )

    if include_immo or scraping_bytes:
        base = audit_report_path.read_bytes()
        if include_immo:
            base = build_consolidated_excel(base, build_immo_excel_export(immo_result))
        if scraping_bytes:
            base = build_consolidated_excel(base, scraping_bytes)
        return _write_bytes_to_workbook(report_path, base)

    return audit_report_path
