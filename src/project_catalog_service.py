from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from core.normalization import canonical_name, extract_person_folder, path_has_segments
from financial_service import detect_financial_companies

ROOT_DIR = Path(__file__).parent.parent.resolve()
OUTPUT_DIR = ROOT_DIR / "output"


def _load_manifest(project_id: str) -> dict[str, Any]:
    manifest_path = OUTPUT_DIR / project_id / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest introuvable pour {project_id}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _load_json_if_exists(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _fingerprint_entries(entries: list[str]) -> str:
    digest = hashlib.sha256()
    for entry in sorted(entries):
        digest.update(entry.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _manifest_signature(file_info: dict[str, Any]) -> str:
    return f"{file_info.get('source_path', '')}::{file_info.get('file_size_bytes', '')}"


def compute_catalog_fingerprints(manifest: dict[str, Any]) -> dict[str, Any]:
    people_entries: list[str] = []
    financial_entries: list[str] = []

    for file_info in manifest.get("files", []) or []:
        source_path = file_info.get("source_path", "")
        if not source_path:
            continue

        signature = _manifest_signature(file_info)
        if extract_person_folder(source_path):
            people_entries.append(signature)
        if path_has_segments(source_path, "*Opérateur/*Eléments financiers") or "bilan" in source_path.lower():
            financial_entries.append(signature)

    return {
        "people": _fingerprint_entries(people_entries),
        "financial": _fingerprint_entries(financial_entries),
        "people_input_count": len(people_entries),
        "financial_input_count": len(financial_entries),
    }


def _build_people_candidates(project_id: str, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    casier_data = _load_json_if_exists(OUTPUT_DIR / project_id / "people_from_casiers.json") or {}

    people_by_key: dict[str, dict[str, Any]] = {}

    for folder_name, people in (casier_data.get("people_by_folder") or {}).items():
        folder_key = canonical_name(folder_name)
        for person in people:
            nom = (person.get("nom") or "").strip()
            prenoms = (person.get("prenoms") or "").strip()
            display_name = f"{prenoms} {nom}".strip() or folder_name
            key = canonical_name(display_name) or folder_key
            people_by_key[key] = {
                "id": key,
                "display_name": display_name,
                "nom": nom,
                "prenoms": prenoms,
                "source": "casier",
                "folder_name": folder_name,
                "birth_date": person.get("date_naissance"),
                "selected": True,
            }

    return sorted(people_by_key.values(), key=lambda item: item["display_name"].lower())


def _build_company_candidates(project_id: str) -> dict[str, Any]:
    extraction_data = _load_json_if_exists(OUTPUT_DIR / project_id / "extraction_results.json") or {}
    financial_companies = detect_financial_companies(project_id)

    companies_by_key: dict[str, dict[str, Any]] = {}

    for company in financial_companies:
        key = canonical_name(company.get("name", ""))
        if not key:
            continue
        companies_by_key[key] = {
            "id": key,
            "display_name": company.get("name"),
            "source": "financial_cache",
            "selected": True,
        }

    company_names = extraction_data.get("company_names") or {}
    for company_name in company_names.values():
        key = canonical_name(company_name or "")
        if not key:
            continue
        companies_by_key.setdefault(
            key,
            {
                "id": key,
                "display_name": company_name,
                "source": "extraction",
                "selected": True,
            },
        )

    default_financial_selection = {}
    for company in financial_companies:
        key = canonical_name(company.get("name", ""))
        files_by_period = company.get("filesByPeriod") or {}
        period = next(iter(files_by_period.keys()), None)
        file_info = files_by_period.get(period, [None])[0] if period else None
        if key and period and file_info:
            default_financial_selection[key] = {
                "company_id": key,
                "period": period,
                "file_id": file_info.get("id"),
                "file_name": file_info.get("name"),
            }

    return {
        "companies": sorted(companies_by_key.values(), key=lambda item: item["display_name"].lower()),
        "financial_companies": financial_companies,
        "default_financial_selection": default_financial_selection,
    }


def build_project_catalog(project_id: str) -> dict[str, Any]:
    manifest = _load_manifest(project_id)
    company_data = _build_company_candidates(project_id)
    people = _build_people_candidates(project_id, manifest)
    fingerprints = compute_catalog_fingerprints(manifest)

    return {
        "project_id": project_id,
        "project_path": manifest.get("project_path"),
        "selected_audit_folder": manifest.get("selected_audit_folder"),
        "people": people,
        "companies": company_data["companies"],
        "financial_companies": company_data["financial_companies"],
        "default_financial_selection": company_data["default_financial_selection"],
        "fingerprints": fingerprints,
        "summary": {
            "people_detected": len(people),
            "companies_detected": len(company_data["companies"]),
            "financial_companies_detected": len(company_data["financial_companies"]),
            "financial_files_detected": sum(
                company.get("fileCount", 0) for company in company_data["financial_companies"]
            ),
            "people_inputs_detected": fingerprints["people_input_count"],
            "financial_inputs_detected": fingerprints["financial_input_count"],
        },
    }


def save_project_catalog(project_id: str) -> dict[str, Any]:
    catalog = build_project_catalog(project_id)
    output_path = OUTPUT_DIR / project_id / "project_catalog.json"
    output_path.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
    return catalog
