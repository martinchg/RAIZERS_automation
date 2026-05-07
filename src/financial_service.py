from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from core.normalization import canonical_name
from pappers.pappers_fetch_comptes import _is_bilan_folder

ROOT_DIR = Path(__file__).parent.parent.resolve()
OUTPUT_DIR = ROOT_DIR / "output"
CACHE_DIR = ROOT_DIR / "cache"


def resolve_project_cache_path(project_id: str) -> Path | None:
    raizers_cache = CACHE_DIR / "RAIZERS - En audit"
    if not raizers_cache.exists():
        return None

    slug_to_folder = {
        path.name.lower().replace(" ", "-"): path
        for path in raizers_cache.iterdir()
        if path.is_dir()
    }
    operation_name = project_id.replace("raizers-en-audit-", "").replace("-", " ")
    return next((value for key, value in slug_to_folder.items() if operation_name in key), None)


def _extract_year_from_path(pdf_path: Path) -> int | None:
    for text in (pdf_path.stem, pdf_path.parent.name):
        match = re.search(r"\b(20\d{2})\b", text)
        if match:
            return int(match.group(1))
    return None


def _company_root(pdf_path: Path, bilan_dir: Path) -> Path:
    try:
        rel = pdf_path.relative_to(bilan_dir)
    except ValueError:
        return pdf_path.parent
    parts = rel.parts
    if len(parts) <= 1:
        return bilan_dir
    return bilan_dir / parts[0]


def _infer_company_name_from_pdf(pdf_path: Path) -> str:
    stem = pdf_path.stem
    stem_clean = re.sub(
        r"\b(bilan|comptes?|annuels?|liasse|etats?\s*financiers?|projet|cac|rapport|\d{4})\b",
        " ",
        stem,
        flags=re.IGNORECASE,
    ).strip(" .-_")
    name = re.sub(r"\s{2,}", " ", stem_clean).strip()
    return name or pdf_path.stem


def detect_financial_companies(project_id: str) -> list[dict[str, Any]]:
    cache_path = resolve_project_cache_path(project_id)
    if not cache_path or not cache_path.exists():
        return []

    groups: dict[str, dict[str, Any]] = {}

    for bilan_dir in cache_path.rglob("*"):
        if not bilan_dir.is_dir() or not _is_bilan_folder(bilan_dir):
            continue

        pdf_paths = sorted(bilan_dir.rglob("*.pdf"))
        for pdf_path in pdf_paths:
            company_root = _company_root(pdf_path, bilan_dir)
            group_id = str(company_root.resolve())

            group = groups.get(group_id)
            if group is None:
                name = company_root.name if company_root != bilan_dir else _infer_company_name_from_pdf(pdf_path)
                group = {
                    "id": group_id,
                    "name": name,
                    "filesByPeriod": {},
                }
                groups[group_id] = group

            year = _extract_year_from_path(pdf_path)
            period = f"Bilan {year}" if year else "Autres bilans"
            relative_path = str(pdf_path.relative_to(cache_path))
            file_id = canonical_name(relative_path.replace("/", " "))
            group["filesByPeriod"].setdefault(period, []).append(
                {
                    "id": file_id,
                    "name": pdf_path.name,
                    "path": relative_path,
                    "year": year,
                }
            )

    items = list(groups.values())
    for item in items:
        item["filesByPeriod"] = {
            period: sorted(
                files,
                key=lambda file_info: (file_info.get("year") or 0, file_info.get("name", "")),
                reverse=True,
            )
            for period, files in sorted(
                item["filesByPeriod"].items(),
                key=lambda entry: entry[0],
                reverse=True,
            )
        }
        item["fileCount"] = sum(len(files) for files in item["filesByPeriod"].values())

    return sorted(items, key=lambda item: item["name"].lower())


def _load_json_if_exists(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def build_financial_payload(project_id: str) -> dict[str, Any]:
    companies = detect_financial_companies(project_id)
    bilan_results = _load_json_if_exists(OUTPUT_DIR / project_id / "bilan_results.json") or []
    extraction_results = _load_json_if_exists(OUTPUT_DIR / project_id / "extraction_results.json") or {}

    summary_cards: list[dict[str, str]] = []
    for result in bilan_results:
        company = result.get("company") or "Société inconnue"
        dates = result.get("dates") or {}
        pdf_n = dates.get("pdf_n") or "n/a"
        pappers_n = dates.get("pappers_n") or "n/a"
        summary_cards.append(
            {
                "label": company,
                "value": f"PDF: {pdf_n} · Pappers: {pappers_n}",
            }
        )

    extraction_summary = extraction_results.get("summary") or {}
    return {
        "project_id": project_id,
        "companies": companies,
        "summary_cards": summary_cards,
        "summary": {
            "detected_companies": len(companies),
            "detected_files": sum(company.get("fileCount", 0) for company in companies),
            "extracted_companies": len(summary_cards),
            "configured_company_fields": extraction_summary.get("configured_company_fields"),
            "answered_company_fields": extraction_summary.get("answered_company_fields"),
            "asked_company_fields": extraction_summary.get("asked_company_fields"),
        },
    }


def extract_selected_financials(
    project_id: str,
    selections: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    from financial.financial_bilan_integrator import run_for_pdf

    cache_path = resolve_project_cache_path(project_id)
    if not cache_path or not cache_path.exists():
        raise FileNotFoundError(f"Cache introuvable pour {project_id}")

    detected_companies = detect_financial_companies(project_id)
    selected_entries: list[tuple[Path, str, int | None]] = []

    if selections:
        companies_by_id = {company["id"]: company for company in detected_companies}
        for selection in selections:
            company = companies_by_id.get(selection.get("company_id"))
            if not company:
                continue

            period = selection.get("period")
            file_id = selection.get("file_id")
            file_options = (company.get("filesByPeriod") or {}).get(period, [])
            selected_file = next((file for file in file_options if file.get("id") == file_id), None)
            if not selected_file:
                continue

            pdf_path = cache_path / selected_file["path"]
            selected_entries.append((pdf_path, company.get("name") or pdf_path.stem, selected_file.get("year")))

    if not selected_entries:
        for company in detected_companies:
            files_by_period = company.get("filesByPeriod") or {}
            default_period = next(iter(files_by_period.keys()), None)
            default_file = files_by_period.get(default_period, [None])[0] if default_period else None
            if not default_file:
                continue
            pdf_path = cache_path / default_file["path"]
            selected_entries.append((pdf_path, company.get("name") or pdf_path.stem, default_file.get("year")))

    results = []
    output_dir = OUTPUT_DIR / project_id
    for pdf_path, company_name, year in selected_entries:
        result = run_for_pdf(
            pdf_path,
            project_id,
            company_name=company_name,
            target_year=str(year) if year else None,
            pdf_year=year,
            output_dir=output_dir,
        )
        result["detected_year"] = year
        results.append(result)

    bilan_results_path = output_dir / "bilan_results.json"
    bilan_results_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    return build_financial_payload(project_id)
