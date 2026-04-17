"""
extract_structured.py : orchestration de l'extraction structuree via LLM.

Usage :
    python src/extract_structured.py --project raizers-en-audit-projet-1
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

_SRC_DIR = Path(__file__).parent.resolve()
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from extract_structured_documents import (
    build_latest_year_per_company_folder,
    extract_company_folder_from_source_path,
    get_doc_financial_year,
    load_filtered_text,
    match_questions_to_doc,
    needs_broad_financial_context,
    resolve_cached_source_file,
    resolve_project_location,
)
from extract_structured_prompts import build_multimodal_financial_prompt, build_prompt
from extract_structured_runtime import (
    call_llm_with_retry,
    json_clone,
    merge_native_fallback_answers,
    parse_json_response,
    render_financial_page_images,
    requested_field_ids,
    stringify_non_table_value,
    write_native_financial_debug,
)
from financial_mapping import (
    _has_meaningful_value,
    financial_answers_quality_report,
    prepare_financial_answers,
    select_better_financial_answers,
    validate_financial_answers,
)
from financial_tables_native import extract_financial_data, render_financial_context
from llm_client import get_llm_client
from normalization import canonical_name, extract_person_folder
from question_config import load_questions_config
from runtime_config import configure_environment

ROOT_DIR = _SRC_DIR.parent.resolve()
configure_environment(ROOT_DIR)

logger = logging.getLogger(__name__)

OUTPUT_DIR = ROOT_DIR / "output"
MAX_CHARS = int(os.environ.get("EXTRACT_MAX_CHARS", "12000"))
FINANCIAL_MAX_CHARS = int(os.environ.get("EXTRACT_FINANCIAL_MAX_CHARS", "28000"))
FINANCIAL_NEIGHBOR_PARENTS = int(os.environ.get("EXTRACT_FINANCIAL_NEIGHBOR_PARENTS", "1"))
FINANCIAL_MAX_RELATIVE_PAGE_WINDOW = int(
    os.environ.get("EXTRACT_FINANCIAL_MAX_RELATIVE_PAGE_WINDOW", "10")
)
WRITE_NATIVE_DEBUG = os.environ.get("EXTRACT_WRITE_NATIVE_DEBUG", "0").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}

PROJECT_LOCATION_FIELD_ID = "localisation_projet"


def _is_per_person_field(field: Dict) -> bool:
    return field.get("excel_sheet") == "{person_name}"


def _is_per_company_field(field: Dict) -> bool:
    return field.get("excel_sheet") == "{company_name}"


def run(project_id: str):
    project_dir = OUTPUT_DIR / project_id
    manifest_path = project_dir / "manifest.json"
    docs_dir = project_dir / "documents"

    if not manifest_path.exists():
        logger.error("Manifest introuvable: %s", manifest_path)
        return

    with open(manifest_path, encoding="utf-8") as handle:
        manifest = json.load(handle)

    questions_config = load_questions_config(ROOT_DIR / "config")

    all_fields = [
        field for field in questions_config["fields"] if isinstance(field, dict) and field.get("field_id")
    ]
    selected_audit_folder = manifest.get("selected_audit_folder")

    global_fields = [
        field
        for field in all_fields
        if not _is_per_person_field(field) and not _is_per_company_field(field)
    ]
    person_fields = [field for field in all_fields if _is_per_person_field(field)]
    company_fields = [field for field in all_fields if _is_per_company_field(field)]

    llm_client = get_llm_client()
    call_llm = llm_client["text_call"]
    call_llm_multimodal = llm_client.get("multimodal_call")
    model_name = llm_client["model"]

    results: Dict[str, Optional[str]] = {field["field_id"]: None for field in global_fields}
    global_field_ids = {field["field_id"] for field in global_fields}
    asked_global_ids: set[str] = set()
    asked_person_keys: set[str] = set()
    asked_company_keys: set[str] = set()

    person_folder_map: Dict[str, int] = {}
    person_folder_display: Dict[str, str] = {}
    person_counter = 0

    company_name_map: Dict[str, int] = {}
    company_name_display: Dict[str, str] = {}
    company_counter = 0

    project_location_candidates: List[Dict] = []
    extraction_log = []

    latest_year_per_company = build_latest_year_per_company_folder(
        manifest["files"],
        company_fields,
        selected_audit_folder,
    )

    logger.info(
        "Debut extraction structuree: projet=%s documents=%s",
        project_id,
        len(manifest["files"]),
    )

    for doc_info in manifest["files"]:
        document_id = doc_info["document_id"]
        filename = doc_info["filename"]
        source_path = doc_info["source_path"]
        doc_path = docs_dir / f"{document_id}.jsonl"

        if not doc_path.exists():
            logger.warning("Document ignore, JSONL manquant: %s", document_id)
            continue

        matched_global = match_questions_to_doc(doc_info, global_fields, selected_audit_folder)
        unanswered_global = [field for field in matched_global if results[field["field_id"]] is None]
        matched_person = match_questions_to_doc(doc_info, person_fields, selected_audit_folder)
        matched_company = match_questions_to_doc(doc_info, company_fields, selected_audit_folder)

        if matched_company and latest_year_per_company:
            doc_year = get_doc_financial_year(doc_info)
            if doc_year is not None:
                folder = extract_company_folder_from_source_path(source_path)
                folder_key = canonical_name(folder) if folder else None
                max_year = latest_year_per_company.get(folder_key) if folder_key else None
                if max_year is not None and doc_year < max_year:
                    matched_company = []

        person_folder = None
        if matched_person:
            person_folder = extract_person_folder(source_path)
            if not person_folder:
                matched_person = []

        if not unanswered_global and not matched_person and not matched_company:
            continue

        asked_global_ids.update(field["field_id"] for field in unanswered_global)

        person_suffix = None
        if matched_person:
            folder_key = canonical_name(person_folder)
            if folder_key not in person_folder_map:
                person_folder_map[folder_key] = person_counter
                person_folder_display[folder_key] = person_folder
                person_counter += 1
            person_suffix = f"__{person_folder_map[folder_key]}"
            asked_person_keys.update(field["field_id"] + person_suffix for field in matched_person)

        questions_to_ask = unanswered_global + matched_person + matched_company
        broad_financial_context = needs_broad_financial_context(questions_to_ask)
        logger.info(
            "Document traite: %s | global=%s person=%s company=%s financial=%s",
            filename,
            len(unanswered_global),
            len(matched_person),
            len(matched_company),
            "oui" if broad_financial_context else "non",
        )

        native_financial_data: Optional[Dict] = None
        native_context = ""
        cached_pdf: Optional[Path] = None
        if broad_financial_context and str(doc_info.get("file_type", "")).lower() == "pdf":
            cached_pdf = resolve_cached_source_file(manifest, source_path, filename)
            if cached_pdf:
                try:
                    native_financial_data = extract_financial_data(cached_pdf)
                    native_context = render_financial_context(
                        native_financial_data,
                        requested_field_ids(questions_to_ask),
                    )
                except Exception as exc:
                    logger.warning("Extraction financiere native indisponible pour %s: %s", filename, exc)
            else:
                logger.warning("PDF source introuvable dans le cache pour %s", filename)

        doc_text = load_filtered_text(
            doc_path,
            questions_to_ask,
            max_chars=FINANCIAL_MAX_CHARS if broad_financial_context else MAX_CHARS,
            preserve_order=broad_financial_context,
            neighbor_parents=FINANCIAL_NEIGHBOR_PARENTS if broad_financial_context else 0,
            append_unmatched_tail=not broad_financial_context,
            max_relative_page_window=(
                FINANCIAL_MAX_RELATIVE_PAGE_WINDOW if broad_financial_context else None
            ),
        )

        prompt = build_prompt(
            doc_text,
            questions_to_ask,
            filename,
            source_path,
            native_context=native_context,
        )

        try:
            raw_response = call_llm_with_retry(call_llm, prompt)
        except Exception as exc:
            logger.error("Erreur LLM sur %s: %s", filename, exc)
            extraction_log.append(
                {
                    "document_id": document_id,
                    "filename": filename,
                    "error": str(exc)[:100],
                }
            )
            continue

        if raw_response is None:
            extraction_log.append(
                {
                    "document_id": document_id,
                    "filename": filename,
                    "error": "max retries exceeded (429)",
                }
            )
            time.sleep(5)
            continue

        answers = parse_json_response(raw_response)
        if not answers and raw_response:
            logger.error("Reponse non JSON pour %s", filename)

        llm_answers_initial: Optional[Dict] = json_clone(answers) if isinstance(answers, dict) else None
        llm_answers_multimodal: Optional[Dict] = None
        final_validation_errors: Dict[str, List[str]] = {}
        quality_report_initial: Dict[str, object] = {}
        quality_report_multimodal: Dict[str, object] = {}
        quality_report_final: Dict[str, object] = {}
        selected_strategy = "text"

        if broad_financial_context:
            answers = merge_native_fallback_answers(answers, native_financial_data, questions_to_ask)
            answers = prepare_financial_answers(answers)
            final_validation_errors = validate_financial_answers(answers)
            quality_report_initial = financial_answers_quality_report(answers, final_validation_errors)

            if quality_report_initial.get("should_retry_multimodal") and call_llm_multimodal and cached_pdf:
                page_images = render_financial_page_images(cached_pdf, native_financial_data)
                if page_images:
                    logger.info(
                        "Strategie financiere: fallback multimodal sur %s (%s)",
                        filename,
                        "; ".join(quality_report_initial.get("reasons", [])) or "score faible",
                    )
                    multimodal_prompt = build_multimodal_financial_prompt(
                        questions_to_ask,
                        filename,
                        source_path,
                        native_financial_data.get("pages", {}) if native_financial_data else {},
                    )
                    try:
                        multimodal_response = call_llm_with_retry(
                            call_llm_multimodal,
                            multimodal_prompt,
                            page_images,
                        )
                    except Exception as exc:
                        logger.warning("Fallback multimodal en echec pour %s: %s", filename, exc)
                    else:
                        multimodal_answers = parse_json_response(multimodal_response)
                        if multimodal_answers:
                            llm_answers_multimodal = json_clone(multimodal_answers)
                            multimodal_answers = prepare_financial_answers(multimodal_answers)
                            multimodal_validation = validate_financial_answers(multimodal_answers)
                            quality_report_multimodal = financial_answers_quality_report(
                                multimodal_answers,
                                multimodal_validation,
                            )
                            answers, quality_report_final, selected_strategy = select_better_financial_answers(
                                answers,
                                quality_report_initial,
                                multimodal_answers,
                                quality_report_multimodal,
                            )
                            final_validation_errors = validate_financial_answers(answers)
                        else:
                            logger.warning("Reponse multimodale non exploitable pour %s", filename)
                else:
                    logger.warning("Pages financieres non rendues pour le fallback multimodal: %s", filename)

            if not quality_report_final:
                quality_report_final = financial_answers_quality_report(answers, final_validation_errors)

            logger.info(
                "Strategie financiere retenue pour %s: %s (score=%s)",
                filename,
                selected_strategy,
                quality_report_final.get("score"),
            )

        debug_path = write_native_financial_debug(
            project_dir=project_dir,
            document_id=document_id,
            filename=filename,
            source_path=source_path,
            requested_financial_field_ids=requested_field_ids(questions_to_ask),
            native_data=native_financial_data,
            llm_answers_initial=llm_answers_initial,
            llm_answers_multimodal=llm_answers_multimodal,
            llm_answers_final=answers if isinstance(answers, dict) else {},
            validation_errors=final_validation_errors,
            selected_strategy=selected_strategy,
            quality_report_initial=quality_report_initial,
            quality_report_multimodal=quality_report_multimodal,
            quality_report_final=quality_report_final,
            enabled=broad_financial_context and WRITE_NATIVE_DEBUG,
        )

        found = 0
        for field_id, value in answers.items():
            if field_id == PROJECT_LOCATION_FIELD_ID and _has_meaningful_value(value):
                project_location_candidates.append(
                    {
                        "document_id": document_id,
                        "filename": filename,
                        "source_path": source_path,
                        "value": stringify_non_table_value(value),
                    }
                )
                continue

            if (
                field_id in results
                and _has_meaningful_value(value)
                and not _has_meaningful_value(results[field_id])
            ):
                results[field_id] = stringify_non_table_value(value)
                found += 1

        if matched_person and person_suffix:
            for field in matched_person:
                field_id = field["field_id"]
                value = answers.get(field_id)
                if not _has_meaningful_value(value):
                    continue
                key = field_id + person_suffix
                if key in results and _has_meaningful_value(results[key]):
                    continue
                if isinstance(value, list):
                    results[key] = (
                        json.dumps(value, ensure_ascii=False)
                        if field.get("type") == "table"
                        else stringify_non_table_value(value)
                    )
                else:
                    results[key] = stringify_non_table_value(value)
                found += 1

        if matched_company:
            raw_company_name = (answers.get("bilan_societe_nom") or "").strip()
            company_name = raw_company_name or Path(filename).stem.strip()
            company_key = canonical_name(company_name)
            if company_key and company_key not in company_name_map:
                company_name_map[company_key] = company_counter
                company_name_display[company_key] = company_name
                company_counter += 1

            if company_key:
                suffix = f"__{company_name_map[company_key]}"
                asked_company_keys.update(field["field_id"] + suffix for field in matched_company)
                for field in matched_company:
                    field_id = field["field_id"]
                    value = answers.get(field_id)
                    if not _has_meaningful_value(value):
                        continue
                    key = field_id + suffix
                    if key in results and _has_meaningful_value(results[key]):
                        continue
                    if isinstance(value, list):
                        results[key] = (
                            json.dumps(value, ensure_ascii=False)
                            if field.get("type") == "table"
                            else stringify_non_table_value(value)
                        )
                    else:
                        results[key] = stringify_non_table_value(value)
                    found += 1

        extraction_log.append(
            {
                "document_id": document_id,
                "filename": filename,
                "questions_asked": len(questions_to_ask),
                "answers_found": found,
                "native_debug_path": str(debug_path.relative_to(project_dir)) if debug_path else None,
                "financial_strategy": selected_strategy if broad_financial_context else None,
                "financial_quality_score": quality_report_final.get("score") if broad_financial_context else None,
            }
        )

        time.sleep(2)

    resolved_project_location = resolve_project_location(project_location_candidates)
    if resolved_project_location:
        results[PROJECT_LOCATION_FIELD_ID] = resolved_project_location
    elif project_location_candidates:
        logger.warning(
            "Localisation projet ignoree: aucune adresse precise corroborree sur deux documents"
        )

    answered_global = sum(1 for field_id in global_field_ids if results.get(field_id) is not None)
    answered_person = sum(1 for key in asked_person_keys if results.get(key) is not None)
    answered_company = sum(1 for key in asked_company_keys if results.get(key) is not None)
    answered = answered_global + answered_person + answered_company
    total = len(asked_global_ids) + len(asked_person_keys) + len(asked_company_keys)

    folder_suffix_map = {
        f"__{index}": person_folder_display.get(folder_key, folder_key)
        for folder_key, index in person_folder_map.items()
    }
    company_suffix_map = {
        f"__{index}": company_name_display.get(company_key, company_key)
        for company_key, index in company_name_map.items()
    }

    results_path = project_dir / "extraction_results.json"
    with open(results_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "project_id": project_id,
                "model": model_name,
                "results": results,
                "person_folders": folder_suffix_map,
                "company_names": company_suffix_map,
                "log": extraction_log,
                "summary": {
                    "answered": answered,
                    "total": total,
                    "configured_global_fields": len(global_fields),
                    "configured_person_fields": len(person_fields),
                    "configured_company_fields": len(company_fields),
                    "answered_global_fields": answered_global,
                    "answered_person_fields": answered_person,
                    "answered_company_fields": answered_company,
                    "asked_global_fields": len(asked_global_ids),
                    "asked_person_fields": len(asked_person_keys),
                    "asked_company_fields": len(asked_company_keys),
                    "persons": person_counter,
                    "companies": company_counter,
                },
            },
            handle,
            ensure_ascii=False,
            indent=2,
        )

    logger.info(
        "Fin extraction structuree: projet=%s answered=%s/%s personnes=%s societes=%s",
        project_id,
        answered,
        total,
        person_counter,
        company_counter,
    )
    logger.info("Resultats ecrits: %s", results_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = argparse.ArgumentParser(description="Extraction structuree via LLM")
    parser.add_argument("--project", "-p", required=True, help="project_id")
    args = parser.parse_args()

    run(args.project)
