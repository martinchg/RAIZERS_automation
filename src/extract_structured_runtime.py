"""Helpers runtime pour extract_structured.py."""

import io
import json
import logging
import re
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional

import fitz
from PIL import Image

from financial_mapping import _has_meaningful_value

logger = logging.getLogger(__name__)


def call_llm_with_retry(call_llm: Callable, *args, max_retries: int = 3, **kwargs) -> Optional[str]:
    for attempt in range(max_retries):
        try:
            return call_llm(*args, **kwargs)
        except Exception as exc:
            err_str = str(exc)
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                delay_match = re.search(r'retry(?:Delay)?["\s:]*(\d+)', err_str, re.IGNORECASE)
                wait_seconds = int(delay_match.group(1)) + 5 if delay_match else 60
                logger.warning(
                    "Rate limit LLM, retry %s/%s dans %ss",
                    attempt + 1,
                    max_retries,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue
            raise
    return None


def parse_json_response(raw_response: Optional[str]) -> Dict:
    if not raw_response:
        return {}
    try:
        parsed = json.loads(raw_response)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        json_match = re.search(r"```json\s*(.*?)\s*```", raw_response, re.DOTALL)
        if json_match:
            parsed = json.loads(json_match.group(1))
            return parsed if isinstance(parsed, dict) else {}
    return {}


def stringify_non_table_value(value) -> str:
    if isinstance(value, list):
        rendered_items = []
        for item in value:
            if isinstance(item, dict):
                name = (item.get("nom") or item.get("nom_complet") or item.get("denomination") or "").strip()
                role = (item.get("fonction") or item.get("qualite") or item.get("role") or "").strip()
                if name and role:
                    rendered_items.append(f"{name} - {role}")
                elif name:
                    rendered_items.append(name)
                elif role:
                    rendered_items.append(role)
                else:
                    rendered_items.append(", ".join(f"{key}: {val}" for key, val in item.items() if val not in (None, "")))
            else:
                rendered_items.append(str(item))
        return " ; ".join(part for part in rendered_items if part)
    if isinstance(value, dict):
        name = (value.get("nom") or value.get("nom_complet") or value.get("denomination") or "").strip()
        role = (value.get("fonction") or value.get("qualite") or value.get("role") or "").strip()
        if name and role:
            return f"{name} - {role}"
        if name:
            return name
        if role:
            return role
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def requested_field_ids(questions: List[Dict]) -> List[str]:
    return [str(question.get("field_id", "")) for question in questions if question.get("field_id")]


def merge_native_fallback_answers(
    answers: Dict,
    native_data: Optional[Dict],
    questions: List[Dict],
) -> Dict:
    if not answers:
        answers = {}
    if not native_data or not native_data.get("_native_available"):
        return answers

    for field_id in requested_field_ids(questions):
        native_value = native_data.get(field_id)
        if native_value is None:
            continue
        if not _has_meaningful_value(answers.get(field_id)):
            answers[field_id] = native_value

    return answers


def json_clone(value):
    return json.loads(json.dumps(value, ensure_ascii=False))


def write_native_financial_debug(
    project_dir: Path,
    document_id: str,
    filename: str,
    source_path: str,
    requested_financial_field_ids: List[str],
    native_data: Optional[Dict],
    llm_answers_initial: Optional[Dict],
    llm_answers_multimodal: Optional[Dict] = None,
    llm_answers_final: Optional[Dict] = None,
    validation_errors: Optional[Dict[str, List[str]]] = None,
    selected_strategy: Optional[str] = None,
    quality_report_initial: Optional[Dict] = None,
    quality_report_multimodal: Optional[Dict] = None,
    quality_report_final: Optional[Dict] = None,
    enabled: bool = False,
) -> Optional[Path]:
    if not enabled:
        return None

    debug_dir = project_dir / "debug_financial_native"
    debug_dir.mkdir(parents=True, exist_ok=True)
    debug_path = debug_dir / f"{document_id}.json"

    payload = {
        "document_id": document_id,
        "filename": filename,
        "source_path": source_path,
        "requested_field_ids": requested_financial_field_ids,
        "native_data": native_data or {},
        "llm_answers_initial": llm_answers_initial or {},
        "llm_answers_multimodal": llm_answers_multimodal or {},
        "llm_answers_final": llm_answers_final or {},
        "validation_errors": validation_errors or {},
        "selected_strategy": selected_strategy,
        "quality_report_initial": quality_report_initial or {},
        "quality_report_multimodal": quality_report_multimodal or {},
        "quality_report_final": quality_report_final or {},
    }

    with open(debug_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)

    return debug_path


def render_financial_page_images(pdf_path: Path, native_data: Optional[Dict]) -> List[Image.Image]:
    if not native_data:
        return []

    page_numbers = sorted(
        {
            int(page)
            for page in (native_data.get("pages", {}) or {}).values()
            if isinstance(page, int) and page > 0
        }
    )
    if not page_numbers:
        return []

    images: List[Image.Image] = []
    document = fitz.open(str(pdf_path))
    try:
        for page_number in page_numbers:
            if page_number > len(document):
                continue
            page = document[page_number - 1]
            pixmap = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
            image = Image.open(io.BytesIO(pixmap.tobytes("png")))
            image.load()
            if max(image.size) > 1800:
                image.thumbnail((1800, 1800))
            images.append(image)
    finally:
        document.close()

    return images
