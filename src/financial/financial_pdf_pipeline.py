"""Pipeline PDF financier ciblé.

Étape 1 : extraction brute multimodale OpenAI (gpt-4o) → lignes brutes {l, n, n1}
Étape 2 : mapping LLM (gpt-4o) → {feature_key: [source_labels]}  — aucune valeur
Étape 3 : résolution Python → sommes exactes à partir des valeurs du first pass
"""

from __future__ import annotations

import argparse
import io
import json
from pathlib import Path

import fitz
from PIL import Image

from extraction.extract_structured_runtime import call_llm_with_retry, parse_json_response
from financial.financial_pdf_llm_prep import (
    build_openai_financial_first_pass_prompt,
    detect_financial_page_spans,
)
from financial.financial_pdf_second_pass import (
    build_mapping_prompt,
    load_feature_keys,
    resolve_mapping,
)
from core.llm_client import get_llm_client
from pappers.pappers_comptes_revelateur import write_revealing_excel
from core.runtime_config import configure_environment


ROOT_DIR = Path(__file__).parent.parent.parent.resolve()
configure_environment(ROOT_DIR)

FIRST_PASS_OPENAI_MODEL = "gpt-4o"
SECOND_PASS_OPENAI_MODEL = "gpt-4o"


def _render_pages(pdf_path: Path, pages: list[int]) -> list[Image.Image]:
    doc = fitz.open(str(pdf_path))
    images: list[Image.Image] = []
    try:
        for page_num in pages:
            page = doc[page_num - 1]
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
            image = Image.open(io.BytesIO(pix.tobytes("png")))
            image.load()
            if max(image.size) > 1800:
                image.thumbnail((1800, 1800))
            images.append(image)
    finally:
        doc.close()
    return images


def run(
    *,
    pdf_path: Path,
    revealing_json_path: Path,
    title: str | None = None,
    target_year: str | None = None,
    first_pass_output_json: Path | None = None,
    second_pass_output_json: Path | None = None,
    second_pass_output_excel: Path | None = None,
) -> dict:
    pdf_path = pdf_path.resolve()
    revealing_json_path = revealing_json_path.resolve()
    resolved_title = title or pdf_path.stem

    spans = detect_financial_page_spans(pdf_path)
    first_prompt = build_openai_financial_first_pass_prompt(
        title=resolved_title,
        target_year=target_year,
        section_spans=spans,
    )

    pages: list[int] = []
    for section in ("bilan_actif", "bilan_passif", "compte_resultat"):
        for page_num in spans.get(section, {}).get("pages", []):
            if page_num not in pages:
                pages.append(page_num)

    images = _render_pages(pdf_path, pages)

    openai_client = get_llm_client(
        model_override={"openai": FIRST_PASS_OPENAI_MODEL},
        preferred_provider="openai",
    )
    first_raw = call_llm_with_retry(openai_client["multimodal_call"], first_prompt, images)
    first_parsed = parse_json_response(first_raw)

    first_payload = {
        "pdf": str(pdf_path),
        "pages": pages,
        "provider": openai_client["provider"],
        "model": openai_client["model"],
        "prompt": first_prompt,
        "raw": first_raw,
        "parsed": first_parsed,
    }
    if first_pass_output_json:
        first_pass_output_json.write_text(
            json.dumps(first_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    feature_keys = load_feature_keys(revealing_json_path)
    second_prompt = build_mapping_prompt(first_parsed, feature_keys)
    second_client = get_llm_client(
        model_override={"openai": SECOND_PASS_OPENAI_MODEL},
        preferred_provider="openai",
    )
    second_raw = call_llm_with_retry(second_client["text_call"], second_prompt)
    second_mapping = parse_json_response(second_raw)
    second_parsed = resolve_mapping(second_mapping, first_parsed, feature_keys)

    second_payload = {
        "pdf": str(pdf_path),
        "feature_keys": feature_keys,
        "provider": second_client["provider"],
        "model": second_client["model"],
        "prompt": second_prompt,
        "raw": second_raw,
        "mapping": second_mapping,
        "parsed": second_parsed,
    }
    if second_pass_output_json:
        second_pass_output_json.write_text(
            json.dumps(second_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    if second_pass_output_excel:
        exercise = first_parsed.get("exercise") or {}
        write_revealing_excel(
            second_pass_output_excel,
            second_parsed,
            title=resolved_title,
            date_cloture=exercise.get("date_cloture"),
            date_cloture_n1=exercise.get("date_cloture_n1"),
            type_comptes=exercise.get("type_comptes"),
        )

    return {
        "first_pass": first_payload,
        "second_pass": second_payload,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline PDF financier ciblé")
    parser.add_argument("--pdf", required=True, help="Chemin du PDF financier")
    parser.add_argument("--revealing-json", required=True, help="JSON révélateur de référence")
    parser.add_argument("--title", default=None, help="Titre logique")
    parser.add_argument("--year", default=None, help="Année cible")
    parser.add_argument("--first-output-json", default=None, help="Sortie JSON 1er pass")
    parser.add_argument("--second-output-json", default=None, help="Sortie JSON 2e pass")
    parser.add_argument("--second-output-excel", default=None, help="Sortie Excel 2e pass")
    args = parser.parse_args()

    payload = run(
        pdf_path=Path(args.pdf),
        revealing_json_path=Path(args.revealing_json),
        title=args.title,
        target_year=args.year,
        first_pass_output_json=Path(args.first_output_json).resolve() if args.first_output_json else None,
        second_pass_output_json=Path(args.second_output_json).resolve() if args.second_output_json else None,
        second_pass_output_excel=Path(args.second_output_excel).resolve() if args.second_output_excel else None,
    )
    print(json.dumps(payload["second_pass"]["parsed"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
