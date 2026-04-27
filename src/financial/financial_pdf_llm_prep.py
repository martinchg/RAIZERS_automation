"""Préparation d'un 1er appel LLM multimodal sur PDF financier.

Objectif :
- éviter de retraiter une année déjà couverte par Pappers
- cibler uniquement les pages financières utiles d'un PDF
- construire un prompt OpenAI de 1er passage qui renvoie une structure
  proche des JSON `entreprise_comptes_*`
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import fitz

from financial.financial_tables_native import (
    CONTENT_SIGNATURES,
    TOC_EXCLUDE_PATTERN,
    detect_target_pages,
    extract_tables_on_page,
    pick_main_table,
)


SECTION_ORDER = ("bilan_actif", "bilan_passif", "compte_resultat")
SECTION_TO_OUTPUT_KEY = {
    "bilan_actif": "actif",
    "bilan_passif": "passif",
    "compte_resultat": "compte_resultat",
}



def _page_text(doc: fitz.Document, page_num: int) -> str:
    return doc[page_num - 1].get_text("text")


def _looks_excluded_page(text: str) -> bool:
    header = text[:1500]
    # On exclut les sommaires et pages manifestement hors états financiers,
    # mais pas les pages "(suite)" qui prolongent souvent un bilan ou un CDR.
    return bool(TOC_EXCLUDE_PATTERN.search(header))


def _section_score(doc: fitz.Document, page_num: int, section_label: str) -> int:
    text = _page_text(doc, page_num)
    if not text or _looks_excluded_page(text):
        return -10

    score = 0
    for pattern in CONTENT_SIGNATURES.get(section_label, []):
        if pattern.search(text):
            score += 2

    try:
        tables = extract_tables_on_page(doc[page_num - 1])
    except Exception:
        tables = []

    if tables:
        main_table = pick_main_table(tables, section_label)
        if main_table:
            score += 3
            score += min(len(main_table), 12) // 4

    numeric_lines = sum(1 for line in text.splitlines() if re.search(r"\d[\d\s.,()]{2,}", line))
    if numeric_lines >= 8:
        score += 1
    if numeric_lines >= 18:
        score += 1

    return score


def detect_financial_page_spans(pdf_path: Path, max_following_pages: int = 1) -> Dict[str, Dict[str, Any]]:
    doc = fitz.open(str(pdf_path))
    try:
        starts = detect_target_pages(doc)
        spans: Dict[str, Dict[str, Any]] = {}

        for idx, section_label in enumerate(SECTION_ORDER):
            start = starts.get(section_label)
            if not start:
                continue

            next_starts = [
                starts.get(other)
                for other in SECTION_ORDER[idx + 1:]
                if starts.get(other) is not None
            ]
            next_start = min(next_starts) if next_starts else None

            pages = [start]
            last_candidate = min(len(doc), start + max_following_pages)
            if next_start is not None:
                last_candidate = min(last_candidate, next_start - 1)

            for candidate in range(start + 1, last_candidate + 1):
                score = _section_score(doc, candidate, section_label)
                if score >= 2:
                    pages.append(candidate)
                else:
                    break

            spans[section_label] = {
                "start_page": start,
                "end_page": pages[-1],
                "pages": pages,
            }

        return spans
    finally:
        doc.close()


def build_openai_financial_first_pass_prompt(
    *,
    title: str,
    target_year: Optional[str],
    section_spans: Dict[str, Dict[str, Any]],
) -> str:
    section_lines = []
    for section_label in SECTION_ORDER:
        span = section_spans.get(section_label)
        if not span:
            continue
        output_key = SECTION_TO_OUTPUT_KEY[section_label]
        pages = ", ".join(str(page) for page in span.get("pages", []))
        section_lines.append(f"- `{output_key}`: pages {pages}")

    sections_block = "\n".join(section_lines) or "- aucune section détectée automatiquement"
    target_year_line = (
        f"- L'exercice cible à extraire prioritairement est `{target_year}`."
        if target_year else
        "- Extrais l'exercice le plus récent visible dans les pages fournies."
    )

    return f"""Lis ces pages d'états financiers et extrais le plus exhaustivement possible les lignes chiffrées du tableau principal.

Pages ciblées :
{sections_block}

Règles :
- Retourne UNIQUEMENT un objet JSON valide.
- Aucune phrase hors JSON.
- Extrais presque toutes les lignes chiffrées lisibles du tableau principal.
- N'essaie pas de sélectionner seulement les lignes importantes.
- Si une ligne du tableau principal a une valeur `Net` lisible, retourne-la.
- Garde les lignes de détail, les sous-totaux et les totaux.
- Utilise UNIQUEMENT les colonnes `Net` pour `n` et `n1`.
- N'utilise jamais `Brut`, `Amortissements`, `Dépréciations` ou toute colonne intermédiaire pour `n` ou `n1`.
- Si plusieurs colonnes existent pour le compte de résultat (`France`, `Export`, `Total`), utilise `Total`.
- Si une valeur nette n'est pas clairement lisible, mets `null`.
- Préserve les libellés source.
- Si une section s'étale sur plusieurs pages fournies, fusionne simplement les lignes.

Exercice :
{target_year_line}

Format strict :
{{
  "title": "{title}",
  "exercise": {{
    "year": "2025" ou null,
    "date_cloture": "YYYY-MM-DD" ou "DD/MM/YYYY" ou null,
    "date_cloture_n1": "YYYY-MM-DD" ou "DD/MM/YYYY" ou null,
    "type_comptes": "Complets" | "Abrégés" | "Consolidés" | null
  }},
  "actif": [
    {{
      "c": "code visible ou null",
      "l": "libellé source",
      "n": 123,
      "n1": 100
    }}
  ],
  "passif": [
    {{
      "c": "code visible ou null",
      "l": "libellé source",
      "n": 123,
      "n1": 100
    }}
  ],
  "compte_resultat": [
    {{
      "c": "code visible ou null",
      "l": "libellé source",
      "n": 123,
      "n1": 100
    }}
  ]
}}

Contraintes :
- `actif`, `passif`, `compte_resultat` doivent toujours être présents.
- Si une section n'est pas visible dans les pages fournies, retourne `[]`.
- Chaque ligne doit contenir exactement `c`, `l`, `n`, `n1`.
- Les nombres doivent être des nombres JSON, pas des strings.
- Pas de champ additionnel.
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Affiche le prompt 1er pass LLM pour un PDF financier")
    parser.add_argument("--pdf", required=True, help="Chemin du PDF financier")
    parser.add_argument("--title", default=None, help="Titre logique de la sortie")
    parser.add_argument("--year", default=None, help="Année cible à extraire")
    args = parser.parse_args()

    pdf_path = Path(args.pdf).resolve()
    spans = detect_financial_page_spans(pdf_path)
    prompt = build_openai_financial_first_pass_prompt(
        title=args.title or pdf_path.stem,
        target_year=args.year,
        section_spans=spans,
    )
    print(prompt)


if __name__ == "__main__":
    main()
