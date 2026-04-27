from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import fitz

try:
    import pymupdf4llm as _pymupdf4llm
except ImportError:
    _pymupdf4llm = None

from core.normalization import canonical_name

TITLE_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("bilan_actif", re.compile(r"^\s*bilan\s+actif\s*$", re.IGNORECASE)),
    ("bilan_passif", re.compile(r"^\s*bilan\s+passif\s*$", re.IGNORECASE)),
    ("compte_resultat", re.compile(r"^\s*compte\s+de\s+r[ée]sultat\s*$", re.IGNORECASE)),
]

CONTENT_SIGNATURES: Dict[str, List[re.Pattern]] = {
    "bilan_actif": [
        re.compile(r"actif\s+immobilis", re.IGNORECASE),
        re.compile(r"actif\s+circulant", re.IGNORECASE),
    ],
    "bilan_passif": [
        re.compile(r"capital\s+social", re.IGNORECASE),
        re.compile(r"report\s+[àa]\s+nouveau", re.IGNORECASE),
        re.compile(r"r[ée]sultat\s+de\s+l[' ]exercice", re.IGNORECASE),
    ],
    "compte_resultat": [
        re.compile(r"chiffre\s+d[' ]affaires", re.IGNORECASE),
        re.compile(r"charges\s+d[' ]exploitation", re.IGNORECASE),
    ],
}

TARGETS_BY_SECTION: Dict[str, Dict[str, List[str]]] = {
    "bilan_actif": {
        "immobilisations_corporelles": [r"immobilisations?\s+corporell", r"actif\s+immobilis"],
        "immobilisations_financieres": [r"immobilisations?\s+financi[eè]r"],
        "creances": [r"^cr[eé]ances", r"clients\s+et\s+comptes\s+rattach"],
        "tresorerie": [r"disponibilit", r"tr[eé]sorerie", r"valeurs\s+mobili[eè]res", r"\bvmp\b"],
        "autres_actif": [r"charges\s+constat[eé]es", r"stocks", r"incorporell"],
        "total_actif": [r"^total\s+actif\b", r"^total\s+g[eé]n[eé]ral\s+actif"],
    },
    "bilan_passif": {
        "capital_social": [r"capital\s+social", r"capital\s+ou\s+individuel"],
        "resultat": [r"r[eé]sultat\s+de\s+l[' ]?exercice", r"^r[eé]sultat\s+net"],
        "capitaux_propres": [r"capitaux\s+propres"],
        "dettes_financieres": [r"emprunts?\s+et\s+dettes", r"bancaires", r"comptes?\s+courants?"],
        "dettes_exploitation": [r"fournisseurs", r"fiscales", r"sociales"],
        "dettes_diverses": [r"autres\s+dettes", r"dettes?\s+diverses"],
        "autres_passif": [r"provisions", r"produits\s+constat[eé]s"],
        "total_passif": [r"^total\s+passif\b", r"^total\s+g[eé]n[eé]ral\s+passif"],
    },
    "compte_resultat": {
        "chiffre_affaires": [r"chiffre\s+d[' ]?affaires"],
        "charges_total": [r"charges\s+d[' ]?exploitation", r"total\s+des\s+charges"],
        "salaires_charges_sociales": [r"salaires", r"charges\s+sociales"],
        "impots_taxes": [r"imp[oô]ts?\s+et\s+taxes"],
        "dotations_amortissements": [r"dotations?\s+aux\s+amortissements", r"amortissements"],
        "resultat_financier": [r"r[eé]sultat\s+financier"],
        "resultat_exceptionnel": [r"r[eé]sultat\s+exceptionnel"],
        "impots_societes": [r"imp[oô]ts?\s+sur\s+les\s+b[eé]n[eé]fices", r"imp[oô]ts?\s+sur\s+les\s+soci[eé]t"],
    },
}

EXCLUDE_PATTERN = re.compile(r"d[ée]taill|\(suite\)", re.IGNORECASE)
_CDR_SUITE_PATTERN = re.compile(r"compte\s+de\s+r[eé]sultat\s*\(suite\)", re.IGNORECASE)
TOC_EXCLUDE_PATTERN = re.compile(
    r"sommaire|table\s+des\s+mati[eè]res|rapport\s+de\s+l[' ]expert"
    r"|detail\s+des\s+comptes|notes\s+sur\s+le\s+bilan|notes\s+sur\s+le\s+compte",
    re.IGNORECASE,
)
DATE_RE = re.compile(r"\b(\d{2})[/-](\d{2})[/-](\d{2,4})\b")
NUMBER_RE = re.compile(r"^\(?-?\d[\d\s\xa0]*([.,]\d+)?\)?$")


def _is_summary_page(text: str) -> bool:
    return bool(TOC_EXCLUDE_PATTERN.search(text[:1500]))


def detect_target_pages(doc: fitz.Document) -> Dict[str, int]:
    found: Dict[str, int] = {}

    for page_index, page in enumerate(doc):
        text = page.get_text("text")
        if not text:
            continue
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        header_zone = "\n".join(lines[:8])
        if EXCLUDE_PATTERN.search(header_zone) or _is_summary_page(text):
            continue
        for label, pattern in TITLE_PATTERNS:
            if label in found:
                continue
            if any(pattern.match(ln) for ln in lines[:8]):
                found[label] = page_index + 1
                break

    missing = [lbl for lbl in ("bilan_actif", "bilan_passif", "compte_resultat") if lbl not in found]
    if missing:
        for page_index, page in enumerate(doc):
            if not missing:
                break
            text = page.get_text("text")
            if not text:
                continue
            if (page_index + 1) in found.values():
                continue
            if EXCLUDE_PATTERN.search(text[:400]) or _is_summary_page(text):
                continue
            for label in list(missing):
                if all(p.search(text) for p in CONTENT_SIGNATURES[label]):
                    found[label] = page_index + 1
                    missing.remove(label)
                    break

    return found


def extract_tables_on_page(page: fitz.Page) -> List[List[List[str]]]:
    finder = page.find_tables()
    tables: List[List[List[str]]] = []
    for table in finder.tables:
        rows: List[List[str]] = []
        for table_row in table.rows:
            row_texts = []
            for cell_rect in table_row.cells:
                if cell_rect is None:
                    row_texts.append("")
                    continue
                text = page.get_text("text", clip=fitz.Rect(cell_rect)).strip()
                row_texts.append(_clean_cell(text))
            rows.append(row_texts)
        tables.append(rows)
    return tables


def _clean_cell(value: str) -> str:
    text = value.replace("\xa0", " ").replace("\r", "\n")
    lines = [" ".join(line.strip().split()) for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _row_has_number(row: List[str]) -> bool:
    return any(_parse_number(cell) is not None for cell in row)


def _table_target_hits(table: List[List[str]], section_label: str) -> int:
    hits = 0
    targets = TARGETS_BY_SECTION.get(section_label, {})
    for patterns in targets.values():
        matched = False
        for row in table:
            if not _row_has_number(row):
                continue
            label = " ".join(cell.replace("\n", " ").strip() for cell in row if cell).lower()
            if not label:
                continue
            if any(re.search(pattern, label, re.IGNORECASE) for pattern in patterns):
                matched = True
                break
        if matched:
            hits += 1
    return hits


def pick_main_table(
    tables: List[List[List[str]]],
    section_label: str,
) -> Optional[List[List[str]]]:
    if not tables:
        return None
    return max(
        tables,
        key=lambda table: (
            _table_target_hits(table, section_label),
            sum(1 for row in table for cell in row if _parse_number(cell) is not None),
            sum(1 for row in table for cell in row if cell),
        ),
    )


def _parse_number(value: str) -> Optional[float]:
    text = value.strip()
    if not text:
        return None
    text = text.replace("\n", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    if not NUMBER_RE.match(text):
        return None
    negative = text.startswith("(") and text.endswith(")")
    cleaned = text.replace("(", "").replace(")", "").replace(" ", "").replace(",", ".")
    try:
        number = float(cleaned)
    except ValueError:
        return None
    return -number if negative else number


def _looks_like_header(text: str) -> bool:
    canon = canonical_name(text)
    if not canon:
        return True
    if DATE_RE.search(text):
        return True
    header_tokens = {
        "bilan",
        "actif",
        "passif",
        "compte",
        "resultat",
        "france",
        "exportation",
        "total",
        "brut",
        "amort",
        "prov",
        "net",
        "periode",
        "du",
        "au",
    }
    tokens = set(canon.split())
    if "du" in tokens and "au" in tokens:
        return True
    return bool(tokens) and tokens.issubset(header_tokens)


def _collapse_pending_labels(pending_labels: List[str]) -> str:
    generic_labels = {
        "bilan actif",
        "bilan passif",
        "compte de resultat",
        "actif",
        "passif",
        "actif immobilise",
        "actif circulant",
        "capitaux propres",
        "emprunts et dettes",
        "produits d exploitation",
        "charges d exploitation",
        "produits financiers",
        "charges financieres",
    }
    tail = pending_labels[-3:]
    filtered = [label for label in tail if canonical_name(label) not in generic_labels]
    if not filtered:
        filtered = tail[-1:]
    if len(filtered) >= 2 and filtered[-1].startswith("-"):
        return f"{filtered[-2]} {filtered[-1]}".strip()
    return " ".join(filtered).strip()


def _pick_value_columns(rows: List[List[str]]) -> Tuple[int, int]:
    if not rows:
        return 1, 2
    n_cols = max(len(row) for row in rows)
    numeric_counts = []
    for col_idx in range(n_cols):
        count = sum(
            1
            for row in rows[1:]
            if col_idx < len(row) and _parse_number(row[col_idx]) is not None
        )
        numeric_counts.append((col_idx, count))

    numeric_cols = [col_idx for col_idx, count in numeric_counts if col_idx > 0 and count > 0]
    if len(numeric_cols) >= 2:
        return numeric_cols[-2], numeric_cols[-1]
    if len(numeric_cols) == 1:
        return numeric_cols[0], numeric_cols[0]
    return min(1, n_cols - 1), min(2, n_cols - 1)


def _extract_label(row: List[str], first_value_col: int) -> str:
    parts: List[str] = []
    for idx, cell in enumerate(row):
        if idx >= first_value_col:
            break
        if not cell:
            continue
        if _parse_number(cell) is not None:
            continue
        parts.append(cell.replace("\n", " ").strip())
    if not parts:
        for idx, cell in enumerate(row):
            if idx in (first_value_col, first_value_col + 1):
                continue
            if cell and _parse_number(cell) is None:
                parts.append(cell.replace("\n", " ").strip())
    return " ".join(part for part in parts if part).strip()


def _normalize_label(label: str) -> str:
    label = label.replace(" ,", ",").replace(" :", ":")
    label = re.sub(r"\s+", " ", label).strip(" |")
    return label


def _looks_like_label_fragment(label: str) -> bool:
    canon = canonical_name(label)
    if not canon:
        return False
    if _looks_like_header(label):
        return False
    return True


def _build_logical_rows(rows: List[List[str]]) -> List[Dict[str, object]]:
    if not rows:
        return []

    col_n, col_n1 = _pick_value_columns(rows)
    first_value_col = min(col_n, col_n1)

    logical_rows: List[Dict[str, object]] = []
    pending_labels: List[str] = []

    for row in rows:
        label = _normalize_label(_extract_label(row, first_value_col))
        amount_n = _parse_number(row[col_n]) if col_n < len(row) else None
        amount_n1 = _parse_number(row[col_n1]) if col_n1 < len(row) else None
        has_amount = amount_n is not None or amount_n1 is not None

        if not has_amount:
            if label and _looks_like_label_fragment(label):
                pending_labels.append(label)
            continue

        commentaires: List[str] = ["Extraction native find_tables"]
        if pending_labels:
            pending_label = _collapse_pending_labels(pending_labels)
            if label:
                if label.startswith("-") or label.lower().startswith("dont "):
                    label = " ".join(part for part in [pending_label, label] if part)
                    commentaires.append("Libelle reconstruit depuis lignes precedentes")
            else:
                label = pending_label
                commentaires.append("Libelle reconstruit depuis lignes precedentes")
            pending_labels = []

        if not label:
            continue

        logical_rows.append(
            {
                "poste": _normalize_label(label),
                "n": _to_json_number(amount_n),
                "n1": _to_json_number(amount_n1),
                "commentaires": " | ".join(commentaires),
            }
        )

    return _dedupe_rows(logical_rows)


def _to_json_number(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    if abs(value - round(value)) < 1e-9:
        return float(int(round(value)))
    return round(value, 2)


def _dedupe_rows(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    deduped: List[Dict[str, object]] = []
    seen = set()
    for row in rows:
        key = (row.get("poste"), row.get("n"), row.get("n1"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _extract_dates_from_text(text: str) -> List[datetime]:
    dates: List[datetime] = []
    for day, month, year in DATE_RE.findall(text):
        year_int = int(year)
        if year_int < 100:
            year_int += 2000 if year_int < 70 else 1900
        try:
            dates.append(datetime(year_int, int(month), int(day)))
        except ValueError:
            continue
    return dates


def _extract_statement_dates(doc: fitz.Document, pages: Dict[str, int]) -> Tuple[Optional[str], Optional[str]]:
    found_dates: List[datetime] = []
    seen = set()
    page_numbers = sorted(set(pages.values()))
    for page_num in page_numbers:
        text = doc[page_num - 1].get_text("text")
        preferred_lines = [
            line for line in text.splitlines()
            if re.search(r"\bau\b|clos le|net au|au 31|au 30|au 29|au 28", line, re.IGNORECASE)
        ]
        candidate_texts = preferred_lines or text.splitlines()
        for line in candidate_texts:
            dates_in_line = _extract_dates_from_text(line)
            if re.search(r"\bdu\b.*\bau\b", line, re.IGNORECASE) and dates_in_line:
                dates_in_line = [dates_in_line[-1]]
            for dt in dates_in_line:
                key = dt.strftime("%Y-%m-%d")
                if key in seen:
                    continue
                seen.add(key)
                found_dates.append(dt)

    found_dates.sort(reverse=True)
    if not found_dates:
        return None, None
    n = found_dates[0].strftime("%d/%m/%Y")
    n1 = found_dates[1].strftime("%d/%m/%Y") if len(found_dates) > 1 else None
    return n, n1


def _extract_company_name(doc: fitz.Document, pages: Dict[str, int]) -> Optional[str]:
    patterns = [
        re.compile(
            r"(?:d[ée]nomination|raison sociale|d[eé]signation de l[' ]entreprise)\s*:?\s*(.+)",
            re.IGNORECASE,
        ),
    ]
    scan_pages = [0] + [page_num - 1 for page_num in sorted(set(pages.values()))[:2]]
    for page_index in scan_pages:
        if page_index < 0 or page_index >= len(doc):
            continue
        text = doc[page_index].get_text("text")
        for line in text.splitlines():
            candidate = " ".join(line.strip().split())
            if not candidate:
                continue
            for pattern in patterns:
                match = pattern.search(candidate)
                if match:
                    value = match.group(1).strip(" :-")
                    if len(value) >= 3:
                        return value
    return None


def extract_financial_data(pdf_path: Path) -> Dict[str, object]:
    result: Dict[str, object] = {
        "_native_source": "pymupdf_find_tables",
        "_native_pdf_path": str(pdf_path),
        "_native_available": False,
    }
    if not pdf_path.exists():
        result["_native_error"] = "pdf introuvable"
        return result

    doc = fitz.open(str(pdf_path))
    try:
        pages = detect_target_pages(doc)
        result["pages"] = pages
        result["bilan_societe_nom"] = _extract_company_name(doc, pages)
        n, n1 = _extract_statement_dates(doc, pages)
        result["bilan_date_arrete_n"] = n
        result["bilan_date_arrete_n1"] = n1

        for section_label, field_id in (
            ("bilan_actif", "bilan_actif_table"),
            ("bilan_passif", "bilan_passif_table"),
            ("compte_resultat", "bilan_compte_resultat_table"),
        ):
            page_num = pages.get(section_label)
            if not page_num:
                continue
            tables = extract_tables_on_page(doc[page_num - 1])
            main_table = pick_main_table(tables, section_label)
            normalized_rows = _build_logical_rows(main_table or [])

            extra_pages: List[int] = []
            if section_label == "compte_resultat":
                for candidate in range(page_num, min(page_num + 3, len(doc))):
                    page_text = doc[candidate].get_text("text")
                    if _CDR_SUITE_PATTERN.search(page_text[:600]):
                        suite_tables = extract_tables_on_page(doc[candidate])
                        suite_main = pick_main_table(suite_tables, section_label)
                        suite_rows = _build_logical_rows(suite_main or [])
                        normalized_rows = _dedupe_rows(normalized_rows + suite_rows)
                        extra_pages.append(candidate + 1)

            result[field_id] = normalized_rows
            result[f"{field_id}__meta"] = {
                "page": page_num,
                "extra_pages": extra_pages,
                "tables_detected": len(tables),
                "rows_normalized": len(normalized_rows),
            }

            if section_label == "compte_resultat" and _pymupdf4llm is not None:
                cdr_pages = [page_num - 1] + [p - 1 for p in extra_pages]
                try:
                    md = _pymupdf4llm.to_markdown(str(pdf_path), pages=cdr_pages)
                    result["bilan_compte_resultat_markdown"] = md
                except Exception:
                    pass

        result["_native_available"] = True
        return result
    finally:
        doc.close()


def render_financial_context(native_data: Dict[str, object], requested_field_ids: List[str]) -> str:
    if not native_data.get("_native_available"):
        return ""

    payload: Dict[str, object] = {
        "source": native_data.get("_native_source"),
        "pages": native_data.get("pages", {}),
    }
    for field_id in requested_field_ids:
        if field_id in native_data and native_data.get(field_id) is not None:
            payload[field_id] = native_data.get(field_id)

    if not payload:
        return ""

    parts = [
        "## EXTRACTION NATIVE PRE-NETTOYEE (prioritaire si coherente)\n"
        "Cette structure vient de PyMuPDF find_tables() avec un pre-nettoyage deterministe :\n"
        "- choix de page\n"
        "- choix de table\n"
        "- normalisation N / N-1\n"
        "- reconstruction partielle des libelles casses\n\n"
        f"```json\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n```\n"
        "Si cette extraction native parait coherente, utilise-la comme source primaire.\n"
        "Reviens au texte brut du document seulement pour lever une ambiguite ou completer une ligne manquante.\n"
    ]

    needs_cdr = any(fid == "bilan_compte_resultat_table" for fid in requested_field_ids)
    cdr_md = native_data.get("bilan_compte_resultat_markdown")
    if needs_cdr and cdr_md:
        parts.append(
            "\n## TABLEAU COMPTE DE RESULTAT (markdown brut, structure de colonnes préservée)\n"
            "Utilise ce tableau pour identifier correctement les colonnes N et N-1 du CDR.\n"
            "Les colonnes 'France' et 'Exportation' sont des détails ; utilise uniquement la colonne 'Total'.\n\n"
            f"{cdr_md}\n"
        )

    return "".join(parts)
