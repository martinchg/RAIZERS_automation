"""
patrimoine_tables_native.py : Extraction pré-LLM du tableau patrimoine immobilier
via PyMuPDF find_tables(). Calqué sur financial_tables_native.py.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Optional

import fitz

from core.normalization import canonical_name

# ---------------------------------------------------------------------------
# Détection de page
# ---------------------------------------------------------------------------

_TITLE_PATTERNS = [
    re.compile(r"patrimoine\s+immobilier", re.IGNORECASE),
    re.compile(r"situation\s+patrimoniale", re.IGNORECASE),
    re.compile(r"patrimoine\s+immobili", re.IGNORECASE),
]

_CONTENT_SIGNATURES = [
    re.compile(r"estimation\s+actuelle", re.IGNORECASE),
    re.compile(r"capital\s+restant\s+d[uû]", re.IGNORECASE),
    re.compile(r"type\s+de\s+d[eé]tention", re.IGNORECASE),
]

# ---------------------------------------------------------------------------
# Mapping header brut → clé canonique
# ---------------------------------------------------------------------------

_HEADER_KEY_MAP: Dict[str, str] = {
    # type de bien
    "typedebien": "type_bien",
    "typebien": "type_bien",
    "nature": "type_bien",
    "naturebien": "type_bien",
    "typedebienimmobilier": "type_bien",
    # adresse
    "adresse": "adresse",
    "adresseetdestination": "adresse",
    "adressedestination": "adresse",
    "localisation": "adresse",
    "localisationetdestination": "adresse",
    # surface habitable
    "surface": "surface",
    "surfacehabitable": "surface",
    "superficie": "surface",
    "superficiehabitable": "surface",
    "surfacem2": "surface",
    # type de détention
    "typededetention": "type_de_detention",
    "typedetention": "type_de_detention",
    "modededetention": "type_de_detention",
    "modedetention": "type_de_detention",
    "formejuridique": "type_de_detention",
    # % de détention
    "dedetention": "pct_detention",
    "pctdetention": "pct_detention",
    "pourcentagededetention": "pct_detention",
    "quotepartdetenue": "pct_detention",
    "tauxdetention": "pct_detention",
    # valeur d'acquisition
    "valeuracquisition": "valeur_acquisition",
    "dacquisition": "valeur_acquisition",
    "prixacquisition": "valeur_acquisition",
    "coutacquisition": "valeur_acquisition",
    "prixdacquisition": "valeur_acquisition",
    "valeurdacquisition": "valeur_acquisition",
    # estimation actuelle
    "estimationactuelle": "valeur_bien",
    "valeuractuelle": "valeur_bien",
    "valeurvenale": "valeur_bien",
    "valeurmarche": "valeur_bien",
    "valeurestimee": "valeur_bien",
    "valeurdubien": "valeur_bien",
    "estimation": "valeur_bien",
    # capital restant dû
    "capitalrestantdu": "valeur_banque",
    "capitalrestant": "valeur_banque",
    "encoursducredit": "valeur_banque",
    "encourscredit": "valeur_banque",
    "encourspret": "valeur_banque",
    "capitaldu": "valeur_banque",
    "restantdu": "valeur_banque",
    # garanties données
    "garantiesdonnees": "garanties_donnees",
    "garanties": "garanties_donnees",
    "typedegarantie": "garanties_donnees",
    "typegarantie": "garanties_donnees",
    "surete": "garanties_donnees",
    # revenus locatifs
    "revenuslocatifs": "revenus_locatifs",
    "revenusfonciers": "revenus_locatifs",
    "loyers": "revenus_locatifs",
    "loyersannuels": "revenus_locatifs",
    "revenusannuels": "revenus_locatifs",
}

_NUMERIC_KEYS = {"surface", "pct_detention", "valeur_acquisition", "valeur_bien", "valeur_banque", "revenus_locatifs"}

_MIN_HEADER_HITS = 3


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_header(text: str) -> str:
    normed = canonical_name(text)
    # garde uniquement les lettres minuscules (retire %, ', espaces, (1)…)
    normed = re.sub(r"[^a-z]", "", normed)
    return normed


def _clean_cell(value: str) -> str:
    text = value.replace("\xa0", " ").replace("\r", "\n")
    lines = [" ".join(line.strip().split()) for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _parse_number(value: str) -> Optional[float]:
    text = value.strip().replace("\xa0", " ").replace(" ", "").replace(",", ".")
    text = re.sub(r"[€$£%]", "", text)
    negative = text.startswith("(") and text.endswith(")")
    text = text.replace("(", "").replace(")", "")
    try:
        return -float(text) if negative else float(text)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Détection de page
# ---------------------------------------------------------------------------

_MAX_PAGES_SCAN = 20  # une attestation patrimoniale tient en moins de 20 pages


def detect_patrimoine_page(doc: fitz.Document) -> Optional[int]:
    """Retourne le numéro de page (1-based) contenant le tableau patrimoine."""
    page_limit = min(len(doc), _MAX_PAGES_SCAN)

    # Lecture unique de toutes les pages dans la limite
    page_texts = {}
    for page_index in range(page_limit):
        t = doc[page_index].get_text("text")
        if t:
            page_texts[page_index] = t

    # Passe 1 : titre explicite dans les premières lignes
    for page_index, text in page_texts.items():
        header_zone = "\n".join(text.splitlines()[:15])
        if any(p.search(header_zone) for p in _TITLE_PATTERNS):
            return page_index + 1

    # Passe 2 : signatures de contenu (fallback, seulement si doc court)
    if len(doc) > _MAX_PAGES_SCAN:
        return None
    for page_index, text in page_texts.items():
        if sum(1 for sig in _CONTENT_SIGNATURES if sig.search(text)) >= 2:
            return page_index + 1

    return None


# ---------------------------------------------------------------------------
# Sélection et parsing de la table
# ---------------------------------------------------------------------------

def _score_table(table: List[List[str]]) -> int:
    """Nombre de colonnes d'en-tête reconnues dans les 3 premières lignes."""
    for row in table[:3]:
        hits = sum(1 for cell in row if _normalize_header(cell or "") in _HEADER_KEY_MAP)
        if hits >= _MIN_HEADER_HITS:
            return hits
    return 0


def pick_patrimoine_table(tables: List[List[List[str]]]) -> Optional[List[List[str]]]:
    if not tables:
        return None
    best = max(tables, key=_score_table)
    return best if _score_table(best) >= _MIN_HEADER_HITS else None


def _find_header_row_idx(table: List[List[str]]) -> Optional[int]:
    for row_idx, row in enumerate(table[:5]):
        hits = sum(1 for cell in row if _normalize_header(cell or "") in _HEADER_KEY_MAP)
        if hits >= _MIN_HEADER_HITS:
            return row_idx
    return None


def _map_columns(header_row: List[str]) -> Dict[int, str]:
    col_map: Dict[int, str] = {}
    for col_idx, cell in enumerate(header_row):
        key = _HEADER_KEY_MAP.get(_normalize_header(cell or ""))
        if key and key not in col_map.values():
            col_map[col_idx] = key
    return col_map


def _build_patrimoine_rows(table: List[List[str]]) -> List[Dict[str, object]]:
    header_idx = _find_header_row_idx(table)
    if header_idx is None:
        return []

    col_map = _map_columns(table[header_idx])
    if not col_map:
        return []

    rows: List[Dict[str, object]] = []
    for row in table[header_idx + 1:]:
        if not any((cell or "").strip() for cell in row):
            continue

        record: Dict[str, object] = {}
        for col_idx, key in col_map.items():
            if col_idx >= len(row):
                continue
            cell = (row[col_idx] or "").replace("\n", " ").strip()
            if not cell:
                record[key] = None
                continue
            if key in _NUMERIC_KEYS:
                num = _parse_number(cell)
                record[key] = num if num is not None else cell
            else:
                record[key] = cell

        # Ignore les lignes sans données textuelles (ex. ligne total)
        text_values = [v for k, v in record.items() if k not in _NUMERIC_KEYS and v]
        if not text_values:
            continue

        rows.append(record)

    return rows


# ---------------------------------------------------------------------------
# Point d'entrée principal
# ---------------------------------------------------------------------------

def extract_patrimoine_data(pdf_path: Path) -> Dict[str, object]:
    result: Dict[str, object] = {
        "_native_source": "pymupdf_find_tables_patrimoine",
        "_native_pdf_path": str(pdf_path),
        "_native_available": False,
    }
    if not pdf_path.exists():
        result["_native_error"] = "pdf introuvable"
        return result

    try:
        doc = fitz.open(str(pdf_path))
    except Exception as exc:
        result["_native_error"] = f"ouverture impossible: {exc}"
        return result

    try:
        page_num = detect_patrimoine_page(doc)
        result["pages"] = {"patrimoine_immobilier": page_num}

        if not page_num:
            result["_native_error"] = "page patrimoine non trouvée"
            return result

        page = doc[page_num - 1]
        finder = page.find_tables()
        tables = [
            [[_clean_cell(cell or "") for cell in row] for row in tbl.extract()]
            for tbl in finder.tables
        ]

        main_table = pick_patrimoine_table(tables)
        if not main_table:
            result["_native_error"] = "tableau patrimoine non identifié sur la page"
            result["tables_detected"] = len(tables)
            return result

        rows = _build_patrimoine_rows(main_table)
        result["patrimoine_immobilier_table"] = rows
        result["_meta"] = {
            "page": page_num,
            "tables_detected": len(tables),
            "rows_extracted": len(rows),
        }
        result["_native_available"] = bool(rows)
        return result
    finally:
        doc.close()


# ---------------------------------------------------------------------------
# Rendu contexte LLM
# ---------------------------------------------------------------------------

def render_patrimoine_context(native_data: Dict[str, object]) -> str:
    if not native_data.get("_native_available"):
        return ""

    rows = native_data.get("patrimoine_immobilier_table") or []
    if not rows:
        return ""

    payload = {
        "source": native_data.get("_native_source"),
        "pages": native_data.get("pages", {}),
        "patrimoine_immobilier_table": rows,
    }

    return (
        "\n## EXTRACTION NATIVE PATRIMOINE IMMOBILIER (prioritaire si cohérente)\n"
        "Cette structure vient de PyMuPDF find_tables() avec pré-nettoyage déterministe.\n\n"
        f"```json\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n```\n"
        "Si cette extraction paraît cohérente, utilise-la comme source primaire "
        "et retourne les données avec les clés canoniques demandées.\n"
    )
