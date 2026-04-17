"""Helpers documentaires pour extract_structured.py."""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional

from normalization import canonical_name, canonical_stem, path_has_segments

LOCAL_CACHE = Path(__file__).parent.parent.resolve() / "cache"
MIN_PARENT_CHARS = 50
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2}|21\d{2})\b")
POSTAL_CODE_RE = re.compile(r"\b\d{5}\b")
ADDRESS_STREET_HINTS = {
    "rue",
    "avenue",
    "av",
    "boulevard",
    "bd",
    "chemin",
    "allee",
    "all",
    "impasse",
    "route",
    "cours",
    "place",
    "quai",
    "faubourg",
}


def match_questions_to_doc(
    doc_info: Dict,
    fields: List[Dict],
    selected_audit_folder: Optional[str] = None,
) -> List[Dict]:
    filename = doc_info.get("filename", "")
    source_path = doc_info.get("source_path", "")
    haystack_canon = f"{canonical_stem(filename)} {canonical_name(source_path)}"

    matched: List[Dict] = []
    for field in fields:
        dirs = field.get("source_dirs")
        if dirs:
            resolved_dirs: List[str] = []
            for directory in dirs:
                if "{selected_audit_folder}" in directory:
                    if selected_audit_folder:
                        resolved_dirs.append(
                            directory.replace("{selected_audit_folder}", selected_audit_folder)
                        )
                else:
                    resolved_dirs.append(directory)
            if resolved_dirs and not any(
                path_has_segments(source_path, directory) for directory in resolved_dirs
            ):
                continue

        keywords = field.get("hint_keywords", [])
        if any(canonical_name(keyword) in haystack_canon for keyword in keywords):
            matched.append(field)

    return matched


def is_financial_field(field: Dict) -> bool:
    field_id = str(field.get("field_id", ""))
    if field.get("excel_sheet") == "{company_name}":
        return True
    return field_id.startswith("bilan_")


def needs_broad_financial_context(questions: List[Dict]) -> bool:
    return any(is_financial_field(question) for question in questions)


def load_filtered_text(
    doc_path: Path,
    questions: List[Dict],
    max_chars: int,
    preserve_order: bool = False,
    neighbor_parents: int = 0,
    append_unmatched_tail: bool = True,
    max_relative_page_window: Optional[int] = None,
) -> str:
    keywords: set[str] = set()
    for question in questions:
        for keyword in question.get("hint_keywords", []):
            canon_kw = canonical_name(keyword)
            if canon_kw:
                keywords.add(canon_kw)
        for keyword in question.get("source_doc_name_variants", []):
            canon_kw = canonical_name(keyword)
            if canon_kw:
                keywords.add(canon_kw)
        for keyword in _extra_keywords_for_field(question):
            canon_kw = canonical_name(keyword)
            if canon_kw:
                keywords.add(canon_kw)

    parents: List[Dict] = []
    with open(doc_path, "r", encoding="utf-8") as handle:
        for line in handle:
            parents.append(json.loads(line))

    scored = []
    for index, parent in enumerate(parents):
        text = parent.get("text", "")
        if len(text) < MIN_PARENT_CHARS:
            continue
        haystack = canonical_name(
            f"{parent.get('section_title', '')} {parent.get('source_path', '')} {text}"
        )
        hits = sum(1 for keyword in keywords if keyword in haystack)
        if hits > 0:
            scored.append((hits, index, parent))

    scored.sort(key=lambda item: item[0], reverse=True)

    texts: List[str] = []
    total = 0
    if preserve_order and scored:
        selected_indices: set[int] = set()
        indexed_parents = {index: parent for _, index, parent in scored}
        for _, index, _parent in scored:
            for candidate_index in range(
                max(0, index - neighbor_parents),
                min(len(parents), index + neighbor_parents + 1),
            ):
                parent = parents[candidate_index]
                if len(parent.get("text", "")) < MIN_PARENT_CHARS:
                    continue
                selected_indices.add(candidate_index)

        if max_relative_page_window is not None and selected_indices:
            matched_pages = [
                _coerce_page_number(parents[index].get("page_start")) for index in selected_indices
            ]
            matched_pages = [page for page in matched_pages if page is not None]
            if matched_pages:
                max_allowed_page = min(matched_pages) + max_relative_page_window
                selected_indices = {
                    index
                    for index in selected_indices
                    if (
                        _coerce_page_number(parents[index].get("page_start")) is None
                        or _coerce_page_number(parents[index].get("page_start")) <= max_allowed_page
                    )
                }

        for index in sorted(selected_indices):
            block = _format_parent(indexed_parents.get(index, parents[index]))
            if total + len(block) > max_chars:
                remaining = max_chars - total
                if remaining > 200:
                    texts.append(block[:remaining] + "\n[…]")
                break
            texts.append(block)
            total += len(block)

        if append_unmatched_tail and total < max_chars:
            for index, parent in enumerate(parents):
                if index in selected_indices or len(parent.get("text", "")) < MIN_PARENT_CHARS:
                    continue
                block = _format_parent(parent)
                if total + len(block) > max_chars:
                    break
                texts.append(block)
                total += len(block)
    else:
        for _, _, parent in scored:
            block = _format_parent(parent)
            if total + len(block) > max_chars:
                remaining = max_chars - total
                if remaining > 200:
                    texts.append(block[:remaining] + "\n[…]")
                break
            texts.append(block)
            total += len(block)

    if not texts:
        for parent in parents:
            if len(parent.get("text", "")) < MIN_PARENT_CHARS:
                continue
            block = _format_parent(parent)
            if total + len(block) > max_chars:
                break
            texts.append(block)
            total += len(block)

    return "\n\n---\n\n".join(texts)


def get_doc_financial_year(doc_info: Dict) -> Optional[int]:
    filename = doc_info.get("filename", "")
    years = _extract_years_from_text(filename)
    if years:
        return years[0]

    source_path = doc_info.get("source_path", "")
    years = _extract_years_from_text(source_path)
    return years[0] if years else None


def extract_company_folder_from_source_path(source_path: str) -> Optional[str]:
    parts = [part for part in source_path.replace("\\", "/").split("/") if part]
    if len(parts) >= 2:
        return parts[-2]
    return None


def build_latest_year_per_company_folder(
    manifest_files: List[Dict],
    company_fields: List[Dict],
    selected_audit_folder: Optional[str] = None,
) -> Dict[str, int]:
    folder_years: Dict[str, int] = {}
    for doc_info in manifest_files:
        matched_company = match_questions_to_doc(doc_info, company_fields, selected_audit_folder)
        if not matched_company:
            continue
        year = get_doc_financial_year(doc_info)
        if year is None:
            continue
        folder = extract_company_folder_from_source_path(doc_info.get("source_path", ""))
        if not folder:
            folder = Path(doc_info.get("filename", "unknown")).stem
        folder_key = canonical_name(folder)
        if folder_key not in folder_years or year > folder_years[folder_key]:
            folder_years[folder_key] = year
    return folder_years


def resolve_project_location(candidates: List[Dict]) -> Optional[str]:
    precise_candidates = [
        candidate
        for candidate in candidates
        if _looks_like_precise_project_address(candidate.get("value"))
    ]
    if len(precise_candidates) < 2:
        return None

    best_cluster: List[Dict] = []
    best_doc_count = 0
    best_value_len = 0

    for candidate in precise_candidates:
        cluster = [
            other
            for other in precise_candidates
            if _same_project_address(candidate.get("value", ""), other.get("value", ""))
        ]
        unique_docs = {item.get("document_id") for item in cluster if item.get("document_id")}
        doc_count = len(unique_docs)
        if doc_count < 2:
            continue
        value_len = max((len(item.get("value", "")) for item in cluster), default=0)
        if doc_count > best_doc_count or (doc_count == best_doc_count and value_len > best_value_len):
            best_cluster = cluster
            best_doc_count = doc_count
            best_value_len = value_len

    if not best_cluster:
        return None

    return max(
        (item.get("value", "").strip() for item in best_cluster),
        key=len,
        default=None,
    ) or None


def resolve_cached_source_file(manifest: Dict, source_path: str, filename: str) -> Optional[Path]:
    project_path = str(manifest.get("project_path") or "").strip("/")
    if project_path:
        direct = LOCAL_CACHE / project_path / source_path
        if direct.exists():
            return direct

    project_root = LOCAL_CACHE / project_path if project_path else LOCAL_CACHE
    if project_root.exists():
        candidates = list(project_root.rglob(filename))
        if len(candidates) == 1:
            return candidates[0]
        for candidate in candidates:
            if canonical_name(str(candidate.relative_to(project_root))) == canonical_name(source_path):
                return candidate

    return None


def _format_parent(parent: Dict) -> str:
    title = parent.get("section_title", "")
    text = parent.get("text", "")
    if title and not text.startswith(f"## {title}"):
        return f"## {title}\n{text}"
    return text


def _coerce_page_number(value) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.isdigit():
            return int(cleaned)
    return None


def _extra_keywords_for_field(field: Dict) -> List[str]:
    field_id = str(field.get("field_id", ""))
    if field_id == "bilan_actif_table":
        return [
            "bilan actif",
            "actif",
            "immobilisations",
            "immobilisations corporelles",
            "immobilisations financieres",
            "creances",
            "clients",
            "autres creances",
            "disponibilites",
            "tresorerie",
            "vmp",
            "stocks",
            "stock",
            "en cours",
            "marchandises",
            "production en cours",
            "charges constatees d avance",
            "total actif",
            "autres actif",
        ]
    if field_id == "bilan_passif_table":
        return [
            "bilan passif",
            "passif",
            "capitaux propres",
            "capital social",
            "resultat",
            "dettes",
            "dettes financieres",
            "dettes exploitation",
            "comptes courants",
            "dettes bancaires",
            "emprunts",
            "fournisseurs",
            "dettes fiscales",
            "dettes sociales",
            "autres dettes",
            "provisions",
            "provisions pour risques",
            "provisions pour charges",
            "produits constates d avance",
            "total passif",
            "autres passif",
        ]
    if field_id == "bilan_compte_resultat_table":
        return [
            "compte de resultat",
            "resultat",
            "produits",
            "chiffre d affaires",
            "charges",
            "achats de marchandises",
            "variation de stock",
            "autres charges externes",
            "salaires",
            "charges sociales",
            "impots",
            "taxes",
            "dotations",
            "dotations aux amortissements",
            "dotations aux provisions",
            "production stockee",
            "subventions d exploitation",
            "reprises sur amortissements",
            "autres charges",
            "autres produits",
            "produits d exploitation",
            "charges d exploitation",
            "subventions",
            "resultat financier",
            "resultat exceptionnel",
        ]
    if field_id == "bilan_societe_nom":
        return ["designation de l entreprise", "denomination", "societe", "raison sociale"]
    if field_id.startswith("bilan_date_arrete"):
        return [
            "date de cloture",
            "exercice n clos le",
            "exercice n 1 clos le",
            "date arrete",
            "31 12",
        ]
    return []


def _extract_years_from_text(text: str) -> List[int]:
    years = {int(match.group(1)) for match in YEAR_RE.finditer(text or "")}
    return sorted(years, reverse=True)


def _looks_like_precise_project_address(value: Optional[str]) -> bool:
    text = (value or "").strip()
    if not text:
        return False

    normalized = canonical_name(text)
    if not normalized:
        return False

    tokens = set(normalized.split())
    has_street_hint = any(token in tokens for token in ADDRESS_STREET_HINTS)
    has_number = re.search(r"\b\d+[a-zA-Z]?\b", text) is not None
    has_postal_code = POSTAL_CODE_RE.search(text) is not None
    return has_street_hint and (has_number or has_postal_code)


def _same_project_address(candidate_a: str, candidate_b: str) -> bool:
    norm_a = canonical_name(candidate_a)
    norm_b = canonical_name(candidate_b)
    if not norm_a or not norm_b:
        return False
    return norm_a == norm_b or norm_a in norm_b or norm_b in norm_a
