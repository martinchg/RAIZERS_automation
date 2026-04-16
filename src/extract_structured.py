"""
extract_structured.py : Extraction structurée via LLM (Gemini / OpenAI).

Lit les documents JSONL + questions.json, envoie doc par doc au LLM,
récupère les réponses JSON, merge et remplit l'Excel.

Usage :
    python extract_structured.py --project raizers-en-audit-projet-1
    python extract_structured.py --project raizers-en-audit-projet-1 --fill Book1.xlsx
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

# Permettre l'exécution directe : python src/extract_structured.py
_SRC_DIR = Path(__file__).parent.resolve()
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from excel_filler import fill_excel_template
from normalization import (
    canonical_name,
    canonical_stem,
    is_archived_path,
    path_has_segments,
)
from runtime_config import configure_environment

ROOT_DIR = _SRC_DIR.parent.resolve()
configure_environment(ROOT_DIR)

logger = logging.getLogger(__name__)

OUTPUT_DIR = ROOT_DIR / "output"

# Budget texte envoyé au LLM par document (réduit le coût API)
MAX_CHARS = int(os.environ.get("EXTRACT_MAX_CHARS", "12000"))
FINANCIAL_MAX_CHARS = int(os.environ.get("EXTRACT_FINANCIAL_MAX_CHARS", "28000"))
FINANCIAL_NEIGHBOR_PARENTS = int(os.environ.get("EXTRACT_FINANCIAL_NEIGHBOR_PARENTS", "1"))
FINANCIAL_MAX_RELATIVE_PAGE_WINDOW = int(
    os.environ.get("EXTRACT_FINANCIAL_MAX_RELATIVE_PAGE_WINDOW", "10")
)
MIN_PARENT_CHARS = 50       # ignorer les parents trop courts

# ---------------------------------------------------------------------------
# LLM Client (Gemini par défaut, OpenAI si configuré)
# ---------------------------------------------------------------------------
def _get_llm_client():
    """Retourne (call_fn, model_name). call_fn(prompt) -> str"""
    # Priorité : OpenAI si dispo, sinon Gemini
    openai_key = os.environ.get("OPENAI_API_KEY")
    gemini_key = os.environ.get("GEMINI_API_KEY")

    if openai_key:
        from openai import OpenAI
        client = OpenAI(api_key=openai_key)
        model = "gpt-4o"
        def call(prompt: str) -> str:
            r = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                response_format={"type": "json_object"},
            )
            return r.choices[0].message.content
        logger.info(f"LLM: OpenAI ({model})")
        return call, model

    if gemini_key:
        from google import genai
        client = genai.Client(api_key=gemini_key)
        model = "gemini-2.5-flash"
        def call(prompt: str) -> str:
            r = client.models.generate_content(
                model=model,
                contents=prompt,
                config={"temperature": 0, "response_mime_type": "application/json"},
            )
            return r.text
        logger.info(f"LLM: Gemini ({model})")
        return call, model

    raise ValueError("Aucune clé API trouvée. Ajoute GEMINI_API_KEY ou OPENAI_API_KEY dans .env")


# ---------------------------------------------------------------------------
# Routing : quelles questions pour quel document ?
# ---------------------------------------------------------------------------
# Rétro-compat : certains appels externes historiques utilisaient _normalize.
_normalize = canonical_name


def _matches_doc_name(norm_filename: str, field: Dict) -> bool:
    """Retourne True si le nom de doc correspond au critère source_doc_name.

    Supporte :
    - source_doc_name: "attestation patrimoniale"
    - source_doc_name_variants: ["attestation patrimoniale", "fiche patrimoniale"]
    """
    candidates: List[str] = []

    single_name = field.get("source_doc_name")
    if isinstance(single_name, str) and single_name.strip():
        candidates.append(single_name)

    variants = field.get("source_doc_name_variants", [])
    if isinstance(variants, list):
        for item in variants:
            if isinstance(item, str) and item.strip():
                candidates.append(item)

    if not candidates:
        return True

    for candidate in candidates:
        words = canonical_name(candidate).split()
        if words and all(w in norm_filename for w in words):
            return True

    return False


def match_questions_to_doc(
    doc_info: Dict,
    fields: List[Dict],
    selected_audit_folder: Optional[str] = None,
) -> List[Dict]:
    """Retourne les questions pertinentes pour un document.

    Logique :

    1. Si ``source_dirs`` est défini, ``source_path`` DOIT contenir l'un
       des chemins indiqués en tant que sous-séquence de segments. La
       comparaison est canonique : insensible à la casse, aux accents, aux
       préfixes numériques ("0.", "1. ", "X.") et aux séparateurs parasites.
       Les chemins comportant ``{selected_audit_folder}`` sont résolus à
       runtime avec le dossier d'audit sélectionné.
    2. Filtrage ensuite par ``hint_keywords`` (recherche canonique dans
       filename + source_path).

    Le matching ``source_doc_name`` reste délégué au LLM (prompt-only) pour
    gérer variantes et fautes de frappe.
    """
    filename = doc_info.get("filename", "")
    source_path = doc_info.get("source_path", "")

    # Haystack canonique pour la recherche par mots-clés
    haystack_canon = f"{canonical_stem(filename)} {canonical_name(source_path)}"

    matched: List[Dict] = []
    for field in fields:
        # 1) Filtre par dossier source (matching canonique par segments)
        dirs = field.get("source_dirs")
        if dirs:
            resolved_dirs: List[str] = []
            for d in dirs:
                if "{selected_audit_folder}" in d:
                    if selected_audit_folder:
                        resolved_dirs.append(
                            d.replace("{selected_audit_folder}", selected_audit_folder)
                        )
                else:
                    resolved_dirs.append(d)
            if resolved_dirs and not any(
                path_has_segments(source_path, d) for d in resolved_dirs
            ):
                continue

        # 2) Filtre par mots-clés (recherche canonique)
        keywords = field.get("hint_keywords", [])
        if any(canonical_name(kw) in haystack_canon for kw in keywords):
            matched.append(field)

    return matched


# ---------------------------------------------------------------------------
# Chargement d'un document JSONL (avec filtrage des parents)
# ---------------------------------------------------------------------------
def _format_parent(parent: dict) -> str:
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


def _is_old_source_path(source_path: str) -> bool:
    return is_archived_path(source_path)


# Aliases canoniques possibles pour le dossier des ressources humaines
_RH_CANON_ALIASES = {"rh", "ressources humaines"}


def _extract_person_folder_from_source_path(source_path: str) -> Optional[str]:
    """Retourne le nom du dossier personne sous RH, sinon None.

    Matching tolérant : '3. RH', 'RH', '3. Ressources Humaines', 'ressources
    humaines' (casse/accents/préfixes ignorés).

    Exemples acceptés :
    - 2. Audit/1. Opérateur/3. RH/Pernod/file.pdf            -> "Pernod"
    - Audit/Opérateur/Ressources Humaines/Juliette/file.pdf  -> "Juliette"

    Exclusions :
    - fichiers directement dans le dossier RH (sans sous-dossier)
    - chemins old/.old/archive
    """
    if is_archived_path(source_path):
        return None

    raw_parts = [p for p in source_path.replace("\\", "/").split("/") if p]
    canon_parts = [canonical_name(p) for p in raw_parts]

    rh_idx = next(
        (i for i, p in enumerate(canon_parts) if p in _RH_CANON_ALIASES),
        None,
    )
    if rh_idx is None:
        return None

    # On veut STRICTEMENT un sous-dossier de RH (et un fichier en dessous).
    if rh_idx + 1 >= len(raw_parts) - 1:
        return None

    candidate = raw_parts[rh_idx + 1]
    if is_archived_path(candidate):
        return None
    return candidate

def load_document_text(doc_path: Path) -> str:
    """Charge un JSONL document et concatène le texte des parents (sans filtrage)."""
    texts = []
    with open(doc_path, "r", encoding="utf-8") as f:
        for line in f:
            texts.append(_format_parent(json.loads(line)))
    return "\n\n---\n\n".join(texts)


def _is_financial_field(field: Dict) -> bool:
    field_id = str(field.get("field_id", ""))
    if field.get("excel_sheet") == "{company_name}":
        return True
    return field_id.startswith("bilan_")


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


def _needs_broad_financial_context(questions: List[Dict]) -> bool:
    return any(_is_financial_field(question) for question in questions)


def load_filtered_text(
    doc_path: Path,
    questions: List[Dict],
    max_chars: int = MAX_CHARS,
    preserve_order: bool = False,
    neighbor_parents: int = 0,
    append_unmatched_tail: bool = True,
    max_relative_page_window: Optional[int] = None,
) -> str:
    """Charge un JSONL et ne garde que les parents pertinents pour les questions,
    dans la limite de *max_chars* caractères.

    Algorithme :
    1. Collecter les hint_keywords des questions matchées.
    2. Scorer chaque parent : nombre de mots-clés trouvés dans
       section_title + source_path + text.
    3. Ignorer les parents < MIN_PARENT_CHARS.
    4. Trier par score décroissant, concaténer jusqu'au budget.
    5. Fallback : si aucun parent ne matche, prendre les premiers
       parents jusqu'au budget (comportement original, tronqué).
    """
    # 1. Mots-clés issus des questions
    keywords: set[str] = set()
    for q in questions:
        for kw in q.get("hint_keywords", []):
            canon_kw = canonical_name(kw)
            if canon_kw:
                keywords.add(canon_kw)
        for kw in q.get("source_doc_name_variants", []):
            canon_kw = canonical_name(kw)
            if canon_kw:
                keywords.add(canon_kw)
        for kw in _extra_keywords_for_field(q):
            canon_kw = canonical_name(kw)
            if canon_kw:
                keywords.add(canon_kw)

    # Charger tous les parents
    parents: list[dict] = []
    with open(doc_path, "r", encoding="utf-8") as f:
        for line in f:
            parents.append(json.loads(line))

    # 2-3. Scorer et filtrer
    scored: list[tuple[int, int, dict]] = []
    for idx, p in enumerate(parents):
        text = p.get("text", "")
        if len(text) < MIN_PARENT_CHARS:
            continue
        haystack = canonical_name(
            f"{p.get('section_title', '')} {p.get('source_path', '')} {text}"
        )
        hits = sum(1 for kw in keywords if kw in haystack)
        if hits > 0:
            scored.append((hits, idx, p))

    # 4. Trier par pertinence, construire le texte dans le budget
    scored.sort(key=lambda x: x[0], reverse=True)

    texts: list[str] = []
    total = 0
    if preserve_order and scored:
        selected_indices: set[int] = set()
        indexed_parents = {idx: p for _, idx, p in scored}
        for _, idx, _ in scored:
            for candidate_idx in range(
                max(0, idx - neighbor_parents),
                min(len(parents), idx + neighbor_parents + 1),
            ):
                parent = parents[candidate_idx]
                if len(parent.get("text", "")) < MIN_PARENT_CHARS:
                    continue
                selected_indices.add(candidate_idx)

        if max_relative_page_window is not None and selected_indices:
            matched_pages = [
                _coerce_page_number(parents[idx].get("page_start"))
                for idx in selected_indices
            ]
            matched_pages = [page for page in matched_pages if page is not None]
            if matched_pages:
                max_allowed_page = min(matched_pages) + max_relative_page_window
                selected_indices = {
                    idx
                    for idx in selected_indices
                    if (
                        _coerce_page_number(parents[idx].get("page_start")) is None
                        or _coerce_page_number(parents[idx].get("page_start")) <= max_allowed_page
                    )
                }

        for idx in sorted(selected_indices):
            block = _format_parent(indexed_parents.get(idx, parents[idx]))
            if total + len(block) > max_chars:
                remaining = max_chars - total
                if remaining > 200:
                    texts.append(block[:remaining] + "\n[…]")
                break
            texts.append(block)
            total += len(block)

        if append_unmatched_tail and total < max_chars:
            for idx, parent in enumerate(parents):
                if idx in selected_indices or len(parent.get("text", "")) < MIN_PARENT_CHARS:
                    continue
                block = _format_parent(parent)
                if total + len(block) > max_chars:
                    break
                texts.append(block)
                total += len(block)
    else:
        for _, _, p in scored:
            block = _format_parent(p)
            if total + len(block) > max_chars:
                remaining = max_chars - total
                if remaining > 200:
                    texts.append(block[:remaining] + "\n[…]")
                break
            texts.append(block)
            total += len(block)

    # 5. Fallback : aucun match → premiers parents jusqu'au budget
    if not texts:
        for p in parents:
            if len(p.get("text", "")) < MIN_PARENT_CHARS:
                continue
            block = _format_parent(p)
            if total + len(block) > max_chars:
                break
            texts.append(block)
            total += len(block)

    logger.debug(f"  filtrage parents : {len(texts)}/{len(parents)} retenus, {total} chars")
    return "\n\n---\n\n".join(texts)


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------
def build_prompt(document_text: str, questions: List[Dict], filename: str, source_path: str) -> str:
    fields_desc = []
    for q in questions:
        desc = f'- "{q["field_id"]}": {q["question"]}'
        if q.get("format_hint"):
            desc += f' (format: {q["format_hint"]})'
        if q.get("context_hint"):
            desc += f' — Contexte: {q["context_hint"]}'

        source_doc_name = q.get("source_doc_name")
        source_doc_name_variants = q.get("source_doc_name_variants")
        if source_doc_name:
            desc += f' — SourceDoc attendu: "{source_doc_name}"'
        elif source_doc_name_variants:
            variants = [v for v in source_doc_name_variants if isinstance(v, str)]
            if variants:
                joined = ", ".join(f'"{v}"' for v in variants)
                desc += f' — SourceDoc attendu (variantes): {joined}'

        fields_desc.append(desc)

    fields_block = "\n".join(fields_desc)
    field_ids = [q["field_id"] for q in questions]

    extra_instructions = ""
    if _needs_broad_financial_context(questions):
        extra_instructions = """
## ATTENTION — Etats financiers

### Colonnes N et N-1
- N = exercice le PLUS RÉCENT. N-1 = exercice précédent.
- Si 4+ colonnes (Brut, Amort, Net N, Net N-1) : utilise TOUJOURS Net.
- Si un seul exercice : N-1 = null.

### Totaux vs sous-lignes
- S'il existe un sous-total affiché, utilise-le.
- Sinon, somme les sous-lignes nettes. Indique la formule en commentaires.
- Ne prends JAMAIS une seule sous-ligne quand il y en a plusieurs.

### INTERDIT — Double comptage
Chaque montant du document ne peut apparaître que dans UN SEUL poste de ta réponse.
Exemple d'erreur fréquente : mettre 501 dans immobilisations_financieres ET dans autres_actif_residuel.

### Vérification obligatoire
Avant de répondre, vérifie : somme de tes postes de détail ≈ total affiché (tolérance 5%).
Si l'écart est trop grand, tu as probablement confondu des colonnes ou double-compté.

### Portée des tableaux
- Pour les champs de type tableau financier, prends UNIQUEMENT les tableaux principaux Actif, Passif et Compte de résultat.
- Privilégie les tableaux présentés au début du document d'états financiers / bilan.
- Ignore les annexes, notes, tableaux détaillés secondaires, SIG et reprises plus loin dans le document.

### Extraction ligne par ligne
- Pour les champs de type tableau, retourne UNE entrée par ligne qui contient au moins un montant lisible en N ou N-1.
- N'ignore pas une ligne chiffrée même si son libellé te paraît inhabituel ou difficile à classifier.
- N'inclus pas les lignes purement textuelles ou les en-têtes sans montant.
"""

    return f"""Tu es un analyste financier expert. Tu extrais des informations précises depuis des documents de projet immobilier (crowdfunding obligataire).

## Document : {filename}
## Source path : {source_path}

{document_text}

## Instructions

Extrais les informations suivantes de ce document. Pour chaque champ :
- Si l'information est clairement présente, retourne la valeur exacte.
- Si l'information N'EST PAS dans ce document, retourne null.
- Ne devine PAS. Ne fabrique PAS de données. Si tu n'es pas sûr, retourne null.
- Respecte le format demandé.
- Si un champ contient une contrainte "SourceDoc attendu" (ou variantes), vérifie intelligemment que le nom du document actuel correspond bien (tolérance: accents, tirets, underscores, fautes mineures, mots manquants comme "complétée"). Si la correspondance n'est pas solide, retourne null pour ce champ.
{extra_instructions}

## ATTENTION — Distinction importante

Dans ce type de montage, il y a souvent DEUX sociétés distinctes :
- La **société portant l'emprunt** (émettrice des obligations, celle qui emprunte via Raizers).
- La **société portant l'opération** (celle qui réalise le projet immobilier). Elle peut être identique ou différente.

Les champs "*_emprunt" concernent la société émettrice. Les champs "*_operation" ne doivent être remplis QUE si la société opération est DIFFÉRENTE de la société emprunt. Sinon, retourne null pour les champs *_operation.

## Champs à extraire

{fields_block}

## Format de réponse

Retourne UNIQUEMENT un objet JSON avec ces clés : {field_ids}
Chaque valeur est soit une string, soit null, soit un array JSON (pour les champs de type tableau).
"""


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
def _is_per_person_field(field: Dict) -> bool:
    """Un champ est 'per-person' s'il cible un onglet dynamique {person_name}."""
    return field.get("excel_sheet") == "{person_name}"


def _is_per_company_field(field: Dict) -> bool:
    """Un champ est 'per-company' s'il cible un onglet dynamique {company_name}."""
    return field.get("excel_sheet") == "{company_name}"


_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2}|21\d{2})\b")
_POSTAL_CODE_RE = re.compile(r"\b\d{5}\b")
_PROJECT_LOCATION_FIELD_ID = "localisation_projet"
_ADDRESS_STREET_HINTS = {
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


def _extract_years_from_text(text: str) -> List[int]:
    years = {int(match.group(1)) for match in _YEAR_RE.finditer(text or "")}
    return sorted(years, reverse=True)


def _get_doc_financial_year(doc_info: Dict) -> Optional[int]:
    """Extrait l'année financière la plus récente visible dans le nom du document.

    On priorise le filename, puis source_path en fallback.
    """
    filename = doc_info.get("filename", "")
    years = _extract_years_from_text(filename)
    if years:
        return years[0]

    source_path = doc_info.get("source_path", "")
    years = _extract_years_from_text(source_path)
    return years[0] if years else None


def _select_latest_financial_years(
    manifest_files: List[Dict],
    company_fields: List[Dict],
    selected_audit_folder: Optional[str] = None,
    limit: int = 2,
) -> set[int]:
    """Détermine les N années les plus récentes présentes dans les docs financiers."""
    years: set[int] = set()
    for doc_info in manifest_files:
        matched_company = match_questions_to_doc(doc_info, company_fields, selected_audit_folder)
        if not matched_company:
            continue
        year = _get_doc_financial_year(doc_info)
        if year is not None:
            years.add(year)

    if not years:
        return set()

    return set(sorted(years, reverse=True)[:limit])


def _extract_company_folder_from_source_path(source_path: str) -> Optional[str]:
    """Retourne le dossier parent immédiat du fichier (= dossier société)."""
    parts = [p for p in source_path.replace("\\", "/").split("/") if p]
    # Dernier élément = fichier, avant-dernier = dossier parent
    if len(parts) >= 2:
        return parts[-2]
    return None


def _build_latest_year_per_company_folder(
    manifest_files: List[Dict],
    company_fields: List[Dict],
    selected_audit_folder: Optional[str] = None,
) -> Dict[str, int]:
    """Retourne {normalized_folder → année_max} pour les docs financiers.

    Permet de ne traiter que le bilan le plus récent par société,
    même si plusieurs bilans (2022, 2023…) sont présents dans le même dossier.
    """
    folder_years: Dict[str, int] = {}
    for doc_info in manifest_files:
        matched_company = match_questions_to_doc(doc_info, company_fields, selected_audit_folder)
        if not matched_company:
            continue
        year = _get_doc_financial_year(doc_info)
        if year is None:
            continue
        folder = _extract_company_folder_from_source_path(doc_info.get("source_path", ""))
        if not folder:
            folder = Path(doc_info.get("filename", "unknown")).stem
        folder_key = canonical_name(folder)
        if folder_key not in folder_years or year > folder_years[folder_key]:
            folder_years[folder_key] = year
    return folder_years


def _looks_like_precise_project_address(value: Optional[str]) -> bool:
    text = (value or "").strip()
    if not text:
        return False

    normalized = canonical_name(text)
    if not normalized:
        return False

    tokens = set(normalized.split())
    has_street_hint = any(token in tokens for token in _ADDRESS_STREET_HINTS)
    has_number = re.search(r"\b\d+[a-zA-Z]?\b", text) is not None
    has_postal_code = _POSTAL_CODE_RE.search(text) is not None
    return has_street_hint and (has_number or has_postal_code)


def _same_project_address(candidate_a: str, candidate_b: str) -> bool:
    norm_a = canonical_name(candidate_a)
    norm_b = canonical_name(candidate_b)
    if not norm_a or not norm_b:
        return False
    return norm_a == norm_b or norm_a in norm_b or norm_b in norm_a


def _resolve_project_location(candidates: List[Dict]) -> Optional[str]:
    precise_candidates = [
        candidate for candidate in candidates
        if _looks_like_precise_project_address(candidate.get("value"))
    ]
    if len(precise_candidates) < 2:
        return None

    best_cluster: List[Dict] = []
    best_doc_count = 0
    best_value_len = 0

    for candidate in precise_candidates:
        cluster = [
            other for other in precise_candidates
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


def _call_llm_with_retry(call_llm, prompt: str, max_retries: int = 3) -> Optional[str]:
    """Appelle le LLM avec retry sur 429."""
    for attempt in range(max_retries):
        try:
            return call_llm(prompt)
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                import re as _re
                delay_match = _re.search(r'retry(?:Delay)?["\s:]*(\d+)', err_str, _re.IGNORECASE)
                wait = int(delay_match.group(1)) + 5 if delay_match else 60
                logger.warning(f"    ⏳ Rate limit (429) — retry {attempt+1}/{max_retries} dans {wait}s")
                time.sleep(wait)
            else:
                raise
    return None


def _stringify_non_table_value(value) -> str:
    """Rend les réponses LLM non-table lisibles dans le JSON final."""
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
                    rendered_items.append(", ".join(f"{k}: {v}" for k, v in item.items() if v not in (None, "")))
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


def _has_meaningful_value(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() not in {"", "null", "[]", "{}"}
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) > 0
    return True


# ---------------------------------------------------------------------------
# Validation post-LLM des tables financières
# ---------------------------------------------------------------------------

# Tolérance pour la vérification des sommes (5%)
_VALIDATION_TOLERANCE = 0.05

# Postes de détail qui doivent sommer vers total_actif
_ACTIF_DETAIL_KEYS = [
    "immobilisations_corporelles", "immobilisations_financieres",
    "stocks", "creances_clients", "autres_creances",
    "disponibilites", "vmp", "charges_constatees_avance",
    "autres_actif_residuel",
]

# Postes de détail qui doivent sommer vers total_passif
_PASSIF_DETAIL_KEYS = [
    "capitaux_propres", "dettes_bancaires", "autres_dettes_financieres",
    "fournisseurs", "dettes_fiscales_sociales", "dettes_diverses",
    "provisions_pour_risques", "provisions_pour_charges",
    "produits_constates_avance", "autres_passif_residuel",
]

# Postes de charges du CDR (doivent sommer vers charges si affiché)
_CDR_CHARGES_KEYS = [
    "achats_marchandises", "variation_stock_marchandises",
    "achats_matieres_premieres", "variation_stock_matieres_premieres",
    "autres_charges_externes", "salaires", "charges_sociales",
    "impots_taxes", "dotations_amortissements", "dotations_provisions",
    "autres_charges_exploitation",
]


# ---------------------------------------------------------------------------
# Mapping poste brut (LLM) → clé canonique (Excel)
# ---------------------------------------------------------------------------
# Chaque entrée : normalized_poste → (canonical_key, is_total)
# is_total=True  → c'est un sous-total ou total de section, on le préfère
# is_total=False → c'est une ligne de détail, on la somme si pas de total
# canonical_key=None → ligne à ignorer (en-tête de section, etc.)

def _normalize_poste(poste: str) -> str:
    """Normalise un libellé de poste pour le matching."""
    return canonical_name(poste).replace(" ", "")


_ACTIF_POSTE_MAP = {
    # --- Immobilisations incorporelles → autres_actif_residuel ---
    "immobilisationsincorporelles": ("autres_actif_residuel", True),
    "totalimmobilisationsincorporelles": ("autres_actif_residuel", True),
    "fraisdestablissement": ("autres_actif_residuel", False),
    "fraisderechercheetdeveloppement": ("autres_actif_residuel", False),
    "concessionsbrevetslicencesmarquesprocedes": ("autres_actif_residuel", False),
    "fondscommercial": ("autres_actif_residuel", False),

    # --- Immobilisations corporelles ---
    "immobilisationscorporelles": ("immobilisations_corporelles", True),
    "totalimmobilisationscorporelles": ("immobilisations_corporelles", True),
    "terrains": ("immobilisations_corporelles", False),
    "constructions": ("immobilisations_corporelles", False),
    "installationstechniquesmaterieletoutillageindustriels": ("immobilisations_corporelles", False),
    "installationstechniques": ("immobilisations_corporelles", False),
    "materieletoutillage": ("immobilisations_corporelles", False),
    "materiel": ("immobilisations_corporelles", False),
    "autresimmobilisationscorporelles": ("immobilisations_corporelles", False),
    "immobilisationscorporellesencours": ("immobilisations_corporelles", False),
    "avancesetacomptessurimmobilisationscorporelles": ("immobilisations_corporelles", False),

    # --- Immobilisations financières ---
    "immobilisationsfinancieres": ("immobilisations_financieres", True),
    "totalimmobilisationsfinancieres": ("immobilisations_financieres", True),
    "participations": ("immobilisations_financieres", False),
    "autresparticipations": ("immobilisations_financieres", False),
    "participationsevalueesparmiseenequivalence": ("immobilisations_financieres", False),
    "creancesrattacheesadesparticipations": ("immobilisations_financieres", False),
    "autrestitresimmobilises": ("immobilisations_financieres", False),
    "prets": ("immobilisations_financieres", False),
    "autresimmobilisationsfinancieres": ("immobilisations_financieres", False),

    # --- Stocks ---
    "stocks": ("stocks", True),
    "stocksetencours": ("stocks", True),
    "totalstocksetencours": ("stocks", True),
    "matierespremieresetautresapprovisionnements": ("stocks", False),
    "matierespremieres": ("stocks", False),
    "encoursdeproduction": ("stocks", False),
    "encoursdeproductiondebiens": ("stocks", False),
    "encoursdeproductiondeservices": ("stocks", False),
    "produitsintermediairesetfinis": ("stocks", False),
    "marchandises": ("stocks", False),

    # --- Créances ---
    "creances": ("creances", True),
    "totalcreances": ("creances", True),
    "creancesclients": ("creances_clients", True),
    "clientsetcomptesrattaches": ("creances_clients", True),
    "clients": ("creances_clients", True),
    "autrescreances": ("autres_creances", True),
    "personnel": ("autres_creances", False),
    "etatetautrescollectivitespubliques": ("autres_creances", False),
    "securitesocialeetautresorganismessociaux": ("autres_creances", False),
    "impotssurlesbenefices": ("autres_creances", False),
    "taxesurlavaleurajoutee": ("autres_creances", False),
    "autrescreancesdiverses": ("autres_creances", False),
    "fournisseursdebiteursavancesetacomptes": ("autres_creances", False),
    "capitalsouscritappelenonverse": ("autres_actif_residuel", False),

    # --- Trésorerie ---
    "tresorerie": ("tresorerie", True),
    "disponibilites": ("disponibilites", True),
    "valeursmobilieresdeplacement": ("vmp", True),
    "vmp": ("vmp", True),

    # --- Divers ---
    "chargesconstateesdavance": ("charges_constatees_avance", True),
    "capitalsouscritnonappele": ("autres_actif_residuel", True),
    "avancesetacomptesversessurcommandes": ("autres_actif_residuel", True),

    # --- Totaux de section à ignorer (on ne les mappe pas) ---
    "actifimmobilise": (None, True),
    "totalactifimmobilise": (None, True),
    "actifcirculant": (None, True),
    "totalactifcirculant": (None, True),

    # --- Total général ---
    "totalactif": ("total_actif", True),
    "totalgeneralactif": ("total_actif", True),
    "totalgeneral": ("total_actif", True),
}

_PASSIF_POSTE_MAP = {
    # --- Capitaux propres ---
    "capitalsocial": ("capital_social", True),
    "capitalindividuel": ("capital_social", True),
    "capital": ("capital_social", True),
    "primesdemission": ("capitaux_propres_detail", False),
    "primesdapport": ("capitaux_propres_detail", False),
    "primesdefusionscission": ("capitaux_propres_detail", False),
    "ecartsderevaluation": ("capitaux_propres_detail", False),
    "reservelegale": ("capitaux_propres_detail", False),
    "reservesstatutairesetcontractuelles": ("capitaux_propres_detail", False),
    "reservesreglementees": ("capitaux_propres_detail", False),
    "autresreserves": ("capitaux_propres_detail", False),
    "reserves": ("capitaux_propres_detail", False),
    "reportanouveau": ("capitaux_propres_detail", False),
    "resultatdelexercice": ("resultat_exercice", True),
    "resultatdeexercice": ("resultat_exercice", True),
    "resultatexercice": ("resultat_exercice", True),
    "benefice": ("resultat_exercice", True),
    "perte": ("resultat_exercice", True),
    "resultat": ("resultat_exercice", True),
    "resultatnet": ("resultat_exercice", True),
    "subventionsdinvestissement": ("capitaux_propres_detail", False),
    "provisionsreglementees": ("capitaux_propres_detail", False),
    "capitauxpropres": ("capitaux_propres", True),
    "totalcapitauxpropres": ("capitaux_propres", True),

    # --- Provisions ---
    "provisionspourrisques": ("provisions_pour_risques", True),
    "provisionspourcharges": ("provisions_pour_charges", True),
    "provisions": ("provisions_pour_risques", True),
    "totalprovisions": ("provisions_pour_risques", True),
    "provisionspourrisquesetcharges": ("provisions_pour_risques", True),

    # --- Dettes financières ---
    "empruntsetdettesaupresdesetablissementsdecredit": ("dettes_bancaires", True),
    "empruntsbancaires": ("dettes_bancaires", True),
    "dettesbancaires": ("dettes_bancaires", True),
    "empruntsaupresdesetablissementsdecredit": ("dettes_bancaires", True),
    "empruntsobligatairesconvertibles": ("autres_dettes_financieres", False),
    "empruntsobligataires": ("autres_dettes_financieres", False),
    "autresempruntsobligataires": ("autres_dettes_financieres", False),
    "comptescourantsdassocies": ("autres_dettes_financieres", True),
    "comptescourantsassocies": ("autres_dettes_financieres", True),
    "comptescourants": ("autres_dettes_financieres", True),
    "cca": ("autres_dettes_financieres", True),
    "autresdettesfinancieres": ("autres_dettes_financieres", True),
    "empruntsetdettesfinancieresdivers": ("autres_dettes_financieres", True),
    "empruntsetdettesfinancieresdiverses": ("autres_dettes_financieres", True),
    "dettesfinancieres": ("dettes_financieres", True),
    "totaldettesfinancieres": ("dettes_financieres", True),

    # --- Dettes d'exploitation ---
    "fournisseurs": ("fournisseurs", True),
    "fournisseursetcomptesrattaches": ("fournisseurs", True),
    "dettesfournisseurs": ("fournisseurs", True),
    "dettesfiscalesetsociales": ("dettes_fiscales_sociales", True),
    "dettesfiscalessociales": ("dettes_fiscales_sociales", True),
    "dettessociales": ("dettes_fiscales_sociales", False),
    "dettesfiscales": ("dettes_fiscales_sociales", False),
    "dettesexploitation": ("dettes_exploitation", True),
    "totaldettesdexploitation": ("dettes_exploitation", True),

    # --- Dettes diverses ---
    "dettessurimmobilisations": ("autres_passif_residuel", True),
    "dettessurimmobilisationsetcomptesrattaches": ("autres_passif_residuel", True),
    "autresdettes": ("dettes_diverses", True),
    "dettesdiverses": ("dettes_diverses", True),

    # --- Produits constatés d'avance ---
    "produitsconstatesdavance": ("produits_constates_avance", True),

    # --- Totaux de section à ignorer ---
    "dettes": (None, True),
    "totaldettes": (None, True),
    "totaldettesdivers": (None, True),

    # --- Total général ---
    "totalpassif": ("total_passif", True),
    "totalgeneralpassif": ("total_passif", True),
    "totalgeneral": ("total_passif", True),
}

_CDR_POSTE_MAP = {
    # --- Produits d'exploitation ---
    "ventesdemarchandises": ("chiffre_affaires", False),
    "productionvenduedebiens": ("chiffre_affaires", False),
    "productionvenduedeservices": ("chiffre_affaires", False),
    "productionvendue": ("chiffre_affaires", False),
    "chiffredaffaires": ("chiffre_affaires", True),
    "chiffredaffairesnet": ("chiffre_affaires", True),
    "montantnetduchiffredaffaires": ("chiffre_affaires", True),
    "ca": ("chiffre_affaires", True),

    "productionstockee": ("production_stockee", True),
    "productionimmobilisee": ("production_stockee", False),

    "subventionsdexploitation": ("subventions_exploitation", True),
    "subventionsdexploitationrecues": ("subventions_exploitation", True),

    "reprisesuramortissementsetprovisionstransfertdecharges": ("reprises_exploitation", True),
    "reprisesuramortissementsetprovisionstransfertsdecharges": ("reprises_exploitation", True),
    "reprisesuramortissementsetprovisions": ("reprises_exploitation", True),
    "reprisessurdepreciationsetprovisions": ("reprises_exploitation", True),
    "transfertsdecharges": ("reprises_exploitation", False),

    "autresproduits": ("autres_produits_exploitation", True),
    "autresproduitsdexploitation": ("autres_produits_exploitation", True),
    "autresproduitsexploitation": ("autres_produits_exploitation", True),

    # --- Charges d'exploitation ---
    "achatsdemarchandises": ("achats_marchandises", True),
    "achatsmarchandises": ("achats_marchandises", True),
    "variationdestockdemarchandises": ("variation_stock_marchandises", True),
    "variationdesstocksdemarchandises": ("variation_stock_marchandises", True),
    "variationdestockmarchandises": ("variation_stock_marchandises", True),

    "achatsdematieresetautresapprovisionnements": ("achats_matieres_premieres", True),
    "achatsdematierespremieres": ("achats_matieres_premieres", True),
    "achatsdematiespremieresetautresapprovisionnements": ("achats_matieres_premieres", True),
    "achatsetapprovisionnements": ("autres_charges_externes", True),
    "variationdestockdematierespremieresetapprovisionnements": ("variation_stock_matieres_premieres", True),
    "variationdesstocksdematierespremieresetapprovisionnements": ("variation_stock_matieres_premieres", True),
    "variationdestockmatierespremieres": ("variation_stock_matieres_premieres", True),

    "autresachatsetchargesexternes": ("autres_charges_externes", True),
    "autresachatschargesexternes": ("autres_charges_externes", True),
    "chargesexternes": ("autres_charges_externes", True),
    "achatsetchargesexternes": ("autres_charges_externes", True),

    "salairesettraitements": ("salaires", True),
    "salaires": ("salaires", True),
    "remunerationsdupersonnel": ("salaires", True),
    "chargessociales": ("charges_sociales", True),
    "chargessocialesdupersonnel": ("charges_sociales", True),

    "impotstaxesetversementsassimiles": ("impots_taxes", True),
    "impotsettaxes": ("impots_taxes", True),
    "impotsetversementsassimiles": ("impots_taxes", True),

    "dotationsauxamortissementssurimmobilisations": ("dotations_amortissements", True),
    "dotationsauxamortissements": ("dotations_amortissements", True),
    "dotationsamortissements": ("dotations_amortissements", True),
    "dotationsauxprovisions": ("dotations_provisions", True),
    "dotationsauxdepreciations": ("dotations_provisions", True),
    "dotationsauxamortissementsetprovisions": ("dotations", True),
    "dotationsauxamortissementsdepreciationsetprovisions": ("dotations", True),
    "dotationsdexploitation": ("dotations", True),
    "dotations": ("dotations", True),

    "autrescharges": ("autres_charges_exploitation", True),
    "autreschargesdexploitation": ("autres_charges_exploitation", True),
    "autreschargesexploitation": ("autres_charges_exploitation", True),

    # --- Totaux et résultats ---
    "totalchargesdexploitation": ("charges", True),
    "chargesdexploitation": ("charges", True),
    "totaldeschargesdexploitation": ("charges", True),
    "totalproduitsdexploitation": (None, True),
    "produitsdexploitation": (None, True),

    "resultatdexploitation": ("resultat_exploitation", True),
    "resultatexploitation": ("resultat_exploitation", True),

    # --- Résultat financier ---
    "produitsfinanciers": (None, True),
    "chargesfinancieres": (None, True),
    "totaldesproduitsfinanciers": (None, True),
    "totaldeschargesfinancieres": (None, True),
    "resultatfinancier": ("resultat_financier", True),

    # --- Résultat exceptionnel ---
    "produitsexceptionnels": (None, True),
    "chargesexceptionnelles": (None, True),
    "totaldesproduitsexceptionnels": (None, True),
    "totaldeschargesexceptionnelles": (None, True),
    "resultatexceptionnel": ("resultat_exceptionnel", True),

    # --- Impôts ---
    "impotssurlesbenefices": ("impots_sur_les_societes", True),
    "impotssurlessocietes": ("impots_sur_les_societes", True),
    "is": ("impots_sur_les_societes", True),

    # --- Totaux globaux à ignorer ---
    "totaldesproduits": (None, True),
    "totaldescharges": (None, True),
    "resultatnet": (None, True),
    "beneficeouperte": (None, True),
}

# Table type → mapping dict
_TABLE_POSTE_MAPS = {
    "bilan_actif_table": _ACTIF_POSTE_MAP,
    "bilan_passif_table": _PASSIF_POSTE_MAP,
    "bilan_compte_resultat_table": _CDR_POSTE_MAP,
}


def _sum_raw_rows(rows: List[Dict], canonical_key: str) -> Dict:
    """Somme les valeurs de plusieurs lignes brutes en un seul objet canonique."""
    total_n = 0.0
    total_n1 = 0.0
    has_n = False
    has_n1 = False
    postes = []

    for r in rows:
        n = _safe_number(r.get("n"))
        n1 = _safe_number(r.get("n1"))
        if n is not None:
            total_n += n
            has_n = True
        if n1 is not None:
            total_n1 += n1
            has_n1 = True
        postes.append(r.get("poste", ""))

    return {
        "key": canonical_key,
        "poste": " + ".join(postes),
        "n": total_n if has_n else None,
        "n1": total_n1 if has_n1 else None,
        "commentaires": f"Somme Python de {len(rows)} sous-lignes",
    }


def _row_has_numeric_amount(row: Dict) -> bool:
    return _safe_number(row.get("n")) is not None or _safe_number(row.get("n1")) is not None


def _build_preserved_raw_row(
    row: Dict,
    table_type: str,
    source_index: int,
    reason: str,
) -> Dict:
    table_suffix = table_type.replace("bilan_", "").replace("_table", "")
    existing_comment = (row.get("commentaires") or "").strip()
    comment_parts = [part for part in [existing_comment, reason] if part]
    return {
        "key": f"raw_unmatched_{table_suffix}_{source_index:03d}",
        "poste": row.get("poste", ""),
        "n": row.get("n"),
        "n1": row.get("n1"),
        "commentaires": " | ".join(comment_parts),
        "_source_index": source_index,
    }


def _map_raw_financial_table(raw_rows: list, table_type: str) -> list:
    """Transforme les lignes brutes du LLM en lignes canoniques avec clé.

    Logique :
    1. Chaque poste est normalisé et cherché dans le mapping.
    2. Si un sous-total (is_total=True) est trouvé pour une clé, il est préféré.
    3. Si seuls des détails (is_total=False) existent, ils sont sommés.
    4. Les lignes inconnues ou à ignorer (canonical_key=None) sont loguées.
    """
    poste_map = _TABLE_POSTE_MAPS.get(table_type, {})
    if not poste_map:
        return raw_rows  # pas de mapping pour ce type, retourner tel quel

    # Grouper par clé canonique : {key → {"totals": [...], "details": [...]}}
    groups: Dict[str, Dict[str, list]] = {}
    unmatched: List[str] = []
    preserved_raw_rows: List[Dict] = []

    for source_index, row in enumerate(raw_rows):
        if not isinstance(row, dict):
            continue
        poste = row.get("poste", "")
        if not poste:
            continue

        norm = _normalize_poste(poste)
        if not norm:
            continue

        match = poste_map.get(norm)
        if match is None:
            # Essai partiel : chercher si le poste normalisé est contenu dans une clé
            match = _fuzzy_match_poste(norm, poste_map)

        if match is None:
            unmatched.append(poste)
            if _row_has_numeric_amount(row):
                preserved_raw_rows.append(
                    _build_preserved_raw_row(
                        row,
                        table_type,
                        source_index,
                        "Ligne conservee: poste non mappe vers une cle Excel",
                    )
                )
            continue

        canonical_key, is_total = match
        if canonical_key is None:
            if _row_has_numeric_amount(row):
                preserved_raw_rows.append(
                    _build_preserved_raw_row(
                        row,
                        table_type,
                        source_index,
                        "Ligne conservee: total ou section non exploite(e) par Excel",
                    )
                )
            continue  # ligne à ignorer (en-tête de section)

        if canonical_key not in groups:
            groups[canonical_key] = {"totals": [], "details": [], "first_index": source_index}

        if is_total:
            groups[canonical_key]["totals"].append(row)
        else:
            groups[canonical_key]["details"].append(row)

    # Résoudre chaque groupe
    result = []
    for canonical_key, group in groups.items():
        if group["totals"]:
            # Prendre le dernier sous-total (dans l'ordre du document)
            best = group["totals"][-1]
            result.append({
                "key": canonical_key,
                "poste": best.get("poste", ""),
                "n": best.get("n"),
                "n1": best.get("n1"),
                "commentaires": best.get("commentaires", ""),
                "_source_index": group["first_index"],
            })
        elif group["details"]:
            if len(group["details"]) == 1:
                row = group["details"][0]
                result.append({
                    "key": canonical_key,
                    "poste": row.get("poste", ""),
                    "n": row.get("n"),
                    "n1": row.get("n1"),
                    "commentaires": row.get("commentaires", ""),
                    "_source_index": group["first_index"],
                })
            else:
                summed_row = _sum_raw_rows(group["details"], canonical_key)
                summed_row["_source_index"] = group["first_index"]
                result.append(summed_row)

    result.extend(preserved_raw_rows)
    result.sort(key=lambda row: row.get("_source_index", 10**9))
    for row in result:
        row.pop("_source_index", None)

    if unmatched:
        logger.info(
            "    ⚠️ %s poste(s) non matché(s) dans %s, %s ligne(s) brute(s) conservée(s)",
            len(unmatched),
            table_type,
            len(preserved_raw_rows),
        )
        logger.debug("    Postes non matchés (%s) : %s", table_type, ", ".join(unmatched[:10]))

    return result


def _fuzzy_match_poste(
    norm_poste: str,
    poste_map: Dict[str, tuple],
) -> Optional[tuple]:
    """Matching approximatif : cherche si norm_poste contient une clé du mapping ou vice-versa."""
    # D'abord : le poste contient-il une clé connue ? (ex: "totalimmobilisationscorporellesnet" contient "immobilisationscorporelles")
    best_match = None
    best_len = 0
    for map_key, value in poste_map.items():
        if value[0] is None:
            continue  # ignorer les entrées à skip
        if len(map_key) < 6:
            continue  # trop court pour un match partiel fiable
        if map_key in norm_poste and len(map_key) > best_len:
            best_match = value
            best_len = len(map_key)
    return best_match


# Gestion spéciale du passif : capitaux_propres_detail doit être ignoré
# (ces lignes sont des sous-composants de capitaux_propres qui est un total)
# On les utilise seulement si capitaux_propres n'est pas trouvé
def _post_process_passif(mapped_rows: list) -> list:
    """Post-traitement du passif : fusionner capitaux_propres_detail si nécessaire."""
    has_capitaux_propres = any(r["key"] == "capitaux_propres" for r in mapped_rows)
    result = []
    for row in mapped_rows:
        if row["key"] == "capitaux_propres_detail":
            if not has_capitaux_propres:
                # Pas de total affiché → on utilise la somme des détails comme capitaux propres.
                row["key"] = "capitaux_propres"
                result.append(row)
            # Sinon on les ignore (déjà inclus dans le total)
        else:
            result.append(row)
    return result


def _safe_number(value) -> Optional[float]:
    """Convertit une valeur en float, retourne None si impossible."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(" ", "").replace("\u00a0", "")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _build_table_dict(table_data) -> Dict[str, Dict]:
    """Construit un dict key → row_data à partir d'un array JSON ou string."""
    if isinstance(table_data, str):
        try:
            table_data = json.loads(table_data)
        except (json.JSONDecodeError, TypeError):
            return {}
    if not isinstance(table_data, list):
        return {}
    result = {}
    for row in table_data:
        if isinstance(row, dict) and row.get("key"):
            result[row["key"]] = row
    return result


def _sum_keys(table_dict: Dict[str, Dict], keys: List[str], period: str = "n") -> Optional[float]:
    """Somme les valeurs d'un ensemble de clés pour une période donnée."""
    total = 0.0
    any_value = False
    for key in keys:
        row = table_dict.get(key)
        if not row:
            continue
        val = _safe_number(row.get(period))
        if val is not None:
            total += val
            any_value = True
    return total if any_value else None


def _validate_actif_table(table_data) -> List[str]:
    """Valide la cohérence du bilan actif. Retourne une liste d'erreurs."""
    d = _build_table_dict(table_data)
    if not d:
        return []
    errors = []

    for period in ("n", "n1"):
        total_row = d.get("total_actif")
        total_val = _safe_number(total_row.get(period)) if total_row else None
        if total_val is None or total_val == 0:
            continue

        detail_sum = _sum_keys(d, _ACTIF_DETAIL_KEYS, period)
        if detail_sum is not None and detail_sum > 0:
            ecart = abs(detail_sum - total_val) / abs(total_val)
            if ecart > _VALIDATION_TOLERANCE:
                errors.append(
                    f"ACTIF {period}: somme détails={detail_sum:.0f} vs total_actif={total_val:.0f} "
                    f"(écart {ecart:.0%}). Probable double-comptage ou poste manquant."
                )

    return errors


def _validate_passif_table(table_data) -> List[str]:
    """Valide la cohérence du bilan passif. Retourne une liste d'erreurs."""
    d = _build_table_dict(table_data)
    if not d:
        return []
    errors = []

    for period in ("n", "n1"):
        total_row = d.get("total_passif")
        total_val = _safe_number(total_row.get(period)) if total_row else None
        if total_val is None or total_val == 0:
            continue

        detail_sum = _sum_keys(d, _PASSIF_DETAIL_KEYS, period)
        if detail_sum is not None and detail_sum > 0:
            ecart = abs(detail_sum - total_val) / abs(total_val)
            if ecart > _VALIDATION_TOLERANCE:
                errors.append(
                    f"PASSIF {period}: somme détails={detail_sum:.0f} vs total_passif={total_val:.0f} "
                    f"(écart {ecart:.0%}). Probable double-comptage ou poste manquant."
                )

    return errors


def _validate_cdr_table(table_data) -> List[str]:
    """Valide la cohérence du compte de résultat."""
    d = _build_table_dict(table_data)
    if not d:
        return []
    errors = []

    for period in ("n", "n1"):
        # Vérif : CA - charges ≈ résultat exploitation (approximatif)
        ca_val = _safe_number(d.get("chiffre_affaires", {}).get(period))
        re_val = _safe_number(d.get("resultat_exploitation", {}).get(period))
        charges_val = _safe_number(d.get("charges", {}).get(period))

        if ca_val is not None and re_val is not None and charges_val is not None:
            expected_re = ca_val - charges_val
            if abs(re_val) > 0:
                ecart = abs(expected_re - re_val) / max(abs(re_val), 1)
                if ecart > _VALIDATION_TOLERANCE:
                    # Vérifier avec les détails des charges
                    detail_charges = _sum_keys(d, _CDR_CHARGES_KEYS, period)
                    if detail_charges is not None and detail_charges > 0:
                        expected_re2 = ca_val - detail_charges
                        ecart2 = abs(expected_re2 - re_val) / max(abs(re_val), 1)
                        if ecart2 > _VALIDATION_TOLERANCE:
                            errors.append(
                                f"CDR {period}: CA={ca_val:.0f} - charges détaillées={detail_charges:.0f} "
                                f"= {expected_re2:.0f} vs résultat_exploitation={re_val:.0f} "
                                f"(écart {ecart2:.0%})."
                            )

        # Vérif : si charges total affiché et somme détails
        if charges_val is not None and charges_val > 0:
            detail_charges = _sum_keys(d, _CDR_CHARGES_KEYS, period)
            if detail_charges is not None and detail_charges > 0:
                ecart = abs(detail_charges - charges_val) / abs(charges_val)
                if ecart > _VALIDATION_TOLERANCE:
                    errors.append(
                        f"CDR {period}: somme charges détaillées={detail_charges:.0f} vs "
                        f"charges total={charges_val:.0f} (écart {ecart:.0%})."
                    )

    return errors


def validate_financial_answers(answers: Dict) -> Dict[str, List[str]]:
    """Valide toutes les tables financières dans les réponses. Retourne {field_id: [erreurs]}."""
    all_errors: Dict[str, List[str]] = {}

    for key, value in answers.items():
        if value is None:
            continue
        if "bilan_actif_table" in key:
            errs = _validate_actif_table(value)
            if errs:
                all_errors[key] = errs
        elif "bilan_passif_table" in key:
            errs = _validate_passif_table(value)
            if errs:
                all_errors[key] = errs
        elif "bilan_compte_resultat_table" in key:
            errs = _validate_cdr_table(value)
            if errs:
                all_errors[key] = errs

    return all_errors


def _build_correction_prompt(
    original_prompt: str,
    answers: Dict,
    errors: Dict[str, List[str]],
) -> str:
    """Construit un prompt de correction ciblé à partir des erreurs de validation."""
    error_details = []
    fields_to_redo = []
    for field_id, errs in errors.items():
        # Extraire le field_id de base (sans suffixe __N)
        base_id = field_id.split("__")[0] if "__" in field_id else field_id
        fields_to_redo.append(base_id)
        for err in errs:
            error_details.append(f"- {err}")

    error_block = "\n".join(error_details)
    fields_block = ", ".join(sorted(set(fields_to_redo)))

    return f"""{original_prompt}

## CORRECTION REQUISE

Ta réponse précédente contenait des incohérences détectées automatiquement :

{error_block}

Ré-extrais en priorité les champs suivants en corrigeant les erreurs : {fields_block}

Causes fréquentes d'erreur :
1. DOUBLE COMPTAGE : un même montant apparaît dans 2 postes différents (ex: immobilisations_financieres ET autres_actif_residuel).
2. MAUVAISE COLONNE : confusion entre Brut et Net, ou entre N et N-1.
3. TOTAL vs DÉTAIL : le total d'une section est mis dans un poste de détail au lieu du sous-total.

Vérifie que la somme de tes postes de détail est cohérente avec le total affiché (tolérance 5%).
Tu peux retourner soit le JSON complet, soit uniquement les champs corrigés.
"""


def run(project_id: str):
    project_dir = OUTPUT_DIR / project_id
    manifest_path = project_dir / "manifest.json"
    docs_dir = project_dir / "documents"

    if not manifest_path.exists():
        logger.error(f"Manifest introuvable : {manifest_path}")
        return

    # Charger manifest + questions
    with open(manifest_path) as f:
        manifest = json.load(f)

    questions_path = ROOT_DIR / "config" / "questions.json"
    with open(questions_path) as f:
        questions_config = json.load(f)
    all_fields = [
        f for f in questions_config["fields"]
        if isinstance(f, dict) and f.get("field_id")
    ]

    selected_audit_folder = manifest.get("selected_audit_folder")
    if selected_audit_folder:
        logger.info(f"  🧭 Dossier audit sélectionné : {selected_audit_folder}")

    # Séparer les champs classiques (1 réponse) des champs dynamiques
    global_fields = [
        f for f in all_fields
        if not _is_per_person_field(f) and not _is_per_company_field(f)
    ]
    person_fields = [f for f in all_fields if _is_per_person_field(f)]
    company_fields = [f for f in all_fields if _is_per_company_field(f)]

    # Init LLM
    call_llm, model_name = _get_llm_client()

    # Résultats globaux : field_id -> valeur (première réponse non-null gagne)
    results: Dict[str, Optional[str]] = {f["field_id"]: None for f in global_fields}
    global_field_ids = {f["field_id"] for f in global_fields}
    asked_global_ids: set[str] = set()
    asked_person_keys: set[str] = set()
    asked_company_keys: set[str] = set()
    # Per-person : regrouper par dossier parent (1 onglet par dossier, pas par doc)
    # folder_name -> suffix index (__0, __1, ...)
    person_folder_map: Dict[str, int] = {}
    person_folder_display: Dict[str, str] = {}
    person_counter = 0
    # Per-company : regrouper par société du bilan
    company_name_map: Dict[str, int] = {}
    company_name_display: Dict[str, str] = {}
    company_counter = 0
    project_location_candidates: List[Dict] = []
    extraction_log = []
    latest_year_per_company = _build_latest_year_per_company_folder(
        manifest["files"],
        company_fields,
        selected_audit_folder,
    )
    if latest_year_per_company:
        logger.info(
            "  📆 Bilan le plus récent par société : %s",
            ", ".join(
                f"{folder}={year}"
                for folder, year in sorted(latest_year_per_company.items())
            ),
        )

    logger.info(f"Extraction structurée : {len(manifest['files'])} documents, "
                f"{len(global_fields)} champs globaux, {len(person_fields)} champs per-person, "
                f"{len(company_fields)} champs per-company")

    for doc_info in manifest["files"]:
        document_id = doc_info["document_id"]
        filename = doc_info["filename"]
        source_path = doc_info["source_path"]
        doc_path = docs_dir / f"{document_id}.jsonl"

        if not doc_path.exists():
            logger.warning(f"  ⚠️ JSONL manquant : {document_id}")
            continue

        # --- Champs globaux (Opérateur) : première réponse non-null gagne ---
        matched_global = match_questions_to_doc(doc_info, global_fields, selected_audit_folder)
        unanswered_global = [f for f in matched_global if results[f["field_id"]] is None]

        # --- Champs per-person (patrimoine) : groupés par dossier sous 3. RH ---
        matched_person = match_questions_to_doc(doc_info, person_fields, selected_audit_folder)
        # --- Champs per-company (bilan) : groupés par société extraite du document ---
        matched_company = match_questions_to_doc(doc_info, company_fields, selected_audit_folder)
        if matched_company and latest_year_per_company:
            doc_year = _get_doc_financial_year(doc_info)
            if doc_year is not None:
                folder = _extract_company_folder_from_source_path(source_path)
                folder_key = canonical_name(folder) if folder else None
                if folder_key and folder_key in latest_year_per_company:
                    max_year = latest_year_per_company[folder_key]
                    if doc_year < max_year:
                        logger.info(
                            f"  ⏭️  Doc financier ignoré (année {doc_year} < {max_year}"
                            f" pour '{folder}') : {filename}"
                        )
                        matched_company = []

        person_folder = None
        if matched_person:
            person_folder = _extract_person_folder_from_source_path(source_path)
            if not person_folder:
                logger.info(f"  🗂️  Ignoré (hors sous-dossier 3. RH ou old): {source_path}")
                matched_person = []

        if not unanswered_global and not matched_person and not matched_company:
            continue

        asked_global_ids.update(f["field_id"] for f in unanswered_global)

        person_suffix = None
        if matched_person:
            folder_key = _normalize(person_folder)
            if folder_key not in person_folder_map:
                person_folder_map[folder_key] = person_counter
                person_folder_display[folder_key] = person_folder
                person_counter += 1
                logger.info(f"    👤 Nouveau dossier personne : {person_folder} → __"
                           f"{person_folder_map[folder_key]}")
            person_suffix = f"__{person_folder_map[folder_key]}"
            asked_person_keys.update(f["field_id"] + person_suffix for f in matched_person)

        # Quelles questions poser ?
        questions_to_ask = unanswered_global + matched_person + matched_company

        # Charger le texte (filtré par pertinence + budget)
        broad_financial_context = _needs_broad_financial_context(questions_to_ask)
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
        token_est = doc_info.get("token_estimate", 0)
        chars_sent = len(doc_text)

        logger.info(
            f"  📄 {filename} ({token_est} tok orig → {chars_sent} chars envoyés"
            f"{' en mode financier élargi' if broad_financial_context else ''}) "
            f"→ {len(unanswered_global)} globales + {len(matched_person)} per-person + "
            f"{len(matched_company)} per-company"
        )

        # Appel LLM
        prompt = build_prompt(doc_text, questions_to_ask, filename, source_path)
        try:
            raw_response = _call_llm_with_retry(call_llm, prompt)
        except Exception as e:
            logger.error(f"    ❌ Erreur LLM : {e}")
            extraction_log.append({
                "document_id": document_id, "filename": filename,
                "error": str(e)[:100],
            })
            continue

        if raw_response is None:
            extraction_log.append({
                "document_id": document_id, "filename": filename,
                "error": "max retries exceeded (429)",
            })
            time.sleep(5)
            continue

        # Parser le JSON
        try:
            answers = json.loads(raw_response)
        except json.JSONDecodeError:
            json_match = re.search(r'```json\s*(.*?)\s*```', raw_response, re.DOTALL)
            if json_match:
                answers = json.loads(json_match.group(1))
            else:
                logger.error(f"    ❌ Réponse non-JSON : {raw_response[:100]}")
                answers = {}

        # --- Mapping postes bruts → clés canoniques ---
        if answers and broad_financial_context:
            for fid in ("bilan_actif_table", "bilan_passif_table", "bilan_compte_resultat_table"):
                raw_val = answers.get(fid)
                if isinstance(raw_val, list):
                    mapped = _map_raw_financial_table(raw_val, fid)
                    if fid == "bilan_passif_table":
                        mapped = _post_process_passif(mapped)
                    answers[fid] = mapped
                    logger.info(
                        f"    🔄 Mapping {fid}: {len(raw_val)} lignes brutes → {len(mapped)} clés canoniques"
                    )

        # --- Validation post-LLM + retry si incohérence ---
        if answers and broad_financial_context:
            validation_errors = validate_financial_answers(answers)
            if validation_errors:
                error_count = sum(len(e) for e in validation_errors.values())
                logger.warning(
                    f"    ⚠️ {error_count} incohérence(s) détectée(s), retry avec correction..."
                )
                for field_id, errs in validation_errors.items():
                    for err in errs:
                        logger.warning(f"      → {err}")

                correction_prompt = _build_correction_prompt(prompt, answers, validation_errors)
                try:
                    corrected_response = _call_llm_with_retry(call_llm, correction_prompt)
                    if corrected_response:
                        try:
                            corrected = json.loads(corrected_response)
                        except json.JSONDecodeError:
                            json_match = re.search(
                                r'```json\s*(.*?)\s*```', corrected_response, re.DOTALL
                            )
                            corrected = json.loads(json_match.group(1)) if json_match else None

                        if corrected:
                            # Re-mapper les résultats corrigés
                            for fid in ("bilan_actif_table", "bilan_passif_table", "bilan_compte_resultat_table"):
                                raw_val = corrected.get(fid)
                                if isinstance(raw_val, list):
                                    mapped = _map_raw_financial_table(raw_val, fid)
                                    if fid == "bilan_passif_table":
                                        mapped = _post_process_passif(mapped)
                                    corrected[fid] = mapped

                            # Vérifier si la correction est meilleure
                            new_errors = validate_financial_answers(corrected)
                            new_error_count = sum(len(e) for e in new_errors.values())
                            if new_error_count < error_count:
                                logger.info(
                                    f"    ✅ Correction acceptée ({error_count} → {new_error_count} erreurs)"
                                )
                                merged_answers = dict(answers)
                                merged_answers.update(corrected)
                                answers = merged_answers
                            else:
                                logger.info(
                                    f"    ⚠️ Correction pas meilleure ({new_error_count} erreurs), "
                                    f"on garde la réponse originale"
                                )
                except Exception as e:
                    logger.warning(f"    ⚠️ Retry de correction échoué : {e}")

                time.sleep(2)

        # --- Merger les champs globaux (première valeur non-null gagne) ---
        found = 0
        for field_id, value in answers.items():
            if field_id == _PROJECT_LOCATION_FIELD_ID and _has_meaningful_value(value):
                project_location_candidates.append({
                    "document_id": document_id,
                    "filename": filename,
                    "source_path": source_path,
                    "value": _stringify_non_table_value(value),
                })
                continue
            if (
                field_id in results
                and _has_meaningful_value(value)
                and not _has_meaningful_value(results[field_id])
            ):
                results[field_id] = _stringify_non_table_value(value)
                found += 1

        # --- Stocker les champs per-person avec suffixe __N ---
        # Regrouper par sous-dossier 3. RH : tous les docs d'un même dossier → même suffixe
        if matched_person and person_suffix:
            suffix = person_suffix
            for f in matched_person:
                fid = f["field_id"]
                value = answers.get(fid)
                if _has_meaningful_value(value):
                    key = fid + suffix
                    # Première valeur non-null gagne (comme pour les globaux)
                    if key in results and _has_meaningful_value(results[key]):
                        continue
                    if isinstance(value, list):
                        if f.get("type") == "table":
                            results[key] = json.dumps(value, ensure_ascii=False)
                        else:
                            results[key] = _stringify_non_table_value(value)
                    else:
                        results[key] = _stringify_non_table_value(value)
                    found += 1

        # --- Stocker les champs per-company avec suffixe __N ---
        if matched_company:
            raw_company_name = (answers.get("bilan_societe_nom") or "").strip()
            fallback_company_name = Path(filename).stem.strip()
            company_name = raw_company_name or fallback_company_name
            company_key = _normalize(company_name)

            if company_key and company_key not in company_name_map:
                company_name_map[company_key] = company_counter
                company_name_display[company_key] = company_name
                company_counter += 1
                logger.info(f"    🏢 Nouvelle société bilan : {company_name} → __"
                            f"{company_name_map[company_key]}")

            if company_key:
                suffix = f"__{company_name_map[company_key]}"
                asked_company_keys.update(f["field_id"] + suffix for f in matched_company)
                for f in matched_company:
                    fid = f["field_id"]
                    value = answers.get(fid)
                    if _has_meaningful_value(value):
                        key = fid + suffix
                        if key in results and _has_meaningful_value(results[key]):
                            continue
                        if isinstance(value, list):
                            if f.get("type") == "table":
                                results[key] = json.dumps(value, ensure_ascii=False)
                            else:
                                results[key] = _stringify_non_table_value(value)
                        else:
                            results[key] = _stringify_non_table_value(value)
                        found += 1

        extraction_log.append({
            "document_id": document_id,
            "filename": filename,
            "questions_asked": len(questions_to_ask),
            "answers_found": found,
        })
        logger.info(f"    ✅ {found}/{len(questions_to_ask)} réponses trouvées")

        # Rate limiting
        time.sleep(2)

    resolved_project_location = _resolve_project_location(project_location_candidates)
    if resolved_project_location:
        results[_PROJECT_LOCATION_FIELD_ID] = resolved_project_location
        logger.info(
            "  📍 Localisation projet corroborée sur 2+ documents : %s",
            resolved_project_location,
        )
    elif project_location_candidates:
        logger.warning(
            "  ⚠️ Localisation projet ignorée : aucune adresse précise corroborée sur 2 documents"
        )

    # --- Résumé ---
    answered_global = sum(1 for field_id in global_field_ids if results.get(field_id) is not None)
    answered_person = sum(1 for key in asked_person_keys if results.get(key) is not None)
    answered_company = sum(1 for key in asked_company_keys if results.get(key) is not None)
    answered = answered_global + answered_person + answered_company
    total = len(asked_global_ids) + len(asked_person_keys) + len(asked_company_keys)
    logger.info("=" * 60)
    logger.info(
        f"Extraction terminée : {answered}/{total} champs remplis "
        f"({person_counter} personne(s) détectée(s), {company_counter} bilan(s) détecté(s))"
    )

    # Mapping suffixe -> nom de dossier affichable (pour nommer les onglets)
    folder_suffix_map = {
        f"__{idx}": person_folder_display.get(folder_key, folder_key)
        for folder_key, idx in person_folder_map.items()
    }
    company_suffix_map = {
        f"__{idx}": company_name_display.get(company_key, company_key)
        for company_key, idx in company_name_map.items()
    }

    # Sauvegarder les résultats bruts
    results_path = project_dir / "extraction_results.json"
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump({
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
        }, f, ensure_ascii=False, indent=2)
    logger.info(f"  {results_path}")

    # Afficher les résultats
    for field_id, value in results.items():
        status = "✅" if value else "❓"
        display = (value[:60] if value else "null")
        logger.info(f"  {status} {field_id}: {display}")

    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(message)s",
    )

    parser = argparse.ArgumentParser(description="Extraction structurée via LLM")
    parser.add_argument("--project", "-p", required=True, help="project_id")
    args = parser.parse_args()

    run(args.project)
