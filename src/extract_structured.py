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
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

# Permettre l'exécution directe : python src/extract_structured.py
_SRC_DIR = Path(__file__).parent.resolve()
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from excel_filler import fill_excel_template
from runtime_config import configure_environment

ROOT_DIR = _SRC_DIR.parent.resolve()
configure_environment(ROOT_DIR)

logger = logging.getLogger(__name__)

OUTPUT_DIR = ROOT_DIR / "output"

# Budget texte envoyé au LLM par document (réduit le coût API)
MAX_CHARS = 12_000          # ~3 000 tokens
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
def _normalize(text: str) -> str:
    """Normalise un chemin pour le matching : minuscules, sans accents, sans espaces multiples."""
    import unicodedata
    import re
    text = text.lower().strip()
    # Supprimer les accents (é→e, è→e, etc.)
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    # Normaliser séparateurs : underscores → espaces, multiples espaces → un
    text = text.replace("_", " ").replace("-", " ")
    # Normaliser les dossiers numérotés : "3.RH" → "3. rh", "2.Audit" → "2. audit"
    text = re.sub(r"(\d)\.\s*", r"\1. ", text)
    # Tolérer les espaces parasites autour des / dans les chemins
    text = re.sub(r"\s*/\s*", "/", text)
    text = re.sub(r"\s+", " ", text)
    return text


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
        words = _normalize(candidate).split()
        if words and all(w in norm_filename for w in words):
            return True

    return False


def match_questions_to_doc(doc_info: Dict, fields: List[Dict],
                          latest_folder: Optional[str] = None) -> List[Dict]:
    """Retourne les questions pertinentes pour un document.

     Logique :
     1. Si ``source_dirs`` est défini, le source_path DOIT commencer par
         un des dossiers (matching normalisé : sans accents, casse, espaces).
     2. Ensuite le filtre classique par ``hint_keywords`` s'applique.
     Le matching ``source_doc_name`` est volontairement délégué au LLM
     (prompt-only) pour gérer les variantes et fautes de frappe.
    """
    filename = doc_info.get("filename", "").lower()
    source_path = doc_info.get("source_path", "").lower()

    # Versions normalisées pour le matching souple
    norm_path = _normalize(source_path)
    norm_filename = _normalize(filename)

    matched = []
    for field in fields:
        # 1. Filtre par dossier source (matching normalisé)
        dirs = field.get("source_dirs")
        if dirs:
            resolved_dirs = []
            for d in dirs:
                if "{latest_folder}" in d and latest_folder:
                    resolved_dirs.append(d.replace("{latest_folder}", latest_folder))
                elif "{latest_folder}" not in d:
                    resolved_dirs.append(d)
            if not any(norm_path.startswith(_normalize(d)) for d in resolved_dirs):
                continue

        # 2. Filtre par mots-clés
        keywords = field.get("hint_keywords", [])
        if any(_normalize(kw) in f"{norm_filename} {norm_path}" for kw in keywords):
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


def _is_old_source_path(source_path: str) -> bool:
    parts = [p.strip().lower() for p in source_path.replace("\\", "/").split("/")]
    return any(part in {"old", ".old", "x - old"} for part in parts)


def _extract_person_folder_from_source_path(source_path: str) -> Optional[str]:
    """Retourne le dossier personne sous 3. RH, sinon None.

    Exemples acceptés :
    - 2. Audit/1. Opérateur/3. RH/Pernod/file.pdf -> "Pernod"
    - 2. Audit/1. Opérateur/3. RH/Juliette et Florent/file.pdf -> "Juliette et Florent"

    Exclusions :
    - fichiers directement dans 3. RH (sans sous-dossier)
    - chemins old/.old
    """
    if _is_old_source_path(source_path):
        return None

    raw_parts = [p for p in source_path.replace("\\", "/").split("/") if p]
    normalized_parts = [_normalize(p) for p in raw_parts]

    try:
        idx = normalized_parts.index(_normalize("3. RH"))
    except ValueError:
        return None

    # On veut STRICTEMENT un sous-dossier de 3. RH
    if idx + 1 >= len(raw_parts) - 1:
        return None

    candidate = raw_parts[idx + 1]
    if _is_old_source_path(candidate):
        return None
    return candidate

def load_document_text(doc_path: Path) -> str:
    """Charge un JSONL document et concatène le texte des parents (sans filtrage)."""
    texts = []
    with open(doc_path, "r", encoding="utf-8") as f:
        for line in f:
            texts.append(_format_parent(json.loads(line)))
    return "\n\n---\n\n".join(texts)


def load_filtered_text(doc_path: Path, questions: List[Dict],
                       max_chars: int = MAX_CHARS) -> str:
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
            keywords.add(kw.lower())

    # Charger tous les parents
    parents: list[dict] = []
    with open(doc_path, "r", encoding="utf-8") as f:
        for line in f:
            parents.append(json.loads(line))

    # 2-3. Scorer et filtrer
    scored: list[tuple[int, dict]] = []
    for p in parents:
        text = p.get("text", "")
        if len(text) < MIN_PARENT_CHARS:
            continue
        haystack = f"{p.get('section_title', '')} {p.get('source_path', '')} {text}".lower()
        hits = sum(1 for kw in keywords if kw in haystack)
        if hits > 0:
            scored.append((hits, p))

    # 4. Trier par pertinence, construire le texte dans le budget
    scored.sort(key=lambda x: x[0], reverse=True)

    texts: list[str] = []
    total = 0
    for _, p in scored:
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

    # Résoudre le nom du dernier dossier audit (pour {latest_folder})
    latest_folder = manifest.get("latest_audit_folder")
    if latest_folder:
        logger.info(f"  🧭 Dernier dossier audit : {latest_folder}")

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
    # Per-person : regrouper par dossier parent (1 onglet par dossier, pas par doc)
    # folder_name -> suffix index (__0, __1, ...)
    person_folder_map: Dict[str, int] = {}
    person_folder_display: Dict[str, str] = {}
    person_counter = 0
    # Per-company : regrouper par société du bilan
    company_name_map: Dict[str, int] = {}
    company_name_display: Dict[str, str] = {}
    company_counter = 0
    extraction_log = []

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
        matched_global = match_questions_to_doc(doc_info, global_fields, latest_folder)
        unanswered_global = [f for f in matched_global if results[f["field_id"]] is None]

        # --- Champs per-person (patrimoine) : groupés par dossier sous 3. RH ---
        matched_person = match_questions_to_doc(doc_info, person_fields, latest_folder)
        # --- Champs per-company (bilan) : groupés par société extraite du document ---
        matched_company = match_questions_to_doc(doc_info, company_fields, latest_folder)

        person_folder = None
        if matched_person:
            person_folder = _extract_person_folder_from_source_path(source_path)
            if not person_folder:
                logger.info(f"  🗂️  Ignoré (hors sous-dossier 3. RH ou old): {source_path}")
                matched_person = []

        if not unanswered_global and not matched_person and not matched_company:
            continue

        # Quelles questions poser ?
        questions_to_ask = unanswered_global + matched_person + matched_company

        # Charger le texte (filtré par pertinence + budget)
        doc_text = load_filtered_text(doc_path, questions_to_ask)
        token_est = doc_info.get("token_estimate", 0)
        chars_sent = len(doc_text)

        logger.info(f"  📄 {filename} ({token_est} tok orig → {chars_sent} chars envoyés) "
                     f"→ {len(unanswered_global)} globales + {len(matched_person)} per-person + "
                     f"{len(matched_company)} per-company")

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
            import re
            json_match = re.search(r'```json\s*(.*?)\s*```', raw_response, re.DOTALL)
            if json_match:
                answers = json.loads(json_match.group(1))
            else:
                logger.error(f"    ❌ Réponse non-JSON : {raw_response[:100]}")
                answers = {}

        # --- Merger les champs globaux (première valeur non-null gagne) ---
        found = 0
        for field_id, value in answers.items():
            if field_id in results and value is not None and results[field_id] is None:
                results[field_id] = _stringify_non_table_value(value)
                found += 1

        # --- Stocker les champs per-person avec suffixe __N ---
        # Regrouper par sous-dossier 3. RH : tous les docs d'un même dossier → même suffixe
        if matched_person:
            folder_key = _normalize(person_folder)

            if folder_key not in person_folder_map:
                person_folder_map[folder_key] = person_counter
                person_folder_display[folder_key] = person_folder
                person_counter += 1
                logger.info(f"    👤 Nouveau dossier personne : {person_folder} → __"
                           f"{person_folder_map[folder_key]}")

            suffix = f"__{person_folder_map[folder_key]}"
            for f in matched_person:
                fid = f["field_id"]
                value = answers.get(fid)
                if value is not None:
                    key = fid + suffix
                    # Première valeur non-null gagne (comme pour les globaux)
                    if key in results and results[key] is not None:
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
                for f in matched_company:
                    fid = f["field_id"]
                    value = answers.get(fid)
                    if value is not None:
                        key = fid + suffix
                        if key in results and results[key] is not None:
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

    # --- Résumé ---
    answered = sum(1 for v in results.values() if v is not None)
    total = len(results)
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
