import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_SRC_DIR = Path(__file__).parent.resolve()
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from core.normalization import (
    canonical_name,
    extract_person_folder,
    is_archived_path,
)
from core.runtime_config import configure_environment
from core.llm_client import get_llm_client

ROOT_DIR = _SRC_DIR.parent.resolve()
configure_environment(ROOT_DIR)

logger = logging.getLogger(__name__)
OUTPUT_DIR = ROOT_DIR / "output"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _normalize_identity_part(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", canonical_name(text))





def _fuzzy_word_match(word: str, target: str, threshold: float = 0.78) -> bool:
    if target in word or word in target:
        return True
    from difflib import SequenceMatcher
    return SequenceMatcher(None, word, target).ratio() >= threshold


def _is_casier_judiciaire_filename(filename: str) -> bool:
    norm = canonical_name(filename)
    words = norm.split()

    has_casier = any(_fuzzy_word_match(w, "casier") for w in words)
    has_judiciaire = any(_fuzzy_word_match(w, "judiciaire") for w in words)
    has_bulletin = "bulletin" in norm
    has_n3 = any(token in norm for token in ["n3", "n 3", "n°3", "numero 3", "num 3"])

    if has_casier and has_judiciaire:
        return True
    if has_bulletin and has_n3:
        return True

    return False


def _format_parent(parent: dict) -> str:
    title = parent.get("section_title", "")
    text = parent.get("text", "")
    if title and not text.startswith(f"## {title}"):
        return f"## {title}\n{text}"
    return text


def load_document_text(doc_path: Path, max_chars: int = 5000) -> str:
    blocks: List[str] = []
    total = 0

    with open(doc_path, "r", encoding="utf-8") as f:
        for line in f:
            parent = json.loads(line)
            block = _format_parent(parent).strip()
            if not block:
                continue

            remaining = max_chars - total
            if remaining <= 0:
                break

            if len(block) > remaining:
                block = block[:remaining]

            blocks.append(block)
            total += len(block)

            if total >= max_chars:
                break

    return "\n\n---\n\n".join(blocks)


def _build_casier_identity_prompt(doc_text: str, filename: str) -> str:
    return f"""Tu analyses un document RH.

Objectif :
1. Déterminer si ce document est bien un casier judiciaire / bulletin n°3 / extrait de casier judiciaire.
2. Si oui, extraire uniquement :
   - nom
   - prenoms
   - date_naissance

Document : {filename}

Contenu :
{doc_text}

Règles :
- Sois strict.
- Ne retourne is_casier=true que si le document ressemble vraiment à un bulletin n°3 / extrait de casier judiciaire.
- nom : en MAJUSCULES
- prenoms : forme lisible, sans inventer
- date_naissance : format JJ/MM/AAAA quand elle est visible, sinon null
- Si ce n'est pas un casier, retourne seulement {{"is_casier": false}}

Réponse JSON uniquement :
- Casier :
  {{"is_casier": true, "nom": "DUPONT", "prenoms": "Jean Pierre", "date_naissance": "01/01/1970"}}
- Sinon :
  {{"is_casier": false}}
"""


def _call_llm_with_retry(call_llm, prompt: str, max_retries: int = 3) -> Optional[str]:
    for attempt in range(max_retries):
        try:
            return call_llm(prompt)
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                delay_match = re.search(r'retry(?:Delay)?["\s:]*(\d+)', err_str, re.IGNORECASE)
                wait = int(delay_match.group(1)) + 3 if delay_match else 30
                logger.warning(f"Rate limit LLM — retry {attempt + 1}/{max_retries} dans {wait}s")
                time.sleep(wait)
                continue
            raise
    return None


def _dedupe_people(people: List[dict]) -> List[dict]:
    seen = set()
    out = []

    for person in people:
        key = (
            _normalize_identity_part(person.get("nom", "")),
            _normalize_identity_part(person.get("prenoms", "")),
            (person.get("date_naissance") or "").strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(person)

    return out


# ---------------------------------------------------------------------------
# Main extraction
# ---------------------------------------------------------------------------
def extract_people_from_project(project_id: str) -> Dict[str, List[dict]]:
    project_dir = OUTPUT_DIR / project_id
    manifest_path = project_dir / "manifest.json"
    docs_dir = project_dir / "documents"

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest introuvable : {manifest_path}")
    if not docs_dir.exists():
        raise FileNotFoundError(f"Dossier documents introuvable : {docs_dir}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    _client = get_llm_client(model_override={"openai": "gpt-4o-mini", "gemini": "gemini-2.5-flash-lite"})
    call_llm = _client["text_call"]
    model_name = _client["model"]
    logger.info(f"Extraction personnes casiers pour {project_id} avec {model_name}")

    candidate_docs: List[Tuple[dict, str]] = []

    for doc_info in manifest.get("files", []):
        filename = doc_info.get("filename", "")
        source_path = doc_info.get("source_path", "")

        if is_archived_path(source_path):
            continue

        person_folder = extract_person_folder(source_path)
        if not person_folder:
            continue

        if _is_casier_judiciaire_filename(filename):
            candidate_docs.append((doc_info, person_folder))

    if not candidate_docs:
        logger.info("Aucun document candidat casier trouvé")
        return {}

    logger.info(f"{len(candidate_docs)} document(s) candidat(s) casier")

    people_by_folder: Dict[str, List[dict]] = {}

    for doc_info, folder_name in candidate_docs:
        document_id = doc_info["document_id"]
        filename = doc_info.get("filename", "")
        doc_path = docs_dir / f"{document_id}.jsonl"

        if not doc_path.exists():
            logger.warning(f"Document JSONL absent: {doc_path}")
            continue

        doc_text = load_document_text(doc_path, max_chars=5000)
        if not doc_text.strip():
            continue

        prompt = _build_casier_identity_prompt(doc_text, filename)

        try:
            raw = _call_llm_with_retry(call_llm, prompt)
            if not raw:
                continue
            answer = json.loads(raw)
        except Exception as e:
            logger.warning(f"Erreur LLM sur {filename}: {e}")
            continue

        if not answer.get("is_casier"):
            logger.info(f"{filename}: rejeté par le LLM (pas un casier)")
            continue

        nom = (answer.get("nom") or "").strip().upper()
        prenoms = (answer.get("prenoms") or "").strip()
        date_naissance = (answer.get("date_naissance") or "").strip() or None

        if not nom:
            logger.warning(f"{filename}: nom manquant, document ignoré")
            continue

        people_by_folder.setdefault(folder_name, []).append(
            {
                "nom": nom,
                "prenoms": prenoms,
                "date_naissance": date_naissance,
            }
        )

        logger.info(f"Casier retenu: {folder_name} -> {nom} {prenoms} ({date_naissance or 'date absente'})")

        # léger throttling
        time.sleep(0.5)

    # dédup finale par dossier
    people_by_folder = {
        folder: _dedupe_people(people)
        for folder, people in people_by_folder.items()
        if people
    }

    return people_by_folder


def main() -> None:
    parser = argparse.ArgumentParser(description="Extraction des personnes depuis les casiers judiciaires")
    parser.add_argument("--project", "-p", required=True, help="project_id")
    parser.add_argument("--output", default=None, help="Chemin de sortie JSON optionnel")
    args = parser.parse_args()

    people_by_folder = extract_people_from_project(args.project)

    payload = {
        "project_id": args.project,
        "people_by_folder": people_by_folder,
    }

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = OUTPUT_DIR / args.project / "people_from_casiers.json"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(f"Résultat écrit : {output_path}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(message)s",
    )
    main()
