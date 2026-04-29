"""Fetche Pappers /entreprise/comptes pour les sociétés ayant un bilan dans le cache.

Flux :
    1. Scanne les dossiers bilan du cache → noms de sociétés
    2. Matche ces noms aux SIRENs des mandats_results.json du projet
    3. Appelle /entreprise/comptes pour chaque SIREN matché
    4. Sauvegarde le JSON dans output/

Usage :
    python pappers_fetch_comptes.py --project raizers-en-audit-ying [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_SRC_DIR = Path(__file__).parent.parent.resolve()
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from core.runtime_config import configure_environment

ROOT_DIR = _SRC_DIR.parent.resolve()
configure_environment(ROOT_DIR)

from pappers.pappers_enrichment import _api_get, PappersError

logger = logging.getLogger(__name__)

CACHE_DIR = ROOT_DIR / "cache"
OUTPUT_DIR = ROOT_DIR / "output"

def _norm_folder(name: str) -> str:
    nfd = unicodedata.normalize("NFD", name.lower())
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def _is_bilan_folder(path: Path) -> bool:
    n = _norm_folder(path.name)
    return bool(re.search(r"etats?\s*financiers?|^comptes?$|\bcomptes?\b", n))


# ── Normalisation ─────────────────────────────────────────────────────────────

_LEGAL_FORMS = re.compile(
    r"\b(sas|sarl|sa|sasu|sci|sccv|snc|eurl|selarl|sca|scs|gie|scp|sel|selas|"
    r"selafa|ei|ae|llp|llc)\b",
    re.IGNORECASE,
)


def _strip_legal(s: str) -> str:
    """Supprime formes juridiques et mots génériques avant matching."""
    s = _LEGAL_FORMS.sub(" ", s)
    return re.sub(r"\s{2,}", " ", s).strip(" .-_")


def _norm(s: str) -> List[str]:
    nfd = unicodedata.normalize("NFD", s.lower().strip())
    s = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9\s]", " ", s).split()


def _compact(s: str) -> str:
    """Supprime tous les séparateurs → utile pour comparer acronymes (F.D. == FD)."""
    nfd = unicodedata.normalize("NFD", s.lower())
    s = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]", "", s)


def _similarity(a: str, b: str) -> float:
    """Score combiné : token overlap + compact + strip formes juridiques."""
    # Brut
    ta, tb = set(_norm(a)), set(_norm(b))
    token_score = len(ta & tb) / len(ta) if ta else 0.0
    ca, cb = _compact(a), _compact(b)
    compact_score = 1.0 if ca == cb else (0.8 if (ca in cb or cb in ca) else 0.0)
    raw = max(token_score, compact_score)

    # Après strip des formes juridiques
    sa, sb = _strip_legal(a), _strip_legal(b)
    ta2, tb2 = set(_norm(sa)), set(_norm(sb))
    token_score2 = len(ta2 & tb2) / len(ta2) if ta2 else 0.0
    ca2, cb2 = _compact(sa), _compact(sb)
    compact_score2 = 1.0 if ca2 == cb2 else (0.8 if (ca2 in cb2 or cb2 in ca2) else 0.0)
    stripped = max(token_score2, compact_score2)

    return max(raw, stripped)


_MATCH_THRESHOLD = 0.35
_LLM_FALLBACK_THRESHOLD = 0.55  # en dessous → on demande au LLM


def _best_match_scored(folder_name: str, candidates: List[dict]) -> Tuple[Optional[dict], float]:
    best, best_score = None, _MATCH_THRESHOLD
    for c in candidates:
        nom = c.get("nom_societe") or ""
        score = max(_similarity(folder_name, nom), _similarity(nom, folder_name))
        if score > best_score:
            best, best_score = c, score
    return best, best_score


def _llm_match(unmatched: List[str], sirens: List[dict]) -> Dict[str, Optional[str]]:
    """Appel LLM cheap pour les cas ambigus : dossier → SIREN ou null."""
    try:
        from core.llm_client import get_llm_client
        from financial.extract_structured_runtime import call_llm_with_retry, parse_json_response

        prompt = (
            "Fais correspondre chaque nom de dossier bilan à la société Pappers la plus proche.\n\n"
            f"Dossiers bilan : {json.dumps(unmatched, ensure_ascii=False)}\n\n"
            f"Sociétés Pappers : {json.dumps(sirens, ensure_ascii=False)}\n\n"
            "Retourne un JSON strict : {\"nom_dossier\": \"SIREN\" ou null}.\n"
            "Mets null si tu n'es pas sûr. N'invente pas de SIREN."
        )
        client = get_llm_client(
            model_override={"openai": "gpt-4o-mini"},
            preferred_provider="openai",
        )
        raw = call_llm_with_retry(client["text_call"], prompt)
        result = parse_json_response(raw)
        if isinstance(result, dict):
            return result
    except Exception as e:
        logger.warning(f"LLM fallback matching échoué : {e}")
    return {}


def match_companies(
    bilan_companies: Dict[str, Path],
    sirens: List[dict],
) -> List[dict]:
    """Retourne la liste des matches {folder_name, siren, nom_societe, match_method}."""
    matches = []
    needs_llm: List[str] = []
    fuzzy_results: Dict[str, Tuple[Optional[dict], float]] = {}

    for folder_name in bilan_companies:
        best, score = _best_match_scored(folder_name, sirens)
        fuzzy_results[folder_name] = (best, score)
        if best is None or score < _LLM_FALLBACK_THRESHOLD:
            needs_llm.append(folder_name)

    # LLM fallback pour les cas peu confiants
    llm_mapping: Dict[str, Optional[str]] = {}
    if needs_llm:
        logger.info(f"LLM fallback pour {len(needs_llm)} dossier(s) : {needs_llm}")
        siren_index = {c["siren"]: c for c in sirens}
        raw_llm = _llm_match(needs_llm, sirens)
        for folder, siren_val in raw_llm.items():
            if siren_val and siren_val in siren_index:
                llm_mapping[folder] = siren_val

    for folder_name, folder_path in bilan_companies.items():
        best, score = fuzzy_results[folder_name]

        if folder_name in llm_mapping:
            siren_val = llm_mapping[folder_name]
            matched = next((c for c in sirens if c["siren"] == siren_val), None)
            if matched:
                matches.append({
                    "folder_name": folder_name,
                    "folder_path": str(folder_path),
                    "siren": matched["siren"],
                    "nom_societe": matched["nom_societe"],
                    "match_method": "llm",
                })
                continue

        if best:
            matches.append({
                "folder_name": folder_name,
                "folder_path": str(folder_path),
                "siren": best["siren"],
                "nom_societe": best["nom_societe"],
                "match_method": f"fuzzy({score:.2f})",
            })
        else:
            logger.warning(f"Aucun SIREN trouvé pour '{folder_name}'")

    return matches


# ── Scan des dossiers bilan ───────────────────────────────────────────────────

def _is_bilan_folder(path: Path) -> bool:
    return bool(re.search(r"etats?\s*financiers?|^comptes?$|\bcomptes?\b", _norm_folder(path.name)))


def scan_bilan_companies(project_cache_path: Path) -> Dict[str, Path]:
    """Retourne {nom_dossier_societe: chemin_dossier}.

    Cherche les dossiers 'Etats Financiers' / 'Comptes' puis prend leurs
    sous-dossiers directs comme noms de sociétés. Si pas de sous-dossiers,
    le nom de dossier parent est utilisé.
    """
    companies: Dict[str, Path] = {}

    for bilan_dir in project_cache_path.rglob("*"):
        if not bilan_dir.is_dir() or not _is_bilan_folder(bilan_dir):
            continue
        subdirs = [d for d in bilan_dir.iterdir() if d.is_dir()]
        if subdirs:
            for subdir in subdirs:
                name = subdir.name.strip()
                if name and name not in companies:
                    companies[name] = subdir
        else:
            # Pas de sous-dossiers : extraire les noms depuis les noms de fichiers PDF
            # Ex: "ENGRENAGE bilan 2022 projet.pdf" → "ENGRENAGE"
            for pdf in bilan_dir.glob("*.pdf"):
                stem = pdf.stem
                # Supprime les mots-clés comptables et l'année pour garder le nom de société
                stem_clean = re.sub(
                    r"\b(bilan|comptes?|annuels?|liasse|etats?\s*financiers?|projet|cac|rapport|\d{4})\b",
                    " ",
                    stem,
                    flags=re.IGNORECASE,
                ).strip(" .-_")
                name = re.sub(r"\s{2,}", " ", stem_clean).strip()
                if name and name not in companies:
                    companies[name] = bilan_dir

    return companies


# ── Lecture des SIRENs depuis mandats_results.json ────────────────────────────

def load_mandats_sirens(project_id: str) -> List[dict]:
    """Retourne la liste dédupliquée de {siren, nom_societe} du projet."""
    path = OUTPUT_DIR / project_id / "mandats_results.json"
    if not path.exists():
        logger.warning(f"mandats_results.json introuvable pour {project_id}")
        return []

    data = json.loads(path.read_text(encoding="utf-8"))
    seen: set = set()
    result = []
    for persons in data.get("societes_par_personne", {}).values():
        for companies in persons.values():
            for c in companies:
                siren = str(c.get("siren") or "").strip()
                if siren and siren not in seen:
                    seen.add(siren)
                    result.append({"siren": siren, "nom_societe": c.get("nom_societe") or ""})
    return result




# ── Appel Pappers /entreprise/comptes ────────────────────────────────────────

def fetch_comptes(siren: str) -> dict:
    return _api_get("/entreprise/comptes", params={"siren": siren})


def fetch_and_save(match: dict, *, dry_run: bool = False) -> Optional[Path]:
    siren = match["siren"]
    output_path = OUTPUT_DIR / f"entreprise_comptes_{siren}.json"

    if output_path.exists():
        logger.info(f"  [SKIP] {siren} ({match['nom_societe']}) — déjà présent")
        return output_path

    if dry_run:
        logger.info(f"  [DRY-RUN] {siren} ({match['nom_societe']}) ← dossier '{match['folder_name']}'")
        return None

    try:
        data = fetch_comptes(siren)
        output_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"  [OK] {siren} ({match['nom_societe']}) → {output_path.name}")
        return output_path
    except PappersError as e:
        err_path = output_path.with_suffix(".error.json")
        err_path.write_text(json.dumps({"siren": siren, "error": str(e)}, ensure_ascii=False, indent=2))
        logger.error(f"  [ERR] {siren} ({match['nom_societe']}): {e}")
        return None


# ── Point d'entrée ────────────────────────────────────────────────────────────

def run(project_id: str, *, dry_run: bool = False) -> List[dict]:
    project_cache = CACHE_DIR / next(
        (d.name for d in CACHE_DIR.iterdir() if d.is_dir()),
        "",
    )
    raizers_cache = CACHE_DIR / "RAIZERS - En audit"

    slug_to_folder = {
        p.name.lower().replace(" ", "-"): p
        for p in raizers_cache.iterdir()
        if p.is_dir()
    }
    operation_name = project_id.replace("raizers-en-audit-", "").replace("-", " ")
    project_cache_path = next(
        (v for k, v in slug_to_folder.items() if operation_name in k),
        None,
    )

    if not project_cache_path:
        raise FileNotFoundError(f"Dossier cache introuvable pour project_id='{project_id}'")

    logger.info(f"Cache projet : {project_cache_path}")

    bilan_companies = scan_bilan_companies(project_cache_path)
    logger.info(f"Sociétés avec bilans : {list(bilan_companies.keys())}")

    sirens = load_mandats_sirens(project_id)
    logger.info(f"SIRENs mandats disponibles : {len(sirens)}")

    matches = match_companies(bilan_companies, sirens)
    logger.info(f"Matches trouvés : {len(matches)}")

    results = []
    for match in matches:
        out = fetch_and_save(match, dry_run=dry_run)
        results.append({**match, "output": str(out) if out else None})

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetche /entreprise/comptes pour les bilans du cache")
    parser.add_argument("--project", "-p", required=True, help="project_id (ex: raizers-en-audit-ying)")
    parser.add_argument("--dry-run", action="store_true", help="Affiche les matches sans appeler l'API")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
    results = run(args.project, dry_run=args.dry_run)
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
