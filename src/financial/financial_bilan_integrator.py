"""Intégration du pipeline financier bilan : PDF + Pappers → 4 colonnes.

Pour chaque document bilan (PDF) :
    1. Déduit le nom de société depuis le chemin du fichier
    2. Matche le nom au SIREN via mandats_results.json
    3. Récupère ou charge le JSON Pappers /entreprise/comptes
    4. Génère le tableau révélateur Pappers (feature keys)
    5. Exécute le pipeline PDF (first pass + second pass) avec ces feature keys
    6. Fusionne en 4 colonnes : PDF N, PDF N-1, Pappers N, Pappers N-1

Schéma de dates typique :
    - Pappers N = 2024, Pappers N-1 = 2023  (dernier exercice déposé)
    - PDF N = 2025, PDF N-1 = 2024          (dernier exercice pas encore déposé)
    → PDF N-1 ≈ Pappers N (cross-validation)
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys

_SRC_DIR = Path(__file__).parent.parent.resolve()
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from core.runtime_config import configure_environment

ROOT_DIR = _SRC_DIR.parent.resolve()
configure_environment(ROOT_DIR)

OUTPUT_DIR = ROOT_DIR / "output"

logger = logging.getLogger(__name__)


# ── Helpers nom/SIREN ─────────────────────────────────────────────────────────

def _norm_name(s: str) -> str:
    nfd = unicodedata.normalize("NFD", s.lower().strip())
    s = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9\s]", " ", s).strip()


def _derive_company_name_from_path(pdf_path: Path) -> str:
    """Tente de déduire le nom de société depuis le chemin du PDF.

    Priorité :
      1. Dossier parent direct (si différent d'un dossier générique de bilans)
      2. Nom du fichier PDF nettoyé
    """
    _BILAN_FOLDER = re.compile(
        r"etats?\s*financiers?|^comptes?$|\bcomptes?\b|^bilans?$|\bbilans?\b",
        re.IGNORECASE,
    )
    _KEYWORDS = re.compile(
        r"\b(bilan|comptes?|annuels?|liasse|etats?\s*financiers?|projet|cac|rapport|\d{4})\b",
        re.IGNORECASE,
    )

    parent = pdf_path.parent
    parent_norm = _norm_name(parent.name)
    if not _BILAN_FOLDER.search(parent_norm):
        return parent.name.strip()

    stem_clean = _KEYWORDS.sub(" ", pdf_path.stem)
    return re.sub(r"\s{2,}", " ", stem_clean).strip(" .-_")


def _load_sirens(project_id: str) -> List[dict]:
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


def _get_sirens_from_dirigeants(project_id: str) -> List[dict]:
    """Extrait le pool de {siren, nom_societe} via /recherche-dirigeants sur les casiers.

    Flux :
        1. Charge people_from_casiers.json si dispo, sinon lance extract_people_from_project
        2. Pour chaque personne → /recherche-dirigeants → entreprises associées
        3. Retourne la liste dédupliquée {siren, nom_societe}
    """
    from pappers.pappers_enrichment import _run_recherche_dirigeants, _clean_text

    project_dir = OUTPUT_DIR / project_id
    people_cache = project_dir / "people_from_casiers.json"

    # Charge ou génère la liste des personnes
    if people_cache.exists():
        data = json.loads(people_cache.read_text(encoding="utf-8"))
        people_by_folder = data.get("people_by_folder") or {}
    else:
        logger.info(f"  people_from_casiers.json absent — extraction depuis les casiers…")
        try:
            from extract_people_from_casiers import extract_people_from_project
            people_by_folder = extract_people_from_project(project_id)
            people_cache.write_text(
                json.dumps({"project_id": project_id, "people_by_folder": people_by_folder},
                           ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning(f"  Extraction casiers échouée : {e}")
            return []

    # Collecte toutes les personnes (tous dossiers confondus)
    all_people: List[dict] = []
    seen_keys: set = set()
    for people in people_by_folder.values():
        for p in people:
            key = (_clean_text(p.get("nom", "")), _clean_text(p.get("prenoms", "")).split()[0] if p.get("prenoms") else "")
            if key not in seen_keys:
                seen_keys.add(key)
                all_people.append(p)

    if not all_people:
        return []

    # /recherche-dirigeants pour chaque personne → pool de sociétés
    seen_sirens: set = set()
    candidates: List[dict] = []

    for person in all_people:
        nom = _clean_text(person.get("nom", ""))
        prenom = _clean_text(person.get("prenoms", "")).split()[0] if person.get("prenoms") else ""
        if not nom or not prenom:
            continue
        try:
            items, _ = _run_recherche_dirigeants(nom, prenom)
            for item in items:
                for company in (item.get("entreprises") or []):
                    siren = _clean_text(str(company.get("siren") or ""))
                    nom_soc = (company.get("nom_entreprise") or company.get("denomination")
                               or company.get("nom_societe") or "")
                    if siren and siren not in seen_sirens:
                        seen_sirens.add(siren)
                        candidates.append({"siren": siren, "nom_societe": nom_soc})
        except Exception as e:
            logger.warning(f"  /recherche-dirigeants échoué pour {nom} {prenom} : {e}")

    logger.info(f"  Pool dirigeants : {len(candidates)} société(s) depuis {len(all_people)} personne(s)")
    return candidates


def _search_siren_by_name(company_name: str) -> Optional[str]:
    """Appel Pappers /recherche → score fuzzy sur les résultats → SIREN le plus proche."""
    try:
        from pappers.pappers_enrichment import _api_get
        from pappers.pappers_fetch_comptes import _best_match_scored, _MATCH_THRESHOLD

        data = _api_get("/recherche", params={"q": company_name, "precision": "approximative", "par_page": 5})
        results = data.get("resultats") or []
        if not results:
            logger.info(f"  /recherche Pappers '{company_name}' : aucun résultat")
            return None

        # Normalise les résultats au format {siren, nom_societe} attendu par _best_match_scored
        candidates = [
            {
                "siren": str(r.get("siren") or "").strip(),
                "nom_societe": r.get("nom_entreprise") or r.get("denomination") or "",
            }
            for r in results
            if r.get("siren")
        ]

        best, score = _best_match_scored(company_name, candidates)
        if best and score >= _MATCH_THRESHOLD:
            logger.info(
                f"  /recherche Pappers '{company_name}' → {best['siren']} "
                f"({best['nom_societe']}) score={score:.2f}"
            )
            return best["siren"]

        # Score trop faible sur tous les candidats → log et abandon
        names = [c["nom_societe"] for c in candidates]
        logger.warning(
            f"  /recherche Pappers '{company_name}' : aucun candidat assez proche "
            f"(meilleur score={score:.2f}, candidats={names})"
        )
    except Exception as e:
        logger.warning(f"  /recherche Pappers échoué pour '{company_name}' : {e}")
    return None


def _best_siren_match(company_name: str, sirens: List[dict]) -> Optional[str]:
    """Retourne le SIREN le mieux scoré depuis mandats, ou None si trop bas."""
    from pappers.pappers_fetch_comptes import _best_match_scored, _LLM_FALLBACK_THRESHOLD

    best, score = _best_match_scored(company_name, sirens)
    if best and score >= _LLM_FALLBACK_THRESHOLD:
        logger.info(f"  SIREN match '{company_name}' → {best['siren']} ({best['nom_societe']}) score={score:.2f}")
        return best["siren"]

    # LLM fallback
    try:
        from pappers.pappers_fetch_comptes import _llm_match
        llm = _llm_match([company_name], sirens)
        siren_val = llm.get(company_name)
        if siren_val:
            logger.info(f"  SIREN LLM '{company_name}' → {siren_val}")
            return siren_val
    except Exception as e:
        logger.warning(f"LLM SIREN fallback échoué : {e}")

    if best:
        logger.warning(f"  SIREN match faible '{company_name}' → {best['siren']} score={score:.2f} (utilisé quand même)")
        return best["siren"]
    logger.warning(f"  Aucun SIREN trouvé pour '{company_name}' dans les mandats")
    return None


# ── Pappers JSON ──────────────────────────────────────────────────────────────

def _get_pappers_json(siren: str, project_output_dir: Path) -> Optional[Path]:
    """Retourne le chemin du JSON Pappers comptes, en le cherchant dans output/."""
    candidates = [
        OUTPUT_DIR / f"entreprise_comptes_{siren}.json",
        project_output_dir / f"entreprise_comptes_{siren}.json",
    ]
    for p in candidates:
        if p.exists():
            return p

    logger.info(f"  Pas de JSON Pappers pour SIREN={siren}, tentative de récupération via API…")
    try:
        from pappers.pappers_fetch_comptes import fetch_comptes
        from pappers.pappers_enrichment import PappersError
        data = fetch_comptes(siren)
        out = OUTPUT_DIR / f"entreprise_comptes_{siren}.json"
        out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"  Pappers JSON sauvegardé : {out.name}")
        return out
    except Exception as e:
        logger.error(f"  Impossible de récupérer les comptes Pappers pour {siren} : {e}")
        return None


# ── Révélateur ────────────────────────────────────────────────────────────────

def _find_pappers_year(pappers_json_path: Path, pdf_year: Optional[int]) -> Optional[str]:
    """Cherche la meilleure année Pappers : N-1 → N-2 → N-3 par rapport au PDF.

    Si pdf_year est inconnu, retourne None (le révélateur prendra entry_index=0).
    """
    try:
        from pappers.pappers_comptes_flatten import flatten_pappers_comptes
        payload = json.loads(pappers_json_path.read_text(encoding="utf-8"))
        compact = flatten_pappers_comptes(payload, source_path=pappers_json_path, detailed=False)
        available_years = {int(e["y"]) for e in (compact.get("entries") or []) if e.get("y")}
    except Exception as e:
        logger.warning(f"  Impossible de lire les années Pappers : {e}")
        return None

    if not available_years:
        return None

    if pdf_year is None:
        best = str(max(available_years))
        logger.info(f"  Années Pappers disponibles : {sorted(available_years)} — année PDF inconnue, retenu : {best}")
        return best

    for offset in (1, 2, 3):
        candidate = pdf_year - offset
        if candidate in available_years:
            logger.info(
                f"  Années Pappers disponibles : {sorted(available_years)} — "
                f"PDF N={pdf_year}, Pappers retenu : {candidate} (N-{offset})"
            )
            return str(candidate)

    # Aucun des N-1/N-2/N-3 trouvé → on prend le plus récent disponible
    best = str(max(available_years))
    logger.warning(
        f"  Aucune année N-1/N-2/N-3 trouvée pour PDF N={pdf_year} "
        f"(disponibles : {sorted(available_years)}) — fallback sur {best}"
    )
    return best


def _get_or_run_revelateur(
    pappers_json_path: Path,
    siren: str,
    project_output_dir: Path,
    pdf_year: Optional[int] = None,
) -> Optional[Path]:
    """Retourne le JSON révélateur, en sélectionnant l'année N-1/N-2/N-3 du PDF."""
    pappers_year = _find_pappers_year(pappers_json_path, pdf_year)
    year_slug = pappers_year or "latest"
    revealing_path = project_output_dir / f"bilan_{siren}_{year_slug}_revealing.json"

    if revealing_path.exists():
        logger.info(f"  Révélateur déjà présent : {revealing_path.name}")
        return revealing_path

    logger.info(f"  Génération du révélateur SIREN={siren} année={pappers_year}…")
    try:
        from pappers.pappers_comptes_revelateur import run as run_revelateur
        run_revelateur(
            pappers_json_path,
            year=pappers_year,
            output_json=revealing_path,
        )
        if revealing_path.exists():
            logger.info(f"  Révélateur généré : {revealing_path.name}")
            return revealing_path
        logger.error("  Le révélateur n'a pas produit de fichier JSON")
        return None
    except Exception as e:
        logger.error(f"  Erreur révélateur SIREN={siren} : {e}", exc_info=True)
        return None


# ── Merge 4 colonnes ──────────────────────────────────────────────────────────

def _merge_4_columns(
    pdf_parsed: Dict[str, Any],
    pappers_table: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    """Fusionne les données PDF et Pappers en 4 colonnes par label."""
    SECTIONS = ("actif", "passif", "compte_resultat")
    result: Dict[str, List] = {}

    for section in SECTIONS:
        pdf_rows = pdf_parsed.get(section) or []
        pappers_rows = (pappers_table.get("tableau_revelateur") or pappers_table).get(section) or []

        pappers_lookup = {r["label"]: r for r in pappers_rows if isinstance(r, dict) and r.get("label")}

        merged = []
        for row in pdf_rows:
            label = str(row.get("label") or "").strip()
            if not label:
                continue
            pap = pappers_lookup.get(label, {})
            merged.append({
                "label": label,
                "pdf_n":     row.get("n"),
                "pdf_n1":    row.get("n1"),
                "pappers_n": pap.get("n"),
                "pappers_n1": pap.get("n1"),
            })

        # Ajoute les lignes Pappers absentes du PDF (en fin de section)
        pdf_labels = {r["label"] for r in merged}
        for pap_row in pappers_rows:
            lbl = str(pap_row.get("label") or "").strip()
            if lbl and lbl not in pdf_labels:
                merged.append({
                    "label": lbl,
                    "pdf_n":     None,
                    "pdf_n1":    None,
                    "pappers_n":  pap_row.get("n"),
                    "pappers_n1": pap_row.get("n1"),
                })

        result[section] = merged

    return result


# ── Point d'entrée principal ──────────────────────────────────────────────────

def run_for_pdf(
    pdf_path: Path,
    project_id: str,
    *,
    company_name: Optional[str] = None,
    target_year: Optional[str] = None,
    pdf_year: Optional[int] = None,
    output_dir: Optional[Path] = None,
    first_pass_output_json: Optional[Path] = None,
    second_pass_output_json: Optional[Path] = None,
) -> Dict[str, Any]:
    """Pipeline complet pour un PDF bilan.

    Returns:
        {
          "company": str,
          "siren": str | None,
          "sections": {"actif": [...], "passif": [...], "compte_resultat": [...]},
          "dates": {"pdf_n": ..., "pdf_n1": ..., "pappers_n": ..., "pappers_n1": ...},
          "pdf_path": str,
        }
    """
    pdf_path = pdf_path.resolve()
    project_output_dir = output_dir or (OUTPUT_DIR / project_id)
    project_output_dir.mkdir(parents=True, exist_ok=True)

    resolved_company = company_name or _derive_company_name_from_path(pdf_path)
    logger.info(f"[bilan_integrator] PDF={pdf_path.name} société='{resolved_company}'")

    # 1. Trouver le SIREN — 3 niveaux de fallback
    sirens = _load_sirens(project_id)
    siren = None

    if sirens:
        # Niveau 1 : mandats_results.json (le plus complet)
        siren = _best_siren_match(resolved_company, sirens)

    if not siren:
        # Niveau 2 : /recherche-dirigeants sur les casiers judiciaires
        logger.info(f"  Fallback /recherche-dirigeants pour '{resolved_company}'")
        dirigeant_pool = _get_sirens_from_dirigeants(project_id)
        if dirigeant_pool:
            siren = _best_siren_match(resolved_company, dirigeant_pool)

    if not siren:
        # Niveau 3 : /recherche Pappers direct par nom de société
        logger.info(f"  Fallback /recherche par nom pour '{resolved_company}'")
        siren = _search_siren_by_name(resolved_company)

    # 2. Pappers JSON
    pappers_json_path = _get_pappers_json(siren, project_output_dir) if siren else None

    # 3. Révélateur (sélection N-1/N-2/N-3 par rapport à l'année PDF)
    resolved_pdf_year = pdf_year or (int(target_year) if target_year and target_year.isdigit() else None)
    revealing_json_path = (
        _get_or_run_revelateur(pappers_json_path, siren, project_output_dir, pdf_year=resolved_pdf_year)
        if pappers_json_path
        else None
    )

    # 4. Pipeline PDF
    from financial.financial_pdf_pipeline import run as run_pdf_pipeline

    pdf_siren_slug = siren or "unknown"
    pdf_stem = pdf_path.stem
    _first_out = first_pass_output_json or (project_output_dir / f"bilan_{pdf_siren_slug}_{pdf_stem}_pass1.json")
    _second_out = second_pass_output_json or (project_output_dir / f"bilan_{pdf_siren_slug}_{pdf_stem}_pass2.json")

    pdf_result = run_pdf_pipeline(
        pdf_path=pdf_path,
        revealing_json_path=revealing_json_path or _get_dummy_revealing_path(project_output_dir, pdf_siren_slug),
        title=resolved_company,
        target_year=target_year,
        first_pass_output_json=_first_out,
        second_pass_output_json=_second_out,
    )

    pdf_parsed = pdf_result["second_pass"]["parsed"]
    pdf_exercise = (pdf_result["first_pass"].get("parsed") or {}).get("exercise") or {}

    # 5. Données Pappers (révélateur)
    pappers_table: Dict[str, Any] = {}
    pappers_dates: Dict[str, Any] = {}
    if revealing_json_path and revealing_json_path.exists():
        rev = json.loads(revealing_json_path.read_text(encoding="utf-8"))
        pappers_table = rev
        ex = rev.get("exercise") or {}
        pappers_dates = {
            "pappers_n":  ex.get("date_cloture"),
            "pappers_n1": ex.get("date_cloture_n1"),
        }

    # 6. Merge 4 colonnes
    sections = _merge_4_columns(pdf_parsed, pappers_table)

    return {
        "company":  resolved_company,
        "siren":    siren,
        "sections": sections,
        "dates": {
            "pdf_n":     pdf_exercise.get("date_cloture"),
            "pdf_n1":    pdf_exercise.get("date_cloture_n1"),
            **pappers_dates,
        },
        "pdf_path": str(pdf_path),
    }


def _get_dummy_revealing_path(output_dir: Path, slug: str) -> Path:
    """Crée un JSON révélateur vide si Pappers n'est pas disponible."""
    dummy = output_dir / f"bilan_{slug}_revealing_empty.json"
    if not dummy.exists():
        dummy.write_text(
            json.dumps({"tableau_revelateur": {"actif": [], "passif": [], "compte_resultat": []}},
                       ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return dummy


# ── Sélection du PDF le plus récent par société ───────────────────────────────

def _extract_year_from_path(pdf_path: Path) -> Optional[int]:
    """Extrait l'année (4 chiffres) depuis le nom du fichier ou du dossier parent."""
    for text in (pdf_path.stem, pdf_path.parent.name):
        m = re.search(r"\b(20\d{2})\b", text)
        if m:
            return int(m.group(1))
    return None


def _company_root(pdf_path: Path, bilan_dir: Path) -> Path:
    """Retourne le dossier société = enfant direct de bilan_dir sur le chemin du PDF.

    Ex: bilan_dir=Comptes, pdf=Comptes/YING/2019/bilan.pdf → Comptes/YING
    Si le PDF est directement dans bilan_dir → bilan_dir lui-même.
    """
    try:
        rel = pdf_path.relative_to(bilan_dir)
    except ValueError:
        return pdf_path.parent
    parts = rel.parts
    if len(parts) <= 1:
        return bilan_dir
    return bilan_dir / parts[0]


def _select_latest_pdf_per_company(
    pdf_paths: List[Path],
    bilan_dirs: List[Path],
) -> List[tuple]:
    """Retourne [(pdf_path, year_or_None)] — un seul PDF par société.

    Regroupement : dossier société = enfant direct du dossier bilan.
    Gère les sous-dossiers d'années (ex: YING/2019/) en les rattachant à YING.
    Si plusieurs PDFs pour une société, garde celui avec l'année max.
    Si aucune année détectable, garde le plus récemment modifié.
    """
    from collections import defaultdict

    # Pour chaque PDF, trouver le bilan_dir dont il est descendant
    def _find_bilan_dir(p: Path) -> Optional[Path]:
        for bd in bilan_dirs:
            try:
                p.relative_to(bd)
                return bd
            except ValueError:
                continue
        return None

    groups: dict = defaultdict(list)
    for p in pdf_paths:
        bd = _find_bilan_dir(p)
        company_root = _company_root(p, bd) if bd else p.parent
        groups[company_root.resolve()].append(p)

    selected = []
    for company_dir, paths in groups.items():
        with_year = [(p, _extract_year_from_path(p)) for p in paths]
        dated = [(p, y) for p, y in with_year if y is not None]
        if dated:
            best_path, best_year = max(dated, key=lambda t: t[1])
        else:
            best_path = max(paths, key=lambda p: p.stat().st_mtime)
            best_year = None
        selected.append((best_path, best_year))
        if len(paths) > 1:
            skipped = [p.name for p in paths if p != best_path]
            logger.info(
                f"  [{Path(company_dir).name}] PDF retenu : {best_path.name} "
                f"(année={best_year}) — ignorés : {skipped}"
            )

    return selected


# ── Scan de dossier + multi-PDF ───────────────────────────────────────────────

def run_for_project(
    project_id: str,
    *,
    cache_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """Exécute le pipeline bilan pour le PDF le plus récent par société.

    Logique d'années :
        - PDF N   : dernière année du bilan Dropbox (pas encore déposée sur Pappers)
        - Pappers : entrée la plus récente disponible (= N-1 par rapport au PDF)
    """
    from pappers.pappers_fetch_comptes import _is_bilan_folder

    cache_dir = ROOT_DIR / "cache"
    if cache_path is None:
        raizers_cache = cache_dir / "RAIZERS - En audit"
        slug_to_folder = {
            p.name.lower().replace(" ", "-"): p
            for p in raizers_cache.iterdir()
            if p.is_dir()
        }
        operation_name = project_id.replace("raizers-en-audit-", "").replace("-", " ")
        cache_path = next(
            (v for k, v in slug_to_folder.items() if operation_name in k),
            None,
        )

    if not cache_path or not cache_path.exists():
        logger.warning(f"Cache introuvable pour project_id='{project_id}'")
        return []

    # Collecte tous les PDFs dans les dossiers bilan + mémorise les bilan_dirs
    all_pdfs: List[Path] = []
    bilan_dirs: List[Path] = []
    for bilan_dir in cache_path.rglob("*"):
        if not bilan_dir.is_dir() or not _is_bilan_folder(bilan_dir):
            continue
        pdfs = list(bilan_dir.rglob("*.pdf"))
        if pdfs:
            bilan_dirs.append(bilan_dir.resolve())
            all_pdfs.extend(pdfs)

    if not all_pdfs:
        logger.warning(f"Aucun PDF trouvé dans les dossiers bilan de {cache_path}")
        return []

    # Ne garder qu'un PDF par société (le plus récent), groupé par dossier société
    selected = _select_latest_pdf_per_company(all_pdfs, bilan_dirs)
    logger.info(
        f"[bilan_integrator] {len(selected)} société(s) — "
        f"{len(all_pdfs)} PDF(s) trouvés, {len(selected)} retenus (1 par société, année N max)"
    )

    results = []
    for pdf_path, detected_year in selected:
        target_year = str(detected_year) if detected_year else None
        try:
            result = run_for_pdf(
                pdf_path,
                project_id,
                target_year=target_year,
                pdf_year=detected_year,
                output_dir=output_dir,
            )
            result["detected_year"] = detected_year
            results.append(result)
        except Exception as e:
            logger.error(f"  Erreur pour {pdf_path.name} : {e}", exc_info=True)
            results.append({"pdf_path": str(pdf_path), "error": str(e)})

    return results
