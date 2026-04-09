import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests


PAPPERS_BASE_URL = os.environ.get("PAPPERS_BASE_URL", "https://api.pappers.fr/v2").rstrip("/")
MAX_RECHERCHE_RESULTS = 30


class PappersError(Exception):
    pass


def _get_api_key() -> str:
    api_key = (os.environ.get("PAPPERS_API_KEY") or "").strip()
    if not api_key:
        raise PappersError("PAPPERS_API_KEY manquante dans l'environnement")
    return api_key


def _api_get(path: str, params: Optional[dict] = None, timeout: int = 30) -> dict:
    url = f"{PAPPERS_BASE_URL}/{path.lstrip('/')}"
    query = dict(params or {})
    query["api_token"] = _get_api_key()

    response = requests.get(url, params=query, timeout=timeout)
    if response.status_code >= 400:
        raise PappersError(f"Pappers API error {response.status_code} on {path}: {response.text[:300]}")

    try:
        return response.json()
    except Exception as exc:
        raise PappersError(f"Réponse non JSON sur {path}") from exc


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _first_prenom(value: str) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        return ""
    return cleaned.split(" ")[0]


def _extract_results_list(data: dict) -> List[dict]:
    for key in ["resultats", "results", "entreprises"]:
        value = data.get(key)
        if isinstance(value, list):
            return value
    return []


def _extract_company_name(item: dict) -> Optional[str]:
    return (
        item.get("nom_entreprise")
        or item.get("denomination")
        or item.get("raison_sociale")
        or item.get("nom")
        or item.get("entreprise")
        or item.get("societe")
    )


def _dedupe_company_rows(rows: List[dict]) -> List[dict]:
    seen = set()
    out = []

    for row in rows:
        nom_societe = _clean_text(row.get("nom_societe", ""))
        siren = _clean_text(str(row.get("siren") or ""))

        dedupe_key = siren or nom_societe.lower()
        if not dedupe_key:
            continue
        if dedupe_key in seen:
            continue

        seen.add(dedupe_key)
        out.append({
            "nom_societe": nom_societe,
            "siren": siren or None,
            "siret": row.get("siret"),
            "entreprise_cessee": row.get("entreprise_cessee"),
            "forme_juridique": row.get("forme_juridique"),
            "statut": row.get("statut"),
            "activite": row.get("activite"),
            "date_creation": row.get("date_creation"),
            "capital": row.get("capital"),
            "chiffre_affaires": row.get("chiffre_affaires"),
            "resultat_net": row.get("resultat_net"),
            "statut_rcs": row.get("statut_rcs"),
            "nb_dirigeants_total": row.get("nb_dirigeants_total"),
        })

    return out


def _run_recherche(nom: str, prenom_dirigeant: str) -> Tuple[List[dict], dict]:
    params = {
        "nom_dirigeant": nom,
        "prenom_dirigeant": prenom_dirigeant,
    }

    data = _api_get("/recherche", params=params)
    items = _extract_results_list(data)
    print(f"[DEBUG] /recherche params={params} -> {len(items)} hit(s)")

    return items, {
        "query_params": params,
        "result_count": len(items),
    }


def _search_companies_for_person(person: dict) -> Tuple[List[dict], List[dict]]:
    nom = _clean_text(person.get("nom", ""))
    prenoms = _clean_text(person.get("prenoms", ""))
    premier_prenom = _first_prenom(prenoms)

    if not nom or not premier_prenom:
        return [], []

    debug_rows: List[dict] = []

    try:
        items, meta = _run_recherche(nom, premier_prenom)
        strategy = "nom_plus_premier_prenom"
    except Exception as e:
        print(f"[WARN] /recherche error for {nom} {premier_prenom}: {e}")
        return [], []

    if len(items) > MAX_RECHERCHE_RESULTS and prenoms and prenoms != premier_prenom:
        try:
            fallback_items, fallback_meta = _run_recherche(nom, prenoms)
            items = fallback_items
            strategy = "nom_plus_prenoms_complets"
            meta = {
                "initial_query": meta,
                "fallback_query": fallback_meta,
                "fallback_triggered": True,
            }
        except Exception as e:
            print(f"[WARN] /recherche fallback error for {nom} {prenoms}: {e}")

    company_rows: List[dict] = []

    for item in items:
        company_name = _extract_company_name(item)

        if company_name:
            company_rows.append({
                "nom_societe": _clean_text(item.get("nom_entreprise") or company_name),
                "siren": item.get("siren"),
                "siret": (item.get("siege") or {}).get("siret"),
                "statut": item.get("statut_consolide") or item.get("statut_rcs"),
                "entreprise_cessee": item.get("entreprise_cessee"),
                "activite": item.get("libelle_code_naf") or item.get("domaine_activite"),
                "date_creation": item.get("date_creation_formate") or item.get("date_creation"),
                "forme_juridique": item.get("forme_juridique"),
                "capital": item.get("capital"),
                "chiffre_affaires": item.get("chiffre_affaires"),
                "resultat_net": item.get("resultat"),
                "statut_rcs": item.get("statut_rcs"),
                "nb_dirigeants_total": item.get("nb_dirigeants_total"),
            })

        debug_rows.append(
            {
                "strategy": strategy,
                "meta": meta,
                "societe": company_name,
                "raw_keys": sorted(list(item.keys())),
                "raw_item": item,
            }
        )

    return _dedupe_company_rows(company_rows), debug_rows


def _fetch_entreprise(siren: str) -> Optional[dict]:
    """Appelle /entreprise pour obtenir les détails (représentants, bénéficiaires)."""
    if not siren or len(siren.strip()) != 9:
        return None
    try:
        return _api_get("/entreprise", params={"siren": siren.strip()})
    except PappersError as e:
        print(f"[WARN] /entreprise error for siren={siren}: {e}")
        return None


def _normalize_for_match(value: str) -> str:
    return re.sub(r"[^a-z]", "", (value or "").lower().strip())


def _extract_name_candidates(*values: Optional[str]) -> set[str]:
    """Construit des variantes normalisées pour fiabiliser le matching des noms/prénoms."""
    candidates: set[str] = set()

    for value in values:
        if not value:
            continue

        raw = str(value).strip()
        normalized = _normalize_for_match(raw)
        if normalized:
            candidates.add(normalized)

        # Découpe souple pour couvrir les formats Pappers du type "Juliette, Lola"
        for part in re.split(r"[\s,;/()-]+", raw):
            normalized_part = _normalize_for_match(part)
            if normalized_part:
                candidates.add(normalized_part)

    return candidates


def _find_role_for_person(data: dict, person: dict) -> Optional[str]:
    """Cherche le rôle de la personne dans les représentants de l'entreprise."""
    representants = data.get("representants") or []
    nom_ref = _normalize_for_match(person.get("nom", ""))
    prenom_ref = _normalize_for_match(_first_prenom(person.get("prenoms", "")))

    if not nom_ref:
        return None

    for rep in representants:
        nom_rep = _normalize_for_match(rep.get("nom") or rep.get("nom_complet") or "")
        prenom_candidates = _extract_name_candidates(
            rep.get("prenom"),
            rep.get("prenom_usuel"),
            rep.get("nom_complet"),
        )

        if nom_ref == nom_rep and (not prenom_ref or prenom_ref in prenom_candidates):
            return _clean_text(rep.get("qualite") or rep.get("fonction") or "")

    return None


def _find_detention_for_person(data: dict, person: dict) -> Optional[str]:
    """Cherche la détention dans les bénéficiaires effectifs."""
    beneficiaires = data.get("beneficiaires_effectifs") or []
    nom_ref = _normalize_for_match(person.get("nom", ""))
    prenom_ref = _normalize_for_match(_first_prenom(person.get("prenoms", "")))

    if not nom_ref:
        return None

    for benef in beneficiaires:
        nom_b = _normalize_for_match(benef.get("nom") or "")
        prenom_candidates = _extract_name_candidates(
            benef.get("prenom"),
            benef.get("prenom_usuel"),
            benef.get("nom_complet"),
        )

        if nom_ref == nom_b and (not prenom_ref or prenom_ref in prenom_candidates):
            parts = []
            detention_directe = benef.get("pourcentage_parts")
            detention_indirecte = benef.get("pourcentage_parts_indirectes")
            detention_vd = benef.get("pourcentage_votes_directs")
            if detention_directe is not None:
                parts.append(f"{detention_directe}% parts")
            if detention_indirecte is not None:
                parts.append(f"{detention_indirecte}% indirect")
            if detention_vd is not None and not parts:
                parts.append(f"{detention_vd}% votes")
            return " / ".join(parts) if parts else None

    return None


def _generate_commentaire(data: dict) -> Optional[str]:
    """Génère un commentaire uniquement en cas d'anomalie forte."""
    signals = []

    # Procédure collective
    procedures = data.get("procedures_collectives") or []
    if procedures:
        derniere = procedures[0]
        type_proc = derniere.get("type") or "procédure collective"
        signals.append(type_proc)

    # Radiation effective
    statut = (data.get("statut_rcs") or "").lower()
    if "radi" in statut:
        signals.append("Radiée du RCS")

    return " | ".join(signals) if signals else None


def _is_eponymous(company_name: str, person: dict) -> bool:
    """Détecte si la société porte le nom de la personne (SCI DUPONT, DUPONT HOLDING, etc.)."""
    nom_ref = _normalize_for_match(person.get("nom", ""))
    nom_societe = _normalize_for_match(company_name)
    if not nom_ref or not nom_societe:
        return False
    # Le nom de famille constitue l'essentiel du nom de la société
    cleaned = nom_societe.replace(nom_ref, "")
    # Ce qui reste après retrait du nom : formes juridiques, mots génériques
    filler = re.sub(r"(sci|sci|sarl|sas|sa|eurl|holding|groupe|invest|immo|gestion|conseil|patrimoine)", "", cleaned)
    return len(filler) <= 3


def _should_enrich(company: dict, person: dict) -> bool:
    """Filtre les sociétés à enrichir via /entreprise (coût x10 vs /recherche)."""
    statut_rcs = (company.get("statut_rcs") or "").lower()
    return statut_rcs == "inscrit"


def _enrich_companies_with_details(
    companies: List[dict],
    person: dict,
    entreprise_cache: Optional[Dict[str, Optional[dict]]] = None,
) -> List[dict]:
    """Enrichit chaque société avec role/detention/commentaires via /entreprise."""
    enriched = []
    skipped = 0
    for company in companies:
        siren = (company.get("siren") or "").strip()
        role = None
        detention = None
        commentaires = None

        if siren and _should_enrich(company, person):
            data = None
            if entreprise_cache is not None and siren in entreprise_cache:
                data = entreprise_cache[siren]
            else:
                data = _fetch_entreprise(siren)
                if entreprise_cache is not None:
                    entreprise_cache[siren] = data
            if data:
                role = _find_role_for_person(data, person)
                detention = _find_detention_for_person(data, person)
                commentaires = _generate_commentaire(data)
        elif siren:
            skipped += 1

        enriched.append({
            **company,
            "role": role,
            "detention": detention,
            "commentaires": commentaires,
        })

    if skipped:
        nom = _clean_text(person.get("nom", ""))
        print(f"[INFO] {nom}: {skipped} société(s) ignorée(s) pour /entreprise (cessée ou éponyme)")

    return enriched


def enrich_people(people: List[dict]) -> Tuple[Dict[str, List[dict]], Dict[str, List[dict]]]:
    results: Dict[str, List[dict]] = {}
    debug_payload: Dict[str, List[dict]] = {}
    entreprise_cache: Dict[str, Optional[dict]] = {}

    for person in people:
        nom = _clean_text(person.get("nom", ""))
        prenoms = _clean_text(person.get("prenoms", ""))
        display_name = f"{nom} {prenoms}".strip()

        companies, debug_rows = _search_companies_for_person(person)
        companies = _enrich_companies_with_details(
            companies,
            person,
            entreprise_cache=entreprise_cache,
        )
        results[display_name] = companies
        debug_payload[display_name] = debug_rows

    return results, debug_payload


def write_debug_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
