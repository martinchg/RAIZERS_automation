import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests


PAPPERS_BASE_URL = os.environ.get("PAPPERS_BASE_URL", "https://api.pappers.fr/v2").rstrip("/")


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
    for key in ["resultats", "results", "entreprises", "dirigeants"]:
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
    merged_rows: Dict[str, dict] = {}
    ordered_keys: List[str] = []

    for row in rows:
        nom_societe = _clean_text(row.get("nom_societe", ""))
        siren = _clean_text(str(row.get("siren") or ""))
        dedupe_key = siren or nom_societe.lower()
        if not dedupe_key:
            continue

        normalized_row = {
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
            "role": row.get("role"),
            "detention": row.get("detention"),
            "commentaires": row.get("commentaires"),
        }

        if dedupe_key not in merged_rows:
            merged_rows[dedupe_key] = normalized_row
            ordered_keys.append(dedupe_key)
            continue

        existing = merged_rows[dedupe_key]
        for key, value in normalized_row.items():
            if existing.get(key) in (None, "", [], {}) and value not in (None, "", [], {}):
                existing[key] = value

    return [merged_rows[key] for key in ordered_keys]


def _normalize_birthdate(value: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    text = _clean_text(str(value or ""))
    if not text:
        return None, None

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text, text[:7]
    if re.fullmatch(r"\d{4}-\d{2}", text):
        return None, text
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", text):
        day, month, year = text.split("/")
        return f"{year}-{month}-{day}", f"{year}-{month}"
    if re.fullmatch(r"\d{2}/\d{4}", text):
        month, year = text.split("/")
        return None, f"{year}-{month}"
    return None, None


def _candidate_signature(item: dict) -> tuple:
    entreprise_sirens = tuple(
        sorted(
            {
                _clean_text(str(company.get("siren") or ""))
                for company in (item.get("entreprises") or [])
                if _clean_text(str(company.get("siren") or ""))
            }
        )
    )
    return (
        _normalize_for_match(item.get("nom_complet") or ""),
        item.get("date_de_naissance") or item.get("date_de_naissance_rgpd") or "",
        entreprise_sirens,
    )


def _dedupe_dirigeant_candidates(items: List[dict]) -> List[dict]:
    seen = set()
    out = []
    for item in items:
        signature = _candidate_signature(item)
        if signature in seen:
            continue
        seen.add(signature)
        out.append(item)
    return out


def _run_recherche_dirigeants_page(nom: str, prenom_dirigeant: str, page: int = 1) -> Tuple[List[dict], dict]:
    params = {
        "nom_dirigeant": nom,
        "prenom_dirigeant": prenom_dirigeant,
        "page": page,
    }

    data = _api_get("/recherche-dirigeants", params=params)
    items = _extract_results_list(data)
    print(f"[DEBUG] /recherche-dirigeants params={params} -> {len(items)} hit(s)")

    return items, {
        "query_params": params,
        "result_count": len(items),
        "total": data.get("total"),
        "page": data.get("page"),
    }


def _run_recherche_dirigeants(nom: str, prenom_dirigeant: str) -> Tuple[List[dict], dict]:
    all_items: List[dict] = []
    page_metas: List[dict] = []
    seen_signatures = set()
    page = 1

    while True:
        items, meta = _run_recherche_dirigeants_page(nom, prenom_dirigeant, page=page)
        page_metas.append(meta)
        if not items:
            break

        new_item_count = 0
        for item in items:
            signature = _candidate_signature(item)
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            all_items.append(item)
            new_item_count += 1

        total = meta.get("total")
        if isinstance(total, int) and len(all_items) >= total:
            break
        if meta.get("result_count", 0) == 0:
            break
        if new_item_count == 0:
            break
        page += 1

    return all_items, {
        "query_params": {"nom_dirigeant": nom, "prenom_dirigeant": prenom_dirigeant},
        "page_count": len(page_metas),
        "result_count": len(all_items),
        "pages": page_metas,
    }


def _run_recherche_by_siren(siren: str) -> Tuple[Optional[dict], dict]:
    siren = _clean_text(str(siren or ""))
    if len(siren) != 9:
        return None, {"query_params": {"siren": siren}, "result_count": 0}

    params = {"siren": siren}
    data = _api_get("/recherche", params=params)
    items = _extract_results_list(data)
    exact_item = next(
        (item for item in items if _clean_text(str(item.get("siren") or "")) == siren),
        items[0] if items else None,
    )
    return exact_item, {
        "query_params": params,
        "result_count": len(items),
    }


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


def _candidate_matches_person_name(item: dict, person: dict) -> bool:
    nom_ref = _normalize_for_match(person.get("nom", ""))
    prenom_ref = _normalize_for_match(_first_prenom(person.get("prenoms", "")))
    if not nom_ref:
        return False

    nom_item = _normalize_for_match(item.get("nom") or item.get("nom_complet") or "")
    prenom_candidates = _extract_name_candidates(
        item.get("prenom"),
        item.get("prenom_usuel"),
        item.get("nom_complet"),
    )
    return nom_ref == nom_item and (not prenom_ref or prenom_ref in prenom_candidates)


def _select_dirigeant_candidates(items: List[dict], person: dict) -> Tuple[List[dict], str]:
    if not items:
        return [], "aucun_resultat"

    named_candidates = [item for item in items if _candidate_matches_person_name(item, person)]
    candidates = named_candidates or items

    target_exact, target_rgpd = _normalize_birthdate(person.get("date_naissance"))

    if target_exact:
        exact_matches = [
            item
            for item in candidates
            if _normalize_birthdate(item.get("date_de_naissance"))[0] == target_exact
        ]
        if exact_matches:
            # Pappers peut éclater un même dirigeant en plusieurs fiches :
            # une avec la date complète, d'autres avec seulement YYYY-MM.
            # Quand on a une date exacte fiable, on réagrège les fiches
            # name-matched qui partagent le même mois/année.
            if target_rgpd:
                merged_matches = list(exact_matches)
                for item in candidates:
                    if item in exact_matches:
                        continue
                    _, item_rgpd = _normalize_birthdate(item.get("date_de_naissance"))
                    if not item_rgpd:
                        _, item_rgpd = _normalize_birthdate(item.get("date_de_naissance_rgpd"))
                    if item_rgpd == target_rgpd:
                        merged_matches.append(item)
                return _dedupe_dirigeant_candidates(merged_matches), "date_de_naissance_plus_rgpd"

            return exact_matches, "date_de_naissance"

    if target_rgpd:
        rgpd_matches = []
        for item in candidates:
            item_exact, item_rgpd = _normalize_birthdate(item.get("date_de_naissance"))
            if not item_rgpd:
                _, item_rgpd = _normalize_birthdate(item.get("date_de_naissance_rgpd"))
            if item_rgpd == target_rgpd:
                rgpd_matches.append(item)
        if rgpd_matches:
            return rgpd_matches, "date_de_naissance_rgpd"

    if len(candidates) == 1:
        return candidates, "candidat_unique"

    return [], "aucun_match_fiable"


def _extract_role_from_company_stub(company_stub: dict, dirigeant_item: dict) -> Optional[str]:
    nested_dirigeant = company_stub.get("dirigeant") or {}
    qualites = nested_dirigeant.get("qualites") or dirigeant_item.get("qualites") or []
    if isinstance(qualites, list):
        values = [_clean_text(value) for value in qualites if _clean_text(value)]
        if values:
            return " / ".join(dict.fromkeys(values))

    return _clean_text(
        nested_dirigeant.get("qualite")
        or dirigeant_item.get("qualite")
        or ""
    ) or None


def _build_company_row_from_sources(company_stub: dict, company_detail: Optional[dict], dirigeant_item: dict) -> dict:
    base = company_detail or {}
    company_name = _clean_text(
        base.get("nom_entreprise")
        or _extract_company_name(base)
        or company_stub.get("nom_entreprise")
        or company_stub.get("denomination")
        or ""
    )

    base_siege = base.get("siege") or {}
    stub_siege = company_stub.get("siege") or {}

    return {
        "nom_societe": company_name,
        "siren": _clean_text(str(base.get("siren") or company_stub.get("siren") or "")) or None,
        "siret": base_siege.get("siret") or stub_siege.get("siret"),
        "entreprise_cessee": base.get("entreprise_cessee", company_stub.get("entreprise_cessee")),
        "forme_juridique": base.get("forme_juridique") or company_stub.get("forme_juridique"),
        "statut": base.get("statut_consolide") or base.get("statut_rcs") or company_stub.get("statut_consolide") or company_stub.get("statut_rcs"),
        "activite": base.get("libelle_code_naf") or base.get("domaine_activite") or company_stub.get("libelle_code_naf") or company_stub.get("domaine_activite"),
        "date_creation": base.get("date_creation_formate") or base.get("date_creation") or company_stub.get("date_creation_formate") or company_stub.get("date_creation"),
        "capital": base.get("capital"),
        "chiffre_affaires": base.get("chiffre_affaires"),
        "resultat_net": base.get("resultat") if base.get("resultat") is not None else base.get("resultat_net"),
        "statut_rcs": base.get("statut_rcs") or company_stub.get("statut_rcs"),
        "nb_dirigeants_total": base.get("nb_dirigeants_total") or company_stub.get("nb_dirigeants_total"),
        "role": _extract_role_from_company_stub(company_stub, dirigeant_item),
        "detention": None,
        "commentaires": None,
    }


def _search_companies_for_person(
    person: dict,
    recherche_cache: Optional[Dict[str, Optional[dict]]] = None,
) -> Tuple[List[dict], dict]:
    nom = _clean_text(person.get("nom", ""))
    prenoms = _clean_text(person.get("prenoms", ""))
    premier_prenom = _first_prenom(prenoms)

    if not nom or not premier_prenom:
        return [], {
            "input_person": person,
            "selection_mode": "nom_ou_prenom_manquant",
            "dirigeants": [],
            "recherche_siren": {},
        }

    try:
        dirigeant_items, meta = _run_recherche_dirigeants(nom, premier_prenom)
        strategy = "nom_plus_premier_prenom"
    except Exception as exc:
        print(f"[WARN] /recherche-dirigeants error for {nom} {premier_prenom}: {exc}")
        return [], {
            "input_person": person,
            "selection_mode": "erreur_recherche_dirigeants",
            "error": str(exc),
            "dirigeants": [],
            "recherche_siren": {},
        }

    if not dirigeant_items and prenoms and prenoms != premier_prenom:
        try:
            dirigeant_items, fallback_meta = _run_recherche_dirigeants(nom, prenoms)
            strategy = "nom_plus_prenoms_complets"
            meta = {
                "initial_query": meta,
                "fallback_query": fallback_meta,
                "fallback_triggered": True,
            }
        except Exception as exc:
            print(f"[WARN] /recherche-dirigeants fallback error for {nom} {prenoms}: {exc}")

    selected_candidates, selection_mode = _select_dirigeant_candidates(dirigeant_items, person)
    selected_signatures = {_candidate_signature(item) for item in selected_candidates}

    recherche_by_siren_meta: Dict[str, dict] = {}
    company_rows: List[dict] = []

    for dirigeant_item in selected_candidates:
        for company_stub in dirigeant_item.get("entreprises") or []:
            siren = _clean_text(str(company_stub.get("siren") or ""))
            company_detail = None
            if len(siren) == 9:
                if recherche_cache is not None and siren in recherche_cache:
                    company_detail = recherche_cache[siren]
                else:
                    try:
                        company_detail, siren_meta = _run_recherche_by_siren(siren)
                    except Exception as exc:
                        company_detail = None
                        siren_meta = {"query_params": {"siren": siren}, "error": str(exc)}
                        print(f"[WARN] /recherche error for siren={siren}: {exc}")
                    if recherche_cache is not None:
                        recherche_cache[siren] = company_detail
                    recherche_by_siren_meta[siren] = siren_meta

            company_rows.append(
                _build_company_row_from_sources(company_stub, company_detail, dirigeant_item)
            )

    debug_payload = {
        "input_person": person,
        "strategy": strategy,
        "selection_mode": selection_mode,
        "query_meta": meta,
        "dirigeants": [
            {
                "selected": _candidate_signature(item) in selected_signatures,
                "nom_complet": item.get("nom_complet"),
                "date_de_naissance": item.get("date_de_naissance"),
                "date_de_naissance_rgpd": item.get("date_de_naissance_rgpd"),
                "age": item.get("age"),
                "nb_entreprises_total": item.get("nb_entreprises_total"),
                "entreprises": [
                    {
                        "siren": company.get("siren"),
                        "nom_entreprise": company.get("nom_entreprise") or company.get("denomination"),
                        "role": _extract_role_from_company_stub(company, item),
                    }
                    for company in (item.get("entreprises") or [])
                ],
            }
            for item in dirigeant_items
        ],
        "recherche_siren": recherche_by_siren_meta,
    }

    return _dedupe_company_rows(company_rows), debug_payload


def enrich_people(people: List[dict]) -> Tuple[Dict[str, List[dict]], Dict[str, dict]]:
    results: Dict[str, List[dict]] = {}
    debug_payload: Dict[str, dict] = {}
    recherche_cache: Dict[str, Optional[dict]] = {}

    for person in people:
        nom = _clean_text(person.get("nom", ""))
        prenoms = _clean_text(person.get("prenoms", ""))
        display_name = f"{nom} {prenoms}".strip()

        companies, person_debug = _search_companies_for_person(
            person,
            recherche_cache=recherche_cache,
        )
        results[display_name] = companies
        debug_payload[display_name] = person_debug

    return results, debug_payload


def write_debug_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
