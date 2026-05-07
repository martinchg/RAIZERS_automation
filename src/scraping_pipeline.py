from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, Iterable, List, Optional

from scraping_excel import compute_scraping_summary, normalize_scraping_results
from tab_immo import GeocoderClient

SCRAPER_KEYS = (
    "consortium_immobilier",
    "lesclesdumidi",
    "terrain_construction",
    "meilleursagents",
)


def derive_department_code(postcode: Optional[str]) -> Optional[str]:
    if not postcode:
        return None
    code = str(postcode).strip()
    if len(code) < 2 or not code[:2].isdigit():
        return None

    if code.startswith(("971", "972", "973", "974", "976")):
        return code[:3]

    if code.startswith("20"):
        try:
            numeric = int(code)
        except ValueError:
            return None
        return "2A" if numeric <= 20190 else "2B"

    return code[:2]


def build_terrain_construction_url(city: str, postcode: str, department_code: str) -> str:
    from scrapers.consortium_immobilier_scraper import DEPARTMENT_SLUGS, slugify_consortium

    department_slug = DEPARTMENT_SLUGS.get(department_code.upper())
    if not department_slug:
        raise ValueError(f"Code departement non gere pour terrain-construction: {department_code}")

    city_slug = slugify_consortium(city)
    return (
        "https://www.terrain-construction.com/prix-moyen-terrain/"
        f"{department_slug}-{department_code.lower()}/{city_slug}-{postcode}"
    )


def _build_subject_context(
    payload: Dict[str, Any],
    *,
    geocoder: Optional[GeocoderClient] = None,
) -> Dict[str, Any]:
    geocoder = geocoder or GeocoderClient()
    geo = geocoder.geocode(payload["address"])

    postcode = payload.get("postal_code") or geo.get("postcode")
    city = payload.get("city") or geo.get("city")
    department_code = payload.get("department_code") or derive_department_code(postcode)

    return {
        "normalized_address": geo.get("normalized_address") or payload["address"],
        "address": payload["address"],
        "street_name": geo.get("street_name"),
        "latitude": geo.get("latitude"),
        "longitude": geo.get("longitude"),
        "city": city,
        "postcode": postcode,
        "department_code": department_code,
        "property_type": payload["property_type"],
        "living_area_sqm": payload["living_area_sqm"],
        "rooms": payload["rooms"],
        "land_area_sqm": payload.get("land_area_sqm"),
    }


def _result_with_metadata(source: str, raw: Dict[str, Any], property_type: str) -> Dict[str, Any]:
    return {
        **raw,
        "source": source,
        "property_type": property_type,
    }


def _error_result(source: str, property_type: str, message: str, **extra: Any) -> Dict[str, Any]:
    return {
        "source": source,
        "property_type": property_type,
        "error": message,
        "status": extra.pop("status", "error"),
        **extra,
    }


def _run_consortium(subject: Dict[str, Any]) -> Dict[str, Any]:
    from scrapers.consortium_immobilier_scraper import build_city_url, get_prix_m2_by_url

    city = subject.get("city")
    postcode = subject.get("postcode")
    if not city or not postcode:
        return _error_result(
            "consortium_immobilier",
            subject["property_type"],
            "Ville ou code postal manquant.",
        )

    url = build_city_url(city, postcode)
    result = get_prix_m2_by_url(url)
    return _result_with_metadata("consortium_immobilier", asdict(result), subject["property_type"])


def _run_lesclesdumidi(subject: Dict[str, Any]) -> Dict[str, Any]:
    from scrapers.lesclesdumidi_scraper import build_url, get_prix_m2_by_url

    city = subject.get("city")
    department_code = subject.get("department_code")
    if not city or not department_code:
        return _error_result(
            "lesclesdumidi",
            subject["property_type"],
            "Ville ou departement manquant.",
        )

    url = build_url(city, department_code)
    result = get_prix_m2_by_url(url)
    return _result_with_metadata("lesclesdumidi", asdict(result), subject["property_type"])


def _run_terrain_construction(subject: Dict[str, Any]) -> Dict[str, Any]:
    from scrapers.terrain_construction_scraper import get_prix_terrain_by_url

    if subject["property_type"] != "maison":
        return _error_result(
            "terrain_construction",
            subject["property_type"],
            "Scraper non applicable aux appartements.",
            status="skipped",
        )

    city = subject.get("city")
    postcode = subject.get("postcode")
    department_code = subject.get("department_code")
    if not city or not postcode or not department_code:
        return _error_result(
            "terrain_construction",
            subject["property_type"],
            "Ville, code postal ou departement manquant.",
        )

    url = build_terrain_construction_url(city, postcode, department_code)
    result = get_prix_terrain_by_url(url)
    return _result_with_metadata("terrain_construction", asdict(result), subject["property_type"])


def _run_meilleursagents(subject: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    from scrapers.meilleursagents_apify_cloud import BienImmobilier, get_prix_m2

    bien = BienImmobilier(
        adresse=subject["normalized_address"],
        code_postal=subject.get("postcode") or "",
        ville=subject.get("city") or "",
        type_bien=subject["property_type"],
        surface_habitable=payload.get("living_area_sqm"),
        surface_terrain=payload.get("land_area_sqm"),
        surface_encore_constructible=payload.get("surface_encore_constructible"),
        nb_pieces=payload.get("rooms"),
        nb_chambres=payload.get("nb_chambres"),
        nb_salles_bain=payload.get("nb_salles_bain"),
        nb_niveaux=payload.get("nb_niveaux"),
        etage=payload.get("etage"),
        nb_etages_immeuble=payload.get("nb_etages_immeuble"),
        ascenseur=bool(payload.get("ascenseur", False)),
        balcon=bool(payload.get("balcon", False)),
        surface_balcon=payload.get("surface_balcon"),
        terrasse=bool(payload.get("terrasse", False)),
        surface_terrasse=payload.get("surface_terrasse"),
        nb_caves=int(payload.get("nb_caves") or 0),
        nb_places_parking=int(payload.get("nb_places_parking") or 0),
        nb_chambres_service=int(payload.get("nb_chambres_service") or 0),
        annee_construction=payload.get("annee_construction"),
        etat_bien=payload.get("etat_bien"),
    )

    result = get_prix_m2(
        bien,
        api_key=payload.get("apify_api_token"),
        email=payload.get("meilleursagents_email"),
        password=payload.get("meilleursagents_password"),
    )
    return _result_with_metadata("meilleursagents", asdict(result), subject["property_type"])


def run_scraping_pipeline(
    payload: Dict[str, Any],
    *,
    geocoder: Optional[GeocoderClient] = None,
) -> Dict[str, Any]:
    requested_scrapers = payload.get("scrapers") or list(SCRAPER_KEYS)
    requested_scrapers = [key for key in requested_scrapers if key in SCRAPER_KEYS]
    if not requested_scrapers:
        raise ValueError("Aucun scraper valide demande.")

    subject = _build_subject_context(payload, geocoder=geocoder)

    runners = {
        "consortium_immobilier": lambda: _run_consortium(subject),
        "lesclesdumidi": lambda: _run_lesclesdumidi(subject),
        "terrain_construction": lambda: _run_terrain_construction(subject),
        "meilleursagents": lambda: _run_meilleursagents(subject, payload),
    }

    raw_results: List[Dict[str, Any]] = []
    for scraper_key in requested_scrapers:
        try:
            raw_results.append(runners[scraper_key]())
        except Exception as exc:
            raw_results.append(
                _error_result(scraper_key, subject["property_type"], str(exc))
            )

    normalized_results = normalize_scraping_results(
        raw_results,
        property_type=subject["property_type"],
    )
    statistics = compute_scraping_summary(normalized_results)

    return {
        "subject": subject,
        "results": normalized_results,
        "statistics": statistics,
        "sources_requested": requested_scrapers,
    }
