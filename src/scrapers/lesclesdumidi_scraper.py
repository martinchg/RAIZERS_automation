from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import asdict
from dataclasses import dataclass
from typing import Optional

import requests
from bs4 import BeautifulSoup
from bs4 import Tag


BASE_URL = "https://www.lesclesdumidi.com/prix/m2-{slug}-{departement_code}"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    )
}
PRICE_NUMBER_PATTERN = re.compile(r"\d[\d\s\u00a0\u202f.,]*")


DEPARTMENT_SLUGS: dict[str, str] = {
    "01": "ain",
    "02": "aisne",
    "03": "allier",
    "04": "alpes_de_haute_provence",
    "05": "hautes_alpes",
    "06": "alpes_maritimes",
    "07": "ardeche",
    "08": "ardennes",
    "09": "ariege",
    "10": "aube",
    "11": "aude",
    "12": "aveyron",
    "13": "bouches_du_rhone",
    "14": "calvados",
    "15": "cantal",
    "16": "charente",
    "17": "charente_maritime",
    "18": "cher",
    "19": "correze",
    "2A": "corse_du_sud",
    "2B": "haute_corse",
    "21": "cote_d_or",
    "22": "cotes_d_armor",
    "23": "creuse",
    "24": "dordogne",
    "25": "doubs",
    "26": "drome",
    "27": "eure",
    "28": "eure_et_loir",
    "29": "finistere",
    "30": "gard",
    "31": "haute_garonne",
    "32": "gers",
    "33": "gironde",
    "34": "herault",
    "35": "ille_et_vilaine",
    "36": "indre",
    "37": "indre_et_loire",
    "38": "isere",
    "39": "jura",
    "40": "landes",
    "41": "loir_et_cher",
    "42": "loire",
    "43": "haute_loire",
    "44": "loire_atlantique",
    "45": "loiret",
    "46": "lot",
    "47": "lot_et_garonne",
    "48": "lozere",
    "49": "maine_et_loire",
    "50": "manche",
    "51": "marne",
    "52": "haute_marne",
    "53": "mayenne",
    "54": "meurthe_et_moselle",
    "55": "meuse",
    "56": "morbihan",
    "57": "moselle",
    "58": "nievre",
    "59": "nord",
    "60": "oise",
    "61": "orne",
    "62": "pas_de_calais",
    "63": "puy_de_dome",
    "64": "pyrenees_atlantiques",
    "65": "hautes_pyrenees",
    "66": "pyrenees_orientales",
    "67": "bas_rhin",
    "68": "haut_rhin",
    "69": "rhone",
    "70": "haute_saone",
    "71": "saone_et_loire",
    "72": "sarthe",
    "73": "savoie",
    "74": "haute_savoie",
    "75": "paris",
    "76": "seine_maritime",
    "77": "seine_et_marne",
    "78": "yvelines",
    "79": "deux_sevres",
    "80": "somme",
    "81": "tarn",
    "82": "tarn_et_garonne",
    "83": "var",
    "84": "vaucluse",
    "85": "vendee",
    "86": "vienne",
    "87": "haute_vienne",
    "88": "vosges",
    "89": "yonne",
    "90": "territoire_de_belfort",
    "91": "essonne",
    "92": "hauts_de_seine",
    "93": "seine_saint_denis",
    "94": "val_de_marne",
    "95": "val_d_oise",
    "971": "guadeloupe",
    "972": "martinique",
    "973": "guyane",
    "974": "la_reunion",
    "976": "mayotte",
}


@dataclass
class PrixM2ByType:
    type_bien: str
    prix_m2_moyen: Optional[float]
    prix_m2_min: Optional[float]
    prix_m2_max: Optional[float]


@dataclass
class LesClesDuMidiResult:
    localisation: Optional[str]
    url_source: str
    method_used: str
    prix: list[PrixM2ByType]
    error: Optional[str] = None


class ParsingError(Exception):
    pass


def _normalize_spaces(value: str) -> str:
    return re.sub(r"[\s\u00a0\u202f]+", " ", value).strip()


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def slugify_lesclesdumidi(ville: str) -> str:
    slug = _strip_accents(ville).lower()
    slug = slug.replace("'", "_")
    slug = slug.replace("-", "_")
    slug = re.sub(r"\bste\b", "sainte", slug)
    slug = re.sub(r"\bst\b", "saint", slug)
    slug = slug.replace("ème", "eme").replace("er", "er")
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug


def build_url(localisation: str, departement_code: str) -> str:
    return BASE_URL.format(
        slug=slugify_lesclesdumidi(localisation),
        departement_code=departement_code.upper(),
    )


def _build_department_url(departement_code: str) -> Optional[str]:
    slug = DEPARTMENT_SLUGS.get(departement_code.upper())
    if not slug:
        return None
    return BASE_URL.format(slug=slug, departement_code=departement_code.upper())


def _extract_localisation(soup: BeautifulSoup) -> Optional[str]:
    title_tag = soup.find("title")
    if title_tag:
        title_text = _normalize_spaces(title_tag.get_text(" ", strip=True))
        patterns = (
            r"m[²2²]\s+(?:a|à|de|du|des|dans|en)\s+(.+?)\s*[-–|]",
            r"immobilier\s+(?:a|à|de|du|des|dans|en)\s+(.+?)\s*[-–|]",
        )
        for pattern in patterns:
            match = re.search(pattern, title_text, re.IGNORECASE)
            if match:
                return _normalize_spaces(match.group(1))

    for heading in soup.find_all(["h1", "h2"]):
        heading_text = _normalize_spaces(heading.get_text(" ", strip=True))
        if "cookie" in heading_text.lower():
            continue
        patterns = (
            r"prix.*?m[²2]\s+(?:a|à|de|du|des|dans|en)\s+(.+)",
            r"prix.*?immobilier\s+(?:a|à|de|du|des|dans|en)\s+(.+)",
        )
        for pattern in patterns:
            match = re.search(pattern, heading_text, re.IGNORECASE)
            if match:
                return _normalize_spaces(match.group(1))
        return heading_text
    return None


def _clean_cell_text(cell: Tag) -> str:
    return _normalize_spaces(cell.get_text(" ", strip=True))


def _normalize_label(value: str) -> str:
    return _strip_accents(_normalize_spaces(value)).casefold()


def _parse_price_value(value: str) -> Optional[float]:
    cleaned = _normalize_spaces(value)
    if not cleaned:
        return None

    match = PRICE_NUMBER_PATTERN.search(cleaned)
    if not match:
        return None

    number = re.sub(r"[^\d.,]", "", match.group(0))
    if not number:
        return None

    if "," in number and "." in number:
        last_comma = number.rfind(",")
        last_dot = number.rfind(".")
        if last_comma > last_dot:
            number = number.replace(".", "").replace(",", ".")
        else:
            number = number.replace(",", "")
    elif "," in number:
        parts = number.split(",")
        if len(parts) == 2 and len(parts[1]) <= 2:
            number = ".".join(parts)
        else:
            number = "".join(parts)
    elif "." in number:
        parts = number.split(".")
        if len(parts) == 2 and len(parts[1]) <= 2:
            number = ".".join(parts)
        else:
            number = "".join(parts)

    try:
        return float(number)
    except ValueError:
        return None


def _extract_table_rows(table: Tag) -> list[list[str]]:
    rows: list[list[str]] = []
    for row in table.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if not cells:
            continue
        values = [_clean_cell_text(cell) for cell in cells]
        if any(values):
            rows.append(values)
    return rows


def _find_first_table(soup: BeautifulSoup) -> Optional[Tag]:
    tabprix = soup.find("table", class_="tabprix")
    if tabprix:
        return tabprix
    for table in soup.find_all("table"):
        direct_rows = table.find_all("tr", recursive=False) or table.find("tbody") and table.find("tbody").find_all("tr", recursive=False)
        for row in (direct_rows or []):
            for cell in row.find_all(["th", "td"], recursive=False):
                text = cell.get_text(" ", strip=True).casefold()
                if "appartement" in text or "maison" in text:
                    return table
    return soup.find("table")


def _find_type_index(headers: list[str]) -> int:
    for index, header in enumerate(headers):
        normalized = _normalize_label(header)
        if any(token in normalized for token in ("type", "bien", "logement")):
            return index
    return 0


def _find_column_mapping(headers: list[str], type_index: int) -> dict[str, int]:
    mapping: dict[str, int] = {}

    for index, header in enumerate(headers):
        if index == type_index:
            continue
        normalized = _normalize_label(header)
        if "moy" in normalized or "median" in normalized:
            mapping["moyen"] = index
        elif "min" in normalized or "bas" in normalized:
            mapping["min"] = index
        elif "max" in normalized or "haut" in normalized:
            mapping["max"] = index

    if {"moyen", "min", "max"} <= set(mapping):
        return mapping

    numeric_indexes = [index for index in range(len(headers)) if index != type_index]
    fallback_order = ("moyen", "min", "max")
    for key, index in zip(fallback_order, numeric_indexes):
        mapping.setdefault(key, index)
    return mapping


def _extract_prix_from_row(
    row: list[str],
    mapping: dict[str, int],
    type_index: int,
) -> Optional[PrixM2ByType]:
    if type_index >= len(row):
        return None

    label = _normalize_label(row[type_index])
    if "appartement" in label:
        type_bien = "appartement"
    elif "maison" in label:
        type_bien = "maison"
    else:
        return None

    def get_value(key: str) -> Optional[float]:
        index = mapping.get(key)
        if index is None or index >= len(row):
            return None
        return _parse_price_value(row[index])

    return PrixM2ByType(
        type_bien=type_bien,
        prix_m2_moyen=get_value("moyen"),
        prix_m2_min=get_value("min"),
        prix_m2_max=get_value("max"),
    )


def parse_lesclesdumidi_page(html: str, url: str, method_used: str) -> LesClesDuMidiResult:
    soup = BeautifulSoup(html, "html.parser")
    localisation = _extract_localisation(soup)
    result = LesClesDuMidiResult(
        localisation=localisation,
        url_source=url,
        method_used=method_used,
        prix=[],
    )

    table = _find_first_table(soup)
    if table is None:
        result.error = "Aucun tableau HTML trouve dans la page."
        return result

    rows = _extract_table_rows(table)
    if not rows:
        result.error = "Le premier tableau HTML est vide ou illisible."
        return result

    headers = rows[0]
    type_index = _find_type_index(headers)
    mapping = _find_column_mapping(headers, type_index)

    parsed_rows: list[PrixM2ByType] = []
    for row in rows[1:]:
        prix_row = _extract_prix_from_row(row, mapping, type_index)
        if prix_row:
            parsed_rows.append(prix_row)

    if not parsed_rows and len(rows) >= 2:
        type_index = 0
        inferred_headers = [f"col_{index}" for index in range(len(rows[0]))]
        mapping = _find_column_mapping(inferred_headers, type_index)
        for row in rows:
            prix_row = _extract_prix_from_row(row, mapping, type_index)
            if prix_row:
                parsed_rows.append(prix_row)

    by_type = {item.type_bien: item for item in parsed_rows}
    ordered_prix = [by_type[key] for key in ("appartement", "maison") if key in by_type]
    result.prix = ordered_prix

    if not result.prix:
        result.error = "Impossible d'extraire les lignes Appartement/Maison du premier tableau."
        return result

    missing_types = [label for label in ("appartement", "maison") if label not in by_type]
    if missing_types:
        result.error = f"Types manquants dans le premier tableau: {', '.join(missing_types)}."

    return result


def _fetch_html_with_requests(url: str, timeout: int = 20) -> str:
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def _fetch_html_with_playwright(url: str, timeout_ms: int = 30000) -> str:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        try:
            page = browser.new_page(user_agent=DEFAULT_HEADERS["User-Agent"])
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(1500)
            return page.content()
        finally:
            browser.close()


def _extract_departement_code_from_url(url: str) -> Optional[str]:
    match = re.search(r"-([0-9A-Za-z]{2,3})/?$", url)
    if not match:
        return None
    return match.group(1).upper()


def _is_arrondissement_slug(slug: str) -> bool:
    normalized = _normalize_spaces(slug.replace("-", "_"))
    return bool(re.search(r"_\d+(?:er|eme)_arrondissement$", normalized, re.IGNORECASE))


def _build_city_url_from_arrondissement_url(url: str) -> Optional[str]:
    match = re.search(
        r"^https://www\.lesclesdumidi\.com/prix/m2-(?P<slug>.+)-(?P<code>[0-9A-Za-z]{2,3})/?$",
        url,
        re.IGNORECASE,
    )
    if not match:
        return None

    slug = match.group("slug")
    departement_code = match.group("code").upper()
    if not _is_arrondissement_slug(slug):
        return None

    city_slug = re.sub(
        r"_\d+(?:er|eme)_arrondissement$",
        "",
        slug,
        flags=re.IGNORECASE,
    ).strip("_")
    if not city_slug:
        return None

    return BASE_URL.format(slug=city_slug, departement_code=departement_code)


def _should_try_department_fallback(result: LesClesDuMidiResult) -> bool:
    if result.prix:
        return False
    return True


def _merge_errors(*errors: Optional[str]) -> Optional[str]:
    messages = [_normalize_spaces(error) for error in errors if error]
    if not messages:
        return None
    deduplicated: list[str] = []
    for message in messages:
        if message not in deduplicated:
            deduplicated.append(message)
    return " | ".join(deduplicated)


def get_prix_m2_by_url(url: str, use_playwright_fallback: bool = True) -> LesClesDuMidiResult:
    request_error: Optional[str] = None
    parsed_requests_result: Optional[LesClesDuMidiResult] = None
    should_try_city_fallback = False

    try:
        html = _fetch_html_with_requests(url)
        parsed_requests_result = parse_lesclesdumidi_page(
            html=html,
            url=url,
            method_used="requests",
        )
        if parsed_requests_result.prix:
            return parsed_requests_result
    except requests.RequestException as exc:
        request_error = f"Erreur requests: {exc}"
        response = getattr(exc, "response", None)
        if response is not None and response.status_code == 404:
            should_try_city_fallback = True
    except Exception as exc:
        request_error = f"Erreur parsing requests: {exc}"

    if should_try_city_fallback:
        city_url = _build_city_url_from_arrondissement_url(url)
        if city_url and city_url != url:
            city_result = get_prix_m2_by_url(
                url=city_url,
                use_playwright_fallback=use_playwright_fallback,
            )
            if city_result.prix:
                city_result.error = _merge_errors(
                    request_error,
                    "Fallback vers la ville.",
                    city_result.error,
                )
                return city_result
            request_error = _merge_errors(
                request_error,
                f"Fallback ville echoue: {city_result.error or 'aucune donnee exploitable'}",
            )

    if use_playwright_fallback:
        try:
            html = _fetch_html_with_playwright(url)
            parsed_playwright_result = parse_lesclesdumidi_page(
                html=html,
                url=url,
                method_used="playwright",
            )
            if parsed_playwright_result.prix:
                parsed_playwright_result.error = _merge_errors(
                    request_error,
                    parsed_requests_result.error if parsed_requests_result else None,
                    parsed_playwright_result.error,
                )
                return parsed_playwright_result
            parsed_requests_result = parsed_playwright_result
        except Exception as exc:
            playwright_error = f"Erreur playwright: {exc}"
            if parsed_requests_result is None:
                parsed_requests_result = LesClesDuMidiResult(
                    localisation=None,
                    url_source=url,
                    method_used="playwright",
                    prix=[],
                    error=_merge_errors(request_error, playwright_error),
                )
            else:
                parsed_requests_result.error = _merge_errors(
                    request_error,
                    parsed_requests_result.error,
                    playwright_error,
                )

    result = parsed_requests_result
    if result is None:
        result = LesClesDuMidiResult(
            localisation=None,
            url_source=url,
            method_used="requests",
            prix=[],
            error=request_error or "Echec inconnu lors de la recuperation de la page.",
        )

    departement_code = _extract_departement_code_from_url(url)
    department_url = _build_department_url(departement_code) if departement_code else None
    if department_url and department_url != url and _should_try_department_fallback(result):
        department_result = get_prix_m2_by_url(
            url=department_url,
            use_playwright_fallback=use_playwright_fallback,
        )
        if department_result.prix:
            department_result.error = _merge_errors(
                result.error,
                "Fallback vers la page departement.",
                department_result.error,
            )
            return department_result
        result.error = _merge_errors(
            result.error,
            f"Fallback departement echoue: {department_result.error or 'aucune donnee exploitable'}",
        )

    return result


def main() -> None:
    examples = [
        ("Paris 2eme arrondissement", "75"),
        ("Marseille 2eme arrondissement", "13"),
        ("Neuilly-sur-Seine", "92"),
    ]

    for localisation, departement_code in examples:
        url = build_url(localisation, departement_code)
        result = get_prix_m2_by_url(url)
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
