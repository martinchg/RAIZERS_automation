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


BASE_URL = "https://www.consortium-immobilier.fr/prix/{slug}.html"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    )
}
PRICE_RE = re.compile(r"([\d][\d\s  ]*)\s*€")


DEPARTMENT_SLUGS: dict[str, str] = {
    "01": "ain",
    "02": "aisne",
    "03": "allier",
    "04": "alpes-de-haute-provence",
    "05": "hautes-alpes",
    "06": "alpes-maritimes",
    "07": "ardeche",
    "08": "ardennes",
    "09": "ariege",
    "10": "aube",
    "11": "aude",
    "12": "aveyron",
    "13": "bouches-du-rhone",
    "14": "calvados",
    "15": "cantal",
    "16": "charente",
    "17": "charente-maritime",
    "18": "cher",
    "19": "correze",
    "2A": "corse-du-sud",
    "2B": "haute-corse",
    "21": "cote-d-or",
    "22": "cotes-d-armor",
    "23": "creuse",
    "24": "dordogne",
    "25": "doubs",
    "26": "drome",
    "27": "eure",
    "28": "eure-et-loir",
    "29": "finistere",
    "30": "gard",
    "31": "haute-garonne",
    "32": "gers",
    "33": "gironde",
    "34": "herault",
    "35": "ille-et-vilaine",
    "36": "indre",
    "37": "indre-et-loire",
    "38": "isere",
    "39": "jura",
    "40": "landes",
    "41": "loir-et-cher",
    "42": "loire",
    "43": "haute-loire",
    "44": "loire-atlantique",
    "45": "loiret",
    "46": "lot",
    "47": "lot-et-garonne",
    "48": "lozere",
    "49": "maine-et-loire",
    "50": "manche",
    "51": "marne",
    "52": "haute-marne",
    "53": "mayenne",
    "54": "meurthe-et-moselle",
    "55": "meuse",
    "56": "morbihan",
    "57": "moselle",
    "58": "nievre",
    "59": "nord",
    "60": "oise",
    "61": "orne",
    "62": "pas-de-calais",
    "63": "puy-de-dome",
    "64": "pyrenees-atlantiques",
    "65": "hautes-pyrenees",
    "66": "pyrenees-orientales",
    "67": "bas-rhin",
    "68": "haut-rhin",
    "69": "rhone",
    "70": "haute-saone",
    "71": "saone-et-loire",
    "72": "sarthe",
    "73": "savoie",
    "74": "haute-savoie",
    "75": "paris",
    "76": "seine-maritime",
    "77": "seine-et-marne",
    "78": "yvelines",
    "79": "deux-sevres",
    "80": "somme",
    "81": "tarn",
    "82": "tarn-et-garonne",
    "83": "var",
    "84": "vaucluse",
    "85": "vendee",
    "86": "vienne",
    "87": "haute-vienne",
    "88": "vosges",
    "89": "yonne",
    "90": "territoire-de-belfort",
    "91": "essonne",
    "92": "hauts-de-seine",
    "93": "seine-saint-denis",
    "94": "val-de-marne",
    "95": "val-d-oise",
    "971": "guadeloupe",
    "972": "martinique",
    "973": "guyane",
    "974": "la-reunion",
    "976": "mayotte",
}


@dataclass
class PrixM2ByType:
    type_bien: str
    prix_m2_moyen: Optional[float]
    prix_m2_min: Optional[float]
    prix_m2_max: Optional[float]


@dataclass
class ConsortiumImmobilierResult:
    localisation: Optional[str]
    url_source: str
    method_used: str
    prix: list[PrixM2ByType]
    error: Optional[str] = None


def _normalize_spaces(value: str) -> str:
    return re.sub(r"[\s  ]+", " ", value).strip()


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(c for c in normalized if not unicodedata.combining(c))


def slugify_consortium(name: str) -> str:
    slug = _strip_accents(name).lower()
    slug = re.sub(r"['’]", "-", slug)
    slug = re.sub(r"[^a-z0-9-]+", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


def build_city_url(city_name: str, postal_code: str) -> str:
    return BASE_URL.format(slug=f"{slugify_consortium(city_name)}-{postal_code}")


def build_department_url(departement_code: str) -> Optional[str]:
    slug = DEPARTMENT_SLUGS.get(departement_code.upper())
    if not slug:
        return None
    return BASE_URL.format(slug=f"{slug}-{departement_code}")


def _extract_localisation(soup: BeautifulSoup) -> Optional[str]:
    h1 = soup.find("h1")
    if h1:
        text = _normalize_spaces(h1.get_text(" ", strip=True))
        for pattern in (
            r"(?:à|a)\s+(.+?)\s*$",
            r"dans\s+(?:le|la|les|l['’])?\s*(.+?)\s*$",
        ):
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return _normalize_spaces(match.group(1)).title()
        return text.title()

    title_tag = soup.find("title")
    if title_tag:
        text = _normalize_spaces(title_tag.get_text(" ", strip=True))
        match = re.search(r"immobilier\s+(.+?)\s+prix\s+m", text, re.IGNORECASE)
        if match:
            return _normalize_spaces(match.group(1)).title()
    return None


def _parse_price_from_cell(cell: Tag) -> Optional[float]:
    text = _normalize_spaces(cell.get_text(" ", strip=True))
    match = PRICE_RE.search(text)
    if not match:
        return None
    raw = re.sub(r"[^\d]", "", match.group(1))
    try:
        return float(raw) if raw else None
    except ValueError:
        return None


def _parse_price_table(table: Tag) -> list[PrixM2ByType]:
    results: list[PrixM2ByType] = []
    rows = table.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        type_text = _strip_accents(_normalize_spaces(cells[1].get_text(" ", strip=True))).casefold()
        if "appartement" in type_text:
            type_bien = "appartement"
        elif "maison" in type_text:
            type_bien = "maison"
        else:
            continue
        results.append(PrixM2ByType(
            type_bien=type_bien,
            prix_m2_min=_parse_price_from_cell(cells[2]),
            prix_m2_moyen=_parse_price_from_cell(cells[3]),
            prix_m2_max=_parse_price_from_cell(cells[4]),
        ))
    by_type = {item.type_bien: item for item in results}
    return [by_type[k] for k in ("appartement", "maison") if k in by_type]


def parse_consortium_page(html: str, url: str, method_used: str) -> ConsortiumImmobilierResult:
    soup = BeautifulSoup(html, "html.parser")
    localisation = _extract_localisation(soup)
    result = ConsortiumImmobilierResult(
        localisation=localisation,
        url_source=url,
        method_used=method_used,
        prix=[],
    )

    table = soup.find("table", class_="graph")
    if table is None:
        result.error = "Aucune table.graph trouvée dans la page."
        return result

    prix = _parse_price_table(table)
    if not prix:
        result.error = "Impossible d'extraire les lignes Appartement/Maison."
        return result

    result.prix = prix
    missing = [t for t in ("appartement", "maison") if not any(p.type_bien == t for p in prix)]
    if missing:
        result.error = f"Types manquants: {', '.join(missing)}."
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


def _merge_errors(*errors: Optional[str]) -> Optional[str]:
    messages = [_normalize_spaces(e) for e in errors if e]
    seen: list[str] = []
    for m in messages:
        if m not in seen:
            seen.append(m)
    return " | ".join(seen) if seen else None


def _extract_dept_code_from_url(url: str) -> Optional[str]:
    slug = url.rstrip("/").removesuffix(".html").split("/")[-1]
    match = re.search(r"-(\d{2,3}|2[AB])$", slug, re.IGNORECASE)
    return match.group(1).upper() if match else None


def _fetch_and_parse(url: str, use_playwright_fallback: bool) -> ConsortiumImmobilierResult:
    request_error: Optional[str] = None
    requests_result: Optional[ConsortiumImmobilierResult] = None

    try:
        html = _fetch_html_with_requests(url)
        requests_result = parse_consortium_page(html, url, "requests")
        if requests_result.prix:
            return requests_result
    except requests.RequestException as exc:
        request_error = f"Erreur requests: {exc}"
    except Exception as exc:
        request_error = f"Erreur parsing requests: {exc}"

    if use_playwright_fallback:
        try:
            html = _fetch_html_with_playwright(url)
            pw_result = parse_consortium_page(html, url, "playwright")
            if pw_result.prix:
                pw_result.error = _merge_errors(
                    request_error,
                    requests_result.error if requests_result else None,
                    pw_result.error,
                )
                return pw_result
            if requests_result is None:
                requests_result = pw_result
        except Exception as exc:
            playwright_error = f"Erreur playwright: {exc}"
            if requests_result is None:
                requests_result = ConsortiumImmobilierResult(
                    localisation=None,
                    url_source=url,
                    method_used="playwright",
                    prix=[],
                    error=_merge_errors(request_error, playwright_error),
                )
            else:
                requests_result.error = _merge_errors(
                    request_error, requests_result.error, playwright_error
                )

    if requests_result is None:
        return ConsortiumImmobilierResult(
            localisation=None,
            url_source=url,
            method_used="requests",
            prix=[],
            error=request_error or "Echec inconnu.",
        )
    return requests_result


def get_prix_m2_by_url(
    url: str,
    use_playwright_fallback: bool = True,
    _visited: Optional[set[str]] = None,
) -> ConsortiumImmobilierResult:
    if _visited is None:
        _visited = set()
    _visited.add(url)

    result = _fetch_and_parse(url, use_playwright_fallback)
    if result.prix:
        return result

    dept_code = _extract_dept_code_from_url(url)

    # city URL failed → try département
    dept_url = build_department_url(dept_code) if dept_code else None
    if dept_url and dept_url not in _visited:
        dept_result = get_prix_m2_by_url(dept_url, use_playwright_fallback, _visited)
        if dept_result.prix:
            dept_result.error = _merge_errors(
                result.error, "Fallback département.", dept_result.error
            )
            return dept_result
        result.error = _merge_errors(
            result.error,
            f"Fallback département échoué: {dept_result.error or 'aucune donnée'}",
        )

    return result


def main() -> None:
    examples = [
        ("https://www.consortium-immobilier.fr/prix/paris-arrondissement-02-75002.html", None),
        ("https://www.consortium-immobilier.fr/prix/neuilly-sur-seine-92200.html", None),
        ("https://www.consortium-immobilier.fr/prix/hauts-de-seine-92.html", None),
    ]

    for url, _ in examples:
        result = get_prix_m2_by_url(url)
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
