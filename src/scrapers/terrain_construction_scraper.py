from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from dataclasses import asdict
from typing import Optional

import requests
from bs4 import BeautifulSoup


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    )
}

PRICE_PATTERN = re.compile(
    r"(?P<value>\d{1,4}(?:[\s\u00a0\u202f]?\d{3})*(?:[.,]\d+)?)\s*€\s*/\s*m(?:²|2)",
    re.IGNORECASE,
)
COUNT_PATTERN = re.compile(
    r"Calculé\s+sur\s+(?P<count>\d{1,3}(?:[\s\u00a0\u202f]?\d{3})*)\s+terrains?\s+à\s+vendre",
    re.IGNORECASE,
)
FRANCE_PRICE_PATTERN = re.compile(
    r"Prix\s+moyen\s+du\s+terrain\s+constructible\s+en\s+France\s*:\s*"
    r"(?P<value>\d{1,4}(?:[\s\u00a0\u202f]?\d{3})*(?:[.,]\d+)?)\s*€\s*/\s*m(?:²|2)",
    re.IGNORECASE,
)
DEPARTMENT_PRICE_PATTERN = re.compile(
    r"Prix\s+moyen\s+du\s+terrain\s+constructible\s+dans\s+.+?\s*:\s*"
    r"(?P<value>\d{1,4}(?:[\s\u00a0\u202f]?\d{3})*(?:[.,]\d+)?)\s*€\s*/\s*m(?:²|2)",
    re.IGNORECASE,
)
CITY_AROUND_PATTERN = re.compile(r"Prix\s+moyen\s+autour\s+de\s+(.+)", re.IGNORECASE)


@dataclass
class TerrainConstructionResult:
    prix_m2_moyen: Optional[float] = None
    nombre_terrains_utilises: Optional[int] = None
    localisation: Optional[str] = None
    url_source: Optional[str] = None
    method_used: Optional[str] = None
    error: Optional[str] = None


def _normalize_spaces(value: str) -> str:
    return re.sub(r"[\u00a0\u202f]", " ", value).strip()


def _parse_float(value: str) -> Optional[float]:
    if not value:
        return None
    cleaned = re.sub(r"[\s\u00a0\u202f]", "", value).replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_int(value: str) -> Optional[int]:
    if not value:
        return None
    cleaned = re.sub(r"[\s\u00a0\u202f]", "", value)
    try:
        return int(cleaned)
    except ValueError:
        return None


def _extract_raw_text(soup: BeautifulSoup) -> str:
    return _normalize_spaces(soup.get_text("\n", strip=True))


def _extract_h1_text(soup: BeautifulSoup) -> Optional[str]:
    heading = soup.find("h1")
    if heading is None:
        heading = soup.find("h2")
    if not heading:
        return None
    return _normalize_spaces(heading.get_text(" ", strip=True))


def _extract_localisation_from_h1(h1_text: Optional[str]) -> Optional[str]:
    if not h1_text:
        return None

    france_match = re.search(
        r"Prix\s+moyen\s+des\s+terrains\s+au\s+m[²2]\s+en\s+France",
        h1_text,
        re.IGNORECASE,
    )
    if france_match:
        return "France"

    city_match = re.search(
        r"Prix\s+moyen\s+des\s+terrains\s+au\s+m[²2]\s+à\s+(.+)",
        h1_text,
        re.IGNORECASE,
    )
    if city_match:
        return _normalize_spaces(city_match.group(1))

    dep_match = re.search(
        r"Prix\s+moyen\s+des\s+terrains\s+au\s+m[²2]\s+en\s+(.+)",
        h1_text,
        re.IGNORECASE,
    )
    if dep_match:
        return _normalize_spaces(dep_match.group(1))

    return h1_text


def _find_count(raw_text: str) -> Optional[int]:
    match = COUNT_PATTERN.search(raw_text)
    if not match:
        return None
    return _parse_int(match.group("count"))


def _find_direct_price(raw_text: str) -> Optional[float]:
    for pattern in (FRANCE_PRICE_PATTERN, DEPARTMENT_PRICE_PATTERN):
        match = pattern.search(raw_text)
        if match:
            return _parse_float(match.group("value"))
    return None


def _collect_text_lines(soup: BeautifulSoup) -> list[str]:
    lines: list[str] = []
    for text in soup.stripped_strings:
        normalized = _normalize_spaces(text)
        if normalized:
            lines.append(normalized)
    return lines


def _strip_postal_code(localisation: str) -> str:
    return _normalize_spaces(re.sub(r"\s*\(\d{5}\)\s*$", "", localisation))


def _find_price_after_city_header(lines: list[str], city_name: str) -> Optional[float]:
    city_name_folded = city_name.casefold()

    for index, line in enumerate(lines):
        normalized_line = _strip_postal_code(line).casefold()
        if normalized_line != city_name_folded:
            continue

        upper_bound = min(index + 4, len(lines))
        for candidate in lines[index + 1 : upper_bound]:
            match = PRICE_PATTERN.search(candidate)
            if match:
                return _parse_float(match.group("value"))

    return None


def _find_city_price_from_nearby_section(
    soup: BeautifulSoup,
    localisation: Optional[str],
) -> Optional[float]:
    if not localisation:
        return None

    lines = _collect_text_lines(soup)
    city_name = _strip_postal_code(localisation)

    price = _find_price_after_city_header(lines, city_name)
    if price is not None:
        return price

    return None


def parse_terrain_page(html: str, url: str, method_used: str) -> TerrainConstructionResult:
    soup = BeautifulSoup(html, "html.parser")
    raw_text = _extract_raw_text(soup)
    h1_text = _extract_h1_text(soup)
    localisation = _extract_localisation_from_h1(h1_text)
    prix_m2_moyen = _find_direct_price(raw_text)

    if prix_m2_moyen is None and h1_text and CITY_AROUND_PATTERN.search(raw_text):
        prix_m2_moyen = _find_city_price_from_nearby_section(soup, localisation)

    result = TerrainConstructionResult(
        prix_m2_moyen=prix_m2_moyen,
        nombre_terrains_utilises=_find_count(raw_text),
        localisation=localisation,
        url_source=url,
        method_used=method_used,
    )

    if result.prix_m2_moyen is None:
        result.error = "Prix moyen introuvable dans le contenu analysé."

    return result


def _fetch_html_with_requests(url: str, timeout: int = 20) -> str:
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def _get_department_url_from_city_url(url: str) -> Optional[str]:
    prefix = "https://www.terrain-construction.com/prix-moyen-terrain/"
    if not url.startswith(prefix):
        return None

    path = url[len(prefix) :].strip("/")
    segments = path.split("/")
    if len(segments) < 2:
        return None

    return prefix + segments[0]


def _fetch_html_with_playwright(url: str, timeout_ms: int = 30000) -> str:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(user_agent=DEFAULT_HEADERS["User-Agent"])
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(1500)

            for selector in (
                'button:has-text("Autoriser")',
                'button:has-text("Tout accepter")',
                'button:has-text("Accepter")',
                'button:has-text("Confirmer les choix")',
            ):
                try:
                    button = page.locator(selector).first
                    if button.is_visible(timeout=1500):
                        button.click(timeout=3000)
                        page.wait_for_timeout(1500)
                        break
                except Exception:
                    pass

            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass

            try:
                page.wait_for_function(
                    """
                    () => !document.body || !document.body.innerText.includes('Chargement...')
                    """,
                    timeout=12000,
                )
            except Exception:
                pass

            try:
                page.wait_for_function(
                    """
                    () => /Calculé\\s+sur\\s+\\d+[\\s\\u00a0\\u202f]*terrains?\\s+à\\s+vendre/i
                        .test(document.body ? document.body.innerText : '')
                    """,
                    timeout=12000,
                )
            except Exception:
                pass

            page.wait_for_timeout(3500)
            return page.content()
        finally:
            browser.close()


def get_prix_terrain_by_url(
    url: str,
    use_playwright_fallback: bool = True,
) -> TerrainConstructionResult:
    request_error: Optional[str] = None

    try:
        html = _fetch_html_with_requests(url)
        result = parse_terrain_page(html=html, url=url, method_used="requests")
        if result.prix_m2_moyen is not None:
            return result
        request_error = result.error
    except requests.RequestException as exc:
        request_error = f"Erreur requests: {exc}"

    if not use_playwright_fallback:
        result = TerrainConstructionResult(
            localisation=None,
            url_source=url,
            method_used="requests",
            error=request_error or "Échec de récupération via requests.",
        )
        return _apply_department_fallback(result)

    try:
        html = _fetch_html_with_playwright(url)
        result = parse_terrain_page(html=html, url=url, method_used="playwright")
        if result.prix_m2_moyen is not None:
            return result

        result.error = (
            f"{request_error} | {result.error}" if request_error else result.error
        )
        return _apply_department_fallback(result)
    except Exception as exc:
        result = TerrainConstructionResult(
            localisation=None,
            url_source=url,
            method_used="playwright",
            error=(
                f"{request_error} | Erreur Playwright: {exc}"
                if request_error
                else f"Erreur Playwright: {exc}"
            ),
        )
        return _apply_department_fallback(result)


def _apply_department_fallback(
    result: TerrainConstructionResult,
) -> TerrainConstructionResult:
    if result.prix_m2_moyen is not None or not result.url_source:
        return result

    department_url = _get_department_url_from_city_url(result.url_source)
    if not department_url:
        return result

    try:
        department_html = _fetch_html_with_requests(department_url)
        department_result = parse_terrain_page(
            html=department_html,
            url=department_url,
            method_used="requests",
        )
    except requests.RequestException as exc:
        result.error = (
            f"{result.error} | Fallback département impossible: {exc}"
            if result.error
            else f"Fallback département impossible: {exc}"
        )
        return result

    if department_result.prix_m2_moyen is None:
        result.error = (
            f"{result.error} | Prix département introuvable."
            if result.error
            else "Prix département introuvable."
        )
        return result

    result.prix_m2_moyen = department_result.prix_m2_moyen
    result.error = None
    if result.localisation:
        result.localisation = f"{result.localisation} [fallback département]"
    else:
        result.localisation = department_result.localisation
    return result


def result_to_json(result: TerrainConstructionResult) -> str:
    return json.dumps(asdict(result), ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape le prix moyen des terrains sur terrain-construction.com"
    )
    parser.add_argument(
        "url",
        nargs="?",
        default=(
            "https://www.terrain-construction.com/prix-moyen-terrain/"
            "hauts-de-seine-92/neuilly-sur-seine-92200"
        ),
        help="URL terrain-construction.com à scraper",
    )
    parser.add_argument(
        "--no-playwright-fallback",
        action="store_true",
        help="Désactive le fallback Playwright",
    )
    parser.add_argument(
        "--output",
        help="Chemin du fichier JSON de sortie",
    )
    args = parser.parse_args()

    result = get_prix_terrain_by_url(
        args.url,
        use_playwright_fallback=not args.no_playwright_fallback,
    )
    payload = result_to_json(result)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as file_obj:
            file_obj.write(payload + "\n")
        return

    print(payload)


if __name__ == "__main__":
    main()
