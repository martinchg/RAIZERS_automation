"""
Scraper efficity.com via Playwright
- Appartement : /estimation-immobiliere/appartement/
- Maison      : /estimation-immobiliere/maison/
- Géocodage   : Nominatim (OpenStreetMap, pas de clé API)
- Proxy Apify : optionnel via APIFY_API_TOKEN

Install : pip install playwright requests python-dotenv
          playwright install chromium
"""

import asyncio
import os
import re
import time
import requests
from dataclasses import dataclass, field
from typing import Optional, List
from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page

load_dotenv()

URL_APPART = "https://www.efficity.com/estimation-immobiliere/appartement/"
URL_MAISON  = "https://www.efficity.com/estimation-immobiliere/maison/"

# ─────────────────────────────────────────────
# Maps
# ─────────────────────────────────────────────

DPE_MAP = {"ne sais pas": 0, "a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7}

BUILDING_MATERIAL_MAP = {
    "beton": 6, "beton cellulaire": 17, "parpaing": 8, "bois": 11,
    "brique": 2, "pierre": 15, "pise": 13, "pierre de paris": 14,
    "ossature bois": 16, "pierre de taille": 1, "pierre et brique": 5,
    "meuliere": 7, "pierre et parpaing": 9, "ardoise zinc": 10,
    "moellon": 12, "autres": 3, "ne sais pas": 4,
}

IMPORTANT_REPAIRS_LABELS = {
    "electricite": 0, "isolation": 1, "plomberie": 2,
    "fenetres": 3, "sols": 4, "autres gros travaux": 8,
    # maison uniquement
    "toiture": 5, "facade": 6, "chauffage": 7,
}

SMALL_REPAIRS_LABELS = {
    "peinture": 0, "salle de bain": 1, "cuisine": 2, "autres": 3,
}


def _annee_to_building_year(annee: int) -> int:
    if annee < 1850:  return 1
    if annee <= 1919: return 2
    if annee <= 1944: return 3
    if annee <= 1969: return 4
    if annee <= 1989: return 5
    if annee <= 2006: return 6
    return 7


# ─────────────────────────────────────────────
# Modèles
# ─────────────────────────────────────────────

@dataclass
class PrixM2Result:
    prix_m2_moyen:    Optional[float] = None
    tranche_basse:    Optional[float] = None
    tranche_haute:    Optional[float] = None
    prix_total_moyen: Optional[float] = None
    prix_total_bas:   Optional[float] = None
    prix_total_haut:  Optional[float] = None
    localisation:     Optional[str]   = None
    raw_output:       Optional[str]   = None

    def __str__(self):
        lines = [
            "──────────────────────────────────────",
            f"  Localisation    : {self.localisation or 'N/A'}",
            f"  Prix/m² moyen   : {self.prix_m2_moyen:,.0f} €/m²"  if self.prix_m2_moyen  else "  Prix/m² moyen   : N/A",
            f"  Tranche basse   : {self.tranche_basse:,.0f} €/m²"  if self.tranche_basse  else "  Tranche basse   : N/A",
            f"  Tranche haute   : {self.tranche_haute:,.0f} €/m²"  if self.tranche_haute  else "  Tranche haute   : N/A",
        ]
        if self.prix_total_moyen:
            lines += [
                f"  Prix total moyen: {self.prix_total_moyen:,.0f} €",
                f"  Prix total bas  : {self.prix_total_bas:,.0f} €"  if self.prix_total_bas  else "",
                f"  Prix total haut : {self.prix_total_haut:,.0f} €" if self.prix_total_haut else "",
            ]
        if self.raw_output and not self.prix_m2_moyen:
            lines.append(f"  Réponse brute   : {self.raw_output[:300]}")
        lines.append("──────────────────────────────────────")
        return "\n".join(l for l in lines if l)


@dataclass
class BienImmobilier:
    adresse:      str
    code_postal:  str = ""
    ville:        str = ""
    type_bien:    str = "appartement"   # "appartement" | "maison"

    # ── Commun ──────────────────────────────────────────
    surface_habitable:  Optional[float] = None
    nb_pieces:          int = 1
    nb_chambres:        int = 1
    dpe:                int = 0         # 0=Ne sais pas, 1=A … 7=G
    building_material:  int = 4         # 4=Ne sais pas (voir BUILDING_MATERIAL_MAP)
    annee_construction: Optional[int] = None   # année réelle, convertie auto
    needs_repairing:    bool = False
    important_repairs:  List[int] = field(default_factory=list)  # ex: [0,2] = élec+plomb
    small_repairs:      List[int] = field(default_factory=list)  # ex: [0] = peinture
    balcony_count:      int = 0
    terrace_count:      int = 0

    # ── Appartement uniquement ───────────────────────────
    flat_floor:             int = 0
    has_lift:               bool = False
    is_rented:              bool = False
    remaining_lease_term:   Optional[int]   = None
    current_rent:           Optional[float] = None
    balcony_area:           Optional[float] = None
    terrace_area:           Optional[float] = None
    cave_count:             int = 0
    cellar_area:            Optional[float] = None
    garage_count:           int = 0
    box_parking_count:      int = 0
    external_parking_count: int = 0
    common_areas_state:     int = 2     # 1=Excellent, 2=Normal, 3=Mauvais

    # ── Maison uniquement ────────────────────────────────
    nb_fitted_floors:       int = 1             # 1/2/3
    parcel_area:            Optional[float] = None
    divisible_parcel:       Optional[bool]  = None
    nb_of_bathrooms:        int = 1             # 1 / 2
    exposure:               int = 0             # 0=Ne sais pas, 1=Nord, 2=Est, 3=Sud, 4=Ouest
    has_attic:              bool = False
    attic_state:            Optional[int]   = None  # 1=aménagé, 2=aménageable, 3=non aménageable
    attic_area:             Optional[float] = None
    has_basement:           bool = False
    basement_state:         Optional[int]   = None  # 1/2/3
    basement_has_windows:   Optional[bool]  = None
    basement_area:          Optional[float] = None
    parking_possible:       bool = False
    parking_count:          int = 0
    has_pool:               bool = False
    adjoining:              int = 0     # 0=aucune mitoyenneté, 1=1 mur, 2=2+ murs
    has_panorama:           bool = False


# ─────────────────────────────────────────────
# Géocodage via Nominatim (pas de clé API)
# ─────────────────────────────────────────────

def _geocode(bien: BienImmobilier) -> dict:
    query = f"{bien.adresse}, {bien.code_postal} {bien.ville}, France".strip(", ")
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "addressdetails": 1, "limit": 1},
            headers={"User-Agent": "RAIZERS-automation/1.0"},
            timeout=10,
        )
        data = resp.json()
        if not data:
            return {}
        r = data[0]
        addr = r.get("address", {})
        return {
            "lat": r.get("lat", ""),
            "lng": r.get("lon", ""),
            "formatted_address": r.get("display_name", query),
            "street_number": addr.get("house_number", ""),
            "route":         addr.get("road", ""),
            "postal_code":   addr.get("postcode", bien.code_postal),
            "locality":      addr.get("city") or addr.get("town") or addr.get("village") or bien.ville,
            "country":       addr.get("country_code", "fr").upper(),
            "location":      f"{r.get('lat', '')},{r.get('lon', '')}",
            "location_type": "ROOFTOP",
        }
    except Exception as e:
        print(f"  Géocodage Nominatim échoué : {e}", flush=True)
        return {}


def _get_proxy() -> Optional[dict]:
    token = os.environ.get("APIFY_API_TOKEN")
    if not token:
        return None
    return {
        "server":   "http://proxy.apify.com:8000",
        "username": "groups-RESIDENTIAL,country-FR",
        "password": token,
    }


# ─────────────────────────────────────────────
# Remplissage formulaire via injection JS (instantané)
# ─────────────────────────────────────────────

def _bool_val(v: bool) -> str:
    return "True" if v else "False"


def _build_address_script(geo: dict) -> str:
    """Remplit uniquement les champs d'adresse cachés."""
    lines = []
    def set_val(el_id: str, value):
        if value is None:
            return
        lines.append(f'(function(){{ var e=document.getElementById("{el_id}"); if(e) e.value="{value}"; }})();')
    if geo:
        set_val("id_formatted_address", geo.get("formatted_address", "").replace('"', "'"))
        set_val("id_lat",           geo.get("lat", ""))
        set_val("id_lng",           geo.get("lng", ""))
        set_val("id_location",      geo.get("location", ""))
        set_val("id_location_type", geo.get("location_type", ""))
        set_val("id_route",         geo.get("route", "").replace('"', "'"))
        set_val("id_street_number", geo.get("street_number", ""))
        set_val("id_postal_code",   geo.get("postal_code", ""))
        set_val("id_locality",      geo.get("locality", "").replace('"', "'"))
        set_val("id_country",       geo.get("country", ""))
    return "\n".join(lines)


def _build_fill_script(bien: BienImmobilier, geo: dict) -> str:
    """Génère un script JS qui remplit tous les champs du formulaire (hors adresse)."""
    lines = []

    def set_val(el_id: str, value):
        if value is None:
            return
        lines.append(f'(function(){{ var e=document.getElementById("{el_id}"); if(e) e.value="{value}"; }})();')

    def set_radio(name: str, value):
        lines.append(f'(function(){{ var e=document.querySelector("input[name=\\"{name}\\"][value=\\"{value}\\"]"); if(e) e.checked=true; }})();')

    def set_checkbox(name: str, value):
        lines.append(f'(function(){{ var e=document.querySelector("input[name=\\"{name}\\"][value=\\"{value}\\"]"); if(e) e.checked=true; }})();')

    # ── Commun ───────────────────────────────────────────────────
    set_val("id_living_area",    int(bien.surface_habitable) if bien.surface_habitable else "")
    set_val("id_nb_of_rooms",    bien.nb_pieces)
    set_val("id_nb_of_bedrooms", bien.nb_chambres)
    set_radio("dpe", bien.dpe)
    set_radio("building_material", bien.building_material)
    if bien.annee_construction:
        set_radio("building_year", _annee_to_building_year(bien.annee_construction))
    set_radio("needs_repairing", _bool_val(bien.needs_repairing))
    for v in bien.important_repairs:
        set_checkbox("important_repairs", v)
    for v in bien.small_repairs:
        set_checkbox("small_repairs", v)
    set_val("id_balcony_count", bien.balcony_count)
    set_val("id_terrace_count", bien.terrace_count)

    # ── Appartement ──────────────────────────────────────────────
    if bien.type_bien.lower() != "maison":
        set_val("id_flat_floor", bien.flat_floor)
        set_radio("has_lift",    _bool_val(bien.has_lift))
        set_radio("is_rented",   _bool_val(bien.is_rented))
        if bien.is_rented:
            set_val("id_remaining_lease_term", bien.remaining_lease_term)
            set_val("id_current_rent",         bien.current_rent)
        if bien.balcony_count and bien.balcony_area:
            set_val("id_balcony_area", int(bien.balcony_area))
        if bien.terrace_count and bien.terrace_area:
            set_val("id_terrace_area", int(bien.terrace_area))
        set_val("id_cave_count",             bien.cave_count)
        if bien.cave_count and bien.cellar_area:
            set_val("id_cellar_area", int(bien.cellar_area))
        set_val("id_garage_count",           bien.garage_count)
        set_val("id_box_parking_count",      bien.box_parking_count)
        set_val("id_external_parking_count", bien.external_parking_count)
        set_radio("common_areas_state",      bien.common_areas_state)

    # ── Maison ───────────────────────────────────────────────────
    else:
        set_radio("nb_fitted_floors", bien.nb_fitted_floors)
        set_val("id_parcel_area", int(bien.parcel_area) if bien.parcel_area else "")
        if bien.divisible_parcel is not None:
            set_radio("divisible_parcel", _bool_val(bien.divisible_parcel))
        set_radio("nb_of_bathrooms", bien.nb_of_bathrooms)
        set_radio("exposure",        bien.exposure)
        set_radio("has_attic",       _bool_val(bien.has_attic))
        if bien.has_attic and bien.attic_state:
            set_radio("attic_state", bien.attic_state)
            set_val("id_attic_area", int(bien.attic_area) if bien.attic_area else "")
        set_radio("has_basement", _bool_val(bien.has_basement))
        if bien.has_basement:
            if bien.basement_state:
                set_radio("basement_state", bien.basement_state)
            if bien.basement_has_windows is not None:
                set_radio("basement_has_windows", _bool_val(bien.basement_has_windows))
            set_val("id_basement_area", int(bien.basement_area) if bien.basement_area else "")
        set_radio("parking_possible", _bool_val(bien.parking_possible))
        if bien.parking_possible:
            set_val("id_parking_count", bien.parking_count)
        set_radio("has_pool",    _bool_val(bien.has_pool))
        set_radio("adjoining",   bien.adjoining)
        set_radio("has_panorama", _bool_val(bien.has_panorama))

    return "\n".join(lines)


# ─────────────────────────────────────────────
# Parsing du résultat
# ─────────────────────────────────────────────

def _nettoyer(s: str) -> Optional[float]:
    try:
        return float(re.sub(r'[\s ,]', '', s.replace('\xa0', '')))
    except Exception:
        return None



async def _extract_result(page: Page, bien: BienImmobilier) -> PrixM2Result:
    """Extrait le résultat Efficity via les sélecteurs CSS du résultat réel."""
    result = PrixM2Result(
        localisation=f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", "),
    )

    for selector, label in [
        ('.valuation-result-low, [class*="result-low"], [class*="price-low"]', 'bas'),
        ('.valuation-result-high, [class*="result-high"], [class*="price-high"]', 'haut'),
        ('.valuation-result-average, [class*="result-average"], [class*="result-main"]', 'moyen'),
        ('.valuation-price-m2, [class*="price-m2"]', 'm2'),
    ]:
        try:
            els = await page.locator(selector).all()
            for el in els:
                txt = await el.inner_text()
                import re as _re
                v = _nettoyer(_re.sub(r'[^\d\s,.\xa0]', '', txt))
                if v and label in ('bas', 'haut', 'moyen') and 10000 < v < 100_000_000:
                    if label == 'bas':   result.prix_total_bas   = v
                    if label == 'haut':  result.prix_total_haut  = v
                    if label == 'moyen': result.prix_total_moyen = v
                elif v and label == 'm2' and 500 < v < 50000:
                    result.prix_m2_moyen = v
        except Exception:
            pass

    text = await page.inner_text("body")
    result.raw_output = text

    if not result.prix_m2_moyen:
        for pat in [
            r'(\d[\d\s\xa0]*)\s*\u20ac\s*/\s*m\s*[\xb22]',
            r'm[\xb22]\s*[:\-]\s*(\d[\d\s\xa0]*)\s*\u20ac',
        ]:
            for m in re.findall(pat, text, re.IGNORECASE):
                v = _nettoyer(m)
                if v and 500 < v < 50000:
                    result.prix_m2_moyen = v
                    break
            if result.prix_m2_moyen:
                break

    if not result.prix_total_moyen:
        for pat, label in [
            (r'(?:basse?|minimum|min)[^\d]{0,40}([\d\xa0\s]{5,})\s*\u20ac', 'bas'),
            (r'(?:moyenne?|estim|valeur)[^\d]{0,40}([\d\xa0\s]{5,})\s*\u20ac', 'moyen'),
            (r'(?:haute?|maximum|max)[^\d]{0,40}([\d\xa0\s]{5,})\s*\u20ac', 'haut'),
        ]:
            for m in re.findall(pat, text, re.IGNORECASE):
                v = _nettoyer(m)
                if v and 10000 < v < 100_000_000:
                    if label == 'bas' and not result.prix_total_bas:    result.prix_total_bas   = v
                    if label == 'moyen' and not result.prix_total_moyen: result.prix_total_moyen = v
                    if label == 'haut' and not result.prix_total_haut:   result.prix_total_haut  = v
                    break

    surface = bien.surface_habitable
    if surface and surface > 0:
        if result.prix_total_moyen and not result.prix_m2_moyen:
            result.prix_m2_moyen = round(result.prix_total_moyen / surface)
        if result.prix_total_bas and not result.tranche_basse:
            result.tranche_basse = round(result.prix_total_bas / surface)
        if result.prix_total_haut and not result.tranche_haute:
            result.tranche_haute = round(result.prix_total_haut / surface)

    return result

def _parser_resultat(text: str, bien: BienImmobilier) -> PrixM2Result:
    result = PrixM2Result(
        localisation=f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", "),
        raw_output=text,
    )

    # Prix au m²
    for pat in [
        r'(\d[\d\s ]*)\s*€\s*/\s*m\s*[²2]',
        r'(\d[\d\s ]*)\s*euros?\s*/\s*m\s*[²2]',
        r'm[²2]\s*[:\-]\s*(\d[\d\s ]*)\s*€',
    ]:
        for m in re.findall(pat, text, re.IGNORECASE):
            v = _nettoyer(m)
            if v and 500 < v < 50000:
                result.prix_m2_moyen = v
                break
        if result.prix_m2_moyen:
            break

    # Prix totaux
    for pat, label in [
        (r'(?:basse?|minimum|min)[^\d]{0,40}([\d\s ]{5,})\s*€', 'bas'),
        (r'(?:moyenne?|estim|valeur)[^\d]{0,40}([\d\s ]{5,})\s*€', 'moyen'),
        (r'(?:haute?|maximum|max)[^\d]{0,40}([\d\s ]{5,})\s*€', 'haut'),
    ]:
        for m in re.findall(pat, text, re.IGNORECASE):
            v = _nettoyer(m)
            if v and 10000 < v < 100_000_000:
                if label == 'bas':   result.prix_total_bas   = v
                if label == 'moyen': result.prix_total_moyen = v
                if label == 'haut':  result.prix_total_haut  = v
                break

    # Déduire €/m² depuis prix totaux si besoin
    surface = bien.surface_habitable
    if surface and surface > 0:
        if result.prix_total_moyen and not result.prix_m2_moyen:
            result.prix_m2_moyen = round(result.prix_total_moyen / surface)
        if result.prix_total_bas:
            result.tranche_basse = round(result.prix_total_bas / surface)
        if result.prix_total_haut:
            result.tranche_haute = round(result.prix_total_haut / surface)

    return result


# ─────────────────────────────────────────────
# Scraper principal
# ─────────────────────────────────────────────

async def _click_tunnel_next(page: Page) -> bool:
    """Clique sur .tunnel-next si visible. Retourne True si cliqué."""
    try:
        btn = page.locator('a.tunnel-next:visible, button.tunnel-next:visible').first
        if await btn.count() > 0:
            await btn.click(timeout=5000)
            await page.wait_for_timeout(600)
            return True
    except Exception:
        pass
    return False


async def _scrape_async(bien: BienImmobilier) -> PrixM2Result:
    url = URL_MAISON if bien.type_bien.lower() == "maison" else URL_APPART
    form_id = "house-form" if bien.type_bien.lower() == "maison" else "flat-form"
    proxy = _get_proxy()

    # 1. Géocodage
    print("[1/5] Géocodage de l'adresse...", flush=True)
    t = time.time()
    geo = _geocode(bien)
    if geo:
        print(f"      OK ({time.time()-t:.1f}s) — {geo.get('formatted_address', '')[:60]}", flush=True)
    else:
        print(f"      Géocodage échoué, on continue sans coordonnées", flush=True)
        geo = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx_opts = {"proxy": proxy} if proxy else {}
        ctx  = await browser.new_context(**ctx_opts)
        page = await ctx.new_page()
        page.set_default_timeout(15000)

        # 2. Charger la page d'estimation
        print(f"[2/5] Chargement de la page ({bien.type_bien})...", flush=True)
        t = time.time()
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(800)
        print(f"      OK ({time.time()-t:.1f}s)", flush=True)

        # 3. Étape 1 — remplir l'adresse et aller sur la page carte
        print("[3/5] Étape 1 — adresse → page carte...", flush=True)
        t = time.time()

        addr_str = f"{bien.adresse}, {bien.code_postal} {bien.ville}".strip(", ")

        # Remplir le champ texte visible
        addr_input = page.locator('#valuation_address_input, input[name="address"], input[placeholder*="adresse" i]').first
        try:
            await addr_input.fill(addr_str, timeout=5000)
        except Exception:
            pass

        # Injecter les champs cachés via JS
        await page.evaluate(_build_address_script(geo))

        # Cliquer sur le bouton qui mène à la page de carte
        clicked = False
        for sel in ['.go-to-localisation-map', 'a[href*="localisation"]', '#step1-next']:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    await btn.click(timeout=5000)
                    clicked = True
                    break
            except Exception:
                continue

        if not clicked:
            # Fallback : cliquer .tunnel-next pour passer l'étape 1
            clicked = await _click_tunnel_next(page)

        if clicked:
            await page.wait_for_load_state("networkidle", timeout=20000)

        print(f"      OK ({time.time()-t:.1f}s) — URL: {page.url}", flush=True)

        # 4. Page de localisation/carte — confirmer les coordonnées et soumettre
        if "localisation" in page.url:
            print("[4/5] Page carte — injection coordonnées + soumission...", flush=True)
            t = time.time()

            lat  = geo.get("lat", "")
            lng  = geo.get("lng", "")
            loc  = geo.get("location", f"{lat},{lng}")
            addr = geo.get("formatted_address", addr_str).replace('"', "'")

            await page.evaluate(f"""
                (function() {{
                    function setField(selectors, value) {{
                        for (var s of selectors) {{
                            var el = document.querySelector(s);
                            if (el) {{ el.value = value; return; }}
                        }}
                    }}
                    setField(['#id_lat', '[name="lat"]', '[name="latitude"]'], "{lat}");
                    setField(['#id_lng', '[name="lng"]', '[name="longitude"]'], "{lng}");
                    setField(['#id_location', '[name="location"]'], "{loc}");
                    setField(['#id_formatted_address', '[name="formatted_address"]', '[name="address"]'], "{addr}");
                    setField(['#id_location_type', '[name="location_type"]'], "ROOFTOP");
                    setField(['#id_postal_code', '[name="postal_code"]'], "{geo.get('postal_code', '')}");
                    setField(['#id_locality', '[name="locality"]'], "{geo.get('locality', '').replace('"', "'")}");
                    setField(['#id_route', '[name="route"]'], "{geo.get('route', '').replace('"', "'")}");
                    setField(['#id_street_number', '[name="street_number"]'], "{geo.get('street_number', '')}");
                    setField(['#id_country', '[name="country"]'], "{geo.get('country', 'FR')}");
                }})();
            """)

            # Essayer de cliquer le bouton de validation de la carte
            map_submitted = False
            for sel in [
                'button[type="submit"]:visible',
                'input[type="submit"]:visible',
                '.btn-primary:visible',
                'a.tunnel-next:visible',
            ]:
                try:
                    btn = page.locator(sel).first
                    if await btn.count() > 0:
                        await btn.click(timeout=5000)
                        map_submitted = True
                        break
                except Exception:
                    continue

            if not map_submitted:
                await page.evaluate("document.querySelector('form').submit()")

            await page.wait_for_load_state("networkidle", timeout=20000)
            print(f"      OK ({time.time()-t:.1f}s) — URL: {page.url}", flush=True)
        else:
            print("[4/5] Pas de page carte détectée, on continue...", flush=True)

        # 5. Wizard étapes 2-6 : injecter tous les champs puis avancer
        print("[5/5] Remplissage des étapes 2-6 et soumission...", flush=True)
        t = time.time()

        # Injecter tous les champs (superficie, pièces, état, options…)
        await page.evaluate(_build_fill_script(bien, geo))
        await page.wait_for_timeout(300)

        # Avancer étape par étape (jusqu'à 6 fois)
        for step_num in range(6):
            advanced = await _click_tunnel_next(page)
            if not advanced:
                break
            # Re-injecter après chaque étape car certains champs peuvent être re-rendus
            await page.evaluate(_build_fill_script(bien, geo))

        # Soumettre le formulaire final
        submitted = False
        for sel in ['button[type="submit"]:visible', 'input[type="submit"]:visible']:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    await btn.click(timeout=10000)
                    submitted = True
                    break
            except Exception:
                continue

        if not submitted:
            await page.evaluate(f"document.getElementById('{form_id}').submit()")

        await page.wait_for_load_state("networkidle", timeout=30000)
        await page.wait_for_timeout(1000)

        final_url = page.url
        print(f"      OK ({time.time()-t:.1f}s) — URL: {final_url}", flush=True)

        result = await _extract_result(page, bien)
        await browser.close()

    return result


def get_prix_m2(bien: BienImmobilier) -> PrixM2Result:
    return asyncio.run(_scrape_async(bien))


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

if __name__ == "__main__":
    # ── Test appartement ──
    bien_appart = BienImmobilier(
        adresse="12 rue de Rivoli",
        code_postal="75001",
        ville="Paris",
        type_bien="appartement",
        surface_habitable=65,
        nb_pieces=3,
        nb_chambres=2,
        flat_floor=4,
        has_lift=True,
        balcony_count=1,
        balcony_area=6,
        cave_count=1,
        annee_construction=1972,
        dpe=4,                   # D
        building_material=15,    # Pierre
        common_areas_state=2,    # Normal
    )

    print("=== Test appartement ===")
    res = get_prix_m2(bien_appart)
    print(res)
