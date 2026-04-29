"""
Scraper meilleursagents.fr — Firecrawl browser + Playwright local via CDP
- Firecrawl crée le navigateur (contourne la détection bot)
- Playwright local pilote le navigateur avec précision (valeurs exactes)
- Interception réseau pour capturer les prix avant l'écran de compte

Variables d'environnement requises :
    FIRECRAWL_API_KEY   → clé API Firecrawl
    MA_EMAIL            → email du compte meilleursagents
    MA_PASSWORD         → mot de passe meilleursagents

Install : pip install firecrawl-py playwright && playwright install chromium
"""

import asyncio
import os
import re
import time
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv
from firecrawl import FirecrawlApp
from playwright.async_api import async_playwright, Page, Response

load_dotenv()


# ─────────────────────────────────────────────
# Modèle de retour
# ─────────────────────────────────────────────

@dataclass
class PrixM2Result:
    prix_m2_moyen: Optional[float] = None
    tranche_basse: Optional[float] = None
    tranche_haute: Optional[float] = None
    prix_total_moyen: Optional[float] = None
    prix_total_bas: Optional[float] = None
    prix_total_haut: Optional[float] = None
    localisation: Optional[str] = None

    def __str__(self):
        lines = [
            "──────────────────────────────────────",
            f"  Localisation    : {self.localisation or 'N/A'}",
            f"  Prix/m² moyen   : {self.prix_m2_moyen:,.0f} €/m²" if self.prix_m2_moyen else "  Prix/m² moyen   : N/A",
            f"  Tranche basse   : {self.tranche_basse:,.0f} €/m²" if self.tranche_basse else "  Tranche basse   : N/A",
            f"  Tranche haute   : {self.tranche_haute:,.0f} €/m²" if self.tranche_haute else "  Tranche haute   : N/A",
        ]
        if self.prix_total_moyen:
            lines += [
                f"  Prix total moyen: {self.prix_total_moyen:,.0f} €",
                f"  Prix total bas  : {self.prix_total_bas:,.0f} €" if self.prix_total_bas else "",
                f"  Prix total haut : {self.prix_total_haut:,.0f} €" if self.prix_total_haut else "",
            ]
        lines.append("──────────────────────────────────────")
        return "\n".join(l for l in lines if l)


# ─────────────────────────────────────────────
# Paramètres du bien
# ─────────────────────────────────────────────

@dataclass
class BienImmobilier:
    adresse: str
    code_postal: str = ""
    ville: str = ""
    type_bien: str = "appartement"          # "appartement" ou "maison"

    surface_habitable: Optional[float] = None
    surface_terrain: Optional[float] = None
    surface_encore_constructible: Optional[float] = None

    nb_pieces: Optional[int] = None
    nb_chambres: Optional[int] = None
    nb_salles_bain: Optional[int] = None

    nb_niveaux: Optional[int] = None
    etage: Optional[int] = None
    nb_etages_immeuble: Optional[int] = None

    ascenseur: bool = False
    balcon: bool = False
    surface_balcon: Optional[float] = None
    terrasse: bool = False
    surface_terrasse: Optional[float] = None
    nb_caves: int = 0
    nb_places_parking: int = 0
    nb_chambres_service: int = 0

    annee_construction: Optional[int] = None
    dpe: Optional[str] = None
    etat_bien: Optional[str] = None
    luminosite: Optional[str] = None
    calme: Optional[str] = None


# ─────────────────────────────────────────────
# Helpers Playwright
# ─────────────────────────────────────────────

async def _cliquer_texte(page: Page, texte: str, timeout: int = 3000) -> bool:
    try:
        await page.get_by_text(texte, exact=False).first.wait_for(timeout=timeout)
        await page.get_by_text(texte, exact=False).first.click()
        return True
    except Exception:
        return False


async def _cliquer_bouton(page: Page, textes: list[str], timeout: int = 2000) -> bool:
    for t in textes:
        try:
            btn = page.locator(f'button:has-text("{t}")').first
            await btn.wait_for(timeout=timeout)
            await btn.click()
            return True
        except Exception:
            continue
    return False


async def _set_counter(page: Page, field_id: str, target: int, label: str = ""):
    """Set a counter input (with +/- buttons) to target value."""
    try:
        current = int(await page.locator(f'#{field_id}').input_value() or 1)
    except Exception:
        current = 1
    delta = target - current
    if delta == 0:
        if label:
            print(f"      ✓ {label} = {target}", flush=True)
        return
    btn_class = '.field__count--up' if delta > 0 else '.field__count--down'
    btn = page.locator(f'.field--counter:has(#{field_id}) {btn_class}')
    for _ in range(abs(delta)):
        await btn.click()
        await page.wait_for_timeout(50)
    if label:
        print(f"      ✓ {label} = {target}", flush=True)


async def _check_amenity(page: Page, field_id: str, label: str = ""):
    """Click a hidden checkbox amenity via its label."""
    try:
        await page.locator(f'label[for="{field_id}"]').first.click()
        await page.wait_for_timeout(200)
        if label:
            print(f"      ✓ {label}", flush=True)
        return True
    except Exception:
        try:
            await page.locator(f'#{field_id}').click(force=True)
            await page.wait_for_timeout(200)
            return True
        except Exception:
            return False


async def _set_select(page: Page, field_id: str, value: str, label: str = ""):
    """Set a <select> element to the given option value."""
    try:
        await page.select_option(f'#{field_id}', value=value, timeout=3000)
        if label:
            print(f"      ✓ {label} = {value}", flush=True)
        return True
    except Exception as e:
        if label:
            print(f"      ✗ {label}: {e}", flush=True)
        return False


def _annee_to_build_period(annee: int) -> str:
    if annee < 1850: return "1849"
    if annee <= 1913: return "1851"
    if annee <= 1947: return "1915"
    if annee <= 1969: return "1949"
    if annee <= 1980: return "1971"
    if annee <= 1991: return "1982"
    if annee <= 2000: return "1993"
    if annee <= 2010: return "2002"
    return "2012"


def _etat_to_renovation_level(etat: str) -> str:
    e = etat.lower()
    if any(k in e for k in ["neuf", "refait", "rénov", "renov"]): return "RENOVATION_LEVEL.GOOD_AS_NEW"
    if any(k in e for k in ["important", "mauvais", "gros"]): return "RENOVATION_LEVEL.HEAVY_WORK_REQUIRED"
    if any(k in e for k in ["rafraich", "léger", "leger", "travaux"]): return "RENOVATION_LEVEL.LIGHT_WORK_REQUIRED"
    return "RENOVATION_LEVEL.STANDARD"


# ─────────────────────────────────────────────
# Remplissage du formulaire
# ─────────────────────────────────────────────

async def _remplir_formulaire(page: Page, bien: BienImmobilier):
    adresse_complete = bien.adresse
    if bien.code_postal or bien.ville:
        adresse_complete = f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", ")

    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_timeout(2000)

    # Cookies
    for sel in ["button:has-text('Tout accepter')", "button:has-text('Accepter')", "button[id*='accept']"]:
        try:
            await page.locator(sel).first.click(timeout=2000)
            break
        except Exception:
            pass

    # ── Page 0 : Adresse ─────────────────────
    print("      Adresse...", flush=True)
    try:
        el = page.locator('#valuation-search-hero-input')
        await el.wait_for(timeout=5000)
        await el.fill(adresse_complete)
        await page.wait_for_timeout(2000)
        for sugg_sel in ["li[role='option']", "[class*='suggestion'] li", "[class*='autocomplete'] li"]:
            try:
                await page.locator(sugg_sel).first.wait_for(timeout=3000)
                await page.locator(sugg_sel).first.click()
                break
            except Exception:
                pass
        else:
            await el.press("ArrowDown")
            await el.press("Enter")
    except Exception as e:
        print(f"      Adresse erreur: {e}", flush=True)
    await page.wait_for_timeout(1500)

    # ── Type de bien ──────────────────────────
    print("      Type de bien...", flush=True)
    type_map = {"appartement": "apartment", "maison": "house",
                "duplex": "duplex", "triplex": "triplex", "loft": "loft"}
    type_id = type_map.get(bien.type_bien.lower(), "apartment")
    try:
        await page.locator(f'#{type_id}').click(timeout=3000)
    except Exception:
        label = "Appartement" if bien.type_bien == "appartement" else "Maison"
        await _cliquer_texte(page, label)
    await page.wait_for_timeout(500)
    await _cliquer_bouton(page, ["Suivant", "Continuer"])
    await page.wait_for_timeout(1000)

    # ── Page 2 : Surfaces et pièces ───────────
    print("      Surfaces et pièces...", flush=True)
    if bien.surface_habitable:
        await page.locator('#area').fill(str(int(bien.surface_habitable)))
        print(f"      ✓ surface = {int(bien.surface_habitable)}", flush=True)

    if bien.surface_terrain and bien.type_bien in ("maison", "duplex", "triplex"):
        try:
            await page.locator('#land_area').fill(str(int(bien.surface_terrain)))
            print(f"      ✓ terrain = {int(bien.surface_terrain)}", flush=True)
        except Exception:
            pass

    if bien.nb_pieces:
        await _set_counter(page, 'room_count', bien.nb_pieces, "pièces")

    if bien.nb_salles_bain:
        await _set_counter(page, 'bathroom_count', bien.nb_salles_bain, "sdb")

    if bien.type_bien == "appartement":
        if bien.etage is not None:
            await _set_counter(page, 'floor', bien.etage, "étage")
        if bien.nb_etages_immeuble:
            await _set_counter(page, 'floor_count', bien.nb_etages_immeuble, "nb étages")
    elif bien.nb_niveaux:
        await _set_counter(page, 'level_count', bien.nb_niveaux, "niveaux")

    await _cliquer_bouton(page, ["Suivant", "Continuer"])
    await page.wait_for_timeout(1000)

    # ── Page 3 : Équipements ──────────────────
    print("      Équipements...", flush=True)
    if bien.ascenseur:
        await _check_amenity(page, 'elevator', "ascenseur")
    if bien.balcon:
        await _check_amenity(page, 'balcony', "balcon")
        if bien.surface_balcon:
            await page.locator('#balcony_area').fill(str(int(bien.surface_balcon)))
            print(f"      ✓ surface balcon = {int(bien.surface_balcon)}", flush=True)
    if bien.terrasse:
        await _check_amenity(page, 'terrace', "terrasse")
        if bien.surface_terrasse:
            await page.locator('#terrace_area').fill(str(int(bien.surface_terrasse)))
            print(f"      ✓ surface terrasse = {int(bien.surface_terrasse)}", flush=True)
    if bien.nb_caves > 0:
        await _check_amenity(page, 'cellar', "cave")
        if bien.nb_caves > 1:
            await _set_counter(page, 'cellar_count', bien.nb_caves, "nb caves")
    if bien.nb_places_parking > 0:
        await _check_amenity(page, 'parking', "parking")
        if bien.nb_places_parking > 1:
            await _set_counter(page, 'parking_count', bien.nb_places_parking, "nb parking")
    if bien.nb_chambres_service > 0:
        await _check_amenity(page, 'secondary_room', "chambre service")
        if bien.nb_chambres_service > 1:
            await _set_counter(page, 'secondary_room_count', bien.nb_chambres_service, "nb ch. service")

    await _cliquer_bouton(page, ["Suivant", "Continuer"])
    await page.wait_for_timeout(1000)

    # ── Page 4 : Précisions (facultatif) ──────
    print("      Précisions...", flush=True)
    if bien.annee_construction:
        await _set_select(page, 'build_period',
                          _annee_to_build_period(bien.annee_construction), "période construction")
    if bien.etat_bien:
        await _set_select(page, 'renovation_level',
                          _etat_to_renovation_level(bien.etat_bien), "état bien")

    await _cliquer_bouton(page, ["Suivant", "Continuer"])
    await page.wait_for_timeout(1000)

    # ── Page 5 : Profil ───────────────────────
    print("      Profil...", flush=True)
    # Non propriétaire + je m'informe
    try:
        await page.locator('#false-profile_owner').click(timeout=2000)
    except Exception:
        pass
    await page.wait_for_timeout(400)
    await _set_select(page, 'profile_buyer', 'BUYER_PROFILE.TOURIST', "raison")

    await _cliquer_bouton(page, ["Estimer", "Voir mon estimation", "Suivant"])
    await page.wait_for_timeout(3000)
    print("      Formulaire soumis", flush=True)


# ─────────────────────────────────────────────
# Extraction des prix depuis la page résultat
# ─────────────────────────────────────────────

async def _extraire_depuis_page(page: Page, bien: BienImmobilier) -> PrixM2Result:
    result = PrixM2Result(
        localisation=f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", ")
    )

    contenu = await page.content()

    def nettoyer(s: str) -> Optional[float]:
        try:
            return float(re.sub(r'[\s\u00a0,]', '', s))
        except Exception:
            return None

    # Prix au m²
    for pat in [r'(\d[\d\s\u00a0]*)\s*€\s*/\s*m\s*[²2]', r'(\d[\d\s\u00a0]*)\s*€/m²']:
        for m in re.findall(pat, contenu):
            v = nettoyer(m)
            if v and 500 < v < 50000:
                result.prix_m2_moyen = v
                break
        if result.prix_m2_moyen:
            break

    # Prix totaux (basse, moyenne, haute)
    totaux_labels = [
        (r'(?:basse|bas|low)[^\d]{0,20}(\d[\d\s\u00a0]{4,})\s*€', 'bas'),
        (r'(?:moyenne?|moyen|average)[^\d]{0,20}(\d[\d\s\u00a0]{4,})\s*€', 'moyen'),
        (r'(?:haute?|haut|high)[^\d]{0,20}(\d[\d\s\u00a0]{4,})\s*€', 'haut'),
    ]
    for pat, label in totaux_labels:
        for m in re.findall(pat, contenu, re.IGNORECASE):
            v = nettoyer(m)
            if v and 50000 < v < 100_000_000:
                if label == 'bas':   result.prix_total_bas = v
                if label == 'moyen': result.prix_total_moyen = v
                if label == 'haut':  result.prix_total_haut = v
                break

    # Calcul €/m² depuis totaux et surface
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

async def get_prix_m2_async(
    bien: BienImmobilier,
    api_key: Optional[str] = None,
) -> PrixM2Result:
    fc_key = api_key or os.environ.get("FIRECRAWL_API_KEY")
    if not fc_key:
        raise ValueError("FIRECRAWL_API_KEY manquante.")

    fc = FirecrawlApp(api_key=fc_key)

    # ── 1. Session navigateur Firecrawl ───────────────────────────
    print("[1/3] Création de la session navigateur Firecrawl...", flush=True)
    t = time.time()
    session = fc.browser(ttl=600)
    cdp_url = session.cdp_url
    print(f"      OK ({time.time()-t:.1f}s)", flush=True)

    async with async_playwright() as pw:
        # ── 2. Connexion Playwright via CDP ───────────────────────
        print("[2/3] Connexion Playwright au navigateur distant...", flush=True)
        t = time.time()
        browser = await pw.chromium.connect_over_cdp(cdp_url)
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = context.pages[0] if context.pages else await context.new_page()
        print(f"      OK ({time.time()-t:.1f}s)", flush=True)

        # Interception réseau
        captured: list[dict] = []
        API_PATTERNS = [r"/estimation", r"/api/", r"/price", r"/valuation"]

        async def on_response(response: Response):
            url = response.url
            if any(re.search(p, url) for p in API_PATTERNS):
                if response.status == 200 and "json" in response.headers.get("content-type", ""):
                    try:
                        body = await response.json()
                        captured.append({"url": url, "data": body})
                    except Exception:
                        pass

        page.on("response", on_response)

        # ── 3. Formulaire d'estimation ─────────────────────────────
        print("[3/3] Formulaire d'estimation...", flush=True)
        t = time.time()
        await page.goto("https://www.meilleursagents.com/estimation-immobiliere/")
        await _remplir_formulaire(page, bien)
        print(f"      OK ({time.time()-t:.1f}s)", flush=True)

        # Attendre résultat (API ou page)
        deadline = time.time() + 20
        while time.time() < deadline:
            await asyncio.sleep(0.5)

        # Debug : afficher un extrait du contenu de la page résultat
        contenu = await page.content()
        # Chercher tout ce qui ressemble à un prix
        prix_trouves = re.findall(r'[\d\s]{3,}\s*[€$]', contenu)
        print(f"      URL finale: {page.url}", flush=True)
        print(f"      Prix trouvés dans le HTML: {prix_trouves[:10]}", flush=True)
        print(f"      Titre page: {await page.title()}", flush=True)

        # Fallback : scrape visuel
        result = await _extraire_depuis_page(page, bien)
        await browser.close()
        fc.delete_browser(session.id)
        return result


def get_prix_m2(
    bien: BienImmobilier,
    api_key: Optional[str] = None,
) -> PrixM2Result:
    return asyncio.run(get_prix_m2_async(bien, api_key=api_key))


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

if __name__ == "__main__":
    bien = BienImmobilier(
        adresse="12 rue de Rivoli",
        code_postal="75001",
        ville="Paris",
        type_bien="appartement",
        surface_habitable=65,
        nb_pieces=3,
        nb_salles_bain=1,
        nb_chambres=2,
        etage=4,
        nb_etages_immeuble=7,
        ascenseur=True,
        balcon=True,
        surface_balcon=6,
        nb_caves=1,
        nb_places_parking=1,
        annee_construction=1972,
        etat_bien="bon état",
        luminosite="clair",
        calme="calme",
    )

    result = get_prix_m2(bien)
    print(result)
