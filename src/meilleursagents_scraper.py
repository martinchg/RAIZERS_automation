"""
Scraper meilleursagents.fr — module d'estimation interactif
Pilote le formulaire étape par étape avec Playwright,
intercepte la réponse API avant l'écran de création de compte.

Retourne uniquement : prix/m² moyen, tranche basse, tranche haute.

Install : pip install playwright && playwright install chromium
"""

import asyncio
import json
import re
import time
from dataclasses import dataclass
from typing import Optional

from playwright.async_api import async_playwright, Page, Response


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
    indice_confiance: Optional[int] = None
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
        if self.indice_confiance:
            lines.append(f"  Indice confiance: {'★' * self.indice_confiance}{'☆' * (5 - self.indice_confiance)}")
        lines.append("──────────────────────────────────────")
        return "\n".join(l for l in lines if l)


# ─────────────────────────────────────────────
# Paramètres du bien
# ─────────────────────────────────────────────

@dataclass
class BienImmobilier:
    # ── Localisation (obligatoire) ─────────────
    adresse: str                            # ex: "12 rue de Rivoli"
    code_postal: str = ""                   # ex: "75001"
    ville: str = ""                         # ex: "Paris"

    # ── Type ───────────────────────────────────
    type_bien: str = "appartement"          # "appartement" ou "maison"

    # ── Surfaces ───────────────────────────────
    surface_habitable: Optional[float] = None       # m² (Carrez pour appart)
    surface_terrain: Optional[float] = None         # m² — maison uniquement
    surface_encore_constructible: Optional[float] = None  # m² — maison uniquement

    # ── Pièces ─────────────────────────────────
    nb_pieces: Optional[int] = None
    nb_chambres: Optional[int] = None
    nb_salles_bain: Optional[int] = None

    # ── Niveaux / étages ───────────────────────
    nb_niveaux: Optional[int] = None            # maison : nombre de niveaux du bien
    etage: Optional[int] = None                 # appartement : étage du bien (0 = RDC)
    nb_etages_immeuble: Optional[int] = None    # appartement : nombre total d'étages

    # ── Ascenseur ──────────────────────────────
    ascenseur: bool = False

    # ── Balcon ─────────────────────────────────
    balcon: bool = False
    surface_balcon: Optional[float] = None      # m²

    # ── Terrasse ───────────────────────────────
    terrasse: bool = False
    surface_terrasse: Optional[float] = None    # m²

    # ── Cave ───────────────────────────────────
    nb_caves: int = 0

    # ── Parking / garage ───────────────────────
    nb_places_parking: int = 0

    # ── Chambre de service ─────────────────────
    nb_chambres_service: int = 0

    # ── Qualité (optionnel mais améliore la précision) ──
    annee_construction: Optional[int] = None
    dpe: Optional[str] = None               # "A", "B", "C", "D", "E", "F", "G"
    etat_bien: Optional[str] = None         # "neuf", "bon état", "à rénover"
    luminosite: Optional[str] = None        # "très clair", "clair", "standard", "sombre"
    calme: Optional[str] = None             # "très calme", "calme", "standard", "bruyant"


# ─────────────────────────────────────────────
# Helpers navigation
# ─────────────────────────────────────────────

async def _attendre_et_cliquer(page: Page, selector: str, timeout: int = 10000):
    await page.wait_for_selector(selector, timeout=timeout)
    await page.click(selector)

async def _cliquer_si_present(page: Page, selector: str, timeout: int = 3000) -> bool:
    try:
        await page.wait_for_selector(selector, timeout=timeout)
        await page.click(selector)
        return True
    except Exception:
        return False

async def _remplir_champ(page: Page, selector: str, valeur: str, timeout: int = 8000):
    await page.wait_for_selector(selector, timeout=timeout)
    await page.fill(selector, valeur)

async def _selectionner_option(page: Page, label_text: str, timeout: int = 8000) -> bool:
    """Clique sur un élément contenant le texte donné."""
    try:
        locator = page.get_by_text(label_text, exact=False)
        await locator.first.wait_for(timeout=timeout)
        await locator.first.click()
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────
# Extraction des prix depuis la réponse JSON
# ─────────────────────────────────────────────

def _extraire_prix(data: dict) -> Optional[PrixM2Result]:
    """
    Tente d'extraire les prix depuis différentes structures JSON
    que meilleursagents peut retourner.
    """
    result = PrixM2Result()

    # Chemins possibles dans la réponse
    estimation = (
        data.get("estimation")
        or data.get("result")
        or data.get("data")
        or data
    )

    # Prix au m²
    for key in ("price_per_sqm", "prix_m2", "price_sqm", "priceSqm"):
        val = estimation.get(key) or estimation.get("average", {}).get(key)
        if val:
            result.prix_m2_moyen = float(val)
            break

    # Fourchettes
    low = estimation.get("low") or estimation.get("min") or estimation.get("fourchette_basse", {})
    high = estimation.get("high") or estimation.get("max") or estimation.get("fourchette_haute", {})

    if isinstance(low, dict):
        result.tranche_basse = float(low.get("price_per_sqm", 0) or low.get("prix_m2", 0) or 0) or None
        result.prix_total_bas = float(low.get("price", 0) or low.get("prix", 0) or 0) or None
    elif isinstance(low, (int, float)):
        result.tranche_basse = float(low)

    if isinstance(high, dict):
        result.tranche_haute = float(high.get("price_per_sqm", 0) or high.get("prix_m2", 0) or 0) or None
        result.prix_total_haut = float(high.get("price", 0) or high.get("prix", 0) or 0) or None
    elif isinstance(high, (int, float)):
        result.tranche_haute = float(high)

    # Prix total moyen
    for key in ("price", "prix", "total_price"):
        val = estimation.get(key) or estimation.get("average", {}).get(key)
        if val:
            result.prix_total_moyen = float(val)
            break

    # Indice de confiance
    for key in ("confidence", "indice_confiance", "confidence_index"):
        val = estimation.get(key)
        if val:
            result.indice_confiance = int(val)
            break

    if result.prix_m2_moyen or result.tranche_basse or result.prix_total_moyen:
        return result
    return None


# ─────────────────────────────────────────────
# Remplissage du formulaire
# ─────────────────────────────────────────────

async def _remplir_formulaire(page: Page, bien: BienImmobilier):
    """Remplit le formulaire d'estimation étape par étape."""

    # ── Étape 1 : Adresse ──────────────────────
    await page.wait_for_load_state("networkidle", timeout=15000)

    # Fermer les cookies si présents
    await _cliquer_si_present(page, "button[id*='accept']", timeout=4000)
    await _cliquer_si_present(page, "button[data-testid*='accept']", timeout=2000)
    await _cliquer_si_present(page, "button:has-text('Accepter')", timeout=2000)
    await _cliquer_si_present(page, "button:has-text('Tout accepter')", timeout=2000)

    # Construire la chaîne adresse complète
    adresse_complete = bien.adresse
    if bien.code_postal or bien.ville:
        adresse_complete = f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", ")

    adresse_selectors = [
        "input[placeholder*='adresse']",
        "input[placeholder*='Adresse']",
        "input[name='address']",
        "input[type='search']",
        "input[autocomplete='off']",
        ".address-input input",
    ]
    for sel in adresse_selectors:
        try:
            await page.wait_for_selector(sel, timeout=4000)
            await page.fill(sel, adresse_complete)
            await page.wait_for_timeout(1500)
            suggestion_sel = "[data-testid='suggestion-item'], .autocomplete-item, .suggestion, li[role='option']"
            try:
                await page.wait_for_selector(suggestion_sel, timeout=5000)
                await page.click(f"{suggestion_sel}:first-child")
            except Exception:
                await page.keyboard.press("ArrowDown")
                await page.keyboard.press("Enter")
            break
        except Exception:
            continue

    await page.wait_for_timeout(1000)

    # ── Étape 2 : Type de bien ─────────────────
    type_label = "Appartement" if bien.type_bien == "appartement" else "Maison"
    await _selectionner_option(page, type_label)
    await page.wait_for_timeout(800)
    await _cliquer_si_present(page, "button:has-text('Suivant')", timeout=2000)
    await _cliquer_si_present(page, "button:has-text('Continuer')", timeout=2000)

    # ── Étape 3 : Surfaces ─────────────────────
    await page.wait_for_timeout(600)

    async def _remplir_input(selectors: list[str], valeur: str, timeout: int = 3000):
        for sel in selectors:
            try:
                await page.wait_for_selector(sel, timeout=timeout)
                await page.fill(sel, valeur)
                return
            except Exception:
                continue

    if bien.surface_habitable:
        await _remplir_input(
            ["input[name='area']", "input[name='living_area']", "input[placeholder*='urface']", "input[placeholder*='m²']"],
            str(int(bien.surface_habitable)),
        )

    # Surface terrain — maison uniquement
    if bien.surface_terrain and bien.type_bien == "maison":
        await _remplir_input(
            ["input[name='land_area']", "input[name='terrain']", "input[placeholder*='errain']"],
            str(int(bien.surface_terrain)),
        )

    # Surface encore constructible — maison uniquement
    if bien.surface_encore_constructible and bien.type_bien == "maison":
        await _remplir_input(
            ["input[name='buildable_area']", "input[placeholder*='onstruc']"],
            str(int(bien.surface_encore_constructible)),
        )

    # ── Pièces / chambres / SDB ────────────────
    if bien.nb_pieces:
        await _remplir_input(
            ["input[name='room_count']", "input[name='rooms']", "input[placeholder*='ièces']"],
            str(bien.nb_pieces),
        )

    if bien.nb_salles_bain:
        await _remplir_input(
            ["input[name='bathroom_count']", "input[name='bathrooms']", "input[placeholder*='bain']"],
            str(bien.nb_salles_bain),
        )

    if bien.nb_chambres:
        await _remplir_input(
            ["input[name='bedroom_count']", "input[name='bedrooms']", "input[placeholder*='hambre']"],
            str(bien.nb_chambres),
        )

    # ── Niveaux / étages ───────────────────────
    if bien.type_bien == "maison" and bien.nb_niveaux:
        await _remplir_input(
            ["input[name='level_count']", "input[name='levels']", "input[placeholder*='iveau']"],
            str(bien.nb_niveaux),
        )

    if bien.type_bien == "appartement":
        if bien.etage is not None:
            await _remplir_input(
                ["input[name='floor']", "input[name='floor_number']", "input[placeholder*='tage']"],
                str(bien.etage),
            )
        if bien.nb_etages_immeuble:
            await _remplir_input(
                ["input[name='floor_count']", "input[name='total_floors']", "input[placeholder*='étages']"],
                str(bien.nb_etages_immeuble),
            )

    await page.wait_for_timeout(400)
    await _cliquer_si_present(page, "button:has-text('Suivant')", timeout=2000)
    await _cliquer_si_present(page, "button:has-text('Continuer')", timeout=2000)

    # ── Étape 4 : Équipements ──────────────────
    await page.wait_for_timeout(800)

    # Ascenseur — checkbox ou bouton
    if bien.ascenseur:
        await _selectionner_option(page, "Ascenseur")

    # Balcon avec surface
    if bien.balcon:
        await _selectionner_option(page, "Balcon")
        await page.wait_for_timeout(300)
        if bien.surface_balcon:
            await _remplir_input(
                ["input[name='balcony_area']", "input[placeholder*='alcon']"],
                str(int(bien.surface_balcon)),
                timeout=2000,
            )

    # Terrasse avec surface
    if bien.terrasse:
        await _selectionner_option(page, "Terrasse")
        await page.wait_for_timeout(300)
        if bien.surface_terrasse:
            await _remplir_input(
                ["input[name='terrace_area']", "input[placeholder*='errasse']"],
                str(int(bien.surface_terrasse)),
                timeout=2000,
            )

    # Cave (nombre)
    if bien.nb_caves > 0:
        await _selectionner_option(page, "Cave")
        await page.wait_for_timeout(300)
        if bien.nb_caves > 1:
            await _remplir_input(
                ["input[name='cellar_count']", "input[name='cave_count']"],
                str(bien.nb_caves),
                timeout=2000,
            )

    # Parking (nombre de places)
    if bien.nb_places_parking > 0:
        await _selectionner_option(page, "Parking")
        await page.wait_for_timeout(300)
        if bien.nb_places_parking > 1:
            await _remplir_input(
                ["input[name='parking_count']", "input[name='parking_spaces']"],
                str(bien.nb_places_parking),
                timeout=2000,
            )

    # Chambres de service (nombre)
    if bien.nb_chambres_service > 0:
        await _selectionner_option(page, "Chambre de service")
        await page.wait_for_timeout(300)
        if bien.nb_chambres_service > 1:
            await _remplir_input(
                ["input[name='service_room_count']", "input[name='chambres_service']"],
                str(bien.nb_chambres_service),
                timeout=2000,
            )

    await _cliquer_si_present(page, "button:has-text('Suivant')", timeout=2000)
    await _cliquer_si_present(page, "button:has-text('Continuer')", timeout=2000)

    # ── Étape 5 : Qualité / année ──────────────
    await page.wait_for_timeout(800)

    if bien.annee_construction:
        await _remplir_input(
            ["input[name='construction_year']", "input[placeholder*='nnée']"],
            str(bien.annee_construction),
        )

    if bien.dpe:
        await _selectionner_option(page, f"DPE {bien.dpe}")
        await _selectionner_option(page, bien.dpe)  # fallback si pas de préfixe

    if bien.etat_bien:
        await _selectionner_option(page, bien.etat_bien)

    if bien.luminosite:
        await _selectionner_option(page, bien.luminosite)

    if bien.calme:
        await _selectionner_option(page, bien.calme)

    await _cliquer_si_present(page, "button:has-text('Suivant')", timeout=2000)
    await _cliquer_si_present(page, "button:has-text('Estimer')", timeout=2000)
    await _cliquer_si_present(page, "button:has-text('Voir')", timeout=2000)

    # ── Étape 6 : Profil ──────────────────────
    await page.wait_for_timeout(1000)
    await _selectionner_option(page, "Propriétaire")
    await page.wait_for_timeout(400)
    await _selectionner_option(page, "Me renseigner")
    await page.wait_for_timeout(400)
    await _cliquer_si_present(page, "button:has-text('Suivant')", timeout=2000)
    await _cliquer_si_present(page, "button:has-text('Estimer')", timeout=3000)


# ─────────────────────────────────────────────
# Scraper principal
# ─────────────────────────────────────────────

async def get_prix_m2_async(
    bien: BienImmobilier,
    headless: bool = True,
    timeout_resultat: int = 30,
) -> PrixM2Result:
    """
    Lance Playwright, remplit le formulaire d'estimation meilleursagents.fr,
    intercepte la réponse API et retourne les prix au m².

    Args:
        bien: caractéristiques du bien
        headless: False pour voir le navigateur (debug)
        timeout_resultat: secondes d'attente max pour la réponse API
    """
    captured: list[dict] = []

    # Patterns d'URL de l'API d'estimation
    API_PATTERNS = [
        r"/estimation",
        r"/api/",
        r"/price",
        r"/valuation",
    ]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            channel="chrome",  # utilise le vrai Chrome installé, pas Chromium
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            locale="fr-FR",
            timezone_id="Europe/Paris",
        )
        # Masquer les traces Playwright
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            Object.defineProperty(navigator, 'languages', { get: () => ['fr-FR', 'fr'] });
            window.chrome = { runtime: {} };
        """)
        page = await context.new_page()

        # ── Interception réseau ────────────────────
        async def on_response(response: Response):
            url = response.url
            if any(re.search(p, url) for p in API_PATTERNS):
                if response.status == 200:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        try:
                            body = await response.json()
                            captured.append({"url": url, "data": body})
                        except Exception:
                            pass

        page.on("response", on_response)

        # ── Navigation ────────────────────────────
        await page.goto(
            "https://www.meilleursagents.com/estimation-immobiliere/",
            wait_until="domcontentloaded",
            timeout=30000,
        )

        await _remplir_formulaire(page, bien)

        # Attendre la réponse API (max timeout_resultat secondes)
        deadline = time.time() + timeout_resultat
        while time.time() < deadline:
            for capture in captured:
                result = _extraire_prix(capture["data"])
                if result:
                    result.localisation = bien.adresse
                    await browser.close()
                    return result
            await asyncio.sleep(0.5)

        # Fallback : scrape visuel de la page résultat
        result = await _scrape_resultats_visuels(page, bien.adresse)
        await browser.close()
        return result


async def _scrape_resultats_visuels(page: Page, adresse: str) -> PrixM2Result:
    """Fallback : extrait les prix directement depuis le DOM si l'API n'a pas été capturée."""
    result = PrixM2Result(localisation=adresse)
    try:
        await page.wait_for_timeout(3000)
        contenu = await page.content()

        # Chercher les patterns de prix dans le HTML
        prix_pattern = re.findall(r'(\d[\d\s]*)\s*€\s*/?\s*m²', contenu)
        nombres = []
        for p in prix_pattern:
            try:
                nombres.append(float(p.replace(" ", "").replace("\xa0", "")))
            except ValueError:
                pass

        nombres = sorted(set(n for n in nombres if 500 < n < 50000))
        if len(nombres) >= 3:
            result.tranche_basse = nombres[0]
            result.prix_m2_moyen = nombres[len(nombres) // 2]
            result.tranche_haute = nombres[-1]
        elif len(nombres) == 2:
            result.tranche_basse = nombres[0]
            result.tranche_haute = nombres[1]
            result.prix_m2_moyen = (nombres[0] + nombres[1]) / 2
        elif len(nombres) == 1:
            result.prix_m2_moyen = nombres[0]

    except Exception:
        pass
    return result


def get_prix_m2(
    bien: BienImmobilier,
    headless: bool = True,
    timeout_resultat: int = 30,
) -> PrixM2Result:
    """Version synchrone de get_prix_m2_async."""
    return asyncio.run(get_prix_m2_async(bien, headless=headless, timeout_resultat=timeout_resultat))


# ─────────────────────────────────────────────
# CLI / test rapide
# ─────────────────────────────────────────────

if __name__ == "__main__":

    # ── Exemple appartement ────────────────────
    appartement = BienImmobilier(
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
        terrasse=False,
        nb_caves=1,
        nb_places_parking=1,
        nb_chambres_service=0,
        annee_construction=1972,
        etat_bien="bon état",
        luminosite="clair",
        calme="calme",
    )

    # ── Exemple maison ─────────────────────────
    # maison = BienImmobilier(
    #     adresse="5 allée des Roses",
    #     code_postal="69003",
    #     ville="Lyon",
    #     type_bien="maison",
    #     surface_habitable=130,
    #     surface_terrain=400,
    #     surface_encore_constructible=80,
    #     nb_pieces=5,
    #     nb_salles_bain=2,
    #     nb_chambres=3,
    #     nb_niveaux=2,
    #     nb_places_parking=2,
    #     annee_construction=1995,
    #     etat_bien="bon état",
    # )

    # headless=False pour voir le navigateur (utile pour débugger)
    resultat = get_prix_m2(appartement, headless=False)
    print(resultat)
