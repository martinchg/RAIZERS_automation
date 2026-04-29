"""
Scraper meilleursagents.fr via Firecrawl /interact
- Se connecte avec un compte existant (email/mdp en variables d'env)
- Remplit le formulaire d'estimation en langage naturel
- Retourne : prix/m² moyen, tranche basse, tranche haute

Variables d'environnement requises :
    FIRECRAWL_API_KEY   → clé API Firecrawl
    MA_EMAIL            → email du compte meilleursagents
    MA_PASSWORD         → mot de passe du compte meilleursagents

Install : pip install firecrawl-py
"""

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv
from firecrawl import FirecrawlApp

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
    indice_confiance: Optional[int] = None
    localisation: Optional[str] = None
    raw_output: Optional[str] = None       # réponse brute si parsing échoue

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
        if self.raw_output and not self.prix_m2_moyen:
            lines.append(f"  Réponse brute   : {self.raw_output[:300]}")
        lines.append("──────────────────────────────────────")
        return "\n".join(l for l in lines if l)


# ─────────────────────────────────────────────
# Paramètres du bien
# ─────────────────────────────────────────────

@dataclass
class BienImmobilier:
    # ── Localisation (obligatoire) ─────────────
    adresse: str
    code_postal: str = ""
    ville: str = ""

    # ── Type ───────────────────────────────────
    type_bien: str = "appartement"          # "appartement" ou "maison"

    # ── Surfaces ───────────────────────────────
    surface_habitable: Optional[float] = None
    surface_terrain: Optional[float] = None               # maison uniquement
    surface_encore_constructible: Optional[float] = None  # maison uniquement

    # ── Pièces ─────────────────────────────────
    nb_pieces: Optional[int] = None
    nb_chambres: Optional[int] = None
    nb_salles_bain: Optional[int] = None

    # ── Niveaux / étages ───────────────────────
    nb_niveaux: Optional[int] = None            # maison : niveaux du bien
    etage: Optional[int] = None                 # appartement : étage (0 = RDC)
    nb_etages_immeuble: Optional[int] = None    # appartement : total étages

    # ── Équipements ────────────────────────────
    ascenseur: bool = False
    balcon: bool = False
    surface_balcon: Optional[float] = None
    terrasse: bool = False
    surface_terrasse: Optional[float] = None
    nb_caves: int = 0
    nb_places_parking: int = 0
    nb_chambres_service: int = 0

    # ── Qualité ────────────────────────────────
    annee_construction: Optional[int] = None
    dpe: Optional[str] = None               # "A" à "G"
    etat_bien: Optional[str] = None         # "neuf", "bon état", "à rénover"
    luminosite: Optional[str] = None        # "très clair", "clair", "standard", "sombre"
    calme: Optional[str] = None             # "très calme", "calme", "standard", "bruyant"


# ─────────────────────────────────────────────
# Construction du prompt de remplissage
# ─────────────────────────────────────────────

def _build_form_prompt(bien: BienImmobilier) -> str:
    adresse_complete = bien.adresse
    if bien.code_postal or bien.ville:
        adresse_complete = f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", ")

    steps = [
        "Remplis le formulaire d'estimation avec les informations EXACTES suivantes.",
        "IMPORTANT : respecte chaque valeur numérique exactement, ne l'arrondis pas.",
    ]
    steps.append(f"- Adresse : {adresse_complete}")
    steps.append(f"- Type de bien : {bien.type_bien}")

    if bien.surface_habitable:
        steps.append(f"- Surface habitable : EXACTEMENT {int(bien.surface_habitable)} m² (pas moins, pas plus)")
    if bien.surface_terrain and bien.type_bien == "maison":
        steps.append(f"- Surface du terrain : EXACTEMENT {int(bien.surface_terrain)} m²")
    if bien.surface_encore_constructible and bien.type_bien == "maison":
        steps.append(f"- Surface encore constructible : EXACTEMENT {int(bien.surface_encore_constructible)} m²")
    if bien.nb_pieces:
        steps.append(f"- Nombre de pièces : EXACTEMENT {bien.nb_pieces}")
    if bien.nb_chambres:
        steps.append(f"- Nombre de chambres : EXACTEMENT {bien.nb_chambres}")
    if bien.nb_salles_bain:
        steps.append(f"- Nombre de salles de bain : {bien.nb_salles_bain}")

    if bien.type_bien == "maison" and bien.nb_niveaux:
        steps.append(f"- Nombre de niveaux : {bien.nb_niveaux}")
    if bien.type_bien == "appartement":
        if bien.etage is not None:
            steps.append(f"- Étage : {bien.etage}")
        if bien.nb_etages_immeuble:
            steps.append(f"- Nombre d'étages de l'immeuble : {bien.nb_etages_immeuble}")

    if bien.ascenseur:
        steps.append("- Ascenseur : oui")
    if bien.balcon:
        surface = f" ({int(bien.surface_balcon)} m²)" if bien.surface_balcon else ""
        steps.append(f"- Balcon : oui{surface}")
    if bien.terrasse:
        surface = f" ({int(bien.surface_terrasse)} m²)" if bien.surface_terrasse else ""
        steps.append(f"- Terrasse : oui{surface}")
    if bien.nb_caves > 0:
        steps.append(f"- Nombre de caves : {bien.nb_caves}")
    if bien.nb_places_parking > 0:
        steps.append(f"- Nombre de places de parking : {bien.nb_places_parking}")
    if bien.nb_chambres_service > 0:
        steps.append(f"- Chambres de service : {bien.nb_chambres_service}")
    if bien.annee_construction:
        steps.append(f"- Année de construction : {bien.annee_construction}")
    if bien.dpe:
        steps.append(f"- DPE : {bien.dpe}")
    if bien.etat_bien:
        steps.append(f"- État du bien : {bien.etat_bien}")
    if bien.luminosite:
        steps.append(f"- Luminosité : {bien.luminosite}")
    if bien.calme:
        steps.append(f"- Calme : {bien.calme}")

    steps.append(
        "Clique sur 'Suivant' ou 'Continuer' entre chaque étape. "
        "Si une étape demande ton profil, sélectionne 'Propriétaire' et 'Me renseigner'. "
        "Ne crée pas de compte, ne remplis pas d'email."
    )

    return "\n".join(steps)


# ─────────────────────────────────────────────
# Parsing de la réponse
# ─────────────────────────────────────────────

def _nettoyer_nombre(s: str) -> Optional[float]:
    try:
        return float(s.replace(" ", "").replace("\xa0", "").replace("\u00a0", "").replace(",", ""))
    except (ValueError, AttributeError):
        return None


def _build_form_code(bien: BienImmobilier) -> str:
    """Code Playwright minimal — valeurs injectées directement, pas de boucles."""
    adresse = bien.adresse
    if bien.code_postal or bien.ville:
        adresse = f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", ")
    type_label = "Appartement" if bien.type_bien == "appartement" else "Maison"

    # Valeurs numériques avec défauts
    surface  = int(bien.surface_habitable) if bien.surface_habitable else 0
    pieces   = bien.nb_pieces or 0
    sdb      = bien.nb_salles_bain or 0
    etage    = bien.etage if bien.etage is not None else 0
    nb_etg   = bien.nb_etages_immeuble or 0
    annee    = bien.annee_construction or 0

    equipements = []
    if bien.ascenseur:             equipements.append("Ascenseur")
    if bien.balcon:                equipements.append("Balcon")
    if bien.terrasse:              equipements.append("Terrasse")
    if bien.nb_caves > 0:          equipements.append("Cave")
    if bien.nb_places_parking > 0: equipements.append("Parking")
    if bien.nb_chambres_service > 0: equipements.append("Chambre de service")

    eq_code = "\n".join(
        f"    try:\n        await page.get_by_text('{eq}', exact=False).first.click()\n    except: pass"
        for eq in equipements
    )

    return f"""
async def run():
    await page.goto('https://www.meilleursagents.com/estimation-immobiliere/')
    await page.wait_for_load_state('domcontentloaded')
    await page.wait_for_timeout(2000)
    print('page ok')

    # Adresse
    inp = page.locator('input').first
    await inp.fill('{adresse}')
    await page.wait_for_timeout(2000)
    try:
        await page.locator('li[role="option"]').first.click()
    except:
        await inp.press('ArrowDown')
        await inp.press('Enter')
    await page.wait_for_timeout(1000)
    print('adresse ok')

    # Type
    try:
        await page.get_by_text('{type_label}', exact=False).first.click()
    except: pass
    await page.wait_for_timeout(500)
    try:
        await page.locator('button:has-text("Suivant")').first.click()
    except: pass
    await page.wait_for_timeout(800)
    print('type ok')

    # Surface
    for sel in ['input[name="area"]', 'input[name="living_area"]']:
        try:
            el = page.locator(sel).first
            await el.clear()
            await el.fill('{surface}')
            print('surface ok')
            break
        except: pass

    # Pièces
    for sel in ['input[name="room_count"]', 'input[name="rooms"]']:
        try:
            el = page.locator(sel).first
            await el.clear()
            await el.fill('{pieces}')
            print('pieces ok')
            break
        except: pass

    # SDB
    for sel in ['input[name="bathroom_count"]', 'input[name="bathrooms"]']:
        try:
            el = page.locator(sel).first
            await el.clear()
            await el.fill('{sdb}')
            break
        except: pass

    # Étage
    for sel in ['input[name="floor"]', 'input[name="floor_number"]']:
        try:
            el = page.locator(sel).first
            await el.clear()
            await el.fill('{etage}')
            break
        except: pass

    # Nb étages immeuble
    for sel in ['input[name="floor_count"]', 'input[name="total_floors"]']:
        try:
            el = page.locator(sel).first
            await el.clear()
            await el.fill('{nb_etg}')
            break
        except: pass

    await page.wait_for_timeout(500)
    try:
        await page.locator('button:has-text("Suivant")').first.click()
    except: pass
    await page.wait_for_timeout(800)
    print('champs numeriques ok')

    # Équipements
{eq_code}
    await page.wait_for_timeout(500)
    try:
        await page.locator('button:has-text("Suivant")').first.click()
    except: pass
    await page.wait_for_timeout(800)
    print('equipements ok')

    # Qualité
    ANNEE_PLACEHOLDER
    ETAT_PLACEHOLDER
    await page.wait_for_timeout(500)
    try:
        await page.locator('button:has-text("Suivant")').first.click()
    except: pass
    try:
        await page.locator('button:has-text("Estimer")').first.click()
    except: pass
    await page.wait_for_timeout(1000)

    # Profil
    try:
        await page.get_by_text('Propriétaire', exact=False).first.click()
    except: pass
    await page.wait_for_timeout(400)
    try:
        await page.get_by_text('Me renseigner', exact=False).first.click()
    except: pass
    try:
        await page.locator('button:has-text("Estimer")').first.click()
    except: pass
    await page.wait_for_timeout(3000)
    print('formulaire termine')

await run()
"""

    # Remplacer les placeholders par du vrai code (évite les problèmes de f-string imbriquées)
    annee_code = f'    await page.locator(\'input[name="construction_year"]\').first.fill("{annee}")' if annee else "    pass  # pas d'année"
    etat_code = (
        f'    try:\n        await page.get_by_text("{bien.etat_bien}", exact=False).first.click()\n    except: pass'
        if bien.etat_bien else "    pass  # pas d'état"
    )
    code = code.replace("    ANNEE_PLACEHOLDER", annee_code)
    code = code.replace("    ETAT_PLACEHOLDER", etat_code)
    return code


def _parser_resultat(output: str, bien: BienImmobilier) -> PrixM2Result:
    """Extrait les prix depuis la réponse en langage naturel de Firecrawl."""
    import re

    result = PrixM2Result(
        localisation=f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", "),
        raw_output=output,
    )

    # ── Prix au m² ─────────────────────────────
    # Patterns: "12 098€ /m²", "12098 €/m²", "prix au m²: 12098"
    for pattern in [
        r'(?:prix\s+au\s+m²\s*[:\*]*\s*)(\d[\d\s\u00a0]*)\s*€',
        r'(\d[\d\s\u00a0]*)\s*€\s*/\s*m²',
        r'(\d[\d\s\u00a0]*)\s*€/m²',
    ]:
        matches = re.findall(pattern, output, re.IGNORECASE)
        for m in matches:
            v = _nettoyer_nombre(m)
            if v and 500 < v < 50000:
                result.prix_m2_moyen = v
                break
        if result.prix_m2_moyen:
            break

    # ── Prix total moyen ───────────────────────
    for pattern in [
        r'(?:prix\s+net\s+vendeur|prix\s+total\s+estim[ée]?|valeur\s+estim[ée]?)\s*[:\*]*\s*(\d[\d\s\u00a0]*)\s*€',
        r'(\d[\d\s\u00a0]{5,})\s*€(?!\s*/\s*m)',
    ]:
        matches = re.findall(pattern, output, re.IGNORECASE)
        for m in matches:
            v = _nettoyer_nombre(m)
            if v and 50000 < v < 100_000_000:
                result.prix_total_moyen = v
                break
        if result.prix_total_moyen:
            break

    # ── Fourchette basse (prix total) ──────────
    for pattern in [
        r'(?:fourchette\s+basse|tranche\s+basse|prix\s+bas)\s*[:\*]*\s*(\d[\d\s\u00a0]*)\s*€',
    ]:
        matches = re.findall(pattern, output, re.IGNORECASE)
        for m in matches:
            v = _nettoyer_nombre(m)
            if v and 50000 < v < 100_000_000:
                result.prix_total_bas = v
                break

    # ── Fourchette haute (prix total) ──────────
    for pattern in [
        r'(?:fourchette\s+haute|tranche\s+haute|prix\s+haut)\s*[:\*]*\s*(\d[\d\s\u00a0]*)\s*€',
    ]:
        matches = re.findall(pattern, output, re.IGNORECASE)
        for m in matches:
            v = _nettoyer_nombre(m)
            if v and 50000 < v < 100_000_000:
                result.prix_total_haut = v
                break

    # ── Calcul €/m² pour les tranches ──────────
    # Le prix au m² retourné par le site EST le prix du bien (total / surface qu'il utilise).
    # Pour les tranches, on divise par la surface qu'on a fournie.
    surface = bien.surface_habitable
    if surface and surface > 0:
        if result.prix_total_bas and not result.tranche_basse:
            result.tranche_basse = round(result.prix_total_bas / surface)
        if result.prix_total_haut and not result.tranche_haute:
            result.tranche_haute = round(result.prix_total_haut / surface)

    return result


# ─────────────────────────────────────────────
# Scraper principal
# ─────────────────────────────────────────────

def get_prix_m2(
    bien: BienImmobilier,
    api_key: Optional[str] = None,
    email: Optional[str] = None,
    password: Optional[str] = None,
) -> PrixM2Result:
    """
    Utilise Firecrawl /interact pour remplir le formulaire d'estimation
    meilleursagents.fr et retourner les prix au m².

    Les credentials du compte meilleursagents permettent d'accéder aux
    résultats complets sans recréer un compte à chaque fois.

    Args:
        bien: caractéristiques du bien
        api_key: clé Firecrawl (ou FIRECRAWL_API_KEY)
        email: email meilleursagents (ou MA_EMAIL)
        password: mot de passe meilleursagents (ou MA_PASSWORD)
    """
    fc_key = api_key or os.environ.get("FIRECRAWL_API_KEY")
    ma_email = email or os.environ.get("MA_EMAIL")
    ma_password = password or os.environ.get("MA_PASSWORD")

    if not fc_key:
        raise ValueError("FIRECRAWL_API_KEY manquante.")
    if not ma_email or not ma_password:
        raise ValueError(
            "Credentials meilleursagents manquants. "
            "Définis MA_EMAIL et MA_PASSWORD (ou passe email= et password=)."
        )

    import time
    app = FirecrawlApp(api_key=fc_key)

    # ── 1. Scrape homepage ─────────────────────
    print("[1/5] Ouverture de meilleursagents.fr...", flush=True)
    t = time.time()
    scrape = app.scrape("https://www.meilleursagents.com/", formats=["markdown"])
    scrape_id = scrape.metadata.scrape_id
    print(f"      OK ({time.time()-t:.1f}s) — session id: {scrape_id}", flush=True)

    # ── 2. Connexion ───────────────────────────
    print("[2/5] Connexion au compte...", flush=True)
    t = time.time()
    r = app.interact(
        scrape_id,
        prompt=(
            "Va sur https://www.meilleursagents.com/compte/connexion/ "
            f"et connecte-toi avec l'email '{ma_email}' et le mot de passe '{ma_password}'. "
            "Clique sur le bouton 'Se connecter' ou 'Connexion' après avoir rempli les champs."
        ),
    )
    print(f"      OK ({time.time()-t:.1f}s) — {getattr(r, 'output', '') or ''}", flush=True)

    # ── 3+4. Navigation + remplissage formulaire ──
    print("[3/5] Navigation + remplissage du formulaire...", flush=True)
    t = time.time()
    code = _build_form_code(bien)
    r = app.interact(scrape_id, code=code, language="python", timeout=280)
    print(f"      OK ({time.time()-t:.1f}s) — stdout: {getattr(r, 'stdout', '') or ''}", flush=True)

    # ── 4. Extraction résultats ────────────────
    print("[4/4] Extraction des prix...", flush=True)
    t = time.time()
    response = app.interact(
        scrape_id,
        prompt=(
            "Lis les résultats de l'estimation affichés sur la page : "
            "le prix au m² moyen, la fourchette basse et la fourchette haute, "
            "ainsi que le prix total estimé si disponible. "
            "Retourne uniquement les valeurs numériques en euros."
        ),
    )
    print(f"      OK ({time.time()-t:.1f}s)", flush=True)

    output = getattr(response, "output", "") or getattr(response, "result", "") or str(response)
    print(f"\n── Réponse brute Firecrawl ──\n{output}\n────────────────────────────\n", flush=True)
    return _parser_resultat(output, bien)


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
        nb_caves=1,
        nb_places_parking=1,
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
    # )

    resultat = get_prix_m2(appartement)
    print(resultat)
