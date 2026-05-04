"""
Scraper meilleursagents.fr via Firecrawl /interact
- Remplit le formulaire avec les vrais IDs HTML (inspectés manuellement)
- Profil : non-propriétaire / je m'informe (aucun compte requis)
- Retourne : prix/m² moyen, tranche basse, tranche haute

Variable d'environnement requise :
    FIRECRAWL_API_KEY   → clé API Firecrawl

Install : pip install firecrawl-py python-dotenv
"""

import os
import re
import time
import unicodedata
import urllib.parse
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
    prix_m2_moyen:   Optional[float] = None
    tranche_basse:   Optional[float] = None
    tranche_haute:   Optional[float] = None
    prix_total_moyen: Optional[float] = None
    prix_total_bas:  Optional[float] = None
    prix_total_haut: Optional[float] = None
    localisation:    Optional[str]   = None
    raw_output:      Optional[str]   = None

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
                f"  Prix total bas  : {self.prix_total_bas:,.0f} €"  if self.prix_total_bas  else "",
                f"  Prix total haut : {self.prix_total_haut:,.0f} €" if self.prix_total_haut else "",
            ]
        if self.raw_output and not self.prix_m2_moyen:
            lines.append(f"  Réponse brute   : {self.raw_output[:300]}")
        lines.append("──────────────────────────────────────")
        return "\n".join(l for l in lines if l)


# ─────────────────────────────────────────────
# Paramètres du bien
# ─────────────────────────────────────────────

@dataclass
class BienImmobilier:
    adresse:    str
    code_postal: str = ""
    ville:       str = ""
    type_bien:   str = "appartement"        # "appartement" | "maison" | "duplex" | "triplex" | "loft"

    surface_habitable:          Optional[float] = None
    surface_terrain:            Optional[float] = None   # maison
    surface_encore_constructible: Optional[float] = None # maison

    nb_pieces:       Optional[int] = None
    nb_chambres:     Optional[int] = None
    nb_salles_bain:  Optional[int] = None

    nb_niveaux:         Optional[int] = None  # maison
    etage:              Optional[int] = None  # appartement (0 = RDC)
    nb_etages_immeuble: Optional[int] = None  # appartement

    ascenseur:        bool = False
    balcon:           bool = False
    surface_balcon:   Optional[float] = None
    terrasse:         bool = False
    surface_terrasse: Optional[float] = None
    nb_caves:              int = 0
    nb_places_parking:     int = 0
    nb_chambres_service:   int = 0

    annee_construction: Optional[int] = None
    etat_bien:          Optional[str] = None  # "neuf" | "standard" | "rafraichissement" | "travaux"


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

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
    if any(k in e for k in ["important", "mauvais", "gros"]):      return "RENOVATION_LEVEL.HEAVY_WORK_REQUIRED"
    if any(k in e for k in ["rafraich", "léger", "leger"]):        return "RENOVATION_LEVEL.LIGHT_WORK_REQUIRED"
    return "RENOVATION_LEVEL.STANDARD"


def _get_city_slug(ville: str, code_postal: str) -> str:
    """Construit le slug meilleursagents pour la page prix-immobilier."""
    cp = code_postal.strip()

    def ordinal(n: int) -> str:
        return "1er" if n == 1 else f"{n}eme"

    # Paris 75001-75020
    if cp.startswith("750") and len(cp) == 5 and cp[3:].isdigit():
        arr = int(cp[3:])
        return f"paris-{ordinal(arr)}-arrondissement-{cp}"

    # Lyon 69001-69009
    if re.match(r"6900[1-9]", cp):
        arr = int(cp[4])
        return f"lyon-{ordinal(arr)}-arrondissement-{cp}"

    # Marseille 13001-13016
    if re.match(r"130[01]\d", cp) and 1 <= int(cp[3:]) <= 16:
        arr = int(cp[3:])
        return f"marseille-{ordinal(arr)}-arrondissement-{cp}"

    # Ville générique : normalise les accents et espaces
    v = unicodedata.normalize("NFD", ville.lower())
    v = "".join(c for c in v if unicodedata.category(c) != "Mn")
    v = re.sub(r"[^a-z0-9]+", "-", v).strip("-")
    return f"{v}-{cp}"


def _get_city_id(app: FirecrawlApp, ville: str, code_postal: str) -> Optional[str]:
    """Récupère le city_id meilleursagents via la page prix-immobilier (pas de CAPTCHA)."""
    slug = _get_city_slug(ville, code_postal)
    url = f"https://www.meilleursagents.com/prix-immobilier/{slug}/"
    try:
        doc = app.scrape(url, formats=["markdown"])
        ids = re.findall(r"item_city_id=(\d+)", doc.markdown or "")
        if ids:
            return ids[0]
    except Exception:
        pass
    return None


def _counter_code(field_id: str, target: int) -> str:
    """Génère le code Python (indenté 4 espaces) pour régler un compteur à la valeur cible (part de 1)."""
    delta = target - 1
    if delta == 0:
        return ""
    direction = "up" if delta > 0 else "down"
    clicks = abs(delta)
    return (
        f"    for _ in range({clicks}):\n"
        f"        await page.locator('.field--counter:has(#{field_id}) .field__count--{direction}').click()\n"
        f"        await page.wait_for_timeout(30)"
    )


# ─────────────────────────────────────────────
# Génération du code — Bloc 1 : adresse + type + page 2
# ─────────────────────────────────────────────

def _build_code_bloc1(bien: BienImmobilier) -> str:
    adresse = bien.adresse
    if bien.code_postal or bien.ville:
        adresse = f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", ")

    type_map = {"appartement": "apartment", "maison": "house",
                "duplex": "duplex", "triplex": "triplex", "loft": "loft"}
    type_id = type_map.get(bien.type_bien.lower(), "apartment")

    surface = int(bien.surface_habitable) if bien.surface_habitable else 0

    # Code pour les compteurs (partent tous de 1)
    rooms_code   = _counter_code("room_count",   bien.nb_pieces or 1)
    sdb_code     = _counter_code("bathroom_count", bien.nb_salles_bain or 1)

    if bien.type_bien == "appartement":
        etage_code   = _counter_code("floor",       (bien.etage or 0) if bien.etage is not None else 1)
        niv_code     = _counter_code("floor_count", bien.nb_etages_immeuble or 1)
    else:
        etage_code   = _counter_code("level_count", bien.nb_niveaux or 1)
        niv_code     = ""

    terrain_code = ""
    if bien.surface_terrain and bien.type_bien in ("maison", "duplex", "triplex"):
        terrain_code = f"await page.locator('#land_area').fill('{int(bien.surface_terrain)}')\n"

    return f"""
async def run():
    # On arrive directement sur la page form (type de bien)
    await page.wait_for_timeout(2000)

    # ── Type de bien ──────────────────────────
    try:
        await page.locator('#{type_id}').click()
        await page.wait_for_timeout(500)
    except: pass
    await page.locator('button:has-text("Suivant")').first.click()
    await page.wait_for_timeout(1000)
    print('type ok')

    # ── Page 2 : surfaces et pièces ───────────
    area = page.locator('#area')
    await area.wait_for(state='visible', timeout=15000)
    await area.fill('{surface}')
    print('surface ok: {surface}')

    {terrain_code}
{rooms_code}
    print('pieces ok: {bien.nb_pieces or 1}')

{sdb_code}
    print('sdb ok: {bien.nb_salles_bain or 1}')

{etage_code}
{niv_code}
    print('etages ok')

    await page.locator('button:has-text("Suivant")').first.click()
    await page.wait_for_timeout(1000)
    print('page2 done')

await run()
"""


# ─────────────────────────────────────────────
# Génération du code — Bloc 2 : équipements + précisions + profil
# ─────────────────────────────────────────────

def _build_code_bloc2(bien: BienImmobilier) -> str:
    equip_lines = []

    if bien.ascenseur:
        equip_lines.append("await page.locator('label[for=\"elevator\"]').first.click()")
        equip_lines.append("await page.wait_for_timeout(200)")

    if bien.balcon:
        equip_lines.append("await page.locator('label[for=\"balcony\"]').first.click()")
        equip_lines.append("await page.wait_for_timeout(200)")
        if bien.surface_balcon:
            equip_lines.append(f"await page.locator('#balcony_area').fill('{int(bien.surface_balcon)}')")

    if bien.terrasse:
        equip_lines.append("await page.locator('label[for=\"terrace\"]').first.click()")
        equip_lines.append("await page.wait_for_timeout(200)")
        if bien.surface_terrasse:
            equip_lines.append(f"await page.locator('#terrace_area').fill('{int(bien.surface_terrasse)}')")

    if bien.nb_caves > 0:
        equip_lines.append("await page.locator('label[for=\"cellar\"]').first.click()")
        equip_lines.append("await page.wait_for_timeout(200)")
        if bien.nb_caves > 1:
            for _ in range(bien.nb_caves - 1):
                equip_lines.append("await page.locator('.field--counter:has(#cellar_count) .field__count--up').click()")

    if bien.nb_places_parking > 0:
        equip_lines.append("await page.locator('label[for=\"parking\"]').first.click()")
        equip_lines.append("await page.wait_for_timeout(200)")
        if bien.nb_places_parking > 1:
            for _ in range(bien.nb_places_parking - 1):
                equip_lines.append("await page.locator('.field--counter:has(#parking_count) .field__count--up').click()")

    if bien.nb_chambres_service > 0:
        equip_lines.append("await page.locator('label[for=\"secondary_room\"]').first.click()")
        equip_lines.append("await page.wait_for_timeout(200)")
        if bien.nb_chambres_service > 1:
            for _ in range(bien.nb_chambres_service - 1):
                equip_lines.append("await page.locator('.field--counter:has(#secondary_room_count) .field__count--up').click()")

    equip_block = "\n    ".join(equip_lines) if equip_lines else "pass  # pas d'équipements"

    # Page 4
    build_period_code = ""
    if bien.annee_construction:
        bp = _annee_to_build_period(bien.annee_construction)
        build_period_code = f"await page.select_option('#build_period', '{bp}')"

    renovation_code = ""
    if bien.etat_bien:
        rl = _etat_to_renovation_level(bien.etat_bien)
        renovation_code = f"await page.select_option('#renovation_level', '{rl}')"

    return f"""
async def run():
    # ── Page 3 : équipements ──────────────────
    {equip_block}
    await page.locator('button:has-text("Suivant")').first.click()
    await page.wait_for_timeout(1000)
    print('page3 done')

    # ── Page 4 : précisions ───────────────────
    try:
        {build_period_code or 'pass'}
    except: pass
    try:
        {renovation_code or 'pass'}
    except: pass
    await page.locator('button:has-text("Suivant")').first.click()
    await page.wait_for_timeout(1000)
    print('page4 done')

    # ── Page 5 : profil ───────────────────────
    try:
        await page.locator('#false-profile_owner').click()
        await page.wait_for_timeout(400)
    except: pass
    try:
        await page.select_option('#profile_buyer', 'BUYER_PROFILE.TOURIST')
    except: pass

    # Soumettre
    try:
        await page.locator('button:has-text("Estimer")').first.click()
    except: pass
    await page.wait_for_timeout(5000)
    print('soumis | url:', page.url)

await run()
"""


# ─────────────────────────────────────────────
# Parsing de la réponse
# ─────────────────────────────────────────────

def _nettoyer(s: str) -> Optional[float]:
    try:
        return float(re.sub(r'[\s\u00a0,]', '', s))
    except Exception:
        return None


def _parser_resultat(output: str, bien: BienImmobilier) -> PrixM2Result:
    result = PrixM2Result(
        localisation=f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", "),
        raw_output=output,
    )

    # Prix au m²
    for pat in [
        r'(\d[\d\s\u00a0]*)\s*€\s*/\s*m\s*[²2]',
        r'prix\s+au\s+m[²2]\s*[:\*]*\s*(\d[\d\s\u00a0]*)\s*€',
    ]:
        for m in re.findall(pat, output, re.IGNORECASE):
            v = _nettoyer(m)
            if v and 500 < v < 50000:
                result.prix_m2_moyen = v
                break
        if result.prix_m2_moyen:
            break

    # Prix totaux
    patterns_totaux = [
        (r'(?:basse|bas)[^\d]{0,30}(\d[\d\s\u00a0]{4,})\s*€',  'bas'),
        (r'(?:moyenne?|moyen)[^\d]{0,30}(\d[\d\s\u00a0]{4,})\s*€', 'moyen'),
        (r'(?:haute?|haut)[^\d]{0,30}(\d[\d\s\u00a0]{4,})\s*€', 'haut'),
    ]
    for pat, label in patterns_totaux:
        for m in re.findall(pat, output, re.IGNORECASE):
            v = _nettoyer(m)
            if v and 50000 < v < 100_000_000:
                if label == 'bas':   result.prix_total_bas   = v
                if label == 'moyen': result.prix_total_moyen = v
                if label == 'haut':  result.prix_total_haut  = v
                break

    # Calcul €/m² pour les tranches depuis les prix totaux
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

def get_prix_m2(
    bien: BienImmobilier,
    api_key: Optional[str] = None,
) -> PrixM2Result:
    fc_key = api_key or os.environ.get("FIRECRAWL_API_KEY")
    if not fc_key:
        raise ValueError("FIRECRAWL_API_KEY manquante.")

    app = FirecrawlApp(api_key=fc_key)

    # ── 1. Récupérer le city_id ───────────────
    print("[1/4] Recherche du city_id...", flush=True)
    t = time.time()
    city_id = _get_city_id(app, bien.ville, bien.code_postal)
    if not city_id:
        raise ValueError(f"city_id introuvable pour {bien.ville} {bien.code_postal}")
    print(f"      OK ({time.time()-t:.1f}s) — city_id={city_id}", flush=True)

    # ── 2. Ouvrir directement la page du formulaire ──
    print("[2/4] Ouverture directe du formulaire (sans barre de recherche)...", flush=True)
    t = time.time()
    adresse_enc = urllib.parse.quote(bien.adresse)
    ville_enc   = urllib.parse.quote(bien.ville)
    form_url = (
        f"https://www.meilleursagents.com/estimation-immobiliere/form"
        f"?item_city_id={city_id}&item_zip={bien.code_postal}"
        f"&item_city_name={ville_enc}&item_address={adresse_enc}"
        f"&action=prefill"
    )
    scrape = app.scrape(form_url, formats=["markdown"])
    scrape_id = scrape.metadata.scrape_id
    print(f"      OK ({time.time()-t:.1f}s) — session: {scrape_id}", flush=True)

    # ── 3. Type + page 2 + page 3 (code) ──────
    print("[3/4] Type, surfaces, équipements...", flush=True)
    t = time.time()
    code1 = _build_code_bloc1(bien)
    r1 = app.interact(scrape_id, code=code1, language="python", timeout=120)
    stdout1 = getattr(r1, 'stdout', '') or ''
    print(f"      OK ({time.time()-t:.1f}s)\n      {stdout1.strip()}", flush=True)

    code2 = _build_code_bloc2(bien)
    r2 = app.interact(scrape_id, code=code2, language="python", timeout=120)
    stdout2 = getattr(r2, 'stdout', '') or ''
    print(f"      {stdout2.strip()}", flush=True)

    # ── 4. Extraction des prix ────────────────
    print("[4/4] Extraction des prix...", flush=True)
    t = time.time()
    r3 = app.interact(
        scrape_id,
        prompt=(
            "Lis les résultats de l'estimation affichés sur la page : "
            "le prix au m² moyen, la fourchette basse en euros, la fourchette haute en euros, "
            "et le prix total estimé si disponible. "
            "Réponds uniquement avec les valeurs numériques en euros."
        ),
    )
    output = getattr(r3, 'output', '') or getattr(r3, 'result', '') or str(r3)
    print(f"      OK ({time.time()-t:.1f}s)", flush=True)
    print(f"\n── Réponse Firecrawl ──\n{output}\n───────────────────────\n", flush=True)

    return _parser_resultat(output, bien)


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
        etage=4,
        nb_etages_immeuble=7,
        ascenseur=True,
        balcon=True,
        surface_balcon=6,
        nb_caves=1,
        nb_places_parking=1,
        annee_construction=1972,
        etat_bien="bon état",
    )

    resultat = get_prix_m2(bien)
    print(resultat)
