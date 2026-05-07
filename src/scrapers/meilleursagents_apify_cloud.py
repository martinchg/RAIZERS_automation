"""
Scraper meilleursagents.fr via Apify Cloud (playwright-scraper actor)
- Le navigateur tourne sur l'infra Apify (pas en local)
- Proxy résidentiel FR intégré, anti-detect géré par Apify
- Retourne : prix/m² moyen, tranche basse, tranche haute

Variables d'environnement :
    APIFY_API_TOKEN  → clé API Apify (obligatoire)

Install :
    pip install apify-client python-dotenv
"""

import json
import os
import re
import time
import unicodedata
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()


@dataclass
class PrixM2Result:
    prix_m2_moyen: Optional[float] = None
    tranche_basse: Optional[float] = None
    tranche_haute: Optional[float] = None
    prix_total_moyen: Optional[float] = None
    prix_total_bas: Optional[float] = None
    prix_total_haut: Optional[float] = None
    localisation: Optional[str] = None
    raw_output: Optional[str] = None

    def __str__(self) -> str:
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
        if self.raw_output and not self.prix_m2_moyen:
            lines.append(f"  Réponse brute   : {self.raw_output[:300]}")
        lines.append("──────────────────────────────────────")
        return "\n".join(line for line in lines if line)


@dataclass
class BienImmobilier:
    adresse: str
    code_postal: str = ""
    ville: str = ""
    type_bien: str = "appartement"

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
    etat_bien: Optional[str] = None


def _annee_to_build_period(annee: int) -> str:
    if annee < 1850:
        return "1849"
    if annee <= 1913:
        return "1851"
    if annee <= 1947:
        return "1915"
    if annee <= 1969:
        return "1949"
    if annee <= 1980:
        return "1971"
    if annee <= 1991:
        return "1982"
    if annee <= 2000:
        return "1993"
    if annee <= 2010:
        return "2002"
    return "2012"


def _etat_to_renovation_level(etat: str) -> str:
    e = etat.lower()
    if any(k in e for k in ["neuf", "refait", "rénov", "renov"]):
        return "RENOVATION_LEVEL.GOOD_AS_NEW"
    if any(k in e for k in ["important", "mauvais", "gros"]):
        return "RENOVATION_LEVEL.HEAVY_WORK_REQUIRED"
    if any(k in e for k in ["rafraich", "léger", "leger"]):
        return "RENOVATION_LEVEL.LIGHT_WORK_REQUIRED"
    return "RENOVATION_LEVEL.STANDARD"


def _get_city_slug(ville: str, code_postal: str) -> str:
    cp = code_postal.strip()

    def ordinal(n: int) -> str:
        return "1er" if n == 1 else f"{n}eme"

    if cp.startswith("750") and len(cp) == 5 and cp[3:].isdigit():
        arr = int(cp[3:])
        return f"paris-{ordinal(arr)}-arrondissement-{cp}"
    if re.match(r"6900[1-9]", cp):
        arr = int(cp[4])
        return f"lyon-{ordinal(arr)}-arrondissement-{cp}"
    if re.match(r"130[01]\d", cp) and 1 <= int(cp[3:]) <= 16:
        arr = int(cp[3:])
        return f"marseille-{ordinal(arr)}-arrondissement-{cp}"

    v = unicodedata.normalize("NFD", ville.lower())
    v = "".join(c for c in v if unicodedata.category(c) != "Mn")
    v = re.sub(r"[^a-z0-9]+", "-", v).strip("-")
    return f"{v}-{cp}"


def _build_form_url(bien: BienImmobilier) -> str:
    adresse_enc = urllib.parse.quote(bien.adresse)
    ville_enc = urllib.parse.quote(bien.ville)
    return (
        "https://www.meilleursagents.com/estimation-immobiliere/form"
        f"?item_zip={bien.code_postal}"
        f"&item_city_name={ville_enc}&item_address={adresse_enc}"
        "&action=prefill"
    )


def _build_page_function(bien: BienImmobilier, email: str = "", password: str = "") -> str:
    """Génère le JS pageFunction pour l'actor apify/playwright-scraper."""
    type_map = {
        "appartement": "apartment",
        "maison": "house",
        "duplex": "duplex",
        "triplex": "triplex",
        "loft": "loft",
    }
    type_id = type_map.get(bien.type_bien.lower(), "apartment")
    surface = int(bien.surface_habitable) if bien.surface_habitable else 0
    nb_pieces = bien.nb_pieces or 1
    nb_sdb = bien.nb_salles_bain or 1

    is_appart = bien.type_bien.lower() == "appartement"
    etage = bien.etage if bien.etage is not None else 1
    nb_etages = bien.nb_etages_immeuble or 1
    nb_niveaux = bien.nb_niveaux or 1

    terrain_js = ""
    if bien.surface_terrain and bien.type_bien.lower() in ("maison", "duplex", "triplex"):
        terrain_js = f"await page.fill('#land_area', '{int(bien.surface_terrain)}');"

    equip_lines = []
    if bien.ascenseur:
        equip_lines.append('await reactClick(\'label[for="elevator"]\');')
    if bien.balcon:
        equip_lines.append('await reactClick(\'label[for="balcony"]\');')
        if bien.surface_balcon:
            equip_lines.append(f"await page.fill('#balcony_area', '{int(bien.surface_balcon)}');")
    if bien.terrasse:
        equip_lines.append('await reactClick(\'label[for="terrace"]\');')
        if bien.surface_terrasse:
            equip_lines.append(f"await page.fill('#terrace_area', '{int(bien.surface_terrasse)}');")
    if bien.nb_caves > 0:
        equip_lines.append('await reactClick(\'label[for="cellar"]\');')
        for _ in range(bien.nb_caves - 1):
            equip_lines.append("await reactClick('.field--counter:has(#cellar_count) .field__count--up');")
    if bien.nb_places_parking > 0:
        equip_lines.append('await reactClick(\'label[for="parking"]\');')
        for _ in range(bien.nb_places_parking - 1):
            equip_lines.append("await reactClick('.field--counter:has(#parking_count) .field__count--up');")
    if bien.nb_chambres_service > 0:
        equip_lines.append('await reactClick(\'label[for="secondary_room"]\');')
        for _ in range(bien.nb_chambres_service - 1):
            equip_lines.append("await reactClick('.field--counter:has(#secondary_room_count) .field__count--up');")
    equip_block = "\n        ".join(equip_lines) if equip_lines else "// pas d'équipements"

    build_period_js = ""
    if bien.annee_construction:
        bp = _annee_to_build_period(bien.annee_construction)
        build_period_js = f"try {{ await page.selectOption('#build_period', '{bp}'); }} catch(e) {{}}"

    renovation_js = ""
    if bien.etat_bien:
        rl = _etat_to_renovation_level(bien.etat_bien)
        renovation_js = f"try {{ await page.selectOption('#renovation_level', '{rl}'); }} catch(e) {{}}"

    def counter_js(field_id: str, target: int, default: int = 1) -> str:
        delta = target - default
        if delta == 0:
            return ""
        direction = "up" if delta > 0 else "down"
        return rf"""
        for (let i = 0; i < {abs(delta)}; i++) {{
            await reactClick('.field--counter:has(#{field_id}) .field__count--{direction}');
            await page.waitForTimeout(50);
        }}"""

    rooms_js = counter_js("room_count", nb_pieces)
    sdb_js = counter_js("bathroom_count", nb_sdb)

    if is_appart:
        floor_js = counter_js("floor", etage)
        floor_count_js = counter_js("floor_count", nb_etages)
    else:
        floor_js = counter_js("level_count", nb_niveaux)
        floor_count_js = ""

    return rf"""
async function pageFunction(context) {{
    const {{ page, request, log }} = context;

    // Intercepter TOUTES les réponses JSON (pour capturer les prix API)
    const capturedResponses = [];
    page.on('response', async (response) => {{
        const url = response.url();
        const ct = response.headers()['content-type'] || '';
        if (ct.includes('application/json')) {{
            try {{
                const data = await response.json();
                const dataStr = JSON.stringify(data);
                capturedResponses.push({{ url, data: dataStr }});
                // Logger toutes les réponses qui pourraient contenir des prix
                if (/estim|valuat|prix|price|result|apprais|m2|m²/i.test(url + dataStr)) {{
                    log.info('API capturée [prix?]: ' + url + ' => ' + dataStr.substring(0, 200));
                }} else {{
                    log.info('API capturée: ' + url);
                }}
            }} catch(e) {{}}
        }}
    }});

    await page.waitForTimeout(3000);

    // Vérifier que la page a chargé (sinon DataDome a bloqué l'IP → retry)
    const initContent = await page.locator('body').innerText().catch(() => '');
    if (!initContent.trim()) {{
        throw new Error('Page vide — IP bloquée par DataDome, retry avec nouvelle IP');
    }}
    log.info('INIT: ' + initContent.substring(0, 200));

    // Helper : tuer Usercentrics (shadow DOM) et le supprimer du DOM
    // (défini ici pour pouvoir l'utiliser pendant le login aussi)
    async function killUsercentrics() {{
        try {{
            await page.evaluate(() => {{
                const root = document.querySelector('#usercentrics-root');
                if (!root) return;
                const sr = root.shadowRoot;
                if (sr) {{
                    const btns = Array.from(sr.querySelectorAll('button'));
                    const accept = btns.find(b => /accept|accepter|tout|allow/i.test(b.textContent || ''));
                    if (accept) accept.click();
                }}
                root.style.pointerEvents = 'none';
                root.style.display = 'none';
                root.remove();
            }});
        }} catch(e) {{ log.warning('usercentrics err: ' + e.message); }}
        await page.waitForTimeout(500);
    }}

    // Helper : supprimer DataDome du DOM
    async function killDataDome() {{
        await page.evaluate(() => {{
            document.querySelectorAll('[id*="ddChallenge"]').forEach(el => {{
                el.style.pointerEvents = 'none';
                el.style.display = 'none';
                el.remove();
            }});
        }});
    }}

    // Helper : cliquer via le handler React interne (bypass DataDome complètement)
    async function reactClick(selector) {{
        await killUsercentrics();
        await killDataDome();
        const clicked = await page.evaluate((sel) => {{
            const el = document.querySelector(sel);
            if (!el) return 'not_found';
            // Chercher __reactProps$ (React 17+)
            const propsKey = Object.keys(el).find(k => k.startsWith('__reactProps'));
            if (propsKey && el[propsKey] && el[propsKey].onClick) {{
                el[propsKey].onClick({{ preventDefault: () => {{}}, stopPropagation: () => {{}}, target: el, currentTarget: el }});
                return 'props_ok';
            }}
            // Chercher __reactFiber$ (traverser l'arbre)
            const fiberKey = Object.keys(el).find(k => k.startsWith('__reactFiber') || k.startsWith('__reactInternalInstance'));
            if (fiberKey) {{
                let fiber = el[fiberKey];
                while (fiber) {{
                    if (fiber.memoizedProps && fiber.memoizedProps.onClick) {{
                        fiber.memoizedProps.onClick({{ preventDefault: () => {{}}, stopPropagation: () => {{}}, target: el }});
                        return 'fiber_ok';
                    }}
                    fiber = fiber.return;
                }}
            }}
            // Fallback DOM
            el.click();
            return 'dom_fallback';
        }}, selector);
        log.info('reactClick(' + selector + '): ' + clicked);
        await page.waitForTimeout(400 + Math.random() * 300);
    }}

    await killUsercentrics();
    await page.waitForTimeout(1000);

    // Credentials (utilisés sur le mur d'inscription après soumission du formulaire)
    const MA_EMAIL = '{email}';
    const MA_PASSWORD = '{password}';

    // Dump après cookie
    const afterCookie = await page.locator('body').innerText().catch(() => 'err');
    log.info('AFTER COOKIE: ' + afterCookie.substring(0, 300));

    // Étape 1 : vérification adresse (toujours présente avec ?action=prefill)
    // L'adresse est déjà préremplie, on vérifie et on clique Suivant
    const addrStep = page.locator('text=Étape 1').first();
    if (await addrStep.isVisible({{ timeout: 3000 }}).catch(() => false)) {{
        log.info('étape adresse détectée');
        // Remplir le champ adresse si vide ou incorrect
        const addrInput = page.locator('input[name="address"], input[name="item_address"], input[placeholder*="adresse" i]').first();
        if (await addrInput.isVisible({{ timeout: 2000 }}).catch(() => false)) {{
            await addrInput.fill('{bien.adresse}');
            await page.waitForTimeout(1200);
            const suggestion = page.locator('[role="option"], [id*="downshift"] li').first();
            if (await suggestion.isVisible({{ timeout: 2000 }}).catch(() => false)) {{
                await suggestion.click();
                await page.waitForTimeout(800);
            }}
        }}
        // Cliquer Suivant de l'étape adresse via React handler
        await reactClick('button[data-next="true"]');
        await page.waitForTimeout(2000);
        log.info('étape adresse validée');
    }}

    // Page 2 (après adresse) : type de bien
    await page.waitForSelector('label[for="{type_id}"], #{type_id}', {{ timeout: 15000 }}).catch(() => null);
    const typeContent = await page.locator('body').innerText().catch(() => '');
    log.info('AFTER ADDR: ' + typeContent.substring(0, 200));

    // Cliquer le label du type via React handler
    await reactClick('label[for="{type_id}"]');
    await page.waitForTimeout(600);
    await reactClick('button[data-next="true"]');
    await page.waitForTimeout(2000);
    log.info('type ok');

    // Page 3 : surfaces et pièces
    const areaSel = await page.waitForSelector('#area', {{ state: 'visible', timeout: 20000 }}).catch(() => null);
    if (!areaSel) {{
        const pageText2 = await page.locator('body').innerText().catch(() => '');
        log.warning('page surfaces non trouvée. Contenu: ' + pageText2.substring(0, 500));
        return {{ url: page.url(), bodyText: pageText2 }};
    }}
    await page.fill('#area', '{surface}');
    await page.waitForTimeout(300);
    {terrain_js}
    {rooms_js}
    {sdb_js}
    {floor_js}
    {floor_count_js}
    await reactClick('button[data-next="true"]');
    await page.waitForTimeout(1500);
    log.info('surfaces ok');

    // Page 4 : équipements
    try {{
        {equip_block}
    }} catch(e) {{ log.warning('equip error: ' + e.message); }}
    await reactClick('button[data-next="true"]');
    await page.waitForTimeout(1500);
    log.info('équipements ok');

    // Page 5 : précisions
    {build_period_js}
    {renovation_js}
    await reactClick('button[data-next="true"]');
    await page.waitForTimeout(1500);
    log.info('précisions ok');

    // Page 6 : profil
    await reactClick('#false-profile_owner');
    await page.waitForTimeout(500);
    try {{ await page.selectOption('#profile_buyer', 'BUYER_PROFILE.TOURIST'); }} catch(e) {{}}
    // Chercher le bouton Estimer via JS natif (pas de :has-text Playwright)
    const estimateSel = await page.evaluate(() => {{
        const btns = Array.from(document.querySelectorAll('button'));
        const estimer = btns.find(b => /estimer|valider|obtenir/i.test(b.textContent || ''));
        if (estimer) {{
            // Ajouter un id temporaire pour le cibler
            estimer.setAttribute('data-estimate-target', 'true');
            return 'button[data-estimate-target="true"]';
        }}
        // Fallback: dernier bouton primary
        const primaries = document.querySelectorAll('button.btn--primary, button[type="submit"]');
        if (primaries.length) {{
            const last = primaries[primaries.length - 1];
            last.setAttribute('data-estimate-target', 'true');
            return 'button[data-estimate-target="true"]';
        }}
        return null;
    }});

    if (estimateSel) {{
        await reactClick(estimateSel);
        log.info('submit via: ' + estimateSel);
    }} else {{
        const bodyDbg = await page.locator('body').innerText().catch(() => '');
        log.warning('bouton Estimer introuvable. Page: ' + bodyDbg.substring(0, 400));
    }}
    log.info('formulaire soumis');

    // Attendre résultats
    await page.waitForLoadState('networkidle', {{ timeout: 30000 }}).catch(() => {{}});
    await page.waitForTimeout(3000);

    // ─── Si mur d'inscription, tenter login via modal ────────────────────────
    if (MA_EMAIL && MA_PASSWORD) {{
        const bodyWall = await page.locator('body').innerText().catch(() => '');
        if (/connectez-vous|créer votre compte|création de votre compte/i.test(bodyWall)) {{
            log.info('Mur inscription détecté — tentative login via modal');

            // Cliquer sur "Connectez-vous"
            const connectBtn = page.locator('a:has-text("Connectez-vous"), button:has-text("Connectez-vous"), a:has-text("Se connecter"), button:has-text("Se connecter")').first();
            if (await connectBtn.isVisible({{ timeout: 3000 }}).catch(() => false)) {{
                await connectBtn.click();
                await page.waitForTimeout(2000);
                log.info('lien Connectez-vous cliqué, URL: ' + page.url());
            }}

            // Remplir email
            const emailEl = page.locator('input[type="email"], input[name="email"]').first();
            if (await emailEl.isVisible({{ timeout: 5000 }}).catch(() => false)) {{
                await emailEl.fill(MA_EMAIL);
                await page.waitForTimeout(300);
                log.info('email rempli (modal login)');
            }} else {{
                log.warning('champ email introuvable dans modal');
            }}

            // Remplir password
            const pwEl = page.locator('input[type="password"]').first();
            if (await pwEl.isVisible({{ timeout: 3000 }}).catch(() => false)) {{
                await pwEl.fill(MA_PASSWORD);
                await page.waitForTimeout(300);
            }}

            // Soumettre le login
            const subBtn = page.locator('button[type="submit"]').first();
            if (await subBtn.isVisible({{ timeout: 3000 }}).catch(() => false)) {{
                await subBtn.click();
            }} else {{
                await pwEl.press('Enter').catch(() => {{}});
            }}
            await page.waitForTimeout(5000);
            await page.waitForLoadState('networkidle', {{ timeout: 20000 }}).catch(() => {{}});
            log.info('login modal soumis, URL: ' + page.url());

            // Vérifier si on est sur la page de résultats ou encore sur le formulaire
            const afterLoginUrl = page.url();
            const afterLoginBody = await page.locator('body').innerText().catch(() => '');
            if (!afterLoginUrl.includes('/mon-compte/') && /connectez-vous|connexion|créer/i.test(afterLoginBody)) {{
                // Login OK mais on est encore sur le formulaire → recliquer Estimer avec contact_id
                log.info('Login effectué — reclic Estimer avec contact_id disponible');
                await page.waitForTimeout(1000);
                await killUsercentrics();
                // Chercher et cliquer le bouton Estimer
                const estimateSel2 = await page.evaluate(() => {{
                    const btns = Array.from(document.querySelectorAll('button'));
                    const estimer = btns.find(b => /estimer|obtenir|valider/i.test(b.textContent || ''));
                    if (estimer) {{
                        estimer.setAttribute('data-estimate-resubmit', 'true');
                        return 'button[data-estimate-resubmit="true"]';
                    }}
                    const primaries = document.querySelectorAll('button.btn--primary, button[type="submit"]');
                    if (primaries.length) {{
                        primaries[primaries.length - 1].setAttribute('data-estimate-resubmit', 'true');
                        return 'button[data-estimate-resubmit="true"]';
                    }}
                    return null;
                }});
                if (estimateSel2) {{
                    await reactClick(estimateSel2);
                    log.info('Estimer recliqué: ' + estimateSel2);
                    await page.waitForTimeout(4000);
                    await page.waitForLoadState('networkidle', {{ timeout: 20000 }}).catch(() => {{}});
                    log.info('après re-submit URL: ' + page.url());
                }}
            }}
        }}
    }}
    // ─────────────────────────────────────────────────────────────────────────

    // Attendre que les prix soient rendus (chercher €/m² dans le texte)
    let waitIter = 0;
    while (waitIter < 10) {{
        await page.waitForTimeout(1500);
        const checkText = await page.locator('body').innerText().catch(() => '');
        if (/\d[\d\s ]+€\s*[/]\s*m/i.test(checkText)) {{
            log.info('Prix détectés dans la page après ' + (waitIter + 1) + ' attentes');
            break;
        }}
        waitIter++;
    }}

    const bodyText = await page.locator('body').innerText();
    // HTML (premiers 5000 chars) pour debug
    const bodyHTML = await page.evaluate(() => document.body.innerHTML.substring(0, 5000)).catch(() => '');
    const url = page.url();
    log.info('url finale: ' + url);
    log.info('body text (500): ' + bodyText.substring(0, 500));
    log.info('réponses API capturées: ' + capturedResponses.length);
    if (capturedResponses.length > 0) {{
        log.info('première API: ' + capturedResponses[0].url + ' — ' + capturedResponses[0].data.substring(0, 300));
    }}

    return {{
        url: url,
        bodyText: bodyText,
        bodyHTML: bodyHTML,
        capturedResponses: capturedResponses,
    }};
}}
"""


def _extraire_depuis_api(responses: List[Dict[str, Any]], bien: BienImmobilier) -> Optional[PrixM2Result]:
    """Tente d'extraire les prix depuis les réponses API JSON capturées."""
    for resp in responses:
        url_r = resp.get("url", "")
        try:
            data = json.loads(resp.get("data", "{}"))
        except Exception:
            continue

        # Format spécifique de myaccountapi.meilleursagents.com/myaccountapi/habitations/<id>
        if not isinstance(data, dict):
            continue
        if "myaccountapi/habitations/" in url_r and "/valuation/" not in url_r and "/additional" not in url_r:
            valuation = data.get("valuation") or {}
            sell = valuation.get("sell") or {}
            if not sell:
                sell = data.get("sell") or {}
            if sell:
                ppm = sell.get("price_per_sqm") or sell.get("price_m2") or sell.get("sqm_price")
                ppm_low = sell.get("price_per_sqm_low") or sell.get("low") or sell.get("min")
                ppm_high = sell.get("price_per_sqm_high") or sell.get("high") or sell.get("max")
                total = sell.get("price") or sell.get("total")
                if ppm and 500 < ppm < 50000:
                    result = PrixM2Result(
                        localisation=f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", "),
                        prix_m2_moyen=round(ppm),
                        tranche_basse=round(ppm_low) if ppm_low else None,
                        tranche_haute=round(ppm_high) if ppm_high else None,
                        prix_total_moyen=round(total) if total else None,
                        raw_output=resp.get("data", "")[:300],
                    )
                    return result

        # Chercher récursivement les clés de prix dans la réponse JSON
        def find_prices(obj, depth=0):
            if depth > 8:
                return {}
            prices = {}
            if isinstance(obj, dict):
                for k, v in obj.items():
                    k_lower = k.lower()
                    if isinstance(v, (int, float)) and 500 < v < 100_000_000:
                        if any(x in k_lower for x in ["avg", "mean", "moyen", "median"]):
                            if v < 50_000:
                                prices["prix_m2_moyen"] = v
                            else:
                                prices["prix_total_moyen"] = v
                        elif any(x in k_lower for x in ["min", "low", "bas", "basse"]):
                            if v < 50_000:
                                prices["tranche_basse"] = v
                            else:
                                prices["prix_total_bas"] = v
                        elif any(x in k_lower for x in ["max", "high", "haut", "haute"]):
                            if v < 50_000:
                                prices["tranche_haute"] = v
                            else:
                                prices["prix_total_haut"] = v
                    elif isinstance(v, (dict, list)):
                        sub = find_prices(v, depth + 1)
                        prices.update({kk: vv for kk, vv in sub.items() if kk not in prices})
            elif isinstance(obj, list):
                for item in obj:
                    sub = find_prices(item, depth + 1)
                    prices.update({kk: vv for kk, vv in sub.items() if kk not in prices})
            return prices

        prices = find_prices(data)
        if prices.get("prix_m2_moyen") or prices.get("prix_total_moyen"):
            result = PrixM2Result(
                localisation=f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", "),
                raw_output=resp.get("data", "")[:500],
                **{k: v for k, v in prices.items() if k in PrixM2Result.__dataclass_fields__},
            )
            # Calculer prix/m² depuis total si nécessaire
            surface = bien.surface_habitable
            if surface and surface > 0:
                if result.prix_total_moyen and not result.prix_m2_moyen:
                    result.prix_m2_moyen = round(result.prix_total_moyen / surface)
                if result.prix_total_bas and not result.tranche_basse:
                    result.tranche_basse = round(result.prix_total_bas / surface)
                if result.prix_total_haut and not result.tranche_haute:
                    result.tranche_haute = round(result.prix_total_haut / surface)
            return result

    return None


def _nettoyer(valeur: str) -> Optional[float]:
    try:
        return float(re.sub(r"[\s ,]", "", valeur))
    except Exception:
        return None


def _parser_resultat(output: str, bien: BienImmobilier) -> PrixM2Result:
    result = PrixM2Result(
        localisation=f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", "),
        raw_output=output,
    )

    for pattern in [
        r"(\d[\d\s ]*)\s*€\s*/\s*m\s*[²2]",
        r"prix\s+au\s+m[²2]\s*[:\*]*\s*(\d[\d\s ]*)\s*€",
    ]:
        for match in re.findall(pattern, output, re.IGNORECASE):
            valeur = _nettoyer(match)
            if valeur and 500 < valeur < 50000:
                result.prix_m2_moyen = valeur
                break
        if result.prix_m2_moyen:
            break

    for pattern, label in [
        (r"(?:basse|bas)[^\d]{0,30}(\d[\d\s ]{4,})\s*€", "bas"),
        (r"(?:moyenne?|moyen)[^\d]{0,30}(\d[\d\s ]{4,})\s*€", "moyen"),
        (r"(?:haute?|haut)[^\d]{0,30}(\d[\d\s ]{4,})\s*€", "haut"),
    ]:
        for match in re.findall(pattern, output, re.IGNORECASE):
            valeur = _nettoyer(match)
            if valeur and 50000 < valeur < 100_000_000:
                if label == "bas":
                    result.prix_total_bas = valeur
                if label == "moyen":
                    result.prix_total_moyen = valeur
                if label == "haut":
                    result.prix_total_haut = valeur
                break

    surface = bien.surface_habitable
    if surface and surface > 0:
        if result.prix_total_moyen and not result.prix_m2_moyen:
            result.prix_m2_moyen = round(result.prix_total_moyen / surface)
        if result.prix_total_bas:
            result.tranche_basse = round(result.prix_total_bas / surface)
        if result.prix_total_haut:
            result.tranche_haute = round(result.prix_total_haut / surface)

    return result


def get_prix_m2(
    bien: BienImmobilier,
    api_key: Optional[str] = None,
    email: Optional[str] = None,
    password: Optional[str] = None,
) -> PrixM2Result:
    token = api_key or os.environ.get("APIFY_API_TOKEN")
    if not token:
        raise ValueError("APIFY_API_TOKEN manquant.")

    ma_email = email or os.environ.get("MA_EMAIL", "")
    ma_password = password or os.environ.get("MA_PASSWORD", "")
    if not ma_email or not ma_password:
        print("      [AVERTISSEMENT] MA_EMAIL/MA_PASSWORD absents — contact_id sera null, l'estimation sera bloquée.", flush=True)

    client = ApifyClient(token)
    form_url = _build_form_url(bien)

    print(f"[1/3] Lancement de l'actor Apify playwright-scraper...", flush=True)
    print(f"      URL: {form_url}", flush=True)
    if ma_email:
        print(f"      Compte: {ma_email}", flush=True)
    t = time.time()

    page_function = _build_page_function(bien, email=ma_email, password=ma_password)

    run_input = {
        "startUrls": [{"url": form_url}],
        "pageFunction": page_function,
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"],
        },
        "launchContext": {
            "useChrome": True,
            "stealth": True,
            "launchOptions": {
                "headless": True,
                "args": [
                    "--disable-blink-features=AutomationControlled",
                    "--lang=fr-FR",
                ],
            },
        },
        "preNavigationHooks": "[async ({ page }, goToOptions) => { goToOptions.waitUntil = 'domcontentloaded'; goToOptions.timeout = 60000; await page.setExtraHTTPHeaders({ 'Accept-Language': 'fr-FR,fr;q=0.9', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8' }); }]",
        "maxRequestRetries": 3,
        "pageFunctionTimeoutSecs": 240,
    }

    actor_run = client.actor("apify/playwright-scraper").call(
        run_input=run_input,
        timeout_secs=600,
    )

    print(f"      Actor terminé ({time.time() - t:.1f}s) — run_id={actor_run['id']}", flush=True)

    print("[2/3] Récupération des résultats...", flush=True)
    t = time.time()

    dataset_items = list(
        client.dataset(actor_run["defaultDatasetId"]).iterate_items()
    )

    if not dataset_items:
        print("      Aucun résultat dans le dataset!", flush=True)
        return PrixM2Result(
            localisation=f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", "),
            raw_output="Aucun résultat retourné par l'actor",
        )

    item = dataset_items[0]
    body_text = item.get("bodyText", "")
    body_html = item.get("bodyHTML", "")
    final_url = item.get("url", "")
    captured_responses = item.get("capturedResponses", [])
    print(f"      OK ({time.time() - t:.1f}s) — URL finale: {final_url}", flush=True)
    print(f"      Texte extrait: {len(body_text)} caractères", flush=True)
    print(f"      HTML extrait: {len(body_html)} caractères", flush=True)
    print(f"      Réponses API interceptées: {len(captured_responses)}", flush=True)
    for r in captured_responses:
        url_r = r.get('url', '')
        data_r = r.get('data', '')
        print(f"        [{url_r}] {data_r[:200]}", flush=True)
    if body_html:
        print(f"      Aperçu HTML: {body_html[:600]}", flush=True)

    print("[3/3] Parsing des prix...", flush=True)
    # Priorité : réponses API JSON capturées > parsing du texte HTML
    result = _extraire_depuis_api(captured_responses, bien)
    if result and result.prix_m2_moyen:
        print("      Prix extraits depuis l'API JSON.", flush=True)
        return result
    result = _parser_resultat(body_text, bien)
    if result.prix_m2_moyen:
        print("      Prix extraits depuis le texte HTML.", flush=True)
    else:
        print("      Aucun prix trouvé — mur d'inscription ou format inattendu.", flush=True)
        print(f"      Aperçu: {body_text[:400]}", flush=True)
    return result


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

    resultat = get_prix_m2(bien)  # MA_EMAIL + MA_PASSWORD lus depuis .env
    print(resultat)
