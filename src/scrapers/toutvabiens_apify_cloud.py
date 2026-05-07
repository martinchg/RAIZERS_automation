"""
Scraper toutvabiens.com via Apify Cloud (playwright-scraper actor)
- Estimation immobilière gratuite sans création de compte
- Proxy résidentiel Apify, anti-detect géré par Apify
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

import urllib.request

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

    annee_construction: Optional[int] = None
    etat_bien: Optional[str] = None
    dpe: Optional[str] = None


def _geocode_ban(adresse: str, code_postal: str, ville: str) -> Dict[str, Any]:
    """Géocode une adresse via l'API BAN (base adresse nationale) gratuite."""
    q = urllib.parse.quote(f"{adresse} {code_postal} {ville}")
    url = f"https://api-adresse.data.gouv.fr/search/?q={q}&limit=1"
    try:
        with urllib.request.urlopen(url, timeout=8) as resp:
            data = json.loads(resp.read())
        feat = data.get("features", [{}])[0]
        props = feat.get("properties", {})
        coords = feat.get("geometry", {}).get("coordinates", [None, None])
        return {
            "latitude": coords[1],
            "longitude": coords[0],
            "cp": props.get("postcode", code_postal),
            "city": props.get("city", ville),
            "adresse_label": props.get("label", f"{adresse} {code_postal} {ville}"),
        }
    except Exception as e:
        print(f"      [WARN] Geocoding BAN échoué: {e}", flush=True)
        return {"latitude": None, "longitude": None, "cp": code_postal, "city": ville, "adresse_label": f"{adresse} {code_postal} {ville}"}


def _build_page_function(bien: BienImmobilier, geo: Dict[str, Any] = None) -> str:
    type_map = {
        "appartement": "appartement",
        "maison": "maison",
        "duplex": "maison",
        "loft": "appartement",
        "studio": "appartement",
    }
    type_label = type_map.get(bien.type_bien.lower(), "appartement")
    surface = int(bien.surface_habitable) if bien.surface_habitable else 0
    nb_pieces = bien.nb_pieces or 3
    geo = geo or {}
    adresse_label = geo.get("adresse_label") or f"{bien.adresse}, {bien.code_postal} {bien.ville}"
    lat = geo.get("latitude") or ""
    lng = geo.get("longitude") or ""
    cp = geo.get("cp") or bien.code_postal
    city = geo.get("city") or bien.ville

    return rf"""
async function pageFunction(context) {{
    const {{ page, request, log }} = context;

    // Capturer toutes les réponses JSON
    const capturedResponses = [];
    page.on('response', async (response) => {{
        const ct = response.headers()['content-type'] || '';
        if (ct.includes('application/json')) {{
            try {{
                const data = await response.json();
                const dataStr = JSON.stringify(data);
                capturedResponses.push({{ url: response.url(), data: dataStr }});
            }} catch(e) {{}}
        }}
    }});

    // Helper : fermer les bandeaux cookies
    async function acceptCookies() {{
        const cookieSels = [
            'button:has-text("Tout accepter")',
            'button:has-text("Accepter tout")',
            'button:has-text("Accepter")',
            'button:has-text("OK")',
            '#axeptio_btn_acceptAll',
            '.axeptio_btn_acceptAll',
            '[class*="axeptio"][class*="accept"]',
            '#didomi-notice-agree-button',
            '.cc-btn.cc-allow',
            '#onetrust-accept-btn-handler',
        ];
        for (const sel of cookieSels) {{
            const btn = page.locator(sel).first();
            if (await btn.isVisible({{ timeout: 1200 }}).catch(() => false)) {{
                await btn.click();
                log.info('cookies acceptés: ' + sel);
                await page.waitForTimeout(600);
                return;
            }}
        }}
    }}

    await page.waitForTimeout(3000);
    const initBody = await page.locator('body').innerText().catch(() => '');
    if (!initBody.trim()) throw new Error('Page vide — retry avec nouvelle IP');
    log.info('PAGE INIT (300): ' + initBody.substring(0, 300));

    await acceptCookies();

    // ── Logger les éléments de formulaire ────────────────────────────────────
    const formEls = await page.evaluate(() => {{
        return Array.from(document.querySelectorAll('input, select, button')).map(el => ({{
            tag: el.tagName, name: el.name || '', type: el.type || '',
            placeholder: el.placeholder || '', value: el.value || '',
            text: (el.textContent || '').trim().substring(0, 50),
            visible: el.offsetParent !== null,
        }}));
    }});
    log.info('FORM ELEMENTS: ' + JSON.stringify(formEls.slice(0, 25)));

    // ══ PHASE 1 : formulaire adresse ══════════════════════════════════════════
    const adresseLabel = '{adresse_label}';
    const geoLat = '{lat}';
    const geoLng = '{lng}';
    const geoCp  = '{cp}';
    const geoCity = '{city}';

    // Remplir le champ texte visible
    const adresseEl = page.locator('input[name="adresse"], input[placeholder*="Saisissez" i], input[placeholder*="adresse" i]').first();
    if (await adresseEl.isVisible({{ timeout: 4000 }}).catch(() => false)) {{
        await adresseEl.fill(adresseLabel);
        await page.waitForTimeout(600);
        log.info('adresse remplie: ' + adresseLabel);
    }} else {{
        log.info('WARN: champ adresse non trouvé');
    }}

    // Injecter les champs cachés directement (un seul objet pour page.evaluate)
    await page.evaluate((args) => {{
        const {{ label, lat, lng, cp, city }} = args;
        const setVal = (name, val) => {{
            const el = document.querySelector(`[name="${{name}}"]`);
            if (el && val !== '') {{
                el.value = val;
                el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                el.dispatchEvent(new Event('input', {{ bubbles: true }}));
            }}
        }};
        setVal('adresse', label);
        setVal('cp', cp);
        setVal('city', city);
        if (lat) setVal('latitude', String(lat));
        if (lng) setVal('longitude', String(lng));
    }}, {{ label: adresseLabel, lat: geoLat, lng: geoLng, cp: geoCp, city: geoCity }});
    log.info('champs cachés injectés: cp=' + geoCp + ' city=' + geoCity + ' lat=' + geoLat);

    // Cliquer le bouton "Estimer" du formulaire d'adresse
    const estimBtn = page.locator('button:has-text("Estimer"), input[type="submit"][value*="Estimer" i], button[type="submit"]').first();
    if (await estimBtn.isVisible({{ timeout: 3000 }}).catch(() => false)) {{
        await estimBtn.click();
        log.info('Bouton Estimer cliqué (formulaire adresse)');
    }} else {{
        log.info('WARN: bouton Estimer non trouvé — essai submit direct');
        await page.evaluate(() => {{
            const form = document.querySelector('form');
            if (form) form.submit();
        }});
    }}

    // Attendre la navigation + résolution éventuelle du bot check wp.com
    await page.waitForLoadState('domcontentloaded', {{ timeout: 30000 }}).catch(() => {{}});

    // Boucle d'attente : si bot check en cours, patienter jusqu'à résolution
    let botWait = 0;
    while (botWait < 15) {{
        await page.waitForTimeout(2000);
        const checkBody = await page.locator('body').innerText().catch(() => '');
        if (!/checking your browser|secured by/i.test(checkBody)) {{
            log.info('Bot check résolu après ' + (botWait + 1) + ' attentes');
            break;
        }}
        botWait++;
    }}
    await acceptCookies();

    const url1 = page.url();
    const body1 = await page.locator('body').innerText().catch(() => '');
    log.info('URL APRES FORM ADRESSE: ' + url1);
    log.info('BODY APRES FORM ADRESSE (600): ' + body1.substring(0, 600));

    // Logger les éléments de formulaire après la soumission
    const formEls2 = await page.evaluate(() => {{
        return Array.from(document.querySelectorAll('input, select, button, label')).map(el => ({{
            tag: el.tagName, name: el.name || '', type: el.type || '',
            placeholder: el.placeholder || '', value: el.value || '',
            text: (el.textContent || '').trim().substring(0, 60),
            visible: el.offsetParent !== null,
        }}));
    }});
    log.info('FORM ELEMENTS APRES SOUMISSION: ' + JSON.stringify(formEls2.slice(0, 30)));

    // ══ PHASE 2 : formulaire multi-étapes (type / surface / pièces) ═══════════

    // Détecter si on est sur une page d'estimation ou de résultats
    const isPrixPage = /[€$]\s*[/]\s*m|prix\s+au\s+m|estimation.*[0-9]/i.test(body1);
    if (isPrixPage) {{
        log.info('Prix détectés directement après soumission adresse');
    }} else {{
        // ── Sélectionner le type de bien ──────────────────────────────────────
        const typeBien = '{type_label}';
        log.info('Sélection type: ' + typeBien);

        // Essai via label/bouton texte
        const typeBtns = page.locator(`label:has-text("${{typeBien}}"), button:has-text("${{typeBien}}")`).first();
        if (await typeBtns.isVisible({{ timeout: 3000 }}).catch(() => false)) {{
            await typeBtns.click();
            log.info('type cliqué via texte: ' + typeBien);
            await page.waitForTimeout(600);
        }} else {{
            // Essai via select ou radio input
            const typeSelect = page.locator('select[name*="type" i], select[id*="type" i], select[name*="bien" i]').first();
            if (await typeSelect.isVisible({{ timeout: 1500 }}).catch(() => false)) {{
                await typeSelect.selectOption({{ label: new RegExp(typeBien, 'i') }}).catch(async () => {{
                    await typeSelect.selectOption({{ index: typeBien === 'maison' ? 1 : 0 }}).catch(() => {{}});
                }});
                log.info('type sélectionné via select');
                await page.waitForTimeout(500);
            }} else {{
                log.info('WARN: aucun sélecteur de type trouvé');
            }}
        }}

        // Bouton Suivant étape type
        const nextSels = ['button:has-text("Suivant")', 'button:has-text("Continuer")', '[data-action*="next"]'];
        for (const sel of nextSels) {{
            const btn = page.locator(sel).first();
            if (await btn.isVisible({{ timeout: 1500 }}).catch(() => false)) {{
                await btn.click();
                log.info('Suivant (type): ' + sel);
                await page.waitForTimeout(1500);
                break;
            }}
        }}
        await acceptCookies();

        const body2 = await page.locator('body').innerText().catch(() => '');
        log.info('AFTER TYPE STEP (300): ' + body2.substring(0, 300));

        // ── Surface ──────────────────────────────────────────────────────────
        const surface = {surface};
        const surfaceSels = [
            'input[name*="surface" i]', 'input[id*="surface" i]',
            'input[placeholder*="surface" i]', 'input[placeholder*="m" i]',
            'input[type="number"]',
        ];
        for (const sel of surfaceSels) {{
            const el = page.locator(sel).first();
            if (await el.isVisible({{ timeout: 1500 }}).catch(() => false)) {{
                await el.fill(String(surface));
                log.info('surface remplie: ' + surface + ' via ' + sel);
                await page.waitForTimeout(400);
                break;
            }}
        }}

        // ── Nombre de pièces ─────────────────────────────────────────────────
        const nbPieces = {nb_pieces};
        const piecesSels = [
            'input[name*="piece" i]', 'select[name*="piece" i]',
            'input[name*="room" i]', 'select[name*="room" i]',
        ];
        for (const sel of piecesSels) {{
            const el = page.locator(sel).first();
            if (await el.isVisible({{ timeout: 1000 }}).catch(() => false)) {{
                const tag = await el.evaluate(e => e.tagName.toLowerCase());
                if (tag === 'select') {{
                    await el.selectOption(String(nbPieces)).catch(() => {{}});
                }} else {{
                    await el.fill(String(nbPieces));
                }}
                log.info('pièces remplies: ' + nbPieces);
                await page.waitForTimeout(400);
                break;
            }}
        }}
        // Essai clic bouton compteur/radio pièces
        const pieceBtns = page.locator(`button:has-text("${{nbPieces}}"), [data-value="${{nbPieces}}"]`).first();
        if (await pieceBtns.isVisible({{ timeout: 800 }}).catch(() => false)) {{
            await pieceBtns.click();
            log.info('pièces cliquées: ' + nbPieces);
            await page.waitForTimeout(400);
        }}

        // Bouton Suivant / Valider / Estimer final
        const finalSels = [
            'button:has-text("Suivant")',
            'button:has-text("Valider")',
            'button:has-text("Calculer")',
            'button:has-text("Obtenir")',
            'button:has-text("Estimer")',
            'button[type="submit"]',
            'input[type="submit"]',
        ];
        for (const sel of finalSels) {{
            const btn = page.locator(sel).first();
            if (await btn.isVisible({{ timeout: 1500 }}).catch(() => false)) {{
                await btn.click();
                log.info('bouton final cliqué: ' + sel);
                await page.waitForTimeout(2000);
                break;
            }}
        }}
        await acceptCookies();

        const body3 = await page.locator('body').innerText().catch(() => '');
        log.info('AFTER SURFACE/PIECES (300): ' + body3.substring(0, 300));

        // S'il y a encore un suivant, continuer
        for (const sel of finalSels) {{
            const btn = page.locator(sel).first();
            if (await btn.isVisible({{ timeout: 1500 }}).catch(() => false)) {{
                await btn.click();
                log.info('bouton extra cliqué: ' + sel);
                await page.waitForTimeout(2000);
                break;
            }}
        }}
    }}

    // ══ PHASE 3 : attendre et récupérer les résultats ══════════════════════════
    await page.waitForLoadState('networkidle', {{ timeout: 30000 }}).catch(() => {{}});

    let waitIter = 0;
    while (waitIter < 12) {{
        await page.waitForTimeout(2000);
        const checkText = await page.locator('body').innerText().catch(() => '');
        if (/\d[\d\s ]+[€$]\s*[/]\s*m|\d+\s*[€$]\/m|prix.*m[²2]|valeur.*[0-9]/i.test(checkText)) {{
            log.info('Résultats prix détectés après ' + (waitIter + 1) + ' attentes');
            break;
        }}
        waitIter++;
    }}

    const bodyFinal = await page.locator('body').innerText().catch(() => '');
    const urlFinal = page.url();
    log.info('URL FINALE: ' + urlFinal);
    log.info('BODY FINAL (800): ' + bodyFinal.substring(0, 800));
    log.info('API capturées: ' + capturedResponses.length);

    for (const r of capturedResponses) {{
        if (/estim|valuat|prix|price|result/i.test(r.url + r.data)) {{
            log.info('API [prix?]: ' + r.url + ' => ' + r.data.substring(0, 300));
        }}
    }}

    return {{
        url: urlFinal,
        bodyText: bodyFinal,
        capturedResponses: capturedResponses,
    }};
}}
"""


def _nettoyer(valeur: str) -> Optional[float]:
    try:
        return float(re.sub(r"[\s ,]", "", valeur))
    except Exception:
        return None


def _parser_resultat(output: str, bien: BienImmobilier) -> PrixM2Result:
    result = PrixM2Result(
        localisation=f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", "),
        raw_output=output,
    )

    # Prix au m²
    for pattern in [
        r"(\d[\d\s ]*)\s*[€$]\s*/\s*m\s*[²2]",
        r"prix\s+(?:au\s+)?m[²2]\s*[:\*]*\s*(\d[\d\s ]*)\s*[€$]",
        r"(\d[\d\s ]*)\s*[€$]\s*/m[²2]",
    ]:
        for match in re.findall(pattern, output, re.IGNORECASE):
            valeur = _nettoyer(match)
            if valeur and 500 < valeur < 50000:
                if not result.prix_m2_moyen:
                    result.prix_m2_moyen = valeur
                elif valeur < result.prix_m2_moyen:
                    result.tranche_basse = valeur
                elif valeur > result.prix_m2_moyen:
                    result.tranche_haute = valeur

    # Prix total
    for pattern, label in [
        (r"(?:basse?|bas|minimum|min)[^\d]{0,40}(\d[\d\s ]{4,})\s*[€$]", "bas"),
        (r"(?:moyen(?:ne?)?|médian)[^\d]{0,40}(\d[\d\s ]{4,})\s*[€$]", "moyen"),
        (r"(?:haute?|haut|maximum|max)[^\d]{0,40}(\d[\d\s ]{4,})\s*[€$]", "haut"),
        (r"(\d[\d\s ]{4,})\s*[€$]\s*-\s*(\d[\d\s ]{4,})\s*[€$]", "range"),
    ]:
        if label == "range":
            for match in re.findall(pattern, output, re.IGNORECASE):
                bas = _nettoyer(match[0])
                haut = _nettoyer(match[1])
                if bas and haut and 10000 < bas < 100_000_000 and bas < haut:
                    if not result.prix_total_bas:
                        result.prix_total_bas = bas
                    if not result.prix_total_haut:
                        result.prix_total_haut = haut
                    if not result.prix_total_moyen:
                        result.prix_total_moyen = round((bas + haut) / 2)
        else:
            for match in re.findall(pattern, output, re.IGNORECASE):
                valeur = _nettoyer(match)
                if valeur and 10000 < valeur < 100_000_000:
                    if label == "bas" and not result.prix_total_bas:
                        result.prix_total_bas = valeur
                    elif label == "moyen" and not result.prix_total_moyen:
                        result.prix_total_moyen = valeur
                    elif label == "haut" and not result.prix_total_haut:
                        result.prix_total_haut = valeur

    surface = bien.surface_habitable
    if surface and surface > 0:
        if result.prix_total_moyen and not result.prix_m2_moyen:
            result.prix_m2_moyen = round(result.prix_total_moyen / surface)
        if result.prix_total_bas and not result.tranche_basse:
            result.tranche_basse = round(result.prix_total_bas / surface)
        if result.prix_total_haut and not result.tranche_haute:
            result.tranche_haute = round(result.prix_total_haut / surface)

    return result


def _extraire_depuis_api(responses: List[Dict[str, Any]], bien: BienImmobilier) -> Optional[PrixM2Result]:
    """Tente d'extraire les prix depuis les réponses API JSON capturées."""
    for resp in responses:
        try:
            data = json.loads(resp.get("data", "{}"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        def find_prices(obj, depth=0):
            if depth > 8:
                return {}
            prices = {}
            if isinstance(obj, dict):
                for k, v in obj.items():
                    kl = k.lower()
                    if isinstance(v, (int, float)) and 500 < v < 100_000_000:
                        if any(x in kl for x in ["avg", "mean", "moyen", "median", "middle"]):
                            prices["prix_m2_moyen" if v < 50_000 else "prix_total_moyen"] = v
                        elif any(x in kl for x in ["min", "low", "bas", "basse", "bottom"]):
                            prices["tranche_basse" if v < 50_000 else "prix_total_bas"] = v
                        elif any(x in kl for x in ["max", "high", "haut", "haute", "top"]):
                            prices["tranche_haute" if v < 50_000 else "prix_total_haut"] = v
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
            surface = bien.surface_habitable
            result = PrixM2Result(
                localisation=f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", "),
                raw_output=resp.get("data", "")[:500],
            )
            for k, v in prices.items():
                if hasattr(result, k):
                    setattr(result, k, v)
            if surface and surface > 0:
                if result.prix_total_moyen and not result.prix_m2_moyen:
                    result.prix_m2_moyen = round(result.prix_total_moyen / surface)
                if result.prix_total_bas and not result.tranche_basse:
                    result.tranche_basse = round(result.prix_total_bas / surface)
                if result.prix_total_haut and not result.tranche_haute:
                    result.tranche_haute = round(result.prix_total_haut / surface)
            return result

    return None


def get_prix_m2(
    bien: BienImmobilier,
    api_key: Optional[str] = None,
) -> PrixM2Result:
    token = api_key or os.environ.get("APIFY_API_TOKEN")
    if not token:
        raise ValueError("APIFY_API_TOKEN manquant.")

    client = ApifyClient(token)
    start_url = "https://toutvabiens.com/estimation-immobiliere-gratuite/"

    print("[1/3] Lancement de l'actor Apify pour toutvabiens.com...", flush=True)
    print(f"      Bien: {bien.adresse}, {bien.ville} {bien.code_postal}", flush=True)

    geo = _geocode_ban(bien.adresse, bien.code_postal, bien.ville)
    print(f"      Géocodage BAN: {geo}", flush=True)

    t = time.time()

    page_function = _build_page_function(bien, geo=geo)

    run_input = {
        "startUrls": [{"url": start_url}],
        "pageFunction": page_function,
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
        "preNavigationHooks": "[async ({ page }, goToOptions) => { goToOptions.waitUntil = 'domcontentloaded'; goToOptions.timeout = 60000; await page.setExtraHTTPHeaders({ 'Accept-Language': 'fr-FR,fr;q=0.9' }); }]",
        "maxRequestRetries": 2,
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
    final_url = item.get("url", "")
    captured_responses = item.get("capturedResponses", [])

    print(f"      OK ({time.time() - t:.1f}s) — URL finale: {final_url}", flush=True)
    print(f"      Texte extrait: {len(body_text)} caractères", flush=True)
    print(f"      Réponses API interceptées: {len(captured_responses)}", flush=True)
    for r in captured_responses:
        url_r = r.get("url", "")
        data_r = r.get("data", "")
        if re.search(r"estim|valuat|prix|price|result", url_r + data_r, re.IGNORECASE):
            print(f"        [prix?] [{url_r}] {data_r[:200]}", flush=True)

    print("[3/3] Parsing des prix...", flush=True)
    result = _extraire_depuis_api(captured_responses, bien)
    if result and result.prix_m2_moyen:
        print("      Prix extraits depuis l'API JSON.", flush=True)
        return result

    result = _parser_resultat(body_text, bien)
    if result.prix_m2_moyen:
        print("      Prix extraits depuis le texte HTML.", flush=True)
    else:
        print("      Aucun prix trouvé — voir logs Apify pour debug.", flush=True)
        print(f"      Aperçu body: {body_text[:500]}", flush=True)
    return result


if __name__ == "__main__":
    bien = BienImmobilier(
        adresse="12 rue de Rivoli",
        code_postal="75004",
        ville="Paris",
        type_bien="appartement",
        surface_habitable=65,
        nb_pieces=3,
        nb_salles_bain=1,
        etage=4,
        nb_etages_immeuble=7,
        annee_construction=1972,
        etat_bien="bon état",
        dpe="D",
    )

    resultat = get_prix_m2(bien)
    print(resultat)
