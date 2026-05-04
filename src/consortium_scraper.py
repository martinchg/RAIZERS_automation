"""
Scraper consortium-immobilier.fr via Firecrawl /interact
- Remplit le formulaire avec les sélecteurs HTML exacts
- Aucun compte requis
- Retourne : prix/m² moyen, tranche basse, tranche haute

Variable d'environnement requise :
    FIRECRAWL_API_KEY

Install : pip install firecrawl-py python-dotenv
"""

import os
import re
import time
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv
from firecrawl import FirecrawlApp

load_dotenv()

URL = "https://www.consortium-immobilier.fr/particuliers/estimer.html?fb=1&p=appartement"


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
    adresse:     str
    code_postal: str = ""
    ville:       str = ""
    type_bien:   str = "appartement"   # "appartement" | "maison" | "terrain"

    surface_habitable:  Optional[float] = None
    surface_terrain:    Optional[float] = None

    nb_pieces:   Optional[int] = None   # 1–9, 10 = 10+
    nb_chambres: Optional[int] = None   # 1–10
    etage:       Optional[int] = None   # 0 = RDC

    annee_construction: Optional[int] = None

    # Équipements
    parking:       bool = False
    garage:        bool = False
    climatisation: bool = False
    cheminee:      bool = False
    piscine:       bool = False
    doublevitrage: bool = False
    balcon:        bool = False
    terrasse:      bool = False
    rdc:           bool = False
    jardin:        bool = False


@dataclass
class ContactInfo:
    nom:      str = "Dupont"
    prenom:   str = "Jean"
    portable: str = "0600000000"
    email:    str = "estimation@example.com"


# ─────────────────────────────────────────────
# Génération du prompt — formulaire complet
# ─────────────────────────────────────────────

def _build_prompt(bien: BienImmobilier, contact: ContactInfo) -> str:
    type_label = {"appartement": "Appartement", "maison": "Maison", "terrain": "Terrain"}.get(
        bien.type_bien.lower(), "Appartement"
    )
    adresse_complete = bien.adresse
    if bien.code_postal or bien.ville:
        adresse_complete = f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", ")

    equip_actifs = [
        name for name, actif in [
            ("parking", bien.parking), ("garage", bien.garage),
            ("climatisation", bien.climatisation), ("cheminee", bien.cheminee),
            ("piscine", bien.piscine), ("doublevitrage", bien.doublevitrage),
            ("balcon", bien.balcon), ("terrasse", bien.terrasse),
            ("rdc", bien.rdc), ("jardin", bien.jardin),
        ] if actif
    ]

    steps = [
        f"1. Dans le menu déroulant 'type de bien' (select name=\"type\"), sélectionne exactement la valeur \"{bien.type_bien.lower()}\".",
        f"2. Dans le champ surface habitable (input name=\"surface\"), saisis exactement \"{int(bien.surface_habitable or 0)}\" (rien d'autre).",
    ]
    step_n = 3

    if bien.surface_terrain:
        steps.append(f"{step_n}. Dans le champ surface terrain (input name=\"terrain\"), saisis \"{int(bien.surface_terrain)}\".")
        step_n += 1

    pieces = min(bien.nb_pieces or 1, 10)
    chambres = min(bien.nb_chambres or 1, 10)
    steps.append(f"{step_n}. Dans le menu pièces (select name=\"piece\"), sélectionne \"{pieces}\".")
    step_n += 1
    steps.append(f"{step_n}. Dans le menu chambres (select name=\"chambre\"), sélectionne \"{chambres}\".")
    step_n += 1

    etage = bien.etage if bien.etage is not None else 0
    steps.append(f"{step_n}. Dans le champ étage (input name=\"etage\"), saisis \"{etage}\".")
    step_n += 1

    steps.append(
        f"{step_n}. Dans le champ adresse (input id=\"autocomplete\"), tape caractère par caractère "
        f"\"{adresse_complete}\", attends que les suggestions Google apparaissent, puis clique sur la première suggestion."
    )
    step_n += 1

    if equip_actifs:
        steps.append(
            f"{step_n}. Coche uniquement ces équipements (checkboxes name=valeur) : {', '.join(equip_actifs)}. "
            f"Ne coche aucun autre équipement."
        )
        step_n += 1

    if bien.annee_construction:
        steps.append(f"{step_n}. Dans le champ année de construction (input name=\"construction\"), saisis \"{bien.annee_construction}\".")
        step_n += 1

    steps += [
        f"{step_n}. Dans le champ nom (input name=\"nom\"), saisis \"{contact.nom}\".",
        f"{step_n+1}. Dans le champ prénom (input name=\"prenom\"), saisis \"{contact.prenom}\".",
        f"{step_n+2}. Dans le champ téléphone (input name=\"portable\"), saisis \"{contact.portable}\".",
        f"{step_n+3}. Dans le champ email (input name=\"email\"), saisis \"{contact.email}\".",
        f"{step_n+4}. Coche la case RGPD (input name=\"check_box_rgpd\").",
        f"{step_n+5}. Clique sur le bouton de soumission du formulaire (Estimer / Valider / Envoyer).",
        f"{step_n+6}. Attends que la page de résultat s'affiche.",
    ]

    instructions = "\n".join(steps)
    return (
        "Remplis le formulaire d'estimation immobilière sur cette page en suivant EXACTEMENT ces étapes, "
        "dans cet ordre, sans interpréter ni modifier les valeurs :\n\n"
        + instructions
        + "\n\nIMPORTANT : utilise uniquement les valeurs indiquées ci-dessus. "
        "Ne saisit pas d'autres valeurs, ne coche pas d'autres cases."
    )


# ─────────────────────────────────────────────
# Parsing du résultat
# ─────────────────────────────────────────────

def _nettoyer(s: str) -> Optional[float]:
    try:
        return float(re.sub(r'[\s ,]', '', s))
    except Exception:
        return None


def _parser_resultat(output: str, bien: BienImmobilier) -> PrixM2Result:
    result = PrixM2Result(
        localisation=f"{bien.adresse}, {bien.ville} {bien.code_postal}".strip(", "),
        raw_output=output,
    )

    for pat in [
        r'(\d[\d\s ]*)\s*€\s*/\s*m\s*[²2]',
        r'prix\s+au\s+m[²2]\s*[:\*]*\s*(\d[\d\s ]*)\s*€',
    ]:
        for m in re.findall(pat, output, re.IGNORECASE):
            v = _nettoyer(m)
            if v and 500 < v < 50000:
                result.prix_m2_moyen = v
                break
        if result.prix_m2_moyen:
            break

    for pat, label in [
        (r'(?:basse|bas|low)[^\d]{0,30}(\d[\d\s ]{4,})\s*€', 'bas'),
        (r'(?:moyen|moyenne|average)[^\d]{0,30}(\d[\d\s ]{4,})\s*€', 'moyen'),
        (r'(?:haute|haut|high)[^\d]{0,30}(\d[\d\s ]{4,})\s*€', 'haut'),
    ]:
        for m in re.findall(pat, output, re.IGNORECASE):
            v = _nettoyer(m)
            if v and 50000 < v < 100_000_000:
                if label == 'bas':   result.prix_total_bas   = v
                if label == 'moyen': result.prix_total_moyen = v
                if label == 'haut':  result.prix_total_haut  = v
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


# ─────────────────────────────────────────────
# Scraper principal
# ─────────────────────────────────────────────

def get_prix_m2(
    bien: BienImmobilier,
    contact: Optional[ContactInfo] = None,
    api_key: Optional[str] = None,
) -> PrixM2Result:
    fc_key = api_key or os.environ.get("FIRECRAWL_API_KEY")
    if not fc_key:
        raise ValueError("FIRECRAWL_API_KEY manquante.")

    if contact is None:
        contact = ContactInfo()

    app = FirecrawlApp(api_key=fc_key)

    # ── 1. Ouvrir la page ─────────────────────────────────────────
    print("[1/3] Ouverture du formulaire...", flush=True)
    t = time.time()
    scrape = app.scrape(URL, formats=["markdown"])
    scrape_id = scrape.metadata.scrape_id
    print(f"      OK ({time.time()-t:.1f}s) — session: {scrape_id}", flush=True)

    # ── 2. Remplissage via prompt précis ──────────────────────────
    print("[2/3] Remplissage du formulaire...", flush=True)
    t = time.time()
    prompt = _build_prompt(bien, contact)
    r1 = app.interact(scrape_id, prompt=prompt, timeout=180)
    output1 = (getattr(r1, 'output', '') or '').lower()
    print(f"      OK ({time.time()-t:.1f}s) — {output1[:120]}", flush=True)
    if any(k in output1 for k in ('captcha', 'unable to complete', 'cannot complete', 'stuck', 'blocked')):
        raise RuntimeError(f"Formulaire bloqué : {output1[:300]}")

    # ── 3. Extraction du résultat ─────────────────────────────────
    print("[3/3] Extraction du résultat...", flush=True)
    t = time.time()
    r2 = app.interact(
        scrape_id,
        prompt=(
            "Lis les résultats de l'estimation immobilière affichés sur la page. "
            "Donne-moi uniquement les valeurs numériques en euros : "
            "prix au m² moyen, fourchette basse, fourchette haute, prix total estimé si disponible. "
            "Réponds uniquement avec des chiffres et le signe €, rien d'autre."
        ),
    )
    output = getattr(r2, 'output', '') or getattr(r2, 'result', '') or str(r2)
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
        nb_chambres=2,
        etage=4,
        annee_construction=1972,
        balcon=True,
        parking=True,
    )

    contact = ContactInfo(
        nom="Dupont",
        prenom="Jean",
        portable="0600000000",
        email="estimation@example.com",
    )

    result = get_prix_m2(bien, contact=contact)
    print(result)
