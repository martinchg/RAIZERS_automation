"""Second pass : mapping sémantique LLM + résolution arithmétique Python.

Le LLM décide UNIQUEMENT quelles lignes brutes correspondent à quelle feature key.
Il ne calcule aucune valeur. Toute l'arithmétique (sommes) est faite en Python,
ce qui élimine les hallucinations de montants.

Flux :
    first_pass (brut)  +  feature_keys (référentiel)
        → LLM : mapping  {feature_key → [source_labels]}
        → Python : resolve_mapping  {feature_key → {n, n1}}
"""

from __future__ import annotations

import argparse
import json
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


SECTION_NAMES = ("actif", "passif", "compte_resultat")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalize(label: str) -> str:
    """Minuscule + suppression des accents pour matching souple."""
    nfd = unicodedata.normalize("NFD", label.lower().strip())
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def _build_lookup(rows: List[Dict[str, Any]]) -> Tuple[Dict, Dict]:
    """Retourne (lookup_exact, lookup_norm) label → (n, n1)."""
    exact: Dict[str, Tuple] = {}
    norm: Dict[str, Tuple] = {}
    for row in rows:
        label = str(row.get("l") or "").strip()
        if label:
            val = (row.get("n"), row.get("n1"))
            exact[label] = val
            norm[_normalize(label)] = val
    return exact, norm


def _find_row(source: str, exact: Dict, norm: Dict) -> Tuple[Optional[float], Optional[float]]:
    if source in exact:
        return exact[source]
    n = _normalize(source)
    if n in norm:
        return norm[n]
    return (None, None)


# ── Chargement des feature keys ───────────────────────────────────────────────

def _extract_revealing_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("tableau_revelateur"), dict):
        return payload["tableau_revelateur"]
    return payload


def load_feature_keys(revealing_json_path: Path) -> Dict[str, List[str]]:
    payload = json.loads(revealing_json_path.read_text(encoding="utf-8"))
    revealing = _extract_revealing_payload(payload)

    result: Dict[str, List[str]] = {}
    for section in SECTION_NAMES:
        labels: List[str] = []
        for row in revealing.get(section) or []:
            if not isinstance(row, dict):
                continue
            label = str(row.get("label") or "").strip()
            if label and label not in labels:
                labels.append(label)
        result[section] = labels
    return result


# ── Prompt mapping ────────────────────────────────────────────────────────────

def build_mapping_prompt(
    first_pass_payload: Dict[str, Any],
    feature_keys: Dict[str, List[str]],
) -> str:
    """Prompt qui demande au LLM UNIQUEMENT de mapper labels bruts → feature keys.
    Format dict : {feature_key_label: [source_labels]}.
    Aucune valeur numérique dans la réponse attendue.
    """
    # Squelette JSON pré-rempli avec les feature keys comme clés (valeurs vides)
    skeleton: Dict[str, Any] = {}
    for section in SECTION_NAMES:
        labels = feature_keys.get(section) or []
        if labels:
            skeleton[section] = {label: [] for label in labels}
        else:
            skeleton[section] = {}

    # Labels disponibles par section
    avail_blocks = []
    for section in SECTION_NAMES:
        rows = first_pass_payload.get(section) or []
        labels = [str(r.get("l") or "").strip() for r in rows if r.get("l")]
        if labels:
            avail_blocks.append(f"{section}: {json.dumps(labels, ensure_ascii=False)}")
    avail_block = "\n".join(avail_blocks)

    return f"""Tu reçois l'extraction comptable brute d'un PDF financier.

TON UNIQUE RÔLE : pour chaque feature key du squelette ci-dessous, indiquer quels labels bruts lui correspondent.
Tu ne calcules AUCUNE somme. Tu n'écris AUCUN chiffre. Tu ne touches à AUCUNE valeur numérique.

═══ Labels bruts disponibles par section ═══
{avail_block}

═══ Règles ═══
- Complète le squelette JSON : chaque clé est une feature key, sa valeur est la liste des labels bruts qui lui correspondent.
- Les labels dans les listes doivent être copiés MOT POUR MOT depuis "Labels bruts disponibles".
- Un label brut ne peut apparaître que dans UNE SEULE feature key (pas de doublon).
- Travaille section par section : les labels bruts de "actif" n'alimentent que les feature keys de "actif", etc.
- Si une ligne brute de total correspond directement à une feature key de total → utilise-la seule (pas les sous-lignes en plus).
- Si tu dois sommer des sous-lignes (pas de sous-total exact disponible), inclus-les toutes : une fois ta liste constituée, relis les labels bruts disponibles pour cette section et demande-toi pour chacun "est-ce que ce label appartient à cette famille ?" — si oui et qu'il n'est pas déjà dans une autre feature key, ajoute-le. Ne l'exclus pas parce que son libellé ressemble à la feature key elle-même (ex. "Emprunts et dettes financières diverses" fait bien partie de "Emprunts et dettes financières").
- Si aucun label brut ne correspond clairement à une feature key → laisse la liste vide [].
- Les sections sans feature keys (dict vide {{}}) → laisser {{}}.
- INTERDIT : écrire un nombre, inventer un label, modifier une clé du squelette.

═══ Squelette à compléter (JSON strict, aucun texte autour) ═══
{json.dumps(skeleton, ensure_ascii=False, indent=2)}
"""


# ── Résolution arithmétique ───────────────────────────────────────────────────

def resolve_mapping(
    mapping: Dict[str, Any],
    first_pass_payload: Dict[str, Any],
    feature_keys: Dict[str, List[str]],
) -> Dict[str, Any]:
    """Applique le mapping LLM sur les vraies valeurs du first pass.

    Le mapping est un dict {section: {feature_key_label: [source_labels]}}.

    - Sections sans feature keys : pass-through des lignes brutes du first pass.
    - Sections avec feature keys : somme exacte des n/n1 des source_labels.
    - Source label non trouvé : ignoré (n reste None si aucune source trouvée).
    """
    result: Dict[str, Any] = {}

    for section in SECTION_NAMES:
        raw_rows = first_pass_payload.get(section) or []
        fk_list = feature_keys.get(section) or []

        # Pas de feature keys → pass-through brut (ex. compte_resultat sans Pappers)
        if not fk_list:
            result[section] = [
                {
                    "label": str(row.get("l") or "").strip(),
                    "n":     row.get("n"),
                    "n1":    row.get("n1"),
                }
                for row in raw_rows
                if str(row.get("l") or "").strip()
            ]
            continue

        exact, norm = _build_lookup(raw_rows)
        section_mapping: Dict[str, List[str]] = mapping.get(section) or {}

        section_result = []
        for fk_label in fk_list:
            sources: List[str] = section_mapping.get(fk_label) or []

            n_vals: List[float] = []
            n1_vals: List[float] = []
            for source in sources:
                n_val, n1_val = _find_row(source, exact, norm)
                if n_val is not None:
                    n_vals.append(n_val)
                if n1_val is not None:
                    n1_vals.append(n1_val)

            section_result.append({
                "label": fk_label,
                "n":     sum(n_vals)  if n_vals  else None,
                "n1":    sum(n1_vals) if n1_vals else None,
            })

        result[section] = section_result

    return result


# ── CLI (debug) ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Affiche le prompt de mapping second pass")
    parser.add_argument("--input", required=True, help="JSON first pass brut")
    parser.add_argument("--revealing-json", required=True, help="JSON révélateur Pappers")
    args = parser.parse_args()

    first_pass = json.loads(Path(args.input).read_text(encoding="utf-8"))
    if isinstance(first_pass.get("parsed"), dict):
        first_pass = first_pass["parsed"]

    feature_keys = load_feature_keys(Path(args.revealing_json))
    print(build_mapping_prompt(first_pass, feature_keys))


if __name__ == "__main__":
    main()
