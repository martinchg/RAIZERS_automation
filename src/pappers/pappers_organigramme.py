import json
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

import requests
import graphviz

PAPPERS_API_KEY = os.environ.get("PAPPERS_API_KEY", "")
PAPPERS_BASE_URL = "https://api.pappers.fr/v2"


# ─── Helpers noms ─────────────────────────────────────────────────────────────

def _normalize_prenom(prenom: str) -> str:
    """Garde uniquement le premier prénom (avant virgule)."""
    return prenom.split(",")[0].strip() if prenom else ""


def _person_id(nom: str, prenom: str) -> str:
    return f"per_{nom.strip().lower()}_{_normalize_prenom(prenom).lower()}"


def _person_label(prenom: str, nom: str, prenom_usuel: str = "") -> str:
    p = prenom_usuel or _normalize_prenom(prenom)
    return f"{p} {nom}".strip()


FORME_ABREV = {
    "société par actions simplifiée": "SAS",
    "société par actions simplifiée unipersonnelle": "SASU",
    "société à responsabilité limitée": "SARL",
    "société à responsabilité limitée unipersonnelle": "EURL",
    "société anonyme": "SA",
    "société civile immobilière": "SCI",
    "société civile": "SC",
    "société en nom collectif": "SNC",
    "société en commandite simple": "SCS",
    "société en commandite par actions": "SCA",
    "groupement d'intérêt économique": "GIE",
    "établissement public": "EP",
    "association": "Asso.",
}

def _abrev_forme(forme: str) -> str:
    """Retourne l'abréviation de la forme juridique."""
    if not forme:
        return ""
    f = forme.lower()
    for key, abrev in FORME_ABREV.items():
        if key in f:
            return abrev
    # Fallback : première partie avant la virgule
    return forme.split(",")[0].strip()


# ─── API helpers ──────────────────────────────────────────────────────────────

def _get(endpoint: str, params: dict) -> dict:
    params["api_token"] = PAPPERS_API_KEY
    r = requests.get(f"{PAPPERS_BASE_URL}/{endpoint}", params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def fetch_entreprise(siren: str) -> dict:
    return _get("entreprise", {"siren": siren})


def fetch_dirigeant(nom: str, prenom: str, date_naissance: str) -> dict:
    return _get("dirigeant", {
        "nom": nom,
        "prenom": prenom,
        "date_de_naissance": date_naissance,
    })


# ─── Lecture SIREN depuis pipeline ───────────────────────────────────────────

def get_siren_from_pipeline(extraction_json_path: str) -> dict:
    """
    Lit un fichier extraction_results.json et retourne:
    {
        siren: str,              # SIREN à utiliser pour l'organigramme
        role_emprunt: bool,      # société est emprunteuse
        role_operation: bool,    # société porte l'opération
        siren_emprunt: str,
        siren_operation: str,
    }
    """
    with open(extraction_json_path, encoding="utf-8") as f:
        data = json.load(f)

    results = data.get("results", data)

    def clean_siren(s):
        return str(s).replace(" ", "").strip() if s else ""

    siren_emprunt   = clean_siren(results.get("siren_emprunt", ""))
    siren_operation = clean_siren(results.get("siren_operation", ""))

    # Logique de sélection
    if siren_operation and siren_operation != siren_emprunt:
        siren = siren_operation
        role_emprunt   = False
        role_operation = True
    elif siren_emprunt:
        siren = siren_emprunt
        role_emprunt   = True
        role_operation = (siren_operation == siren_emprunt or not siren_operation)
    else:
        raise ValueError("Aucun SIREN trouvé dans le fichier d'extraction")

    return {
        "siren":           siren,
        "role_emprunt":    role_emprunt,
        "role_operation":  role_operation,
        "siren_emprunt":   siren_emprunt,
        "siren_operation": siren_operation,
    }


# ─── Collecte niveau 0 + 1 ───────────────────────────────────────────────────

def build_network(siren: str, roles: dict = None) -> dict:
    nodes = {}
    edges = []
    visited_sirens = set()

    print(f"[0] Fetching entreprise {siren}...")
    data = fetch_entreprise(siren)

    nom_entreprise  = data.get("denomination") or data.get("nom_entreprise", siren)
    forme           = data.get("forme_juridique", "")
    forme_abrev     = _abrev_forme(forme)
    siege           = data.get("siege", {})
    capital_raw     = data.get("capital") or 0
    capital_formate = data.get("capital_formate", "")

    # Tags de rôle
    role_tags = []
    if roles:
        if roles.get("role_emprunt") and roles.get("role_operation"):
            role_tags.append("Emprunteuse &amp; Porteuse de l&#39;op&#233;ration")
        elif roles.get("role_emprunt"):
            role_tags.append("Soci&#233;t&#233; emprunteuse")
        elif roles.get("role_operation"):
            role_tags.append("Porteuse de l&#39;op&#233;ration")

    main_id = f"soc_{siren}"
    nodes[main_id] = {
        "label":           nom_entreprise,
        "capital_raw":     capital_raw,
        "capital_formate": capital_formate,
        "sub3":            "",
        "role_tags":       role_tags,
        "type":            "main",
        "rank":            0,
    }
    visited_sirens.add(siren)

    # ── Niveau 1a : représentants ──
    for rep in data.get("representants", []):
        if rep.get("personne_morale"):
            rep_siren = rep.get("siren", "")
            if rep_siren and rep_siren not in visited_sirens:
                _add_societe_liee(rep_siren, main_id, rep.get("qualite", "Lié"),
                                  nodes, edges, visited_sirens)
            continue

        nom          = rep.get("nom", "")
        prenom       = rep.get("prenom", "")
        prenom_usuel = rep.get("prenom_usuel", "")
        qualite      = rep.get("qualite", "Dirigeant")
        ddn          = rep.get("date_de_naissance", "")

        person_id  = _person_id(nom, prenom)
        nb_mandats = 1
        procedure  = False

        if nom and prenom and ddn:
            time.sleep(0.2)
            print(f"  [1] Fetching dirigeant {_normalize_prenom(prenom)} {nom}...")
            try:
                ddata = fetch_dirigeant(nom, _normalize_prenom(prenom), ddn)
                nb_mandats = ddata.get("nb_mandats_dirigeant", 1)
                procedure  = ddata.get("nb_procedures_collectives", 0) > 0
                for ent in ddata.get("entreprises", []):
                    ent_siren = ent.get("siren", "")
                    if ent_siren and ent_siren != siren and ent_siren not in visited_sirens:
                        if not ent.get("entreprise_cessee"):
                            _add_societe_liee(
                                ent_siren, person_id,
                                ent.get("qualites", ["Dirigeant"])[0],
                                nodes, edges, visited_sirens,
                                via_person=True
                            )
            except Exception as e:
                print(f"    ⚠ Erreur dirigeant {_normalize_prenom(prenom)} {nom}: {e}")

        nodes[person_id] = {
            "label": _person_label(prenom, nom, prenom_usuel),
            "type":  "person_alert" if procedure else "person",
            "rank":  1,
        }
        edges.append((main_id, person_id, qualite, False))

    # ── Niveau 1c : prédécesseurs du siège ──
    for pred in siege.get("predecesseurs", []):
        pred_siren = pred.get("siret", "")[:9]
        if pred_siren and pred_siren not in visited_sirens:
            print(f"  [1] Fetching prédécesseur {pred_siren}...")
            time.sleep(0.2)
            _add_societe_liee(pred_siren, main_id, "Prédécesseur",
                              nodes, edges, visited_sirens, dashed=True)

    return {"nodes": nodes, "edges": edges}


def _add_societe_liee(siren: str, linked_to_id: str, relation: str,
                      nodes: dict, edges: list, visited_sirens: set,
                      dashed: bool = False, via_person: bool = False):
    try:
        time.sleep(0.2)
        d = fetch_entreprise(siren)
        visited_sirens.add(siren)

        nom             = d.get("denomination") or d.get("nom_entreprise", siren)
        forme           = d.get("forme_juridique", "")
        forme_abrev     = _abrev_forme(forme)
        capital_raw     = d.get("capital") or 0
        capital_formate = d.get("capital_formate", "")
        cessee          = d.get("entreprise_cessee", False)
        node_rank       = 2 if via_person else 1

        node_id = f"soc_{siren}"
        nodes[node_id] = {
            "label":           nom,
            "sub3":            "CESSÉE" if cessee else "",
            "capital_raw":     capital_raw,
            "capital_formate": capital_formate,
            "type":            "company_ceased" if cessee else "company",
            "rank":            node_rank,
        }

        if via_person:
            edges.append((linked_to_id, node_id, relation, False))
        else:
            edges.append((linked_to_id, node_id, relation, dashed))

        # Dirigeants de la société liée
        for rep in d.get("representants", []):
            if rep.get("personne_morale"):
                continue
            nom_r          = rep.get("nom", "")
            prenom_r       = rep.get("prenom", "")
            prenom_usuel_r = rep.get("prenom_usuel", "")
            qualite_r      = rep.get("qualite", "Dirigeant")

            person_id = _person_id(nom_r, prenom_r)
            if person_id not in nodes:
                nodes[person_id] = {
                    "label": _person_label(prenom_r, nom_r, prenom_usuel_r),
                    "type":  "person",
                    "rank":  node_rank + 1,
                }
            edges.append((node_id, person_id, qualite_r, False))

    except Exception as e:
        print(f"    ⚠ Erreur société {siren}: {e}")


# ─── Génération Graphviz ──────────────────────────────────────────────────────

COLORS = {
    "main":           {"fill": "#4DA6FF", "border": "#1A7FE0", "text": "white",   "sub2text": "#C0DFFF"},
    "company":        {"fill": "#EBF4FF", "border": "#93C5FD", "text": "#1E3A5F", "sub2text": "#64748B"},
    "company_ceased": {"fill": "#F3F4F6", "border": "#D1D5DB", "text": "#6B7280", "sub2text": "#9CA3AF"},
    "person":         {"fill": "white",   "border": "#93C5FD", "text": "#1E293B", "sub2text": "#94A3B8"},
    "person_alert":   {"fill": "#FEF2F2", "border": "#F87171", "text": "#7F1D1D", "sub2text": "#EF4444"},
}


def _node_label(node: dict) -> str:
    c = COLORS[node["type"]]
    is_company = node["type"] in ("main", "company", "company_ceased")
    size_title = "15" if node["type"] == "main" else "13"

    rows = [f'<TR><TD><FONT FACE="Helvetica Neue Bold" POINT-SIZE="{size_title}" COLOR="{c["text"]}">{node["label"]}</FONT></TD></TR>']

    if is_company:
        # Tags de rôle — mis en avant avec couleur blanche bold
        for tag in node.get("role_tags", []):
            tag_color = "white" if node["type"] == "main" else "#1D4ED8"
            rows.append(f'<TR><TD><FONT FACE="Helvetica Neue Bold" POINT-SIZE="8" COLOR="{tag_color}"><I>{tag}</I></FONT></TD></TR>')
        # Capital
        capital_raw = node.get("capital_raw", 0)
        sub3 = node.get("sub3", "")
        if "CESSÉE" in sub3:
            rows.append(f'<TR><TD><FONT FACE="Helvetica Neue" POINT-SIZE="8" COLOR="{c["sub2text"]}">Soci&#233;t&#233; cess&#233;e</FONT></TD></TR>')
        elif capital_raw and capital_raw > 0:
            capital_str = node.get("capital_formate", "")
            rows.append(f'<TR><TD><FONT FACE="Helvetica Neue" POINT-SIZE="8" COLOR="{c["sub2text"]}">Capital : {capital_str}</FONT></TD></TR>')

    return f'<<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="2">{"".join(rows)}</TABLE>>'


def generate_graph(network: dict, output_path: str) -> str:
    g = graphviz.Digraph("organigramme", format="png")
    g.attr(
        rankdir="TB",
        bgcolor="transparent",
        fontname="Helvetica Neue",
        pad="1.0",
        splines="curved",
        concentrate="true",
        nodesep="0.8",
        ranksep="1.1",
        dpi="200",
    )
    g.attr("edge",
        fontname="Helvetica Neue",
        fontsize="9",
        fontcolor="#94A3B8",
        color="#BFDBFE",
        arrowsize="0.6",
        penwidth="1.5",
        arrowhead="vee",
    )

    nodes = network["nodes"]
    edges = network["edges"]

    # Dédupliquer les edges pour éviter les labels en double
    seen_edges = set()
    deduped_edges = []
    for src, dst, label, dashed in edges:
        key = (src, dst)
        if key not in seen_edges:
            seen_edges.add(key)
            deduped_edges.append((src, dst, label, dashed))

    # Nœuds
    for node_id, node in nodes.items():
        c = COLORS[node["type"]]
        g.node(node_id,
            label=_node_label(node),
            shape="rectangle",
            style="filled,rounded",
            fillcolor=c["fill"],
            color=c["border"],
            penwidth="1.5",
            margin="0.1",
        )

    # Subgraphs par rang
    ranks = defaultdict(list)
    for node_id, node in nodes.items():
        ranks[node.get("rank", 1)].append(node_id)
    for rank_level in sorted(ranks.keys()):
        with g.subgraph() as s:
            s.attr(rank="same")
            for node_id in ranks[rank_level]:
                s.node(node_id)

    # Edges
    for src, dst, label, dashed in deduped_edges:
        if src not in nodes or dst not in nodes:
            continue
        g.edge(src, dst,
            xlabel=f"  {label}  ",
            style="dashed" if dashed else "solid",
            color="#94A3B8" if dashed else "#BFDBFE",
            fontcolor="#94A3B8",
        )

    out = g.render(output_path, cleanup=True)
    print(f"PNG généré : {out}")
    return out


# ─── Point d'entrée ──────────────────────────────────────────────────────────

def organigramme(siren: str = None, output_path: str = None,
                 extraction_json: str = None, roles: dict = None) -> str:
    from dotenv import load_dotenv
    load_dotenv()

    global PAPPERS_API_KEY
    PAPPERS_API_KEY = os.environ.get("PAPPERS_API_KEY", PAPPERS_API_KEY)

    # Lecture depuis pipeline si fourni
    if extraction_json:
        info   = get_siren_from_pipeline(extraction_json)
        siren  = info["siren"]
        roles  = info
        print(f"SIREN détecté depuis pipeline : {siren} "
              f"({'emprunteuse' if info['role_emprunt'] else ''}"
              f"{'+ ' if info['role_emprunt'] and info['role_operation'] else ''}"
              f"{'opération' if info['role_operation'] else ''})")

    if not siren:
        raise ValueError("Fournir un SIREN ou un fichier extraction_json")

    if not output_path:
        output_path = f"organigramme_{siren}"

    network = build_network(siren, roles=roles)
    return generate_graph(network, output_path)


if __name__ == "__main__":
    import sys
    # Usage :
    #   python3 -m src.pappers.pappers_organigramme 885189985
    #   python3 -m src.pappers.pappers_organigramme --json path/to/extraction_results.json
    args = sys.argv[1:]
    if args and args[0] == "--json":
        json_path   = args[1]
        output_path = args[2] if len(args) > 2 else None
        organigramme(extraction_json=json_path, output_path=output_path)
    else:
        siren       = args[0] if args else "885189985"
        output_path = args[1] if len(args) > 1 else None
        organigramme(siren=siren, output_path=output_path)
