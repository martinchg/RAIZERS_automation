"""
financial_mapping.py : Mapping postes bruts → clés canoniques Excel,
validation comptable et scoring qualité des tables financières.

Utilisé par extract_structured.py pour normaliser les réponses LLM
avant écriture dans l'Excel.
"""

import json
import logging
from typing import Dict, List, Optional

from core.normalization import canonical_name

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers champs avec suffixe (__0, __1…)
# ---------------------------------------------------------------------------

def split_field_suffix(field_id: str) -> tuple[str, str]:
    if "__" in field_id:
        base, suffix = field_id.split("__", 1)
        return base, f"__{suffix}"
    return field_id, ""


def append_validation_error(all_errors: Dict[str, List[str]], field_id: str, message: str) -> None:
    all_errors.setdefault(field_id, []).append(message)


# ---------------------------------------------------------------------------
# Validation post-LLM des tables financières
# ---------------------------------------------------------------------------

_VALIDATION_TOLERANCE = 0.05

_ACTIF_DETAIL_KEYS = [
    "immobilisations_corporelles", "immobilisations_financieres",
    "stocks", "creances_clients", "autres_creances",
    "disponibilites", "vmp", "charges_constatees_avance",
    "autres_actif_residuel",
]

_PASSIF_DETAIL_KEYS = [
    "capitaux_propres", "dettes_bancaires", "autres_dettes_financieres",
    "fournisseurs", "dettes_fiscales_sociales", "dettes_diverses",
    "provisions_pour_risques", "provisions_pour_charges",
    "produits_constates_avance", "autres_passif_residuel",
]

_CDR_CHARGES_KEYS = [
    "achats_marchandises", "variation_stock_marchandises",
    "achats_matieres_premieres", "variation_stock_matieres_premieres",
    "autres_charges_externes", "salaires", "charges_sociales",
    "impots_taxes", "dotations_amortissements", "dotations_provisions",
    "autres_charges_exploitation",
]


# ---------------------------------------------------------------------------
# Mapping poste brut (LLM) → clé canonique (Excel)
# ---------------------------------------------------------------------------
# Chaque entrée : normalized_poste → (canonical_key, is_total)
# is_total=True  → sous-total ou total de section, préféré
# is_total=False → ligne de détail, sommée si pas de total
# canonical_key=None → ligne à ignorer (en-tête de section)

def _normalize_poste(poste: str) -> str:
    return canonical_name(poste).replace(" ", "")


_ACTIF_POSTE_MAP = {
    # --- Immobilisations incorporelles → autres_actif_residuel ---
    "immobilisationsincorporelles": ("autres_actif_residuel", True),
    "totalimmobilisationsincorporelles": ("autres_actif_residuel", True),
    "fraisdestablissement": ("autres_actif_residuel", False),
    "fraisderechercheetdeveloppement": ("autres_actif_residuel", False),
    "concessionsbrevetslicencesmarquesprocedes": ("autres_actif_residuel", False),
    "fondscommercial": ("autres_actif_residuel", False),

    # --- Immobilisations corporelles ---
    "immobilisationscorporelles": ("immobilisations_corporelles", True),
    "totalimmobilisationscorporelles": ("immobilisations_corporelles", True),
    "terrains": ("immobilisations_corporelles", False),
    "constructions": ("immobilisations_corporelles", False),
    "installationstechniquesmaterieletoutillageindustriels": ("immobilisations_corporelles", False),
    "installationstechniques": ("immobilisations_corporelles", False),
    "materieletoutillage": ("immobilisations_corporelles", False),
    "materiel": ("immobilisations_corporelles", False),
    "autresimmobilisationscorporelles": ("immobilisations_corporelles", False),
    "immobilisationscorporellesencours": ("immobilisations_corporelles", False),
    "avancesetacomptessurimmobilisationscorporelles": ("immobilisations_corporelles", False),

    # --- Immobilisations financières ---
    "immobilisationsfinancieres": ("immobilisations_financieres", True),
    "totalimmobilisationsfinancieres": ("immobilisations_financieres", True),
    "participations": ("immobilisations_financieres", False),
    "autresparticipations": ("immobilisations_financieres", False),
    "participationsevalueesparmiseenequivalence": ("immobilisations_financieres", False),
    "creancesrattacheesadesparticipations": ("immobilisations_financieres", False),
    "autrestitresimmobilises": ("immobilisations_financieres", False),
    "prets": ("immobilisations_financieres", False),
    "autresimmobilisationsfinancieres": ("immobilisations_financieres", False),

    # --- Stocks ---
    "stocks": ("stocks", True),
    "stocksetencours": ("stocks", True),
    "totalstocksetencours": ("stocks", True),
    "matierespremieresetautresapprovisionnements": ("stocks", False),
    "matierespremieres": ("stocks", False),
    "encoursdeproduction": ("stocks", False),
    "encoursdeproductiondebiens": ("stocks", False),
    "encoursdeproductiondeservices": ("stocks", False),
    "produitsintermediairesetfinis": ("stocks", False),
    "marchandises": ("stocks", False),

    # --- Créances ---
    "creances": ("creances", True),
    "totalcreances": ("creances", True),
    "creancesclients": ("creances_clients", True),
    "clientsetcomptesrattaches": ("creances_clients", True),
    "clients": ("creances_clients", True),
    "autrescreances": ("autres_creances", True),
    "personnel": ("autres_creances", False),
    "etatetautrescollectivitespubliques": ("autres_creances", False),
    "securitesocialeetautresorganismessociaux": ("autres_creances", False),
    "impotssurlesbenefices": ("autres_creances", False),
    "taxesurlavaleurajoutee": ("autres_creances", False),
    "autrescreancesdiverses": ("autres_creances", False),
    "fournisseursdebiteursavancesetacomptes": ("autres_creances", False),
    "capitalsouscritappelenonverse": ("autres_actif_residuel", False),

    # --- Trésorerie ---
    "tresorerie": ("tresorerie", True),
    "disponibilites": ("disponibilites", True),
    "valeursmobilieresdeplacement": ("vmp", True),
    "vmp": ("vmp", True),

    # --- Divers ---
    "chargesconstateesdavance": ("charges_constatees_avance", True),
    "capitalsouscritnonappele": ("autres_actif_residuel", True),
    "avancesetacomptesversessurcommandes": ("autres_actif_residuel", True),

    # --- Totaux de section à ignorer ---
    "actifimmobilise": (None, True),
    "totalactifimmobilise": (None, True),
    "actifcirculant": (None, True),
    "totalactifcirculant": (None, True),

    # --- Total général ---
    "totalactif": ("total_actif", True),
    "totalgeneralactif": ("total_actif", True),
    "totalgeneral": ("total_actif", True),
}

_PASSIF_POSTE_MAP = {
    # --- Capitaux propres ---
    "capitalsocial": ("capital_social", True),
    "capitalindividuel": ("capital_social", True),
    "capital": ("capital_social", True),
    "primesdemission": ("capitaux_propres_detail", False),
    "primesdapport": ("capitaux_propres_detail", False),
    "primesdefusionscission": ("capitaux_propres_detail", False),
    "ecartsderevaluation": ("capitaux_propres_detail", False),
    "reservelegale": ("capitaux_propres_detail", False),
    "reservesstatutairesetcontractuelles": ("capitaux_propres_detail", False),
    "reservesreglementees": ("capitaux_propres_detail", False),
    "autresreserves": ("capitaux_propres_detail", False),
    "reserves": ("capitaux_propres_detail", False),
    "reportanouveau": ("capitaux_propres_detail", False),
    "resultatdelexercice": ("resultat_exercice", True),
    "resultatdeexercice": ("resultat_exercice", True),
    "resultatexercice": ("resultat_exercice", True),
    "benefice": ("resultat_exercice", True),
    "perte": ("resultat_exercice", True),
    "resultat": ("resultat_exercice", True),
    "resultatnet": ("resultat_exercice", True),
    "subventionsdinvestissement": ("capitaux_propres_detail", False),
    "provisionsreglementees": ("capitaux_propres_detail", False),
    "capitauxpropres": ("capitaux_propres", True),
    "totalcapitauxpropres": ("capitaux_propres", True),

    # --- Provisions ---
    "provisionspourrisques": ("provisions_pour_risques", True),
    "provisionspourcharges": ("provisions_pour_charges", True),
    "provisions": ("provisions_pour_risques", True),
    "totalprovisions": ("provisions_pour_risques", True),
    "provisionspourrisquesetcharges": ("provisions_pour_risques", True),

    # --- Dettes financières ---
    "empruntsetdettesaupresdesetablissementsdecredit": ("dettes_bancaires", True),
    "empruntsbancaires": ("dettes_bancaires", True),
    "dettesbancaires": ("dettes_bancaires", True),
    "empruntsaupresdesetablissementsdecredit": ("dettes_bancaires", True),
    "empruntsobligatairesconvertibles": ("autres_dettes_financieres", False),
    "empruntsobligataires": ("autres_dettes_financieres", False),
    "autresempruntsobligataires": ("autres_dettes_financieres", False),
    "comptescourantsdassocies": ("autres_dettes_financieres", True),
    "comptescourantsassocies": ("autres_dettes_financieres", True),
    "comptescourants": ("autres_dettes_financieres", True),
    "cca": ("autres_dettes_financieres", True),
    "autresdettesfinancieres": ("autres_dettes_financieres", True),
    "empruntsetdettesfinancieresdivers": ("autres_dettes_financieres", True),
    "empruntsetdettesfinancieresdiverses": ("autres_dettes_financieres", True),
    "dettesfinancieres": ("dettes_financieres", True),
    "totaldettesfinancieres": ("dettes_financieres", True),

    # --- Dettes d'exploitation ---
    "fournisseurs": ("fournisseurs", True),
    "fournisseursetcomptesrattaches": ("fournisseurs", True),
    "dettesfournisseurs": ("fournisseurs", True),
    "dettesfiscalesetsociales": ("dettes_fiscales_sociales", True),
    "dettesfiscalessociales": ("dettes_fiscales_sociales", True),
    "dettessociales": ("dettes_fiscales_sociales", False),
    "dettesfiscales": ("dettes_fiscales_sociales", False),
    "dettesexploitation": ("dettes_exploitation", True),
    "totaldettesdexploitation": ("dettes_exploitation", True),

    # --- Dettes diverses ---
    "dettessurimmobilisations": ("autres_passif_residuel", True),
    "dettessurimmobilisationsetcomptesrattaches": ("autres_passif_residuel", True),
    "autresdettes": ("dettes_diverses", True),
    "dettesdiverses": ("dettes_diverses", True),

    # --- Produits constatés d'avance ---
    "produitsconstatesdavance": ("produits_constates_avance", True),

    # --- Totaux de section à ignorer ---
    "dettes": (None, True),
    "totaldettes": (None, True),
    "totaldettesdivers": (None, True),

    # --- Total général ---
    "totalpassif": ("total_passif", True),
    "totalgeneralpassif": ("total_passif", True),
    "totalgeneral": ("total_passif", True),
}

_CDR_POSTE_MAP = {
    # --- Produits d'exploitation ---
    "ventesdemarchandises": ("chiffre_affaires", False),
    "productionvenduedebiens": ("chiffre_affaires", False),
    "productionvenduedeservices": ("chiffre_affaires", False),
    "productionvendue": ("chiffre_affaires", False),
    "chiffredaffaires": ("chiffre_affaires", True),
    "chiffredaffairesnet": ("chiffre_affaires", True),
    "montantnetduchiffredaffaires": ("chiffre_affaires", True),
    "ca": ("chiffre_affaires", True),

    "productionstockee": ("production_stockee", True),
    "productionimmobilisee": ("production_stockee", False),

    "subventionsdexploitation": ("subventions_exploitation", True),
    "subventionsdexploitationrecues": ("subventions_exploitation", True),

    "reprisesuramortissementsetprovisionstransfertdecharges": ("reprises_exploitation", True),
    "reprisesuramortissementsetprovisionstransfertsdecharges": ("reprises_exploitation", True),
    "reprisesuramortissementsetprovisions": ("reprises_exploitation", True),
    "reprisessurdepreciationsetprovisions": ("reprises_exploitation", True),
    "transfertsdecharges": ("reprises_exploitation", False),

    "autresproduits": ("autres_produits_exploitation", True),
    "autresproduitsdexploitation": ("autres_produits_exploitation", True),
    "autresproduitsexploitation": ("autres_produits_exploitation", True),

    # --- Charges d'exploitation ---
    "achatsdemarchandises": ("achats_marchandises", True),
    "achatsmarchandises": ("achats_marchandises", True),
    "variationdestockdemarchandises": ("variation_stock_marchandises", True),
    "variationdesstocksdemarchandises": ("variation_stock_marchandises", True),
    "variationdestockmarchandises": ("variation_stock_marchandises", True),

    "achatsdematieresetautresapprovisionnements": ("achats_matieres_premieres", True),
    "achatsdematierespremieres": ("achats_matieres_premieres", True),
    "achatsdematiespremieresetautresapprovisionnements": ("achats_matieres_premieres", True),
    "achatsetapprovisionnements": ("autres_charges_externes", True),
    "variationdestockdematierespremieresetapprovisionnements": ("variation_stock_matieres_premieres", True),
    "variationdesstocksdematierespremieresetapprovisionnements": ("variation_stock_matieres_premieres", True),
    "variationdestockmatierespremieres": ("variation_stock_matieres_premieres", True),

    "autresachatsetchargesexternes": ("autres_charges_externes", True),
    "autresachatschargesexternes": ("autres_charges_externes", True),
    "chargesexternes": ("autres_charges_externes", True),
    "achatsetchargesexternes": ("autres_charges_externes", True),

    "salairesettraitements": ("salaires", True),
    "salaires": ("salaires", True),
    "remunerationsdupersonnel": ("salaires", True),

    "chargessociales": ("charges_sociales", True),
    "chargessocialesdupersonnel": ("charges_sociales", True),

    "impotstaxesetversementsassimiles": ("impots_taxes", True),
    "impotsettaxes": ("impots_taxes", True),
    "impotsetversementsassimiles": ("impots_taxes", True),

    "dotationsauxamortissementssurimmobilisations": ("dotations_amortissements", True),
    "dotationsauxamortissements": ("dotations_amortissements", True),
    "dotationsamortissements": ("dotations_amortissements", True),
    "dotationsauxprovisions": ("dotations_provisions", True),
    "dotationsauxdepreciations": ("dotations_provisions", True),
    "dotationsauxamortissementsetprovisions": ("dotations", True),
    "dotationsauxamortissementsdepreciationsetprovisions": ("dotations", True),
    "dotationsdexploitation": ("dotations", True),
    "dotations": ("dotations", True),

    "autrescharges": ("autres_charges_exploitation", True),
    "autreschargesdexploitation": ("autres_charges_exploitation", True),
    "autreschargesexploitation": ("autres_charges_exploitation", True),

    # --- Totaux et résultats ---
    "totalchargesdexploitation": ("charges", True),
    "chargesdexploitation": ("charges", True),
    "totaldeschargesdexploitation": ("charges", True),
    "totalproduitsdexploitation": (None, True),
    "produitsdexploitation": (None, True),

    "resultatdexploitation": ("resultat_exploitation", True),
    "resultatexploitation": ("resultat_exploitation", True),
    "operationsencommun": ("operations_en_commun", True),
    "resultatcourantavantimpots": ("resultat_courant_avant_impots", True),

    # --- Résultat financier ---
    "produitsfinanciers": (None, True),
    "chargesfinancieres": (None, True),
    "totaldesproduitsfinanciers": (None, True),
    "totaldeschargesfinancieres": (None, True),
    "resultatfinancier": ("resultat_financier", True),

    # --- Résultat exceptionnel ---
    "produitsexceptionnels": (None, True),
    "chargesexceptionnelles": (None, True),
    "totaldesproduitsexceptionnels": (None, True),
    "totaldeschargesexceptionnelles": (None, True),
    "resultatexceptionnel": ("resultat_exceptionnel", True),

    # --- Impôts ---
    "impotssurlesbenefices": ("impots_sur_les_societes", True),
    "impotssurlessocietes": ("impots_sur_les_societes", True),
    "is": ("impots_sur_les_societes", True),
    "participationdessalariesauxresultats": ("participation_salaries", True),
    "participationdessalariesauxresultatsdelentreprise": ("participation_salaries", True),

    # --- Totaux globaux à ignorer ---
    "totaldesproduits": (None, True),
    "totaldescharges": (None, True),
    "resultatnet": ("resultat_net", True),
    "beneficeouperte": ("resultat_net", True),
}

_TABLE_POSTE_MAPS = {
    "bilan_actif_table": _ACTIF_POSTE_MAP,
    "bilan_passif_table": _PASSIF_POSTE_MAP,
    "bilan_compte_resultat_table": _CDR_POSTE_MAP,
}

FINANCIAL_TABLE_FIELD_IDS = tuple(_TABLE_POSTE_MAPS.keys())

FINANCIAL_TABLE_VALUE_ALIASES = {
    "n": ("n", "value_n", "valeur_n", "exercice_n", "montant_n"),
    "n1": ("n1", "value_n1", "valeur_n1", "exercice_n1", "montant_n1"),
    "commentaires": ("commentaires", "commentaire", "comments"),
}

FINANCIAL_LEGACY_FIELD_BASES = {
    "bilan": {
        "immobilisations_corporelles": "bilan_immobilisations_corporelles",
        "immobilisations_financieres": "bilan_immobilisations_financieres",
        "stocks": "bilan_stocks",
        "creances": "bilan_creances",
        "tresorerie": "bilan_tresorerie",
        "capital_social": "bilan_capital_social",
        "resultat_exercice": "bilan_resultat_exercice",
        "capitaux_propres": "bilan_capitaux_propres",
        "dettes_financieres": "bilan_dettes_financieres",
        "dettes_exploitation": "bilan_dettes_exploitation",
        "dettes_diverses": "bilan_dettes_diverses",
        "dettes_bancaires": "bilan_dettes_bancaires",
        "chiffre_affaires": "bilan_chiffre_affaires",
    },
    "compte_resultat": {
        "chiffre_affaires": "bilan_chiffre_affaires",
    },
}

FINANCIAL_COMPONENT_RULES = {
    "bilan": {
        "creances": {
            "components": ("creances_clients", "autres_creances"),
        },
        "tresorerie": {
            "components": ("disponibilites", "vmp"),
        },
        "autres_actif": {
            "components": ("charges_constatees_avance", "autres_actif_residuel"),
        },
        "dettes_exploitation": {
            "components": ("fournisseurs", "dettes_fiscales_sociales"),
        },
        "dettes_financieres": {
            "components": ("dettes_bancaires", "autres_dettes_financieres"),
        },
        "autres_passif": {
            "components": (
                "provisions_pour_risques",
                "provisions_pour_charges",
                "produits_constates_avance",
                "autres_passif_residuel",
            ),
        },
    },
    "compte_resultat": {
        "charges": {
            "components": (
                "achats_marchandises",
                "variation_stock_marchandises",
                "achats_matieres_premieres",
                "variation_stock_matieres_premieres",
                "autres_charges_externes",
            ),
        },
        "salaires_charges_sociales": {
            "components": ("salaires", "charges_sociales"),
        },
        "dotations": {
            "components": ("dotations_amortissements", "dotations_provisions"),
        },
        "autres_elements": {
            "components": (
                "production_stockee",
                "subventions_exploitation",
                "reprises_exploitation",
                "autres_produits_exploitation",
                "autres_charges_exploitation",
            ),
            "subtract_keys": (
                "production_stockee",
                "subventions_exploitation",
                "reprises_exploitation",
                "autres_produits_exploitation",
            ),
        },
    },
}


def _collect_table_key_aliases() -> Dict[str, Dict[str, str]]:
    aliases: Dict[str, Dict[str, str]] = {}
    for table_type, poste_map in _TABLE_POSTE_MAPS.items():
        table_aliases: Dict[str, str] = {}
        for raw_poste, (canonical_key, _is_total) in poste_map.items():
            if canonical_key:
                table_aliases[raw_poste] = canonical_key
                table_aliases[_normalize_poste(canonical_key)] = canonical_key

        statement_type = (
            "compte_resultat" if table_type == "bilan_compte_resultat_table" else "bilan"
        )
        for canonical_key in FINANCIAL_COMPONENT_RULES.get(statement_type, {}):
            table_aliases[_normalize_poste(canonical_key)] = canonical_key
        for canonical_key in FINANCIAL_LEGACY_FIELD_BASES.get(statement_type, {}):
            table_aliases[_normalize_poste(canonical_key)] = canonical_key

        aliases[table_type] = table_aliases
    return aliases


FINANCIAL_TABLE_KEY_ALIASES = _collect_table_key_aliases()


# ---------------------------------------------------------------------------
# Mapping + résolution
# ---------------------------------------------------------------------------

def _fuzzy_match_poste(norm_poste: str, poste_map: Dict[str, tuple]) -> Optional[tuple]:
    """Matching approximatif : norm_poste contient-il une clé connue ?"""
    best_match = None
    best_len = 0
    for map_key, value in poste_map.items():
        if value[0] is None:
            continue
        if len(map_key) < 6:
            continue
        if map_key in norm_poste and len(map_key) > best_len:
            best_match = value
            best_len = len(map_key)
    return best_match


def _safe_number(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(" ", "").replace("\u00a0", "")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _row_has_numeric_amount(row: Dict) -> bool:
    return _safe_number(row.get("n")) is not None or _safe_number(row.get("n1")) is not None


def _build_preserved_raw_row(row: Dict, table_type: str, source_index: int, reason: str) -> Dict:
    table_suffix = table_type.replace("bilan_", "").replace("_table", "")
    existing_comment = (row.get("commentaires") or "").strip()
    comment_parts = [part for part in [existing_comment, reason] if part]
    return {
        "key": f"raw_unmatched_{table_suffix}_{source_index:03d}",
        "poste": row.get("poste", ""),
        "n": row.get("n"),
        "n1": row.get("n1"),
        "commentaires": " | ".join(comment_parts),
        "_source_index": source_index,
    }


def _sum_raw_rows(rows: List[Dict], canonical_key: str) -> Dict:
    total_n = 0.0
    total_n1 = 0.0
    has_n = False
    has_n1 = False
    postes = []
    for r in rows:
        n = _safe_number(r.get("n"))
        n1 = _safe_number(r.get("n1"))
        if n is not None:
            total_n += n
            has_n = True
        if n1 is not None:
            total_n1 += n1
            has_n1 = True
        postes.append(r.get("poste", ""))
    return {
        "key": canonical_key,
        "poste": " + ".join(postes),
        "n": total_n if has_n else None,
        "n1": total_n1 if has_n1 else None,
        "commentaires": f"Somme Python de {len(rows)} sous-lignes",
    }


def map_raw_financial_table(raw_rows: list, table_type: str) -> list:
    """Transforme les lignes brutes LLM en lignes canoniques avec clé Excel."""
    poste_map = _TABLE_POSTE_MAPS.get(table_type, {})
    if not poste_map:
        return raw_rows

    groups: Dict[str, Dict[str, list]] = {}
    unmatched: List[str] = []
    preserved_raw_rows: List[Dict] = []

    for source_index, row in enumerate(raw_rows):
        if not isinstance(row, dict):
            continue
        poste = row.get("poste", "")
        if not poste:
            continue

        norm = _normalize_poste(poste)
        if not norm:
            continue

        match = poste_map.get(norm)
        if match is None:
            match = _fuzzy_match_poste(norm, poste_map)

        if match is None:
            unmatched.append(poste)
            if _row_has_numeric_amount(row):
                preserved_raw_rows.append(
                    _build_preserved_raw_row(row, table_type, source_index,
                                             "Ligne conservee: poste non mappe vers une cle Excel")
                )
            continue

        canonical_key, is_total = match
        if canonical_key is None:
            if _row_has_numeric_amount(row):
                preserved_raw_rows.append(
                    _build_preserved_raw_row(row, table_type, source_index,
                                             "Ligne conservee: total ou section non exploite(e) par Excel")
                )
            continue

        if canonical_key not in groups:
            groups[canonical_key] = {"totals": [], "details": [], "first_index": source_index}

        if is_total:
            groups[canonical_key]["totals"].append(row)
        else:
            groups[canonical_key]["details"].append(row)

    result = []
    for canonical_key, group in groups.items():
        if group["totals"]:
            best = group["totals"][-1]
            result.append({
                "key": canonical_key,
                "poste": best.get("poste", ""),
                "n": best.get("n"),
                "n1": best.get("n1"),
                "commentaires": best.get("commentaires", ""),
                "_source_index": group["first_index"],
            })
        elif group["details"]:
            if len(group["details"]) == 1:
                row = group["details"][0]
                result.append({
                    "key": canonical_key,
                    "poste": row.get("poste", ""),
                    "n": row.get("n"),
                    "n1": row.get("n1"),
                    "commentaires": row.get("commentaires", ""),
                    "_source_index": group["first_index"],
                })
            else:
                summed_row = _sum_raw_rows(group["details"], canonical_key)
                summed_row["_source_index"] = group["first_index"]
                result.append(summed_row)

    result.extend(preserved_raw_rows)
    result.sort(key=lambda row: row.get("_source_index", 10**9))
    for row in result:
        row.pop("_source_index", None)

    if unmatched:
        logger.warning(
            "%s poste(s) non matché(s) dans %s, %s ligne(s) brute(s) conservée(s)",
            len(unmatched), table_type, len(preserved_raw_rows),
        )

    return result


def _post_process_passif(mapped_rows: list) -> list:
    """Fusionne capitaux_propres_detail si capitaux_propres total est absent."""
    has_capitaux_propres = any(r["key"] == "capitaux_propres" for r in mapped_rows)
    result = []
    for row in mapped_rows:
        if row["key"] == "capitaux_propres_detail":
            if not has_capitaux_propres:
                row["key"] = "capitaux_propres"
                result.append(row)
        else:
            result.append(row)
    return result


# ---------------------------------------------------------------------------
# Normalisation canonique (pour réponses LLM déjà structurées avec key/n/n1)
# ---------------------------------------------------------------------------

def normalize_canonical_financial_rows(raw_rows: list, table_type: str) -> list:
    """Normalise des lignes déjà structurées (key présente) vers les clés Excel canoniques."""
    poste_map = _TABLE_POSTE_MAPS.get(table_type, {})
    if not poste_map:
        return []
    if not isinstance(raw_rows, list):
        return []
    explicit_key_rows = sum(1 for row in raw_rows if isinstance(row, dict) and row.get("key"))
    if explicit_key_rows == 0:
        return []

    normalized_rows = []
    for source_index, row in enumerate(raw_rows):
        if not isinstance(row, dict):
            continue
        raw_key = row.get("key") or row.get("poste") or row.get("poste_source") or row.get("label")
        if not raw_key:
            continue
        norm = _normalize_poste(str(raw_key))
        match = poste_map.get(norm)
        if match is None:
            match = _fuzzy_match_poste(norm, poste_map)
        if match is None:
            continue
        canonical_key, _is_total = match
        if canonical_key is None:
            continue
        normalized_rows.append({
            "key": canonical_key,
            "poste_source": row.get("poste_source") or row.get("poste") or row.get("label") or str(raw_key),
            "n": row.get("n"),
            "n1": row.get("n1"),
            "_source_index": source_index,
        })

    deduped = []
    seen = set()
    for row in normalized_rows:
        dedupe_key = (row.get("key"), row.get("n"), row.get("n1"))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        deduped.append(row)

    for row in deduped:
        row.pop("_source_index", None)

    return deduped


def prepare_financial_answers(answers: Dict) -> Dict:
    """Mappe les tables financières brutes LLM → clés canoniques Excel."""
    if not isinstance(answers, dict):
        return {}

    prepared = dict(answers)
    for fid in FINANCIAL_TABLE_FIELD_IDS:
        raw_val = prepared.get(fid)
        if isinstance(raw_val, list):
            canonical_rows = normalize_canonical_financial_rows(raw_val, fid)
            if canonical_rows:
                mapped = canonical_rows
            else:
                mapped = map_raw_financial_table(raw_val, fid)
            if fid == "bilan_passif_table":
                mapped = _post_process_passif(mapped)
            prepared[fid] = mapped
    return prepared


def parse_financial_table_rows(value) -> List[Dict]:
    if not value:
        return []
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [row for row in parsed if isinstance(row, dict)]


def _resolve_row_key(row_data: Dict, table_type: str) -> Optional[str]:
    raw_key = (
        row_data.get("key")
        or row_data.get("poste")
        or row_data.get("poste_source")
        or row_data.get("label")
    )
    if not raw_key:
        return None
    normalized = _normalize_poste(str(raw_key))
    return FINANCIAL_TABLE_KEY_ALIASES.get(table_type, {}).get(normalized)


def build_financial_table_lookup(value, table_type: str) -> Dict[str, Dict]:
    lookup: Dict[str, Dict] = {}
    for row_data in parse_financial_table_rows(value):
        key = _resolve_row_key(row_data, table_type)
        if key and key not in lookup:
            lookup[key] = row_data
    return lookup


def get_financial_row_field(row_data: Dict, target: str):
    if not row_data:
        return None
    aliases = FINANCIAL_TABLE_VALUE_ALIASES[target]
    for candidate in aliases:
        if candidate in row_data and row_data[candidate] is not None:
            return row_data[candidate]
        normalized_candidate = _normalize_poste(candidate)
        for key, value in row_data.items():
            if _normalize_poste(str(key)) == normalized_candidate and value is not None:
                return value
    return None


def _sum_component_values(
    table_lookup: Dict[str, Dict],
    component_keys: tuple[str, ...],
    period: str,
    subtract_keys: tuple[str, ...] = (),
):
    values = []
    present_keys = []
    subtract_key_set = set(subtract_keys)

    for key in component_keys:
        row_data = table_lookup.get(key)
        if not row_data:
            continue
        value = _safe_number(get_financial_row_field(row_data, period))
        if value is None:
            continue
        present_keys.append(key)
        values.append(-value if key in subtract_key_set else value)

    if not values:
        return None, present_keys

    total = sum(values)
    return int(total) if float(total).is_integer() else total, present_keys


def resolve_financial_metric_value(
    results: Dict,
    suffix: str,
    table_lookup: Dict[str, Dict],
    key: str,
    period: str,
    statement_type: str,
):
    row_data = table_lookup.get(key)
    direct_value = get_financial_row_field(row_data, period) if row_data else None

    component_rule = FINANCIAL_COMPONENT_RULES.get(statement_type, {}).get(key)
    if component_rule:
        component_total, present_keys = _sum_component_values(
            table_lookup,
            component_rule.get("components", ()),
            period,
            component_rule.get("subtract_keys", ()),
        )
        if len(present_keys) == len(component_rule.get("components", ())):
            return component_total
        if direct_value is not None:
            return direct_value
        return component_total

    if direct_value is not None:
        return direct_value

    legacy_base = FINANCIAL_LEGACY_FIELD_BASES.get(statement_type, {}).get(key)
    if not legacy_base:
        return None
    return results.get(f"{legacy_base}_{period}{suffix}")


# ---------------------------------------------------------------------------
# Validation comptable
# ---------------------------------------------------------------------------

def _build_table_dict(table_data) -> Dict[str, Dict]:
    if isinstance(table_data, str):
        try:
            table_data = json.loads(table_data)
        except (json.JSONDecodeError, TypeError):
            return {}
    if not isinstance(table_data, list):
        return {}
    result = {}
    for row in table_data:
        if isinstance(row, dict) and row.get("key"):
            result[row["key"]] = row
    return result


def _sum_keys(table_dict: Dict[str, Dict], keys: List[str], period: str = "n") -> Optional[float]:
    total = 0.0
    any_value = False
    for key in keys:
        row = table_dict.get(key)
        if not row:
            continue
        val = _safe_number(row.get(period))
        if val is not None:
            total += val
            any_value = True
    return total if any_value else None


def _validate_actif_table(table_data) -> List[str]:
    d = _build_table_dict(table_data)
    if not d:
        return []
    errors = []
    for period in ("n", "n1"):
        total_row = d.get("total_actif")
        total_val = _safe_number(total_row.get(period)) if total_row else None
        if total_val is None or total_val == 0:
            continue
        detail_sum = _sum_keys(d, _ACTIF_DETAIL_KEYS, period)
        if detail_sum is not None and detail_sum > 0:
            ecart = abs(detail_sum - total_val) / abs(total_val)
            if ecart > _VALIDATION_TOLERANCE:
                errors.append(
                    f"ACTIF {period}: somme détails={detail_sum:.0f} vs total_actif={total_val:.0f} "
                    f"(écart {ecart:.0%}). Probable double-comptage ou poste manquant."
                )
    return errors


def _validate_passif_table(table_data) -> List[str]:
    d = _build_table_dict(table_data)
    if not d:
        return []
    errors = []
    for period in ("n", "n1"):
        total_row = d.get("total_passif")
        total_val = _safe_number(total_row.get(period)) if total_row else None
        if total_val is None or total_val == 0:
            continue
        detail_sum = _sum_keys(d, _PASSIF_DETAIL_KEYS, period)
        if detail_sum is not None and detail_sum > 0:
            ecart = abs(detail_sum - total_val) / abs(total_val)
            if ecart > _VALIDATION_TOLERANCE:
                errors.append(
                    f"PASSIF {period}: somme détails={detail_sum:.0f} vs total_passif={total_val:.0f} "
                    f"(écart {ecart:.0%}). Probable double-comptage ou poste manquant."
                )
    return errors


def _validate_cdr_table(table_data) -> List[str]:
    d = _build_table_dict(table_data)
    if not d:
        return []
    errors = []
    for period in ("n", "n1"):
        ca_val = _safe_number(d.get("chiffre_affaires", {}).get(period))
        re_val = _safe_number(d.get("resultat_exploitation", {}).get(period))
        charges_val = _safe_number(d.get("charges", {}).get(period))

        if ca_val is not None and re_val is not None and charges_val is not None:
            expected_re = ca_val - charges_val
            if abs(re_val) > 0:
                ecart = abs(expected_re - re_val) / max(abs(re_val), 1)
                if ecart > _VALIDATION_TOLERANCE:
                    detail_charges = _sum_keys(d, _CDR_CHARGES_KEYS, period)
                    if detail_charges is not None and detail_charges > 0:
                        expected_re2 = ca_val - detail_charges
                        ecart2 = abs(expected_re2 - re_val) / max(abs(re_val), 1)
                        if ecart2 > _VALIDATION_TOLERANCE:
                            errors.append(
                                f"CDR {period}: CA={ca_val:.0f} - charges détaillées={detail_charges:.0f} "
                                f"= {expected_re2:.0f} vs résultat_exploitation={re_val:.0f} "
                                f"(écart {ecart2:.0%})."
                            )

        if charges_val is not None and charges_val > 0:
            detail_charges = _sum_keys(d, _CDR_CHARGES_KEYS, period)
            if detail_charges is not None and detail_charges > 0:
                ecart = abs(detail_charges - charges_val) / abs(charges_val)
                if ecart > _VALIDATION_TOLERANCE:
                    errors.append(
                        f"CDR {period}: somme charges détaillées={detail_charges:.0f} vs "
                        f"charges total={charges_val:.0f} (écart {ecart:.0%})."
                    )

        rcai_val = _safe_number(d.get("resultat_courant_avant_impots", {}).get(period))
        rfin_val = _safe_number(d.get("resultat_financier", {}).get(period))
        opcom_val = _safe_number(d.get("operations_en_commun", {}).get(period)) or 0.0
        if rcai_val is not None and re_val is not None and rfin_val is not None:
            expected_rcai = re_val + rfin_val + opcom_val
            ecart = abs(expected_rcai - rcai_val) / max(abs(rcai_val), 1)
            if ecart > _VALIDATION_TOLERANCE:
                errors.append(
                    f"CDR {period}: résultat exploitation={re_val:.0f} + résultat financier={rfin_val:.0f} "
                    f"+ opérations en commun={opcom_val:.0f} = {expected_rcai:.0f} vs "
                    f"résultat courant avant impôts={rcai_val:.0f} (écart {ecart:.0%})."
                )

        rnet_val = _safe_number(d.get("resultat_net", {}).get(period))
        rex_val = _safe_number(d.get("resultat_exceptionnel", {}).get(period))
        is_val = _safe_number(d.get("impots_sur_les_societes", {}).get(period))
        part_val = _safe_number(d.get("participation_salaries", {}).get(period)) or 0.0
        if rnet_val is not None and rcai_val is not None and rex_val is not None and is_val is not None:
            expected_rnet = rcai_val + rex_val - is_val - part_val
            ecart = abs(expected_rnet - rnet_val) / max(abs(rnet_val), 1)
            if ecart > _VALIDATION_TOLERANCE:
                errors.append(
                    f"CDR {period}: résultat courant avant impôts={rcai_val:.0f} + résultat exceptionnel={rex_val:.0f} "
                    f"- IS={is_val:.0f} - participation salariés={part_val:.0f} = {expected_rnet:.0f} vs "
                    f"résultat net={rnet_val:.0f} (écart {ecart:.0%})."
                )

    return errors


def validate_financial_answers(answers: Dict) -> Dict[str, List[str]]:
    """Valide toutes les tables financières. Retourne {field_id: [erreurs]}."""
    all_errors: Dict[str, List[str]] = {}

    for key, value in answers.items():
        if value is None:
            continue
        if "bilan_actif_table" in key:
            errs = _validate_actif_table(value)
            if errs:
                all_errors[key] = errs
        elif "bilan_passif_table" in key:
            errs = _validate_passif_table(value)
            if errs:
                all_errors[key] = errs
        elif "bilan_compte_resultat_table" in key:
            errs = _validate_cdr_table(value)
            if errs:
                all_errors[key] = errs

    grouped: Dict[str, Dict[str, object]] = {}
    for key, value in answers.items():
        if value is None:
            continue
        base_key, suffix = split_field_suffix(key)
        grouped.setdefault(suffix, {})[base_key] = value

    for suffix, group in grouped.items():
        actif = _build_table_dict(group.get("bilan_actif_table"))
        passif = _build_table_dict(group.get("bilan_passif_table"))
        if not actif or not passif:
            continue

        actif_key = f"bilan_actif_table{suffix}"
        passif_key = f"bilan_passif_table{suffix}"
        for period in ("n", "n1"):
            total_actif = _safe_number(actif.get("total_actif", {}).get(period))
            total_passif = _safe_number(passif.get("total_passif", {}).get(period))
            if total_actif is None or total_passif is None:
                continue
            ecart = abs(total_actif - total_passif) / max(abs(total_actif), abs(total_passif), 1)
            if ecart > _VALIDATION_TOLERANCE:
                message = (
                    f"Bilans croisés {period}: total_actif={total_actif:.0f} vs "
                    f"total_passif={total_passif:.0f} (écart {ecart:.0%})."
                )
                append_validation_error(all_errors, actif_key, message)
                append_validation_error(all_errors, passif_key, message)

    return all_errors


# ---------------------------------------------------------------------------
# Scoring qualité
# ---------------------------------------------------------------------------

def _row_contains_suspicious_amount(row: Dict) -> bool:
    for period in ("n", "n1"):
        value = _safe_number(row.get(period))
        if value is None:
            continue
        integer_digits = len(str(int(abs(value)))) if abs(value) >= 1 else 1
        if abs(value) >= 1_000_000_000 or integer_digits >= 10:
            return True
    return False


def _table_quality_stats(table_data) -> Dict[str, int]:
    rows = table_data if isinstance(table_data, list) else []
    mapped_rows = 0
    raw_unmatched_rows = 0
    suspicious_rows = 0
    critical_rows = 0
    informative_rows = 0

    critical_keys = {
        "total_actif", "total_passif", "chiffre_affaires", "charges",
        "resultat_exploitation", "resultat_courant_avant_impots", "resultat_net",
    }

    for row in rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("key") or "")
        if key.startswith("raw_unmatched_"):
            raw_unmatched_rows += 1
        elif key:
            mapped_rows += 1
        if _row_has_numeric_amount(row):
            informative_rows += 1
        if key in critical_keys:
            critical_rows += 1
        if _row_contains_suspicious_amount(row):
            suspicious_rows += 1

    return {
        "rows": len(rows),
        "mapped_rows": mapped_rows,
        "raw_unmatched_rows": raw_unmatched_rows,
        "suspicious_rows": suspicious_rows,
        "critical_rows": critical_rows,
        "informative_rows": informative_rows,
    }


def financial_answers_quality_report(
    answers: Dict,
    validation_errors: Optional[Dict[str, List[str]]] = None,
) -> Dict[str, object]:
    validation_errors = validation_errors or {}
    report: Dict[str, object] = {
        "score": 0,
        "reasons": [],
        "tables": {},
        "validation_error_count": sum(len(errs) for errs in validation_errors.values()),
        "should_retry_multimodal": False,
    }
    reasons: List[str] = report["reasons"]  # type: ignore[assignment]

    score = 0
    for fid in FINANCIAL_TABLE_FIELD_IDS:
        stats = _table_quality_stats(answers.get(fid))
        report["tables"][fid] = stats
        score += min(stats["mapped_rows"], 8)
        score += stats["critical_rows"] * 3
        score -= stats["raw_unmatched_rows"] * 4
        score -= stats["suspicious_rows"] * 8

        if stats["mapped_rows"] == 0 and stats["informative_rows"] > 0:
            reasons.append(f"{fid}: lignes chiffrées non mappées")
        if stats["raw_unmatched_rows"] >= max(2, stats["mapped_rows"]):
            reasons.append(f"{fid}: trop de lignes brutes non mappées")
        if stats["suspicious_rows"] > 0:
            reasons.append(f"{fid}: montants suspects ou concaténés")

    error_count = report["validation_error_count"]
    score -= error_count * 6
    if error_count:
        reasons.append(f"{error_count} incohérence(s) comptable(s) détectée(s)")

    if score < 12:
        reasons.append("score global trop faible")
    report["score"] = score
    report["should_retry_multimodal"] = bool(
        score < 12
        or error_count > 0
        or any("montants suspects" in reason for reason in reasons)
        or any("non mappées" in reason for reason in reasons)
    )
    return report


def select_better_financial_answers(
    primary_answers: Dict,
    primary_report: Dict,
    fallback_answers: Optional[Dict],
    fallback_report: Optional[Dict],
) -> tuple[Dict, Dict, str]:
    if not fallback_answers or not fallback_report:
        return primary_answers, primary_report, "text"

    primary_score = int(primary_report.get("score") or 0)
    fallback_score = int(fallback_report.get("score") or 0)
    financial_field_ids = {
        "bilan_societe_nom",
        "bilan_date_arrete_n",
        "bilan_date_arrete_n1",
        *FINANCIAL_TABLE_FIELD_IDS,
    }

    if fallback_score > primary_score:
        merged_answers = dict(primary_answers)
        for field_id in financial_field_ids:
            if field_id in fallback_answers and _has_meaningful_value(fallback_answers.get(field_id)):
                merged_answers[field_id] = fallback_answers[field_id]
        return merged_answers, fallback_report, "multimodal"

    return primary_answers, primary_report, "text"


def _has_meaningful_value(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() not in {"", "null", "[]", "{}"}
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) > 0
    return True
