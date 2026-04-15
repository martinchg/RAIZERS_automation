import argparse
import json
import logging
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

from normalization import canonical_name, is_archived_path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------
HEADER_FONT = Font(bold=True, size=11, color="FFFFFF")
HEADER_FILL = PatternFill(start_color="2F5496", end_color="2F5496", fill_type="solid")
RADIATED_ROW_FILL = PatternFill(start_color="E6E6E6", end_color="E6E6E6", fill_type="solid")
HEADER_ALIGNMENT = Alignment(horizontal="center", vertical="center", wrap_text=True)

LABEL_FONT = Font(bold=True, size=10)
VALUE_FONT = Font(size=10)
HYPERLINK_FONT = Font(size=10, color="0563C1", underline="single")

THIN_BORDER = Border(
    left=Side(style="thin"),
    right=Side(style="thin"),
    top=Side(style="thin"),
    bottom=Side(style="thin"),
)

SECTION_FONT = Font(bold=True, size=11, color="2F5496")
NUMBER_FORMAT_INTEGER = '# ##0'
NUMBER_FORMAT_DECIMAL = '# ##0.00'

_OPERATION_FIXED_ROWS_AFTER = {
    "montant_collecte": [
        ("Montant d'une obligation", "1"),
        ("Ticket Minimum", "1 000"),
    ]
}


def _style_header_row(ws, row: int, max_col: int):
    for col in range(1, max_col + 1):
        cell = ws.cell(row=row, column=col)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = HEADER_ALIGNMENT
        cell.border = THIN_BORDER


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _normalize_folder_name(value: str) -> str:
    """Clé compacte (alphanumérique) basée sur la canonicalisation partagée."""
    return re.sub(r"[^a-z0-9]+", "", canonical_name(value or ""))


_RH_CANON_ALIASES = {"rh", "ressources humaines"}


def _extract_person_folder_from_source_path(source_path: str) -> Optional[str]:
    """Nom du sous-dossier personne sous RH. Matching flexible (alias : 'RH', 'Ressources Humaines')."""
    if not source_path or _is_old_label(source_path):
        return None

    raw_parts = [p for p in source_path.replace("\\", "/").split("/") if p]
    canon_parts = [canonical_name(p) for p in raw_parts]

    rh_idx = next(
        (i for i, p in enumerate(canon_parts) if p in _RH_CANON_ALIASES),
        None,
    )
    if rh_idx is None:
        return None

    if rh_idx + 1 >= len(raw_parts) - 1:
        return None

    candidate = raw_parts[rh_idx + 1]
    if _is_old_label(candidate):
        return None
    return candidate


def _derive_person_folder_map_from_manifest(manifest_path: Path) -> Dict[str, str]:
    if not manifest_path.exists():
        return {}

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    folder_map: Dict[str, str] = {}
    counter = 0
    for doc in manifest.get("files", []):
        folder_name = _extract_person_folder_from_source_path(doc.get("source_path", ""))
        if not folder_name:
            continue
        key = _normalize_folder_name(folder_name)
        if key not in folder_map:
            folder_map[f"__{counter}"] = folder_name
            counter += 1

    return folder_map


def _is_old_label(value: str) -> bool:
    return is_archived_path(value)


def _normalize_key(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _to_number(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(" ", "").replace("\u202f", "").replace("€", "").replace("%", "")
    text = text.replace(",", ".")
    try:
        number = float(text)
        return int(number) if number.is_integer() else number
    except ValueError:
        return None


def _format_number_with_spaces(value) -> str:
    number = _to_number(value)
    if number is None:
        return str(value)

    if isinstance(number, float) and not number.is_integer():
        text = f"{number:,.2f}".rstrip("0").rstrip(".")
    else:
        text = f"{int(number):,}"
    return text.replace(",", " ")


def _format_display_value(value):
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return _format_number_with_spaces(value)

    text = str(value).strip()
    if not text:
        return ""
    if text.startswith("="):
        return text
    if "%" in text:
        return text
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2,4}", text):
        return text

    compact = text.replace(" ", "").replace("\u202f", "").replace("€", "")
    if re.fullmatch(r"-?\d+(?:[.,]\d+)?", compact):
        return _format_number_with_spaces(text)

    return text


def _apply_numeric_format(cell, value=None):
    target = cell.value if value is None else value
    number = _to_number(target)
    if number is None:
        return
    cell.number_format = NUMBER_FORMAT_INTEGER if float(number).is_integer() else NUMBER_FORMAT_DECIMAL


def _formula_literal_number(value, default: str = "0") -> str:
    number = _to_number(value)
    if number is None:
        return f"={default}"
    if float(number).is_integer():
        return f"={int(number)}"
    return f"={number}"


def _formula_indirect_or_zero(sheet_name: str, cell_ref: str) -> str:
    return f'=IFERROR(INDIRECT("\'{sheet_name}\'!{cell_ref}"),0)'


def _resolve_table_value(row_data: dict, key: str):
    alias_map = {
        "type_detention": ["type_detention", "type_de_detention", "type_bien"],
        "pct_detention": ["pct_detention"],
        "adresse": ["adresse", "adresse_destination"],
        "valeur_bien": ["valeur_bien", "estimation_actuelle"],
        "valeur_banque": ["valeur_banque", "capital_restant_du"],
        "valeur_nette_detenue": ["valeur_nette_detenue"],
        "revenus_locatifs": ["revenus_locatifs"],
        "garanties_donnees": ["garanties_donnees"],
        "societe": ["societe", "raison_sociale"],
        "activite": ["activite"],
        "creation": ["creation"],
        "infos_financieres": ["infos_financieres", "chiffres_cles"],
        "role_detention": ["role_detention", "pct_detention"],
        "commentaires": ["commentaires"],
    }

    candidates = alias_map.get(key, [key])
    normalized_row = {_normalize_key(str(k)): v for k, v in row_data.items()}

    for candidate in candidates:
        if candidate in row_data and row_data[candidate] is not None:
            return row_data[candidate]
        normalized_candidate = _normalize_key(candidate)
        if normalized_candidate in normalized_row and normalized_row[normalized_candidate] is not None:
            return normalized_row[normalized_candidate]

    if key == "valeur_nette_detenue":
        value_bien = _to_number(_resolve_table_value(row_data, "valeur_bien"))
        value_banque = _to_number(_resolve_table_value(row_data, "valeur_banque"))
        if value_bien is not None and value_banque is not None:
            net = value_bien - value_banque
            return int(net) if float(net).is_integer() else net

    return None


def _resolve_person_sheets(results: Dict, person_folder_map: Optional[Dict[str, str]] = None) -> List[Tuple[str, str]]:
    persons: List[Tuple[str, str]] = []

    if person_folder_map:
        for suffix, folder_name in sorted(person_folder_map.items()):
            if _is_old_label(folder_name):
                continue
            persons.append((suffix, folder_name))
        if persons:
            return persons

    for fid, value in sorted(results.items()):
        if fid.startswith("patrimoine_personne_nom") and value:
            suffix = fid.replace("patrimoine_personne_nom", "")
            persons.append((suffix, value.strip()))

    return persons


def _slugify_company_name(value: str) -> str:
    value = unicodedata.normalize("NFKD", value or "")
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value


def _build_pappers_company_url(company: dict) -> Optional[str]:
    nom_societe = (company.get("nom_societe") or company.get("societe") or "").strip()
    siren = str(company.get("siren") or "").strip()

    if not nom_societe or not siren:
        return None

    slug = _slugify_company_name(nom_societe)
    if not slug:
        return None

    return f"https://www.pappers.fr/entreprise/{slug}-{siren}"


def _build_infos_financieres(company: dict) -> Optional[str]:
    parts = []

    capital = company.get("capital")
    if capital is not None:
        parts.append(f"Capital: {_format_number_with_spaces(capital)}")

    chiffre_affaires = company.get("chiffre_affaires")
    if chiffre_affaires is not None:
        parts.append(f"CA: {_format_number_with_spaces(chiffre_affaires)}")

    resultat_net = company.get("resultat_net")
    if resultat_net is not None:
        parts.append(f"RN: {_format_number_with_spaces(resultat_net)}")

    if not parts:
        return None

    return " | ".join(parts)


def _build_statut(company: dict) -> Optional[str]:
    parts = []

    statut = company.get("statut")
    if statut:
        parts.append(statut)

    statut_rcs = company.get("statut_rcs")
    if statut_rcs:
        parts.append(f"RCS: {statut_rcs}")

    if not parts:
        return None

    return " | ".join(parts)


def _is_radiated_company(company: dict) -> bool:
    statut = canonical_name(str(company.get("statut") or ""))
    statut_rcs = canonical_name(str(company.get("statut_rcs") or ""))
    return "radie" in statut or "radie" in statut_rcs


# ---------------------------------------------------------------------------
# Sheet builders
# ---------------------------------------------------------------------------
def _build_operation_sheet(wb: Workbook, results: Dict, fields: List[Dict]):
    """Crée l'onglet Opération avec les champs simples, groupés par section."""
    ws = wb.active
    ws.title = "Opération"

    ws.column_dimensions["B"].width = 35
    ws.column_dimensions["C"].width = 50

    # Grouper les champs par section
    SECTIONS = [
        ("emprunt", "Société emprunteuse"),
        ("operation", "Société opération"),
    ]

    # Inclure les champs Opération + les champs sans sheet (localisation_projet, etc.)
    op_fields = [f for f in fields if f.get("excel_sheet", "") not in ("{person_name}", "{company_name}") and f.get("type") != "table"]

    row = 1
    filled = 0
    current_section = None

    for field in op_fields:
        field_id = field["field_id"]

        # Détecter le changement de section
        section_key = None
        for key, title in SECTIONS:
            if field_id.endswith(f"_{key}"):
                section_key = key
                break

        if section_key and section_key != current_section:
            if current_section is not None:
                row += 1  # ligne vide entre sections

            current_section = section_key
            section_title = dict(SECTIONS).get(section_key, "")
            section_cell = ws.cell(row=row, column=2, value=section_title)
            section_cell.font = Font(bold=True, size=12, color="FFFFFF")
            section_cell.fill = PatternFill(start_color="2F5496", end_color="2F5496", fill_type="solid")
            section_cell.alignment = Alignment(horizontal="center")
            ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=3)
            row += 1

        # Champs hors section (objet_financement, taux, etc.)
        if section_key is None and current_section is not None:
            if current_section != "__other":
                row += 1
                other_cell = ws.cell(row=row, column=2, value="Opération")
                other_cell.font = Font(bold=True, size=12, color="FFFFFF")
                other_cell.fill = PatternFill(start_color="2F5496", end_color="2F5496", fill_type="solid")
                other_cell.alignment = Alignment(horizontal="center")
                ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=3)
                row += 1
                current_section = "__other"

        label = field.get("label", field_id)
        value = results.get(field_id)

        ws.cell(row=row, column=2, value=label).font = LABEL_FONT
        ws.cell(row=row, column=2).border = THIN_BORDER

        val_cell = ws.cell(row=row, column=3, value=_format_display_value(value))
        val_cell.font = VALUE_FONT
        val_cell.border = THIN_BORDER
        val_cell.alignment = Alignment(wrap_text=True)

        # Lien Google Earth pour la localisation
        if field_id == "localisation_projet" and value:
            earth_url = f"https://earth.google.com/web/search/{quote(value)}"
            val_cell.hyperlink = earth_url
            val_cell.font = HYPERLINK_FONT

        if value:
            filled += 1

        for fixed_label, fixed_value in _OPERATION_FIXED_ROWS_AFTER.get(field_id, []):
            row += 1

            ws.cell(row=row, column=2, value=fixed_label).font = LABEL_FONT
            ws.cell(row=row, column=2).border = THIN_BORDER

            fixed_val_cell = ws.cell(row=row, column=3, value=fixed_value)
            fixed_val_cell.font = VALUE_FONT
            fixed_val_cell.border = THIN_BORDER
            fixed_val_cell.alignment = Alignment(wrap_text=True)

        row += 1

    logger.info(f"  Opération : {filled} valeurs remplies")
    return filled


def _build_patrimoine_sheet(wb: Workbook, results: Dict, fields: List[Dict],
                            person_folder_map: Optional[Dict[str, str]] = None):
    """Crée l'onglet Patrimoine avec toutes les personnes."""
    persons = _resolve_person_sheets(results, person_folder_map=person_folder_map)
    if not persons:
        return 0

    ws = wb.create_sheet("Patrimoine")
    ws.column_dimensions["A"].width = 5
    ws.column_dimensions["B"].width = 30
    ws.column_dimensions["C"].width = 24
    ws.column_dimensions["D"].width = 30
    ws.column_dimensions["E"].width = 18
    ws.column_dimensions["F"].width = 18
    ws.column_dimensions["G"].width = 18
    ws.column_dimensions["H"].width = 18
    ws.column_dimensions["I"].width = 25

    filled = 0
    current_row = 1

    # Récupérer les champs patrimoine
    simple_fields = [f for f in fields if f.get("excel_sheet") == "{person_name}" and f.get("type") != "table"]
    table_fields = [f for f in fields if f.get("excel_sheet") == "{person_name}" and f.get("type") == "table"]

    for person_idx, (suffix, person_name) in enumerate(persons):
        # Section header pour la personne
        section_cell = ws.cell(row=current_row, column=2, value=person_name)
        section_cell.font = Font(bold=True, size=13, color="2F5496")
        current_row += 1

        # Champs simples (ex: régime matrimonial)
        for field in simple_fields:
            field_id = field["field_id"]
            actual_fid = field_id + suffix
            actual_value = results.get(actual_fid)

            label = field.get("label", field_id)
            ws.cell(row=current_row, column=2, value=label).font = LABEL_FONT
            ws.cell(row=current_row, column=2).border = THIN_BORDER
            val_cell = ws.cell(row=current_row, column=3, value=_format_display_value(actual_value))
            val_cell.font = VALUE_FONT
            val_cell.border = THIN_BORDER
            if actual_value:
                filled += 1
            current_row += 1

        # Tables (ex: patrimoine immobilier)
        for field in table_fields:
            field_id = field["field_id"]
            actual_fid = field_id + suffix
            actual_value = results.get(actual_fid)

            if not actual_value:
                continue

            try:
                rows = json.loads(actual_value) if isinstance(actual_value, str) else actual_value
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"  {actual_fid}: valeur table non parseable")
                continue

            if not rows:
                continue

            current_row += 1
            table_label = field.get("label", field_id)
            ws.cell(row=current_row, column=2, value=table_label).font = SECTION_FONT
            current_row += 1

            col_map = field.get("column_mapping", {})
            # Insérer valeur_nette_detenue en colonne G si c'est le tableau patrimoine
            is_patrimoine_table = "valeur_bien" in col_map and "valeur_banque" in col_map
            display_headers = list(col_map.keys())
            if is_patrimoine_table and "valeur_nette_detenue" not in display_headers:
                # Insérer après valeur_banque (col F) → position G
                insert_idx = display_headers.index("valeur_banque") + 1 if "valeur_banque" in display_headers else len(display_headers)
                display_headers.insert(insert_idx, "valeur_nette_detenue")

            # Headers du tableau
            for col_idx, header in enumerate(display_headers):
                cell = ws.cell(row=current_row, column=2 + col_idx, value=header.replace("_", " ").title())
                cell.font = HEADER_FONT
                cell.fill = HEADER_FILL
                cell.alignment = HEADER_ALIGNMENT
                cell.border = THIN_BORDER
            current_row += 1

            # Colonnes numériques pour la ligne TOTAL
            numeric_keys = {"valeur_bien", "valeur_banque", "valeur_nette_detenue", "revenus_locatifs"}

            # Données
            data_start_row = current_row
            for row_data in rows:
                for col_idx, key in enumerate(display_headers):
                    cell = ws.cell(row=current_row, column=2 + col_idx)
                    cell.border = THIN_BORDER

                    if key == "valeur_nette_detenue" and is_patrimoine_table:
                        col_e = col_map.get("valeur_bien", "E")
                        col_f = col_map.get("valeur_banque", "F")
                        col_c = col_map.get("pct_detention", "C")
                        r = current_row
                        cell.value = f"=({col_e}{r}-{col_f}{r})*{col_c}{r}"
                        cell.font = VALUE_FONT
                        cell.number_format = NUMBER_FORMAT_INTEGER
                    else:
                        cell_val = _resolve_table_value(row_data, key)
                        cell.value = _format_display_value(cell_val) if not isinstance(cell_val, (int, float)) else cell_val
                        cell.font = VALUE_FONT
                        _apply_numeric_format(cell, cell_val)
                        if cell_val is not None:
                            filled += 1
                current_row += 1
            data_end_row = current_row - 1

            # Ligne TOTAL
            if len(rows) > 0:
                total_font = Font(bold=True, size=10)
                ws.cell(row=current_row, column=2, value="TOTAL").font = total_font
                ws.cell(row=current_row, column=2).border = THIN_BORDER
                for col_idx, key in enumerate(display_headers):
                    cell = ws.cell(row=current_row, column=2 + col_idx)
                    cell.border = THIN_BORDER
                    if key in numeric_keys:
                        col_letter = get_column_letter(2 + col_idx)
                        cell.value = f"=SUM({col_letter}{data_start_row}:{col_letter}{data_end_row})"
                        cell.font = total_font
                        cell.number_format = NUMBER_FORMAT_INTEGER
                current_row += 1

        current_row += 2  # espace entre personnes

    logger.info(f"  Patrimoine : {filled} valeurs remplies")
    return filled


def _resolve_company_sheets(results: Dict) -> List[Tuple[str, str]]:
    """Identifie les entreprises à partir des champs bilan_societe_nom__0, __1, etc."""
    companies: List[Tuple[str, str]] = []
    for fid, value in sorted(results.items()):
        if fid.startswith("bilan_societe_nom") and value:
            suffix = fid.replace("bilan_societe_nom", "")
            companies.append((suffix, value.strip()))
    return companies


# ---------------------------------------------------------------------------
# Bilan layout constants
# ---------------------------------------------------------------------------
_BILAN_ACTIF_ROWS = [
    ("Immobilisations corporelles", "immobilisations_corporelles"),
    ("Immobilisations financières", "immobilisations_financieres"),
    ("Stocks", "stocks"),
    ("Créances", "creances"),
    ("Trésorerie", "tresorerie"),
    ("Autres éléments d'actif", "autres_actif"),
]

_BILAN_PASSIF_DETAIL_ROWS = [
    ("Capital social", "capital_social"),
    ("Résultat", "resultat_exercice"),
]

_BILAN_PASSIF_ROWS = [
    ("Capitaux propres", "capitaux_propres"),
    ("Dettes financières", "dettes_financieres"),
    ("Dettes d'exploitation", "dettes_exploitation"),
    ("Dettes diverses", "dettes_diverses"),
    ("Autres éléments de passif", "autres_passif"),
]

_BILAN_EXTRA_ROWS = [
    ("Dettes bancaires", "dettes_bancaires"),
    ("Chiffre d'affaires", "chiffre_affaires"),
]

_BILAN_LEGACY_FIELD_BASES = {
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
}

_BILAN_TABLE_KEY_ALIASES = {
    "immobilisationscorporelles": "immobilisations_corporelles",
    "immobilisationscorporellesnet": "immobilisations_corporelles",
    "immobilisationsfinancieres": "immobilisations_financieres",
    "immobilisationsfinancieresnet": "immobilisations_financieres",
    "stocks": "stocks",
    "stock": "stocks",
    "stocksetencours": "stocks",
    "stocksencours": "stocks",
    "creances": "creances",
    "creancesnet": "creances",
    "creancesclients": "creances_clients",
    "clientsetcomptesrattaches": "creances_clients",
    "autrescreances": "autres_creances",
    "tresorerie": "tresorerie",
    "disponibilites": "disponibilites",
    "tresoreriedisponibilites": "disponibilites",
    "vmp": "vmp",
    "valeursmobilieresdeplacement": "vmp",
    "chargesconstateesdavance": "charges_constatees_avance",
    "autresactifresiduel": "autres_actif_residuel",
    "autresactif": "autres_actif",
    "autreselementsdactif": "autres_actif",
    "autrespostesdactif": "autres_actif",
    "avancesetacomptesversessurcommandes": "autres_actif_residuel",
    "capitalsouscritappelenonverse": "autres_actif_residuel",
    "totalactif": "total_actif",
    "totalgeneralactif": "total_actif",
    "capitalsocial": "capital_social",
    "resultatexercice": "resultat_exercice",
    "resultatnet": "resultat_exercice",
    "capitauxpropres": "capitaux_propres",
    "dettesfinancieres": "dettes_financieres",
    "dettesdexploitation": "dettes_exploitation",
    "dettesexploitation": "dettes_exploitation",
    "dettesdiverses": "dettes_diverses",
    "autresdettes": "dettes_diverses",
    "fournisseurs": "fournisseurs",
    "fournisseursetcomptesrattaches": "fournisseurs",
    "dettesfiscalesetsociales": "dettes_fiscales_sociales",
    "dettesfiscalessociales": "dettes_fiscales_sociales",
    "comptescourants": "autres_dettes_financieres",
    "comptescourantsassocies": "autres_dettes_financieres",
    "comptescourantsdassocies": "autres_dettes_financieres",
    "cca": "autres_dettes_financieres",
    "dettesbancaires": "dettes_bancaires",
    "empruntsetdettesaupresdesetablissementsdecredit": "dettes_bancaires",
    "empruntsbancaires": "dettes_bancaires",
    "autresdettesfinancieres": "autres_dettes_financieres",
    "empruntsetdettesfinancieresdivers": "autres_dettes_financieres",
    "empruntsetdettesfinancieresdiverses": "autres_dettes_financieres",
    "autresempruntsobligataires": "autres_dettes_financieres",
    "empruntsobligatairesconvertibles": "autres_dettes_financieres",
    "provisionspourrisques": "provisions_pour_risques",
    "provisionspourcharges": "provisions_pour_charges",
    "produitsconstatesdavance": "produits_constates_avance",
    "autrespassifresiduel": "autres_passif_residuel",
    "autrespassif": "autres_passif",
    "autreselementsdepassif": "autres_passif",
    "autrespostesdepassif": "autres_passif",
    "totalpassif": "total_passif",
    "totalgeneralpassif": "total_passif",
    "chiffredaffaires": "chiffre_affaires",
    "ca": "chiffre_affaires",
}

_BILAN_TABLE_VALUE_ALIASES = {
    "n": ["n", "value_n", "valeur_n", "exercice_n", "montant_n"],
    "n1": ["n1", "value_n1", "valeur_n1", "exercice_n1", "montant_n1"],
    "commentaires": ["commentaires", "commentaire", "comments"],
}

_COMPTE_RESULTAT_ROWS = [
    ("Chiffre d'affaires", "chiffre_affaires"),
    ("Charges", "charges"),
    ("Salaires et charges sociales", "salaires_charges_sociales"),
    ("Impôts et taxes", "impots_taxes"),
    ("Dotations aux amortissements", "dotations"),
    ("Autres éléments", "autres_elements"),
    ("Résultat financier", "resultat_financier"),
    ("Résultat exceptionnel", "resultat_exceptionnel"),
    ("Impôts sur les sociétés", "impots_sur_les_societes"),
]

_COMPTE_RESULTAT_LEGACY_FIELD_BASES = {
    "chiffre_affaires": "bilan_chiffre_affaires",
}

_COMPTE_RESULTAT_TABLE_KEY_ALIASES = {
    "chiffredaffaires": "chiffre_affaires",
    "chiffreaffaires": "chiffre_affaires",
    "ca": "chiffre_affaires",
    "charges": "charges",
    "achatsetmarchandises": "achats_marchandises",
    "achatsdemarchandises": "achats_marchandises",
    "variationdestockmarchandises": "variation_stock_marchandises",
    "variationdestockdesmarchandises": "variation_stock_marchandises",
    "achatsdematierespremieresetautresapprovisionnements": "achats_matieres_premieres",
    "achatsmatierespremieresetautresapprovisionnements": "achats_matieres_premieres",
    "variationdestockmatierespremieresetapprovisionnements": "variation_stock_matieres_premieres",
    "variationdestockdesmatierespremieresetapprovisionnements": "variation_stock_matieres_premieres",
    "autresachatsetchargesexternes": "autres_charges_externes",
    "autresachatschargesexternes": "autres_charges_externes",
    "chargesexternes": "autres_charges_externes",
    "achatsetchargesexternes": "autres_charges_externes",
    "achatschargesexternes": "autres_charges_externes",
    "achatsetapprovisionnements": "autres_charges_externes",
    "salairesetchargessociales": "salaires_charges_sociales",
    "salaireschargessociales": "salaires_charges_sociales",
    "salairescharges": "salaires_charges_sociales",
    "salaires": "salaires",
    "salairesettraitements": "salaires",
    "remunerationsdupersonnel": "salaires",
    "chargessociales": "charges_sociales",
    "impotsettaxes": "impots_taxes",
    "dotations": "dotations",
    "dotationsauxamortissements": "dotations_amortissements",
    "dotationsauxprovisions": "dotations_provisions",
    "autreselements": "autres_elements",
    "productionstockee": "production_stockee",
    "subventionsdexploitation": "subventions_exploitation",
    "reprisesuramortissementsetprovisionstransfertsdecharges": "reprises_exploitation",
    "reprisesuramortissementsetprovision": "reprises_exploitation",
    "autresproduits": "autres_produits_exploitation",
    "autrescharges": "autres_charges_exploitation",
    "autreschargesdexploitation": "autres_charges_exploitation",
    "autreschargesexploitation": "autres_charges_exploitation",
    "autresproduitsdexploitation": "autres_produits_exploitation",
    "autresproduitsexploitation": "autres_produits_exploitation",
    "resultatexploitation": "resultat_exploitation",
    "resultatdexploitation": "resultat_exploitation",
    "resultatfinancier": "resultat_financier",
    "resultatexceptionnel": "resultat_exceptionnel",
    "impotssurlessocietes": "impots_sur_les_societes",
    "is": "impots_sur_les_societes",
}

RED_FONT = Font(bold=True, size=10, color="CC0000")
RED_FONT_ITALIC = Font(bold=True, italic=True, size=10, color="CC0000")
GREEN_FONT = Font(bold=True, size=10, color="006600")
BOLD_FONT = Font(bold=True, size=10)
ITALIC_FONT = Font(italic=True, size=10)
TOTAL_FILL = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
GEARING_FILL = PatternFill(start_color="4BACC6", end_color="4BACC6", fill_type="solid")


def _parse_json_array(value) -> List[Dict]:
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


def _resolve_metric_table_key(row_data: Dict, key_aliases: Dict[str, str]) -> Optional[str]:
    raw_key = row_data.get("key") or row_data.get("poste") or row_data.get("label")
    if not raw_key:
        return None
    normalized = _normalize_key(str(raw_key))
    return key_aliases.get(normalized)


def _build_metric_table_lookup(value, key_aliases: Dict[str, str]) -> Dict[str, Dict]:
    lookup: Dict[str, Dict] = {}
    for row_data in _parse_json_array(value):
        key = _resolve_metric_table_key(row_data, key_aliases)
        if key and key not in lookup:
            lookup[key] = row_data
    return lookup


def _get_bilan_table_field(row_data: Dict, target: str):
    if not row_data:
        return None
    for candidate in _BILAN_TABLE_VALUE_ALIASES[target]:
        if candidate in row_data and row_data[candidate] is not None:
            return row_data[candidate]
        normalized_candidate = _normalize_key(candidate)
        for key, value in row_data.items():
            if _normalize_key(str(key)) == normalized_candidate and value is not None:
                return value
    return None


_BILAN_COMPONENT_KEYS = {
    "creances": ["creances_clients", "autres_creances"],
    "tresorerie": ["disponibilites", "vmp"],
    "autres_actif": ["charges_constatees_avance", "autres_actif_residuel"],
    "dettes_exploitation": ["fournisseurs", "dettes_fiscales_sociales"],
    "dettes_financieres": ["dettes_bancaires", "autres_dettes_financieres"],
    "autres_passif": ["provisions_pour_risques", "provisions_pour_charges", "produits_constates_avance", "autres_passif_residuel"],
}

_COMPTE_RESULTAT_COMPONENT_KEYS = {
    "charges": [
        "achats_marchandises",
        "variation_stock_marchandises",
        "achats_matieres_premieres",
        "variation_stock_matieres_premieres",
        "autres_charges_externes",
    ],
    "salaires_charges_sociales": ["salaires", "charges_sociales"],
    "dotations": ["dotations_amortissements", "dotations_provisions"],
    "autres_elements": [
        "production_stockee",
        "subventions_exploitation",
        "reprises_exploitation",
        "autres_produits_exploitation",
        "autres_charges_exploitation",
    ],
}


def _sum_component_values(table_lookup: Dict[str, Dict], keys: List[str], period: str, subtract_keys: Optional[set[str]] = None):
    values = []
    present_keys = []
    subtract_keys = subtract_keys or set()
    for key in keys:
        row_data = table_lookup.get(key)
        if not row_data:
            continue
        value = _to_number(_get_bilan_table_field(row_data, period))
        if value is None:
            continue
        present_keys.append(key)
        values.append(-value if key in subtract_keys else value)

    if not values:
        return None, present_keys

    total = sum(values)
    return int(total) if float(total).is_integer() else total, present_keys


def _resolve_aggregated_value(
    direct_value,
    table_lookup: Dict[str, Dict],
    component_keys: List[str],
    period: str,
    subtract_keys: Optional[set[str]] = None,
):
    component_total, present_keys = _sum_component_values(
        table_lookup,
        component_keys,
        period,
        subtract_keys=subtract_keys,
    )
    if len(present_keys) == len(component_keys):
        return component_total
    if direct_value is not None:
        return direct_value
    return component_total


def _resolve_aggregated_comment(
    direct_comment: Optional[str],
    table_lookup: Dict[str, Dict],
    component_keys: List[str],
    subtract_keys: Optional[set[str]] = None,
) -> Optional[str]:
    subtract_keys = subtract_keys or set()
    present_parts: List[str] = []
    for key in component_keys:
        row_data = table_lookup.get(key)
        if not row_data:
            continue
        value = _to_number(_get_bilan_table_field(row_data, "n"))
        value_n1 = _to_number(_get_bilan_table_field(row_data, "n1"))
        if value is None and value_n1 is None:
            continue
        prefix = "-" if key in subtract_keys else "+"
        present_parts.append(f"{prefix}{key}")

    if len(present_parts) == len(component_keys):
        formula = " ".join(present_parts).lstrip("+")
        return f"Calcul Python: {formula}"
    return direct_comment


def _get_bilan_value(results: Dict, suffix: str, table_lookup: Dict[str, Dict], key: str, period: str):
    row_data = table_lookup.get(key)
    direct_value = None
    if row_data:
        direct_value = _get_bilan_table_field(row_data, period)

    component_keys = _BILAN_COMPONENT_KEYS.get(key)
    if component_keys:
        return _resolve_aggregated_value(direct_value, table_lookup, component_keys, period)

    if direct_value is not None:
        return direct_value

    legacy_base = _BILAN_LEGACY_FIELD_BASES.get(key)
    if not legacy_base:
        return None
    return results.get(f"{legacy_base}_{period}{suffix}")


def _get_bilan_comment(table_lookup: Dict[str, Dict], key: str) -> Optional[str]:
    row_data = table_lookup.get(key)
    direct_comment = None
    if row_data:
        value = _get_bilan_table_field(row_data, "commentaires")
        if value is not None:
            text = str(value).strip()
            direct_comment = text or None

    component_keys = _BILAN_COMPONENT_KEYS.get(key)
    if component_keys:
        return _resolve_aggregated_comment(direct_comment, table_lookup, component_keys)

    return direct_comment


def _get_metric_value(results: Dict, suffix: str, table_lookup: Dict[str, Dict], key: str, period: str,
                      legacy_field_bases: Dict[str, str]):
    row_data = table_lookup.get(key)
    direct_value = None
    if row_data:
        direct_value = _get_bilan_table_field(row_data, period)

    component_keys = _COMPTE_RESULTAT_COMPONENT_KEYS.get(key)
    if component_keys:
        subtract_keys = (
            {
                "production_stockee",
                "subventions_exploitation",
                "reprises_exploitation",
                "autres_produits_exploitation",
            }
            if key == "autres_elements"
            else None
        )
        return _resolve_aggregated_value(
            direct_value,
            table_lookup,
            component_keys,
            period,
            subtract_keys=subtract_keys,
        )

    if direct_value is not None:
        return direct_value

    legacy_base = legacy_field_bases.get(key)
    if not legacy_base:
        return None
    return results.get(f"{legacy_base}_{period}{suffix}")


def _get_metric_comment(table_lookup: Dict[str, Dict], key: str) -> Optional[str]:
    row_data = table_lookup.get(key)
    direct_comment = None
    if row_data:
        value = _get_bilan_table_field(row_data, "commentaires")
        if value is not None:
            text = str(value).strip()
            direct_comment = text or None

    component_keys = _COMPTE_RESULTAT_COMPONENT_KEYS.get(key)
    if component_keys:
        subtract_keys = (
            {
                "production_stockee",
                "subventions_exploitation",
                "reprises_exploitation",
                "autres_produits_exploitation",
            }
            if key == "autres_elements"
            else None
        )
        return _resolve_aggregated_comment(
            direct_comment,
            table_lookup,
            component_keys,
            subtract_keys=subtract_keys,
        )

    return direct_comment


def _build_bilan_sheet(wb: Workbook, results: Dict, fields: List[Dict]):
    """Crée l'onglet Bilan avec toutes les entreprises (pattern patrimoine)."""
    companies = _resolve_company_sheets(results)
    if not companies:
        return 0

    ws = wb.create_sheet("Bilan")
    ws.column_dimensions["A"].width = 5
    ws.column_dimensions["B"].width = 35
    ws.column_dimensions["C"].width = 18
    ws.column_dimensions["D"].width = 18
    ws.column_dimensions["E"].width = 30

    filled = 0
    row = 1

    for company_idx, (suffix, company_name) in enumerate(companies):
        if company_idx > 0:
            row += 3  # espace entre entreprises

        # --- Titre entreprise ---
        title_cell = ws.cell(row=row, column=2, value=company_name)
        title_cell.font = Font(bold=True, size=13, color="2F5496")
        row += 1

        # --- Dates ---
        date_n = results.get(f"bilan_date_arrete_n{suffix}", "")
        date_n1 = results.get(f"bilan_date_arrete_n1{suffix}", "")
        col_n_label = f"Au {date_n}" if date_n else "Exercice N"
        col_n1_label = f"Au {date_n1}" if date_n1 else "Exercice N-1"

        # --- En-tête du tableau ---
        headers = ["En €", col_n_label, col_n1_label, "Commentaires"]
        for col_idx, header in enumerate(headers):
            cell = ws.cell(row=row, column=2 + col_idx, value=header)
            cell.font = HEADER_FONT
            cell.fill = HEADER_FILL
            cell.alignment = HEADER_ALIGNMENT
            cell.border = THIN_BORDER
        row += 1

        bilan_table_lookup = {}
        bilan_table_lookup.update(_build_metric_table_lookup(results.get(f"bilan_actif_table{suffix}"), _BILAN_TABLE_KEY_ALIASES))
        bilan_table_lookup.update(_build_metric_table_lookup(results.get(f"bilan_passif_table{suffix}"), _BILAN_TABLE_KEY_ALIASES))

        # --- Helper pour écrire une ligne de données ---
        def _write_data_row(r, label, field_key, font=VALUE_FONT, fill=None, is_formula_n=None, is_formula_n1=None):
            nonlocal filled
            ws.cell(row=r, column=2, value=label).font = font
            ws.cell(row=r, column=2).border = THIN_BORDER

            # Colonne N
            cell_n = ws.cell(row=r, column=3)
            cell_n.border = THIN_BORDER
            if is_formula_n:
                cell_n.value = is_formula_n
                cell_n.number_format = NUMBER_FORMAT_INTEGER
            else:
                val_n = _get_bilan_value(results, suffix, bilan_table_lookup, field_key, "n") if field_key else None
                num_n = _to_number(val_n)
                if num_n is not None:
                    cell_n.value = num_n
                    cell_n.number_format = NUMBER_FORMAT_INTEGER
                    filled += 1
                elif val_n:
                    cell_n.value = _format_display_value(val_n)
                    filled += 1
            cell_n.font = font

            # Colonne N-1
            cell_n1 = ws.cell(row=r, column=4)
            cell_n1.border = THIN_BORDER
            if is_formula_n1:
                cell_n1.value = is_formula_n1
                cell_n1.number_format = NUMBER_FORMAT_INTEGER
            else:
                val_n1 = _get_bilan_value(results, suffix, bilan_table_lookup, field_key, "n1") if field_key else None
                num_n1 = _to_number(val_n1)
                if num_n1 is not None:
                    cell_n1.value = num_n1
                    cell_n1.number_format = NUMBER_FORMAT_INTEGER
                    filled += 1
                elif val_n1:
                    cell_n1.value = _format_display_value(val_n1)
                    filled += 1
            cell_n1.font = font

            # Commentaires
            comment_cell = ws.cell(row=r, column=5)
            comment_cell.border = THIN_BORDER

            if fill:
                for c in range(2, 6):
                    ws.cell(row=r, column=c).fill = fill

        # --- ACTIF ---
        actif_start_row = row
        for label, field_key in _BILAN_ACTIF_ROWS:
            _write_data_row(row, label, field_key)
            row += 1
        actif_end_row = row - 1

        # Total Actif (formule)
        _write_data_row(
            row, "Total Actif", None,
            font=BOLD_FONT,
            is_formula_n=f"=SUM(C{actif_start_row}:C{actif_end_row})",
            is_formula_n1=f"=SUM(D{actif_start_row}:D{actif_end_row})",
        )
        total_actif_row = row
        row += 1

        _write_data_row(row, "Total Actif (source doc)", "total_actif", font=ITALIC_FONT)
        total_actif_source_row = row
        row += 1

        # --- PASSIF détail (Capital social, Résultat) en italique ---
        for label, field_key in _BILAN_PASSIF_DETAIL_ROWS:
            _write_data_row(row, f"          {label}", field_key, font=ITALIC_FONT)
            row += 1

        # --- PASSIF lignes principales ---
        capitaux_propres_row = row
        _write_data_row(row, "Capitaux propres", "capitaux_propres", font=BOLD_FONT)
        row += 1

        dettes_financieres_row = row
        _write_data_row(row, "Dettes financières", "dettes_financieres")
        row += 1

        dettes_exploitation_row = row
        _write_data_row(row, "Dettes d'exploitation", "dettes_exploitation")
        row += 1

        dettes_diverses_row = row
        _write_data_row(row, "Dettes diverses", "dettes_diverses")
        row += 1

        autres_passif_row = row
        _write_data_row(row, "Autres éléments de passif", "autres_passif")
        row += 1

        # Total Passif (formule)
        _write_data_row(
            row, "Total Passif", None,
            font=BOLD_FONT,
            is_formula_n=(
                f"=C{capitaux_propres_row}+C{dettes_financieres_row}"
                f"+C{dettes_exploitation_row}+C{dettes_diverses_row}+C{autres_passif_row}"
            ),
            is_formula_n1=(
                f"=D{capitaux_propres_row}+D{dettes_financieres_row}"
                f"+D{dettes_exploitation_row}+D{dettes_diverses_row}+D{autres_passif_row}"
            ),
        )
        total_passif_row = row
        row += 1

        _write_data_row(row, "Total Passif (source doc)", "total_passif", font=ITALIC_FONT)
        total_passif_source_row = row
        row += 2

        # --- Checks ---
        # Check équilibre : Total Actif == Total Passif
        ws.cell(row=row, column=2, value="Check équilibre").font = RED_FONT
        ws.cell(row=row, column=2).border = THIN_BORDER
        for col, tr_actif, tr_passif in [(3, f"C{total_actif_row}", f"C{total_passif_row}"),
                                          (4, f"D{total_actif_row}", f"D{total_passif_row}")]:
            cell = ws.cell(row=row, column=col, value=f"={tr_actif}={tr_passif}")
            cell.font = RED_FONT
            cell.border = THIN_BORDER
        row += 1

        ws.cell(row=row, column=2, value="Check total actif source").font = RED_FONT
        ws.cell(row=row, column=2).border = THIN_BORDER
        for col, total_col in [(3, "C"), (4, "D")]:
            cell = ws.cell(
                row=row,
                column=col,
                value=f'=IF({total_col}{total_actif_source_row}="",TRUE,{total_col}{total_actif_row}={total_col}{total_actif_source_row})',
            )
            cell.font = RED_FONT
            cell.border = THIN_BORDER
        row += 1

        ws.cell(row=row, column=2, value="Check total passif source").font = RED_FONT
        ws.cell(row=row, column=2).border = THIN_BORDER
        for col, total_col in [(3, "C"), (4, "D")]:
            cell = ws.cell(
                row=row,
                column=col,
                value=f'=IF({total_col}{total_passif_source_row}="",TRUE,{total_col}{total_passif_row}={total_col}{total_passif_source_row})',
            )
            cell.font = RED_FONT
            cell.border = THIN_BORDER
        row += 1

        # Check résultat : Capitaux propres == Capital social + Résultat
        cap_soc_row = capitaux_propres_row - 2  # Capital social = 2 rows above capitaux_propres
        res_row = capitaux_propres_row - 1       # Résultat = 1 row above capitaux_propres
        ws.cell(row=row, column=2, value="Check résultat").font = RED_FONT
        ws.cell(row=row, column=2).border = THIN_BORDER
        for col in [3, 4]:
            cl = get_column_letter(col)
            cell = ws.cell(row=row, column=col,
                           value=f"={cl}{capitaux_propres_row}=({cl}{cap_soc_row}+{cl}{res_row})")
            cell.font = RED_FONT
            cell.border = THIN_BORDER
        row += 1

        row += 3

        # --- Calcul du gearing (formules uniquement) ---
        section_title = ws.cell(row=row, column=2, value="Calcul du Gearing")
        section_title.font = Font(bold=True, underline="single", size=11)
        row += 1

        dettes_bancaires_n = _to_number(_get_bilan_value(results, suffix, bilan_table_lookup, "dettes_bancaires", "n")) or 0
        dettes_bancaires_n1 = _to_number(_get_bilan_value(results, suffix, bilan_table_lookup, "dettes_bancaires", "n1")) or 0

        cp_cca_row = row
        _write_data_row(
            row,
            "Capitaux propres au dernier arrêté",
            None,
            is_formula_n=f"=C{capitaux_propres_row}",
            is_formula_n1=f"=D{capitaux_propres_row}",
        )
        row += 1

        tresorerie_date_row = row
        _write_data_row(
            row,
            "Trésorerie à date",
            None,
            is_formula_n=_formula_indirect_or_zero("Trésorerie", "B5"),
            is_formula_n1=_formula_indirect_or_zero("Trésorerie", "B5"),
        )
        row += 1

        dettes_bancaires_row = row
        _write_data_row(
            row,
            "Dettes bancaires",
            None,
            is_formula_n=_formula_literal_number(dettes_bancaires_n),
            is_formula_n1=_formula_literal_number(dettes_bancaires_n1),
        )
        comment_cell = ws.cell(row=row, column=5, value="A màj éventuellement avec Tableau des opés en cours")
        comment_cell.font = RED_FONT_ITALIC
        comment_cell.border = THIN_BORDER
        row += 1

        gearing_row = row
        _write_data_row(
            row,
            "Gearing",
            None,
            font=BOLD_FONT,
            fill=GEARING_FILL,
            is_formula_n=f'=IFERROR(C{dettes_bancaires_row}/(C{tresorerie_date_row}+C{cp_cca_row}),0)',
            is_formula_n1=f'=IFERROR(D{dettes_bancaires_row}/(D{tresorerie_date_row}+D{cp_cca_row}),0)',
        )
        for col in [2, 3, 4]:
            ws.cell(row=gearing_row, column=col).font = HEADER_FONT
        row += 3

        # --- Calcul du ratio d'endettement post-opé (formules uniquement) ---
        section_title = ws.cell(row=row, column=2, value="Calcul du ratio d'endettement post-opé")
        section_title.font = Font(bold=True, underline="single", size=11)
        row += 1

        ca_precommercialisation_row = row
        _write_data_row(
            row,
            "CA pré-commercialisation",
            None,
            is_formula_n=_formula_indirect_or_zero("Lots", "I17"),
        )
        row += 1

        ca_precommercialisation_opes_row = row
        _write_data_row(
            row,
            "Ca pré-commercialisation opés en cours",
            None,
            is_formula_n="=0",
        )
        comment_cell = ws.cell(row=row, column=5, value="Cf. tableau des opés en cours")
        comment_cell.font = RED_FONT_ITALIC
        comment_cell.border = THIN_BORDER
        row += 1

        ratio_dettes_bancaires_row = row
        _write_data_row(
            row,
            "Dettes bancaires",
            None,
            is_formula_n=f"=C{dettes_bancaires_row}",
        )
        row += 1

        future_raizers_row = row
        _write_data_row(
            row,
            "Future dette de Raizers",
            None,
            is_formula_n=_formula_indirect_or_zero("Financement", "F6"),
        )
        row += 1

        future_bancaire_row = row
        _write_data_row(
            row,
            "Future dette bancaire",
            None,
            is_formula_n=_formula_indirect_or_zero("Financement", "F7"),
        )
        row += 1

        ratio_row = row
        _write_data_row(
            row,
            "Ratio",
            None,
            font=BOLD_FONT,
            fill=GEARING_FILL,
            is_formula_n=(
                f"=IFERROR((C{ca_precommercialisation_row}+C{ca_precommercialisation_opes_row})/"
                f"(C{ratio_dettes_bancaires_row}+C{future_raizers_row}+C{future_bancaire_row}),0)"
            ),
        )
        ws.cell(row=ratio_row, column=3).number_format = "0%"
        for col in [2, 3, 4]:
            ws.cell(row=ratio_row, column=col).font = HEADER_FONT
        row += 1

    logger.info(f"  Bilan : {filled} valeurs remplies pour {len(companies)} entreprise(s)")
    return filled


def _build_compte_resultat_sheet(wb: Workbook, results: Dict):
    """Crée l'onglet Compte de résultat à partir d'une table par société."""
    companies = _resolve_company_sheets(results)
    if not companies:
        return 0

    ws = wb.create_sheet("Compte de résultat")
    ws.column_dimensions["A"].width = 5
    ws.column_dimensions["B"].width = 35
    ws.column_dimensions["C"].width = 18
    ws.column_dimensions["D"].width = 18
    ws.column_dimensions["E"].width = 30

    filled = 0
    row = 1

    for company_idx, (suffix, company_name) in enumerate(companies):
        if company_idx > 0:
            row += 3

        title_cell = ws.cell(row=row, column=2, value=company_name)
        title_cell.font = Font(bold=True, size=13, color="2F5496")
        row += 1

        date_n = results.get(f"bilan_date_arrete_n{suffix}", "")
        date_n1 = results.get(f"bilan_date_arrete_n1{suffix}", "")
        col_n_label = f"Au {date_n}" if date_n else "Exercice N"
        col_n1_label = f"Au {date_n1}" if date_n1 else "Exercice N-1"

        headers = ["En €", col_n_label, col_n1_label, "Commentaires"]
        for col_idx, header in enumerate(headers):
            cell = ws.cell(row=row, column=2 + col_idx, value=header)
            cell.font = HEADER_FONT
            cell.fill = HEADER_FILL
            cell.alignment = HEADER_ALIGNMENT
            cell.border = THIN_BORDER
        row += 1

        table_lookup = _build_metric_table_lookup(
            results.get(f"bilan_compte_resultat_table{suffix}"),
            _COMPTE_RESULTAT_TABLE_KEY_ALIASES,
        )

        def _write_row(r: int, label: str, field_key: Optional[str], font=VALUE_FONT,
                       fill=None, is_formula_n=None, is_formula_n1=None, percent=False):
            nonlocal filled
            ws.cell(row=r, column=2, value=label).font = font
            ws.cell(row=r, column=2).border = THIN_BORDER

            for col, period, formula in [(3, "n", is_formula_n), (4, "n1", is_formula_n1)]:
                cell = ws.cell(row=r, column=col)
                cell.border = THIN_BORDER
                if formula:
                    cell.value = formula
                else:
                    value = _get_metric_value(
                        results, suffix, table_lookup, field_key, period, _COMPTE_RESULTAT_LEGACY_FIELD_BASES
                    ) if field_key else None
                    number = _to_number(value)
                    if number is not None:
                        cell.value = number
                        filled += 1
                    elif value:
                        cell.value = _format_display_value(value)
                        filled += 1
                cell.font = font
                if percent:
                    cell.number_format = "0%"
                else:
                    _apply_numeric_format(cell)

            comment_cell = ws.cell(row=r, column=5)
            comment_cell.border = THIN_BORDER

            if fill:
                for c in range(2, 6):
                    ws.cell(row=r, column=c).fill = fill

        chiffre_affaires_row = row
        _write_row(row, "Chiffre d'affaires", "chiffre_affaires")
        row += 1

        charges_row = row
        _write_row(row, "Charges", "charges")
        row += 1

        salaires_row = row
        _write_row(row, "Salaires et charges sociales", "salaires_charges_sociales")
        row += 1

        impots_taxes_row = row
        _write_row(row, "Impôts et taxes", "impots_taxes")
        row += 1

        dotations_row = row
        _write_row(row, "Dotations aux amortissements", "dotations")
        row += 1

        autres_elements_row = row
        _write_row(row, "Autres éléments", "autres_elements")
        row += 1

        resultat_exploitation_row = row
        _write_row(
            row,
            "Résultat d'exploitation",
            None,
            font=BOLD_FONT,
            is_formula_n=f"=C{chiffre_affaires_row}-SUM(C{charges_row}:C{autres_elements_row})",
            is_formula_n1=f"=D{chiffre_affaires_row}-SUM(D{charges_row}:D{autres_elements_row})",
        )
        row += 1

        _write_row(row, "Résultat d'exploitation (source doc)", "resultat_exploitation", font=ITALIC_FONT)
        resultat_exploitation_source_row = row
        row += 1

        ws.cell(row=row, column=2, value="Check résultat d'exploitation source").font = RED_FONT
        ws.cell(row=row, column=2).border = THIN_BORDER
        for col, total_col in [(3, "C"), (4, "D")]:
            cell = ws.cell(
                row=row,
                column=col,
                value=f'=IF({total_col}{resultat_exploitation_source_row}="",TRUE,{total_col}{resultat_exploitation_row}={total_col}{resultat_exploitation_source_row})',
            )
            cell.font = RED_FONT
            cell.border = THIN_BORDER
        row += 1

        _write_row(
            row,
            "En % du CA",
            None,
            font=ITALIC_FONT,
            is_formula_n=f"=IFERROR(C{resultat_exploitation_row}/C{chiffre_affaires_row},0)",
            is_formula_n1=f"=IFERROR(D{resultat_exploitation_row}/D{chiffre_affaires_row},0)",
            percent=True,
        )
        row += 1

        resultat_financier_row = row
        _write_row(row, "Résultat financier", "resultat_financier")
        row += 1

        resultat_exceptionnel_row = row
        _write_row(row, "Résultat exceptionnel", "resultat_exceptionnel")
        row += 1

        impots_societes_row = row
        _write_row(row, "Impôts sur les sociétés", "impots_sur_les_societes")
        row += 1

        resultat_net_row = row
        _write_row(
            row,
            "Résultat net",
            None,
            font=BOLD_FONT,
            is_formula_n=(
                f"=C{resultat_exploitation_row}+C{resultat_financier_row}"
                f"+C{resultat_exceptionnel_row}-C{impots_societes_row}"
            ),
            is_formula_n1=(
                f"=D{resultat_exploitation_row}+D{resultat_financier_row}"
                f"+D{resultat_exceptionnel_row}-D{impots_societes_row}"
            ),
        )
        row += 1

        _write_row(
            row,
            "En % du CA",
            None,
            font=ITALIC_FONT,
            is_formula_n=f"=IFERROR(C{resultat_net_row}/C{chiffre_affaires_row},0)",
            is_formula_n1=f"=IFERROR(D{resultat_net_row}/D{chiffre_affaires_row},0)",
            percent=True,
        )
        row += 1

    logger.info(f"  Compte de résultat : {filled} valeurs remplies pour {len(companies)} entreprise(s)")
    return filled


def _build_mandats_sheet(wb: Workbook, pappers_mandats: Dict):
    """Crée l'onglet Mandats avec les sociétés Pappers."""
    if not pappers_mandats:
        return 0

    ws = wb.create_sheet("Mandats")
    ws.column_dimensions["A"].width = 5
    ws.column_dimensions["B"].width = 30
    ws.column_dimensions["C"].width = 30
    ws.column_dimensions["D"].width = 36
    ws.column_dimensions["E"].width = 12
    ws.column_dimensions["F"].width = 30
    ws.column_dimensions["G"].width = 12
    ws.column_dimensions["H"].width = 20
    ws.column_dimensions["I"].width = 16
    ws.column_dimensions["J"].width = 15
    ws.column_dimensions["K"].width = 30

    # Header
    headers = ["Société", "Rôle", "Activité", "Création", "Infos financières", "SIREN", "Statut", "Nb représentants", "Détention", "Commentaires"]
    for col_idx, header in enumerate(headers):
        cell = ws.cell(row=1, column=2 + col_idx, value=header)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = HEADER_ALIGNMENT
        cell.border = THIN_BORDER

    filled = 0
    current_row = 3

    for folder_name, folder_payload in pappers_mandats.items():
        if not folder_payload:
            continue

        # Titre du dossier
        folder_cell = ws.cell(row=current_row, column=2, value=folder_name)
        folder_cell.font = Font(bold=True, size=12, color="2F5496")
        current_row += 1

        def _write_company(row: int, company: dict) -> int:
            nonlocal filled

            nom = (company.get("nom_societe") or company.get("societe") or "").strip()
            statut_rcs = (company.get("statut_rcs") or "").strip().lower()
            row_fill = RADIATED_ROW_FILL if _is_radiated_company(company) else None
            role_value = company.get("role")
            if statut_rcs and statut_rcs != "inscrit" and not role_value:
                role_value = "X"

            cell = ws.cell(row=row, column=2, value=nom)
            cell.border = THIN_BORDER
            if row_fill:
                cell.fill = row_fill

            url = _build_pappers_company_url(company)
            if url:
                cell.hyperlink = url
                cell.font = HYPERLINK_FONT
            else:
                cell.font = VALUE_FONT

            values = [
                role_value,
                company.get("activite"),
                company.get("date_creation") or company.get("creation"),
                _build_infos_financieres(company),
                str(company.get("siren") or "").strip() or None,
                _build_statut(company),
                company.get("nb_dirigeants_total"),
                company.get("detention"),
                company.get("commentaires"),
            ]
            for col_idx, val in enumerate(values):
                display_val = _format_display_value(val) if not isinstance(val, (int, float)) else val
                c = ws.cell(row=row, column=3 + col_idx, value=display_val)
                c.font = VALUE_FONT
                c.border = THIN_BORDER
                if row_fill:
                    c.fill = row_fill
                _apply_numeric_format(c, val)
                if val:
                    filled += 1

            if nom:
                filled += 1
            return row + 1

        if isinstance(folder_payload, list):
            for company in folder_payload:
                current_row = _write_company(current_row, company)
            total_cell = ws.cell(row=current_row, column=2, value=f"Total : {len(folder_payload)} société(s)")
            total_cell.font = Font(bold=True, size=10)
            current_row += 2
            continue

        if isinstance(folder_payload, dict):
            for person_name, companies in folder_payload.items():
                if not companies:
                    continue

                person_cell = ws.cell(row=current_row, column=2, value=person_name)
                person_cell.font = Font(bold=True, italic=True, size=10)
                current_row += 1

                for company in companies:
                    current_row = _write_company(current_row, company)

                total_cell = ws.cell(row=current_row, column=2, value=f"Total : {len(companies)} société(s)")
                total_cell.font = Font(bold=True, italic=True, size=9)
                current_row += 2

        current_row += 1

    logger.info(f"  Mandats : {filled} cellules remplies")
    return filled


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def fill_excel(results: Dict, fields: List[Dict], output_dir: Path,
               person_folder_map: Optional[Dict[str, str]] = None,
               pappers_mandats: Optional[Dict[str, List[dict]]] = None) -> Path:
    """Crée un Excel from scratch avec les données extraites."""
    output_path = output_dir / "rapport.xlsx"
    wb = Workbook()

    filled = 0
    filled += _build_operation_sheet(wb, results, fields)
    filled += _build_patrimoine_sheet(wb, results, fields, person_folder_map=person_folder_map)
    filled += _build_bilan_sheet(wb, results, fields)
    filled += _build_compte_resultat_sheet(wb, results)
    filled += _build_mandats_sheet(wb, pappers_mandats or {})

    wb.save(str(output_path))
    logger.info(f"  Excel créé : {filled} cellules -> {output_path}")
    return output_path


# Backward-compatible alias
def fill_excel_template(results: Dict, fields: List[Dict], template_path: str, output_dir: Path,
                        person_folder_map: Optional[Dict[str, str]] = None,
                        pappers_mandats: Optional[Dict[str, List[dict]]] = None):
    return fill_excel(results, fields, output_dir,
                      person_folder_map=person_folder_map,
                      pappers_mandats=pappers_mandats)


def main() -> None:
    parser = argparse.ArgumentParser(description="Crée un Excel depuis extraction_results.json")
    _root = Path(__file__).parent.parent.resolve()
    parser.add_argument(
        "--results",
        default=str(_root / "output" / "extraction_results.json"),
        help="Chemin vers extraction_results.json",
    )
    parser.add_argument(
        "--questions",
        default=str(_root / "config" / "questions.json"),
        help="Chemin vers questions.json",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Dossier de sortie (par defaut: dossier du fichier extraction_results.json)",
    )
    args = parser.parse_args()

    results_path = Path(args.results)
    questions_path = Path(args.questions)
    output_dir = Path(args.output_dir) if args.output_dir else results_path.parent
    manifest_path = output_dir / "manifest.json"

    if not results_path.exists():
        project_hint = output_dir.name if output_dir.name else None
        hint = (
            f" Lance d'abord `python run.py extract --project {project_hint}` puis relance `fill`."
            if project_hint
            else ""
        )
        raise FileNotFoundError(
            f"extraction_results.json introuvable: {results_path}.{hint}"
        )

    if not questions_path.exists():
        raise FileNotFoundError(f"questions.json introuvable: {questions_path}")

    extraction_data = json.loads(results_path.read_text(encoding="utf-8"))
    questions_data = json.loads(questions_path.read_text(encoding="utf-8"))

    person_folder_map = extraction_data.get("person_folders")
    if not person_folder_map:
        person_folder_map = _derive_person_folder_map_from_manifest(manifest_path)

    mandats_results_path = output_dir / "mandats_results.json"

    pappers_mandats = None
    if mandats_results_path.exists():
        mandats_data = json.loads(mandats_results_path.read_text(encoding="utf-8"))
        pappers_mandats = mandats_data.get("societes_par_personne")

    fields = [
        f for f in questions_data["fields"]
        if isinstance(f, dict) and f.get("field_id")
    ]

    fill_excel(
        results=extraction_data["results"],
        fields=fields,
        output_dir=output_dir,
        person_folder_map=person_folder_map,
        pappers_mandats=pappers_mandats,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(message)s",
    )
    main()
