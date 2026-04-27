"""Construction des onglets Bilan et Compte de resultat."""

from typing import Dict, List, Optional, Tuple

from openpyxl import Workbook
from openpyxl.styles import Font

from core.excel_utils import (
    BOLD_FONT,
    HEADER_ALIGNMENT,
    HEADER_FILL,
    HEADER_FONT,
    ITALIC_FONT,
    NUMBER_FORMAT_INTEGER,
    RED_FONT,
    THIN_BORDER,
    VALUE_FONT,
    apply_numeric_format,
    format_display_value,
    to_number,
)
from financial.financial_mapping import build_financial_table_lookup, resolve_financial_metric_value

_BILAN_ACTIF_ROWS = (
    ("Immobilisations corporelles", "immobilisations_corporelles", VALUE_FONT),
    ("Immobilisations financières", "immobilisations_financieres", VALUE_FONT),
    ("Stocks", "stocks", VALUE_FONT),
    ("Créances", "creances", VALUE_FONT),
    ("Trésorerie", "tresorerie", VALUE_FONT),
    ("Autres éléments d'actif", "autres_actif", VALUE_FONT),
)

_BILAN_PASSIF_DETAIL_ROWS = (
    ("          Capital social", "capital_social", ITALIC_FONT),
    ("          Résultat", "resultat_exercice", ITALIC_FONT),
)

_BILAN_PASSIF_ROWS = (
    ("Capitaux propres", "capitaux_propres", BOLD_FONT),
    ("Dettes financières", "dettes_financieres", VALUE_FONT),
    ("Dettes d'exploitation", "dettes_exploitation", VALUE_FONT),
    ("Dettes diverses", "dettes_diverses", VALUE_FONT),
    ("Autres éléments de passif", "autres_passif", VALUE_FONT),
)

_COMPTE_RESULTAT_ROWS = (
    ("Chiffre d'affaires", "chiffre_affaires", VALUE_FONT),
    ("Charges", "charges", VALUE_FONT),
    ("Salaires et charges sociales", "salaires_charges_sociales", VALUE_FONT),
    ("Impôts et taxes", "impots_taxes", VALUE_FONT),
    ("Dotations aux amortissements", "dotations", VALUE_FONT),
    ("Autres éléments", "autres_elements", VALUE_FONT),
    ("Résultat d'exploitation", "resultat_exploitation", BOLD_FONT),
    ("Résultat financier", "resultat_financier", VALUE_FONT),
    ("Résultat exceptionnel", "resultat_exceptionnel", VALUE_FONT),
    ("Impôts sur les sociétés", "impots_sur_les_societes", VALUE_FONT),
    ("Résultat net", "resultat_net", BOLD_FONT),
)


def _resolve_company_sheets(results: Dict) -> List[Tuple[str, str]]:
    companies: List[Tuple[str, str]] = []
    for field_id, value in sorted(results.items()):
        if field_id.startswith("bilan_societe_nom") and value:
            suffix = field_id.replace("bilan_societe_nom", "")
            companies.append((suffix, value.strip()))
    return companies


def _configure_sheet(ws) -> None:
    ws.column_dimensions["A"].width = 5
    ws.column_dimensions["B"].width = 35
    ws.column_dimensions["C"].width = 18
    ws.column_dimensions["D"].width = 18
    ws.column_dimensions["E"].width = 30


def _write_headers(ws, row: int, date_n, date_n1) -> int:
    col_n_label = f"Au {date_n}" if date_n else "Exercice N"
    col_n1_label = f"Au {date_n1}" if date_n1 else "Exercice N-1"
    for offset, header in enumerate(("En €", col_n_label, col_n1_label, "Commentaires"), start=2):
        cell = ws.cell(row=row, column=offset, value=header)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = HEADER_ALIGNMENT
        cell.border = THIN_BORDER
    return row + 1


def _write_metric_row(
    ws,
    row: int,
    label: str,
    key: Optional[str],
    font,
    results: Dict,
    suffix: str,
    table_lookup: Dict[str, Dict],
    statement_type: str,
) -> int:
    filled = 0
    label_cell = ws.cell(row=row, column=2, value=label)
    label_cell.font = font
    label_cell.border = THIN_BORDER

    for column, period in ((3, "n"), (4, "n1")):
        cell = ws.cell(row=row, column=column)
        cell.border = THIN_BORDER
        cell.font = font

        value = (
            resolve_financial_metric_value(results, suffix, table_lookup, key, period, statement_type)
            if key
            else None
        )
        number = to_number(value)
        if number is not None:
            cell.value = number
            apply_numeric_format(cell, number)
            filled += 1
        elif value:
            cell.value = format_display_value(value)
            filled += 1

    comment_cell = ws.cell(row=row, column=5)
    comment_cell.border = THIN_BORDER
    return filled


def _write_formula_row(
    ws,
    row: int,
    label: str,
    font,
    formula_n: str,
    formula_n1: str,
    percent: bool = False,
) -> None:
    label_cell = ws.cell(row=row, column=2, value=label)
    label_cell.font = font
    label_cell.border = THIN_BORDER

    for column, formula in ((3, formula_n), (4, formula_n1)):
        cell = ws.cell(row=row, column=column, value=formula)
        cell.font = font
        cell.border = THIN_BORDER
        cell.number_format = "0%" if percent else NUMBER_FORMAT_INTEGER

    ws.cell(row=row, column=5).border = THIN_BORDER


def _combined_bilan_lookup(results: Dict, suffix: str) -> Dict[str, Dict]:
    lookup = build_financial_table_lookup(results.get(f"bilan_actif_table{suffix}"), "bilan_actif_table")
    lookup.update(
        build_financial_table_lookup(results.get(f"bilan_passif_table{suffix}"), "bilan_passif_table")
    )
    return lookup


def _build_bilan_sheet(wb: Workbook, results: Dict, fields: List[Dict]):
    companies = _resolve_company_sheets(results)
    if not companies:
        return 0

    ws = wb.create_sheet("Bilan")
    _configure_sheet(ws)

    filled = 0
    row = 1

    for company_index, (suffix, company_name) in enumerate(companies):
        if company_index > 0:
            row += 3

        title_cell = ws.cell(row=row, column=2, value=company_name)
        title_cell.font = Font(bold=True, size=13, color="2F5496")
        row += 1

        row = _write_headers(
            ws,
            row,
            results.get(f"bilan_date_arrete_n{suffix}", ""),
            results.get(f"bilan_date_arrete_n1{suffix}", ""),
        )

        table_lookup = _combined_bilan_lookup(results, suffix)
        actif_start_row = row
        for label, key, font in _BILAN_ACTIF_ROWS:
            filled += _write_metric_row(ws, row, label, key, font, results, suffix, table_lookup, "bilan")
            row += 1
        actif_end_row = row - 1

        _write_formula_row(
            ws,
            row,
            "Total Actif",
            BOLD_FONT,
            f"=SUM(C{actif_start_row}:C{actif_end_row})",
            f"=SUM(D{actif_start_row}:D{actif_end_row})",
        )
        total_actif_row = row
        row += 1

        filled += _write_metric_row(
            ws,
            row,
            "Total Actif (source doc)",
            "total_actif",
            ITALIC_FONT,
            results,
            suffix,
            table_lookup,
            "bilan",
        )
        total_actif_source_row = row
        row += 1

        for label, key, font in _BILAN_PASSIF_DETAIL_ROWS:
            filled += _write_metric_row(ws, row, label, key, font, results, suffix, table_lookup, "bilan")
            row += 1

        passif_rows = {}
        for label, key, font in _BILAN_PASSIF_ROWS:
            passif_rows[key] = row
            filled += _write_metric_row(ws, row, label, key, font, results, suffix, table_lookup, "bilan")
            row += 1

        _write_formula_row(
            ws,
            row,
            "Total Passif",
            BOLD_FONT,
            (
                f"=C{passif_rows['capitaux_propres']}+C{passif_rows['dettes_financieres']}"
                f"+C{passif_rows['dettes_exploitation']}+C{passif_rows['dettes_diverses']}"
                f"+C{passif_rows['autres_passif']}"
            ),
            (
                f"=D{passif_rows['capitaux_propres']}+D{passif_rows['dettes_financieres']}"
                f"+D{passif_rows['dettes_exploitation']}+D{passif_rows['dettes_diverses']}"
                f"+D{passif_rows['autres_passif']}"
            ),
        )
        total_passif_row = row
        row += 1

        filled += _write_metric_row(
            ws,
            row,
            "Total Passif (source doc)",
            "total_passif",
            ITALIC_FONT,
            results,
            suffix,
            table_lookup,
            "bilan",
        )
        total_passif_source_row = row
        row += 2

        check_labels = (
            ("Check équilibre", f"=C{total_actif_row}=C{total_passif_row}", f"=D{total_actif_row}=D{total_passif_row}"),
            (
                "Check total actif source",
                f'=IF(C{total_actif_source_row}="",TRUE,C{total_actif_row}=C{total_actif_source_row})',
                f'=IF(D{total_actif_source_row}="",TRUE,D{total_actif_row}=D{total_actif_source_row})',
            ),
            (
                "Check total passif source",
                f'=IF(C{total_passif_source_row}="",TRUE,C{total_passif_row}=C{total_passif_source_row})',
                f'=IF(D{total_passif_source_row}="",TRUE,D{total_passif_row}=D{total_passif_source_row})',
            ),
            (
                "Check résultat",
                (
                    f"=C{passif_rows['capitaux_propres']}="
                    f"(C{passif_rows['capitaux_propres'] - 2}+C{passif_rows['capitaux_propres'] - 1})"
                ),
                (
                    f"=D{passif_rows['capitaux_propres']}="
                    f"(D{passif_rows['capitaux_propres'] - 2}+D{passif_rows['capitaux_propres'] - 1})"
                ),
            ),
        )
        for label, formula_n, formula_n1 in check_labels:
            label_cell = ws.cell(row=row, column=2, value=label)
            label_cell.font = RED_FONT
            label_cell.border = THIN_BORDER
            for column, formula in ((3, formula_n), (4, formula_n1)):
                cell = ws.cell(row=row, column=column, value=formula)
                cell.font = RED_FONT
                cell.border = THIN_BORDER
            row += 1

        row += 1

    return filled


def _build_compte_resultat_sheet(wb: Workbook, results: Dict):
    companies = _resolve_company_sheets(results)
    if not companies:
        return 0

    ws = wb.create_sheet("Compte de résultat")
    _configure_sheet(ws)

    filled = 0
    row = 1

    for company_index, (suffix, company_name) in enumerate(companies):
        if company_index > 0:
            row += 3

        title_cell = ws.cell(row=row, column=2, value=company_name)
        title_cell.font = Font(bold=True, size=13, color="2F5496")
        row += 1

        row = _write_headers(
            ws,
            row,
            results.get(f"bilan_date_arrete_n{suffix}", ""),
            results.get(f"bilan_date_arrete_n1{suffix}", ""),
        )

        table_lookup = build_financial_table_lookup(
            results.get(f"bilan_compte_resultat_table{suffix}"),
            "bilan_compte_resultat_table",
        )

        row_positions = {}
        for label, key, font in _COMPTE_RESULTAT_ROWS:
            row_positions[key] = row
            filled += _write_metric_row(
                ws,
                row,
                label,
                key,
                font,
                results,
                suffix,
                table_lookup,
                "compte_resultat",
            )
            row += 1

            if key in {"resultat_exploitation", "resultat_net"}:
                chiffre_affaires_row = row_positions["chiffre_affaires"]
                formula_row = row
                _write_formula_row(
                    ws,
                    formula_row,
                    "En % du CA",
                    ITALIC_FONT,
                    f"=IFERROR(C{row_positions[key]}/C{chiffre_affaires_row},0)",
                    f"=IFERROR(D{row_positions[key]}/D{chiffre_affaires_row},0)",
                    percent=True,
                )
                row += 1

    return filled
