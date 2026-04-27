import argparse
import json
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_SRC_DIR = Path(__file__).parent.parent.resolve()
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from core.normalization import canonical_name

ROOT_DIR = _SRC_DIR.parent.resolve()
DEFAULT_INPUT_PATH = ROOT_DIR / "output" / "entreprise_comptes_900614082_2022.json"

_PRIMARY_N_LABELS = {
    "actif": ["Net année N", "Total année N", "Montant année N"],
    "passif": ["Montant année N", "Total année N", "Net année N"],
    "compte_resultat": ["Total année N", "Montant année N", "Net année N"],
    "annexe": [
        "Montant année N",
        "Montant fin d’exercice",
        "Montant fin d'exercice",
        "Net année N",
        "Total année N",
        "Montant brut",
    ],
}

_PRIMARY_N1_LABELS = {
    "actif": ["Net année N-1", "Total année N-1", "Montant année N-1"],
    "passif": ["Montant année N-1", "Total année N-1", "Net année N-1"],
    "compte_resultat": ["Total année N-1", "Montant année N-1", "Net année N-1"],
    "annexe": [
        "Montant année N-1",
        "Net année N-1",
        "Total année N-1",
    ],
}


def _slugify(label: str) -> str:
    if not label:
        return ""
    text = label.strip().lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(char for char in text if unicodedata.category(char) != "Mn")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def _get_primary_value(
    column_values: Dict[str, Any],
    section_side: str,
    *,
    previous_year: bool,
) -> Tuple[Optional[str], Any]:
    label_candidates = _PRIMARY_N1_LABELS if previous_year else _PRIMARY_N_LABELS
    for label in label_candidates.get(section_side, []):
        if label in column_values:
            return label, column_values.get(label)
    return None, None


def _detect_statement(section_label: str, code: Optional[str]) -> Tuple[str, str]:
    canon = canonical_name(section_label)

    if "compte de resultat" in canon:
        return "compte_resultat", "compte_resultat"

    if "actif" in canon and "passif" not in canon:
        return "bilan", "actif"

    if "passif" in canon and "actif" not in canon:
        return "bilan", "passif"

    if "bilan" in canon:
        try:
            numeric_code = int(str(code))
        except (TypeError, ValueError):
            numeric_code = None

        if numeric_code is not None:
            if numeric_code < 120:
                return "bilan", "actif"
            if numeric_code <= 180:
                return "bilan", "passif"
            return "annexe", "annexe"

        return "bilan", "bilan"

    return "annexe", "annexe"


def _compact_row(code: Optional[str], poste_source: str, n_value: Any, n1_value: Any) -> Dict[str, Any]:
    return {
        "c": code,
        "l": poste_source,
        "n": n_value,
        "n1": n1_value,
    }


def flatten_pappers_comptes(
    payload: Dict[str, Any],
    source_path: Optional[Path] = None,
    *,
    detailed: bool = False,
) -> Dict[str, Any]:
    entries_out: List[Dict[str, Any]] = []
    row_count = 0

    for annee, entries in payload.items():
        if not isinstance(entries, list):
            continue

        for entry_index, entry in enumerate(entries):
            grouped_entry: Dict[str, Any] = {
                "y": annee,
                "i": entry_index,
                "cloture": entry.get("date_cloture"),
                "cloture_n1": entry.get("date_cloture_n-1"),
                "type": entry.get("libelle_type_comptes") or entry.get("type_comptes"),
                "actif": [],
                "passif": [],
                "compte_resultat": [],
                "annexe": [],
            }
            sections = entry.get("sections") or []

            for section_index, section in enumerate(sections):
                section_label = section.get("libelle") or ""
                liasses = section.get("liasses") or []

                for liasse_index, liasse in enumerate(liasses):
                    code = liasse.get("code")
                    poste_source = liasse.get("libelle") or ""
                    row_statement_type, row_section_side = _detect_statement(section_label, code)

                    raw_columns: Dict[str, Any] = {}
                    normalized_columns: Dict[str, Any] = {}

                    for column in liasse.get("colonnes") or []:
                        column_label = column.get("libelle") or ""
                        value = column.get("valeur")
                        raw_columns[column_label] = value
                        normalized_columns[_slugify(column_label)] = value

                    n_label, n_value = _get_primary_value(raw_columns, row_section_side, previous_year=False)
                    n1_label, n1_value = _get_primary_value(raw_columns, row_section_side, previous_year=True)
                    row_count += 1

                    row = _compact_row(code, poste_source, n_value, n1_value)
                    if detailed:
                        row.update(
                            {
                                "statement_type": row_statement_type,
                                "statement_side": row_section_side,
                                "section_index": section_index,
                                "liasse_index": liasse_index,
                                "section_libelle": section_label,
                                "section_slug": _slugify(section_label),
                                "poste_slug": _slugify(poste_source),
                                "n_label": n_label,
                                "n1_label": n1_label,
                                "raw_columns": raw_columns,
                                "normalized_columns": normalized_columns,
                            }
                        )

                    grouped_entry[row_section_side].append(row)

            entries_out.append(grouped_entry)

    return {
        "source_file": str(source_path) if source_path else None,
        "entry_count": len(entries_out),
        "row_count": row_count,
        "years": sorted(payload.keys()),
        "entries": entries_out,
    }


def _default_output_path(input_path: Path) -> Path:
    if input_path.suffix.lower() == ".json":
        return input_path.with_name(f"{input_path.stem}_flattened.json")
    return input_path.with_name(f"{input_path.name}_flattened.json")


def main() -> None:
    parser = argparse.ArgumentParser(description="Aplatit une réponse Pappers /entreprise/comptes en lignes homogènes")
    parser.add_argument(
        "--input",
        "-i",
        default=str(DEFAULT_INPUT_PATH),
        help="Chemin du JSON Pappers /entreprise/comptes",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Chemin de sortie JSON aplati (par défaut: <input>_flattened.json)",
    )
    parser.add_argument(
        "--detailed",
        action="store_true",
        help="Inclut les métadonnées verbeuses et les colonnes brutes pour debug",
    )
    args = parser.parse_args()

    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve() if args.output else _default_output_path(input_path)

    payload = json.loads(input_path.read_text(encoding="utf-8"))
    flattened = flatten_pappers_comptes(payload, source_path=input_path, detailed=args.detailed)
    output_path.write_text(json.dumps(flattened, ensure_ascii=False, indent=2), encoding="utf-8")

    print(output_path)
    print(f"rows={flattened['row_count']}")


if __name__ == "__main__":
    main()
