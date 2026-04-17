"""Chargement centralise des fichiers de questions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Set

DEFAULT_SPLIT_FILENAMES = (
    "questions_operateur.json",
    "questions_patrimoine.json",
    "questions_finance.json",
)
LEGACY_FILENAME = "questions.json"
SHARED_FINANCIAL_FIELD_IDS = {
    "bilan_societe_nom",
    "bilan_date_arrete_n",
    "bilan_date_arrete_n1",
}
BILAN_FIELD_IDS = {
    "bilan_actif_table",
    "bilan_passif_table",
}
COMPTE_RESULTAT_FIELD_IDS = {
    "bilan_compte_resultat_table",
}


def discover_question_files(config_dir: Path, explicit_path: Optional[str] = None) -> List[Path]:
    if explicit_path:
        return [Path(explicit_path)]

    split_paths = [config_dir / name for name in DEFAULT_SPLIT_FILENAMES if (config_dir / name).exists()]
    if split_paths:
        return split_paths

    legacy_path = config_dir / LEGACY_FILENAME
    if legacy_path.exists():
        return [legacy_path]

    return []


def load_questions_config(config_dir: Path, explicit_path: Optional[str] = None) -> Dict:
    question_files = discover_question_files(config_dir, explicit_path=explicit_path)
    if not question_files:
        target = explicit_path or str(config_dir / LEGACY_FILENAME)
        raise FileNotFoundError(f"Fichier(s) de questions introuvable(s): {target}")

    merged_fields: List[Dict] = []
    seen_field_ids: Dict[str, Path] = {}
    source_files: List[str] = []

    for question_file in question_files:
        if not question_file.exists():
            raise FileNotFoundError(f"Fichier de questions introuvable: {question_file}")

        data = json.loads(question_file.read_text(encoding="utf-8"))
        fields = data.get("fields", [])
        if not isinstance(fields, list):
            raise ValueError(f"Format invalide dans {question_file}: 'fields' doit etre une liste")

        source_files.append(str(question_file))
        # derive a short source tag from the filename (e.g. "operateur", "patrimoine", "finance")
        stem = question_file.stem  # e.g. "questions_operateur"
        source_tag = stem.split("_", 1)[1] if "_" in stem else stem

        for field in fields:
            if not isinstance(field, dict) or not field.get("field_id"):
                continue
            field_id = str(field["field_id"])
            if field_id in seen_field_ids:
                first_file = seen_field_ids[field_id]
                raise ValueError(
                    f"field_id duplique '{field_id}' entre {first_file.name} et {question_file.name}"
                )
            seen_field_ids[field_id] = question_file
            field = dict(field)
            field.setdefault("_source", source_tag)
            merged_fields.append(field)

    return {
        "fields": merged_fields,
        "source_files": source_files,
    }


def load_question_fields(config_dir: Path, explicit_path: Optional[str] = None) -> List[Dict]:
    return load_questions_config(config_dir, explicit_path=explicit_path)["fields"]


def field_targets_excel_tabs(field: Dict) -> Set[str]:
    """Associe un champ de config aux onglets Excel qu'il alimente."""
    excel_sheet = field.get("excel_sheet")
    field_id = str(field.get("field_id") or "")

    if excel_sheet == "{person_name}":
        return {"patrimoine"}

    if excel_sheet == "{company_name}":
        if field_id in SHARED_FINANCIAL_FIELD_IDS:
            return {"bilan", "compte_resultat"}
        if field_id in BILAN_FIELD_IDS:
            return {"bilan"}
        if field_id in COMPTE_RESULTAT_FIELD_IDS:
            return {"compte_resultat"}
        return {"bilan", "compte_resultat"}

    return {"operation"}


def filter_fields_for_excel_tabs(
    fields: List[Dict],
    *,
    include_operation: bool = True,
    include_patrimoine: bool = True,
    include_bilan: bool = True,
    include_compte_resultat: bool = True,
) -> List[Dict]:
    enabled_tabs = set()
    if include_operation:
        enabled_tabs.add("operation")
    if include_patrimoine:
        enabled_tabs.add("patrimoine")
    if include_bilan:
        enabled_tabs.add("bilan")
    if include_compte_resultat:
        enabled_tabs.add("compte_resultat")

    filtered_fields: List[Dict] = []
    for field in fields:
        if not isinstance(field, dict) or not field.get("field_id"):
            continue
        if field_targets_excel_tabs(field) & enabled_tabs:
            filtered_fields.append(field)

    return filtered_fields
