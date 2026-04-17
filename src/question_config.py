"""Chargement centralise des fichiers de questions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

DEFAULT_SPLIT_FILENAMES = (
    "questions_operateur.json",
    "questions_patrimoine.json",
    "questions_finance.json",
)
LEGACY_FILENAME = "questions.json"


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
