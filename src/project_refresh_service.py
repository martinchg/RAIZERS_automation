from __future__ import annotations

import json
import unicodedata
from pathlib import Path
from typing import Any

from core.normalization import extract_person_folder, path_has_segments
from extract_people_from_casiers import save_people_from_project
from project_catalog_service import compute_catalog_fingerprints, save_project_catalog

ROOT_DIR = Path(__file__).parent.parent.resolve()
OUTPUT_DIR = ROOT_DIR / "output"


def _load_json_if_exists(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_source_path(source_path: str) -> str:
    normalized = unicodedata.normalize("NFC", source_path or "")
    normalized = normalized.replace("\\", "/").strip("/")
    return normalized.casefold()


def _build_file_index(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for file_info in manifest.get("files", []) or []:
        source_path = file_info.get("source_path")
        if not source_path:
            continue
        index[_normalize_source_path(source_path)] = file_info
    return index


def _classify_paths(paths: list[str]) -> dict[str, bool]:
    people_related = False
    financial_related = False

    for source_path in paths:
        if extract_person_folder(source_path):
            people_related = True
        if path_has_segments(source_path, "*Opérateur/*Eléments financiers") or "bilan" in source_path.lower():
            financial_related = True

    return {
        "people_related": people_related,
        "financial_related": financial_related,
    }


def diff_manifests(previous_manifest: dict[str, Any], next_manifest: dict[str, Any]) -> dict[str, Any]:
    previous_index = _build_file_index(previous_manifest or {})
    next_index = _build_file_index(next_manifest or {})

    added = sorted(
        next_index[path].get("source_path", path)
        for path in next_index
        if path not in previous_index
    )
    removed = sorted(
        previous_index[path].get("source_path", path)
        for path in previous_index
        if path not in next_index
    )
    modified = sorted(
        next_index[path].get("source_path", path)
        for path in next_index
        if path in previous_index
        and next_index[path].get("file_size_bytes") != previous_index[path].get("file_size_bytes")
    )

    changed_paths = sorted(set(added + removed + modified))
    flags = _classify_paths(changed_paths)

    return {
        "added": added,
        "removed": removed,
        "modified": modified,
        "changed_paths": changed_paths,
        "summary": {
            "added": len(added),
            "removed": len(removed),
            "modified": len(modified),
            "changed": len(changed_paths),
            **flags,
        },
    }


def refresh_project_state(project_id: str, manifest: dict[str, Any], manifest_diff: dict[str, Any]) -> dict[str, Any]:
    extraction_data = _load_json_if_exists(OUTPUT_DIR / project_id / "extraction_results.json") or {}
    previous_catalog = _load_json_if_exists(OUTPUT_DIR / project_id / "project_catalog.json") or {}
    summary = extraction_data.get("summary") or {}
    people_missing_from_extraction = int(summary.get("persons") or 0) == 0
    current_fingerprints = compute_catalog_fingerprints(manifest or {})
    previous_fingerprints = previous_catalog.get("fingerprints") or {}
    people_fingerprint_changed = current_fingerprints.get("people") != previous_fingerprints.get("people")
    financial_fingerprint_changed = current_fingerprints.get("financial") != previous_fingerprints.get("financial")
    people_file_exists = (OUTPUT_DIR / project_id / "people_from_casiers.json").exists()

    people_recomputed = False
    if (
        current_fingerprints.get("people_input_count", 0) > 0
        and (people_fingerprint_changed or people_missing_from_extraction or not people_file_exists)
    ):
        save_people_from_project(project_id)
        people_recomputed = True

    catalog = save_project_catalog(project_id)
    return {
        "catalog": catalog,
        "people_recomputed": people_recomputed,
        "financial_recomputed": financial_fingerprint_changed,
        "fingerprints": {
            "current": current_fingerprints,
            "previous": previous_fingerprints,
            "people_changed": people_fingerprint_changed,
            "financial_changed": financial_fingerprint_changed,
        },
    }
