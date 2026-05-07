from __future__ import annotations

from pathlib import Path
import re

from dropbox.files import FileMetadata, FolderMetadata

from core.normalization import matches_pattern
from dropbox_client import get_client

EN_AUDIT_PATTERNS = ["en audit", "*en audit*", "audit", "*audit*"]
AUDIT_PATTERNS = ["audit", "*audit", "audit*", "*audit*"]
OPERATEUR_PATTERNS = ["operateur", "*operateur", "operateur*", "*operateur*"]


def slugify(text: str) -> str:
    text = text.strip("/").lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def get_dropbox_client():
    return get_client()


def list_dropbox_entries(path: str) -> tuple[list[str], list[str]]:
    dbx = get_dropbox_client()
    result = dbx.files_list_folder(path, recursive=False)
    folders: list[str] = []
    files: list[str] = []

    while True:
        for entry in result.entries:
            if isinstance(entry, FolderMetadata):
                folders.append(entry.name)
            elif isinstance(entry, FileMetadata):
                files.append(entry.name)
        if not result.has_more:
            break
        result = dbx.files_list_folder_continue(result.cursor)

    return sorted(folders), sorted(files)


def find_audit_root(max_depth: int = 6) -> str:
    dbx = get_dropbox_client()
    last_error: Exception | None = None

    def walk(path: str, depth: int) -> str | None:
        nonlocal last_error
        if depth > max_depth:
            return None
        try:
            result = dbx.files_list_folder(path, recursive=False)
            entries = []
            while True:
                entries.extend(result.entries)
                if not result.has_more:
                    break
                result = dbx.files_list_folder_continue(result.cursor)

            for entry in entries:
                if isinstance(entry, FolderMetadata) and any(
                    matches_pattern(entry.name, pattern) for pattern in EN_AUDIT_PATTERNS
                ):
                    return entry.path_display

            for entry in entries:
                if isinstance(entry, FolderMetadata):
                    found = walk(entry.path_display, depth + 1)
                    if found:
                        return found
        except Exception as exc:
            last_error = exc
        return None

    found = walk("", 0)
    if found:
        return found
    if last_error is not None:
        raise RuntimeError(f"Dropbox inaccessible pendant la recherche du dossier audit: {last_error}")
    raise RuntimeError("Impossible de trouver un dossier 'En audit' dans Dropbox.")


def list_projects() -> dict[str, object]:
    audit_root = find_audit_root()
    folders, _ = list_dropbox_entries(audit_root)
    items = [
        {
            "id": slugify(f"{audit_root}/{folder}"),
            "name": folder,
            "path": f"{audit_root}/{folder}",
        }
        for folder in folders
    ]
    return {
        "root_path": audit_root,
        "items": items,
    }


def find_audit_folder(project_path: str, max_depth: int = 6) -> str | None:
    def walk(path: str, depth: int) -> str | None:
        folders, _ = list_dropbox_entries(path)
        for name in folders:
            if any(matches_pattern(name, pattern) for pattern in AUDIT_PATTERNS):
                return f"{path}/{name}"
        if depth < max_depth:
            for name in folders:
                found = walk(f"{path}/{name}", depth + 1)
                if found:
                    return found
        return None

    return walk(project_path, 0)


def list_audit_subfolders(project_path: str) -> list[str]:
    audit_path = find_audit_folder(project_path, max_depth=6)
    if not audit_path:
        return []

    folders, _ = list_dropbox_entries(audit_path)
    return [
        folder_name
        for folder_name in folders
        if not any(matches_pattern(folder_name, pattern) for pattern in OPERATEUR_PATTERNS)
    ]


def read_manifest_stats(project_id: str, root_dir: Path) -> dict[str, object] | None:
    manifest_path = root_dir / "output" / project_id / "manifest.json"
    if not manifest_path.exists():
        return None

    import json

    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    stats = data.get("stats", {})
    return {
        "project_id": project_id,
        "selected_audit_folder": data.get("selected_audit_folder"),
        "files_found": stats.get("files_found"),
        "files_in_scope": stats.get("files_in_scope"),
        "files_processed": stats.get("files_processed"),
        "files_reused_from_cache": stats.get("files_reused_from_cache"),
        "files_ready": stats.get("files_ready"),
        "total_tokens": stats.get("total_tokens"),
    }
