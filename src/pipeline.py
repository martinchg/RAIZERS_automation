import argparse
import contextlib
import importlib
import json
import logging
import os
import re
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

# Permettre l'exécution directe : python src/pipeline.py
_SRC_DIR = Path(__file__).parent.resolve()
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

ROOT_DIR = _SRC_DIR.parent.resolve()
from runtime_config import configure_environment

configure_environment(ROOT_DIR)

from tqdm import tqdm

import dropbox
from dropbox.files import FolderMetadata

import dropbox_client as dropbox_client_module
from ingestion import extract
from chunking import build_parents, make_document_id
from normalization import (
    canonical_name,
    find_folder_by_canonical,
    is_archived_path,
    iter_direct_subfolders,
    matches_pattern,
    path_has_segments,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OUTPUT_DIR = ROOT_DIR / "output"
LOCAL_CACHE = ROOT_DIR / "cache"
QUESTIONS_PATH = ROOT_DIR / "config" / "questions.json"

SUPPORTED_EXT = {".pdf", ".docx", ".txt", ".pptx", ".xlsx", ".xls", ".md", ".ppt"}
PARENT_SIZE = 2000
DEFAULT_DOC_TIMEOUT_SECONDS = int(os.environ.get("PIPELINE_DOC_TIMEOUT_SECONDS", "90"))
ENABLE_PIPELINE_PREFILTER = os.environ.get("PIPELINE_ENABLE_PREFILTER", "1").strip().lower() not in {"0", "false", "no", "off"}

AUDIT_PATTERNS = ["audit", "*audit", "audit*", "*audit*"]
OPERATEUR_PATTERNS = ["operateur", "*operateur", "operateur*", "*operateur*"]

PIPELINE_ALWAYS_KEEP_HINTS = {
    "kbis",
    "statut",
    "statuts",
    "contrat obligataire",
    "contrat",
    "gfa",
    "avenant",
    "garantie",
    "hypotheque",
    "promesse de vente",
    "compromis",
    "pv",
    "proces verbal",
    "devis",
    "bilan",
    "comptes annuels",
    "liasse fiscale",
    "etats financiers",
    "attestation patrimoniale",
    "fiche patrimoniale",
    "patrimoniale",
    "casier judiciaire",
    "extrait de casier",
    "bulletin n3",
    "bulletin numero 3",
    "avis d impot",
    "carte identite",
    "piece identite",
    "organigramme",
    "planning",
    "track record",
}
PIPELINE_AUDIT_FOLDER_HINTS = {
    "acquisition",
    "garantie",
    "garanties",
    "hypotheque",
    "construction",
    "travaux",
    "elements techniques",
    "elements financiers",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    text = text.strip("/").lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")



def write_jsonl(path: Path, records: List[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    logger.info(f"  📝 {path.name}: {len(records)} lignes")



def _matches_any_pattern(name: str, patterns: List[str]) -> bool:
    canon = canonical_name(name)
    return any(matches_pattern(name, p) or matches_pattern(canon, p) for p in patterns)



def _load_question_fields() -> List[dict]:
    if not QUESTIONS_PATH.exists():
        logger.warning(f"Questions introuvables : {QUESTIONS_PATH}")
        return []

    with open(QUESTIONS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    return [
        field for field in data.get("fields", [])
        if isinstance(field, dict) and field.get("field_id")
    ]


def _load_manual_exclude_patterns() -> List[str]:
    raw = os.environ.get("PIPELINE_EXCLUDE_PATTERNS", "")
    return [pattern.strip() for pattern in raw.split(",") if pattern.strip()]


def _matches_loose_pattern(value: str, pattern: str) -> bool:
    normalized_value = canonical_name(value.replace("/", " ").replace("\\", " "))
    normalized_pattern = canonical_name(pattern.replace("*", " "))
    return bool(normalized_pattern) and normalized_pattern in normalized_value


def _contains_canonical_phrase(haystack: str, phrase: str) -> bool:
    normalized_phrase = canonical_name(phrase)
    if not normalized_phrase:
        return False
    regex = r"(?<!\w)" + re.escape(normalized_phrase).replace(r"\ ", r"\s+") + r"(?!\w)"
    return re.search(regex, haystack) is not None


def _resolve_source_dirs(fields: List[dict], selected_audit_folder: Optional[str]) -> List[str]:
    resolved_dirs: List[str] = []
    for field in fields:
        for raw_dir in field.get("source_dirs", []):
            if "{selected_audit_folder}" in raw_dir:
                if not selected_audit_folder:
                    continue
                resolved = raw_dir.replace("{selected_audit_folder}", selected_audit_folder)
            else:
                resolved = raw_dir
            if resolved not in resolved_dirs:
                resolved_dirs.append(resolved)
    return resolved_dirs


def _is_specific_source_dir(path_pattern: str) -> bool:
    informative_segments = [
        canonical_name(segment.replace("*", ""))
        for segment in path_pattern.split("/")
        if canonical_name(segment.replace("*", ""))
    ]
    if len(informative_segments) < 3:
        return False
    return informative_segments[-1] not in {
        "audit",
        "operateur",
        "rh",
        "ressources humaines",
        "elements juridiques",
        "elements financiers",
    }


def _matching_specific_source_dirs(source_path: str, resolved_source_dirs: List[str]) -> List[str]:
    return [
        path_pattern
        for path_pattern in resolved_source_dirs
        if _is_specific_source_dir(path_pattern) and path_has_segments(source_path, path_pattern)
    ]


def _matching_keep_hints(source_path: str, filename: str) -> List[str]:
    haystack = canonical_name(f"{Path(filename).stem} {source_path.replace('/', ' ')}")
    hits = [
        hint
        for hint in PIPELINE_ALWAYS_KEEP_HINTS
        if _contains_canonical_phrase(haystack, hint)
    ]
    return sorted(set(hits))


def _is_in_selected_audit_folder(source_path: str, selected_audit_folder: Optional[str]) -> bool:
    if not selected_audit_folder:
        return False
    return path_has_segments(source_path, f"*Audit/{selected_audit_folder}")


def _matching_audit_folder_hints(source_path: str, selected_audit_folder: Optional[str]) -> List[str]:
    if not _is_in_selected_audit_folder(source_path, selected_audit_folder):
        return []
    path_canon = canonical_name(source_path.replace("/", " "))
    hits = [
        hint
        for hint in PIPELINE_AUDIT_FOLDER_HINTS
        if _contains_canonical_phrase(path_canon, hint)
    ]
    return sorted(set(hits))


def _is_manually_excluded(source_path: str, filename: str, patterns: List[str]) -> bool:
    if not patterns:
        return False

    candidates = [source_path, filename, Path(filename).stem]
    return any(
        _matches_loose_pattern(candidate, pattern)
        for candidate in candidates
        for pattern in patterns
    )


def prefilter_files_for_extraction(
    files: List[Path],
    project_local_root: Path,
    selected_audit_folder: Optional[str] = None,
) -> List[Path]:
    if not files:
        return []

    manual_excludes = _load_manual_exclude_patterns()
    manually_filtered: List[Path] = []
    manually_skipped = 0

    for file_path in files:
        source_path = compute_source_path(file_path, project_local_root)
        if _is_manually_excluded(source_path, file_path.name, manual_excludes):
            manually_skipped += 1
            continue
        manually_filtered.append(file_path)

    if manual_excludes:
        logger.info(
            "  🚫 Exclusions manuelles : %s fichier(s) ignoré(s) via PIPELINE_EXCLUDE_PATTERNS=%s",
            manually_skipped,
            ",".join(manual_excludes),
        )

    fields = _load_question_fields()
    if not fields:
        return manually_filtered

    resolved_source_dirs = _resolve_source_dirs(fields, selected_audit_folder)
    try:
        from extract_structured import match_questions_to_doc
    except Exception:
        match_questions_to_doc = None
    try:
        from extract_people_from_casiers import _is_casier_judiciaire_filename
    except Exception:
        _is_casier_judiciaire_filename = None

    relevant_files: List[Path] = []
    kept_reasons: List[str] = []
    kept_by_questions = 0
    kept_by_source_dir = 0
    kept_by_doc_family = 0
    kept_by_audit_folder = 0
    skipped_by_prefilter = 0

    for file_path in manually_filtered:
        source_path = compute_source_path(file_path, project_local_root)
        doc_info = {
            "filename": file_path.name,
            "source_path": source_path,
            "file_type": file_path.suffix.lstrip(".").lower(),
        }
        reasons: List[str] = []

        matched_fields = (
            match_questions_to_doc(doc_info, fields, selected_audit_folder)
            if match_questions_to_doc else []
        )
        if matched_fields:
            kept_by_questions += 1
            reasons.append(f"questions:{len(matched_fields)}")

        specific_dir_hits = _matching_specific_source_dirs(source_path, resolved_source_dirs)
        if specific_dir_hits:
            kept_by_source_dir += 1
            reasons.append("dossier_source")

        is_casier = bool(
            _is_casier_judiciaire_filename
            and _is_casier_judiciaire_filename(file_path.name)
        )
        if is_casier:
            kept_by_doc_family += 1
            reasons.append("famille:casier")

        keep_hint_hits = _matching_keep_hints(source_path, file_path.name)
        if keep_hint_hits:
            kept_by_doc_family += 1
            reasons.append(f"famille:{', '.join(keep_hint_hits[:2])}")

        audit_folder_hits = _matching_audit_folder_hints(source_path, selected_audit_folder)
        if audit_folder_hits:
            kept_by_audit_folder += 1
            reasons.append(f"dossier_audit:{', '.join(audit_folder_hits[:2])}")

        if reasons:
            relevant_files.append(file_path)
            kept_reasons.append(f"{source_path} [{'; '.join(reasons)}]")
            continue

        skipped_by_prefilter += 1

    logger.info(
        "  🎯 Préfiltrage pipeline : %s/%s fichier(s) gardé(s), %s ignoré(s)",
        len(relevant_files),
        len(manually_filtered),
        skipped_by_prefilter,
    )
    logger.info(
        "    ↳ raisons : questions=%s, dossiers_source=%s, familles=%s, dossiers_audit=%s",
        kept_by_questions,
        kept_by_source_dir,
        kept_by_doc_family,
        kept_by_audit_folder,
    )
    for line in kept_reasons[:15]:
        logger.info("    ✅ %s", line)
    return relevant_files


def _folder_debug_listing(dbx, path: str) -> List[str]:
    try:
        return [f"{child.name} | {child.path_display}" for child in _list_subfolders(dbx, path)]
    except Exception as exc:
        return [f"<erreur listing {path}: {exc}>"]


def _list_subfolders(dbx: dropbox.Dropbox, dropbox_folder: str) -> List[FolderMetadata]:
    folders: List[FolderMetadata] = []
    result = dbx.files_list_folder(dropbox_folder, recursive=False)
    while True:
        for entry in result.entries:
            if isinstance(entry, FolderMetadata):
                folders.append(entry)
        if not result.has_more:
            break
        result = dbx.files_list_folder_continue(result.cursor)
    return sorted(folders, key=lambda folder: folder.name.lower())


def _get_dropbox_client() -> dropbox.Dropbox:
    module = importlib.reload(dropbox_client_module)
    return module.get_client()


def _sync_dropbox_folders(
    dropbox_folders: List[str],
    local_dir: str | Path,
    *,
    recursive: bool = True,
    dbx: dropbox.Dropbox | None = None,
) -> List[Path]:
    module = importlib.reload(dropbox_client_module)

    sync_many = getattr(module, "sync_folders", None)
    if callable(sync_many):
        return sync_many(
            dropbox_folders=dropbox_folders,
            local_dir=local_dir,
            recursive=recursive,
            dbx=dbx,
        )

    sync_one = getattr(module, "sync_folder", None)
    if callable(sync_one):
        downloaded: List[Path] = []
        for folder in dropbox_folders:
            downloaded.extend(sync_one(folder, local_dir=local_dir, recursive=recursive))
        return downloaded

    list_files = getattr(module, "list_files", None)
    download_files = getattr(module, "download_files", None)
    get_client = getattr(module, "get_client", None)
    if callable(list_files) and callable(download_files) and callable(get_client):
        client = dbx or get_client()
        all_files = []
        for folder in dropbox_folders:
            all_files.extend(list_files(client, folder, recursive=recursive))
        return download_files(client, all_files, local_dir)

    raise ImportError("dropbox_client ne fournit ni sync_folders, ni sync_folder, ni list_files/download_files")


def _find_audit_base(project_local_root: Path) -> Optional[Path]:
    for pattern in AUDIT_PATTERNS:
        found = find_folder_by_canonical(
            project_local_root,
            pattern,
            recursive=True,
            max_depth=6,
        )
        if found is not None:
            return found
    return None



def _find_operateur_folder(audit_base: Path) -> Optional[Path]:
    for child in iter_direct_subfolders(audit_base):
        if _matches_any_pattern(child.name, OPERATEUR_PATTERNS):
            return child
    return None



def _dropbox_child_path(parent: str, child_name: str) -> str:
    parent = parent.rstrip("/")
    return f"{parent}/{child_name}" if parent else f"/{child_name}"



def _find_dropbox_folder_by_patterns(
    dbx,
    base_path: str,
    patterns: List[str],
    *,
    recursive: bool = False,
    max_depth: int = 1,
) -> Optional[str]:
    def _walk(path: str, depth: int) -> Optional[str]:
        children = _list_subfolders(dbx, path)
        logger.info(
            "[Dropbox scan] path=%r depth=%s children=%s",
            path,
            depth,
            [child.name for child in children],
        )

        # 1) match direct sur les enfants du niveau courant
        for child in children:
            if _matches_any_pattern(child.name, patterns):
                resolved = child.path_display or _dropbox_child_path(path, child.name)
                logger.info("[Dropbox match] %r -> %s", child.name, resolved)
                return resolved

        # 2) descente récursive
        if recursive and depth < max_depth:
            for child in children:
                child_path = child.path_display or _dropbox_child_path(path, child.name)
                found = _walk(child_path, depth + 1)
                if found:
                    return found
        return None

    return _walk(base_path, 0)



def _list_available_audit_folders_dropbox(dbx, audit_path: str) -> List[str]:
    folders = []
    for folder in _list_subfolders(dbx, audit_path):
        if not _matches_any_pattern(folder.name, OPERATEUR_PATTERNS):
            folders.append(folder.name)
    return folders



def _resolve_dropbox_sync_targets(
    dbx,
    project_path: str,
    selected_audit_folder: Optional[str] = None,
) -> tuple[List[str], Optional[str]]:
    logger.info("[Dropbox] résolution depuis project_path=%r", project_path)

    audit_path = _find_dropbox_folder_by_patterns(
        dbx,
        project_path,
        AUDIT_PATTERNS,
        recursive=True,
        max_depth=6,
    )
    if not audit_path:
        direct_children = _folder_debug_listing(dbx, project_path)
        raise ValueError(
            "Dossier 'Audit' introuvable dans Dropbox pour ce projet. "
            f"project_path={project_path!r}. Sous-dossiers directs vus: {direct_children}"
        )

    sync_targets: List[str] = []
    selected_folder_name: Optional[str] = None

    operateur_path = _find_dropbox_folder_by_patterns(
        dbx,
        audit_path,
        OPERATEUR_PATTERNS,
        recursive=False,
    )
    if operateur_path:
        sync_targets.append(operateur_path)
        logger.info(f"  🔹 Sync Dropbox Opérateur : {operateur_path}")
    else:
        logger.info("  🔹 Pas de dossier Opérateur trouvé dans Dropbox")

    if selected_audit_folder:
        selected_path = _find_dropbox_folder_by_patterns(
            dbx,
            audit_path,
            [selected_audit_folder, f"*{selected_audit_folder}*"],
            recursive=False,
        )
        if selected_path is None:
            available = ", ".join(_list_available_audit_folders_dropbox(dbx, audit_path)) or "aucun"
            raise ValueError(
                f"Dossier d'audit introuvable dans Dropbox: {selected_audit_folder!r}. "
                f"Dossiers disponibles sous {audit_path}: {available}"
            )
        sync_targets.append(selected_path)
        selected_folder_name = selected_path.rstrip("/").rsplit("/", 1)[-1]
        logger.info(f"  🔹 Sync Dropbox audit choisi : {selected_path}")

    unique_targets = list(dict.fromkeys(sync_targets))
    return unique_targets, selected_folder_name



def list_available_audit_folders(project_local_root: Path) -> List[str]:
    audit_base = _find_audit_base(project_local_root)
    if not audit_base:
        return []

    return [
        d.name
        for d in iter_direct_subfolders(audit_base)
        if not _matches_any_pattern(d.name, OPERATEUR_PATTERNS)
    ]



def filter_audit_files(
    all_files: List[Path],
    project_local_root: Path,
    selected_audit_folder: Optional[str] = None,
) -> tuple[List[Path], Optional[str]]:
    audit_base = _find_audit_base(project_local_root)
    if not audit_base:
        logger.warning("Dossier 'Audit' introuvable dans le projet → aucun fichier retenu")
        return [], None

    target_dirs: List[Path] = []
    selected_folder_name: Optional[str] = None

    operateur_dir = _find_operateur_folder(audit_base)
    if operateur_dir is not None:
        target_dirs.append(operateur_dir)
        logger.info(f"  🔹 Opérateur : {operateur_dir.name}")
    else:
        logger.info("  🔹 Pas de dossier Opérateur trouvé")

    if selected_audit_folder:
        selected_path = None
        for child in iter_direct_subfolders(audit_base):
            if _matches_any_pattern(child.name, [selected_audit_folder, f"*{selected_audit_folder}*"]):
                selected_path = child
                break

        if selected_path is None:
            available = ", ".join(list_available_audit_folders(project_local_root)) or "aucun"
            raise ValueError(
                f"Dossier d'audit introuvable: {selected_audit_folder!r}. "
                f"Dossiers disponibles: {available}"
            )

        target_dirs.append(selected_path)
        selected_folder_name = selected_path.name
        logger.info(f"  🔹 Dossier audit choisi : {selected_path.name}")

    if not target_dirs:
        logger.warning("Aucun dossier cible (ni Opérateur, ni sous-dossier choisi) → aucun fichier retenu")
        return [], None

    filtered = [f for f in all_files if any(f.is_relative_to(d) for d in target_dirs)]
    logger.info(f"  📂 {len(filtered)}/{len(all_files)} fichiers retenus")
    return filtered, selected_folder_name



def compute_source_path(file_path: Path, project_local_root: Path) -> str:
    try:
        return str(file_path.relative_to(project_local_root))
    except ValueError:
        return file_path.name



def is_old_folder_path(file_path: Path) -> bool:
    return is_archived_path(str(file_path))


class DocumentProcessingTimeoutError(TimeoutError):
    pass


@contextlib.contextmanager
def _document_time_limit(timeout_seconds: int):
    if timeout_seconds <= 0:
        yield
        return

    if os.name == "nt" or threading.current_thread() is not threading.main_thread():
        yield
        return

    def _handle_timeout(signum, frame):
        raise DocumentProcessingTimeoutError(f"timeout après {timeout_seconds}s")

    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _handle_timeout)
    signal.setitimer(signal.ITIMER_REAL, float(timeout_seconds))
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run(project_path: str, selected_audit_folder: Optional[str] = None):
    project_id = slugify(project_path)
    project_out = OUTPUT_DIR / project_id
    project_out.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info(f"Projet : {project_path}")
    logger.info(f"ID     : {project_id}")
    logger.info("=" * 60)

    logger.info("ÉTAPE 1 — Sync Dropbox")
    dbx = _get_dropbox_client()
    sync_targets, resolved_selected_audit_folder = _resolve_dropbox_sync_targets(
        dbx,
        project_path,
        selected_audit_folder=selected_audit_folder,
    )
    if not sync_targets:
        logger.warning("Aucun dossier Dropbox cible à synchroniser. Fin.")
        return

    downloaded = _sync_dropbox_folders(
        dropbox_folders=sync_targets,
        local_dir=str(LOCAL_CACHE),
        dbx=dbx,
    )
    logger.info(f"  📥 {len(downloaded)} fichier(s)")

    relative_project = project_path.strip("/")
    project_local_root = LOCAL_CACHE / relative_project

    discovered_files = sorted(
        p for p in project_local_root.rglob("*")
        if p.suffix.lower() in SUPPORTED_EXT and p.is_file() and not is_old_folder_path(p)
    )
    logger.info(f"  📂 {len(discovered_files)} fichier(s) trouvé(s)")

    scoped_files, selected_audit_folder_name = filter_audit_files(
        discovered_files,
        project_local_root,
        selected_audit_folder=selected_audit_folder,
    )
    if selected_audit_folder_name is None:
        selected_audit_folder_name = resolved_selected_audit_folder

    all_files = scoped_files
    if ENABLE_PIPELINE_PREFILTER:
        all_files = prefilter_files_for_extraction(
            scoped_files,
            project_local_root,
            selected_audit_folder=selected_audit_folder_name,
        )
    else:
        logger.info(
            "  🎯 Préfiltrage pipeline désactivé : %s fichier(s) gardé(s) dans le scope",
            len(all_files),
        )

    if not all_files:
        logger.warning("Aucun fichier pertinent après filtrage. Fin.")
        return

    logger.info("ÉTAPE 2 — Extraction + Chunking")
    all_parents = []
    file_manifest = []
    warnings_list = []
    total_skipped = 0
    timed_out_files = 0
    doc_timeout_seconds = DEFAULT_DOC_TIMEOUT_SECONDS

    total_files_to_process = len(all_files)

    for index, file_path in enumerate(tqdm(all_files, desc="Processing", unit="file"), start=1):
        source_path = compute_source_path(file_path, project_local_root)
        document_id = make_document_id(project_id, source_path)
        started = time.perf_counter()

        logger.info(
            "  🔄 [%s/%s] %s",
            index,
            total_files_to_process,
            source_path,
        )

        try:
            with _document_time_limit(doc_timeout_seconds):
                elements, stats = extract(file_path, source_path=source_path)
                if not elements:
                    warnings_list.append(f"Aucun contenu extrait: {source_path}")
                    continue

                parents, skipped = build_parents(
                    elements,
                    project_id=project_id,
                    document_id=document_id,
                    parent_size_tokens=PARENT_SIZE,
                )

            elapsed = round(time.perf_counter() - started, 2)

            if not elements:
                warnings_list.append(f"Aucun contenu extrait: {source_path}")
                continue

            total_skipped += skipped
            all_parents.extend(parents)

            category = list(stats.keys())[0] if stats else "unknown"
            file_type = file_path.suffix.lstrip(".").lower()
            max_page = None
            for el in elements:
                pl = el.get("metadata", {}).get("page_label")
                if pl and (max_page is None or pl > max_page):
                    max_page = pl

            doc_tokens = sum(p["token_estimate"] for p in parents)

            file_manifest.append({
                "document_id": document_id,
                "filename": file_path.name,
                "source_path": source_path,
                "file_type": file_type,
                "category": category,
                "pages": max_page,
                "parents_informative": len(parents),
                "parents_skipped": skipped,
                "token_estimate": doc_tokens,
                "processing_seconds": elapsed,
            })

            logger.info(
                "    ✅ %s parent(s), %s ignoré(s), %s tok, %.2fs",
                len(parents),
                skipped,
                doc_tokens,
                elapsed,
            )

        except DocumentProcessingTimeoutError as e:
            timed_out_files += 1
            elapsed = round(time.perf_counter() - started, 2)
            logger.warning(f"⏭️  Timeout document ({elapsed:.2f}s) : {source_path}")
            warnings_list.append(f"Timeout ({elapsed:.2f}s): {source_path}: {e}")
        except Exception as e:
            elapsed = round(time.perf_counter() - started, 2)
            logger.error(f"❌ {file_path.name}: {e}", exc_info=True)
            warnings_list.append(f"Erreur ({elapsed:.2f}s): {source_path}: {str(e)[:200]}")

    logger.info("ÉTAPE 3 — Écriture output")
    docs_dir = project_out / "documents"
    docs_dir.mkdir(parents=True, exist_ok=True)

    from collections import defaultdict
    parents_by_doc = defaultdict(list)
    for p in all_parents:
        parents_by_doc[p["document_id"]].append(p)

    for doc_id, doc_parents in parents_by_doc.items():
        write_jsonl(docs_dir / f"{doc_id}.jsonl", doc_parents)

    total_tokens = sum(p["token_estimate"] for p in all_parents)

    manifest = {
        "project_id": project_id,
        "project_path": project_path,
        "selected_audit_folder": selected_audit_folder_name,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "files": file_manifest,
        "stats": {
            "files_found": len(discovered_files),
            "files_in_scope": len(scoped_files),
            "files_processed": len(file_manifest),
            "files_timed_out": timed_out_files,
            "parents_informative": len(all_parents),
            "parents_skipped": total_skipped,
            "total_tokens": total_tokens,
        },
        "warnings": warnings_list if warnings_list else None,
    }
    with open(project_out / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    logger.info("  📋 manifest.json")

    logger.info("=" * 60)
    logger.info(
        f"✅ {len(all_parents)} parents ({total_tokens:,} tok) | "
        f"{len(parents_by_doc)} documents | {total_skipped} filtrés"
    )
    logger.info(f"   Output : {project_out}")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline Dropbox → Parents JSONL")
    parser.add_argument(
        "--project", "-p",
        required=True,
        help='Chemin Dropbox du projet (ex: "/RAIZERS - En audit/SIGNATURE")',
    )
    parser.add_argument(
        "--audit-folder",
        default=None,
        help="Nom du dossier d'audit à inclure en plus du dossier Opérateur (ex: '3. Opération - Rue de la Loge')",
    )
    args = parser.parse_args()
    run(args.project, selected_audit_folder=args.audit_folder)
