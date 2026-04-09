"""
pipeline.py : Point d'entrée principal.

Usage :
    python pipeline.py --project "/RAIZERS - En audit/SIGNATURE"

Flux :
    1. Sync Dropbox → local
    2. Extraction texte par document (ingestion.py)
    3. Chunking en parents (chunking.py)
    4. Écriture parents.jsonl + manifest.json dans output/<project_id>/
"""

import argparse
import json
import logging
import re
import sys
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

from dropbox_client import sync_folder
from ingestion import extract
from chunking import build_parents, make_document_id, count_tokens

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OUTPUT_DIR = ROOT_DIR / "output"
LOCAL_CACHE = ROOT_DIR / "cache"

SUPPORTED_EXT = {".pdf", ".docx", ".txt", ".pptx", ".xlsx", ".xls", ".md", ".ppt"}

PARENT_SIZE = 2000  # tokens max par parent

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(ROOT_DIR / "logs" / "pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def slugify(text: str) -> str:
    text = text.strip("/").lower()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')


def write_jsonl(path: Path, records: List[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    logger.info(f"  📝 {path.name}: {len(records)} lignes")


def filter_audit_files(all_files: List[Path], project_local_root: Path) -> tuple[List[Path], Optional[str]]:
    """
    Filtre les fichiers pour ne garder que ceux dans :
    - SIGNATURE/2. Audit /1. Opérateur/
    - SIGNATURE/2. Audit /<dossier avec le plus grand numéro X>/

    Si la structure n'est pas trouvée, retourne tous les fichiers.

    Retourne:
    - filtered_files: fichiers retenus
    - latest_folder_name: nom du dossier numéroté avec le plus grand X
    """
    # Chercher le dossier "2. Audit " (ou similaire)
    audit_base: Optional[Path] = None
    for d in sorted(project_local_root.rglob("*")):
        if d.is_dir() and re.match(r"^2\.\s*", d.name) and "audit" in d.name.lower():
            audit_base = d
            break

    if not audit_base:
        logger.warning("Dossier '2. Audit ' non trouvé → traitement de tous les fichiers")
        return all_files, None

    # Identifier les sous-dossiers numérotés (ex: "1. Opérateur", "3. Nom")
    numbered_dirs: dict[int, Path] = {}
    for d in audit_base.iterdir():
        if d.is_dir():
            m = re.match(r"^(\d+)\.\s*", d.name)
            if m:
                numbered_dirs[int(m.group(1))] = d

    if not numbered_dirs:
        logger.warning("Aucun sous-dossier numéroté dans '2. Audit'")
        return all_files, None

    # Toujours inclure "1. Opérateur" + le dossier avec le plus grand numéro
    target_dirs: List[Path] = []
    if 1 in numbered_dirs:
        target_dirs.append(numbered_dirs[1])
    max_num = max(numbered_dirs.keys())
    latest_folder_name = numbered_dirs[max_num].name
    if max_num != 1:
        target_dirs.append(numbered_dirs[max_num])

    logger.info(f"  📁 Dossiers audit sélectionnés : {[d.name for d in target_dirs]}")

    filtered = [f for f in all_files if any(f.is_relative_to(d) for d in target_dirs)]
    logger.info(f"  📂 {len(filtered)}/{len(all_files)} fichiers retenus")
    logger.info(f"  🧭 Dossier avec le plus grand X : {latest_folder_name}")
    return filtered, latest_folder_name



def compute_source_path(file_path: Path, project_local_root: Path) -> str:
    try:
        return str(file_path.relative_to(project_local_root))
    except ValueError:
        return file_path.name


def is_old_folder_path(file_path: Path) -> bool:
    """Ignore les documents archivés (.old/old) dans l'arborescence."""
    old_markers = {"old", ".old", "x - old"}
    return any(part.strip().lower() in old_markers for part in file_path.parts)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def run(project_path: str):
    project_id = slugify(project_path)
    project_out = OUTPUT_DIR / project_id
    project_out.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info(f"Projet : {project_path}")
    logger.info(f"ID     : {project_id}")
    logger.info("=" * 60)

    # --- 1. Sync Dropbox ---
    logger.info("ÉTAPE 1 — Sync Dropbox")
    downloaded = sync_folder(
        dropbox_folder=project_path,
        local_dir=str(LOCAL_CACHE),
    )
    logger.info(f"  📥 {len(downloaded)} fichier(s)")

    relative_project = project_path.strip("/")
    project_local_root = LOCAL_CACHE / relative_project

    all_files = sorted(
        p for p in project_local_root.rglob("*")
        if p.suffix.lower() in SUPPORTED_EXT and p.is_file() and not is_old_folder_path(p)
    )
    logger.info(f"  📂 {len(all_files)} fichier(s) trouvé(s)")

    # Filtrer : ne garder que 1. Opérateur + dossier le plus récent (plus grand numéro)
    all_files, latest_audit_folder = filter_audit_files(all_files, project_local_root)

    if not all_files:
        logger.warning("Aucun fichier trouvé. Fin.")
        return

    # --- 2. Extract + Build parents (document par document) ---
    logger.info("ÉTAPE 2 — Extraction + Chunking")
    all_parents = []
    file_manifest = []
    warnings_list = []
    total_skipped = 0

    for file_path in tqdm(all_files, desc="Processing", unit="file"):
        source_path = compute_source_path(file_path, project_local_root)
        document_id = make_document_id(project_id, source_path)

        try:
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

            total_skipped += skipped
            all_parents.extend(parents)

            # Stats pour le manifest
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
                "pages": max_page,
                "parents_informative": len(parents),
                "parents_skipped": skipped,
                "token_estimate": doc_tokens,
            })

            if skipped > 0:
                logger.debug(f"  {source_path}: {skipped} section(s) non informatives filtrées")

        except Exception as e:
            logger.error(f"❌ {file_path.name}: {e}", exc_info=True)
            warnings_list.append(f"Erreur: {source_path}: {str(e)[:100]}")

    # --- 3. Écriture : 1 JSONL par document ---
    logger.info("ÉTAPE 3 — Écriture output")
    docs_dir = project_out / "documents"
    docs_dir.mkdir(parents=True, exist_ok=True)

    # Grouper les parents par document_id
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
        "latest_audit_folder": latest_audit_folder,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "files": file_manifest,
        "stats": {
            "files_processed": len(file_manifest),
            "parents_informative": len(all_parents),
            "parents_skipped": total_skipped,
            "total_tokens": total_tokens,
        },
        "warnings": warnings_list if warnings_list else None,
    }
    with open(project_out / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    logger.info(f"  📋 manifest.json")

    # --- Résumé ---
    logger.info("=" * 60)
    logger.info(f"✅ {len(all_parents)} parents ({total_tokens:,} tok) | {len(parents_by_doc)} documents | {total_skipped} filtrés")
    logger.info(f"   Output : {project_out}")
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline Dropbox → Parents JSONL")
    parser.add_argument(
        "--project", "-p",
        required=True,
        help='Chemin Dropbox du projet (ex: "/RAIZERS - En audit/SIGNATURE")',
    )
    args = parser.parse_args()
    run(args.project)
