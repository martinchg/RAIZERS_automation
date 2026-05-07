"""
dropbox_client.py : Télécharge les fichiers depuis un dossier Dropbox
vers un répertoire local temporaire pour traitement.

Auth : refresh token (permanent) via .env, ou access token (fallback dev).
Nécessite : pip install dropbox python-dotenv
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List

import dropbox
from dropbox.files import FileMetadata, FolderMetadata
from core.runtime_config import configure_environment

ROOT_DIR = Path(__file__).parent.parent.resolve()
configure_environment(ROOT_DIR)

logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".xlsx", ".xls", ".html",
                        ".txt", ".md", ".pptx", ".ppt"}
DEFAULT_DOWNLOAD_WORKERS = max(1, int(os.environ.get("DROPBOX_DOWNLOAD_WORKERS", "6")))


def get_client() -> dropbox.Dropbox:
    """
    Crée un client Dropbox authentifié.
    Priorité : refresh token (permanent) > access token (4h).
    """
    app_key = os.environ.get("DROPBOX_APP_KEY", "")
    app_secret = os.environ.get("DROPBOX_APP_SECRET", "")
    refresh_token = os.environ.get("DROPBOX_REFRESH_TOKEN", "").strip("'\"")

    if refresh_token and app_key and app_secret:
        logger.info("Auth Dropbox via refresh token (permanent)")
        return dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )

    access_token = os.environ.get("DROPBOX_ACCESS_TOKEN", "")
    if access_token:
        logger.warning("Auth Dropbox via access token (expire en ~4h)")
        return dropbox.Dropbox(access_token)

    raise ValueError(
        "Aucune auth Dropbox dans .env. "
        "Renseigne DROPBOX_REFRESH_TOKEN + APP_KEY + APP_SECRET."
    )


def list_files(dbx: dropbox.Dropbox,
               dropbox_folder: str,
               recursive: bool = True) -> List[FileMetadata]:
    """
    Liste tous les fichiers supportés dans un dossier Dropbox.
    """
    files: List[FileMetadata] = []
    try:
        result = dbx.files_list_folder(dropbox_folder, recursive=recursive)
        while True:
            for entry in result.entries:
                if isinstance(entry, FileMetadata):
                    ext = Path(entry.name).suffix.lower()
                    if ext in SUPPORTED_EXTENSIONS:
                        files.append(entry)
            if not result.has_more:
                break
            result = dbx.files_list_folder_continue(result.cursor)
    except dropbox.exceptions.ApiError as e:
        logger.error(f"Erreur Dropbox API (list_folder) : {e}")
        raise
    logger.info(f"📂 {len(files)} fichier(s) trouvé(s) dans Dropbox:{dropbox_folder}")
    return files


def list_subfolders(dbx: dropbox.Dropbox, dropbox_folder: str) -> List[FolderMetadata]:
    """
    Liste les sous-dossiers directs d'un dossier Dropbox.
    """
    folders: List[FolderMetadata] = []
    try:
        result = dbx.files_list_folder(dropbox_folder, recursive=False)
        while True:
            for entry in result.entries:
                if isinstance(entry, FolderMetadata):
                    folders.append(entry)
            if not result.has_more:
                break
            result = dbx.files_list_folder_continue(result.cursor)
    except dropbox.exceptions.ApiError as e:
        logger.error(f"Erreur Dropbox API (list_subfolders) : {e}")
        raise
    return sorted(folders, key=lambda folder: folder.name.lower())


def download_files(dbx: dropbox.Dropbox,
                   files: List[FileMetadata],
                   local_dir: str | Path) -> List[Path]:
    """
    Télécharge les fichiers Dropbox vers un répertoire local.
    Conserve l'arborescence relative.
    Retourne la liste des chemins locaux téléchargés.
    """
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)

    def _download_one(file_meta: FileMetadata) -> Path | None:
        relative_path = file_meta.path_display.lstrip("/")
        local_path = local_dir / relative_path
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            worker_client = get_client()
            worker_client.files_download_to_file(str(local_path), file_meta.path_lower)
            logger.info(f"  ✅ {relative_path}")
            return local_path
        except Exception as e:
            logger.error(f"  ❌ Échec téléchargement {relative_path}: {e}")
            return None

    downloaded: List[Path] = []
    worker_count = min(DEFAULT_DOWNLOAD_WORKERS, max(1, len(files)))

    if worker_count == 1:
        for file_meta in files:
            downloaded_path = _download_one(file_meta)
            if downloaded_path is not None:
                downloaded.append(downloaded_path)
    else:
        logger.info("📦 Téléchargement Dropbox en parallèle (%s workers)", worker_count)
        try:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                for downloaded_path in executor.map(_download_one, files):
                    if downloaded_path is not None:
                        downloaded.append(downloaded_path)
        except Exception as exc:
            logger.warning(
                "Échec du mode parallèle Dropbox (%s). Repli en séquentiel.",
                exc,
            )
            downloaded = []
            for file_meta in files:
                downloaded_path = _download_one(file_meta)
                if downloaded_path is not None:
                    downloaded.append(downloaded_path)

    logger.info(f"📥 {len(downloaded)}/{len(files)} fichier(s) téléchargé(s)")
    return downloaded


def sync_folder(dropbox_folder: str,
                local_dir: str | Path = "Dropbox_exctraction",
                recursive: bool = True) -> List[Path]:
    """
    Point d'entrée simple : liste + télécharge tous les fichiers supportés
    depuis un dossier Dropbox vers un répertoire local.
    """
    dbx = get_client()
    files = list_files(dbx, dropbox_folder, recursive=recursive)
    return download_files(dbx, files, local_dir)


def _cleanup_stale_cache(dropbox_folders: List[str],
                         remote_files: List[FileMetadata],
                         local_dir: Path) -> int:
    remote_paths = {f.path_display.lstrip("/") for f in remote_files}
    deleted = 0
    for folder in dropbox_folders:
        local_root = local_dir / folder.lstrip("/")
        if not local_root.exists():
            continue
        for local_file in local_root.rglob("*"):
            if not local_file.is_file():
                continue
            rel = str(local_file.relative_to(local_dir))
            if rel not in remote_paths:
                local_file.unlink()
                deleted += 1
                logger.info("  🗑️  Supprimé du cache (absent Dropbox) : %s", rel)
        for local_subdir in sorted(local_root.rglob("*"), reverse=True):
            if local_subdir.is_dir() and not any(local_subdir.iterdir()):
                local_subdir.rmdir()
    if deleted:
        logger.info("🧹 %d fichier(s) obsolète(s) supprimé(s) du cache local", deleted)
    return deleted


def sync_folders(dropbox_folders: List[str],
                 local_dir: str | Path = "Dropbox_exctraction",
                 recursive: bool = True,
                 dbx: dropbox.Dropbox | None = None) -> List[Path]:
    """
    Télécharge plusieurs dossiers Dropbox ciblés dans un même cache local.
    Supprime les fichiers locaux qui n'existent plus dans Dropbox.
    """
    if not dropbox_folders:
        return []

    client = dbx or get_client()
    all_files: List[FileMetadata] = []

    for folder in dropbox_folders:
        all_files.extend(list_files(client, folder, recursive=recursive))

    _cleanup_stale_cache(dropbox_folders, all_files, Path(local_dir))

    return download_files(client, all_files, local_dir)
