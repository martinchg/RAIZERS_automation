"""
RAIZERS — Interface d'audit automatique (Streamlit).

Usage local  : streamlit run app.py
Déploiement  : Streamlit Community Cloud (gratuit)
"""

import json
import logging
import os
import re
import smtplib
import sys
import time
from email.message import EmailMessage
from io import StringIO
from pathlib import Path

import streamlit as st

# Setup paths
ROOT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(ROOT_DIR / "src"))

from runtime_config import configure_environment

configure_environment(ROOT_DIR)

OUTPUT_DIR = ROOT_DIR / "output"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slugify(text: str) -> str:
    text = text.strip("/").lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def _get_dropbox_client():
    """Retourne un client Dropbox (cached dans session_state)."""
    if "dbx_client" not in st.session_state:
        from dropbox_client import get_client
        st.session_state.dbx_client = get_client()
    return st.session_state.dbx_client


def _find_audit_root() -> str:
    """Détecte automatiquement le dossier 'Raizers - En audit' dans le Dropbox de l'utilisateur.

    Cherche dans la racine → dossier utilisateur → sous-dossiers pour trouver
    un dossier dont le nom contient 'en audit' (insensible à la casse).
    """
    from dropbox.files import FolderMetadata
    dbx = _get_dropbox_client()

    def _find_audit_in(path: str, depth: int = 0) -> str | None:
        if depth > 2:  # ne pas chercher trop profond
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
                if isinstance(entry, FolderMetadata) and "en audit" in entry.name.lower():
                    return entry.path_display
            # Pas trouvé ici, chercher un niveau plus bas
            for entry in entries:
                if isinstance(entry, FolderMetadata):
                    found = _find_audit_in(entry.path_display, depth + 1)
                    if found:
                        return found
        except Exception:
            pass
        return None

    found = _find_audit_in("")
    return found or ""


def _list_dropbox_entries(path: str) -> tuple[list[str], list[str]]:
    """Liste les sous-dossiers et fichiers d'un chemin Dropbox.

    Returns (folders, files) — noms uniquement, triés alphabétiquement.
    """
    try:
        from dropbox.files import FolderMetadata, FileMetadata
        dbx = _get_dropbox_client()
        result = dbx.files_list_folder(path, recursive=False)
        folders, files = [], []
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
    except Exception as e:
        st.error(f"Erreur connexion Dropbox : {e}")
        return [], []


def _send_email(to: str, subject: str, body: str, attachment_path: Path | None = None):
    """Envoie un email avec pièce jointe optionnelle via SMTP."""
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")

    if not smtp_user or not smtp_pass:
        st.warning("SMTP non configuré (SMTP_USER / SMTP_PASS). Email non envoyé.")
        return False

    msg = EmailMessage()
    msg["From"] = smtp_user
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body)

    if attachment_path and attachment_path.exists():
        with open(attachment_path, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=attachment_path.name,
            )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
    return True


class StreamlitLogHandler(logging.Handler):
    """Capture les logs dans un buffer pour affichage Streamlit."""
    def __init__(self):
        super().__init__()
        self.buffer = StringIO()

    def emit(self, record):
        self.buffer.write(self.format(record) + "\n")

    def get_logs(self) -> str:
        return self.buffer.getvalue()


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="RAIZERS Audit", page_icon="📊", layout="centered")

st.title("RAIZERS — Audit automatique")
st.markdown("---")

# --- Sélection du dossier (navigateur Dropbox) ---
st.subheader("1. Dossier à auditer")

if "dbx_root" not in st.session_state:
    with st.spinner("Connexion Dropbox..."):
        st.session_state.dbx_root = _find_audit_root()

DROPBOX_ROOT = st.session_state.dbx_root

if not DROPBOX_ROOT:
    st.error("Impossible de trouver un dossier 'En audit' dans votre Dropbox. "
             "Vérifiez la connexion et la structure de votre Dropbox.")
    st.stop()

st.caption(f"Racine détectée : `{DROPBOX_ROOT}`")

if "dbx_folders" not in st.session_state:
    st.session_state.dbx_folders = []

def _load_projects():
    folders, _ = _list_dropbox_entries(DROPBOX_ROOT)
    st.session_state.dbx_folders = folders

if not st.session_state.dbx_folders:
    _load_projects()

col1, col2 = st.columns([4, 1])
with col2:
    if st.button("Rafraichir"):
        _load_projects()
        st.rerun()

if st.session_state.dbx_folders:
    with col1:
        selected_project = st.selectbox(
            "Projet à auditer",
            st.session_state.dbx_folders,
            label_visibility="collapsed",
        )
    selected_path = f"{DROPBOX_ROOT}/{selected_project}"
else:
    st.warning("Aucun dossier trouvé dans Dropbox.")
    selected_path = ""

# --- Options ---
st.subheader("2. Options")

col_a, col_b = st.columns(2)
with col_a:
    run_extract = st.checkbox("Extraction LLM", value=True)
    run_mandats = st.checkbox("Mandats Pappers", value=True)
with col_b:
    run_fill = st.checkbox("Générer Excel", value=True)
    send_email = st.checkbox("Envoyer par email", value=False)

email_to = ""
if send_email:
    email_to = st.text_input("Email de notification", placeholder="prenom@raizers.com")

# --- Lancement ---
st.markdown("---")

if st.button("Lancer l'audit", type="primary", use_container_width=True):
    if not selected_path:
        st.error("Sélectionne un dossier.")
        st.stop()

    project_path = selected_path  # already a full Dropbox path
    project_id = _slugify(project_path)
    project_name = project_path.rstrip("/").rsplit("/", 1)[-1]  # dernier segment
    project_dir = OUTPUT_DIR / project_id

    # Setup logging capture
    log_handler = StreamlitLogHandler()
    log_handler.setFormatter(logging.Formatter("%(asctime)s — %(message)s", datefmt="%H:%M:%S"))
    root_logger = logging.getLogger()
    root_logger.addHandler(log_handler)
    root_logger.setLevel(logging.INFO)

    status = st.status("Audit en cours...", expanded=True)
    results_summary = {}

    # --- ÉTAPE 1 : Pipeline Dropbox ---
    try:
        status.update(label="Pipeline Dropbox...")
        st.write("**Pipeline** — Sync Dropbox + extraction texte...")
        from pipeline import run as run_pipeline
        run_pipeline(project_path)

        manifest_path = project_dir / "manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            n_files = manifest.get("stats", {}).get("files_processed", 0)
            n_tokens = manifest.get("stats", {}).get("total_tokens", 0)
            results_summary["pipeline"] = f"{n_files} fichiers, {n_tokens:,} tokens"
            st.write(f"Pipeline : {n_files} fichiers, {n_tokens:,} tokens")
        else:
            results_summary["pipeline"] = "OK"
            st.write("Pipeline : OK")
    except Exception as e:
        st.error(f"Pipeline : {e}")
        results_summary["pipeline"] = f"ERREUR : {e}"

    # --- ÉTAPE 2 : Extraction LLM ---
    if run_extract:
        try:
            status.update(label="Extraction LLM...")
            st.write("**Extraction LLM** en cours...")
            from extract_structured import run as run_extraction
            run_extraction(project_id)

            results_path = project_dir / "extraction_results.json"
            if results_path.exists():
                data = json.loads(results_path.read_text(encoding="utf-8"))
                summary = data.get("summary", {})
                answered = summary.get("answered", 0)
                total = summary.get("total", 0)
                results_summary["extract"] = f"{answered}/{total} champs"
                st.write(f"Extraction : {answered}/{total} champs remplis")
            else:
                results_summary["extract"] = "OK"
        except Exception as e:
            st.error(f"Extraction : {e}")
            results_summary["extract"] = f"ERREUR : {e}"

    # --- ÉTAPE 3 : Mandats Pappers ---
    if run_mandats:
        try:
            status.update(label="Mandats Pappers...")
            st.write("**Mandats Pappers** en cours...")
            from mandats_pipeline import run as run_mandats_pipeline
            run_mandats_pipeline(project_id)

            mandats_path = project_dir / "mandats_results.json"
            if mandats_path.exists():
                data = json.loads(mandats_path.read_text(encoding="utf-8"))
                summary = data.get("summary", {})
                n_societes = summary.get("societes", 0)
                n_persons = summary.get("persons", 0)
                results_summary["mandats"] = f"{n_societes} sociétés, {n_persons} personnes"
                st.write(f"Mandats : {n_societes} sociétés pour {n_persons} personnes")
            else:
                results_summary["mandats"] = "OK"
        except Exception as e:
            st.error(f"Mandats : {e}")
            results_summary["mandats"] = f"ERREUR : {e}"

    # --- ÉTAPE 4 : Excel ---
    excel_path = None
    if run_fill:
        try:
            status.update(label="Génération Excel...")
            st.write("**Excel** — Génération du rapport...")
            from excel_filler import fill_excel

            results_path = project_dir / "extraction_results.json"
            questions_path = ROOT_DIR / "config" / "questions.json"

            if results_path.exists() and questions_path.exists():
                extraction_data = json.loads(results_path.read_text(encoding="utf-8"))
                questions_data = json.loads(questions_path.read_text(encoding="utf-8"))

                person_folder_map = extraction_data.get("person_folders")
                pappers_mandats = None
                mandats_path = project_dir / "mandats_results.json"
                if mandats_path.exists():
                    mandats_data = json.loads(mandats_path.read_text(encoding="utf-8"))
                    pappers_mandats = mandats_data.get("societes_par_personne")

                excel_path = fill_excel(
                    results=extraction_data["results"],
                    fields=questions_data["fields"],
                    output_dir=project_dir,
                    person_folder_map=person_folder_map,
                    pappers_mandats=pappers_mandats,
                )
                results_summary["fill"] = "rapport.xlsx"
                st.write("Excel : rapport.xlsx généré")
            else:
                st.warning("extraction_results.json manquant — Excel non généré")
                results_summary["fill"] = "SKIP (pas de résultats)"
        except Exception as e:
            st.error(f"Excel : {e}")
            results_summary["fill"] = f"ERREUR : {e}"

    # --- ÉTAPE 5 : Email ---
    if send_email and email_to and excel_path and excel_path.exists():
        try:
            status.update(label="Envoi email...")
            sent = _send_email(
                to=email_to,
                subject=f"RAIZERS Audit — {project_name}",
                body=f"Rapport d'audit pour le projet {project_name} ci-joint.",
                attachment_path=excel_path,
            )
            if sent:
                results_summary["email"] = f"envoyé à {email_to}"
                st.write(f"Email envoyé à {email_to}")
        except Exception as e:
            st.error(f"Email : {e}")
            results_summary["email"] = f"ERREUR : {e}"

    # --- Résumé final ---
    root_logger.removeHandler(log_handler)

    all_ok = all("ERREUR" not in str(v) for v in results_summary.values())
    status.update(
        label="Audit terminé" if all_ok else "Audit terminé (avec erreurs)",
        state="complete" if all_ok else "error",
        expanded=False,
    )

    st.markdown("---")
    st.subheader("Résultat")

    for step, result in results_summary.items():
        icon = "✅" if "ERREUR" not in str(result) and "SKIP" not in str(result) else "❌"
        step_labels = {
            "pipeline": "Pipeline",
            "extract": "Extraction",
            "mandats": "Mandats",
            "fill": "Excel",
            "email": "Email",
        }
        st.write(f"{icon} **{step_labels.get(step, step)}** : {result}")

    # Bouton téléchargement Excel
    if excel_path and excel_path.exists():
        with open(excel_path, "rb") as f:
            st.download_button(
                label="Télécharger le rapport Excel",
                data=f.read(),
                file_name=f"rapport_{project_name}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

    # Logs détaillés (masqués par défaut)
    with st.expander("Logs détaillés"):
        st.code(log_handler.get_logs(), language="text")
