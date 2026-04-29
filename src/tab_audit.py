"""
RAIZERS — Logique metier et rendu de l'onglet Audit.
"""

import contextlib
import importlib
import json
import logging
import os
import re
import smtplib
import time
from datetime import datetime
from email.message import EmailMessage
from io import StringIO
from pathlib import Path

import streamlit as st

from core.normalization import matches_pattern
from extraction.question_config import filter_fields_for_excel_tabs, load_questions_config

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).parent.parent.resolve()
OUTPUT_DIR = ROOT_DIR / "output"
HISTORY_PATH = OUTPUT_DIR / "audit_history.json"

EN_AUDIT_PATTERNS = ["en audit", "*en audit*", "audit", "*audit*"]
AUDIT_PATTERNS = ["audit", "*audit", "audit*", "*audit*"]
OPERATEUR_PATTERNS = ["operateur", "*operateur", "operateur*", "*operateur*"]


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
    """Detecte automatiquement le dossier 'Raizers - En audit' dans le Dropbox."""
    from dropbox.files import FolderMetadata
    dbx = _get_dropbox_client()
    last_error: Exception | None = None

    def _find_audit_in(path: str, depth: int = 0) -> str | None:
        nonlocal last_error
        if depth > 6:
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
                    found = _find_audit_in(entry.path_display, depth + 1)
                    if found:
                        return found
        except Exception as exc:
            last_error = exc
        return None

    found = _find_audit_in("")
    if not found and last_error is not None:
        raise RuntimeError(f"Dropbox inaccessible pendant la recherche du dossier audit: {last_error}")
    return found or ""


def _list_dropbox_entries(path: str) -> tuple[list[str], list[str]]:
    """Liste les sous-dossiers et fichiers d'un chemin Dropbox."""
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


def _find_audit_folder_dropbox(project_path: str, max_depth: int = 6) -> str | None:
    """Trouve le dossier 'Audit' dans un projet Dropbox (matching flexible)."""
    def _walk(path: str, depth: int) -> str | None:
        folders, _ = _list_dropbox_entries(path)
        for name in folders:
            if any(matches_pattern(name, pattern) for pattern in AUDIT_PATTERNS):
                return f"{path}/{name}"
        if depth < max_depth:
            for name in folders:
                found = _walk(f"{path}/{name}", depth + 1)
                if found:
                    return found
        return None

    return _walk(project_path, 0)


def _list_audit_subfolders_dropbox(project_path: str) -> list[str]:
    """Liste les sous-dossiers du dossier Audit selectionnables (hors Operateur)."""
    audit_path = _find_audit_folder_dropbox(project_path, max_depth=6)
    if not audit_path:
        return []
    folders, _ = _list_dropbox_entries(audit_path)
    return [
        f for f in folders
        if not any(matches_pattern(f, pattern) for pattern in OPERATEUR_PATTERNS)
    ]


def _send_email(to: str, subject: str, body: str, attachment_path: Path | None = None):
    """Envoie un email avec piece jointe optionnelle via SMTP."""
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")

    if not smtp_user or not smtp_pass:
        st.warning("SMTP non configure (SMTP_USER / SMTP_PASS). Email non envoye.")
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


def _get_smtp_config_status() -> tuple[bool, list[str]]:
    """Retourne l'etat minimal de configuration SMTP attendue."""
    missing: list[str] = []
    for key in ("SMTP_USER", "SMTP_PASS"):
        if not os.environ.get(key, "").strip():
            missing.append(key)
    return (len(missing) == 0, missing)


def _load_audit_history() -> list[dict]:
    if not HISTORY_PATH.exists():
        return []
    try:
        payload = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    return payload if isinstance(payload, list) else []


def _append_audit_history(entry: dict, max_entries: int = 25) -> None:
    history = _load_audit_history()
    history.insert(0, entry)
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    HISTORY_PATH.write_text(
        json.dumps(history[:max_entries], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _remember_latest_excel(excel_path: Path | None, project_name: str) -> None:
    if not excel_path or not excel_path.exists():
        return
    st.session_state["latest_excel_path"] = str(excel_path)
    st.session_state["latest_excel_project_name"] = project_name


def _get_latest_excel_info() -> tuple[Path | None, str]:
    session_excel_path = st.session_state.get("latest_excel_path")
    session_project_name = st.session_state.get("latest_excel_project_name", "")
    if session_excel_path:
        excel_path = Path(session_excel_path)
        if excel_path.exists():
            return excel_path, session_project_name

    for entry in _load_audit_history():
        project_id = entry.get("project_id")
        if not project_id:
            continue
        excel_path = OUTPUT_DIR / project_id / "rapport.xlsx"
        if excel_path.exists():
            return excel_path, entry.get("project_name", project_id)

    return None, ""


def _reset_audit_subfolder_selection() -> None:
    st.session_state.pop("selected_audit_subfolder", None)
    st.session_state.pop("_audit_subfolder_project_path", None)


def _render_audit_history() -> None:
    history = _load_audit_history()[:5]
    if not history:
        st.caption("Aucun audit lance pour le moment.")
        return

    for entry in history:
        started_at = entry.get("started_at", "")
        project_name = entry.get("project_name", "Projet inconnu")
        duration = entry.get("duration_seconds")
        duration_label = f"{duration:.1f}s" if isinstance(duration, (int, float)) else "n/a"
        status = "OK" if entry.get("success") else "Avec erreurs"
        summary = entry.get("summary", {})

        detail_parts = []
        for key in ("pipeline", "extract", "mandats", "fill", "email"):
            value = summary.get(key)
            if value:
                detail_parts.append(f"{key}: {value}")

        st.markdown(
            (
                '<div class="history-card">'
                f'<strong>{project_name}</strong><br>'
                f'<span>{started_at} • {status} • {duration_label}</span>'
                + (f'<br><span>{" | ".join(detail_parts)}</span>' if detail_parts else "")
                + "</div>"
            ),
            unsafe_allow_html=True,
        )


class StreamlitLogHandler(logging.Handler):
    """Capture les logs dans un buffer pour affichage Streamlit."""

    def __init__(self):
        super().__init__()
        self.buffer = StringIO()

    def emit(self, record):
        line = self.format(record)
        self.buffer.write(line + "\n")

    def get_logs(self) -> str:
        return self.buffer.getvalue()


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------


def render_audit_tab():

    # --- Section 1 : Dossier a auditer (projet + sous-dossier) ---
    st.markdown("""
    <div class="step-card">
        <h3><span class="step-number">1</span> Dossier à auditer</h3>
    </div>
    """, unsafe_allow_html=True)

    if "dbx_root" not in st.session_state:
        with st.spinner("Connexion Dropbox..."):
            try:
                st.session_state.dbx_root = _find_audit_root()
                st.session_state.pop("dbx_root_error", None)
            except Exception as exc:
                st.session_state.dbx_root = ""
                st.session_state.dbx_root_error = str(exc)
    if "dbx_folders" not in st.session_state:
        st.session_state.dbx_folders = []

    DROPBOX_ROOT = st.session_state.dbx_root
    selected_project = ""
    selected_path = ""

    def _load_projects():
        dropbox_root = st.session_state.get("dbx_root", "")
        if not dropbox_root:
            st.session_state.dbx_folders = []
            return
        folders, _ = _list_dropbox_entries(dropbox_root)
        st.session_state.dbx_folders = folders

    if not DROPBOX_ROOT:
        root_error = st.session_state.get("dbx_root_error")
        if root_error:
            st.error(f"Connexion Dropbox : {root_error}")
        else:
            st.error("Impossible de trouver un dossier 'En audit' dans votre Dropbox.")
        if st.button("Reessayer la connexion Dropbox", type="primary"):
            st.session_state.pop("dbx_root", None)
            st.session_state.pop("dbx_root_error", None)
            st.session_state.pop("dbx_client", None)
            st.session_state.pop("dbx_folders", None)
            st.rerun()
        return

    if not st.session_state.dbx_folders:
        _load_projects()

    st.caption("Dossier")
    col1, col2 = st.columns([5, 1])
    if st.session_state.dbx_folders:
        with col1:
            selected_project = st.selectbox(
                "Projet",
                st.session_state.dbx_folders,
                label_visibility="collapsed",
                key="selected_project_name",
                on_change=_reset_audit_subfolder_selection,
            )
        selected_path = f"{DROPBOX_ROOT}/{selected_project}"
    else:
        st.warning("Aucun dossier trouve dans Dropbox.")
    with col2:
        if st.button("↺", help="Rafraichir la liste"):
            _load_projects()
            st.session_state.pop("audit_subfolders_cache", None)
            st.session_state.pop("selected_audit_subfolder", None)
            st.session_state.pop("_audit_subfolder_project_path", None)
            st.rerun()

    st.caption("Sous-dossier")
    selected_audit_subfolder: str | None = None
    if selected_path:
        previous_audit_project = st.session_state.get("_audit_subfolder_project_path")
        if previous_audit_project != selected_path:
            st.session_state.pop("selected_audit_subfolder", None)
            st.session_state["_audit_subfolder_project_path"] = selected_path

        cache = st.session_state.setdefault("audit_subfolders_cache", {})
        if selected_path not in cache:
            with st.spinner("Lecture des sous-dossiers d'audit..."):
                cache[selected_path] = _list_audit_subfolders_dropbox(selected_path)
        audit_subfolders = cache[selected_path]

        if audit_subfolders:
            current_subfolder = st.session_state.get("selected_audit_subfolder")
            if current_subfolder not in audit_subfolders:
                st.session_state.pop("selected_audit_subfolder", None)

            col_sub, col_sub_refresh = st.columns([5, 1])
            with col_sub:
                selected_audit_subfolder = st.selectbox(
                    "Sous-dossier d'audit a ingerer (en plus du dossier Operateur)",
                    audit_subfolders,
                    label_visibility="collapsed",
                    key="selected_audit_subfolder",
                )
            with col_sub_refresh:
                if st.button("↺", key="refresh_audit_subfolders", help="Rafraichir les sous-dossiers"):
                    cache.pop(selected_path, None)
                    st.session_state.pop("selected_audit_subfolder", None)
                    st.rerun()
            st.caption(
                "Seuls ce sous-dossier et le dossier Operateur associe seront ingeres."
            )
        else:
            st.warning(
                "Aucun sous-dossier d'audit trouve (hors Operateur). "
                "Seul le dossier Operateur sera ingere s'il existe."
            )

    # --- Section 2 : Options ---
    st.markdown("""
    <div class="step-card">
        <h3><span class="step-number">2</span> Options</h3>
    </div>
    """, unsafe_allow_html=True)

    st.caption("L'extraction LLM et la generation Excel sont toujours activees.")

    col_left, col_right = st.columns(2)
    with col_left:
        run_operation = st.toggle("Opération", value=True)
        run_bilan = st.toggle("Bilan", value=True)
        run_mandats = st.toggle("Mandats Pappers", value=True)
        send_email = st.toggle("Envoyer par email", value=False)
    with col_right:
        run_patrimoine = st.toggle("Patrimoine", value=True)
        run_compte_resultat = st.toggle("Compte de résultat", value=True)
        run_lots = st.toggle("Lots", value=True)

    """
    col_a, col_b = st.columns(2)
    with col_a:
        run_mandats = st.toggle("Mandats Pappers", value=True)
    with col_b:
        send_email = st.toggle("Envoyer par email", value=False)
    """

    smtp_configured, smtp_missing = _get_smtp_config_status()
    email_to = ""
    if send_email:
        email_to = st.text_input("Email de notification", placeholder="prenom@raizers.com")
        if not smtp_configured:
            st.warning(
                "Email inactive: configuration SMTP manquante "
                f"({', '.join(smtp_missing)}). Ajoute ces variables dans `.env`."
            )
    
    # --- Historique ---
    st.markdown("""
    <div class="step-card">
        <h3><span class="step-number">3</span> Historique recent</h3>
    </div>
    """, unsafe_allow_html=True)

    _render_audit_history()

    latest_excel_path, latest_excel_project_name = _get_latest_excel_info()
    if latest_excel_path and latest_excel_path.exists():
        st.caption(f"Dernier rapport disponible : {latest_excel_project_name}")
        with open(latest_excel_path, "rb") as f:
            st.download_button(
                label="Telecharger le dernier rapport Excel",
                data=f.read(),
                file_name=f"rapport_{latest_excel_project_name}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="download_latest_excel",
            )

    # --- Lancement ---
    st.markdown("<br>", unsafe_allow_html=True)

    if st.button("Lancer l'audit", type="primary", use_container_width=True):
        if not selected_path:
            st.error("Selectionne un dossier.")
            return
        if not any((run_operation, run_patrimoine, run_bilan, run_compte_resultat, run_lots)):
            st.error("Selectionne au moins un onglet Excel a generer.")
            return
        if send_email and not email_to:
            st.error("Renseigne un email de notification pour activer l'envoi.")
            return
        if send_email and not smtp_configured:
            st.error(
                "Impossible d'envoyer l'email: configuration SMTP manquante "
                f"({', '.join(smtp_missing)})."
            )
            return

        project_path = selected_path
        project_id = _slugify(project_path)
        project_name = project_path.rstrip("/").rsplit("/", 1)[-1]
        project_dir = OUTPUT_DIR / project_id
        started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        started_perf = time.perf_counter()

        # Mandats avant extract quand bilan activé (nécessaire pour les SIRENs)
        mandats_before_extract = run_mandats and run_bilan

        execution_plan = [("pipeline", "Pipeline Dropbox")]
        if mandats_before_extract:
            execution_plan.append(("mandats", "Mandats Pappers"))
        execution_plan.append(("extract", "Extraction LLM"))
        if run_mandats and not mandats_before_extract:
            execution_plan.append(("mandats", "Mandats Pappers"))
        execution_plan.append(("fill", "Generation Excel"))
        if send_email and email_to:
            execution_plan.append(("email", "Envoi email"))
        total_steps = len(execution_plan)
        completed_steps = 0

        # Setup logging capture
        log_handler = StreamlitLogHandler()
        log_handler.setFormatter(logging.Formatter("%(asctime)s — %(message)s", datefmt="%H:%M:%S"))
        root_logger = logging.getLogger()
        root_logger.addHandler(log_handler)
        root_logger.setLevel(logging.INFO)

        status = st.status("Audit en cours...", expanded=True)
        results_summary = {}
        progress_bar = st.progress(0.0, text=f"Etape 1/{total_steps} — {execution_plan[0][1]}")

        # --- ETAPE 1 : Pipeline Dropbox ---
        try:
            status.update(label="Pipeline Dropbox...")
            st.write("**Pipeline** — Sync Dropbox + extraction texte...")
            import pipeline as pipeline_module
            pipeline_module = importlib.reload(pipeline_module)
            pipeline_module.run(project_path, selected_audit_folder=selected_audit_subfolder)

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
        completed_steps += 1
        progress_bar.progress(
            completed_steps / total_steps,
            text=f"Etape {completed_steps}/{total_steps} — Pipeline Dropbox",
        )

        def _run_mandats_step():
            nonlocal completed_steps
            progress_bar.progress(
                completed_steps / total_steps,
                text=f"Etape {completed_steps + 1}/{total_steps} — Mandats Pappers",
            )
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
                    results_summary["mandats"] = f"{n_societes} societes, {n_persons} personnes"
                    st.write(f"Mandats : {n_societes} societes pour {n_persons} personnes")
                else:
                    results_summary["mandats"] = "OK"
            except Exception as e:
                st.error(f"Mandats : {e}")
                results_summary["mandats"] = f"ERREUR : {e}"
            completed_steps += 1
            progress_bar.progress(
                completed_steps / total_steps,
                text=f"Etape {completed_steps}/{total_steps} — Mandats Pappers",
            )

        # --- ETAPE 2 : Mandats Pappers (si bilan activé, mandats d'abord pour les SIRENs) ---
        if mandats_before_extract:
            _run_mandats_step()

        # --- ETAPE suivante : Extraction LLM ---
        progress_bar.progress(
            completed_steps / total_steps,
            text=f"Etape {completed_steps + 1}/{total_steps} — Extraction LLM",
        )
        try:
            status.update(label="Extraction LLM...")
            st.write("**Extraction LLM** en cours...")
            from extraction.extract_structured import run as run_extraction
            run_extraction(
                project_id,
                include_operateur=run_operation,
                include_patrimoine=run_patrimoine,
                include_bilan=run_bilan,
                include_compte_resultat=run_compte_resultat,
                include_lots=run_lots,
            )

            results_path = project_dir / "extraction_results.json"
            if results_path.exists():
                data = json.loads(results_path.read_text(encoding="utf-8"))
                summary = data.get("summary", {})
                answered = summary.get("answered", 0)
                total = summary.get("total", 0)
                asked_globals = summary.get("asked_global_fields")
                asked_person = summary.get("asked_person_fields")
                asked_company = summary.get("asked_company_fields")
                if all(isinstance(v, int) for v in (asked_globals, asked_person, asked_company)):
                    results_summary["extract"] = (
                        f"{answered}/{total} champs "
                        f"(global {summary.get('answered_global_fields', 0)}/{asked_globals}, "
                        f"personne {summary.get('answered_person_fields', 0)}/{asked_person}, "
                        f"societe {summary.get('answered_company_fields', 0)}/{asked_company})"
                    )
                else:
                    results_summary["extract"] = f"{answered}/{total} champs"
                st.write(f"Extraction : {results_summary['extract']}")
            else:
                results_summary["extract"] = "OK"
        except Exception as e:
            st.error(f"Extraction : {e}")
            results_summary["extract"] = f"ERREUR : {e}"
        completed_steps += 1
        progress_bar.progress(
            completed_steps / total_steps,
            text=f"Etape {completed_steps}/{total_steps} — Extraction LLM",
        )

        # --- Mandats Pappers (si bilan non activé, après extract comme avant) ---
        if run_mandats and not mandats_before_extract:
            _run_mandats_step()

        # --- ETAPE 4 : Excel ---
        excel_path = None
        progress_bar.progress(
            completed_steps / total_steps,
            text=f"Etape {completed_steps + 1}/{total_steps} — Generation Excel",
        )
        try:
            status.update(label="Generation Excel...")
            st.write("**Excel** — Generation du rapport...")
            from sheets.excel_filler import fill_excel

            results_path = project_dir / "extraction_results.json"
            if results_path.exists():
                extraction_data = json.loads(results_path.read_text(encoding="utf-8"))
                questions_data = load_questions_config(ROOT_DIR / "config")

                person_folder_map = extraction_data.get("person_folders")
                pappers_mandats = None
                mandats_path = project_dir / "mandats_results.json"
                if mandats_path.exists():
                    mandats_data = json.loads(mandats_path.read_text(encoding="utf-8"))
                    pappers_mandats = mandats_data.get("societes_par_personne")

                bilan_results = None
                bilan_results_path = project_dir / "bilan_results.json"
                if run_bilan and bilan_results_path.exists():
                    bilan_results = json.loads(bilan_results_path.read_text(encoding="utf-8"))

                fields = filter_fields_for_excel_tabs(
                    questions_data["fields"],
                    include_operation=run_operation,
                    include_patrimoine=run_patrimoine,
                    include_bilan=run_bilan,
                    include_compte_resultat=run_compte_resultat,
                    include_lots=run_lots,
                )
                excel_path = fill_excel(
                    results=extraction_data["results"],
                    fields=fields,
                    output_dir=project_dir,
                    person_folder_map=person_folder_map,
                    pappers_mandats=pappers_mandats,
                    bilan_results=bilan_results,
                    include_operation=run_operation,
                    include_patrimoine=run_patrimoine,
                    include_bilan=run_bilan,
                    include_compte_resultat=run_compte_resultat,
                    include_lots=run_lots,
                )
                results_summary["fill"] = "rapport.xlsx"
                st.write("Excel : rapport.xlsx genere")
            else:
                st.warning("extraction_results.json manquant — Excel non genere")
                results_summary["fill"] = "SKIP (pas de resultats)"
        except Exception as e:
            st.error(f"Excel : {e}")
            results_summary["fill"] = f"ERREUR : {e}"
        completed_steps += 1
        progress_bar.progress(
            completed_steps / total_steps,
            text=f"Etape {completed_steps}/{total_steps} — Generation Excel",
        )

        # --- ETAPE 5 : Email ---
        if send_email and email_to and excel_path and excel_path.exists():
            progress_bar.progress(
                completed_steps / total_steps,
                text=f"Etape {completed_steps + 1}/{total_steps} — Envoi email",
            )
            try:
                status.update(label="Envoi email...")
                sent = _send_email(
                    to=email_to,
                    subject=f"RAIZERS Audit — {project_name}",
                    body=f"Rapport d'audit pour le projet {project_name} ci-joint.",
                    attachment_path=excel_path,
                )
                if sent:
                    results_summary["email"] = f"envoye a {email_to}"
                    st.write(f"Email envoye a {email_to}")
            except Exception as e:
                st.error(f"Email : {e}")
                results_summary["email"] = f"ERREUR : {e}"
            completed_steps += 1
            progress_bar.progress(
                completed_steps / total_steps,
                text=f"Etape {completed_steps}/{total_steps} — Envoi email",
            )

        # --- Resume final ---
        with contextlib.suppress(Exception):
            root_logger.removeHandler(log_handler)

        all_ok = all("ERREUR" not in str(v) for v in results_summary.values())
        duration_seconds = time.perf_counter() - started_perf
        progress_bar.progress(1.0, text=f"Termine en {duration_seconds:.1f}s")
        status.update(
            label="Audit termine" if all_ok else "Audit termine (avec erreurs)",
            state="complete" if all_ok else "error",
            expanded=False,
        )
        _append_audit_history(
            {
                "started_at": started_at,
                "project_name": project_name,
                "project_path": project_path,
                "project_id": project_id,
                "success": all_ok,
                "duration_seconds": round(duration_seconds, 1),
                "options": {
                    "operation": run_operation,
                    "patrimoine": run_patrimoine,
                    "bilan": run_bilan,
                    "compte_resultat": run_compte_resultat,
                    "lots": run_lots,
                    "extract": True,
                    "mandats": run_mandats,
                    "fill": True,
                    "send_email": send_email,
                },
                "summary": results_summary,
            }
        )
        _remember_latest_excel(project_dir / "rapport.xlsx", project_name)

        st.markdown("<br>", unsafe_allow_html=True)

        step_labels = {
            "pipeline": ("\U0001f4c2", "Pipeline"),
            "extract": ("\U0001f916", "Extraction"),
            "mandats": ("\U0001f3e2", "Mandats"),
            "fill": ("\U0001f4ca", "Excel"),
            "email": ("\U0001f4e7", "Email"),
        }

        for step, result in results_summary.items():
            is_ok = "ERREUR" not in str(result) and "SKIP" not in str(result)
            icon, label = step_labels.get(step, ("", step))
            css_class = "result-ok" if is_ok else "result-err"
            st.markdown(
                f'<div class="{css_class}"><strong>{icon} {label}</strong> &mdash; {result}</div>',
                unsafe_allow_html=True,
            )

        # Bouton telechargement Excel
        if excel_path and excel_path.exists():
            st.markdown("<br>", unsafe_allow_html=True)
            with open(excel_path, "rb") as f:
                st.download_button(
                    label="Telecharger le rapport Excel",
                    data=f.read(),
                    file_name=f"rapport_{project_name}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary",
                    use_container_width=True,
                )

        # Logs detailles
        with st.expander("Logs detailles"):
            st.code(log_handler.get_logs(), language="text")
