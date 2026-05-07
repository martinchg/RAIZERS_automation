from __future__ import annotations

import json
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, ValidationError

ROOT_DIR = Path(__file__).parent.resolve()

import sys

if str(ROOT_DIR / "src") not in sys.path:
    sys.path.insert(0, str(ROOT_DIR / "src"))

from core.runtime_config import configure_environment
from audit_service import list_audit_subfolders, list_projects, read_manifest_stats, slugify
from export_service import build_report_filename, generate_excel_report, get_export_status
from financial_service import build_financial_payload, extract_selected_financials
from immo_scoring import ComparableScorer
from operation_service import build_operation_payload
from patrimoine_service import build_patrimoine_payload, build_people_by_folder_from_selection
from project_catalog_service import build_project_catalog, save_project_catalog
from project_refresh_service import diff_manifests, refresh_project_state
from scraping_cache_service import load_scraping_cache, save_scraping_cache
from scraping_pipeline import SCRAPER_KEYS, run_scraping_pipeline
from scraping_service import build_scraping_report_filename, generate_scraping_excel_report
from tab_immo import ComparablePipeline, ComparableRequest, DVFClient, GeocoderClient, get_address_suggestions

configure_environment(ROOT_DIR)

app = FastAPI(title="RAIZERS API", version="0.1.0")


def _get_allowed_origins() -> list[str]:
    raw_origins = os.environ.get("RAIZERS_ALLOWED_ORIGINS", "").strip()
    if raw_origins:
        return [origin.strip() for origin in raw_origins.split(",") if origin.strip()]
    return [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ComparePayload(BaseModel):
    address: str = Field(..., min_length=5)
    property_type: Literal["appartement", "maison"]
    living_area_sqm: float = Field(..., gt=0)
    rooms: int = Field(..., ge=1, le=20)
    land_area_sqm: float | None = Field(None, ge=0)
    search_radius_m: int = Field(..., ge=50, le=5000)
    api_min_year: int | None = Field(None, ge=2000, le=2100)


class PipelineStartPayload(BaseModel):
    project_path: str = Field(..., min_length=3)
    audit_folder: str | None = None


class PipelineRefreshPayload(BaseModel):
    project_id: str = Field(..., min_length=3)
    project_path: str = Field(..., min_length=3)
    audit_folder: str | None = None


class OperationExtractPayload(BaseModel):
    project_id: str = Field(..., min_length=3)
    include_operateur: bool = True
    include_patrimoine: bool = True
    include_lots: bool = True


class FinancialExtractPayload(BaseModel):
    project_id: str = Field(..., min_length=3)
    selections: list[dict[str, Any]] | None = None


class PatrimoineExtractPayload(BaseModel):
    project_id: str = Field(..., min_length=3)
    people: list[dict[str, Any]] = Field(default_factory=list)


class ExportGeneratePayload(BaseModel):
    project_id: str = Field(..., min_length=3)
    tabs: list[str] = Field(default_factory=list)
    immo_result: dict[str, Any] | None = None


class ScrapingExportPayload(BaseModel):
    results: list[dict[str, Any]] = Field(default_factory=list)
    property_type: Literal["appartement", "maison"] | None = None
    filename: str | None = None
    project_id: str | None = None


class ScrapingRunPayload(BaseModel):
    project_id: str | None = None
    address: str = Field(..., min_length=5)
    property_type: Literal["appartement", "maison"]
    living_area_sqm: float = Field(..., gt=0)
    rooms: int = Field(..., ge=1, le=20)
    land_area_sqm: float | None = Field(None, ge=0)
    city: str | None = None
    postal_code: str | None = None
    department_code: str | None = None
    scrapers: list[str] = Field(default_factory=lambda: list(SCRAPER_KEYS))

    nb_chambres: int | None = Field(None, ge=0, le=20)
    nb_salles_bain: int | None = Field(None, ge=0, le=20)
    nb_niveaux: int | None = Field(None, ge=0, le=20)
    etage: int | None = Field(None, ge=0, le=200)
    nb_etages_immeuble: int | None = Field(None, ge=0, le=200)
    ascenseur: bool = False
    balcon: bool = False
    surface_balcon: float | None = Field(None, ge=0)
    terrasse: bool = False
    surface_terrasse: float | None = Field(None, ge=0)
    nb_caves: int | None = Field(None, ge=0, le=20)
    nb_places_parking: int | None = Field(None, ge=0, le=20)
    nb_chambres_service: int | None = Field(None, ge=0, le=20)
    annee_construction: int | None = Field(None, ge=1800, le=2100)
    etat_bien: str | None = None
    surface_encore_constructible: float | None = Field(None, ge=0)
    apify_api_token: str | None = None
    meilleursagents_email: str | None = None
    meilleursagents_password: str | None = None


class AuditJobStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, dict[str, Any]] = {}

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._jobs[payload["job_id"]] = payload
            return dict(payload)

    def update(self, job_id: str, **fields: Any) -> dict[str, Any]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            job.update(fields)
            return dict(job)

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return dict(job) if job is not None else None


job_store = AuditJobStore()

PIPELINE_STEP_DEFS = [
    {"key": "sync_dropbox", "label": "Synchronisation Dropbox"},
    {"key": "scope_files", "label": "Qualification des fichiers"},
    {"key": "ingest_chunk", "label": "Ingestion et chunking"},
    {"key": "index_manifest", "label": "Indexation locale"},
    {"key": "project_catalog", "label": "Mise à jour du catalogue"},
]


def _build_pipeline_steps(
    current_key: str | None = None,
    *,
    detail: str | None = None,
) -> list[dict[str, Any]]:
    current_index = None
    if current_key:
        current_index = next(
            (index for index, step in enumerate(PIPELINE_STEP_DEFS) if step["key"] == current_key),
            None,
        )

    steps: list[dict[str, Any]] = []
    for index, step in enumerate(PIPELINE_STEP_DEFS):
        if current_index is None:
            status = "pending"
        elif index < current_index:
            status = "done"
        elif index == current_index:
            status = "running"
        else:
            status = "pending"

        item = {
            "key": step["key"],
            "label": step["label"],
            "status": status,
        }
        if step["key"] == current_key and detail:
            item["detail"] = detail
        steps.append(item)

    return steps


def _mark_pipeline_done_steps() -> list[dict[str, Any]]:
    return [
        {
            "key": step["key"],
            "label": step["label"],
            "status": "done",
        }
        for step in PIPELINE_STEP_DEFS
    ]


def _update_pipeline_progress(job_id: str, payload: dict[str, Any]) -> None:
    step_key = payload["step_key"]
    step_label = payload["step_label"]
    detail = payload.get("detail")
    step_index = int(payload["step_index"])
    total_steps = int(payload["total_steps"]) + 1

    job_store.update(
        job_id,
        stage=step_key,
        stage_label=step_label,
        current_step=step_index,
        total_steps=total_steps,
        progress_ratio=step_index / total_steps,
        pipeline_steps=_build_pipeline_steps(step_key, detail=detail),
    )


def _serialize_result(result: dict[str, Any]) -> dict[str, Any]:
    subject = result.get("subject") or {}
    statistics = result.get("statistics") or {}
    comparables = result.get("comparables") or []

    return {
        "subject": {
            "normalized_address": subject.get("normalized_address"),
            "property_type": subject.get("property_type"),
            "living_area_sqm": subject.get("living_area_sqm"),
            "rooms": subject.get("rooms"),
            "land_area_sqm": subject.get("land_area_sqm"),
            "city": subject.get("city"),
            "postcode": subject.get("postcode"),
            "latitude": subject.get("latitude"),
            "longitude": subject.get("longitude"),
        },
        "statistics": statistics,
        "comparables": comparables,
    }


def _now_timestamp() -> float:
    return time.time()


def _run_pipeline_job(job_id: str, project_path: str, audit_folder: str | None) -> None:
    try:
        job_store.update(
            job_id,
            status="running",
            stage="queued",
            stage_label="Préparation du job",
            started_at=_now_timestamp(),
        )

        import pipeline as pipeline_module

        manifest = pipeline_module.run(
            project_path,
            selected_audit_folder=audit_folder,
            progress_callback=lambda payload: _update_pipeline_progress(job_id, payload),
        )
        if not manifest:
            raise ValueError("Aucun fichier pertinent n'a été synchronisé ou ingéré pour ce dossier.")
        project_id = slugify(project_path)
        stats = read_manifest_stats(project_id, ROOT_DIR)
        manifest_path = ROOT_DIR / "output" / project_id / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else manifest
        job_store.update(
            job_id,
            stage="project_catalog",
            stage_label="Mise à jour du catalogue",
            current_step=5,
            total_steps=5,
            progress_ratio=1.0,
            pipeline_steps=_build_pipeline_steps(
                "project_catalog",
                detail="Détection des personnes, sociétés et bilans",
            ),
        )
        refresh_state = refresh_project_state(
            project_id,
            manifest,
            {
                "added": [],
                "removed": [],
                "modified": [],
                "changed_paths": [],
                "summary": {
                    "added": 0,
                    "removed": 0,
                    "modified": 0,
                    "changed": 0,
                    "people_related": False,
                    "financial_related": False,
                },
            },
        )

        job_store.update(
            job_id,
            status="done",
            stage="done",
            stage_label="Terminé",
            current_step=5,
            total_steps=5,
            progress_ratio=1.0,
            pipeline_steps=_mark_pipeline_done_steps(),
            finished_at=_now_timestamp(),
            result={
                "project_id": project_id,
                "project_path": project_path,
                "audit_folder": audit_folder,
                "stats": stats,
                **refresh_state,
            },
        )
    except Exception as exc:
        job_store.update(
            job_id,
            status="error",
            stage="error",
            finished_at=_now_timestamp(),
            error=str(exc),
        )


def _run_pipeline_refresh_job(
    job_id: str,
    project_id: str,
    project_path: str,
    audit_folder: str | None,
) -> None:
    try:
        job_store.update(
            job_id,
            status="running",
            stage="queued",
            stage_label="Préparation du refresh",
            started_at=_now_timestamp(),
        )

        project_output_dir = ROOT_DIR / "output" / project_id
        previous_manifest_path = project_output_dir / "manifest.json"
        previous_manifest = {}
        if previous_manifest_path.exists():
            previous_manifest = json.loads(previous_manifest_path.read_text(encoding="utf-8"))

        import pipeline as pipeline_module

        next_manifest = pipeline_module.run(
            project_path,
            selected_audit_folder=audit_folder,
            progress_callback=lambda payload: _update_pipeline_progress(job_id, payload),
        )
        if not next_manifest:
            raise ValueError("Aucun fichier pertinent n'a été synchronisé ou ingéré pour ce dossier.")
        manifest_diff = diff_manifests(previous_manifest, next_manifest or {})
        job_store.update(
            job_id,
            stage="project_catalog",
            stage_label="Analyse des changements",
            current_step=5,
            total_steps=5,
            progress_ratio=1.0,
            pipeline_steps=_build_pipeline_steps(
                "project_catalog",
                detail="Comparaison des manifests et refresh du catalogue",
            ),
        )
        refresh_state = refresh_project_state(project_id, next_manifest or {}, manifest_diff)
        stats = read_manifest_stats(project_id, ROOT_DIR)

        job_store.update(
            job_id,
            status="done",
            stage="done",
            stage_label="Terminé",
            current_step=5,
            total_steps=5,
            progress_ratio=1.0,
            pipeline_steps=_mark_pipeline_done_steps(),
            finished_at=_now_timestamp(),
            result={
                "project_id": project_id,
                "project_path": project_path,
                "audit_folder": audit_folder,
                "stats": stats,
                "manifest_diff": manifest_diff,
                **refresh_state,
            },
        )
    except Exception as exc:
        job_store.update(
            job_id,
            status="error",
            stage="error",
            finished_at=_now_timestamp(),
            error=str(exc),
        )


def _run_operation_extract_job(
    job_id: str,
    project_id: str,
    include_operateur: bool,
    include_patrimoine: bool,
    include_lots: bool,
) -> None:
    try:
        job_store.update(
            job_id,
            status="running",
            stage="extract_operation",
            started_at=_now_timestamp(),
        )

        from extraction.extract_structured import run as run_extraction

        run_extraction(
            project_id,
            include_operateur=include_operateur,
            include_patrimoine=include_patrimoine,
            include_bilan=False,
            include_compte_resultat=False,
            include_lots=include_lots,
        )

        job_store.update(
            job_id,
            status="done",
            stage="done",
            finished_at=_now_timestamp(),
            result=build_operation_payload(project_id),
        )
    except Exception as exc:
        job_store.update(
            job_id,
            status="error",
            stage="error",
            finished_at=_now_timestamp(),
            error=str(exc),
        )


def _run_financial_extract_job(
    job_id: str,
    project_id: str,
    selections: list[dict[str, Any]] | None,
) -> None:
    try:
        job_store.update(
            job_id,
            status="running",
            stage="extract_financial",
            started_at=_now_timestamp(),
        )

        job_store.update(
            job_id,
            status="done",
            stage="done",
            finished_at=_now_timestamp(),
            result=extract_selected_financials(project_id, selections=selections),
        )
    except Exception as exc:
        job_store.update(
            job_id,
            status="error",
            stage="error",
            finished_at=_now_timestamp(),
            error=str(exc),
        )


def _run_patrimoine_extract_job(
    job_id: str,
    project_id: str,
    people: list[dict[str, Any]],
) -> None:
    try:
        job_store.update(
            job_id,
            status="running",
            stage="extract_patrimoine",
            started_at=_now_timestamp(),
        )

        from mandats_pipeline import run as run_mandats

        people_by_folder = build_people_by_folder_from_selection(people)
        run_mandats(project_id, people_by_folder=people_by_folder)

        job_store.update(
            job_id,
            status="done",
            stage="done",
            finished_at=_now_timestamp(),
            result=build_patrimoine_payload(project_id),
        )
    except Exception as exc:
        job_store.update(
            job_id,
            status="error",
            stage="error",
            finished_at=_now_timestamp(),
            error=str(exc),
        )


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/audit/projects")
def audit_projects() -> dict[str, Any]:
    try:
        return list_projects()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Impossible de charger les projets Dropbox: {exc}") from exc


@app.get("/api/audit/subfolders")
def audit_subfolders(project_path: str = Query(..., min_length=3)) -> dict[str, Any]:
    try:
        return {
            "project_path": project_path,
            "items": list_audit_subfolders(project_path),
        }
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Impossible de charger les sous-dossiers d'audit: {exc}") from exc


@app.post("/api/audit/pipeline/start")
def start_audit_pipeline(payload: PipelineStartPayload) -> dict[str, Any]:
    project_id = slugify(payload.project_path)
    job_id = uuid.uuid4().hex
    job = job_store.create(
        {
            "job_id": job_id,
            "status": "queued",
            "stage": "queued",
            "stage_label": "En attente",
            "current_step": 0,
            "total_steps": len(PIPELINE_STEP_DEFS),
            "progress_ratio": 0,
            "pipeline_steps": _build_pipeline_steps(),
            "project_id": project_id,
            "project_path": payload.project_path,
            "audit_folder": payload.audit_folder,
            "created_at": _now_timestamp(),
            "started_at": None,
            "finished_at": None,
            "error": None,
            "result": None,
        }
    )

    worker = threading.Thread(
        target=_run_pipeline_job,
        args=(job_id, payload.project_path, payload.audit_folder),
        daemon=True,
    )
    worker.start()
    return job


@app.post("/api/audit/pipeline/refresh")
def refresh_audit_pipeline(payload: PipelineRefreshPayload) -> dict[str, Any]:
    job_id = uuid.uuid4().hex
    job = job_store.create(
        {
            "job_id": job_id,
            "status": "queued",
            "stage": "queued",
            "stage_label": "En attente",
            "current_step": 0,
            "total_steps": len(PIPELINE_STEP_DEFS),
            "progress_ratio": 0,
            "pipeline_steps": _build_pipeline_steps(),
            "job_type": "refresh_pipeline",
            "project_id": payload.project_id,
            "project_path": payload.project_path,
            "audit_folder": payload.audit_folder,
            "created_at": _now_timestamp(),
            "started_at": None,
            "finished_at": None,
            "error": None,
            "result": None,
        }
    )

    worker = threading.Thread(
        target=_run_pipeline_refresh_job,
        args=(job_id, payload.project_id, payload.project_path, payload.audit_folder),
        daemon=True,
    )
    worker.start()
    return job


@app.get("/api/audit/jobs/{job_id}")
def audit_job_status(job_id: str) -> dict[str, Any]:
    job = job_store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job introuvable.")
    return job


@app.get("/api/audit/projects/{project_id}/operation")
def operation_results(project_id: str) -> dict[str, Any]:
    try:
        return build_operation_payload(project_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Impossible de charger les résultats opération: {exc}") from exc


@app.post("/api/audit/extract/operation/start")
def start_operation_extract(payload: OperationExtractPayload) -> dict[str, Any]:
    job_id = uuid.uuid4().hex
    job = job_store.create(
        {
            "job_id": job_id,
            "status": "queued",
            "stage": "queued",
            "job_type": "extract_operation",
            "project_id": payload.project_id,
            "created_at": _now_timestamp(),
            "started_at": None,
            "finished_at": None,
            "error": None,
            "result": None,
        }
    )

    worker = threading.Thread(
        target=_run_operation_extract_job,
        args=(
            job_id,
            payload.project_id,
            payload.include_operateur,
            payload.include_patrimoine,
            payload.include_lots,
        ),
        daemon=True,
    )
    worker.start()
    return job


@app.get("/api/audit/projects/{project_id}/financial")
def financial_results(project_id: str) -> dict[str, Any]:
    try:
        return build_financial_payload(project_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Impossible de charger les résultats financiers: {exc}") from exc


@app.post("/api/audit/extract/financial/start")
def start_financial_extract(payload: FinancialExtractPayload) -> dict[str, Any]:
    job_id = uuid.uuid4().hex
    job = job_store.create(
        {
            "job_id": job_id,
            "status": "queued",
            "stage": "queued",
            "job_type": "extract_financial",
            "project_id": payload.project_id,
            "created_at": _now_timestamp(),
            "started_at": None,
            "finished_at": None,
            "error": None,
            "result": None,
        }
    )

    worker = threading.Thread(
        target=_run_financial_extract_job,
        args=(job_id, payload.project_id, payload.selections),
        daemon=True,
    )
    worker.start()
    return job


@app.get("/api/audit/projects/{project_id}/patrimoine")
def patrimoine_results(project_id: str) -> dict[str, Any]:
    try:
        return build_patrimoine_payload(project_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Impossible de charger les résultats patrimoine: {exc}") from exc


@app.post("/api/audit/extract/patrimoine/start")
def start_patrimoine_extract(payload: PatrimoineExtractPayload) -> dict[str, Any]:
    job_id = uuid.uuid4().hex
    job = job_store.create(
        {
            "job_id": job_id,
            "status": "queued",
            "stage": "queued",
            "job_type": "extract_patrimoine",
            "project_id": payload.project_id,
            "created_at": _now_timestamp(),
            "started_at": None,
            "finished_at": None,
            "error": None,
            "result": None,
        }
    )

    worker = threading.Thread(
        target=_run_patrimoine_extract_job,
        args=(job_id, payload.project_id, payload.people),
        daemon=True,
    )
    worker.start()
    return job


@app.get("/api/audit/projects/{project_id}/catalog")
def project_catalog(project_id: str) -> dict[str, Any]:
    try:
        return build_project_catalog(project_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Impossible de charger le catalogue projet: {exc}") from exc


@app.get("/api/audit/projects/{project_id}/export")
def export_status(project_id: str) -> dict[str, Any]:
    try:
        return get_export_status(project_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Impossible de charger le statut export: {exc}") from exc


@app.post("/api/audit/export/report")
def generate_export_report(payload: ExportGeneratePayload) -> dict[str, Any]:
    try:
        report_path = generate_excel_report(
            payload.project_id,
            selected_tabs=payload.tabs,
            immo_result=payload.immo_result,
        )
        report_filename = build_report_filename(payload.project_id)
        return {
            "project_id": payload.project_id,
            "report_exists": report_path.exists(),
            "report_filename": report_filename,
            "report_download_url": f"/api/audit/projects/{payload.project_id}/export/report",
            "selected_tabs": payload.tabs,
        }
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Impossible de générer le rapport Excel: {exc}") from exc


@app.get("/api/audit/projects/{project_id}/export/report")
def download_export_report(project_id: str):
    report_path = ROOT_DIR / "output" / project_id / "rapport.xlsx"
    if not report_path.exists():
        raise HTTPException(status_code=404, detail="rapport.xlsx introuvable.")
    report_filename = build_report_filename(project_id)
    return FileResponse(
        path=report_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=report_filename,
    )


OUTPUT_DIR = ROOT_DIR / "output"


@app.get("/api/audit/projects/{project_id}/immo-draft")
def get_immo_draft(project_id: str) -> dict[str, Any]:
    path = OUTPUT_DIR / project_id / "immo_draft.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Lecture immo_draft impossible: {exc}") from exc


@app.post("/api/audit/projects/{project_id}/immo-draft")
def save_immo_draft(project_id: str, draft: dict[str, Any]) -> dict[str, Any]:
    project_dir = OUTPUT_DIR / project_id
    project_dir.mkdir(parents=True, exist_ok=True)
    path = project_dir / "immo_draft.json"
    try:
        path.write_text(json.dumps(draft, ensure_ascii=False, indent=2), encoding="utf-8")
        return {"saved": True}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Sauvegarde immo_draft impossible: {exc}") from exc


@app.get("/api/immo/suggestions")
def immo_suggestions(q: str = Query(..., min_length=3), limit: int = Query(6, ge=1, le=15)) -> dict[str, Any]:
    try:
        suggestions = get_address_suggestions(q, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Impossible de charger les suggestions: {exc}") from exc

    normalized = [
        {
            "id": f"suggestion-{index}",
            "label": item.get("label"),
            "city": item.get("city"),
            "postcode": item.get("postcode"),
            "street": item.get("street"),
            "latitude": item.get("latitude"),
            "longitude": item.get("longitude"),
        }
        for index, item in enumerate(suggestions)
    ]
    return {"items": normalized}


@app.post("/api/immo/compare")
def immo_compare(payload: ComparePayload) -> dict[str, Any]:
    try:
        request = ComparableRequest(**payload.model_dump())
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    pipeline = ComparablePipeline(
        geocoder=GeocoderClient(),
        dvf_client=DVFClient(),
        scorer=ComparableScorer(),
    )

    try:
        result = pipeline.run(request)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Erreur DVF/geocodage: {exc}") from exc

    return _serialize_result(result)


def _run_scraping_job(job_id: str, payload_dict: dict[str, Any]) -> None:
    try:
        job_store.update(job_id, status="running", started_at=_now_timestamp())
        result = run_scraping_pipeline(payload_dict)
        save_scraping_cache(
            payload_dict.get("project_id"),
            result,
            address=payload_dict.get("address"),
        )
        job_store.update(job_id, status="done", finished_at=_now_timestamp(), result=result)
    except ValueError as exc:
        job_store.update(job_id, status="error", finished_at=_now_timestamp(), error=str(exc))
    except Exception as exc:
        job_store.update(job_id, status="error", finished_at=_now_timestamp(), error=f"Erreur scraping: {exc}")


@app.post("/api/scraping/run")
def scraping_run(payload: ScrapingRunPayload) -> dict[str, Any]:
    job_id = uuid.uuid4().hex
    job = job_store.create({
        "job_id": job_id,
        "status": "queued",
        "job_type": "scraping",
        "created_at": _now_timestamp(),
        "started_at": None,
        "finished_at": None,
        "error": None,
        "result": None,
    })
    threading.Thread(
        target=_run_scraping_job,
        args=(job_id, payload.model_dump()),
        daemon=True,
    ).start()
    return job


@app.get("/api/scraping/jobs/{job_id}")
def scraping_job_status(job_id: str) -> dict[str, Any]:
    job = job_store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job introuvable.")
    return job


@app.get("/api/scraping/cache")
def scraping_cache(project_id: str = Query(..., min_length=1), address: str | None = None) -> dict[str, Any]:
    try:
        return load_scraping_cache(project_id, address=address)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Erreur lecture cache scraping: {exc}") from exc


@app.post("/api/scraping/export")
def scraping_export(payload: ScrapingExportPayload) -> Response:
    if not payload.results:
        raise HTTPException(status_code=400, detail="Aucun resultat de scraping a exporter.")

    try:
        excel_bytes = generate_scraping_excel_report(
            payload.results,
            property_type=payload.property_type,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Erreur export scraping: {exc}") from exc

    filename = payload.filename or build_scraping_report_filename(payload.property_type)
    return Response(
        content=excel_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )
