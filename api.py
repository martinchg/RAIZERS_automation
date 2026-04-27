from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ValidationError

ROOT_DIR = Path(__file__).parent.resolve()

import sys

if str(ROOT_DIR / "src") not in sys.path:
    sys.path.insert(0, str(ROOT_DIR / "src"))

from core.runtime_config import configure_environment
from immo_scoring import ComparableScorer
from tab_immo import ComparablePipeline, ComparableRequest, DVFClient, GeocoderClient, get_address_suggestions

configure_environment(ROOT_DIR)

app = FastAPI(title="RAIZERS API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
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


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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
