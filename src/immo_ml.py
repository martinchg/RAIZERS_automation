"""Briques ML/robustes pour fiabiliser le scoring immobilier.

Ce module reste volontairement sans dependance ML lourde pour une V1 :
- construction de features comparables / sujet ;
- detection robuste d'outliers via mediane + MAD ;
- calcul d'un score de coherence prix local.

Il est concu pour etre branche ensuite dans ``tab_immo.py`` sans remplacer
le scoring metier existant.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import log1p
from statistics import median
from typing import Any, Dict, Iterable, List, Optional

from immo_scoring import SubjectProperty, percent_gap


MIN_COMPARABLES_FOR_OUTLIER_FILTER = 5
DEFAULT_OUTLIER_Z_THRESHOLD = 3.5


@dataclass(frozen=True)
class ComparableMLFeatures:
    """Features simples et robustes pour comparer un bien a un comparable."""

    distance_m: Optional[float]
    living_area_gap_pct: Optional[float]
    rooms_gap_abs: Optional[int]
    land_area_gap_pct: Optional[float]
    sale_recency_days: Optional[int]
    price_per_sqm_eur: Optional[float]
    local_price_gap_pct: Optional[float]
    log_distance: Optional[float]
    log_price_per_sqm: Optional[float]

    def to_dict(self) -> Dict[str, Optional[float]]:
        return {
            "distance_m": self.distance_m,
            "living_area_gap_pct": self.living_area_gap_pct,
            "rooms_gap_abs": float(self.rooms_gap_abs) if self.rooms_gap_abs is not None else None,
            "land_area_gap_pct": self.land_area_gap_pct,
            "sale_recency_days": float(self.sale_recency_days) if self.sale_recency_days is not None else None,
            "price_per_sqm_eur": self.price_per_sqm_eur,
            "local_price_gap_pct": self.local_price_gap_pct,
            "log_distance": self.log_distance,
            "log_price_per_sqm": self.log_price_per_sqm,
        }


def _safe_median(values: Iterable[Optional[float]]) -> Optional[float]:
    cleaned = [float(v) for v in values if v is not None]
    if not cleaned:
        return None
    return float(median(cleaned))


def _mad(values: Iterable[Optional[float]], *, center: Optional[float] = None) -> Optional[float]:
    cleaned = [float(v) for v in values if v is not None]
    if not cleaned:
        return None
    center = float(median(cleaned)) if center is None else float(center)
    deviations = [abs(v - center) for v in cleaned]
    return float(median(deviations))


def robust_modified_z_score(value: Optional[float], values: Iterable[Optional[float]]) -> Optional[float]:
    """Renvoie un score robuste de type z-score base sur la MAD."""
    if value is None:
        return None
    center = _safe_median(values)
    if center is None:
        return None
    mad = _mad(values, center=center)
    if mad is None or mad == 0:
        return 0.0
    return 0.6745 * (float(value) - center) / mad


def build_comparable_features(
    subject: SubjectProperty,
    comparable: Dict[str, Any],
    *,
    valuation_date: Optional[date] = None,
    local_median_price_per_sqm: Optional[float] = None,
) -> ComparableMLFeatures:
    """Construit des features numeriques stables pour un comparable."""
    sale_date = comparable.get("sale_date")
    sale_recency_days = None
    if valuation_date is not None and sale_date is not None:
        sale_recency_days = max((valuation_date - sale_date).days, 0)

    distance_m = _to_float(comparable.get("distance_m"))
    price_per_sqm = _to_float(comparable.get("price_per_sqm_eur"))
    living_area_gap_pct = percent_gap(subject.living_area_sqm, _to_float(comparable.get("living_area_sqm")))

    subject_land = _to_float(subject.land_area_sqm)
    comp_land = _to_float(comparable.get("land_area_sqm"))
    land_area_gap_pct = percent_gap(subject_land, comp_land) if subject_land and comp_land else None

    rooms_gap_abs = None
    comp_rooms = _to_int(comparable.get("rooms"))
    if comp_rooms is not None:
        rooms_gap_abs = abs(int(subject.rooms) - comp_rooms)

    local_price_gap_pct = percent_gap(local_median_price_per_sqm, price_per_sqm)

    return ComparableMLFeatures(
        distance_m=distance_m,
        living_area_gap_pct=living_area_gap_pct,
        rooms_gap_abs=rooms_gap_abs,
        land_area_gap_pct=land_area_gap_pct,
        sale_recency_days=sale_recency_days,
        price_per_sqm_eur=price_per_sqm,
        local_price_gap_pct=local_price_gap_pct,
        log_distance=log1p(distance_m) if distance_m is not None and distance_m >= 0 else None,
        log_price_per_sqm=log1p(price_per_sqm) if price_per_sqm is not None and price_per_sqm >= 0 else None,
    )


def annotate_price_outliers(
    comparables: List[Dict[str, Any]],
    *,
    z_threshold: float = DEFAULT_OUTLIER_Z_THRESHOLD,
) -> List[Dict[str, Any]]:
    """Ajoute des metadonnees d'outlier robustes aux comparables.

    La V1 cible le prix au m2, qui est souvent la source principale d'anomalies
    DVF. On pourra ensuite etendre a un score multi-features.
    """
    prices = [comp.get("price_per_sqm_eur") for comp in comparables]

    if len([p for p in prices if p is not None]) < MIN_COMPARABLES_FOR_OUTLIER_FILTER:
        return [dict(comp, ml_outlier_score=None, ml_is_outlier=False) for comp in comparables]

    annotated: List[Dict[str, Any]] = []
    for comp in comparables:
        score = robust_modified_z_score(comp.get("price_per_sqm_eur"), prices)
        is_outlier = abs(score) > z_threshold if score is not None else False
        annotated.append(
            dict(
                comp,
                ml_outlier_score=round(score, 3) if score is not None else None,
                ml_is_outlier=is_outlier,
            )
        )
    return annotated


def exclude_price_outliers(
    comparables: List[Dict[str, Any]],
    *,
    z_threshold: float = DEFAULT_OUTLIER_Z_THRESHOLD,
) -> List[Dict[str, Any]]:
    """Retourne les comparables non aberrants selon le prix au m2."""
    annotated = annotate_price_outliers(comparables, z_threshold=z_threshold)
    retained = [comp for comp in annotated if not comp.get("ml_is_outlier")]
    return retained or annotated


def local_price_coherence_score(
    comparable: Dict[str, Any],
    *,
    local_median_price_per_sqm: Optional[float],
) -> float:
    """Score simple de coherence prix pour enrichir le scoring metier.

    Plus le comparable s'eloigne de la mediane locale en prix/m2, plus le score
    baisse. Le resultat est borne entre 0 et 1.
    """
    price = _to_float(comparable.get("price_per_sqm_eur"))
    if price is None or local_median_price_per_sqm is None or local_median_price_per_sqm <= 0:
        return 0.5

    gap = percent_gap(local_median_price_per_sqm, price)
    if gap is None:
        return 0.5
    if gap <= 0.05:
        return 1.0
    if gap <= 0.10:
        return 0.9
    if gap <= 0.20:
        return 0.7
    if gap <= 0.30:
        return 0.45
    if gap <= 0.40:
        return 0.25
    return 0.0


def _to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None
