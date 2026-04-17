"""immo_scoring.py : types, constantes de scoring et ComparableScorer."""

from __future__ import annotations

import re
import unicodedata
from datetime import date
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


PropertyType = Literal["appartement", "maison"]


class SubjectProperty(BaseModel):
    normalized_address: str
    latitude: float
    longitude: float
    street_name: Optional[str] = None
    city: Optional[str] = None
    postcode: Optional[str] = None
    property_type: PropertyType
    living_area_sqm: float
    rooms: int
    land_area_sqm: Optional[float] = None
    assumed_condition: str = "recent_bon_tres_bon_etat"


# -----------------------------------------------------------------------------
# Scoring constants
# -----------------------------------------------------------------------------

COMMON_RECENCY_SCORE_BANDS = (
    (90, 15),
    (180, 13),
    (270, 11),
    (365, 8),
    (540, 4),
    (730, 1),
)

COMMON_ROOMS_SCORE_MAP = {
    0: 20,
    1: 6,
    2: 0,
}

PREMIUM_PRICE_PER_SQM_SCORE_BANDS = (
    (0.80, 0),
    (1.00, 5),
    (1.10, 10),
    (1.20, 15),
)

LOW_PRICE_PER_SQM_PENALTY_BANDS = (
    (0.50, -18),
    (0.60, -12),
    (0.70, -8),
    (0.80, -4),
)

PREMIUM_CONDITION_VALUES = {"recent_bon_tres_bon_etat"}

MIN_VALID_COMPARABLE_SCORE = 45

COMPARABLE_RULES: Dict[str, Dict[str, Any]] = {
    "appartement": {
        "primary_min_score": 70,
        "fallback_min_score": MIN_VALID_COMPARABLE_SCORE,
        "distance_score_bands": (
            (50, 35),
            (100, 30),
            (200, 21),
            (350, 14),
            (500, 7),
            (800, 2),
        ),
        "surface_score_bands": (
            (0.05, 30),
            (0.10, 25),
            (0.15, 20),
            (0.20, 12),
            (0.25, 5),
        ),
        "recency_score_bands": COMMON_RECENCY_SCORE_BANDS,
        "rooms_score_map": COMMON_ROOMS_SCORE_MAP,
    },
    "maison": {
        "primary_min_score": 60,
        "fallback_min_score": MIN_VALID_COMPARABLE_SCORE,
        "distance_score_bands": (
            (100, 25),
            (250, 21),
            (500, 16),
            (800, 10),
            (1200, 5),
            (2000, 2),
        ),
        "surface_score_bands": (
            (0.08, 20),
            (0.15, 16),
            (0.25, 10),
            (0.35, 4),
        ),
        "land_score_bands": (
            (0.10, 20),
            (0.20, 16),
            (0.35, 10),
            (0.50, 5),
            (0.80, 2),
        ),
        "recency_score_bands": COMMON_RECENCY_SCORE_BANDS,
        "rooms_score_map": COMMON_ROOMS_SCORE_MAP,
    },
}

MICRO_LOCATION_STOPWORDS = {
    "rue",
    "avenue",
    "av",
    "avn",
    "boulevard",
    "bd",
    "place",
    "chemin",
    "ch",
    "route",
    "allee",
    "all",
    "impasse",
    "imp",
    "quai",
    "cours",
    "passage",
    "square",
    "residence",
    "lotissement",
    "lieu",
    "dit",
    "ld",
    "le",
    "la",
    "les",
    "de",
    "du",
    "des",
    "d",
    "l",
}


def normalize_micro_location(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = [
        token
        for token in text.split()
        if token and not token.isdigit() and token not in MICRO_LOCATION_STOPWORDS
    ]
    if not tokens:
        return None
    return " ".join(tokens)


def percent_gap(reference: Optional[float], value: Optional[float]) -> Optional[float]:
    if reference is None or value is None or reference <= 0:
        return None
    return abs(value - reference) / reference


class ComparableScorer:
    def score(
        self,
        subject: SubjectProperty,
        comp: Dict[str, Any],
        reference_date: Optional[date] = None,
    ) -> float:
        rules = COMPARABLE_RULES[subject.property_type]
        score = 0
        score += self._score_distance(subject, comp, rules)
        score += self._score_micro_location(subject, comp)
        score += self._score_surface(subject, comp, rules)
        score += self._score_recency(comp, rules, reference_date=reference_date)
        score += self._score_rooms(subject, comp, rules)

        if subject.property_type == "maison":
            score += self._score_land(subject, comp, rules)

        return round(float(score), 2)

    def price_per_sqm_bonus(
        self,
        subject: SubjectProperty,
        comp: Dict[str, Any],
        *,
        local_median_price_per_sqm: Optional[float],
    ) -> int:
        if subject.assumed_condition not in PREMIUM_CONDITION_VALUES:
            return 0

        comparable_price_per_sqm = comp.get("price_per_sqm_eur")
        if (
            comparable_price_per_sqm is None
            or local_median_price_per_sqm is None
            or local_median_price_per_sqm <= 0
        ):
            return 0

        relative_price_level = comparable_price_per_sqm / local_median_price_per_sqm
        if relative_price_level < 1:
            return 0

        bonus = 0
        for threshold, points in PREMIUM_PRICE_PER_SQM_SCORE_BANDS:
            if relative_price_level >= threshold:
                bonus = points
        return bonus

    def price_per_sqm_penalty(
        self,
        comp: Dict[str, Any],
        *,
        local_median_price_per_sqm: Optional[float],
    ) -> int:
        comparable_price_per_sqm = comp.get("price_per_sqm_eur")
        if (
            comparable_price_per_sqm is None
            or local_median_price_per_sqm is None
            or local_median_price_per_sqm <= 0
        ):
            return 0

        relative_price_level = comparable_price_per_sqm / local_median_price_per_sqm
        for threshold, penalty in LOW_PRICE_PER_SQM_PENALTY_BANDS:
            if relative_price_level <= threshold:
                return penalty
        return 0

    def _score_distance(
        self,
        subject: SubjectProperty,
        comp: Dict[str, Any],
        rules: Dict[str, Any],
    ) -> int:
        distance_m = comp.get("distance_m")
        if distance_m is None:
            return 0
        return self._score_from_bands(distance_m, rules["distance_score_bands"])

    def _score_micro_location(self, subject: SubjectProperty, comp: Dict[str, Any]) -> int:
        distance_m = comp.get("distance_m")
        if distance_m is None:
            return 0

        same_micro_location = self._is_same_micro_location(subject, comp)
        subject_key = self._subject_micro_location_key(subject)
        comp_key = comp.get("_micro_location_key")

        if subject.property_type == "appartement":
            if same_micro_location and distance_m <= 100:
                return 15
            if same_micro_location and distance_m <= 300:
                return 10
            if same_micro_location and distance_m <= 500:
                return 6
            if (not subject_key or not comp_key) and distance_m <= 30:
                return 4
            return 0

        if same_micro_location and distance_m <= 300:
            return 10
        if same_micro_location and distance_m <= 800:
            return 6
        if distance_m <= 50:
            return 3
        return 0

    def _score_surface(
        self,
        subject: SubjectProperty,
        comp: Dict[str, Any],
        rules: Dict[str, Any],
    ) -> int:
        surface_gap = percent_gap(subject.living_area_sqm, comp.get("living_area_sqm"))
        if surface_gap is None:
            return 0
        return self._score_from_bands(surface_gap, rules["surface_score_bands"])

    def _score_land(
        self,
        subject: SubjectProperty,
        comp: Dict[str, Any],
        rules: Dict[str, Any],
    ) -> int:
        land_gap = percent_gap(subject.land_area_sqm, comp.get("land_area_sqm"))
        if land_gap is None:
            return 0
        return self._score_from_bands(land_gap, rules["land_score_bands"])

    def _score_recency(
        self,
        comp: Dict[str, Any],
        rules: Dict[str, Any],
        *,
        reference_date: Optional[date] = None,
    ) -> int:
        sale_age_days = self._sale_age_days(comp, reference_date=reference_date)
        if sale_age_days is None:
            return 0
        return self._score_from_bands(sale_age_days, rules["recency_score_bands"])

    def _score_rooms(
        self,
        subject: SubjectProperty,
        comp: Dict[str, Any],
        rules: Dict[str, Any],
    ) -> int:
        rooms_delta = self._rooms_delta(subject, comp)
        if rooms_delta is None:
            return 0
        return rules["rooms_score_map"].get(rooms_delta, 0)

    def _sale_age_days(
        self,
        comp: Dict[str, Any],
        *,
        reference_date: Optional[date] = None,
    ) -> Optional[int]:
        sale_date = comp.get("sale_date")
        if sale_date is None:
            return None
        anchor_date = reference_date or date.today()
        days = (anchor_date - sale_date).days
        if days < 0:
            return None
        return days

    def _rooms_delta(self, subject: SubjectProperty, comp: Dict[str, Any]) -> Optional[int]:
        comparable_rooms = comp.get("rooms")
        if comparable_rooms is None:
            return None
        return int(comparable_rooms) - int(subject.rooms)

    def _is_same_micro_location(self, subject: SubjectProperty, comp: Dict[str, Any]) -> bool:
        subject_key = self._subject_micro_location_key(subject)
        comp_key = comp.get("_micro_location_key")
        return bool(subject_key and comp_key and subject_key == comp_key)

    @staticmethod
    def _subject_micro_location_key(subject: SubjectProperty) -> Optional[str]:
        return normalize_micro_location(subject.street_name or subject.normalized_address)

    @staticmethod
    def _score_from_bands(value: float, bands: Any) -> int:
        for max_value, points in bands:
            if value <= max_value:
                return points
        return 0
