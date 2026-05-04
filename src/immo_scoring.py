"""immo_scoring.py : types, constantes de scoring et ComparableScorer."""

from __future__ import annotations

import math
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

SIMILARITY_RULES: Dict[str, Dict[str, Any]] = {
    "appartement": {
        "distance_decay_m": 180.0,
        "surface_gap_decay": 0.14,
        "room_density_gap_decay": 0.22,
        "recency_decay_days": 300.0,
        "weights": {
            "distance": 22.0,
            "micro_location": 8.0,
            "very_close_distance": 8.0,
            "surface": 24.0,
            "recency": 12.0,
            "rooms": 26.0,
            "room_density": 10.0,
        },
    },
    "maison": {
        "distance_decay_m": 420.0,
        "surface_gap_decay": 0.18,
        "room_density_gap_decay": 0.26,
        "land_gap_decay": 0.28,
        "recency_decay_days": 360.0,
        "weights": {
            "distance": 24.0,
            "micro_location": 5.0,
            "very_close_distance": 4.0,
            "surface": 22.0,
            "recency": 12.0,
            "rooms": 16.0,
            "room_density": 6.0,
            "land": 15.0,
        },
    },
}

OUTLIER_MIN_SAMPLE_SIZE = 5
OUTLIER_MIN_MAD = 1e-6
OUTLIER_LOW_Z = {
    "appartement": -2.2,
    "maison": -2.0,
}
OUTLIER_HIGH_Z = {
    "appartement": 2.2,
    "maison": 3.6,
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
        rules = SIMILARITY_RULES[subject.property_type]
        weights = rules["weights"]
        score = 0.0
        score += weights["distance"] * self._distance_similarity(subject, comp, rules)
        score += weights["micro_location"] * self._micro_location_similarity(subject, comp)
        score += weights["very_close_distance"] * self._very_close_distance_similarity(subject, comp)
        score += weights["surface"] * self._surface_similarity(subject, comp, rules)
        score += weights["recency"] * self._recency_similarity(comp, rules, reference_date=reference_date)
        score += weights["rooms"] * self._rooms_similarity(subject, comp)
        score += weights["room_density"] * self._room_density_similarity(subject, comp, rules)

        if subject.property_type == "maison":
            score += weights["land"] * self._land_similarity(subject, comp, rules)

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

    def _distance_similarity(
        self,
        subject: SubjectProperty,
        comp: Dict[str, Any],
        rules: Dict[str, Any],
    ) -> float:
        distance_m = comp.get("distance_m")
        if distance_m is None:
            return 0.0
        return self._exp_decay_similarity(distance_m, rules["distance_decay_m"])

    def _micro_location_similarity(self, subject: SubjectProperty, comp: Dict[str, Any]) -> float:
        distance_m = comp.get("distance_m")
        if distance_m is None:
            return 0.0

        same_micro_location = self._is_same_micro_location(subject, comp)
        subject_key = self._subject_micro_location_key(subject)
        comp_key = comp.get("_micro_location_key")

        if subject.property_type == "appartement":
            if same_micro_location and distance_m <= 100:
                return 1.0
            if same_micro_location and distance_m <= 300:
                return 0.7
            if same_micro_location and distance_m <= 500:
                return 0.45
            if (not subject_key or not comp_key) and distance_m <= 30:
                return 0.3
            return 0.0

        if same_micro_location and distance_m <= 300:
            return 1.0
        if same_micro_location and distance_m <= 800:
            return 0.6
        if distance_m <= 50:
            return 0.25
        return 0.0

    def _very_close_distance_similarity(
        self,
        subject: SubjectProperty,
        comp: Dict[str, Any],
    ) -> float:
        distance_m = comp.get("distance_m")
        if distance_m is None:
            return 0.0

        if subject.property_type == "appartement":
            if distance_m <= 20:
                return 1.0
            if distance_m <= 40:
                return 0.65
            if distance_m <= 70:
                return 0.3
            return 0.0

        if distance_m <= 40:
            return 1.0
        if distance_m <= 80:
            return 0.5
        return 0.0

    def _surface_similarity(
        self,
        subject: SubjectProperty,
        comp: Dict[str, Any],
        rules: Dict[str, Any],
    ) -> float:
        surface_gap = percent_gap(subject.living_area_sqm, comp.get("living_area_sqm"))
        if surface_gap is None:
            return 0.0
        return self._exp_decay_similarity(surface_gap, rules["surface_gap_decay"])

    def _land_similarity(
        self,
        subject: SubjectProperty,
        comp: Dict[str, Any],
        rules: Dict[str, Any],
    ) -> float:
        land_gap = percent_gap(subject.land_area_sqm, comp.get("land_area_sqm"))
        if land_gap is None:
            return 0.0
        return self._exp_decay_similarity(land_gap, rules["land_gap_decay"])

    def _recency_similarity(
        self,
        comp: Dict[str, Any],
        rules: Dict[str, Any],
        *,
        reference_date: Optional[date] = None,
    ) -> float:
        sale_age_days = self._sale_age_days(comp, reference_date=reference_date)
        if sale_age_days is None:
            return 0.0
        return self._exp_decay_similarity(sale_age_days, rules["recency_decay_days"])

    def _rooms_similarity(
        self,
        subject: SubjectProperty,
        comp: Dict[str, Any],
    ) -> float:
        rooms_delta = self._rooms_delta(subject, comp)
        if rooms_delta is None:
            return 0.0
        absolute_delta = abs(rooms_delta)
        if absolute_delta == 0:
            return 1.0
        if absolute_delta == 1:
            return 0.25
        if absolute_delta == 2:
            return 0.0 if subject.property_type == "appartement" else 0.05
        return 0.0

    def _room_density_similarity(
        self,
        subject: SubjectProperty,
        comp: Dict[str, Any],
        rules: Dict[str, Any],
    ) -> float:
        comparable_rooms = comp.get("rooms")
        comparable_area = comp.get("living_area_sqm")
        if (
            subject.rooms <= 0
            or comparable_rooms is None
            or int(comparable_rooms) <= 0
            or comparable_area is None
            or comparable_area <= 0
        ):
            return 0.0

        subject_density = subject.living_area_sqm / float(subject.rooms)
        comparable_density = float(comparable_area) / float(comparable_rooms)
        gap = percent_gap(subject_density, comparable_density)
        if gap is None:
            return 0.0
        return self._exp_decay_similarity(gap, rules["room_density_gap_decay"])

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
    def _exp_decay_similarity(value: float, decay: float) -> float:
        if decay <= 0:
            return 0.0
        return math.exp(-float(value) / float(decay))


class ComparableOutlierFilter:
    def apply(
        self,
        subject: SubjectProperty,
        comparables: List[Dict[str, Any]],
    ) -> None:
        candidates = [
            comp
            for comp in comparables
            if comp.get("included", True) and comp.get("price_per_sqm_eur") is not None and comp.get("price_per_sqm_eur") > 0
        ]
        if len(candidates) < OUTLIER_MIN_SAMPLE_SIZE:
            return

        log_prices = [math.log(float(comp["price_per_sqm_eur"])) for comp in candidates]
        center = self._median(log_prices)
        deviations = [abs(value - center) for value in log_prices]
        mad = self._median(deviations)
        if mad is None or mad < OUTLIER_MIN_MAD:
            return

        robust_scale = 1.4826 * mad
        low_threshold = OUTLIER_LOW_Z[subject.property_type]
        high_threshold = OUTLIER_HIGH_Z[subject.property_type]

        for comp, log_price in zip(candidates, log_prices):
            z_score = (log_price - center) / robust_scale
            comp["outlier_mad_zscore"] = round(z_score, 3)
            if z_score < low_threshold:
                comp["included"] = False
                comp["exclusion_reason"] = "price_outlier_low"
            elif z_score > high_threshold:
                comp["included"] = False
                comp["exclusion_reason"] = "price_outlier_high"

    @staticmethod
    def _median(values: List[float]) -> Optional[float]:
        ordered = sorted(float(value) for value in values)
        if not ordered:
            return None
        mid = len(ordered) // 2
        if len(ordered) % 2 == 1:
            return ordered[mid]
        return (ordered[mid - 1] + ordered[mid]) / 2
