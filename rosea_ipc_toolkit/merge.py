"""Feature aggregation helpers for IPC datasets."""

from __future__ import annotations

import copy
from typing import Any, Dict, Iterable, List, Optional

from .dates import DATE_FROM_KEYS, DATE_TO_KEYS, first_present
from .feature_utils import feature_key

Feature = Dict[str, Any]
Aggregated = Dict[str, Dict[str, Any]]


def merge_features(
    aggregate: Aggregated,
    features: Iterable[Feature],
    *,
    priority: int,
    source_year: Optional[int],
    source_label: str,
) -> Dict[str, int]:
    """Merge feature collections favouring newer or higher-priority entries.
    
    When priorities and years are equal, prefer features with more recent analysis dates.
    This matches the PySpark deduplication logic for parity.
    """

    stats = {"added": 0, "updated": 0, "skipped": 0}

    for feature in features:
        if not isinstance(feature, dict):
            continue

        feature_copy = copy.deepcopy(feature)
        props = feature_copy.get("properties") or {}
        key = feature_key(feature_copy)
        candidate = {
            "feature": feature_copy,
            "priority": priority,
            "source_year": props.get("year") if props.get("year") is not None else source_year,
            "source_label": source_label,
            "title": props.get("title"),
            # Extract analysis date info for tie-breaking using robust key lookup
            "to_date": first_present(props, DATE_TO_KEYS),
            "from_date": first_present(props, DATE_FROM_KEYS),
        }

        existing = aggregate.get(key)
        if existing is None:
            aggregate[key] = candidate
            stats["added"] += 1
            continue

        replace = False
        if priority > existing.get("priority", -1):
            replace = True
        elif priority == existing.get("priority", -1):
            candidate_year = candidate.get("source_year") or 0
            existing_year = existing.get("source_year") or 0
            if candidate_year > existing_year:
                replace = True
            elif candidate_year == existing_year:
                # When same year and priority, compare analysis dates
                replace = _should_replace_by_dates(candidate, existing)

        if replace:
            aggregate[key] = candidate
            stats["updated"] += 1
        else:
            stats["skipped"] += 1

    return stats


def _should_replace_by_dates(candidate: Dict[str, Any], existing: Dict[str, Any]) -> bool:
    """Compare analysis dates to determine if candidate should replace existing feature.
    
    Prefers records with:
    1. Later 'from' date (analysis start date)
    2. Later 'to' date when 'from' dates are equal (with year boundary check)
    
    Year boundary check: if to_date year exceeds the record's year, it's ignored
    and we fall back to from_date comparison only.
    
    This matches the PySpark deduplication logic for parity.
    """
    from .dates import parse_iso_datetime
    
    # Compare 'from' dates first (analysis start date)
    candidate_from = parse_iso_datetime(candidate.get("from_date"))
    existing_from = parse_iso_datetime(existing.get("from_date"))
    
    if candidate_from and existing_from:
        if candidate_from > existing_from:
            return True
        elif candidate_from < existing_from:
            return False
    elif candidate_from and not existing_from:
        return True
    elif existing_from and not candidate_from:
        return False
    
    # If 'from' dates are equal or both missing, compare 'to' dates with year boundary check
    candidate_year = candidate.get("source_year")
    existing_year = existing.get("source_year")
    
    candidate_to = parse_iso_datetime(candidate.get("to_date"))
    existing_to = parse_iso_datetime(existing.get("to_date"))
    
    # Apply year boundary check: ignore to_date if it exceeds the record's year
    if candidate_to and candidate_year and candidate_to.year > candidate_year:
        candidate_to = None
    if existing_to and existing_year and existing_to.year > existing_year:
        existing_to = None
    
    # Fall back to from_date if to_date was nullified
    candidate_effective = candidate_to or candidate_from
    existing_effective = existing_to or existing_from
    
    if candidate_effective and existing_effective:
        if candidate_effective > existing_effective:
            return True
        elif candidate_effective < existing_effective:
            return False
    elif candidate_effective and not existing_effective:
        return True
    elif existing_effective and not candidate_effective:
        return False
    
    # Default: don't replace if we can't determine recency
    return False


def extract_years(aggregate: Aggregated) -> List[int]:
    years = [
        entry.get("source_year")
        for entry in aggregate.values()
        if isinstance(entry.get("source_year"), int)
    ]
    return sorted(years) if years else []


def flatten_features(aggregate: Aggregated) -> List[Feature]:
    sorted_entries = sorted(aggregate.items(), key=lambda item: item[0])
    return [entry["feature"] for _, entry in sorted_entries]
