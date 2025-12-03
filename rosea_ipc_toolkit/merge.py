"""Feature aggregation helpers for IPC datasets."""

from __future__ import annotations

import copy
from typing import Any, Dict, Iterable, List, Optional

from .dates import DATE_FROM_KEYS, DATE_TO_KEYS, DATE_UPDATED_KEYS, first_present
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
            "updated_at": first_present(props, DATE_UPDATED_KEYS),
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
    """Compare analysis dates to determine if candidate should replace existing feature."""
    from .dates import parse_iso_datetime
    
    # Compare 'to' dates first (analysis end date)
    candidate_to = parse_iso_datetime(candidate.get("to_date"))
    existing_to = parse_iso_datetime(existing.get("to_date"))
    
    if candidate_to and existing_to:
        if candidate_to > existing_to:
            return True
        elif candidate_to < existing_to:
            return False
    elif candidate_to and not existing_to:
        return True
    elif existing_to and not candidate_to:
        return False
    
    # If 'to' dates are equal or both missing, compare 'from' dates
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
    
    # If period dates are equal/missing, compare updated_at timestamps
    candidate_updated = parse_iso_datetime(candidate.get("updated_at"))
    existing_updated = parse_iso_datetime(existing.get("updated_at"))
    
    if candidate_updated and existing_updated:
        return candidate_updated > existing_updated
    elif candidate_updated and not existing_updated:
        return True
    
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
