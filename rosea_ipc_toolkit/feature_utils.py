"""Feature-level helpers shared across the IPC toolkit and CLI."""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any, Dict, List, Optional

Feature = Dict[str, Any]

Geometry = Dict[str, Any]


def normalize_title(title: str | None) -> str:
    if not title:
        return ""
    return " ".join(title.split()).strip().lower()


def feature_key(feature: Feature) -> str:
    """Generate a unique key for a feature, including year for per-year deduplication.
    
    Keys are structured as:
    - id::{country}::{year}::{area_id} (preferred)
    - title::{country}::{year}::{title} (fallback)
    - geometry::{hash} (last resort)
    
    Including year ensures one feature per area per year is retained,
    matching PySpark's partitionBy("country", "year") behaviour.
    """
    props = feature.get("properties") or {}
    area_id = props.get("id")
    iso_value = (props.get("iso3") or props.get("country") or "").strip().lower()
    year_value = props.get("year")
    year_str = str(year_value) if year_value is not None else ""

    if area_id is not None:
        return f"id::{iso_value}::{year_str}::{str(area_id).strip()}"

    title_key = normalize_title(props.get("title"))
    if title_key:
        if iso_value:
            return f"title::{iso_value}::{year_str}::{title_key}"
        return f"title::{year_str}::{title_key}" if year_str else f"title::{title_key}"

    geometry = feature.get("geometry")
    if geometry:
        geometry_str = json.dumps(geometry, sort_keys=True)
        digest = hashlib.sha1(geometry_str.encode("utf-8")).hexdigest()
        return f"geometry::{digest}"

    fallback_str = json.dumps(feature, sort_keys=True)
    digest = hashlib.sha1(fallback_str.encode("utf-8")).hexdigest()
    return f"feature::{digest}"


def sanitise_geometry(geometry: Any) -> Optional[Geometry]:
    """Return a deep-copied GeoJSON geometry or ``None`` if invalid."""

    if not isinstance(geometry, dict):
        return None

    geom_type = geometry.get("type")

    if geom_type == "GeometryCollection":
        geometries_field = geometry.get("geometries")
        if not isinstance(geometries_field, list):
            return None
        members: List[Geometry] = []
        for child in geometries_field:
            cleaned = sanitise_geometry(child)
            if cleaned is not None:
                members.append(cleaned)

        if not members:
            return None

        result: Geometry = {"type": "GeometryCollection", "geometries": members}
        if "bbox" in geometry and isinstance(geometry["bbox"], list):
            result["bbox"] = copy.deepcopy(geometry["bbox"])
        return result

    if geom_type in {"Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon"}:
        coordinates = geometry.get("coordinates")
        if coordinates is None:
            return None
        result = {"type": geom_type, "coordinates": copy.deepcopy(coordinates)}
        if "bbox" in geometry and isinstance(geometry["bbox"], list):
            result["bbox"] = copy.deepcopy(geometry["bbox"])
        return result

    if geom_type in {"CircularString", "CompoundCurve", "CurvePolygon"}:
        # Non-standard GeoJSON types sometimes returned by upstream sources.
        coordinates = geometry.get("coordinates")
        if coordinates is None:
            return None
        result = {"type": geom_type, "coordinates": copy.deepcopy(coordinates)}
        if "bbox" in geometry and isinstance(geometry["bbox"], list):
            result["bbox"] = copy.deepcopy(geometry["bbox"])
        return result

    try:
        return json.loads(json.dumps(geometry))
    except (TypeError, ValueError):
        return None


def extract_polygonal_geometry(geometry: Geometry | None) -> Optional[Geometry]:
    """Return only the polygonal components of a geometry.

    Geometry collections can include points or lines that the topojson library
    cannot triangulate into ``arcs``. We retain Polygon and MultiPolygon
    members, discarding everything else. If no polygonal content remains the
    caller should skip the feature.
    """

    if not isinstance(geometry, dict):
        return None

    geom_type = geometry.get("type")
    if geom_type in {"Polygon", "MultiPolygon"}:
        return copy.deepcopy(geometry)

    if geom_type == "GeometryCollection":
        members = geometry.get("geometries")
        if not isinstance(members, list):
            return None

        filtered: List[Geometry] = []
        for member in members:
            cleaned = extract_polygonal_geometry(member)
            if cleaned is not None:
                filtered.append(cleaned)

        if not filtered:
            return None

        if len(filtered) == 1:
            return filtered[0]

        result: Geometry = {"type": "GeometryCollection", "geometries": filtered}
        if "bbox" in geometry and isinstance(geometry["bbox"], list):
            result["bbox"] = copy.deepcopy(geometry["bbox"])
        return result

    # Non-surface geometry types are ignored for topology conversion.
    return None
