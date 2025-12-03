"""TopoJSON IO helpers shared across the IPC toolkit and CLI."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import topojson as tp

from .config import REPO_ROOT

Feature = Dict[str, Any]


def convert_geojson_to_topology(geojson: Dict[str, Any]) -> Dict[str, Any]:
    topology = tp.Topology(geojson, prequantize=False)
    result = topology.to_dict()
    # Some point-only datasets omit the ``arcs`` array, but downstream tooling
    # (and the topojson library itself when reloading the file) expects the key
    # to exist. Normalise by inserting an empty list so later processes can
    # safely apply rounding or other mutations without KeyError.
    result.setdefault("arcs", [])
    return result


def _wrap_point_coordinates(geometry: Dict[str, Any]) -> None:
    geom_type = geometry.get("type")
    if geom_type == "GeometryCollection":
        members = geometry.get("geometries")
        if isinstance(members, list):
            for member in members:
                if isinstance(member, dict):
                    _wrap_point_coordinates(member)
    elif geom_type == "Point":
        coords = geometry.get("coordinates")
        if isinstance(coords, list) and coords and isinstance(coords[0], (int, float)):
            geometry["coordinates"] = [coords]


def _wrap_topology_points(payload: Dict[str, Any]) -> Dict[str, Any]:
    wrapped = json.loads(json.dumps(payload))
    if "arcs" not in wrapped:
        wrapped["arcs"] = []
    objects = wrapped.get("objects")
    if not isinstance(objects, dict):
        return wrapped

    for obj in objects.values():
        if not isinstance(obj, dict):
            continue
        geometries = obj.get("geometries")
        if not isinstance(geometries, list):
            continue
        for geometry in geometries:
            if isinstance(geometry, dict):
                _wrap_point_coordinates(geometry)

    return wrapped


def _restore_point_coordinates(geometry: Dict[str, Any]) -> None:
    geom_type = geometry.get("type")
    if geom_type == "GeometryCollection":
        members = geometry.get("geometries")
        if isinstance(members, list):
            for member in members:
                if isinstance(member, dict):
                    _restore_point_coordinates(member)
    elif geom_type == "Point":
        coords = geometry.get("coordinates")
        if (
            isinstance(coords, list)
            and len(coords) == 1
            and isinstance(coords[0], list)
            and coords[0]
            and isinstance(coords[0][0], (int, float))
        ):
            geometry["coordinates"] = coords[0]


def _extract_point_features(payload: Dict[str, Any]) -> Tuple[List[Feature], Dict[str, Any]]:
    """Extract Point/MultiPoint features that have direct coordinates.
    
    The topojson library has issues with Points that have direct coordinates
    (not arc references). This function extracts them before library processing
    and returns them separately along with a modified payload.
    """
    extracted: List[Feature] = []
    modified = json.loads(json.dumps(payload))
    
    objects = modified.get("objects")
    if not isinstance(objects, dict):
        return extracted, modified
    
    for obj in objects.values():
        if not isinstance(obj, dict):
            continue
        geometries = obj.get("geometries")
        if not isinstance(geometries, list):
            continue
        
        remaining: List[Dict[str, Any]] = []
        for geometry in geometries:
            if not isinstance(geometry, dict):
                remaining.append(geometry)
                continue
            
            geom_type = geometry.get("type")
            coords = geometry.get("coordinates")
            
            # Check if this is a Point/MultiPoint with direct float coordinates
            if geom_type == "Point" and isinstance(coords, list) and coords:
                if isinstance(coords[0], (int, float)):
                    # Direct coordinates - extract as GeoJSON feature
                    feature: Feature = {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": coords},
                        "properties": geometry.get("properties", {}),
                    }
                    if "id" in geometry:
                        feature["id"] = geometry["id"]
                    extracted.append(feature)
                    continue
            elif geom_type == "MultiPoint" and isinstance(coords, list) and coords:
                if isinstance(coords[0], list) and coords[0] and isinstance(coords[0][0], (int, float)):
                    # Direct coordinates - extract as GeoJSON feature
                    feature = {
                        "type": "Feature",
                        "geometry": {"type": "MultiPoint", "coordinates": coords},
                        "properties": geometry.get("properties", {}),
                    }
                    if "id" in geometry:
                        feature["id"] = geometry["id"]
                    extracted.append(feature)
                    continue
            
            remaining.append(geometry)
        
        obj["geometries"] = remaining
    
    return extracted, modified


def load_topojson_features(path: Path) -> List[Feature]:
    with path.open("r", encoding="utf-8") as handle:
        topo_payload = json.load(handle)

    # Extract Point/MultiPoint features with direct coordinates first
    point_features, modified_payload = _extract_point_features(topo_payload)
    
    # Check if there are any remaining geometries to process
    has_remaining = False
    objects = modified_payload.get("objects")
    if isinstance(objects, dict):
        for obj in objects.values():
            if isinstance(obj, dict):
                geoms = obj.get("geometries")
                if isinstance(geoms, list) and geoms:
                    has_remaining = True
                    break
    
    arc_features: List[Feature] = []
    if has_remaining:
        if "arcs" not in modified_payload:
            modified_payload["arcs"] = []
        topology = tp.Topology(modified_payload, topology=True, prequantize=False)
        geojson_payload = json.loads(topology.to_geojson())
        
        features = geojson_payload.get("features") if isinstance(geojson_payload, dict) else None
        if isinstance(features, list):
            for feature in features:
                if isinstance(feature, dict):
                    arc_features.append(feature)
    
    # Combine point features with arc-based features
    return point_features + arc_features


def save_topology(topojson_data: Dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(exist_ok=True, parents=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(topojson_data, handle, separators=(",", ":"))

    return path


def display_relative(path: Path) -> str:
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def infer_feature_count(path: Path) -> Optional[int]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None

    objects = payload.get("objects") if isinstance(payload, dict) else None
    if not isinstance(objects, dict) or not objects:
        return None

    first_object = next(iter(objects.values()), None)
    geometries = first_object.get("geometries") if isinstance(first_object, dict) else None
    if isinstance(geometries, list):
        return len(geometries)

    return None
