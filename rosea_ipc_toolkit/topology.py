"""TopoJSON IO helpers shared across the IPC toolkit and CLI."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import topojson as tp

from .config import REPO_ROOT

Feature = Dict[str, Any]


def _has_valid_geometry(geometry: Optional[Dict[str, Any]]) -> bool:
    """Check if a geometry has valid coordinates."""
    if geometry is None:
        return False
    
    geom_type = geometry.get("type")
    
    if geom_type == "GeometryCollection":
        geometries = geometry.get("geometries")
        if not geometries:
            return False
        # At least one nested geometry must be valid
        return any(_has_valid_geometry(g) for g in geometries)
    
    coords = geometry.get("coordinates")
    if coords is None:
        return False
    
    # Check for empty coordinate arrays
    if geom_type in ("Point",):
        return isinstance(coords, list) and len(coords) >= 2
    elif geom_type in ("MultiPoint", "LineString"):
        return isinstance(coords, list) and len(coords) > 0
    elif geom_type in ("Polygon", "MultiLineString"):
        return isinstance(coords, list) and len(coords) > 0 and any(len(ring) > 0 for ring in coords)
    elif geom_type == "MultiPolygon":
        return isinstance(coords, list) and len(coords) > 0
    
    return True


def sanitize_features(features: List[Feature]) -> List[Feature]:
    """Remove features with null or empty geometries.
    
    Returns a new list containing only features with valid geometries.
    """
    valid_features = []
    removed_count = 0
    
    for feature in features:
        geometry = feature.get("geometry")
        if _has_valid_geometry(geometry):
            valid_features.append(feature)
        else:
            removed_count += 1
            props = feature.get("properties", {})
            print(f"  Warning: Removing feature with invalid geometry: "
                  f"{props.get('title', 'Unknown')} ({props.get('country', '?')}, "
                  f"{props.get('from', '?')} - {props.get('to', '?')})")
    
    if removed_count > 0:
        print(f"  Removed {removed_count} feature(s) with invalid geometries")
    
    return valid_features


def _flatten_geometry_collection(geometry: Dict[str, Any]) -> Dict[str, Any]:
    """Convert GeometryCollection of Polygons to MultiPolygon.
    
    The Python topojson library has a bug with GeometryCollections when there are
    shared arcs - the arc chains get broken. Converting to MultiPolygon avoids this.
    
    If the GeometryCollection contains only Polygons/MultiPolygons, it becomes a MultiPolygon.
    Otherwise, returns the geometry unchanged.
    """
    if geometry.get("type") != "GeometryCollection":
        return geometry
    
    nested = geometry.get("geometries", [])
    if not nested:
        return geometry
    
    # Collect all polygon coordinates
    all_coords = []
    for g in nested:
        gtype = g.get("type")
        if gtype == "Polygon":
            coords = g.get("coordinates")
            if coords:
                all_coords.append(coords)
        elif gtype == "MultiPolygon":
            coords = g.get("coordinates")
            if coords:
                all_coords.extend(coords)
        else:
            # Contains non-polygon geometry, can't flatten
            return geometry
    
    if not all_coords:
        return geometry
    
    return {
        "type": "MultiPolygon",
        "coordinates": all_coords
    }


def _preprocess_features_for_topojson(features: List[Feature]) -> List[Feature]:
    """Preprocess features to work around topojson library bugs.
    
    - Converts GeometryCollections containing only Polygons to MultiPolygons
      to avoid broken arc chains in the output.
    """
    result = []
    converted_count = 0
    
    for feature in features:
        geometry = feature.get("geometry")
        if geometry and geometry.get("type") == "GeometryCollection":
            new_geometry = _flatten_geometry_collection(geometry)
            if new_geometry.get("type") != "GeometryCollection":
                converted_count += 1
                feature = {**feature, "geometry": new_geometry}
        result.append(feature)
    
    if converted_count > 0:
        print(f"  Converted {converted_count} GeometryCollection(s) to MultiPolygon")
    
    return result


def _explode_multipart_geometries(features: List[Feature]) -> Tuple[List[Feature], Dict[int, List[int]]]:
    """Explode multi-part geometries into simple polygons.
    
    Returns:
        - List of exploded features (all simple Polygons)
        - Mapping from original feature index to list of exploded feature indices
    """
    exploded: List[Feature] = []
    mapping: Dict[int, List[int]] = {}  # original_idx -> [exploded_idx, ...]
    
    for orig_idx, feature in enumerate(features):
        geometry = feature.get("geometry")
        if not geometry:
            # Keep as-is
            mapping[orig_idx] = [len(exploded)]
            exploded.append(feature)
            continue
        
        geom_type = geometry.get("type")
        
        if geom_type == "GeometryCollection":
            # Explode GeometryCollection
            nested = geometry.get("geometries", [])
            indices = []
            for nested_geom in nested:
                if nested_geom.get("type") in ("Polygon", "MultiPolygon"):
                    if nested_geom.get("type") == "MultiPolygon":
                        # Further explode MultiPolygon
                        for poly_coords in nested_geom.get("coordinates", []):
                            indices.append(len(exploded))
                            exploded.append({
                                "type": "Feature",
                                "properties": feature.get("properties", {}),
                                "geometry": {"type": "Polygon", "coordinates": poly_coords},
                                "_original_idx": orig_idx,
                                "_part_type": "gc_multipolygon"
                            })
                    else:
                        indices.append(len(exploded))
                        exploded.append({
                            "type": "Feature",
                            "properties": feature.get("properties", {}),
                            "geometry": nested_geom,
                            "_original_idx": orig_idx,
                            "_part_type": "gc_polygon"
                        })
                else:
                    # Non-polygon in GeometryCollection (Point, etc.) - keep separately
                    indices.append(len(exploded))
                    exploded.append({
                        "type": "Feature",
                        "properties": feature.get("properties", {}),
                        "geometry": nested_geom,
                        "_original_idx": orig_idx,
                        "_part_type": "gc_other"
                    })
            mapping[orig_idx] = indices if indices else [len(exploded)]
            if not indices:
                exploded.append(feature)  # Empty GC, keep as-is
                
        elif geom_type == "MultiPolygon":
            # Explode MultiPolygon into separate Polygons
            coords = geometry.get("coordinates", [])
            indices = []
            for poly_coords in coords:
                indices.append(len(exploded))
                exploded.append({
                    "type": "Feature",
                    "properties": feature.get("properties", {}),
                    "geometry": {"type": "Polygon", "coordinates": poly_coords},
                    "_original_idx": orig_idx,
                    "_part_type": "multipolygon"
                })
            mapping[orig_idx] = indices if indices else [len(exploded)]
            if not indices:
                exploded.append(feature)  # Empty MultiPolygon, keep as-is
        else:
            # Simple geometry, keep as-is
            mapping[orig_idx] = [len(exploded)]
            exploded.append(feature)
    
    return exploded, mapping


def _rebuild_multipart_from_topology(
    topo_dict: Dict[str, Any],
    original_features: List[Feature],
    explode_mapping: Dict[int, List[int]]
) -> Dict[str, Any]:
    """Rebuild multi-part geometries from exploded topology.
    
    Takes the topology with exploded features and reconstructs the original
    multi-part structure (GeometryCollection, MultiPolygon) using the correct arcs.
    """
    objects = topo_dict.get("objects", {})
    if not objects:
        return topo_dict
    
    # Get the geometries from the topology
    obj_name = list(objects.keys())[0]
    topo_geoms = objects[obj_name].get("geometries", [])
    
    # Build reconstructed geometries
    reconstructed: List[Dict[str, Any]] = []
    
    for orig_idx, feature in enumerate(original_features):
        geometry = feature.get("geometry")
        if not geometry:
            # Find the corresponding topo geometry
            if orig_idx in explode_mapping and explode_mapping[orig_idx]:
                exp_idx = explode_mapping[orig_idx][0]
                if exp_idx < len(topo_geoms):
                    reconstructed.append(topo_geoms[exp_idx])
            continue
        
        geom_type = geometry.get("type")
        exp_indices = explode_mapping.get(orig_idx, [])
        
        if geom_type == "GeometryCollection":
            # Reconstruct GeometryCollection from exploded parts
            nested_geoms = []
            for exp_idx in exp_indices:
                if exp_idx < len(topo_geoms):
                    exp_geom = topo_geoms[exp_idx]
                    # Strip wrapper properties, keep just the geometry part
                    nested_geoms.append({
                        "type": exp_geom.get("type"),
                        "arcs": exp_geom.get("arcs"),
                    })
            
            reconstructed.append({
                "type": "GeometryCollection",
                "geometries": nested_geoms,
                "properties": feature.get("properties", {}),
                "id": topo_geoms[exp_indices[0]].get("id") if exp_indices else None,
            })
            
        elif geom_type == "MultiPolygon":
            # Reconstruct MultiPolygon from exploded Polygons
            multi_arcs = []
            for exp_idx in exp_indices:
                if exp_idx < len(topo_geoms):
                    exp_geom = topo_geoms[exp_idx]
                    if exp_geom.get("type") == "Polygon":
                        multi_arcs.append(exp_geom.get("arcs", []))
            
            reconstructed.append({
                "type": "MultiPolygon",
                "arcs": multi_arcs,
                "properties": feature.get("properties", {}),
                "id": topo_geoms[exp_indices[0]].get("id") if exp_indices else None,
            })
        else:
            # Simple geometry, use as-is
            if exp_indices and exp_indices[0] < len(topo_geoms):
                reconstructed.append(topo_geoms[exp_indices[0]])
    
    # Update the topology with reconstructed geometries
    result = dict(topo_dict)
    result["objects"] = {
        obj_name: {
            "type": "GeometryCollection",
            "geometries": reconstructed
        }
    }
    
    return result


def convert_geojson_to_topology(geojson: Dict[str, Any]) -> Dict[str, Any]:
    """Convert GeoJSON to TopoJSON with proper topology preservation.
    
    Uses a custom TopologyBuilder that correctly handles arc sharing,
    avoiding bugs in the Python topojson library that cause broken
    arc chains for GeometryCollections and shared edges.
    """
    from .topojson_builder import TopologyBuilder
    
    if "features" not in geojson:
        # Use standard library for non-FeatureCollection inputs
        topology = tp.Topology(geojson, prequantize=False)
        result = topology.to_dict()
        result.setdefault("arcs", [])
        return result
    
    # Sanitize features
    features = sanitize_features(geojson["features"])
    
    # Use custom builder for proper arc sharing
    builder = TopologyBuilder()
    result = builder.build({"type": "FeatureCollection", "features": features})
    
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
