"""Custom TopoJSON builder that preserves topology correctly.

The Python topojson library has bugs with arc assignments when there are shared
edges. This module provides a custom builder that correctly handles arc sharing
and produces valid TopoJSON output.
"""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

# Type alias for coordinates
Coord = Tuple[float, float]
Feature = Dict[str, Any]


class TopologyBuilder:
    """Builds TopoJSON from GeoJSON with correct arc sharing.
    
    This builder uses a two-pass approach:
    1. First pass: Collect all coordinates and identify shared vertices
    2. Second pass: Build arcs, splitting at shared vertices
    """
    
    def __init__(self, precision: int = 6):
        """Initialize the builder.
        
        Args:
            precision: Decimal places for coordinate comparison (default 6)
        """
        self.precision = precision
        self.arcs: List[List[Coord]] = []
        self.arc_index: Dict[str, int] = {}  # hash -> arc index
        self.vertex_count: Dict[Coord, int] = defaultdict(int)  # How many times each vertex appears
        
    def _round_coord(self, coord: List[float]) -> Coord:
        """Round coordinate to precision for comparison."""
        return (
            round(coord[0], self.precision),
            round(coord[1], self.precision)
        )
    
    def _hash_arc(self, coords: List[Coord]) -> str:
        """Create hash for arc (order-independent for shared edges)."""
        forward = tuple(coords)
        reverse = tuple(reversed(coords))
        canonical = min(forward, reverse)
        return json.dumps(canonical)
    
    def _add_arc(self, coords: List[Coord]) -> Tuple[int, bool]:
        """Add arc to the collection, returning (index, is_reversed).
        
        If the arc already exists (possibly reversed), returns the existing index.
        """
        if len(coords) < 2:
            raise ValueError("Arc must have at least 2 coordinates")
        
        forward_hash = self._hash_arc(coords)
        if forward_hash in self.arc_index:
            existing_idx = self.arc_index[forward_hash]
            existing_arc = self.arcs[existing_idx]
            if existing_arc[0] == coords[0]:
                return (existing_idx, False)
            else:
                return (existing_idx, True)
        
        idx = len(self.arcs)
        self.arcs.append(list(coords))
        self.arc_index[forward_hash] = idx
        return (idx, False)
    
    def _collect_vertices(self, features: List[Dict[str, Any]]) -> None:
        """First pass: collect all vertices and count occurrences."""
        for feature in features:
            geometry = feature.get("geometry")
            if geometry:
                self._collect_geometry_vertices(geometry)
    
    def _collect_geometry_vertices(self, geometry: Dict[str, Any]) -> None:
        """Recursively collect vertices from a geometry."""
        geom_type = geometry.get("type")
        
        if geom_type == "Polygon":
            for ring in geometry.get("coordinates", []):
                for coord in ring:
                    v = self._round_coord(coord)
                    self.vertex_count[v] += 1
                    
        elif geom_type == "MultiPolygon":
            for poly in geometry.get("coordinates", []):
                for ring in poly:
                    for coord in ring:
                        v = self._round_coord(coord)
                        self.vertex_count[v] += 1
                        
        elif geom_type == "GeometryCollection":
            for g in geometry.get("geometries", []):
                self._collect_geometry_vertices(g)
                
        elif geom_type == "LineString":
            for coord in geometry.get("coordinates", []):
                v = self._round_coord(coord)
                self.vertex_count[v] += 1
                
        elif geom_type == "MultiLineString":
            for line in geometry.get("coordinates", []):
                for coord in line:
                    v = self._round_coord(coord)
                    self.vertex_count[v] += 1
    
    def _is_shared_vertex(self, v: Coord) -> bool:
        """Check if a vertex is shared (appears more than once)."""
        return self.vertex_count[v] > 1
    
    def _ring_to_arcs(self, ring: List[List[float]]) -> List[int]:
        """Convert a ring of coordinates to arc references.
        
        Splits the ring at shared vertices to enable arc sharing between polygons.
        """
        if len(ring) < 4:
            return []
        
        coords = [self._round_coord(c) for c in ring]
        
        # Find split points (shared vertices)
        split_indices = []
        for i, v in enumerate(coords[:-1]):  # Skip last (same as first for closed ring)
            if self._is_shared_vertex(v):
                split_indices.append(i)
        
        if len(split_indices) < 2:
            # No shared vertices or only one - treat whole ring as single arc
            idx, is_rev = self._add_arc(coords)
            return [~idx if is_rev else idx]
        
        # Split ring into arcs at shared vertices
        arc_refs = []
        n = len(split_indices)
        
        for i in range(n):
            start_idx = split_indices[i]
            end_idx = split_indices[(i + 1) % n]
            
            # Extract arc coordinates
            if end_idx > start_idx:
                arc_coords = coords[start_idx:end_idx + 1]
            else:
                # Wrap around
                arc_coords = coords[start_idx:] + coords[1:end_idx + 1]
            
            if len(arc_coords) >= 2:
                idx, is_rev = self._add_arc(arc_coords)
                arc_refs.append(~idx if is_rev else idx)
        
        return arc_refs
    
    def _convert_polygon(self, polygon_coords: List[List[List[float]]]) -> List[List[int]]:
        """Convert polygon coordinates to TopoJSON arcs."""
        result = []
        for ring in polygon_coords:
            arcs = self._ring_to_arcs(ring)
            if arcs:
                result.append(arcs)
        return result
    
    def _convert_geometry(self, geometry: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a GeoJSON geometry to TopoJSON geometry."""
        if geometry is None:
            return None
            
        geom_type = geometry.get("type")
        
        if geom_type == "Point":
            return {
                "type": "Point",
                "coordinates": geometry.get("coordinates")
            }
        
        elif geom_type == "MultiPoint":
            return {
                "type": "MultiPoint",
                "coordinates": geometry.get("coordinates")
            }
        
        elif geom_type == "LineString":
            coords = geometry.get("coordinates", [])
            if len(coords) >= 2:
                rounded = [self._round_coord(c) for c in coords]
                idx, is_rev = self._add_arc(rounded)
                return {
                    "type": "LineString",
                    "arcs": [~idx if is_rev else idx]
                }
            return {"type": "LineString", "arcs": []}
        
        elif geom_type == "MultiLineString":
            arcs = []
            for line in geometry.get("coordinates", []):
                if len(line) >= 2:
                    rounded = [self._round_coord(c) for c in line]
                    idx, is_rev = self._add_arc(rounded)
                    arcs.append([~idx if is_rev else idx])
            return {
                "type": "MultiLineString",
                "arcs": arcs
            }
        
        elif geom_type == "Polygon":
            return {
                "type": "Polygon",
                "arcs": self._convert_polygon(geometry.get("coordinates", []))
            }
        
        elif geom_type == "MultiPolygon":
            polys = []
            for poly_coords in geometry.get("coordinates", []):
                polys.append(self._convert_polygon(poly_coords))
            return {
                "type": "MultiPolygon",
                "arcs": polys
            }
        
        elif geom_type == "GeometryCollection":
            nested = []
            for g in geometry.get("geometries", []):
                converted = self._convert_geometry(g)
                if converted:
                    nested.append(converted)
            return {
                "type": "GeometryCollection",
                "geometries": nested
            }
        
        else:
            return geometry
    
    def _convert_feature(self, feature: Dict[str, Any], idx: int) -> Dict[str, Any]:
        """Convert a GeoJSON feature to TopoJSON geometry with properties."""
        geometry = feature.get("geometry")
        converted_geom = self._convert_geometry(geometry) if geometry else None
        
        if converted_geom is None:
            converted_geom = {"type": None}
        
        result = {
            **converted_geom,
            "properties": feature.get("properties", {}),
            "id": f"feature_{idx}"
        }
        
        if "id" in feature:
            result["id"] = feature["id"]
            
        return result
    
    def build(self, geojson: Dict[str, Any]) -> Dict[str, Any]:
        """Convert GeoJSON FeatureCollection to TopoJSON.
        
        Args:
            geojson: GeoJSON FeatureCollection
            
        Returns:
            TopoJSON topology
        """
        # Reset state
        self.arcs = []
        self.arc_index = {}
        self.vertex_count = defaultdict(int)
        
        features = geojson.get("features", [])
        
        # First pass: collect vertices
        self._collect_vertices(features)
        
        # Second pass: build topology
        geometries = []
        for idx, feature in enumerate(features):
            converted = self._convert_feature(feature, idx)
            geometries.append(converted)
        
        # Calculate bbox
        bbox = None
        if self.arcs:
            all_coords = [c for arc in self.arcs for c in arc]
            if all_coords:
                xs = [c[0] for c in all_coords]
                ys = [c[1] for c in all_coords]
                bbox = [min(xs), min(ys), max(xs), max(ys)]
        
        topology = {
            "type": "Topology",
            "objects": {
                "data": {
                    "type": "GeometryCollection",
                    "geometries": geometries
                }
            },
            "arcs": [[[c[0], c[1]] for c in arc] for arc in self.arcs]
        }
        
        if bbox:
            topology["bbox"] = bbox
            
        return topology


def convert_geojson_to_topology_custom(geojson: Dict[str, Any]) -> Dict[str, Any]:
    """Convert GeoJSON to TopoJSON using custom builder.
    
    This function provides a drop-in replacement for the buggy topojson library.
    """
    builder = TopologyBuilder()
    return builder.build(geojson)
