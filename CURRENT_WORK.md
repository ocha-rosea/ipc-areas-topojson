# Current Work – IPC Areas TopoJSON Toolkit

## Status

**Active** – Geometry structure preservation review

## Problem Statement

The current pipeline filters geometries to polygonal types only via `extract_polygonal_geometry()`. This discards Points, LineStrings, and other geometry types, altering the source data structure. Downstream consumers expect the original geometry hierarchy (including nested GeometryCollections) to be preserved—only simplified and with trimmed properties.

## Planned Fixes

### 1. Preserve Original Geometry Types (Priority: High)

**Current behaviour:** `extract_polygonal_geometry()` strips non-polygon geometries entirely.  
**Desired behaviour:** Retain all geometry types; apply simplification only to compatible types (Polygon, MultiPolygon, LineString, MultiLineString) while passing through Points unchanged.

**Files to modify:**

- `rosea_ipc_toolkit/feature_utils.py` – remove or bypass polygon-only filtering
- `rosea_ipc_toolkit/downloader.py` – adjust `_filter_and_process()` to keep geometry as-is after sanitisation

### 2. Handle GeometryCollections Transparently

**Current behaviour:** GeometryCollections are recursively filtered, potentially collapsing to a single child or `None`.  
**Desired behaviour:** Maintain GeometryCollection wrapper and all member geometries; simplify each member according to its type.

**Files to modify:**

- `cli/simplify_ipc_global_areas.py` – extend `simplify_geometry()` to recurse into GeometryCollections without discarding members
- `rosea_ipc_toolkit/feature_utils.py` – update `sanitise_geometry()` to preserve structure

### 3. TopoJSON Compatibility for Non-Polygon Types

**Current behaviour:** Point-only datasets caused `KeyError: 'arcs'`; workaround wraps/unwraps coordinates.  
**Desired behaviour:** Confirm the workaround is stable across mixed geometry collections; add integration tests.

**Files to modify:**

- `rosea_ipc_toolkit/topology.py` – verify `_wrap_topology_points` handles all edge cases
- Add test coverage in a new `tests/` directory (optional)

### 4. Property Trimming (Already Implemented)

- `color` and `year` removed from global dataset
- `from` and `to` stripped from `global_areas_min.topojson`
- No further changes required unless new properties surface

## Acceptance Criteria

- [x] Output TopoJSON retains all geometry types present in source GeoJSON
- [x] Nested GeometryCollections remain nested in output
- [x] Simplification applies only to line/polygon rings; points pass through
- [ ] File sizes remain comparable (property trimming offsets geometry retention)
- [ ] Existing downstream map renders correctly with updated datasets

## Notes

- IPC API sometimes returns mixed GeometryCollections (polygons + points for area centroids)
- Simplification tolerance and precision flags should continue to work as documented
- Extra-minified global output (`global_areas_min.topojson`) still strips `from`/`to`

---

Last updated: 2025-12-02
