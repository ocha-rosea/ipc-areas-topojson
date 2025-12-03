#!/usr/bin/env python3
"""Simplify TopoJSON datasets with optional precision reduction.

Reads a TopoJSON file, rounds geometry coordinates to a configurable precision,
optionally simplifies geometries, converts the result back to TopoJSON, and writes
an updated dataset alongside a size report. Helper functions can be imported by
other scripts (e.g., the combiner) to reuse the logic programmatically.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from rosea_ipc_toolkit.topology import load_topojson_features

try:
    import topojson as tp
except ImportError as exc:  # pragma: no cover - the script exits immediately
    raise SystemExit(
        "Missing dependency: install project requirements with 'pip install -r requirements.txt'."
    ) from exc

try:
    from shapely.geometry import shape
    from shapely.geometry.base import BaseGeometry
except ImportError:  # pragma: no cover - simplification is optional
    shape = None  # type: ignore[assignment]
    BaseGeometry = object  # type: ignore[assignment]

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
DEFAULT_SOURCE_NAME = "combined_areas.topojson"


def ensure_source(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Could not find {path}")


def load_combined_features(source: Path) -> List[Dict[str, Any]]:
    return load_topojson_features(source)


def round_nested(value: Any, digits: int) -> Any:
    if isinstance(value, list):
        return [round_nested(item, digits) for item in value]
    if isinstance(value, float):
        return round(value, digits)
    return value


FailureRecord = Dict[str, Any]


# Geometry types that cannot or should not be simplified
NON_SIMPLIFIABLE_TYPES = frozenset({
    "Point",
    "MultiPoint",
    "LineString",
    "MultiLineString",
})

# Geometry types that support simplification
SIMPLIFIABLE_TYPES = frozenset({
    "Polygon",
    "MultiPolygon",
})


def simplify_geometry(geometry: Dict[str, Any], tolerance: float) -> Tuple[Dict[str, Any], Optional[FailureRecord]]:
    """Simplify a geometry, preserving structure for GeometryCollections.

    Only Polygon and MultiPolygon types are simplified. Points, LineStrings,
    and their Multi variants pass through unchanged and are documented as skipped.
    GeometryCollections are handled recursively, preserving all members.
    """
    if tolerance <= 0:
        return geometry, None

    geom_type = geometry.get("type")

    # Non-simplifiable types pass through unchanged but are documented
    if geom_type in NON_SIMPLIFIABLE_TYPES:
        return geometry, {
            "reason": "skipped",
            "detail": f"{geom_type} geometry cannot be simplified",
            "geometry_type": geom_type,
        }

    # Handle GeometryCollections recursively
    if geom_type == "GeometryCollection":
        members = geometry.get("geometries")
        if not isinstance(members, list):
            return geometry, {
                "reason": "invalid_geometry",
                "detail": "GeometryCollection has no geometries",
                "geometry_type": geom_type,
            }

        simplified_members: List[Dict[str, Any]] = []
        collected_failures: List[Dict[str, Any]] = []
        for member in members:
            if not isinstance(member, dict):
                continue
            simplified_member, failure = simplify_geometry(member, tolerance)
            simplified_members.append(simplified_member)
            if failure:
                collected_failures.append(failure)

        result: Dict[str, Any] = {"type": "GeometryCollection", "geometries": simplified_members}
        if "bbox" in geometry and isinstance(geometry["bbox"], list):
            result["bbox"] = geometry["bbox"]

        if collected_failures:
            # Summarise member failures
            skipped_types = [f.get("geometry_type", "unknown") for f in collected_failures if f.get("reason") == "skipped"]
            other_failures = [f.get("detail", "unknown") for f in collected_failures if f.get("reason") != "skipped"]
            detail_parts = []
            if skipped_types:
                detail_parts.append(f"skipped: {', '.join(skipped_types)}")
            if other_failures:
                detail_parts.append("; ".join(other_failures))
            return result, {
                "reason": "partial_simplification",
                "detail": "; ".join(detail_parts) if detail_parts else "some members not simplified",
                "geometry_type": geom_type,
                "member_failures": collected_failures,
            }
        return result, None

    # Unknown geometry type - pass through and document
    if geom_type not in SIMPLIFIABLE_TYPES:
        return geometry, {
            "reason": "unknown_type",
            "detail": f"Unrecognised geometry type: {geom_type}",
            "geometry_type": geom_type,
        }

    # Standard simplification for polygon types only
    if shape is None:
        print(
            "Warning: shapely is not installed, skipping simplification step.",
            file=sys.stderr,
        )
        return geometry, {
            "reason": "dependency_missing",
            "detail": "shapely is not installed",
            "geometry_type": geom_type,
        }

    try:
        geom_obj: BaseGeometry = shape(geometry)  # type: ignore[arg-type]
    except Exception as exc:  # noqa: BLE001
        return geometry, {
            "reason": "invalid_geometry",
            "detail": str(exc),
            "geometry_type": geom_type,
        }

    try:
        simplified = geom_obj.simplify(tolerance, preserve_topology=True)
    except Exception as exc:  # noqa: BLE001
        return geometry, {
            "reason": "simplification_error",
            "detail": str(exc),
            "geometry_type": geom_type,
        }

    if simplified.is_empty:
        return geometry, {
            "reason": "empty_geometry",
            "detail": "Simplification produced an empty geometry",
            "geometry_type": geom_type,
        }

    try:
        if simplified.equals(geom_obj):
            return geometry, {
                "reason": "no_change",
                "detail": "Simplified geometry matches original",
                "geometry_type": geom_type,
            }
    except Exception as exc:  # noqa: BLE001
        return geometry, {
            "reason": "comparison_failed",
            "detail": str(exc),
            "geometry_type": geom_type,
        }

    return json.loads(json.dumps(simplified.__geo_interface__)), None


def simplify_feature(
    feature: Dict[str, Any],
    digits: int,
    tolerance: float,
) -> Tuple[Dict[str, Any], Optional[FailureRecord]]:
    feature_copy = json.loads(json.dumps(feature))  # deep copy to avoid mutating original
    geometry = feature_copy.get("geometry")
    failure: Optional[FailureRecord] = None
    if isinstance(geometry, dict):
        if tolerance > 0:
            geometry, failure = simplify_geometry(geometry, tolerance)
            feature_copy["geometry"] = geometry
        if "coordinates" in geometry:
            geometry["coordinates"] = round_nested(geometry["coordinates"], digits)
    return feature_copy, failure


def build_topology(features: List[Dict[str, Any]]) -> Dict[str, Any]:
    feature_collection = {
        "type": "FeatureCollection",
        "features": features,
    }
    topology = tp.Topology(feature_collection, prequantize=False)
    result = topology.to_dict()
    result.setdefault("arcs", [])
    return result


def write_output(target: Path, topology: Dict[str, Any]) -> None:
    target.parent.mkdir(exist_ok=True)
    with open(target, "w", encoding="utf-8") as handle:
        json.dump(topology, handle, separators=(",", ":"))


def _build_failure_entry(
    feature: Dict[str, Any],
    failure: FailureRecord,
    source: Path,
) -> FailureRecord:
    properties = feature.get("properties") or {}
    try:
        source_display = source.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        source_display = source.as_posix()
    entry: FailureRecord = {
        "title": properties.get("title") or properties.get("name") or feature.get("id"),
        "id": properties.get("id") or feature.get("id"),
        "country": properties.get("country"),
        "year": properties.get("year"),
        "reason": failure.get("reason"),
        "detail": failure.get("detail"),
        "geometry_type": failure.get("geometry_type"),
        "source_dataset": source_display,
    }

    for key in ("analysis_label", "analysis_id", "from", "to"):
        if key in properties:
            entry[key] = properties.get(key)

    return entry


def simplify_features(
    features: List[Dict[str, Any]],
    *,
    precision: int,
    simplify_tolerance: float,
    source: Path,
) -> Tuple[List[Dict[str, Any]], List[FailureRecord]]:
    simplified: List[Dict[str, Any]] = []
    failures: List[FailureRecord] = []

    for feature in features:
        simplified_feature, failure = simplify_feature(feature, precision, simplify_tolerance)
        simplified.append(simplified_feature)
        if failure:
            failures.append(_build_failure_entry(feature, failure, source))

    return simplified, failures


def _write_unsimplified_report(target: Path, failures: List[FailureRecord], quiet: bool) -> None:
    report_path = target.with_name(target.stem + "_unsimplified.json")

    if not failures:
        if report_path.exists():
            report_path.unlink()
            if not quiet:
                print(f"Removed {report_path.name}; all features simplified successfully.")
        return

    try:
        target_display = target.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        target_display = target.as_posix()

    payload = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source_file": target_display,
        "total_unsimplified": len(failures),
        "items": failures,
    }

    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)

    if not quiet:
        print(
            f"{len(failures)} feature(s) retained without simplification; "
            f"details written to {report_path.name}"
        )


def simplify_topojson(
    source: Path,
    *,
    output: Path | None = None,
    precision: int = 4,
    simplify_tolerance: float = 0.0,
    quiet: bool = False,
) -> Dict[str, int | float]:
    ensure_source(source)

    features = load_combined_features(source)
    if not features:
        raise ValueError("No features available to simplify")

    processed, failures = simplify_features(
        features,
        precision=precision,
        simplify_tolerance=simplify_tolerance,
        source=source,
    )
    topology = build_topology(processed)

    target = output or source
    write_output(target, topology)
    _write_unsimplified_report(target, failures, quiet)

    original_size = source.stat().st_size
    new_size = target.stat().st_size
    saved = original_size - new_size
    ratio = (new_size / original_size) if original_size else 0.0

    stats = {
        "original_size": original_size,
        "new_size": new_size,
        "saved_bytes": saved,
        "size_ratio": ratio,
        "precision": precision,
        "simplify_tolerance": simplify_tolerance,
        "output_path": str(target),
        "unsimplified_features": len(failures),
    }

    if not quiet:
        print(
            f"Simplified dataset written to {target} with precision {precision} decimal places"
        )
        if simplify_tolerance > 0:
            print(f"Simplification tolerance applied: {simplify_tolerance}")
        print(
            f"Size reduced from {original_size:,} bytes to {new_size:,} bytes "
            f"({ratio:.2%} of original, saved {saved:,} bytes)"
        )

    return stats


def minify_topojson(
    source: Path,
    *,
    output: Path | None = None,
    precision: int = 4,
    simplify_tolerance: float = 0.0,
    quiet: bool = False,
) -> Dict[str, int | float]:
    """Backward compatible alias for the previous function name."""

    return simplify_topojson(
        source,
        output=output,
        precision=precision,
        simplify_tolerance=simplify_tolerance,
        quiet=quiet,
    )


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--precision",
        type=int,
        default=3,
        help="Number of decimal places to retain in coordinates (default: 3)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output path for the simplified TopoJSON file (defaults to overwriting the input)",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DATA_DIR / DEFAULT_SOURCE_NAME,
        help="Path to the source combined TopoJSON file",
    )
    parser.add_argument(
        "--simplify-tolerance",
        type=float,
        default=0.001,
        help="Simplification tolerance in coordinate units; set to 0 to disable",
    )
    args = parser.parse_args(argv)

    try:
        simplify_topojson(
            args.input,
            output=args.output,
            precision=args.precision,
            simplify_tolerance=args.simplify_tolerance,
            quiet=False,
        )
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
