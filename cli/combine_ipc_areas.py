#!/usr/bin/env python3
"""Combine IPC area TopoJSON datasets into a single, simplified global file.

By default this utility reads the per-country combined outputs produced by the
downloader (``*_combined_areas.topojson``), converts them to feature objects,
deduplicates by IPC id (falling back to geometry hash), stores an aggregated
TopoJSON file, and optionally simplifies the result using shared geometry helpers.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

# Add the parent directory to Python path so we can import the toolkit package
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from .simplify_ipc_combined_areas import simplify_topojson
except ImportError:  # pragma: no cover - fallback for direct script execution
    from simplify_ipc_combined_areas import simplify_topojson

from rosea_ipc_toolkit.feature_utils import feature_key
from rosea_ipc_toolkit.topology import (
    convert_geojson_to_topology,
    display_relative,
    load_topojson_features,
    save_topology,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
DEFAULT_OUTPUT_FILENAME = "combined_areas.topojson"
COMBINED_SUFFIX = "_combined_areas.topojson"


def collect_all_features(files: Iterable[Path]) -> List[Dict[str, Any]]:
    """Aggregate features from multiple TopoJSON files, deduplicated by key."""
    aggregate: Dict[str, Dict[str, Any]] = {}

    for filepath in files:
        try:
            features = load_topojson_features(filepath)
        except Exception as exc:  # noqa: BLE001 - surface path-specific failures
            print(f"Warning: failed to read {filepath}: {exc}", file=sys.stderr)
            continue

        for feature in features:
            key = feature_key(feature)
            if key not in aggregate:
                aggregate[key] = feature

    sorted_items = sorted(aggregate.items(), key=lambda item: item[0])
    return [item[1] for item in sorted_items]


def discover_topojson_files(skip_path: Path, *, include_per_year: bool) -> List[Path]:
    """Return TopoJSON files under data/, excluding the target output file."""
    if not DATA_DIR.exists():
        raise FileNotFoundError("data directory not found; run cli/download_ipc_areas.py first")

    skip_resolved = skip_path.resolve()
    files: List[Path] = []
    for path in DATA_DIR.rglob("*.topojson"):
        if path.resolve() == skip_resolved:
            continue
        if not path.is_file():
            continue

        if not include_per_year and not path.name.endswith(COMBINED_SUFFIX):
            continue

        files.append(path)
    return sorted(files)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Path for the aggregated TopoJSON output (default: data/global_areas.topojson)",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=3,
        help="Decimal precision for coordinate rounding during minification (default: 3)",
    )
    parser.add_argument(
        "--simplify-tolerance",
        type=float,
        default=0.001,
        help="Simplification tolerance applied after combination; set to 0 to disable",
    )
    parser.add_argument(
        "--skip-simplify",
        "--skip-minify",
        dest="skip_simplify",
        action="store_true",
        help="Skip the simplification pass if you plan to process the output separately",
    )
    parser.add_argument(
        "--include-per-year",
        action="store_true",
        help="Include per-year files (ISO3_YYYY_areas.topojson) in addition to combined outputs",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    output_path = args.output or (DATA_DIR / DEFAULT_OUTPUT_FILENAME)
    if not output_path.is_absolute():
        output_path = (REPO_ROOT / output_path).resolve()

    try:
        topo_files = discover_topojson_files(output_path, include_per_year=args.include_per_year)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if not topo_files:
        print("No TopoJSON files found under data/.", file=sys.stderr)
        return 1

    features = collect_all_features(topo_files)
    if not features:
        print("No features extracted; aborting.", file=sys.stderr)
        return 1

    topology = convert_geojson_to_topology({"type": "FeatureCollection", "features": features})
    save_topology(topology, output_path)
    print(f"Wrote {len(features)} features to {display_relative(output_path)}")

    if not args.skip_simplify:
        stats = simplify_topojson(
            output_path,
            precision=args.precision,
            simplify_tolerance=args.simplify_tolerance,
            quiet=True,
        )
        ratio = stats.get("size_ratio", 0.0)
        saved = stats.get("saved_bytes", 0)
        print(
            f"Simplified global dataset with precision {args.precision} and tolerance "
            f"{args.simplify_tolerance}; saved {saved:,} bytes ({ratio:.2%} of original)."
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
