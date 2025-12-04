# ROSEA IPC Areas Toolkit

Automation for downloading and harmonising IPC (Integrated Food Security Phase Classification) boundary data. The toolkit keeps the latest analysis per year, merges country datasets, and publishes global artefacts ready for maps or dashboards.

## Outputs

- `data/{ISO3}/{ISO3}_combined_areas.topojson` – all assessments for a country, deduplicated by IPC ID with rounded coordinates.
- `data/combined_areas.topojson` – aggregation of every combined country file with configurable rounding and simplification (defaults to conservative values).
- `data/combined_areas_min.topojson` – optional extra-minified combined dataset produced when requested for lightweight previews.
- `data/index.json` – catalogue of exported datasets, feature counts, timestamps, and optional CDN URLs.
- `data/**/*_unsimplified.json` – optional reports listing features kept at full detail when simplification fails or has no effect.

## Quick Start

1. Install Python 3.11.
2. `pip install -r requirements.txt`
3. Set your IPC API token: PowerShell → `$env:IPC_KEY = 'your_api_key'`.
4. Run `python -m cli.download_ipc_areas`.

## Common Commands

- Limit scope: `python -m cli.download_ipc_areas --countries SD --years 2025 2024`
- Custom precision: `python -m cli.download_ipc_areas --precision 2 --simplify-tolerance 0.0005`
- Rebuild global only: `python -m cli.combine_ipc_areas`
- Simplify an existing file: `python -m cli.simplify_ipc_combined_areas --help`
- Programmatic use: `from rosea_ipc_toolkit import DownloadConfig, IPCAreaDownloader`
- Skip index generation: `python -m cli.download_ipc_areas --skip-index`
- Generate extra-minified global output: `python -m cli.download_ipc_areas --extra-global-simplification`
- Only regenerate the extra-minified global output: `python -m cli.download_ipc_areas --extra-global-only`

## GitHub Workflows

### Data Refresh Workflow

- `.github/workflows/refresh-ipc-areas.yml` runs every Monday at 06:00 UTC and supports manual dispatch.
- Inputs:
  - `full_refresh` – process the default year set (current year).
  - `specific_years` – comma-separated override for exact years.
  - `country_codes` – comma-separated ISO2/ISO3 filter.
  - `skip_index` – omit index generation, useful for exploratory runs.
  - `extra_global_simplification` – emit an additional aggressively simplified global TopoJSON file.
- Workflow regenerates combined/global files, refreshes `data/index.json` (unless skipped), and opens a pull request.

### Mapbox Tileset Workflow

- `.github/workflows/update-mapbox-tileset.yml` runs every Monday at 07:00 UTC (1 hour after data refresh) and supports manual dispatch.
- Uploads `data/combined_areas.topojson` to Mapbox as a tileset source and publishes the tileset.
- Required secrets (configure in GitHub repo settings):
  - `MAPBOX_USERNAME` – your Mapbox account username.
  - `MAPBOX_ACCESS_TOKEN` – Mapbox access token with tilesets:write scope.
  - `TILESET_SOURCE_ID` – identifier for the tileset source (e.g., `ipc-areas-source`).
  - `TILESET_ID` – identifier for the tileset (e.g., `ipc-areas`).

## Development Notes

- Core logic lives under `rosea_ipc_toolkit/`; CLI wrappers sit in `cli/`.
- `DownloadConfig` controls years, precision, simplification, rate limiting, and country filters.
- Geometry content now mirrors the source analysis: every geometry type (points, lines, polygons, collections) is retained after sanitisation, and the TopoJSON loader handles point-heavy collections without sidecar formats.
- The downloader defaults to the current assessment year; specify additional years with the `--years` flag when needed.
- CDN URLs default to the next semantic git tag; set `CDN_RELEASE_TAG` to override.

## Technical: Custom TopoJSON Builder

### The Problem

The Python `topojson` library (v1.7) has a bug with **arc sharing for adjacent polygons**. When polygons share edges, the library assigns incorrect arc indices, causing:

- Broken arc chains where consecutive arcs don't connect end-to-end
- GeometryCollections with invalid topology that fail to convert back to GeoJSON
- `topo2geo` (from topojson-client) outputting null coordinates for affected features

This manifested as features like "Gaalkacyo (1)" appearing to overlap the entire map when visualized, because the arc references pointed to wrong coordinates.

### The Solution

We implemented a custom `TopologyBuilder` class (`rosea_ipc_toolkit/topojson_builder.py`) that correctly handles arc sharing:

1. **Two-pass algorithm**:
   - First pass: Collect all vertices across all features and count occurrences to identify shared vertices
   - Second pass: Build arcs, splitting rings at shared vertices to enable proper arc reuse

2. **Correct arc sharing**: When two polygons share an edge, the same arc is referenced with opposite directions (positive index for forward, negative/bitwise-complement for reversed)

3. **Full geometry support**: Handles Point, LineString, Polygon, MultiPolygon, and GeometryCollection geometries

### Validation

The custom builder produces valid TopoJSON that:

- ✅ All arc chains form closed rings (first coordinate equals last)
- ✅ Converts cleanly with `topo2geo` with zero null geometries
- ✅ Tested against 4,500+ features from all IPC countries

For IPC API issues contact the IPC Info team; for toolkit questions open a GitHub issue or review CLI logs.
