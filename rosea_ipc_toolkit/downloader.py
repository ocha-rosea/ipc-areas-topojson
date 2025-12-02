"""High-level orchestrator for fetching and consolidating IPC area datasets."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from .analysis import select_latest_analysis
from .auth import resolve_ipc_key
from .config import (
    API_BASE_URL,
    COUNTRY_COMBINED_SUFFIX,
    COUNTRY_FILENAME_SUFFIX,
    DATA_DIR,
    DEFAULT_YEARS,
    AVAILABLE_YEARS,
    GLOBAL_EXTRA_OUTPUT_PATH,
    GLOBAL_INFO,
    GLOBAL_OUTPUT_PATH,
)
from .countries import load_countries as load_country_rows
from .dates import (
    ANALYSIS_ID_KEYS,
    ANALYSIS_LABEL_KEYS,
    DATE_FROM_KEYS,
    DATE_PUBLISHED_KEYS,
    DATE_TO_KEYS,
    DATE_UPDATED_KEYS,
    first_present,
)
from .feature_utils import feature_key, sanitise_geometry
from .git_utils import resolve_release_tag
from .index import IndexBuilder
from .merge import extract_years, flatten_features, merge_features
from .topology import (
    convert_geojson_to_topology,
    load_topojson_features,
    save_topology,
    display_relative,
)


@dataclass(frozen=True)
class DownloadConfig:
    years_to_try: Optional[List[int]] = None
    precision: int = 3
    simplify_tolerance: float = 0.001
    ocha_region: Optional[str] = "ROSEA"
    request_timeout: int = 30
    retry_delay: float = 0.5
    rate_limit_delay: float = 1.0
    country_codes: Optional[List[str]] = None
    build_index: bool = True
    extra_global_simplification: bool = False
    extra_global_only: bool = False


class IPCAreaDownloader:
    def __init__(self, config: DownloadConfig) -> None:
        self.config = config
        self.extra_global_only = config.extra_global_only
        self.ipc_key = resolve_ipc_key()
        if not self.ipc_key and not self.extra_global_only:
            raise ValueError("IPC_KEY environment variable is required")

        self.years_to_try = self._normalise_years(config.years_to_try)
        if not self.years_to_try:
            raise ValueError("At least one assessment year must be configured")
        if config.precision < 0:
            raise ValueError("Precision must be non-negative")
        if config.simplify_tolerance < 0:
            raise ValueError("Simplification tolerance must be non-negative")

        self.session = self._build_session()
        self.release_tag = resolve_release_tag()
        self.index_builder = (
            IndexBuilder(release_tag=self.release_tag, output_dir=DATA_DIR)
            if config.build_index and not self.extra_global_only
            else None
        )
        self.extra_global_simplification = config.extra_global_simplification or self.extra_global_only
        self.country_combined_files: List[Path] = []
        self.country_combined_feature_map: Dict[str, List[Dict[str, Any]]] = {}
        self.iso2_to_iso3: Dict[str, str] = {}
        self.country_filter = self._normalise_country_codes(config.country_codes)
        self.current_date = datetime.utcnow().date()

    @staticmethod
    def _normalise_years(years: Optional[Iterable[int]]) -> List[int]:
        if not years:
            return list(DEFAULT_YEARS)

        normalised: List[int] = []
        seen = set()
        for year in years:
            value = int(year)
            if value in seen:
                continue
            normalised.append(value)
            seen.add(value)

        if not normalised:
            raise ValueError("At least one valid assessment year must be provided")

        return normalised

    @staticmethod
    def _normalise_country_codes(codes: Optional[Iterable[str]]) -> List[str]:
        if not codes:
            return []

        normalised: List[str] = []
        seen = set()
        for code in codes:
            text = str(code).strip().upper()
            if not text or text in seen:
                continue
            normalised.append(text)
            seen.add(text)

        return normalised

    def _filter_countries(self, countries: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        if not self.country_filter:
            return countries

        selected: Dict[str, Dict[str, str]] = {}
        matched: set[str] = set()

        for iso2, info in countries.items():
            iso2_code = iso2.upper()
            iso3_code = (info.get("iso3") or "").upper()

            if iso2_code in self.country_filter or iso3_code in self.country_filter:
                selected[iso2] = info
                matched.add(iso2_code)
                if iso3_code:
                    matched.add(iso3_code)

        missing = [code for code in self.country_filter if code not in matched]
        if missing:
            print("Warning: requested country codes not found in countries.csv: " + ", ".join(missing))

        if not selected:
            raise ValueError("Country filter excluded all available countries")

        return selected

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": "IPC-Areas-Downloader/1.0"})
        return session

    def _normalise_iso3(self, props: Dict[str, Any], country_info: Dict[str, str]) -> str:
        iso3_candidate = (props.get("iso3") or "").strip()
        if len(iso3_candidate) == 3:
            return iso3_candidate.upper()

        country_field = (props.get("country") or "").strip()
        if len(country_field) == 3:
            return country_field.upper()

        if len(country_field) == 2:
            mapped = self.iso2_to_iso3.get(country_field.upper())
            if mapped:
                return mapped

        # Fall back to the configured country since the API parameter drives the dataset.
        return country_info["iso3"]

    def run(self) -> None:
        print("IPC Areas Download Script")
        print("=" * 50)
        
        # Ensure the data directory exists
        DATA_DIR.mkdir(exist_ok=True)

        if self.extra_global_only:
            self._generate_extra_global_only()
            return
        
        print("Loading countries data…")
        countries = load_country_rows(ocha_region=self.config.ocha_region)
        if self.country_filter:
            countries = self._filter_countries(countries)
            print(
                "Country filter applied: "
                + ", ".join(self.country_filter)
                + f" → {len(countries)} match(es)"
            )
        else:
            print(f"Loaded {len(countries)} countries")

        self.iso2_to_iso3 = {
            code.upper(): (info.get("iso3") or "").upper()
            for code, info in countries.items()
            if info.get("iso3")
        }
        region_label = self.config.ocha_region or "All regions"
        print(f"OCHA region filter: {region_label}")
        preset_years = ", ".join(str(year) for year in AVAILABLE_YEARS)
        print(f"Preset year window: {preset_years}")
        print("Assessment years: " + ", ".join(str(year) for year in self.years_to_try))

        successful = 0
        failed = 0

        for iso2, country_info in countries.items():
            try:
                if self.process_country(iso2, country_info):
                    successful += 1
                else:
                    failed += 1
            except Exception as exc:  # noqa: BLE001
                print(f"Error processing {country_info['name']}: {exc}")
                failed += 1

            time.sleep(self.config.rate_limit_delay)

        self.build_global_dataset()
        if self.index_builder:
            self.index_builder.write()

        print("\n" + "=" * 50)
        print("Processing complete!")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Data saved in: {DATA_DIR.resolve()}")

    # Country processing -------------------------------------------------
    def process_country(self, country_code: str, country_info: Dict[str, str]) -> bool:
        print(f"\nProcessing {country_info['name']} ({country_code})…")

        iso3 = country_info["iso3"]
        country_dir = DATA_DIR / iso3
        country_dir.mkdir(parents=True, exist_ok=True)

        legacy_combined = country_dir / f"{iso3}{COUNTRY_FILENAME_SUFFIX}"
        modern_combined = country_dir / f"{iso3}{COUNTRY_COMBINED_SUFFIX}"
        self._migrate_legacy_combined(legacy_combined, modern_combined)

        aggregate: Dict[str, Dict[str, Any]] = {}
        year_feature_counts: Dict[int, Dict[str, Any]] = {}

        if modern_combined.exists():
            existing_features = load_topojson_features(modern_combined)
            if existing_features:
                stats = merge_features(
                    aggregate,
                    existing_features,
                    priority=-5,
                    source_year=None,
                    source_label="legacy_combined",
                )
                if stats["added"] or stats["updated"]:
                    print(
                        "    Legacy combined dataset provided "
                        f"{stats['added']} baseline and {stats['updated']} refreshed feature(s)"
                    )

        for path in sorted(country_dir.glob(f"{iso3}_*{COUNTRY_FILENAME_SUFFIX}")):
            year = self._extract_year_from_path(path, iso3)
            if year is None:
                continue

            features = load_topojson_features(path)
            if not features:
                continue

            stats = merge_features(
                aggregate,
                features,
                priority=0,
                source_year=year,
                source_label=f"existing:{year}",
            )
            if stats["added"] or stats["updated"]:
                print(
                    f"    Existing year {year} dataset contributed {stats['added']} new "
                    f"and {stats['updated']} updated features"
                )
            year_feature_counts[year] = {
                "path": path,
                "feature_count": len(features),
                "analysis": None,
            }

        for year in self.years_to_try:
            areas_data = self._download_areas(country_code, year)
            if not areas_data:
                time.sleep(self.config.retry_delay)
                continue

            geojson, analysis_meta = self._filter_and_process(areas_data, country_info, year)
            if not geojson:
                print(f"    No valid polygon features found for year {year}")
                time.sleep(self.config.retry_delay)
                continue

            # Enrich features with analysis metadata for better merge prioritization
            for feature in geojson["features"]:
                if "properties" not in feature:
                    feature["properties"] = {}
                props = feature["properties"]
                
                # Add analysis date metadata if not already present
                if "to" not in props and analysis_meta.get("to_date"):
                    props["to"] = analysis_meta["to_date"]
                if "from" not in props and analysis_meta.get("from_date"):
                    props["from"] = analysis_meta["from_date"]
                if "updated_at" not in props and analysis_meta.get("updated_at"):
                    props["updated_at"] = analysis_meta["updated_at"]

            topojson_payload = convert_geojson_to_topology(geojson)
            year_path = country_dir / f"{iso3}_{year}{COUNTRY_FILENAME_SUFFIX}"
            # Skip saving individual year files to save space - only save combined and global files
            # save_topology(topojson_payload, year_path)
            # print(f"    Saved: {display_relative(year_path)}")

            year_feature_counts[year] = {
                "path": year_path,
                "feature_count": len(geojson["features"]),
                "analysis": analysis_meta,
            }

            stats = merge_features(
                aggregate,
                geojson["features"],
                priority=10,
                source_year=year,
                source_label=f"download:{year}",
            )
            detail = self._format_analysis_details(analysis_meta)
            print(
                f"    Year {year}: {len(geojson['features'])} features retained "
                f"({stats['added']} new, {stats['updated']} updated){detail}"
            )

            time.sleep(self.config.retry_delay)

        if not aggregate:
            print(f"    No data found for {country_info['name']} in any year")
            return False

        final_features = flatten_features(aggregate)
        combined_topology = convert_geojson_to_topology(
            {"type": "FeatureCollection", "features": final_features}
        )
        combined_path = save_topology(combined_topology, modern_combined)
        self._simplify_output(combined_path)
        self.country_combined_files.append(combined_path)
        self.country_combined_feature_map[iso3] = final_features

        feature_count = len(final_features)
        available_years = sorted(year_feature_counts.keys()) or extract_years(aggregate)

        if self.index_builder:
            for year in available_years:
                stats = year_feature_counts.get(year)
                if not stats:
                    continue
                analysis_info = stats.get("analysis") or {}
                updated_hint = analysis_info.get("updated_at") or analysis_info.get("to_date")
                self.index_builder.add_entry(
                    country_info,
                    year=year,
                    path=stats["path"],
                    feature_count=stats.get("feature_count"),
                    variant="year",
                    updated_at=updated_hint,
                )

            representative_year = available_years[-1] if available_years else None
            self.index_builder.add_entry(
                country_info,
                year=representative_year,
                path=combined_path,
                feature_count=feature_count,
                variant="combined",
            )

        print(
            f"    Combined dataset saved with {feature_count} features across "
            f"{len(available_years)} assessment year(s)"
        )

        return True

    # Download helpers ---------------------------------------------------
    def _download_areas(self, country_code: str, year: int) -> Optional[Dict[str, Any]]:
        params = {
            "format": "geojson",
            "country": country_code,
            "year": year,
            "type": "A",
            "key": self.ipc_key,
        }

        try:
            print(f"  Downloading data for {country_code} - {year}…")
            response = self.session.get(
                API_BASE_URL,
                params=params,
                timeout=self.config.request_timeout,
            )
        except requests.exceptions.RequestException as exc:
            print(f"    Request failed for {country_code} - {year}: {exc}")
            return None

        if response.status_code != 200:
            print(f"    HTTP {response.status_code} for {country_code} - {year}")
            return None

        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            print(f"    Invalid JSON response for {country_code} - {year}: {exc}")
            return None

        if (
            isinstance(data, dict)
            and isinstance(data.get("features"), list)
            and data["features"]
        ):
            return data

        print(f"    No data available for {country_code} in {year}")
        return None

    def _filter_and_process(
        self,
        areas_data: Dict[str, Any],
        country_info: Dict[str, str],
        year: int,
    ) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
        raw_features = areas_data.get("features", []) if isinstance(areas_data, dict) else []
        selected_features, analysis_meta = select_latest_analysis(
            raw_features,
            target_year=year,
            current_date=self.current_date,
        )

        cleaned_features: List[Dict[str, Any]] = []
        seen_geometries: set[str] = set()
        seen_ids: set[str] = set()

        for feature in selected_features:
            original_geometry = feature.get("geometry")
            geometry = sanitise_geometry(original_geometry)
            if not geometry:
                continue

            geometry_str = json.dumps(geometry, sort_keys=True)
            if geometry_str in seen_geometries:
                continue

            props = feature.get("properties") or {}
            feature_id = props.get("id")
            feature_id_str = str(feature_id).strip() if feature_id is not None else None
            if feature_id_str and feature_id_str in seen_ids:
                continue

            seen_geometries.add(geometry_str)
            if feature_id_str:
                seen_ids.add(feature_id_str)

            attributes: Dict[str, Any] = {
                "country": self._normalise_iso3(props, country_info),
                "title": props.get("title") or "",
                "color": props.get("color"),
                "year": props.get("year") or year,
            }

            if feature_id is not None:
                attributes["id"] = feature_id

            from_value = first_present(props, DATE_FROM_KEYS)
            if from_value is not None:
                attributes["from"] = from_value

            to_value = first_present(props, DATE_TO_KEYS)
            if to_value is not None:
                attributes["to"] = to_value

            cleaned_features.append(
                {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": attributes,
                }
            )

        analysis_meta["feature_count"] = len(cleaned_features)
        if not cleaned_features:
            return None, analysis_meta

        return {"type": "FeatureCollection", "features": cleaned_features}, analysis_meta

    # Global dataset -----------------------------------------------------
    def build_global_dataset(self) -> None:
        print("\nBuilding global dataset…")

        aggregate: Dict[str, Dict[str, Any]] = {}

        processed_iso3: set[str] = set()
        for iso3, features in self.country_combined_feature_map.items():
            processed_iso3.add(iso3)
            merge_features(
                aggregate,
                features,
                priority=0,
                source_year=None,
                source_label=f"memory:{iso3}",
            )

        for country_dir in sorted(DATA_DIR.iterdir()):
            if not country_dir.is_dir():
                continue
            iso3 = country_dir.name
            if iso3 in processed_iso3:
                continue

            topo_candidate = country_dir / f"{iso3}{COUNTRY_COMBINED_SUFFIX}"

            features: List[Dict[str, Any]] = []

            if topo_candidate.exists():
                try:
                    features = load_topojson_features(topo_candidate)
                except Exception as exc:  # noqa: BLE001
                    print(
                        f"  Warning: unable to read existing dataset {display_relative(topo_candidate)}: {exc}"
                    )
                    continue

            if not features:
                continue

            merge_features(
                aggregate,
                features,
                priority=0,
                source_year=None,
                source_label=topo_candidate.name,
            )

        if not aggregate:
            print("  Warning: no features discovered while building the global dataset")
            return

        final_features = flatten_features(aggregate)
        
        # Remove color and year properties from global dataset features to reduce file size
        for feature in final_features:
            if "properties" in feature:
                feature["properties"].pop("color", None)
                feature["properties"].pop("year", None)
        
        final_topology = convert_geojson_to_topology(
            {"type": "FeatureCollection", "features": final_features}
        )
        
        # Apply aggressive coordinate rounding to reduce global dataset size
        if 'arcs' in final_topology:
            final_topology['arcs'] = self._round_coordinates(final_topology['arcs'], precision=2)
        saved_global = save_topology(final_topology, GLOBAL_OUTPUT_PATH)
        
        # Apply aggressive simplification to global dataset
        extra_global_path: Optional[Path] = None

        try:
            # Import here to avoid circular import
            from cli.simplify_ipc_global_areas import simplify_topojson
            
            simplify_topojson(
                saved_global,
                precision=2,  # More aggressive precision for global file
                simplify_tolerance=0.002,  # Higher tolerance for global file
                quiet=True,
            )

            if self.extra_global_simplification:
                extra_global_path = GLOBAL_EXTRA_OUTPUT_PATH
                simplify_topojson(
                    saved_global,
                    output=extra_global_path,
                    precision=1,
                    simplify_tolerance=0.01,
                    quiet=True,
                )
                self._strip_global_properties(extra_global_path, keys=("from", "to"))
        except Exception as exc:  # noqa: BLE001
            print(f"    Warning: unable to apply additional simplification to global dataset: {exc}")

        years_seen = extract_years(aggregate)
        representative_year = years_seen[-1] if years_seen else None

        if self.index_builder:
            self.index_builder.add_entry(
                GLOBAL_INFO,
                year=representative_year,
                path=saved_global,
                feature_count=len(final_features),
                variant="global",
            )

            if self.extra_global_simplification and extra_global_path and extra_global_path.exists():
                self.index_builder.add_entry(
                    GLOBAL_INFO,
                    year=representative_year,
                    path=extra_global_path,
                    feature_count=len(final_features),
                    variant="global_min",
                )

        if self.extra_global_simplification and extra_global_path and extra_global_path.exists():
            print(
                "  Extra simplified global dataset saved to "
                f"{display_relative(extra_global_path)}"
            )

        legacy_path = DATA_DIR / "ipc_global_areas.topojson"
        if legacy_path.exists() and legacy_path != saved_global:
            try:
                legacy_path.unlink()
                print(f"  Removed legacy global dataset {display_relative(legacy_path)}")
            except OSError as exc:  # noqa: BLE001
                print(f"  Warning: unable to remove legacy global dataset {legacy_path}: {exc}")

        print(
            f"  Global dataset saved to {display_relative(saved_global)} "
            f"with {len(final_features)} features"
        )

    def _generate_extra_global_only(self) -> None:
        print("Extra global-only mode: skipping downloads and regenerating minified global dataset")

        if not GLOBAL_OUTPUT_PATH.exists():
            raise FileNotFoundError(
                f"Global dataset not found at {display_relative(GLOBAL_OUTPUT_PATH)}. "
                "Run without --extra-global-only to rebuild it first."
            )

        try:
            from cli.simplify_ipc_global_areas import simplify_topojson

            simplify_topojson(
                GLOBAL_OUTPUT_PATH,
                output=GLOBAL_EXTRA_OUTPUT_PATH,
                precision=1,
                simplify_tolerance=0.01,
                quiet=True,
            )

            self._strip_global_properties(GLOBAL_EXTRA_OUTPUT_PATH, keys=("from", "to"))
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"Unable to regenerate extra simplified global dataset: {exc}"
            ) from exc

        print(
            "  Extra simplified global dataset saved to "
            f"{display_relative(GLOBAL_EXTRA_OUTPUT_PATH)}"
        )

    # Utility functions --------------------------------------------------
    def _extract_year_from_path(self, filepath: Path, iso3: str) -> Optional[int]:
        name = filepath.name
        if not name.startswith(f"{iso3}_") or not name.endswith(COUNTRY_FILENAME_SUFFIX):
            return None

        core = name[len(iso3) + 1 : -len(COUNTRY_FILENAME_SUFFIX)]
        try:
            return int(core)
        except ValueError:
            return None

    def _migrate_legacy_combined(self, legacy_path: Path, modern_path: Path) -> None:
        if legacy_path.exists() and not modern_path.exists():
            try:
                legacy_path.rename(modern_path)
                print(
                    f"    Renamed legacy combined dataset {legacy_path.name} -> {modern_path.name}"
                )
            except OSError as exc:  # noqa: BLE001
                print(f"    Warning: unable to rename legacy combined dataset {legacy_path}: {exc}")
        elif legacy_path.exists() and modern_path.exists():
            try:
                legacy_path.unlink()
                print(f"    Removed legacy dataset {legacy_path.name}")
            except OSError as exc:  # noqa: BLE001
                print(f"    Warning: unable to remove legacy dataset {legacy_path}: {exc}")

    def _round_coordinates(self, obj, precision=3):
        """Recursively round all coordinate values to specified precision."""
        if isinstance(obj, list):
            return [self._round_coordinates(item, precision) for item in obj]
        elif isinstance(obj, (int, float)):
            return round(obj, precision)
        elif isinstance(obj, dict):
            return {key: self._round_coordinates(value, precision) for key, value in obj.items()}
        else:
            return obj

    def _simplify_output(self, topo_path: Path) -> None:
        try:
            # Import here to avoid circular import
            from cli.simplify_ipc_global_areas import simplify_topojson
            
            # Apply geometric simplification
            simplify_topojson(
                topo_path,
                precision=self.config.precision,
                simplify_tolerance=self.config.simplify_tolerance,
                quiet=True,
            )
            
            # Apply additional coordinate rounding for combined files to reduce size
            self._apply_coordinate_rounding(topo_path, precision=self.config.precision)
            
        except Exception as exc:  # noqa: BLE001
            print(f"    Warning: unable to simplify {topo_path.name}: {exc}")
    
    def _apply_coordinate_rounding(self, topo_path: Path, precision: int = 2) -> None:
        """Apply coordinate rounding to a TopoJSON file to reduce file size."""
        try:
            import json
            
            # Read the file
            with open(topo_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Round coordinates in arcs
            if 'arcs' in data:
                data['arcs'] = self._round_coordinates(data['arcs'], precision)
            
            # Write back with compact JSON
            with open(topo_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, separators=(',', ':'))
                
        except Exception as exc:  # noqa: BLE001
            print(f"    Warning: unable to round coordinates in {topo_path.name}: {exc}")

    def _strip_global_properties(self, topo_path: Path, keys: Tuple[str, ...]) -> None:
        try:
            with topo_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)

            objects = payload.get("objects") if isinstance(payload, dict) else None
            if not isinstance(objects, dict):
                return

            changed = False
            for obj in objects.values():
                geometries = obj.get("geometries") if isinstance(obj, dict) else None
                if not isinstance(geometries, list):
                    continue
                for geom in geometries:
                    props = geom.get("properties") if isinstance(geom, dict) else None
                    if not isinstance(props, dict):
                        continue
                    for key in keys:
                        if key in props:
                            props.pop(key, None)
                            changed = True

            if changed:
                with topo_path.open("w", encoding="utf-8") as handle:
                    json.dump(payload, handle, separators=(",", ":"))
        except Exception as exc:  # noqa: BLE001
            print(f"    Warning: unable to strip properties from {topo_path.name}: {exc}")

    @staticmethod
    def _format_analysis_details(meta: Dict[str, Any]) -> str:
        parts = []
        for key in ("analysis_id", "analysis_label", "to_date"):
            value = meta.get(key)
            if value:
                parts.append(str(value))
        return f" [{', '.join(parts)}]" if parts else ""
