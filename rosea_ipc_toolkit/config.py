"""Shared configuration constants for IPC area utilities."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
COUNTRIES_CSV = REPO_ROOT / "countries.csv"
COUNTRY_FILENAME_SUFFIX = "_areas.topojson"
COUNTRY_COMBINED_SUFFIX = "_combined_areas.topojson"
COMBINED_FILENAME = "combined_areas.topojson"
COMBINED_OUTPUT_PATH = DATA_DIR / COMBINED_FILENAME
COMBINED_EXTRA_FILENAME = "combined_areas_min.topojson"
COMBINED_EXTRA_OUTPUT_PATH = DATA_DIR / COMBINED_EXTRA_FILENAME
COMBINED_INFO = {"name": "Combined", "iso2": "CB", "iso3": "CMB"}

API_BASE_URL = "https://api.ipcinfo.org/areas"
CURRENT_YEAR = datetime.utcnow().year
AVAILABLE_YEARS = [CURRENT_YEAR]
DEFAULT_YEARS = [CURRENT_YEAR]
