"""Command-line helpers for IPC Areas data processing."""

import sys
from pathlib import Path

# Add the parent directory to Python path so we can import the toolkit package
sys.path.insert(0, str(Path(__file__).parent.parent))

from rosea_ipc_toolkit import IPCAreaDownloader

from .combine_ipc_areas import main as combine_main
from .simplify_ipc_combined_areas import simplify_topojson, minify_topojson

__all__ = [
    "IPCAreaDownloader",
    "combine_main",
    "simplify_topojson",
    "minify_topojson",
]
