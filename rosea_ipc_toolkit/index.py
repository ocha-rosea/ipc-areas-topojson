"""Index generation utilities for exported IPC datasets."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import REPO_ROOT
from .topology import display_relative, infer_feature_count

IndexEntry = Dict[str, Any]


class IndexBuilder:
    def __init__(self, *, release_tag: str, output_dir: Path) -> None:
        self.release_tag = release_tag
        self.output_dir = output_dir
        self.entries: List[IndexEntry] = []

    def add_entry(
        self,
        country_info: Dict[str, str],
        *,
        year: Optional[int],
        path: Path,
        feature_count: Optional[int],
        variant: str,
        updated_at: Optional[str] = None,
    ) -> None:
        try:
            relative_path = path.relative_to(REPO_ROOT)
        except ValueError:
            relative_path = path

        feature_count = feature_count or infer_feature_count(path)
        updated_at = updated_at or datetime.utcnow().isoformat(timespec="seconds") + "Z"

        entry: IndexEntry = {
            "country": country_info.get("name", country_info.get("iso2")),
            "iso2": country_info.get("iso2"),
            "iso3": country_info.get("iso3"),
            "year": year,
            "relative_path": relative_path.as_posix(),
            "file_name": path.name,
            "feature_count": feature_count,
            "cdn_url": (
                f"https://cdn.jsdelivr.net/gh/im4sea/ipc-areas@{self.release_tag}/"
                f"{display_relative(path)}"
            ),
            "updated_at": updated_at,
            "variant": variant,
        }

        if variant == "combined":
            for field in ("iso2", "iso3", "year"):
                entry.pop(field, None)

        self.entries.append(entry)

    def write(self) -> None:
        index_path = self.output_dir / "index.json"
        index_payload = {
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "cdn_release_tag": self.release_tag,
            "total_files": len(self.entries),
            "items": sorted(
                self.entries,
                key=lambda entry: (
                    entry.get("iso3", ""),
                    entry.get("variant", ""),
                    entry.get("year") if isinstance(entry.get("year"), int) else -1,
                    entry.get("file_name", ""),
                ),
            ),
        }

        index_path.parent.mkdir(exist_ok=True, parents=True)
        with index_path.open("w", encoding="utf-8") as handle:
            json.dump(index_payload, handle, indent=2)

        print(f"Index updated: {display_relative(index_path)}")
