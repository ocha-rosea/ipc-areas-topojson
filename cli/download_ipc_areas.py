#!/usr/bin/env python3
"""Download, tidy, and publish IPC area datasets."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional

# Add the parent directory to Python path so we can import the toolkit package
sys.path.insert(0, str(Path(__file__).parent.parent))

from rosea_ipc_toolkit import DownloadConfig, IPCAreaDownloader


def parse_cli_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download IPC area datasets and rebuild local artefacts",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        help="Override the list of assessment years to attempt (e.g. --years 2025 2024)",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=3,
        help="Decimal precision applied during simplification (default: 3)",
    )
    parser.add_argument(
        "--simplify-tolerance",
        type=float,
        default=0.001,
        help="Simplification tolerance applied to combined outputs (default: 0.001)",
    )
    parser.add_argument(
        "--ocha-region",
        type=str,
        default="ROSEA",
        help="Restrict processing to countries within the specified OCHA region (default: ROSEA)",
    )
    parser.add_argument(
        "--countries",
        type=str,
        nargs="+",
        help="Restrict processing to the provided ISO2/ISO3 country codes (e.g. --countries UG SO)",
    )
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=30,
        help="Timeout in seconds for IPC API requests (default: 30)",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=0.5,
        help="Delay in seconds after a failed year fetch before retrying the next (default: 0.5)",
    )
    parser.add_argument(
        "--rate-limit-delay",
        type=float,
        default=1.0,
        help="Delay in seconds between countries to respect IPC rate limits (default: 1.0)",
    )
    parser.add_argument(
        "--skip-index",
        action="store_true",
        help="Disable index.json generation (useful for ad-hoc runs)",
    )
    parser.add_argument(
        "--extra-combined-simplification",
        action="store_true",
        help="Write an additional aggressively simplified combined_areas_min.topojson",
    )
    parser.add_argument(
        "--extra-combined-only",
        action="store_true",
        help="Skip downloads and regenerate only the extra simplified combined_areas_min.topojson",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_cli_args(argv)

    extra_combined_only = args.extra_combined_only

    config = DownloadConfig(
        years_to_try=args.years,
        precision=args.precision,
        simplify_tolerance=args.simplify_tolerance,
        ocha_region=args.ocha_region,
        request_timeout=args.request_timeout,
        retry_delay=args.retry_delay,
        rate_limit_delay=args.rate_limit_delay,
        country_codes=args.countries,
        build_index=False if extra_combined_only else not args.skip_index,
        extra_combined_simplification=args.extra_combined_simplification or extra_combined_only,
        extra_combined_only=extra_combined_only,
    )

    try:
        downloader = IPCAreaDownloader(config)
        downloader.run()
    except KeyboardInterrupt:
        print("\nScript interrupted by user")
        return 1
    except Exception as exc:
        print(f"Script failed: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
