#!/usr/bin/env python3
"""
Bulk canvas count verification script for AHRB IIIF volumes.

Loads all volumes from volumes-all.csv, fetches each manifest from
iiif.zasqua.org in parallel, and compares the canvas count (len of items)
against the expected image_count from the CSV.

AHRB_N1_024bis (image_count=0, never processed) is treated as a known skip.

Exit code: 0 if no mismatches and no errors, 1 otherwise.

Usage:
    python verify_counts.py
    python verify_counts.py --csv scripts/iiif/volumes-all.csv
    python verify_counts.py --workers 20
"""

import argparse
import concurrent.futures
import csv
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://iiif.zasqua.org"
DEFAULT_CSV = Path(__file__).parent / "volumes-all.csv"
DEFAULT_WORKERS = 10
RETRY_DELAY = 2  # seconds before retry on 429/5xx
PROGRESS_INTERVAL = 50  # print progress every N volumes

# Volumes expected to be absent (never processed).
KNOWN_SKIPS = {"co-ahrb-n1-024bis"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}", flush=True)


def derive_volume_slug(fond: str, volume: str) -> str:
    """Derive the URL-safe volume slug used in iiif.zasqua.org paths."""
    return f"co-{fond.lower().replace('_', '-')}-{volume}"


def load_volumes(fileobj) -> list[dict]:
    """Load volumes from a CSV file-like object.

    Returns a list of dicts with keys: fond, volume, image_dir, image_count.
    image_count is converted to int.
    """
    reader = csv.DictReader(fileobj)
    volumes = []
    for row in reader:
        row["image_count"] = int(row["image_count"])
        volumes.append(row)
    return volumes


def is_known_skip(slug: str) -> bool:
    """Return True if the slug is a known skip (should not be on iiif.zasqua.org)."""
    return slug in KNOWN_SKIPS


def compare_count(canvas_count: int, expected_count: int) -> dict:
    """Compare actual canvas count against expected count.

    Returns dict with 'status' key set to 'match' or 'mismatch'.
    On mismatch, also includes 'canvas_count' and 'expected_count'.
    """
    if canvas_count == expected_count:
        return {"status": "match"}
    return {
        "status": "mismatch",
        "canvas_count": canvas_count,
        "expected_count": expected_count,
    }


def fetch_manifest_canvas_count(slug: str, base_url: str, timeout: int = 30) -> tuple[int | None, str | None]:
    """Fetch a manifest and return the number of canvases. Returns (count, error)."""
    url = f"{base_url}/{slug}/manifest.json"

    def _fetch() -> tuple[int | None, str | None]:
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
                manifest = json.loads(data)
                return len(manifest.get("items", [])), None
        except urllib.error.HTTPError as exc:
            return None, f"HTTP {exc.code}"
        except urllib.error.URLError as exc:
            return None, f"URL error: {exc.reason}"
        except json.JSONDecodeError as exc:
            return None, f"JSON error: {exc}"

    count, err = _fetch()

    # Retry once on 429 or 5xx
    if err and (err.startswith("HTTP 429") or err.startswith("HTTP 5")):
        time.sleep(RETRY_DELAY)
        count, err = _fetch()

    return count, err


def check_volume(vol: dict, base_url: str) -> dict:
    """Check one volume. Returns a result dict."""
    fond = vol["fond"]
    volume = vol["volume"]
    expected = vol["image_count"]
    slug = derive_volume_slug(fond, volume)

    if is_known_skip(slug):
        return {"slug": slug, "status": "skip"}

    count, err = fetch_manifest_canvas_count(slug, base_url)

    if err:
        return {"slug": slug, "status": "error", "error": err}

    result = compare_count(count, expected)
    result["slug"] = slug
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(volumes_path: Path, base_url: str, workers: int) -> bool:
    """Run bulk count verification. Returns True if all pass (no mismatches, no errors)."""
    with open(volumes_path, newline="", encoding="utf-8") as f:
        volumes = load_volumes(f)

    total = len(volumes)
    log(f"Loaded {total} volumes from {volumes_path}")
    log(f"Fetching manifests with {workers} workers ...")

    results = []
    completed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_vol = {
            executor.submit(check_volume, vol, base_url): vol
            for vol in volumes
        }
        for future in concurrent.futures.as_completed(future_to_vol):
            result = future.result()
            results.append(result)
            completed += 1
            if completed % PROGRESS_INTERVAL == 0:
                log(f"  Progress: {completed}/{total}")

    # Categorise results
    matches = [r for r in results if r["status"] == "match"]
    mismatches = [r for r in results if r["status"] == "mismatch"]
    errors = [r for r in results if r["status"] == "error"]
    skips = [r for r in results if r["status"] == "skip"]

    # Print summary
    print()
    print("=" * 72)
    print("BULK COUNT VERIFICATION SUMMARY")
    print("=" * 72)
    print(f"Total volumes:  {total}")
    print(f"Matches:        {len(matches)}")
    print(f"Mismatches:     {len(mismatches)}")
    print(f"Errors:         {len(errors)}")
    print(f"Known skips:    {len(skips)}")

    if mismatches:
        print()
        print("MISMATCHES:")
        for r in mismatches:
            print(f"  {r['slug']}: expected {r['expected_count']}, got {r['canvas_count']}")

    if errors:
        print()
        print("ERRORS:")
        for r in errors:
            print(f"  {r['slug']}: {r['error']}")

    if skips:
        print()
        print("KNOWN SKIPS:")
        for r in skips:
            print(f"  {r['slug']}")

    print("=" * 72)

    all_pass = not mismatches and not errors
    if all_pass:
        print("Result: PASS")
    else:
        print("Result: FAIL")

    return all_pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify canvas counts for all AHRB volumes against iiif.zasqua.org"
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help=f"Path to volumes CSV (default: {DEFAULT_CSV})",
    )
    parser.add_argument(
        "--base-url",
        default=BASE_URL,
        help=f"Base IIIF URL (default: {BASE_URL})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"ThreadPoolExecutor workers (default: {DEFAULT_WORKERS})",
    )
    args = parser.parse_args()

    ok = run(args.csv, args.base_url, args.workers)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
