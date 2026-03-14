#!/usr/bin/env python3
"""
Spot-check verification script for AHRB IIIF volumes.

Checks 13 edge-case volumes across all 5 fonds:
  - Fetches the IIIF Presentation v3 manifest from iiif.zasqua.org
  - Validates manifest structure (@context, type, items)
  - HEAD-checks the tile URL for the first and last canvas

Exit code: 0 if all checks pass, 1 if any fail.

Usage:
    python verify_spot_check.py
    python verify_spot_check.py --base-url https://iiif.zasqua.org
"""

import argparse
import json
import sys
import urllib.request
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://iiif.zasqua.org"
IIIF_V3_CONTEXT = "http://iiif.io/api/presentation/3/context.json"

# 13 edge-case volumes spanning all 5 fonds.
# Targets: special slug (Cabildos unico), N1 split boundary (096/097),
# smallest and largest volumes per fond.
SPOT_CHECK_VOLUMES = [
    # AHT: small, mid, large-ish
    ("AHRB_AHT", "008"),
    ("AHRB_AHT", "134"),
    ("AHRB_AHT", "126"),
    # Cabildos: unico (non-numeric slug), two numeric
    ("AHRB_Cabildos", "unico"),
    ("AHRB_Cabildos", "044"),
    ("AHRB_Cabildos", "029"),
    # N1: straddles the droplet split boundary (096/097) + one more
    ("AHRB_N1", "096"),
    ("AHRB_N1", "097"),
    ("AHRB_N1", "170"),
    # N2: first and last
    ("AHRB_N2", "001"),
    ("AHRB_N2", "157"),
    # NVL: two samples
    ("AHRB_NVL", "003"),
    ("AHRB_NVL", "039"),
]

# Volumes expected to be absent from iiif.zasqua.org (never processed).
KNOWN_SKIPS = {"co-ahrb-n1-024bis"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}", flush=True)


def derive_volume_slug(fond: str, volume: str) -> str:
    """Derive the URL-safe volume slug used in iiif.zasqua.org paths.

    Examples:
        ('AHRB_AHT', '003') -> 'co-ahrb-aht-003'
        ('AHRB_Cabildos', 'unico') -> 'co-ahrb-cabildos-unico'
    """
    return f"co-{fond.lower().replace('_', '-')}-{volume}"


def validate_manifest_structure(manifest: dict) -> bool:
    """Return True if manifest is a valid IIIF Presentation v3 manifest.

    Checks:
      - @context is the IIIF v3 context URL
      - type is 'Manifest'
      - items is a non-empty list
      - first item has type 'Canvas'
    """
    if manifest.get("@context") != IIIF_V3_CONTEXT:
        return False
    if manifest.get("type") != "Manifest":
        return False
    items = manifest.get("items")
    if not items:
        return False
    if items[0].get("type") != "Canvas":
        return False
    return True


def extract_tile_url(canvas: dict) -> str:
    """Return the full/max tile URL for a canvas.

    Reads body.service[0].id from the canvas and appends
    /full/max/0/default.jpg as specified by IIIF Image API v3.
    """
    service_id = canvas["body"]["service"][0]["id"]
    return f"{service_id}/full/max/0/default.jpg"


def fetch_manifest(slug: str, base_url: str, timeout: int = 30) -> tuple[dict | None, str | None]:
    """Fetch and parse a manifest JSON. Returns (manifest, error_msg)."""
    url = f"{base_url}/{slug}/manifest.json"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            return json.loads(data), None
    except urllib.error.HTTPError as exc:
        return None, f"HTTP {exc.code}"
    except urllib.error.URLError as exc:
        return None, f"URL error: {exc.reason}"
    except json.JSONDecodeError as exc:
        return None, f"JSON parse error: {exc}"


def check_tile_url(url: str, timeout: int = 30) -> tuple[bool, str]:
    """HEAD-request a tile URL. Returns (ok, status_or_error)."""
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status == 200, str(resp.status)
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except urllib.error.URLError as exc:
        return False, f"URL error: {exc.reason}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(base_url: str) -> bool:
    """Run spot-check for all SPOT_CHECK_VOLUMES. Returns True if all pass."""
    results = []

    for fond, volume in SPOT_CHECK_VOLUMES:
        slug = derive_volume_slug(fond, volume)

        if slug in KNOWN_SKIPS:
            log(f"SKIP {slug} (known skip)")
            results.append((slug, "SKIP", "SKIP", "SKIP"))
            continue

        log(f"Checking {slug} ...")

        # 1. Fetch manifest
        manifest, err = fetch_manifest(slug, base_url)
        if err:
            log(f"  FAIL manifest: {err}")
            results.append((slug, f"FAIL ({err})", "—", "—"))
            continue

        # 2. Validate structure
        if not validate_manifest_structure(manifest):
            log(f"  FAIL manifest structure invalid")
            results.append((slug, "FAIL (structure)", "—", "—"))
            continue

        manifest_status = "OK"
        items = manifest["items"]
        log(f"  manifest OK ({len(items)} canvases)")

        # 3. Extract tile URLs for first and last canvas
        first_url = extract_tile_url(items[0])
        last_url = extract_tile_url(items[-1])

        first_ok, first_status = check_tile_url(first_url)
        last_ok, last_status = check_tile_url(last_url)

        log(f"  first tile {first_status} — last tile {last_status}")
        results.append((
            slug,
            manifest_status,
            "OK" if first_ok else f"FAIL ({first_status})",
            "OK" if last_ok else f"FAIL ({last_status})",
        ))

    # Print summary table
    print()
    print("=" * 80)
    print(f"{'SLUG':<35} {'MANIFEST':<18} {'FIRST TILE':<14} LAST TILE")
    print("-" * 80)
    all_pass = True
    for slug, manifest_st, first_st, last_st in results:
        print(f"{slug:<35} {manifest_st:<18} {first_st:<14} {last_st}")
        if "FAIL" in (manifest_st, first_st, last_st):
            all_pass = False
    print("=" * 80)

    if all_pass:
        print("\nResult: PASS — all spot-check volumes OK")
    else:
        print("\nResult: FAIL — one or more checks failed")

    return all_pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Spot-check 13 AHRB volumes across iiif.zasqua.org"
    )
    parser.add_argument(
        "--base-url",
        default=BASE_URL,
        help=f"Base IIIF URL (default: {BASE_URL})",
    )
    args = parser.parse_args()

    ok = run(args.base_url)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
