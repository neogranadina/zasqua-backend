"""
Unit tests for verify_spot_check.py -- spot-check validation helpers.

All tests use mock data (no network calls).
"""

import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from verify_spot_check import (
    KNOWN_SKIPS,
    SPOT_CHECK_VOLUMES,
    derive_volume_slug,
    extract_tile_url,
    validate_manifest_structure,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

VALID_CANVAS = {
    "type": "Canvas",
    "body": {
        "service": [
            {"id": "https://iiif.zasqua.org/co-ahrb-n1-001/page-0001"}
        ]
    },
}

VALID_MANIFEST = {
    "@context": "http://iiif.io/api/presentation/3/context.json",
    "type": "Manifest",
    "items": [
        VALID_CANVAS,
        {
            "type": "Canvas",
            "body": {
                "service": [
                    {"id": "https://iiif.zasqua.org/co-ahrb-n1-001/page-0099"}
                ]
            },
        },
    ],
}


# ---------------------------------------------------------------------------
# Test: derive_volume_slug
# ---------------------------------------------------------------------------

class TestDeriveVolumeSlug:
    def test_cabildos_unico(self):
        assert derive_volume_slug("AHRB_Cabildos", "unico") == "co-ahrb-cabildos-unico"

    def test_n1_numeric(self):
        assert derive_volume_slug("AHRB_N1", "096") == "co-ahrb-n1-096"

    def test_aht_numeric(self):
        assert derive_volume_slug("AHRB_AHT", "008") == "co-ahrb-aht-008"

    def test_n2_numeric(self):
        assert derive_volume_slug("AHRB_N2", "001") == "co-ahrb-n2-001"

    def test_nvl_numeric(self):
        assert derive_volume_slug("AHRB_NVL", "003") == "co-ahrb-nvl-003"


# ---------------------------------------------------------------------------
# Test: validate_manifest_structure
# ---------------------------------------------------------------------------

class TestValidateManifestStructure:
    def test_valid_manifest_returns_true(self):
        assert validate_manifest_structure(VALID_MANIFEST) is True

    def test_missing_context_returns_false(self):
        bad = {k: v for k, v in VALID_MANIFEST.items() if k != "@context"}
        assert validate_manifest_structure(bad) is False

    def test_wrong_context_returns_false(self):
        bad = {**VALID_MANIFEST, "@context": "http://iiif.io/api/presentation/2/context.json"}
        assert validate_manifest_structure(bad) is False

    def test_wrong_type_returns_false(self):
        bad = {**VALID_MANIFEST, "type": "Collection"}
        assert validate_manifest_structure(bad) is False

    def test_empty_items_returns_false(self):
        bad = {**VALID_MANIFEST, "items": []}
        assert validate_manifest_structure(bad) is False

    def test_missing_items_returns_false(self):
        bad = {k: v for k, v in VALID_MANIFEST.items() if k != "items"}
        assert validate_manifest_structure(bad) is False

    def test_first_item_not_canvas_returns_false(self):
        bad_canvas = {**VALID_CANVAS, "type": "AnnotationPage"}
        bad = {**VALID_MANIFEST, "items": [bad_canvas]}
        assert validate_manifest_structure(bad) is False


# ---------------------------------------------------------------------------
# Test: extract_tile_url
# ---------------------------------------------------------------------------

class TestExtractTileUrl:
    def test_returns_full_max_url(self):
        url = extract_tile_url(VALID_CANVAS)
        assert url == "https://iiif.zasqua.org/co-ahrb-n1-001/page-0001/full/max/0/default.jpg"

    def test_first_and_last_canvas_differ(self):
        first_url = extract_tile_url(VALID_MANIFEST["items"][0])
        last_url = extract_tile_url(VALID_MANIFEST["items"][-1])
        assert first_url != last_url

    def test_url_suffix_format(self):
        url = extract_tile_url(VALID_CANVAS)
        assert url.endswith("/full/max/0/default.jpg")


# ---------------------------------------------------------------------------
# Test: SPOT_CHECK_VOLUMES list
# ---------------------------------------------------------------------------

class TestSpotCheckVolumes:
    def test_exactly_13_entries(self):
        assert len(SPOT_CHECK_VOLUMES) == 13

    def test_all_5_fonds_represented(self):
        fonds = {fond for fond, _ in SPOT_CHECK_VOLUMES}
        assert "AHRB_AHT" in fonds
        assert "AHRB_Cabildos" in fonds
        assert "AHRB_N1" in fonds
        assert "AHRB_N2" in fonds
        assert "AHRB_NVL" in fonds

    def test_cabildos_unico_included(self):
        assert ("AHRB_Cabildos", "unico") in SPOT_CHECK_VOLUMES

    def test_n1_split_boundary_included(self):
        # Volumes 096 and 097 straddle the N1 droplet split
        volumes_by_fond = {f: v for f, v in SPOT_CHECK_VOLUMES}
        n1_volumes = [v for f, v in SPOT_CHECK_VOLUMES if f == "AHRB_N1"]
        assert "096" in n1_volumes
        assert "097" in n1_volumes


# ---------------------------------------------------------------------------
# Test: KNOWN_SKIPS
# ---------------------------------------------------------------------------

class TestKnownSkips:
    def test_024bis_in_known_skips(self):
        assert "co-ahrb-n1-024bis" in KNOWN_SKIPS
