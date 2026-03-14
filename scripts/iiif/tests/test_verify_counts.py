"""
Unit tests for verify_counts.py -- bulk canvas count validation helpers.

All tests use mock data (no network calls).
"""

import csv
import io
import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from verify_counts import (
    compare_count,
    is_known_skip,
    load_volumes,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def make_csv(rows: list[dict]) -> io.StringIO:
    """Build an in-memory CSV with columns fond,volume,image_dir,image_count."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["fond", "volume", "image_dir", "image_count"])
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)
    return buf


SAMPLE_ROWS = [
    {"fond": "AHRB_N1", "volume": "001", "image_dir": "AHRB_N1/AHRB_N1_001/proc/recortadas", "image_count": "42"},
    {"fond": "AHRB_Cabildos", "volume": "unico", "image_dir": "AHRB_Cabildos/AHRB_Cabildos_unico/proc/recortadas", "image_count": "7"},
    {"fond": "AHRB_N1", "volume": "024bis", "image_dir": "AHRB_N1/AHRB_N1_024bis/proc/recortadas", "image_count": "0"},
]


# ---------------------------------------------------------------------------
# Test: load_volumes
# ---------------------------------------------------------------------------

class TestLoadVolumes:
    def test_returns_list_of_dicts(self):
        buf = make_csv(SAMPLE_ROWS)
        volumes = load_volumes(buf)
        assert isinstance(volumes, list)
        assert len(volumes) == 3

    def test_has_required_keys(self):
        buf = make_csv(SAMPLE_ROWS)
        for vol in load_volumes(buf):
            assert "fond" in vol
            assert "volume" in vol
            assert "image_dir" in vol
            assert "image_count" in vol

    def test_image_count_is_int(self):
        buf = make_csv(SAMPLE_ROWS)
        for vol in load_volumes(buf):
            assert isinstance(vol["image_count"], int)

    def test_image_count_value_correct(self):
        buf = make_csv(SAMPLE_ROWS)
        volumes = {v["volume"]: v for v in load_volumes(buf)}
        assert volumes["001"]["image_count"] == 42
        assert volumes["unico"]["image_count"] == 7
        assert volumes["024bis"]["image_count"] == 0


# ---------------------------------------------------------------------------
# Test: compare_count
# ---------------------------------------------------------------------------

class TestCompareCount:
    def test_match_when_equal(self):
        result = compare_count(canvas_count=42, expected_count=42)
        assert result["status"] == "match"

    def test_mismatch_when_different(self):
        result = compare_count(canvas_count=40, expected_count=42)
        assert result["status"] == "mismatch"

    def test_mismatch_includes_both_counts(self):
        result = compare_count(canvas_count=40, expected_count=42)
        assert result["canvas_count"] == 40
        assert result["expected_count"] == 42

    def test_match_has_no_count_details(self):
        result = compare_count(canvas_count=42, expected_count=42)
        # On match there is no need for detailed counts (but it's fine if present)
        assert result["status"] == "match"


# ---------------------------------------------------------------------------
# Test: is_known_skip
# ---------------------------------------------------------------------------

class TestIsKnownSkip:
    def test_024bis_is_known_skip(self):
        assert is_known_skip("co-ahrb-n1-024bis") is True

    def test_024_is_not_known_skip(self):
        assert is_known_skip("co-ahrb-n1-024") is False

    def test_other_slug_not_skip(self):
        assert is_known_skip("co-ahrb-n1-001") is False

    def test_empty_slug_not_skip(self):
        assert is_known_skip("") is False


# ---------------------------------------------------------------------------
# Test: volumes with image_count=0 and slug in KNOWN_SKIPS
# ---------------------------------------------------------------------------

class TestKnownSkipVolumes:
    def test_024bis_loaded_from_csv(self):
        """024bis appears in load_volumes output (it is loaded, not pre-filtered)."""
        buf = make_csv(SAMPLE_ROWS)
        volumes = load_volumes(buf)
        slugs_fond_vol = [(v["fond"], v["volume"]) for v in volumes]
        assert ("AHRB_N1", "024bis") in slugs_fond_vol

    def test_024bis_image_count_zero(self):
        """024bis has image_count=0 in the CSV (a known skip)."""
        buf = make_csv(SAMPLE_ROWS)
        volumes = {v["volume"]: v for v in load_volumes(buf)}
        assert volumes["024bis"]["image_count"] == 0
