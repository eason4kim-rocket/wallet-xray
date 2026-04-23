"""Tests for fetch.py — focused on the early-stop paging behavior."""

from __future__ import annotations

from unittest.mock import patch

from wallet_xray import fetch as fetch_mod


def _rows(start_ts: int, n: int, step: int = -60) -> list[dict]:
    """Generate n rows, newest first (ts descending by default)."""
    return [{"timestamp": start_ts + i * step, "type": "TRADE"} for i in range(n)]


def test_fetch_stops_when_page_smaller_than_limit():
    """Terminating condition: last page returns fewer than _PAGE_SIZE rows."""
    pages = [_rows(1_000_000, 500), _rows(970_000, 123)]
    with patch.object(fetch_mod, "_http_get", side_effect=pages):
        rows = fetch_mod.fetch_activity("0xabc", progress=False)
    assert len(rows) == 623
    assert rows[0]["timestamp"] == 1_000_000
    assert rows[-1]["timestamp"] == 970_000 + 122 * -60


def test_fetch_stops_on_empty_page():
    """Explicit empty page also terminates the loop."""
    pages = [_rows(1_000_000, 500), []]
    with patch.object(fetch_mod, "_http_get", side_effect=pages):
        rows = fetch_mod.fetch_activity("0xabc", progress=False)
    assert len(rows) == 500


def test_fetch_stops_on_max_records():
    pages = [_rows(1_000_000, 500), _rows(970_000, 500), _rows(940_000, 500)]
    with patch.object(fetch_mod, "_http_get", side_effect=pages):
        rows = fetch_mod.fetch_activity("0xabc", max_records=750, progress=False)
    assert len(rows) == 750


def test_fetch_early_stop_when_page_oldest_before_cutoff():
    """Should break after the first page whose oldest row < min_ts,
    avoiding further paging."""
    # page 1: 500 rows, newest=1_000_000, oldest=1_000_000 + 499*-60 = 970_060
    # page 2: 500 rows, newest=970_000, oldest=940_060
    # page 3 would be even older, but we should never hit it.
    pages = [_rows(1_000_000, 500), _rows(970_000, 500), _rows(940_000, 500)]
    # cutoff = 975_000 — page 1's oldest (970_060) is below cutoff, so we stop
    # after including page 1 and NOT paging.
    with patch.object(fetch_mod, "_http_get", side_effect=pages):
        rows = fetch_mod.fetch_activity("0xabc", min_ts=975_000, progress=False)
    # we got the full first page (some of its rows may actually be < cutoff;
    # that's filtered later at window-build time)
    assert len(rows) == 500
    assert rows[0]["timestamp"] == 1_000_000


def test_fetch_early_stop_continues_when_page_still_in_range():
    """Should NOT stop when oldest row in a page is still newer than the cutoff."""
    pages = [_rows(1_000_000, 500), _rows(970_000, 500), _rows(940_000, 123)]
    # cutoff = 900_000 — every page we see is in range, should fetch all 3
    with patch.object(fetch_mod, "_http_get", side_effect=pages):
        rows = fetch_mod.fetch_activity("0xabc", min_ts=900_000, progress=False)
    assert len(rows) == 500 + 500 + 123


def test_fetch_early_stop_on_second_page():
    """Page 1 is all in range, page 2's oldest falls below cutoff: stop after page 2."""
    pages = [
        _rows(1_000_000, 500),  # oldest=970_060 still in range if cutoff=960_000
        _rows(970_000, 500),    # oldest=940_060 below cutoff=960_000 → stop
        _rows(940_000, 500),    # must NOT be fetched
    ]
    with patch.object(fetch_mod, "_http_get", side_effect=pages) as mock_get:
        rows = fetch_mod.fetch_activity("0xabc", min_ts=960_000, progress=False)
    assert len(rows) == 1000
    assert mock_get.call_count == 2  # critical: no 3rd call


def test_fetch_no_min_ts_behaves_as_before():
    """Omitting min_ts = old behavior: page until exhausted."""
    pages = [_rows(1_000_000, 500), _rows(970_000, 500), _rows(940_000, 100)]
    with patch.object(fetch_mod, "_http_get", side_effect=pages) as mock_get:
        rows = fetch_mod.fetch_activity("0xabc", progress=False)
    assert len(rows) == 1100
    assert mock_get.call_count == 3
