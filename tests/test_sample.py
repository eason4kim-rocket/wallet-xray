"""Tests for stratified sampling."""

from __future__ import annotations

from tests.test_metrics import _win
from wallet_xray.sample import stratified_sample


def test_stratified_sample_below_size():
    wins = [_win(ts=1774578600 + i * 300) for i in range(5)]
    s = stratified_sample(wins, size=100)
    assert len(s) == 5


def test_stratified_sample_trims_to_size():
    wins = [_win(ts=1774578600 + i * 300) for i in range(200)]
    s = stratified_sample(wins, size=50)
    assert len(s) <= 50


def test_stratified_sample_covers_strata():
    """Ensure both winners and losers are represented even when unbalanced."""
    winners = [
        _win(ts=1774578600 + i * 300, user_won=True, directional_win=True)
        for i in range(150)
    ]
    losers = [
        _win(
            ts=1774578600 + (150 + i) * 300,
            user_won=False,
            directional_win=False,
            pnl=-10.0,
            redeemed=0.0,
        )
        for i in range(10)
    ]
    wins = winners + losers
    s = stratified_sample(wins, size=50)
    n_winners = sum(1 for w in s if w["user_won"])
    n_losers = sum(1 for w in s if not w["user_won"])
    assert n_losers >= 1
    assert n_winners >= 1


def test_stratified_sample_keeps_recent():
    """Most recent windows should appear in the sample."""
    wins = [_win(ts=1774578600 + i * 300) for i in range(200)]
    s = stratified_sample(wins, size=50, keep_recent=5)
    sample_ts = {w["ts"] for w in s}
    # at least some of the top 10 most-recent should be in the sample
    recent_ts = {1774578600 + i * 300 for i in range(190, 200)}
    assert len(sample_ts & recent_ts) >= 3
