"""Unit tests for windows.py — focused on winner inference, per-window PnL,
and the three 'win' concepts.
"""

from __future__ import annotations

import time

from wallet_xray.windows import (
    compute_window,
    group_by_slug,
    infer_winner_from_redeems,
    parse_slug,
)

# ── parse_slug ─────────────────────────────────────────────────────


def test_parse_slug_btc_5m():
    assert parse_slug("btc-updown-5m-1774578600") == ("btc", "5m", 1774578600)


def test_parse_slug_eth_15m():
    assert parse_slug("eth-updown-15m-1774574100") == ("eth", "15m", 1774574100)


def test_parse_slug_sol_1h():
    assert parse_slug("sol-updown-1h-1774500000") == ("sol", "1h", 1774500000)


def test_parse_slug_rejects_non_crypto_updown():
    assert parse_slug("russia-ukraine-ceasefire-554") is None
    assert parse_slug("") is None


# ── infer_winner_from_redeems ──────────────────────────────────────


def _buy(outcome, size, price, ts=1774578650):
    return {
        "type": "TRADE",
        "side": "BUY",
        "outcome": outcome,
        "size": size,
        "price": price,
        "usdcSize": round(size * price, 4),
        "timestamp": ts,
    }


def _redeem(size, ts=1774579000):
    return {"type": "REDEEM", "size": size, "usdcSize": size, "timestamp": ts}


def test_winner_one_sided_won():
    # bought Up 10 shares, redeemed 10 → Up won
    buys = [_buy("Up", 10, 0.5)]
    redeems = [_redeem(10)]
    assert infer_winner_from_redeems(buys, redeems) == "Up"


def test_winner_one_sided_lost():
    # bought Up 10 shares, redeem 0 → Down won
    buys = [_buy("Up", 10, 0.5)]
    redeems = [_redeem(0)]
    assert infer_winner_from_redeems(buys, redeems) == "Down"


def test_winner_two_sided_up_wins():
    # bought Up 10 + Down 5, sum_redeem ≈ 10 → Up won
    buys = [_buy("Up", 10, 0.5), _buy("Down", 5, 0.3)]
    redeems = [_redeem(10), _redeem(0)]
    assert infer_winner_from_redeems(buys, redeems) == "Up"


def test_winner_two_sided_down_wins():
    buys = [_buy("Up", 10, 0.5), _buy("Down", 5, 0.3)]
    redeems = [_redeem(0), _redeem(5)]
    assert infer_winner_from_redeems(buys, redeems) == "Down"


def test_winner_no_redeems_returns_none():
    buys = [_buy("Up", 10, 0.5)]
    assert infer_winner_from_redeems(buys, []) is None


# ── compute_window ─────────────────────────────────────────────────


def test_compute_window_one_sided_won():
    slug = "btc-updown-5m-1774578600"  # Mar 27 2026 02:30 UTC (historic)
    rows = [
        _buy("Up", 40, 0.5, ts=1774578650),
        _redeem(40, ts=1774579100),
    ]
    w = compute_window(slug, rows, allow_gamma=False)
    assert w is not None
    assert w["symbol"] == "btc"
    assert w["tf"] == "5m"
    assert w["ts"] == 1774578600
    assert w["winner_side"] == "Up"
    assert w["user_primary_direction"] == "Up"
    assert w["directional_win"] is True
    assert w["user_won"] is True
    assert w["two_sided"] is False
    assert w["up_shares"] == 40
    assert w["down_shares"] == 0
    assert w["invested"] == 20.0  # 40 * 0.5
    assert w["redeemed"] == 40.0  # winner shares * $1
    assert w["pnl"] == 20.0
    assert w["first_offset_sec"] == 50  # 1774578650 - 1774578600
    assert w["n_trades"] == 1


def test_compute_window_one_sided_lost():
    slug = "btc-updown-5m-1774578600"
    rows = [
        _buy("Up", 40, 0.5, ts=1774578650),
        _redeem(0, ts=1774579100),
    ]
    w = compute_window(slug, rows, allow_gamma=False)
    assert w is not None
    assert w["winner_side"] == "Down"
    assert w["user_primary_direction"] == "Up"
    assert w["directional_win"] is False
    assert w["user_won"] is False
    assert w["pnl"] == -20.0


def test_compute_window_two_sided_hedge_directional_right_but_pnl_wrong():
    """Classic hedge-cost scenario: bet mostly on Up at high price, Up wins,
    but small Down hedge + high entry cost means PnL is still negative."""
    slug = "btc-updown-5m-1774578600"
    rows = [
        _buy("Up", 100, 0.95, ts=1774578610),  # $95
        _buy("Down", 50, 0.20, ts=1774578620),  # $10
        _redeem(100, ts=1774579100),
        _redeem(0, ts=1774579101),
    ]
    w = compute_window(slug, rows, allow_gamma=False)
    assert w is not None
    assert w["winner_side"] == "Up"
    assert w["user_primary_direction"] == "Up"
    assert w["directional_win"] is True
    assert w["two_sided"] is True
    assert w["invested"] == 105.0
    assert w["redeemed"] == 100.0
    assert w["pnl"] == -5.0
    assert w["user_won"] is False  # money lost despite correct direction


def test_compute_window_two_sided_successful_hedge():
    """Directional wrong but hedge saves the day with net positive PnL."""
    slug = "btc-updown-5m-1774578600"
    rows = [
        _buy("Up", 10, 0.5, ts=1774578610),  # $5
        _buy("Down", 20, 0.15, ts=1774578620),  # $3
        _redeem(0, ts=1774579100),
        _redeem(20, ts=1774579101),
    ]
    w = compute_window(slug, rows, allow_gamma=False)
    assert w is not None
    assert w["winner_side"] == "Down"
    assert w["user_primary_direction"] == "Up"  # Up spent $5 > Down $3
    assert w["directional_win"] is False
    assert w["two_sided"] is True
    assert w["invested"] == 8.0
    assert w["redeemed"] == 20.0
    assert w["pnl"] == 12.0
    assert w["user_won"] is True  # hedge won despite wrong primary direction


def test_compute_window_unresolved_returns_none():
    """A window that has no redeems and gamma disabled should return None."""
    future_ts = int(time.time()) - 1000  # settled
    slug = f"btc-updown-5m-{future_ts}"
    rows = [_buy("Up", 10, 0.5, ts=future_ts + 10)]
    # no redeems, no gamma allowed → unresolved
    w = compute_window(slug, rows, allow_gamma=False)
    assert w is None


def test_compute_window_skips_unsettled():
    """A window that is still active (window_end + 60s > now) must be skipped."""
    now = int(time.time())
    ws = now  # window just started
    slug = f"btc-updown-5m-{ws}"
    rows = [_buy("Up", 10, 0.5, ts=ws + 10), _redeem(10, ts=ws + 120)]
    w = compute_window(slug, rows, allow_gamma=False)
    assert w is None


# ── group_by_slug ──────────────────────────────────────────────────


def test_group_by_slug_filters_non_updown():
    rows = [
        {"slug": "btc-updown-5m-1774578600", "type": "TRADE"},
        {"slug": "russia-ukraine-ceasefire", "type": "TRADE"},
        {"slug": "eth-updown-15m-1774574100", "type": "TRADE"},
    ]
    by = group_by_slug(rows)
    assert set(by.keys()) == {
        "btc-updown-5m-1774578600",
        "eth-updown-15m-1774574100",
    }
