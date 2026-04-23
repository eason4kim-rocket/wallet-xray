"""Tests for aggregation logic in metrics.py."""

from __future__ import annotations

from wallet_xray.metrics import (
    OFFSET_BUCKETS_5M,
    POSITION_BUCKETS,
    PRICE_BUCKETS,
    _bucket_of,
    build_all_sections,
    section_direction_bias,
    section_overview,
    section_two_sided_behavior,
)


def _win(
    *,
    ts=1774578600,
    symbol="btc",
    tf="5m",
    winner="Up",
    primary="Up",
    directional_win=True,
    user_won=True,
    two_sided=False,
    invested=20.0,
    redeemed=40.0,
    pnl=20.0,
    first_price=0.50,
    first_offset_sec=50,
    last_offset_sec=55,
    n_trades=1,
    up_shares=40.0,
    down_shares=0.0,
    up_spent=20.0,
    down_spent=0.0,
    vwap_up=0.50,
    vwap_down=None,
):
    return {
        "slug": f"{symbol}-updown-{tf}-{ts}",
        "symbol": symbol,
        "tf": tf,
        "ts": ts,
        "date_utc": "2026-03-27",
        "hour_utc": 2,
        "winner_side": winner,
        "user_primary_direction": primary,
        "directional_win": directional_win,
        "user_won": user_won,
        "two_sided": two_sided,
        "up_shares": up_shares,
        "down_shares": down_shares,
        "up_spent": up_spent,
        "down_spent": down_spent,
        "invested": invested,
        "redeemed": redeemed,
        "pnl": pnl,
        "vwap_up": vwap_up,
        "vwap_down": vwap_down,
        "first_price": first_price,
        "first_outcome": primary if primary != "Both" else "Up",
        "first_offset_sec": first_offset_sec,
        "last_offset_sec": last_offset_sec,
        "n_trades": n_trades,
        "trades": [],
    }


def test_bucket_of_position():
    assert _bucket_of(3.0, POSITION_BUCKETS) == "<$5"
    assert _bucket_of(5.0, POSITION_BUCKETS) == "$5-20"
    assert _bucket_of(20.0, POSITION_BUCKETS) == "$20-100"
    assert _bucket_of(1000.0, POSITION_BUCKETS) == "$500+"


def test_bucket_of_price():
    assert _bucket_of(0.05, PRICE_BUCKETS) == "0.00-0.10"
    assert _bucket_of(0.50, PRICE_BUCKETS) == "0.50-0.70"
    assert _bucket_of(0.99, PRICE_BUCKETS) == "0.90-1.00"


def test_bucket_of_offset():
    assert _bucket_of(10, OFFSET_BUCKETS_5M) == "T+0-30s (early)"
    assert _bucket_of(299, OFFSET_BUCKETS_5M) == "T+240-300s (late)"
    assert _bucket_of(500, OFFSET_BUCKETS_5M) == "T+300s+ (post-close)"


def test_overview_empty():
    ov = section_overview([])
    assert ov["windows"] == 0


def test_overview_basic():
    wins = [
        _win(user_won=True, directional_win=True, pnl=5.0, invested=10.0, redeemed=15.0),
        _win(user_won=False, directional_win=False, pnl=-10.0, invested=10.0, redeemed=0.0),
    ]
    ov = section_overview(wins)
    assert ov["windows"] == 2
    assert ov["wins_pnl"] == 1
    assert ov["wins_directional"] == 1
    assert ov["win_rate_pnl"] == 0.5
    assert ov["invested"] == 20.0
    assert ov["pnl"] == -5.0


def test_two_sided_behavior_splits():
    wins = [
        _win(two_sided=True, user_won=True, pnl=5.0),
        _win(two_sided=True, user_won=False, pnl=-3.0),
        _win(two_sided=False, user_won=True, pnl=10.0),
    ]
    ts = section_two_sided_behavior(wins)
    assert ts["total_windows"] == 3
    assert ts["two_sided_count"] == 2
    assert ts["one_sided_count"] == 1
    assert abs(ts["two_sided_pct"] - 0.6667) < 0.01
    assert ts["two_sided"]["windows"] == 2
    assert ts["one_sided"]["windows"] == 1


def test_direction_bias_winner_accounting():
    wins = [
        _win(primary="Up", winner="Up", directional_win=True),
        _win(primary="Down", winner="Up", directional_win=False),
        _win(primary="Down", winner="Down", directional_win=True),
    ]
    db = section_direction_bias(wins)
    assert db["user_up_windows"] == 1
    assert db["user_down_windows"] == 2
    assert db["market_up_winners"] == 2
    assert db["market_down_winners"] == 1
    assert abs(db["market_up_win_rate"] - 0.6667) < 0.01


def test_build_all_sections_has_all_keys():
    wins = [_win()]
    sections = build_all_sections(wins)
    required = {
        "overview",
        "by_symbol_tf",
        "by_date",
        "by_hour_utc",
        "position_sizing",
        "first_price_distribution",
        "entry_timing",
        "multi_trade_behavior",
        "two_sided_behavior",
        "direction_bias",
    }
    assert required.issubset(sections.keys())
