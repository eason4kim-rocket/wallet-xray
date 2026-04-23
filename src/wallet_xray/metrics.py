"""Compute the 12 report sections from a list of resolved per-window dicts."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

# ── Bucket definitions ───────────────────────────────────────────────

POSITION_BUCKETS: list[tuple[str, float, float]] = [
    ("<$5", 0.0, 5.0),
    ("$5-20", 5.0, 20.0),
    ("$20-100", 20.0, 100.0),
    ("$100-500", 100.0, 500.0),
    ("$500+", 500.0, float("inf")),
]

PRICE_BUCKETS: list[tuple[str, float, float]] = [
    ("0.00-0.10", 0.0, 0.10),
    ("0.10-0.30", 0.10, 0.30),
    ("0.30-0.50", 0.30, 0.50),
    ("0.50-0.70", 0.50, 0.70),
    ("0.70-0.90", 0.70, 0.90),
    ("0.90-1.00", 0.90, 1.01),
]

# Note: for 5m markets 300s = window end. Offsets > 300 are POST-CLOSE entries
# (result already known, adverse-selection/sweep territory). We surface this
# explicitly because it's a critical strategy signal.
OFFSET_BUCKETS_5M: list[tuple[str, int, int]] = [
    ("T+0-30s (early)", 0, 30),
    ("T+30-60s", 30, 60),
    ("T+60-120s", 60, 120),
    ("T+120-240s", 120, 240),
    ("T+240-300s (late)", 240, 300),
    ("T+300s+ (post-close)", 300, 10**9),
]


# ── Helpers ──────────────────────────────────────────────────────────


def _bucket_of(value: float, buckets: list[tuple[str, float, float]]) -> str:
    for name, lo, hi in buckets:
        if lo <= value < hi:
            return name
    return buckets[-1][0]


def _agg_base() -> dict:
    return {
        "windows": 0,
        "wins_directional": 0,
        "wins_pnl": 0,
        "invested": 0.0,
        "redeemed": 0.0,
        "pnl": 0.0,
    }


def _agg_add(agg: dict, w: dict) -> None:
    agg["windows"] += 1
    if w["directional_win"]:
        agg["wins_directional"] += 1
    if w["user_won"]:
        agg["wins_pnl"] += 1
    agg["invested"] += w["invested"]
    agg["redeemed"] += w["redeemed"]
    agg["pnl"] += w["pnl"]


def _agg_finalize(agg: dict) -> dict:
    n = agg["windows"]
    inv = agg["invested"]
    return {
        "windows": n,
        "wins_directional": agg["wins_directional"],
        "wins_pnl": agg["wins_pnl"],
        "win_rate_directional": round(agg["wins_directional"] / n, 4) if n else 0.0,
        "win_rate_pnl": round(agg["wins_pnl"] / n, 4) if n else 0.0,
        "invested": round(inv, 2),
        "redeemed": round(agg["redeemed"], 2),
        "pnl": round(agg["pnl"], 2),
        "roi": round(agg["pnl"] / inv, 4) if inv > 0 else 0.0,
    }


def _group_and_agg(windows: list[dict], key_fn) -> dict[Any, dict]:
    groups: dict[Any, dict] = defaultdict(_agg_base)
    for w in windows:
        _agg_add(groups[key_fn(w)], w)
    return {k: _agg_finalize(v) for k, v in groups.items()}


# ── Section builders ─────────────────────────────────────────────────


def section_overview(windows: list[dict]) -> dict:
    if not windows:
        return {
            "windows": 0,
            "win_rate_directional": 0.0,
            "win_rate_pnl": 0.0,
            "invested": 0.0,
            "redeemed": 0.0,
            "pnl": 0.0,
            "roi": 0.0,
            "avg_trades_per_window": 0.0,
            "active_days": 0,
            "date_range": None,
        }
    agg = _agg_base()
    total_trades = 0
    dates: set[str] = set()
    for w in windows:
        _agg_add(agg, w)
        total_trades += w["n_trades"]
        dates.add(w["date_utc"])
    out = _agg_finalize(agg)
    out["total_trades"] = total_trades
    out["avg_trades_per_window"] = round(total_trades / len(windows), 2)
    out["active_days"] = len(dates)
    out["date_range"] = {"start": min(dates), "end": max(dates)}
    return out


def section_by_symbol_tf(windows: list[dict]) -> list[dict]:
    grouped = _group_and_agg(windows, lambda w: (w["symbol"], w["tf"]))
    out = []
    for (sym, tf), agg in sorted(grouped.items()):
        out.append({"market": f"{sym.upper()}-{tf}", "symbol": sym, "tf": tf, **agg})
    return out


def section_by_date(windows: list[dict]) -> list[dict]:
    grouped = _group_and_agg(windows, lambda w: w["date_utc"])
    return [{"date": d, **agg} for d, agg in sorted(grouped.items())]


def section_by_hour_utc(windows: list[dict]) -> list[dict]:
    grouped = _group_and_agg(windows, lambda w: w["hour_utc"])
    return [{"hour": h, **agg} for h, agg in sorted(grouped.items())]


def section_position_sizing(windows: list[dict]) -> list[dict]:
    grouped = _group_and_agg(
        windows,
        lambda w: _bucket_of(w["invested"], POSITION_BUCKETS),
    )
    return [
        {"bucket": b[0], **grouped.get(b[0], _agg_finalize(_agg_base()))}
        for b in POSITION_BUCKETS
    ]


def section_first_price_distribution(windows: list[dict]) -> list[dict]:
    grouped = _group_and_agg(
        windows,
        lambda w: _bucket_of(w["first_price"], PRICE_BUCKETS),
    )
    return [
        {"bucket": b[0], **grouped.get(b[0], _agg_finalize(_agg_base()))}
        for b in PRICE_BUCKETS
    ]


def section_entry_timing(windows: list[dict]) -> list[dict]:
    # normalize offset to percentage of window duration, but also bucket as if 5m
    grouped = _group_and_agg(
        windows,
        lambda w: _bucket_of(w["first_offset_sec"], OFFSET_BUCKETS_5M),
    )
    return [
        {"bucket": b[0], **grouped.get(b[0], _agg_finalize(_agg_base()))}
        for b in OFFSET_BUCKETS_5M
    ]


def section_multi_trade_behavior(windows: list[dict]) -> dict:
    if not windows:
        return {"n_trades_distribution": {}, "span_seconds": {}}
    n_trades_counts: dict[int, int] = defaultdict(int)
    spans: list[int] = []
    for w in windows:
        n_trades_counts[w["n_trades"]] += 1
        spans.append(max(0, w["last_offset_sec"] - w["first_offset_sec"]))
    # summarize spans
    spans_sorted = sorted(spans)

    def pct(p: float) -> int:
        if not spans_sorted:
            return 0
        idx = min(len(spans_sorted) - 1, int(len(spans_sorted) * p))
        return spans_sorted[idx]

    return {
        "n_trades_distribution": dict(sorted(n_trades_counts.items())),
        "single_trade_windows": sum(1 for w in windows if w["n_trades"] == 1),
        "multi_trade_windows": sum(1 for w in windows if w["n_trades"] > 1),
        "avg_n_trades": round(sum(w["n_trades"] for w in windows) / len(windows), 2),
        "span_seconds": {
            "min": spans_sorted[0] if spans_sorted else 0,
            "p25": pct(0.25),
            "median": pct(0.5),
            "p75": pct(0.75),
            "max": spans_sorted[-1] if spans_sorted else 0,
        },
    }


def section_two_sided_behavior(windows: list[dict]) -> dict:
    if not windows:
        return {"total_windows": 0, "two_sided_count": 0, "two_sided_pct": 0.0}
    two = [w for w in windows if w["two_sided"]]
    one = [w for w in windows if not w["two_sided"]]
    two_agg = _agg_base()
    one_agg = _agg_base()
    for w in two:
        _agg_add(two_agg, w)
    for w in one:
        _agg_add(one_agg, w)
    return {
        "total_windows": len(windows),
        "two_sided_count": len(two),
        "one_sided_count": len(one),
        "two_sided_pct": round(len(two) / len(windows), 4),
        "two_sided": _agg_finalize(two_agg),
        "one_sided": _agg_finalize(one_agg),
    }


def section_direction_bias(windows: list[dict]) -> dict:
    up = [w for w in windows if w["user_primary_direction"] == "Up"]
    dn = [w for w in windows if w["user_primary_direction"] == "Down"]
    both = [w for w in windows if w["user_primary_direction"] == "Both"]
    up_agg = _agg_base()
    dn_agg = _agg_base()
    both_agg = _agg_base()
    for w in up:
        _agg_add(up_agg, w)
    for w in dn:
        _agg_add(dn_agg, w)
    for w in both:
        _agg_add(both_agg, w)
    market_up = sum(1 for w in windows if w["winner_side"] == "Up")
    market_dn = sum(1 for w in windows if w["winner_side"] == "Down")
    return {
        "user_up_windows": len(up),
        "user_down_windows": len(dn),
        "user_both_windows": len(both),
        "up_agg": _agg_finalize(up_agg),
        "down_agg": _agg_finalize(dn_agg),
        "both_agg": _agg_finalize(both_agg),
        "market_up_winners": market_up,
        "market_down_winners": market_dn,
        "market_up_win_rate": round(market_up / len(windows), 4) if windows else 0.0,
    }


# ── Master builder ──────────────────────────────────────────────────


def build_all_sections(windows: list[dict]) -> dict:
    """Build sections 2-11 (meta is set by cli; per_window_sample is built in sample.py)."""
    return {
        "overview": section_overview(windows),
        "by_symbol_tf": section_by_symbol_tf(windows),
        "by_date": section_by_date(windows),
        "by_hour_utc": section_by_hour_utc(windows),
        "position_sizing": section_position_sizing(windows),
        "first_price_distribution": section_first_price_distribution(windows),
        "entry_timing": section_entry_timing(windows),
        "multi_trade_behavior": section_multi_trade_behavior(windows),
        "two_sided_behavior": section_two_sided_behavior(windows),
        "direction_bias": section_direction_bias(windows),
    }
