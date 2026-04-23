"""Window merging, winner inference, and per-window metric computation."""

from __future__ import annotations

import json
import re
import sys
import time
from collections import defaultdict

from wallet_xray.fetch import fetch_market_by_slug

SLUG_RE = re.compile(r"(btc|eth|sol|xrp)-updown-(5m|15m|1h)-(\d+)", re.IGNORECASE)

TIMEFRAME_SECONDS = {"5m": 300, "15m": 900, "1h": 3600}

# tolerance (shares) when matching REDEEM size to a user side's total shares
_MATCH_TOL = 0.01


def parse_slug(slug: str) -> tuple[str, str, int] | None:
    """Return (symbol_lower, timeframe, window_start_epoch) or None."""
    m = SLUG_RE.search(slug or "")
    if not m:
        return None
    return m.group(1).lower(), m.group(2).lower(), int(m.group(3))


def group_by_slug(rows: list[dict]) -> dict[str, list[dict]]:
    """Group activity rows by slug, keeping only rows whose slug matches a Crypto UpDown market."""
    by: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        slug = r.get("slug") or r.get("market") or ""
        if not parse_slug(slug):
            continue
        by[slug].append(r)
    return dict(by)


def _outcome_prices(market: dict) -> dict[str, float]:
    outs_raw = market.get("outcomes") or "[]"
    prices_raw = market.get("outcomePrices") or "[]"
    try:
        outs = outs_raw if isinstance(outs_raw, list) else json.loads(outs_raw)
        prices = prices_raw if isinstance(prices_raw, list) else json.loads(prices_raw)
    except Exception:  # noqa: BLE001
        return {}
    return {o: float(p) for o, p in zip(outs, prices)}


def resolve_winner_via_gamma(slug: str) -> str | None:
    """Call Gamma as fallback. Returns 'Up', 'Down', or None."""
    m = fetch_market_by_slug(slug)
    if not m or not m.get("closed"):
        return None
    prices = _outcome_prices(m)
    for o, p in prices.items():
        if p >= 0.999:
            return o
    return None


def infer_winner_from_redeems(
    buys: list[dict],
    redeems: list[dict],
) -> str | None:
    """Infer winner side from user's REDEEM rows.

    Logic:
    - Sum up user shares per outcome from BUYs
    - Sum up REDEEM size (USDC received ~ shares × $1 for 50/50 binary markets)
    - If sum_redeem == 0 → user held only losing side → winner = opposite of what user bought
    - If sum_redeem > 0:
        * If only one side was bought → that side won
        * Else (two-sided): the side whose shares match sum_redeem is the winner
    - If user has NO REDEEMs at all → return None (caller should try Gamma)
    """
    if not redeems:
        return None

    up_shares = 0.0
    dn_shares = 0.0
    for b in buys:
        if b.get("side") != "BUY":
            continue
        sz = float(b.get("size") or 0)
        oc = b.get("outcome") or ""
        if oc == "Up":
            up_shares += sz
        elif oc == "Down":
            dn_shares += sz

    sum_redeem = sum(float(r.get("size") or 0) for r in redeems)

    bought_up = up_shares > 0
    bought_dn = dn_shares > 0

    if sum_redeem <= _MATCH_TOL:
        # all redeems are 0 → user's held side(s) all lost → winner = opposite
        if bought_up and not bought_dn:
            return "Down"
        if bought_dn and not bought_up:
            return "Up"
        if bought_up and bought_dn:
            # both sides bought, both redeems 0 — this shouldn't happen in a binary market
            # (one side must win). Fall back to Gamma.
            return None
        return None

    # sum_redeem > 0 → user held the winning side
    if bought_up and not bought_dn:
        return "Up"
    if bought_dn and not bought_up:
        return "Down"
    if bought_up and bought_dn:
        # Match sum_redeem to whichever side's shares it's closer to.
        d_up = abs(sum_redeem - up_shares)
        d_dn = abs(sum_redeem - dn_shares)
        return "Up" if d_up <= d_dn else "Down"
    return None


def _vwap(trades: list[dict], outcome: str) -> float | None:
    num = 0.0
    denom = 0.0
    for t in trades:
        if t.get("side") != "BUY" or (t.get("outcome") or "") != outcome:
            continue
        sz = float(t.get("size") or 0)
        px = float(t.get("price") or 0)
        num += sz * px
        denom += sz
    if denom <= 0:
        return None
    return num / denom


def compute_window(
    slug: str,
    slug_rows: list[dict],
    *,
    allow_gamma: bool = True,
) -> dict | None:
    """Compute per-window metrics. Returns None if the window is unresolved or has no BUYs.

    Output dict shape (keys documented here, consumed by metrics.py / report.py):
        slug, symbol, tf, ts (window_start_epoch), date_utc, hour_utc,
        winner_side, user_primary_direction, directional_win, user_won, two_sided,
        up_shares, down_shares, up_spent, down_spent,
        invested, redeemed, pnl,
        vwap_up, vwap_down,
        first_price, first_outcome, first_offset_sec, last_offset_sec,
        n_trades, trades (list of {side, outcome, price, size, ts, offset_sec})
    """
    parsed = parse_slug(slug)
    if not parsed:
        return None
    symbol, tf, ws = parsed
    dur = TIMEFRAME_SECONDS.get(tf)
    if not dur:
        return None

    # Skip windows that haven't finished yet (+ small buffer for resolution lag)
    if int(time.time()) <= ws + dur + 60:
        return None

    buys = [r for r in slug_rows if r.get("type") == "TRADE" and r.get("side") == "BUY"]
    redeems = [r for r in slug_rows if r.get("type") == "REDEEM"]
    if not buys:
        return None

    # --- winner inference ---
    winner = infer_winner_from_redeems(buys, redeems)
    if winner is None and allow_gamma:
        winner = resolve_winner_via_gamma(slug)
    if winner is None:
        return None  # unresolved → skip

    # --- per-side aggregates ---
    up_shares = sum(
        float(t.get("size") or 0) for t in buys if (t.get("outcome") or "") == "Up"
    )
    dn_shares = sum(
        float(t.get("size") or 0) for t in buys if (t.get("outcome") or "") == "Down"
    )
    up_spent = sum(
        float(t.get("usdcSize") or 0) for t in buys if (t.get("outcome") or "") == "Up"
    )
    dn_spent = sum(
        float(t.get("usdcSize") or 0) for t in buys if (t.get("outcome") or "") == "Down"
    )
    invested = up_spent + dn_spent
    two_sided = up_shares > 0 and dn_shares > 0

    if winner == "Up":
        redeemed = up_shares
    else:
        redeemed = dn_shares
    pnl = redeemed - invested
    user_won = pnl > 0

    # user_primary_direction = side with larger $ spent (or "Both" if identical and two-sided)
    if two_sided and abs(up_spent - dn_spent) < 1e-6:
        primary = "Both"
    elif up_spent > dn_spent:
        primary = "Up"
    elif dn_spent > up_spent:
        primary = "Down"
    else:
        primary = "Up" if up_shares > 0 else "Down"
    directional_win = primary == winner  # "Both" never matches a single winner

    # --- timing ---
    trades_sorted = sorted(buys, key=lambda r: int(r.get("timestamp") or 0))
    first = trades_sorted[0]
    last = trades_sorted[-1]
    first_ts = int(first.get("timestamp") or ws)
    last_ts = int(last.get("timestamp") or ws)
    first_offset = first_ts - ws
    last_offset = last_ts - ws
    first_price = float(first.get("price") or 0)
    first_outcome = first.get("outcome") or ""

    # --- per-trade detail (trimmed) ---
    trades_detail = []
    for t in trades_sorted:
        ts = int(t.get("timestamp") or ws)
        trades_detail.append(
            {
                "side": t.get("side"),
                "outcome": t.get("outcome"),
                "price": round(float(t.get("price") or 0), 4),
                "size": round(float(t.get("size") or 0), 4),
                "usdc": round(float(t.get("usdcSize") or 0), 4),
                "ts": ts,
                "offset_sec": ts - ws,
            }
        )

    return {
        "slug": slug,
        "symbol": symbol,
        "tf": tf,
        "ts": ws,
        "date_utc": time.strftime("%Y-%m-%d", time.gmtime(ws)),
        "hour_utc": int(time.strftime("%H", time.gmtime(ws))),
        "winner_side": winner,
        "user_primary_direction": primary,
        "directional_win": directional_win,
        "user_won": user_won,
        "two_sided": two_sided,
        "up_shares": round(up_shares, 4),
        "down_shares": round(dn_shares, 4),
        "up_spent": round(up_spent, 4),
        "down_spent": round(dn_spent, 4),
        "invested": round(invested, 4),
        "redeemed": round(redeemed, 4),
        "pnl": round(pnl, 4),
        "vwap_up": round(_vwap(buys, "Up"), 4) if up_shares > 0 else None,
        "vwap_down": round(_vwap(buys, "Down"), 4) if dn_shares > 0 else None,
        "first_price": round(first_price, 4),
        "first_outcome": first_outcome,
        "first_offset_sec": first_offset,
        "last_offset_sec": last_offset,
        "n_trades": len(buys),
        "trades": trades_detail,
    }


def build_windows(
    rows: list[dict],
    *,
    symbols: list[str] | None = None,
    tfs: list[str] | None = None,
    min_window_start: int | None = None,
    allow_gamma: bool = True,
    progress: bool = True,
) -> tuple[list[dict], dict]:
    """Build all resolved windows from raw activity rows.

    Returns (windows, skipped_stats) where skipped_stats describes filtering/rejection counts.
    """
    by_slug = group_by_slug(rows)
    symbols_lc = [s.lower() for s in symbols] if symbols else None
    tfs_lc = [t.lower() for t in tfs] if tfs else None

    skipped = {
        "filtered_symbol": 0,
        "filtered_tf": 0,
        "filtered_time": 0,
        "no_buys": 0,
        "unresolved": 0,
        "total_candidate_slugs": len(by_slug),
    }

    windows: list[dict] = []
    gamma_calls = 0
    for i, (slug, slug_rows) in enumerate(by_slug.items(), 1):
        parsed = parse_slug(slug)
        if not parsed:
            continue
        sym, tf, ws = parsed
        if symbols_lc and sym not in symbols_lc:
            skipped["filtered_symbol"] += 1
            continue
        if tfs_lc and tf not in tfs_lc:
            skipped["filtered_tf"] += 1
            continue
        if min_window_start is not None and ws < min_window_start:
            skipped["filtered_time"] += 1
            continue

        w = compute_window(slug, slug_rows, allow_gamma=allow_gamma)
        if w is None:
            # differentiate: was it unresolved or had no buys?
            has_buys = any(
                r.get("type") == "TRADE" and r.get("side") == "BUY" for r in slug_rows
            )
            if not has_buys:
                skipped["no_buys"] += 1
            else:
                skipped["unresolved"] += 1
            continue

        # track gamma calls (approx): only a miss on REDEEM triggers gamma
        redeems = [r for r in slug_rows if r.get("type") == "REDEEM"]
        if not redeems:
            gamma_calls += 1

        windows.append(w)
        if progress and i % 200 == 0:
            print(
                f"  processed {i}/{len(by_slug)} slugs ({len(windows)} resolved, {gamma_calls} gamma calls)",
                file=sys.stderr,
            )

    skipped["gamma_calls"] = gamma_calls
    windows.sort(key=lambda w: w["ts"])
    return windows, skipped
