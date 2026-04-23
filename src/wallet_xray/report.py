"""Markdown renderer for the full report.

Renders the same 12 sections as the JSON file, in a human-friendly Markdown layout.
Printed to stdout by cli.py.
"""

from __future__ import annotations

import time
from typing import Any


def _fmt_pct(x: float | None) -> str:
    if x is None:
        return "-"
    return f"{x * 100:.1f}%"


def _fmt_money(x: float | None) -> str:
    if x is None:
        return "-"
    if x >= 0:
        return f"${x:,.2f}"
    return f"-${-x:,.2f}"


def _fmt_roi(x: float | None) -> str:
    if x is None:
        return "-"
    sign = "+" if x >= 0 else ""
    return f"{sign}{x * 100:.1f}%"


def _table(headers: list[str], rows: list[list[Any]]) -> str:
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join("---" for _ in headers) + "|")
    for r in rows:
        lines.append("| " + " | ".join(str(c) for c in r) + " |")
    return "\n".join(lines)


# ── Section renderers ───────────────────────────────────────────────


def render_meta(meta: dict) -> str:
    rows = [
        ["Wallet", f"`{meta['wallet']}`"],
        ["Generated", meta["generated_at"]],
        ["Filters", meta.get("filters", "-")],
        ["Activity rows fetched", meta.get("activity_rows", 0)],
        ["Resolved windows", meta.get("resolved_windows", 0)],
        ["Gamma fallback calls", meta.get("gamma_calls", 0)],
        ["Skipped (unresolved)", meta.get("skipped_unresolved", 0)],
    ]
    lb = meta.get("lb_api_profit")
    if lb is not None:
        rows.append(["lb-api total PnL (cross-check)", _fmt_money(lb)])
    body = _table(["Field", "Value"], rows)
    return f"## meta\n\n{body}\n"


def render_overview(ov: dict) -> str:
    if not ov or ov.get("windows", 0) == 0:
        return "## overview\n\n*No resolved windows.*\n"
    dr = ov.get("date_range") or {}
    rows = [
        ["Total resolved windows", ov["windows"]],
        ["Total trades", ov.get("total_trades", "-")],
        ["Avg trades/window", ov.get("avg_trades_per_window", "-")],
        ["Active days", ov.get("active_days", "-")],
        ["Date range (UTC)", f"{dr.get('start', '-')} → {dr.get('end', '-')}"],
        ["Total invested", _fmt_money(ov["invested"])],
        ["Total redeemed", _fmt_money(ov["redeemed"])],
        ["Net PnL", _fmt_money(ov["pnl"])],
        ["ROI", _fmt_roi(ov["roi"])],
        ["Win rate (directional)", _fmt_pct(ov["win_rate_directional"])],
        ["Win rate (PnL > 0)", _fmt_pct(ov["win_rate_pnl"])],
    ]
    return "## overview\n\n" + _table(["Metric", "Value"], rows) + "\n"


def render_agg_table(
    title: str,
    items: list[dict],
    key_field: str,
    key_label: str,
) -> str:
    if not items:
        return f"## {title}\n\n*No data.*\n"
    headers = [
        key_label,
        "Windows",
        "WinR (dir)",
        "WinR ($$)",
        "Invested",
        "PnL",
        "ROI",
    ]
    rows = []
    for it in items:
        rows.append(
            [
                it[key_field],
                it["windows"],
                _fmt_pct(it["win_rate_directional"]),
                _fmt_pct(it["win_rate_pnl"]),
                _fmt_money(it["invested"]),
                _fmt_money(it["pnl"]),
                _fmt_roi(it["roi"]),
            ]
        )
    return f"## {title}\n\n" + _table(headers, rows) + "\n"


def render_multi_trade_behavior(mt: dict) -> str:
    if not mt or not mt.get("n_trades_distribution"):
        return "## multi_trade_behavior\n\n*No data.*\n"
    dist = mt["n_trades_distribution"]
    span = mt.get("span_seconds", {})
    dist_rows = [[str(k), v] for k, v in sorted(dist.items(), key=lambda x: int(x[0]))]
    body = "### Trades per window\n\n" + _table(["n_trades", "window_count"], dist_rows)
    rows = [
        ["Single-trade windows", mt.get("single_trade_windows", 0)],
        ["Multi-trade windows", mt.get("multi_trade_windows", 0)],
        ["Avg trades/window", mt.get("avg_n_trades", 0)],
        ["Span min (s)", span.get("min", 0)],
        ["Span p25 (s)", span.get("p25", 0)],
        ["Span median (s)", span.get("median", 0)],
        ["Span p75 (s)", span.get("p75", 0)],
        ["Span max (s)", span.get("max", 0)],
    ]
    body += "\n\n### Summary\n\n" + _table(["Metric", "Value"], rows)
    return "## multi_trade_behavior\n\n" + body + "\n"


def render_two_sided_behavior(ts: dict) -> str:
    if not ts or ts.get("total_windows", 0) == 0:
        return "## two_sided_behavior\n\n*No data.*\n"
    rows = [
        ["Total windows", ts["total_windows"]],
        [
            "Two-sided count (pct)",
            f"{ts['two_sided_count']} ({_fmt_pct(ts['two_sided_pct'])})",
        ],
        ["One-sided count", ts["one_sided_count"]],
    ]
    body = _table(["Metric", "Value"], rows)
    if ts.get("two_sided", {}).get("windows"):
        body += "\n\n### Two-sided aggregate\n\n" + _table(
            ["Metric", "Value"],
            [
                ["Windows", ts["two_sided"]["windows"]],
                ["WinR directional", _fmt_pct(ts["two_sided"]["win_rate_directional"])],
                ["WinR PnL>0", _fmt_pct(ts["two_sided"]["win_rate_pnl"])],
                ["Invested", _fmt_money(ts["two_sided"]["invested"])],
                ["PnL", _fmt_money(ts["two_sided"]["pnl"])],
                ["ROI", _fmt_roi(ts["two_sided"]["roi"])],
            ],
        )
    if ts.get("one_sided", {}).get("windows"):
        body += "\n\n### One-sided aggregate\n\n" + _table(
            ["Metric", "Value"],
            [
                ["Windows", ts["one_sided"]["windows"]],
                ["WinR directional", _fmt_pct(ts["one_sided"]["win_rate_directional"])],
                ["WinR PnL>0", _fmt_pct(ts["one_sided"]["win_rate_pnl"])],
                ["Invested", _fmt_money(ts["one_sided"]["invested"])],
                ["PnL", _fmt_money(ts["one_sided"]["pnl"])],
                ["ROI", _fmt_roi(ts["one_sided"]["roi"])],
            ],
        )
    return "## two_sided_behavior\n\n" + body + "\n"


def render_direction_bias(db: dict) -> str:
    if not db:
        return "## direction_bias\n\n*No data.*\n"
    rows = [
        ["User primary Up windows", db.get("user_up_windows", 0)],
        ["User primary Down windows", db.get("user_down_windows", 0)],
        ["User Both-sided windows", db.get("user_both_windows", 0)],
        ["Market actual Up winners", db.get("market_up_winners", 0)],
        ["Market actual Down winners", db.get("market_down_winners", 0)],
        ["Market Up win rate (objective)", _fmt_pct(db.get("market_up_win_rate", 0))],
    ]
    body = _table(["Metric", "Value"], rows)
    for label, k in [("Up primary", "up_agg"), ("Down primary", "down_agg"), ("Both-sided", "both_agg")]:
        agg = db.get(k) or {}
        if agg.get("windows", 0) == 0:
            continue
        body += f"\n\n### {label}\n\n" + _table(
            ["Metric", "Value"],
            [
                ["Windows", agg["windows"]],
                ["WinR directional", _fmt_pct(agg["win_rate_directional"])],
                ["WinR PnL>0", _fmt_pct(agg["win_rate_pnl"])],
                ["Invested", _fmt_money(agg["invested"])],
                ["PnL", _fmt_money(agg["pnl"])],
                ["ROI", _fmt_roi(agg["roi"])],
            ],
        )
    return "## direction_bias\n\n" + body + "\n"


def render_per_window_sample(sample: list[dict], max_show: int = 30) -> str:
    """Render up to `max_show` most-recent sample windows in a compact table."""
    if not sample:
        return "## per_window_sample\n\n*No windows.*\n"
    shown = sorted(sample, key=lambda w: w["ts"], reverse=True)[:max_show]
    headers = [
        "UTC",
        "Mkt",
        "Winner",
        "User→",
        "DirW",
        "$Win",
        "2-sided",
        "1st$",
        "T+",
        "n",
        "Invested",
        "PnL",
    ]
    rows = []
    for w in shown:
        ts_str = time.strftime("%m-%d %H:%M", time.gmtime(w["ts"]))
        rows.append(
            [
                ts_str,
                f"{w['symbol'].upper()}-{w['tf']}",
                w["winner_side"],
                w["user_primary_direction"],
                "✓" if w["directional_win"] else "✗",
                "✓" if w["user_won"] else "✗",
                "Y" if w["two_sided"] else "-",
                f"${w['first_price']:.2f}",
                f"{w['first_offset_sec']}s",
                w["n_trades"],
                _fmt_money(w["invested"]),
                _fmt_money(w["pnl"]),
            ]
        )
    note = (
        f"*Showing {len(shown)} most-recent of {len(sample)} sampled windows. "
        f"Full stratified sample is in the JSON file.*"
    )
    return "## per_window_sample\n\n" + _table(headers, rows) + f"\n\n{note}\n"


# ── Master renderer ─────────────────────────────────────────────────


def render_markdown(report: dict) -> str:
    parts = []
    meta = report.get("meta", {})
    wallet_short = meta.get("wallet", "")[:10] + "..." if meta.get("wallet") else "?"
    parts.append(f"# Polymarket 钱包画像：{wallet_short}")
    parts.append(f"*Generated {meta.get('generated_at', '-')} by wallet-xray*\n")
    parts.append(render_meta(meta))
    parts.append(render_overview(report.get("overview", {})))
    parts.append(
        render_agg_table(
            "by_symbol_tf",
            report.get("by_symbol_tf", []),
            "market",
            "Market",
        )
    )
    parts.append(
        render_agg_table(
            "by_date",
            report.get("by_date", []),
            "date",
            "Date (UTC)",
        )
    )
    parts.append(
        render_agg_table(
            "by_hour_utc",
            report.get("by_hour_utc", []),
            "hour",
            "Hour (UTC)",
        )
    )
    parts.append(
        render_agg_table(
            "position_sizing",
            report.get("position_sizing", []),
            "bucket",
            "Size bucket",
        )
    )
    parts.append(
        render_agg_table(
            "first_price_distribution",
            report.get("first_price_distribution", []),
            "bucket",
            "Price bucket",
        )
    )
    parts.append(
        render_agg_table(
            "entry_timing",
            report.get("entry_timing", []),
            "bucket",
            "Offset bucket",
        )
    )
    parts.append(render_multi_trade_behavior(report.get("multi_trade_behavior", {})))
    parts.append(render_two_sided_behavior(report.get("two_sided_behavior", {})))
    parts.append(render_direction_bias(report.get("direction_bias", {})))
    parts.append(render_per_window_sample(report.get("per_window_sample", [])))
    return "\n".join(parts)
