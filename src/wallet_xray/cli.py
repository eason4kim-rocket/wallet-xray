"""Command-line entry point for wallet-xray."""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from wallet_xray import __version__
from wallet_xray.fetch import fetch_activity, fetch_profit
from wallet_xray.metrics import build_all_sections
from wallet_xray.report import render_markdown
from wallet_xray.sample import stratified_sample
from wallet_xray.windows import build_windows

_ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="wallet-xray",
        description=(
            "Polymarket wallet strategy profiler. Prints a Markdown report to stdout "
            "and saves a structured JSON for LLM analysis."
        ),
    )
    p.add_argument("wallet", nargs="?", help="Ethereum address (0x...)")
    p.add_argument(
        "--days",
        default="21",
        help=(
            "Only analyze windows from last N days (default 21, ~3 weeks, "
            "sweet spot for 200-500 resolved windows on typical wallets; "
            "'all' for full history)"
        ),
    )
    p.add_argument(
        "--symbols",
        default="",
        help="Comma-separated symbols filter (btc,eth,sol,xrp). Default: all",
    )
    p.add_argument(
        "--tfs",
        default="",
        help="Comma-separated timeframes filter (5m,15m,1h). Default: all",
    )
    p.add_argument(
        "--out-dir",
        default="reports",
        help="JSON output directory (default: ./reports/)",
    )
    p.add_argument(
        "--no-gamma",
        action="store_true",
        help="Skip Gamma fallback (rely only on REDEEM inference)",
    )
    p.add_argument(
        "--sample-size",
        type=int,
        default=100,
        help="Stratified sample size for per_window_sample (default 100)",
    )
    p.add_argument("--no-save", action="store_true", help="Skip JSON file write")
    p.add_argument("--quiet", action="store_true", help="Suppress stderr progress")
    p.add_argument("--version", action="version", version=f"wallet-xray {__version__}")
    return p.parse_args(argv)


def _prompt_wallet() -> str:
    try:
        print("请粘贴钱包地址并回车（Paste wallet address and press Enter）：", file=sys.stderr, end="")
        sys.stderr.flush()
        addr = input().strip()
    except (EOFError, KeyboardInterrupt):
        print("\nAborted.", file=sys.stderr)
        sys.exit(1)
    return addr


def _validate_wallet(addr: str) -> str:
    addr = addr.strip().lower()
    if not _ADDR_RE.match(addr):
        print(f"Error: '{addr}' is not a valid 0x Ethereum address.", file=sys.stderr)
        sys.exit(2)
    return addr


def _short_addr(addr: str) -> str:
    return addr[:6] + addr[-4:]


def _parse_list(s: str) -> list[str] | None:
    s = (s or "").strip()
    if not s:
        return None
    return [x.strip().lower() for x in s.split(",") if x.strip()]


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    wallet = args.wallet or _prompt_wallet()
    wallet = _validate_wallet(wallet)

    progress = not args.quiet

    # ── time range ───────────────────────────────────────────────
    if args.days.lower() == "all":
        min_ws: int | None = None
        days_label = "all"
    else:
        try:
            days = int(args.days)
        except ValueError:
            print(f"Error: --days must be an integer or 'all' (got '{args.days}')", file=sys.stderr)
            return 2
        min_ws = int(time.time()) - days * 86400
        days_label = f"last {days}d"

    symbols = _parse_list(args.symbols)
    tfs = _parse_list(args.tfs)

    filters = {
        "days": days_label,
        "symbols": symbols or "all",
        "tfs": tfs or "all",
    }

    if progress:
        print(
            f"wallet-xray {__version__}  wallet={wallet}  filters={filters}",
            file=sys.stderr,
        )

    # ── fetch ────────────────────────────────────────────────────
    if progress:
        print("[1/3] Fetching activity ...", file=sys.stderr)
    # Pass min_ws so fetch can early-stop once a page goes out of range.
    # A small buffer (2h) protects against unsettled windows that may still
    # produce REDEEMs slightly after window_end.
    fetch_min_ts = (min_ws - 7200) if min_ws is not None else None
    rows = fetch_activity(wallet, min_ts=fetch_min_ts, progress=progress)
    if progress:
        print(f"  total activity rows: {len(rows)}", file=sys.stderr)

    if progress:
        print("[2/3] Building windows ...", file=sys.stderr)
    windows, skipped = build_windows(
        rows,
        symbols=symbols,
        tfs=tfs,
        min_window_start=min_ws,
        allow_gamma=not args.no_gamma,
        progress=progress,
    )
    if progress:
        print(
            f"  resolved {len(windows)} windows; "
            f"unresolved={skipped.get('unresolved', 0)}, "
            f"gamma_calls={skipped.get('gamma_calls', 0)}",
            file=sys.stderr,
        )
        # Actionable hint on window count
        n = len(windows)
        if n < 100:
            print(
                f"  💡 only {n} windows resolved — sample thin. Consider --days "
                f"{max(int(args.days) * 3 if args.days.isdigit() else 90, 60)} "
                "for a larger sample.",
                file=sys.stderr,
            )
        elif n > 1000:
            print(
                f"  💡 {n} windows is a lot — strategy may have drifted. "
                "Consider --days 7 to focus on the most recent behavior.",
                file=sys.stderr,
            )

    # ── cross-check via lb-api ──────────────────────────────────
    lb_profit = None
    try:
        p = fetch_profit(wallet, "all")
        if p and "amount" in p:
            lb_profit = float(p["amount"])
        elif p and "profit" in p:
            lb_profit = float(p["profit"])
    except Exception:  # noqa: BLE001
        lb_profit = None

    # ── build sections ───────────────────────────────────────────
    if progress:
        print("[3/3] Computing sections and rendering ...", file=sys.stderr)
    sections = build_all_sections(windows)
    sample = stratified_sample(windows, args.sample_size)

    report = {
        "meta": {
            "wallet": wallet,
            "generated_at": _iso_now(),
            "tool_version": __version__,
            "filters": filters,
            "activity_rows": len(rows),
            "resolved_windows": len(windows),
            "gamma_calls": skipped.get("gamma_calls", 0),
            "skipped_unresolved": skipped.get("unresolved", 0),
            "skipped_no_buys": skipped.get("no_buys", 0),
            "skipped_filtered_symbol": skipped.get("filtered_symbol", 0),
            "skipped_filtered_tf": skipped.get("filtered_tf", 0),
            "skipped_filtered_time": skipped.get("filtered_time", 0),
            "lb_api_profit": lb_profit,
        },
        **sections,
        "per_window_sample": sample,
    }

    # ── save JSON ────────────────────────────────────────────────
    json_path = None
    if not args.no_save:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        json_path = out_dir / f"{_short_addr(wallet)}_{stamp}.json"
        with open(json_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        if progress:
            print(f"✅ JSON saved to {json_path}", file=sys.stderr)

    # ── print Markdown to stdout ────────────────────────────────
    print(render_markdown(report))
    if json_path:
        print(f"\n---\n✅ JSON 已保存（已包含全部 12 个 section）：`{json_path}`")
        print(
            "把这个 JSON 内容贴给 ChatGPT/Claude，并附上 README 里的 AI prompt 模板，即可获得策略解读。"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
