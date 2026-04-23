"""HTTP access layer for Polymarket public APIs.

All endpoints used here are public and keyless.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
LB_API = "https://lb-api.polymarket.com"

HEADERS = {
    "User-Agent": "wallet-xray/0.1 (+https://github.com/eason4kim-rocket/wallet-xray)"
}

_PAGE_SIZE = 500


def _http_get(url: str, timeout: int = 30, retries: int = 2) -> Any:
    """GET JSON with retries. Returns parsed JSON or raises the last error."""
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise
            last_err = e
        except Exception as e:  # noqa: BLE001
            last_err = e
        if attempt < retries:
            time.sleep(0.5 * (attempt + 1))
    assert last_err is not None
    raise last_err


def fetch_activity(
    wallet: str,
    max_records: int | None = None,
    min_ts: int | None = None,
    progress: bool = True,
) -> list[dict]:
    """Fetch all activity rows for a wallet, paginating until the API returns empty.

    If ``max_records`` is None (default), fetches until exhaustion.
    If ``min_ts`` is provided, stops paging once a page's oldest row is already
    older than the cutoff — since data-api returns rows in descending timestamp
    order, any further pages would all be out of range. The current page is
    still included (it may contain some in-range rows that will be filtered at
    window-build time).
    """
    wallet = wallet.lower().strip()
    rows: list[dict] = []
    offset = 0
    while True:
        url = f"{DATA_API}/activity?user={wallet}&limit={_PAGE_SIZE}&offset={offset}"
        try:
            page = _http_get(url)
        except Exception as e:  # noqa: BLE001
            if progress:
                print(f"  fetch error at offset={offset}: {e}", file=sys.stderr)
            break
        if not isinstance(page, list) or not page:
            break
        rows.extend(page)
        if progress:
            print(
                f"  fetched offset={offset}: {len(page)} rows (total {len(rows)})",
                file=sys.stderr,
            )
        if len(page) < _PAGE_SIZE:
            break
        if max_records is not None and len(rows) >= max_records:
            rows = rows[:max_records]
            break
        if min_ts is not None:
            # page is timestamp-descending; if its oldest row is already older
            # than the cutoff, no future page can contain in-range rows.
            oldest_ts = page[-1].get("timestamp") if isinstance(page[-1], dict) else None
            if isinstance(oldest_ts, (int, float)) and oldest_ts < min_ts:
                if progress:
                    print(
                        f"  early-stop: page ends at ts={int(oldest_ts)} "
                        f"(before cutoff {min_ts}); no further pages needed",
                        file=sys.stderr,
                    )
                break
        offset += _PAGE_SIZE
    return rows


def fetch_market_by_slug(slug: str, timeout: int = 10) -> dict | None:
    """Fetch a market by slug from Gamma. Returns None if not found."""
    url = f"{GAMMA_API}/markets/slug/{urllib.parse.quote(slug)}"
    try:
        data = _http_get(url, timeout=timeout)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        return None
    except Exception:  # noqa: BLE001
        return None
    if isinstance(data, dict) and data.get("conditionId"):
        return data
    return None


def fetch_profit(wallet: str, window: str = "all") -> dict | None:
    """Fetch overall PnL from the leaderboard API for cross-check."""
    url = f"{LB_API}/profit?window={window}&address={wallet.lower().strip()}"
    try:
        data = _http_get(url)
    except Exception:  # noqa: BLE001
        return None
    if isinstance(data, list) and data:
        return data[0] if isinstance(data[0], dict) else None
    if isinstance(data, dict):
        return data
    return None
