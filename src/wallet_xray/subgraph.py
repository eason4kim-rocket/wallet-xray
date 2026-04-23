"""Fetch trade activity via the Polymarket Goldsky orderbook subgraph.

This module exists to bypass the hard 3500-row cap on the Polymarket data-api
``/activity`` endpoint, which makes data-api unusable for whale wallets whose
daily activity alone exceeds 3500 events.

Flow:
    1. Paginate ``orderFilledEvents`` for a wallet (as both maker and taker).
    2. Collect all unique non-USDC token IDs appearing in those events.
    3. Resolve token_id -> conditionId via the subgraph's ``marketData``.
    4. Resolve conditionId -> (slug, outcomes, clobTokenIds) via gamma-api.
    5. Translate subgraph events into data-api-shaped activity rows that
       the rest of the pipeline (``windows.py`` etc.) expects.

Known limitations:
    - Subgraph does not index REDEEM events. Winner inference for windows
      will therefore rely on the existing gamma fallback path. This is
      slower than REDEEM inference but correct.
    - Subgraph pagination caps each page at 1000 rows. We cursor with
      ``timestamp_lt`` for unbounded history.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

SUBGRAPH_URL = (
    "https://api.goldsky.com/api/public/"
    "project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"
)
GAMMA_API = "https://gamma-api.polymarket.com"

USDC_ASSET_ID = "0"
_PAGE_SIZE = 1000
_HEADERS = {
    "User-Agent": "wallet-xray/0.1 (+https://github.com/eason4kim-rocket/wallet-xray)",
    "Content-Type": "application/json",
}


# ── HTTP helpers ────────────────────────────────────────────────────────────


def _post(payload: dict, timeout: int = 30, retries: int = 2) -> dict:
    """POST JSON to the subgraph with retries. Raises on final failure."""
    data = json.dumps(payload).encode()
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                SUBGRAPH_URL, data=data, headers=_HEADERS, method="POST"
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except Exception as e:  # noqa: BLE001
            last_err = e
        if attempt < retries:
            time.sleep(0.5 * (attempt + 1))
    assert last_err is not None
    raise last_err


def _get(url: str, timeout: int = 10, retries: int = 2) -> object:
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": _HEADERS["User-Agent"]}
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            # 404 is a real result — no market under that conditionId.
            if e.code == 404:
                return []
            last_err = e
        except Exception as e:  # noqa: BLE001
            last_err = e
        if attempt < retries:
            time.sleep(0.5 * (attempt + 1))
    assert last_err is not None
    raise last_err


# ── Public API ──────────────────────────────────────────────────────────────


def fetch_trades_subgraph(
    wallet: str,
    min_ts: int | None = None,
    progress: bool = True,
) -> list[dict]:
    """Paginate ``orderFilledEvents`` for wallet as both maker and taker.

    Returns a de-duplicated list of raw subgraph events (not data-api rows).
    Call :func:`translate_to_activity_rows` to convert them to the shape
    windows.py expects.
    """
    wallet = wallet.lower().strip()
    all_events: list[dict] = []

    for role in ("taker", "maker"):
        cursor = 9_999_999_999  # effectively +infinity
        page = 0
        while True:
            where = f'{role}: "{wallet}", timestamp_lt: {cursor}'
            if min_ts is not None:
                where += f", timestamp_gte: {int(min_ts)}"
            query = (
                "{ orderFilledEvents("
                f"first: {_PAGE_SIZE}, "
                f"where: {{{where}}}, "
                "orderBy: timestamp, orderDirection: desc"
                ") { id timestamp transactionHash maker taker "
                "makerAssetId takerAssetId makerAmountFilled takerAmountFilled fee } }"
            )
            try:
                resp = _post({"query": query})
            except Exception as e:  # noqa: BLE001
                if progress:
                    print(
                        f"  subgraph [{role}] error at cursor={cursor}: {e}",
                        file=sys.stderr,
                    )
                break
            events = (resp.get("data") or {}).get("orderFilledEvents") or []
            if not events:
                break
            all_events.extend(events)
            page += 1
            if progress:
                print(
                    f"  subgraph [{role}] page {page}: +{len(events)} "
                    f"(total {len(all_events)})",
                    file=sys.stderr,
                )
            if len(events) < _PAGE_SIZE:
                break
            cursor = int(events[-1]["timestamp"])

    # Dedupe by event id (maker & taker queries may overlap if wallet self-trades)
    seen: set[str] = set()
    uniq: list[dict] = []
    for e in all_events:
        eid = e.get("id")
        if not eid or eid in seen:
            continue
        seen.add(eid)
        uniq.append(e)
    if progress and len(uniq) != len(all_events):
        print(
            f"  subgraph: deduped {len(all_events)} → {len(uniq)} unique events",
            file=sys.stderr,
        )
    return uniq


def resolve_token_to_condition(
    token_ids: list[str], progress: bool = True
) -> dict[str, str]:
    """Map token_ids to their conditionIds via subgraph ``marketData``.

    Returns ``{token_id: condition_id}``. Tokens with no corresponding
    ``marketData`` record are omitted from the result.
    """
    tids = [t for t in token_ids if t and t != USDC_ASSET_ID]
    if not tids:
        return {}
    out: dict[str, str] = {}
    chunk_size = 100
    for i in range(0, len(tids), chunk_size):
        chunk = tids[i : i + chunk_size]
        ids_frag = ", ".join(f'"{t}"' for t in chunk)
        query = (
            f"{{ marketDatas(first: {len(chunk)}, where: {{id_in: [{ids_frag}]}}) "
            "{ id condition } }"
        )
        try:
            resp = _post({"query": query})
        except Exception as e:  # noqa: BLE001
            if progress:
                print(f"  marketData chunk {i}: {e}", file=sys.stderr)
            continue
        for md in (resp.get("data") or {}).get("marketDatas") or []:
            tid = md.get("id")
            cid = md.get("condition")
            if tid and cid:
                out[tid] = cid
    if progress:
        print(
            f"  resolved {len(out)}/{len(tids)} tokens → conditions",
            file=sys.stderr,
        )
    return out


def resolve_condition_to_market(
    condition_ids: list[str], progress: bool = True
) -> dict[str, dict]:
    """Map conditionIds to market info via gamma-api.

    Returns ``{condition_id: {"slug": str, "outcomes": list[str],
    "token_ids": list[str], "outcome_prices": list[str]}}``.

    The ``outcome_prices`` field is useful for pre-resolving winners:
    after settlement, the winning outcome's price is 1.0 and loser's is 0.0.
    """
    cids = sorted({c for c in condition_ids if c})
    if not cids:
        return {}
    out: dict[str, dict] = {}
    chunk_size = 20  # gamma URL length + param repetition safety
    for i in range(0, len(cids), chunk_size):
        chunk = cids[i : i + chunk_size]
        params: list[tuple[str, str]] = [("closed", "true"), ("limit", "100")]
        for c in chunk:
            params.append(("condition_ids", c))
        url = f"{GAMMA_API}/markets?{urllib.parse.urlencode(params)}"
        try:
            mkts = _get(url)
        except Exception as e:  # noqa: BLE001
            if progress:
                print(f"  gamma chunk {i}: {e}", file=sys.stderr)
            continue
        if not isinstance(mkts, list):
            continue
        for m in mkts:
            cid = m.get("conditionId")
            if not cid:
                continue
            outcomes = m.get("outcomes") or []
            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                except Exception:  # noqa: BLE001
                    outcomes = []
            token_ids = m.get("clobTokenIds") or []
            if isinstance(token_ids, str):
                try:
                    token_ids = json.loads(token_ids)
                except Exception:  # noqa: BLE001
                    token_ids = []
            outcome_prices = m.get("outcomePrices") or []
            if isinstance(outcome_prices, str):
                try:
                    outcome_prices = json.loads(outcome_prices)
                except Exception:  # noqa: BLE001
                    outcome_prices = []
            out[cid] = {
                "slug": m.get("slug"),
                "outcomes": list(outcomes),
                "token_ids": list(token_ids),
                "outcome_prices": list(outcome_prices),
            }
    if progress:
        print(
            f"  resolved {len(out)}/{len(cids)} conditions → slugs",
            file=sys.stderr,
        )
    return out


def build_winner_cache(cid_info: dict[str, dict]) -> dict[str, str]:
    """Pre-compute ``{slug: winner_side}`` from resolved condition info.

    The gamma ``outcomePrices`` field is 1.0 for the winning outcome and 0.0
    for the loser after settlement. This lets us avoid a live gamma call
    inside ``build_windows`` for every subgraph-sourced window.
    """
    cache: dict[str, str] = {}
    for info in cid_info.values():
        slug = info.get("slug")
        outcomes = info.get("outcomes") or []
        prices = info.get("outcome_prices") or []
        if not slug or len(outcomes) != len(prices):
            continue
        for outcome, raw in zip(outcomes, prices):
            try:
                p = float(raw)
            except (TypeError, ValueError):
                continue
            if p >= 0.999:
                cache[slug] = outcome
                break
    return cache


def translate_to_activity_rows(
    events: list[dict],
    wallet: str,
    progress: bool = True,
) -> tuple[list[dict], dict[str, str]]:
    """Convert subgraph ``orderFilledEvents`` into data-api-shaped activity rows.

    Returns ``(rows, winner_cache)``. The winner cache maps
    ``{slug: "Up"|"Down"}`` for every settled market seen during translation
    and should be passed into :func:`windows.build_windows` to avoid a live
    gamma call per window.

    Only TRADE-type rows are produced. The subgraph does not index REDEEM
    events, so the winner cache (derived from gamma ``outcomePrices``) is the
    primary winner-resolution mechanism for subgraph-sourced data.
    """
    wallet = wallet.lower().strip()

    # Unique token IDs in the event set (excluding USDC)
    token_ids: set[str] = set()
    for e in events:
        for field in ("makerAssetId", "takerAssetId"):
            tid = e.get(field)
            if tid and tid != USDC_ASSET_ID:
                token_ids.add(tid)

    if progress:
        print(
            f"  translating {len(events)} events, "
            f"resolving {len(token_ids)} unique tokens",
            file=sys.stderr,
        )

    tid_to_cid = resolve_token_to_condition(sorted(token_ids), progress=progress)
    cid_info = resolve_condition_to_market(
        sorted(set(tid_to_cid.values())), progress=progress
    )

    # Build token_id -> {slug, outcome}
    tid_map: dict[str, dict] = {}
    for tid, cid in tid_to_cid.items():
        info = cid_info.get(cid)
        if not info:
            continue
        tids_list = info.get("token_ids", [])
        outcomes = info.get("outcomes", [])
        if tid in tids_list:
            idx = tids_list.index(tid)
            tid_map[tid] = {
                "slug": info.get("slug"),
                "outcome": outcomes[idx] if idx < len(outcomes) else None,
            }

    rows: list[dict] = []
    dropped = 0
    for e in events:
        mkr_asset = e.get("makerAssetId")
        tkr_asset = e.get("takerAssetId")
        maker = (e.get("maker") or "").lower()
        taker = (e.get("taker") or "").lower()

        is_wallet_maker = maker == wallet
        is_wallet_taker = taker == wallet
        if not (is_wallet_maker or is_wallet_taker):
            dropped += 1
            continue

        # Only handle USDC<->token trades (the normal case for outcome trading)
        if mkr_asset == USDC_ASSET_ID and tkr_asset not in (None, "", USDC_ASSET_ID):
            # Maker posts USDC -> acquires token; taker brings token
            token_id = tkr_asset
            usdc_amt = int(e.get("makerAmountFilled") or 0) / 1e6
            token_amt = int(e.get("takerAmountFilled") or 0) / 1e6
            side = "BUY" if is_wallet_maker else "SELL"
        elif tkr_asset == USDC_ASSET_ID and mkr_asset not in (None, "", USDC_ASSET_ID):
            # Maker posts token -> acquires USDC; taker brings USDC
            token_id = mkr_asset
            token_amt = int(e.get("makerAmountFilled") or 0) / 1e6
            usdc_amt = int(e.get("takerAmountFilled") or 0) / 1e6
            side = "SELL" if is_wallet_maker else "BUY"
        else:
            dropped += 1
            continue

        info = tid_map.get(token_id)
        if not info or not info.get("slug"):
            dropped += 1
            continue

        price = (usdc_amt / token_amt) if token_amt else 0.0

        rows.append(
            {
                "timestamp": int(e["timestamp"]),
                "type": "TRADE",
                "side": side,
                "outcome": info.get("outcome"),
                "size": token_amt,
                "usdcSize": usdc_amt,
                "price": price,
                "slug": info.get("slug"),
                "conditionId": tid_to_cid.get(token_id),
                "txHash": e.get("transactionHash"),
            }
        )

    rows.sort(key=lambda r: r["timestamp"], reverse=True)
    winner_cache = build_winner_cache(cid_info)

    if progress:
        print(
            f"  translated: {len(rows)} rows ({dropped} events dropped: "
            "unresolved slug or non-USDC trade)",
            file=sys.stderr,
        )
        print(
            f"  winner_cache: {len(winner_cache)} slugs pre-resolved "
            "(no per-window gamma calls needed)",
            file=sys.stderr,
        )
    return rows, winner_cache
