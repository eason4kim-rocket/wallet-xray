"""Tests for the subgraph translator.

The fetch side is not unit-tested (it's a thin HTTP wrapper with retries).
The translator is the interesting piece: it has to map raw on-chain
orderFilledEvents to data-api-shaped activity rows, which is surprisingly
tricky because BUY/SELL + token outcome depend on which side is USDC and
whether the wallet is maker or taker.
"""

from __future__ import annotations

from unittest.mock import patch

from wallet_xray import subgraph as sg

WALLET = "0xabcdef0000000000000000000000000000000001"
COUNTERPARTY = "0xabcdef0000000000000000000000000000000002"

# A known Up/Down token pair for an imaginary BTC 5m window.
UP_TOKEN = "11111111111111111111111111111111111111111111111111111111111111111111"
DOWN_TOKEN = "22222222222222222222222222222222222222222222222222222222222222222222"
COND_ID = "0xdeadbeefcafe0000000000000000000000000000000000000000000000000001"
SLUG = "btc-updown-5m-1770000000"


def _mock_resolve_returns():
    """Returns the three mocks the translator needs, ready to patch in."""
    def token_to_condition(tids, progress=True):
        return {t: COND_ID for t in tids if t in (UP_TOKEN, DOWN_TOKEN)}

    def condition_to_market(cids, progress=True):
        if COND_ID in cids:
            return {
                COND_ID: {
                    "slug": SLUG,
                    "outcomes": ["Up", "Down"],
                    "token_ids": [UP_TOKEN, DOWN_TOKEN],
                    "outcome_prices": ["1", "0"],
                }
            }
        return {}

    return token_to_condition, condition_to_market


def test_translate_wallet_as_maker_buying_up():
    """Wallet posts USDC and wants UP token. makerAssetId=0, takerAssetId=UP.

    Interpretation: wallet is BUYing the UP outcome.
    """
    events = [
        {
            "id": "ev1",
            "timestamp": "1770000042",
            "transactionHash": "0xabc",
            "maker": WALLET,
            "taker": COUNTERPARTY,
            "makerAssetId": "0",
            "takerAssetId": UP_TOKEN,
            "makerAmountFilled": "5000000",   # $5 USDC
            "takerAmountFilled": "10000000",  # 10 Up shares
            "fee": "0",
        }
    ]
    t2c, c2m = _mock_resolve_returns()
    with (
        patch.object(sg, "resolve_token_to_condition", t2c),
        patch.object(sg, "resolve_condition_to_market", c2m),
    ):
        rows, _ = sg.translate_to_activity_rows(events, WALLET, progress=False)
    assert len(rows) == 1
    row = rows[0]
    assert row["type"] == "TRADE"
    assert row["side"] == "BUY"
    assert row["outcome"] == "Up"
    assert row["slug"] == SLUG
    assert row["size"] == 10.0
    assert row["usdcSize"] == 5.0
    assert row["price"] == 0.5
    assert row["timestamp"] == 1770000042


def test_translate_wallet_as_taker_buying_down():
    """Wallet is taker; maker offered DOWN tokens for USDC.

    Flow: maker posts DOWN tokens, wants USDC → maker is SELLING DOWN.
    Taker fills by providing USDC → taker is BUYING DOWN.
    """
    events = [
        {
            "id": "ev2",
            "timestamp": "1770000100",
            "transactionHash": "0xdef",
            "maker": COUNTERPARTY,
            "taker": WALLET,
            "makerAssetId": DOWN_TOKEN,
            "takerAssetId": "0",
            "makerAmountFilled": "20000000",  # 20 Down shares
            "takerAmountFilled": "8000000",   # $8 USDC
            "fee": "0",
        }
    ]
    t2c, c2m = _mock_resolve_returns()
    with (
        patch.object(sg, "resolve_token_to_condition", t2c),
        patch.object(sg, "resolve_condition_to_market", c2m),
    ):
        rows, _ = sg.translate_to_activity_rows(events, WALLET, progress=False)
    assert len(rows) == 1
    row = rows[0]
    assert row["side"] == "BUY"
    assert row["outcome"] == "Down"
    assert row["size"] == 20.0
    assert row["usdcSize"] == 8.0
    assert row["price"] == 0.4


def test_translate_wallet_as_maker_selling_up():
    """Wallet posts UP tokens wanting USDC → wallet is SELLing UP."""
    events = [
        {
            "id": "ev3",
            "timestamp": "1770000200",
            "transactionHash": "0x",
            "maker": WALLET,
            "taker": COUNTERPARTY,
            "makerAssetId": UP_TOKEN,
            "takerAssetId": "0",
            "makerAmountFilled": "10000000",  # 10 Up shares
            "takerAmountFilled": "6000000",   # $6 USDC received
            "fee": "0",
        }
    ]
    t2c, c2m = _mock_resolve_returns()
    with (
        patch.object(sg, "resolve_token_to_condition", t2c),
        patch.object(sg, "resolve_condition_to_market", c2m),
    ):
        rows, _ = sg.translate_to_activity_rows(events, WALLET, progress=False)
    assert rows[0]["side"] == "SELL"
    assert rows[0]["outcome"] == "Up"
    assert rows[0]["price"] == 0.6


def test_translate_drops_unresolved_slugs():
    """Events whose token can't be resolved to a slug are dropped, not counted."""
    events = [
        {
            "id": "ev4",
            "timestamp": "1770000300",
            "transactionHash": "0x",
            "maker": WALLET,
            "taker": COUNTERPARTY,
            "makerAssetId": "0",
            "takerAssetId": "99999999999999999999",  # unknown token
            "makerAmountFilled": "5000000",
            "takerAmountFilled": "10000000",
            "fee": "0",
        }
    ]
    with (
        patch.object(sg, "resolve_token_to_condition", lambda tids, progress=True: {}),
        patch.object(sg, "resolve_condition_to_market", lambda cids, progress=True: {}),
    ):
        rows, _ = sg.translate_to_activity_rows(events, WALLET, progress=False)
    assert rows == []


def test_translate_drops_events_not_involving_wallet():
    """Events where wallet is neither maker nor taker are silently dropped
    (defensive — shouldn't happen if fetch is correct)."""
    events = [
        {
            "id": "ev5",
            "timestamp": "1770000400",
            "transactionHash": "0x",
            "maker": "0xstranger0000000000000000000000000000000000",
            "taker": COUNTERPARTY,
            "makerAssetId": "0",
            "takerAssetId": UP_TOKEN,
            "makerAmountFilled": "5000000",
            "takerAmountFilled": "10000000",
            "fee": "0",
        }
    ]
    t2c, c2m = _mock_resolve_returns()
    with (
        patch.object(sg, "resolve_token_to_condition", t2c),
        patch.object(sg, "resolve_condition_to_market", c2m),
    ):
        rows, _ = sg.translate_to_activity_rows(events, WALLET, progress=False)
    assert rows == []


def test_translate_drops_token_to_token_trades():
    """Only USDC <-> token trades are normalized. Skip degenerate cases."""
    events = [
        {
            "id": "ev6",
            "timestamp": "1770000500",
            "transactionHash": "0x",
            "maker": WALLET,
            "taker": COUNTERPARTY,
            "makerAssetId": UP_TOKEN,
            "takerAssetId": DOWN_TOKEN,
            "makerAmountFilled": "10000000",
            "takerAmountFilled": "10000000",
            "fee": "0",
        }
    ]
    t2c, c2m = _mock_resolve_returns()
    with (
        patch.object(sg, "resolve_token_to_condition", t2c),
        patch.object(sg, "resolve_condition_to_market", c2m),
    ):
        rows, _ = sg.translate_to_activity_rows(events, WALLET, progress=False)
    assert rows == []


def test_translate_sorts_newest_first():
    """Output rows are timestamp-descending to match data-api convention."""
    t2c, c2m = _mock_resolve_returns()
    events = []
    for i, ts in enumerate([1770000100, 1770000500, 1770000300]):
        events.append(
            {
                "id": f"ev{i}",
                "timestamp": str(ts),
                "transactionHash": "0x",
                "maker": WALLET,
                "taker": COUNTERPARTY,
                "makerAssetId": "0",
                "takerAssetId": UP_TOKEN,
                "makerAmountFilled": "5000000",
                "takerAmountFilled": "10000000",
                "fee": "0",
            }
        )
    with (
        patch.object(sg, "resolve_token_to_condition", t2c),
        patch.object(sg, "resolve_condition_to_market", c2m),
    ):
        rows, _ = sg.translate_to_activity_rows(events, WALLET, progress=False)
    assert [r["timestamp"] for r in rows] == [1770000500, 1770000300, 1770000100]


def test_fetch_trades_subgraph_dedupes_overlapping_roles():
    """When a wallet shows up as both maker and taker in the same event
    (self-trade edge case), the event id is kept only once."""
    shared = {
        "id": "shared-event",
        "timestamp": "1770000000",
        "transactionHash": "0x",
        "maker": WALLET,
        "taker": WALLET,
        "makerAssetId": "0",
        "takerAssetId": UP_TOKEN,
        "makerAmountFilled": "1",
        "takerAmountFilled": "1",
        "fee": "0",
    }
    # First call (taker role) returns [shared]; then empty to stop.
    # Second call (maker role) also returns [shared]; then empty.
    # fetch loops roles in the order ("taker", "maker").
    responses = [
        {"data": {"orderFilledEvents": [shared]}},
        {"data": {"orderFilledEvents": [shared]}},
    ]

    def fake_post(payload, timeout=30, retries=2):
        return responses.pop(0)

    with patch.object(sg, "_post", side_effect=fake_post):
        events = sg.fetch_trades_subgraph(WALLET, progress=False)
    assert len(events) == 1
    assert events[0]["id"] == "shared-event"


def test_build_winner_cache_up_wins():
    cid_info = {
        COND_ID: {
            "slug": SLUG,
            "outcomes": ["Up", "Down"],
            "token_ids": [UP_TOKEN, DOWN_TOKEN],
            "outcome_prices": ["1", "0"],
        }
    }
    cache = sg.build_winner_cache(cid_info)
    assert cache == {SLUG: "Up"}


def test_build_winner_cache_down_wins():
    cid_info = {
        COND_ID: {
            "slug": SLUG,
            "outcomes": ["Up", "Down"],
            "token_ids": [UP_TOKEN, DOWN_TOKEN],
            "outcome_prices": ["0.0", "1.0"],
        }
    }
    assert sg.build_winner_cache(cid_info) == {SLUG: "Down"}


def test_build_winner_cache_unresolved_market():
    """A not-yet-settled market (both prices < 1) should be skipped."""
    cid_info = {
        COND_ID: {
            "slug": SLUG,
            "outcomes": ["Up", "Down"],
            "token_ids": [UP_TOKEN, DOWN_TOKEN],
            "outcome_prices": ["0.6", "0.4"],
        }
    }
    assert sg.build_winner_cache(cid_info) == {}


def test_build_winner_cache_malformed_data_is_ignored():
    cid_info = {
        "c1": {"slug": "s1", "outcomes": ["Up", "Down"], "outcome_prices": []},
        "c2": {"slug": None, "outcomes": ["Up", "Down"], "outcome_prices": ["1", "0"]},
        "c3": {"slug": "s3", "outcomes": ["Up", "Down"], "outcome_prices": ["nope", "0"]},
    }
    cache = sg.build_winner_cache(cid_info)
    assert cache == {}


def test_translate_returns_tuple_with_winner_cache():
    """Translator must now return (rows, cache). Verify the cache reflects gamma prices."""
    events = [
        {
            "id": "ev7",
            "timestamp": "1770000000",
            "transactionHash": "0x",
            "maker": WALLET,
            "taker": COUNTERPARTY,
            "makerAssetId": "0",
            "takerAssetId": UP_TOKEN,
            "makerAmountFilled": "5000000",
            "takerAmountFilled": "10000000",
            "fee": "0",
        }
    ]
    t2c, c2m = _mock_resolve_returns()
    with (
        patch.object(sg, "resolve_token_to_condition", t2c),
        patch.object(sg, "resolve_condition_to_market", c2m),
    ):
        rows, cache = sg.translate_to_activity_rows(events, WALLET, progress=False)
    assert len(rows) == 1
    assert cache == {SLUG: "Up"}
