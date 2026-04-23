"""Stratified window sampling for AI consumption.

Goal: produce a fixed-size (~100) subset of per-window records that is representative
across (won × two_sided × price_bucket × offset_bucket), so an LLM sees the full
behavioral spectrum without the full 10k+ window dump.
"""

from __future__ import annotations

from collections import defaultdict

from wallet_xray.metrics import OFFSET_BUCKETS_5M, PRICE_BUCKETS, _bucket_of


def _strata_key(w: dict) -> tuple:
    return (
        bool(w["user_won"]),
        bool(w["two_sided"]),
        _bucket_of(w["first_price"], PRICE_BUCKETS),
        _bucket_of(w["first_offset_sec"], OFFSET_BUCKETS_5M),
    )


def stratified_sample(windows: list[dict], size: int, keep_recent: int = 10) -> list[dict]:
    """Return up to `size` windows chosen proportionally from each stratum.

    - Groups by (won, two_sided, price_bucket, offset_bucket)
    - Allocates each stratum at least 1 slot (if non-empty) and then proportionally
    - Always keeps the most-recent `keep_recent` windows regardless of stratum balance
    """
    if not windows:
        return []
    if len(windows) <= size:
        # sorted by ts asc already; return copy
        return list(windows)

    # strata
    strata: dict[tuple, list[dict]] = defaultdict(list)
    for w in windows:
        strata[_strata_key(w)].append(w)

    # sort each stratum by ts desc so "evenly sampled" ≈ most recent first within stratum
    for k in strata:
        strata[k].sort(key=lambda w: w["ts"], reverse=True)

    total = len(windows)

    # reserve `keep_recent` slots for the tail
    reserve = min(keep_recent, size // 4)
    main_budget = size - reserve

    # allocate at least 1 per non-empty stratum up to main_budget
    allocation: dict[tuple, int] = {}
    remaining = main_budget
    # start with 1 per stratum (if budget allows)
    for k in strata:
        if remaining <= 0:
            allocation[k] = 0
            continue
        allocation[k] = 1
        remaining -= 1

    # proportional allocation of remaining budget
    if remaining > 0:
        props = {k: len(v) / total for k, v in strata.items()}
        extras = {k: int(round(props[k] * remaining)) for k in strata}
        # fix rounding drift
        drift = remaining - sum(extras.values())
        # distribute drift to largest strata first
        order = sorted(strata.keys(), key=lambda k: -len(strata[k]))
        i = 0
        while drift != 0 and order:
            k = order[i % len(order)]
            if drift > 0:
                extras[k] += 1
                drift -= 1
            else:
                if extras[k] > 0:
                    extras[k] -= 1
                    drift += 1
            i += 1
            if i > 1000:
                break
        for k in strata:
            allocation[k] += extras[k]

    # collect
    picked: list[dict] = []
    picked_slugs: set[str] = set()
    for k, v in strata.items():
        n = min(allocation.get(k, 0), len(v))
        for w in v[:n]:
            if w["slug"] not in picked_slugs:
                picked.append(w)
                picked_slugs.add(w["slug"])

    # add recent tail
    recent = sorted(windows, key=lambda w: w["ts"], reverse=True)[:reserve]
    for w in recent:
        if w["slug"] not in picked_slugs:
            picked.append(w)
            picked_slugs.add(w["slug"])

    # sort final result chronologically
    picked.sort(key=lambda w: w["ts"])

    # trim if over-size due to rounding + recent tail overlap
    if len(picked) > size:
        # keep most recent AND ensure strata coverage by priority: recent > stratum reps
        picked = picked[-size:]
    return picked
