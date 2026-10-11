#!/usr/bin/env python3
"""Independent reference for retained add/cancel-only public fixtures.

Usage: python3 derive_add_cancel_snapshot.py public-active.jsonl > public-active-final.json
Executions, resets, and unknown messages fail rather than borrowing engine logic.
"""
import bisect
import json
import sys

books = None
sequence = None
market = None
for line in open(sys.argv[1], encoding="utf-8"):
    message = json.loads(line)
    kind = message["type"]
    if kind in ("pong", "subscribed"):
        continue
    if kind == "snapshot":
        assert books is None, "multiple snapshots require a separate reference"
        snapshot = message["data"]
        market = int(snapshot["market_id"])
        sequence = int(snapshot["latest_seq_num"])
        books = {}
        for side, key in [(1, "ask_agg_data"), (2, "bid_agg_data")]:
            levels = snapshot[key]
            books[side] = {int(level["ticks"]): int(level["qty"]) for level in levels}
            assert len(books[side]) == len(levels), "duplicate snapshot price"
    else:
        assert kind == "md_event_detail" and books is not None
        event = message["data"]
        data = event["data"]
        assert int(event["order"]["market_id"]) == market
        assert int(data["seq_num"]) == sequence + 1
        sequence += 1
        assert (data["event_type"], data["event_qual"]) in ((1, 0), (2, 3))
        side = data["event_side"]
        price = int(data["event_abs_ticks"])
        delta = int(data["event_level_delta_qty"])
        assert (delta > 0) == (data["event_type"] == 1)
        assert int(data["event_delta_qty"]) == delta
        assert int(data["event_total_px"]) == price * delta
        ladder = books[side]
        prices = sorted(ladder)
        index = bisect.bisect_left(prices, price) if side == 1 else len(prices) - bisect.bisect_right(prices, price)
        assert int(data["event_level_depth"]) == index
        begin = ladder.get(price, 0)
        assert int(data["event_level_begin_qty"]) == begin
        after = begin + delta
        assert after >= 0
        if after:
            ladder[price] = after
        else:
            del ladder[price]
        for direction, name in [(1, "ask"), (2, "bid")]:
            levels = books[direction]
            best = (min(levels) if direction == 1 else max(levels)) if levels else 0
            assert int(event["tob"][name + "_ticks"]) == best
            assert int(event["tob"][name + "_qty"]) == levels.get(best, 0)

assert books is not None
result = {"market_id": str(market), "latest_seq_num": str(sequence)}
for side, key in [(1, "ask_agg_data"), (2, "bid_agg_data")]:
    result[key] = [{"ticks": price, "qty": str(books[side][price])}
                   for price in sorted(books[side], reverse=side == 2)]
print(json.dumps({"type": "snapshot", "data": result}, separators=(",", ":")))
