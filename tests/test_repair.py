"""Tests for the repair path (--repair-date) and the watermark-hole fix.

The two failures these guard against are real ones from July 2026:
Jul 22-25 lost 30 to 70 percent of their trades when a capped catch-up
advanced the watermark silently, and Jul 27-29 were never committed at all
when the CI push died on GitHub's 100 MB file block. repair_day() is the
recovery tool for both, so its own failure modes (writing a partial day,
touching state when it should not) get pinned down here.

Everything is offline: a fake getter plays the Kalshi API.
"""
import archiver


def raw_trade(trade_id: str, ticker: str, created_time: str, count: float = 10.0,
               yes_price: str = "0.1200", no_price: str = "0.8800",
               taker_side: str = "yes") -> dict:
    """A trade the way GET /markets/trades returns it (raw, not normalized)."""
    return {
        "trade_id": trade_id,
        "ticker": ticker,
        "count_fp": f"{count:.2f}",
        "yes_price_dollars": yes_price,
        "no_price_dollars": no_price,
        "taker_outcome_side": taker_side,
        "taker_book_side": "bid" if taker_side == "yes" else "ask",
        "created_time": created_time,
        "is_block_trade": False,
    }


def make_getter(trade_pages: list[list[dict]]):
    """A fake http_get_json. Serves `trade_pages` one page per
    /markets/trades call (with a cursor while pages remain), and empty
    but well-formed answers for the metadata endpoints."""
    calls = {"trades": 0}

    def getter(url, params=None, retries=None):
        if url.endswith("/markets/trades"):
            i = calls["trades"]
            calls["trades"] += 1
            if i >= len(trade_pages):
                return {"trades": [], "cursor": ""}
            more = i + 1 < len(trade_pages)
            return {"trades": trade_pages[i], "cursor": "next" if more else ""}
        if "/series/" in url:
            return {"series": {}}
        if url.endswith("/markets"):
            return {"markets": []}
        return {}

    return getter


def test_repair_day_replaces_the_whole_file_instead_of_merging(redirect_storage):
    # A wrong, partial day is already on disk (the Jul 22-25 situation).
    stale = [archiver.normalize_trade(
        raw_trade("old1", "TICK-A", "2026-07-22T01:00:00Z", count=5.0))]
    archiver.save_day_agg("2026-07-22", archiver.aggregate_trades(stale))
    assert archiver.load_day_agg("2026-07-22")["trade_count"].sum() == 1

    # The full re-pull finds THREE trades for the day, then one older
    # trade that proves the pager crossed the start-of-day boundary.
    pages = [
        [raw_trade("t1", "TICK-A", "2026-07-22T23:00:00Z", count=10.0),
         raw_trade("t2", "TICK-A", "2026-07-22T12:00:00Z", count=7.0)],
        [raw_trade("t3", "TICK-B", "2026-07-22T00:30:00Z", count=3.0),
         raw_trade("prev", "TICK-A", "2026-07-21T23:59:00Z", count=1.0)],
    ]
    result = archiver.repair_day("2026-07-22", getter=make_getter(pages))

    assert result["ok"] is True
    assert result["trades"] == 3  # the Jul 21 boundary trade is filtered out

    repaired = archiver.load_day_agg("2026-07-22")
    # Replaced, not merged: the stale bucket's 5 contracts are gone.
    assert repaired["trade_count"].sum() == 3
    assert repaired["contracts"].sum() == 20.0
    assert set(repaired["ticker"]) == {"TICK-A", "TICK-B"}


def test_repair_day_refuses_to_write_a_partial_day(redirect_storage):
    # Existing good file must survive a failed repair untouched.
    good = [archiver.normalize_trade(
        raw_trade("keep", "TICK-A", "2026-07-22T01:00:00Z", count=5.0))]
    archiver.save_day_agg("2026-07-22", archiver.aggregate_trades(good))

    # Page cap of 1 stops the pager before it reaches the start of the day.
    pages = [
        [raw_trade("t1", "TICK-A", "2026-07-22T23:00:00Z")],
        [raw_trade("t2", "TICK-A", "2026-07-22T12:00:00Z")],
    ]
    result = archiver.repair_day("2026-07-22", max_pages=1, getter=make_getter(pages))

    assert result["ok"] is False
    assert "page cap" in result["reason"]
    # The old file was not touched.
    assert archiver.load_day_agg("2026-07-22")["trade_count"].sum() == 1


def test_repair_day_refuses_when_retention_no_longer_covers_the_day(redirect_storage):
    # Kalshi runs out of cursor (retention edge) before the day start:
    # one in-day page, then the API says "nothing older".
    pages = [
        [raw_trade("t1", "TICK-A", "2026-07-22T23:00:00Z")],
    ]
    result = archiver.repair_day("2026-07-22", getter=make_getter(pages))
    assert result["ok"] is False
    assert "retention" in result["reason"]
    assert archiver.load_day_agg("2026-07-22").empty


def test_repair_day_update_state_advances_watermark_and_hot_buffer(redirect_storage):
    pages = [
        [raw_trade("t1", "TICK-A", "2026-07-22T23:00:00Z", count=10.0)],
        [raw_trade("prev", "TICK-A", "2026-07-21T23:59:00Z", count=1.0)],
    ]
    result = archiver.repair_day("2026-07-22", update_state=True,
                                  getter=make_getter(pages))
    assert result["ok"] is True

    state = archiver.load_state()
    # Watermark is the newest ts the pager saw.
    assert state["newest_ts"] == archiver.parse_iso_ts("2026-07-22T23:00:00Z")
    # Hot buffer holds the day's trades in the normal full-row schema.
    buffer = archiver.load_hot_trades()
    assert "t1" in set(buffer["trade_id"])
    assert list(buffer.columns) == archiver.TRADE_COLUMNS


def test_run_records_a_hole_when_catchup_hits_the_page_cap(redirect_storage):
    # Seed a state with an old watermark the capped catch-up never reaches.
    archiver.save_state({"newest_ts": archiver.parse_iso_ts("2026-07-20T00:00:00Z"),
                          "frontier_ts": archiver.parse_iso_ts("2026-07-19T00:00:00Z"),
                          "backfill_complete": True, "last_run_utc": None, "holes": []})

    # Every page is newer than the watermark and always has a cursor, so a
    # 2-page budget caps out mid-air.
    pages = [
        [raw_trade("n1", "TICK-A", "2026-07-26T10:00:00Z")],
        [raw_trade("n2", "TICK-A", "2026-07-26T09:00:00Z")],
        [raw_trade("n3", "TICK-A", "2026-07-26T08:00:00Z")],
    ]
    summary = archiver.run(catchup_max_pages=2, backfill_max_pages=0,
                            getter=make_getter(pages))

    state = archiver.load_state()
    assert len(state["holes"]) == 1
    hole = state["holes"][0]
    assert hole["from_ts"] == archiver.parse_iso_ts("2026-07-20T00:00:00Z")
    assert hole["to_ts"] == archiver.parse_iso_ts("2026-07-26T09:00:00Z")
    # The watermark still advanced (no double counting next run).
    assert summary["newest_ts"] == archiver.parse_iso_ts("2026-07-26T10:00:00Z")
