"""Tests for the aggregation layer: turning raw trades into daily
per-band buckets, band bucketing from Kalshi's dollar-string prices, and
making the aggregate idempotent across runs without keeping a full trade
history on disk. Every test here is offline (no network) and uses the
`redirect_storage` fixture from conftest.py where it touches disk, so
nothing ever writes to the real data/ folder.
"""
import archiver


def make_trade(trade_id: str, ticker: str, created_time: str, taker_side: str = "yes",
                yes_price: str = "0.1200", no_price: str = "0.8800", count: float = 10.0) -> dict:
    raw = {
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
    return archiver.normalize_trade(raw)


def test_band_from_price_buckets_clean_cent_values_and_rejects_out_of_range():
    assert archiver.band_from_price(0.12) == 12
    assert archiver.band_from_price(0.01) == 1
    assert archiver.band_from_price(0.99) == 99
    assert archiver.band_from_price(0.0) is None   # 0 cents: out of the 1-99 band range
    assert archiver.band_from_price(1.0) is None   # 100 cents: out of the 1-99 band range
    assert archiver.band_from_price(None) is None


def test_taker_price_picks_the_side_the_taker_actually_paid():
    yes_taker = make_trade("t1", "TICK-A", "2026-07-19T00:00:00Z", taker_side="yes",
                            yes_price="0.1200", no_price="0.8800")
    no_taker = make_trade("t2", "TICK-A", "2026-07-19T00:00:00Z", taker_side="no",
                           yes_price="0.1200", no_price="0.8800")
    unknown_side = make_trade("t3", "TICK-A", "2026-07-19T00:00:00Z", taker_side="yes")
    unknown_side["taker_side"] = "sideways"  # simulate an unrecognized value
    assert archiver.taker_price(yes_taker) == 0.12
    assert archiver.taker_price(no_taker) == 0.88
    assert archiver.taker_price(unknown_side) is None


def test_aggregate_trades_groups_by_date_ticker_band_side_and_sums():
    trades = [
        # Two trades in the same bucket: TICK-A, 2026-07-19, 12 cents, yes.
        make_trade("t1", "TICK-A", "2026-07-19T01:00:00Z", "yes", "0.1200", "0.8800", count=10.0),
        make_trade("t2", "TICK-A", "2026-07-19T02:00:00Z", "yes", "0.1200", "0.8800", count=5.0),
        # A different band on the same ticker/day/side: does not merge with the above.
        make_trade("t3", "TICK-A", "2026-07-19T03:00:00Z", "yes", "0.1300", "0.8700", count=1.0),
        # The "no" side at the same 12-cent band: still a separate bucket.
        make_trade("t4", "TICK-A", "2026-07-19T04:00:00Z", "no", "0.8800", "0.1200", count=2.0),
        # A different ticker entirely.
        make_trade("t5", "TICK-B", "2026-07-19T05:00:00Z", "yes", "0.1200", "0.8800", count=7.0),
    ]
    agg = archiver.aggregate_trades(trades)
    buckets = {(r["ticker"], r["band_cents"], r["taker_side"]): r for r in agg.to_dict("records")}

    a12yes = buckets[("TICK-A", 12, "yes")]
    assert a12yes["date"] == "2026-07-19"
    assert a12yes["trade_count"] == 2
    assert a12yes["contracts"] == 15.0
    assert a12yes["dollars"] == 10.0 * 0.12 + 5.0 * 0.12

    a13yes = buckets[("TICK-A", 13, "yes")]
    assert a13yes["trade_count"] == 1
    assert a13yes["contracts"] == 1.0

    a12no = buckets[("TICK-A", 12, "no")]
    assert a12no["trade_count"] == 1
    assert a12no["contracts"] == 2.0
    assert a12no["dollars"] == 2.0 * 0.12  # taker paid the "no" price, which is 12 cents here

    b12yes = buckets[("TICK-B", 12, "yes")]
    assert b12yes["trade_count"] == 1
    assert len(agg) == 4  # exactly four distinct buckets, nothing collapsed or split wrong


def test_aggregate_trades_skips_rows_missing_a_usable_band_or_side():
    trades = [
        make_trade("t1", "TICK-A", "2026-07-19T01:00:00Z", "yes", "1.0000", "0.0000"),  # 100 cents: out of range
        make_trade("t2", "", "2026-07-19T01:00:00Z", "yes"),  # no ticker
    ]
    trades[1]["taker_side"] = "unknown"  # and an unrecognized side, for good measure
    agg = archiver.aggregate_trades(trades)
    assert agg.empty


def test_append_aggregates_is_idempotent_on_the_same_batch(redirect_storage):
    trades = [
        make_trade("t1", "TICK-A", "2026-07-19T01:00:00Z", "yes", "0.1200", "0.8800", count=10.0),
        make_trade("t2", "TICK-A", "2026-07-19T02:00:00Z", "yes", "0.1200", "0.8800", count=5.0),
    ]
    # Same pipeline archiver.run() uses: dedupe against the hot buffer, then
    # aggregate only what survives that filter.
    new_1 = archiver.dedupe_for_aggregation(trades)
    added_1 = archiver.append_aggregates(new_1)
    assert added_1 == {"2026-07": 1}  # one bucket: TICK-A / 12 cents / yes

    agg = archiver.load_month_agg("2026-07")
    row = agg.iloc[0]
    assert row["trade_count"] == 2
    assert row["contracts"] == 15.0

    # Re-running with the EXACT same fetch (the "same day re-run" case):
    # nothing should be aggregated twice.
    new_2 = archiver.dedupe_for_aggregation(trades)
    assert new_2 == []
    added_2 = archiver.append_aggregates(new_2)
    assert added_2 == {}

    agg_again = archiver.load_month_agg("2026-07")
    assert len(agg_again) == 1
    row_again = agg_again.iloc[0]
    assert row_again["trade_count"] == 2   # still 2, not 4
    assert row_again["contracts"] == 15.0  # still 15, not 30


def test_append_aggregates_sums_genuinely_new_trades_onto_an_existing_bucket(redirect_storage):
    # Run 1: two trades land in the same bucket.
    run_1_trades = [
        make_trade("t1", "TICK-A", "2026-07-19T01:00:00Z", "yes", "0.1200", "0.8800", count=10.0),
        make_trade("t2", "TICK-A", "2026-07-19T02:00:00Z", "yes", "0.1200", "0.8800", count=5.0),
    ]
    archiver.append_aggregates(archiver.dedupe_for_aggregation(run_1_trades))

    # Run 2: a genuinely new trade (different trade_id) in the SAME bucket.
    run_2_trades = [
        make_trade("t3", "TICK-A", "2026-07-19T03:00:00Z", "yes", "0.1200", "0.8800", count=3.0),
    ]
    archiver.append_aggregates(archiver.dedupe_for_aggregation(run_2_trades))

    agg = archiver.load_month_agg("2026-07")
    assert len(agg) == 1  # still one bucket
    row = agg.iloc[0]
    assert row["trade_count"] == 3    # 2 + 1
    assert row["contracts"] == 18.0   # 15 + 3


def test_dedupe_for_aggregation_filters_out_previously_seen_trade_ids(redirect_storage):
    first_batch = [
        make_trade("t1", "TICK-A", "2026-07-19T01:00:00Z"),
        make_trade("t2", "TICK-A", "2026-07-19T02:00:00Z"),
    ]
    new_from_first = archiver.dedupe_for_aggregation(first_batch)
    assert {t["trade_id"] for t in new_from_first} == {"t1", "t2"}

    # A second fetch that re-includes t2 (the boundary-page overlap
    # page_trades can produce between runs) plus one genuinely new trade.
    second_batch = [
        make_trade("t2", "TICK-A", "2026-07-19T02:00:00Z"),  # already aggregated
        make_trade("t3", "TICK-A", "2026-07-19T03:00:00Z"),  # new
    ]
    new_from_second = archiver.dedupe_for_aggregation(second_batch)
    assert {t["trade_id"] for t in new_from_second} == {"t3"}
