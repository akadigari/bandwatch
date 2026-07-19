"""Tests for the on-disk side: dedupe logic, monthly parquet append, and
the metadata upsert. Every test uses the `redirect_storage` fixture from
conftest.py, so nothing here ever touches the real data/ folder.
"""
import pandas as pd

import archiver


def make_trade(trade_id: str, ticker: str, created_time: str, count: float = 1.0) -> dict:
    raw = {
        "trade_id": trade_id,
        "ticker": ticker,
        "count_fp": f"{count:.2f}",
        "yes_price_dollars": "0.5000",
        "no_price_dollars": "0.5000",
        "taker_outcome_side": "yes",
        "taker_book_side": "bid",
        "created_time": created_time,
        "is_block_trade": False,
    }
    return archiver.normalize_trade(raw)


def test_dedupe_trades_keeps_one_row_per_trade_id():
    existing = pd.DataFrame([
        make_trade("t1", "KXHIGHLAX-26JUL19-T80", "2026-07-19T03:00:00Z"),
        make_trade("t2", "KXHIGHLAX-26JUL19-T80", "2026-07-19T03:01:00Z"),
    ], columns=archiver.TRADE_COLUMNS)
    new_rows = [
        make_trade("t2", "KXHIGHLAX-26JUL19-T80", "2026-07-19T03:01:00Z"),  # duplicate
        make_trade("t3", "KXHIGHLAX-26JUL19-T80", "2026-07-19T03:02:00Z"),  # genuinely new
    ]
    merged = archiver.dedupe_trades(existing, new_rows)
    assert sorted(merged["trade_id"].tolist()) == ["t1", "t2", "t3"]
    assert merged["trade_id"].is_unique


def test_append_trades_writes_monthly_files_and_is_idempotent_on_rerun(redirect_storage):
    trades = [
        make_trade("t1", "KXHIGHLAX-26JUL19-T80", "2026-07-19T03:00:00Z"),
        make_trade("t2", "KXHIGHLAX-26JUL19-T80", "2026-07-19T03:01:00Z"),
        make_trade("t3", "KXHIGHLAX-26AUG01-T80", "2026-08-01T00:00:00Z"),
    ]
    added = archiver.append_trades(trades)
    assert added == {"2026-07": 2, "2026-08": 1}

    july = archiver.load_month_parquet("2026-07")
    august = archiver.load_month_parquet("2026-08")
    assert len(july) == 2
    assert len(august) == 1
    assert set(july["trade_id"]) == {"t1", "t2"}
    assert set(august["trade_id"]) == {"t3"}

    # Running the exact same archive pass again must not create duplicate
    # rows or grow the files: this is the "safe to run twice" contract.
    added_again = archiver.append_trades(trades)
    assert added_again == {"2026-07": 0, "2026-08": 0}
    assert len(archiver.load_month_parquet("2026-07")) == 2
    assert len(archiver.load_month_parquet("2026-08")) == 1


def test_upsert_meta_snapshot_overwrites_same_day_key(redirect_storage, tmp_path):
    path = tmp_path / "meta_test.parquet"
    first_rows = [{"snapshot_date": "2026-07-19", "ticker": "KXHIGHLAX-26JUL19-T80",
                   "status": "active", "volume_fp": 100.0}]
    archiver.upsert_meta_snapshot(path, first_rows, ["snapshot_date", "ticker"])

    second_rows = [{"snapshot_date": "2026-07-19", "ticker": "KXHIGHLAX-26JUL19-T80",
                     "status": "closed", "volume_fp": 150.0}]
    total_rows = archiver.upsert_meta_snapshot(path, second_rows, ["snapshot_date", "ticker"])

    assert total_rows == 1
    df = pd.read_parquet(path)
    assert len(df) == 1
    assert df.iloc[0]["status"] == "closed"
    assert df.iloc[0]["volume_fp"] == 150.0
