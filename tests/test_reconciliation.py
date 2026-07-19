"""Tests for the reconciliation math behind GATES.md gate 1 (archived
trade count vs. Kalshi's own volume_fp, per market) and for the metadata
fetchers that feed it, using the tests/fixtures/*.json canned responses
instead of the network.
"""
import archiver


def test_sum_counts_by_ticker():
    trades = [
        {"ticker": "A", "count": 5.0},
        {"ticker": "A", "count": 3.0},
        {"ticker": "B", "count": 10.0},
        {"ticker": None, "count": 99.0},  # no ticker: must be ignored
        {"ticker": "C", "count": None},   # no count: must be ignored
    ]
    totals = archiver.sum_counts_by_ticker(trades)
    assert totals == {"A": 8.0, "B": 10.0}


def test_reconciliation_report_pct_diff_and_tolerance_flag():
    # Numbers picked so the division comes out exact, so this checks the
    # gate math itself rather than floating-point rounding.
    archived = {"A": 98.0, "B": 80.0, "C": 0.0}
    api_volume = {"A": 100.0, "B": 100.0, "C": 0.0}
    report = archiver.reconciliation_report(archived, api_volume, tolerance=0.02)
    rows = {r["ticker"]: r for r in report["rows"]}
    assert rows["A"]["pct_diff"] == 0.02   # exactly at the 2% bar
    assert rows["A"]["within_tolerance"] is True
    assert rows["B"]["pct_diff"] == 0.2    # way off
    assert rows["B"]["within_tolerance"] is False
    assert rows["C"]["pct_diff"] == 0.0    # both zero: a perfect match
    assert rows["C"]["within_tolerance"] is True


def test_reconciliation_report_gate_threshold_is_95_percent():
    # 19 of 20 markets reconcile exactly, one is way off: 95% pass rate,
    # right at the gate's own bar, so it must still pass.
    archived = {f"m{i}": 100.0 for i in range(19)}
    archived["m19"] = 50.0
    api_volume = {f"m{i}": 100.0 for i in range(20)}
    report = archiver.reconciliation_report(archived, api_volume, tolerance=0.02)
    assert report["total_markets"] == 20
    assert report["passing_markets"] == 19
    assert report["pass_rate"] == 0.95
    assert report["gate_pass"] is True

    # Drop one more market off the good side: 18 of 20 (90%) must fail.
    archived["m18"] = 50.0
    report2 = archiver.reconciliation_report(archived, api_volume, tolerance=0.02)
    assert report2["passing_markets"] == 18
    assert report2["gate_pass"] is False


def test_fetch_markets_meta_parses_batch_response(fixture):
    payload = fixture("markets_batch.json")

    def getter(url, params=None):
        return payload

    rows = archiver.fetch_markets_meta(
        ["KXHIGHLAX-26JUL19-T80", "KXBTC15M-26JUL190000-00"], getter=getter,
    )
    by_ticker = {r["ticker"]: r for r in rows}
    assert by_ticker["KXHIGHLAX-26JUL19-T80"]["status"] == "active"
    assert by_ticker["KXHIGHLAX-26JUL19-T80"]["volume_fp"] == 40402.48
    assert by_ticker["KXBTC15M-26JUL190000-00"]["result"] == "yes"
    assert by_ticker["KXBTC15M-26JUL190000-00"]["volume_fp"] == 20.50


def test_resolve_series_tickers_falls_back_past_a_404():
    # 'KXHIGHLAX-26JUL19-T80' resolves on the first (1-segment) guess.
    # 'KXMLBWINS-LAD-26-T95' and 'KXMLBWINS-LAD-26-T115' both 404 on the
    # 1-segment guess ('KXMLBWINS') and only resolve on the 2-segment one
    # ('KXMLBWINS-LAD'): this is the real shape found live on 2026-07-19.
    # Both tickers share that candidate, so it must only be requested once.
    call_counts: dict[str, int] = {}

    def getter(url, params=None):
        candidate = url.rsplit("/", 1)[-1]
        call_counts[candidate] = call_counts.get(candidate, 0) + 1
        if candidate == "KXHIGHLAX":
            return {"series": {"ticker": "KXHIGHLAX"}}
        if candidate == "KXMLBWINS-LAD":
            return {"series": {"ticker": "KXMLBWINS-LAD"}}
        return None  # 404: 'KXMLBWINS' alone is not a real series

    resolved = archiver.resolve_series_tickers(
        ["KXHIGHLAX-26JUL19-T80", "KXMLBWINS-LAD-26-T95", "KXMLBWINS-LAD-26-T115"],
        getter=getter,
    )
    assert resolved["KXHIGHLAX-26JUL19-T80"] == "KXHIGHLAX"
    assert resolved["KXMLBWINS-LAD-26-T95"] == "KXMLBWINS-LAD"
    assert resolved["KXMLBWINS-LAD-26-T115"] == "KXMLBWINS-LAD"
    assert call_counts["KXMLBWINS-LAD"] == 1  # deduped, not once per ticker


def test_fetch_series_meta_parses_response(fixture):
    payload = fixture("series_khighlax.json")

    def getter(url, params=None):
        assert url.endswith("/series/KXHIGHLAX")
        return payload

    rows = archiver.fetch_series_meta(["KXHIGHLAX"], getter=getter)
    assert len(rows) == 1
    assert rows[0]["ticker"] == "KXHIGHLAX"
    assert rows[0]["fee_type"] == "quadratic"
    assert rows[0]["fee_multiplier"] == 1
    assert rows[0]["category"] == "Climate and Weather"
