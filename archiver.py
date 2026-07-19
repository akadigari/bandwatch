"""Bandwatch archiver.

This script saves Kalshi's public trades before they age out of Kalshi's
own API. Kalshi's free /markets/trades endpoint only keeps about 60 to 65
days of history. Once a trade rolls off that window, it is gone for good
unless something already saved it. That is the whole reason this script
exists.

What it does, every time you run it:

1. Pull new trades from GET /markets/trades (paged, newest first) and add
   any we do not already have to a monthly parquet file under
   data/trades/YYYY-MM.parquet. Every trade has a trade_id, so we can
   always tell a trade we already saved from a new one.
2. Keep chipping away at older history in the background (a separate
   "backfill" pointer that works backward in time, a bit further each
   day) so we grab as much of the pre-existing window as we can before it
   rolls off.
3. Take a daily snapshot of market metadata (status, result, close_time,
   volume_fp) and series metadata (fee_type, fee_multiplier, category) for
   every ticker that traded that day, so a later analysis can check our
   trade counts against Kalshi's own volume numbers.

Run it once a day. It is safe to run it twice in the same day too: nothing
gets double counted, because everything is deduped by trade_id (trades) or
by (snapshot_date, ticker) (metadata).

See GATES.md for the rules this project has to pass before anyone trusts a
number that comes out of the data this script collects.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Confirmed live against https://docs.kalshi.com/api-reference (2026-07-18)
# and matches the host the owner's ../tipoff scanner already uses in
# production for the same API family. docs.kalshi.com also lists
# external-api.kalshi.com as an equivalent host; we stick with the one
# already proven out by tipoff.
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
TRADES_DIR = DATA_DIR / "trades"
META_DIR = DATA_DIR / "meta"
STATE_FILE = META_DIR / "state.json"
MARKETS_META_FILE = META_DIR / "markets.parquet"
SERIES_META_FILE = META_DIR / "series.parquet"

PAGE_LIMIT = 1000  # Kalshi's own max page size for /markets/trades

# Kalshi's public trade firehose (all markets, no ticker filter) runs at
# roughly 8 to 9 million trades a day platform-wide as of 2026-07, mostly
# from bot-driven 15-minute crypto range markets and sports micro-markets.
# That is far more than a "quiet API" number, so the page budgets below are
# split into two jobs instead of one open-ended loop:
#
#   - catch-up: trades since the last run. This is the one that must never
#     fall behind, because a gap here is a gap in the forward-looking
#     pre-Sept-1 window we cannot get back later.
#   - backfill: older trades, working backward from wherever we stopped
#     last time. This is best-effort: given the volume above, walking all
#     the way back through the full ~60-65 day window will take many days
#     of runs and may not finish before the oldest data ages out. That is
#     an honest limitation, not a bug; see README "What this can't do yet."
CATCHUP_MAX_PAGES = 6000
BACKFILL_MAX_PAGES = 2000

MARKETS_PER_META_CALL = 100  # ticker batch size for GET /markets?tickers=

HTTP_TIMEOUT = 20
HTTP_RETRIES = 3

_session = requests.Session()
_session.headers["User-Agent"] = "bandwatch-archiver/1.0 (research archive; keyless; no orders)"


def http_get_json(url: str, params: dict | None = None, retries: int = HTTP_RETRIES):
    """GET a URL and parse the body as JSON.

    Kalshi's payloads can contain raw control characters in free-text
    fields, so we parse leniently with strict=False instead of calling
    resp.json() directly. This is the same fix the owner's tipoff scanner
    uses on the same API family.
    """
    last_err = None
    for attempt in range(retries + 1):
        try:
            resp = _session.get(url, params=params, timeout=HTTP_TIMEOUT)
            if resp.status_code == 429:
                time.sleep(2.0 * (attempt + 1))
                continue
            resp.raise_for_status()
            return json.loads(resp.text, strict=False)
        except (requests.RequestException, json.JSONDecodeError) as err:
            last_err = err
            time.sleep(1.0 * (attempt + 1))
    print(f"  [warn] GET {url} failed after retries: {last_err}")
    return None


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_dollars(value) -> float | None:
    """Kalshi sends prices as dollar strings like '0.1200'. Turn one into a
    plain float. Returns None if the value is missing or not a real number."""
    if value is None or value == "":
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN check without importing math just for this
        return None
    return f


def parse_count(value) -> float | None:
    """Kalshi sends contract counts as fixed-point strings like '10.00'.
    Same shape as a dollar string, so this reuses the same parser."""
    return parse_dollars(value)


def parse_iso_ts(value: str | None) -> float | None:
    """Turn an ISO 8601 timestamp like '2026-07-19T03:52:14.217089Z' into a
    Unix timestamp in seconds. Returns None if it cannot be parsed."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def month_key(created_time: str) -> str:
    """'2026-07-19T03:52:14.21Z' -> '2026-07'. Used to pick which monthly
    parquet file a trade belongs in."""
    return created_time[:7]


def series_ticker_from_market_ticker(ticker: str) -> str:
    """A Kalshi market ticker is SERIESTICKER-EVENTDATE-STRIKE (e.g.
    'KXHIGHLAX-26JUL19-T80' -> 'KXHIGHLAX'). Confirmed live against
    GET /events/{ticker}, which returns series_ticker directly: the prefix
    before the first '-' always matches it. Using the prefix means we
    never have to pay for an extra /events call just to find a market's
    series."""
    return ticker.split("-", 1)[0]


def normalize_trade(raw: dict) -> dict:
    """Turn one raw trade from GET /markets/trades into the row shape we
    store on disk."""
    created_time = raw.get("created_time") or ""
    return {
        "trade_id": raw.get("trade_id"),
        "ticker": raw.get("ticker"),
        "created_time": created_time,
        "created_ts": parse_iso_ts(created_time),
        "count": parse_count(raw.get("count_fp")),
        "yes_price": parse_dollars(raw.get("yes_price_dollars")),
        "no_price": parse_dollars(raw.get("no_price_dollars")),
        "taker_side": raw.get("taker_outcome_side") or raw.get("taker_side"),
        "taker_book_side": raw.get("taker_book_side"),
        "is_block_trade": bool(raw.get("is_block_trade", False)),
    }


# ---------------------------------------------------------------------------
# Trade paging
# ---------------------------------------------------------------------------

def fetch_trades_page(cursor: str | None = None, max_ts: float | None = None,
                       limit: int = PAGE_LIMIT, getter=http_get_json) -> dict | None:
    """Fetch one page of GET /markets/trades. Returns the raw JSON dict, or
    None if the request failed after retries."""
    params: dict = {"limit": limit}
    if cursor:
        params["cursor"] = cursor
    if max_ts is not None:
        params["max_ts"] = int(max_ts)
    return getter(f"{KALSHI_BASE}/markets/trades", params)


def page_trades(max_ts: float | None = None, stop_at_ts: float | None = None,
                 max_pages: int = CATCHUP_MAX_PAGES, limit: int = PAGE_LIMIT,
                 getter=http_get_json) -> tuple[list[dict], bool, bool]:
    """Page backward through GET /markets/trades and collect normalized
    trades.

    Starts at `max_ts` (or "now" if max_ts is None) and pages back in time
    using the cursor Kalshi hands back each call. Stops when any of these
    happen:

      - Kalshi returns an empty cursor: we hit the true edge of whatever
        window it still has for us. This is the only case that means
        "there is nothing older to get."
      - `stop_at_ts` is set and every trade on a page is at or before it:
        we caught back up to trades we already archived last run.
      - we hit `max_pages`: a safety valve so one run can never run
        forever. Real Kalshi trade volume is high enough that this will
        often be the reason a run stops, especially during backfill.

    Returns (trades, reached_end, hit_page_cap):
      - trades: normalized trade dicts collected this call
      - reached_end: True only if we stopped because Kalshi ran out of
        cursor (the true edge of its retention window)
      - hit_page_cap: True if we stopped because of max_pages
    """
    collected: list[dict] = []
    cursor: str | None = None
    reached_end = False
    hit_page_cap = True

    for _ in range(max_pages):
        data = fetch_trades_page(cursor=cursor, max_ts=max_ts, limit=limit, getter=getter)
        if not data:
            # A request failed even after retries. Stop this pass rather
            # than looping on a dead endpoint; whatever we already
            # collected this call still gets saved.
            hit_page_cap = False
            break

        raw_trades = data.get("trades") or []
        if not raw_trades:
            reached_end = True
            hit_page_cap = False
            break

        page_trades_norm = [normalize_trade(t) for t in raw_trades]
        oldest_ts_on_page = min(
            (t["created_ts"] for t in page_trades_norm if t["created_ts"] is not None),
            default=None,
        )

        if stop_at_ts is not None and oldest_ts_on_page is not None \
                and oldest_ts_on_page <= stop_at_ts:
            # We have reached trades we already archived last run. Keep
            # the whole page rather than filtering by timestamp: Kalshi
            # batch-matches trades, so several trades can share the exact
            # same microsecond timestamp as our watermark, and a strict
            # "newer than" filter would silently drop real siblings of
            # that instant. trade_id dedup at write time makes it safe to
            # just keep everything and let duplicates fall out there.
            collected.extend(page_trades_norm)
            hit_page_cap = False
            break

        collected.extend(page_trades_norm)

        cursor = data.get("cursor") or None
        if not cursor:
            reached_end = True
            hit_page_cap = False
            break
    else:
        hit_page_cap = True

    return collected, reached_end, hit_page_cap


# ---------------------------------------------------------------------------
# Parquet storage: trades
# ---------------------------------------------------------------------------

TRADE_COLUMNS = [
    "trade_id", "ticker", "created_time", "created_ts", "count",
    "yes_price", "no_price", "taker_side", "taker_book_side", "is_block_trade",
]


def _empty_trades_df() -> pd.DataFrame:
    return pd.DataFrame(columns=TRADE_COLUMNS)


def load_month_parquet(month: str) -> pd.DataFrame:
    """Load an existing monthly trade file, or an empty frame with the
    right columns if it does not exist yet."""
    path = TRADES_DIR / f"{month}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return _empty_trades_df()


def save_month_parquet(month: str, df: pd.DataFrame) -> None:
    TRADES_DIR.mkdir(parents=True, exist_ok=True)
    path = TRADES_DIR / f"{month}.parquet"
    df = df.sort_values("created_ts", kind="stable").reset_index(drop=True)
    df.to_parquet(path, index=False)


def dedupe_trades(existing: pd.DataFrame, new_rows: list[dict]) -> pd.DataFrame:
    """Merge new trade rows into an existing frame, keeping exactly one row
    per trade_id."""
    if not new_rows:
        return existing
    new_df = pd.DataFrame(new_rows, columns=TRADE_COLUMNS)
    merged = pd.concat([existing, new_df], ignore_index=True)
    merged = merged.drop_duplicates(subset="trade_id", keep="last")
    return merged


def append_trades(trades: list[dict]) -> dict[str, int]:
    """Group trades by month and append them to the right monthly parquet
    file, deduped by trade_id. Returns {month: new_row_count_added}."""
    by_month: dict[str, list[dict]] = {}
    for t in trades:
        if not t.get("trade_id") or not t.get("created_time"):
            continue
        by_month.setdefault(month_key(t["created_time"]), []).append(t)

    added: dict[str, int] = {}
    for month, rows in by_month.items():
        existing = load_month_parquet(month)
        before = len(existing)
        merged = dedupe_trades(existing, rows)
        save_month_parquet(month, merged)
        added[month] = len(merged) - before
    return added


# ---------------------------------------------------------------------------
# Metadata: markets and series
# ---------------------------------------------------------------------------

def fetch_markets_meta(tickers: list[str], getter=http_get_json,
                        batch_size: int = MARKETS_PER_META_CALL) -> list[dict]:
    """Fetch status/result/close_time/volume_fp for a list of market
    tickers via GET /markets?tickers=..., batched so the query string
    stays a sane length."""
    out: list[dict] = []
    unique_tickers = sorted(set(t for t in tickers if t))
    for i in range(0, len(unique_tickers), batch_size):
        batch = unique_tickers[i:i + batch_size]
        data = getter(f"{KALSHI_BASE}/markets", {"tickers": ",".join(batch), "limit": batch_size})
        if not data:
            continue
        for m in data.get("markets") or []:
            out.append({
                "ticker": m.get("ticker"),
                "event_ticker": m.get("event_ticker"),
                "status": m.get("status"),
                "result": m.get("result"),
                "close_time": m.get("close_time"),
                "volume_fp": parse_count(m.get("volume_fp")),
            })
    return out


def fetch_series_meta(series_tickers: list[str], getter=http_get_json) -> list[dict]:
    """Fetch fee_type/fee_multiplier/category for a list of series tickers
    via GET /series/{ticker}, one call per series (there is no batch
    endpoint for series)."""
    out: list[dict] = []
    for ticker in sorted(set(t for t in series_tickers if t)):
        data = getter(f"{KALSHI_BASE}/series/{ticker}", None)
        if not data:
            continue
        s = data.get("series") or {}
        if not s:
            continue
        out.append({
            "ticker": s.get("ticker", ticker),
            "fee_type": s.get("fee_type"),
            "fee_multiplier": s.get("fee_multiplier"),
            "category": s.get("category"),
        })
    return out


def upsert_meta_snapshot(path: Path, new_rows: list[dict], key_cols: list[str]) -> int:
    """Append new metadata rows to a parquet file, keeping only the latest
    row for each key (so re-running the same day overwrites instead of
    duplicating). Returns the number of rows in the file after the
    upsert."""
    if not new_rows:
        return _existing_row_count(path)
    new_df = pd.DataFrame(new_rows)
    if path.exists():
        existing = pd.read_parquet(path)
        merged = pd.concat([existing, new_df], ignore_index=True)
    else:
        merged = new_df
    merged = merged.drop_duplicates(subset=key_cols, keep="last")
    META_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(path, index=False)
    return len(merged)


def _existing_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    return len(pd.read_parquet(path))


def snapshot_metadata(tickers: list[str], snapshot_date: str, getter=http_get_json) -> dict:
    """Snapshot market metadata for `tickers` and series metadata for the
    series they belong to. Returns a small summary dict for logging."""
    market_rows = fetch_markets_meta(tickers, getter=getter)
    for row in market_rows:
        row["snapshot_date"] = snapshot_date

    series_tickers = [series_ticker_from_market_ticker(t) for t in tickers]
    series_rows = fetch_series_meta(series_tickers, getter=getter)
    for row in series_rows:
        row["snapshot_date"] = snapshot_date

    markets_total = upsert_meta_snapshot(MARKETS_META_FILE, market_rows, ["snapshot_date", "ticker"])
    series_total = upsert_meta_snapshot(SERIES_META_FILE, series_rows, ["snapshot_date", "ticker"])
    return {
        "markets_snapshotted": len(market_rows),
        "series_snapshotted": len(series_rows),
        "markets_file_rows": markets_total,
        "series_file_rows": series_total,
    }


# ---------------------------------------------------------------------------
# Reconciliation math (gate 1 support)
# ---------------------------------------------------------------------------

def sum_counts_by_ticker(trades: list[dict]) -> dict[str, float]:
    """Sum the contract count of every archived trade, grouped by ticker.
    This is what gets compared against a market's volume_fp for gate 1."""
    totals: dict[str, float] = {}
    for t in trades:
        ticker = t.get("ticker")
        count = t.get("count")
        if not ticker or count is None:
            continue
        totals[ticker] = totals.get(ticker, 0.0) + count
    return totals


def reconciliation_report(archived_counts: dict[str, float],
                           api_volumes: dict[str, float],
                           tolerance: float = 0.02) -> dict:
    """Compare our archived, summed trade counts per ticker against
    Kalshi's own volume_fp per ticker. This is the math behind GATES.md
    gate 1: at least 95% of markets need to be within `tolerance` (2%
    by default) or the gate fails.
    """
    rows = []
    for ticker, api_volume in api_volumes.items():
        archived = archived_counts.get(ticker, 0.0)
        if api_volume == 0:
            pct_diff = 0.0 if archived == 0 else 1.0
        else:
            pct_diff = abs(archived - api_volume) / api_volume
        rows.append({
            "ticker": ticker,
            "archived_count": archived,
            "api_volume": api_volume,
            "pct_diff": pct_diff,
            "within_tolerance": pct_diff <= tolerance,
        })

    total = len(rows)
    passing = sum(1 for r in rows if r["within_tolerance"])
    pass_rate = passing / total if total else 1.0
    return {
        "rows": rows,
        "total_markets": total,
        "passing_markets": passing,
        "pass_rate": pass_rate,
        "gate_pass": pass_rate >= 0.95,
    }


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"newest_ts": None, "frontier_ts": None, "backfill_complete": False,
            "last_run_utc": None}


def save_state(state: dict) -> None:
    META_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(catchup_max_pages: int = CATCHUP_MAX_PAGES,
        backfill_max_pages: int = BACKFILL_MAX_PAGES,
        getter=http_get_json) -> dict:
    """Run one full archive cycle. Returns a summary dict (also used by the
    live verification run and printed by __main__)."""
    state = load_state()
    newest_ts = state.get("newest_ts")
    frontier_ts = state.get("frontier_ts")
    backfill_complete = state.get("backfill_complete", False)

    all_trades: list[dict] = []
    reached_absolute_end = False

    if newest_ts is None:
        # Bootstrap: nothing archived yet. One pass, starting from "now",
        # doubles as both the first catch-up and the start of backfill.
        trades, reached_end, _ = page_trades(
            max_ts=None, stop_at_ts=None,
            max_pages=catchup_max_pages + backfill_max_pages, getter=getter,
        )
        all_trades.extend(trades)
        ts_values = [t["created_ts"] for t in trades if t["created_ts"] is not None]
        if ts_values:
            newest_ts = max(ts_values)
            frontier_ts = min(ts_values)
        backfill_complete = reached_end
        reached_absolute_end = reached_end
    else:
        # Catch-up: whatever is newer than the last trade we archived.
        catchup_trades, _, _ = page_trades(
            max_ts=None, stop_at_ts=newest_ts,
            max_pages=catchup_max_pages, getter=getter,
        )
        all_trades.extend(catchup_trades)
        ts_values = [t["created_ts"] for t in catchup_trades if t["created_ts"] is not None]
        if ts_values:
            newest_ts = max(newest_ts, max(ts_values))

        # Backfill: keep working backward from wherever we stopped before,
        # unless we already confirmed we reached the true edge.
        if not backfill_complete:
            backfill_trades, reached_end, _ = page_trades(
                max_ts=frontier_ts, stop_at_ts=None,
                max_pages=backfill_max_pages, getter=getter,
            )
            all_trades.extend(backfill_trades)
            ts_values = [t["created_ts"] for t in backfill_trades if t["created_ts"] is not None]
            if ts_values:
                frontier_ts = min(frontier_ts, min(ts_values)) if frontier_ts is not None else min(ts_values)
            backfill_complete = reached_end
            reached_absolute_end = reached_end

    added_by_month = append_trades(all_trades)

    tickers_seen = sorted(set(t["ticker"] for t in all_trades if t.get("ticker")))
    snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    meta_summary = snapshot_metadata(tickers_seen, snapshot_date, getter=getter)

    state = {
        "newest_ts": newest_ts,
        "frontier_ts": frontier_ts,
        "backfill_complete": backfill_complete,
        "last_run_utc": datetime.now(timezone.utc).isoformat(),
    }
    save_state(state)

    return {
        "trades_pulled": len(all_trades),
        "trades_new_by_month": added_by_month,
        "tickers_seen": len(tickers_seen),
        "newest_ts": newest_ts,
        "frontier_ts": frontier_ts,
        "backfill_complete": backfill_complete,
        "reached_absolute_end_this_run": reached_absolute_end,
        "meta": meta_summary,
    }


def _fmt_ts(ts: float | None) -> str:
    if ts is None:
        return "n/a"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Archive Kalshi public trades before they age out.")
    parser.add_argument("--catchup-max-pages", type=int, default=CATCHUP_MAX_PAGES,
                         help="page budget for catching up to new trades this run")
    parser.add_argument("--backfill-max-pages", type=int, default=BACKFILL_MAX_PAGES,
                         help="page budget for walking further back into history this run")
    args = parser.parse_args(argv)

    summary = run(catchup_max_pages=args.catchup_max_pages,
                  backfill_max_pages=args.backfill_max_pages)

    print(f"trades pulled this run: {summary['trades_pulled']}")
    for month, n in sorted(summary["trades_new_by_month"].items()):
        print(f"  {month}.parquet: +{n} new rows")
    print(f"distinct tickers seen: {summary['tickers_seen']}")
    print(f"newest trade archived: {_fmt_ts(summary['newest_ts'])}")
    print(f"oldest trade archived (backfill frontier): {_fmt_ts(summary['frontier_ts'])}")
    print(f"backfill complete: {summary['backfill_complete']}")
    print(f"market metadata snapshotted: {summary['meta']['markets_snapshotted']} markets, "
          f"{summary['meta']['series_snapshotted']} series")
    return 0


if __name__ == "__main__":
    sys.exit(main())
