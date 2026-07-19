"""Bandwatch archiver.

This script saves Kalshi's public trades before they age out of Kalshi's
own API. Kalshi's free /markets/trades endpoint only keeps about 60 to 65
days of history. Once a trade rolls off that window, it is gone for good
unless something already saved it. That is the whole reason this script
exists.

What it does, every time you run it:

1. Pull new trades from GET /markets/trades (paged, newest first). Every
   trade has a trade_id, so we can always tell a trade we already saved
   from a new one.
2. Group those trades into daily per-band aggregates: for every (UTC
   date, market ticker, price band in cents 1-99, taker side), add up
   how many trades happened, how many contracts, and how many dollars.
   That goes to data/agg/YYYY-MM.parquet. This is the default and the
   only thing the daily cron job writes: see "Why aggregates, not raw
   ticks" in README for the full reasoning, but the short version is a
   live measurement showed Kalshi's public trade firehose runs about 8
   to 9 million trades a day, which turns into roughly 470-500 MB/day of
   raw parquet: past GitHub's 100 MB file limit within a day or two of
   running. Daily band aggregates are tiny by comparison and are all the
   price-band curve analysis in GATES.md actually needs.
3. Optionally, if run with --raw, also write every individual trade to a
   monthly parquet file under data/raw/YYYY-MM.parquet. This is local
   only: data/raw/ is gitignored, so nothing there ever gets committed.
   It exists for anyone who wants full tick-level detail on their own
   machine, not for the repo.
4. Keep chipping away at older history in the background (a separate
   "backfill" pointer that works backward in time, a bit further each
   day) so we grab as much of the pre-existing window as we can before it
   rolls off.
5. Take a daily snapshot of market metadata (status, result, close_time,
   volume_fp) and series metadata (fee_type, fee_multiplier, category) for
   every ticker that traded that day, so a later analysis can check our
   trade counts against Kalshi's own volume numbers.

Run it once a day. It is safe to run it twice in the same day too: nothing
gets double counted. Aggregation only ever folds in trades that have never
been aggregated before (see dedupe_for_aggregation), raw trades (--raw) are
deduped by trade_id same as always, and metadata is deduped by
(snapshot_date, ticker).

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
RAW_DIR = DATA_DIR / "raw"     # --raw only; gitignored, local machine only
AGG_DIR = DATA_DIR / "agg"     # the default: daily per-band aggregates, tracked in git
META_DIR = DATA_DIR / "meta"
STATE_FILE = META_DIR / "state.json"
MARKETS_META_FILE = META_DIR / "markets.parquet"
SERIES_META_FILE = META_DIR / "series.parquet"
HOT_TRADES_FILE = META_DIR / "hot_trades.parquet"

# How many rows the hot-trades dedupe buffer keeps at each edge (the
# newest trades and the oldest/frontier trades). See dedupe_for_aggregation
# for why this can stay small no matter how many trades a run pulls.
HOT_BUFFER_KEEP = 3000

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
            if resp.status_code == 404:
                # A clean "not found," not a glitch. Some callers (see
                # resolve_series_tickers) deliberately probe a few
                # candidate URLs and expect some to 404, so this returns
                # right away instead of burning retries and a warning on
                # an outcome that is already handled.
                return None
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
    """The most common shape of a Kalshi market ticker is
    SERIESTICKER-EVENTDATE-STRIKE (e.g. 'KXHIGHLAX-26JUL19-T80' ->
    'KXHIGHLAX'), confirmed live against GET /events/{ticker}. This is
    just the first-guess prefix, not a guarantee: some series tickers
    have a dash baked into them (see resolve_series_tickers below), so
    this alone is not enough to find every market's real series."""
    return ticker.split("-", 1)[0]


def resolve_series_tickers(market_tickers: list[str], getter=http_get_json,
                            max_segments: int = 3) -> dict[str, str]:
    """Map each market ticker to its real series ticker, confirmed live
    against GET /series/{candidate}.

    Most series tickers are just the market ticker's first dash segment
    ('KXHIGHLAX-26JUL19-T80' -> 'KXHIGHLAX'). But some sports series carry
    a dash inside the series ticker itself: 'KXMLBWINS-LAD-26-T95' is
    really series 'KXMLBWINS-LAD' (one series per team), which 404s on
    the plain first-segment guess. Confirmed live on 2026-07-19: 2 of 722
    series lookups in one run 404'd for exactly this reason.

    This tries the 1-segment guess first, and only for tickers where that
    404s, tries the 2-segment guess, then 3, grouping tickers that share a
    candidate so each candidate URL is only ever requested once. Tickers
    that still do not resolve within `max_segments` are left out of the
    returned dict; callers should treat a missing ticker as "series
    metadata unavailable," not raise.
    """
    remaining = {t: t.split("-") for t in set(market_tickers) if t}
    resolved: dict[str, str] = {}

    for n in range(1, max_segments + 1):
        if not remaining:
            break
        candidates: dict[str, list[str]] = {}
        for ticker, segments in remaining.items():
            if n > len(segments):
                continue  # this ticker has no more segments left to try
            candidate = "-".join(segments[:n])
            candidates.setdefault(candidate, []).append(ticker)

        for candidate, tickers in candidates.items():
            data = getter(f"{KALSHI_BASE}/series/{candidate}")
            series = (data or {}).get("series")
            if not series:
                continue
            real_ticker = series.get("ticker", candidate)
            for t in tickers:
                resolved[t] = real_ticker
                remaining.pop(t, None)

    return resolved


def trade_date(created_time: str) -> str | None:
    """'2026-07-19T03:52:14.21Z' -> '2026-07-19', the UTC calendar date
    used to group daily aggregates. Returns None for empty input."""
    if not created_time:
        return None
    return created_time[:10]


def taker_price(trade: dict) -> float | None:
    """The price the taker actually paid: yes_price if they took the yes
    side, no_price if they took the no side. Returns None if taker_side
    isn't one of the two known values, so a bad/missing side gets left
    out of the aggregate instead of silently banded under the wrong
    price."""
    side = trade.get("taker_side")
    if side == "yes":
        return trade.get("yes_price")
    if side == "no":
        return trade.get("no_price")
    return None


def band_from_price(price_dollars: float | None) -> int | None:
    """Turn a dollar price like 0.12 into a 1-99 cent price band. Returns
    None if the price is missing or outside the valid 1-99 range (a price
    of exactly $0 or $1 means one side of the contract was free, which can
    happen right around settlement and isn't a normal trading band)."""
    if price_dollars is None:
        return None
    cents = round(price_dollars * 100)
    if cents < 1 or cents > 99:
        return None
    return int(cents)


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
# Parquet storage: raw trades (optional, --raw only, local machine only)
# ---------------------------------------------------------------------------
#
# Everything in this section is unchanged from the original archiver except
# that it now writes under data/raw/ instead of data/trades/, and it only
# runs when --raw is passed. data/raw/ is gitignored: nothing here is ever
# committed. It's here for anyone who wants full tick-level detail on their
# own disk. The default path the daily cron job actually uses is the
# aggregate section further down.

TRADE_COLUMNS = [
    "trade_id", "ticker", "created_time", "created_ts", "count",
    "yes_price", "no_price", "taker_side", "taker_book_side", "is_block_trade",
]


def _empty_trades_df() -> pd.DataFrame:
    return pd.DataFrame(columns=TRADE_COLUMNS)


def load_month_parquet(month: str) -> pd.DataFrame:
    """Load an existing monthly raw-trade file, or an empty frame with the
    right columns if it does not exist yet."""
    path = RAW_DIR / f"{month}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return _empty_trades_df()


def save_month_parquet(month: str, df: pd.DataFrame) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"{month}.parquet"
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
    """Group trades by month and append them to the right monthly raw
    parquet file under data/raw/, deduped by trade_id. Returns {month:
    new_row_count_added}. Only called when --raw is passed."""
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
# Bounded trade-id watermark for aggregation idempotency
# ---------------------------------------------------------------------------
#
# Aggregation needs the same guarantee raw storage always had: run the
# archiver twice on the same trades and nothing gets double counted. The
# old way to know "have I already saved this trade_id" was to check the
# full trade history on disk. That still works when --raw is on, but it is
# not available by default any more, since the whole point of aggregating
# is to stop keeping that full history in git.
#
# The fix leans on how page_trades actually overlaps between runs (see its
# docstring): a run can only ever re-fetch trades right at its own current
# watermark (newest_ts) or its own current backfill frontier (frontier_ts),
# bounded to roughly one page's worth (PAGE_LIMIT) on each side. It can
# never re-fetch trades from the middle of a previous run's haul. So this
# only needs to remember trade_ids near those two edges, not every trade
# ever aggregated. That is what keeps this buffer small (HOT_BUFFER_KEEP
# rows per edge) even though a single run can pull millions of trades.

def load_hot_trades() -> pd.DataFrame:
    if HOT_TRADES_FILE.exists():
        return pd.read_parquet(HOT_TRADES_FILE)
    return _empty_trades_df()


def save_hot_trades(df: pd.DataFrame) -> None:
    META_DIR.mkdir(parents=True, exist_ok=True)
    if df.empty:
        df = _empty_trades_df()
    df.to_parquet(HOT_TRADES_FILE, index=False)


def trim_hot_trades(df: pd.DataFrame, keep: int = HOT_BUFFER_KEEP) -> pd.DataFrame:
    """Keep only the newest `keep` rows and the oldest `keep` rows by
    created_ts (plus any rows with no timestamp, which get kept as-is
    since they cannot be dropped safely). That covers both edges a future
    run's boundary page could touch (see module note above) without
    keeping anywhere near a full day of trades around."""
    if len(df) <= keep * 2:
        return df.drop_duplicates(subset="trade_id", keep="last")
    with_ts = df.dropna(subset=["created_ts"]).sort_values("created_ts")
    without_ts = df[df["created_ts"].isna()]
    trimmed = pd.concat([with_ts.head(keep), with_ts.tail(keep), without_ts])
    return trimmed.drop_duplicates(subset="trade_id", keep="last")


def dedupe_for_aggregation(fetched_trades: list[dict]) -> list[dict]:
    """Filter `fetched_trades` down to the ones never folded into an
    aggregate before, using the hot-trades buffer as a bounded stand-in
    for "the full trade history," then refresh that buffer.

    Feeding the exact same trades in twice in a row (a same-day re-run)
    returns an empty list the second time, since every trade_id will
    already be in the buffer from the first call.
    """
    if not fetched_trades:
        return []
    existing = load_hot_trades()
    already_seen = set(existing["trade_id"]) if not existing.empty else set()
    merged = dedupe_trades(existing, fetched_trades)
    new_rows = merged[~merged["trade_id"].isin(already_seen)]
    save_hot_trades(trim_hot_trades(merged))
    return new_rows.to_dict("records")


# ---------------------------------------------------------------------------
# Parquet storage: daily per-band aggregates (the default)
# ---------------------------------------------------------------------------

AGG_COLUMNS = ["date", "ticker", "band_cents", "taker_side", "trade_count", "contracts", "dollars"]
AGG_GROUP_KEY = ["date", "ticker", "band_cents", "taker_side"]


def aggregate_trades(trades: list[dict]) -> pd.DataFrame:
    """Group trades into (UTC date, ticker, price band in cents, taker
    side) buckets and sum trade_count / contracts / dollars for each
    bucket. Trades missing a ticker, a usable taker side/price, or a band
    in the valid 1-99 cent range are left out rather than guessed at."""
    buckets: dict[tuple, dict] = {}
    for t in trades:
        date = trade_date(t.get("created_time"))
        ticker = t.get("ticker")
        side = t.get("taker_side")
        price = taker_price(t)
        band = band_from_price(price)
        count = t.get("count")
        if not date or not ticker or side not in ("yes", "no") or band is None or count is None:
            continue
        key = (date, ticker, band, side)
        bucket = buckets.setdefault(key, {"trade_count": 0, "contracts": 0.0, "dollars": 0.0})
        bucket["trade_count"] += 1
        bucket["contracts"] += count
        bucket["dollars"] += count * price

    rows = [
        {"date": date, "ticker": ticker, "band_cents": band, "taker_side": side, **vals}
        for (date, ticker, band, side), vals in buckets.items()
    ]
    return pd.DataFrame(rows, columns=AGG_COLUMNS)


def load_month_agg(month: str) -> pd.DataFrame:
    """Load an existing monthly aggregate file, or an empty frame with the
    right columns if it does not exist yet."""
    path = AGG_DIR / f"{month}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame(columns=AGG_COLUMNS)


def save_month_agg(month: str, df: pd.DataFrame) -> None:
    AGG_DIR.mkdir(parents=True, exist_ok=True)
    path = AGG_DIR / f"{month}.parquet"
    df = df.sort_values(AGG_GROUP_KEY, kind="stable").reset_index(drop=True)
    df.to_parquet(path, index=False)


def merge_aggregates(existing: pd.DataFrame, new_agg: pd.DataFrame) -> pd.DataFrame:
    """Merge new aggregate rows into an existing monthly frame by ADDING
    onto any bucket that already exists for the same group key, not
    overwriting it. This is safe because the caller only ever passes in
    aggregates built from trades dedupe_for_aggregation confirmed were
    never aggregated before, so every row here is a genuinely new
    contribution to its bucket."""
    if new_agg.empty:
        return existing
    combined = new_agg if existing.empty else pd.concat([existing, new_agg], ignore_index=True)
    grouped = combined.groupby(AGG_GROUP_KEY, as_index=False)[["trade_count", "contracts", "dollars"]].sum()
    return grouped[AGG_COLUMNS]


def append_aggregates(trades: list[dict]) -> dict[str, int]:
    """Aggregate `trades` and merge the result into the right monthly
    data/agg/YYYY-MM.parquet file(s), summing onto existing buckets.
    Returns {month: new_bucket_row_count_added} (not trade counts: a
    "row" here is a (date, ticker, band, side) bucket)."""
    new_agg = aggregate_trades(trades)
    if new_agg.empty:
        return {}
    new_agg = new_agg.assign(month=new_agg["date"].str[:7])

    added: dict[str, int] = {}
    for month, group in new_agg.groupby("month"):
        group = group.drop(columns="month")
        existing = load_month_agg(month)
        before = len(existing)
        merged = merge_aggregates(existing, group)
        save_month_agg(month, merged)
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

    resolved = resolve_series_tickers(tickers, getter=getter)
    series_rows = fetch_series_meta(sorted(set(resolved.values())), getter=getter)
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


def sum_contracts_by_ticker_from_aggregates(agg_rows: list[dict]) -> dict[str, float]:
    """Sum the 'contracts' field of aggregate rows, grouped by ticker. This
    is what feeds GATES.md gate 1 now that the archive stores daily
    per-band aggregates instead of one row per trade: same reconciliation
    math as sum_counts_by_ticker, just summing 'contracts' across every
    band and taker side for a ticker instead of summing 'count' across
    every raw trade for a ticker. `agg_rows` can be any iterable of dicts
    with 'ticker' and 'contracts' keys, e.g. a data/agg parquet file's
    df.to_dict('records')."""
    totals: dict[str, float] = {}
    for row in agg_rows:
        ticker = row.get("ticker")
        contracts = row.get("contracts")
        if not ticker or contracts is None:
            continue
        totals[ticker] = totals.get(ticker, 0.0) + contracts
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
        write_raw: bool = False,
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

    new_for_aggregation = dedupe_for_aggregation(all_trades)
    agg_added_by_month = append_aggregates(new_for_aggregation)

    raw_added_by_month: dict[str, int] = {}
    if write_raw:
        raw_added_by_month = append_trades(all_trades)

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
        "trades_new_for_aggregation": len(new_for_aggregation),
        "agg_buckets_new_by_month": agg_added_by_month,
        "raw_enabled": write_raw,
        "trades_new_by_month": raw_added_by_month,
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
    parser.add_argument("--raw", action="store_true",
                         help="also write every individual trade to data/raw/YYYY-MM.parquet "
                              "(local only: data/raw/ is gitignored, never committed)")
    args = parser.parse_args(argv)

    summary = run(catchup_max_pages=args.catchup_max_pages,
                  backfill_max_pages=args.backfill_max_pages,
                  write_raw=args.raw)

    print(f"trades pulled this run: {summary['trades_pulled']}")
    print(f"trades new to aggregation (never seen before): {summary['trades_new_for_aggregation']}")
    for month, n in sorted(summary["agg_buckets_new_by_month"].items()):
        print(f"  data/agg/{month}.parquet: +{n} new (date, ticker, band, side) buckets")
    if summary["raw_enabled"]:
        for month, n in sorted(summary["trades_new_by_month"].items()):
            print(f"  data/raw/{month}.parquet: +{n} new rows")
    else:
        print("raw per-trade storage: off (pass --raw to also write data/raw/, local only)")
    print(f"distinct tickers seen: {summary['tickers_seen']}")
    print(f"newest trade archived: {_fmt_ts(summary['newest_ts'])}")
    print(f"oldest trade archived (backfill frontier): {_fmt_ts(summary['frontier_ts'])}")
    print(f"backfill complete: {summary['backfill_complete']}")
    print(f"market metadata snapshotted: {summary['meta']['markets_snapshotted']} markets, "
          f"{summary['meta']['series_snapshotted']} series")
    return 0


if __name__ == "__main__":
    sys.exit(main())
