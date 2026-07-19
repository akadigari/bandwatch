# Bandwatch

**A sensor that watches which Kalshi price bands make money and which ones
lose money, and saves the raw trades before Kalshi's own API forgets them.**

This is the live calibration monitor that survived out of a bigger strategy
factory idea. Everything else got cut for now. This piece got kept because
it is time-critical: if I don't start archiving today, the data I need
later is gone for good.

## Why the clock matters

Kalshi's free public trades endpoint only keeps about 60 to 65 days of
history. Once a trade rolls off that window, there is no way to get it
back, keyless or otherwise.

On September 1, 2026, Kalshi's maker-subsidy program (the "LIP" liquidity
incentive) is set to expire. That is a real natural experiment: trading
economics before and after one clean date. But it only works as an
experiment if I have clean "before" data, and every day that passes
without archiving eats into how much "before" I'll actually have once
September 1 shows up in the rearview mirror. So the archiver goes in now,
runs every day from here forward, and does not wait for the rest of the
project to be built.

## What this actually replicates

The question underneath all of this: **which price bands are structurally
profitable to take, and which ones are a bad trade almost every time?**

This project replicates, monthly, the core finding of a GWU working paper
on prediction market taker returns:

**GWU Working Paper 2026-001**: https://www2.gwu.edu/~forcpgm/2026-001.pdf

That paper found negative post-fee returns for takers in low-price bands
(roughly 1 to 30 cents). Bandwatch's job is to check whether that finding
still holds, month by month, on live Kalshi data, and whether it changes
once the maker subsidy goes away on September 1.

## What's built right now, and what isn't

This repo right now is **only the archiver and the scaffold**. That's on
purpose. The monthly price-band curve analysis (the actual GWU
replication) is a later phase and is not built yet. GATES.md is registered
now, before that analysis exists, so the rules can't get bent later to fit
whatever the data happens to show.

What's here:
- `archiver.py`: pulls trades and metadata, saves them to disk
- `GATES.md`: the pre-registered rules the later analysis has to pass
- `tests/`: offline tests for the archiver's logic (no network calls)
- `.github/workflows/bandwatch.yml`: runs the archiver once a day, automatically

What's not here yet: any chart, any price-band curve, any claim about
whether a price band is actually profitable. That's phase 2.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python archiver.py
```

No API key, no login, nothing to configure. Kalshi's trades and markets
endpoints are public and keyless. The GitHub Actions workflow needs no
secrets either: it just runs `archiver.py` once a day and commits whatever
new parquet files came out of that run.

Run the tests with:

```bash
pytest
```

They're all offline, using canned fixture responses under
`tests/fixtures/`, so they run the same with or without a network
connection.

## Files

- `archiver.py`: the archiver itself. See the module docstring at the
  top of the file for exactly how it pages, dedupes, and snapshots.
- `data/trades/YYYY-MM.parquet`: one file per month, every trade Kalshi
  has given us for that month, deduped by `trade_id`.
- `data/meta/markets.parquet`: a daily snapshot (`status`, `result`,
  `close_time`, `volume_fp`) for every ticker that traded that day.
- `data/meta/series.parquet`: a daily snapshot (`fee_type`,
  `fee_multiplier`, `category`) for every series that traded that day.
- `data/meta/state.json`: where the archiver remembers how far it has
  caught up on new trades and how far back its backfill has reached, so
  the next run picks up where the last one left off instead of
  re-pulling everything from scratch.
- `GATES.md`: the pre-registered gates for the analysis phase.

## Fleet quirks this project inherited

Same API family the owner's `../tipoff` scanner already uses
(`api.elections.kalshi.com/trade-api/v2`), so it inherits the same known
quirks:

- **Dollar-string prices.** Kalshi sends `"0.1200"`, not `0.12`. Every
  price and count field gets parsed through `parse_dollars` /
  `parse_count` rather than trusted as a raw number.
- **`strict=False` JSON parsing.** Kalshi's payloads can carry raw control
  characters in free-text fields, so this uses `json.loads(text,
  strict=False)` instead of calling `.json()` directly on the response.
- **Cursor pagination.** `GET /markets/trades` pages backward in time.
  An empty cursor means there is nothing older left for Kalshi to give us,
  not an error.
- **Series ticker isn't a field on the market object.** It's the prefix of
  the market ticker before the first dash (`KXHIGHLAX-26JUL19-T80` →
  `KXHIGHLAX`), confirmed live against `GET /events/{ticker}`, which does
  return `series_ticker` directly and matches that prefix exactly. Using
  the prefix means one less API call per market.

## What this can't do yet (an honest limit, not a bug)

Kalshi's public trade firehose, unfiltered across every market on the
platform, runs at roughly **8 to 9 million trades a day**, mostly from
bot-driven 15-minute crypto range markets and high-frequency sports
micro-markets. That's real, measured live while building this, not a
guess.

That volume means two different things for the two things this archiver
does:

- **Going forward, from today on, nothing gets lost.** The daily catch-up
  pass is sized to keep up with a full day's new trades, and even if one
  run falls behind, the next run just picks up a bigger backlog: nothing
  falls out of the 60-65 day window from a single slow day.
- **Going backward into history that already exists is best-effort.**
  Walking all the way back through the full ~60-65 day window at this
  trade volume takes many days of runs, chipping away a bounded number of
  pages each day (see the comments above `CATCHUP_MAX_PAGES` and
  `BACKFILL_MAX_PAGES` in `archiver.py`). It may not fully reach the
  oldest available trades before they age out on their own. That's fine:
  the part that actually matters for the September 1 natural experiment
  is capturing every day from now forward, and that part is not
  best-effort, it's guaranteed by the daily cron.

If a deeper one-time backfill matters later, `archiver.py` takes
`--catchup-max-pages` and `--backfill-max-pages` flags so it can be run by
hand with a much bigger budget than the daily job uses.
