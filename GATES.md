# PRE-REGISTERED GATES: frozen 2026-07-19

> These rules are written down and approved BEFORE any analysis of the
> archived data exists. Bandwatch has not produced a single price-band
> curve yet: this file is registered first, on purpose, so results can't
> talk us into moving the goalposts later. Same house rule as
> MechLab/TrendLab/mm_bot: a failed gate is a published negative verdict,
> not a tuned re-run.

Bandwatch's job right now is only the archiver: pulling Kalshi's public
trades and metadata before they age out of the 60-65 day retention window.
The monthly price-band curve analysis is a later phase and is not built
yet. These gates apply to that later phase, registered now while the
archiver is still new and nobody has looked at a single result.

---

## Gate 1: the archive has to actually match Kalshi's own numbers

For at least **95% of markets**, our archived trade count (summed contract
count per ticker) has to reconcile with Kalshi's own `volume_fp` field for
that market, within **2%**.

If fewer than 95% of markets reconcile that closely, halt. Do not analyze
the data further until the gap is understood and fixed. A miss here almost
always means paging, dedup, or a cursor edge case dropped or double
counted trades, not that Kalshi's own number is wrong.

## Gate 2: power gate

Before trusting any price-band claim, at least **5 of the 10** one-cent
price bands from 1 cent to 30 cents need a bootstrap 95% confidence
interval narrower than **4 cents**. If fewer than 5 bands clear that bar,
the sample is too thin to say anything yet: keep archiving, don't publish
a curve.

## Gate 3: replication sanity check

The first full analysis has to show **negative post-fee taker returns in
the 1-30 cent price bands**. That is the core finding of the GWU paper
this whole project replicates (see README for the paper link). If the
first analysis does not show that pattern, one of two things is true:
either there has been a real regime change worth documenting, or there is
a join bug in the pipeline. Default assumption is the join bug. Halt and
find it before publishing anything, unless the regime change can be
documented directly (a fee change, a rule change, something concrete, not
just "the numbers came out different").

## Gate 4: calendar gate

We need at least **4 weeks of archived pre-September-1 data** in hand by
**August 31, 2026**. If we don't have that by then, the maker-subsidy
natural experiment (Kalshi's program ending September 1) is dropped from
every claim this project makes. A half-baseline is not a baseline: no
squinting at 10 days of data and calling it "before."

## Gate 5: scope guard

Bandwatch publishes curves. That is all it does. No simulated trading bot
may consume Bandwatch's output until that bot has registered its own
GATES.md, the same way this file exists before any analysis. A curve is
not a strategy, and this project does not become one by accident.

---

## Verdict

- **Passes gates 1-4** for a given analysis window → the curve for that
  window is trustworthy enough to publish and reference elsewhere.
- **Fails any gate** → publish the honest miss, fix what's fixable, and do
  not let a partial result get quoted as if it were a full one.

Gate 5 is a standing rule, not a per-run check: it never "passes," it just
holds until something else earns its own gates doc.

## Amendment log

- 2026-07-19: initial registration, before the archiver has completed a
  single full backfill and before any analysis code exists. Any later
  edit must be logged here with its reason, and may only ADD a gate or
  TIGHTEN an existing one.
