# 000g — 009 and 010 share Binance data; two outcome-adjacent statistics in 009 drafting

Ledger entry, not a hypothesis. Written 2026-10-11 by the orchestrator,
from the independent review of 009 (`registrations/reviews/009-review.md`,
B1 and M4).

## What happened

1. **Data overlap.** `data/carry/` (010) holds Binance funding 2023-05 →
   2026-06, 8h klines to 2024-12, and daily closes to 2026-06. That covers
   F1's payoff (price plus funding) over 009's holdout A (2024-01-01 →
   2025-07-27) and holdout B, outside 009's guard. 010's discovery run
   (2023-06 → 2024-12) would compute Binance funding-leg P&L across 2024,
   which is holdout A for 009's F1. **No such run has happened.**
   Unconditional Binance funding medians by year were printed by the L2
   scout (aggregate levels, not the cross-sectional carry).
2. **Two outcome-adjacent statistics in 009 drafting.**
   - F2's weekly Spearman with minus the past 28-day return. The past
     window at S overlaps the forward windows of the signals at S−4w … S−1w,
     and F2 is autocorrelated, so this is indirectly forward. It informed
     Revision 2's choice of the contracts version.
   - Leg counts near delisting cuts, used to argue the direction of the
     delisting bias.

## What must not follow from it

- **Ordering rule, binding on both registrations.** 010's runner may not
  compute any outcome on Binance data dated on or after 2024-01-01 until
  009's holdout A is either read (after a discovery pass) or closed (after a
  discovery fail). 009 runs first.
- No 009 code reads `data/carry/`. No 010 code reads `data/binance_years/`.
- F2 is dropped from 009 (rejected on turnover with drift costs included;
  see 009 Revision 3). The F2 statistics above therefore decide nothing
  further.

## Addendum (2026-10-11, after the second 009 review)

Two more programme-local stores hold Binance data inside 009's holdout
periods:
- `data/scout_l2/binance/funding` holds 213 symbols' funding, including
  seven holdout-A months. The L2 scout printed only aggregate yearly
  medians and HL-minus-CEX quantiles from it.
- `data/binance/um` holds six majors' aggTrades on HL-cascade days inside
  holdout B (006's data).

Neither has been used for any 009 statistic. **The ordering rule is now
programme-wide:** no registration computes outcomes on Binance data inside
009's holdout A until 009's holdout A reaches a terminal state (discovery
fail, conditions unmet, owner decline, or read). 009 and 010 read only
whitelisted directories of their own.

## Addendum 2 (2026-10-11, 009 Revision 4)

- **A third outcome-adjacent statistic in 009 drafting.** The market-move
  statistics used in the power simulations are realised equal-weight
  market returns over the discovery outcome windows, not paired with any
  signal. From them, the implied beta P&L of F1's book is about +1.5 bp a
  week. They set no 009 parameter. The beta-adjusted pull condition
  (D13 a) exists because of this kind of exposure.
- **The terminal states** that unlock other registrations' outcomes on
  Binance data dated 2024-01-01 → 2025-07-28 are these 009 rows in
  `results.jsonl`, checked across the tree and `git log --all`:
  - a discovery `close`;
  - a discovery `close_conditions_unmet`;
  - `holdout_A_declined`;
  - `holdout_A`.

  010's `ordering_failures` is aligned to these.
