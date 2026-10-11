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
