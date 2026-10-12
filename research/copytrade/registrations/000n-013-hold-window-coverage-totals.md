# 000n — 013 drafting: coverage totals over the hold windows were outcome-adjacent

Ledger entry, not a hypothesis. Written 2026-10-13 by the orchestrator,
from the second independent review of 013 (`reviews/013-review-b.md`, M5),
checked against 013 §2.4.

## What happened

013's build counted coverage over each event's whole window (d0 … d0+9) and
printed calendar totals in §2.4, for all three calendars including the
confirmation calendar (2025-07-28 → 2026-06-21). Per-event values are
stored in `data/r013/panel_*/events.json`, readable without a guard.

## What was seen (calendar totals only)

- **Funding rows in d0 … d0+9:** 8,441 / 15,578 / 9,551 (reported /
  discovery / confirmation), over 255 / 257 / 148 events. Settlement
  switches from 4 h to 1 h happen when funding hits its cap, typically in a
  squeeze, so the excess of rows over the scheduled cadence is an aggregate
  of capped-funding episodes. For the confirmation calendar: about 6.45
  rows per event-day against 6 at a 4 h cadence.
- **Zero-trade 1m rows:** 42,236 / 3,068 / 303. Zero-trade rows inside a
  window encode halts and dead tails.
- **Missing minutes inside a window:** 1 event / 660 min; 0; 1 event /
  1,440 min.

No price, return, funding *rate* or P&L was computed, and no count was
split by any outcome.

## What must not follow from it

- No 013 parameter (entry delay, hold, sizing, caps, T2's funding rule,
  stress multipliers) may change because of these totals.
- The confirmation calendar remains 013's decision calendar, read as an
  existence test; these totals are disclosed in 013 §13 as weak,
  aggregate exposure about squeeze frequency.

## Mitigation

013 Revision 1 limits per-event coverage to pre-entry facts, rebuilds the
panels, and gates `events.json` behind the guard.
