# 000m — 014 drafting: book losses computed on out-of-calendar stress weeks

Ledger entry, not a hypothesis. Written 2026-10-12 by the orchestrator,
before the computation it records was run.

## What is authorised

The 014 drafter computes the H24 short-vol book's realistic hedged loss on
real Deribit data, **only** for Friday-08:00 entries whose 7-day hold ends
before 2021-03-24 00:00 UTC (DVOL start and the start of 014's discovery
calendar). Weeks are selected only by the underlying's weekly excursion:
the 2020-03 week (from 2020-03-06), the 2021-01 week (from 2021-01-01), and
any other pre-2021-03-24 week with a BTC or ETH weekly excursion of at least
25%, listed before any book number is computed. The mechanics are 014's
DRAFT mechanics, unchanged. Purpose: replace the adversarial tail bound
that sets 014's size and tail charge.

## What this is and is not

- These weeks lie outside both 014 calendars (discovery 2021-03-24 →
  2023-12-31; holdout 2024-01-01 → 2026-06-30). The numbers are payoffs on
  real prices, so they are *seen*, and they inform sizing only.
- They are selected for being the worst weeks, so they are a tail
  measurement, not an estimate of the premium.

## What must not follow from it

- No entry, instrument, hedge, criterion or calendar of 014 may change
  because of these numbers. Only n (the size), the tail charge and the
  pre-registered worst week/month may be set from them, and 014 says so.
- Any later short-vol registration that uses pre-2021-03-24 Deribit data as
  discovery states that its worst weeks were seen here.
