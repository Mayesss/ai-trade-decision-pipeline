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

## Addendum (2026-10-13, after the computation)

- **Selected (from underlying excursion alone, before any book number):**
  21 Friday entries 2019-03-29 → 2021-03-05, plus the 2020-03 cluster
  (entries 2020-02-14 → 04-03) for its 4-week path only. Every hold ends
  before 2021-03-24. Results in `data/r014/tail_pre2021/tail_results.json`
  (orchestrator re-read: worst −14.1% of notional on 2019-10-25; sum of the
  21 weeks −67.0%; best +5.7%; no liquidation at n 0.10 / 0.25 / 0.45).
- **Beyond the authorisation:** the drafter also computed the *unhedged*
  straddle on the same 28 out-of-calendar weeks, as a reported comparison
  (2020-03-06: −36% BTC / −40% ETH; 2021-01-01: −25% / −52%). Same weeks,
  same calendar status; it sets nothing. Kept, disclosed.
- **What it set:** n = 0.40 (¼ of fat-world tail-aware Kelly at VRP 0.10,
  capped by the adversarial bound at 0.46), the realistic tail charge
  (0.172% of notional a week), and the registered realistic worst week and
  month. Nothing else in 014 changed.
