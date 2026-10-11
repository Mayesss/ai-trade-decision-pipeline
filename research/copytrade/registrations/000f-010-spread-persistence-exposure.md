# 000f — 010 drafting: spread persistence read, in discovery and in L2's holdout

Ledger entry, not a hypothesis. Written 2026-10-11 by the orchestrator,
from the independent review of registration 010
(`registrations/reviews/010-review.md`, B1). Confirmed by the orchestrator
in the draft's own §3 decision funnel.

## What happened

The 010 drafter counted, for every 8-hour decision time, whether a coin had
a qualifying pair (|trailing-24 h spread| ≥ θ), and separately whether the
coin was already held. The orchestrator's brief allowed "counts of
qualifying entries" in both periods. It did not foresee that splitting
those counts by "already held" turns them into a persistence statistic.

- **Discovery (2023-06 → 2024-12).** Of 133,225 qualifying coin-decisions,
  125,799 fell while the coin was held. Read as the reviewer did: about
  **51% of in-hold decision times still qualify, against about 1.9%
  outside holds.**
- **L2 holdout (2025-01-15 → 2026-06-30, CEX–CEX only).** About **36%
  against 0.6%** (`data/carry/logs/describe.json`).
- The figures were printed by `r010_run.py --events` and
  `r010_describe.py` before any guard. The orchestrator and the reviewer
  have both seen them.

## What was seen

A conditional, forward statistic of the payoff series: given an entry at
|s| ≥ θ, the spread stays beyond θ for about half of the next 13–18 days in
discovery, and about a third in the L2 holdout. That is most of the answer
to "does the funding leg of the carry persist". No P&L, no funding accrual,
no price gap and no cost was computed.

## What must not follow from it

- The L2 holdout (2025-01 → 2026-06) **can no longer decide 010.** It is
  exposed on the payoff's main component. It may be read only as a further
  discovery-grade block, labelled as such.
- No 010 parameter (θ, H, the window, the exit rule) may change because of
  these numbers. θ and H stay set by cost arithmetic. An open question
  about the hold length closes at the drafted value.
- The decisive holdout for 010 must be data nobody has read: funding from
  **2026-07-01 onward**, on every venue, with an end date fixed before any
  of it is fetched. The owner rules on this.

## Mitigation

- Briefs that allow counts must name the forbidden conditionings: no count
  split by position state, or by any future of the signal.
- `--events` modes print only unconditional counts before the guard.
