# 000c — L1 scout: counts of holdout-period rows seen

Ledger entry, not a hypothesis. Written 2026-10-11 by the orchestrator, from
the L1 vault scout's report (`research/copytrade/scouts/l1-vaults.md`).

## What happened

1. An early schema query in the scout counted the rows of
   `data/derived/hyperliquid/snapshot_accounts.parquet` with
   `snapshot_ms >= 2026-07-01`: **8,651,725**. It also printed all-period
   totals (384 snapshots, 703,272 users). **The local snapshot table holds
   holdout-period snapshots**, about 98 days of them beyond the 286
   discovery snapshots.
2. The list size before the cutoff (9,463) set against the cached list's
   9,477 implies that 14 vaults were created on or after 2026-07-01.
3. Three today's-state vault fields were kept for the sample, as the brief
   allowed: `allowDeposits`, `alwaysCloseOnWithdraw`, `userRole`.

## What was seen

Counts only. No account value, position, return, NAV or P&L dated on or after
2026-07-01 was printed, saved or computed. Every later query in the scout
filtered on the cutoff.

## What must not follow from it

- Nothing. Row counts and the number of vaults created carry no outcome.
- `allowDeposits` (today) must not select vaults in any registration.
  Deposit availability is taken point-in-time from discovery-period
  evidence.

## Mitigation

Any script that reads `snapshot_accounts.parquet` filters
`snapshot_ms < 1782864000000` in the query itself. A registration that uses
the table states the filter, and its guard hashes the code that applies it.
