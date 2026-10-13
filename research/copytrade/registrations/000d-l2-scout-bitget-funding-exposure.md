# 000d — L2 scout: Bitget ARBUSDT funding rows from the holdout printed

Ledger entry, not a hypothesis. Written 2026-10-11 by the orchestrator, from
the L2 carry scout's report (`research/copytrade/scouts/l2-carry.md`, the
"Holdout incident" section, which holds the exact values).

## What happened

The scout ran `head -c 400` on a pre-existing cache file,
`data/cache/bg-funding/113661ad35e25a2321da967e.json` (Bitget ARBUSDT
funding, key `ARBUSDT:2026-10-07`). The orchestrator's scope addition had
pointed the scout at that cache without knowing it was holdout-dated. The
command printed fifteen 8-hourly settlements, 2026-07-09 16:00 → 2026-07-14
08:00 UTC, all inside the holdout.

## What was seen

Fifteen Bitget ARBUSDT funding rates (mostly the 0.01% default), printed to
the terminal only. Nothing was saved or computed from them. A later count
(rows dropped at load, counts printed) found the whole `bg-funding` cache is
holdout-dated: 0 rows before 2026-07-01 and 7,783 on or after. The
`hl-funding` cache (end 2026-07-10) also extends into the holdout. Only its
first 600 bytes were read, and they held only 2025-10-20 rows.

## What must not follow from it

- No Bitget-based or ARB-based threshold, cascade or carry rule may be
  chosen because of these rows.
- Any registration using Bitget funding must not use 2026-07-09/14 as
  discovery evidence.

## Mitigation

- `data/cache/bg-funding/` is holdout-only data and is read only by a
  REGISTERED rule's holdout run.
- Readers of `data/cache/hl-funding/` filter `time < 1782864000000` at load.
- Scout briefs name a cache only after its date range has been checked by
  count, never by printing.
