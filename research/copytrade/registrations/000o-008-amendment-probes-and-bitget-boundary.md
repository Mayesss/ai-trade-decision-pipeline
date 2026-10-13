# 000o — 008 Amendment 1: a probe that returned holdout funding rows, and the Bitget boundary erratum

Ledger entry, not a hypothesis. Written 2026-10-13 by the orchestrator,
from 008's Run note 1 and Amendment 1 (`008-inverted-positioning-holdout.md`
§16).

## What happened

1. **Holdout rows in memory.** To measure HL `fundingHistory`'s page cap,
   the 008 drafter asked for BTC funding 2026-06-15 → 2026-07-15. HL
   returned 500 rows to about 2026-07-05 18:00, so about 138 rows dated on
   or after 2026-07-01 sat in process memory. Only the count (500), the
   span (498 h) and sortedness were printed. No funding value was printed,
   saved or computed. The same series is part of 008's own claimed holdout
   reads (D4), and attempt 1 had already stored it through the guard.
2. **The orchestrator's probe** of Bitget `history-candles` (BTCUSDT 1H,
   one block from 2026-05-27, discovery period) printed counts and
   offsets only.
3. **Erratum, programme-wide.** Bitget's `endTime` is exclusive. Besides
   008's frozen fetch (fixed by Amendment 1), `ct/bitget.py`'s `candle_1h`
   and 1-minute `_block` request `start + BLOCK − step`, so they drop the
   last hour (or minute) of every 200-step block. 001 (T2, T3, replay) and
   the forced-flow registrations that read Bitget candles through them have
   had about 1 in 200 bars missing. Those registrations are closed; their
   results stand as registered (one run each). The effect is a sparse,
   outcome-independent gap; how each consumer treats a missing bar decides
   whether it can bias, and none was re-run. `ct/bitget.py` is left
   unchanged so that closed runs stay reproducible.

## What must not follow from it

- No 008 parameter changes. The funding rows seen in memory set nothing.
- Any new registration that reads Bitget candles uses a fetcher with
  `endTime = start + BLOCK`, tested against an exclusive-end mock.

## Mitigation

Amendment 1 fixes 008's fetch. The next-round prompt carries the erratum.

## Addendum (2026-10-13, before fetch attempt 2; Amendment 1 recheck A1/A2)

- **A2, fill reuse.** Attempt 1 recorded no fill hashes. The orchestrator listed the 33 holdout fill files (mtime, bytes, path) before attempt 2: every mtime is at or before attempt 1's end commit `e069efb` (committer time 1791861876; max file mtime 1791858951); total 2,739,624,605 bytes; no other files in those partitions. `data/r008/p3_gates.json` is deleted so G1/G2 and the G3 sample re-run on all 33 days. Attempt 2 records the hashes.
- **A1, procedural check (binding on the orchestrator).** Before the read's approval line is written, the fetched row's `registration_commit` must equal `963dcc8` (full sha recorded at that time), and `git diff 2bd855a..963dcc8` must touch only `r008_fetch.py`, `r008_run.py` among guarded code (verified by the reviewer). Both checks are recorded here before approval.

```
1791851386 117555970 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-01/fills.parquet
1791851618 110069975 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-02/fills.parquet
1791851834 94038637 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-03/fills.parquet
1791852043 80165246 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-04/fills.parquet
1791852209 74630034 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-05/fills.parquet
1791852446 108670981 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-06/fills.parquet
1791852708 100083135 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-07/fills.parquet
1791852994 99241868 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-08/fills.parquet
1791853302 80251765 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-09/fills.parquet
1791853536 79029366 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-10/fills.parquet
1791853734 69145850 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-11/fills.parquet
1791853919 65644173 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-12/fills.parquet
1791854245 93374789 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-13/fills.parquet
1791854546 91085944 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-14/fills.parquet
1791854776 94766310 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-15/fills.parquet
1791855039 96735825 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-16/fills.parquet
1791855375 99235414 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-17/fills.parquet
1791855538 56564525 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-18/fills.parquet
1791855715 59026438 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-19/fills.parquet
1791855967 88783424 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-20/fills.parquet
1791856195 82266911 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-21/fills.parquet
1791856457 81767264 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-22/fills.parquet
1791856723 76128789 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-23/fills.parquet
1791856942 72742584 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-24/fills.parquet
1791857093 53542591 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-25/fills.parquet
1791857277 63948205 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-26/fills.parquet
1791857593 93850698 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-27/fills.parquet
1791857868 85177514 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-28/fills.parquet
1791858115 98026583 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-29/fills.parquet
1791858328 72037091 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-30/fills.parquet
1791858576 80547272 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-07-31/fills.parquet
1791858732 56966161 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-08-01/fills.parquet
1791858951 64523273 data/pruned/by_dex/hyperliquid/fills/perp/all/date=2026-08-02/fills.parquet
```
