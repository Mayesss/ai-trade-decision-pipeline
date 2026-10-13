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
