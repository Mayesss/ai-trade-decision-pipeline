# 000h — 012 drafting: reads and one incidental print

Ledger entry, not a hypothesis. Written 2026-10-11 by the orchestrator,
from the 012 drafter's report.

## What happened

Every read was end-bounded before 2026-07-01, and 0 rows on or after it
were received. No P&L, funding accrual or gap change was computed.
- **New fetch.** Bybit 240-minute klines for 209 symbols, 2024-11-01 →
  2026-06-30 (188 with rows), into `data/carry012/`.
- **Stage-1 panel.** Built from 010's registered panel plus its raw
  Binance klines and HL candles. The rebuild is byte-identical.
- **Printed counts.**
  - Unconditional opened counts for HL pairs in 2025-01-15 → 2026-06-30:
    679 (HL–BN 389, HL–BY 290; 533 in 2025, 146 in 2026-H1).
  - |s| and cost quantiles at entry.
  - 337 discovery positions under 012's rule.
  - These are not split by position state (`000f`).
- **Incidental print.** About 600 characters of the then-uncommitted 007
  result row: per-date counts only, no outcome fields. 007 has since been
  run and recorded, so this changes nothing.

## What must not follow from it

No 012 threshold may be chosen from these counts beyond the entry-rate
input to power, which the registration states.
