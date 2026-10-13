# 000p — A1 scout: house-side products, current-state reads and post-cutoff facts

Ledger entry, not a hypothesis. Written 2026-10-13 by the orchestrator, from
the A1 scout's report (`research/alpha/scouts/a1-house-side.md` §5).

## What happened

1. **Series pulled, all end-dated 2026-06-30** (orchestrator re-check: 15
   series, 0 rows on or after 2026-07-01, checksums pass), into
   `research/alpha/data/scout_a1/`: Lighter LLP (529 days) and XLP (280)
   daily `/pnl`; GMX GM ETH/BTC daily prices (from 2023-07-05) with fee-APR
   and trader-PnL-APR snapshots (from 2023-09-26); GLV ETH/BTC (from
   2024-09); JLP market price, cap and volume (259 days from 2025-10-15).
   **Only coverage counts were computed.** No return, mean, Sharpe,
   drawdown or volatility of any NAV, price or APR series.
2. **Lighter `/pnl` returned 2 rows per pool on or after 2026-07-01**
   despite the end timestamp. They were dropped in memory, never printed or
   written.
3. **Current-state metadata and docs:** Lighter `systemConfig` (key names
   and pool index), GMX `/glvs` (a GLV listed 2026-10-04), GMX GraphQL
   schema and changelog (2026-07-14 → 09-01), GeckoTerminal JLP pool list,
   Pyth JLP feed metadata, current docs of Jupiter, Lighter, Aster,
   Paradex, GMX (incl. a "USDG launch boost" 8% APR target), gTrade, edgeX,
   HIP-3, Kinetiq.
4. **Post-cutoff facts read:** the Ostium OLP drain on 2026-07-15 (−72%);
   Ostium's redesign routing directional flow off-pool; Pyth Benchmarks
   keyed or deprecated.
5. **Return-like snippets seen in search results, not used:** current or
   undated LLP, GLV, Paradex Gigavault APR/TVL figures; a valuation implying
   a HYPE price.

## What must not follow from it

- No house-product rule may be sized or selected from the current-state
  APR/TVL snippets.
- The Ostium drain may inform the **exploit base rate** (it is a tail fact,
  stated as post-cutoff), not any return estimate.
- Any registration on LLP, GMX or JLP series states that coverage (not
  values) was read through 2026-06-30, and that 2026-07-01 → is unread.

## Mitigation

A1 spends no trial (programme doc §3, 2026-10-13). A later registration
re-pulls with its own end date and filters rows server-returned past it.
