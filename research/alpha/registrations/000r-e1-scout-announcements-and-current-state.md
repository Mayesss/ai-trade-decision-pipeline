# 000r — E1 scout: Binance announcement catalogs, current-state metadata, post-cutoff event lists

Ledger entry, not a hypothesis. Written 2026-10-13 by the orchestrator, from
the E1 scout's report (`research/alpha/scouts/e1-structural-payers.md` §5.1).

## What happened

1. **Binance CMS catalogs (48, 49, 93) cannot be end-dated.** Articles dated
   on or after 2026-07-01 were dropped at load and only counted (88 / 263 /
   123); no title was printed. 110 detail bodies to 2026-06-30 were stored
   (`research/alpha/data/scout_e1/`). The orchestrator recounted the
   reward-terms table: 97 events, last announced 2026-06-30, none edited on
   or after the cutoff; HODLer 64 rows (66 projects by ordinal: combined
   announcements and one non-standard title), Launchpool 30, Megadrop 3 rows
   (the report names 4).
2. **Launchpool `listV3`:** per-pool BNB staked; it shows no Launchpool
   between 2026-03-03 and today (a current-state fact). Posted per-pool APR
   was dropped at load, unread. One probe printed a current cumulative
   aggregate (`grossValue` ≈ 7.05e11, unit unknown); not used.
3. **trade.xyz pre-IPO index (current):** 5 post-cutoff events with dates and
   statuses only (SKHY, CXMT, UNITREE, SHEIN converted; OURA live).
4. **Current-state metadata by design:** BNB perp leverage, OI and depth on
   HL and Bitget; Capital.com's fee table.
5. **Search snippets seen, not used:** Aave USDC supply rates (~2026-10),
   LlamaRisk PT and sUSDe rates (2026-08-25), Kalshi and Polymarket August
   2026 volumes, "no second HL airdrop as of 2026-10".
6. No reward-token price, yield series, or listing-day path was fetched or
   computed.

## What must not follow from it

- No Binance-reward rule may be sized from current APRs or the post-cutoff
  Launchpool gap; the 2026-07 → reward calendar is partly seen (event
  existence, not values) and states this if used as a holdout.
- Pre-IPO convergence: the post-cutoff conversion list may not count as
  unseen events for any pre-IPO rule.

## Mitigation

E1 spends no trial (programme doc §3, 2026-10-13).
