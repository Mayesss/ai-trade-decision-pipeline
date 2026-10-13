# 000q — D1 scout: post-move path quantities on seasoned coins of 009's Binance calendar

Ledger entry, not a hypothesis. Written 2026-10-13 by the orchestrator, from
the D1 scout's report (`research/alpha/scouts/d1-listings-squeeze.md` §6;
outputs `research/alpha/data/scout_d1/derived/seasoned_paths.txt`).

## What happened

1. **A behaviour look with outcomes, on a closed calendar.** To size squeeze
   mechanics for a listings rule, the scout read 009's panel
   (`research/copytrade/data/binance_years/panel`: 1h bars, funding,
   `life.json`, 2021-11 → 2024-01-07) and computed, on **seasoned**
   coin-weeks only (age ≥ 30 days, ≥ 7 days before the segment end, BTC and
   ETH excluded):
   - how often a coin's high touched +50% / +90% within 7 days;
   - the distribution of the 7-day exit given such a touch (e.g. after a
     +90% touch the median exit is ×1.6–1.8, 11% reach ×2.86);
   - the mean difference and SD between stop, margin-depth and frozen
     rules.
   These are continuation-after-large-move statistics: they bear on
   momentum, trend and stop rules.
2. **No listing window was touched** (days 0–29 after first trade excluded).
3. **013 reads:** `data/r013/schedule.json` (first-trade times and
   premarket/continuation flags, all calendars; pre-entry facts used only as
   event times in synthetic draws) and `MANIFEST.json` (hashes). No count of
   a pre-entry fact was made on the confirmation calendar.
4. **One current-state page:** Binance's mark-price FAQ (last updated
   2026-09-25), contract mechanics only.

## What must not follow from it

- Any **trend-following, momentum, breakout or stop rule** that uses
  Binance 2021-12 → 2024-01 as discovery states this look. Its thresholds
  and lookbacks may not be chosen near the +50% / +90% touch levels because
  of it.
- Binance 2021–23 family count stays at 1 (009); this look is not a trial,
  but it is outcome-adjacent and is disclosed.
- A listings trial 3 (if ever drafted) states this look and 013's Result §
  strata as seen.

## Mitigation

D1 spends no trial (programme doc §3). A trend registration (C1) is told of
this entry before drafting.
