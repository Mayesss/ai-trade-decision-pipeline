# 000s — B1 scout: equity and rates calendars, unconditional vols, post-cutoff literature

Ledger entry, not a hypothesis. Written 2026-10-13 by the orchestrator, from
the B1 scout's report (`research/alpha/scouts/b1-equities-flows.md` §5).

## What happened

1. **Free pulls** under `research/alpha/data/scout_b1/` (checksummed): Ken
   French daily US and regional factors (static zips that hold 43–44 rows
   on or after 2026-07-01, **dropped at load, never printed**), Treasury par
   yields (2026 CSVs hold 142 post-cutoff rows, dropped at load), the
   fiscaldata auction calendar (calendar fields only, end-dated), the Fed's
   FOMC calendar (post-2026-07 dates dropped at load), a Nasdaq price probe
   (AAPL 2016-10-12 → 2026-06-30, 2,441 rows; no return computed) and a
   Nasdaq earnings-file probe (field names only).
2. **Derived calendars** (`derived_20261013T092459Z/`, dates only):
   month-end windows 2014-01 → 2026-06 (150 rows; 149 complete: T+3 44,
   T+2 80, T+1 25), FOMC cycle weeks, 2y/5y/10y auction dates. The
   orchestrator recounted the month-end file.
3. **Unconditional levels read for power only:** daily volatility
   2014-01 → 2026-06 of the US market (1.12%), regions (0.85–1.17%), the
   10y yield change (5.21 bp); the 13-week bill range 2025-07 → 2026-06
   (carry opportunity cost). No window, event or calendar-conditional
   return was computed.
4. **Literature that is outcome-adjacent for our calendars:**
   - Liberty Street Economics (published 2026-07-01): overnight drift ≈ 0
     in 2021–2025;
   - NY Fed Staff Report 1188 (revised 2026-07): intraday auction pressure
     persists, "not increased in recent years";
   - Harvey, Mazzoleni & Melone (NBER w33554, 2026 draft): the rebalancing
     front-run over 1997-09 → 2023-03, and a footnote that it strengthened
     through 2025, i.e. **our 2014 → 2026 calendar is inside its authors'
     sample**;
   - Etula et al.'s international settlement-change test (2013-10 →
     2015-10), which overlaps our first 22 months;
   - search summaries on NightShares, Heater et al., and the 2020-02 /
     2025-04 crashes.
5. **Current-state reads:** SEC, CSA/ASC and ASX settlement pages;
   Capital.com fee page; a FINRA header probe; failures (403/timeout) from
   SEC EDGAR (no contact User-Agent; the owner's email was not used), FRED,
   IBKR, JPX, SSRN.

## What must not follow from it

- No month-end, FOMC or auction window, duration or threshold may be chosen
  from anything here beyond what the cited papers fix; the windows are
  Etula's, shifted by the settlement lag in force (SEC Releases 34-80295
  and 34-96930).
- A month-end registration states that 2014-01 → 2015-10 overlaps Etula's
  international test, that the HMM rebalancing calendar is in its authors'
  sample (so a pass there is a replication, not out-of-sample evidence),
  and that the unconditional US daily vol was read.
- No overnight-drift rule may treat 2021–2025 as unseen.

## Mitigation

The month-end registration (015) decides on its own guarded computation
over 2014-01 → 2026-05; 2026-07 → stays held back.
