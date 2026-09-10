# Venue-anchored primary bars ("shift the bar clock") — spec for a later session

Status: NOT started. Written 2026-09-10 as the second step after the session
decision-window gate (`session_window_gate`, same day). Read
`docs/week-one-review-2026-09-10.md` §9 first for the measurement that
motivates both.

## The problem this solves, and the one it does not

Primary (4H) bars are UTC-aligned for every instrument. For a session-traded
CFD that is arbitrary: the 12:00–16:00 UTC bar on US100 holds 90 minutes of
pre-open drift, the opening drive and the first two hours of cash trade in one
candle; the 04:00–08:00 UTC bar on DE40 straddles the Xetra open. Every
primary-timeframe reading — ATR, EMA20 distance, swing pivots, BOS/CHoCH,
regression channel — is computed on candles that mix liquidity regimes.

The gate fixes *when we decide*. This would fix *what we measure*. It does NOT
replace the gate: two thirds of last week's awkward-hour decisions were wakes
(bands, session-reclaim triggers), and a wake fires at the open whatever the
bar alignment. Keep the gate underneath.

## Design

1. **Per-venue anchor.** Each venue calendar (`lib/swing/sessionEvents.ts`
   `CALENDARS`) gets a `barAnchor`: the venue-local wall-clock time at which a
   primary bar closes, chosen so the post-open bar closes after the opening
   drive has settled and no close lands inside a decision window:
   - US_INDEX / NYSE / METALS / ENERGY: bars close 11:00, 15:00, 19:00, 23:00,
     03:00, 07:00 UTC (15:00 = 90 min after the cash open). Check against
     the windows: 11:00 is 2h30 pre-open — outside the 120-min window; 15:00
     is 30 min before the European close — inside no window, but the model
     will be told the close is 30 min away.
   - XETRA / LSE: 09:00, 13:00, 17:00, 21:00, 01:00, 05:00 UTC (09:00 = 2h
     after the Xetra open; 13:00 is 30 min before the US open — inside the
     120-min US pre-open window for DE40, so that close is skipped by the
     gate and its owed look lands at 14:00; acceptable, but it means the
     European venues get 5 decisions a day, not 6, and the anchor may be
     better at 10:00/14:00/18:00... — decide with data).
   - FX: unchanged (UTC) — no single home venue.
   - Crypto: unchanged (UTC).
   The anchor is DST-aware because it is defined in venue-local time and
   converted with the same `utcMsForLocalTime` the events use.

2. **Resampling.** Capital serves UTC-aligned candles. Build the primary
   series from 1H candles (`fetchMarketBundle(symbol, '1H', { candleLimit })`)
   grouped into 4-bar buckets on the venue anchor. 200 primary bars for the
   S/R scan = 800 hourly candles — check Capital's per-request cap and the
   rate limiter (`CAPITAL_MAX_REQUESTS_PER_SECOND`); two paged fetches are
   fine, four are not. Macro (1D) and context (1W) stay venue-provided:
   1D bars should ideally close at the venue close, but that is a separate,
   smaller change. Micro (1H) and nano (15m) are unaffected.

3. **Cadence.** `primary_close_gate` compares `now` to the anchor grid instead
   of the UTC grid; `primaryCloseTime` derives from the venue anchor. The
   15-min cron is unchanged. `wake-watch`'s `timeframeToMs` users need the
   anchor for anything that reasons about "bars since".

4. **Everything downstream reads bars, not clocks.** Indicators, S/R,
   `waveGeometry`, `signals`, `positionExtrema` take candle arrays — no change.
   `chartCache` / `pages/api/chart.ts` must resample the same way or the chart
   shows different bars than the decision used (the overlay markers then sit
   on candles the model never saw). `positionDecisionMatch.resolveBracketAtExit`
   and the post-mortem dossier replay ticks, not bars — unaffected.

5. **Prompt.** TIMEFRAMES section: "primary=4H closes at HH:MM venue time"
   for session instruments; CADENCE sentence unchanged otherwise.

## Migration and measurement

- Contract fixtures: `capital-EURUSD` is FX and stays UTC — no re-capture
  needed for the existing suite. Add one US-index fixture (`US100`) captured
  at a venue-anchored close for the new path; re-run
  `npm run test:fixtures:capture -- US100 capital index`.
- Lessons: symbol-scoped lessons quoting "4H close" keep meaning; nothing
  quotes UTC hours.
- History: `swing.decisions` before the switch used UTC bars. Any
  before/after comparison of structure signals (BOS rate, breakout-retest
  frequency, ATR percentile) must be split at the switch date. Record the
  date in `decisionConfig.ts` next to the anchor table.
- The success metric is NOT PnL (too slow). It is: per-venue, do
  breakout/breakdown confirmations on the new bars get followed through more
  often than on UTC bars? `scripts/study-decision-quality.ts` already
  computes forward moves per decision and can be run over both bar
  definitions on the same tick history.

## Env / rollout

`SWING_PRIMARY_BAR_ANCHOR=venue|utc` (default `utc` until measured), read at
call time like the session-window config so a single symbol can be flipped in
a dry-run tick (`swing-tick` skill) before the crons see it.
