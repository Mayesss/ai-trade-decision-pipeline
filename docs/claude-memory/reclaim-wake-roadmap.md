---
name: reclaim-wake-roadmap
description: "Reclaim-wake feature COMPLETE & DEPLOYED — phase 1 (band sweeps, 9a7a378) + phase 2 (session/prior-day pool sweeps, c5bdc5b) live in prod (verified in decision 17574 prompt, 2026-08-30)"
metadata: 
  node_type: memory
  type: project
  originSessionId: 7b5ee593-5334-44ef-931e-2f3b40306147
  modified: 2026-08-30T10:24:47.643Z
---

**STATUS UPDATE (same day): PHASE 1 IMPLEMENTED as designed below** — native, no flag.
Delivered: reclaimWakeEligible + constants (0.2 ATR depth floor, 10 min freshness) in
lib/swing/wakeWatch.ts; reclaim_looked_at_ms one-shot column + claimSwingReclaimLook
(atomic; reset on cooldown upsert) in pg.ts; watcher fires on eligible PURE-sweep
transitions only (not 'arm' side-flips); analyze claims the look when no band crossed,
reuses cooldownWakeActive for the quality-gate bypass (bouncer skipped), builds
market.reclaim_wake {side, level, extreme, depth_atr, held/reclaimed minutes, note};
READ-ONLY rule enforced (HOLD → cooldown_* ignored, note reclaim_look_read_only, row
untouched; a PLACED entry consumes the row); prompt bullet "Reclaim-wake trigger" with
the measured mechanical-fade warning; tick stage reclaim_wake; snapshot.reclaimWake for
SQL; reclaim HOLDs flow into refusal investigations. Tests: unit reclaimWake.test.ts +
contract live-reclaim-wake (claim is the only row write; prompt carries the payload).
324/324 pass, lint clean. Committed 9a7a378.

**PHASE 2 ALSO IMPLEMENTED same day (commit c5bdc5b, not pushed):** analyze stamps
sessionLevelsRefKey KV (last-session + prior-day highs/lows, per-level validity
horizons: last-session until current session end, prior-day until UTC midnight; atr;
26h TTL) whenever forexSessionContext builds; watcher (wake-watch step 1b) iterates
getCronSymbolConfigs() session categories (forex/commodity/index), two batched MGETs
per minute (refs + touch states), runs pure sessionSweepStep (arm nearest violated
pool / extend / reclaim / abandon >30min = break, not sweep); eligible reclaims (same
0.2-ATR depth + 10-min freshness via reclaimWakeEligible) write sessionSweepEventKey
(15-min TTL) + per-pool looked marker (6h TTL) and fire analyze. Analyze reads the
event on flat ticks, precedence: fired band > band reclaim > session reclaim; same
quality-gate bypass, bouncer skip, read-only cooldown rule (reclaim_look_read_only);
market.session_reclaim {kind, side, level, extreme, depth_atr, held/reclaimed min};
tick stage session_reclaim; snapshot.sessionReclaim; refusal investigations included;
event consumed (kvDel) after persist. Tests: sessionSweep.test.ts (state machine) +
contract live-session-reclaim (seeded KV event → read-only look → consume). 329/329.

PUSHED + DEPLOYED (main == origin/main, prod prompt carries the reclaim-wake bullet
as of decision 17574, 2026-08-30). REMAINING: watch cohorts via tick_log stages
wake_auto_entry / reclaim_wake / session_reclaim and snapshot fields.

Approved sketch 2026-08-29 (user: "i also like the reclaim-wake... sketch this then save
for next session"). Builds on [[wake-retest-refusal-loop]] (which holds the backtest
evidence and the shipped wake-auto-entry feature).

**Goal:** a sweep (touch + reclaim before the sustain window) of an AI-armed wake band
fires an immediate AI look — the bounce moment becomes actionable instead of arriving
hours later as stale market.wake_band_sweeps evidence.

**Phase 1 design:**
- Detection exists: watcher's sustainedWakeStep() returns {kind:'sweep', sweep:{side,
  level, touchedAtMs, reclaimedAtMs, extreme}} (lib/swing/wakeWatch.ts); watcher today
  only persists via replaceSwingWakeSweeps. Add: after persisting, fire analyze early
  (reuse wakeWatchFiredKey double-fire marker).
- Analyze cooldown handler: cooldown row active + NO band crossed + newest wake_sweeps
  entry reclaimed within ~5 min + not yet consumed → reclaim-wake path: bypass flat
  QUALITY gates (same as cooldownWakeActive), build market.reclaim_wake {side, level,
  depth_atr, held_minutes, reclaimed_minutes_ago, note}.
- Noise floor: fire only if sweep depth ≥ ~0.2 primary ATR (wake_atr on row); ONE
  reclaim look per band side per row lifetime (new column reclaim_looked_at_ms,
  consumed only after decision durably recorded — mirror the wake claim-lease pattern).
- Gates: HARD gates always apply (base exec, blackout, margin, closed); flat quality
  gates bypassed; ai-bouncer skipped (already skips wake ticks); postprocessDecision
  constraints kept (sweep-reclaim re-entry exception already lives there).
- JUDGMENT-GATED ONLY, never mechanical: 236-sweep backtest = 21% win, −0.73R/trade
  net for mechanical fades. The look lets the AI fade WITH structural confluence
  (stop beyond sweep extreme = natural invalidation).
- Reclaim look is READ-ONLY wrt the cooldown row: band plan did not fire, so a HOLD
  leaves band + cooldown untouched (ignore its cooldown_* outputs unless it enters).
  No fold semantics, no ratchet surface.
- Eval: persist reclaimWake in snapshot_json (like cooldownWake), tick_log stage
  'reclaim_wake'; HOLDs flow into refusal investigations naturally.
- Prompt: market.reclaim_wake guidance — liquidity-grab reclaim of YOUR watched level,
  first-class bounce setup, entry toward range, invalidation beyond sweep extreme,
  explicitly NOT a breakout check; warn that mechanical fades lost (confluence needed).

**Phase 2 (later):** same pattern for session-level sweeps — bullishLiquidityReclaim /
bearishLiquidityRejection detectors exist but compute at analyze time only; watcher
would need cached session levels for per-minute checks.

**Effort:** ~half day. Watcher branch + cooldown-handler branch + 1 PG column + prompt
block + contract tests (watcher sweep-fire, analyze reclaim look).

**Status:** NOT implemented (user paused for AI credits). Also pending: the whole
wake-auto-entry feature sits UNCOMMITTED in the working tree (see
[[wake-retest-refusal-loop]]) — commit/deploy first before starting this.
