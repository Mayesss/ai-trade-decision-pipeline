---
name: wake-retest-refusal-loop
description: "DB-proven 2026-08-29 — wake-band fires almost never convert to entries; AI re-arms new bands (\"retest\") in a loop; sustained-confirmed wakes convert at 3.3%"
metadata: 
  node_type: memory
  type: project
  originSessionId: 7b5ee593-5334-44ef-931e-2f3b40306147
  modified: 2026-08-29T18:04:53.427Z
---

Study over swing.decisions (60d window, 2026-06-30→08-29, ran 2026-08-29): of 361
wake-triggered AI looks, only 24 entered (6.6%); 327 re-armed a new wake band on HOLD;
208 of those HOLDs mention "retest". Worst: **sustained-confirmation wakes (held N min,
the AI's own breakout plan confirmed) = 121 fires → 4 entries (3.3%)** vs instant-touch
wakes 20/240 (8.3%) and non-wake looks ~17% entry rate. 70 consecutive-wake chains ≥2;
51 died with no entry ever (US100: 21 wakes/4 days all HOLD; BNBUSDT 17 wakes/30h).

**Why:** Structural contradiction — the sustain window guarantees worse entry location
(price extended by confirm time), then the model's location/extension filters reject the
entry it planned; re-arming is free and stateless (each wake is a fresh flat scan), so
plans ratchet forever. User instinct (2026-08-29): no retest after confirmed break;
enter on confirmation with SL/failed-break watch as the safety net (break_triggers
infra already exists).

**Backtest 2026-08-29** (Bitget public candles exact / Yahoo proxies for Capital, 1h,
321 refused wakes replayed, deduped portfolio, ~0.08% costs): blanket enter-on-wake
LOSES (all wakes net −9 to −16R; instant-touch wakes worst at −0.35R/trade net).
Sustained-confirmed + failed-break-close exit is the only positive cohort
(+20.9R net over 48 trades) but ONE trade (SOL 2026-08-18 +29R) carries it — without
it net negative; fixed 2-ATR TP destroys the cohort (−14.4R): edge, if any, is a
15%-win-rate fat-tail regime requiring uncapped winners. AI's own 24 wake entries:
18/23 losers. Conclusion: wake loop has no measured entry alpha either direction;
the certain win is cutting its AI-call churn, and any auto-entry should ship as
dry_run shadow mode (sustained-only, failed-break exit, no TP) first.

**Bounce follow-up 2026-08-29:** prompt already treats level bounces as first-class
(lib/ai.ts "Level-bounce entries are a first-class setup") — the breakout bias is
architectural (wakes fire on breaks; touch-and-reclaim is only stale sweep evidence).
Mechanical sweep-reclaim fade backtest (236 sweeps from prompts): 21% win, −0.73R/trade
net — levels get re-poked 5–8×, stops beyond extremes get run over. NO bounce leg.

**IMPLEMENTED 2026-08-29 (uncommitted):** NATIVE, always-on — user removed both env
flags (SWING_WAKE_AUTO_ENTRY, RISK_FRACTION) and the 25% starter sizing: mechanical
wake entries use the repo's standard risk sizing (SWING_RISK_EQUITY_PCT; note the 2x
equity exposure ceiling in riskSizing.ts binds on tight stops). Confirmed sustained
wake fires skip the AI call and execute a synthetic decision through the normal pipeline
(lib/swing/wakeAutoEntry.ts; analyze.ts injects at the callSwingDecision site): entry
in break direction, SL 1.5 ATR, TP 8 ATR backstop, entry_trigger_price=level arms the
failed-break exit, 25% risk. postprocessDecision confirmedWakeEntry param bypasses ONLY
micro_entry_ok (trend guard/re-entry cooldown/base gates stay). Same-side re-arm after
a refused fresh wake is dropped in code (wake_rearm_same_side_dropped). Tests:
test/unit/swing/wakeAutoEntry.test.ts + test/contract/analyze/live-wake-auto-entry
(proves order placed, no ai-gateway call, $250 risk on 10k equity).

**Design decision 2026-08-29 (user asked about AI plan-time choice + must-act-with-enum
veto):** rejected the enum veto for the pilot — prompt-level commitment language already
failed (refusals continued at 97%), legit vetoes are code gates that already run
(blackout/base gates/trend guard; reclaim can't occur at a confirmed fire), and AI
selection would contaminate the 25%-size cohort sample. The plan-time choice IS the
sustain field (sustain set = pre-authorized mechanical entry; null = first-touch wake,
AI decides); made explicit to the model via flag-gated EXECUTION CONTRACT prose in the
flat-cooldown guidance (lib/ai.ts, wakeAutoEntryGuidance). Revisit enum veto only if
the pilot cohort dies to a nameable situation — then encode it as a code gate first.
Independent review (other Claude session) fixes adopted 2026-08-29: (1) chase guard —
builder refuses fires > 1.0 ATR past the level (WAKE_AUTO_ENTRY_MAX_EXTENSION_ATR;
geometry: 1.5-ATR stop must stay ≥0.5 ATR inside the level) → falls back to AI;
(2) postprocess-demoted synthetic entries fall back to the normal AI call (also kills
false refusal-investigations); (3) ratchet guard drops cooldown_minutes too when no
band survives (no blind quiet period the AI never signed). Rejected: consuming the
wake row on gate-block (keep-lease retry is intentional pre-existing design, bounded
by plan horizon; chase guard bounds the damage), dropping prompt persistence on
wake-auto rows (kept for post-mortem forensics; rows marked ai_provider wake-auto).
Contract test live-wake-auto-fallback pins the fallback+fold chain.
User follow-up ("must act, not hold, on any wake"): rejected forced ENTRY on instant
wakes (replay: mechanical entry −0.35R AND fade −0.21R net — no forceable direction at
first touch); instead instant wakes are ACT-OR-FOLD (enter / arm opposite side / fold
and lose the watch — flag-gated prose in the wake-trigger section replaces the old
"re-set if fake-out" permission, matching the code guard).

**How to apply:** When touching wake/cooldown logic ([[forex-scalp-cleanup-plan]] repo:
lib/swing/wakeWatch.ts, lib/ai.ts cooldown_wake_*), treat wake plans as commitments,
not reminders: cap re-arms per plan, or execute confirmed breakout plans with reduced
size + stop inside the range, veto only via enumerated disqualifiers. Study scripts in
session scratchpad (wake-pattern-study2.mjs, wake-chains.mjs) — reusable via cached
neonctl npx dir c58b2a555394695b.
