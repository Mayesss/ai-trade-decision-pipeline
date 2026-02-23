# AGENTS.md

Guidance for coding agents working in this repository.

## Goal
Maintain and iterate an AI-driven trading decision pipeline safely. Prompt and decision logic changes should be observable, reversible, and testable in dry-run mode.

## Repo Map
- `lib/ai.ts`: prompt builder (`buildPrompt`), AI call (`callAI`), and decision post-processing (`postprocessDecision`).
- `pages/api/analyze.ts`: single-symbol analysis pipeline.
- `lib/trading.ts`: execution layer (market orders, close/reverse handling, leverage).
- `pages/api/evaluate.ts`: LLM-based evaluation/audit of recent decisions.
- `pages/api/evaluations.ts`: dashboard aggregate payload (evaluation + latest prompt/decision + 7D PnL/open-position context).
- `pages/api/chart.ts`: chart candles, decision markers, and position overlays for the requested timeframe/window.
- `lib/history.ts`: KV persistence for prompts, decisions, snapshots, and exec results.
- `pages/index.tsx`: dashboard shell, admin gate, live ticker socket, and live PnL recomputation.
- `components/ChartPanel.tsx`: chart rendering (`lightweight-charts`), overlays, live pulse marker, and fullscreen mode.

## Safety Rules
1. Treat analysis routes as potentially live-trading.
2. Always pass `dryRun=true` for manual tests unless the user explicitly requests live execution.
3. Never log secrets or full API keys. If needed, log only key presence or short masked prefixes.
4. Avoid widening live-trading behavior by default in docs or code.

## Current API Contract (important)
- `GET /api/analyze` (not POST).
- `dryRun` currently defaults to `false` if omitted.
- `platform` query selects execution/data provider (`bitget` default, `capital` optional).
- `newsSource` query selects news provider (`coindesk` or `marketaux`, default depends on platform).
- Timeframes in analyze routes are fixed by `lib/constants.ts`:
  - `MICRO_TIMEFRAME=1H`
  - `PRIMARY_TIMEFRAME=4H`
  - `MACRO_TIMEFRAME=1D`
  - `CONTEXT_TIMEFRAME=1W`
- `GET /api/evaluations` returns latest evaluation plus 7D realized PnL and open-position fields used by UI live updates:
  - `openDirection`, `openLeverage`, `openEntryPrice`, `openPnl`, `pnl7d`, `pnl7dWithOpen`.
- `GET /api/chart` defaults to a 7-day window if `limit` is omitted.
  - Timeframe input is normalized to Bitget-compatible values (for example `1h` becomes `1H`).
  - The dashboard uses `timeframe=1H&limit=168` as the standard 7D chart window.

## Dashboard Data Contract
1. Keep `pages/api/evaluations.ts` response shape aligned with `EvaluationEntry` in `pages/index.tsx`.
2. Keep `pages/api/chart.ts` response shape aligned with `ChartApiResponse`/overlay types in `components/ChartPanel.tsx`.
3. Live ticker comes from Bitget public WS (`ticker` channel on `USDT-FUTURES`).
4. Live open-PnL formula in UI is leverage-aware:
   - `((livePrice - entryPrice) / entryPrice) * sideSign * leverage * 100`.
5. When changing PnL semantics, update both:
   - server-side values in `pages/api/evaluations.ts`
   - client-side live recomputation in `pages/index.tsx` and open overlay refresh in `components/ChartPanel.tsx`.

## Prompt-Change Workflow
1. Edit `lib/ai.ts` only where needed.
2. Keep output JSON schema stable unless user asks to change it.
3. Verify downstream assumptions in:
   - `postprocessDecision` in `lib/ai.ts`
   - execution expectations in `lib/trading.ts`
   - evaluation parsing in `pages/api/evaluate.ts`
4. Run a dry-run request and inspect stored prompt/decision via history/evaluation routes.

## Temporary Debug Pattern
When debugging prompt/decision issues:
1. Add targeted logs at boundaries:
   - after `buildPrompt`
   - after `callAI`
   - after `postprocessDecision`
2. Keep logs short and structured (single-line JSON where possible).
3. Gate verbose logging behind a temporary flag (query/env) when practical.
4. Remove debug-only logs before finishing unless user asks to keep them.

## Forex Strategy Self-Improvement Loop
Use this loop for forex execution/management changes so improvements are measurable and reversible.

1. Keep validation offline-first:
   - Run replay/matrix tooling first (`npm run test:forex`, `npm run replay:matrix`).
   - Do not use Capital live calls for strategy iteration.
   - Keep manual API checks in `dryRun=true`.
2. Use two-stage validation:
   - Stage A (offline): prove the change with replay/matrix before any runtime checks.
   - Stage B (limited live dry-run): run 1-2 cycles on 1-2 pairs with `dryRun=true` and confirm no unexpected orders, expected gating, and expected journaling.
3. Treat churn as a symptom:
   - Diagnose what creates churn (`breakevenExitPct`, `partialTradePct`, `shortHold10mPct`, `shortHoldTradePct`) before changing weights.
   - `shortHoldTradePctForChurn` is the thresholded metric used in `churnPenaltyPct`; tune it with `--churnShortHoldMin`.
   - Use fixture/scenario artifacts to identify which management rule is causing exits.
4. Use one-change/one-hypothesis experiments:
   - Change one rule at a time (for example BE threshold, trailing activation, time-stop guard).
   - Rerun invariants + matrix after each change and compare deltas.
5. Run a metric/fixture feasibility check before strict constraints:
   - For trade-active scenarios, inspect min/median/max for `churnPenaltyPct`, `tailGap`, and `tradeCoveragePct`.
   - If `min(churnPenaltyPct) > --maxChurn`, the strict run is definitionally infeasible on that fixture pack.
   - If infeasible, adjust thresholds or `--churnShortHoldMin` before treating strict runs as pass/fail.
6. Use constrained shortlist as policy gate:
   - Use `topRobustScenariosConstrained` as pass/fail policy, not raw score ranking.
   - CLI knobs:
     - `--minCoverage`
     - `--maxChurn`
     - `--maxTailGap`
     - `--churnShortHoldMin` (core fixtures default to 10; longer-horizon fixture packs can use 60)
     - `--topKConstrained`
7. Track rejection reasons:
   - Review `rejectedByCoverage`, `rejectedByChurn`, `rejectedByTailGap`, `rejectedByMultiple`.
   - Optimize the binding constraint first (most common reject reason).
8. Pick the next lever using binding constraint + `worstFixtureHeatmap`:
   - rollover tails -> pre-rollover close/derisk policy.
   - event tails -> event window/gating policy.
   - stop-out churn/re-entry loops -> stop-invalidation cooldown and spread-aware invalidation handling.
   - Verify `worstFixtureHeatmap` and `worstTradedAvgR` improve, not just `robustnessScore`.
9. Fix catastrophic tails before cosmetic score gains:
   - Prioritize extreme tail-risk control (event-gap/rollover) before score tuning.
   - Success signal: frontier shift (better worst/tail metrics without collapsing coverage).
10. Keep every experiment reversible:
   - Put each policy experiment behind config/CLI flags or maintain a clean revert path.
   - If constrained eligibility drops materially or worst tails worsen, roll back immediately.

### Forex Iteration Commands
```bash
# baseline matrix
npm run replay:matrix

# stricter constrained policy run
node --import tsx scripts/forex-replay-matrix.ts \
  --fixtures core \
  --minCoverage 50 \
  --maxChurn 65 \
  --maxTailGap 2.8 \
  --churnShortHoldMin 10 \
  --topKConstrained 10 \
  --outDir /tmp/forex-replay-matrix-strict
```

## Useful Local Checks
```bash
# If ADMIN_ACCESS_SECRET is set, include:
#   -H "x-admin-access-secret: $ADMIN_ACCESS_SECRET"

# env presence (redacted)
curl "http://localhost:3000/api/debug-env-values"

# safe pipeline run (single symbol)
curl "http://localhost:3000/api/analyze?symbol=ETHUSDT&dryRun=true&notional=100"

# safe pipeline run (Capital + non-crypto)
curl "http://localhost:3000/api/analyze?symbol=QQQUSDT&platform=capital&newsSource=marketaux&dryRun=true&notional=100"

# recent stored decisions
curl "http://localhost:3000/api/rest-history?symbol=ETHUSDT"

# run evaluator
curl "http://localhost:3000/api/evaluate?symbol=ETHUSDT&limit=20&batchSize=5"

# chart payload (1H bars, 7D window)
curl "http://localhost:3000/api/chart?symbol=BTCUSDT&timeframe=1H&limit=168"

# dashboard aggregate payload
curl "http://localhost:3000/api/evaluations"
```

## Done Checklist
- Docs updated if behavior changed.
- Manual verification done with `dryRun=true`.
- No sensitive logs added.
- No unrelated files modified.
