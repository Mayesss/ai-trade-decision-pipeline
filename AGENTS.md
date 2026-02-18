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
