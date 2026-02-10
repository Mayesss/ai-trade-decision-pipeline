# AGENTS.md

Guidance for coding agents working in this repository.

## Goal
Maintain and iterate an AI-driven trading decision pipeline safely. Prompt and decision logic changes should be observable, reversible, and testable in dry-run mode.

## Repo Map
- `lib/ai.ts`: prompt builder (`buildPrompt`), AI call (`callAI`), and decision post-processing (`postprocessDecision`).
- `pages/api/analyze.ts`: single-symbol analysis pipeline.
- `pages/api/analyzeMultiple.ts`: multi-symbol analysis pipeline with concurrency + retries.
- `lib/trading.ts`: execution layer (market orders, close/reverse handling, leverage).
- `pages/api/evaluate.ts`: LLM-based evaluation/audit of recent decisions.
- `lib/history.ts`: KV persistence for prompts, decisions, snapshots, and exec results.

## Safety Rules
1. Treat analysis routes as potentially live-trading.
2. Always pass `dryRun=true` for manual tests unless the user explicitly requests live execution.
3. Never log secrets or full API keys. If needed, log only key presence or short masked prefixes.
4. Avoid widening live-trading behavior by default in docs or code.

## Current API Contract (important)
- `GET /api/analyze` and `GET /api/analyzeMultiple` (not POST).
- `dryRun` currently defaults to `false` if omitted.
- Timeframes in analyze routes are fixed by `lib/constants.ts`:
  - `MICRO_TIMEFRAME=1H`
  - `PRIMARY_TIMEFRAME=4H`
  - `MACRO_TIMEFRAME=1D`
  - `CONTEXT_TIMEFRAME=1W`

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
# env presence (redacted)
curl "http://localhost:3000/api/debug-env-values"

# safe pipeline run (single symbol)
curl "http://localhost:3000/api/analyze?symbol=ETHUSDT&dryRun=true&notional=100"

# safe pipeline run (multi symbol)
curl "http://localhost:3000/api/analyzeMultiple?symbols=BTCUSDT&symbols=ETHUSDT&dryRun=true&notional=100"

# recent stored decisions
curl "http://localhost:3000/api/rest-history?symbol=ETHUSDT"

# run evaluator
curl "http://localhost:3000/api/evaluate?symbol=ETHUSDT&limit=20&batchSize=5"
```

## Done Checklist
- Docs updated if behavior changed.
- Manual verification done with `dryRun=true`.
- No sensitive logs added.
- No unrelated files modified.
