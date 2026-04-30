# AI Trade Decision Pipeline

Next.js app that runs an AI-driven trading loop for multiple platforms (Bitget + Capital.com): pull market data, compute flow/indicator gates, ask an OpenAI-compatible model for an action, optionally place the trade, store the decision history, and render a dashboard with PnL, prompts, and aspect evaluations.

---

## What’s Inside

- **Next.js API routes** for analysis (`/api/analyze`), AI-driven evaluations (`/api/evaluate`), history/PNL enrichment (`/api/evaluations`, `/api/rest-history`, `/api/chart`), scalp cron execution, and health/debug helpers.
- **Platform integrations**: Bitget (futures) and Capital.com (CFD/spot-style market access) with platform-selected market/execution paths.
- **Signal stack**: multi-timeframe indicators (context/macro/primary/micro), support/resistance levels, momentum/extension gates, and provider-selected news sentiment (`coindesk` or `marketaux`).
- **LLM prompts** built in `lib/ai.ts` with guardrails and momentum overrides; responses are persisted for replay and review.
- **Dashboard** (`pages/index.tsx` + `components/ChartPanel.tsx`) showing latest decisions, prompts, aspect ratings, 7D PnL, open positions, live ticker updates, and chart overlays of recent trades.
- **KV storage** (Upstash-compatible REST) for decision history, evaluations, and cached news (7d TTL for history, 1h for news).

### Scalp Strategy Registry Notes

- Newly available scalp strategy ids (in addition to existing registry ids):
  - `anchored_vwap_reversion_m15_m3`
  - `funding_oi_exhaustion_proxy_m15_m3`
  - `basis_dislocation_reversion_proxy_m15_m3`
  - `relative_value_spread_proxy_m15_m3`
  - `session_seasonality_bias_m15_m3`
  - `adaptive_meta_selector_m15_m3` (disabled by default, PG-backed snapshots/decisions)
- These are proxy implementations designed to run with existing candle/quote inputs (no extra funding/OI feed required).

## Requirements

- Node.js 18+ (matches Next 16)
- Bitget API key (futures) with trading permissions
- Capital.com Open API credentials (for `platform=capital`)
- OpenAI-compatible API key (used by `callAI`)
- CoinDesk API key (required for `newsSource=coindesk`)
- Marketaux API key (required for `newsSource=marketaux`)
- ForexFactory public calendar feed (used for forex event-gate routes; no API key required)
- Upstash/Redis REST endpoint + token for KV (`upstash_payasyougo_KV_REST_API_URL`, `upstash_payasyougo_KV_REST_API_TOKEN`)
- Optional admin secret for protected routes (`ADMIN_ACCESS_SECRET`)

## Setup

1. Install deps:

```bash
npm install
```

2. Create `.env.local` with your secrets:

```bash
# Bitget
BITGET_API_KEY=...
BITGET_API_SECRET=...
BITGET_API_PASSPHRASE=...

# Capital.com
CAPITAL_API_KEY=...
CAPITAL_IDENTIFIER=...
CAPITAL_PASSWORD=...
# Optional custom ticker->epic map override (JSON string)
# CAPITAL_TICKER_EPIC_MAP={"QQQUSDT":"QQQ","XAUUSDT":"XAUUSD"}

# AI
OPENAI_API_KEY=...

# News
COINDESK_API_KEY=...
MARKETAUX_API_KEY=...

# Forex event calendar (ForexFactory)
# Optional override:
# FOREX_FACTORY_CALENDAR_URL=https://nfs.faireconomy.media/ff_calendar_thisweek.json
# FOREX_EVENT_REFRESH_MINUTES=15
# FOREX_EVENT_STALE_MINUTES=45
# FOREX_EVENT_PRE_BLOCK_MINUTES=30
# FOREX_EVENT_POST_BLOCK_MINUTES=15
# FOREX_EVENT_BLOCK_IMPACTS=HIGH
# FOREX_EVENT_CALL_WARN_THRESHOLD=180
# FOREX_DEFAULT_NOTIONAL_USD=100
# FOREX_SESSION_TRANSITION_BUFFER_MINUTES=20
# FOREX_SELECTOR_TRANSITION_SPREAD_TO_ATR_MULTIPLIER=0.8
# FOREX_RISK_SESSION_TRANSITION_BUFFER_MINUTES=20
# FOREX_RISK_TRANSITION_SPREAD_TO_ATR_MULTIPLIER=0.75
# FOREX_ROLLOVER_UTC_HOUR=0
# FOREX_ROLLOVER_ENTRY_BLOCK_MINUTES=45
# FOREX_ROLLOVER_FORCE_CLOSE_MINUTES=0
# FOREX_ROLLOVER_FORCE_CLOSE_SPREAD_TO_ATR1H_MIN=0.12
# FOREX_ROLLOVER_FORCE_CLOSE_MODE=close
# FOREX_ROLLOVER_DERISK_WINNER_MFE_R_MIN=0.8
# FOREX_ROLLOVER_DERISK_LOSER_CLOSE_R_MAX=0.2
# FOREX_ROLLOVER_DERISK_PARTIAL_CLOSE_PCT=50
# FOREX_REENTRY_LOCK_MINUTES=5
# FOREX_REENTRY_LOCK_MINUTES_STOP_INVALIDATED=0
# FOREX_REENTRY_LOCK_MINUTES_STOP_INVALIDATED_STRESS=0
# FOREX_STOP_INVALIDATION_MIN_HOLD_MINUTES=0  # default disabled; staged rollout recommendation: 8 (after dry-run validation)
# FOREX_REENTRY_LOCK_MINUTES_TIME_STOP=5
# FOREX_REENTRY_LOCK_MINUTES_REGIME_FLIP=10
# FOREX_REENTRY_LOCK_MINUTES_EVENT_RISK=20

# Scalp strategy (Capital-oriented)
# SCALP_ENABLED=true
# SCALP_DRY_RUN_DEFAULT=true
# SCALP_LIVE_ENABLED=false              # keep false by default (fail-closed)
# SCALP_DEFAULT_SYMBOL=EURUSD
# SCALP_SESSION_CLOCK_MODE=LONDON_TZ    # or UTC_FIXED
# SCALP_BLOCK_SUNDAY_ENTRIES=true       # global entry gate (default true): block new entries on Sunday by session clock
# SCALP_ALLOW_SUNDAY_REPLAY=false       # default false: block replay/backtest jobs on Sunday UTC (override only when explicitly needed)
# Entry session profile env is no longer used; canonical baseline session id is berlin.
# Use `?session=berlin|tokyo|newyork|pacific|sydney` on scalp planner/worker/executor routes.
# SCALP_ENTRY_ORDER_TYPE=MARKET         # or LIMIT
# SCALP_RISK_PER_TRADE_PCT=0.35
# SCALP_REFERENCE_EQUITY_USD=10000
# SCALP_MAX_TRADES_PER_SYMBOL_PER_DAY=2
# SCALP_MAX_OPEN_POSITIONS_PER_SYMBOL=1
# CANDLE_HISTORY_STORE=auto             # auto | file | kv
# CANDLE_HISTORY_DIR=data/candles-history
# Optional legacy guard-profile envs for root strategyId=regime_pullback_m15_m3 (prefer deployment configOverride)
# SCALP_XAUUSD_GUARD_TP1_CLOSE_PCT=20
# SCALP_XAUUSD_GUARD_TRAIL_ATR_MULT=1.6
# SCALP_XAUUSD_GUARD_TIME_STOP_BARS=18
# SCALP_XAUUSD_GUARD_BLOCKED_HOURS_VARIANT=xauusd_return   # xauusd_return | xauusd_low_dd | xauusd_high_pf | off
# SCALP_XAUUSD_GUARD_BLOCKED_HOURS_BERLIN=15               # explicit hour list override (wins over variant)
# SCALP_BTCUSDT_GUARD_TP1_CLOSE_PCT=20
# SCALP_BTCUSDT_GUARD_TRAIL_ATR_MULT=1.4
# SCALP_BTCUSDT_GUARD_TIME_STOP_BARS=15
# SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_EXPERIMENT=false          # default off; set true to use experimental btcusdt_high_pf profile
# SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_VARIANT=btcusdt_high_pf   # btcusdt_return | btcusdt_low_dd | btcusdt_high_pf | off
# SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_BERLIN=10,11            # explicit hour list override (wins over variant)
# SCALP_REQUIRE_PROMOTION_ELIGIBLE=false                     # when true, execute-deployments runs only promotionGate.eligible entries
# SCALP_DEPLOYMENTS_REGISTRY_STORE=auto                      # auto | kv | file
# SCALP_DEPLOYMENTS_REGISTRY_KV_KEY=scalp:deployments:registry:v1
# SCALP_DEPLOYMENT_FORWARD_GATE_MIN_ROLLS=6
# SCALP_DEPLOYMENT_FORWARD_GATE_MIN_PROFITABLE_PCT=55
# SCALP_DEPLOYMENT_FORWARD_GATE_MIN_MEAN_EXPECTANCY_R=0
# SCALP_DEPLOYMENT_FORWARD_GATE_MIN_TRADES_PER_WINDOW=2
# SCALP_DEPLOYMENT_FORWARD_GATE_MAX_DRAWDOWN_R=
# SCALP_DEPLOYMENT_FORWARD_GATE_MIN_WEEKLY_SLICES=
# SCALP_DEPLOYMENT_FORWARD_GATE_MIN_WEEKLY_PROFITABLE_PCT=
# SCALP_DEPLOYMENT_FORWARD_GATE_MIN_WEEKLY_MEDIAN_EXPECTANCY_R=
# SCALP_DEPLOYMENT_FORWARD_GATE_MAX_WEEKLY_TOP_WEEK_PNL_CONCENTRATION_PCT=
# SCALP_WEEKLY_ROBUSTNESS_ENABLED=true
# SCALP_WEEKLY_ROBUSTNESS_TOPK_PER_SYMBOL=2
# SCALP_WEEKLY_ROBUSTNESS_LOOKBACK_DAYS=90
# SCALP_WEEKLY_ROBUSTNESS_MIN_CANDLES_PER_SLICE=180
# SCALP_WEEKLY_ROBUSTNESS_REQUIRE_WINNER_SHORTLIST=true
# SCALP_WEEKLY_ROBUSTNESS_MIN_SLICES=8
# SCALP_WEEKLY_ROBUSTNESS_MIN_PROFITABLE_PCT=45
# SCALP_WEEKLY_ROBUSTNESS_MIN_MEDIAN_EXPECTANCY_R=0
# SCALP_WEEKLY_ROBUSTNESS_MIN_4W_NET_R=8
# SCALP_WEEKLY_ROBUSTNESS_MAX_TOP_WEEK_PNL_CONCENTRATION_PCT=80
# SCALP_STAGED_EVAL_ENABLED=true
# SCALP_STAGED_EVAL_STAGE_A_WEEKS=2
# SCALP_STAGED_EVAL_STAGE_B_WEEKS=6
# SCALP_STAGED_EVAL_STAGE_C_WEEKS=12
# SCALP_STAGED_EVAL_STAGE_A_KEEP_SHARE=0.40
# SCALP_STAGED_EVAL_STAGE_B_KEEP_SHARE=0.35
# SCALP_STAGED_EVAL_MIN_SURVIVORS=1
# SCALP_STAGED_EVAL_REOPEN_ON_EPOCH=true
# SCALP_ADAPTIVE_ENABLED=true
# SCALP_ADAPTIVE_RETRAIN_CADENCE_WEEKS=1
# SCALP_ADAPTIVE_LOCK_DAYS=14
# SCALP_ADAPTIVE_MIN_EXPECTANCY_DELTA_R=0.02
# SCALP_ADAPTIVE_MIN_SUPPORT=30
# SCALP_ADAPTIVE_MIN_CONFIDENCE=0.60
# SCALP_ADAPTIVE_PILOT_SIZE=6
# SCALP_ADAPTIVE_PILOT_ROTATE_WEEKS=4
# SCALP_DEPLOYMENT_ALLOW_INELIGIBLE_ENABLE=false            # emergency override; avoid enabling unless needed
# SCALP_SYMBOL_DISCOVERY_POLICY_PATH=data/scalp-symbol-discovery-policy.json
# SCALP_SYMBOL_UNIVERSE_STORE=auto                           # auto | kv | file
# SCALP_SYMBOL_UNIVERSE_PATH=data/scalp-symbol-universe.json # used when store=file
# SCALP_RESEARCH_SYMBOL_COOLDOWN_ENABLED=true
# SCALP_RESEARCH_SYMBOL_COOLDOWN_FAILURE_THRESHOLD=3
# SCALP_RESEARCH_SYMBOL_COOLDOWN_WINDOW_MS=1800000
# SCALP_RESEARCH_SYMBOL_COOLDOWN_DURATION_MS=10800000
# SCALP_RESEARCH_SYMBOL_COOLDOWN_MAX_TRACKED_SYMBOLS=400
# SCALP_RESEARCH_WORKER_CONCURRENCY=4
# SCALP_RESEARCH_WORKER_MAX_CONCURRENCY=16
# SCALP_RESEARCH_WORKER_MAX_RUNS_CAP=200
# SCALP_V1_RESEARCH_MAX_CANDIDATES_CAP=80
# SCALP_V1_RESEARCH_MAX_SELF_HOPS_CAP=8               # must be >= cron selfMaxHops for v2 cycle/research auto-continue
# SCALP_V2_ENABLED=true
# SCALP_V2_LIVE_ENABLED=false                           # fail-closed by default
# SCALP_V2_DRY_RUN_DEFAULT=true
# SCALP_V2_DEFAULT_STRATEGY_ID=model_guided_composer_v2
# SCALP_V2_SEED_SYMBOLS_BITGET=BTCUSDT
# SCALP_V2_SEED_SYMBOLS_CAPITAL=EURUSD
# SCALP_V2_SEED_LIVE_SYMBOLS_BITGET=BTCUSDT
# SCALP_V2_SEED_LIVE_SYMBOLS_CAPITAL=EURUSD
# SCALP_V2_FORCE_FIXED_SEED_SCOPE=true                  # default true: clamp v2 scope to the fixed 48-symbol research universe
# SCALP_V2_COMPOSER_MAX_CANDIDATES_PER_SESSION=24       # bounded candidate pool generated per venue+symbol+session scope
# SCALP_V2_DISCOVER_PERSIST_CANDIDATES=false            # default false: discover updates cursors only; evaluate persists rotated slice
# SCALP_V2_MAX_CANDIDATES_TOTAL=200
# SCALP_V2_MAX_CANDIDATES_PER_SYMBOL=4
# SCALP_V2_MAX_ENABLED_DEPLOYMENTS=12
# SCALP_SYMBOL_DISCOVERY_SEED_ALLOW_BOOTSTRAP_SYMBOLS=false
# SCALP_GUARDRAIL_AUTO_PAUSE=true
# SCALP_GUARDRAIL_MIN_TRADES_30D=8
# SCALP_GUARDRAIL_MIN_EXPECTANCY_R_30D=-0.15
# SCALP_GUARDRAIL_MAX_DRAWDOWN_R_30D=
# SCALP_GUARDRAIL_MAX_EXPECTANCY_DRIFT_R_30D=
# SCALP_GUARDRAIL_MAX_TRADES_PER_DAY_30D=
# SCALP_GUARDRAIL_MIN_FORWARD_PROFITABLE_PCT=
# SCALP_HOUSEKEEPING_CYCLE_RETENTION_DAYS=1095             # weekly metrics retention (3 years)
# SCALP_HOUSEKEEPING_LOCK_MAX_AGE_MINUTES=45
# SCALP_HOUSEKEEPING_MAX_SCAN_KEYS=4000
# SCALP_HOUSEKEEPING_REFRESH_REPORT=false                  # no-op in async-job mode
# SCALP_HOUSEKEEPING_JOURNAL_MAX=500
# SCALP_HOUSEKEEPING_TRADE_LEDGER_MAX=10000

# KV (Upstash REST)
upstash_payasyougo_KV_REST_API_URL=https://...
upstash_payasyougo_KV_REST_API_TOKEN=...

# Optional
TAKER_FEE_RATE=0.0006          # used in prompts/edge checks
# AI_DECISION_POLICY=strict     # strict (default) | balanced
# ADMIN_ACCESS_SECRET=...       # enables auth on /api/evaluations and /api/chart
# BITGET_ACCOUNT_TYPE is set in lib/constants.ts (default: usdt-futures)
# AI_MODEL and AI_BASE_URL are set in lib/constants.ts (current defaults: gpt-5.2 @ api.openai.com)
```

3. Run the app:

```bash
npm run dev
# open http://localhost:3000
```

Build/start:

```bash
npm run build
npm run start
```

## API Routes (current behavior)

- `GET /api/swing/analyze`
  - Legacy alias: `GET /api/analyze`
  - Query params:
    - `symbol` (default `ETHUSDT`)
    - `platform` (`bitget|capital`, default `bitget`)
    - `newsSource` (`coindesk|marketaux`, default depends on platform: `bitget->coindesk`, `capital->marketaux`)
    - `category` (optional metadata tag, e.g. `forex|crypto|index|commodity|equity`)
    - `dryRun` (`true|false`, default `false`)
    - `notional` (default `100`)
    - `decisionPolicy` (`strict|balanced`, default `strict`)
  - Timeframes are currently fixed from `lib/constants.ts`:
    - `MICRO_TIMEFRAME=1H`, `PRIMARY_TIMEFRAME=4H`, `MACRO_TIMEFRAME=1D`, `CONTEXT_TIMEFRAME=1W`
  - Persists prompt, decision, execution result, and snapshot to KV (including `platform`, `newsSource`, `category`, and instrument identifier).
  - For `category=forex`, a compact macro-event context block is attached to prompt/snapshot as advisory input only (non-blocking).
  - When swing cron hard-deactivate is enabled, Vercel cron-triggered `/api/swing/analyze` requests no-op with a HOLD response (manual/admin-triggered calls still run).
- `GET /api/swing/ops/cron-control`
  - Returns current swing cron control state used by dashboard UI:
    - `hardDeactivated`
    - `reason`
    - `updatedAtMs`
    - `updatedBy`
- `POST /api/swing/ops/cron-control?hardDeactivated=true|false&reason=...&updatedBy=...`
  - Updates swing cron hard-deactivate state (stored in KV).
  - Intended for dashboard toggle / ops control.
- `GET /api/swing/evaluate`
  - Legacy alias: `GET /api/evaluate`
  - Query params: `symbol` (required), `limit` (clamped `5..30`, default `30`), `batchSize` (clamped `2..10`, default `5`), `includeBatchEvaluations` (`true|false`), `async` (`true|false`)
  - Async mode returns a `jobId`; poll with `GET /api/swing/evaluate?jobId=...`.
- `GET /api/swing/evaluations`
  - Legacy alias: `GET /api/evaluations`
  - Aggregated payload for dashboard (evaluation + latest prompt/decision + 7D PnL context).
  - Includes open-position fields used for live UI recomputation (direction, leverage, entry price).
  - Requires admin secret header when `ADMIN_ACCESS_SECRET` is set.
- `GET /api/swing/chart?symbol=...&timeframe=1H&limit=168`
  - Legacy alias: `GET /api/chart`
  - Candles + decision markers + recent position overlays for the requested window.
  - Defaults to a 7-day window if `limit` is omitted.
  - Normalizes timeframe strings to Bitget-compatible granularity (for example `1h` -> `1H`).
  - Requires admin secret header when `ADMIN_ACCESS_SECRET` is set.
- `GET /api/swing/rest-history?symbol=...&platform=...`
  - Legacy alias: `GET /api/rest-history`
  - Returns recent history entries for a symbol.
- `DELETE /api/swing/rest-history`
  - Legacy alias: `DELETE /api/rest-history`
  - Clears all decision history.
- `GET /api/health`
  - Liveness check.
- `GET /api/bitget-ping`
  - Public Bitget connectivity check.
- `GET /api/debug-env-values`
  - Redacted env presence check.
- `POST /api/admin-auth`
  - Body: `{ "secret": "..." }` to validate admin access when `ADMIN_ACCESS_SECRET` is set.
- Admin protection policy
  - All API routes except `/api/admin-auth` require `x-admin-access-secret: <ADMIN_ACCESS_SECRET>` (or `Authorization: Bearer <ADMIN_ACCESS_SECRET>`) when `ADMIN_ACCESS_SECRET` is set.
  - Unauthenticated exception for automation routes: `/api/swing/analyze`, `/api/scalp/v2/cron/discover`, `/api/scalp/v2/cron/load-candles`, `/api/scalp/v2/cron/evaluate`, `/api/scalp/v2/cron/worker`, `/api/scalp/v2/cron/promote`, `/api/scalp/v2/cron/execute`, `/api/scalp/v2/cron/reconcile`, `/api/scalp/v2/cron/cycle`.
- `GET /api/forex/*`
  - Forex mode routes are deprecated and currently return `410` with `error=forex_mode_deprecated`.
- `GET /api/scalp/cron/*` and `GET /api/scalp/ops/*` (legacy v1)
  - Retired after full v2 cutover; endpoints return `410 Gone` and migration guidance to `/api/scalp/v2/*`.
- `GET /api/scalp/v2/cron/discover`
  - Legacy alias kept for compatibility: `/api/scalp/cron/v2/discover`.
  - Native scalp-v2 candidate discovery (model-guided composer primitives), not legacy v1 symbol-discovery pipeline.
  - Scans model-guided candidate pools only from scalp-v2 runtime seed symbols (`seedSymbolsByVenue`) and supported sessions.
  - Default mode is sparse/scan-only (`SCALP_V2_DISCOVER_PERSIST_CANDIDATES=false`): updates research cursors without bulk candidate persistence.
  - Optional persistence mode (`SCALP_V2_DISCOVER_PERSIST_CANDIDATES=true`) upserts discovered candidates and applies runtime candidate-budget trimming.
  - By default, runtime seed scope is fixed to:
    - Bitget: 34 crypto symbols.
    - Capital: 14 forex/metals symbols.
  - `SCALP_V2_FORCE_FIXED_SEED_SCOPE=true` (default) enforces this clamp even if runtime/DB config contains wider seeds.
  - Candidate pool size per scope is bounded by `SCALP_V2_COMPOSER_MAX_CANDIDATES_PER_SESSION` (default `24`, max `96`).
  - Optional orchestration query params:
    - `autoSuccessor=true|false` (default `true`) to trigger `/api/scalp/v2/cron/evaluate`.
    - `evaluateBatchSize=<int>` batch size for downstream evaluate (default `200`).
    - `autoContinue=true|false` + `selfHop/selfMaxHops` for bounded detached recalls.
  - Legacy discover params (`dryRun`, `includeLiveQuotes`, `maxCandidates`) are accepted but ignored by native v2 discover.
- `GET /api/scalp/v2/cron/research?batchSize=100`
  - Unified v2 research pass (replaces legacy evaluate+worker split internally).
  - `batchSize` bounds how many selected candidates are backtested per invocation (default `100`).
  - Debug timing:
    - `debug=true` adds `job.details.timing` (per-label total/count/avg/max ms) for hotspot analysis.
    - Env alternative: `SCALP_V2_RESEARCH_DEBUG_TIMING=true`.
  - Optional orchestration query params:
    - `autoContinue=true|false` + `selfHop/selfMaxHops` for bounded detached self-recalls when `pendingAfter > 0`.
    - `autoSuccessor=true|false` (default `true`) to trigger `/api/scalp/v2/cron/promote` only after `pendingAfter` reaches `0`.
  - Local bulk support:
    - Use `scripts/research-local-bulk.ts` to support the Vercel research cron from local terminals when draining a large fresh candidate set.
    - Keep `BULK_WORK_LEASES=1` enabled. Each process uses its own research job scope and claims candidate rows with DB leases, so parallel terminals should not backtest the same candidate at the same time.
    - After a full research reset, start one terminal first until warm-up/candidate discovery completes, then start the remaining terminals. Starting all at once is safe, but can waste warm-up work.
    - For multiple terminals, shard symbol scanning with the same `BULK_SHARD_COUNT` and a unique zero-based `BULK_SHARD_INDEX` per terminal.

  ```bash
  # Terminal 1 of 5. Use BULK_SHARD_INDEX=1,2,3,4 in the other terminals.
  BULK_WORK_LEASES=1 \
  BULK_SHARD_COUNT=5 \
  BULK_SHARD_INDEX=0 \
  BULK_DISABLE_TIME_BUDGET=1 \
  BULK_SYMBOLS_PER_BATCH=10 \
  BULK_CANDIDATE_BATCH_SIZE=180 \
  BULK_BACKTEST_CONCURRENCY=4 \
  BULK_MIN_BACKTEST_CONCURRENCY=2 \
  BULK_LOW_PROGRESS_STREAK_FOR_BACKOFF=3 \
  npm exec tsx scripts/research-local-bulk.ts
  ```

  - With five terminals and `BULK_BACKTEST_CONCURRENCY=4`, expect up to 20 local replay workers. If the machine, DB, or network becomes the bottleneck, reduce `BULK_BACKTEST_CONCURRENCY` to `3` or `BULK_CANDIDATE_BATCH_SIZE` to `120`.
- `GET /api/scalp/v2/cron/evaluate?batchSize=200`
  - Native model-guided scoring pass over deterministic candidate pools for each venue+symbol+session scope.
  - Uses research cursor offsets (`scalp_v2_research_cursor.last_candidate_offset`) to rotate evaluation windows instead of rescoring full pools every run.
  - Persists only the selected window slice as `evaluated` candidates, keeping DB writes bounded while maintaining broad candidate coverage over successive runs.
  - Cursor offsets are advanced per scope and preserved across discover/promote updates.
  - Optional orchestration query params:
    - `autoSuccessor=true|false` (default `true`) to trigger `/api/scalp/v2/cron/worker`.
    - `workerBatchSize=<int>` for downstream worker batch size (default `12`).
- `GET /api/scalp/v2/cron/worker?batchSize=12`
  - Legacy alias kept for compatibility: `/api/scalp/cron/v2/worker`.
  - Native staged replay worker over `evaluated|shadow|promoted` candidates in runtime scope.
  - Runs phased validation with early cutoff:
    - Stage A: last 4 completed weeks.
    - Stage B: last 6 completed weeks.
    - Stage C: last 12 completed weeks.
  - Each stage enforces gates on min trades, netR, consecutive winning weeks, profit factor, and max drawdown.
  - If a stage fails, later stages are blocked to save compute.
  - Stores only compact stage metadata on each candidate (`metadata.worker.*`) and persists remarkable highlights for Stage-C passes.
  - Promotion now fail-closes on worker evidence: candidates missing Stage-C pass are rejected (`worker_stage_c_missing|worker_stage_c_failed`).
  - Sunday UTC safeguard: worker replay is blocked by default (`reason=sunday_utc_worker_blocked`); override only with `SCALP_V2_ALLOW_SUNDAY_WORKER=true`.
  - Optional orchestration query params:
    - `autoSuccessor=true|false` (default `true`) to trigger `/api/scalp/v2/cron/promote`.
- `GET /api/scalp/v2/cron/load-candles?batchSize=8&autoSuccessor=true&autoContinue=true&successor=cycle`
  - Legacy alias kept for compatibility: `/api/scalp/cron/v2/load-candles`.
  - Legacy-backed candle-maintenance job kept for Sunday data refresh/backfill only.
  - Processing scope is hard-filtered to scalp-v2 runtime symbols (seed + live-seed union, currently the fixed 48-symbol research universe by default).
  - Completed-week rollover now advances on UTC Sunday so Monday-Saturday candles count as the fresh completed week.
  - Sunday candles are excluded from storage ingest; on Sunday UTC, fetch windows are clamped to Saturday close.
  - Success criterion is strict by default: at least `12` completed Monday-Saturday weeks of 1m candles.
  - Symbols that cannot reach the required completed-week floor (for example newly listed instruments) are marked `load_status=failed` with `load_error=insufficient_completed_week_coverage`.
  - Loader behavior is progressive by default: prewarms recent candles first, then backfills older weeks in chunks.
  - Symbol source is `scalp_discovered_symbols` plus active deployment symbols seeded into that queue (`enabled` or `in_universe` or dirty flags), so active tracks keep 12-week freshness even if discovery churns.
  - Optional env knobs:
    - `SCALP_PIPELINE_LOAD_PREWARM_WEEKS` (default `1`)
    - `SCALP_PIPELINE_LOAD_BACKFILL_CHUNK_WEEKS` (default matches prewarm)
    - `SCALP_V2_LOAD_CANDLES_STALE_RECOVERY_DAYS` (default `14`) to widen incremental fetch windows for stale symbols and prevent Sunday freshness deadlocks.
      - Backward-compatible alias accepted: `CALP_V2_LOAD_CANDLES_STALE_RECOVERY_DAYS`.
  - Claims rows from `scalp_discovered_symbols` and updates per-symbol load status.
  - Successor chaining (after drain, `pendingAfter=0`):
    - `successor=cycle` (default) triggers `/api/scalp/v2/cron/cycle`.
    - `successor=discover` triggers `/api/scalp/v2/cron/discover?autoSuccessor=1`.
    - `successorDryRun=true|false` controls `cycle?dryRun=...` handoff (default `false`).
  - Debug query mode:
    - `debug=true` adds route-level diagnostics to the response (`debug.selectedScope`, `debug.timing`, `debug.scopeStats`).
    - `debug.scopeStats` includes per-symbol before/after 1m candle stats to verify whether a run actually advanced `toTsMs`.
  - No downstream chaining to v1-style `prepare/worker`; handoff is native v2 only.
- `GET /api/scalp/v2/cron/prepare?batchSize=6&autoSuccessor=true&autoContinue=true&session=berlin`
  - Legacy alias kept for compatibility: `/api/scalp/cron/v2/prepare`.
  - Deprecated compatibility no-op in scalp-v2 mode.
  - Returns `ok=true` with `details.reason=legacy_prepare_deprecated_for_v2`.
  - Replacement path is native v2 loop: `discover -> evaluate -> worker -> promote -> execute -> reconcile`.
- Legacy scalp-v2 route aliases
  - `/api/scalp/cron/v2/discover`, `/api/scalp/cron/v2/load-candles`, and `/api/scalp/cron/v2/prepare` are compatibility aliases that forward to `/api/scalp/v2/cron/*`.
- `GET/POST /api/scalp/v2/control`
  - Runtime control for scalp-v2 (`enabled`, `liveEnabled`, budgets, risk profile, seed symbols).
- `GET /api/scalp/v2/cron/cycle?dryRun=true|false`
  - Recommended production tuning for sustained research draining:
    - `batchSize=100`
    - `autoContinue=true`
    - `selfMaxHops=8` (ensure `SCALP_V1_RESEARCH_MAX_SELF_HOPS_CAP>=8` or route-level value will be clamped)
  - Full v2 auto loop in one run: discover -> evaluate -> worker -> promote -> execute -> reconcile.
  - Sunday UTC safeguard: execution step is skipped by default (`reason=sunday_utc_execution_blocked`); override only with `SCALP_V2_ALLOW_SUNDAY_EXECUTE=true`.
- `GET /api/scalp/v2/dashboard/summary`
  - V2 monitoring payload: runtime config, summary counters, deployments, events, and recent ledger rows.
- `GET /api/scalp/v2/ops/state`
  - V2 ops state plus phase-1 research cursor/highlight snapshots.
- `GET /api/scalp/v2/ops/research-primitives`
  - Phase-1 strategy decomposition payload: interpretable primitive catalog, per-strategy primitive references, and bounded candidate DSL preview for a venue/symbol/session.
- `GET/POST /api/scalp/v2/ops/research-state`
  - Phase-1 sparse research persistence surface:
    - Cursor/checkpoint rows in `scalp_v2_research_cursor`.
    - Remarkable outcomes only in `scalp_v2_research_highlights`.
  - Supports cursor updates and highlight upserts without storing full sweep traces.
- `POST /api/scalp/v2/ops/migrate-v1-ledger?limit=50000&parityDays=30`
  - Runs cutover backfill from legacy `scalp_trade_ledger`, `scalp_sessions`, and `scalp_journal` into v2 tables and returns table-level migration counters + parity summary.
- `GET /api/scalp/research/universe`, `GET /api/scalp/dashboard/summary`, `GET|POST /api/scalp/strategy/control`, and `GET /api/scalp/deployments/registry`
  - Legacy v1 surfaces are retired after full v2 cutover; endpoints return `410 Gone` with migration guidance to `/api/scalp/v2/*`.
- `GET /api/scalp/adaptive/summary?symbol=<SYMBOL>&session=berlin`
  - Returns active/candidate adaptive snapshots, lock state, arm-selection stats, and recent adaptive arm decisions for the symbol/session.
- `GET /api/scalp/v2/ops/strategy-catalog`
  - V2 strategy catalog for UI/backtest strategy pickers (default strategy id + registry rows).
- `GET /api/scalp/fill-candles-history?symbol=EURUSD&timeframe=15m&direction=backfill&days=30&dryRun=true`
  - Alias: `GET /api/scalp/candles-history/fill?...`
  - Fills persisted candle history for backtesting (`data/candles-history` locally, KV when `CANDLE_HISTORY_STORE=kv` or KV is auto-detected).
  - `direction=backfill` prepends older candles, `direction=forward` appends from the latest stored candle up to now.
  - `dryRun=true` reports coverage/added counts without writing.
- `GET /api/scalp/backtest/assets`
  - Returns Capital backtest symbol choices (merged default map + optional env overrides), grouped by inferred category.
- `POST /api/scalp/backtest/run`
  - Runs offline scalp replay on fetched Capital candles with parameter overrides.
  - Sunday UTC safeguard: blocked by default (`error=sunday_utc_replay_blocked`); override only with `SCALP_ALLOW_SUNDAY_REPLAY=true`.
  - Backtest runtime enforces the same TF pair as live: `M15` base and `M3` confirmation.
  - Supports optional `strategyId` to select a registered scalp strategy (default: runtime default strategy).
  - `/scalp-backtest` UI exposes a strategy selector sourced from `/api/scalp/v2/ops/strategy-catalog` and sends the selected `strategyId` on each run.
  - Supports both:
    - lookback mode (`lookbackPastValue`, `lookbackPastUnit`; optional `lookbackCandles` fallback)
    - date-range mode (`fromTsMs`, `toTsMs`) with paginated historical fetch (safe-capped to 90 days per run).
  - Optional history-source mode: `useStoredHistory=true` (plus optional `historyBackend=file|kv`, `historyTimeframe=15m`) to backtest from persisted candle cache before live Capital fetch fallback.
  - In history-source mode with `M15/M3`, replay uses `15m` candles for base/chart and `1m` candles for confirmation/price (aggregated to `M3`) to mirror live logic and reduce runtime cost.
  - Auto-falls back source resolution (`1m -> 5m -> 15m -> 1h`) if `1m` prices are unavailable for the symbol/range.
  - Returns summary, trades, and chart payload (`candles`, `markers`, `tradeSegments`) for UI rendering.

## Dry-Run Safety

`dryRun` defaults to `false` in the analysis routes. If you are testing and do not want real orders, pass `dryRun=true` explicitly.

Examples:

```bash
# Safe single-symbol run
curl "http://localhost:3000/api/swing/analyze?symbol=ETHUSDT&platform=bitget&newsSource=coindesk&dryRun=true&notional=100"

# Safe single-symbol run with looser AI guardrails
curl "http://localhost:3000/api/swing/analyze?symbol=ETHUSDT&platform=bitget&dryRun=true&notional=100&decisionPolicy=balanced"

# Safe non-crypto run on Capital.com
curl "http://localhost:3000/api/swing/analyze?symbol=QQQUSDT&platform=capital&newsSource=marketaux&dryRun=true&notional=100"
```

## Forex Replay Harness (Phase 1)

- Purpose:
  - Lightweight, quote-aware replay for forex execution/management logic validation.
  - Validates bid/ask stop sides, spread stress windows, partial/BE/trailing sequencing, contextual re-entry locks, and rollover fee effects.
- Safety:
  - Replay mode is fully offline (`scripts/forex-replay.ts`) and does not call Capital APIs.
  - Capital rate-limit note: keep live/capture integrations at or below `10 req/s` (recommended headroom target: `<=8 req/s`).
- Inputs:
  - JSON quote stream with `ts`, `bid`, `ask` (plus optional `eventRisk`, `shock`, `rollover`, `spreadMultiplier`, `forceCloseReasonCode`).
  - Optional entry signals (`side`, `stopPrice`, optional `takeProfitPrice`/`notionalUsd`).
- Usage:

```bash
# run sample replay and write artifacts to /tmp/forex-replay
npm run replay:run

# run spread/slippage/shock stress matrix and write /tmp/forex-replay-matrix
npm run replay:matrix

# run every curated fixture
node --import tsx scripts/forex-replay-matrix.ts --fixtures all --fixturesIndex data/replay/fixtures/index.json --outDir /tmp/forex-replay-matrix

# run a fixture subset
node --import tsx scripts/forex-replay-matrix.ts --fixtures london_whip_spread_spike,event_gap_through_stop --outDir /tmp/forex-replay-matrix

# run ad-hoc single file (no fixture index)
node --import tsx scripts/forex-replay-matrix.ts --input data/replay/eurusd.sample.json --outDir /tmp/forex-replay-matrix

# override shock profiles (none, occasional, clustered, frequent)
node --import tsx scripts/forex-replay-matrix.ts --fixtures core --shockProfiles none,clustered,frequent --outDir /tmp/forex-replay-matrix

# override constrained shortlist thresholds
node --import tsx scripts/forex-replay-matrix.ts --fixtures core --minCoverage 55 --maxChurn 70 --maxTailGap 2.5 --topKConstrained 10 --outDir /tmp/forex-replay-matrix

# run derisk pre-rollover profile and fixture-scale churn threshold
node --import tsx scripts/forex-replay-matrix.ts --fixtures core --rolloverForceCloseMin 20 --rolloverForceCloseMode derisk --rolloverDeriskPartialClosePct 50 --churnShortHoldMin 10 --outDir /tmp/forex-replay-matrix

# compare stop-invalidation cooldown profiles (disabled vs 30/60)
node --import tsx scripts/forex-replay-matrix.ts --fixtures core --rolloverForceCloseMin 20 --rolloverForceCloseSpreadToAtr 0.30 --rolloverForceCloseMode close --churnShortHoldMin 10 --reentryStopInvalidatedLockMin 30 --reentryStopInvalidatedLockStressMin 60 --outDir /tmp/forex-replay-matrix

# compare stop-invalidation minimum hold windows
node --import tsx scripts/forex-replay-matrix.ts --fixtures core --minCoverage 50 --maxChurn 65 --maxTailGap 2.8 --churnShortHoldMin 10 --stopInvalidateMinHoldMin 8 --outDir /tmp/forex-replay-matrix

# deterministic fixture to assert STOP_INVALIDATION_MIN_HOLD_ACTIVE appears in timeline output
node --import tsx scripts/forex-replay.ts --input data/replay/fixtures/stop_invalidation_min_hold_window.json --stopInvalidateMinHoldMin 8 --outDir /tmp/forex-replay-stop-hold
rg -n "STOP_INVALIDATION_MIN_HOLD_ACTIVE" /tmp/forex-replay-stop-hold/timeline.json

# run replay tests
npm run test:forex
```

- Artifacts written:
  - `summary.json`
  - `equity.json`
  - `timeline.json`
  - `ledger.csv`
  - matrix mode writes `matrix.summary.json`, `matrix.summary.csv`, and `matrix.frontier.csv` grouped by fixture + scenario, with fixture-level and scenario-level aggregates
  - `matrix.summary.csv` includes `shortHoldTradePct` (<=60m), `shortHold10mPct`, and `shortHoldTradePctForChurn` (threshold-controlled)
  - `matrix.frontier.csv` includes scenario tradeoff columns (`robustnessScore`, `tailGap`, `churnPenaltyPct`, `tradeCoveragePct`, `worstFixtureId`)
  - `matrix.summary.json` includes scenario robustness fields (`tradeCoveragePct`, `medianAvgRTraded`, `worstTradedAvgR`, `tailGap`, `costDragBps`, `churnPenaltyPct`, `robustnessScore`)
  - overview includes `churnShortHoldThresholdMinutes`, `topRobustScenarios`, `topRobustScenariosConstrained` (coverage/churn/tail-gated shortlist with rejection counts), `robustnessFrontier`, `topRobustByShockProfile`, `worstFixtureHeatmap`, and `scoreAudit`
  - score-audit dominance flags now trigger only on combined thresholds (`|rho| > 0.92` and top-k overlap `> 80%`)
  - curated fixture manifest lives at `data/replay/fixtures/index.json`

## Scalp Replay Harness (Phase 4)

- Purpose:
  - Offline replay/backtest for the scalp state machine (Asia range -> sweep/rejection -> displacement + MSS -> iFVG retest -> deterministic entry/exit).
  - Deterministic outputs for parameter tuning across fixtures with spread/slippage stress.
- Safety:
  - Replay runs are fully offline and never call Capital APIs.
  - Use replay first for strategy iteration; only move to cron dry-runs after replay behavior is acceptable.
- Usage:

```bash
# single fixture replay
npm run replay:scalp

# single fixture replay with parameter overrides
node --import tsx scripts/scalp-replay.ts \
  --input data/scalp-replay/fixtures/eurusd.sample.json \
  --strategyId regime_pullback_m15_m3 \
  --tpR 1.5 \
  --riskPct 0.25 \
  --sweepBufferPips 10 \
  --mssLookbackBars 2 \
  --ifvgEntryMode first_touch \
  --outDir /tmp/scalp-replay

# matrix replay on core fixtures (spread/slippage defaults)
npm run replay:scalp:matrix

# matrix with parameter grid
node --import tsx scripts/scalp-replay-matrix.ts \
  --fixtures core \
  --strategyIds regime_pullback_m15_m3,compression_breakout_pullback_m15_m3,trend_day_reacceleration_m15_m3 \
  --tpRs 1,1.5,2 \
  --riskPcts 0.2,0.35 \
  --sweepBufferPips 8,12 \
  --mssLookbackBars 2,3 \
  --ifvgEntryModes first_touch,midline_touch \
  --outDir /tmp/scalp-replay-matrix

# matrix with explicit scenario file
node --import tsx scripts/scalp-replay-matrix.ts \
  --fixtures core \
  --scenarioFile data/scalp-replay/scenarios/example.json \
  --outDir /tmp/scalp-replay-matrix
```

- Optional scenario-file shape:

```json
{
  "scenarios": [
    {
      "id": "base_fast",
      "strategyId": "regime_pullback_m15_m3",
      "spreadFactor": 1.0,
      "slippagePips": 0.15,
      "tpR": 1.5,
      "riskPct": 0.25,
      "sweepBufferPips": 10,
      "mssLookbackBars": 2,
      "ifvgEntryMode": "first_touch"
    }
  ]
}
```

- Artifacts:
  - Single replay: `summary.json`, `config.json`, `trades.json`, `timeline.json`, `trades.csv`, `timeline.csv`
  - Matrix replay: `matrix.summary.json`, `matrix.summary.csv`, `matrix.scenarios.csv`
  - `matrix.summary.json` includes both per-run rows and scenario-level robustness summaries (`tradeCoveragePct`, `avgNetR`, `worstMaxDrawdownR`, `robustnessScore`).

## Data Flow

1. **Analyze** selects provider by `platform`, pulls market data + selected news source, computes indicators/analytics, builds the prompt, calls the LLM, and (optionally) executes the trade.
2. The decision, snapshot, prompt, and execution result are appended to KV history.
3. **Evaluate** replays recent history through another LLM to score data quality, action logic, guardrails, etc., then stores a single latest evaluation per symbol.
4. The dashboard consumes `/api/swing/evaluations` and `/api/swing/chart` to render PnL, prompts, biases, aspect ratings, and recent position overlays.

## Frontend Notes

- Dashboard shell/state lives in `pages/index.tsx`; chart rendering lives in `components/ChartPanel.tsx`.
- Charts use `lightweight-charts` with custom overlays, a live pulse marker, and a fullscreen toggle.
- The UI uses Bitget public WebSocket ticker stream for live price updates and live open-PnL display.
- If no evaluations are present, run `GET /api/swing/analyze?...&dryRun=true` then `GET /api/swing/evaluate?...` to seed data before opening the page.

## Deployment

- Vercel-ready (`vercel.json` routes `/api/*` to Next API handlers). Provide the same env vars in Vercel’s dashboard or your host of choice.
- KV REST endpoint/token must be reachable from the runtime; Bitget/AI/News calls require outbound network access.
- Current cron entries include:
  - `/api/swing/analyze?...&dryRun=false` hourly (live-trading mode).
  - `/api/scalp/v2/cron/discover` every 3 hours.
  - `/api/scalp/v2/cron/evaluate?batchSize=200&autoSuccessor=false` every 5 minutes.
  - `/api/scalp/v2/cron/worker?batchSize=12&autoSuccessor=false` every 5 minutes (offset at minute `2`).
  - `/api/scalp/v2/cron/promote` every 10 minutes.
  - `/api/scalp/v2/cron/reconcile` every 2 minutes.
  - `/api/scalp/v2/cron/execute?dryRun=false` every minute (live v2 execution loop).
  - `/api/scalp/v2/cron/load-candles?batchSize=40&autoSuccessor=true&autoContinue=true&successor=discover` hourly on Sunday (`10 * * * 0`) to backfill/refresh 1m candle coverage, then hand off into native discover/evaluate path.
  - Symbol-specific scalp tuning is pinned by deployment row (`strategyId` + `tuneId`) in `data/scalp-deployments.json`.
- Cron-declared routes are intentionally allowed without admin secret; non-cron routes remain protected when `ADMIN_ACCESS_SECRET` is set.

## Troubleshooting

- `GET /api/debug-env-values` to confirm env vars are detected.
- `GET /api/swing/bitget-ping` to verify Bitget credentials/connectivity.
- Watch server logs for KV errors (missing `upstash_payasyougo_KV_REST_API_URL`/`upstash_payasyougo_KV_REST_API_TOKEN`) or provider failures (`COINDESK_API_KEY`, `MARKETAUX_API_KEY`, ForexFactory feed reachability, Capital credentials).
- UI backtest lab: open `/scalp-backtest` after signing in with `ADMIN_ACCESS_SECRET`.
  - Includes local preset save/load/delete and multi-run comparison overlays on one chart.
