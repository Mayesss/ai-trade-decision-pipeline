# AI Trade Decision Pipeline

Next.js app for Bitget USDT futures that generates an hourly AI plan and runs a deterministic executor against it. Plans and execution logs are persisted to KV and surfaced in a dashboard.

## How It Works
- **Plan** (`POST /api/plan`) uses an LLM to produce a strict `plan_v1` JSON plan for the next hour, using market data, gates, and news sentiment.
- **Execute** (`POST /api/execute`) runs deterministic logic against the plan and live data, producing `WAIT`, `ENTER_LONG`, `ENTER_SHORT`, `CLOSE`, or `TRIM` decisions and logging each run.
- **Evaluate** (`POST /api/evaluate_plan`, `POST /api/evaluate_execute`) uses an LLM to score plan/executor quality from recent logs.
- **Dashboard** (`pages/index.tsx`) pulls `/api/evaluations` and `/api/chart` for plan/execution visibility.
- **Legacy** endpoints live under `/api/legacy/*` and `/legacy` (deprecated).

## Requirements
- Node.js 18+
- Bitget futures API key with trading permissions
- OpenAI-compatible API key
- CoinDesk API key (news sentiment)
- Upstash-compatible KV REST endpoint + token

## Environment
Create `.env.local`:
```bash
# Bitget
BITGET_API_KEY=...
BITGET_API_SECRET=...
BITGET_API_PASSPHRASE=...

# AI
OPENAI_API_KEY=...

# News
COINDESK_API_KEY=...

# KV (Upstash REST)
KV_REST_API_URL=https://...
KV_REST_API_TOKEN=...

# Optional
TAKER_FEE_RATE=0.0006
TRIM_PROXIMITY_ATR_CONSERVATIVE=0.35
TRIM_PROXIMITY_ATR_DEFAULT=0.5
TRIM_MIN_HOLD_SECONDS_CONSERVATIVE=300
TRIM_MIN_HOLD_SECONDS_DEFAULT=180
```

Defaults are set in `lib/constants.ts`:
- `AI_MODEL` (default: `gpt-4.1-mini`)
- `AI_BASE_URL` (default: `https://api.openai.com/v1`)
- `BITGET_ACCOUNT_TYPE` (default: `usdt-futures`)

## Run
```bash
npm install
npm run dev
```
Build/start:
```bash
npm run build
npm run start
```

## Core API Routes
- `POST /api/plan?symbol=...` — generate and persist a `plan_v1` hourly plan.
- `POST /api/execute?symbol=...&dryRun=true|false` — deterministic executor for the current plan.
- `POST /api/evaluate_plan` — LLM critique of plan quality.
- `POST /api/evaluate_execute` — LLM critique of execution quality.
- `GET /api/evaluations` — aggregate payload for the dashboard.
- `GET /api/chart?symbol=...&timeframe=15m` — candles + markers for the dashboard.
- `GET /api/rest-history-plan?symbol=...&limit=...` — plan history from KV.
- `GET /api/rest-history-execute?symbol=...&limit=...` — execution history from KV.
- `GET /api/health` — liveness; `GET /api/bitget-ping` — Bitget connectivity.

## Execution Payload (Key Fields)
Every `/api/execute` response is logged and returned with structured fields for analysis:
- **Plan + state**: `plan_ts`, `plan_allowed_directions`, `plan_risk_mode`, `plan_entry_mode`, `position_state`, `entries_disabled`, `entry_blockers`
- **Market + signals**: `market`, `indicators`, `levels`, `entry_eval`, `gatesNow`
- **Exit context**: `invalidation_eval`, `exit_cause`, `trigger`
- **Position metrics**: `position_size_base`, `position_notional`
- **Exit order details**: `exit_order_reduce_only`, `exit_order_size_base`, `exit_order_size_notional`
- **Invalidation metadata**: `invalidation_notes`, `invalidation_rule_direction`
- **Order summary**: `order_details` (compact execution metadata)

## Behavior Notes
- Plans are bucketed to the last confirmed 1H close (`plan_ts`) and treated as stale after `horizon_minutes + 10m`.
- The executor obeys `allowed_directions`, `risk_mode`, `entry_mode`, `no_trade_rules`, and cooldowns before entering.
- Invalidations and direction mismatches exit using `CLOSE` (trims use reduce-only).
- `dryRun` defaults to `true`; set `dryRun=false` to place real orders.

## Troubleshooting
- `GET /api/debug-env-values` confirms env detection (redacted).
- If KV is missing, plans/logs cannot be persisted and `/api/plan` will return `persisted: false`.
