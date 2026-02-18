# AI Trade Decision Pipeline

Next.js app that runs an AI-driven trading loop for multiple platforms (Bitget + Capital.com): pull market data, compute flow/indicator gates, ask an OpenAI-compatible model for an action, optionally place the trade, store the decision history, and render a dashboard with PnL, prompts, and aspect evaluations.

---

## What’s Inside
- **Next.js API routes** for analysis (`/api/analyze`), AI-driven evaluations (`/api/evaluate`), history/PNL enrichment (`/api/evaluations`, `/api/rest-history`, `/api/chart`), and health/debug helpers.
- **Platform integrations**: Bitget (futures) and Capital.com (CFD/spot-style market access) with platform-selected market/execution paths.
- **Signal stack**: multi-timeframe indicators (context/macro/primary/micro), support/resistance levels, momentum/extension gates, and provider-selected news sentiment (`coindesk` or `marketaux`).
- **LLM prompts** built in `lib/ai.ts` with guardrails and momentum overrides; responses are persisted for replay and review.
- **Dashboard** (`pages/index.tsx` + `components/ChartPanel.tsx`) showing latest decisions, prompts, aspect ratings, 7D PnL, open positions, live ticker updates, and chart overlays of recent trades.
- **KV storage** (Upstash-compatible REST) for decision history, evaluations, and cached news (7d TTL for history, 1h for news).

## Requirements
- Node.js 18+ (matches Next 16)
- Bitget API key (futures) with trading permissions
- Capital.com Open API credentials (for `platform=capital`)
- OpenAI-compatible API key (used by `callAI`)
- CoinDesk API key (required for `newsSource=coindesk`)
- Marketaux API key (required for `newsSource=marketaux`)
- Upstash/Redis REST endpoint + token for KV (`KV_REST_API_URL`, `KV_REST_API_TOKEN`)
- Optional admin secret for protected routes (`ADMIN_ACCESS_SECRET`)

## Setup
1) Install deps:
```bash
npm install
```

2) Create `.env.local` with your secrets:
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

# KV (Upstash REST)
KV_REST_API_URL=https://...
KV_REST_API_TOKEN=...

# Optional
TAKER_FEE_RATE=0.0006          # used in prompts/edge checks
# AI_DECISION_POLICY=strict     # strict (default) | balanced
# ADMIN_ACCESS_SECRET=...       # enables auth on /api/evaluations and /api/chart
# BITGET_ACCOUNT_TYPE is set in lib/constants.ts (default: usdt-futures)
# AI_MODEL and AI_BASE_URL are set in lib/constants.ts (current defaults: gpt-5.2 @ api.openai.com)
```

3) Run the app:
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
- `GET /api/analyze`
  - Query params:
    - `symbol` (default `ETHUSDT`)
    - `platform` (`bitget|capital`, default `bitget`)
    - `newsSource` (`coindesk|marketaux`, default depends on platform: `bitget->coindesk`, `capital->marketaux`)
    - `dryRun` (`true|false`, default `false`)
    - `notional` (default `100`)
    - `decisionPolicy` (`strict|balanced`, default `strict`)
  - Timeframes are currently fixed from `lib/constants.ts`:
    - `MICRO_TIMEFRAME=1H`, `PRIMARY_TIMEFRAME=4H`, `MACRO_TIMEFRAME=1D`, `CONTEXT_TIMEFRAME=1W`
  - Persists prompt, decision, execution result, and snapshot to KV (including `platform`, `newsSource`, and instrument identifier).
- `GET /api/evaluate`
  - Query params: `symbol` (required), `limit` (clamped `5..30`, default `30`), `batchSize` (clamped `2..10`, default `5`), `includeBatchEvaluations` (`true|false`), `async` (`true|false`)
  - Async mode returns a `jobId`; poll with `GET /api/evaluate?jobId=...`.
- `GET /api/evaluations`
  - Aggregated payload for dashboard (evaluation + latest prompt/decision + 7D PnL context).
  - Includes open-position fields used for live UI recomputation (direction, leverage, entry price).
  - Requires `x-admin-access-secret` header only when `ADMIN_ACCESS_SECRET` is set.
- `GET /api/chart?symbol=...&timeframe=1H&limit=168`
  - Candles + decision markers + recent position overlays for the requested window.
  - Defaults to a 7-day window if `limit` is omitted.
  - Normalizes timeframe strings to Bitget-compatible granularity (for example `1h` -> `1H`).
  - Requires `x-admin-access-secret` header only when `ADMIN_ACCESS_SECRET` is set.
- `GET /api/rest-history?symbol=...&platform=...`
  - Returns recent history entries for a symbol.
- `DELETE /api/rest-history`
  - Clears all decision history.
- `GET /api/health`
  - Liveness check.
- `GET /api/bitget-ping`
  - Public Bitget connectivity check.
- `GET /api/debug-env-values`
  - Redacted env presence check.
- `POST /api/admin-auth`
  - Body: `{ "secret": "..." }` to validate admin access when `ADMIN_ACCESS_SECRET` is set.

## Dry-Run Safety
`dryRun` defaults to `false` in the analysis routes. If you are testing and do not want real orders, pass `dryRun=true` explicitly.

Examples:
```bash
# Safe single-symbol run
curl "http://localhost:3000/api/analyze?symbol=ETHUSDT&platform=bitget&newsSource=coindesk&dryRun=true&notional=100"

# Safe single-symbol run with looser AI guardrails
curl "http://localhost:3000/api/analyze?symbol=ETHUSDT&platform=bitget&dryRun=true&notional=100&decisionPolicy=balanced"

# Safe non-crypto run on Capital.com
curl "http://localhost:3000/api/analyze?symbol=QQQUSDT&platform=capital&newsSource=marketaux&dryRun=true&notional=100"
```

## Data Flow
1) **Analyze** selects provider by `platform`, pulls market data + selected news source, computes indicators/analytics, builds the prompt, calls the LLM, and (optionally) executes the trade.  
2) The decision, snapshot, prompt, and execution result are appended to KV history.  
3) **Evaluate** replays recent history through another LLM to score data quality, action logic, guardrails, etc., then stores a single latest evaluation per symbol.  
4) The dashboard consumes `/api/evaluations` and `/api/chart` to render PnL, prompts, biases, aspect ratings, and recent position overlays.

## Frontend Notes
- Dashboard shell/state lives in `pages/index.tsx`; chart rendering lives in `components/ChartPanel.tsx`.
- Charts use `lightweight-charts` with custom overlays, a live pulse marker, and a fullscreen toggle.
- The UI uses Bitget public WebSocket ticker stream for live price updates and live open-PnL display.
- If no evaluations are present, run `GET /api/analyze?...&dryRun=true` then `GET /api/evaluate?...` to seed data before opening the page.

## Deployment
- Vercel-ready (`vercel.json` routes `/api/*` to Next API handlers). Provide the same env vars in Vercel’s dashboard or your host of choice.
- KV REST endpoint/token must be reachable from the runtime; Bitget/AI/News calls require outbound network access.
- Current cron entries in `vercel.json` call `/api/analyze?...&dryRun=false` hourly, which is live-trading mode.

## Troubleshooting
- `GET /api/debug-env-values` to confirm env vars are detected.
- `GET /api/bitget-ping` to verify Bitget credentials/connectivity.
- Watch server logs for KV errors (missing `KV_REST_API_URL`/`KV_REST_API_TOKEN`) or news/provider auth failures (`COINDESK_API_KEY`, `MARKETAUX_API_KEY`, Capital credentials).
