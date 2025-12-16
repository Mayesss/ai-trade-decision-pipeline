# AI Trade Decision Pipeline

Next.js app that runs an AI-driven trading loop for Bitget futures: pull market data, compute flow/indicator gates, ask an OpenAI-compatible model for an action, optionally place the trade, store the decision history, and render a dashboard with PnL, prompts, and aspect evaluations.

---

## What’s Inside
- **Next.js API routes** for analysis (`/api/analyze`, `/api/analyzeMultiple`), AI-driven evaluations (`/api/evaluate`), history/PNL enrichment (`/api/evaluations`, `/api/rest-history`, `/api/chart`), and health/debug helpers.
- **Bitget integration** for market data, positions, and live order placement (dry-run by default).
- **Signal stack**: order-flow analytics, multi-timeframe indicators (context/macro/primary/micro), support/resistance levels, momentum/extension gates, and CoinDesk news sentiment.
- **LLM prompts** built in `lib/ai.ts` with guardrails and momentum overrides; responses are persisted for replay and review.
- **Dashboard** (`pages/index.tsx`) showing latest decisions, prompts, aspect ratings, PnL, open positions, and chart overlays of recent trades.
- **KV storage** (Upstash-compatible REST) for decision history, evaluations, and cached news (7d TTL for history, 1h for news).

## Requirements
- Node.js 18+ (matches Next 16)
- Bitget API key (futures) with trading permissions
- OpenAI-compatible API key (used by `callAI`)
- CoinDesk API key (required for news sentiment)
- Upstash/Redis REST endpoint + token for KV (`KV_REST_API_URL`, `KV_REST_API_TOKEN`)

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

# AI
OPENAI_API_KEY=...

# News
COINDESK_API_KEY=...

# KV (Upstash REST)
KV_REST_API_URL=https://...
KV_REST_API_TOKEN=...

# Optional
TAKER_FEE_RATE=0.0006          # used in prompts/edge checks
# BITGET_ACCOUNT_TYPE is set in lib/constants.ts (default: usdt-futures)
# AI_MODEL and AI_BASE_URL are set in lib/constants.ts (default: gpt-4.1-mini @ api.openai.com)
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

## Core API Routes (quick reference)
- `POST /api/analyze` — run the pipeline for one symbol. Body fields: `symbol` (e.g., `ETHUSDT`), `timeFrame` (default `1H`), `microTimeFrame` (`15m`), `macroTimeFrame` (`4H`), `contextTimeFrame` (`1D`), `dryRun` (defaults to `true`), `notional` (USDT sizing). Persists prompt, AI decision, metrics, and optional exec result.
- `POST /api/analyzeMultiple` — same as above, but concurrent over a list of symbols with built-in rate limiting/backoff.
- `POST /api/evaluate` — LLM critique of recent decisions for a symbol; saves the latest evaluation used by the dashboard.
- `GET /api/evaluations` — aggregated dashboard payload: last evaluation, last decision/prompt/metrics, open PnL, 24h realized PnL, win-rate sparkline, bias timeframes.
- `GET /api/chart?symbol=...&timeframe=15m` — candles + markers + position overlays for the dashboard.
- `GET /api/rest-history?symbol=...&limit=...` — raw stored decision history (KV).
- `GET /api/health` — liveness; `GET /api/bitget-ping` — Bitget connectivity; `GET /api/debug-env-values` — redacted env status for debugging.

**Dry-run vs live trades:** `dryRun` defaults to `true`. Set `dryRun:false` to place real market orders on Bitget (uses isolated USDT futures and clamps leverage 1–5x).

## Data Flow
1) **Analyze** pulls Bitget market data (ticker, candles, order book, funding, OI, trades optional) + news sentiment, computes indicators/analytics, builds the prompt, calls the LLM, and (optionally) executes the trade.  
2) The decision, snapshot, prompt, and execution result are appended to KV history.  
3) **Evaluate** replays recent history through another LLM to score data quality, action logic, guardrails, etc., then stores a single latest evaluation per symbol.  
4) The dashboard consumes `/api/evaluations` and `/api/chart` to render PnL, prompts, biases, aspect ratings, and recent position overlays.

## Frontend Notes
- UI lives in `pages/index.tsx`; Tailwind 4 utility classes are inlined (no config required).
- Charts use `lightweight-charts` with custom overlays; resize-safe.
- If no evaluations are present, run `POST /api/analyze` then `POST /api/evaluate` to seed data before opening the page.

## Deployment
- Vercel-ready (`vercel.json` routes `/api/*` to Next API handlers). Provide the same env vars in Vercel’s dashboard or your host of choice.
- KV REST endpoint/token must be reachable from the runtime; Bitget/AI/News calls require outbound network access.

## Troubleshooting
- `GET /api/debug-env-values` to confirm env vars are detected.
- `GET /api/bitget-ping` to verify Bitget credentials/connectivity.
- Watch server logs for KV errors (missing `KV_REST_API_URL`/`KV_REST_API_TOKEN`) or CoinDesk auth failures.
