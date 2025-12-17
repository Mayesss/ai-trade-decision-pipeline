# AI Trade Decision Pipeline

Next.js app that runs an AI-driven trading loop for Bitget futures: pull market data, compute flow/indicator gates, ask an OpenAI-compatible model for an action, optionally place the trade, store the decision history, and render a dashboard with PnL, prompts, and aspect evaluations.

---

## What’s Inside
- **Next.js API routes** for plan generation/execution (`/api/plan`, `/api/execute`), plan/execute evaluations (`/api/evaluate_plan`, `/api/evaluate_execute`), history/PNL enrichment (`/api/evaluations`, `/api/rest-history-plan`, `/api/rest-history-execute`, `/api/chart`), and health/debug helpers.
- **Deprecated legacy endpoints** moved under `/api/legacy/*` (`/api/legacy/analyze`, `/api/legacy/analyzeMultiple`, `/api/legacy/evaluate`, `/api/legacy/rest-history`) and a legacy dashboard at `/legacy`.
- **Bitget integration** for market data, positions, and live order placement (dry-run by default).
- **Signal stack**: order-flow analytics, multi-timeframe indicators (context/macro/primary/micro), support/resistance levels, momentum/extension gates, and CoinDesk news sentiment.
- **LLM prompts** built in `lib/ai.ts` with guardrails and momentum overrides; responses are persisted for replay and review.
- **Dashboard** (`pages/index.tsx`) showing the current plan, plan/execute evaluations, PnL, open positions, and chart overlays of recent execution events.
- **KV storage** (Upstash-compatible REST) for plan/executor history, evaluations, and cached news (7d TTL for histories, 1h for news).

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
- `POST /api/plan?symbol=...` — generate an hourly plan (`plan_v1`) and persist it (includes prompt).
- `POST /api/execute?symbol=...&dryRun=true|false` — deterministic executor that follows the current plan (logs recent runs).
- `POST /api/evaluate_plan` — LLM critique of plan quality for a symbol; saves the latest plan evaluation.
- `POST /api/evaluate_execute` — LLM critique of executor quality for a symbol; saves the latest execute evaluation.
- `GET /api/evaluations` — aggregated dashboard payload: plan/execute evaluations + open PnL + 24h realized PnL + win-rate sparkline.
- `GET /api/chart?symbol=...&timeframe=15m` — candles + markers + position overlays for the dashboard.
- `GET /api/rest-history-plan?symbol=...&limit=...` — raw stored plan history (KV).
- `GET /api/rest-history-execute?symbol=...&limit=...` — raw stored executor history (KV).
- `GET /api/health` — liveness; `GET /api/bitget-ping` — Bitget connectivity; `GET /api/debug-env-values` — redacted env status for debugging.

**Dry-run vs live trades:** `dryRun` defaults to `true`. Set `dryRun:false` to place real market orders on Bitget (uses isolated USDT futures and clamps leverage 1–5x).

## Data Flow
1) **Plan** pulls market data + gates + news sentiment, calls the LLM, and persists an hourly `plan_v1` (plus prompt).  
2) **Execute** runs deterministic entry/exit logic for the next hour using the plan and market gates; executor runs are appended to KV history.  
3) **Evaluate plan/execute** replays recent plan/executor history (plus recent PnL stats) through another LLM to score quality and guardrails, then stores a latest evaluation per symbol.  
4) The dashboard consumes `/api/evaluations` and `/api/chart` to render plan/executor quality, PnL, and recent position overlays.

## Frontend Notes
- UI lives in `pages/index.tsx`; Tailwind 4 utility classes are inlined (no config required).
- Charts use `lightweight-charts` with custom overlays; resize-safe.
- If no evaluations are present, run `POST /api/plan` then `POST /api/evaluate_plan` / `POST /api/evaluate_execute` to seed data before opening the page.

## Deployment
- Vercel-ready (`vercel.json` routes `/api/*` to Next API handlers). Provide the same env vars in Vercel’s dashboard or your host of choice.
- KV REST endpoint/token must be reachable from the runtime; Bitget/AI/News calls require outbound network access.

## Troubleshooting
- `GET /api/debug-env-values` to confirm env vars are detected.
- `GET /api/bitget-ping` to verify Bitget credentials/connectivity.
- Watch server logs for KV errors (missing `KV_REST_API_URL`/`KV_REST_API_TOKEN`) or CoinDesk auth failures.
