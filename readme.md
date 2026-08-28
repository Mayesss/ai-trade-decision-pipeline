# AI Trade Decision Pipeline

Next.js app that runs an AI-driven swing-trading loop for multiple platforms (Bitget + Capital.com): pull market data, compute flow/indicator gates, ask an LLM for an action, optionally place the trade, store the decision history in Postgres/KV, and render a dashboard with PnL, prompts, and aspect evaluations.

> **Note (2026-08):** the former forex and scalp subsystems were removed; `lib/swing`
> is the only trading actor. See `docs/cleanup-forex-scalp.md` for the retirement plan
> and git history for the removed code.

---

## What's Inside

- **Next.js API routes** for analysis (`/api/swing/analyze`), AI-driven evaluations (`/api/swing/evaluate`), history/PNL enrichment (`/api/swing/evaluations`, `/api/swing/rest-history`, `/api/swing/chart`), swing crons (wake-watch, postmortem drain, weekly digest), and health/debug helpers.
- **Platform integrations**: Bitget (futures) and Capital.com (CFD/spot-style market access) with platform-selected market/execution paths.
- **Signal stack**: multi-timeframe indicators (context/macro/primary/micro), support/resistance levels, momentum/extension gates, and provider-selected news sentiment (`coindesk` or `marketaux`).
- **LLM prompts** built in `lib/ai.ts` with guardrails and momentum overrides; responses are persisted for replay and review.
- **Dashboard** (`pages/index.tsx` + `components/ChartPanel.tsx`) showing latest decisions, prompts, aspect ratings, 7D PnL, open positions, live ticker updates, and chart overlays of recent trades.
- **Storage**: Neon Postgres (`swing.*` schema via the shared client in `lib/db/`) as the durable source of truth, with KV (Upstash-compatible REST) in front for decision history, evaluations, and cached news.

## Requirements

- Node.js 18+ (matches Next 16)
- Bitget API key (futures) with trading permissions
- Capital.com Open API credentials (for `platform=capital`)
- Vercel AI Gateway access (automatic via OIDC on Vercel; optional `AI_GATEWAY_API_KEY` for non-Vercel environments). Provider keys (OpenAI/Anthropic) live in the gateway's BYOK settings, not in env vars
- CoinDesk API key (required for `newsSource=coindesk`)
- Marketaux API key (required for `newsSource=marketaux`)
- ForexFactory public calendar feed (used for the swing economic-event context; no API key required)
- Neon Postgres connection string (`SCALP_PG_CONNECTION_STRING` or `NEON__DATABASE_URL` — the env names are historical and intentionally unchanged)
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

# AI — all calls route through the Vercel AI Gateway.
# Auth: AI_GATEWAY_API_KEY if set, otherwise the VERCEL_OIDC_TOKEN that Vercel
# provisions automatically (locally: refreshed by `vercel env pull`).
# Provider billing (BYOK vs Vercel credits) is configured in the AI Gateway
# dashboard, not via env vars.
# AI_GATEWAY_API_KEY=...

# News
COINDESK_API_KEY=...
MARKETAUX_API_KEY=...

# Forex event calendar (ForexFactory) — swing prompt context only
# Optional override:
# FOREX_FACTORY_CALENDAR_URL=https://nfs.faireconomy.media/ff_calendar_thisweek.json

# Postgres (Neon) — env names are historical ("SCALP_"), do not rename
SCALP_PG_CONNECTION_STRING=postgres://...
# SCALP_PG_USE_HTTP=false        # use the Neon HTTP driver instead of the pg pool

# Capital sizing/leverage floors (used by lib/capital.ts)
# SCALP_CAPITAL_LEVERAGE_FOREX=30
# SCALP_CAPITAL_LEVERAGE_INDEX=20
# SCALP_CAPITAL_LEVERAGE_COMMODITY=10
# SCALP_CAPITAL_LEVERAGE_METAL=20
# SCALP_CAPITAL_LEVERAGE_CRYPTO=2
# SCALP_CAPITAL_LEVERAGE_EQUITY=5
# SCALP_CAPITAL_LEVERAGE_OTHER=5
# SCALP_CAPITAL_USE_ACCOUNT_LEVERAGE=false
# SCALP_MAX_LEVERAGE=...
# SCALP_SYMBOL_PIP_SIZE_MAP={"XAUUSD":0.01}

# Cron chaining (wake-watch -> analyze)
# SCALP_ORCHESTRATOR_BASE_URL=...   # base URL override for self-invocation

# KV (Upstash REST)
upstash_payasyougo_KV_REST_API_URL=https://...
upstash_payasyougo_KV_REST_API_TOKEN=...

# Optional
TAKER_FEE_RATE=0.0006          # used in prompts/edge checks
# AI_DECISION_POLICY=strict     # strict (default) | balanced
# ADMIN_ACCESS_SECRET=...       # enables auth on /api/evaluations and /api/chart
# BITGET_ACCOUNT_TYPE is set in lib/constants.ts (default: usdt-futures)
# DEFAULT_AI_MODEL, FALLBACK_AI_MODEL and AI_BASE_URL are set in lib/constants.ts; the provider
# is inferred from the model id ('claude' → Anthropic, 'gpt' → OpenAI). Current pair:
# default openai/gpt-5.6-sol, fallback anthropic/claude-opus-4.8 (AI Gateway slugs).
# SWING_AI_PROVIDER (env) still forces a provider.
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

Tests and DB health:

```bash
npm test                # both Vitest projects: unit + boundary contracts
npm run test:unit       # pure-kernel tests (test/unit/, mirrors lib/)
npm run test:contract   # msw boundary-contract tests with conversation snapshots (test/contract/)
npm run db:pg:health    # counts the swing.* tables via the shared Neon client
```

Contract tests assert only what crosses the process boundary — the ordered
outgoing conversation (HTTP requests incl. full AI prompts, SQL) recorded by
`test/harness/` and snapshotted under `test/contract/**/__snapshots__/`. No test
ever reaches the real network: msw runs with `onUnhandledRequest: 'error'` and
Postgres is a fake client planted on `global.__pgClient`.

The `test/contract/analyze/` scenarios replay a full `/api/swing/analyze` tick
against real market data recorded in `test/contract/fixtures/`, with the test
clock frozen at the fixture's capture time. Bitget: flat-HOLD, entry-SELL,
in-position-manage, gated — plus LIVE (dryRun=false) scenarios pinning the
actual order bodies (set-leverage / place-order with preset bracket,
modify-tpsl-order with transcript replay). Capital: flat-gated,
in-position-manage (the full capital prompt + marketaux path — capital's
flat-only gates don't apply in-position), market-closed, and the live PUT
bracket amendment (whole-bracket replacement). Cross-cutting: event-blackout
and a production-shaped quarter cron tick (kill switch, last-scan, warm
latch). `test/contract/postmortem.contract.test.ts` and
`evaluate.contract.test.ts` net the other two AI prompt surfaces, and
`wake-watch*.contract.test.ts` cover the 1-minute watcher (band/emergency/
failed-break/close detection and the analyze self-fire, which msw intercepts
via the request's own host header).

`npm run test:fixtures:capture -- SYMBOL [capital [category]]` re-records a
fixture from one live dryRun tick. Bitget needs no credentials (public market
endpoints); Capital needs real `CAPITAL_API_KEY/IDENTIFIER/PASSWORD` in the
env (`vercel env pull`) for one read-only session — the session call is never
recorded and account-state endpoints are stubbed, so fixtures contain market
data only. Scenarios are calibrated to the captured market's structure, so
after a re-capture expect to re-check gate outcomes (promptSkipped), re-pick
the entry direction and refresh the snapshots. Capital tick tests run ~12s
each: the rate limiter serializes calls against the frozen clock.

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
  - Persists prompt, decision, execution result, and snapshot (including `platform`, `newsSource`, `category`, and instrument identifier).
  - For `category=forex`, a compact macro-event context block is attached to prompt/snapshot as advisory input only (non-blocking).
  - When swing cron hard-deactivate is enabled, Vercel cron-triggered `/api/swing/analyze` requests no-op with a HOLD response (manual/admin-triggered calls still run).
- `GET /api/swing/ops/cron-control`
  - Returns current swing cron control state used by dashboard UI:
    - `hardDeactivated`, `reason`, `updatedAtMs`, `updatedBy`
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
- `GET /api/swing/wake-watch` (cron, every minute)
  - Watches open positions/cooldown wake bands and fires `/api/swing/analyze` for symbols that need attention.
- `GET /api/swing/postmortem-drain` (cron)
  - Drains queued post-mortem/win-evaluation analyses.
- `GET /api/swing/weekly-digest?store=1` (cron, Sunday)
  - Builds and stores the deterministic weekly digest over the `swing.*` tables.
- `GET /api/dashboard/*`
  - Dashboard aggregates (`summary`, `decision`, `timeline`, `live-price`, `symbols`, `summary-warm-fallback`).
- `GET /api/health`
  - Liveness check.
- `GET /api/swing/bitget-ping` (legacy alias: `/api/bitget-ping`)
  - Public Bitget connectivity check.
- `GET /api/debug-env-values`
  - Redacted env presence check.
- `POST /api/admin-auth`
  - Body: `{ "secret": "..." }` to validate admin access when `ADMIN_ACCESS_SECRET` is set.
- Admin protection policy
  - All API routes except `/api/admin-auth` require `x-admin-access-secret: <ADMIN_ACCESS_SECRET>` (or `Authorization: Bearer <ADMIN_ACCESS_SECRET>`) when `ADMIN_ACCESS_SECRET` is set.
  - Unauthenticated exception for automation routes: `/api/swing/analyze`, `/api/swing/wake-watch`, `/api/swing/postmortem-drain`, `/api/swing/weekly-digest`, `/api/dashboard/summary-warm-fallback`.
- Removed: all `/api/forex/*` and `/api/scalp/*` routes (deleted with the forex/scalp retirement; they now return 404).

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

## Data Flow

1. **Analyze** selects provider by `platform`, pulls market data + selected news source, computes indicators/analytics, builds the prompt, calls the LLM, and (optionally) executes the trade.
2. The decision, snapshot, prompt, and execution result are persisted (Postgres `swing.*` + KV cache).
3. **Evaluate** replays recent history through another LLM to score data quality, action logic, guardrails, etc., then stores a single latest evaluation per symbol.
4. The dashboard consumes `/api/swing/evaluations`, `/api/dashboard/*`, and `/api/swing/chart` to render PnL, prompts, biases, aspect ratings, and recent position overlays.

## Frontend Notes

- Dashboard shell/state lives in `pages/index.tsx`; chart rendering lives in `components/ChartPanel.tsx`.
- Charts use `lightweight-charts` with custom overlays, a live pulse marker, and a fullscreen toggle.
- The UI uses Bitget public WebSocket ticker stream for live price updates and live open-PnL display.
- If no evaluations are present, run `GET /api/swing/analyze?...&dryRun=true` then `GET /api/swing/evaluate?...` to seed data before opening the page.

## Deployment

- Vercel-ready (`vercel.json` routes `/api/*` to Next API handlers). Provide the same env vars in Vercel's dashboard or your host of choice.
- Postgres + KV endpoints must be reachable from the runtime; Bitget/Capital/AI/News calls require outbound network access.
- Current cron entries (see `vercel.json`):
  - `/api/swing/analyze?...&dryRun=false` every 15 minutes per symbol (live-trading mode) across the crypto (Bitget) and capital (indices/commodities/forex) universes.
  - `/api/swing/wake-watch` every minute (position/cooldown wake sweep that fires analyze on demand).
  - `/api/swing/postmortem-drain` four times per hour.
  - `/api/dashboard/summary-warm-fallback` four times per hour.
  - `/api/swing/weekly-digest?store=1` Sunday mornings.
- Cron-declared routes are intentionally allowed without admin secret; non-cron routes remain protected when `ADMIN_ACCESS_SECRET` is set.

## Troubleshooting

- `GET /api/debug-env-values` to confirm env vars are detected.
- `GET /api/swing/bitget-ping` to verify Bitget credentials/connectivity.
- `npm run db:pg:health` to verify the Neon connection and `swing.*` table counts.
- Watch server logs for KV errors (missing `upstash_payasyougo_KV_REST_API_URL`/`upstash_payasyougo_KV_REST_API_TOKEN`) or provider failures (`COINDESK_API_KEY`, `MARKETAUX_API_KEY`, ForexFactory feed reachability, Capital credentials).
