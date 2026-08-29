// Global test env — runs before EVERY test file, in both projects
// (vitest.config.ts: setupFiles). Two jobs:
//
//  1. SCRUB anything that could reach production. Several lib modules read
//     process.env at import time, and a developer shell (or a leaked .env)
//     may carry real credentials — no test run may ever see them.
//
//  2. NEUTRALIZE retries and pacing so handler call counts are deterministic
//     and suites stay fast. These must be set here (not per test): the
//     Capital tuning consts and the KV client config freeze at module import.

// --- 1. Scrub -----------------------------------------------------------------

const SCRUBBED_ENV = [
    // credentials
    'BITGET_API_KEY',
    'BITGET_API_SECRET',
    'BITGET_API_PASSPHRASE',
    'BITGET_PAPTRADING',
    'CAPITAL_API_KEY',
    'CAPITAL_IDENTIFIER',
    'CAPITAL_PASSWORD',
    'CAPITAL_API_BASE',
    'AI_GATEWAY_API_KEY',
    'VERCEL_OIDC_TOKEN',
    'COINDESK_API_KEY',
    'MARKETAUX_API_KEY',
    'upstash_payasyougo_KV_REST_API_URL',
    'upstash_payasyougo_KV_REST_API_TOKEN',
    'ADMIN_ACCESS_SECRET',
    'VERCEL_AUTOMATION_BYPASS_SECRET',
    // Postgres — every URL lib/db/client.ts resolves
    'SCALP_PG_USE_HTTP',
    'SCALP_PG_CONNECTION_STRING',
    'NEON__DATABASE_URL',
    'NEON__POSTGRES_PRISMA_URL',
    'NEON__POSTGRES_URL',
    'NEON_DATABASE_URL',
    'NEON_POSTGRES_PRISMA_URL',
    'NEON_POSTGRES_URL',
    'DATABASE_URL',
    'POSTGRES_PRISMA_URL',
    'POSTGRES_URL',
    'PRISMA_CONNECTION_STRING',
    'PRISMA_PG_POSTGRES_URL',
    'PGHOST',
    'PGUSER',
    'PGDATABASE',
    'PGPASSWORD',
    // self-referential fetch targets — unset means clean no-op (the postmortem
    // worker trigger is fire-and-forget and would otherwise escape into a
    // later test's unhandled-request error)
    'VERCEL_URL',
    'SWING_POSTMORTEM_BASE_URL',
    'SCALP_ORCHESTRATOR_BASE_URL',
    'APP_BASE_URL',
    'URL',
    // behavior switches tests must own explicitly
    'SWING_AI_PROVIDER',
    'SWING_AI_CLAUDE_MODEL',
    'SWING_AI_CLAUDE_EFFORT',
    'SWING_BTC_CONTEXT_ENABLED',
    'AI_DECISION_POLICY',
    'SWING_PERPLEXITY_ENABLED',
    'SWING_PERPLEXITY_MODEL',
    'SWING_PERPLEXITY_TTL_SECONDS',
    'SWING_PERPLEXITY_FRESH_HOURS',
    'SWING_AI_BOUNCER_ENABLED',
    'SWING_AI_BOUNCER_MODEL',
];

for (const name of SCRUBBED_ENV) {
    delete process.env[name];
}

// --- 2. Neutralize ------------------------------------------------------------

Object.assign(process.env, {
    TZ: 'UTC',
    // Bitget: no GET retries, no market-endpoint pacing sleep
    BITGET_GET_MAX_ATTEMPTS: '1',
    BITGET_MARKET_MIN_INTERVAL_MS: '0',
    // KV: minimum retry budget ('0' would fall back to the default of 3 —
    // the env parse is `Number(...) || 3`), fastest backoff
    KV_MAX_RETRIES: '1',
    KV_RETRY_BASE_MS: '25',
    KV_RETRY_MAX_DELAY_MS: '50',
    // Capital: collapse the global rate limiter to ~1ms, no 429 retries
    CAPITAL_MAX_REQUESTS_PER_SECOND: '1000',
    CAPITAL_RATE_LIMIT_SAFETY_MS: '0',
    CAPITAL_MAX_429_RETRIES: '0',
});
