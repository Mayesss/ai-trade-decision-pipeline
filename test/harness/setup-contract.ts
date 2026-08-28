// Contract-project env — runs after setup-env.ts, before the test file
// imports anything. The boundary worlds (test/harness/worlds/) answer on real
// production hosts; msw intercepts them, so these values never leave the
// process — but they must EXIST, because the lib modules refuse to speak
// without credentials, and the KV base URL freezes at module import.

import { KV_TEST_URL } from './worlds/kv';

Object.assign(process.env, {
    // Upstash KV — import-time frozen in lib/kv.ts, lib/news.ts,
    // lib/history.ts and lib/utils.ts
    upstash_payasyougo_KV_REST_API_URL: KV_TEST_URL,
    upstash_payasyougo_KV_REST_API_TOKEN: 'test-kv-token',

    // AI Gateway (both providers resolve the same key, lazily per call)
    AI_GATEWAY_API_KEY: 'test-gateway-key',

    // Bitget (lazy reads inside bitgetFetch/signBitget)
    BITGET_API_KEY: 'test-bitget-key',
    BITGET_API_SECRET: 'test-bitget-secret',
    BITGET_API_PASSPHRASE: 'test-bitget-passphrase',

    // Capital.com (credentials are lazy; the base URL default is the real host)
    CAPITAL_API_KEY: 'test-capital-key',
    CAPITAL_IDENTIFIER: 'test-capital-user',
    CAPITAL_PASSWORD: 'test-capital-pass',

    // News providers (lazy)
    COINDESK_API_KEY: 'test-coindesk-key',
    MARKETAUX_API_KEY: 'test-marketaux-key',

    // Postgres: isPgConfigured() must be true so lib/swing/pg.ts does not
    // silently no-op — the connection itself never happens, because the
    // harness plants a fake client on global.__pgClient (test/harness/pg.ts).
    SCALP_PG_CONNECTION_STRING: 'postgresql://test:test@pg.boundary.test:5432/swing',
});
