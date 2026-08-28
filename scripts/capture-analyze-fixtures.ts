// Captures REAL market-data responses for the analyze-tick contract tests
// (test/contract/analyze/*.contract.test.ts).
//
// Runs one real `dryRun=true` tick through the actual /api/analyze handler.
// What goes over the real network depends on the platform:
//
//   bitget   api.bitget.com/api/v2/mix/market/*  — public, unauthenticated;
//            recorded. Private endpoints (positions/accounts/orders) stubbed.
//
//   capital  api-capital.backend-capital.com — market data requires a REAL
//            session (set CAPITAL_API_KEY/IDENTIFIER/PASSWORD in the env, e.g.
//            `vercel env pull` to a file and source it). The session POST is
//            passed through but NEVER recorded; /api/v1/prices* and
//            /api/v1/markets* (pure market metadata) are recorded; the
//            account-state endpoints (positions, accounts, workingorders,
//            history/transactions) are stubbed so no real account data can
//            land in a fixture.
//
// Everything else (KV, AI gateway, news, ForexFactory) is stubbed in-process.
// The tests replay these fixtures through msw with the clock frozen at
// `capturedAtMs`, so age/staleness math reproduces this run exactly.
//
// Usage:
//   node --import tsx scripts/capture-analyze-fixtures.ts [SYMBOL]
//   node --import tsx scripts/capture-analyze-fixtures.ts EURUSD capital forex

import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';

const SYMBOL = (process.argv[2] || 'ETHUSDT').toUpperCase();
const PLATFORM = (process.argv[3] || 'bitget').toLowerCase();
const CATEGORY = (process.argv[4] || (PLATFORM === 'capital' ? 'forex' : 'crypto')).toLowerCase();
const FIXTURE_DIR = path.join(process.cwd(), 'test', 'contract', 'fixtures');

// --- env BEFORE any lib import (KV url and Capital consts freeze at import) --
const KV_STUB_URL = 'https://kv.capture.invalid';
Object.assign(process.env, {
    upstash_payasyougo_KV_REST_API_URL: KV_STUB_URL,
    upstash_payasyougo_KV_REST_API_TOKEN: 'capture-kv-token',
    AI_GATEWAY_API_KEY: 'capture-gateway-key', // AI is always stubbed — never bill a capture
    COINDESK_API_KEY: 'capture-coindesk-key',
    MARKETAUX_API_KEY: 'capture-marketaux-key',
});
// Real credentials (capital capture) come from the caller's env and win;
// these fillers only exist so a bitget capture signs with SOMETHING.
for (const [name, filler] of [
    ['BITGET_API_KEY', 'capture-bitget-key'],
    ['BITGET_API_SECRET', 'capture-bitget-secret'],
    ['BITGET_API_PASSPHRASE', 'capture-bitget-passphrase'],
    ['CAPITAL_API_KEY', 'capture-capital-key'],
    ['CAPITAL_IDENTIFIER', 'capture-capital-user'],
    ['CAPITAL_PASSWORD', 'capture-capital-pass'],
] as const) {
    if (!process.env[name]) process.env[name] = filler;
}
// No PG env: isPgConfigured() stays false and every pg helper no-ops.
for (const name of [
    'SCALP_PG_CONNECTION_STRING',
    'DATABASE_URL',
    'POSTGRES_URL',
    'POSTGRES_PRISMA_URL',
    'NEON_DATABASE_URL',
    'NEON__DATABASE_URL',
    'NEON__POSTGRES_URL',
    'NEON__POSTGRES_PRISMA_URL',
    'VERCEL_URL',
    'SWING_POSTMORTEM_BASE_URL',
    'SWING_AI_PROVIDER',
]) {
    delete process.env[name];
}

// --- recording fetch wrapper --------------------------------------------------

export interface CapturedMarketCall {
    path: string;
    /** Query params minus the volatile ones (startTime/endTime/after/from/to). */
    query: Record<string, string>;
    /** The full response body exactly as received. */
    body: unknown;
}

const VOLATILE_PARAMS = new Set(['startTime', 'endTime', 'after', 'idLessThan', 'from', 'to']);

function stableQuery(params: URLSearchParams): Record<string, string> {
    const out: Record<string, string> = {};
    for (const [key, value] of [...params.entries()].sort(([a], [b]) => a.localeCompare(b))) {
        if (!VOLATILE_PARAMS.has(key)) out[key] = value;
    }
    return out;
}

const captured = new Map<string, CapturedMarketCall>();
const capturedAtMs = Date.now();

function isRecordedMarketData(url: URL): boolean {
    if (url.hostname === 'api.bitget.com') return url.pathname.startsWith('/api/v2/mix/market/');
    if (url.hostname === 'api-capital.backend-capital.com') {
        // Market data and metadata only — account state never lands in fixtures.
        return url.pathname.startsWith('/api/v1/prices') || url.pathname.startsWith('/api/v1/markets');
    }
    return false;
}

/** Real network, no recording — the capital session login. */
function isPassthrough(url: URL): boolean {
    return url.hostname === 'api-capital.backend-capital.com' && url.pathname === '/api/v1/session';
}

// Canned HOLD answer satisfying SWING_DECISION_SCHEMA — the AI response is
// never captured (the contract tests can their own decisions per scenario).
const HOLD_DECISION = {
    action: 'HOLD',
    summary: 'capture run',
    reason: 'canned capture decision',
    exit_size_pct: null,
    leverage: null,
    raise_leverage_to: null,
    move_stop_to_be: null,
    take_profit_price: null,
    stop_loss_price: null,
    entry_limit_price: null,
    entry_trigger_price: null,
    cooldown_minutes: null,
    cooldown_wake_above: null,
    cooldown_wake_below: null,
    cooldown_wake_sustain_minutes: null,
    cooldown_wake_note: null,
};

function json(body: unknown): Response {
    return new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } });
}

function bitgetEnvelope(data: unknown): Response {
    return json({ code: '00000', msg: 'success', requestTime: 0, data });
}

function stubKv(url: URL, init?: RequestInit): Response {
    // Both Upstash conventions: JSON array body at the base, /CMD/args in the path.
    let command = url.pathname.split('/').filter(Boolean)[0]?.toUpperCase() || '';
    if (!command && typeof init?.body === 'string') {
        try {
            command = String((JSON.parse(init.body) as unknown[])[0] || '').toUpperCase();
        } catch {
            command = '';
        }
    }
    if (command === 'MGET' || command === 'LRANGE' || command === 'ZREVRANGE' || command === 'ZREVRANGEBYSCORE') {
        return json({ result: [] });
    }
    if (command === 'SCAN') return json({ result: ['0', []] });
    if (command === 'GET' || command === 'ZSCORE') return json({ result: null });
    return json({ result: 'OK' });
}

function stubBoundary(url: URL, init?: RequestInit): Response {
    if (url.hostname === 'api.bitget.com') {
        if (url.pathname === '/api/v2/mix/position/all-position') return bitgetEnvelope([]);
        if (url.pathname === '/api/v2/mix/position/history-position') return bitgetEnvelope({ list: [] });
        if (url.pathname === '/api/v2/mix/account/accounts') {
            return bitgetEnvelope([
                {
                    marginCoin: 'USDT',
                    accountEquity: '10000',
                    usdtEquity: '10000',
                    available: '9500',
                    crossedMaxAvailable: '9500',
                    isolatedMaxAvailable: '9500',
                },
            ]);
        }
        if (url.pathname === '/api/v2/mix/order/orders-pending') return bitgetEnvelope({ entrustedList: [] });
        if (url.pathname === '/api/v2/mix/order/orders-plan-pending') return bitgetEnvelope({ entrustedList: [] });
        throw new Error(`capture: unexpected private Bitget endpoint ${url.pathname}`);
    }
    if (url.hostname === 'api-capital.backend-capital.com') {
        if (url.pathname === '/api/v1/positions') return json({ positions: [] });
        if (url.pathname === '/api/v1/accounts') {
            return json({ accounts: [{ balance: { equity: 10000, available: 9500, balance: 10000 } }] });
        }
        if (url.pathname === '/api/v1/workingorders') return json({ workingOrders: [] });
        if (url.pathname === '/api/v1/history/transactions') return json({ transactions: [] });
        throw new Error(`capture: unexpected Capital endpoint ${url.pathname} (mutations are never allowed here)`);
    }
    if (url.href.startsWith(KV_STUB_URL)) return stubKv(url, init);
    if (url.hostname === 'ai-gateway.vercel.sh') {
        return json({
            id: 'resp_capture',
            model: 'openai/gpt-5.6-sol',
            output: [
                {
                    type: 'message',
                    role: 'assistant',
                    content: [{ type: 'output_text', text: JSON.stringify(HOLD_DECISION) }],
                },
            ],
            usage: { input_tokens: 0, output_tokens: 0 },
        });
    }
    if (url.hostname === 'data-api.coindesk.com') return json({ Data: [] });
    if (url.hostname === 'api.marketaux.com') return json({ data: [] });
    if (url.hostname === 'nfs.faireconomy.media') return json([]);
    throw new Error(`capture: unexpected host ${url.hostname} (${url.href})`);
}

const realFetch = globalThis.fetch;
globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    const method = String(init?.method ?? (input instanceof Request ? input.method : 'GET')).toUpperCase();
    if (isPassthrough(url)) return realFetch(input as never, init);
    if (isRecordedMarketData(url)) {
        if (method !== 'GET') throw new Error(`capture: refusing non-GET on market data: ${method} ${url.href}`);
        const res = await realFetch(input as never, init);
        const body = (await res.clone().json()) as unknown;
        const entry: CapturedMarketCall = { path: url.pathname, query: stableQuery(url.searchParams), body };
        const key = `${entry.path}?${JSON.stringify(entry.query)}`;
        if (!captured.has(key)) captured.set(key, entry); // keep the first — bundles repeat calls
        return res;
    }
    return stubBoundary(url, init);
}) as typeof fetch;

// --- minimal Next req/res -------------------------------------------------------

function createRes() {
    const state = { statusCode: 0, body: null as unknown };
    return {
        state,
        status(code: number) {
            state.statusCode = code;
            return this;
        },
        json(body: unknown) {
            state.body = body;
            return this;
        },
    };
}

// Extra public endpoints later scenarios need but a gated/HOLD-y tick may not
// have touched. Everything goes through the recording wrapper.
async function captureExtras() {
    if (PLATFORM === 'bitget') {
        const market = 'https://api.bitget.com/api/v2/mix/market';
        for (const extra of [
            `${market}/contracts?productType=usdt-futures`,
            `${market}/candles?symbol=${SYMBOL}&productType=usdt-futures&granularity=15m&limit=110`,
            `${market}/candles?symbol=${SYMBOL}&productType=usdt-futures&granularity=1D&limit=95`,
            `${market}/candles?symbol=BTCUSDT&productType=usdt-futures&granularity=1D&limit=95`,
            `${market}/candles?symbol=BTCUSDT&productType=usdt-futures&granularity=1H&limit=172`,
        ]) {
            await globalThis.fetch(extra);
        }
        return;
    }
    // Capital: authed candle reads must go through capitalFetch (session +
    // rate limiter), so use the lib itself. fetchCapitalCandlesByEpic passes
    // `limit` straight through as `max` (the bundle callers add their +10
    // before calling it), so these pairs ARE the (resolution, max)
    // combinations the tick issues: HOUR_4/40, MINUTE_15/116, MINUTE_15/120,
    // HOUR/130, HOUR/200, HOUR_4/200, DAY/200, WEEK/200.
    const { fetchCapitalCandlesByEpic, fetchCapitalMarketTradeability, resolveCapitalEpicRuntime } = await import(
        '../lib/capital'
    );
    const { epic } = await resolveCapitalEpicRuntime(SYMBOL);
    await fetchCapitalMarketTradeability(SYMBOL); // records /markets/{epic}
    for (const [timeframe, max] of [
        ['4H', 40],
        ['15m', 116],
        ['15m', 120],
        ['1H', 130],
        ['1H', 200],
        ['4H', 200],
        ['1D', 200],
        ['1W', 200],
    ] as const) {
        await fetchCapitalCandlesByEpic(epic, timeframe, max);
    }
}

async function main() {
    if (PLATFORM === 'capital' && (process.env.CAPITAL_API_KEY || '').startsWith('capture-')) {
        throw new Error(
            'capital capture needs real CAPITAL_API_KEY/IDENTIFIER/PASSWORD in the env (vercel env pull)',
        );
    }

    // Dynamic import AFTER env + fetch wrapper are in place.
    const { default: handler } = await import('../pages/api/analyze');

    const query: Record<string, string> = { symbol: SYMBOL, platform: PLATFORM, dryRun: 'true' };
    if (PLATFORM === 'capital') query.category = CATEGORY;
    const search = new URLSearchParams(query).toString();
    const req = { method: 'GET', url: `/api/swing/analyze?${search}`, query, headers: {} };
    const res = createRes();

    await handler(req as never, res as never);
    await captureExtras();

    mkdirSync(FIXTURE_DIR, { recursive: true });
    const outPath = path.join(FIXTURE_DIR, `${PLATFORM}-${SYMBOL}.json`);
    writeFileSync(
        outPath,
        `${JSON.stringify({ symbol: SYMBOL, platform: PLATFORM, capturedAtMs, entries: [...captured.values()] }, null, 2)}\n`,
    );

    const body = res.state.body as Record<string, unknown> | null;
    const decision = body?.decision as { action?: string; summary?: string; reason?: string } | undefined;
    console.log(`handler status: ${res.state.statusCode}`);
    console.log(`decision: ${JSON.stringify(decision)}`);
    console.log(`promptSkipped: ${body?.promptSkipped ?? false} | usedTape: ${body?.usedTape}`);
    console.log(`captured ${captured.size} unique market-data responses -> ${outPath}`);
    for (const entry of captured.values()) {
        console.log(`  ${entry.path} ${JSON.stringify(entry.query)}`);
    }
}

main().catch((err) => {
    console.error('capture failed:', err);
    process.exitCode = 1;
});
