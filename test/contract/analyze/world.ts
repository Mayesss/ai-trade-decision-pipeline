// The shared world of an analyze tick — private Bitget endpoints, the swing
// Postgres, and the decision payloads the canned model answers with.
//
// Market data comes from captured fixtures (test/contract/fixtures/, see
// scripts/capture-analyze-fixtures.ts); everything here is hand-canned shape
// work. Each scenario lives in its OWN test file: pages/api/analyze.ts and
// lib/capital.ts keep module-level caches (persist map, resolved epics), so
// per-file worker isolation is what keeps conversations independent.

import handler from '../../../pages/api/analyze';
import { getSwingAiThread } from '../../../lib/swing/pg';
import { createApiRequest, createApiResponse } from '../../harness/next';
import { bitgetData, bitgetGet } from '../../harness/worlds/bitget';
import { capitalGet, capitalSession } from '../../harness/worlds/capital';

import type { ApiResponseState } from '../../harness/next';
import type { PgResponder } from '../../harness/pg';
import type { RequestHandler } from 'msw';

import { resetEntries } from '../../harness/recorder';

// --- private Bitget endpoints ---------------------------------------------------

const ACCOUNT_ROW = {
    marginCoin: 'USDT',
    accountEquity: '10000',
    usdtEquity: '10000',
    available: '9500',
    crossedMaxAvailable: '9500',
    isolatedMaxAvailable: '9500',
};

/** No open position, no resting orders, empty venue history. */
export function flatPrivateWorld(): RequestHandler[] {
    return [
        bitgetGet('/api/v2/mix/position/all-position', []),
        bitgetGet('/api/v2/mix/account/accounts', [ACCOUNT_ROW]),
        // The TTL sweep reads BOTH entry books: resting limits from the plain
        // order book, resting stops from the normal_plan trigger book.
        bitgetGet('/api/v2/mix/order/orders-pending', { entrustedList: [] }),
        bitgetGet('/api/v2/mix/order/orders-plan-pending', { entrustedList: [] }),
        bitgetGet('/api/v2/mix/position/history-position', { list: [] }),
    ];
}

/** One open long with a standing exchange bracket (TP/SL plan orders). */
export function inPositionPrivateWorld(params: {
    symbol: string;
    entryPrice: string;
    markPrice: string;
    openedAtMs: number;
    takeProfit: string;
    stopLoss: string;
}): RequestHandler[] {
    return [
        bitgetGet('/api/v2/mix/position/all-position', [
            {
                symbol: params.symbol,
                holdSide: 'long',
                openPriceAvg: params.entryPrice,
                cTime: String(params.openedAtMs),
                posMode: 'one_way_mode',
                marginCoin: 'USDT',
                available: '0.05',
                total: '0.05',
                markPrice: params.markPrice,
                leverage: '5',
                unrealizedPL: '127.18',
            },
        ]),
        bitgetGet('/api/v2/mix/order/orders-plan-pending', {
            entrustedList: [
                { planType: 'pos_profit', triggerPrice: params.takeProfit, orderId: 'plan-tp-1', size: '0.05' },
                { planType: 'pos_loss', triggerPrice: params.stopLoss, orderId: 'plan-sl-1', size: '0.05' },
            ],
        }),
        bitgetGet('/api/v2/mix/account/accounts', [ACCOUNT_ROW]),
        bitgetGet('/api/v2/mix/position/history-position', { list: [] }),
    ];
}

export { bitgetData };

// --- private Capital.com endpoints ------------------------------------------------
// Market data and /markets metadata come from the captured fixture
// (capitalMarketWorld); these are the account-state endpoints, always canned.

const CAPITAL_ACCOUNTS = {
    accounts: [{ balance: { equity: 10000, available: 9500, balance: 10000 } }],
};

/** No open position, no resting working orders. */
export function capitalFlatPrivateWorld(): RequestHandler[] {
    return [
        capitalSession(),
        capitalGet('/api/v1/positions', { positions: [] }),
        capitalGet('/api/v1/accounts', CAPITAL_ACCOUNTS),
        capitalGet('/api/v1/workingorders', { workingOrders: [] }),
    ];
}

/** One open long with its bracket ON the position row (capital keeps TP/SL there). */
export function capitalInPositionPrivateWorld(params: {
    epic: string;
    entryLevel: number;
    bid: number;
    offer: number;
    openedAtMs: number;
    stopLevel: number;
    profitLevel: number;
}): RequestHandler[] {
    return [
        capitalSession(),
        capitalGet('/api/v1/positions', {
            positions: [
                {
                    market: { epic: params.epic, bid: params.bid, offer: params.offer },
                    position: {
                        direction: 'BUY',
                        level: params.entryLevel,
                        size: 10000,
                        leverage: 30,
                        createdDateUTC: new Date(params.openedAtMs).toISOString().slice(0, 19),
                        dealId: 'deal-test-1',
                        dealReference: 'ref-test-1',
                        stopLevel: params.stopLevel,
                        profitLevel: params.profitLevel,
                    },
                },
            ],
        }),
        capitalGet('/api/v1/accounts', CAPITAL_ACCOUNTS),
        capitalGet('/api/v1/workingorders', { workingOrders: [] }),
    ];
}

// --- swing Postgres ---------------------------------------------------------------

/** Answers the queries a tick issues; anything unexpected throws named. */
export const analyzePg: PgResponder = (text) => {
    const kind = text.split(' ')[0].toUpperCase();
    if (!['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH'].includes(kind)) return 0; // schema bootstrap DDL
    if (text.startsWith('INSERT INTO swing.decisions')) return [{ id: 4711 }];
    if (kind === 'INSERT' || kind === 'UPDATE' || kind === 'DELETE') return 1;
    if (text.includes('FROM swing.positions')) return [];
    // Bracket trail (which TP/SL was resting when a position closed).
    if (text.includes('FROM swing.decisions')) return [];
    if (text.includes('FROM swing.lessons')) return [];
    if (text.includes('FROM swing.ai_threads')) return [];
    if (text.includes('FROM swing.ai_cooldowns')) return [];
    if (text.includes('FROM swing.break_triggers')) return [];
    throw new Error(`analyze pg world: unexpected query: ${text}`);
};

/**
 * analyzePg with per-scenario answers layered on top: `override` may answer a
 * query (rows or count) or return undefined to fall through to the base.
 */
export function analyzePgWith(
    override: (text: string, values: unknown[]) => Record<string, unknown>[] | number | undefined,
): PgResponder {
    return (text, values) => override(text, values) ?? analyzePg(text, values);
}

// --- canned decisions --------------------------------------------------------------

/** Every SWING_DECISION_SCHEMA field, all-null except the action triple. */
export function decisionBase(action: string, summary: string, reason: string): Record<string, unknown> {
    return {
        action,
        summary,
        reason,
        exit_size_pct: null,
        leverage: null,
        raise_leverage_to: null,
        move_stop_to_be: null,
        take_profit_price: null,
        stop_loss_price: null,
        entry_limit_price: null,
        entry_stop_price: null,
        withdraw_resting_entry: null,
        entry_trigger_price: null,
        strategy: null,
        cooldown_minutes: null,
        cooldown_wake_above: null,
        cooldown_wake_below: null,
        cooldown_wake_confirm_minutes: null,
        cooldown_wake_note: null,
    };
}

// --- driving the handler --------------------------------------------------------------

/**
 * One dryRun analyze tick through the real Next handler (manual cadence: no
 * cron header, so the quiet/quarter gates stay out of the way).
 *
 * The first Postgres access in a worker also runs the ~60-statement schema
 * bootstrap; that is warmed (and the recorder reset) BEFORE the tick so the
 * snapshot holds only the tick's own conversation.
 */
export async function runAnalyzeTick(
    query: Record<string, string>,
    headers: Record<string, string> = {},
): Promise<ApiResponseState> {
    await getSwingAiThread('bitget', 'SCHEMA-WARMUP');
    resetEntries();

    const req = createApiRequest({ path: '/api/swing/analyze', query, headers });
    const { res, state } = createApiResponse();
    await handler(req as never, res as never);
    return state;
}
