// SITUATIONAL DOCTRINE gating: each block of "how to read market.X" prose is
// rendered ONLY on ticks whose payload X is actually present, in one section at
// the END of the system prompt (per-tick variation must live in the tail or it
// fragments the cache prefix that every symbol in a cron sweep shares).
//
// This is the regression net for that gating. Before it existed the doctrine
// was unconditional, so every contract snapshot covered it; now most blocks are
// absent from a routine tick and would vanish unnoticed if a gate broke.

import assert from 'node:assert/strict';
import { test } from 'vitest';

import { computeSwingState } from '../../../lib/swing/prompt';

const NOW_MS = 1_750_000_000_000;
const bundle: any = { ticker: [{ lastPr: '100', change24h: '0' }], candles: [] };
const indicators: any = {
    micro: '',
    macro: '',
    primary: { summary: '', timeframe: '4h' },
    context: { summary: '', timeframe: '1d' },
    microTimeFrame: '1h',
    macroTimeFrame: '1d',
    sr: {},
    rawCandles: {},
};
const momentum: any = { microExtensionInAtr: 0, info: { microEntryOk: false } };

type Opts = {
    symbol?: string;
    category?: string;
    positionContext?: any;
    cooldownWake?: any;
    failedBreak?: any;
    positionWake?: any;
    wakeSweeps?: any;
    reclaimWake?: any;
    lessons?: Array<{ scope: string; lesson: string }>;
    eventReaction?: any;
    btcContext?: any;
};

// Builds the SYSTEM prompt for one tick shape. Argument order mirrors
// computeSwingState's signature; assemble() takes the prompt-only extras.
function systemFor(opts: Opts = {}): string {
    const state = computeSwingState(
        opts.symbol ?? 'ETHUSDT',
        '4h',
        bundle,
        {},
        opts.positionContext ? 'long' : 'none',
        null,
        null,
        indicators,
        {},
        opts.positionContext ?? null,
        momentum,
        [],
        null,
        true,
        5,
        undefined,
        opts.category ?? 'crypto',
        'bitget',
        null,
        NOW_MS,
        null,
        opts.cooldownWake ?? null,
        opts.failedBreak ?? null,
        opts.positionWake ?? null,
        opts.wakeSweeps ?? null,
        opts.reclaimWake ?? null,
    );
    return state.assemble(
        null,
        [],
        null,
        null,
        opts.eventReaction ?? null,
        opts.btcContext ?? null,
        opts.lessons ?? null,
    ).system;
}

// The SECTION header specifically — the INPUTS pointer sentence also names
// "SITUATIONAL DOCTRINE", so a bare substring would match that instead.
const HEADER = 'SITUATIONAL DOCTRINE (optional blocks that ARE present on this tick';
const openPosition = { side: 'long', hold_minutes: 10, unrealized_pnl_pct_on_margin: 1 };

test('a routine tick renders NO situational section at all', () => {
    const sys = systemFor({ symbol: 'BTCUSDT' });
    assert.ok(!sys.includes(HEADER), 'routine tick should carry no situational section');
    // …and the INPUTS pointer stays honest rather than dangling.
    assert.ok(sys.includes('This tick carries none beyond the keys described above.'));
});

// Each case: the payload that must summon the block, and a phrase unique to it.
const cases: Array<[string, Opts, string]> = [
    [
        'wake-band trigger',
        { cooldownWake: { crossed: 'above', level: 105, setAtMs: NOW_MS - 60_000, note: 'breakout check' } },
        'Wake-band trigger (market.cooldown_wake)',
    ],
    [
        'wake-band sweeps',
        {
            wakeSweeps: [
                { side: 'above', level: 105, touchedAtMs: NOW_MS - 600_000, reclaimedAtMs: NOW_MS - 540_000, extreme: 106 },
            ],
        },
        'market.wake_band_sweeps: earlier touches',
    ],
    [
        'reclaim trigger',
        {
            reclaimWake: {
                side: 'above',
                level: 105,
                extreme: 106,
                atr: 2,
                touchedAtMs: NOW_MS - 600_000,
                reclaimedAtMs: NOW_MS - 540_000,
                note: 'sweep',
            },
        },
        'Reclaim trigger (market.reclaim_wake',
    ],
    [
        'failed break',
        {
            positionContext: openPosition,
            failedBreak: { side: 'long', triggerPrice: 98, barClose: 97, barClosedAtMs: NOW_MS - 300_000 },
        },
        'Failed-break trigger (market.failed_break)',
    ],
    [
        'position wake',
        {
            positionContext: openPosition,
            positionWake: { fired: { crossed: 'below', level: 95, setAtMs: NOW_MS - 60_000, note: 'exit check' } },
        },
        'Position-wake trigger (market.position_wake)',
    ],
    [
        'event reaction',
        {
            eventReaction: [
                {
                    title: 'CPI',
                    minutes_since_release: 60,
                    ret_since_release_bp: 40,
                    range_since_release_bp: 90,
                    retrace_pct: 0.2,
                },
            ],
        },
        'Post-event reaction (market.event_reaction)',
    ],
    [
        'btc regime',
        {
            btcContext: {
                reference: 'BTCUSDT',
                corr_30d: 0.8,
                corr_90d: 0.8,
                beta_90d: 1,
                btc: { ret_1h_bp: 1, ret_4h_bp: 1, ret_24h_bp: 1, ret_7d_bp: 1 },
                alt_vs_btc_residual_7d_bp: 5,
            },
        },
        'BTC regime (market.btc_context)',
    ],
    [
        'lessons',
        { lessons: [{ scope: 'global', lesson: 'do not chase breaks' }] },
        'LESSONS (user turn):',
    ],
];

for (const [name, opts, phrase] of cases) {
    test(`${name}: doctrine renders in the situational tail when its payload is present`, () => {
        const sys = systemFor(opts);
        assert.ok(sys.includes(HEADER), `${name}: expected a situational section`);
        assert.ok(sys.includes(phrase), `${name}: expected doctrine phrase "${phrase}"`);
        // It must live in the TAIL — after YOUR JOB, so the stable prefix is untouched.
        assert.ok(sys.indexOf(phrase) > sys.indexOf(HEADER), `${name}: doctrine must sit inside the situational section`);
    });

    test(`${name}: doctrine is absent when its payload is absent`, () => {
        // Same tick shape minus the payload (position context kept so the
        // in-position blocks are compared against the right variant).
        const sys = systemFor(opts.positionContext ? { positionContext: openPosition } : {});
        assert.ok(!sys.includes(phrase), `${name}: doctrine leaked onto a tick without the payload`);
    });
}

test('BTC own ticks drop the BTC-regime doctrine, non-BTC crypto keeps it', () => {
    const btcCtx = {
        reference: 'BTCUSDT' as const,
        corr_30d: 0.8,
        corr_90d: 0.8,
        beta_90d: 1,
        btc: { ret_1h_bp: 1, ret_4h_bp: 1, ret_24h_bp: 1, ret_7d_bp: 1 },
        alt_vs_btc_residual_7d_bp: 5,
    };
    // BTC's own tick never carries the payload (loadBtcContext no-ops on it).
    assert.ok(!systemFor({ symbol: 'BTCUSDT' }).includes('BTC regime (market.btc_context)'));
    assert.ok(systemFor({ symbol: 'SOLUSDT', btcContext: btcCtx }).includes('BTC regime (market.btc_context)'));
});

test('the situational section sits after YOUR JOB and before OUTPUT', () => {
    const sys = systemFor({ cooldownWake: { crossed: 'above', level: 105, setAtMs: NOW_MS, note: 'x' } });
    assert.ok(sys.indexOf('YOUR JOB') < sys.indexOf(HEADER));
    assert.ok(sys.indexOf(HEADER) < sys.indexOf('OUTPUT (every response)'));
});
