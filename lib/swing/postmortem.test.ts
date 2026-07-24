import assert from 'node:assert/strict';
import test from 'node:test';

import type { SwingDecisionFullRow, SwingTickLogRow } from './pg';
import {
    buildPostmortemDossier,
    mergeSkips,
    pickPivotalDecisions,
    postmortemPnl,
    shouldEnqueuePostmortem,
    summarizePostExitBars,
    truncateMiddle,
} from './postmortem';

const MIN = 60_000;
const T0 = 1_750_000_000_000;
const at = (min: number) => T0 + min * MIN;

const win = (extra: Record<string, any> = {}) => ({
    id: 'pos-1',
    symbol: 'BTCUSDT',
    side: 'long' as const,
    entryTimestamp: at(0),
    exitTimestamp: at(600),
    entryPrice: 100,
    exitPrice: 95,
    pnlNet: null,
    pnlPct: null,
    pnlGross: null,
    pnlGrossPct: null,
    notional: null,
    leverage: null,
    ...extra,
});

const decision = (min: number, action: string, extra: Partial<SwingDecisionFullRow> = {}): SwingDecisionFullRow => ({
    id: 1000 + min,
    decidedAtMs: at(min),
    symbol: 'BTCUSDT',
    platform: 'bitget',
    action,
    dryRun: false,
    prompt: { system: 'SYSTEM PROMPT TEXT', user: `USER PROMPT AT MIN ${min}` },
    aiDecision: { action, summary: `sum-${min}`, reason: `reason-${min}` },
    execResult: {},
    snapshot: {},
    ...extra,
});

const tickSkip = (min: number, stage: string, metrics: Record<string, any> | null = null): SwingTickLogRow => ({
    id: min,
    tsMs: at(min),
    symbol: 'BTCUSDT',
    platform: 'bitget',
    kind: 'skip',
    stage,
    reason: `r-${stage}-${min}`,
    cadence: 'quarter',
    dryRun: false,
    gates: null,
    metrics,
});

test('shouldEnqueuePostmortem: loss-mode takes losses only, unknown pnl skipped', () => {
    assert.equal(shouldEnqueuePostmortem(win({ pnlNet: -12 }), 'loss'), true);
    assert.equal(shouldEnqueuePostmortem(win({ pnlPct: -3.4 }), 'loss'), true);
    assert.equal(shouldEnqueuePostmortem(win({ pnlNet: 8 }), 'loss'), false);
    // all-null pnl: not knowably a loss — the next re-sync (with pnl) retries
    assert.equal(shouldEnqueuePostmortem(win(), 'loss'), false);
    assert.equal(shouldEnqueuePostmortem(win({ pnlNet: 8 }), 'all'), true);
    assert.equal(shouldEnqueuePostmortem(win({ pnlNet: -8 }), 'off'), false);
    // still-open windows never qualify
    assert.equal(shouldEnqueuePostmortem(win({ exitTimestamp: null, pnlNet: -5 }), 'loss'), false);
});

test('postmortemPnl: net wins over pct over gross; zero is a valid value', () => {
    assert.equal(postmortemPnl({ pnlNet: -1, pnlPct: 5, pnlGross: null, pnlGrossPct: null }), -1);
    assert.equal(postmortemPnl({ pnlNet: null, pnlPct: 2, pnlGross: -9, pnlGrossPct: null }), 2);
    assert.equal(postmortemPnl({ pnlNet: 0, pnlPct: -2, pnlGross: null, pnlGrossPct: null }), 0);
    assert.equal(postmortemPnl({ pnlNet: null, pnlPct: null, pnlGross: null, pnlGrossPct: null }), null);
});

test('pickPivotalDecisions: entries/amends/last ranked above plain HOLDs; promptless rows excluded', () => {
    const rows = [
        decision(0, 'BUY', { execResult: { placed: true } }),
        decision(60, 'HOLD'),
        decision(120, 'HOLD', { execResult: { tpsl: { stopLoss: { applied: true } } } }),
        decision(180, 'HOLD', { prompt: null, execResult: { placed: true } }),
        decision(240, 'HOLD'), // last call → pivotal via recency bonus
    ];
    const picked = pickPivotalDecisions(rows);
    const ids = picked.map((d) => d.decidedAtMs);
    assert.equal(ids[0], at(0)); // entry outranks everything
    assert.ok(ids.includes(at(120))); // bracket amend
    assert.ok(ids.includes(at(240))); // last call
    assert.ok(!ids.includes(at(180))); // no stored prompt → can't show it
    assert.ok(!ids.includes(at(60))); // plain mid-trade HOLD
});

test('mergeSkips: same minute+stage dedupes preferring the metrics-bearing tick_log row', () => {
    const fromTicks = [tickSkip(10, 'flat_cooldown', { minutesLeft: 30 })].map((t) => ({
        ts: t.tsMs,
        iso: new Date(t.tsMs).toISOString(),
        stage: t.stage,
        reason: t.reason,
        metrics: t.metrics,
    }));
    const fromDecisions = [
        { ts: at(10) + 5_000, iso: '', stage: 'flat_cooldown', reason: 'dup', metrics: null },
        { ts: at(20), iso: '', stage: 'event_blackout_gate', reason: 'ecb', metrics: null },
    ];
    const merged = mergeSkips(fromTicks, fromDecisions);
    assert.equal(merged.length, 2);
    const cooldown = merged.find((s) => s.stage === 'flat_cooldown');
    assert.deepEqual(cooldown?.metrics, { minutesLeft: 30 });
});

test('truncateMiddle: keeps window start and end', () => {
    const rows = Array.from({ length: 10 }, (_, i) => i);
    const { rows: kept, dropped } = truncateMiddle(rows, 4);
    assert.deepEqual(kept, [0, 1, 8, 9]);
    assert.equal(dropped, 6);
    assert.equal(truncateMiddle(rows, 20).dropped, 0);
});

test('buildPostmortemDossier: digests calls, merges skips, inlines pivotal prompts once-off system prompt', () => {
    const decisions = [
        decision(0, 'BUY', {
            execResult: { placed: true, orderId: 'o-1' },
            aiDecision: {
                action: 'BUY',
                summary: 'entry',
                reason: 'breakout',
                take_profit_price: 110,
                stop_loss_price: 97,
                ai_model: 'claude-opus-4-8',
                ai_usage: { input_tokens: 9000, output_tokens: 400, cache_read_input_tokens: 8000 },
            },
        }),
        decision(60, 'HOLD'),
        // skip row persisted as a decision (hourly path)
        decision(120, 'HOLD', {
            prompt: null,
            aiDecision: { action: 'HOLD', decision_source: 'pre_ai_skip', skipStage: 'flat_cooldown', reason: 'cooldown' },
            snapshot: { skipStage: 'flat_cooldown', skipReason: 'cooldown' },
        }),
        // dry-run rows never pollute a real trade's dossier
        decision(180, 'BUY', { dryRun: true }),
        decision(540, 'CLOSE'),
    ];
    const ticks = [tickSkip(240, 'quiet_position', { moveAtr: 0.1 }), tickSkip(120, 'flat_cooldown', { minutesLeft: 12 })];
    const { dossier, aiUserMessage } = buildPostmortemDossier({
        position: { symbol: 'BTCUSDT', pnl_pct: -4 },
        fromMs: at(-30),
        toMs: at(600),
        decisions,
        ticks,
    });

    assert.equal(dossier.counts.ai_calls, 3); // BUY, HOLD, CLOSE (dry-run + skip excluded)
    assert.equal(dossier.counts.skipped_ticks, 2); // cooldown deduped with tick_log + quiet_position
    assert.ok(dossier.pivotal_decision_ids.includes(1000)); // entry
    assert.ok(dossier.pivotal_decision_ids.includes(1540)); // close
    const entryCall = dossier.ai_calls.find((c) => c.action === 'BUY');
    assert.equal(entryCall?.model, 'claude-opus-4-8');
    assert.deepEqual(entryCall?.tokens, { in: 9000, out: 400, cached: 8000 });
    assert.equal(entryCall?.stop_loss_price, 97);

    assert.ok(aiUserMessage.includes('USER PROMPT AT MIN 0'));
    assert.ok(aiUserMessage.includes('USER PROMPT AT MIN 540'));
    assert.ok(!aiUserMessage.includes('USER PROMPT AT MIN 180')); // dry-run excluded
    assert.equal(aiUserMessage.split('SYSTEM PROMPT TEXT').length, 2); // exactly once
    assert.ok(aiUserMessage.includes('quiet_position'));
});

// ---------------------------------------------------------------------------
// Post-exit market summary (pure over venue candle arrays)
// ---------------------------------------------------------------------------

test('summarizePostExitBars: measures high/low/last vs exit price inside the window', () => {
    const exitMs = T0;
    const bars = [
        [T0 - 15 * MIN, 100, 101, 99, 100.5], // before exit — excluded
        [T0 + 15 * MIN, 100, 104, 99.5, 103],
        [T0 + 30 * MIN, 103, 105, 102, 102.5],
        [T0 + 999 * MIN, 102, 120, 80, 90], // past toMs — excluded
    ];
    const out = summarizePostExitBars({ exitPrice: 100, exitMs, toMs: T0 + 60 * MIN, bars });
    assert.ok(out);
    assert.equal(out?.bars, 2);
    assert.equal(out?.high, 105);
    assert.equal(out?.low, 99.5);
    assert.equal(out?.last_close, 102.5);
    assert.equal(out?.max_up_from_exit_pct, 5);
    assert.equal(out?.max_down_from_exit_pct, -0.5);
    assert.equal(out?.last_from_exit_pct, 2.5);
});

test('summarizePostExitBars: null without a usable exit price or without in-window bars', () => {
    const bars = [[T0 + MIN, 100, 101, 99, 100]];
    assert.equal(summarizePostExitBars({ exitPrice: null, exitMs: T0, toMs: T0 + 60 * MIN, bars }), null);
    assert.equal(summarizePostExitBars({ exitPrice: 0, exitMs: T0, toMs: T0 + 60 * MIN, bars }), null);
    assert.equal(summarizePostExitBars({ exitPrice: 100, exitMs: T0, toMs: T0 + 60 * MIN, bars: [] }), null);
    assert.equal(
        summarizePostExitBars({
            exitPrice: 100,
            exitMs: T0,
            toMs: T0 + 60 * MIN,
            bars: [[T0 - 60 * MIN, 100, 101, 99, 100]],
        }),
        null,
    );
});
