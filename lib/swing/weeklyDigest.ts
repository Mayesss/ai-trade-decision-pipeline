// Weekly system digest (option "a" of the review design): a DETERMINISTIC
// aggregation over the durable swing tables — closed-trade performance, equity
// path, tick/gate health, postmortem verdict mix, lesson library churn —
// persisted per window by the Sunday cron. No AI involvement: the judgment
// pass (prompt changes, capability add/drop decisions) happens interactively
// with this digest as evidence. Numbers are segmented per capability where the
// data allows (side, platform, entry action) so "should we keep X" questions
// are answered by a number, not a vibe.
import { isScalpPgConfigured, scalpPrisma } from '../db/client';
import { sql } from '../db/sql';
import { ensureSwingSchema } from './pg';

const DAY_MS = 24 * 60 * 60 * 1000;

export type DigestTradeStats = {
    closed: number;
    wins: number;
    losses: number;
    winRate: number | null;
    pnlNetTotal: number;
    pnlGrossTotal: number;
    feesPaid: number;
    avgWinUsd: number | null;
    avgLossUsd: number | null;
    expectancyUsd: number | null;
    avgHoldingHours: number | null;
};

export type SwingWeeklyDigest = {
    generatedAtMs: number;
    window: { fromMs: number; toMs: number; days: number };
    equity: Array<{
        platform: string;
        readings: number;
        firstEquity: number | null;
        lastEquity: number | null;
        minEquity: number | null;
        maxEquity: number | null;
        changePct: number | null;
    }>;
    trades: DigestTradeStats & {
        largestWin: { symbol: string; platform: string; pnlNet: number } | null;
        largestLoss: { symbol: string; platform: string; pnlNet: number } | null;
        bySide: Array<{ side: string; closed: number; wins: number; pnlNetTotal: number }>;
        byPlatform: Array<{ platform: string; closed: number; wins: number; pnlNetTotal: number }>;
        bySymbol: Array<{ symbol: string; platform: string; closed: number; wins: number; pnlNetTotal: number }>;
    };
    prevWindowTrades: DigestTradeStats;
    decisions: Array<{ action: string; count: number }>;
    ticks: {
        total: number;
        aiCalls: number;
        skips: number;
        handlerErrors: number;
        aiUnavailable: number;
        topSkipStages: Array<{ stage: string; count: number }>;
    };
    postmortems: {
        total: number;
        byVerdict: Array<{ verdict: string; count: number }>;
        badLuck: number;
        processFaults: number;
        processFaultShare: number | null;
        unfinished: number;
    };
    lessons: {
        activeByScope: Array<{ scope: string; count: number; avgConfidence: number }>;
        addedInWindow: Array<{ scope: string; lesson: string; confidence: number }>;
        reinforcedInWindow: number;
        retiredInWindow: number;
    };
    openNow: Array<{ platform: string; symbol: string; status: string; turns: number }>;
};

function num(value: unknown): number | null {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

async function tradeStats(fromMs: number, toMs: number): Promise<DigestTradeStats> {
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<any>>(sql`
        SELECT count(*)::int AS closed,
               count(*) FILTER (WHERE pnl_net > 0)::int AS wins,
               count(*) FILTER (WHERE pnl_net <= 0)::int AS losses,
               coalesce(sum(pnl_net), 0)::float8 AS pnl_net_total,
               coalesce(sum(pnl_gross), 0)::float8 AS pnl_gross_total,
               avg(pnl_net) FILTER (WHERE pnl_net > 0)::float8 AS avg_win,
               avg(pnl_net) FILTER (WHERE pnl_net <= 0)::float8 AS avg_loss,
               avg(pnl_net)::float8 AS expectancy,
               avg((exit_ts_ms - entry_ts_ms) / 3600000.0)
                   FILTER (WHERE entry_ts_ms IS NOT NULL)::float8 AS avg_holding_hours
        FROM swing.positions
        WHERE status = 'closed' AND pnl_net IS NOT NULL
          AND exit_ts_ms >= ${fromMs} AND exit_ts_ms < ${toMs};
    `);
    const r = rows?.[0] ?? {};
    const closed = Number(r.closed) || 0;
    const wins = Number(r.wins) || 0;
    return {
        closed,
        wins,
        losses: Number(r.losses) || 0,
        winRate: closed > 0 ? wins / closed : null,
        pnlNetTotal: Number(r.pnl_net_total) || 0,
        pnlGrossTotal: Number(r.pnl_gross_total) || 0,
        feesPaid: (Number(r.pnl_gross_total) || 0) - (Number(r.pnl_net_total) || 0),
        avgWinUsd: num(r.avg_win),
        avgLossUsd: num(r.avg_loss),
        expectancyUsd: num(r.expectancy),
        avgHoldingHours: num(r.avg_holding_hours),
    };
}

export async function buildSwingWeeklyDigest(opts?: {
    toMs?: number;
    days?: number;
}): Promise<SwingWeeklyDigest | null> {
    if (!isScalpPgConfigured()) return null;
    await ensureSwingSchema();
    const db = scalpPrisma();

    const days = Math.max(1, Math.min(90, opts?.days ?? 7));
    const toMs = opts?.toMs ?? Date.now();
    const fromMs = toMs - days * DAY_MS;

    const [trades, prevWindowTrades] = await Promise.all([
        tradeStats(fromMs, toMs),
        tradeStats(fromMs - days * DAY_MS, fromMs),
    ]);

    const [extremes, bySide, byPlatform, bySymbol, equity, decisions, ticks, topSkips, pms, verdicts, lessonState, lessonChurn, lessonAdds, threads] =
        await Promise.all([
            db.$queryRaw<Array<any>>(sql`
                (SELECT 'win' AS kind, symbol, platform, pnl_net::float8 AS pnl_net FROM swing.positions
                 WHERE status = 'closed' AND pnl_net > 0 AND exit_ts_ms >= ${fromMs} AND exit_ts_ms < ${toMs}
                 ORDER BY pnl_net DESC LIMIT 1)
                UNION ALL
                (SELECT 'loss', symbol, platform, pnl_net::float8 FROM swing.positions
                 WHERE status = 'closed' AND pnl_net <= 0 AND exit_ts_ms >= ${fromMs} AND exit_ts_ms < ${toMs}
                 ORDER BY pnl_net ASC LIMIT 1);
            `),
            db.$queryRaw<Array<any>>(sql`
                SELECT coalesce(side, 'unknown') AS side, count(*)::int AS closed,
                       count(*) FILTER (WHERE pnl_net > 0)::int AS wins,
                       coalesce(sum(pnl_net), 0)::float8 AS pnl_net_total
                FROM swing.positions
                WHERE status = 'closed' AND pnl_net IS NOT NULL AND exit_ts_ms >= ${fromMs} AND exit_ts_ms < ${toMs}
                GROUP BY 1 ORDER BY closed DESC;
            `),
            db.$queryRaw<Array<any>>(sql`
                SELECT platform, count(*)::int AS closed,
                       count(*) FILTER (WHERE pnl_net > 0)::int AS wins,
                       coalesce(sum(pnl_net), 0)::float8 AS pnl_net_total
                FROM swing.positions
                WHERE status = 'closed' AND pnl_net IS NOT NULL AND exit_ts_ms >= ${fromMs} AND exit_ts_ms < ${toMs}
                GROUP BY 1 ORDER BY closed DESC;
            `),
            db.$queryRaw<Array<any>>(sql`
                SELECT symbol, platform, count(*)::int AS closed,
                       count(*) FILTER (WHERE pnl_net > 0)::int AS wins,
                       coalesce(sum(pnl_net), 0)::float8 AS pnl_net_total
                FROM swing.positions
                WHERE status = 'closed' AND pnl_net IS NOT NULL AND exit_ts_ms >= ${fromMs} AND exit_ts_ms < ${toMs}
                GROUP BY 1, 2 ORDER BY abs(coalesce(sum(pnl_net), 0)) DESC LIMIT 15;
            `),
            // Equity trace has two sources: hourly account_snapshots (equity
            // populated since 2026-07-28 — NULL before that) and the broker
            // reading captured into decision.risk_sizing at ENTRY time. Union
            // both: entries backfill the pre-fix history and cover snapshot
            // fetch failures.
            db.$queryRaw<Array<any>>(sql`
                WITH readings AS (
                    SELECT platform, captured_at_ms AS ts_ms, equity::float8 AS eq
                    FROM swing.account_snapshots
                    WHERE equity IS NOT NULL AND captured_at_ms >= ${fromMs} AND captured_at_ms < ${toMs}
                    UNION ALL
                    SELECT platform, decided_at_ms,
                           (ai_decision_json -> 'risk_sizing' ->> 'equity_usd')::float8
                    FROM swing.decisions
                    WHERE decided_at_ms >= ${fromMs} AND decided_at_ms < ${toMs}
                      AND jsonb_typeof(ai_decision_json -> 'risk_sizing' -> 'equity_usd') = 'number'
                )
                SELECT platform,
                       count(*)::int AS readings,
                       (array_agg(eq ORDER BY ts_ms ASC))[1]::float8 AS first_equity,
                       (array_agg(eq ORDER BY ts_ms DESC))[1]::float8 AS last_equity,
                       min(eq)::float8 AS min_equity,
                       max(eq)::float8 AS max_equity
                FROM readings GROUP BY platform;
            `),
            db.$queryRaw<Array<any>>(sql`
                SELECT coalesce(action, 'unknown') AS action, count(*)::int AS count
                FROM swing.decisions
                WHERE decided_at_ms >= ${fromMs} AND decided_at_ms < ${toMs} AND NOT dry_run
                GROUP BY 1 ORDER BY count DESC;
            `),
            db.$queryRaw<Array<any>>(sql`
                SELECT count(*)::int AS total,
                       count(*) FILTER (WHERE kind = 'ai_call')::int AS ai_calls,
                       count(*) FILTER (WHERE kind = 'skip')::int AS skips,
                       count(*) FILTER (WHERE stage = 'handler_error')::int AS handler_errors,
                       count(*) FILTER (WHERE stage = 'ai_unavailable')::int AS ai_unavailable
                FROM swing.tick_log
                WHERE ts_ms >= ${fromMs} AND ts_ms < ${toMs} AND NOT dry_run;
            `),
            db.$queryRaw<Array<any>>(sql`
                SELECT stage, count(*)::int AS count
                FROM swing.tick_log
                WHERE kind = 'skip' AND ts_ms >= ${fromMs} AND ts_ms < ${toMs} AND NOT dry_run
                GROUP BY stage ORDER BY count DESC LIMIT 10;
            `),
            db.$queryRaw<Array<any>>(sql`
                SELECT count(*)::int AS total,
                       count(*) FILTER (WHERE status IN ('queued', 'running', 'failed'))::int AS unfinished
                FROM swing.postmortems
                WHERE created_at >= to_timestamp(${fromMs} / 1000.0) AND created_at < to_timestamp(${toMs} / 1000.0);
            `),
            db.$queryRaw<Array<any>>(sql`
                SELECT coalesce(verdict, 'none') AS verdict, count(*)::int AS count
                FROM swing.postmortems
                WHERE status = 'succeeded'
                  AND created_at >= to_timestamp(${fromMs} / 1000.0) AND created_at < to_timestamp(${toMs} / 1000.0)
                GROUP BY 1 ORDER BY count DESC;
            `),
            db.$queryRaw<Array<any>>(sql`
                SELECT scope, count(*)::int AS count, avg(confidence)::float8 AS avg_confidence
                FROM swing.lessons WHERE status = 'active' GROUP BY scope;
            `),
            db.$queryRaw<Array<any>>(sql`
                SELECT count(*) FILTER (
                           WHERE created_at < to_timestamp(${fromMs} / 1000.0)
                             AND updated_at >= to_timestamp(${fromMs} / 1000.0) AND updated_at < to_timestamp(${toMs} / 1000.0)
                             AND status = 'active'
                       )::int AS reinforced,
                       count(*) FILTER (
                           WHERE status = 'retired'
                             AND updated_at >= to_timestamp(${fromMs} / 1000.0) AND updated_at < to_timestamp(${toMs} / 1000.0)
                       )::int AS retired
                FROM swing.lessons;
            `),
            db.$queryRaw<Array<any>>(sql`
                SELECT scope, lesson, confidence::float8 AS confidence
                FROM swing.lessons
                WHERE created_at >= to_timestamp(${fromMs} / 1000.0) AND created_at < to_timestamp(${toMs} / 1000.0)
                ORDER BY created_at DESC LIMIT 20;
            `),
            db.$queryRaw<Array<any>>(sql`
                SELECT platform, symbol, status, turns FROM swing.ai_threads ORDER BY updated_at DESC;
            `),
        ]);

    const win = (extremes || []).find((r) => r.kind === 'win');
    const loss = (extremes || []).find((r) => r.kind === 'loss');
    const verdictRows = (verdicts || []).map((r) => ({ verdict: String(r.verdict), count: Number(r.count) || 0 }));
    const badLuck = verdictRows.find((v) => v.verdict === 'bad_luck')?.count ?? 0;
    const judged = verdictRows.filter((v) => v.verdict !== 'none').reduce((s, v) => s + v.count, 0);
    const processFaults = judged - badLuck;

    return {
        generatedAtMs: Date.now(),
        window: { fromMs, toMs, days },
        equity: (equity || []).map((r) => {
            const first = num(r.first_equity);
            const last = num(r.last_equity);
            return {
                platform: String(r.platform),
                readings: Number(r.readings) || 0,
                firstEquity: first,
                lastEquity: last,
                minEquity: num(r.min_equity),
                maxEquity: num(r.max_equity),
                changePct: first && last && first > 0 ? ((last - first) / first) * 100 : null,
            };
        }),
        trades: {
            ...trades,
            largestWin: win ? { symbol: String(win.symbol), platform: String(win.platform), pnlNet: Number(win.pnl_net) } : null,
            largestLoss: loss ? { symbol: String(loss.symbol), platform: String(loss.platform), pnlNet: Number(loss.pnl_net) } : null,
            bySide: (bySide || []).map((r) => ({
                side: String(r.side), closed: Number(r.closed) || 0, wins: Number(r.wins) || 0, pnlNetTotal: Number(r.pnl_net_total) || 0,
            })),
            byPlatform: (byPlatform || []).map((r) => ({
                platform: String(r.platform), closed: Number(r.closed) || 0, wins: Number(r.wins) || 0, pnlNetTotal: Number(r.pnl_net_total) || 0,
            })),
            bySymbol: (bySymbol || []).map((r) => ({
                symbol: String(r.symbol), platform: String(r.platform), closed: Number(r.closed) || 0, wins: Number(r.wins) || 0, pnlNetTotal: Number(r.pnl_net_total) || 0,
            })),
        },
        prevWindowTrades,
        decisions: (decisions || []).map((r) => ({ action: String(r.action), count: Number(r.count) || 0 })),
        ticks: {
            total: Number(ticks?.[0]?.total) || 0,
            aiCalls: Number(ticks?.[0]?.ai_calls) || 0,
            skips: Number(ticks?.[0]?.skips) || 0,
            handlerErrors: Number(ticks?.[0]?.handler_errors) || 0,
            aiUnavailable: Number(ticks?.[0]?.ai_unavailable) || 0,
            topSkipStages: (topSkips || []).map((r) => ({ stage: String(r.stage), count: Number(r.count) || 0 })),
        },
        postmortems: {
            total: Number(pms?.[0]?.total) || 0,
            byVerdict: verdictRows,
            badLuck,
            processFaults,
            processFaultShare: judged > 0 ? processFaults / judged : null,
            unfinished: Number(pms?.[0]?.unfinished) || 0,
        },
        lessons: {
            activeByScope: (lessonState || []).map((r) => ({
                scope: String(r.scope), count: Number(r.count) || 0, avgConfidence: Number(r.avg_confidence) || 0,
            })),
            addedInWindow: (lessonAdds || []).map((r) => ({
                scope: String(r.scope), lesson: String(r.lesson), confidence: Number(r.confidence) || 0,
            })),
            reinforcedInWindow: Number(lessonChurn?.[0]?.reinforced) || 0,
            retiredInWindow: Number(lessonChurn?.[0]?.retired) || 0,
        },
        openNow: (threads || []).map((r) => ({
            platform: String(r.platform), symbol: String(r.symbol), status: String(r.status), turns: Number(r.turns) || 0,
        })),
    };
}

// Persist one digest per window (cron); re-runs of the same window overwrite —
// later data (drained postmortems, late reconciles) makes the re-run strictly
// better informed.
export async function storeSwingWeeklyDigest(digest: SwingWeeklyDigest): Promise<number | null> {
    if (!isScalpPgConfigured()) return null;
    await ensureSwingSchema();
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<any>>(sql`
        INSERT INTO swing.weekly_digests (window_from_ms, window_to_ms, digest_json)
        VALUES (${digest.window.fromMs}, ${digest.window.toMs}, ${JSON.stringify(digest)}::jsonb)
        ON CONFLICT (window_from_ms, window_to_ms)
        DO UPDATE SET digest_json = EXCLUDED.digest_json
        RETURNING id;
    `);
    return Number(rows?.[0]?.id) || null;
}

export type StoredDigestMeta = {
    id: number;
    windowFromMs: number;
    windowToMs: number;
    createdAtMs: number;
};

export async function listSwingWeeklyDigests(limit = 26): Promise<StoredDigestMeta[]> {
    if (!isScalpPgConfigured()) return [];
    await ensureSwingSchema();
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<any>>(sql`
        SELECT id, window_from_ms, window_to_ms,
               (extract(epoch FROM created_at) * 1000)::float8 AS created_at_ms
        FROM swing.weekly_digests
        ORDER BY window_to_ms DESC
        LIMIT ${Math.max(1, Math.min(100, limit))};
    `);
    return (rows || []).map((r) => ({
        id: Number(r.id),
        windowFromMs: Number(r.window_from_ms),
        windowToMs: Number(r.window_to_ms),
        createdAtMs: Number(r.created_at_ms) || 0,
    }));
}

export async function loadSwingWeeklyDigestById(id: number): Promise<SwingWeeklyDigest | null> {
    if (!isScalpPgConfigured() || !Number.isFinite(id)) return null;
    await ensureSwingSchema();
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<any>>(sql`
        SELECT digest_json FROM swing.weekly_digests WHERE id = ${Math.trunc(id)} LIMIT 1;
    `);
    const raw = rows?.[0]?.digest_json;
    if (!raw) return null;
    return (typeof raw === 'string' ? JSON.parse(raw) : raw) as SwingWeeklyDigest;
}

const fmtUsd = (n: number | null | undefined) => (n === null || n === undefined ? 'n/a' : `$${n.toFixed(2)}`);
const fmtPct = (n: number | null | undefined) => (n === null || n === undefined ? 'n/a' : `${(n * 100).toFixed(0)}%`);

// Human/AI-pasteable rendering — this string is what gets dropped into the
// interactive judgment session, so it must stand alone without the JSON.
export function renderSwingWeeklyDigestMarkdown(d: SwingWeeklyDigest): string {
    const from = new Date(d.window.fromMs).toISOString().slice(0, 10);
    const to = new Date(d.window.toMs).toISOString().slice(0, 10);
    const t = d.trades;
    const p = d.prevWindowTrades;
    const lines: string[] = [
        `# Swing weekly digest ${from} → ${to}`,
        '',
        `## Trades (closed)`,
        `- ${t.closed} closed (prev window ${p.closed}): ${t.wins}W/${t.losses}L, win rate ${fmtPct(t.winRate)} (prev ${fmtPct(p.winRate)})`,
        `- PnL net ${fmtUsd(t.pnlNetTotal)} (prev ${fmtUsd(p.pnlNetTotal)}); gross ${fmtUsd(t.pnlGrossTotal)}; fees ${fmtUsd(t.feesPaid)}`,
        `- expectancy/trade ${fmtUsd(t.expectancyUsd)}; avg win ${fmtUsd(t.avgWinUsd)} vs avg loss ${fmtUsd(t.avgLossUsd)}; avg hold ${t.avgHoldingHours === null ? 'n/a' : `${t.avgHoldingHours.toFixed(1)}h`}`,
        t.largestWin ? `- largest win ${t.largestWin.symbol} ${fmtUsd(t.largestWin.pnlNet)}; largest loss ${t.largestLoss ? `${t.largestLoss.symbol} ${fmtUsd(t.largestLoss.pnlNet)}` : 'none'}` : '- no closed trades',
        ...t.bySide.map((s) => `- side ${s.side}: ${s.closed} closed, ${s.wins}W, net ${fmtUsd(s.pnlNetTotal)}`),
        ...t.byPlatform.map((s) => `- platform ${s.platform}: ${s.closed} closed, ${s.wins}W, net ${fmtUsd(s.pnlNetTotal)}`),
        '',
        `## Symbols (by |PnL|)`,
        ...(t.bySymbol.length ? t.bySymbol.map((s) => `- ${s.symbol} (${s.platform}): ${s.closed} closed, ${s.wins}W, net ${fmtUsd(s.pnlNetTotal)}`) : ['- none']),
        '',
        `## Equity`,
        ...(d.equity.length
            ? d.equity.map((e) =>
                  `- ${e.platform}: ${fmtUsd(e.firstEquity)} → ${fmtUsd(e.lastEquity)} (${e.changePct === null ? 'n/a' : `${e.changePct >= 0 ? '+' : ''}${e.changePct.toFixed(1)}%`}), range ${fmtUsd(e.minEquity)}–${fmtUsd(e.maxEquity)}, ${e.readings} readings`)
            : ['- no equity readings in window']),
        '',
        `## Post-mortems`,
        `- ${d.postmortems.total} enqueued, ${d.postmortems.unfinished} unfinished; verdicts: ${d.postmortems.byVerdict.map((v) => `${v.verdict} ${v.count}`).join(', ') || 'none'}`,
        `- process-fault share ${fmtPct(d.postmortems.processFaultShare)} (${d.postmortems.processFaults} faults vs ${d.postmortems.badLuck} bad_luck) — rising share = the single best "system is degrading" signal`,
        '',
        `## Lessons`,
        `- library: ${d.lessons.activeByScope.map((s) => `${s.scope} ${s.count} (avg conf ${s.avgConfidence.toFixed(2)})`).join(', ') || 'empty'}`,
        `- this window: ${d.lessons.addedInWindow.length} added, ${d.lessons.reinforcedInWindow} reinforced, ${d.lessons.retiredInWindow} retired`,
        ...d.lessons.addedInWindow.map((l) => `  - new [${l.scope}] (${l.confidence.toFixed(2)}) ${l.lesson}`),
        '',
        `## Ticks & gates`,
        `- ${d.ticks.total} ticks: ${d.ticks.aiCalls} AI calls, ${d.ticks.skips} skips, ${d.ticks.handlerErrors} handler errors, ${d.ticks.aiUnavailable} ai_unavailable`,
        `- top skip stages: ${d.ticks.topSkipStages.map((s) => `${s.stage} ${s.count}`).join(', ') || 'none'}`,
        `- decisions: ${d.decisions.map((a) => `${a.action} ${a.count}`).join(', ') || 'none'}`,
        '',
        `## Open now`,
        ...(d.openNow.length
            ? d.openNow.map((o) => `- ${o.symbol} (${o.platform}) ${o.status}, ${o.turns} AI turns`)
            : ['- flat everywhere']),
    ];
    return lines.join('\n');
}
