// Swing Postgres access layer (Neon). Durable source of truth for decisions,
// positions and account snapshots. Reuses the generic Neon client that the
// scalp subsystem already configures (same DB, same pool — no second pool).
// Reads project away bulky columns (e.g. prompt_json) to keep egress low; KV
// stays in front of these as a cache (step 3).
import { isScalpPgConfigured, scalpPrisma } from '../scalp/pg/client';
import { sql } from '../scalp/pg/sql';
import type { DecisionHistoryEntry } from '../history';
import type { PositionWindow } from '../analytics';

export function isSwingPgConfigured(): boolean {
    return isScalpPgConfigured();
}

function swingPg() {
    return scalpPrisma();
}

function normalizePlatform(value?: string | null): string {
    const raw = String(value || '').trim().toLowerCase();
    return raw || 'bitget';
}

function finitePos(value: unknown): number | null {
    const n = Number(value);
    return Number.isFinite(n) && n > 0 ? n : null;
}

function finite(value: unknown): number | null {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

// --------------------------------------------------------------------------
// Schema bootstrap (idempotent runtime DDL — this project manages schema via
// ensure* functions on the Neon client, not Prisma migrations). The Neon HTTP
// driver rejects multi-statement queries, so each statement is issued
// separately. Runs at most once per process.
// --------------------------------------------------------------------------
let swingSchemaReady: Promise<void> | null = null;

async function ensureSwingSchema(): Promise<void> {
    if (swingSchemaReady) return swingSchemaReady;
    swingSchemaReady = (async () => {
        const db = swingPg();
        await db.$executeRaw(sql`CREATE SCHEMA IF NOT EXISTS swing`);

        await db.$executeRaw(sql`
            CREATE OR REPLACE FUNCTION swing.set_updated_at() RETURNS trigger AS $$
            BEGIN
              NEW.updated_at = NOW();
              RETURN NEW;
            END;
            $$ LANGUAGE plpgsql`);

        // Provenance ranking: captured-at-open > derived(notional/margin) > broker.
        await db.$executeRaw(sql`
            CREATE OR REPLACE FUNCTION swing.lev_rank(src TEXT) RETURNS INT AS $$
              SELECT CASE lower(COALESCE(src, ''))
                WHEN 'captured' THEN 3
                WHEN 'derived'  THEN 2
                WHEN 'broker'   THEN 1
                ELSE 0
              END;
            $$ LANGUAGE sql IMMUTABLE`);

        // decisions: durable record per decision (replaces the 7-day-TTL KV blob
        // as source of truth). prompt_json isolated so reads can project it away.
        await db.$executeRaw(sql`
            CREATE TABLE IF NOT EXISTS swing.decisions (
              id                   BIGSERIAL PRIMARY KEY,
              decided_at_ms        BIGINT NOT NULL,
              decided_at           TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(decided_at_ms / 1000.0)) STORED,
              symbol               TEXT NOT NULL,
              platform             TEXT NOT NULL DEFAULT 'bitget',
              category             TEXT,
              instrument_id        TEXT,
              news_source          TEXT,
              time_frame           TEXT NOT NULL DEFAULT '',
              dry_run              BOOLEAN NOT NULL DEFAULT FALSE,
              action               TEXT,
              applied_leverage     NUMERIC,
              target_leverage      NUMERIC,
              prompt_json          JSONB,
              ai_decision_json     JSONB NOT NULL DEFAULT '{}'::jsonb,
              exec_result_json     JSONB NOT NULL DEFAULT '{}'::jsonb,
              snapshot_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
              bias_timeframes_json JSONB,
              created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              CONSTRAINT decisions_natural_key UNIQUE (decided_at_ms, platform, symbol)
            )`);
        await db.$executeRaw(sql`CREATE INDEX IF NOT EXISTS decisions_symbol_platform_time_idx ON swing.decisions (symbol, platform, decided_at DESC)`);
        await db.$executeRaw(sql`CREATE INDEX IF NOT EXISTS decisions_time_idx ON swing.decisions (decided_at DESC)`);
        await db.$executeRaw(sql`CREATE INDEX IF NOT EXISTS decisions_platform_time_idx ON swing.decisions (platform, decided_at DESC)`);
        await db.$executeRaw(sql`
            DO $$
            BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'decisions_set_updated_at') THEN
                CREATE TRIGGER decisions_set_updated_at BEFORE UPDATE ON swing.decisions
                  FOR EACH ROW EXECUTE FUNCTION swing.set_updated_at();
              END IF;
            END $$`);

        // positions: durable per-position record. entry_leverage captured at open
        // (ground truth — Bitget returns neither leverage nor margin on closed
        // positions for this account). leverage_source records provenance.
        await db.$executeRaw(sql`
            CREATE TABLE IF NOT EXISTS swing.positions (
              id                 BIGSERIAL PRIMARY KEY,
              platform           TEXT NOT NULL DEFAULT 'bitget',
              symbol             TEXT NOT NULL,
              position_key       TEXT NOT NULL,
              broker_position_id TEXT,
              side               TEXT,
              status             TEXT NOT NULL DEFAULT 'closed',
              entry_ts_ms        BIGINT,
              entry_ts           TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(entry_ts_ms / 1000.0)) STORED,
              exit_ts_ms         BIGINT,
              exit_ts            TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(exit_ts_ms / 1000.0)) STORED,
              entry_price        NUMERIC,
              exit_price         NUMERIC,
              notional           NUMERIC,
              entry_leverage     NUMERIC,
              entry_margin       NUMERIC,
              leverage_source    TEXT,
              pnl_net            NUMERIC,
              pnl_gross          NUMERIC,
              pnl_pct            NUMERIC,
              pnl_gross_pct      NUMERIC,
              decision_id        BIGINT REFERENCES swing.decisions(id) ON DELETE SET NULL,
              created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              CONSTRAINT positions_key UNIQUE (platform, position_key)
            )`);
        await db.$executeRaw(sql`CREATE INDEX IF NOT EXISTS positions_symbol_platform_time_idx ON swing.positions (symbol, platform, entry_ts DESC)`);
        await db.$executeRaw(sql`CREATE INDEX IF NOT EXISTS positions_status_idx ON swing.positions (status, platform)`);
        await db.$executeRaw(sql`CREATE INDEX IF NOT EXISTS positions_decision_idx ON swing.positions (decision_id) WHERE decision_id IS NOT NULL`);
        await db.$executeRaw(sql`
            DO $$
            BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'positions_set_updated_at') THEN
                CREATE TRIGGER positions_set_updated_at BEFORE UPDATE ON swing.positions
                  FOR EACH ROW EXECUTE FUNCTION swing.set_updated_at();
              END IF;
            END $$`);

        // account_snapshots: append-only history of the *current* leverage/margin
        // Bitget reports (previously lost/overwritten in KV).
        await db.$executeRaw(sql`
            CREATE TABLE IF NOT EXISTS swing.account_snapshots (
              id                 BIGSERIAL PRIMARY KEY,
              platform           TEXT NOT NULL DEFAULT 'bitget',
              symbol             TEXT,
              captured_at_ms     BIGINT NOT NULL,
              captured_at        TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(captured_at_ms / 1000.0)) STORED,
              leverage           NUMERIC,
              equity             NUMERIC,
              available          NUMERIC,
              margin_used        NUMERIC,
              open_position_json JSONB,
              created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )`);
        await db.$executeRaw(sql`CREATE INDEX IF NOT EXISTS account_snapshots_lookup_idx ON swing.account_snapshots (platform, symbol, captured_at DESC)`);
    })().catch((err) => {
        // allow a later retry rather than caching a failed bootstrap
        swingSchemaReady = null;
        throw err;
    });
    return swingSchemaReady;
}

// Exposed for healthchecks / scripts that want to provision the schema eagerly.
export { ensureSwingSchema };

// --------------------------------------------------------------------------
// Decisions
// --------------------------------------------------------------------------
export type SwingDecisionId = number;

export async function upsertSwingDecision(entry: DecisionHistoryEntry): Promise<SwingDecisionId | null> {
    if (!isSwingPgConfigured()) return null;
    const decidedAtMs = Number(entry.timestamp);
    if (!Number.isFinite(decidedAtMs) || decidedAtMs <= 0) return null;

    await ensureSwingSchema();
    const platform = normalizePlatform(entry.platform);
    const symbol = String(entry.symbol || '').toUpperCase();
    const exec = (entry.execResult || {}) as Record<string, any>;
    const appliedLeverage = finitePos(exec.leverage);
    const targetLeverage = finitePos(exec.targetLeverage);
    const action = (entry.aiDecision as any)?.action ?? null;

    const db = swingPg();
    const rows = await db.$queryRaw<Array<{ id: number | string }>>(sql`
        INSERT INTO swing.decisions (
            decided_at_ms, symbol, platform, category, instrument_id, news_source,
            time_frame, dry_run, action, applied_leverage, target_leverage,
            prompt_json, ai_decision_json, exec_result_json, snapshot_json, bias_timeframes_json
        ) VALUES (
            ${decidedAtMs}, ${symbol}, ${platform}, ${entry.category ?? null}, ${entry.instrumentId ?? null},
            ${entry.newsSource ?? null}, ${entry.timeFrame ?? ''}, ${Boolean(entry.dryRun)}, ${action},
            ${appliedLeverage}, ${targetLeverage},
            ${entry.prompt ? JSON.stringify(entry.prompt) : null}::jsonb,
            ${JSON.stringify(entry.aiDecision ?? {})}::jsonb,
            ${JSON.stringify(entry.execResult ?? {})}::jsonb,
            ${JSON.stringify(entry.snapshot ?? {})}::jsonb,
            ${entry.biasTimeframes ? JSON.stringify(entry.biasTimeframes) : null}::jsonb
        )
        ON CONFLICT (decided_at_ms, platform, symbol) DO UPDATE SET
            category = EXCLUDED.category,
            instrument_id = EXCLUDED.instrument_id,
            news_source = EXCLUDED.news_source,
            time_frame = EXCLUDED.time_frame,
            dry_run = EXCLUDED.dry_run,
            action = EXCLUDED.action,
            applied_leverage = EXCLUDED.applied_leverage,
            target_leverage = EXCLUDED.target_leverage,
            prompt_json = EXCLUDED.prompt_json,
            ai_decision_json = EXCLUDED.ai_decision_json,
            exec_result_json = EXCLUDED.exec_result_json,
            snapshot_json = EXCLUDED.snapshot_json,
            bias_timeframes_json = EXCLUDED.bias_timeframes_json
        RETURNING id;
    `);
    const id = rows?.[0]?.id;
    return id == null ? null : Number(id);
}

export type SwingDecisionSummary = {
    id: number;
    decidedAtMs: number;
    symbol: string;
    platform: string;
    action: string | null;
    appliedLeverage: number | null;
    targetLeverage: number | null;
    dryRun: boolean;
};

// Lightweight projected read — never selects prompt/snapshot/decision JSON,
// so list views stay cheap on egress.
export async function loadRecentSwingDecisions(
    opts: { symbol?: string; platform?: string; limit?: number } = {},
): Promise<SwingDecisionSummary[]> {
    if (!isSwingPgConfigured()) return [];
    await ensureSwingSchema();
    const limit = Math.max(1, Math.min(500, opts.limit ?? 50));
    const symbol = opts.symbol ? opts.symbol.toUpperCase() : null;
    const platform = opts.platform ? normalizePlatform(opts.platform) : null;
    const db = swingPg();
    const rows = await db.$queryRaw<Array<any>>(sql`
        SELECT id, decided_at_ms, symbol, platform, action, applied_leverage, target_leverage, dry_run
        FROM swing.decisions
        WHERE (${symbol}::text IS NULL OR symbol = ${symbol})
          AND (${platform}::text IS NULL OR platform = ${platform})
        ORDER BY decided_at_ms DESC
        LIMIT ${limit};
    `);
    return (rows || []).map((r) => ({
        id: Number(r.id),
        decidedAtMs: Number(r.decided_at_ms),
        symbol: String(r.symbol),
        platform: String(r.platform),
        action: r.action ?? null,
        appliedLeverage: finite(r.applied_leverage),
        targetLeverage: finite(r.target_leverage),
        dryRun: Boolean(r.dry_run),
    }));
}

// --------------------------------------------------------------------------
// Positions
// --------------------------------------------------------------------------
export type SwingPositionInput = PositionWindow & {
    status?: 'open' | 'closed';
    entryLeverage?: number | null;
    entryMargin?: number | null;
    leverageSource?: 'captured' | 'derived' | 'broker' | null;
    decisionId?: number | null;
};

export async function upsertSwingPosition(
    platform: string,
    p: SwingPositionInput,
): Promise<void> {
    if (!isSwingPgConfigured()) return;
    await ensureSwingSchema();
    const plat = normalizePlatform(platform);
    const symbol = String(p.symbol || '').toUpperCase();
    const positionKey = String(p.id || `${symbol}-${p.entryTimestamp ?? 'nots'}`);
    const status = p.status ?? (p.exitTimestamp ? 'closed' : 'open');
    const entryMs = finite(p.entryTimestamp);
    // When the caller doesn't supply a decision_id, link to the most recent
    // decision for this symbol/platform at or just before entry (the one that
    // opened the position). 6h lookback keeps it to the same cron neighbourhood.
    const linkLowerMs = entryMs == null ? null : entryMs - 6 * 60 * 60 * 1000;
    const db = swingPg();
    await db.$executeRaw(sql`
        INSERT INTO swing.positions AS p (
            platform, symbol, position_key, broker_position_id, side, status,
            entry_ts_ms, exit_ts_ms, entry_price, exit_price, notional,
            entry_leverage, entry_margin, leverage_source,
            pnl_net, pnl_gross, pnl_pct, pnl_gross_pct, decision_id
        ) VALUES (
            ${plat}, ${symbol}, ${positionKey}, ${p.id ?? null}, ${p.side ?? null}, ${status},
            ${finite(p.entryTimestamp)}, ${finite(p.exitTimestamp)}, ${finite(p.entryPrice)},
            ${finite(p.exitPrice)}, ${finite(p.notional)},
            ${finitePos(p.entryLeverage ?? p.leverage)}, ${finitePos(p.entryMargin)},
            ${p.leverageSource ?? null},
            ${finite(p.pnlNet)}, ${finite(p.pnlGross)}, ${finite(p.pnlPct)}, ${finite(p.pnlGrossPct)},
            COALESCE(
                ${p.decisionId ?? null}::bigint,
                (SELECT d.id FROM swing.decisions d
                   WHERE d.platform = ${plat} AND d.symbol = ${symbol}
                     AND d.decided_at_ms <= ${entryMs}
                     AND d.decided_at_ms >= ${linkLowerMs}
                   ORDER BY d.decided_at_ms DESC LIMIT 1)
            )
        )
        ON CONFLICT (platform, position_key) DO UPDATE SET
            symbol = EXCLUDED.symbol,
            broker_position_id = COALESCE(EXCLUDED.broker_position_id, p.broker_position_id),
            side = COALESCE(EXCLUDED.side, p.side),
            status = EXCLUDED.status,
            entry_ts_ms = COALESCE(EXCLUDED.entry_ts_ms, p.entry_ts_ms),
            exit_ts_ms = COALESCE(EXCLUDED.exit_ts_ms, p.exit_ts_ms),
            entry_price = COALESCE(EXCLUDED.entry_price, p.entry_price),
            exit_price = COALESCE(EXCLUDED.exit_price, p.exit_price),
            notional = COALESCE(EXCLUDED.notional, p.notional),
            -- leverage fields are gated by provenance rank: a weaker source
            -- (broker < derived < captured) never overwrites a stronger one.
            entry_leverage = CASE
                WHEN p.entry_leverage IS NULL THEN EXCLUDED.entry_leverage
                WHEN EXCLUDED.entry_leverage IS NULL THEN p.entry_leverage
                WHEN swing.lev_rank(EXCLUDED.leverage_source) >= swing.lev_rank(p.leverage_source)
                    THEN EXCLUDED.entry_leverage
                ELSE p.entry_leverage
            END,
            entry_margin = CASE
                WHEN swing.lev_rank(EXCLUDED.leverage_source) >= swing.lev_rank(p.leverage_source)
                    THEN COALESCE(EXCLUDED.entry_margin, p.entry_margin)
                ELSE COALESCE(p.entry_margin, EXCLUDED.entry_margin)
            END,
            leverage_source = CASE
                WHEN p.entry_leverage IS NULL THEN EXCLUDED.leverage_source
                WHEN EXCLUDED.entry_leverage IS NULL THEN p.leverage_source
                WHEN swing.lev_rank(EXCLUDED.leverage_source) >= swing.lev_rank(p.leverage_source)
                    THEN EXCLUDED.leverage_source
                ELSE p.leverage_source
            END,
            pnl_net = COALESCE(EXCLUDED.pnl_net, p.pnl_net),
            pnl_gross = COALESCE(EXCLUDED.pnl_gross, p.pnl_gross),
            pnl_pct = COALESCE(EXCLUDED.pnl_pct, p.pnl_pct),
            pnl_gross_pct = COALESCE(EXCLUDED.pnl_gross_pct, p.pnl_gross_pct),
            decision_id = COALESCE(EXCLUDED.decision_id, p.decision_id);
    `);
}

export async function loadClosedSwingPositions(opts: {
    platform?: string | null;
    symbol?: string | null;
    fromMs: number;
    toMs?: number;
    limit?: number;
}): Promise<PositionWindow[]> {
    if (!isSwingPgConfigured()) return [];
    await ensureSwingSchema();
    const platform = opts.platform ? normalizePlatform(opts.platform) : null;
    const symbol = opts.symbol ? String(opts.symbol).toUpperCase() : null;
    const fromMs = finite(opts.fromMs) ?? 0;
    const toMs = finite(opts.toMs) ?? Date.now();
    const limit = Math.max(1, Math.min(5000, Math.floor(Number(opts.limit) || 1000)));
    const db = swingPg();
    const rows = await db.$queryRaw<Array<any>>(sql`
        SELECT
            position_key, symbol, side, entry_ts_ms, exit_ts_ms, entry_price, exit_price,
            pnl_net, pnl_gross, pnl_pct, pnl_gross_pct, notional, entry_leverage
        FROM swing.positions
        WHERE status = 'closed'
          AND (${platform}::text IS NULL OR platform = ${platform})
          AND (${symbol}::text IS NULL OR symbol = ${symbol})
          AND (
            (exit_ts_ms IS NOT NULL AND exit_ts_ms >= ${fromMs} AND exit_ts_ms <= ${toMs})
            OR (exit_ts_ms IS NULL AND entry_ts_ms IS NOT NULL AND entry_ts_ms >= ${fromMs} AND entry_ts_ms <= ${toMs})
          )
        ORDER BY COALESCE(exit_ts_ms, entry_ts_ms, 0) ASC
        LIMIT ${limit};
    `);

    return (rows || []).map((row) => ({
        id: String(row.position_key),
        symbol: String(row.symbol || symbol || ''),
        side: row.side === 'long' || row.side === 'short' ? row.side : null,
        entryTimestamp: finite(row.entry_ts_ms),
        exitTimestamp: finite(row.exit_ts_ms),
        entryPrice: finite(row.entry_price),
        exitPrice: finite(row.exit_price),
        pnlNet: finite(row.pnl_net),
        pnlGross: finite(row.pnl_gross),
        pnlPct: finite(row.pnl_pct),
        pnlGrossPct: finite(row.pnl_gross_pct),
        notional: finite(row.notional),
        leverage: finite(row.entry_leverage),
    }));
}

// --------------------------------------------------------------------------
// Account snapshots (append-only)
// --------------------------------------------------------------------------
export type SwingAccountSnapshotInput = {
    platform: string;
    symbol?: string | null;
    capturedAtMs: number;
    leverage?: number | null;
    equity?: number | null;
    available?: number | null;
    marginUsed?: number | null;
    openPosition?: unknown;
};

export async function insertSwingAccountSnapshot(snap: SwingAccountSnapshotInput): Promise<void> {
    if (!isSwingPgConfigured()) return;
    const capturedAtMs = Number(snap.capturedAtMs);
    if (!Number.isFinite(capturedAtMs) || capturedAtMs <= 0) return;
    await ensureSwingSchema();
    const db = swingPg();
    await db.$executeRaw(sql`
        INSERT INTO swing.account_snapshots (
            platform, symbol, captured_at_ms, leverage, equity, available, margin_used, open_position_json
        ) VALUES (
            ${normalizePlatform(snap.platform)}, ${snap.symbol ? String(snap.symbol).toUpperCase() : null},
            ${capturedAtMs}, ${finitePos(snap.leverage)}, ${finite(snap.equity)}, ${finite(snap.available)},
            ${finite(snap.marginUsed)},
            ${snap.openPosition == null ? null : JSON.stringify(snap.openPosition)}::jsonb
        );
    `);
}
