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
    // Number(null) === 0, so a bare Number() coercion fabricates zeros out of
    // explicit nulls (e.g. Capital transaction imports pass entryTimestamp: null,
    // which then persisted as entry_ts_ms = 0 and — being non-null — clobbered
    // enriched values through the upsert's COALESCE on every re-sync).
    if (value == null) return null;
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

        // ai_threads: one active AI conversation per (platform, symbol). A thread
        // starts when an entry order is placed (market fill or resting pullback
        // limit), survives the limit fill into position management AND
        // unfilled-limit re-evaluations (sweep + re-issue continues the same
        // conversation), and ends when the entry is dropped without a re-issue
        // or the position closes. Where the conversation LIVES depends on the
        // provider: OpenAI Responses API is stateful server-side (last_response_id
        // is the chain head), Claude Messages API is stateless (transcript holds
        // the full message history we resend each tick; last_response_id then
        // stores the msg_... id for dashboard linkage only).
        await db.$executeRaw(sql`
            CREATE TABLE IF NOT EXISTS swing.ai_threads (
              platform         TEXT NOT NULL,
              symbol           TEXT NOT NULL,
              status           TEXT NOT NULL,
              last_response_id TEXT NOT NULL,
              provider         TEXT NOT NULL DEFAULT 'openai',
              transcript       JSONB,
              turns            INT NOT NULL DEFAULT 1,
              created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              CONSTRAINT ai_threads_status_check CHECK (status IN ('pending_entry', 'in_position')),
              CONSTRAINT ai_threads_key PRIMARY KEY (platform, symbol)
            )`);
        // Existing deployments predate the provider/transcript columns.
        await db.$executeRaw(sql`ALTER TABLE swing.ai_threads ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'openai'`);
        await db.$executeRaw(sql`ALTER TABLE swing.ai_threads ADD COLUMN IF NOT EXISTS transcript JSONB`);

        // ai_cooldowns: AI-requested quiet periods on FLAT symbols ("nothing to
        // do here, don't re-evaluate for N minutes — unless price crosses a wake
        // band"). One row per (platform, symbol); consulted by the pre-AI
        // cooldown gate on fresh flat scans only (in-position ticks and
        // pending-entry re-evaluations are never suppressed) and deleted when it
        // expires, a wake band triggers, or the next AI call goes through.
        await db.$executeRaw(sql`
            CREATE TABLE IF NOT EXISTS swing.ai_cooldowns (
              platform    TEXT NOT NULL,
              symbol      TEXT NOT NULL,
              until_ms    BIGINT NOT NULL,
              wake_above  NUMERIC,
              wake_below  NUMERIC,
              set_at_ms   BIGINT NOT NULL,
              created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              CONSTRAINT ai_cooldowns_key PRIMARY KEY (platform, symbol)
            )`);

        // tick_log: one row per analyze-tick OUTCOME — gate skips (with the
        // stage/reason and gate measurements) and AI calls alike. Quarter-tick
        // and cooldown skips never reach swing.decisions and the KV scan-tick
        // ring buffer only holds ~2 days, so this is the only durable record
        // from which a post-loss post-mortem can reconstruct the full tick
        // series around a trade. No prompts here — rows stay small.
        await db.$executeRaw(sql`
            CREATE TABLE IF NOT EXISTS swing.tick_log (
              id           BIGSERIAL PRIMARY KEY,
              ts_ms        BIGINT NOT NULL,
              ts           TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(ts_ms / 1000.0)) STORED,
              symbol       TEXT NOT NULL,
              platform     TEXT NOT NULL DEFAULT 'bitget',
              kind         TEXT NOT NULL,
              stage        TEXT NOT NULL,
              reason       TEXT,
              cadence      TEXT,
              dry_run      BOOLEAN NOT NULL DEFAULT FALSE,
              gates_json   JSONB,
              metrics_json JSONB,
              created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              CONSTRAINT tick_log_kind_check CHECK (kind IN ('skip', 'ai_call'))
            )`);
        await db.$executeRaw(sql`CREATE INDEX IF NOT EXISTS tick_log_symbol_platform_time_idx ON swing.tick_log (symbol, platform, ts_ms DESC)`);

        // postmortems: one AI-generated forensic report per closed position
        // (loss-triggered by default). The UNIQUE (platform, position_key) is
        // the enqueue idempotency lock — closes get re-synced/re-reconciled
        // many times, but only the first insert wins and fires the worker.
        // verdict + lesson are real columns (cheap to query/feed forward);
        // the full report and the per-tick dossier live in JSONB.
        await db.$executeRaw(sql`
            CREATE TABLE IF NOT EXISTS swing.postmortems (
              id             BIGSERIAL PRIMARY KEY,
              platform       TEXT NOT NULL DEFAULT 'bitget',
              symbol         TEXT NOT NULL,
              position_key   TEXT NOT NULL,
              status         TEXT NOT NULL DEFAULT 'queued',
              trigger_source TEXT NOT NULL DEFAULT 'close',
              side           TEXT,
              entry_ts_ms    BIGINT,
              exit_ts_ms     BIGINT,
              entry_price    NUMERIC,
              exit_price     NUMERIC,
              pnl_pct        NUMERIC,
              pnl_net        NUMERIC,
              verdict        TEXT,
              lesson         TEXT,
              report_json    JSONB,
              dossier_json   JSONB,
              model          TEXT,
              usage_json     JSONB,
              error          TEXT,
              attempts       INT NOT NULL DEFAULT 0,
              created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              CONSTRAINT postmortems_status_check CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
              CONSTRAINT postmortems_position UNIQUE (platform, position_key)
            )`);
        await db.$executeRaw(sql`CREATE INDEX IF NOT EXISTS postmortems_symbol_time_idx ON swing.postmortems (symbol, platform, created_at DESC)`);
        await db.$executeRaw(sql`CREATE INDEX IF NOT EXISTS postmortems_status_idx ON swing.postmortems (status)`);
        await db.$executeRaw(sql`
            DO $$
            BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'postmortems_set_updated_at') THEN
                CREATE TRIGGER postmortems_set_updated_at BEFORE UPDATE ON swing.postmortems
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
// Tick log (per-tick outcomes: gate skips + AI calls)
// --------------------------------------------------------------------------
export type SwingTickLogEntry = {
    tsMs: number;
    symbol: string;
    platform: string;
    kind: 'skip' | 'ai_call';
    stage: string;
    reason?: string | null;
    cadence?: 'hourly' | 'quarter' | 'manual' | null;
    dryRun?: boolean;
    gates?: Record<string, unknown> | null;
    metrics?: Record<string, unknown> | null;
};

// Best-effort: never throws into the trading path — a lost row only costs
// post-mortem completeness, never the tick.
export async function insertSwingTickLog(entry: SwingTickLogEntry): Promise<void> {
    if (!isSwingPgConfigured()) return;
    try {
        await ensureSwingSchema();
        const db = swingPg();
        await db.$executeRaw(sql`
            INSERT INTO swing.tick_log (ts_ms, symbol, platform, kind, stage, reason, cadence, dry_run, gates_json, metrics_json)
            VALUES (
                ${Math.floor(entry.tsMs)}, ${String(entry.symbol || '').toUpperCase()}, ${normalizePlatform(entry.platform)},
                ${entry.kind}, ${entry.stage}, ${entry.reason ?? null}, ${entry.cadence ?? null}, ${Boolean(entry.dryRun)},
                ${entry.gates ? JSON.stringify(entry.gates) : null}::jsonb,
                ${entry.metrics ? JSON.stringify(entry.metrics) : null}::jsonb
            )`);
        // Opportunistic retention sweep (~1 in 500 inserts): rows are tiny but
        // the table is append-only — 90 days comfortably covers any post-mortem
        // lookback while keeping it bounded.
        if (Math.random() < 0.002) {
            await db.$executeRaw(sql`DELETE FROM swing.tick_log WHERE ts_ms < ${Date.now() - 90 * 24 * 60 * 60 * 1000}`);
        }
    } catch (err) {
        console.warn(`tick_log write failed for ${entry.symbol}:`, err);
    }
}

export type SwingTickLogRow = {
    id: number;
    tsMs: number;
    symbol: string;
    platform: string;
    kind: 'skip' | 'ai_call';
    stage: string;
    reason: string | null;
    cadence: string | null;
    dryRun: boolean;
    gates: Record<string, unknown> | null;
    metrics: Record<string, unknown> | null;
};

// Chronological (oldest first) — the shape a post-mortem dossier walks.
export async function loadSwingTickLog(opts: {
    symbol: string;
    platform?: string | null;
    fromMs: number;
    toMs?: number | null;
    limit?: number;
}): Promise<SwingTickLogRow[]> {
    if (!isSwingPgConfigured()) return [];
    await ensureSwingSchema();
    const limit = Math.max(1, Math.min(5000, opts.limit ?? 2000));
    const toMs = opts.toMs ?? Date.now();
    const db = swingPg();
    const rows = await db.$queryRaw<Array<any>>(sql`
        SELECT id, ts_ms, symbol, platform, kind, stage, reason, cadence, dry_run, gates_json, metrics_json
        FROM swing.tick_log
        WHERE symbol = ${String(opts.symbol || '').toUpperCase()}
          AND (${opts.platform ? normalizePlatform(opts.platform) : null}::text IS NULL OR platform = ${
              opts.platform ? normalizePlatform(opts.platform) : null
          })
          AND ts_ms >= ${Math.floor(opts.fromMs)}
          AND ts_ms <= ${Math.floor(toMs)}
        ORDER BY ts_ms ASC
        LIMIT ${limit};
    `);
    return (rows || []).map((r) => ({
        id: Number(r.id),
        tsMs: Number(r.ts_ms),
        symbol: String(r.symbol),
        platform: String(r.platform),
        kind: r.kind === 'ai_call' ? 'ai_call' : 'skip',
        stage: String(r.stage),
        reason: r.reason ?? null,
        cadence: r.cadence ?? null,
        dryRun: Boolean(r.dry_run),
        gates: r.gates_json ?? null,
        metrics: r.metrics_json ?? null,
    }));
}

// --------------------------------------------------------------------------
// AI threads (Responses API conversation chains)
// --------------------------------------------------------------------------
export type SwingAiThreadStatus = 'pending_entry' | 'in_position';

export type SwingAiThread = {
    status: SwingAiThreadStatus;
    lastResponseId: string;
    turns: number;
    // 'openai' (conversation lives server-side, lastResponseId is the chain
    // head) or 'claude' (conversation is the transcript below). Rows written
    // before the provider column default to 'openai'.
    provider: string;
    // Claude only: full message history ({role, content}[]) resent each tick.
    transcript: unknown[] | null;
};

export async function getSwingAiThread(platform: string, symbol: string): Promise<SwingAiThread | null> {
    if (!isSwingPgConfigured()) return null;
    await ensureSwingSchema();
    const db = swingPg();
    const rows = await db.$queryRaw<
        Array<{ status: string; last_response_id: string; turns: number; provider: string | null; transcript: unknown }>
    >(sql`
        SELECT status, last_response_id, turns, provider, transcript
        FROM swing.ai_threads
        WHERE platform = ${normalizePlatform(platform)} AND symbol = ${String(symbol || '').toUpperCase()}
    `);
    const row = rows?.[0];
    if (!row || !row.last_response_id) return null;
    const status = row.status === 'pending_entry' ? 'pending_entry' : 'in_position';
    return {
        status,
        lastResponseId: row.last_response_id,
        turns: Number(row.turns) || 1,
        provider: row.provider === 'claude' ? 'claude' : 'openai',
        transcript: Array.isArray(row.transcript) ? row.transcript : null,
    };
}

// Insert or advance the conversation. A fresh entry decision REPLACES any prior
// thread outright (new conversation); an in-position tick advances turns.
// OpenAI advances the chain head only; Claude also replaces the stored
// transcript with the caller-provided continuation (already truncated).
export async function upsertSwingAiThread(params: {
    platform: string;
    symbol: string;
    status: SwingAiThreadStatus;
    lastResponseId: string;
    provider?: string;
    transcript?: unknown[] | null;
}): Promise<void> {
    if (!isSwingPgConfigured()) return;
    if (!params.lastResponseId) return;
    await ensureSwingSchema();
    const db = swingPg();
    const provider = params.provider === 'claude' ? 'claude' : 'openai';
    const transcriptJson = Array.isArray(params.transcript) && params.transcript.length ? JSON.stringify(params.transcript) : null;
    await db.$executeRaw(sql`
        INSERT INTO swing.ai_threads (platform, symbol, status, last_response_id, provider, transcript)
        VALUES (${normalizePlatform(params.platform)}, ${String(params.symbol || '').toUpperCase()}, ${params.status}, ${params.lastResponseId}, ${provider}, ${transcriptJson}::jsonb)
        ON CONFLICT (platform, symbol) DO UPDATE SET
            status = EXCLUDED.status,
            last_response_id = EXCLUDED.last_response_id,
            provider = EXCLUDED.provider,
            transcript = EXCLUDED.transcript,
            turns = swing.ai_threads.turns + 1,
            updated_at = NOW()
    `);
}

// All symbols whose conversation is parked on a resting pullback limit — one
// query for the dashboard summary's pending-entry flags (pill ordering).
export async function listSwingPendingEntryThreads(): Promise<Array<{ platform: string; symbol: string }>> {
    if (!isSwingPgConfigured()) return [];
    await ensureSwingSchema();
    const db = swingPg();
    const rows = await db.$queryRaw<Array<{ platform: string; symbol: string }>>(sql`
        SELECT platform, symbol
        FROM swing.ai_threads
        WHERE status = 'pending_entry'
    `);
    return rows.map((row) => ({ platform: row.platform, symbol: row.symbol }));
}

// Resting pullback limit filled → the same conversation now manages the position.
export async function markSwingAiThreadInPosition(platform: string, symbol: string): Promise<void> {
    if (!isSwingPgConfigured()) return;
    await ensureSwingSchema();
    const db = swingPg();
    await db.$executeRaw(sql`
        UPDATE swing.ai_threads
        SET status = 'in_position', updated_at = NOW()
        WHERE platform = ${normalizePlatform(platform)} AND symbol = ${String(symbol || '').toUpperCase()}
    `);
}

// --------------------------------------------------------------------------
// AI cooldowns (flat-symbol quiet periods, optionally price-banded)
// --------------------------------------------------------------------------
export type SwingAiCooldown = {
    untilMs: number;
    wakeAbove: number | null;
    wakeBelow: number | null;
    setAtMs: number;
};

export async function getSwingAiCooldown(platform: string, symbol: string): Promise<SwingAiCooldown | null> {
    if (!isSwingPgConfigured()) return null;
    await ensureSwingSchema();
    const db = swingPg();
    const rows = await db.$queryRaw<
        Array<{ until_ms: unknown; wake_above: unknown; wake_below: unknown; set_at_ms: unknown }>
    >(sql`
        SELECT until_ms, wake_above, wake_below, set_at_ms
        FROM swing.ai_cooldowns
        WHERE platform = ${normalizePlatform(platform)} AND symbol = ${String(symbol || '').toUpperCase()}
    `);
    const row = rows?.[0];
    if (!row) return null;
    const untilMs = Number(row.until_ms);
    if (!Number.isFinite(untilMs) || untilMs <= 0) return null;
    return {
        untilMs,
        wakeAbove: finitePos(row.wake_above),
        wakeBelow: finitePos(row.wake_below),
        setAtMs: Number(row.set_at_ms) || 0,
    };
}

export async function upsertSwingAiCooldown(params: {
    platform: string;
    symbol: string;
    untilMs: number;
    wakeAbove?: number | null;
    wakeBelow?: number | null;
}): Promise<void> {
    if (!isSwingPgConfigured()) return;
    if (!Number.isFinite(params.untilMs) || params.untilMs <= Date.now()) return;
    await ensureSwingSchema();
    const db = swingPg();
    await db.$executeRaw(sql`
        INSERT INTO swing.ai_cooldowns (platform, symbol, until_ms, wake_above, wake_below, set_at_ms)
        VALUES (
            ${normalizePlatform(params.platform)},
            ${String(params.symbol || '').toUpperCase()},
            ${Math.floor(params.untilMs)},
            ${finitePos(params.wakeAbove)},
            ${finitePos(params.wakeBelow)},
            ${Date.now()}
        )
        ON CONFLICT (platform, symbol) DO UPDATE SET
            until_ms = EXCLUDED.until_ms,
            wake_above = EXCLUDED.wake_above,
            wake_below = EXCLUDED.wake_below,
            set_at_ms = EXCLUDED.set_at_ms,
            updated_at = NOW()
    `);
}

export async function clearSwingAiCooldown(platform: string, symbol: string): Promise<void> {
    if (!isSwingPgConfigured()) return;
    await ensureSwingSchema();
    const db = swingPg();
    await db.$executeRaw(sql`
        DELETE FROM swing.ai_cooldowns
        WHERE platform = ${normalizePlatform(platform)} AND symbol = ${String(symbol || '').toUpperCase()}
    `);
}

// Limit expired unfilled, or position closed → conversation over.
export async function endSwingAiThread(platform: string, symbol: string): Promise<void> {
    if (!isSwingPgConfigured()) return;
    await ensureSwingSchema();
    const db = swingPg();
    await db.$executeRaw(sql`
        DELETE FROM swing.ai_threads
        WHERE platform = ${normalizePlatform(platform)} AND symbol = ${String(symbol || '').toUpperCase()}
    `);
}

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

// --------------------------------------------------------------------------
// Post-mortems (per-closed-position forensic reports)
// --------------------------------------------------------------------------
export type SwingPostmortemStatus = 'queued' | 'running' | 'succeeded' | 'failed';
export type SwingPostmortemTrigger = 'close' | 'manual' | 'backfill';

export type SwingPostmortemEnqueueInput = {
    platform: string;
    symbol: string;
    positionKey: string;
    trigger: SwingPostmortemTrigger;
    side?: string | null;
    entryTsMs?: number | null;
    exitTsMs?: number | null;
    entryPrice?: number | null;
    exitPrice?: number | null;
    pnlPct?: number | null;
    pnlNet?: number | null;
};

export type SwingPostmortemRow = {
    id: number;
    platform: string;
    symbol: string;
    positionKey: string;
    status: SwingPostmortemStatus;
    trigger: string;
    side: string | null;
    entryTsMs: number | null;
    exitTsMs: number | null;
    entryPrice: number | null;
    exitPrice: number | null;
    pnlPct: number | null;
    pnlNet: number | null;
    verdict: string | null;
    lesson: string | null;
    report: Record<string, any> | null;
    dossier: Record<string, any> | null;
    model: string | null;
    usage: Record<string, any> | null;
    error: string | null;
    attempts: number;
    createdAtMs: number;
    updatedAtMs: number;
};

function mapPostmortemRow(r: any): SwingPostmortemRow {
    return {
        id: Number(r.id),
        platform: String(r.platform),
        symbol: String(r.symbol),
        positionKey: String(r.position_key),
        status: String(r.status) as SwingPostmortemStatus,
        trigger: String(r.trigger_source),
        side: r.side ?? null,
        entryTsMs: finite(r.entry_ts_ms),
        exitTsMs: finite(r.exit_ts_ms),
        entryPrice: finite(r.entry_price),
        exitPrice: finite(r.exit_price),
        pnlPct: finite(r.pnl_pct),
        pnlNet: finite(r.pnl_net),
        verdict: r.verdict ?? null,
        lesson: r.lesson ?? null,
        report: r.report_json ?? null,
        dossier: r.dossier_json ?? null,
        model: r.model ?? null,
        usage: r.usage_json ?? null,
        error: r.error ?? null,
        attempts: Number(r.attempts) || 0,
        createdAtMs: r.created_at ? new Date(r.created_at).getTime() : 0,
        updatedAtMs: r.updated_at ? new Date(r.updated_at).getTime() : 0,
    };
}

// Idempotent enqueue: the UNIQUE (platform, position_key) makes the first
// insert win — re-syncs of the same close return null (already enqueued or
// already analyzed) so the caller knows not to fire another worker.
export async function enqueueSwingPostmortem(input: SwingPostmortemEnqueueInput): Promise<number | null> {
    if (!isSwingPgConfigured()) return null;
    await ensureSwingSchema();
    const db = swingPg();
    const rows = await db.$queryRaw<Array<{ id: number | string }>>(sql`
        INSERT INTO swing.postmortems (
            platform, symbol, position_key, trigger_source, side,
            entry_ts_ms, exit_ts_ms, entry_price, exit_price, pnl_pct, pnl_net
        ) VALUES (
            ${normalizePlatform(input.platform)}, ${String(input.symbol || '').toUpperCase()}, ${input.positionKey},
            ${input.trigger}, ${input.side ?? null},
            ${finite(input.entryTsMs)}, ${finite(input.exitTsMs)}, ${finite(input.entryPrice)},
            ${finite(input.exitPrice)}, ${finite(input.pnlPct)}, ${finite(input.pnlNet)}
        )
        ON CONFLICT (platform, position_key) DO NOTHING
        RETURNING id;
    `);
    const id = rows?.[0]?.id;
    return id == null ? null : Number(id);
}

// Claim by id for a worker run. Without force only queued/failed rows (or a
// stale 'running' — a crashed worker's leftover, >15 min old) are claimable;
// force re-claims anything (manual regenerate).
export async function claimSwingPostmortemById(
    id: number,
    opts: { force?: boolean } = {},
): Promise<SwingPostmortemRow | null> {
    if (!isSwingPgConfigured()) return null;
    await ensureSwingSchema();
    const db = swingPg();
    const staleBefore = new Date(Date.now() - 15 * 60 * 1000);
    const rows = await db.$queryRaw<Array<any>>(sql`
        UPDATE swing.postmortems
        SET status = 'running', attempts = attempts + 1, error = NULL
        WHERE id = ${Math.floor(id)}
          AND (
            ${Boolean(opts.force)} = TRUE
            OR status IN ('queued', 'failed')
            OR (status = 'running' AND updated_at < ${staleBefore})
          )
        RETURNING *;
    `);
    return rows?.length ? mapPostmortemRow(rows[0]) : null;
}

// Claim the oldest queued rows (drain mode). Sequential worker — small limit.
export async function claimQueuedSwingPostmortems(limit: number): Promise<SwingPostmortemRow[]> {
    if (!isSwingPgConfigured()) return [];
    await ensureSwingSchema();
    const capped = Math.max(1, Math.min(10, Math.floor(limit)));
    const db = swingPg();
    const rows = await db.$queryRaw<Array<any>>(sql`
        UPDATE swing.postmortems
        SET status = 'running', attempts = attempts + 1, error = NULL
        WHERE id IN (
            SELECT id FROM swing.postmortems
            WHERE status = 'queued'
            ORDER BY created_at ASC
            LIMIT ${capped}
        )
        RETURNING *;
    `);
    return (rows || []).map(mapPostmortemRow);
}

export async function completeSwingPostmortem(
    id: number,
    result: {
        verdict: string | null;
        lesson: string | null;
        report: Record<string, any>;
        dossier: Record<string, any>;
        model: string | null;
        usage: Record<string, any> | null;
    },
): Promise<void> {
    if (!isSwingPgConfigured()) return;
    await ensureSwingSchema();
    const db = swingPg();
    await db.$executeRaw(sql`
        UPDATE swing.postmortems
        SET status = 'succeeded',
            verdict = ${result.verdict},
            lesson = ${result.lesson},
            report_json = ${JSON.stringify(result.report)}::jsonb,
            dossier_json = ${JSON.stringify(result.dossier)}::jsonb,
            model = ${result.model},
            usage_json = ${result.usage ? JSON.stringify(result.usage) : null}::jsonb,
            error = NULL
        WHERE id = ${Math.floor(id)};
    `);
}

export async function failSwingPostmortem(id: number, error: string): Promise<void> {
    if (!isSwingPgConfigured()) return;
    await ensureSwingSchema();
    const db = swingPg();
    await db.$executeRaw(sql`
        UPDATE swing.postmortems
        SET status = 'failed', error = ${String(error).slice(0, 2000)}
        WHERE id = ${Math.floor(id)};
    `);
}

// List projection: report/dossier JSON stay out (egress); lesson/verdict ride
// along — they're the compact face of a post-mortem.
export type SwingPostmortemSummary = Omit<SwingPostmortemRow, 'report' | 'dossier'>;

export async function loadSwingPostmortems(
    opts: { symbol?: string | null; platform?: string | null; status?: SwingPostmortemStatus | null; limit?: number } = {},
): Promise<SwingPostmortemSummary[]> {
    if (!isSwingPgConfigured()) return [];
    await ensureSwingSchema();
    const limit = Math.max(1, Math.min(200, opts.limit ?? 50));
    const symbol = opts.symbol ? String(opts.symbol).toUpperCase() : null;
    const platform = opts.platform ? normalizePlatform(opts.platform) : null;
    const status = opts.status ?? null;
    const db = swingPg();
    const rows = await db.$queryRaw<Array<any>>(sql`
        SELECT id, platform, symbol, position_key, status, trigger_source, side,
               entry_ts_ms, exit_ts_ms, entry_price, exit_price, pnl_pct, pnl_net,
               verdict, lesson, model, usage_json, error, attempts, created_at, updated_at
        FROM swing.postmortems
        WHERE (${symbol}::text IS NULL OR symbol = ${symbol})
          AND (${platform}::text IS NULL OR platform = ${platform})
          AND (${status}::text IS NULL OR status = ${status})
        ORDER BY created_at DESC
        LIMIT ${limit};
    `);
    return (rows || []).map((r) => {
        const { report: _r, dossier: _d, ...summary } = mapPostmortemRow({ ...r, report_json: null, dossier_json: null });
        return summary;
    });
}

export async function loadSwingPostmortemById(id: number): Promise<SwingPostmortemRow | null> {
    if (!isSwingPgConfigured()) return null;
    await ensureSwingSchema();
    const db = swingPg();
    const rows = await db.$queryRaw<Array<any>>(sql`
        SELECT * FROM swing.postmortems WHERE id = ${Math.floor(id)} LIMIT 1;
    `);
    return rows?.length ? mapPostmortemRow(rows[0]) : null;
}

// Full decision rows (prompt_json INCLUDED) for a trade window — the dossier
// loader. Every other decision reader deliberately projects prompts away;
// post-mortems are the one consumer that needs them back. Chronological.
export type SwingDecisionFullRow = {
    id: number;
    decidedAtMs: number;
    symbol: string;
    platform: string;
    action: string | null;
    dryRun: boolean;
    prompt: { system?: string; user?: string } | null;
    aiDecision: Record<string, any>;
    execResult: Record<string, any>;
    snapshot: Record<string, any>;
};

export async function loadSwingDecisionWindow(opts: {
    symbol: string;
    platform?: string | null;
    fromMs: number;
    toMs: number;
    limit?: number;
}): Promise<SwingDecisionFullRow[]> {
    if (!isSwingPgConfigured()) return [];
    await ensureSwingSchema();
    const limit = Math.max(1, Math.min(1000, opts.limit ?? 500));
    const platform = opts.platform ? normalizePlatform(opts.platform) : null;
    const db = swingPg();
    const rows = await db.$queryRaw<Array<any>>(sql`
        SELECT id, decided_at_ms, symbol, platform, action, dry_run,
               prompt_json, ai_decision_json, exec_result_json, snapshot_json
        FROM swing.decisions
        WHERE symbol = ${String(opts.symbol || '').toUpperCase()}
          AND (${platform}::text IS NULL OR platform = ${platform})
          AND decided_at_ms >= ${Math.floor(opts.fromMs)}
          AND decided_at_ms <= ${Math.floor(opts.toMs)}
        ORDER BY decided_at_ms ASC
        LIMIT ${limit};
    `);
    return (rows || []).map((r) => ({
        id: Number(r.id),
        decidedAtMs: Number(r.decided_at_ms),
        symbol: String(r.symbol),
        platform: String(r.platform),
        action: r.action ?? null,
        dryRun: Boolean(r.dry_run),
        prompt: r.prompt_json ?? null,
        aiDecision: r.ai_decision_json ?? {},
        execResult: r.exec_result_json ?? {},
        snapshot: r.snapshot_json ?? {},
    }));
}

// One position row by its natural key — manual post-mortem enqueue.
export async function loadSwingPositionByKey(
    platform: string,
    positionKey: string,
): Promise<(PositionWindow & { status: string }) | null> {
    if (!isSwingPgConfigured()) return null;
    await ensureSwingSchema();
    const db = swingPg();
    const rows = await db.$queryRaw<Array<any>>(sql`
        SELECT position_key, symbol, side, status, entry_ts_ms, exit_ts_ms,
               entry_price, exit_price, notional, entry_leverage, pnl_net, pnl_gross, pnl_pct, pnl_gross_pct
        FROM swing.positions
        WHERE platform = ${normalizePlatform(platform)} AND position_key = ${positionKey}
        LIMIT 1;
    `);
    if (!rows?.length) return null;
    const r = rows[0];
    return {
        id: String(r.position_key),
        symbol: String(r.symbol),
        side: (r.side ?? null) as 'long' | 'short' | null,
        status: String(r.status),
        entryTimestamp: finite(r.entry_ts_ms),
        exitTimestamp: finite(r.exit_ts_ms),
        entryPrice: finite(r.entry_price),
        exitPrice: finite(r.exit_price),
        notional: finite(r.notional),
        leverage: finite(r.entry_leverage),
        pnlNet: finite(r.pnl_net),
        pnlGross: finite(r.pnl_gross),
        pnlPct: finite(r.pnl_pct),
        pnlGrossPct: finite(r.pnl_gross_pct),
    };
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
