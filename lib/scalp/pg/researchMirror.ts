import { Prisma } from "@prisma/client";

import { normalizeScalpTuneId } from "../deployments";
import { scalpPrisma } from "./client";

type ScalpCycleStatus = "running" | "completed" | "failed" | "stalled";
type ScalpResearchTaskStatusKv =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "aborted";
type ScalpResearchTaskStatusPg =
  | "pending"
  | "running"
  | "completed"
  | "failed_permanent";

export interface PgResearchCycleRow {
  cycleId: string;
  status: ScalpCycleStatus;
  paramsJson: Record<string, unknown>;
  latestSummaryJson?: Record<string, unknown> | null;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface PgResearchTaskRow {
  taskId: string;
  cycleId: string;
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  windowFromTs: number;
  windowToTs: number;
  status: ScalpResearchTaskStatusKv;
  attempts: number;
  maxAttempts: number;
  workerId?: string | null;
  startedAtMs?: number | null;
  finishedAtMs?: number | null;
  errorCode?: string | null;
  errorMessage?: string | null;
  result?: Record<string, unknown> | null;
  createdAtMs: number;
  updatedAtMs: number;
  priority?: number;
}

export interface PgSymbolCooldownEntryRow {
  symbol: string;
  failureCount: number;
  windowStartedAtMs: number;
  blockedUntilMs: number;
  lastFailureCode?: string | null;
  lastFailureMessage?: string | null;
  cycleId?: string | null;
  updatedAtMs: number;
}

function toDate(valueMs: unknown, fallbackMs = Date.now()): Date {
  const n = Number(valueMs);
  if (!Number.isFinite(n) || n <= 0) return new Date(Math.floor(fallbackMs));
  return new Date(Math.floor(n));
}

function normalizeOptionalText(value: unknown, maxLen: number): string | null {
  const normalized = String(value || "").trim();
  if (!normalized) return null;
  return normalized.slice(0, maxLen);
}

function asJsonObject(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function mapTaskStatusToPg(
  status: ScalpResearchTaskStatusKv,
  attempts: number,
  maxAttempts: number,
  errorCode: string | null,
  errorMessage: string | null,
): ScalpResearchTaskStatusPg {
  if (status === "pending") return "pending";
  if (status === "running") return "running";
  if (status === "completed") return "completed";
  if (status === "aborted") return "failed_permanent";
  const normalizedCode = String(errorCode || "")
    .trim()
    .toLowerCase();
  const normalizedMessage = String(errorMessage || "")
    .trim()
    .toLowerCase();
  if (
    normalizedCode === "symbol_cooldown_active" ||
    normalizedMessage.includes("symbol_cooldown_active_until")
  ) {
    return "pending";
  }
  if (
    Math.max(0, Math.floor(attempts)) >=
    Math.max(1, Math.floor(maxAttempts || 1))
  ) {
    return "failed_permanent";
  }
  return "pending";
}

export async function upsertResearchCycleToPg(
  row: PgResearchCycleRow,
): Promise<number> {
  const cycleId = String(row.cycleId || "").trim();
  if (!cycleId) return 0;
  const status = row.status;
  const paramsJson = asJsonObject(row.paramsJson);
  const latestSummaryJson = row.latestSummaryJson
    ? asJsonObject(row.latestSummaryJson)
    : null;
  const createdAt = toDate(row.createdAtMs, Date.now());
  const updatedAt = toDate(row.updatedAtMs, Date.now());
  const completedAt =
    status === "completed" || status === "failed" ? updatedAt : null;

  const db = scalpPrisma();
  const updated = await db.$executeRaw(
    Prisma.sql`
        INSERT INTO scalp_research_cycles(
            cycle_id,
            status,
            params_json,
            latest_summary_json,
            created_at,
            updated_at,
            completed_at
        )
        VALUES(
            ${cycleId},
            ${status}::scalp_cycle_status,
            ${JSON.stringify(paramsJson)}::jsonb,
            ${latestSummaryJson ? Prisma.sql`${JSON.stringify(latestSummaryJson)}::jsonb` : Prisma.sql`NULL::jsonb`},
            ${createdAt},
            ${updatedAt},
            ${completedAt}
        )
        ON CONFLICT(cycle_id)
        DO UPDATE SET
            status = EXCLUDED.status,
            params_json = EXCLUDED.params_json,
            latest_summary_json = EXCLUDED.latest_summary_json,
            updated_at = EXCLUDED.updated_at,
            completed_at = EXCLUDED.completed_at;
        `,
  );
  return Number(updated || 0);
}

export async function upsertResearchTasksBulkToPg(
  rows: PgResearchTaskRow[],
): Promise<number> {
  const payload = rows
    .map((row) => {
      const taskId = String(row.taskId || "").trim();
      const cycleId = String(row.cycleId || "").trim();
      const deploymentId = String(row.deploymentId || "").trim();
      const symbol = String(row.symbol || "")
        .trim()
        .toUpperCase();
      const strategyId = String(row.strategyId || "")
        .trim()
        .toLowerCase();
      const tuneId = normalizeScalpTuneId(row.tuneId, "default");
      if (
        !taskId ||
        !cycleId ||
        !deploymentId ||
        !symbol ||
        !strategyId ||
        !tuneId
      )
        return null;
      const attempts = Math.max(0, Math.floor(Number(row.attempts) || 0));
      const maxAttempts = Math.max(1, Math.floor(Number(row.maxAttempts) || 1));
      return {
        task_id: taskId,
        cycle_id: cycleId,
        deployment_id: deploymentId,
        symbol,
        strategy_id: strategyId,
        tune_id: tuneId,
        window_from: toDate(row.windowFromTs).toISOString(),
        window_to: toDate(row.windowToTs).toISOString(),
        status: mapTaskStatusToPg(
          row.status,
          attempts,
          maxAttempts,
          row.errorCode || null,
          row.errorMessage || null,
        ),
        attempts,
        max_attempts: maxAttempts,
        next_eligible_at: toDate(row.updatedAtMs).toISOString(),
        worker_id: normalizeOptionalText(row.workerId, 120),
        started_at:
          Number.isFinite(Number(row.startedAtMs)) &&
          Number(row.startedAtMs) > 0
            ? toDate(row.startedAtMs).toISOString()
            : null,
        finished_at:
          Number.isFinite(Number(row.finishedAtMs)) &&
          Number(row.finishedAtMs) > 0
            ? toDate(row.finishedAtMs).toISOString()
            : null,
        error_code: normalizeOptionalText(row.errorCode, 80),
        error_message: normalizeOptionalText(row.errorMessage, 300),
        result_json: row.result ? asJsonObject(row.result) : null,
        priority: Math.max(1, Math.floor(Number(row.priority) || 100)),
        created_at: toDate(row.createdAtMs).toISOString(),
        updated_at: toDate(row.updatedAtMs).toISOString(),
      };
    })
    .filter((row): row is NonNullable<typeof row> => Boolean(row));

  if (!payload.length) return 0;

  const db = scalpPrisma();
  const payloadJson = JSON.stringify(payload);
  const updated = await db.$executeRaw(
    Prisma.sql`
        WITH input AS (
            SELECT *
            FROM jsonb_to_recordset(${payloadJson}::jsonb) AS x(
                task_id text,
                cycle_id text,
                deployment_id text,
                symbol text,
                strategy_id text,
                tune_id text,
                window_from timestamptz,
                window_to timestamptz,
                status text,
                attempts int,
                max_attempts int,
                next_eligible_at timestamptz,
                worker_id text,
                started_at timestamptz,
                finished_at timestamptz,
                error_code text,
                error_message text,
                result_json jsonb,
                priority int,
                created_at timestamptz,
                updated_at timestamptz
            )
        ),
        dep_map AS (
            SELECT
                x.symbol,
                x.strategy_id,
                x.tune_id,
                COALESCE(
                    d.deployment_id,
                    MIN(x.deployment_id)
                ) AS deployment_id
            FROM input x
            LEFT JOIN scalp_deployments d
              ON d.symbol = x.symbol
             AND d.strategy_id = x.strategy_id
             AND d.tune_id = x.tune_id
            GROUP BY x.symbol, x.strategy_id, x.tune_id, d.deployment_id
        ),
        dep AS (
            INSERT INTO scalp_deployments(
                deployment_id,
                symbol,
                strategy_id,
                tune_id,
                source,
                enabled,
                config_override,
                updated_by
            )
            SELECT DISTINCT
                m.deployment_id,
                m.symbol,
                m.strategy_id,
                m.tune_id,
                'backtest',
                FALSE,
                '{}'::jsonb,
                'phase_g_pg_primary'
            FROM dep_map m
            ON CONFLICT(deployment_id) DO NOTHING
        ),
        task_input AS (
            SELECT
                x.task_id,
                x.cycle_id,
                m.deployment_id,
                x.symbol,
                x.strategy_id,
                x.tune_id,
                x.window_from,
                x.window_to,
                x.status,
                x.attempts,
                x.max_attempts,
                x.next_eligible_at,
                x.worker_id,
                x.started_at,
                x.finished_at,
                x.error_code,
                x.error_message,
                x.result_json,
                x.priority,
                x.created_at,
                x.updated_at
            FROM input x
            INNER JOIN dep_map m
              ON m.symbol = x.symbol
             AND m.strategy_id = x.strategy_id
             AND m.tune_id = x.tune_id
        )
        INSERT INTO scalp_research_tasks(
            task_id,
            cycle_id,
            deployment_id,
            symbol,
            strategy_id,
            tune_id,
            window_from,
            window_to,
            status,
            attempts,
            max_attempts,
            next_eligible_at,
            worker_id,
            started_at,
            finished_at,
            error_code,
            error_message,
            result_json,
            priority,
            created_at,
            updated_at
        )
        SELECT
            x.task_id,
            x.cycle_id,
            x.deployment_id,
            x.symbol,
            x.strategy_id,
            x.tune_id,
            x.window_from,
            x.window_to,
            x.status::scalp_research_task_status,
            x.attempts,
            x.max_attempts,
            x.next_eligible_at,
            x.worker_id,
            x.started_at,
            x.finished_at,
            x.error_code,
            x.error_message,
            x.result_json,
            x.priority,
            x.created_at,
            x.updated_at
        FROM task_input x
        ON CONFLICT(task_id)
        DO UPDATE SET
            cycle_id = EXCLUDED.cycle_id,
            deployment_id = EXCLUDED.deployment_id,
            symbol = EXCLUDED.symbol,
            strategy_id = EXCLUDED.strategy_id,
            tune_id = EXCLUDED.tune_id,
            window_from = EXCLUDED.window_from,
            window_to = EXCLUDED.window_to,
            status = EXCLUDED.status,
            attempts = EXCLUDED.attempts,
            max_attempts = EXCLUDED.max_attempts,
            next_eligible_at = EXCLUDED.next_eligible_at,
            worker_id = EXCLUDED.worker_id,
            started_at = EXCLUDED.started_at,
            finished_at = EXCLUDED.finished_at,
            error_code = EXCLUDED.error_code,
            error_message = EXCLUDED.error_message,
            result_json = EXCLUDED.result_json,
            priority = EXCLUDED.priority,
            updated_at = EXCLUDED.updated_at;
        `,
  );

  return Number(updated || 0);
}

export async function upsertSymbolCooldownSnapshotToPg(
  rows: PgSymbolCooldownEntryRow[],
): Promise<number> {
  const payload = rows
    .map((row) => {
      const symbol = String(row.symbol || "")
        .trim()
        .toUpperCase();
      if (!symbol) return null;
      return {
        symbol,
        failure_count: Math.max(0, Math.floor(Number(row.failureCount) || 0)),
        window_started_at:
          Number.isFinite(Number(row.windowStartedAtMs)) &&
          Number(row.windowStartedAtMs) > 0
            ? toDate(row.windowStartedAtMs).toISOString()
            : null,
        blocked_until:
          Number.isFinite(Number(row.blockedUntilMs)) &&
          Number(row.blockedUntilMs) > 0
            ? toDate(row.blockedUntilMs).toISOString()
            : null,
        last_error_code: normalizeOptionalText(row.lastFailureCode, 80),
        last_error_message: normalizeOptionalText(row.lastFailureMessage, 300),
        cycle_id: normalizeOptionalText(row.cycleId, 120),
        updated_at: toDate(row.updatedAtMs).toISOString(),
      };
    })
    .filter((row): row is NonNullable<typeof row> => Boolean(row));

  if (!payload.length) return 0;

  const db = scalpPrisma();
  const payloadJson = JSON.stringify(payload);
  const updated = await db.$executeRaw(
    Prisma.sql`
        WITH input AS (
            SELECT *
            FROM jsonb_to_recordset(${payloadJson}::jsonb) AS x(
                symbol text,
                failure_count int,
                window_started_at timestamptz,
                blocked_until timestamptz,
                last_error_code text,
                last_error_message text,
                cycle_id text,
                updated_at timestamptz
            )
        )
        INSERT INTO scalp_symbol_cooldowns(
            symbol,
            failure_count,
            window_started_at,
            blocked_until,
            last_error_code,
            last_error_message,
            cycle_id,
            updated_at
        )
        SELECT
            x.symbol,
            x.failure_count,
            x.window_started_at,
            x.blocked_until,
            x.last_error_code,
            x.last_error_message,
            x.cycle_id,
            x.updated_at
        FROM input x
        ON CONFLICT(symbol)
        DO UPDATE SET
            failure_count = EXCLUDED.failure_count,
            window_started_at = EXCLUDED.window_started_at,
            blocked_until = EXCLUDED.blocked_until,
            last_error_code = EXCLUDED.last_error_code,
            last_error_message = EXCLUDED.last_error_message,
            cycle_id = EXCLUDED.cycle_id,
            updated_at = EXCLUDED.updated_at;
        `,
  );

  return Number(updated || 0);
}
