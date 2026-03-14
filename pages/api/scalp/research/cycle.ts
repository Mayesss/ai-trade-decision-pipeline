export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import {
  aggregateScalpResearchCycle,
  listResearchTasksPage,
  listResearchCycleTasks,
  loadActiveResearchCycleId,
  loadLatestCompletedResearchCycleId,
  loadResearchCycle,
  loadResearchCycleSummary,
  loadResearchWorkerHeartbeat,
  retryResearchTask,
  type ScalpResearchCycleSnapshot,
} from "../../../../lib/scalp/researchCycle";

const ACTIVE_CYCLE_STALE_AFTER_MS = (() => {
  const value = Number(
    process.env.SCALP_PIPELINE_ACTIVE_CYCLE_STALE_AFTER_MS ?? 20 * 60_000,
  );
  if (!Number.isFinite(value)) return 20 * 60_000;
  return Math.max(60_000, Math.floor(value));
})();

function parseBoolParam(
  value: string | string[] | undefined,
  fallback: boolean,
): boolean {
  if (value === undefined) return fallback;
  const first = Array.isArray(value) ? value[0] : value;
  if (first === undefined) return fallback;
  const normalized = String(first).trim().toLowerCase();
  if (["false", "0", "no", "off"].includes(normalized)) return false;
  if (["true", "1", "yes", "on"].includes(normalized)) return true;
  return fallback;
}

function firstQueryValue(
  value: string | string[] | undefined,
): string | undefined {
  if (typeof value === "string") return value.trim() || undefined;
  if (Array.isArray(value) && value.length > 0)
    return String(value[0] || "").trim() || undefined;
  return undefined;
}

function parsePositiveInt(value: string | undefined): number | undefined {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return undefined;
  return Math.floor(n);
}

function parseNonNegativeInt(value: string | undefined): number | undefined {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return undefined;
  return Math.floor(n);
}

function setNoStoreHeaders(res: NextApiResponse): void {
  res.setHeader(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, proxy-revalidate",
  );
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
}

function isFreshWorkerHeartbeatForCycle(params: {
  cycleId: string | null;
  nowMs: number;
  workerHeartbeat: Awaited<ReturnType<typeof loadResearchWorkerHeartbeat>>;
}): boolean {
  const cycleId = String(params.cycleId || "").trim();
  if (!cycleId) return false;
  const heartbeat = params.workerHeartbeat;
  const heartbeatCycleId = String(heartbeat?.cycleId || "").trim();
  if (!heartbeatCycleId || heartbeatCycleId !== cycleId) return false;
  const heartbeatStatus = String(heartbeat?.status || "")
    .trim()
    .toLowerCase();
  if (heartbeatStatus !== "started") return false;
  const updatedAtMs = Number(heartbeat?.updatedAtMs);
  if (!Number.isFinite(updatedAtMs) || updatedAtMs <= 0) return false;
  return params.nowMs - Math.floor(updatedAtMs) <= ACTIVE_CYCLE_STALE_AFTER_MS;
}

function isStaleRunningCycle(params: {
  cycle: ScalpResearchCycleSnapshot | null;
  workerHeartbeat: Awaited<ReturnType<typeof loadResearchWorkerHeartbeat>>;
  nowMs: number;
}): boolean {
  const cycle = params.cycle;
  if (!cycle || cycle.status !== "running") return false;
  if (params.nowMs - cycle.updatedAtMs <= ACTIVE_CYCLE_STALE_AFTER_MS) {
    return false;
  }
  return !isFreshWorkerHeartbeatForCycle({
    cycleId: cycle.cycleId,
    nowMs: params.nowMs,
    workerHeartbeat: params.workerHeartbeat,
  });
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    if (req.method === "GET") {
      const requestedCycleId = firstQueryValue(req.query.cycleId);
      const includeTasks = parseBoolParam(req.query.includeTasks, false);
      const refreshSummary = parseBoolParam(req.query.refreshSummary, false);
      const allowLatestCompletedFallback = parseBoolParam(
        req.query.allowLatestCompletedFallback,
        true,
      );
      const fallbackToRecentTasks = parseBoolParam(
        req.query.fallbackToRecentTasks,
        true,
      );
      const tasksModeRaw = String(firstQueryValue(req.query.tasksMode) || "")
        .trim()
        .toLowerCase();
      const tasksMode = tasksModeRaw === "cycle" ? "cycle" : "all";
      const taskLimit = Math.max(
        1,
        Math.min(
          5000,
          parsePositiveInt(firstQueryValue(req.query.taskLimit)) || 250,
        ),
      );
      const taskOffset = Math.max(
        0,
        Math.min(
          500_000,
          parseNonNegativeInt(firstQueryValue(req.query.taskOffset)) || 0,
        ),
      );

      const nowMs = Date.now();
      const activeCycleId = await loadActiveResearchCycleId();
      const workerHeartbeat = await loadResearchWorkerHeartbeat();
      const activeCycle =
        !requestedCycleId && activeCycleId
          ? await loadResearchCycle(activeCycleId)
          : null;
      const staleActiveCycle = isStaleRunningCycle({
        cycle: activeCycle,
        workerHeartbeat,
        nowMs,
      });
      const latestCompletedCycleId =
        !requestedCycleId && allowLatestCompletedFallback
          ? await loadLatestCompletedResearchCycleId()
          : null;
      const fallbackToCompleted =
        !requestedCycleId &&
        staleActiveCycle &&
        latestCompletedCycleId &&
        latestCompletedCycleId !== activeCycleId;
      const cycleId = requestedCycleId
        ? requestedCycleId
        : fallbackToCompleted
          ? latestCompletedCycleId
          : activeCycleId || latestCompletedCycleId;
      const cycleSource = requestedCycleId
        ? "requested"
        : fallbackToCompleted
          ? "stale_active_fallback"
          : activeCycleId
            ? "active"
            : latestCompletedCycleId
              ? "latest_completed_fallback"
              : "none";
      if (!cycleId) {
        if (includeTasks && fallbackToRecentTasks) {
          const page = await listResearchTasksPage({
            limit: taskLimit,
            offset: taskOffset,
          });
          return res.status(200).json({
            ok: true,
            cycleId: null,
            cycleSource,
            cycle: null,
            summary: null,
            workerHeartbeat,
            tasks: page.tasks,
            taskCountReturned: page.tasks.length,
            includeTasks,
            taskLimit: page.limit,
            taskOffset: page.offset,
            tasksMode: "all",
            tasksTotal: page.total,
            tasksHasMore: page.hasMore,
            message:
              "No active/completed cycle found; returning paginated recent research tasks fallback.",
          });
        }
        return res.status(404).json({
          error: "research_cycle_not_found",
          message:
            "No active or completed research cycle found and no cycleId was provided.",
          cycleSource,
          activeCycleId: activeCycleId || null,
          latestCompletedCycleId: latestCompletedCycleId || null,
        });
      }

      const aggregated = refreshSummary
        ? await aggregateScalpResearchCycle({
            cycleId,
            finalizeWhenDone: false,
          })
        : null;
      const cycle =
        aggregated?.cycle ||
        (requestedCycleId
          ? await loadResearchCycle(cycleId)
          : fallbackToCompleted && latestCompletedCycleId === cycleId
            ? await loadResearchCycle(cycleId)
            : activeCycle?.cycleId === cycleId
              ? activeCycle
              : await loadResearchCycle(cycleId));
      if (!cycle) {
        return res.status(404).json({
          error: "research_cycle_not_found",
          message: `Research cycle '${cycleId}' was not found.`,
        });
      }

      const summary =
        aggregated?.summary ||
        cycle.latestSummary ||
        (await loadResearchCycleSummary(cycle.cycleId));
      const pagedTasks =
        includeTasks && tasksMode === "all"
          ? await listResearchTasksPage({
              limit: taskLimit,
              offset: taskOffset,
            })
          : null;
      const tasks = includeTasks
        ? tasksMode === "all"
          ? pagedTasks?.tasks || []
          : await listResearchCycleTasks(cycle.cycleId, taskLimit)
        : [];
      return res.status(200).json({
        ok: true,
        cycleId: cycle.cycleId,
        cycleSource,
        cycle,
        summary,
        workerHeartbeat:
          workerHeartbeat && workerHeartbeat.cycleId === cycle.cycleId
            ? workerHeartbeat
            : null,
        staleActiveCycleId:
          staleActiveCycle && activeCycleId ? activeCycleId : null,
        tasks: includeTasks ? tasks : undefined,
        taskCountReturned: includeTasks ? tasks.length : 0,
        includeTasks,
        taskLimit:
          tasksMode === "all" ? (pagedTasks?.limit ?? taskLimit) : taskLimit,
        taskOffset: tasksMode === "all" ? (pagedTasks?.offset ?? 0) : 0,
        tasksMode,
        tasksTotal:
          tasksMode === "all"
            ? (pagedTasks?.total ?? tasks.length)
            : tasks.length,
        tasksHasMore:
          tasksMode === "all" ? Boolean(pagedTasks?.hasMore) : false,
      });
    }

    if (req.method === "POST") {
      const body =
        req.body && typeof req.body === "object"
          ? (req.body as Record<string, unknown>)
          : {};
      const action = String(body.action || "").trim();
      if (action !== "retryTask") {
        return res.status(400).json({
          error: "invalid_action",
          message: "Supported actions: retryTask",
        });
      }

      const cycleId = String(body.cycleId || "").trim() || undefined;
      const taskId = String(body.taskId || "").trim();
      if (!taskId) {
        return res.status(400).json({
          error: "task_id_required",
          message: "Provide taskId in request body.",
        });
      }

      const retried = await retryResearchTask({ cycleId, taskId });
      const aggregate = await aggregateScalpResearchCycle({
        cycleId: retried.cycle.cycleId,
        finalizeWhenDone: false,
      });

      return res.status(200).json({
        ok: true,
        action,
        cycleId: retried.cycle.cycleId,
        taskId: retried.task.taskId,
        status: retried.task.status,
        summary: aggregate?.summary || retried.cycle.latestSummary || null,
      });
    }

    return res
      .status(405)
      .json({ error: "Method Not Allowed", message: "Use GET or POST" });
  } catch (err: any) {
    const code = String(err?.code || "").trim();
    if (
      code === "task_id_required" ||
      code === "task_not_failed" ||
      code === "research_cycle_not_running" ||
      code === "research_cycle_not_active"
    ) {
      return res.status(409).json({
        error: code,
        message: err?.message || code,
      });
    }
    if (code === "research_cycle_not_found" || code === "task_not_found") {
      return res.status(404).json({
        error: code,
        message: err?.message || code,
      });
    }
    if (code === "task_locked") {
      return res.status(423).json({
        error: code,
        message: err?.message || code,
      });
    }
    return res.status(500).json({
      error: "research_cycle_read_failed",
      message: err?.message || String(err),
    });
  }
}
