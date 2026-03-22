export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import {
  invokeCronEndpointDetached,
  type CronInvokeResult,
} from "../../../../lib/scalp/cronChaining";
import { runWorkerPipelineJob } from "../../../../lib/scalp/pipelineJobs";
import {
  listScalpEntrySessionProfiles,
  parseScalpEntrySessionProfileStrict,
} from "../../../../lib/scalp/sessions";

function firstQueryValue(
  value: string | string[] | undefined,
): string | undefined {
  if (typeof value === "string") return value.trim() || undefined;
  if (Array.isArray(value) && value.length > 0)
    return String(value[0] || "").trim() || undefined;
  return undefined;
}

function parseBool(
  value: string | string[] | undefined,
  fallback: boolean,
): boolean {
  const first = firstQueryValue(value);
  if (!first) return fallback;
  const normalized = first.toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

function parseIntBounded(
  value: string | string[] | undefined,
  fallback: number,
  min: number,
  max: number,
): number {
  const first = firstQueryValue(value);
  const n = Number(first);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

function parseEntrySessionProfile(value: string | string[] | undefined) {
  return parseScalpEntrySessionProfileStrict(firstQueryValue(value));
}

function setNoStoreHeaders(res: NextApiResponse): void {
  res.setHeader(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, proxy-revalidate",
  );
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (req.method !== "GET") {
    return res
      .status(405)
      .json({ error: "Method Not Allowed", message: "Use GET" });
  }
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  const batchSize = parseIntBounded(req.query.batchSize, 80, 1, 600);
  const maxAttempts = parseIntBounded(req.query.maxAttempts, 5, 1, 20);
  const minCandlesPerWeek = parseIntBounded(
    req.query.minCandlesPerWeek,
    180,
    20,
    20_000,
  );
  const autoSuccessor = parseBool(req.query.autoSuccessor, true);
  const autoContinue = parseBool(req.query.autoContinue, true);
  const selfHop = parseIntBounded(req.query.selfHop, 0, 0, 50);
  const selfMaxHops = parseIntBounded(req.query.selfMaxHops, 10, 0, 100);
  const session = parseEntrySessionProfile(req.query.session);
  if (!session) {
    return res.status(400).json({
      error: "invalid_session",
      message: `Use session=${listScalpEntrySessionProfiles().join("|")}.`,
    });
  }

  const result = await runWorkerPipelineJob({
    batchSize,
    maxAttempts,
    minCandlesPerWeek,
    entrySessionProfile: session,
  });

  let downstream: CronInvokeResult | null = null;
  let selfRecall: CronInvokeResult | null = null;

  if (
    result.ok &&
    !result.busy &&
    autoSuccessor &&
    result.downstreamRequested
  ) {
    downstream = await invokeCronEndpointDetached(
      req,
      "/api/scalp/cron/promotion",
      {
        autoContinue: 1,
        autoSuccessor: 1,
        selfHop: 0,
        selfMaxHops,
        triggeredBy: "worker",
        session,
      },
      850,
    );
  }

  if (
    result.ok &&
    !result.busy &&
    autoContinue &&
    result.pendingAfter > 0 &&
    selfHop < selfMaxHops
  ) {
    selfRecall = await invokeCronEndpointDetached(
      req,
      "/api/scalp/cron/worker",
      {
        batchSize,
        maxAttempts,
        minCandlesPerWeek,
        autoContinue: 1,
        autoSuccessor: autoSuccessor ? 1 : 0,
        selfHop: selfHop + 1,
        selfMaxHops,
        session,
      },
      700,
    );
  }

  return res.status(200).json({
    ok: result.ok,
    busy: result.busy,
    job: result,
    chaining: {
      autoSuccessor,
      autoContinue,
      selfHop,
      selfMaxHops,
      session,
      downstream,
      selfRecall,
    },
  });
}
