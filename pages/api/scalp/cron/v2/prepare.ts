export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  invokeCronEndpointDetached,
  type CronInvokeResult,
} from "../../../../../lib/scalp/cronChaining";
import { runPreparePipelineJob } from "../../../../../lib/scalp/pipelineJobs";
import {
  listScalpEntrySessionProfiles,
  parseScalpEntrySessionProfileStrict,
} from "../../../../../lib/scalp/sessions";
import {
  clampScalpV1HardCap,
  maybeRespondScalpV1ResearchPaused,
  resolveScalpV1ResearchHardCaps,
} from "../../../../../lib/scalp/v1CostBrake";

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
  if (
    maybeRespondScalpV1ResearchPaused({
      req,
      res,
      routeId: "prepare-v2",
    })
  ) {
    return;
  }

  const hardCaps = resolveScalpV1ResearchHardCaps();
  const batchSize = clampScalpV1HardCap(
    parseIntBounded(req.query.batchSize, 4, 1, 80),
    hardCaps.maxBatchSizePrepare,
  );
  const maxAttempts = clampScalpV1HardCap(
    parseIntBounded(req.query.maxAttempts, 5, 1, 20),
    hardCaps.maxAttempts,
  );
  const autoSuccessor = parseBool(req.query.autoSuccessor, true);
  const autoContinue = parseBool(req.query.autoContinue, true);
  const selfHop = parseIntBounded(req.query.selfHop, 0, 0, 40);
  const selfMaxHops = Math.min(
    parseIntBounded(req.query.selfMaxHops, 8, 0, 50),
    hardCaps.maxSelfHops,
  );
  const session = parseEntrySessionProfile(req.query.session);
  if (!session) {
    return res.status(400).json({
      error: "invalid_session",
      message: `Use session=${listScalpEntrySessionProfiles().join("|")}.`,
    });
  }

  const result = await runPreparePipelineJob({
    batchSize,
    maxAttempts,
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
      "/api/scalp/cron/worker",
      {
        autoContinue: 1,
        autoSuccessor: 1,
        selfHop: 0,
        selfMaxHops,
        triggeredBy: "prepare-v2",
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
      "/api/scalp/cron/v2/prepare",
      {
        batchSize,
        maxAttempts,
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
    v2: true,
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
