export const config = { runtime: "nodejs", maxDuration: 800 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  invokeScalpV2CronEndpointDetached,
  type ScalpV2CronInvokeResult,
} from "../../../../../lib/scalp-v2/cronChaining";
import {
  clampScalpV2HardCap,
  resolveScalpV2ResearchHardCaps,
} from "../../../../../lib/scalp-v2/costControls";
import { runScalpV2DiscoverJob } from "../../../../../lib/scalp-v2/pipeline";

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

  const hardCaps = resolveScalpV2ResearchHardCaps();
  const includeLiveQuotes = parseBool(req.query.includeLiveQuotes, true);
  const dryRun = parseBool(req.query.dryRun, false);
  const maxCandidates = clampScalpV2HardCap(
    parseIntBounded(req.query.maxCandidates, 250, 20, 2_000),
    hardCaps.maxCandidates,
  );
  const evaluateBatchSize = parseIntBounded(req.query.evaluateBatchSize, 200, 1, 2_000);
  const autoSuccessor = parseBool(req.query.autoSuccessor, true);
  const autoContinue = parseBool(req.query.autoContinue, true);
  const selfHop = parseIntBounded(req.query.selfHop, 0, 0, 20);
  const selfMaxHops = Math.min(
    parseIntBounded(req.query.selfMaxHops, 6, 0, 50),
    hardCaps.maxSelfHops,
  );

  const result = await runScalpV2DiscoverJob();

  let downstream: ScalpV2CronInvokeResult | null = null;
  let selfRecall: ScalpV2CronInvokeResult | null = null;

  if (
    result.ok &&
    !result.busy &&
    autoSuccessor &&
    result.processed > 0
  ) {
    downstream = await invokeScalpV2CronEndpointDetached(
      req,
      "/api/scalp/v2/cron/evaluate",
      {
        batchSize: evaluateBatchSize,
        triggeredBy: "discover-v2-native",
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
    selfRecall = await invokeScalpV2CronEndpointDetached(
      req,
      "/api/scalp/v2/cron/discover",
      {
        includeLiveQuotes: includeLiveQuotes ? 1 : 0,
        dryRun: dryRun ? 1 : 0,
        maxCandidates,
        autoContinue: 1,
        autoSuccessor: autoSuccessor ? 1 : 0,
        selfHop: selfHop + 1,
        selfMaxHops,
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
      compatIgnored: {
        includeLiveQuotes,
        dryRun,
        maxCandidates,
      },
      downstream,
      selfRecall,
    },
  });
}
