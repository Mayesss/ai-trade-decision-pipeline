export const config = { runtime: "nodejs", maxDuration: 800 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  invokeScalpComposerCronEndpointDetached,
  type ScalpComposerCronInvokeResult,
} from "../../../../../lib/scalp/composer/cronChaining";
import {
  clampScalpComposerHardCap,
  resolveScalpComposerResearchHardCaps,
} from "../../../../../lib/scalp/composer/costControls";
import {
  parseBool,
  parseIntBounded,
  setNoStoreHeaders,
} from "../../../../../lib/scalp/composer/http";
import { runScalpComposerWorkerJob } from "../../../../../lib/scalp/composer/pipeline";
import { runScalpRegimeResearchJob } from "../../../../../lib/scalp/regimes";

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

  const hardCaps = resolveScalpComposerResearchHardCaps();
  const batchSize = clampScalpComposerHardCap(
    parseIntBounded(req.query.batchSize, 12, 1, 600),
    hardCaps.maxBatchSizeWorker,
  );
  const autoSuccessor = parseBool(req.query.autoSuccessor, true);
  const legacyV2 = parseBool(req.query.legacyV2, false);
  if (!legacyV2) {
    const job = await runScalpRegimeResearchJob({
      maxCandidatesPerCall: Math.max(0, Math.min(500, batchSize)),
      candidateFetchLimit: Math.max(Math.min(500, batchSize) * 4, 50),
      forceValidity: parseBool(req.query.forceValidity, false),
    });
    return res.status(200).json({
      ok: job.ok,
      busy: job.busy,
      job,
      version: "v4",
      legacyRoute: "/api/scalp/composer/cron/worker",
      message:
        "v2 worker is disabled by default; pass legacyV2=true to run the old v2/v3 path.",
      chaining: {
        autoSuccessor: false,
        downstream: null,
      },
    });
  }

  const job = await runScalpComposerWorkerJob({ batchSize });

  let downstream: ScalpComposerCronInvokeResult | null = null;
  if (job.ok && !job.busy && autoSuccessor) {
    downstream = await invokeScalpComposerCronEndpointDetached(
      req,
      "/api/scalp/composer/cron/promote",
      {
        triggeredBy: "worker-v2",
      },
      850,
    );
  }

  return res.status(200).json({
    ok: job.ok,
    busy: job.busy,
    job,
    chaining: {
      autoSuccessor,
      downstream,
    },
  });
}
