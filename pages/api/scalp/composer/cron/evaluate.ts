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
import { runScalpComposerEvaluateJob } from "../../../../../lib/scalp/composer/pipeline";

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
  const batchSize = parseIntBounded(req.query.batchSize, 200, 1, 2_000);
  const workerBatchSize = clampScalpComposerHardCap(
    parseIntBounded(req.query.workerBatchSize, 12, 1, 600),
    hardCaps.maxBatchSizeWorker,
  );
  const autoSuccessor = parseBool(req.query.autoSuccessor, true);
  const job = await runScalpComposerEvaluateJob({ batchSize });

  let downstream: ScalpComposerCronInvokeResult | null = null;
  if (job.ok && !job.busy && autoSuccessor) {
    downstream = await invokeScalpComposerCronEndpointDetached(
      req,
      "/api/scalp/composer/cron/worker",
      {
        batchSize: workerBatchSize,
        triggeredBy: "evaluate-v2",
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
      workerBatchSize,
      downstream,
    },
  });
}
