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
import {
  parseBool,
  parseIntBounded,
  setNoStoreHeaders,
} from "../../../../../lib/scalp-v2/http";
import { runScalpV2EvaluateJob } from "../../../../../lib/scalp-v2/pipeline";

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
  const batchSize = parseIntBounded(req.query.batchSize, 200, 1, 2_000);
  const workerBatchSize = clampScalpV2HardCap(
    parseIntBounded(req.query.workerBatchSize, 12, 1, 600),
    hardCaps.maxBatchSizeWorker,
  );
  const autoSuccessor = parseBool(req.query.autoSuccessor, true);
  const job = await runScalpV2EvaluateJob({ batchSize });

  let downstream: ScalpV2CronInvokeResult | null = null;
  if (job.ok && !job.busy && autoSuccessor) {
    downstream = await invokeScalpV2CronEndpointDetached(
      req,
      "/api/scalp/v2/cron/worker",
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
