export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
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

  const batchSize = parseIntBounded(req.query.batchSize, 200, 1, 2_000);
  const job = await runScalpV2EvaluateJob({ batchSize });
  return res.status(200).json({ ok: job.ok, busy: job.busy, job });
}
