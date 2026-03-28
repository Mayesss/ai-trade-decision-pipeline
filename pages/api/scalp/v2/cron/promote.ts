export const config = { runtime: "nodejs", maxDuration: 800 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../../lib/scalp-v2/http";
import { runScalpV2PromoteJob } from "../../../../../lib/scalp-v2/pipeline";

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

  const job = await runScalpV2PromoteJob();
  return res.status(200).json({ ok: job.ok, busy: job.busy, job });
}
