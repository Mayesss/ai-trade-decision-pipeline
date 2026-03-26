export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { runScalpV2DiscoverJob } from "../../../../../lib/scalp-v2/pipeline";
import { setNoStoreHeaders } from "../../../../../lib/scalp-v2/http";

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

  const job = await runScalpV2DiscoverJob();
  return res.status(200).json({ ok: job.ok, busy: job.busy, job });
}
