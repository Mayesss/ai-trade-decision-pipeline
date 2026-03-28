export const config = { runtime: "nodejs", maxDuration: 800 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  parseBool,
  parseSession,
  parseVenue,
  setNoStoreHeaders,
} from "../../../../../lib/scalp-v2/http";
import { runScalpV2ExecuteJob } from "../../../../../lib/scalp-v2/pipeline";

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

  const dryRun = parseBool(req.query.dryRun, true);
  const venue = parseVenue(req.query.venue);
  const session = parseSession(req.query.session);

  const job = await runScalpV2ExecuteJob({
    dryRun,
    venue,
    session,
  });
  return res.status(200).json({ ok: job.ok, busy: job.busy, job });
}
