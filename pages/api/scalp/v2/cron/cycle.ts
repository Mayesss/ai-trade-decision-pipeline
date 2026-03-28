export const config = { runtime: "nodejs", maxDuration: 800 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../../lib/scalp-v2/http";
import { runScalpV2FullAutoCycle } from "../../../../../lib/scalp-v2/pipeline";

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

  const out = await runScalpV2FullAutoCycle();

  return res.status(200).json({
    ok:
      out.discover.ok &&
      out.evaluate.ok &&
      out.worker.ok &&
      out.promote.ok,
    out,
  });
}
