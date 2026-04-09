export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { paginateScalpV2Candidates } from "../../../../../lib/scalp-v2/db";
import {
  parseSession,
  parseVenue,
  parseIntBounded,
  setNoStoreHeaders,
} from "../../../../../lib/scalp-v2/http";

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

  try {
    const session = parseSession(req.query.session);
    const venue = parseVenue(req.query.venue);
    const offset = parseIntBounded(req.query.offset, 0, 0, 100_000);
    const limit = parseIntBounded(req.query.limit, 100, 1, 500);

    const result = await paginateScalpV2Candidates({
      session,
      venue,
      offset,
      limit,
    });

    return res.status(200).json({
      ok: true,
      rows: result.rows,
      total: result.total,
      offset,
      limit,
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "candidates_paginate_failed",
      message: err?.message || String(err),
    });
  }
}
