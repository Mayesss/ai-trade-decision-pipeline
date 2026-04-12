export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  parseIntBounded,
  setNoStoreHeaders,
} from "../../../../../lib/scalp-v2/http";
import { runScalpV2CutoverMigration } from "../../../../../lib/scalp-v2/migration";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (req.method !== "POST") {
    return res
      .status(405)
      .json({ error: "Method Not Allowed", message: "Use POST" });
  }
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const limit = parseIntBounded(req.query.limit, 50_000, 100, 500_000);
    const parityWindowDays = parseIntBounded(req.query.parityDays, 30, 1, 3650);
    const journalParityLimit = parseIntBounded(
      req.query.journalParityLimit,
      2_000,
      100,
      50_000,
    );
    const out = await runScalpV2CutoverMigration({
      limit,
      parityWindowDays,
      journalParityLimit,
    });

    return res.status(200).json({
      ok: true,
      operation: "v2_cutover_migration",
      ...out,
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_v2_cutover_migration_failed",
      message: err?.message || String(err),
    });
  }
}
