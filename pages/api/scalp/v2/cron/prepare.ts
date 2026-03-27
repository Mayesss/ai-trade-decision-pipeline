export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../../lib/scalp-v2/http";

function firstQueryValue(
  value: string | string[] | undefined,
): string | undefined {
  if (typeof value === "string") return value.trim() || undefined;
  if (Array.isArray(value) && value.length > 0)
    return String(value[0] || "").trim() || undefined;
  return undefined;
}

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

  const session = firstQueryValue(req.query.session) || null;

  return res.status(200).json({
    ok: true,
    busy: false,
    v2: true,
    deprecated: true,
    job: {
      ok: true,
      busy: false,
      jobKind: "prepare",
      processed: 0,
      succeeded: 0,
      failed: 0,
      pendingAfter: 0,
      details: {
        skipped: true,
        reason: "legacy_prepare_deprecated_for_v2",
        replacement: "/api/scalp/v2/cron/evaluate",
      },
    },
    chaining: {
      autoSuccessor: false,
      autoContinue: false,
      selfHop: 0,
      selfMaxHops: 0,
      session,
      downstream: null,
      selfRecall: null,
    },
  });
}
