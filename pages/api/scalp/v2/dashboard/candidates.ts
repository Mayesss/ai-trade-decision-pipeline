export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { paginateScalpV2Candidates } from "../../../../../lib/scalp-v2/db";
import type { ScalpV2CandidateStatus } from "../../../../../lib/scalp-v2/types";
import {
  firstQueryValue,
  parseSession,
  parseVenue,
  parseIntBounded,
  setNoStoreHeaders,
} from "../../../../../lib/scalp-v2/http";

function parseCandidateStatus(
  value: string | string[] | undefined,
): ScalpV2CandidateStatus | undefined {
  const raw = firstQueryValue(value);
  if (!raw) return undefined;
  const normalized = raw.toLowerCase();
  if (normalized === "discovered") return "discovered";
  if (normalized === "evaluated") return "evaluated";
  if (normalized === "promoted") return "promoted";
  if (normalized === "rejected") return "rejected";
  return undefined;
}

function parseCandidateState(
  value: string | string[] | undefined,
): "all" | "enabled" | ScalpV2CandidateStatus {
  const raw = firstQueryValue(value);
  if (!raw) return "all";
  const normalized = raw.toLowerCase();
  if (normalized === "all") return "all";
  if (normalized === "enabled") return "enabled";
  return parseCandidateStatus(normalized) || "all";
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

  try {
    const session = parseSession(req.query.session);
    const venue = parseVenue(req.query.venue);
    const offset = parseIntBounded(req.query.offset, 0, 0, 100_000);
    const limit = parseIntBounded(req.query.limit, 100, 1, 500);
    const state = parseCandidateState(req.query.state);

    const status =
      state === "all" || state === "enabled" ? undefined : state;
    const deploymentEnabled =
      state === "enabled"
        ? true
        : state === "all"
          ? null
          : false;

    const result = await paginateScalpV2Candidates({
      session,
      venue,
      status,
      deploymentEnabled,
      offset,
      limit,
    });

    return res.status(200).json({
      ok: true,
      rows: result.rows,
      total: result.total,
      state,
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
