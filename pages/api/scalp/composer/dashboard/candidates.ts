export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  loadScalpComposerRuntimeConfig,
  paginateScalpComposerCandidates,
} from "../../../../../lib/scalp/composer/db";
import type { ScalpComposerCandidateStatus } from "../../../../../lib/scalp/composer/types";
import {
  firstQueryValue,
  parseBool,
  parseSession,
  parseVenue,
  parseIntBounded,
  setNoStoreHeaders,
} from "../../../../../lib/scalp/composer/http";

function parseCandidateStatus(
  value: string | string[] | undefined,
): ScalpComposerCandidateStatus | undefined {
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
): "all" | "enabled" | ScalpComposerCandidateStatus {
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
    const inScope = parseBool(req.query.inScope, true);

    const status =
      state === "all" || state === "enabled" ? undefined : state;
    const deploymentEnabled =
      state === "enabled"
        ? true
        : state === "all"
          ? null
          : false;

    let scopeSymbols: string[] | undefined;
    if (inScope) {
      const runtime = await loadScalpComposerRuntimeConfig();
      const seed = runtime.seedSymbolsByVenue || ({} as Record<string, string[]>);
      const live = runtime.seedLiveSymbolsByVenue || ({} as Record<string, string[]>);
      scopeSymbols = Array.from(
        new Set(
          [
            ...(seed.bitget || []),
            ...(seed.capital || []),
            ...(live.bitget || []),
            ...(live.capital || []),
          ].map((s) => String(s || "").trim().toUpperCase()).filter(Boolean),
        ),
      );
    }

    const result = await paginateScalpComposerCandidates({
      session,
      venue,
      status,
      symbols: scopeSymbols,
      deploymentEnabled,
      offset,
      limit,
    });

    return res.status(200).json({
      ok: true,
      rows: result.rows,
      total: result.total,
      state,
      inScope,
      scopeSymbols: scopeSymbols || null,
      visibleOnly: true,
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
