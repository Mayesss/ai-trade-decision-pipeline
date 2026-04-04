export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { loadScalpV2RuntimeConfig } from "../../../../../lib/scalp-v2/db";
import {
  firstQueryValue,
  parseIntBounded,
  parseSession,
  parseVenue,
  setNoStoreHeaders,
} from "../../../../../lib/scalp-v2/http";
import {
  buildScalpV2CandidateDslGrid,
  buildScalpV2ModelGuidedComposerGrid,
  buildScalpV2PrimitiveCatalogByFamily,
  listScalpV2StrategyPrimitiveReferences,
  strategyPrimitiveCoverageSummary,
} from "../../../../../lib/scalp-v2/research";
import type {
  ScalpV2Session,
  ScalpV2Venue,
} from "../../../../../lib/scalp-v2/types";

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function fallbackSession(value: unknown): ScalpV2Session {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (
    normalized === "tokyo" ||
    normalized === "berlin" ||
    normalized === "newyork" ||
    normalized === "pacific" ||
    normalized === "sydney"
  ) {
    return normalized;
  }
  return "berlin";
}

function fallbackVenue(value: unknown): ScalpV2Venue {
  return String(value || "").trim().toLowerCase() === "capital"
    ? "capital"
    : "bitget";
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
    const runtime = await loadScalpV2RuntimeConfig();
    const venue =
      parseVenue(req.query.venue) ||
      fallbackVenue(runtime.supportedVenues[0] || "bitget");
    const session =
      parseSession(req.query.session) ||
      fallbackSession(runtime.supportedSessions[0] || "berlin");
    const symbolFromQuery = normalizeSymbol(firstQueryValue(req.query.symbol));
    const symbolFallback = normalizeSymbol(
      (runtime.seedSymbolsByVenue[venue] || [])[0] ||
        (venue === "capital" ? "EURUSD" : "BTCUSDT"),
    );
    const symbol = symbolFromQuery || symbolFallback;
    const previewLimit = parseIntBounded(req.query.previewLimit, 24, 1, 250);
    const references = listScalpV2StrategyPrimitiveReferences();
    const previewCandidates = buildScalpV2CandidateDslGrid({
      venue,
      symbol,
      entrySessionProfile: session,
      maxCandidates: previewLimit,
    });
    const previewComposerCandidates = buildScalpV2ModelGuidedComposerGrid({
      venue,
      symbol,
      entrySessionProfile: session,
      maxCandidates: previewLimit,
    });

    return res.status(200).json({
      ok: true,
      mode: "scalp_v2",
      coverage: strategyPrimitiveCoverageSummary(),
      context: {
        venue,
        symbol,
        entrySessionProfile: session,
        previewLimit,
      },
      primitiveCatalog: buildScalpV2PrimitiveCatalogByFamily(),
      strategyReferences: references,
      previewCandidates,
      previewComposerCandidates,
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_v2_ops_research_primitives_failed",
      message: err?.message || String(err),
    });
  }
}
