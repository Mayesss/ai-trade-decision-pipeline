export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  listScalpV2ResearchCursors,
  listScalpV2ResearchHighlights,
  loadScalpV2ResearchCursor,
  upsertScalpV2ResearchCursor,
  upsertScalpV2ResearchHighlights,
} from "../../../../../lib/scalp-v2/db";
import {
  firstQueryValue,
  parseIntBounded,
  parseSession,
  parseVenue,
  setNoStoreHeaders,
} from "../../../../../lib/scalp-v2/http";
import { toScalpV2ResearchCursorKey } from "../../../../../lib/scalp-v2/research";
import type {
  ScalpV2ResearchCursor,
  ScalpV2Session,
  ScalpV2Venue,
} from "../../../../../lib/scalp-v2/types";

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function parseVenueLoose(value: unknown): ScalpV2Venue | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "bitget") return "bitget";
  if (normalized === "capital") return "capital";
  return null;
}

function parseSessionLoose(value: unknown): ScalpV2Session | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (
    normalized === "tokyo" ||
    normalized === "berlin" ||
    normalized === "newyork" ||
    normalized === "sydney"
  ) {
    return normalized;
  }
  return null;
}

function parsePhaseLoose(
  value: unknown,
): ScalpV2ResearchCursor["phase"] | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "scan") return "scan";
  if (normalized === "score") return "score";
  if (normalized === "validate") return "validate";
  if (normalized === "promote") return "promote";
  return null;
}

function parseCursorBody(
  body: Record<string, unknown>,
):
  | {
      cursorKey: string;
      venue: ScalpV2Venue;
      symbol: string;
      entrySessionProfile: ScalpV2Session;
      phase: ScalpV2ResearchCursor["phase"];
      lastCandidateOffset: number;
      lastWeekStartMs: number | null;
      progress: Record<string, unknown>;
    }
  | null {
  const venue = parseVenueLoose(body.venue);
  const symbol = normalizeSymbol(body.symbol);
  const entrySessionProfile = parseSessionLoose(body.entrySessionProfile);
  if (!venue || !symbol || !entrySessionProfile) return null;

  const phase = parsePhaseLoose(body.phase) || "scan";
  const lastCandidateOffset = Math.max(
    0,
    Math.floor(Number(body.lastCandidateOffset || 0)),
  );
  const lastWeekStartMs = Number.isFinite(Number(body.lastWeekStartMs))
    ? Math.floor(Number(body.lastWeekStartMs))
    : null;
  const progress = asRecord(body.progress);
  const providedCursorKey = String(body.cursorKey || "").trim();
  const cursorKey =
    providedCursorKey ||
    toScalpV2ResearchCursorKey({
      venue,
      symbol,
      entrySessionProfile,
    });

  return {
    cursorKey,
    venue,
    symbol,
    entrySessionProfile,
    phase,
    lastCandidateOffset,
    lastWeekStartMs,
    progress,
  };
}

function parseHighlightRowsBody(
  value: unknown,
): Parameters<typeof upsertScalpV2ResearchHighlights>[0]["rows"] {
  if (!Array.isArray(value)) return [];
  const rows: Parameters<typeof upsertScalpV2ResearchHighlights>[0]["rows"] = [];
  for (const raw of value) {
    const row = asRecord(raw);
    const candidateId = String(row.candidateId || "").trim();
    const venue = parseVenueLoose(row.venue);
    const symbol = normalizeSymbol(row.symbol);
    const entrySessionProfile = parseSessionLoose(row.entrySessionProfile);
    if (!candidateId || !venue || !symbol || !entrySessionProfile) continue;
    rows.push({
      candidateId,
      venue,
      symbol,
      entrySessionProfile,
      score: Number.isFinite(Number(row.score)) ? Number(row.score) : 0,
      trades12w: Math.max(0, Math.floor(Number(row.trades12w || 0))),
      winningWeeks12w: Math.max(0, Math.floor(Number(row.winningWeeks12w || 0))),
      consecutiveWinningWeeks: Math.max(
        0,
        Math.floor(Number(row.consecutiveWinningWeeks || 0)),
      ),
      robustness: asRecord(row.robustness),
      dsl: asRecord(row.dsl),
      notes:
        row.notes === undefined || row.notes === null
          ? null
          : String(row.notes),
      remarkable: row.remarkable !== false,
    });
  }
  return rows;
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    if (req.method === "GET") {
      const venue = parseVenue(req.query.venue);
      const entrySessionProfile = parseSession(req.query.entrySessionProfile);
      const symbol = normalizeSymbol(firstQueryValue(req.query.symbol));
      const cursorKeyQuery = String(
        firstQueryValue(req.query.cursorKey) || "",
      ).trim();
      const limitCursors = parseIntBounded(req.query.cursorLimit, 100, 1, 5_000);
      const limitHighlights = parseIntBounded(
        req.query.highlightLimit,
        200,
        1,
        5_000,
      );
      const remarkableOnly = String(
        firstQueryValue(req.query.remarkableOnly) || "true",
      ).trim().toLowerCase() !== "false";

      const impliedCursorKey =
        cursorKeyQuery ||
        (venue && symbol && entrySessionProfile
          ? toScalpV2ResearchCursorKey({
              venue,
              symbol,
              entrySessionProfile,
            })
          : "");

      const [cursor, cursors, highlights] = await Promise.all([
        impliedCursorKey
          ? loadScalpV2ResearchCursor({ cursorKey: impliedCursorKey })
          : Promise.resolve(null),
        listScalpV2ResearchCursors({
          venue,
          symbol,
          entrySessionProfile,
          limit: limitCursors,
        }),
        listScalpV2ResearchHighlights({
          venue,
          symbol,
          entrySessionProfile,
          remarkableOnly,
          limit: limitHighlights,
        }),
      ]);

      return res.status(200).json({
        ok: true,
        mode: "scalp_v2",
        cursor,
        cursors,
        highlights,
      });
    }

    if (req.method === "POST") {
      const body = asRecord(req.body);
      const cursorBody = parseCursorBody(asRecord(body.cursor));
      const highlightRows = parseHighlightRowsBody(body.highlights);

      const [cursor, highlightsWritten] = await Promise.all([
        cursorBody
          ? upsertScalpV2ResearchCursor(cursorBody)
          : Promise.resolve(null),
        highlightRows.length
          ? upsertScalpV2ResearchHighlights({ rows: highlightRows })
          : Promise.resolve(0),
      ]);

      return res.status(200).json({
        ok: true,
        mode: "scalp_v2",
        cursor,
        highlightsWritten,
      });
    }

    return res
      .status(405)
      .json({ error: "Method Not Allowed", message: "Use GET or POST" });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_v2_ops_research_state_failed",
      message: err?.message || String(err),
    });
  }
}
