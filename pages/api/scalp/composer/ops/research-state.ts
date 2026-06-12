export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  listScalpComposerResearchCursors,
  listScalpComposerResearchHighlights,
  loadScalpComposerResearchCursor,
  upsertScalpComposerResearchCursor,
  upsertScalpComposerResearchHighlights,
} from "../../../../../lib/scalp/composer/db";
import {
  firstQueryValue,
  parseIntBounded,
  parseSession,
  parseVenue,
  setNoStoreHeaders,
} from "../../../../../lib/scalp/composer/http";
import { toScalpComposerResearchCursorKey } from "../../../../../lib/scalp/composer/research";
import type {
  ScalpComposerResearchCursor,
  ScalpComposerSession,
  ScalpComposerVenue,
} from "../../../../../lib/scalp/composer/types";

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

function parseVenueLoose(value: unknown): ScalpComposerVenue | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "bitget") return "bitget";
  if (normalized === "capital") return "capital";
  return null;
}

function parseSessionLoose(value: unknown): ScalpComposerSession | null {
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
  return null;
}

function parsePhaseLoose(
  value: unknown,
): ScalpComposerResearchCursor["phase"] | null {
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
      venue: ScalpComposerVenue;
      symbol: string;
      entrySessionProfile: ScalpComposerSession;
      phase: ScalpComposerResearchCursor["phase"];
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
    toScalpComposerResearchCursorKey({
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
): Parameters<typeof upsertScalpComposerResearchHighlights>[0]["rows"] {
  if (!Array.isArray(value)) return [];
  const rows: Parameters<typeof upsertScalpComposerResearchHighlights>[0]["rows"] = [];
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
          ? toScalpComposerResearchCursorKey({
              venue,
              symbol,
              entrySessionProfile,
            })
          : "");

      const [cursor, cursors, highlights] = await Promise.all([
        impliedCursorKey
          ? loadScalpComposerResearchCursor({ cursorKey: impliedCursorKey })
          : Promise.resolve(null),
        listScalpComposerResearchCursors({
          venue,
          symbol,
          entrySessionProfile,
          limit: limitCursors,
        }),
        listScalpComposerResearchHighlights({
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
          ? upsertScalpComposerResearchCursor(cursorBody)
          : Promise.resolve(null),
        highlightRows.length
          ? upsertScalpComposerResearchHighlights({ rows: highlightRows })
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
