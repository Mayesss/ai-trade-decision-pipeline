import type { NextApiRequest, NextApiResponse } from "next";

export type AdminAccessResult = { ok: boolean; required: boolean };
const UNAUTHENTICATED_CRON_ROUTES = new Set<string>([
  "/api/swing/analyze",
  "/api/scalp/composer/cron/discover",
  "/api/scalp/composer/cron/load-candles",
  "/api/scalp/composer/cron/evaluate",
  "/api/scalp/composer/cron/worker",
  "/api/scalp/composer/cron/promote",
  "/api/scalp/composer/cron/execute",
  "/api/scalp/composer/cron/reconcile",
  "/api/scalp/composer/cron/research",
  "/api/scalp/composer/cron/cycle",
  // v5 crons. Same pattern as the v2 list above — Vercel cron requests don't
  // carry x-admin-access-secret, so these routes have to be admin-secret-
  // exempt or the handlers 401 and the cron silently no-ops.
  "/api/scalp/research/cron/evaluate",
  "/api/scalp/research/cron/promote",
  "/api/scalp/research/cron/trim-tail",
  "/api/scalp/research/cron/cull-bottom",
  "/api/scalp/research/cron/load-live-candles",
  "/api/scalp/research/cron/preflight-candles",
  "/api/scalp/regimes/cron/build-regimes",
]);

function firstHeaderValue(value: string | string[] | undefined): string {
  if (typeof value === "string") return value.trim();
  if (Array.isArray(value) && value.length > 0)
    return String(value[0] || "").trim();
  return "";
}

function parseBearerToken(value: string): string {
  const match = value.match(/^Bearer\s+(.+)$/i);
  return match ? String(match[1] || "").trim() : "";
}

function requestPath(req: NextApiRequest): string {
  const raw = String(req.url || "").trim();
  if (!raw) return "";
  try {
    if (raw.startsWith("http://") || raw.startsWith("https://")) {
      return new URL(raw).pathname;
    }
    return new URL(raw, "http://localhost").pathname;
  } catch {
    return raw.split("?")[0] || "";
  }
}

function isUnauthenticatedCronRoute(req: NextApiRequest): boolean {
  return UNAUTHENTICATED_CRON_ROUTES.has(requestPath(req));
}

export function checkAdminAccessHeader(req: NextApiRequest): AdminAccessResult {
  const expected = process.env.ADMIN_ACCESS_SECRET;
  if (!expected) {
    return { ok: true, required: false };
  }
  if (isUnauthenticatedCronRoute(req)) {
    return { ok: true, required: false };
  }

  const explicitHeader = firstHeaderValue(req.headers["x-admin-access-secret"]);
  const bearerHeader = parseBearerToken(
    firstHeaderValue(req.headers.authorization),
  );
  const provided = explicitHeader || bearerHeader;
  return { ok: provided === expected, required: true };
}

export function requireAdminAccess(
  req: NextApiRequest,
  res: NextApiResponse,
): boolean {
  const result = checkAdminAccessHeader(req);
  if (result.required && !result.ok) {
    res.status(401).json({ error: "Unauthorized" });
    return false;
  }
  return true;
}
