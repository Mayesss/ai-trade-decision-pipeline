import type { NextApiRequest, NextApiResponse } from "next";

export type AdminAccessResult = { ok: boolean; required: boolean };
const UNAUTHENTICATED_CRON_ROUTES = new Set<string>([
  "/api/swing/analyze",
  "/api/scalp/cron/execute-deployments",
  "/api/scalp/cron/discover-symbols",
  "/api/scalp/cron/load-candles",
  "/api/scalp/cron/prepare",
  "/api/scalp/cron/v2/discover",
  "/api/scalp/cron/v2/load-candles",
  "/api/scalp/cron/v2/prepare",
  "/api/scalp/cron/worker",
  "/api/scalp/cron/promotion",
  "/api/scalp/cron/live-guardrail-monitor",
  "/api/scalp/cron/housekeeping",
  "/api/scalp/v2/cron/discover",
  "/api/scalp/v2/cron/load-candles",
  "/api/scalp/v2/cron/prepare",
  "/api/scalp/v2/cron/evaluate",
  "/api/scalp/v2/cron/promote",
  "/api/scalp/v2/cron/execute",
  "/api/scalp/v2/cron/reconcile",
  "/api/scalp/v2/cron/cycle",
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
