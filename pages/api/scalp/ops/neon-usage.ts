export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../lib/scalp-v2/http";

const NEON_API_BASE = "https://console.neon.tech/api/v2";
const DEFAULT_TRANSFER_ALLOWANCE_GB = 100;

type NeonMetricName = "public_network_transfer_bytes" | "private_network_transfer_bytes";

type HourlyUsageRow = {
  timeframeStart: string | null;
  timeframeEnd: string | null;
  publicNetworkTransferBytes: number;
  privateNetworkTransferBytes: number;
  totalNetworkTransferBytes: number;
};

function envString(name: string): string {
  return String(process.env[name] || "").trim();
}

function toFiniteNumber(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function resolveAllowanceBytes(): number {
  const gb = toFiniteNumber(process.env.NEON_PUBLIC_TRANSFER_ALLOWANCE_GB, DEFAULT_TRANSFER_ALLOWANCE_GB);
  return Math.max(1, gb) * 1024 ** 3;
}

async function neonFetch<T>(path: string, apiKey: string): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10_000);
  try {
    const res = await fetch(`${NEON_API_BASE}${path}`, {
      method: "GET",
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      signal: controller.signal,
    });
    const text = await res.text();
    let body: unknown = null;
    if (text) {
      try {
        body = JSON.parse(text);
      } catch {
        body = { message: text.slice(0, 240) };
      }
    }
    if (!res.ok) {
      const message =
        body && typeof body === "object"
          ? String((body as Record<string, unknown>).message || (body as Record<string, unknown>).error || `HTTP ${res.status}`)
          : `HTTP ${res.status}`;
      throw new Error(message);
    }
    return body as T;
  } finally {
    clearTimeout(timeout);
  }
}

function extractMetricValue(metrics: unknown, name: NeonMetricName): number {
  if (!Array.isArray(metrics)) return 0;
  const row = metrics.find((item) => {
    if (!item || typeof item !== "object") return false;
    return String((item as Record<string, unknown>).metric_name || "") === name;
  }) as Record<string, unknown> | undefined;
  return toFiniteNumber(row?.value, 0);
}

function collectHourlyRows(value: unknown, out: HourlyUsageRow[] = []): HourlyUsageRow[] {
  if (!value || typeof value !== "object") return out;
  if (Array.isArray(value)) {
    for (const item of value) collectHourlyRows(item, out);
    return out;
  }
  const row = value as Record<string, unknown>;
  if (Array.isArray(row.metrics)) {
    const publicBytes = extractMetricValue(row.metrics, "public_network_transfer_bytes");
    const privateBytes = extractMetricValue(row.metrics, "private_network_transfer_bytes");
    if (publicBytes > 0 || privateBytes > 0 || row.timeframe_start || row.timeframe_end) {
      out.push({
        timeframeStart: row.timeframe_start ? String(row.timeframe_start) : null,
        timeframeEnd: row.timeframe_end ? String(row.timeframe_end) : null,
        publicNetworkTransferBytes: publicBytes,
        privateNetworkTransferBytes: privateBytes,
        totalNetworkTransferBytes: publicBytes + privateBytes,
      });
    }
  }
  for (const item of Object.values(row)) collectHourlyRows(item, out);
  return out;
}

function compactHourlyRows(rows: HourlyUsageRow[]): HourlyUsageRow[] {
  const byKey = new Map<string, HourlyUsageRow>();
  for (const row of rows) {
    const key = row.timeframeStart || row.timeframeEnd || `idx:${byKey.size}`;
    const prev = byKey.get(key);
    if (!prev) {
      byKey.set(key, { ...row });
      continue;
    }
    prev.publicNetworkTransferBytes += row.publicNetworkTransferBytes;
    prev.privateNetworkTransferBytes += row.privateNetworkTransferBytes;
    prev.totalNetworkTransferBytes += row.totalNetworkTransferBytes;
  }
  return Array.from(byKey.values()).sort((a, b) =>
    String(a.timeframeStart || a.timeframeEnd || "").localeCompare(String(b.timeframeStart || b.timeframeEnd || "")),
  );
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    return res.status(405).json({ ok: false, error: "Method Not Allowed", message: "Use GET" });
  }
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  const apiKey = envString("NEON_API_KEY");
  const projectId = envString("NEON_PROJECT_ID");
  const branchId = envString("NEON_BRANCH_ID");
  const orgId = envString("NEON_ORG_ID");
  const allowanceBytes = resolveAllowanceBytes();
  const now = new Date();
  const from = new Date(now.getTime() - 24 * 60 * 60_000);

  if (!apiKey || !projectId) {
    return res.status(200).json({
      ok: true,
      configured: false,
      generatedAtMs: Date.now(),
      requiredEnv: {
        NEON_API_KEY: Boolean(apiKey),
        NEON_PROJECT_ID: Boolean(projectId),
        NEON_ORG_ID: Boolean(orgId),
      },
      message: "Set NEON_API_KEY and NEON_PROJECT_ID to enable Neon usage monitoring.",
    });
  }

  try {
    const projectPromise = neonFetch<{ project?: Record<string, unknown> }>(`/projects/${encodeURIComponent(projectId)}`, apiKey);
    const branchPromise = branchId
      ? neonFetch<{ branch?: Record<string, unknown> }>(
          `/projects/${encodeURIComponent(projectId)}/branches/${encodeURIComponent(branchId)}`,
          apiKey,
        ).catch((err) => ({ error: err instanceof Error ? err.message : String(err) }))
      : Promise.resolve(null);
    const consumptionPromise = orgId
      ? neonFetch<unknown>(
          `/consumption_history/v2/projects?org_id=${encodeURIComponent(orgId)}&from=${encodeURIComponent(
            from.toISOString(),
          )}&to=${encodeURIComponent(now.toISOString())}&granularity=hourly&project_ids=${encodeURIComponent(
            projectId,
          )}&metrics=public_network_transfer_bytes,private_network_transfer_bytes`,
          apiKey,
        ).catch((err) => ({ error: err instanceof Error ? err.message : String(err) }))
      : Promise.resolve(null);

    const [projectBody, branchBody, consumptionBody] = await Promise.all([
      projectPromise,
      branchPromise,
      consumptionPromise,
    ]);

    const project = projectBody.project || {};
    const projectTransferBytes = Math.max(0, toFiniteNumber(project.data_transfer_bytes, 0));
    const branch =
      branchBody && "branch" in branchBody && branchBody.branch && typeof branchBody.branch === "object"
        ? (branchBody.branch as Record<string, unknown>)
        : null;
    const branchTransferBytes = branch ? Math.max(0, toFiniteNumber(branch.data_transfer_bytes, 0)) : null;
    const hourlyRows =
      consumptionBody && typeof consumptionBody === "object" && "error" in consumptionBody
        ? []
        : compactHourlyRows(collectHourlyRows(consumptionBody));
    const public24hBytes = hourlyRows.reduce((acc, row) => acc + row.publicNetworkTransferBytes, 0);
    const private24hBytes = hourlyRows.reduce((acc, row) => acc + row.privateNetworkTransferBytes, 0);

    return res.status(200).json({
      ok: true,
      configured: true,
      generatedAtMs: Date.now(),
      project: {
        id: String(project.id || projectId),
        name: project.name ? String(project.name) : null,
        dataTransferBytes: projectTransferBytes,
        allowanceBytes,
        allowanceGb: allowanceBytes / 1024 ** 3,
        allowanceUsedPct: allowanceBytes > 0 ? (projectTransferBytes / allowanceBytes) * 100 : null,
      },
      branch: branchId
        ? {
            id: String(branch?.id || branchId),
            name: branch?.name ? String(branch.name) : null,
            dataTransferBytes: branchTransferBytes,
          }
        : null,
      consumption24h: {
        available: Boolean(orgId) && !(consumptionBody && typeof consumptionBody === "object" && "error" in consumptionBody),
        error:
          consumptionBody && typeof consumptionBody === "object" && "error" in consumptionBody
            ? String((consumptionBody as Record<string, unknown>).error || "consumption_unavailable")
            : null,
        from: from.toISOString(),
        to: now.toISOString(),
        granularity: "hourly",
        publicNetworkTransferBytes: public24hBytes,
        privateNetworkTransferBytes: private24hBytes,
        totalNetworkTransferBytes: public24hBytes + private24hBytes,
        hourly: hourlyRows.slice(-24),
      },
    });
  } catch (err) {
    return res.status(502).json({
      ok: false,
      error: "neon_usage_fetch_failed",
      message: err instanceof Error ? err.message : String(err),
    });
  }
}
