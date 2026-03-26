export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import {
  loadScalpV2RuntimeConfig,
  upsertScalpV2RuntimeConfig,
} from "../../../../lib/scalp-v2/db";
import { setNoStoreHeaders } from "../../../../lib/scalp-v2/http";
import type { ScalpV2RuntimeConfig } from "../../../../lib/scalp-v2/types";

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function mergeRuntimeConfig(
  base: ScalpV2RuntimeConfig,
  patch: Record<string, unknown>,
): ScalpV2RuntimeConfig {
  const merged = {
    ...base,
    ...patch,
  } as ScalpV2RuntimeConfig;

  merged.budgets = {
    ...base.budgets,
    ...asRecord(patch.budgets),
  };
  merged.riskProfile = {
    ...base.riskProfile,
    ...asRecord(patch.riskProfile),
  };

  const seedSymbolsByVenue = asRecord(patch.seedSymbolsByVenue);
  const seedLiveSymbolsByVenue = asRecord(patch.seedLiveSymbolsByVenue);

  merged.seedSymbolsByVenue = {
    ...base.seedSymbolsByVenue,
    ...seedSymbolsByVenue,
  } as ScalpV2RuntimeConfig["seedSymbolsByVenue"];
  merged.seedLiveSymbolsByVenue = {
    ...base.seedLiveSymbolsByVenue,
    ...seedLiveSymbolsByVenue,
  } as ScalpV2RuntimeConfig["seedLiveSymbolsByVenue"];

  return merged;
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    if (req.method === "GET") {
      const runtime = await loadScalpV2RuntimeConfig();
      return res.status(200).json({
        ok: true,
        mode: "scalp_v2",
        runtime,
      });
    }

    if (req.method === "POST") {
      const current = await loadScalpV2RuntimeConfig();
      const patch = asRecord(req.body);
      const next = mergeRuntimeConfig(current, patch);
      const runtime = await upsertScalpV2RuntimeConfig(next);
      return res.status(200).json({
        ok: true,
        mode: "scalp_v2",
        runtime,
      });
    }

    return res
      .status(405)
      .json({ error: "Method Not Allowed", message: "Use GET or POST" });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_v2_control_failed",
      message: err?.message || String(err),
    });
  }
}
