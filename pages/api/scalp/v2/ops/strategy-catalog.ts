export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../../lib/scalp/composer/http";
import {
  getDefaultScalpStrategy,
  listScalpStrategies,
} from "../../../../../lib/scalp/strategies/registry";

const RETIRED_CATALOG_STRATEGY_IDS = new Set([
  "model_guided_composer_v2",
  "day_model_guided_composer_v1",
]);

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
    const defaultStrategy = getDefaultScalpStrategy();
    const strategies = listScalpStrategies()
      .filter((strategy) => !RETIRED_CATALOG_STRATEGY_IDS.has(strategy.id))
      .map((strategy) => ({
        strategyId: strategy.id,
        shortName: strategy.shortName || strategy.id,
        longName: strategy.longName || strategy.shortName || strategy.id,
        enabled: true,
      }))
      .sort((a, b) => a.shortName.localeCompare(b.shortName));

    return res.status(200).json({
      ok: true,
      mode: "scalp_v2",
      strategyId: defaultStrategy.id,
      defaultStrategyId: defaultStrategy.id,
      strategies,
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_v2_strategy_catalog_failed",
      message: err?.message || String(err),
    });
  }
}
