import type { NextApiRequest, NextApiResponse } from "next";

import defaultTickerEpicMap from "../../../../data/capitalTickerMap.json";
import { requireAdminAccess } from "../../../../lib/admin";
import { inferScalpAssetCategory } from "../../../../lib/scalp/symbolInfo";

type AssetRow = {
  symbol: string;
  epic: string;
  category: "forex" | "crypto" | "index" | "commodity" | "equity" | "other";
};

function parseEnvTickerMap(): Record<string, string> {
  const raw = process.env.CAPITAL_TICKER_EPIC_MAP;
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
      return {};
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
      if (typeof v !== "string") continue;
      const symbol = String(k || "")
        .trim()
        .toUpperCase();
      const epic = String(v || "")
        .trim()
        .toUpperCase();
      if (!symbol || !epic) continue;
      out[symbol] = epic;
    }
    return out;
  } catch {
    return {};
  }
}

function categoryRank(category: AssetRow["category"]): number {
  if (category === "forex") return 1;
  if (category === "index") return 2;
  if (category === "commodity") return 3;
  if (category === "equity") return 4;
  if (category === "crypto") return 5;
  return 6;
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

  const defaultMap = defaultTickerEpicMap as Record<string, string>;
  const envMap = parseEnvTickerMap();
  const merged = { ...defaultMap, ...envMap };

  const assets: AssetRow[] = Object.entries(merged)
    .map(([symbol, epic]) => {
      const normalizedSymbol = String(symbol || "")
        .trim()
        .toUpperCase();
      const normalizedEpic = String(epic || "")
        .trim()
        .toUpperCase();
      if (!normalizedSymbol || !normalizedEpic) return null;
      return {
        symbol: normalizedSymbol,
        epic: normalizedEpic,
        category: inferScalpAssetCategory(normalizedSymbol),
      } satisfies AssetRow;
    })
    .filter((row): row is AssetRow => Boolean(row))
    .sort((a, b) => {
      const byCategory = categoryRank(a.category) - categoryRank(b.category);
      if (byCategory !== 0) return byCategory;
      return a.symbol.localeCompare(b.symbol);
    });

  return res.status(200).json({
    count: assets.length,
    assets,
  });
}
