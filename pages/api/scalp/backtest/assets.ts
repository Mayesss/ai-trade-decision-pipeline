import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { bitgetFetch, resolveProductType } from "../../../../lib/bitget";
import { inferScalpAssetCategory } from "../../../../lib/scalp/symbolInfo";

type AssetRow = {
  symbol: string;
  epic: string;
  category: "forex" | "crypto" | "index" | "commodity" | "equity" | "other";
};

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
  try {
    const contracts = await bitgetFetch("GET", "/api/v2/mix/market/contracts", {
      productType: String(resolveProductType() || "usdt-futures")
        .trim()
        .toUpperCase(),
    });
    const rows = Array.isArray(contracts) ? contracts : [];
    const assets: AssetRow[] = rows
      .map((row) => {
        const normalizedSymbol = String((row as any)?.symbol || "")
          .trim()
          .toUpperCase();
        if (!normalizedSymbol) return null;
        return {
          symbol: normalizedSymbol,
          epic: normalizedSymbol,
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
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_assets_fetch_failed",
      message: err?.message || String(err),
    });
  }
}
