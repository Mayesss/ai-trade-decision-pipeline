import { Prisma } from "@prisma/client";

import { fetchSymbolMeta } from "../analytics";
import { resolveProductType } from "../bitget";
import { fetchCapitalSymbolMarketMetadata } from "../capital";

import { isScalpPgConfigured, scalpPrisma } from "./pg/client";
import type { ScalpSymbolMarketMetadata } from "./symbolMarketMetadata";
import {
  buildHeuristicScalpSymbolMarketMetadata,
  normalizeScalpSymbolMarketMetadata,
} from "./symbolMarketMetadata";
import { inferScalpAssetCategory } from "./symbolInfo";
import {
  loadScalpSymbolMarketMetadata,
  saveScalpSymbolMarketMetadata,
} from "./symbolMarketMetadataStore";
import { normalizeScalpVenue, type ScalpVenue } from "./venue";

const DEFAULT_SYMBOL_MARKET_METADATA_MAX_AGE_MS = 7 * 24 * 60 * 60_000;

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function toFinite(value: unknown): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : NaN;
}

function toPositive(value: unknown): number | null {
  const n = toFinite(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function decimalPipSize(decimalPlacesFactor: number | null): number | null {
  if (decimalPlacesFactor === null) return null;
  const places = Math.max(0, Math.floor(decimalPlacesFactor));
  const pip = 10 ** -places;
  return Number.isFinite(pip) && pip > 0 ? pip : null;
}

function resolveBitgetTickSize(
  meta: Record<string, unknown>,
  pricePlace: number | null,
): number | null {
  const decimalStep = decimalPipSize(pricePlace);
  const endStep = toPositive(meta.priceEndStep);
  if (endStep !== null && decimalStep !== null) {
    return endStep * decimalStep;
  }
  const priceStep = toPositive(meta.priceStep);
  if (
    priceStep !== null &&
    decimalStep !== null &&
    Number.isInteger(priceStep) &&
    priceStep >= 1
  ) {
    return priceStep * decimalStep;
  }
  return priceStep ?? decimalStep ?? endStep;
}

function resolveBitgetAssetCategory(symbol: string) {
  const inferred = inferScalpAssetCategory(symbol);
  if (inferred === "equity" || inferred === "other") return "crypto";
  return inferred;
}

async function resolvePreferredVenue(
  symbol: string,
  fallbackVenue: ScalpVenue,
): Promise<ScalpVenue> {
  if (!symbol || !isScalpPgConfigured()) return fallbackVenue;
  const db = scalpPrisma();
  try {
    const rows = await db.$queryRaw<
      Array<{ bitgetCount: bigint | number; capitalCount: bigint | number }>
    >(Prisma.sql`
        SELECT
          COALESCE(SUM(CASE WHEN deployment_id LIKE 'bitget:%' THEN 1 ELSE 0 END), 0)::bigint AS "bitgetCount",
          COALESCE(SUM(CASE WHEN deployment_id NOT LIKE 'bitget:%' THEN 1 ELSE 0 END), 0)::bigint AS "capitalCount"
        FROM scalp_deployments
        WHERE symbol = ${symbol};
      `);
    const bitgetCount = Math.max(0, Number(rows[0]?.bitgetCount || 0));
    const capitalCount = Math.max(0, Number(rows[0]?.capitalCount || 0));
    if (bitgetCount > 0 && capitalCount === 0) return "bitget";
    if (capitalCount > 0 && bitgetCount === 0) return "capital";
  } catch {
    return fallbackVenue;
  }
  return fallbackVenue;
}

async function fetchBitgetSymbolMarketMetadata(
  symbol: string,
): Promise<ScalpSymbolMarketMetadata> {
  const nowMs = Date.now();
  const meta = await fetchSymbolMeta(symbol, resolveProductType());
  const pricePlace = Number.isFinite(toFinite((meta as any).pricePlace))
    ? Math.max(0, Math.floor(toFinite((meta as any).pricePlace)))
    : null;
  const volumePlace = Number.isFinite(toFinite((meta as any).volumePlace))
    ? Math.max(0, Math.floor(toFinite((meta as any).volumePlace)))
    : null;
  const tickSize = resolveBitgetTickSize(meta as Record<string, unknown>, pricePlace);
  const pipSize = tickSize ?? decimalPipSize(pricePlace);
  const instrumentType = String(
    (meta as any).symbolType || (meta as any).symbolTypeName || "PERPETUAL",
  )
    .trim()
    .toUpperCase();
  const base = buildHeuristicScalpSymbolMarketMetadata(symbol, {
    epic: symbol,
    source: "bitget",
    fetchedAtMs: nowMs,
  });
  return normalizeScalpSymbolMarketMetadata({
    ...base,
    symbol,
    epic: symbol,
    source: "bitget",
    assetCategory: resolveBitgetAssetCategory(symbol),
    instrumentType,
    marketStatus: "TRADEABLE",
    pipSize: pipSize ?? base.pipSize,
    pipPosition:
      pricePlace !== null ? Math.max(0, pricePlace - 1) : base.pipPosition,
    tickSize,
    decimalPlacesFactor: pricePlace,
    scalingFactor: null,
    minDealSize: toPositive((meta as any).minTradeNum),
    sizeDecimals: volumePlace,
    fetchedAtMs: nowMs,
  });
}

export function resolveScalpSymbolMarketMetadataMaxAgeMs(): number {
  const raw = Number(process.env.SCALP_SYMBOL_MARKET_METADATA_MAX_AGE_MS);
  if (!Number.isFinite(raw) || raw <= 0) {
    return DEFAULT_SYMBOL_MARKET_METADATA_MAX_AGE_MS;
  }
  return Math.max(60_000, Math.floor(raw));
}

export function isScalpSymbolMarketMetadataStale(
  metadata: ScalpSymbolMarketMetadata | null | undefined,
  nowMs = Date.now(),
  maxAgeMs = resolveScalpSymbolMarketMetadataMaxAgeMs(),
): boolean {
  if (!metadata) return true;
  const fetchedAtMs = Math.floor(Number(metadata.fetchedAtMs) || 0);
  if (!(Number.isFinite(fetchedAtMs) && fetchedAtMs > 0)) return true;
  return nowMs - fetchedAtMs > Math.max(60_000, Math.floor(maxAgeMs));
}

export async function ensureScalpSymbolMarketMetadata(
  symbolRaw: string,
  opts: {
    forceRefresh?: boolean;
    fetchIfMissing?: boolean;
    maxAgeMs?: number;
    venue?: ScalpVenue;
  } = {},
): Promise<ScalpSymbolMarketMetadata | null> {
  const symbol = normalizeSymbol(symbolRaw);
  if (!symbol) return null;
  const nowMs = Date.now();
  const fetchIfMissing = opts.fetchIfMissing !== false;
  const maxAgeMs = Math.max(
    60_000,
    Math.floor(opts.maxAgeMs ?? resolveScalpSymbolMarketMetadataMaxAgeMs()),
  );

  const stored = await loadScalpSymbolMarketMetadata(symbol);
  const storedVenue =
    stored?.source === "bitget"
      ? "bitget"
      : stored?.source === "capital"
        ? "capital"
        : null;
  const requestedVenue = normalizeScalpVenue(
    opts.venue ?? storedVenue,
    "capital",
  );
  const preferredVenue = await resolvePreferredVenue(symbol, requestedVenue);
  const venueMismatch =
    preferredVenue === "bitget" &&
    stored !== null &&
    stored?.source !== "bitget";
  const stale = isScalpSymbolMarketMetadataStale(stored, nowMs, maxAgeMs);
  if (
    !opts.forceRefresh &&
    stored &&
    !stale &&
    !venueMismatch
  ) {
    return stored;
  }
  if (!fetchIfMissing && !opts.forceRefresh && !venueMismatch) {
    return stored;
  }

  try {
    const fetched =
      preferredVenue === "bitget"
        ? await fetchBitgetSymbolMarketMetadata(symbol)
        : await fetchCapitalSymbolMarketMetadata(symbol);
    await saveScalpSymbolMarketMetadata(fetched);
    return fetched;
  } catch {
    return stored ?? null;
  }
}
