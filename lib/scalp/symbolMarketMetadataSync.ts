import { fetchCapitalSymbolMarketMetadata } from "../capital";

import type { ScalpSymbolMarketMetadata } from "./symbolMarketMetadata";
import {
  loadScalpSymbolMarketMetadata,
  saveScalpSymbolMarketMetadata,
} from "./symbolMarketMetadataStore";

const DEFAULT_SYMBOL_MARKET_METADATA_MAX_AGE_MS = 7 * 24 * 60 * 60_000;

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
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
  if (
    !opts.forceRefresh &&
    stored &&
    !isScalpSymbolMarketMetadataStale(stored, nowMs, maxAgeMs)
  ) {
    return stored;
  }
  if (!fetchIfMissing && !opts.forceRefresh) {
    return stored;
  }

  try {
    const fetched = await fetchCapitalSymbolMarketMetadata(symbol);
    await saveScalpSymbolMarketMetadata(fetched);
    return fetched;
  } catch {
    return stored ?? null;
  }
}
