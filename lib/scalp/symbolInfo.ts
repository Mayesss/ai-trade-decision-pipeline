export type ScalpAssetCategory =
  | "forex"
  | "crypto"
  | "commodity"
  | "index"
  | "equity"
  | "other";

type ScalpPipMetadataLike = {
  pipSize?: unknown;
} | null;

const FX_CODES = new Set([
  "USD",
  "EUR",
  "GBP",
  "JPY",
  "CHF",
  "CAD",
  "AUD",
  "NZD",
]);
const CRYPTO_CODES = new Set([
  "BTC",
  "ETH",
  "SOL",
  "XRP",
  "ADA",
  "DOGE",
  "AVAX",
  "MATIC",
  "DOT",
  "LTC",
]);
const INDEX_TOKENS = [
  "US500",
  "NAS100",
  "US30",
  "GER40",
  "UK100",
  "FRA40",
  "JPN225",
  "QQQ",
];
const METAL_SYMBOLS = new Set([
  "XAUUSD",
  "XAUUSDT",
  "XAGUSD",
  "XAGUSDT",
  "XPTUSD",
  "XPTUSDT",
  "XPDUSD",
  "XPDUSDT",
  "GOLD",
  "SILVER",
]);
const COMMODITY_SYMBOLS = new Set(["USOIL", "UKOIL", "NGAS"]);

function resolvePipSizeOverride(symbol: string): number | null {
  const raw = String(process.env.SCALP_SYMBOL_PIP_SIZE_MAP || "").trim();
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
      return null;
    const value = Number(parsed[symbol]);
    return Number.isFinite(value) && value > 0 ? value : null;
  } catch {
    return null;
  }
}

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function stripQuoteSuffix(symbol: string): string {
  for (const suffix of ["USDT", "USDC", "USD", "EUR", "GBP", "JPY"]) {
    if (symbol.endsWith(suffix) && symbol.length > suffix.length) {
      return symbol.slice(0, symbol.length - suffix.length);
    }
  }
  return symbol;
}

function isFxSymbol(symbol: string): boolean {
  if (!/^[A-Z]{6}$/.test(symbol)) return false;
  const lhs = symbol.slice(0, 3);
  const rhs = symbol.slice(3, 6);
  return FX_CODES.has(lhs) && FX_CODES.has(rhs) && !symbol.endsWith("USDT");
}

export function isPreciousMetalFamilySymbol(symbol: string): boolean {
  const normalized = normalizeSymbol(symbol);
  if (!normalized) return false;
  if (METAL_SYMBOLS.has(normalized)) return true;
  return (
    normalized.startsWith("XAU") ||
    normalized.startsWith("XAG") ||
    normalized.startsWith("XPT") ||
    normalized.startsWith("XPD")
  );
}

export function inferScalpAssetCategory(symbol: string): ScalpAssetCategory {
  const normalized = normalizeSymbol(symbol);
  if (!normalized) return "other";
  if (isFxSymbol(normalized)) return "forex";

  const base = stripQuoteSuffix(normalized);
  if (CRYPTO_CODES.has(base)) return "crypto";
  if (
    INDEX_TOKENS.some(
      (token) => normalized.startsWith(token) || normalized.includes(token),
    )
  )
    return "index";
  if (
    isPreciousMetalFamilySymbol(normalized) ||
    COMMODITY_SYMBOLS.has(normalized)
  )
    return "commodity";
  if (normalized.endsWith("USDT")) return "equity";
  return "other";
}

export function isWeekendClosedScalpSymbol(symbol: string): boolean {
  return inferScalpAssetCategory(symbol) !== "crypto";
}

export function pipSizeForScalpSymbol(
  symbol: string,
  metadata?: ScalpPipMetadataLike,
): number {
  const normalized = normalizeSymbol(symbol);
  if (!normalized) return 0.0001;
  const metadataPipSize = Number(metadata?.pipSize);
  if (Number.isFinite(metadataPipSize) && metadataPipSize > 0) {
    return metadataPipSize;
  }
  const override = resolvePipSizeOverride(normalized);
  if (override !== null) return override;
  if (isPreciousMetalFamilySymbol(normalized)) return 0.01;
  if (isFxSymbol(normalized)) return normalized.includes("JPY") ? 0.01 : 0.0001;
  return 0.0001;
}
