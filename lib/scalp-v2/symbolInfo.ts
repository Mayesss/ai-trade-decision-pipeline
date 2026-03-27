export type ScalpV2AssetCategory =
  | "forex"
  | "crypto"
  | "commodity"
  | "index"
  | "equity"
  | "other";

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

export function isScalpV2PreciousMetalFamilySymbol(symbol: string): boolean {
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

export function inferScalpV2AssetCategory(symbol: string): ScalpV2AssetCategory {
  const normalized = normalizeSymbol(symbol);
  if (!normalized) return "other";
  if (isFxSymbol(normalized)) return "forex";

  const base = stripQuoteSuffix(normalized);
  if (CRYPTO_CODES.has(base)) return "crypto";
  if (
    INDEX_TOKENS.some(
      (token) => normalized.startsWith(token) || normalized.includes(token),
    )
  ) {
    return "index";
  }
  if (
    isScalpV2PreciousMetalFamilySymbol(normalized) ||
    COMMODITY_SYMBOLS.has(normalized)
  ) {
    return "commodity";
  }
  if (normalized.endsWith("USDT")) return "equity";
  return "other";
}
