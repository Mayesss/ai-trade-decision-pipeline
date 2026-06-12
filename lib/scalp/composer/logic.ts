import type {
  ScalpComposerBudgetConfig,
  ScalpComposerCandidate,
  ScalpComposerCloseType,
  ScalpComposerDeployment,
  ScalpComposerEventType,
  ScalpComposerRiskProfile,
  ScalpComposerVenue,
} from "./types";
import {
  inferScalpComposerAssetCategory,
  isScalpComposerPreciousMetalFamilySymbol,
} from "./symbolInfo";

const STOCK_OR_INDEX_BASES = new Set([
  "AAPL",
  "AMZN",
  "GOOG",
  "GOOGL",
  "META",
  "MSFT",
  "NVDA",
  "TSLA",
  "SPY",
  "QQQ",
  "US500",
  "NAS100",
  "US30",
  "GER40",
  "UK100",
  "FRA40",
  "JPN225",
  "HK50",
  "DAX",
]);

const FIAT_BASES = new Set([
  "USD",
  "EUR",
  "GBP",
  "JPY",
  "CHF",
  "CAD",
  "AUD",
  "NZD",
  "CNY",
  "HKD",
  "SGD",
]);

const CRYPTO_BASE_HINTS = new Set([
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
  "BNB",
  "LINK",
  "ATOM",
  "UNI",
  "TRX",
  "BCH",
  "XLM",
  "ETC",
  "EOS",
  "NEAR",
  "SUI",
  "APT",
  "ARB",
  "OP",
  "SHIB",
  "PEPE",
  "FIL",
  "ALGO",
  "AAVE",
  "MKR",
]);

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function stripKnownQuoteSuffix(symbol: string): string {
  for (const suffix of ["USDT", "USDC", "BUSD", "USD", "EUR", "GBP", "JPY"]) {
    if (symbol.endsWith(suffix) && symbol.length > suffix.length) {
      return symbol.slice(0, symbol.length - suffix.length);
    }
  }
  return symbol;
}

function isLikelyCryptoSymbol(symbolRaw: string): boolean {
  const symbol = normalizeSymbol(symbolRaw);
  if (!symbol) return false;
  if (symbol.endsWith("USDT") || symbol.endsWith("USDC") || symbol.endsWith("BUSD")) {
    return true;
  }
  if (inferScalpComposerAssetCategory(symbol) === "crypto") return true;

  const base = stripKnownQuoteSuffix(symbol);
  if (!base) return false;
  return CRYPTO_BASE_HINTS.has(base);
}

function isBitgetCryptoOnlyAllowed(symbolRaw: string): boolean {
  const symbol = normalizeSymbol(symbolRaw);
  if (!symbol) return false;
  if (!symbol.endsWith("USDT")) return false;
  if (isScalpComposerPreciousMetalFamilySymbol(symbol)) return false;
  if (symbol.includes("OIL") || symbol.includes("GAS")) return false;

  const base = stripKnownQuoteSuffix(symbol);
  if (!base) return false;
  if (FIAT_BASES.has(base)) return false;
  if (STOCK_OR_INDEX_BASES.has(base)) return false;
  if (base.includes("ETF")) return false;
  return true;
}

function isCapitalNonCryptoAllowed(symbolRaw: string): boolean {
  const symbol = normalizeSymbol(symbolRaw);
  if (!symbol) return false;
  return !isLikelyCryptoSymbol(symbol);
}

export function isScalpComposerDiscoverSymbolAllowed(
  venue: ScalpComposerVenue,
  symbolRaw: string,
): boolean {
  if (venue === "bitget") return isBitgetCryptoOnlyAllowed(symbolRaw);
  if (venue === "capital") return isCapitalNonCryptoAllowed(symbolRaw);
  return false;
}

export function normalizeReasonCodes(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  const out: string[] = [];
  for (const item of value) {
    const code = String(item || "")
      .trim()
      .toUpperCase();
    if (!code) continue;
    out.push(code.slice(0, 120));
    if (out.length >= 64) break;
  }
  return Array.from(new Set(out));
}

export function deriveCloseTypeFromReasonCodes(
  reasonCodes: string[],
): ScalpComposerCloseType {
  const codes = normalizeReasonCodes(reasonCodes);
  if (codes.some((code) => code.includes("LIQUID"))) return "liquidation";
  if (codes.some((code) => code.includes("TRAIL_STOP"))) {
    return "trailing_stop";
  }
  if (codes.some((code) => code.includes("TIME_STOP"))) {
    return "manual_close";
  }
  if (codes.some((code) => code.includes("STOP") || code.includes("SL_"))) {
    return "stop_loss";
  }
  if (codes.some((code) => code.includes("RECONCILE"))) {
    return "reconcile_close";
  }
  return "manual_close";
}

export function toLedgerCloseTypeFromEvent(
  eventType: ScalpComposerEventType,
  reasonCodes: string[],
): ScalpComposerCloseType | null {
  if (eventType === "fill") return "fill";
  if (eventType === "stop_loss") return "stop_loss";
  if (eventType === "liquidation") return "liquidation";
  if (eventType === "manual_close") return "manual_close";
  if (eventType === "reconcile_close") return "reconcile_close";
  if (eventType === "order_submitted" || eventType === "order_rejected") {
    return null;
  }
  if (eventType === "position_snapshot") {
    return null;
  }
  return deriveCloseTypeFromReasonCodes(reasonCodes);
}

export function toDeploymentId(params: {
  venue: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  session: string;
}): string {
  const venue = String(params.venue || "bitget")
    .trim()
    .toLowerCase();
  const symbol = String(params.symbol || "")
    .trim()
    .toUpperCase();
  const strategyId = String(params.strategyId || "")
    .trim()
    .toLowerCase();
  const tuneId = String(params.tuneId || "default")
    .trim()
    .toLowerCase();
  const session = String(params.session || "berlin")
    .trim()
    .toLowerCase();
  return `${venue}:${symbol}~${strategyId}~${tuneId}__sp_${session}`;
}

/**
 * Compute an effective score for budget ranking that blends in actual
 * backtest evidence when available. Candidates with a passing stage C
 * get a significant boost; candidates with positive netR get a smaller
 * boost proportional to their backtest performance.
 */
function effectiveBudgetScore(candidate: ScalpComposerCandidate): number {
  const base = candidate.score;
  const worker = candidate.metadata?.worker as Record<string, unknown> | undefined;
  if (!worker) return base;
  const stageC = worker.stageC as Record<string, unknown> | undefined;
  if (!stageC) return base;
  const passed = stageC.passed === true;
  const netR = Number(stageC.netR);
  if (passed) {
    // Stage C pass: large boost (100 points) + netR contribution
    return base + 100 + (Number.isFinite(netR) ? Math.max(0, netR) * 5 : 0);
  }
  if (Number.isFinite(netR) && netR > 0) {
    // Positive netR but didn't fully pass: moderate boost
    return base + netR * 3;
  }
  return base;
}

export function enforceCandidateBudgets(params: {
  candidates: ScalpComposerCandidate[];
  budgets: ScalpComposerBudgetConfig;
}): {
  kept: ScalpComposerCandidate[];
  dropped: ScalpComposerCandidate[];
} {
  const { candidates, budgets } = params;
  const sorted = candidates
    .slice()
    .sort((a, b) => effectiveBudgetScore(b) - effectiveBudgetScore(a) || a.updatedAtMs - b.updatedAtMs);

  const kept: ScalpComposerCandidate[] = [];
  const dropped: ScalpComposerCandidate[] = [];
  const symbolCounts = new Map<string, number>();

  for (const candidate of sorted) {
    if (kept.length >= budgets.maxCandidatesTotal) {
      dropped.push(candidate);
      continue;
    }
    const symbolKey = `${candidate.venue}:${candidate.symbol}`;
    const symbolCount = symbolCounts.get(symbolKey) || 0;
    if (symbolCount >= budgets.maxCandidatesPerSymbol) {
      dropped.push(candidate);
      continue;
    }
    kept.push(candidate);
    symbolCounts.set(symbolKey, symbolCount + 1);
  }

  return { kept, dropped };
}

export function enforceDeploymentBudget(params: {
  deployments: ScalpComposerDeployment[];
  budgets: ScalpComposerBudgetConfig;
}): {
  live: ScalpComposerDeployment[];
  shadow: ScalpComposerDeployment[];
} {
  const sorted = params.deployments
    .slice()
    .sort((a, b) => b.updatedAtMs - a.updatedAtMs);

  const live: ScalpComposerDeployment[] = [];
  const shadow: ScalpComposerDeployment[] = [];
  for (const deployment of sorted) {
    if (deployment.enabled && live.length < params.budgets.maxEnabledDeployments) {
      live.push(deployment);
      continue;
    }
    shadow.push({ ...deployment, enabled: false, liveMode: "shadow" });
  }
  return { live, shadow };
}

export function defaultRiskProfile(): ScalpComposerRiskProfile {
  return {
    riskPerTradePct: 0.35,
    maxOpenPositionsPerSymbol: 1,
    autoPauseDailyR: -3,
    autoPause30dR: -8,
  };
}

export function isScalpComposerSundayUtc(tsMs: number): boolean {
  const ts = Math.floor(Number(tsMs));
  if (!Number.isFinite(ts) || ts <= 0) return false;
  return new Date(ts).getUTCDay() === 0;
}

export type V1LedgerLikeRow = {
  id: string;
  exitAtMs: number;
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  rMultiple: number;
  reasonCodes: string[];
};

export type V2LedgerLikeRow = {
  id: string;
  tsExitMs: number;
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  closeType: ScalpComposerCloseType;
  rMultiple: number;
};

export function mapV1LedgerRowToV2(row: V1LedgerLikeRow): V2LedgerLikeRow {
  const reasonCodes = normalizeReasonCodes(row.reasonCodes);
  return {
    id: String(row.id),
    tsExitMs: Math.floor(Number(row.exitAtMs) || Date.now()),
    deploymentId: String(row.deploymentId || "").trim(),
    symbol: String(row.symbol || "").trim().toUpperCase(),
    strategyId: String(row.strategyId || "").trim().toLowerCase(),
    tuneId: String(row.tuneId || "").trim().toLowerCase(),
    closeType: deriveCloseTypeFromReasonCodes(reasonCodes),
    rMultiple: Number.isFinite(Number(row.rMultiple)) ? Number(row.rMultiple) : 0,
  };
}
