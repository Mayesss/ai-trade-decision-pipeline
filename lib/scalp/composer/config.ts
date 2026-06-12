import type {
  ScalpComposerBudgetConfig,
  ScalpComposerRiskProfile,
  ScalpComposerRuntimeConfig,
  ScalpComposerSession,
  ScalpComposerVenue,
} from "./types";
import {
  DEFAULT_SCALP_COMPOSER_STRATEGY_ID,
  DEFAULT_SCALP_COMPOSER_TUNE_ID,
} from "./constants";

const DEFAULT_SESSIONS: ScalpComposerSession[] = [
  "tokyo",
  "berlin",
  "newyork",
  "pacific",
  "sydney",
];
const DEFAULT_VENUES: ScalpComposerVenue[] = ["bitget", "capital"];
const FIXED_SCOPE_SYMBOLS_BY_VENUE: Record<ScalpComposerVenue, string[]> =
  Object.freeze({
    bitget: [
      "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT",
      "LINKUSDT", "DOTUSDT", "ADAUSDT", "LTCUSDT", "AVAXUSDT",
      "SUIUSDT", "WLDUSDT", "ARBUSDT", "OPUSDT", "APTUSDT",
      "NEARUSDT", "TONUSDT", "INJUSDT", "PEPEUSDT", "FETUSDT",
      "BNBUSDT", "ATOMUSDT", "UNIUSDT", "TRXUSDT",
      "BCHUSDT", "XLMUSDT", "ETCUSDT", "FILUSDT", "AAVEUSDT",
      "ICPUSDT", "SHIBUSDT", "TAOUSDT", "HYPEUSDT", "ONDOUSDT",
      "WIFUSDT", "ENAUSDT", "PENDLEUSDT", "ALGOUSDT", "CHZUSDT",
    ],
    capital: [
      "EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "AUDUSD",
      "USDCAD", "XAGUSD", "EURGBP", "NZDUSD", "USDCHF",
      "EURJPY", "GBPJPY", "AUDJPY", "CHFJPY", "CADJPY",
      "NZDJPY", "CADCHF", "AUDCAD", "AUDNZD", "EURAUD",
      "EURCAD", "GBPCAD", "GBPCHF",
    ],
  });

function toBool(value: string | undefined, fallback: boolean): boolean {
  if (value === undefined) return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

function toPositiveInt(value: string | undefined, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

function toNumber(value: string | undefined, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return n;
}

function parseVenue(value: unknown): ScalpComposerVenue | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "bitget") return "bitget";
  if (normalized === "capital") return "capital";
  return null;
}

function parseSession(value: unknown): ScalpComposerSession | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (
    normalized === "tokyo" ||
    normalized === "berlin" ||
    normalized === "newyork" ||
    normalized === "pacific" ||
    normalized === "sydney"
  ) {
    return normalized;
  }
  return null;
}

function parseCsvUnique(value: string | undefined): string[] {
  const raw = String(value || "").trim();
  if (!raw) return [];
  const out: string[] = [];
  for (const token of raw.split(",")) {
    const normalized = token.trim();
    if (!normalized) continue;
    out.push(normalized);
  }
  return Array.from(new Set(out));
}

function parseVenues(value: string | undefined): ScalpComposerVenue[] {
  const parsed = parseCsvUnique(value)
    .map(parseVenue)
    .filter((row): row is ScalpComposerVenue => Boolean(row));
  return parsed.length > 0 ? parsed : DEFAULT_VENUES.slice();
}

function parseSessions(value: string | undefined): ScalpComposerSession[] {
  const parsed = parseCsvUnique(value)
    .map(parseSession)
    .filter((row): row is ScalpComposerSession => Boolean(row));
  return parsed.length > 0 ? parsed : DEFAULT_SESSIONS.slice();
}

function normalizeSymbols(values: string[]): string[] {
  const out: string[] = [];
  for (const value of values) {
    const normalized = String(value || "")
      .trim()
      .toUpperCase()
      .replace(/[^A-Z0-9._-]/g, "");
    if (!normalized) continue;
    out.push(normalized);
  }
  return Array.from(new Set(out));
}

function parseSeedSymbols(
  venue: ScalpComposerVenue,
  value: string | undefined,
): string[] {
  const fromEnv = normalizeSymbols(parseCsvUnique(value));
  if (fromEnv.length > 0) return fromEnv;
  return FIXED_SCOPE_SYMBOLS_BY_VENUE[venue].slice();
}

function parseSeedLiveSymbols(
  venue: ScalpComposerVenue,
  value: string | undefined,
): string[] {
  const fromEnv = normalizeSymbols(parseCsvUnique(value));
  if (fromEnv.length > 0) return fromEnv;
  return FIXED_SCOPE_SYMBOLS_BY_VENUE[venue].slice();
}

function clampVenueSeedSymbols(params: {
  venue: ScalpComposerVenue;
  symbols: string[];
  enforceFixedScope: boolean;
}): string[] {
  const normalized = normalizeSymbols(params.symbols);
  if (!params.enforceFixedScope) return normalized;
  const allowed = FIXED_SCOPE_SYMBOLS_BY_VENUE[params.venue] || [];
  const allowedSet = new Set(allowed);
  const intersected = normalized.filter((symbol) => allowedSet.has(symbol));
  return intersected.length > 0 ? intersected : allowed.slice();
}

export function isScalpComposerFixedSeedScopeEnabled(): boolean {
  return toBool(process.env.SCALP_COMPOSER_FORCE_FIXED_SEED_SCOPE, true);
}

export function applyScalpComposerFixedSeedScope(
  runtime: ScalpComposerRuntimeConfig,
): ScalpComposerRuntimeConfig {
  const enforceFixedScope = isScalpComposerFixedSeedScopeEnabled();
  if (!enforceFixedScope) return runtime;
  return {
    ...runtime,
    seedSymbolsByVenue: {
      bitget: clampVenueSeedSymbols({
        venue: "bitget",
        symbols: runtime.seedSymbolsByVenue.bitget || [],
        enforceFixedScope,
      }),
      capital: clampVenueSeedSymbols({
        venue: "capital",
        symbols: runtime.seedSymbolsByVenue.capital || [],
        enforceFixedScope,
      }),
    },
    seedLiveSymbolsByVenue: {
      bitget: clampVenueSeedSymbols({
        venue: "bitget",
        symbols: runtime.seedLiveSymbolsByVenue.bitget || [],
        enforceFixedScope,
      }),
      capital: clampVenueSeedSymbols({
        venue: "capital",
        symbols: runtime.seedLiveSymbolsByVenue.capital || [],
        enforceFixedScope,
      }),
    },
  };
}

export function isScalpComposerRuntimeSymbolInScope(params: {
  runtime: ScalpComposerRuntimeConfig;
  venue: ScalpComposerVenue;
  symbol: string;
  includeLiveSeeds?: boolean;
}): boolean {
  const symbol = String(params.symbol || "").trim().toUpperCase();
  if (!symbol) return false;
  const seedSymbols = new Set(
    normalizeSymbols(params.runtime.seedSymbolsByVenue[params.venue] || []),
  );
  const liveSymbols = params.includeLiveSeeds
    ? normalizeSymbols(params.runtime.seedLiveSymbolsByVenue[params.venue] || [])
    : [];
  for (const row of liveSymbols) seedSymbols.add(row);
  return seedSymbols.has(symbol);
}

export function getScalpComposerDefaultBudgets(): ScalpComposerBudgetConfig {
  return {
    maxCandidatesTotal: Math.max(
      1,
      Math.min(6_000, toPositiveInt(process.env.SCALP_COMPOSER_MAX_CANDIDATES_TOTAL, 6_000)),
    ),
    maxCandidatesPerSymbol: Math.max(
      1,
      Math.min(50, toPositiveInt(process.env.SCALP_COMPOSER_MAX_CANDIDATES_PER_SYMBOL, 16)),
    ),
    maxEnabledDeployments: Math.max(
      1,
      Math.min(200, toPositiveInt(process.env.SCALP_COMPOSER_MAX_ENABLED_DEPLOYMENTS, 12)),
    ),
  };
}

export function getScalpComposerDefaultRiskProfile(): ScalpComposerRiskProfile {
  return {
    riskPerTradePct: Math.max(
      0.01,
      Math.min(5, toNumber(process.env.SCALP_COMPOSER_RISK_PER_TRADE_PCT, 0.35)),
    ),
    maxOpenPositionsPerSymbol: Math.max(
      1,
      Math.min(5, toPositiveInt(process.env.SCALP_COMPOSER_MAX_OPEN_POSITIONS_PER_SYMBOL, 1)),
    ),
    autoPauseDailyR: Math.min(-0.1, toNumber(process.env.SCALP_COMPOSER_AUTO_PAUSE_DAILY_R, -3)),
    autoPause30dR: Math.min(-0.1, toNumber(process.env.SCALP_COMPOSER_AUTO_PAUSE_30D_R, -8)),
  };
}

export function getScalpComposerRuntimeConfig(): ScalpComposerRuntimeConfig {
  const supportedVenues = parseVenues(process.env.SCALP_COMPOSER_SUPPORTED_VENUES);
  const supportedSessions = parseSessions(process.env.SCALP_COMPOSER_SUPPORTED_SESSIONS);

  const seedSymbolsByVenue: Record<ScalpComposerVenue, string[]> = {
    bitget: parseSeedSymbols("bitget", process.env.SCALP_COMPOSER_SEED_SYMBOLS_BITGET),
    capital: parseSeedSymbols("capital", process.env.SCALP_COMPOSER_SEED_SYMBOLS_CAPITAL),
  };
  const seedLiveSymbolsByVenue: Record<ScalpComposerVenue, string[]> = {
    bitget: parseSeedLiveSymbols(
      "bitget",
      process.env.SCALP_COMPOSER_SEED_LIVE_SYMBOLS_BITGET,
    ),
    capital: parseSeedLiveSymbols(
      "capital",
      process.env.SCALP_COMPOSER_SEED_LIVE_SYMBOLS_CAPITAL,
    ),
  };

  const runtime: ScalpComposerRuntimeConfig = {
    enabled: toBool(process.env.SCALP_COMPOSER_ENABLED, true),
    liveEnabled: toBool(process.env.SCALP_COMPOSER_LIVE_ENABLED, false),
    dryRunDefault: toBool(process.env.SCALP_COMPOSER_DRY_RUN_DEFAULT, true),
    defaultStrategyId: String(
      process.env.SCALP_COMPOSER_DEFAULT_STRATEGY_ID || DEFAULT_SCALP_COMPOSER_STRATEGY_ID,
    )
      .trim()
      .toLowerCase(),
    defaultTuneId: String(
      process.env.SCALP_COMPOSER_DEFAULT_TUNE_ID || DEFAULT_SCALP_COMPOSER_TUNE_ID,
    )
      .trim()
      .toLowerCase(),
    supportedVenues,
    supportedSessions,
    seedSymbolsByVenue,
    seedLiveSymbolsByVenue,
    budgets: getScalpComposerDefaultBudgets(),
    riskProfile: getScalpComposerDefaultRiskProfile(),
    prunedScopes: {},
    scopePruneMeta: {
      lastPruneWindowToTs: null,
      lastPrunedAtMs: null,
      lastActiveScopeCount: 0,
      lastNewlyPrunedScopeCount: 0,
    },
  };
  return applyScalpComposerFixedSeedScope(runtime);
}
