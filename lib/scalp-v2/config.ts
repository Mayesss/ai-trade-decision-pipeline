import type {
  ScalpV2BudgetConfig,
  ScalpV2RiskProfile,
  ScalpV2RuntimeConfig,
  ScalpV2Session,
  ScalpV2Venue,
} from "./types";
import {
  DEFAULT_SCALP_V2_STRATEGY_ID,
  DEFAULT_SCALP_V2_TUNE_ID,
} from "./constants";

const DEFAULT_SESSIONS: ScalpV2Session[] = [
  "tokyo",
  "berlin",
  "newyork",
  "pacific",
  "sydney",
];
const DEFAULT_VENUES: ScalpV2Venue[] = ["bitget", "capital"];
const FIXED_SCOPE_SYMBOLS_BY_VENUE: Record<ScalpV2Venue, string[]> =
  Object.freeze({
    bitget: ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "LINKUSDT", "DOTUSDT", "ADAUSDT", "LTCUSDT", "AVAXUSDT"],
    capital: ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "AUDUSD", "USDCAD", "XAGUSD", "EURGBP", "NZDUSD", "USDCHF"],
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

function parseVenue(value: unknown): ScalpV2Venue | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "bitget") return "bitget";
  if (normalized === "capital") return "capital";
  return null;
}

function parseSession(value: unknown): ScalpV2Session | null {
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

function parseVenues(value: string | undefined): ScalpV2Venue[] {
  const parsed = parseCsvUnique(value)
    .map(parseVenue)
    .filter((row): row is ScalpV2Venue => Boolean(row));
  return parsed.length > 0 ? parsed : DEFAULT_VENUES.slice();
}

function parseSessions(value: string | undefined): ScalpV2Session[] {
  const parsed = parseCsvUnique(value)
    .map(parseSession)
    .filter((row): row is ScalpV2Session => Boolean(row));
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
  venue: ScalpV2Venue,
  value: string | undefined,
): string[] {
  const fromEnv = normalizeSymbols(parseCsvUnique(value));
  if (fromEnv.length > 0) return fromEnv;
  return FIXED_SCOPE_SYMBOLS_BY_VENUE[venue].slice();
}

function parseSeedLiveSymbols(
  venue: ScalpV2Venue,
  value: string | undefined,
): string[] {
  const fromEnv = normalizeSymbols(parseCsvUnique(value));
  if (fromEnv.length > 0) return fromEnv;
  return FIXED_SCOPE_SYMBOLS_BY_VENUE[venue].slice();
}

function clampVenueSeedSymbols(params: {
  venue: ScalpV2Venue;
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

export function isScalpV2FixedSeedScopeEnabled(): boolean {
  return toBool(process.env.SCALP_V2_FORCE_FIXED_SEED_SCOPE, true);
}

export function applyScalpV2FixedSeedScope(
  runtime: ScalpV2RuntimeConfig,
): ScalpV2RuntimeConfig {
  const enforceFixedScope = isScalpV2FixedSeedScopeEnabled();
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

export function isScalpV2RuntimeSymbolInScope(params: {
  runtime: ScalpV2RuntimeConfig;
  venue: ScalpV2Venue;
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

export function getScalpV2DefaultBudgets(): ScalpV2BudgetConfig {
  return {
    maxCandidatesTotal: Math.max(
      1,
      Math.min(2_000, toPositiveInt(process.env.SCALP_V2_MAX_CANDIDATES_TOTAL, 200)),
    ),
    maxCandidatesPerSymbol: Math.max(
      1,
      Math.min(50, toPositiveInt(process.env.SCALP_V2_MAX_CANDIDATES_PER_SYMBOL, 16)),
    ),
    maxEnabledDeployments: Math.max(
      1,
      Math.min(200, toPositiveInt(process.env.SCALP_V2_MAX_ENABLED_DEPLOYMENTS, 12)),
    ),
  };
}

export function getScalpV2DefaultRiskProfile(): ScalpV2RiskProfile {
  return {
    riskPerTradePct: Math.max(
      0.01,
      Math.min(5, toNumber(process.env.SCALP_V2_RISK_PER_TRADE_PCT, 0.35)),
    ),
    maxOpenPositionsPerSymbol: Math.max(
      1,
      Math.min(5, toPositiveInt(process.env.SCALP_V2_MAX_OPEN_POSITIONS_PER_SYMBOL, 1)),
    ),
    autoPauseDailyR: Math.min(-0.1, toNumber(process.env.SCALP_V2_AUTO_PAUSE_DAILY_R, -3)),
    autoPause30dR: Math.min(-0.1, toNumber(process.env.SCALP_V2_AUTO_PAUSE_30D_R, -8)),
  };
}

export function getScalpV2RuntimeConfig(): ScalpV2RuntimeConfig {
  const supportedVenues = parseVenues(process.env.SCALP_V2_SUPPORTED_VENUES);
  const supportedSessions = parseSessions(process.env.SCALP_V2_SUPPORTED_SESSIONS);

  const seedSymbolsByVenue: Record<ScalpV2Venue, string[]> = {
    bitget: parseSeedSymbols("bitget", process.env.SCALP_V2_SEED_SYMBOLS_BITGET),
    capital: parseSeedSymbols("capital", process.env.SCALP_V2_SEED_SYMBOLS_CAPITAL),
  };
  const seedLiveSymbolsByVenue: Record<ScalpV2Venue, string[]> = {
    bitget: parseSeedLiveSymbols(
      "bitget",
      process.env.SCALP_V2_SEED_LIVE_SYMBOLS_BITGET,
    ),
    capital: parseSeedLiveSymbols(
      "capital",
      process.env.SCALP_V2_SEED_LIVE_SYMBOLS_CAPITAL,
    ),
  };

  const runtime: ScalpV2RuntimeConfig = {
    enabled: toBool(process.env.SCALP_V2_ENABLED, true),
    liveEnabled: toBool(process.env.SCALP_V2_LIVE_ENABLED, false),
    dryRunDefault: toBool(process.env.SCALP_V2_DRY_RUN_DEFAULT, true),
    defaultStrategyId: String(
      process.env.SCALP_V2_DEFAULT_STRATEGY_ID || DEFAULT_SCALP_V2_STRATEGY_ID,
    )
      .trim()
      .toLowerCase(),
    defaultTuneId: String(
      process.env.SCALP_V2_DEFAULT_TUNE_ID || DEFAULT_SCALP_V2_TUNE_ID,
    )
      .trim()
      .toLowerCase(),
    supportedVenues,
    supportedSessions,
    seedSymbolsByVenue,
    seedLiveSymbolsByVenue,
    budgets: getScalpV2DefaultBudgets(),
    riskProfile: getScalpV2DefaultRiskProfile(),
  };
  return applyScalpV2FixedSeedScope(runtime);
}
