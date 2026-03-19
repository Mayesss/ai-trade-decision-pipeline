import crypto from "crypto";

import {
  computeOrderSize,
  fetchSymbolMeta,
  type SymbolMeta,
} from "../../analytics";
import { bitgetFetch, resolveProductType } from "../../bitget";
import { getScalpVenueFeeSchedule } from "../fees";
import {
  buildHeuristicScalpSymbolMarketMetadata,
  normalizeScalpSymbolMarketMetadata,
  type ScalpSymbolMarketMetadata,
} from "../symbolMarketMetadata";
import { inferScalpAssetCategory } from "../symbolInfo";
import type {
  ScalpBrokerPositionSnapshot,
  ScalpVenueAdapter,
  ScalpVenueCandles,
} from "./types";

const BITGET_METADATA_CACHE_TTL_MS = Math.max(
  60_000,
  Math.floor(Number(process.env.SCALP_BITGET_METADATA_CACHE_TTL_MS) || 5 * 60_000),
);
const BITGET_OWNERSHIP_CACHE_TTL_MS = Math.max(
  10 * 60_000,
  Math.floor(
    Number(process.env.SCALP_BITGET_OWNERSHIP_CACHE_TTL_MS) || 24 * 60 * 60_000,
  ),
);
const BITGET_RISK_MARGIN_LEVERAGE_SAFETY_BUFFER = Math.max(
  0,
  Math.floor(
    Number(process.env.SCALP_BITGET_RISK_MARGIN_LEVERAGE_SAFETY_BUFFER) || 5,
  ),
);

type CachedContractMeta = {
  fetchedAtMs: number;
  meta: SymbolMeta & Record<string, unknown>;
  metadata: ScalpSymbolMarketMetadata;
};

type OwnershipRef = {
  symbol: string;
  holdSide: "long" | "short";
  updatedAtMs: number;
};

const contractMetaCache = new Map<string, CachedContractMeta>();
const ownershipByClientOid = new Map<string, OwnershipRef>();

function toFinite(value: unknown, fallback = NaN): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toPositive(value: unknown): number | null {
  const n = toFinite(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function decimalStepFromPricePlace(pricePlace: number | null): number | null {
  if (pricePlace === null) return null;
  const places = Math.max(0, Math.floor(pricePlace));
  const step = 10 ** -places;
  return Number.isFinite(step) && step > 0 ? step : null;
}

function resolveBitgetTickSize(
  meta: Record<string, unknown>,
  pricePlace: number | null,
): number | null {
  const decimalStep = decimalStepFromPricePlace(pricePlace);
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

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function normalizeProductTypeForQuery(value: unknown): string {
  return String(value || "usdt-futures")
    .trim()
    .toUpperCase();
}

function normalizeTimestampMs(value: unknown): number | null {
  const n = toFinite(value);
  if (!(Number.isFinite(n) && n > 0)) return null;
  return n > 1e12 ? Math.floor(n) : Math.floor(n * 1000);
}

function parsePositionSide(value: unknown): "long" | "short" | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "long" || normalized === "buy") return "long";
  if (normalized === "short" || normalized === "sell") return "short";
  return null;
}

function parsePositionMode(value: unknown): "one_way_mode" | "hedge_mode" | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "one_way_mode") return "one_way_mode";
  if (normalized === "hedge_mode") return "hedge_mode";
  return null;
}

function isBitgetReduceOnlyParamError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const message = err.message.toUpperCase();
  return message.includes("40017") && message.includes("REDUCEONLY");
}

function clampLeverage(value: unknown): number | null {
  const raw = toFinite(value);
  if (!(Number.isFinite(raw) && raw > 0)) return null;
  const maxCap = Math.max(
    1,
    Math.floor(Number(process.env.SCALP_BITGET_MAX_LEVERAGE) || 125),
  );
  return Math.max(1, Math.min(maxCap, Math.floor(raw)));
}

function resolveConfiguredBitgetMaxLeverage(): number {
  return clampLeverage(Number.MAX_SAFE_INTEGER) ?? 125;
}

function resolveSymbolBitgetMaxLeverage(
  meta: SymbolMeta & Record<string, unknown>,
): number {
  const configuredCap = resolveConfiguredBitgetMaxLeverage();
  const symbolCap = toPositive((meta as any).maxLever);
  if (!(Number.isFinite(symbolCap as number) && Number(symbolCap) > 0)) {
    return configuredCap;
  }
  return Math.max(1, Math.min(configuredCap, Math.floor(Number(symbolCap))));
}

function normalizeBitgetGranularity(value: string): string {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (!normalized) return "1m";
  if (normalized === "1m" || normalized === "3m" || normalized === "5m")
    return normalized;
  if (
    normalized === "15m" ||
    normalized === "30m" ||
    normalized === "1h" ||
    normalized === "4h" ||
    normalized === "6h" ||
    normalized === "12h" ||
    normalized === "1d" ||
    normalized === "1w"
  ) {
    return normalized;
  }
  if (normalized === "minute") return "1m";
  if (normalized === "hour") return "1h";
  if (normalized === "day") return "1d";
  return "1m";
}

function normalizeClientOid(value: unknown, fallbackPrefix: string): string {
  const normalized = String(value || "")
    .trim()
    .slice(0, 64);
  if (normalized) return normalized;
  return `${fallbackPrefix}-${crypto.randomUUID()}`.slice(0, 64);
}

function parseBitgetCandles(raw: unknown): ScalpVenueCandles {
  if (!Array.isArray(raw)) return [];
  const out: ScalpVenueCandles = [];
  for (const row of raw) {
    const ts = normalizeTimestampMs((row as any)?.[0] ?? (row as any)?.ts);
    const open = toFinite((row as any)?.[1] ?? (row as any)?.open);
    const high = toFinite((row as any)?.[2] ?? (row as any)?.high);
    const low = toFinite((row as any)?.[3] ?? (row as any)?.low);
    const close = toFinite((row as any)?.[4] ?? (row as any)?.close);
    const volume = toFinite((row as any)?.[5] ?? (row as any)?.volume, 0);
    if (!(ts !== null && ts > 0)) continue;
    if (![open, high, low, close].every((v) => Number.isFinite(v) && v > 0))
      continue;
    out.push([ts, open, high, low, close, volume]);
  }
  return out;
}

function resolveBitgetAssetCategory(symbol: string) {
  const inferred = inferScalpAssetCategory(symbol);
  if (inferred === "equity" || inferred === "other") return "crypto";
  return inferred;
}

function buildSyntheticPositionId(
  symbol: string,
  holdSide: "long" | "short",
): string {
  return `${symbol}:${holdSide}`;
}

function parseSyntheticPositionId(
  value: unknown,
): { symbol: string; holdSide: "long" | "short" } | null {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const parts = raw.split(":");
  if (parts.length !== 2) return null;
  const symbol = normalizeSymbol(parts[0]);
  const holdSide = parsePositionSide(parts[1]);
  if (!symbol || !holdSide) return null;
  return { symbol, holdSide };
}

function cleanupOwnershipCache(nowMs: number): void {
  for (const [key, row] of ownershipByClientOid.entries()) {
    if (nowMs - row.updatedAtMs > BITGET_OWNERSHIP_CACHE_TTL_MS) {
      ownershipByClientOid.delete(key);
    }
  }
}

async function loadContractMeta(symbolRaw: string): Promise<CachedContractMeta> {
  const symbol = normalizeSymbol(symbolRaw);
  if (!symbol) throw new Error("Bitget symbol is required");
  const nowMs = Date.now();
  const cached = contractMetaCache.get(symbol);
  if (cached && nowMs - cached.fetchedAtMs <= BITGET_METADATA_CACHE_TTL_MS) {
    return cached;
  }

  const productType = resolveProductType();
  const meta = (await fetchSymbolMeta(
    symbol,
    productType,
  )) as SymbolMeta & Record<string, unknown>;
  const base = buildHeuristicScalpSymbolMarketMetadata(symbol, {
    epic: symbol,
    source: "bitget",
    fetchedAtMs: nowMs,
  });
  const pricePlace = Number.isFinite(toFinite(meta.pricePlace))
    ? Math.max(0, Math.floor(toFinite(meta.pricePlace)))
    : null;
  const volumePlace = Number.isFinite(toFinite(meta.volumePlace))
    ? Math.max(0, Math.floor(toFinite(meta.volumePlace)))
    : null;
  const tickSize = resolveBitgetTickSize(meta, pricePlace);
  const pipSize = tickSize ?? decimalStepFromPricePlace(pricePlace);
  const instrumentType = String(
    (meta as any).symbolType || (meta as any).symbolTypeName || "PERPETUAL",
  )
    .trim()
    .toUpperCase();
  const normalized = normalizeScalpSymbolMarketMetadata({
    ...base,
    symbol,
    epic: symbol,
    source: "bitget",
    assetCategory: resolveBitgetAssetCategory(symbol),
    instrumentType,
    marketStatus: "TRADEABLE",
    pipSize: pipSize ?? base.pipSize,
    pipPosition: pricePlace !== null ? Math.max(0, pricePlace - 1) : base.pipPosition,
    tickSize,
    decimalPlacesFactor: pricePlace,
    scalingFactor: null,
    minDealSize: toPositive(meta.minTradeNum),
    sizeDecimals: volumePlace,
    maxLeverage: resolveSymbolBitgetMaxLeverage(meta),
    fetchedAtMs: nowMs,
  });
  const row: CachedContractMeta = { fetchedAtMs: nowMs, meta, metadata: normalized };
  contractMetaCache.set(symbol, row);
  return row;
}

function extractRows(payload: unknown): any[] {
  if (Array.isArray(payload)) return payload;
  if (payload && typeof payload === "object") {
    const row = payload as Record<string, unknown>;
    if (Array.isArray(row.list)) return row.list as any[];
    if (Array.isArray(row.data)) return row.data as any[];
  }
  return [];
}

async function fetchBitgetAccountEquityUsd(): Promise<number | null> {
  const productTypeRaw = resolveProductType();
  const productTypeQuery = normalizeProductTypeForQuery(productTypeRaw);

  const parseEquity = (rowsRaw: unknown): number | null => {
    const rows = extractRows(rowsRaw);
    let best: number | null = null;
    for (const row of rows) {
      const equity =
        toPositive((row as any)?.usdtEquity) ??
        toPositive((row as any)?.accountEquity) ??
        toPositive((row as any)?.equity) ??
        toPositive((row as any)?.totalEquity) ??
        toPositive((row as any)?.available) ??
        toPositive((row as any)?.crossedMaxAvailable);
      if (equity === null) continue;
      if (best === null || equity > best) best = equity;
    }
    return best;
  };

  try {
    const res = await bitgetFetch("GET", "/api/v2/mix/account/accounts", {
      productType: productTypeQuery,
    });
    const best = parseEquity(res);
    if (best !== null) return best;
  } catch {
    // Continue to fallback endpoint.
  }

  const fallbackSymbol = normalizeSymbol(
    process.env.SCALP_BITGET_ACCOUNT_SYMBOL || "BTCUSDT",
  );
  try {
    const res = await bitgetFetch("GET", "/api/v2/mix/account/account", {
      productType: productTypeQuery,
      symbol: fallbackSymbol,
    });
    return parseEquity(res);
  } catch {
    return null;
  }
}

async function fetchBitgetAccountAvailableUsd(): Promise<number | null> {
  const productTypeRaw = resolveProductType();
  const productTypeQuery = normalizeProductTypeForQuery(productTypeRaw);

  const parseAvailable = (rowsRaw: unknown): number | null => {
    const rows = extractRows(rowsRaw);
    let best: number | null = null;
    for (const row of rows) {
      const available =
        toPositive((row as any)?.available) ??
        toPositive((row as any)?.crossedMaxAvailable) ??
        toPositive((row as any)?.maxTransferOut) ??
        toPositive((row as any)?.accountEquity) ??
        toPositive((row as any)?.usdtEquity);
      if (available === null) continue;
      if (best === null || available > best) best = available;
    }
    return best;
  };

  try {
    const res = await bitgetFetch("GET", "/api/v2/mix/account/accounts", {
      productType: productTypeQuery,
    });
    const best = parseAvailable(res);
    if (best !== null) return best;
  } catch {
    // Continue to fallback endpoint.
  }

  const fallbackSymbol = normalizeSymbol(
    process.env.SCALP_BITGET_ACCOUNT_SYMBOL || "BTCUSDT",
  );
  try {
    const res = await bitgetFetch("GET", "/api/v2/mix/account/account", {
      productType: productTypeQuery,
      symbol: fallbackSymbol,
    });
    return parseAvailable(res);
  } catch {
    return null;
  }
}

async function listBitgetOpenPositionSnapshots(): Promise<
  ScalpBrokerPositionSnapshot[]
> {
  const productTypeRaw = resolveProductType();
  const rows = extractRows(
    await bitgetFetch("GET", "/api/v2/mix/position/all-position", {
      productType: productTypeRaw,
    }),
  );
  const updatedAtMs = Date.now();
  const out: ScalpBrokerPositionSnapshot[] = [];

  for (const row of rows) {
    const symbol = normalizeSymbol((row as any)?.symbol);
    const holdSide = parsePositionSide((row as any)?.holdSide ?? (row as any)?.side);
    if (!symbol || !holdSide) continue;
    const sizeAbs = Math.abs(
      toFinite((row as any)?.total ?? (row as any)?.available ?? (row as any)?.size),
    );
    if (!(Number.isFinite(sizeAbs) && sizeAbs > 0)) continue;
    const entryPrice =
      toPositive((row as any)?.openPriceAvg) ??
      toPositive((row as any)?.entryPrice) ??
      null;
    const mark =
      toPositive((row as any)?.markPrice) ??
      toPositive((row as any)?.markPx) ??
      null;
    const leverage =
      toPositive((row as any)?.leverage) ??
      toPositive((row as any)?.marginLeverage) ??
      toPositive((row as any)?.lever) ??
      null;
    const sideSign = holdSide === "long" ? 1 : -1;
    const pnlPct =
      entryPrice && mark && leverage
        ? ((mark - entryPrice) / entryPrice) * sideSign * leverage * 100
        : null;

    out.push({
      epic: symbol,
      dealId: buildSyntheticPositionId(symbol, holdSide),
      dealReference: null,
      side: holdSide,
      posMode: parsePositionMode((row as any)?.posMode),
      entryPrice,
      leverage,
      size: sizeAbs,
      pnlPct: Number.isFinite(toFinite(pnlPct)) ? Number(pnlPct) : null,
      bid: toPositive((row as any)?.bidPrice),
      offer: toPositive((row as any)?.askPrice),
      createdAtMs: normalizeTimestampMs(
        (row as any)?.cTime ??
          (row as any)?.createTime ??
          (row as any)?.openTime ??
          (row as any)?.uTime,
      ),
      updatedAtMs,
    });
  }

  return out;
}

function quantizeDownSize(params: {
  rawSize: number;
  sizeMultiplier: number | null;
  volumePlace: number | null;
}): number {
  const sizeMultiplier =
    params.sizeMultiplier && params.sizeMultiplier > 0
      ? params.sizeMultiplier
      : null;
  if (sizeMultiplier) {
    const rounded = Math.floor(params.rawSize / sizeMultiplier) * sizeMultiplier;
    const decimals =
      params.volumePlace !== null ? Math.max(0, params.volumePlace) : 8;
    return Number(rounded.toFixed(decimals));
  }
  const decimals = params.volumePlace !== null ? Math.max(0, params.volumePlace) : 8;
  const factor = 10 ** decimals;
  return Math.floor(params.rawSize * factor) / factor;
}

function isBitgetLiveEnabled(): boolean {
  const normalized = String(process.env.SCALP_BITGET_LIVE_ENABLED || "")
    .trim()
    .toLowerCase();
  return (
    normalized === "1" ||
    normalized === "true" ||
    normalized === "yes" ||
    normalized === "on"
  );
}

async function setBitgetLeverage(params: {
  symbol: string;
  direction: "BUY" | "SELL";
  leverage: number | null;
  dryRun: boolean;
}): Promise<void> {
  const leverage = clampLeverage(params.leverage);
  if (!leverage) return;
  if (params.dryRun) return;
  const holdSide = params.direction === "BUY" ? "long" : "short";
  const productTypeRaw = resolveProductType();
  await bitgetFetch("POST", "/api/v2/mix/account/set-leverage", {}, {
    symbol: params.symbol,
    productType: productTypeRaw,
    marginCoin: "USDT",
    marginMode: "isolated",
    leverage: String(leverage),
    holdSide,
  });
}

function resolveLeverageForRiskMarginTarget(params: {
  notionalUsd: number;
  riskMarginTargetUsd: number | null;
  fallbackLeverage: number;
}): number {
  const fallback = Math.max(1, Math.floor(params.fallbackLeverage));
  if (!(Number.isFinite(params.notionalUsd) && params.notionalUsd > 0)) {
    return fallback;
  }
  const riskTarget = toPositive(params.riskMarginTargetUsd);
  if (!(Number.isFinite(riskTarget as number) && Number(riskTarget) > 0)) {
    return fallback;
  }
  const rawLeverage = params.notionalUsd / Number(riskTarget);
  if (!(Number.isFinite(rawLeverage) && rawLeverage > 0)) {
    return fallback;
  }
  if (rawLeverage < 1) return 1;
  const exactTargetLeverage = Math.floor(rawLeverage);
  const bufferedLeverage =
    exactTargetLeverage - BITGET_RISK_MARGIN_LEVERAGE_SAFETY_BUFFER;
  return Math.max(1, bufferedLeverage);
}

async function executeBitgetScalpEntry(params: {
  symbol: string;
  direction: "BUY" | "SELL";
  notionalUsd: number;
  riskUsd?: number | null;
  leverage?: number | null;
  dryRun?: boolean;
  clientOid?: string;
  orderType?: "MARKET" | "LIMIT";
  limitLevel?: number | null;
  stopLevel?: number | null;
  profitLevel?: number | null;
}) {
  const symbol = normalizeSymbol(params.symbol);
  if (!symbol) throw new Error("Bitget scalp entry requires symbol");
  const notionalUsd = toFinite(params.notionalUsd);
  if (!(Number.isFinite(notionalUsd) && notionalUsd > 0)) {
    throw new Error(`Invalid bitget scalp notional for ${symbol}`);
  }
  const orderType: "MARKET" | "LIMIT" =
    params.orderType === "LIMIT" ? "LIMIT" : "MARKET";
  const direction: "BUY" | "SELL" =
    params.direction === "SELL" ? "SELL" : "BUY";
  const holdSide = direction === "BUY" ? "long" : "short";
  const clientOid = normalizeClientOid(params.clientOid, "bg-scl");
  const dryRun = params.dryRun !== false;
  const requestedLeverage = clampLeverage(params.leverage ?? null) ?? 1;
  const riskMarginTargetUsd = toPositive(params.riskUsd);
  const marginTargetLeverage = resolveLeverageForRiskMarginTarget({
    notionalUsd,
    riskMarginTargetUsd,
    fallbackLeverage: requestedLeverage,
  });
  const syntheticDealId = buildSyntheticPositionId(symbol, holdSide);

  if (!dryRun && !isBitgetLiveEnabled()) {
    return {
      placed: false,
      dryRun: false,
      orderId: null,
      dealId: syntheticDealId,
      dealReference: clientOid,
      clientOid,
      symbol,
      direction,
      notionalUsd,
      leverage: marginTargetLeverage,
      orderType,
      size: null,
      epic: symbol,
      dealStatus: "REJECTED",
      confirmStatus: "LIVE_DISABLED",
      rejectReason: "BITGET_LIVE_DISABLED",
    };
  }

  if (dryRun) {
    return {
      placed: false,
      dryRun: true,
      orderId: null,
      dealId: syntheticDealId,
      dealReference: clientOid,
      clientOid,
      symbol,
      direction,
      notionalUsd,
      leverage: marginTargetLeverage,
      orderType,
      size: null,
      epic: symbol,
      dealStatus: null,
      confirmStatus: null,
      rejectReason: null,
    };
  }

  const { meta } = await loadContractMeta(symbol);
  const symbolLeverageCap = resolveSymbolBitgetMaxLeverage(meta);
  const accountAvailableUsd = await fetchBitgetAccountAvailableUsd().catch(
    () => null,
  );
  if (
    riskMarginTargetUsd !== null &&
    Number.isFinite(riskMarginTargetUsd) &&
    riskMarginTargetUsd > notionalUsd + 1e-9
  ) {
    return {
      placed: false,
      dryRun: false,
      orderId: null,
      dealId: syntheticDealId,
      dealReference: clientOid,
      clientOid,
      symbol,
      direction,
      notionalUsd,
      leverage: marginTargetLeverage,
      orderType,
      size: null,
      epic: symbol,
      dealStatus: "REJECTED",
      confirmStatus: "MARGIN_TARGET_INVALID",
      rejectReason: "RISK_MARGIN_TARGET_EXCEEDS_NOTIONAL",
    };
  }
  if (
    riskMarginTargetUsd !== null &&
    Number.isFinite(accountAvailableUsd as number) &&
    Number(accountAvailableUsd) > 0 &&
    Number(accountAvailableUsd) + 1e-9 < riskMarginTargetUsd
  ) {
    return {
      placed: false,
      dryRun: false,
      orderId: null,
      dealId: syntheticDealId,
      dealReference: clientOid,
      clientOid,
      symbol,
      direction,
      notionalUsd,
      leverage: marginTargetLeverage,
      orderType,
      size: null,
      epic: symbol,
      dealStatus: "REJECTED",
      confirmStatus: "MARGIN_INSUFFICIENT",
      rejectReason: "INSUFFICIENT_AVAILABLE_MARGIN_FOR_RISK_TARGET",
    };
  }
  const requiredLeverageByAvailable =
    Number.isFinite(accountAvailableUsd as number) && Number(accountAvailableUsd) > 0
      ? Math.max(
          1,
          Math.ceil((notionalUsd * 1.01) / Number(accountAvailableUsd)),
        )
      : 1;
  const targetLeverage = Math.max(
    marginTargetLeverage,
    requiredLeverageByAvailable,
  );
  const leverage = Math.max(1, Math.min(symbolLeverageCap, targetLeverage));
  const isolatedMarginUsd = notionalUsd / leverage;

  if (requiredLeverageByAvailable > symbolLeverageCap) {
    return {
      placed: false,
      dryRun: false,
      orderId: null,
      dealId: syntheticDealId,
      dealReference: clientOid,
      clientOid,
      symbol,
      direction,
      notionalUsd,
      leverage,
      orderType,
      size: null,
      epic: symbol,
      dealStatus: "REJECTED",
      confirmStatus: "MARGIN_INSUFFICIENT",
      rejectReason: "INSUFFICIENT_BALANCE_FOR_NOTIONAL",
    };
  }
  if (
    riskMarginTargetUsd !== null &&
    Number.isFinite(isolatedMarginUsd) &&
    isolatedMarginUsd + 1e-9 < riskMarginTargetUsd
  ) {
    return {
      placed: false,
      dryRun: false,
      orderId: null,
      dealId: syntheticDealId,
      dealReference: clientOid,
      clientOid,
      symbol,
      direction,
      notionalUsd,
      leverage,
      orderType,
      size: null,
      epic: symbol,
      dealStatus: "REJECTED",
      confirmStatus: "MARGIN_TARGET_UNATTAINABLE",
      rejectReason: "RISK_MARGIN_TARGET_UNATTAINABLE_WITH_LEVERAGE",
    };
  }

  await setBitgetLeverage({
    symbol,
    direction,
    leverage,
    dryRun: false,
  });

  const productTypeRaw = resolveProductType();
  const size = await computeOrderSize(symbol, notionalUsd, productTypeRaw);
  if (!(Number.isFinite(size) && size > 0)) {
    throw new Error(`Bitget returned invalid order size for ${symbol}`);
  }
  const body: Record<string, unknown> = {
    symbol,
    productType: productTypeRaw,
    marginCoin: "USDT",
    marginMode: "isolated",
    side: direction.toLowerCase(),
    orderType: orderType === "LIMIT" ? "limit" : "market",
    size: String(size),
    clientOid,
    force: "gtc",
    holdSide,
  };
  if (
    orderType === "LIMIT" &&
    Number.isFinite(toFinite(params.limitLevel)) &&
    Number(params.limitLevel) > 0
  ) {
    body.price = Number(params.limitLevel);
  }
  if (Number.isFinite(toFinite(params.stopLevel)) && Number(params.stopLevel) > 0) {
    body.presetStopLossPrice = String(Number(params.stopLevel));
  }
  if (
    Number.isFinite(toFinite(params.profitLevel)) &&
    Number(params.profitLevel) > 0
  ) {
    body.presetStopSurplusPrice = String(Number(params.profitLevel));
  }

  try {
    const res = await bitgetFetch(
      "POST",
      "/api/v2/mix/order/place-order",
      {},
      body,
    );
    const orderId =
      String((res as any)?.orderId || (res as any)?.order_id || "").trim() || null;
    ownershipByClientOid.set(clientOid, {
      symbol,
      holdSide,
      updatedAtMs: Date.now(),
    });
    cleanupOwnershipCache(Date.now());
    return {
      placed: true,
      dryRun: false,
      orderId,
      dealId: syntheticDealId,
      dealReference: clientOid,
      clientOid,
      symbol,
      direction,
      notionalUsd,
      leverage,
      orderType,
      size,
      epic: symbol,
      dealStatus: "ACCEPTED",
      confirmStatus: "SUBMITTED",
      rejectReason: null,
      raw: res,
    };
  } finally {
    if (leverage > 1) {
      await setBitgetLeverage({
        symbol,
        direction,
        leverage: 1,
        dryRun: false,
      }).catch(() => null);
    }
  }
}

function resolveCloseTarget(params: {
  dealId?: string | null;
  dealReference?: string | null;
}): { symbol: string; holdSide: "long" | "short" } | null {
  const fromDealId = parseSyntheticPositionId(params.dealId);
  if (fromDealId) return fromDealId;
  const ref = String(params.dealReference || "").trim();
  if (!ref) return null;
  const cached = ownershipByClientOid.get(ref);
  if (!cached) return null;
  return { symbol: cached.symbol, holdSide: cached.holdSide };
}

async function closeBitgetPositionByOwnership(params: {
  dealId?: string | null;
  dealReference?: string | null;
  partialClosePct?: number | null;
  clientOid?: string;
}) {
  const clientOid = normalizeClientOid(params.clientOid, "bg-close");
  const ownership = resolveCloseTarget({
    dealId: params.dealId,
    dealReference: params.dealReference,
  });
  if (!ownership) {
    return {
      closed: false,
      orderId: null,
      clientOid,
      partial: false,
      note: "no_matching_position",
    };
  }

  if (!isBitgetLiveEnabled()) {
    return {
      closed: false,
      orderId: null,
      clientOid,
      partial: false,
      note: "live_disabled",
    };
  }

  const snapshots = await listBitgetOpenPositionSnapshots();
  const matches = snapshots.filter(
    (row) =>
      row.epic === ownership.symbol &&
      row.side === ownership.holdSide &&
      Number.isFinite(toFinite(row.size)) &&
      Number(row.size) > 0,
  );
  if (matches.length === 0) {
    return {
      closed: false,
      orderId: null,
      clientOid,
      partial: false,
      note: "no_matching_position",
    };
  }
  if (matches.length > 1) {
    return {
      closed: false,
      orderId: null,
      clientOid,
      partial: false,
      note: "ambiguous_matching_positions",
    };
  }
  const match = matches[0]!;
  const positionSize = Math.abs(toFinite(match.size));
  if (!(Number.isFinite(positionSize) && positionSize > 0)) {
    return {
      closed: false,
      orderId: null,
      clientOid,
      partial: false,
      note: "no_matching_position",
    };
  }

  const rawClosePct = toFinite(params.partialClosePct, 100);
  const closePct = Math.max(0, Math.min(100, rawClosePct));
  if (!(closePct > 0)) {
    return {
      closed: false,
      orderId: null,
      clientOid,
      partial: false,
      note: "invalid_partial_pct",
    };
  }
  const productTypeRaw = resolveProductType();
  const { meta } = await loadContractMeta(ownership.symbol);
  const step = toPositive((meta as any).sizeMultiplier);
  const volumePlace = Number.isFinite(toFinite((meta as any).volumePlace))
    ? Math.max(0, Math.floor(toFinite((meta as any).volumePlace)))
    : null;
  const closeSizeRaw =
    closePct >= 100 ? positionSize : (positionSize * closePct) / 100;
  const quantizedCloseSize = quantizeDownSize({
    rawSize: closeSizeRaw,
    sizeMultiplier: step,
    volumePlace,
  });
  if (!(Number.isFinite(quantizedCloseSize) && quantizedCloseSize > 0)) {
    return {
      closed: false,
      orderId: null,
      clientOid,
      partial: closePct < 100,
      note: "partial_size_below_step",
    };
  }

  const isHedgeMode = match.posMode === "hedge_mode";
  const closeBody: Record<string, unknown> = {
    symbol: ownership.symbol,
    productType: productTypeRaw,
    marginCoin: "USDT",
    marginMode: "isolated",
    orderType: "market",
    size: String(quantizedCloseSize),
    clientOid,
    force: "gtc",
  };
  if (isHedgeMode) {
    closeBody.side = ownership.holdSide === "long" ? "buy" : "sell";
    closeBody.tradeSide = "close";
    closeBody.holdSide = ownership.holdSide;
  } else {
    closeBody.side = ownership.holdSide === "long" ? "sell" : "buy";
    closeBody.reduceOnly = "YES";
  }

  let res: any;
  let usedFlashCloseFallback = false;
  try {
    res = await bitgetFetch(
      "POST",
      "/api/v2/mix/order/place-order",
      {},
      closeBody,
    );
  } catch (err) {
    if (!(closePct >= 100 && isBitgetReduceOnlyParamError(err))) {
      throw err;
    }
    const flashBody: Record<string, unknown> = {
      productType: productTypeRaw,
      symbol: ownership.symbol,
    };
    if (isHedgeMode) {
      flashBody.holdSide = ownership.holdSide;
    }
    res = await bitgetFetch("POST", "/api/v2/mix/order/close-positions", {}, flashBody);
    usedFlashCloseFallback = true;
  }
  if (usedFlashCloseFallback) {
    const successList = Array.isArray((res as any)?.successList)
      ? ((res as any).successList as any[])
      : [];
    const failureList = Array.isArray((res as any)?.failureList)
      ? ((res as any).failureList as any[])
      : [];
    if (successList.length === 0 && failureList.length > 0) {
      return {
        closed: false,
        orderId: null,
        clientOid,
        partial: closePct < 100,
        note: "flash_close_failed",
      };
    }
    const firstSuccess = successList[0] as Record<string, unknown> | undefined;
    const fallbackOrderId =
      String(
        firstSuccess?.orderId ||
          firstSuccess?.order_id ||
          (res as any)?.orderId ||
          (res as any)?.order_id ||
          "",
      ).trim() || null;
    return {
      closed: true,
      orderId: fallbackOrderId,
      clientOid,
      partial: closePct < 100,
    };
  }

  const orderId =
    String((res as any)?.orderId || (res as any)?.order_id || "").trim() || null;
  return {
    closed: true,
    orderId,
    clientOid,
    partial: closePct < 100,
  };
}

export const bitgetScalpVenueAdapter: ScalpVenueAdapter = {
  venue: "bitget",
  fees: getScalpVenueFeeSchedule("bitget"),
  market: {
    async resolveEpicRuntime(symbol: string) {
      const normalized = normalizeSymbol(symbol);
      await loadContractMeta(normalized);
      return {
        ticker: normalized,
        epic: normalized,
        source: "discovered",
      };
    },
    async fetchLivePrice(symbol: string) {
      const normalized = normalizeSymbol(symbol);
      const productTypeQuery = normalizeProductTypeForQuery(resolveProductType());
      const payload = await bitgetFetch("GET", "/api/v2/mix/market/ticker", {
        symbol: normalized,
        productType: productTypeQuery,
      });
      const ticker = Array.isArray(payload) ? payload[0] : payload;
      const last =
        toPositive((ticker as any)?.lastPr) ??
        toPositive((ticker as any)?.last) ??
        toPositive((ticker as any)?.close) ??
        null;
      if (!(last && last > 0)) {
        throw new Error(`Bitget live quote unavailable for ${normalized}`);
      }
      return {
        symbol: normalized,
        epic: normalized,
        price: last,
        bid: toPositive((ticker as any)?.bidPr ?? (ticker as any)?.bidPrice),
        offer: toPositive((ticker as any)?.askPr ?? (ticker as any)?.askPrice),
        ts: normalizeTimestampMs((ticker as any)?.ts ?? (ticker as any)?.timestamp) ??
          Date.now(),
        mappingSource: "bitget_symbol",
      };
    },
    async fetchCandlesByEpic(epic: string, timeframe: string, limit: number) {
      const symbol = normalizeSymbol(epic);
      const productTypeQuery = normalizeProductTypeForQuery(resolveProductType());
      const candles = await bitgetFetch("GET", "/api/v2/mix/market/candles", {
        symbol,
        productType: productTypeQuery,
        granularity: normalizeBitgetGranularity(timeframe),
        limit: Math.max(20, Math.min(1000, Math.floor(limit || 200))),
      });
      return parseBitgetCandles(candles);
    },
    async ensureSymbolMarketMetadata(symbol: string) {
      const normalized = normalizeSymbol(symbol);
      if (!normalized) return null;
      try {
        const cached = await loadContractMeta(normalized);
        return cached.metadata;
      } catch {
        return buildHeuristicScalpSymbolMarketMetadata(normalized, {
          epic: normalized,
          source: "bitget",
          fetchedAtMs: Date.now(),
        });
      }
    },
  },
  broker: {
    fetchAccountEquityUsd() {
      return fetchBitgetAccountEquityUsd();
    },
    fetchOpenPositionSnapshots() {
      cleanupOwnershipCache(Date.now());
      return listBitgetOpenPositionSnapshots();
    },
    executeScalpEntry(params) {
      cleanupOwnershipCache(Date.now());
      return executeBitgetScalpEntry(params);
    },
    closePositionByOwnership(params) {
      cleanupOwnershipCache(Date.now());
      return closeBitgetPositionByOwnership(params);
    },
  },
};
