// lib/gates.ts
// Adaptive pre-trade gates for FUTURES-ONLY flow.
// Dynamic banded depth, expected-slippage gate, tiered fallbacks, and convenience wrappers.

///////////////////////////////
// Types
///////////////////////////////

export type Tier = 'BTC' | 'ETH' | 'MAJOR' | 'MID' | 'SMALL';

export interface OrderbookSideLevelArr extends Array<[number, number]> {} // [price, size]
export interface OrderbookSideObjArr extends Array<{ price: number; size: number }> {}

export interface GatesInput {
  symbol: string; // e.g. "BTCUSDT"
  last: number;   // last traded price
  orderbook: {
    bids: OrderbookSideLevelArr | OrderbookSideObjArr;
    asks: OrderbookSideLevelArr | OrderbookSideObjArr;
  };
  notionalUSDT: number;          // intended order size (USD)
  atrAbs1h: number;              // ATR(1H), same units as price
  regime: 'up' | 'down' | 'neutral'; // from macro indicators (e.g., EMA(20)>EMA(50) on 1h)
  positionOpen: boolean;

  // Optional adaptive histories; falls back to tier defaults if absent/short.
  spreadBpsHistory?: number[];   // spread/last * 1e4 (bps), sampled e.g. each minute
  top5BidUsdHistory?: number[];  // historical depth metric (legacy); you may migrate to banded history later
  atrPctHistory?: number[];      // ATR(1H)% = atrAbs/close
  slippageBpsHistory?: number[]; // realized slippage bps for your fills (optional)

  // Optional for tiering
  vol24hUSD?: number;
  medianSpreadBps24h?: number;
}

/** Output of the adaptive gate computation */
export interface GatesOutput {
  tier: Tier;
  metrics: {
    // Now
    spreadBpsNow: number;
    bandBps: number;
    bidBandUsdNow: number;
    askBandUsdNow: number;
    expectedSlippageBps: number;
    obImbNow: number;     // [-1,1], from top sizes
    atrPctNow: number;    // 0.0123 => 1.23%

    // Percentiles (if available)
    spreadBpsP50?: number;
    spreadBpsP75?: number;
    depthUsdP50?: number;
    atrPctP25?: number;
    atrPctP85?: number;

    // Fallback thresholds used
    spreadBpsMax: number;
    depthMinUSD: number;
    atrPctFloor: number;
    atrPctCeil: number;
    slippageBpsMax: number;
  };
  gates: {
    spread_ok: boolean;
    liquidity_ok: boolean;
    atr_ok: boolean;
    slippage_ok: boolean;
  };
  allowed_actions: ('BUY' | 'SELL' | 'HOLD' | 'CLOSE')[];
}

export interface GatesHistories {
  spreadBpsHistory?: number[];
  top5BidUsdHistory?: number[];
  atrPctHistory?: number[];
  slippageBpsHistory?: number[];
  vol24hUSD?: number;
  medianSpreadBps24h?: number;
}

///////////////////////////////
// Helpers
///////////////////////////////

function toPriceSizeArrays(side: OrderbookSideLevelArr | OrderbookSideObjArr): [number, number][] {
  if (!Array.isArray(side) || side.length === 0) return [];
  const first = side[0] as any;
  if (Array.isArray(first)) return side as [number, number][];
  return (side as OrderbookSideObjArr).map(l => [Number(l.price), Number(l.size)]);
}

function sumTopSize(levels: [number, number][], n: number): number {
  return levels.slice(0, n).reduce((a, [, s]) => a + Number(s), 0);
}

function obImbalance(bids: [number, number][], asks: [number, number][], n = 5): number {
  const b = sumTopSize(bids, n);
  const a = sumTopSize(asks, n);
  const denom = Math.max(1e-9, b + a);
  return (b - a) / denom; // [-1,1]
}

function percentile(arr: number[], p: number): number | undefined {
  if (!arr || arr.length === 0) return undefined;
  const a = arr.slice().sort((x, y) => x - y);
  const i = Math.max(0, Math.min(a.length - 1, Math.round(p * (a.length - 1))));
  return a[i];
}
function robustMedian(arr: number[]): number | undefined {
  return percentile(arr, 0.5);
}

function midPrice(bids: [number, number][], asks: [number, number][]) {
  const bb = Number(bids?.[0]?.[0] ?? 0);
  const ba = Number(asks?.[0]?.[0] ?? 0);
  if (bb > 0 && ba > 0) return (bb + ba) / 2;
  return 0;
}

/** USD notional within a price band (±bandBps) around mid on the specified side */
function bandedDepthUsd(
  levels: [number, number][],
  mid: number,
  side: 'bid' | 'ask',
  bandBps = 10,
  maxLevels = 100
): number {
  if (mid <= 0) return 0;
  const band = mid * (bandBps / 1e4);
  const lo = mid - band, hi = mid + band;

  let total = 0;
  for (const [p, s] of levels.slice(0, maxLevels)) {
    if (side === 'bid' && p >= lo && p <= mid) total += p * s;
    if (side === 'ask' && p <= hi && p >= mid) total += p * s;
  }
  return total;
}

function dynamicBandBps(spreadBpsNow: number, atrPctNow: number): number {
  // Base 10 bps; widen with spread and volatility; clamp 50 bps.
  const atrBps = Number.isFinite(atrPctNow) ? atrPctNow * 1e4 : 0;
  const bps = Math.max(10, 3 * spreadBpsNow, 0.5 * atrBps);
  return Math.min(50, bps);
}

/** Simulate sweeping the book with a notional and compute average price vs mid (bps) */
function expectedSlippageBps(
  side: 'buy' | 'sell',
  notionalUSDT: number,
  bids: [number, number][],
  asks: [number, number][],
  mid: number
): number {
  if (notionalUSDT <= 0 || mid <= 0) return 0;

  let remain = notionalUSDT;
  let costMidUnits = 0;

  if (side === 'buy') {
    for (const [p, s] of asks) {
      const availUSD = p * s;
      const take = Math.min(remain, availUSD);
      costMidUnits += take * (p / mid);
      remain -= take;
      if (remain <= 0) break;
    }
  } else {
    for (const [p, s] of bids) {
      const availUSD = p * s;
      const take = Math.min(remain, availUSD);
      costMidUnits += take * (p / mid);
      remain -= take;
      if (remain <= 0) break;
    }
  }

  if (remain > 0) return Infinity; // book too thin

  const avgPxVsMid = costMidUnits / notionalUSDT; // weighted (price/mid)
  return Math.abs(avgPxVsMid - 1) * 1e4; // bps
}

///////////////////////////////
// Tiering & thresholds
///////////////////////////////

function tierFor(symbol: string, stats?: { vol24hUSD?: number; medianSpreadBps24h?: number }): Tier {
  const upper = symbol.toUpperCase();
  if (upper.startsWith('BTC')) return 'BTC';
  if (upper.startsWith('ETH')) return 'ETH';
  const vol = stats?.vol24hUSD ?? 0;
  const medSp = stats?.medianSpreadBps24h ?? 99;
  if (vol >= 500_000_000 && medSp <= 6) return 'MAJOR';
  if (vol >= 100_000_000 && medSp <= 12) return 'MID';
  return 'SMALL';
}

function tierThresholds(tier: Tier) {
  return {
    spreadBpsMax:
      tier === 'BTC' ? 3 :
      tier === 'ETH' ? 4 :
      tier === 'MAJOR' ? 6 :
      tier === 'MID' ? 12 : 20,

    // Lower, realistic floors; adaptivity will override via p50 when available.
    depthMinUSD:
      tier === 'BTC' ? 1_000_000 :
      tier === 'ETH' ?   250_000 :
      tier === 'MAJOR' ? 150_000 :
      tier === 'MID' ?   100_000 : 50_000,

    obImbAbsMin: tier === 'SMALL' ? 0.25 : 0.15, // optional assist

    slippageBpsMax:
      tier === 'BTC' ? 2 :
      tier === 'ETH' ? 3 :
      tier === 'MAJOR' ? 5 :
      tier === 'MID' ? 8 : 12,

    atrPctRange:
      tier === 'BTC' ? [0.0007, 0.015] :
      tier === 'ETH' ? [0.0008, 0.020] :
      [0.0010, 0.025] as [number, number],
  };
}

///////////////////////////////
// Main: computeAdaptiveGates
///////////////////////////////

const EXCLUDED_SYMBOLS = new Set(['AAPLUSDT', 'TSLAUSDT', 'MSFTUSDT', 'NVDAUSDT']); // tokenized stocks: exclude by default

export function computeAdaptiveGates(input: GatesInput): GatesOutput {
  const {
    symbol, last, orderbook, notionalUSDT, atrAbs1h,
    spreadBpsHistory, top5BidUsdHistory, atrPctHistory, slippageBpsHistory,
    vol24hUSD, medianSpreadBps24h, regime, positionOpen,
  } = input;

  const bids = toPriceSizeArrays(orderbook.bids);
  const asks = toPriceSizeArrays(orderbook.asks);

  const bestBid = Number(bids?.[0]?.[0] ?? 0);
  const bestAsk = Number(asks?.[0]?.[0] ?? 0);
  const spread = Math.max(0, bestAsk - bestBid);
  const spreadBpsNow = last > 0 ? (spread / last) * 1e4 : Infinity;

  const obImbNow = obImbalance(bids, asks, 5);
  const atrPctNow = last > 0 ? atrAbs1h / last : NaN;

  // Dynamic band and banded depth
  const mp = midPrice(bids, asks);
  const bandBps = dynamicBandBps(spreadBpsNow, atrPctNow);
  const bidBandUsd = bandedDepthUsd(bids, mp, 'bid', bandBps, 100);
  const askBandUsd = bandedDepthUsd(asks, mp, 'ask', bandBps, 100);

  // Percentiles from histories (adaptive), only if enough samples
  const spreadP50 = spreadBpsHistory && spreadBpsHistory.length >= 20 ? robustMedian(spreadBpsHistory) : undefined;
  const spreadP75 = spreadBpsHistory && spreadBpsHistory.length >= 20 ? percentile(spreadBpsHistory, 0.75) : undefined;

  const depthP50 = top5BidUsdHistory && top5BidUsdHistory.length >= 20 ? robustMedian(top5BidUsdHistory) : undefined;

  const atrP25 = atrPctHistory && atrPctHistory.length >= 20 ? percentile(atrPctHistory, 0.25) : undefined;
  const atrP85 = atrPctHistory && atrPctHistory.length >= 20 ? percentile(atrPctHistory, 0.85) : undefined;

  const slipP75 = slippageBpsHistory && slippageBpsHistory.length >= 20 ? percentile(slippageBpsHistory, 0.75) : undefined;

  // Tier & fallbacks
  const tier = tierFor(symbol, { vol24hUSD, medianSpreadBps24h });
  const t = tierThresholds(tier);

  // Spread gate (slightly relaxed if band widened substantially)
  const spreadCapBase = Math.min(spreadP75 ?? Infinity, t.spreadBpsMax);
  const spreadCap = bandBps > 20 ? spreadCapBase * 1.25 : spreadCapBase;
  const spread_ok = spreadBpsNow <= spreadCap;

  // ATR gate (adaptive by percentiles if available; else tier range)
  const [atrFloor, atrCeil] = t.atrPctRange;
  const atrLo = atrP25 ?? atrFloor;
  const atrHi = atrP85 ?? atrCeil;
  const atr_ok = Number.isFinite(atrPctNow) ? atrPctNow >= atrLo && atrPctNow <= atrHi : true;

  // Liquidity gate (banded depth) with adaptive floor: prefer history, clamp by tier floor, always ≥ 10× notional
  const depthP50Adj = (depthP50 ?? 0) * 0.8;
  const depthFloor = Math.max(10 * (notionalUSDT || 0), Math.min(t.depthMinUSD, depthP50Adj || Infinity));
  const liquidity_ok = bidBandUsd >= depthFloor;

  // Expected slippage (primary realism check)
  const slipBuyBps  = expectedSlippageBps('buy',  notionalUSDT, bids, asks, mp);
  const slipSellBps = expectedSlippageBps('sell', notionalUSDT, bids, asks, mp);
  const slipBpsNow = Math.min(slipBuyBps, slipSellBps); // best-case for "can I trade at all?"

  const slippageCap = Math.min(slipP75 ?? Infinity, t.slippageBpsMax);
  const slippage_ok = slipBpsNow <= slippageCap;

  // Exclusion (tokenized stocks, etc.)
  const isExcluded = EXCLUDED_SYMBOLS.has(symbol.toUpperCase());

  // Final pass condition: must not be excluded; spread & ATR OK; and (liquidity OR slippage) OK
  const gatesPass = !isExcluded && spread_ok && atr_ok && (liquidity_ok || slippage_ok);

  // Allowed actions from gates + regime + position
  let allowed_actions: GatesOutput['allowed_actions'] = ['HOLD'];
  if (positionOpen) {
    allowed_actions = ['HOLD', 'CLOSE']; // you decide exit logic using same gates/info
  } else if (gatesPass) {
    if (regime === 'up') allowed_actions = ['BUY', 'HOLD'];
    else if (regime === 'down') allowed_actions = ['SELL', 'HOLD'];
    else allowed_actions = ['HOLD'];
  } else {
    allowed_actions = ['HOLD']; // keep it safe
  }

  return {
    tier,
    metrics: {
      spreadBpsNow,
      bandBps,
      bidBandUsdNow: bidBandUsd,
      askBandUsdNow: askBandUsd,
      expectedSlippageBps: slipBpsNow,
      obImbNow,
      atrPctNow,

      spreadBpsP50: spreadP50,
      spreadBpsP75: spreadP75,
      depthUsdP50: depthP50,
      atrPctP25: atrP25,
      atrPctP85: atrP85,

      spreadBpsMax: t.spreadBpsMax,
      depthMinUSD: t.depthMinUSD,
      atrPctFloor: atrFloor,
      atrPctCeil: atrCeil,
      slippageBpsMax: t.slippageBpsMax,
    },
    gates: { spread_ok, liquidity_ok, atr_ok, slippage_ok },
    allowed_actions,
  };
}

///////////////////////////////
// Convenience wrappers for analyze.ts
///////////////////////////////

export function extractLastPrice(bundle: any, fallback?: number): number {
  const t = Array.isArray(bundle?.ticker) ? bundle.ticker[0] : bundle?.ticker;
  const last = Number(t?.lastPr ?? t?.last ?? t?.close ?? t?.price ?? fallback ?? NaN);
  return Number.isFinite(last) ? last : 0;
}

export function parseRegimeFromIndicators(indicators: { macro: string }): 'up' | 'down' | 'neutral' {
  if (!indicators?.macro) return 'neutral';
  if (indicators.macro.includes('trend=up')) return 'up';
  if (indicators.macro.includes('trend=down')) return 'down';
  return 'neutral';
}

export function parseAtr1hAbs(indicators: { macro: string }): number {
  const m = indicators?.macro?.match(/ATR=([\d.]+)/);
  return m ? Number(m[1]) : NaN;
}

/**
 * High-level helper: compute gates + allowed_actions and optionally short-circuit HOLD when gates fail.
 * Keeps api/analyze.ts lightweight.
 */
// This should be in a shared utility file, e.g., lib/utils.ts and imported
// Or, you can just place it above getGates in the same file.
const readNum = (name: string, src: string): number | null => {
    const m = src.match(new RegExp(`${name}=([+-]?[\\d\\.]+)`));
    return m ? Number(m[1]) : null;
};

// --- Your Modified Function ---

export function getGates(args: {
  symbol: string;
  bundle: any;
  analytics: any;
  indicators: { micro: string; macro: string };
  notionalUSDT: number;
  positionOpen: boolean;
  histories?: GatesHistories;
}): {
  allowed_actions: ('BUY' | 'SELL' | 'HOLD' | 'CLOSE')[];
  gates: {
    spread_ok: boolean;
    liquidity_ok: boolean;
    atr_ok: boolean;
    slippage_ok: boolean;
    regime_trend_up: boolean;
    regime_trend_down: boolean;
    momentum_long: boolean;     // <--- ADDED
    momentum_short: boolean;    // <--- ADDED
    extension_ok: boolean;      // <--- ADDED
    tier: string;
  };
  metrics: GatesOutput['metrics'];
  preDecision?: {
    action: 'HOLD' | 'CLOSE';
    bias: 'UP' | 'DOWN' | 'NEUTRAL';
    signal_strength: 'LOW' | 'MEDIUM' | 'HIGH';
    summary: string;
    reason: string;
  };
} {
  const { symbol, bundle, analytics, indicators, notionalUSDT, positionOpen, histories } = args;

  const last = analytics?.last || extractLastPrice(bundle, NaN);
  const atrAbs1h = parseAtr1hAbs(indicators);
  const regime = parseRegimeFromIndicators(indicators);

  const bids = bundle?.orderbook?.bids ?? [];
  const asks = bundle?.orderbook?.asks ?? [];
  
  // --- Start: Momentum & Extension Logic (from ai.ts) ---
  const micro = indicators.micro || '';
  const macro = indicators.macro || ''; // (already used by parseRegime/parseAtr1hAbs)

  // Parse 1m indicators
  const ema9_1m = readNum('EMA9', micro);
  const ema21_1m = readNum('EMA21', micro);
  const ema20_1m = readNum('EMA20', micro);
  const slope21_1m = readNum('slopeEMA21_10', micro) ?? 0; // % per bar
  const atr_1m = readNum('ATR', micro);

  // Momentum gates (1m)
  const slopeThresh = 0.01; // 0.01% per 1m bar (tune 0.005–0.02)
  const momentum_long = (ema9_1m ?? 0) > (ema21_1m ?? 0) && slope21_1m > slopeThresh;
  const momentum_short = (ema9_1m ?? 0) < (ema21_1m ?? 0) && slope21_1m < -slopeThresh;

  // Extension guard vs EMA20(1m)
  const extension_ok =
    Number.isFinite(atr_1m as number) && (atr_1m as number) > 0 && Number.isFinite(ema20_1m as number) && last > 0
      ? Math.abs(last - (ema20_1m as number)) / (atr_1m as number) <= 1.5
      // Default to true if data is missing, letting AI make the final call
      : true; 
  // --- End: Momentum & Extension Logic ---


  const out = computeAdaptiveGates({
    symbol,
    last,
    orderbook: { bids, asks },
    notionalUSDT,
    atrAbs1h,
    regime,
    positionOpen,
    spreadBpsHistory: histories?.spreadBpsHistory,
    top5BidUsdHistory: histories?.top5BidUsdHistory,
    atrPctHistory: histories?.atrPctHistory,
    slippageBpsHistory: histories?.slippageBpsHistory,
    vol24hUSD: histories?.vol24hUSD,
    medianSpreadBps24h: histories?.medianSpreadBps24h,
  });

  const gates = {
    spread_ok: out.gates.spread_ok,
    liquidity_ok: out.gates.liquidity_ok,
    atr_ok: out.gates.atr_ok,
    slippage_ok: out.gates.slippage_ok,
    regime_trend_up: regime === 'up',
    regime_trend_down: regime === 'down',
    momentum_long: momentum_long,     // <--- ADDED
    momentum_short: momentum_short,   // <--- ADDED
    extension_ok: extension_ok,       // <--- ADDED
    tier: out.tier,
  };

  // Short-circuit: if no open position and only HOLD is allowed, skip model to save tokens.
  let preDecision: undefined | {
    action: 'HOLD' | 'CLOSE';
    bias: 'UP' | 'DOWN' | 'NEUTRAL';
    signal_strength: 'LOW' | 'MEDIUM' | 'HIGH';
    summary: string;
    reason: string;
  };

  // --- This logic is now more comprehensive ---
  // We check *all* gates. If a trade (BUY/SELL) isn't possible, we skip.
  const can_buy = gates.spread_ok && gates.liquidity_ok && gates.atr_ok && gates.slippage_ok && gates.extension_ok && gates.regime_trend_up && gates.momentum_long;
  const can_sell = gates.spread_ok && gates.liquidity_ok && gates.atr_ok && gates.slippage_ok && gates.extension_ok && gates.regime_trend_down && gates.momentum_short;
  
  if (!positionOpen && !can_buy && !can_sell) {
    preDecision = {
      action: 'HOLD',
      bias: 'NEUTRAL',
      signal_strength: 'LOW',
      summary: 'Pre-trade gates not satisfied; skipping evaluation.',
      // Updated reason string to be more explicit
      reason: `Gates failed: spread_ok=${gates.spread_ok}, liquidity_ok=${gates.liquidity_ok}, atr_ok=${gates.atr_ok}, slippage_ok=${gates.slippage_ok}, extension_ok=${gates.extension_ok}, (regime_up=${gates.regime_trend_up}, momentum_long=${gates.momentum_long}), (regime_down=${gates.regime_trend_down}, momentum_short=${gates.momentum_short})`,
    };
  }

  return {
    // Note: allowed_actions from computeAdaptiveGates is now *less* important
    // than the `can_buy` / `can_sell` logic we just wrote.
    // You might want to simplify this and just pass:
    // allowed_actions: ['HOLD', 'CLOSE', ...(can_buy ? ['BUY'] : []), ...(can_sell ? ['SELL'] : [])],
    // For now, I'll leave your original `out.allowed_actions`
    allowed_actions: out.allowed_actions, 
    gates,
    metrics: out.metrics,
    ...(preDecision ? { preDecision } : {}),
  };
}
