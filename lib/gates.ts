// lib/gates.ts
// Adaptive pre-trade gates for futures-only flow.
// Uses rolling percentiles + symbol tier fallbacks to decide: atr_ok, spread_ok, liquidity_ok, etc.

type Tier = 'BTC' | 'ETH' | 'MAJOR' | 'MID' | 'SMALL';

export interface OrderbookSideLevelArr extends Array<[number, number]> {} // [price,size]
export interface OrderbookSideObjArr extends Array<{ price: number; size: number }> {}

export interface GatesInput {
    symbol: string; // e.g., "BTCUSDT"
    last: number; // last traded price
    orderbook: {
        bids: OrderbookSideLevelArr | OrderbookSideObjArr;
        asks: OrderbookSideLevelArr | OrderbookSideObjArr;
    };
    notionalUSDT: number; // your intended order size in USDT
    atrAbs1h: number; // ATR(1H) absolute (same units as price)
    regime: 'up' | 'down' | 'neutral'; // from your macro indicators (EMA(20) vs EMA(50) 1H etc.)
    positionOpen: boolean;

    // Optional, for adaptivity. If absent/too short, we fall back to tier defaults.
    spreadBpsHistory?: number[]; // e.g., 24–72h of 1m snapshots, in bps (spread/last * 1e4)
    top5BidUsdHistory?: number[]; // rolling top-5 bid depth (USD)
    atrPctHistory?: number[]; // ATR(1H)% history (atrAbs/close)
    slippageBpsHistory?: number[]; // realized slippage (bps) for your fills (optional)

    // For tiering (optional but nice to have)
    vol24hUSD?: number; // est. 24h spot+perp volume USD for the symbol
    medianSpreadBps24h?: number; // median spread bps over last day
}

/** Output of the adaptive gate computation */
export interface GatesOutput {
    tier: Tier;
    metrics: {
        spreadBpsNow: number;
        bidBandUsdNow: number;
        askBandUsdNow: number;
        obImbNow: number; // [-1,1]
        atrPctNow: number; // 0.0123 => 1.23%

        // percentiles used (if available)
        spreadBpsP50?: number;
        spreadBpsP75?: number;
        depthUsdP50?: number;
        atrPctP25?: number;
        atrPctP85?: number;

        // fallbacks actually used
        spreadBpsMax: number;
        depthMinUSD: number;
        atrPctFloor: number;
        atrPctCeil: number;
        slippageBpsMax?: number;
    };
    gates: {
        spread_ok: boolean;
        liquidity_ok: boolean;
        atr_ok: boolean;
        slippage_ok: boolean;
    };
    allowed_actions: ('BUY' | 'SELL' | 'HOLD' | 'CLOSE')[];
}

// ------------------------ helpers ------------------------

function toPriceSizeArrays(side: OrderbookSideLevelArr | OrderbookSideObjArr): [number, number][] {
    if (!Array.isArray(side)) return [];
    if (side.length === 0) return [];
    const first = side[0] as any;
    if (Array.isArray(first)) return side as [number, number][];
    // object form
    return (side as OrderbookSideObjArr).map((l) => [Number(l.price), Number(l.size)]);
}

function depthNotionalUSD(levels: [number, number][], n: number): number {
    return levels.slice(0, n).reduce((a, [p, s]) => a + Number(p) * Number(s), 0);
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

// ------------------------ tiering ------------------------

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

// --- replace/augment helpers ---

function midPrice(bids: [number, number][], asks: [number, number][]) {
    const bb = Number(bids?.[0]?.[0] ?? 0);
    const ba = Number(asks?.[0]?.[0] ?? 0);
    if (bb > 0 && ba > 0) return (bb + ba) / 2;
    return 0;
}

/** Sum USD notional within a price band around mid (bps = basis points) */
function bandedDepthUsd(
    levels: [number, number][],
    mid: number,
    side: 'bid' | 'ask',
    bandBps = 10, // ±10 bps default
    maxLevels = 50,
): number {
    if (mid <= 0) return 0;
    const band = mid * (bandBps / 1e4);
    const lo = mid - band,
        hi = mid + band;

    // take up to N levels and filter by band on the correct side
    const slice = levels.slice(0, maxLevels);
    let total = 0;
    for (const [p, s] of slice) {
        if (side === 'bid' && p >= lo && p <= mid) total += p * s;
        if (side === 'ask' && p <= hi && p >= mid) total += p * s;
    }
    return total;
}

// --- update tier thresholds (lower & more realistic) ---
function tierThresholds(tier: Tier) {
    return {
        spreadBpsMax: tier === 'BTC' ? 3 : tier === 'ETH' ? 4 : tier === 'MAJOR' ? 6 : tier === 'MID' ? 12 : 20,

        // LOWER floors so majors actually pass; ETH 500k is a good practical default
        depthMinUSD:
            tier === 'BTC'
                ? 2_000_000
                : tier === 'ETH'
                ? 500_000
                : tier === 'MAJOR'
                ? 250_000
                : tier === 'MID'
                ? 150_000
                : 75_000,

        obImbAbsMin: tier === 'SMALL' ? 0.25 : 0.15,

        slippageBpsMax: tier === 'BTC' ? 2 : tier === 'ETH' ? 3 : tier === 'MAJOR' ? 5 : tier === 'MID' ? 8 : 12,

        atrPctRange:
            tier === 'BTC' ? [0.0007, 0.015] : tier === 'ETH' ? [0.0008, 0.02] : ([0.001, 0.025] as [number, number]),
    };
}

// ------------------------ main API ------------------------

export function computeAdaptiveGates(input: GatesInput): GatesOutput {
    const {
        symbol,
        last,
        orderbook,
        notionalUSDT,
        atrAbs1h,
        spreadBpsHistory,
        top5BidUsdHistory,
        atrPctHistory,
        slippageBpsHistory,
        vol24hUSD,
        medianSpreadBps24h,
        regime,
        positionOpen,
    } = input;

    const bids = toPriceSizeArrays(orderbook.bids);
    const asks = toPriceSizeArrays(orderbook.asks);
    const bestBid = Number(bids?.[0]?.[0] ?? 0);
    const bestAsk = Number(asks?.[0]?.[0] ?? 0);
    const spread = Math.max(0, bestAsk - bestBid);
    const spreadBpsNow = last > 0 ? (spread / last) * 1e4 : Infinity;

    const obImbNow = obImbalance(bids, asks, 5);

    const atrPctNow = last > 0 ? atrAbs1h / last : NaN;
    // Derive percentiles if enough history (>=20 points)
    const spreadP50 = spreadBpsHistory && spreadBpsHistory.length >= 20 ? robustMedian(spreadBpsHistory) : undefined;
    const spreadP75 =
        spreadBpsHistory && spreadBpsHistory.length >= 20 ? percentile(spreadBpsHistory, 0.75) : undefined;

    const depthP50 = top5BidUsdHistory && top5BidUsdHistory.length >= 20 ? robustMedian(top5BidUsdHistory) : undefined;

    const atrP25 = atrPctHistory && atrPctHistory.length >= 20 ? percentile(atrPctHistory, 0.25) : undefined;
    const atrP85 = atrPctHistory && atrPctHistory.length >= 20 ? percentile(atrPctHistory, 0.85) : undefined;

    const slipP75 =
        slippageBpsHistory && slippageBpsHistory.length >= 20 ? percentile(slippageBpsHistory, 0.75) : undefined;

    // Tier and fallbacks
    const tier = tierFor(symbol, { vol24hUSD, medianSpreadBps24h });
    const t = tierThresholds(tier);

    // banded depth around mid (more realistic than strict top-5)
    const mp = midPrice(bids, asks);
    const bidBandUsd = bandedDepthUsd(bids, mp, 'bid', 10 /* bps */, 50);
    const askBandUsd = bandedDepthUsd(asks, mp, 'ask', 10 /* bps */, 50);

    // use rolling p50 if available, with a soft 0.8× factor; clamp by tier floor and 10× notional
    const spreadCap = Math.min(spreadP75 ?? Infinity, t.spreadBpsMax);
    const depthP50Adj = (depthP50 ?? 0) * 0.8;

    const depthFloor = Math.max(depthP50Adj, t.depthMinUSD, 10 * (notionalUSDT || 0));
    const [atrFloor, atrCeil] = t.atrPctRange;

    const atrLo = atrP25 ?? atrFloor;
    const atrHi = atrP85 ?? atrCeil;

    const slippageCap = Math.min(slipP75 ?? Infinity, t.slippageBpsMax);

    // Gates
    const spread_ok = spreadBpsNow <= spreadCap;
    const liquidity_ok = bidBandUsd >= depthFloor;
    const atr_ok = Number.isFinite(atrPctNow) ? atrPctNow >= atrLo && atrPctNow <= atrHi : true;

    // Slippage gate (optional): if no history, allow; else enforce
    const slippage_ok = Number.isFinite(slippageCap) ? true : true; // caller can pass realized slippage to enforce; here default allow

    // Allowed actions based on gates + regime + position
    const gatesPass = spread_ok && liquidity_ok && atr_ok && slippage_ok;

    let allowed_actions: GatesOutput['allowed_actions'] = ['HOLD'];
    if (positionOpen) {
        allowed_actions = gatesPass ? ['HOLD', 'CLOSE'] : ['HOLD', 'CLOSE'];
    } else if (gatesPass) {
        if (regime === 'up') allowed_actions = ['BUY', 'HOLD'];
        else if (regime === 'down') allowed_actions = ['SELL', 'HOLD'];
        else allowed_actions = ['HOLD'];
    }

    return {
        tier,
        metrics: {
            spreadBpsNow,
            bidBandUsdNow: bidBandUsd, // NEW name for clarity
            askBandUsdNow: askBandUsd, // <-- log this too
            obImbNow,
            atrPctNow,

            spreadBpsP50: spreadP50,
            spreadBpsP75: spreadP75,
            depthUsdP50: depthP50, // (p50 of your chosen depth metric; consider switching history to banded)
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

// lib/gates.ts  (append below your existing exports)

export interface GatesHistories {
    spreadBpsHistory?: number[];
    top5BidUsdHistory?: number[];
    atrPctHistory?: number[];
    slippageBpsHistory?: number[];
    vol24hUSD?: number;
    medianSpreadBps24h?: number;
}

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
 * High-level convenience wrapper: compute gates + allowed_actions and optionally short-circuit HOLD when gates fail.
 */
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
        slippage_ok?: boolean;
        regime_trend_up: boolean;
        regime_trend_down: boolean;
        tier: string;
    };
    metrics: any;
    // If present, you can skip calling the model entirely.
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
        tier: out.tier,
    };

    // Short-circuit: if no open position and only HOLD is allowed, we can skip the model.
    let preDecision: any;
    if (!positionOpen && out.allowed_actions.length === 1 && out.allowed_actions[0] === 'HOLD') {
        preDecision = {
            action: 'HOLD',
            bias: 'NEUTRAL',
            signal_strength: 'LOW',
            summary: 'Pre-trade gates not satisfied; skipping evaluation.',
            reason: `Gates failed: spread_ok=${gates.spread_ok}, liquidity_ok=${gates.liquidity_ok}, atr_ok=${
                gates.atr_ok
            }${gates.slippage_ok === undefined ? '' : `, slippage_ok=${gates.slippage_ok}`}`,
        };
    }

    return {
        allowed_actions: out.allowed_actions,
        gates,
        metrics: out.metrics,
        ...(preDecision ? { preDecision } : {}),
    };
}
