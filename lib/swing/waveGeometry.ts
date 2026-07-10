// Wave geometry for the swing prompt: WHERE inside the current wave/channel is
// price? The prompt already carries structure (BOS/CHoCH), levels and
// EMA-extension, but no wave shape — which is how entries landed on wave
// crests in clean trends ("bought the peak of a bullish wave"). This module
// turns raw candles into compact measurements (per the measurements-not-
// verdicts prompt rule): a regression channel (slope + position within it),
// swing-pivot trendlines, and the last swing high/low. Pure functions, no I/O;
// tolerant of both venues' candle shapes ([ts,o,h,l,c,...] arrays or objects).

type Ohlc = { high: number; low: number; close: number };

export type TrendlineMeasure = {
    // Where the trendline sits RIGHT NOW (projected to the latest bar).
    price_now: number;
    // Trendline slope per bar, normalized by ATR (+0.10 = rising 0.1 ATR/bar).
    slope_atr: number;
    // Number of swing pivots the line is fitted through (2–4).
    touches: number;
};

export type SwingPointMeasure = {
    price: number;
    // Signed distance from current close in ATR (positive = above price).
    dist_atr: number;
    bars_ago: number;
};

export type WaveGeometry = {
    // Regression slope of closes per bar, in ATR. ~0 = flat, sign = direction.
    slope_atr: number;
    // Position of the last close inside the regression channel: 0 = channel
    // low, 1 = channel high. Low values in an up-slope = wave trough.
    channel_pos: number;
    channel_width_atr: number;
    support_trendline: TrendlineMeasure | null;
    resistance_trendline: TrendlineMeasure | null;
    last_swing_high: SwingPointMeasure | null;
    last_swing_low: SwingPointMeasure | null;
};

export type NanoContext = WaveGeometry & {
    bias: 'UP' | 'DOWN' | 'NEUTRAL';
    // From the last two swing highs + lows: HH_HL (up), LH_LL (down), mixed.
    structure: 'HH_HL' | 'LH_LL' | 'mixed' | null;
    // (close − EMA20) / ATR on this timeframe.
    extension_atr: number;
};

const round = (x: number, d = 3): number => Number(x.toFixed(d));
const roundPrice = (x: number): number => Number(x.toPrecision(7));

function normalizeOhlc(raw: unknown): Ohlc[] {
    return (Array.isArray(raw) ? raw : [])
        .map((c: any): Ohlc | null => {
            const high = Number(Array.isArray(c) ? c[2] : c?.high);
            const low = Number(Array.isArray(c) ? c[3] : c?.low);
            const close = Number(Array.isArray(c) ? c[4] : c?.close);
            if (!Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close)) return null;
            return { high, low, close };
        })
        .filter((c): c is Ohlc => c !== null);
}

function emaSeries(values: number[], period: number): number[] {
    const k = 2 / (period + 1);
    const out: number[] = [];
    let prev = values[0] ?? 0;
    for (let i = 0; i < values.length; i++) {
        prev = i === 0 ? values[0] : values[i] * k + prev * (1 - k);
        out.push(prev);
    }
    return out;
}

function atr(candles: Ohlc[], period = 14): number | null {
    if (candles.length < period + 1) return null;
    const trs: number[] = [];
    for (let i = 1; i < candles.length; i++) {
        const prevClose = candles[i - 1].close;
        trs.push(
            Math.max(
                candles[i].high - candles[i].low,
                Math.abs(candles[i].high - prevClose),
                Math.abs(candles[i].low - prevClose),
            ),
        );
    }
    const window = trs.slice(-period);
    const value = window.reduce((s, v) => s + v, 0) / window.length;
    return Number.isFinite(value) && value > 0 ? value : null;
}

function linreg(points: Array<{ x: number; y: number }>): { slope: number; intercept: number } | null {
    const n = points.length;
    if (n < 2) return null;
    let sx = 0;
    let sy = 0;
    let sxx = 0;
    let sxy = 0;
    for (const p of points) {
        sx += p.x;
        sy += p.y;
        sxx += p.x * p.x;
        sxy += p.x * p.y;
    }
    const denom = n * sxx - sx * sx;
    if (denom === 0) return null;
    const slope = (n * sxy - sx * sy) / denom;
    return { slope, intercept: (sy - slope * sx) / n };
}

type Pivot = { index: number; price: number; kind: 'high' | 'low' };

// Fractal pivots: a bar whose high (low) exceeds (undercuts) the k bars on
// each side. The last k bars can't confirm a pivot yet — by construction.
export function findPivots(candles: Ohlc[], k = 2): Pivot[] {
    const out: Pivot[] = [];
    for (let i = k; i < candles.length - k; i++) {
        let isHigh = true;
        let isLow = true;
        for (let j = 1; j <= k; j++) {
            if (candles[i].high <= candles[i - j].high || candles[i].high < candles[i + j].high) isHigh = false;
            if (candles[i].low >= candles[i - j].low || candles[i].low > candles[i + j].low) isLow = false;
            if (!isHigh && !isLow) break;
        }
        if (isHigh) out.push({ index: i, price: candles[i].high, kind: 'high' });
        if (isLow) out.push({ index: i, price: candles[i].low, kind: 'low' });
    }
    return out;
}

function fitTrendline(
    pivots: Pivot[],
    kind: 'high' | 'low',
    lastIndex: number,
    atrValue: number,
    maxTouches = 4,
): TrendlineMeasure | null {
    const pts = pivots.filter((p) => p.kind === kind).slice(-maxTouches);
    if (pts.length < 2) return null;
    const fit = linreg(pts.map((p) => ({ x: p.index, y: p.price })));
    if (!fit) return null;
    const priceNow = fit.intercept + fit.slope * lastIndex;
    if (!Number.isFinite(priceNow) || priceNow <= 0) return null;
    return {
        price_now: roundPrice(priceNow),
        slope_atr: round(fit.slope / atrValue),
        touches: pts.length,
    };
}

function swingPoint(
    pivots: Pivot[],
    kind: 'high' | 'low',
    lastIndex: number,
    close: number,
    atrValue: number,
): SwingPointMeasure | null {
    const p = pivots.filter((x) => x.kind === kind).at(-1);
    if (!p) return null;
    return {
        price: roundPrice(p.price),
        dist_atr: round((p.price - close) / atrValue),
        bars_ago: lastIndex - p.index,
    };
}

// Geometry over the last `window` bars of one timeframe. Returns null when the
// series is too short to say anything honest (<30 bars).
export function computeWaveGeometry(rawCandles: unknown, window = 80): WaveGeometry | null {
    const all = normalizeOhlc(rawCandles);
    if (all.length < 30) return null;
    const candles = all.slice(-Math.max(30, window));
    const atrValue = atr(candles);
    if (!atrValue) return null;
    const lastIndex = candles.length - 1;
    const close = candles[lastIndex].close;

    const fit = linreg(candles.map((c, i) => ({ x: i, y: c.close })));
    if (!fit) return null;
    // Channel = regression line shifted to the extreme residuals, so it hugs
    // the actual wave envelope rather than a fixed ±kσ band.
    let maxAbove = 0;
    let maxBelow = 0;
    for (let i = 0; i < candles.length; i++) {
        const resid = candles[i].close - (fit.intercept + fit.slope * i);
        if (resid > maxAbove) maxAbove = resid;
        if (resid < maxBelow) maxBelow = resid;
    }
    const lineNow = fit.intercept + fit.slope * lastIndex;
    const upper = lineNow + maxAbove;
    const lower = lineNow + maxBelow;
    const width = upper - lower;
    const channelPos = width > 0 ? (close - lower) / width : 0.5;

    const pivots = findPivots(candles);
    return {
        slope_atr: round(fit.slope / atrValue),
        channel_pos: round(Math.max(0, Math.min(1, channelPos)), 2),
        channel_width_atr: round(width / atrValue, 1),
        support_trendline: fitTrendline(pivots, 'low', lastIndex, atrValue),
        resistance_trendline: fitTrendline(pivots, 'high', lastIndex, atrValue),
        last_swing_high: swingPoint(pivots, 'high', lastIndex, close, atrValue),
        last_swing_low: swingPoint(pivots, 'low', lastIndex, close, atrValue),
    };
}

function classifyStructure(pivots: Pivot[]): NanoContext['structure'] {
    const highs = pivots.filter((p) => p.kind === 'high').slice(-2);
    const lows = pivots.filter((p) => p.kind === 'low').slice(-2);
    if (highs.length < 2 || lows.length < 2) return null;
    const hh = highs[1].price > highs[0].price;
    const hl = lows[1].price > lows[0].price;
    if (hh && hl) return 'HH_HL';
    if (!hh && !hl) return 'LH_LL';
    return 'mixed';
}

// Nano (15m) entry-timing context: the wave geometry plus a cheap bias,
// structure label and EMA20 extension on the nano timeframe itself.
export function computeNanoContext(rawCandles: unknown, window = 96): NanoContext | null {
    const geometry = computeWaveGeometry(rawCandles, window);
    if (!geometry) return null;
    const candles = normalizeOhlc(rawCandles).slice(-Math.max(30, window));
    const closes = candles.map((c) => c.close);
    const ema20 = emaSeries(closes, 20);
    const atrValue = atr(candles);
    if (!atrValue) return null;
    const last = closes.length - 1;
    const close = closes[last];
    const emaNow = ema20[last];
    const emaSlope = last >= 5 ? emaNow - ema20[last - 5] : 0;
    const bias: NanoContext['bias'] =
        close > emaNow && emaSlope > 0 ? 'UP' : close < emaNow && emaSlope < 0 ? 'DOWN' : 'NEUTRAL';
    return {
        ...geometry,
        bias,
        structure: classifyStructure(findPivots(candles)),
        extension_atr: round((close - emaNow) / atrValue, 2),
    };
}
