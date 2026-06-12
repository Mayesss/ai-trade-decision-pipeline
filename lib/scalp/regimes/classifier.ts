import {
  SCALP_V4_ONE_WEEK_MS,
  startOfUtcWeekMondayMs,
  validityWeekStartFromCompletedWeekMs,
} from "./week";
import type {
  ScalpRegimeAxisBucket,
  ScalpRegimeCandle,
  ScalpRegimeCellId,
  ScalpRegimeClassifierOptions,
  ScalpRegimeMarketContext,
  ScalpRegimeRawRegimeLabel,
  ScalpRegimeSnapshot,
  ScalpRegimeRiskAxis,
  ScalpRegimeTrendAxis,
  ScalpRegimeVenue,
  ScalpRegimeWeeklyBar,
} from "./types";

export const SCALP_V4_CLASSIFIER_VERSION = "scalp_v4_macro_weekly_r1";

const DEFAULTS = {
  minVolLookbackWeeks: 26,
  preferredVolLookbackWeeks: 52,
  hysteresisWeeks: 4,
  trendFastWeeks: 20,
  trendSlowWeeks: 60,
  adxWeeks: 14,
};

function finite(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function mean(values: number[]): number {
  return values.length ? values.reduce((acc, row) => acc + row, 0) / values.length : 0;
}

function percentileRank(values: number[], current: number): number | null {
  const rows = values.filter((row) => Number.isFinite(row)).sort((a, b) => a - b);
  if (!rows.length || !Number.isFinite(current)) return null;
  let count = 0;
  for (const row of rows) {
    if (row <= current) count += 1;
  }
  return (count / rows.length) * 100;
}

function axisFromPctile(pctile: number | null): ScalpRegimeAxisBucket {
  if (pctile === null) return "unknown";
  if (pctile < 33.333) return "low";
  if (pctile < 66.667) return "mid";
  return "high";
}

function cellId(volAxis: ScalpRegimeAxisBucket, trendAxis: ScalpRegimeTrendAxis, riskAxis: ScalpRegimeRiskAxis): ScalpRegimeCellId {
  if (volAxis === "unknown" || trendAxis === "unknown" || riskAxis === "unknown") return "unknown";
  return `vol=${volAxis}|trend=${trendAxis}|risk=${riskAxis}` as ScalpRegimeCellId;
}

export function normalizeScalpRegimeCandles(rows: Array<ScalpRegimeCandle | [number, number, number, number, number, number?]>): ScalpRegimeCandle[] {
  const normalized: ScalpRegimeCandle[] = [];
  for (const row of rows || []) {
    const raw = Array.isArray(row)
      ? { ts: row[0], open: row[1], high: row[2], low: row[3], close: row[4], volume: row[5] }
      : row;
    const ts = finite(raw.ts);
    const open = finite(raw.open);
    const high = finite(raw.high);
    const low = finite(raw.low);
    const close = finite(raw.close);
    const volume = finite(raw.volume, 0);
    if (![ts, open, high, low, close].every((v) => Number.isFinite(v) && v > 0)) continue;
    normalized.push({ ts: Math.floor(ts), open, high, low, close, volume });
  }
  return normalized.sort((a, b) => a.ts - b.ts);
}

export function buildScalpRegimeWeeklyBars(candles: Array<ScalpRegimeCandle | [number, number, number, number, number, number?]>): ScalpRegimeWeeklyBar[] {
  const byWeek = new Map<number, ScalpRegimeCandle[]>();
  for (const candle of normalizeScalpRegimeCandles(candles)) {
    const weekStartMs = startOfUtcWeekMondayMs(candle.ts);
    const bucket = byWeek.get(weekStartMs) || [];
    bucket.push(candle);
    byWeek.set(weekStartMs, bucket);
  }
  return Array.from(byWeek.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([weekStartMs, rows]) => {
      rows.sort((a, b) => a.ts - b.ts);
      return {
        weekStartMs,
        open: rows[0]!.open,
        high: Math.max(...rows.map((row) => row.high)),
        low: Math.min(...rows.map((row) => row.low)),
        close: rows[rows.length - 1]!.close,
        volume: rows.reduce((acc, row) => acc + finite(row.volume), 0),
      };
    });
}

function trueRange(bar: ScalpRegimeWeeklyBar, prevClose: number | null): number {
  if (prevClose === null || !Number.isFinite(prevClose)) return bar.high - bar.low;
  return Math.max(bar.high - bar.low, Math.abs(bar.high - prevClose), Math.abs(bar.low - prevClose));
}

function atrPctSeries(bars: ScalpRegimeWeeklyBar[], period: number): Array<number | null> {
  const tr: number[] = [];
  for (let idx = 0; idx < bars.length; idx += 1) {
    tr.push(trueRange(bars[idx]!, idx > 0 ? bars[idx - 1]!.close : null));
  }
  return bars.map((bar, idx) => {
    if (idx + 1 < period) return null;
    const atr = mean(tr.slice(idx + 1 - period, idx + 1));
    return bar.close > 0 ? (atr / bar.close) * 100 : null;
  });
}

function sma(bars: ScalpRegimeWeeklyBar[], idx: number, period: number): number | null {
  if (idx + 1 < period) return null;
  return mean(bars.slice(idx + 1 - period, idx + 1).map((row) => row.close));
}

function adxSeries(bars: ScalpRegimeWeeklyBar[], period: number): Array<number | null> {
  const dx: Array<number | null> = [null];
  for (let idx = 1; idx < bars.length; idx += 1) {
    const prev = bars[idx - 1]!;
    const cur = bars[idx]!;
    const upMove = cur.high - prev.high;
    const downMove = prev.low - cur.low;
    const plusDm = upMove > downMove && upMove > 0 ? upMove : 0;
    const minusDm = downMove > upMove && downMove > 0 ? downMove : 0;
    const tr = trueRange(cur, prev.close);
    if (tr <= 0) {
      dx.push(null);
      continue;
    }
    const plusDi = (plusDm / tr) * 100;
    const minusDi = (minusDm / tr) * 100;
    const denom = plusDi + minusDi;
    dx.push(denom > 0 ? (Math.abs(plusDi - minusDi) / denom) * 100 : 0);
  }
  return bars.map((_bar, idx) => {
    const window = dx.slice(Math.max(0, idx + 1 - period), idx + 1).filter((row): row is number => row !== null);
    return window.length >= period ? mean(window) : null;
  });
}

function resolveTrendAxis(bars: ScalpRegimeWeeklyBar[], idx: number, opts: typeof DEFAULTS): { axis: ScalpRegimeTrendAxis; strength: number | null } {
  const fast = sma(bars, idx, opts.trendFastWeeks);
  const slow = sma(bars, idx, opts.trendSlowWeeks);
  const prevFast = sma(bars, idx - 1, opts.trendFastWeeks);
  const prevSlow = sma(bars, idx - 1, opts.trendSlowWeeks);
  const adx = adxSeries(bars, opts.adxWeeks)[idx];
  if ([fast, slow, prevFast, prevSlow, adx].some((row) => row === null)) {
    return { axis: "unknown", strength: null };
  }
  const slope = ((fast! - prevFast!) / Math.max(1e-9, prevFast!)) + ((slow! - prevSlow!) / Math.max(1e-9, prevSlow!));
  const separation = (fast! - slow!) / Math.max(1e-9, slow!);
  const strength = Math.abs(slope) * 100 + Math.abs(separation) * 25 + adx! / 100;
  if (adx! < 18 || Math.abs(separation) < 0.0025) return { axis: "choppy", strength };
  if (fast! > slow! && slope > 0) return { axis: "trending_up", strength };
  if (fast! < slow! && slope < 0) return { axis: "trending_down", strength };
  return { axis: "choppy", strength };
}

function weeklyDirection(bars: ScalpRegimeWeeklyBar[], validWeekStartMs: number, lookbackWeeks: number): number | null {
  const completedWeekStart = validWeekStartMs - SCALP_V4_ONE_WEEK_MS;
  const idx = bars.findIndex((row) => row.weekStartMs === completedWeekStart);
  if (idx < lookbackWeeks || idx < 0) return null;
  const prev = bars[idx - lookbackWeeks]!;
  const cur = bars[idx]!;
  if (prev.close <= 0) return null;
  return (cur.close - prev.close) / prev.close;
}

function resolveFxRiskAxis(validWeekStartMs: number, ctx: ScalpRegimeMarketContext): { axis: ScalpRegimeRiskAxis; strength: number | null; weeks: number } {
  const usdJpy = ctx.usdJpy || [];
  const audJpy = ctx.audJpy || [];
  const u = weeklyDirection(usdJpy, validWeekStartMs, 4);
  const a = weeklyDirection(audJpy, validWeekStartMs, 4);
  const weeks = Math.min(usdJpy.length, audJpy.length);
  if (u === null || a === null) return { axis: "unknown", strength: null, weeks };
  const strength = (Math.abs(u) + Math.abs(a)) * 100;
  if (u > 0.002 && a > 0.002) return { axis: "risk_on", strength, weeks };
  if (u < -0.002 && a < -0.002) return { axis: "risk_off", strength, weeks };
  return { axis: "neutral", strength, weeks };
}

function resolveCryptoRiskAxis(validWeekStartMs: number, ctx: ScalpRegimeMarketContext): { axis: ScalpRegimeRiskAxis; strength: number | null; weeks: number } {
  const btc = ctx.btcUsdt || [];
  const completedWeekStart = validWeekStartMs - SCALP_V4_ONE_WEEK_MS;
  const idx = btc.findIndex((row) => row.weekStartMs === completedWeekStart);
  if (idx < 26) return { axis: "unknown", strength: null, weeks: btc.length };
  const atrPct = atrPctSeries(btc, 14);
  const currentVol = atrPct[idx];
  const volRank = percentileRank(
    atrPct.slice(Math.max(0, idx - 52), idx + 1).filter((row): row is number => row !== null),
    currentVol ?? Number.NaN,
  );
  const trend = weeklyDirection(btc, validWeekStartMs, 4);
  if (volRank === null || trend === null) return { axis: "unknown", strength: null, weeks: btc.length };
  const strength = Math.abs(trend) * 100 + Math.abs(volRank - 50) / 50;
  if (trend > 0.01 && volRank < 80) return { axis: "risk_on", strength, weeks: btc.length };
  if (trend < -0.01 || volRank >= 85) return { axis: "risk_off", strength, weeks: btc.length };
  return { axis: "neutral", strength, weeks: btc.length };
}

export function classifyScalpRegimeRawRegimes(params: {
  venue: ScalpRegimeVenue;
  symbol: string;
  weeklyBars: ScalpRegimeWeeklyBar[];
  marketContext?: ScalpRegimeMarketContext;
  options?: ScalpRegimeClassifierOptions;
}): ScalpRegimeRawRegimeLabel[] {
  const opts = { ...DEFAULTS, ...(params.options || {}) };
  const classifierVersion = params.options?.classifierVersion || SCALP_V4_CLASSIFIER_VERSION;
  const bars = (params.weeklyBars || []).slice().sort((a, b) => a.weekStartMs - b.weekStartMs);
  const atrPct = atrPctSeries(bars, 14);
  const out: ScalpRegimeRawRegimeLabel[] = [];
  for (let idx = 0; idx < bars.length; idx += 1) {
    const completedBar = bars[idx]!;
    const validWeekStartMs = validityWeekStartFromCompletedWeekMs(completedBar.weekStartMs);
    const lookback = atrPct
      .slice(Math.max(0, idx + 1 - opts.preferredVolLookbackWeeks), idx + 1)
      .filter((row): row is number => row !== null);
    const currentAtrPct = atrPct[idx];
    const volPctile =
      lookback.length >= opts.minVolLookbackWeeks && currentAtrPct !== null
        ? percentileRank(lookback, currentAtrPct)
        : null;
    const volAxis = axisFromPctile(volPctile);
    const trend = resolveTrendAxis(bars, idx, opts);
    const risk =
      params.venue === "capital"
        ? resolveFxRiskAxis(validWeekStartMs, params.marketContext || {})
        : resolveCryptoRiskAxis(validWeekStartMs, params.marketContext || {});
    const rawCellId = cellId(volAxis, trend.axis, risk.axis);
    const warmupEnoughForKnown =
      idx + 1 >= Math.max(opts.minVolLookbackWeeks, opts.trendSlowWeeks, opts.adxWeeks) &&
      risk.weeks >= (params.venue === "capital" ? 4 : 26);
    out.push({
      weekStartMs: validWeekStartMs,
      classifierVersion,
      venue: params.venue,
      symbol: String(params.symbol || "").trim().toUpperCase(),
      volAxis,
      trendAxis: trend.axis,
      riskAxis: risk.axis,
      rawCellId,
      confidence: {
        volDistancePct: volPctile === null ? null : Math.min(Math.abs(volPctile - 33.333), Math.abs(volPctile - 66.667)),
        trendStrength: trend.strength,
        riskStrength: risk.strength,
      },
      sourceCoverage: {
        symbolWeeks: idx + 1,
        riskWeeks: risk.weeks,
        warmupComplete: Boolean(rawCellId && rawCellId !== ("unknown" as ScalpRegimeCellId)),
        postWarmupUnknown: rawCellId === "unknown" && warmupEnoughForKnown,
      },
      details: {
        completedWeekStartMs: completedBar.weekStartMs,
        atrPct: currentAtrPct,
        volPctile,
      },
    });
  }
  return out;
}

export function applyScalpRegimeHysteresis(
  rawLabels: ScalpRegimeRawRegimeLabel[],
  options: Pick<ScalpRegimeClassifierOptions, "hysteresisWeeks"> = {},
): ScalpRegimeSnapshot[] {
  const required = Math.max(1, Math.floor(options.hysteresisWeeks || DEFAULTS.hysteresisWeeks));
  let confirmed: ScalpRegimeCellId | null = null;
  let pending: ScalpRegimeCellId | null = null;
  let pendingWeeks = 0;
  const out: ScalpRegimeSnapshot[] = [];
  for (const raw of rawLabels.slice().sort((a, b) => a.weekStartMs - b.weekStartMs)) {
    let transition: ScalpRegimeSnapshot["transition"] = null;
    if (raw.rawCellId === "unknown") {
      out.push({ ...raw, cellId: confirmed || "unknown", pendingCellId: pending, pendingWeeks, transition });
      continue;
    }
    if (confirmed === null) {
      confirmed = raw.rawCellId;
      pending = null;
      pendingWeeks = 0;
      transition = { fromCellId: null, toCellId: confirmed };
    } else if (raw.rawCellId === confirmed) {
      pending = null;
      pendingWeeks = 0;
    } else if (raw.rawCellId === pending) {
      pendingWeeks += 1;
      if (pendingWeeks >= required) {
        transition = { fromCellId: confirmed, toCellId: raw.rawCellId };
        confirmed = raw.rawCellId;
        pending = null;
        pendingWeeks = 0;
      }
    } else {
      pending = raw.rawCellId;
      pendingWeeks = 1;
    }
    out.push({
      ...raw,
      cellId: confirmed || "unknown",
      pendingCellId: pending,
      pendingWeeks,
      transition,
    });
  }
  return out;
}

export function countScalpRegimeEpochs(snapshots: ScalpRegimeSnapshot[]): number {
  let epochs = 0;
  let prev: ScalpRegimeCellId | null = null;
  for (const row of snapshots) {
    if (row.cellId === "unknown") continue;
    if (row.cellId !== prev) {
      epochs += 1;
      prev = row.cellId;
    }
  }
  return epochs;
}
