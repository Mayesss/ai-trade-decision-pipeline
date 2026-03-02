import type { NextApiRequest, NextApiResponse } from 'next';

import { fetchCapitalCandlesByEpic, fetchCapitalCandlesByEpicDateRange, resolveCapitalEpicRuntime } from '../../../../lib/capital';
import { requireAdminAccess } from '../../../../lib/admin';
import { type CandleHistoryBackend, loadScalpCandleHistory, normalizeHistoryTimeframe } from '../../../../lib/scalp/candleHistory';
import { defaultScalpReplayConfig, normalizeScalpReplayInput, runScalpReplay } from '../../../../lib/scalp/replay/harness';
import { pipSizeForScalpSymbol } from '../../../../lib/scalp/marketData';
import type { ScalpReplayInputFile, ScalpReplayRuntimeConfig } from '../../../../lib/scalp/replay/types';
import type { ScalpCandle } from '../../../../lib/scalp/types';

type BacktestRequestBody = {
  symbol?: string;
  fromTsMs?: number | string;
  toTsMs?: number | string;
  lookbackCandles?: number;
  lookbackPastValue?: number | string;
  lookbackPastUnit?: 'minutes' | 'hours' | 'days' | string;
  spreadPips?: number;
  spreadFactor?: number;
  slippagePips?: number;
  defaultSpreadPips?: number;
  executeMinutes?: number;
  preferStopWhenBothHit?: boolean;
  forceCloseAtEnd?: boolean;
  debug?: boolean | string;
  cachedSourceTimeframe?: string;
  cachedCandles?: unknown;
  useStoredHistory?: boolean | string;
  historyBackend?: 'file' | 'kv' | string;
  historyTimeframe?: string;
  strategy?: Partial<ScalpReplayRuntimeConfig['strategy']>;
};

const SOURCE_TIMEFRAME = '1m';
const SOURCE_TIMEFRAME_CANDIDATES = ['1m', '5m', '15m', '1h'] as const;
const MIN_LOOKBACK_CANDLES = 180;
const MAX_LOOKBACK_CANDLES = 1000;
const DEFAULT_LOOKBACK_CANDLES = 720;
const MAX_DATE_RANGE_DAYS = 90;
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const ONE_MINUTE_MS = 60_000;

type FetchAttemptStatus = 'ok' | 'empty' | 'prices_not_found' | 'error';

type FetchAttemptDiagnostic = {
  timeframe: string;
  mode: 'DATE_RANGE' | 'LOOKBACK';
  fetchPath: 'effective_range' | 'lookback_limit';
  status: FetchAttemptStatus;
  candles: number;
  durationMs: number;
  fromTsMs: number | null;
  toTsMs: number | null;
  lookbackLimit: number | null;
  errorMessage?: string;
};

type FallbackDiagnostics = {
  mode: 'DATE_RANGE' | 'LOOKBACK';
  dataFetchMode: 'DATE_RANGE' | 'LOOKBACK_DURATION' | 'LOOKBACK_CANDLES';
  attempts: FetchAttemptDiagnostic[];
  selectedTimeframe: string | null;
};

type BacktestError = Error & {
  code?: string;
  details?: Record<string, unknown>;
};

function toFinite(value: unknown): number | undefined {
  const n = Number(value);
  return Number.isFinite(n) ? n : undefined;
}

function toPositiveNumber(value: unknown, fallback: number): number {
  const n = toFinite(value);
  if (n === undefined || n <= 0) return fallback;
  return n;
}

function toNonNegativeNumber(value: unknown, fallback: number): number {
  const n = toFinite(value);
  if (n === undefined || n < 0) return fallback;
  return n;
}

function toPositiveInt(value: unknown, fallback: number): number {
  return Math.max(1, Math.floor(toPositiveNumber(value, fallback)));
}

function toNonNegativeInt(value: unknown, fallback: number): number {
  return Math.max(0, Math.floor(toNonNegativeNumber(value, fallback)));
}

function toBool(value: unknown, fallback: boolean): boolean {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true' || normalized === '1' || normalized === 'yes' || normalized === 'on') return true;
    if (normalized === 'false' || normalized === '0' || normalized === 'no' || normalized === 'off') return false;
  }
  return fallback;
}

function logBacktest(event: string, payload: Record<string, unknown>, enabled: boolean) {
  if (!enabled) return;
  try {
    console.info(`[scalp-backtest] ${JSON.stringify({ event, ...payload })}`);
  } catch {
    console.info('[scalp-backtest]', event, payload);
  }
}

function formatAttemptSummary(attempts: FetchAttemptDiagnostic[]): string {
  if (!attempts.length) return 'no attempts';
  return attempts
    .map((a) => `${a.timeframe}:${a.status}:${a.candles}`)
    .join(', ');
}

function attachBacktestError(
  message: string,
  code: string,
  details: Record<string, unknown>,
  cause?: unknown,
): BacktestError {
  const err = new Error(message) as BacktestError;
  err.code = code;
  err.details = details;
  if (cause instanceof Error && cause.stack) {
    err.stack = `${err.stack ?? ''}\nCaused by: ${cause.stack}`;
  }
  return err;
}

function parseBaseTf(value: unknown, fallback: ScalpReplayRuntimeConfig['strategy']['asiaBaseTf']) {
  const normalized = String(value || '')
    .trim()
    .toUpperCase();
  if (normalized === 'M1' || normalized === 'M3' || normalized === 'M5' || normalized === 'M15') return normalized;
  return fallback;
}

function parseConfirmTf(value: unknown, fallback: ScalpReplayRuntimeConfig['strategy']['confirmTf']) {
  const normalized = String(value || '')
    .trim()
    .toUpperCase();
  if (normalized === 'M1' || normalized === 'M3') return normalized;
  return fallback;
}

function parseClockMode(value: unknown, fallback: ScalpReplayRuntimeConfig['strategy']['sessionClockMode']) {
  const normalized = String(value || '')
    .trim()
    .toUpperCase();
  if (normalized === 'UTC_FIXED') return 'UTC_FIXED';
  if (normalized === 'LONDON_TZ') return 'LONDON_TZ';
  return fallback;
}

function parseIfvgEntryMode(value: unknown, fallback: ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode']) {
  const normalized = String(value || '')
    .trim()
    .toLowerCase();
  if (normalized === 'first_touch' || normalized === 'midline_touch' || normalized === 'full_fill') return normalized;
  return fallback;
}

function normalizeSymbol(value: unknown): string {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, '');
}

function parseTimestampMs(value: unknown): number | null {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? Math.floor(value) : null;
  }
  const raw = String(value || '').trim();
  if (!raw) return null;
  const asNumber = Number(raw);
  if (Number.isFinite(asNumber)) return Math.floor(asNumber);
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseLookbackUnit(value: unknown): 'minutes' | 'hours' | 'days' {
  const normalized = String(value || '')
    .trim()
    .toLowerCase();
  if (normalized === 'minutes') return 'minutes';
  if (normalized === 'hours') return 'hours';
  if (normalized === 'days') return 'days';
  return 'days';
}

function parseHistoryBackend(value: unknown): CandleHistoryBackend | undefined {
  const normalized = String(value || '')
    .trim()
    .toLowerCase();
  if (normalized === 'file') return 'file';
  if (normalized === 'kv') return 'kv';
  return undefined;
}

function unitToMs(unit: 'minutes' | 'hours' | 'days'): number {
  if (unit === 'minutes') return 60_000;
  if (unit === 'hours') return 60 * 60_000;
  return ONE_DAY_MS;
}

function isPricesNotFoundError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err || '');
  const lower = msg.toLowerCase();
  if (lower.includes('error.prices.not-found')) return true;
  if (lower.includes('prices.not-found')) return true;
  if (lower.includes('prices not found')) return true;
  return false;
}

function timeframeToMinutesLocal(tf: string): number {
  const normalized = String(tf || '').trim().toLowerCase();
  const match = normalized.match(/^(\\d+)([mhd])$/);
  if (!match) return 1;
  const amount = Number(match[1]);
  const unit = match[2];
  if (!Number.isFinite(amount) || amount <= 0) return 1;
  if (unit === 'm') return amount;
  if (unit === 'h') return amount * 60;
  return amount * 24 * 60;
}

function lookbackLimitForTimeframe(baseLookbackCandles: number, tf: string): number {
  const minutes = timeframeToMinutesLocal(tf);
  const scaled = Math.ceil(baseLookbackCandles / Math.max(1, minutes));
  return Math.max(MIN_LOOKBACK_CANDLES, Math.min(MAX_LOOKBACK_CANDLES, scaled + 30));
}

function toRawRowsFromStoredHistory(rows: ScalpCandle[]): any[] {
  return rows.map((row) => [row[0], row[1], row[2], row[3], row[4], row[5]]);
}

function selectStoredHistoryRows(params: {
  rows: ScalpCandle[];
  hasEffectiveRange: boolean;
  fromTsMs: number | null;
  toTsMs: number | null;
  lookbackCandles: number;
  timeframe: string;
}): any[] {
  const sorted = params.rows.slice().sort((a, b) => a[0] - b[0]);
  if (!sorted.length) return [];
  if (params.hasEffectiveRange && params.fromTsMs !== null && params.toTsMs !== null) {
    const filtered = sorted.filter((row) => row[0] >= params.fromTsMs! && row[0] <= params.toTsMs!);
    return toRawRowsFromStoredHistory(filtered);
  }
  const limit = lookbackLimitForTimeframe(params.lookbackCandles, params.timeframe);
  return toRawRowsFromStoredHistory(sorted.slice(-limit));
}

async function fetchReplayCandlesWithFallback(params: {
  epic: string;
  lookbackCandles: number;
  requestedMode: 'DATE_RANGE' | 'LOOKBACK';
  dataFetchMode: 'DATE_RANGE' | 'LOOKBACK_DURATION' | 'LOOKBACK_CANDLES';
  hasEffectiveRange: boolean;
  fromTsMs: number | null;
  toTsMs: number | null;
  debugEnabled: boolean;
}): Promise<{
  candles: any[];
  sourceTimeframe: string;
  fallbackUsed: boolean;
  attempted: string[];
  diagnostics: FallbackDiagnostics;
}> {
  const mode: 'DATE_RANGE' | 'LOOKBACK' = params.requestedMode;
  const fetchPath: 'effective_range' | 'lookback_limit' = params.hasEffectiveRange ? 'effective_range' : 'lookback_limit';
  const attempted: string[] = [];
  const attempts: FetchAttemptDiagnostic[] = [];
  for (let i = 0; i < SOURCE_TIMEFRAME_CANDIDATES.length; i += 1) {
    const tf = SOURCE_TIMEFRAME_CANDIDATES[i]!;
    attempted.push(tf);
    const startedAtMs = Date.now();
    const lookbackLimit = params.hasEffectiveRange ? null : lookbackLimitForTimeframe(params.lookbackCandles, tf);
    try {
      const rows = params.hasEffectiveRange
        ? await fetchCapitalCandlesByEpicDateRange(params.epic, tf, params.fromTsMs!, params.toTsMs!, {
            maxPerRequest: MAX_LOOKBACK_CANDLES,
            maxRequests: 220,
            debug: params.debugEnabled,
            debugLabel: `${params.epic}:${tf}`,
          })
        : await fetchCapitalCandlesByEpic(params.epic, tf, lookbackLimit!);
      const candles = Array.isArray(rows) ? rows.length : 0;
      const status: FetchAttemptStatus = candles > 0 ? 'ok' : 'empty';
      const attempt: FetchAttemptDiagnostic = {
        timeframe: tf,
        mode,
        fetchPath,
        status,
        candles,
        durationMs: Date.now() - startedAtMs,
        fromTsMs: params.hasEffectiveRange ? params.fromTsMs : null,
        toTsMs: params.hasEffectiveRange ? params.toTsMs : null,
        lookbackLimit,
      };
      attempts.push(attempt);
      logBacktest('capital_fetch_attempt', attempt, params.debugEnabled);
      if (Array.isArray(rows) && rows.length > 0) {
        return {
          candles: rows,
          sourceTimeframe: tf,
          fallbackUsed: i > 0,
          attempted,
          diagnostics: {
            mode,
            dataFetchMode: params.dataFetchMode,
            attempts,
            selectedTimeframe: tf,
          },
        };
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err || 'unknown error');
      const pricesNotFound = isPricesNotFoundError(err);
      const attempt: FetchAttemptDiagnostic = {
        timeframe: tf,
        mode,
        fetchPath,
        status: pricesNotFound ? 'prices_not_found' : 'error',
        candles: 0,
        durationMs: Date.now() - startedAtMs,
        fromTsMs: params.hasEffectiveRange ? params.fromTsMs : null,
        toTsMs: params.hasEffectiveRange ? params.toTsMs : null,
        lookbackLimit,
        errorMessage: message,
      };
      attempts.push(attempt);
      logBacktest('capital_fetch_attempt', attempt, params.debugEnabled);
      if (!pricesNotFound) {
        throw attachBacktestError(
          `Capital fetch failed at timeframe ${tf}: ${message}`,
          'capital_fetch_failed',
          {
            mode,
            dataFetchMode: params.dataFetchMode,
            attempted,
            attempts,
          },
          err,
        );
      }
    }
  }
  throw attachBacktestError(
    `Capital prices unavailable for this symbol/time window. Attempts: ${formatAttemptSummary(attempts)}. ` +
      `The selected window may include closed-market periods; try ending the range during active market hours.`,
    'no_prices_in_range',
    {
      mode,
      dataFetchMode: params.dataFetchMode,
      attempted,
      attempts,
    },
  );
}

function applyRuntimeOverrides(runtime: ScalpReplayRuntimeConfig, body: BacktestRequestBody): ScalpReplayRuntimeConfig {
  const next: ScalpReplayRuntimeConfig = JSON.parse(JSON.stringify(runtime));
  const strategy = body.strategy || {};

  next.executeMinutes = toPositiveInt(body.executeMinutes, next.executeMinutes);
  next.spreadFactor = toPositiveNumber(body.spreadFactor, next.spreadFactor);
  next.slippagePips = toNonNegativeNumber(body.slippagePips, next.slippagePips);
  next.defaultSpreadPips = toNonNegativeNumber(body.defaultSpreadPips, next.defaultSpreadPips);
  next.preferStopWhenBothHit = toBool(body.preferStopWhenBothHit, next.preferStopWhenBothHit);
  next.forceCloseAtEnd = toBool(body.forceCloseAtEnd, next.forceCloseAtEnd);

  next.strategy.sessionClockMode = parseClockMode(strategy.sessionClockMode, next.strategy.sessionClockMode);
  next.strategy.asiaBaseTf = parseBaseTf(strategy.asiaBaseTf, next.strategy.asiaBaseTf);
  next.strategy.confirmTf = parseConfirmTf(strategy.confirmTf, next.strategy.confirmTf);
  next.strategy.maxTradesPerDay = toPositiveInt(strategy.maxTradesPerDay, next.strategy.maxTradesPerDay);
  next.strategy.riskPerTradePct = toPositiveNumber(strategy.riskPerTradePct, next.strategy.riskPerTradePct);
  next.strategy.referenceEquityUsd = toPositiveNumber(strategy.referenceEquityUsd, next.strategy.referenceEquityUsd);
  next.strategy.minNotionalUsd = toPositiveNumber(strategy.minNotionalUsd, next.strategy.minNotionalUsd);
  next.strategy.maxNotionalUsd = toPositiveNumber(strategy.maxNotionalUsd, next.strategy.maxNotionalUsd);
  next.strategy.takeProfitR = toPositiveNumber(strategy.takeProfitR, next.strategy.takeProfitR);
  next.strategy.stopBufferPips = toNonNegativeNumber(strategy.stopBufferPips, next.strategy.stopBufferPips);
  next.strategy.stopBufferSpreadMult = toNonNegativeNumber(strategy.stopBufferSpreadMult, next.strategy.stopBufferSpreadMult);
  next.strategy.minStopDistancePips = toPositiveNumber(strategy.minStopDistancePips, next.strategy.minStopDistancePips);
  next.strategy.sweepBufferPips = toNonNegativeNumber(strategy.sweepBufferPips, next.strategy.sweepBufferPips);
  next.strategy.sweepBufferAtrMult = toNonNegativeNumber(strategy.sweepBufferAtrMult, next.strategy.sweepBufferAtrMult);
  next.strategy.sweepBufferSpreadMult = toNonNegativeNumber(strategy.sweepBufferSpreadMult, next.strategy.sweepBufferSpreadMult);
  next.strategy.sweepRejectInsidePips = toNonNegativeNumber(strategy.sweepRejectInsidePips, next.strategy.sweepRejectInsidePips);
  next.strategy.sweepRejectMaxBars = toPositiveInt(strategy.sweepRejectMaxBars, next.strategy.sweepRejectMaxBars);
  next.strategy.sweepMinWickBodyRatio = toNonNegativeNumber(strategy.sweepMinWickBodyRatio, next.strategy.sweepMinWickBodyRatio);
  next.strategy.displacementBodyAtrMult = toNonNegativeNumber(strategy.displacementBodyAtrMult, next.strategy.displacementBodyAtrMult);
  next.strategy.displacementRangeAtrMult = toNonNegativeNumber(strategy.displacementRangeAtrMult, next.strategy.displacementRangeAtrMult);
  next.strategy.displacementCloseInExtremePct = toPositiveNumber(
    strategy.displacementCloseInExtremePct,
    next.strategy.displacementCloseInExtremePct,
  );
  next.strategy.mssLookbackBars = toPositiveInt(strategy.mssLookbackBars, next.strategy.mssLookbackBars);
  next.strategy.mssBreakBufferPips = toNonNegativeNumber(strategy.mssBreakBufferPips, next.strategy.mssBreakBufferPips);
  next.strategy.mssBreakBufferAtrMult = toNonNegativeNumber(strategy.mssBreakBufferAtrMult, next.strategy.mssBreakBufferAtrMult);
  next.strategy.confirmTtlMinutes = toPositiveInt(strategy.confirmTtlMinutes, next.strategy.confirmTtlMinutes);
  next.strategy.ifvgMinAtrMult = toNonNegativeNumber(strategy.ifvgMinAtrMult, next.strategy.ifvgMinAtrMult);
  next.strategy.ifvgMaxAtrMult = toPositiveNumber(strategy.ifvgMaxAtrMult, next.strategy.ifvgMaxAtrMult);
  next.strategy.ifvgTtlMinutes = toPositiveInt(strategy.ifvgTtlMinutes, next.strategy.ifvgTtlMinutes);
  next.strategy.ifvgEntryMode = parseIfvgEntryMode(strategy.ifvgEntryMode, next.strategy.ifvgEntryMode);
  next.strategy.atrPeriod = toPositiveInt(strategy.atrPeriod, next.strategy.atrPeriod);
  next.strategy.minAsiaCandles = toPositiveInt(strategy.minAsiaCandles, next.strategy.minAsiaCandles);
  next.strategy.minBaseCandles = toPositiveInt(strategy.minBaseCandles, next.strategy.minBaseCandles);
  next.strategy.minConfirmCandles = toPositiveInt(strategy.minConfirmCandles, next.strategy.minConfirmCandles);

  return next;
}

function toReplayInputCandles(rawCandles: any[], spreadPips: number): ScalpReplayInputFile['candles'] {
  const mapped: Array<ScalpReplayInputFile['candles'][number] | null> = rawCandles.map((row) => {
      const ts = Number(row?.[0]);
      const open = Number(row?.[1]);
      const high = Number(row?.[2]);
      const low = Number(row?.[3]);
      const close = Number(row?.[4]);
      const volume = Number(row?.[5] ?? 0);
      if (![ts, open, high, low, close].every((v) => Number.isFinite(v) && v > 0)) return null;
      return {
        ts,
        open,
        high,
        low,
        close,
        volume: Number.isFinite(volume) ? volume : 0,
        spreadPips,
      };
    });
  return mapped
    .filter((c): c is ScalpReplayInputFile['candles'][number] => c !== null)
    .sort((a, b) => Number(a.ts) - Number(b.ts));
}

function normalizeCachedCandleRows(
  value: unknown,
  params: {
    hasEffectiveRange: boolean;
    fromTsMs: number | null;
    toTsMs: number | null;
  },
): any[] {
  if (!Array.isArray(value)) return [];
  const rows: any[] = [];
  for (const item of value) {
    const row = (item || {}) as Record<string, unknown>;
    let ts = Number(row.ts ?? row.time);
    if (!Number.isFinite(ts)) continue;
    // UI chart uses epoch seconds; normalize to ms.
    if (ts > 0 && ts < 1_000_000_000_000) ts *= 1000;
    const open = Number(row.open);
    const high = Number(row.high);
    const low = Number(row.low);
    const close = Number(row.close);
    const volume = Number(row.volume ?? 0);
    if (![ts, open, high, low, close].every((v) => Number.isFinite(v) && v > 0)) continue;
    rows.push([Math.floor(ts), open, high, low, close, Number.isFinite(volume) ? volume : 0]);
  }
  rows.sort((a, b) => Number(a[0]) - Number(b[0]));
  const deduped: any[] = [];
  let lastTs = -1;
  for (const row of rows) {
    const ts = Number(row[0]);
    if (!(Number.isFinite(ts) && ts > 0)) continue;
    if (ts === lastTs) {
      deduped[deduped.length - 1] = row;
      continue;
    }
    deduped.push(row);
    lastTs = ts;
  }
  if (!params.hasEffectiveRange || params.fromTsMs === null || params.toTsMs === null) return deduped;
  return deduped.filter((row) => {
    const ts = Number(row[0]);
    return Number.isFinite(ts) && ts >= params.fromTsMs! && ts <= params.toTsMs!;
  });
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use POST' });
  }
  if (!requireAdminAccess(req, res)) return;

  try {
    const body = (req.body || {}) as BacktestRequestBody;
    const debugEnabled = toBool(body.debug, process.env.SCALP_BACKTEST_DEBUG === '1');
    const symbol = normalizeSymbol(body.symbol);
    if (!symbol) {
      return res.status(400).json({ error: 'symbol_required', message: 'Provide a valid symbol (e.g. EURUSD).' });
    }

    const lookbackCandlesRaw = toPositiveInt(body.lookbackCandles, DEFAULT_LOOKBACK_CANDLES);
    const lookbackCandles = Math.max(MIN_LOOKBACK_CANDLES, Math.min(MAX_LOOKBACK_CANDLES, lookbackCandlesRaw));
    const lookbackPastValue = toPositiveNumber(body.lookbackPastValue, 3);
    const lookbackPastUnit = parseLookbackUnit(body.lookbackPastUnit);
    const fromTsMs = parseTimestampMs(body.fromTsMs);
    const toTsMs = parseTimestampMs(body.toTsMs);
    const hasDateRange = fromTsMs !== null && toTsMs !== null && toTsMs > fromTsMs;
    const requestedMode: 'DATE_RANGE' | 'LOOKBACK' = hasDateRange ? 'DATE_RANGE' : 'LOOKBACK';
    const nowTsMs = Math.floor(Date.now() / ONE_MINUTE_MS) * ONE_MINUTE_MS;
    const computedLookbackMs = lookbackPastValue * unitToMs(lookbackPastUnit);
    const hasLookbackDuration = Number.isFinite(computedLookbackMs) && computedLookbackMs > 0;
    const effectiveRequestedFromTsMs = hasDateRange
      ? Math.floor(fromTsMs! / ONE_MINUTE_MS) * ONE_MINUTE_MS
      : hasLookbackDuration
      ? Math.floor((nowTsMs - computedLookbackMs) / ONE_MINUTE_MS) * ONE_MINUTE_MS
      : null;
    const effectiveRequestedToTsMs = hasDateRange
      ? Math.floor(toTsMs! / ONE_MINUTE_MS) * ONE_MINUTE_MS
      : hasLookbackDuration
      ? nowTsMs
      : null;
    const hasEffectiveRange =
      effectiveRequestedFromTsMs !== null &&
      effectiveRequestedToTsMs !== null &&
      effectiveRequestedToTsMs > effectiveRequestedFromTsMs;
    const dataFetchMode: 'DATE_RANGE' | 'LOOKBACK_DURATION' | 'LOOKBACK_CANDLES' = hasEffectiveRange
      ? hasDateRange
        ? 'DATE_RANGE'
        : 'LOOKBACK_DURATION'
      : 'LOOKBACK_CANDLES';
    logBacktest(
      'request',
      {
        symbol,
        mode: requestedMode,
        dataFetchMode,
        hasEffectiveRange,
        fromTsMs: effectiveRequestedFromTsMs,
        toTsMs: effectiveRequestedToTsMs,
        lookbackPastValue: hasDateRange ? null : lookbackPastValue,
        lookbackPastUnit: hasDateRange ? null : lookbackPastUnit,
        lookbackCandlesRaw,
      },
      debugEnabled,
    );
    const clampedBySourceLimit = !hasEffectiveRange && lookbackCandles !== lookbackCandlesRaw;
    if (hasEffectiveRange) {
      const rangeDays = Math.abs(effectiveRequestedToTsMs - effectiveRequestedFromTsMs) / ONE_DAY_MS;
      if (rangeDays > MAX_DATE_RANGE_DAYS) {
        return res.status(400).json({
          error: 'date_range_too_wide',
          message: `Date range too wide (${rangeDays.toFixed(1)}d). Max allowed is ${MAX_DATE_RANGE_DAYS}d per run.`,
        });
      }
    }

    const runtimeBase = defaultScalpReplayConfig(symbol);
    const runtime = applyRuntimeOverrides(runtimeBase, body);
    runtime.symbol = symbol;

    const spreadPips = toNonNegativeNumber(body.spreadPips, runtime.defaultSpreadPips);
    runtime.defaultSpreadPips = spreadPips;

    const resolved = await resolveCapitalEpicRuntime(symbol);
    logBacktest(
      'epic_resolved',
      {
        symbol,
        epic: resolved.epic,
        source: resolved.source,
      },
      debugEnabled,
    );
    const cachedRows = normalizeCachedCandleRows(body.cachedCandles, {
      hasEffectiveRange,
      fromTsMs: effectiveRequestedFromTsMs,
      toTsMs: effectiveRequestedToTsMs,
    });
    const cachedSourceTf = String(body.cachedSourceTimeframe || '')
      .trim()
      .toLowerCase();
    const useCachedRows = cachedRows.length >= MIN_LOOKBACK_CANDLES;
    const useStoredHistory = toBool(body.useStoredHistory, false);
    const requestedHistoryBackend = parseHistoryBackend(body.historyBackend);
    const requestedHistoryTf = normalizeHistoryTimeframe(String(body.historyTimeframe || cachedSourceTf || SOURCE_TIMEFRAME));
    let storedHistoryRows: any[] = [];
    let storedHistoryDiagnostics: FetchAttemptDiagnostic | null = null;
    if (!useCachedRows && useStoredHistory) {
      const startedAtMs = Date.now();
      try {
        const storedHistory = await loadScalpCandleHistory(symbol, requestedHistoryTf, { backend: requestedHistoryBackend });
        storedHistoryRows = selectStoredHistoryRows({
          rows: storedHistory.record?.candles ?? [],
          hasEffectiveRange,
          fromTsMs: effectiveRequestedFromTsMs,
          toTsMs: effectiveRequestedToTsMs,
          lookbackCandles,
          timeframe: requestedHistoryTf,
        });
        storedHistoryDiagnostics = {
          timeframe: `history_store:${requestedHistoryTf}`,
          mode: requestedMode,
          fetchPath: hasEffectiveRange ? ('effective_range' as const) : ('lookback_limit' as const),
          status: storedHistoryRows.length > 0 ? ('ok' as const) : ('empty' as const),
          candles: storedHistoryRows.length,
          durationMs: Date.now() - startedAtMs,
          fromTsMs: hasEffectiveRange ? effectiveRequestedFromTsMs : null,
          toTsMs: hasEffectiveRange ? effectiveRequestedToTsMs : null,
          lookbackLimit: hasEffectiveRange ? null : lookbackLimitForTimeframe(lookbackCandles, requestedHistoryTf),
        };
        logBacktest('stored_history_attempt', storedHistoryDiagnostics, debugEnabled);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err || 'unknown error');
        storedHistoryDiagnostics = {
          timeframe: `history_store:${requestedHistoryTf}`,
          mode: requestedMode,
          fetchPath: hasEffectiveRange ? ('effective_range' as const) : ('lookback_limit' as const),
          status: 'error',
          candles: 0,
          durationMs: Date.now() - startedAtMs,
          fromTsMs: hasEffectiveRange ? effectiveRequestedFromTsMs : null,
          toTsMs: hasEffectiveRange ? effectiveRequestedToTsMs : null,
          lookbackLimit: hasEffectiveRange ? null : lookbackLimitForTimeframe(lookbackCandles, requestedHistoryTf),
          errorMessage: message,
        };
        logBacktest('stored_history_attempt', storedHistoryDiagnostics, debugEnabled);
      }
    }
    const useStoredHistoryRows = !useCachedRows && storedHistoryRows.length >= MIN_LOOKBACK_CANDLES;

    const fetched = useCachedRows
      ? {
          candles: cachedRows,
          sourceTimeframe: cachedSourceTf || 'cached',
          fallbackUsed: false,
          attempted: ['ui_cache'],
          diagnostics: {
            mode: requestedMode,
            dataFetchMode,
            attempts: [
              {
                timeframe: `ui_cache:${cachedSourceTf || 'cached'}`,
                mode: requestedMode,
                fetchPath: hasEffectiveRange ? ('effective_range' as const) : ('lookback_limit' as const),
                status: 'ok' as const,
                candles: cachedRows.length,
                durationMs: 0,
                fromTsMs: hasEffectiveRange ? effectiveRequestedFromTsMs : null,
                toTsMs: hasEffectiveRange ? effectiveRequestedToTsMs : null,
                lookbackLimit: null,
              },
            ],
            selectedTimeframe: cachedSourceTf || 'cached',
          },
        }
      : useStoredHistoryRows
      ? {
          candles: storedHistoryRows,
          sourceTimeframe: requestedHistoryTf,
          fallbackUsed: false,
          attempted: [`history_store:${requestedHistoryTf}`],
          diagnostics: {
            mode: requestedMode,
            dataFetchMode,
            attempts: [storedHistoryDiagnostics!],
            selectedTimeframe: requestedHistoryTf,
          },
        }
      : await fetchReplayCandlesWithFallback({
          epic: resolved.epic,
          lookbackCandles,
          requestedMode,
          dataFetchMode,
          hasEffectiveRange,
          fromTsMs: effectiveRequestedFromTsMs,
          toTsMs: effectiveRequestedToTsMs,
          debugEnabled,
        });
    const candleSource = useCachedRows ? 'ui_cache' : useStoredHistoryRows ? 'history_store' : 'capital_api';
    logBacktest(
      useCachedRows ? 'using_cached_candles' : useStoredHistoryRows ? 'using_history_store_candles' : 'using_capital_candles',
      {
        symbol,
        sourceTimeframe: fetched.sourceTimeframe,
        candles: fetched.candles.length,
      },
      debugEnabled,
    );
    const rawCandles = fetched.candles;
    if (!Array.isArray(rawCandles) || rawCandles.length < MIN_LOOKBACK_CANDLES) {
      return res.status(422).json({
        error: 'insufficient_candles',
        message: `Need at least ${MIN_LOOKBACK_CANDLES} candles to backtest. Received ${Array.isArray(rawCandles) ? rawCandles.length : 0}.`,
      });
    }

    const replayInput: ScalpReplayInputFile = {
      symbol,
      pipSize: pipSizeForScalpSymbol(symbol),
      candles: toReplayInputCandles(rawCandles, spreadPips),
    };
    const normalized = normalizeScalpReplayInput(replayInput);
    const replay = runScalpReplay({
      candles: normalized.candles,
      pipSize: normalized.pipSize,
      config: runtime,
    });
    const reasonCodeCounts = replay.timeline.reduce<Record<string, number>>((acc, event) => {
      const codes = Array.isArray(event.reasonCodes) ? event.reasonCodes : [];
      for (const code of codes) {
        const key = String(code || '').trim().toUpperCase();
        if (!key) continue;
        acc[key] = (acc[key] || 0) + 1;
      }
      return acc;
    }, {});
    const topReasonCodes = Object.entries(reasonCodeCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 20)
      .map(([code, count]) => ({ code, count }));
    const stateCounts = replay.timeline.reduce<Record<string, number>>((acc, event) => {
      if (event.type !== 'state' || !event.state) return acc;
      const key = String(event.state).trim().toUpperCase();
      if (!key) return acc;
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
    logBacktest(
      'replay_complete',
      {
        symbol,
        sourceTimeframe: fetched.sourceTimeframe,
        fetchedCandles: normalized.candles.length,
        trades: replay.summary.trades,
        winRatePct: replay.summary.winRatePct,
        netR: replay.summary.netR,
      },
      debugEnabled,
    );

    const chartCandles = normalized.candles.map((c) => ({
      time: Math.floor(c.ts / 1000),
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
      volume: c.volume,
    }));

    const markers = replay.trades.flatMap((trade) => {
      const entryTime = Math.floor(trade.entryTs / 1000);
      const exitTime = Math.floor(trade.exitTs / 1000);
      const entryColor = trade.side === 'BUY' ? '#22c55e' : '#ef4444';
      const exitColor = trade.rMultiple >= 0 ? '#14b8a6' : '#f97316';
      return [
        {
          time: entryTime,
          position: trade.side === 'BUY' ? ('belowBar' as const) : ('aboveBar' as const),
          color: entryColor,
          shape: trade.side === 'BUY' ? ('arrowUp' as const) : ('arrowDown' as const),
          text: `ENTRY ${trade.side}`,
        },
        {
          time: exitTime,
          position: trade.side === 'BUY' ? ('aboveBar' as const) : ('belowBar' as const),
          color: exitColor,
          shape: trade.side === 'BUY' ? ('arrowDown' as const) : ('arrowUp' as const),
          text: `EXIT ${trade.exitReason} ${trade.rMultiple.toFixed(2)}R`,
        },
      ];
    });

    const tradeSegments = replay.trades.map((trade) => ({
      id: trade.id,
      side: trade.side,
      entryTime: Math.floor(trade.entryTs / 1000),
      exitTime: Math.floor(trade.exitTs / 1000),
      entryPrice: trade.entryPrice,
      exitPrice: trade.exitPrice,
      stopPrice: trade.stopPrice,
      takeProfitPrice: trade.takeProfitPrice,
      rMultiple: trade.rMultiple,
      pnlUsd: trade.pnlUsd,
      exitReason: trade.exitReason,
      holdMinutes: trade.holdMinutes,
    }));

    return res.status(200).json({
      symbol,
      epic: resolved.epic,
      candleSource,
      historyEnabled: useStoredHistory,
      historyBackendRequested: requestedHistoryBackend || 'auto',
      historyTimeframeRequested: requestedHistoryTf,
      historyRowsAvailable: storedHistoryRows.length,
      mappingSource: resolved.source,
      sourceTimeframe: fetched.sourceTimeframe,
      sourceFallbackUsed: fetched.fallbackUsed,
      attemptedSourceTimeframes: fetched.attempted,
      fetchDiagnostics: fetched.diagnostics,
      rangeMode: requestedMode,
      dataFetchMode,
      requestedFromTsMs: hasEffectiveRange ? effectiveRequestedFromTsMs : null,
      requestedToTsMs: hasEffectiveRange ? effectiveRequestedToTsMs : null,
      requestedLookbackPastValue: hasDateRange ? null : lookbackPastValue,
      requestedLookbackPastUnit: hasDateRange ? null : lookbackPastUnit,
      effectiveFromTsMs: normalized.candles[0]?.ts ?? null,
      effectiveToTsMs: normalized.candles[normalized.candles.length - 1]?.ts ?? null,
      requestedLookbackCandles: lookbackCandlesRaw,
      fetchedCandles: normalized.candles.length,
      clampedBySourceLimit,
      pipSize: normalized.pipSize,
      summary: replay.summary,
      trades: replay.trades,
      timeline: replay.timeline.slice(-1500),
      chart: {
        candles: chartCandles,
        markers,
        tradeSegments,
      },
      effectiveConfig: runtime,
      diagnostics: {
        topReasonCodes,
        stateCounts,
      },
      debugEnabled,
    });
  } catch (err: any) {
    const typedErr = err as BacktestError;
    const message = String(typedErr?.message || 'Unknown scalp backtest failure');
    const code = String(typedErr?.code || '');
    const details = typedErr?.details ?? {};
    console.error(
      `[scalp-backtest] ${JSON.stringify({
        event: 'error',
        code: code || 'scalp_backtest_failed',
        message,
        details,
      })}`,
    );
    if (code === 'no_prices_in_range' || isPricesNotFoundError(err) || message.includes('Capital prices unavailable for this symbol/time window')) {
      return res.status(422).json({
        error: code || 'no_prices_in_range',
        message,
        details,
      });
    }
    return res.status(500).json({
      error: code || 'scalp_backtest_failed',
      message,
      details,
    });
  }
}
