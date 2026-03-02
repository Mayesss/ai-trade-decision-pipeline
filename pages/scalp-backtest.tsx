import Head from 'next/head';
import dynamic from 'next/dynamic';
import React, { useEffect, useMemo, useRef, useState } from 'react';

import type {
  ScalpBacktestChartCandle,
  ScalpBacktestChartMarker,
  ScalpBacktestTradeSegment,
} from '../components/ScalpBacktestChart';
import type { ScalpReplaySummary, ScalpReplayTrade } from '../lib/scalp/replay/types';

type BacktestAsset = {
  symbol: string;
  epic: string;
  category: 'forex' | 'crypto' | 'index' | 'commodity' | 'equity' | 'other';
};

type BacktestAssetsResponse = {
  count: number;
  assets: BacktestAsset[];
};

type BacktestMode = 'LOOKBACK' | 'DATE_RANGE';
type AggressivityLevel = 'conservative' | 'medium' | 'aggressive';

type BacktestRunResponse = {
  symbol: string;
  epic: string;
  candleSource?: 'ui_cache' | 'capital_api';
  mappingSource?: 'env' | 'default' | 'passthrough' | 'discovered';
  sourceTimeframe: string;
  sourceFallbackUsed?: boolean;
  attemptedSourceTimeframes?: string[];
  dataFetchMode?: 'DATE_RANGE' | 'LOOKBACK_DURATION' | 'LOOKBACK_CANDLES';
  fetchDiagnostics?: {
    mode: BacktestMode;
    dataFetchMode?: 'DATE_RANGE' | 'LOOKBACK_DURATION' | 'LOOKBACK_CANDLES';
    selectedTimeframe: string | null;
    attempts: Array<{
      timeframe: string;
      mode: BacktestMode;
      fetchPath?: 'effective_range' | 'lookback_limit';
      status: 'ok' | 'empty' | 'prices_not_found' | 'error';
      candles: number;
      durationMs: number;
      errorMessage?: string;
    }>;
  };
  rangeMode?: BacktestMode;
  requestedFromTsMs?: number | null;
  requestedToTsMs?: number | null;
  effectiveFromTsMs?: number | null;
  effectiveToTsMs?: number | null;
  requestedLookbackCandles: number;
  fetchedCandles: number;
  clampedBySourceLimit: boolean;
  summary: ScalpReplaySummary;
  trades: ScalpReplayTrade[];
  chart: {
    candles: ScalpBacktestChartCandle[];
    markers: ScalpBacktestChartMarker[];
    tradeSegments: ScalpBacktestTradeSegment[];
  };
  diagnostics?: {
    topReasonCodes?: Array<{ code: string; count: number }>;
    stateCounts?: Record<string, number>;
  };
  effectiveConfig: any;
};

type CachedCandleEntry = {
  candles: ScalpBacktestChartCandle[];
  sourceTimeframe: string;
  cachedAtMs: number;
};

type FormState = {
  aggressivity: AggressivityLevel;
  backtestMode: BacktestMode;
  rangeStartLocal: string;
  rangeEndLocal: string;
  lookbackPastValue: string;
  lookbackPastUnit: 'minutes' | 'hours' | 'days';
  lookbackCandles: string;
  executeMinutes: string;
  spreadPips: string;
  spreadFactor: string;
  slippagePips: string;
  asiaBaseTf: 'M1' | 'M3' | 'M5' | 'M15';
  confirmTf: 'M1' | 'M3';
  riskPerTradePct: string;
  takeProfitR: string;
  maxTradesPerDay: string;
  sweepBufferPips: string;
  sweepRejectMaxBars: string;
  displacementBodyAtrMult: string;
  displacementRangeAtrMult: string;
  mssLookbackBars: string;
  ifvgMinAtrMult: string;
  ifvgMaxAtrMult: string;
  ifvgEntryMode: 'first_touch' | 'midline_touch' | 'full_fill';
  debugVerbose: boolean;
};

type SavedPreset = {
  id: string;
  name: string;
  symbol: string;
  form: FormState;
  createdAtMs: number;
  updatedAtMs: number;
};

type ComparedRun = {
  id: string;
  label: string;
  color: string;
  visible: boolean;
  result: BacktestRunResponse;
};

const ADMIN_SECRET_STORAGE_KEY = 'admin_access_secret';
const PRESETS_STORAGE_KEY = 'scalp_backtest_presets_v1';
const COMPARE_COLORS = ['#f59e0b', '#a855f7', '#10b981', '#3b82f6', '#ef4444', '#eab308', '#14b8a6'];
const CANDLE_CACHE_TTL_MS = 20 * 60_000;
const AGGRESSIVITY_LEVELS: AggressivityLevel[] = ['conservative', 'medium', 'aggressive'];
const DEFAULT_AGGRESSIVITY_LEVEL: AggressivityLevel = 'medium';

const AGGRESSIVITY_PRESETS: Record<
  AggressivityLevel,
  Pick<
    FormState,
    | 'takeProfitR'
    | 'maxTradesPerDay'
    | 'sweepBufferPips'
    | 'sweepRejectMaxBars'
    | 'displacementBodyAtrMult'
    | 'displacementRangeAtrMult'
    | 'mssLookbackBars'
    | 'ifvgMinAtrMult'
    | 'ifvgMaxAtrMult'
  >
> = {
  conservative: {
    takeProfitR: '2.0',
    maxTradesPerDay: '2',
    sweepBufferPips: '1.0',
    sweepRejectMaxBars: '6',
    displacementBodyAtrMult: '0.25',
    displacementRangeAtrMult: '0.40',
    mssLookbackBars: '2',
    ifvgMinAtrMult: '0.02',
    ifvgMaxAtrMult: '0.80',
  },
  medium: {
    takeProfitR: '1.5',
    maxTradesPerDay: '4',
    sweepBufferPips: '0.50',
    sweepRejectMaxBars: '12',
    displacementBodyAtrMult: '0.12',
    displacementRangeAtrMult: '0.22',
    mssLookbackBars: '1',
    ifvgMinAtrMult: '0.01',
    ifvgMaxAtrMult: '1.50',
  },
  aggressive: {
    takeProfitR: '1.2',
    maxTradesPerDay: '6',
    sweepBufferPips: '0.10',
    sweepRejectMaxBars: '20',
    displacementBodyAtrMult: '0.05',
    displacementRangeAtrMult: '0.10',
    mssLookbackBars: '1',
    ifvgMinAtrMult: '0.00',
    ifvgMaxAtrMult: '3.00',
  },
};

const ScalpBacktestChart = dynamic(() => import('../components/ScalpBacktestChart'), {
  ssr: false,
  loading: () => (
    <div className="h-[520px] w-full animate-pulse rounded-2xl border border-slate-700 bg-slate-900/70" />
  ),
});

function pad2(value: number): string {
  return String(value).padStart(2, '0');
}

function toLocalDateTimeValue(tsMs: number): string {
  const d = new Date(tsMs);
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}T${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
}

const nowMs = Date.now();
const DEFAULT_FORM: FormState = {
  aggressivity: DEFAULT_AGGRESSIVITY_LEVEL,
  backtestMode: 'LOOKBACK',
  rangeStartLocal: toLocalDateTimeValue(nowMs - 3 * 24 * 60 * 60 * 1000),
  rangeEndLocal: toLocalDateTimeValue(nowMs),
  lookbackPastValue: '3',
  lookbackPastUnit: 'days',
  lookbackCandles: '720',
  executeMinutes: '3',
  spreadPips: '1.1',
  spreadFactor: '1',
  slippagePips: '0.15',
  asiaBaseTf: 'M1',
  confirmTf: 'M1',
  riskPerTradePct: '0.35',
  ...AGGRESSIVITY_PRESETS[DEFAULT_AGGRESSIVITY_LEVEL],
  ifvgEntryMode: 'first_touch',
  debugVerbose: false,
};

function normalizeForm(input: Partial<FormState> | null | undefined): FormState {
  const value = input || {};
  const mode = value.backtestMode === 'DATE_RANGE' ? 'DATE_RANGE' : 'LOOKBACK';
  const aggressivity: AggressivityLevel =
    value.aggressivity === 'conservative' || value.aggressivity === 'medium' || value.aggressivity === 'aggressive'
      ? value.aggressivity
      : DEFAULT_AGGRESSIVITY_LEVEL;
  return {
    ...DEFAULT_FORM,
    ...value,
    aggressivity,
    backtestMode: mode,
    lookbackPastUnit:
      value.lookbackPastUnit === 'minutes' || value.lookbackPastUnit === 'hours' || value.lookbackPastUnit === 'days'
        ? value.lookbackPastUnit
        : DEFAULT_FORM.lookbackPastUnit,
    asiaBaseTf:
      value.asiaBaseTf === 'M1' || value.asiaBaseTf === 'M3' || value.asiaBaseTf === 'M5' || value.asiaBaseTf === 'M15'
        ? value.asiaBaseTf
        : DEFAULT_FORM.asiaBaseTf,
    confirmTf: value.confirmTf === 'M1' || value.confirmTf === 'M3' ? value.confirmTf : DEFAULT_FORM.confirmTf,
    ifvgEntryMode:
      value.ifvgEntryMode === 'first_touch' || value.ifvgEntryMode === 'midline_touch' || value.ifvgEntryMode === 'full_fill'
        ? value.ifvgEntryMode
        : DEFAULT_FORM.ifvgEntryMode,
    debugVerbose: Boolean(value.debugVerbose),
  };
}

function levelFromSliderIndex(value: number): AggressivityLevel {
  if (value <= 0) return 'conservative';
  if (value >= 2) return 'aggressive';
  return 'medium';
}

function sliderIndexFromLevel(level: AggressivityLevel): number {
  return AGGRESSIVITY_LEVELS.indexOf(level);
}

function formatRunErrorMessage(payload: { message?: string; details?: any; error?: string }, status: number): string {
  const base = payload?.message || `Backtest request failed (${status})`;
  const attempts = payload?.details?.attempts;
  if (!Array.isArray(attempts) || attempts.length === 0) return base;
  const compact = attempts
    .map((row: any) => {
      const tf = String(row?.timeframe || '?');
      const statusValue = String(row?.status || 'unknown');
      const candles = Number(row?.candles || 0);
      return `${tf}:${statusValue}:${candles}`;
    })
    .join(', ');
  return `${base} | attempts=${compact}`;
}

function buildCandleCacheKey(symbol: string, form: FormState): string | null {
  const normalizedSymbol = String(symbol || '').trim().toUpperCase();
  if (!normalizedSymbol) return null;
  if (form.backtestMode === 'DATE_RANGE') {
    const fromMs = parseLocalDateTimeToMs(form.rangeStartLocal);
    const toMs = parseLocalDateTimeToMs(form.rangeEndLocal);
    if (!(Number.isFinite(fromMs as number) && Number.isFinite(toMs as number) && (toMs as number) > (fromMs as number))) return null;
    return `${normalizedSymbol}|DATE_RANGE|${fromMs}|${toMs}`;
  }
  const lookbackValue = toNumber(form.lookbackPastValue, 3);
  if (!(Number.isFinite(lookbackValue) && lookbackValue > 0)) return null;
  return `${normalizedSymbol}|LOOKBACK|${lookbackValue}|${form.lookbackPastUnit}`;
}

function lookbackUnitToMinutes(unit: FormState['lookbackPastUnit']): number {
  if (unit === 'minutes') return 1;
  if (unit === 'hours') return 60;
  return 60 * 24;
}

function toNumber(value: string, fallback: number): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toPositiveInt(value: string, fallback: number): number {
  const n = Math.floor(toNumber(value, fallback));
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return n;
}

function parseLocalDateTimeToMs(value: string): number | null {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const ts = Date.parse(raw);
  return Number.isFinite(ts) ? ts : null;
}

function formatSigned(value: number, digits = 2): string {
  if (!Number.isFinite(value)) return '—';
  return `${value >= 0 ? '+' : ''}${value.toFixed(digits)}`;
}

function formatUsd(value: number): string {
  if (!Number.isFinite(value)) return '—';
  return `${value >= 0 ? '+' : '-'}$${Math.abs(value).toFixed(2)}`;
}

function formatTs(tsMs: number | null | undefined): string {
  if (!Number.isFinite(tsMs as number)) return '—';
  const d = new Date(Number(tsMs));
  return d.toLocaleString('en-GB', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function parseStoredPresets(raw: string | null): SavedPreset[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw) as any;
    const rows = Array.isArray(parsed) ? parsed : [];
    return rows
      .map((row) => {
        const id = String(row?.id || '').trim();
        const name = String(row?.name || '').trim();
        const symbol = String(row?.symbol || '').trim().toUpperCase();
        if (!id || !name || !symbol) return null;
        return {
          id,
          name,
          symbol,
          form: normalizeForm(row?.form),
          createdAtMs: Number.isFinite(Number(row?.createdAtMs)) ? Number(row.createdAtMs) : Date.now(),
          updatedAtMs: Number.isFinite(Number(row?.updatedAtMs)) ? Number(row.updatedAtMs) : Date.now(),
        } satisfies SavedPreset;
      })
      .filter((row): row is SavedPreset => Boolean(row))
      .sort((a, b) => b.updatedAtMs - a.updatedAtMs);
  } catch {
    return [];
  }
}

export default function ScalpBacktestPage() {
  const [adminReady, setAdminReady] = useState(false);
  const [adminGranted, setAdminGranted] = useState(false);
  const [adminSecret, setAdminSecret] = useState('');
  const [adminInput, setAdminInput] = useState('');
  const [adminError, setAdminError] = useState<string | null>(null);
  const [adminSubmitting, setAdminSubmitting] = useState(false);

  const [assets, setAssets] = useState<BacktestAsset[]>([]);
  const [assetLoading, setAssetLoading] = useState(false);
  const [assetError, setAssetError] = useState<string | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState('EURUSD');

  const [form, setForm] = useState<FormState>(DEFAULT_FORM);
  const [runLoading, setRunLoading] = useState(false);
  const [runError, setRunError] = useState<string | null>(null);
  const [result, setResult] = useState<BacktestRunResponse | null>(null);

  const [presets, setPresets] = useState<SavedPreset[]>([]);
  const [selectedPresetId, setSelectedPresetId] = useState('');
  const [presetName, setPresetName] = useState('');

  const [comparedRuns, setComparedRuns] = useState<ComparedRun[]>([]);
  const [compareLabel, setCompareLabel] = useState('');
  const candleCacheRef = useRef<Map<string, CachedCandleEntry>>(new Map());

  const categoryCounts = useMemo(() => {
    return assets.reduce<Record<string, number>>((acc, row) => {
      acc[row.category] = (acc[row.category] || 0) + 1;
      return acc;
    }, {});
  }, [assets]);

  const selectedAsset = useMemo(() => assets.find((a) => a.symbol === selectedSymbol) || null, [assets, selectedSymbol]);

  const estimatedRangeCandles = useMemo(() => {
    if (form.backtestMode === 'LOOKBACK') {
      const amount = toNumber(form.lookbackPastValue, 3);
      if (!Number.isFinite(amount) || amount <= 0) return null;
      return Math.floor(amount * lookbackUnitToMinutes(form.lookbackPastUnit)) + 1;
    }
    const fromMs = parseLocalDateTimeToMs(form.rangeStartLocal);
    const toMs = parseLocalDateTimeToMs(form.rangeEndLocal);
    if (fromMs === null || toMs === null || toMs <= fromMs) return null;
    return Math.floor((toMs - fromMs) / 60_000) + 1;
  }, [form.backtestMode, form.lookbackPastValue, form.lookbackPastUnit, form.rangeStartLocal, form.rangeEndLocal]);

  const chartTrades = useMemo(() => {
    if (!result) return [] as ScalpBacktestTradeSegment[];
    const primary = result.chart.tradeSegments.map((trade) => ({
      ...trade,
      id: `primary:${trade.id}`,
      lineStyle: 0,
      runLabel: 'current',
    }));
    const compared = comparedRuns
      .filter((run) => run.visible)
      .flatMap((run) =>
        run.result.chart.tradeSegments.map((trade) => ({
          ...trade,
          id: `${run.id}:${trade.id}`,
          color: run.color,
          lineStyle: 2,
          runLabel: run.label,
        })),
      );
    return [...primary, ...compared];
  }, [result, comparedRuns]);

  const loadAssets = async (secret: string) => {
    setAssetLoading(true);
    setAssetError(null);
    try {
      const res = await fetch('/api/scalp/backtest/assets', {
        headers: secret ? { 'x-admin-access-secret': secret } : undefined,
      });
      const payload = (await res.json().catch(() => ({}))) as BacktestAssetsResponse & { message?: string };
      if (!res.ok) {
        throw new Error(payload?.message || `assets request failed (${res.status})`);
      }
      const rows = Array.isArray(payload.assets) ? payload.assets : [];
      setAssets(rows);
      if (rows.length && !rows.some((r) => r.symbol === selectedSymbol)) {
        setSelectedSymbol(rows[0]!.symbol);
      }
    } catch (err: any) {
      setAssetError(err?.message || 'Failed to load Capital assets.');
      setAssets([]);
    } finally {
      setAssetLoading(false);
    }
  };

  const validateAdmin = async (secret: string): Promise<boolean> => {
    const trimmed = secret.trim();
    if (!trimmed) return false;
    try {
      const res = await fetch('/api/admin-auth', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ secret: trimmed }),
      });
      if (!res.ok) return false;
      const payload = await res.json().catch(() => ({}));
      return Boolean(payload?.ok);
    } catch {
      return false;
    }
  };

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const parsed = parseStoredPresets(window.localStorage.getItem(PRESETS_STORAGE_KEY));
    setPresets(parsed);
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    try {
      window.localStorage.setItem(PRESETS_STORAGE_KEY, JSON.stringify(presets));
    } catch {
      // noop
    }
  }, [presets]);

  useEffect(() => {
    const stored = typeof window !== 'undefined' ? window.localStorage.getItem(ADMIN_SECRET_STORAGE_KEY) || '' : '';
    if (!stored.trim()) {
      setAdminReady(true);
      return;
    }
    let cancelled = false;
    (async () => {
      const ok = await validateAdmin(stored);
      if (cancelled) return;
      if (ok) {
        setAdminGranted(true);
        setAdminSecret(stored.trim());
        setAdminInput(stored.trim());
        setAdminError(null);
      } else {
        if (typeof window !== 'undefined') window.localStorage.removeItem(ADMIN_SECRET_STORAGE_KEY);
        setAdminGranted(false);
        setAdminSecret('');
        setAdminInput('');
        setAdminError('Stored admin secret is invalid. Re-enter it.');
      }
      setAdminReady(true);
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!adminGranted || !adminSecret) return;
    void loadAssets(adminSecret);
  }, [adminGranted, adminSecret]);

  const submitAdminSecret = async () => {
    const candidate = adminInput.trim();
    if (!candidate) {
      setAdminError('Enter ADMIN_ACCESS_SECRET first.');
      return;
    }
    setAdminSubmitting(true);
    setAdminError(null);
    try {
      const ok = await validateAdmin(candidate);
      if (!ok) {
        setAdminGranted(false);
        setAdminSecret('');
        setAdminError('Invalid admin secret.');
        return;
      }
      if (typeof window !== 'undefined') {
        window.localStorage.setItem(ADMIN_SECRET_STORAGE_KEY, candidate);
      }
      setAdminGranted(true);
      setAdminSecret(candidate);
      setAdminError(null);
      await loadAssets(candidate);
    } finally {
      setAdminSubmitting(false);
      setAdminReady(true);
    }
  };

  const savePreset = () => {
    const name = presetName.trim();
    if (!name) {
      setRunError('Preset name is required before saving.');
      return;
    }

    const now = Date.now();
    const existing = presets.find((p) => p.id === selectedPresetId) || null;
    const id = existing?.id || `preset_${now}_${Math.random().toString(16).slice(2, 8)}`;
    const nextPreset: SavedPreset = {
      id,
      name,
      symbol: selectedSymbol,
      form: normalizeForm(form),
      createdAtMs: existing?.createdAtMs ?? now,
      updatedAtMs: now,
    };

    setPresets((prev) => {
      const without = prev.filter((p) => p.id !== id);
      return [nextPreset, ...without].sort((a, b) => b.updatedAtMs - a.updatedAtMs);
    });
    setSelectedPresetId(id);
    setRunError(null);
  };

  const applyPreset = (presetId: string) => {
    const preset = presets.find((p) => p.id === presetId);
    if (!preset) return;
    setSelectedPresetId(preset.id);
    setPresetName(preset.name);
    setSelectedSymbol(preset.symbol);
    setForm(normalizeForm(preset.form));
  };

  const deletePreset = () => {
    if (!selectedPresetId) return;
    setPresets((prev) => prev.filter((p) => p.id !== selectedPresetId));
    setSelectedPresetId('');
    setPresetName('');
  };

  const applyAggressivityPreset = (level: AggressivityLevel) => {
    const preset = AGGRESSIVITY_PRESETS[level];
    setForm((prev) => ({
      ...prev,
      aggressivity: level,
      ...preset,
    }));
  };

  const runBacktest = async () => {
    if (!selectedSymbol) {
      setRunError('Select a symbol first.');
      return;
    }
    if (!adminSecret) {
      setRunError('Admin secret is required.');
      return;
    }
    setRunLoading(true);
    setRunError(null);
    try {
      const presetFallback = AGGRESSIVITY_PRESETS[form.aggressivity] || AGGRESSIVITY_PRESETS[DEFAULT_AGGRESSIVITY_LEVEL];
      const candleCacheKey = buildCandleCacheKey(selectedSymbol, form);
      const payload: any = {
        symbol: selectedSymbol,
        debug: form.debugVerbose,
        executeMinutes: toPositiveInt(form.executeMinutes, 3),
        spreadPips: toNumber(form.spreadPips, 1.1),
        spreadFactor: toNumber(form.spreadFactor, 1),
        slippagePips: toNumber(form.slippagePips, 0.15),
        strategy: {
          asiaBaseTf: form.asiaBaseTf,
          confirmTf: form.confirmTf,
          riskPerTradePct: toNumber(form.riskPerTradePct, 0.35),
          takeProfitR: toNumber(form.takeProfitR, toNumber(presetFallback.takeProfitR, 1.5)),
          maxTradesPerDay: toPositiveInt(form.maxTradesPerDay, toPositiveInt(presetFallback.maxTradesPerDay, 4)),
          sweepBufferPips: toNumber(form.sweepBufferPips, toNumber(presetFallback.sweepBufferPips, 0.5)),
          sweepRejectMaxBars: toPositiveInt(form.sweepRejectMaxBars, toPositiveInt(presetFallback.sweepRejectMaxBars, 12)),
          displacementBodyAtrMult: toNumber(
            form.displacementBodyAtrMult,
            toNumber(presetFallback.displacementBodyAtrMult, 0.12),
          ),
          displacementRangeAtrMult: toNumber(
            form.displacementRangeAtrMult,
            toNumber(presetFallback.displacementRangeAtrMult, 0.22),
          ),
          mssLookbackBars: toPositiveInt(form.mssLookbackBars, toPositiveInt(presetFallback.mssLookbackBars, 1)),
          ifvgMinAtrMult: toNumber(form.ifvgMinAtrMult, toNumber(presetFallback.ifvgMinAtrMult, 0.01)),
          ifvgMaxAtrMult: toNumber(form.ifvgMaxAtrMult, toNumber(presetFallback.ifvgMaxAtrMult, 1.5)),
          ifvgEntryMode: form.ifvgEntryMode,
        },
      };

      if (form.backtestMode === 'DATE_RANGE') {
        const fromMs = parseLocalDateTimeToMs(form.rangeStartLocal);
        const toMs = parseLocalDateTimeToMs(form.rangeEndLocal);
        if (fromMs === null || toMs === null) {
          throw new Error('Invalid date range. Use valid start and end timestamps.');
        }
        if (toMs <= fromMs) {
          throw new Error('Date range end must be after start.');
        }
        payload.fromTsMs = fromMs;
        payload.toTsMs = toMs;
      } else {
        payload.lookbackPastValue = toNumber(form.lookbackPastValue, 3);
        payload.lookbackPastUnit = form.lookbackPastUnit;
        payload.lookbackCandles = toPositiveInt(form.lookbackCandles, 720);
      }

      if (candleCacheKey) {
        const cached = candleCacheRef.current.get(candleCacheKey);
        if (cached && Date.now() - cached.cachedAtMs <= CANDLE_CACHE_TTL_MS && cached.candles.length >= 180) {
          payload.cachedCandles = cached.candles;
          payload.cachedSourceTimeframe = cached.sourceTimeframe;
        }
      }

      const res = await fetch('/api/scalp/backtest/run', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-admin-access-secret': adminSecret,
        },
        body: JSON.stringify(payload),
      });
      const data = (await res.json().catch(() => ({}))) as BacktestRunResponse & { message?: string };
      if (!res.ok) {
        throw new Error(formatRunErrorMessage(data as any, res.status));
      }
      if (candleCacheKey && Array.isArray(data?.chart?.candles) && data.chart.candles.length >= 180) {
        candleCacheRef.current.set(candleCacheKey, {
          candles: data.chart.candles,
          sourceTimeframe: String(data.sourceTimeframe || 'cached'),
          cachedAtMs: Date.now(),
        });
      }
      setResult(data);
    } catch (err: any) {
      setRunError(err?.message || 'Backtest failed.');
    } finally {
      setRunLoading(false);
    }
  };

  const addCurrentRunToCompare = () => {
    if (!result) return;
    const label = compareLabel.trim() || `${result.symbol} ${new Date().toLocaleTimeString('en-GB')}`;
    const id = `cmp_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
    const color = COMPARE_COLORS[comparedRuns.length % COMPARE_COLORS.length] || '#f59e0b';
    const item: ComparedRun = {
      id,
      label,
      color,
      visible: true,
      result,
    };
    setComparedRuns((prev) => [item, ...prev].slice(0, 8));
    setCompareLabel('');
  };

  return (
    <>
      <Head>
        <title>Scalp Backtest Lab</title>
      </Head>

      <main className="min-h-screen bg-slate-950 text-slate-100">
        <div className="mx-auto w-full max-w-[1480px] px-4 py-6 sm:px-6 lg:px-8">
          <div className="mb-6 rounded-3xl border border-slate-800 bg-gradient-to-r from-cyan-900/30 via-slate-900 to-emerald-900/20 p-6">
            <p className="text-xs uppercase tracking-[0.24em] text-cyan-300">Scalp Lab</p>
            <h1 className="mt-2 text-2xl font-semibold text-white">Capital Asset Backtest Workbench</h1>
            <p className="mt-2 max-w-4xl text-sm text-slate-300">
              Pick Capital symbols, run range-based backtests with strategy parameters, save reusable presets, and compare multiple runs on one chart.
            </p>
          </div>

          {!adminReady ? (
            <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5 text-slate-300">Checking admin access…</div>
          ) : !adminGranted ? (
            <div className="max-w-xl rounded-2xl border border-amber-700/60 bg-slate-900 p-6">
              <h2 className="text-lg font-semibold text-amber-200">Admin Access Required</h2>
              <p className="mt-2 text-sm text-slate-300">Enter `ADMIN_ACCESS_SECRET` to open scalp backtest APIs.</p>
              <div className="mt-4 flex flex-col gap-3 sm:flex-row">
                <input
                  className="w-full rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100 outline-none focus:border-cyan-400"
                  placeholder="ADMIN_ACCESS_SECRET"
                  value={adminInput}
                  onChange={(e) => setAdminInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') void submitAdminSecret();
                  }}
                />
                <button
                  type="button"
                  onClick={() => void submitAdminSecret()}
                  disabled={adminSubmitting}
                  className="rounded-xl bg-cyan-500 px-4 py-2 text-sm font-semibold text-slate-950 disabled:opacity-60"
                >
                  {adminSubmitting ? 'Checking…' : 'Unlock'}
                </button>
              </div>
              {adminError ? <p className="mt-3 text-sm text-rose-300">{adminError}</p> : null}
            </div>
          ) : (
            <div className="grid grid-cols-1 gap-6 xl:grid-cols-[440px_minmax(0,1fr)]">
              <section className="space-y-4">
                <div className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
                  <h2 className="text-sm font-semibold uppercase tracking-wide text-cyan-300">Assets</h2>
                  <div className="mt-3">
                    <label className="mb-1 block text-xs text-slate-400">Capital Symbol</label>
                    <select
                      className="w-full rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-sm outline-none focus:border-cyan-400"
                      value={selectedSymbol}
                      onChange={(e) => setSelectedSymbol(e.target.value)}
                    >
                      {assets.map((asset) => (
                        <option key={asset.symbol} value={asset.symbol}>
                          {asset.symbol} · {asset.epic} · {asset.category}
                        </option>
                      ))}
                    </select>
                    {assetLoading ? <p className="mt-2 text-xs text-slate-400">Loading Capital assets…</p> : null}
                    {assetError ? <p className="mt-2 text-xs text-rose-300">{assetError}</p> : null}
                    {selectedAsset ? (
                      <p className="mt-2 text-xs text-slate-400">
                        Selected: <span className="text-slate-200">{selectedAsset.symbol}</span> ({selectedAsset.epic}) · {selectedAsset.category}
                      </p>
                    ) : null}
                  </div>
                  <div className="mt-3 grid grid-cols-2 gap-2 text-xs text-slate-400">
                    <div>Forex: {categoryCounts.forex || 0}</div>
                    <div>Indices: {categoryCounts.index || 0}</div>
                    <div>Commodities: {categoryCounts.commodity || 0}</div>
                    <div>Equities: {categoryCounts.equity || 0}</div>
                  </div>
                </div>

                <div className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
                  <h2 className="text-sm font-semibold uppercase tracking-wide text-cyan-300">Presets</h2>
                  <div className="mt-3 grid grid-cols-1 gap-3">
                    <input
                      className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm"
                      placeholder="Preset name"
                      value={presetName}
                      onChange={(e) => setPresetName(e.target.value)}
                    />
                    <div className="flex gap-2">
                      <button
                        type="button"
                        onClick={savePreset}
                        className="flex-1 rounded-lg border border-cyan-500/60 bg-cyan-500/10 px-3 py-2 text-sm text-cyan-200"
                      >
                        Save Preset
                      </button>
                      <button
                        type="button"
                        onClick={deletePreset}
                        disabled={!selectedPresetId}
                        className="rounded-lg border border-rose-500/50 bg-rose-500/10 px-3 py-2 text-sm text-rose-200 disabled:opacity-50"
                      >
                        Delete
                      </button>
                    </div>
                    <select
                      className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm"
                      value={selectedPresetId}
                      onChange={(e) => {
                        const id = e.target.value;
                        setSelectedPresetId(id);
                        if (id) applyPreset(id);
                      }}
                    >
                      <option value="">Select saved preset…</option>
                      {presets.map((preset) => (
                        <option key={preset.id} value={preset.id}>
                          {preset.name} · {preset.symbol}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                <div className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
                  <h2 className="text-sm font-semibold uppercase tracking-wide text-cyan-300">Replay Params</h2>
                  <div className="mt-3 grid grid-cols-2 gap-3">
                    <label className="col-span-2 text-xs text-slate-300">
                      Defaults aggressivity: <span className="font-semibold text-cyan-200">{form.aggressivity}</span>
                      <input
                        type="range"
                        min={0}
                        max={2}
                        step={1}
                        value={sliderIndexFromLevel(form.aggressivity)}
                        onChange={(e) => {
                          const level = levelFromSliderIndex(Number(e.target.value));
                          applyAggressivityPreset(level);
                        }}
                        className="mt-2 w-full accent-cyan-400"
                      />
                      <div className="mt-1 flex items-center justify-between text-[11px] uppercase tracking-wide text-slate-400">
                        {AGGRESSIVITY_LEVELS.map((level) => (
                          <span key={level} className={form.aggressivity === level ? 'text-cyan-200' : undefined}>
                            {level}
                          </span>
                        ))}
                      </div>
                    </label>

                    <label className="col-span-2 text-xs text-slate-300">
                      Backtest mode
                      <select
                        value={form.backtestMode}
                        onChange={(e) => setForm((prev) => ({ ...prev, backtestMode: e.target.value as BacktestMode }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      >
                        <option value="LOOKBACK">LOOKBACK</option>
                        <option value="DATE_RANGE">DATE_RANGE</option>
                      </select>
                    </label>

                    {form.backtestMode === 'LOOKBACK' ? (
                      <>
                        <label className="text-xs text-slate-300">
                          Lookback amount
                          <input
                            value={form.lookbackPastValue}
                            onChange={(e) => setForm((prev) => ({ ...prev, lookbackPastValue: e.target.value }))}
                            className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                          />
                        </label>
                        <label className="text-xs text-slate-300">
                          Unit
                          <select
                            value={form.lookbackPastUnit}
                            onChange={(e) =>
                              setForm((prev) => ({
                                ...prev,
                                lookbackPastUnit: e.target.value as FormState['lookbackPastUnit'],
                              }))
                            }
                            className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                          >
                            <option value="minutes">minutes</option>
                            <option value="hours">hours</option>
                            <option value="days">days</option>
                          </select>
                        </label>
                        <div className="col-span-2 text-xs text-slate-400">
                          Lookback fetches from now back by this duration (max 90 days).
                        </div>
                      </>
                    ) : (
                      <>
                        <label className="col-span-2 text-xs text-slate-300">
                          Range start (local)
                          <input
                            type="datetime-local"
                            value={form.rangeStartLocal}
                            onChange={(e) => setForm((prev) => ({ ...prev, rangeStartLocal: e.target.value }))}
                            className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                          />
                        </label>
                        <label className="col-span-2 text-xs text-slate-300">
                          Range end (local)
                          <input
                            type="datetime-local"
                            value={form.rangeEndLocal}
                            onChange={(e) => setForm((prev) => ({ ...prev, rangeEndLocal: e.target.value }))}
                            className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                          />
                        </label>
                      </>
                    )}
                    <div className="col-span-2 text-xs text-slate-400">
                      Estimated 1m candles: {estimatedRangeCandles !== null ? estimatedRangeCandles.toLocaleString() : 'invalid range'}
                    </div>

                    <label className="text-xs text-slate-300">
                      Execute minutes
                      <input
                        value={form.executeMinutes}
                        onChange={(e) => setForm((prev) => ({ ...prev, executeMinutes: e.target.value }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      />
                    </label>
                    <label className="text-xs text-slate-300">
                      Spread pips
                      <input
                        value={form.spreadPips}
                        onChange={(e) => setForm((prev) => ({ ...prev, spreadPips: e.target.value }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      />
                    </label>
                    <label className="text-xs text-slate-300">
                      Spread factor
                      <input
                        value={form.spreadFactor}
                        onChange={(e) => setForm((prev) => ({ ...prev, spreadFactor: e.target.value }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      />
                    </label>
                    <label className="text-xs text-slate-300">
                      Slippage pips
                      <input
                        value={form.slippagePips}
                        onChange={(e) => setForm((prev) => ({ ...prev, slippagePips: e.target.value }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      />
                    </label>
                    <label className="text-xs text-slate-300">
                      Take profit R
                      <input
                        value={form.takeProfitR}
                        onChange={(e) => setForm((prev) => ({ ...prev, takeProfitR: e.target.value }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      />
                    </label>
                    <label className="text-xs text-slate-300">
                      Risk % per trade
                      <input
                        value={form.riskPerTradePct}
                        onChange={(e) => setForm((prev) => ({ ...prev, riskPerTradePct: e.target.value }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      />
                    </label>
                    <label className="text-xs text-slate-300">
                      Max trades/day
                      <input
                        value={form.maxTradesPerDay}
                        onChange={(e) => setForm((prev) => ({ ...prev, maxTradesPerDay: e.target.value }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      />
                    </label>
                    <label className="text-xs text-slate-300">
                      Asia base TF
                      <select
                        value={form.asiaBaseTf}
                        onChange={(e) => setForm((prev) => ({ ...prev, asiaBaseTf: e.target.value as FormState['asiaBaseTf'] }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      >
                        <option value="M1">M1</option>
                        <option value="M3">M3</option>
                        <option value="M5">M5</option>
                        <option value="M15">M15</option>
                      </select>
                    </label>
                    <label className="text-xs text-slate-300">
                      Confirm TF
                      <select
                        value={form.confirmTf}
                        onChange={(e) => setForm((prev) => ({ ...prev, confirmTf: e.target.value as FormState['confirmTf'] }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      >
                        <option value="M1">M1</option>
                        <option value="M3">M3</option>
                      </select>
                    </label>
                    <label className="text-xs text-slate-300">
                      Sweep buffer (pips)
                      <input
                        value={form.sweepBufferPips}
                        onChange={(e) => setForm((prev) => ({ ...prev, sweepBufferPips: e.target.value }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      />
                    </label>
                    <label className="text-xs text-slate-300">
                      Sweep reject bars
                      <input
                        value={form.sweepRejectMaxBars}
                        onChange={(e) => setForm((prev) => ({ ...prev, sweepRejectMaxBars: e.target.value }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      />
                    </label>
                    <label className="text-xs text-slate-300">
                      Displacement body ATR
                      <input
                        value={form.displacementBodyAtrMult}
                        onChange={(e) => setForm((prev) => ({ ...prev, displacementBodyAtrMult: e.target.value }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      />
                    </label>
                    <label className="text-xs text-slate-300">
                      Displacement range ATR
                      <input
                        value={form.displacementRangeAtrMult}
                        onChange={(e) => setForm((prev) => ({ ...prev, displacementRangeAtrMult: e.target.value }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      />
                    </label>
                    <label className="text-xs text-slate-300">
                      MSS lookback bars
                      <input
                        value={form.mssLookbackBars}
                        onChange={(e) => setForm((prev) => ({ ...prev, mssLookbackBars: e.target.value }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      />
                    </label>
                    <label className="text-xs text-slate-300">
                      iFVG min ATR mult
                      <input
                        value={form.ifvgMinAtrMult}
                        onChange={(e) => setForm((prev) => ({ ...prev, ifvgMinAtrMult: e.target.value }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      />
                    </label>
                    <label className="text-xs text-slate-300">
                      iFVG max ATR mult
                      <input
                        value={form.ifvgMaxAtrMult}
                        onChange={(e) => setForm((prev) => ({ ...prev, ifvgMaxAtrMult: e.target.value }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      />
                    </label>
                    <label className="col-span-2 text-xs text-slate-300">
                      iFVG entry mode
                      <select
                        value={form.ifvgEntryMode}
                        onChange={(e) => setForm((prev) => ({ ...prev, ifvgEntryMode: e.target.value as FormState['ifvgEntryMode'] }))}
                        className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
                      >
                        <option value="first_touch">first_touch</option>
                        <option value="midline_touch">midline_touch</option>
                        <option value="full_fill">full_fill</option>
                      </select>
                    </label>
                    <label className="col-span-2 flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-950 px-2 py-2 text-xs text-slate-300">
                      <input
                        type="checkbox"
                        checked={form.debugVerbose}
                        onChange={(e) => setForm((prev) => ({ ...prev, debugVerbose: e.target.checked }))}
                      />
                      Enable verbose backtest diagnostics (server logs + fallback trace)
                    </label>
                  </div>

                  <div className="mt-4 flex gap-2">
                    <button
                      type="button"
                      disabled={runLoading || !selectedSymbol}
                      onClick={() => void runBacktest()}
                      className="flex-1 rounded-xl bg-cyan-500 px-4 py-2.5 text-sm font-semibold text-slate-950 disabled:opacity-60"
                    >
                      {runLoading ? 'Running Backtest…' : 'Run Backtest'}
                    </button>
                    <button
                      type="button"
                      onClick={() => setForm(DEFAULT_FORM)}
                      className="rounded-xl border border-slate-700 px-3 py-2.5 text-sm text-slate-200"
                    >
                      Reset
                    </button>
                  </div>
                  {runError ? <p className="mt-3 text-sm text-rose-300">{runError}</p> : null}
                </div>
              </section>

              <section className="space-y-4">
                {!result ? (
                  <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6 text-slate-300">
                    Run a backtest to render chart, metrics, and comparison controls.
                  </div>
                ) : (
                  <>
                    <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
                      <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
                        <p className="text-xs text-slate-400">Trades</p>
                        <p className="mt-1 text-lg font-semibold">{result.summary.trades}</p>
                      </div>
                      <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
                        <p className="text-xs text-slate-400">Win Rate</p>
                        <p className="mt-1 text-lg font-semibold">{result.summary.winRatePct.toFixed(2)}%</p>
                      </div>
                      <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
                        <p className="text-xs text-slate-400">Avg R</p>
                        <p className={`mt-1 text-lg font-semibold ${result.summary.avgR >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                          {formatSigned(result.summary.avgR, 3)}
                        </p>
                      </div>
                      <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
                        <p className="text-xs text-slate-400">Net R</p>
                        <p className={`mt-1 text-lg font-semibold ${result.summary.netR >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                          {formatSigned(result.summary.netR, 3)}
                        </p>
                      </div>
                      <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
                        <p className="text-xs text-slate-400">Net PnL</p>
                        <p className={`mt-1 text-lg font-semibold ${result.summary.netPnlUsd >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                          {formatUsd(result.summary.netPnlUsd)}
                        </p>
                      </div>
                    </div>

                    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
                      <div className="flex flex-col gap-2 text-xs text-slate-400 sm:flex-row sm:items-center sm:justify-between">
                        <div>
                          {result.symbol} ({result.epic}) · source TF {result.sourceTimeframe} · candles {result.fetchedCandles}
                          {result.clampedBySourceLimit ? ' (clamped by source limit)' : ''}
                          {result.mappingSource ? ` · map ${result.mappingSource}` : ''}
                          {result.candleSource ? ` · data ${result.candleSource}` : ''}
                        </div>
                        <div>
                          Range: {formatTs(result.effectiveFromTsMs)} {'->'} {formatTs(result.effectiveToTsMs)} · fetch mode{' '}
                          {result.dataFetchMode || result.fetchDiagnostics?.dataFetchMode || '—'}
                        </div>
                      </div>
                      {result.sourceFallbackUsed ? (
                        <p className="mt-2 text-xs text-amber-300">
                          1m history unavailable. Fallback source TF used: {result.sourceTimeframe} (tried:{' '}
                          {(result.attemptedSourceTimeframes || []).join(', ')}).
                        </p>
                      ) : null}
                      {result.fetchDiagnostics?.attempts?.length ? (
                        <div className="mt-3 overflow-x-auto">
                          <table className="min-w-full border-collapse text-left text-xs text-slate-300">
                            <thead>
                              <tr className="text-slate-400">
                                <th className="border-b border-slate-800 px-2 py-1">TF</th>
                                <th className="border-b border-slate-800 px-2 py-1">Status</th>
                                <th className="border-b border-slate-800 px-2 py-1">Candles</th>
                                <th className="border-b border-slate-800 px-2 py-1">Duration</th>
                                <th className="border-b border-slate-800 px-2 py-1">Error</th>
                              </tr>
                            </thead>
                            <tbody>
                              {result.fetchDiagnostics.attempts.map((attempt, idx) => (
                                <tr key={`${attempt.timeframe}_${idx}`} className="align-top">
                                  <td className="border-b border-slate-900 px-2 py-1.5 font-medium text-slate-200">{attempt.timeframe}</td>
                                  <td className="border-b border-slate-900 px-2 py-1.5">{attempt.status}</td>
                                  <td className="border-b border-slate-900 px-2 py-1.5">{attempt.candles}</td>
                                  <td className="border-b border-slate-900 px-2 py-1.5">{attempt.durationMs}ms</td>
                                  <td className="border-b border-slate-900 px-2 py-1.5 text-slate-400">{attempt.errorMessage || '—'}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      ) : null}
                    </div>

                    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
                      <h3 className="text-sm font-semibold text-cyan-200">Compare Runs</h3>
                      <div className="mt-3 flex flex-col gap-2 sm:flex-row">
                        <input
                          value={compareLabel}
                          onChange={(e) => setCompareLabel(e.target.value)}
                          placeholder="Comparison label (optional)"
                          className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm"
                        />
                        <button
                          type="button"
                          onClick={addCurrentRunToCompare}
                          className="rounded-lg border border-cyan-500/60 bg-cyan-500/10 px-3 py-2 text-sm text-cyan-200"
                        >
                          Add Current Run
                        </button>
                        <button
                          type="button"
                          onClick={() => setComparedRuns([])}
                          className="rounded-lg border border-slate-700 px-3 py-2 text-sm text-slate-300"
                        >
                          Clear
                        </button>
                      </div>
                      <div className="mt-3 space-y-2">
                        {comparedRuns.length === 0 ? (
                          <p className="text-xs text-slate-400">No comparison runs yet.</p>
                        ) : (
                          comparedRuns.map((run) => (
                            <div key={run.id} className="flex items-center justify-between rounded-lg border border-slate-800 bg-slate-950 px-3 py-2 text-xs">
                              <label className="flex items-center gap-2 text-slate-200">
                                <input
                                  type="checkbox"
                                  checked={run.visible}
                                  onChange={(e) => {
                                    const checked = e.target.checked;
                                    setComparedRuns((prev) => prev.map((item) => (item.id === run.id ? { ...item, visible: checked } : item)));
                                  }}
                                />
                                <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ backgroundColor: run.color }} />
                                {run.label}
                              </label>
                              <div className="flex items-center gap-3">
                                <span className={run.result.summary.netR >= 0 ? 'text-emerald-300' : 'text-rose-300'}>
                                  {formatSigned(run.result.summary.netR, 3)}R
                                </span>
                                <button
                                  type="button"
                                  onClick={() => setComparedRuns((prev) => prev.filter((item) => item.id !== run.id))}
                                  className="text-rose-300"
                                >
                                  Remove
                                </button>
                              </div>
                            </div>
                          ))
                        )}
                      </div>
                    </div>

                    <ScalpBacktestChart candles={result.chart.candles} markers={result.chart.markers} trades={chartTrades} />
                    {result.summary.trades === 0 && (result.diagnostics?.topReasonCodes?.length || result.diagnostics?.stateCounts) ? (
                      <div className="rounded-2xl border border-amber-700/40 bg-slate-900 p-4">
                        <h3 className="text-sm font-semibold text-amber-200">No-Trade Diagnostics</h3>
                        <p className="mt-1 text-xs text-slate-300">
                          No entries were triggered. Use these counters to identify the dominant gate.
                        </p>
                        {result.diagnostics?.topReasonCodes?.length ? (
                          <div className="mt-3 overflow-x-auto">
                            <table className="min-w-full text-left text-xs text-slate-200">
                              <thead className="text-slate-400">
                                <tr>
                                  <th className="border-b border-slate-800 px-2 py-1.5">Reason Code</th>
                                  <th className="border-b border-slate-800 px-2 py-1.5">Count</th>
                                </tr>
                              </thead>
                              <tbody>
                                {result.diagnostics.topReasonCodes.map((row) => (
                                  <tr key={row.code}>
                                    <td className="border-b border-slate-900 px-2 py-1.5">{row.code}</td>
                                    <td className="border-b border-slate-900 px-2 py-1.5">{row.count}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        ) : null}
                        {result.diagnostics?.stateCounts && Object.keys(result.diagnostics.stateCounts).length ? (
                          <p className="mt-3 text-xs text-slate-400">
                            State counts:{' '}
                            {Object.entries(result.diagnostics.stateCounts)
                              .map(([state, count]) => `${state}=${count}`)
                              .join(', ')}
                          </p>
                        ) : null}
                      </div>
                    ) : null}

                    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
                      <div className="mt-1 overflow-x-auto">
                        <table className="min-w-full text-left text-xs">
                          <thead className="text-slate-400">
                            <tr>
                              <th className="px-2 py-2">Trade ID</th>
                              <th className="px-2 py-2">Side</th>
                              <th className="px-2 py-2">Entry</th>
                              <th className="px-2 py-2">Exit</th>
                              <th className="px-2 py-2">R</th>
                              <th className="px-2 py-2">PnL USD</th>
                              <th className="px-2 py-2">Reason</th>
                            </tr>
                          </thead>
                          <tbody>
                            {result.trades.slice(-60).map((trade) => (
                              <tr key={trade.id} className="border-t border-slate-800 text-slate-200">
                                <td className="px-2 py-2">{trade.id}</td>
                                <td className="px-2 py-2">{trade.side}</td>
                                <td className="px-2 py-2">{formatTs(trade.entryTs)}</td>
                                <td className="px-2 py-2">{formatTs(trade.exitTs)}</td>
                                <td className={`px-2 py-2 ${trade.rMultiple >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                                  {formatSigned(trade.rMultiple, 3)}
                                </td>
                                <td className={`px-2 py-2 ${trade.pnlUsd >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                                  {formatUsd(trade.pnlUsd)}
                                </td>
                                <td className="px-2 py-2">{trade.exitReason}</td>
                              </tr>
                            ))}
                            {!result.trades.length ? (
                              <tr>
                                <td colSpan={7} className="px-2 py-3 text-slate-400">
                                  No trades generated for current parameters.
                                </td>
                              </tr>
                            ) : null}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </>
                )}
              </section>
            </div>
          )}
        </div>
      </main>
    </>
  );
}
