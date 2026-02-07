import React, { useEffect, useRef, useState } from 'react';
import Head from 'next/head';
import {
  Activity,
  BarChart3,
  BookOpen,
  ShieldPlus,
  Wand2,
  Circle,
  Cpu,
  Database,
  ListChecks,
  Braces,
  Layers3,
  PenTool,
  Repeat,
  ShieldCheck,
  Zap,
  Star,
  ArrowUpRight,
  ArrowDownRight,
  type LucideIcon,
} from 'lucide-react';

type AspectEvaluation = {
  rating?: number;
  comment?: string;
  improvements?: string[];
  checks?: string[];
  findings?: string[];
};

type Evaluation = {
  overall_rating?: number;
  overview?: string;
  what_went_well?: string[];
  issues?: string[];
  improvements?: string[];
  confidence?: string;
  aspects?: Record<string, AspectEvaluation>;
};

type EvaluationEntry = {
  symbol: string;
  evaluation: Evaluation;
  evaluationTs?: number | null;
  lastBiasTimeframes?: Record<string, string | undefined> | null;
  pnl24h?: number | null;
  pnl24hWithOpen?: number | null;
  pnl24hNet?: number | null;
  pnl24hGross?: number | null;
  pnl24hTrades?: number | null;
  pnlSpark?: number[] | null;
  openPnl?: number | null;
  openDirection?: 'long' | 'short' | null;
  openLeverage?: number | null;
  lastPositionPnl?: number | null;
  lastPositionDirection?: 'long' | 'short' | null;
  lastPositionLeverage?: number | null;
  lastDecisionTs?: number | null;
  lastDecision?: {
    action?: string;
    summary?: string;
    reason?: string;
    signal_strength?: string;
    [key: string]: any;
  } | null;
  lastPrompt?: { system?: string; user?: string } | null;
  lastMetrics?: Record<string, any> | null;
  winRate?: number | null;
  avgWinPct?: number | null;
  avgLossPct?: number | null;
};

type EvaluationsResponse = {
  symbols: string[];
  data: EvaluationEntry[];
};

type EvaluateJobStatus = 'queued' | 'running' | 'succeeded' | 'failed';

type EvaluateJobRecord = {
  id: string;
  status: EvaluateJobStatus;
  updatedAt?: number;
  error?: string;
};

type DecisionBrief = {
  timestamp?: number | null;
  action?: string;
  summary?: string;
  reason?: string;
};

type PositionOverlay = {
  id: string;
  status: 'open' | 'closed';
  side?: 'long' | 'short' | null;
  entryTime: number | null;
  exitTime?: number | null;
  pnlPct?: number | null;
  leverage?: number | null;
  entryPrice?: number | null;
  exitPrice?: number | null;
  entryDecision?: DecisionBrief | null;
  exitDecision?: DecisionBrief | null;
};

type RenderedOverlay = PositionOverlay & {
  left: number;
  width: number;
  startX: number;
  endX: number;
  clampedEntry: number;
  clampedExit: number;
  showEntryWall: boolean;
};

const formatCompactPrice = (value: number) => {
  const abs = Math.abs(value);
  if (abs >= 1_000_000) {
    const v = value / 1_000_000;
    return `${v.toFixed(1)}M`;
  }
  if (abs >= 1_000) {
    const v = value / 1_000;
    return `${v.toFixed(1)}K`;
  }
  return value.toFixed(2);
};

const CURRENCY_SYMBOL = '₮'; // Tether-style symbol
const formatUsd = (value: number) => {
  const abs = Math.abs(value);
  const sign = value < 0 ? '-' : '';
  const v = Math.abs(value);
  if (abs >= 1_000_000) return `${sign}${CURRENCY_SYMBOL}${(v / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${sign}${CURRENCY_SYMBOL}${(v / 1_000).toFixed(1)}K`;
  return `${sign}${CURRENCY_SYMBOL}${v.toFixed(0)}`;
};

const BERLIN_TZ = 'Europe/Berlin';

const formatBerlinTime = (time: any, opts: Intl.DateTimeFormatOptions = {}) => {
  const seconds =
    typeof time === 'number'
      ? time
      : typeof time === 'object' && time !== null && 'timestamp' in time
      ? Number((time as any).timestamp)
      : Number(time);
  if (!Number.isFinite(seconds)) return '';
  const date = new Date(seconds * 1000);
  return new Intl.DateTimeFormat('de-DE', {
    timeZone: BERLIN_TZ,
    hour: '2-digit',
    minute: '2-digit',
    ...opts,
  }).format(date);
};

const ADMIN_SECRET_STORAGE_KEY = 'admin_access_secret';
const ADMIN_AUTH_TIMEOUT_MS = 4000;

export default function Home() {
  const [adminReady, setAdminReady] = useState(false);
  const [adminGranted, setAdminGranted] = useState(false);
  const [adminSecret, setAdminSecret] = useState<string | null>(null);
  const [adminInput, setAdminInput] = useState('');
  const [adminError, setAdminError] = useState<string | null>(null);
  const [adminSubmitting, setAdminSubmitting] = useState(false);
  const [symbols, setSymbols] = useState<string[]>([]);
  const [active, setActive] = useState(0);
  const [tabData, setTabData] = useState<Record<string, EvaluationEntry>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showAspects, setShowAspects] = useState(false);
  const [showRawEvaluation, setShowRawEvaluation] = useState(false);
  const [showPrompt, setShowPrompt] = useState(false);
  const [chartData, setChartData] = useState<{ time: number; value: number }[]>([]);
  const [chartMarkers, setChartMarkers] = useState<any[]>([]);
  const [positionOverlays, setPositionOverlays] = useState<PositionOverlay[]>([]);
  const [chartLoading, setChartLoading] = useState(false);
  const [renderedOverlays, setRenderedOverlays] = useState<RenderedOverlay[]>([]);
  const [hoveredOverlay, setHoveredOverlay] = useState<RenderedOverlay | null>(null);
  const [hoverX, setHoverX] = useState<number | null>(null);
  const [chartInitToken, setChartInitToken] = useState(0);
  const [evaluateJobs, setEvaluateJobs] = useState<Record<string, EvaluateJobRecord>>({});
  const [evaluateSubmittingSymbol, setEvaluateSubmittingSymbol] = useState<string | null>(null);
  const chartContainerRef = useRef<HTMLDivElement | null>(null);
  const overlayLayerRef = useRef<HTMLDivElement | null>(null);
  const chartInstanceRef = useRef<any>(null);
  const evaluatePollTimersRef = useRef<Record<string, number>>({});

  const validateAdminAccess = async (secret: string | null) => {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), ADMIN_AUTH_TIMEOUT_MS);
    try {
      const res = await fetch('/api/admin-auth', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ secret: secret || '' }),
        signal: controller.signal,
      });
      const json = await res.json().catch(() => null);
      const required = json?.required !== false;
      const ok = Boolean(json?.ok);
      return { ok: res.ok && ok, required };
    } catch {
      return { ok: false, required: true };
    } finally {
      window.clearTimeout(timeout);
    }
  };

  const handleAdminSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setAdminError(null);
    setAdminSubmitting(true);
    const result = await validateAdminAccess(adminInput);
    if (result.ok) {
      if (result.required) {
        window.localStorage.setItem(ADMIN_SECRET_STORAGE_KEY, adminInput);
        setAdminSecret(adminInput);
      }
      setAdminGranted(true);
      setAdminInput('');
    } else {
      setAdminError('Invalid access secret.');
    }
    setAdminSubmitting(false);
  };

  const aspectMeta: Record<string, { Icon: LucideIcon; color: string; bg: string }> = {
    data_quality: { Icon: Database, color: 'text-sky-700', bg: 'bg-sky-100' },
    data_quantity: { Icon: Layers3, color: 'text-cyan-700', bg: 'bg-cyan-100' },
    ai_performance: { Icon: Cpu, color: 'text-indigo-700', bg: 'bg-indigo-100' },
    strategy_performance: { Icon: BarChart3, color: 'text-emerald-700', bg: 'bg-emerald-100' },
    signal_strength_clarity: { Icon: Activity, color: 'text-amber-700', bg: 'bg-amber-100' },
    risk_management: { Icon: ShieldCheck, color: 'text-rose-700', bg: 'bg-rose-100' },
    consistency: { Icon: Repeat, color: 'text-blue-700', bg: 'bg-blue-100' },
    explainability: { Icon: BookOpen, color: 'text-purple-700', bg: 'bg-purple-100' },
    responsiveness: { Icon: Zap, color: 'text-teal-700', bg: 'bg-teal-100' },
    prompt_engineering: { Icon: PenTool, color: 'text-fuchsia-700', bg: 'bg-fuchsia-100' },
    prompt_consistency: { Icon: ListChecks, color: 'text-lime-700', bg: 'bg-lime-100' },
    action_logic: { Icon: Braces, color: 'text-orange-700', bg: 'bg-orange-100' },
    ai_freedom: { Icon: Wand2, color: 'text-indigo-700', bg: 'bg-indigo-100' },
    guardrail_coverage: { Icon: ShieldPlus, color: 'text-rose-700', bg: 'bg-rose-100' },
  };

  const formatLabel = (key: string) => key.replace(/_/g, ' ');
  const clearEvaluatePollTimer = (symbol: string) => {
    const timerId = evaluatePollTimersRef.current[symbol];
    if (timerId) {
      window.clearInterval(timerId);
      delete evaluatePollTimersRef.current[symbol];
    }
  };
  const pollEvaluationJob = async (symbol: string, jobId: string) => {
    try {
      const res = await fetch(`/api/evaluate?jobId=${encodeURIComponent(jobId)}`, {
        headers: adminSecret ? { 'x-admin-access-secret': adminSecret } : undefined,
      });
      if (!res.ok) return;
      const json = await res.json();
      const status = String(json?.status || '') as EvaluateJobStatus;
      if (!status) return;
      setEvaluateJobs((prev) => ({
        ...prev,
        [symbol]: {
          id: jobId,
          status,
          updatedAt: Number(json?.updatedAt) || Date.now(),
          error: typeof json?.error === 'string' ? json.error : undefined,
        },
      }));

      if (status === 'succeeded' || status === 'failed') {
        clearEvaluatePollTimer(symbol);
        if (status === 'succeeded') {
          await loadEvaluations();
        }
      }
    } catch {
      // keep polling on transient fetch issues
    }
  };
  const triggerEvaluation = async (symbol: string) => {
    if (!symbol || evaluateSubmittingSymbol) return;
    setEvaluateSubmittingSymbol(symbol);
    setError(null);
    try {
      const params = new URLSearchParams({
        symbol,
        async: 'true',
      });
      const res = await fetch(`/api/evaluate?${params.toString()}`, {
        headers: adminSecret ? { 'x-admin-access-secret': adminSecret } : undefined,
      });
      if (!res.ok) {
        let msg = `Failed to queue evaluation (${res.status})`;
        try {
          const body = await res.json();
          msg = body?.error ? `${msg}: ${String(body.error)}` : msg;
        } catch {}
        throw new Error(msg);
      }
      const json = await res.json();
      const jobId = String(json?.jobId || '');
      if (!jobId) throw new Error('Missing evaluation job ID');
      setEvaluateJobs((prev) => ({
        ...prev,
        [symbol]: { id: jobId, status: 'queued', updatedAt: Date.now() },
      }));
      clearEvaluatePollTimer(symbol);
      void pollEvaluationJob(symbol, jobId);
      evaluatePollTimersRef.current[symbol] = window.setInterval(() => {
        void pollEvaluationJob(symbol, jobId);
      }, 5000);
    } catch (err: any) {
      setError(err?.message || 'Failed to queue evaluation');
    } finally {
      setEvaluateSubmittingSymbol(null);
    }
  };
  const loadEvaluations = async () => {
    try {
      setLoading(true);
      const res = await fetch('/api/evaluations', {
        headers: adminSecret ? { 'x-admin-access-secret': adminSecret } : undefined,
      });
      if (!res.ok) {
        throw new Error(`Request failed with status ${res.status}`);
      }
      const json: EvaluationsResponse = await res.json();
      setSymbols(json.symbols || []);
      const mapped: Record<string, EvaluationEntry> = {};
      for (const entry of json.data || []) {
        mapped[entry.symbol] = entry;
      }
      setTabData(mapped);
      setActive(0);
      setError(null);
    } catch (err: any) {
      setError(err?.message || 'Failed to load evaluations');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const stored = window.localStorage.getItem(ADMIN_SECRET_STORAGE_KEY);
    (async () => {
      let result = { ok: false, required: true };
      try {
        result = await validateAdminAccess(stored);
      } catch {
        result = { ok: false, required: true };
      }
      if (result.ok) {
        if (result.required && stored) {
          setAdminSecret(stored);
        }
        setAdminGranted(true);
      } else if (stored) {
        window.localStorage.removeItem(ADMIN_SECRET_STORAGE_KEY);
      }
      setAdminReady(true);
      if (!result.ok) {
        setLoading(false);
      }
    })();
  }, []);

  useEffect(() => {
    if (!adminGranted) return;
    loadEvaluations();
  }, [adminGranted]);

  useEffect(() => {
    return () => {
      Object.keys(evaluatePollTimersRef.current).forEach((symbol) => {
        clearEvaluatePollTimer(symbol);
      });
    };
  }, []);

  useEffect(() => {
    setShowAspects(false);
    setShowRawEvaluation(false);
    setShowPrompt(false);
  }, [active, symbols]);

  useEffect(() => {
    setHoveredOverlay(null);
    setHoverX(null);
  }, [positionOverlays, active]);

  useEffect(() => {
    const fetchChart = async () => {
      if (!adminGranted || !symbols[active]) return;
      try {
        setChartLoading(true);
        const res = await fetch(`/api/chart?symbol=${symbols[active]}&timeframe=15m`, {
          headers: adminSecret ? { 'x-admin-access-secret': adminSecret } : undefined,
        });
        if (!res.ok) throw new Error('Failed to load chart');
        const json = await res.json();
        const mapped = (json.candles || []).map((c: any) => ({ time: c.time, value: c.close }));
        setChartData(mapped);
        setChartMarkers(json.markers || []);
        setPositionOverlays(json.positions || []);
      } catch {
        setChartData([]);
        setChartMarkers([]);
        setPositionOverlays([]);
      } finally {
        setChartLoading(false);
      }
    };
    fetchChart();
  }, [active, symbols, adminGranted, adminSecret]);

  useEffect(() => {
    const container = chartContainerRef.current;
    if (!container || !chartData.length) return;
    let chart: any;

    (async () => {
      const lw = await import('lightweight-charts');
      const createChart =
        typeof lw.createChart === 'function'
          ? lw.createChart
          : typeof (lw as any)?.default?.createChart === 'function'
          ? (lw as any).default.createChart
          : null;
      if (!createChart) {
        console.warn('No createChart found in lightweight-charts');
        return;
      }

      const priceScaleWidth = 56;
      chart = createChart(container, {
        width: container.clientWidth,
        height: 260,
        handleScroll: false,
        handleScale: false,
        layout: {
          background: { type: 'solid', color: 'transparent' },
          textColor: '#0f172a',
        },
        grid: {
          vertLines: { color: '#e2e8f0' },
          horzLines: { color: '#e2e8f0' },
        },
        rightPriceScale: {
          borderVisible: false,
          minimumWidth: priceScaleWidth,
        },
        timeScale: {
          borderVisible: false,
          fixLeftEdge: true,
          fixRightEdge: true,
          timeVisible: true,
          secondsVisible: false,
        },
        localization: {
          priceFormatter: formatCompactPrice,
          timeFormatter: (time: any) =>
            formatBerlinTime(time, {
              day: '2-digit',
              month: '2-digit',
              hour: '2-digit',
              minute: '2-digit',
            }),
        },
      });

      chartInstanceRef.current = chart;
      chart.timeScale().applyOptions({
        tickMarkFormatter: (time: any) => formatBerlinTime(time),
      });

      const AreaSeriesCtor = (lw as any).AreaSeries || (lw as any)?.default?.AreaSeries;
      const LineSeriesCtor = (lw as any).LineSeries || (lw as any)?.default?.LineSeries;

      const series =
        typeof chart.addAreaSeries === 'function'
          ? chart.addAreaSeries({
              lineColor: '#0ea5e9',
              topColor: 'rgba(14,165,233,0.3)',
              bottomColor: 'rgba(14,165,233,0.05)',
            })
          : typeof chart.addSeries === 'function' && AreaSeriesCtor
          ? chart.addSeries(AreaSeriesCtor, {
              lineColor: '#0ea5e9',
              topColor: 'rgba(14,165,233,0.3)',
              bottomColor: 'rgba(14,165,233,0.05)',
            })
          : typeof chart.addLineSeries === 'function'
          ? chart.addLineSeries({ color: '#0ea5e9', lineWidth: 2 })
          : typeof chart.addSeries === 'function' && LineSeriesCtor
          ? chart.addSeries(LineSeriesCtor, { color: '#0ea5e9', lineWidth: 2 })
          : null;

      if (!series) {
        console.warn('No series API available on chart');
        return;
      }

      series.setData(chartData);
      if (Array.isArray(chartMarkers) && chartMarkers.length && typeof series.setMarkers === 'function') {
        series.setMarkers(chartMarkers);
      }
      chart.timeScale().fitContent();
      setChartInitToken((t) => t + 1);
    })();

    const handleResize = () => {
      if (chart && chartContainerRef.current) {
        chart.applyOptions({ width: chartContainerRef.current.clientWidth });
      }
    };

    window.addEventListener('resize', handleResize);
    return () => {
      window.removeEventListener('resize', handleResize);
      if (chart) {
        chart.remove();
        chartInstanceRef.current = null;
      }
    };
  }, [chartData, chartMarkers]);

  useEffect(() => {
    const recalcOverlays = () => {
      const chart = chartInstanceRef.current;
      const layer = overlayLayerRef.current;
      if (!chart || !layer || !chartData.length || !positionOverlays.length) {
        setRenderedOverlays([]);
        return;
      }
      const timeScale = chart.timeScale?.();
      if (!timeScale?.timeToCoordinate) return;
      const minTime = chartData[0].time;
      const maxTime = chartData[chartData.length - 1].time;
      const candleTimes = chartData.map((c) => c.time);
      const nearestTime = (target: number) => {
        return candleTimes.reduce((prev, curr) => {
          return Math.abs(curr - target) < Math.abs(prev - target) ? curr : prev;
        }, candleTimes[0]);
      };
      const mapped = positionOverlays
        .map((pos) => {
          const boundedEntry = Math.min(Math.max(pos.entryTime ?? minTime, minTime), maxTime);
          const boundedExit = pos.exitTime ? Math.min(Math.max(pos.exitTime, minTime), maxTime) : maxTime;
          const entryTime = nearestTime(boundedEntry);
          const exitTime = pos.exitTime ? nearestTime(boundedExit) : maxTime;
          const showEntryWall = pos.entryTime !== null && pos.entryTime >= minTime;
          const startCoord = timeScale.timeToCoordinate(entryTime as any);
          const endCoord = timeScale.timeToCoordinate(exitTime as any);
          if (
            startCoord === null ||
            endCoord === null ||
            !Number.isFinite(startCoord as number) ||
            !Number.isFinite(endCoord as number)
          ) {
            console.log('overlay recalc: skipping pos (no coords)', {
              id: pos.id,
              entry: pos.entryTime,
              exit: pos.exitTime,
              boundedEntry,
              boundedExit,
              entryTime,
              exitTime,
              startCoord,
              endCoord,
            });
            return null;
          }
          const left = Math.min(startCoord as number, endCoord as number);
          const width = Math.max(4, Math.abs((endCoord as number) - (startCoord as number)));
          return {
            ...pos,
            left,
            width,
            startX: startCoord as number,
            endX: endCoord as number,
            clampedEntry: boundedEntry,
            clampedExit: boundedExit,
            showEntryWall,
          };
        })
        .filter(Boolean) as RenderedOverlay[];
      setRenderedOverlays(mapped);
    };
    recalcOverlays();
    window.addEventListener('resize', recalcOverlays);
    return () => window.removeEventListener('resize', recalcOverlays);
  }, [positionOverlays, chartData, chartInitToken]);

  const formatDecisionTime = (ts?: number | null) => {
    if (!ts) return '';
    const d = new Date(ts);
    const now = new Date();
    const sameDay =
      d.getFullYear() === now.getFullYear() &&
      d.getMonth() === now.getMonth() &&
      d.getDate() === now.getDate();
    const time = d.toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit', timeZone: BERLIN_TZ });
    if (sameDay) return `– ${time}`;
    const date = d.toLocaleDateString('de-DE', { timeZone: BERLIN_TZ });
    return `– ${date} ${time}`;
  };

  const formatOverlayTime = (tsSeconds?: number | null) => {
    if (!tsSeconds) return '—';
    const d = new Date(tsSeconds * 1000);
    return d.toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit', timeZone: BERLIN_TZ });
  };

  const formatOverlayDecisionTs = (tsMs?: number | null) => {
    if (!tsMs) return '—';
    const d = new Date(tsMs);
    return `${d.toLocaleDateString('de-DE', { timeZone: BERLIN_TZ })} ${d.toLocaleTimeString('de-DE', {
      hour: '2-digit',
      minute: '2-digit',
      timeZone: BERLIN_TZ,
    })}`;
  };

  const renderPromptContent = (text?: string | null) => {
    if (!text?.trim()) {
      return <span className="text-[11px] text-slate-500">Not available</span>;
    }
    const blocks = text.split(/\n\s*\n/);
    const rendered = blocks
      .map((block, idx) => {
        const trimmed = block.trim();
        if (!trimmed) return null;
        const looksJson =
          (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
          (trimmed.startsWith('[') && trimmed.endsWith(']'));
        if (looksJson) {
          try {
            const parsed = JSON.parse(trimmed);
            return (
              <pre
                key={`json-${idx}`}
                className="overflow-auto rounded-lg border border-slate-800 bg-slate-900/95 px-3 py-2 font-mono text-[11px] leading-snug text-slate-100 shadow-sm"
              >
                {JSON.stringify(parsed, null, 2)}
              </pre>
            );
          } catch {
            // fall through to raw text
          }
        }
        return (
          <pre
            key={`txt-${idx}`}
            className="whitespace-pre-wrap break-words rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 font-mono text-[11px] leading-snug text-slate-800"
          >
            {trimmed}
          </pre>
        );
      })
      .filter(Boolean);

    if (!rendered.length) {
      return <span className="text-[11px] text-slate-500">Not available</span>;
    }
    return <div className="space-y-2">{rendered}</div>;
  };

  const current = symbols[active] ? tabData[symbols[active]] : null;
  const activeSymbol = symbols[active] || null;
  const currentEvalJob = activeSymbol ? evaluateJobs[activeSymbol] : null;
  const evaluateRunning = Boolean(
    activeSymbol &&
      currentEvalJob &&
      (currentEvalJob.status === 'queued' || currentEvalJob.status === 'running'),
  );
  const hasLastDecision =
    !!(
      current &&
      ('lastDecision' in current ||
        'lastDecisionTs' in current ||
        'lastPrompt' in current ||
        'lastMetrics' in current ||
        'lastBiasTimeframes' in current)
    );
  const hasDetails =
    !!(
      current?.evaluation?.what_went_well?.length ||
      current?.evaluation?.issues?.length ||
      current?.evaluation?.improvements?.length
    );
  const biasOrder = [
    { key: 'context_bias', label: 'Context' },
    { key: 'macro_bias', label: 'Macro' },
    { key: 'primary_bias', label: 'Primary' },
    { key: 'micro_bias', label: 'Micro' },
  ] as const;

  return (
    <>
      <Head>
        <title>AI Trade Dashboard</title>
        <link rel="icon" href="/favicon.svg" type="image/svg+xml" />
      </Head>
      <div className="min-h-screen bg-slate-50 text-slate-900 flex items-center justify-center px-4 py-10 relative">
        {adminReady && !adminGranted && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-sm px-4">
            <div className="w-full max-w-md rounded-2xl border border-slate-200 bg-white p-6 shadow-2xl pointer-events-auto">
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 items-center justify-center rounded-full bg-slate-100">
                  <ShieldCheck className="h-5 w-5 text-slate-700" />
                </div>
                <div>
                  <div className="text-xs uppercase tracking-[0.2em] text-slate-500">Admin Access</div>
                  <h2 className="text-xl font-semibold text-slate-900">Enter access secret</h2>
                </div>
              </div>
              <form className="mt-5 space-y-3" onSubmit={handleAdminSubmit}>
                <input
                  type="password"
                  autoComplete="current-password"
                  autoFocus
                  value={adminInput}
                  onChange={(event) => setAdminInput(event.target.value)}
                  placeholder="ADMIN_ACCESS_SECRET"
                  className="w-full rounded-xl border border-slate-200 px-4 py-3 text-sm font-semibold text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none"
                />
                {adminError && <div className="text-sm font-semibold text-rose-600">{adminError}</div>}
                <button
                  type="submit"
                  disabled={adminSubmitting || !adminInput.trim()}
                  className="w-full rounded-xl bg-slate-900 px-4 py-3 text-sm font-semibold text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-400"
                >
                  {adminSubmitting ? 'Checking…' : 'Unlock dashboard'}
                </button>
              </form>
            </div>
          </div>
        )}
        <div className="w-full max-w-6xl rounded-3xl border border-slate-200 bg-white shadow-xl">
        <div className="flex items-center justify-between gap-4 border-b border-slate-200 px-6 py-5">
          <div>
            <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Performance</p>
            <h1 className="text-3xl font-semibold leading-tight text-slate-900">AI Trade Dashboard</h1>
            {activeSymbol && currentEvalJob ? (
              <p className="mt-1 text-xs text-slate-500">
                Eval job for {activeSymbol}:{' '}
                <span className="font-semibold text-slate-700">{currentEvalJob.status}</span>
                {currentEvalJob.error ? ` (${currentEvalJob.error})` : ''}
              </p>
            ) : null}
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => (activeSymbol ? triggerEvaluation(activeSymbol) : undefined)}
              disabled={!adminGranted || !activeSymbol || !!evaluateSubmittingSymbol || evaluateRunning}
              className="rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-800 transition hover:border-emerald-300 hover:bg-emerald-50 hover:text-emerald-800 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {evaluateSubmittingSymbol ? 'Queueing…' : evaluateRunning ? 'Evaluating…' : 'Run Evaluation'}
            </button>
            <button
              onClick={loadEvaluations}
              disabled={!adminGranted}
              className="rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-800 transition hover:border-sky-300 hover:bg-sky-50 hover:text-sky-800 disabled:cursor-not-allowed disabled:opacity-60"
            >
              Refresh
            </button>
          </div>
        </div>

        {error && (
          <div className="mx-6 mt-4 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm font-semibold text-rose-700">
            Could not load evaluations: {error}
          </div>
        )}

        {!error && (
          <div className="flex flex-wrap items-center gap-2 px-6 py-4">
            {symbols.map((sym, i) => {
              const isActive = i === active;
              const tab = tabData[sym];
              const pnl24hValue =
                typeof tab?.pnl24hWithOpen === 'number'
                  ? tab.pnl24hWithOpen
                  : typeof tab?.pnl24h === 'number'
                  ? tab.pnl24h
                  : null;
              const pnlTone =
                typeof pnl24hValue === 'number' ? (pnl24hValue < 0 ? 'negative' : 'positive') : 'neutral';
              return (
                <button
                  key={sym}
                  onClick={() => setActive(i)}
                  className={`rounded-full border px-4 py-2 text-sm font-semibold transition ${
                    pnlTone === 'positive'
                      ? 'border-emerald-200 bg-emerald-50 text-emerald-700 hover:border-emerald-300 hover:text-emerald-800'
                      : pnlTone === 'negative'
                      ? 'border-rose-200 bg-rose-50 text-rose-700 hover:border-rose-300 hover:text-rose-800'
                      : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:text-slate-800'
                  } ${
                    isActive
                      ? 'shadow-md ring-2 ring-slate-400/70 outline outline-2 outline-offset-2 outline-slate-200/80'
                      : ''
                  }`}
                >
                  {sym}
                </button>
              );
            })}
          </div>
        )}

        <div className="px-6 pb-8">
          {loading ? (
            <div className="flex items-center justify-center py-12 text-sm font-semibold text-slate-500">
              Loading...
            </div>
          ) : !symbols.length ? (
            <div className="flex items-center justify-center py-12 text-sm font-semibold text-slate-500">
              No evaluations found.
            </div>
          ) : current ? (
            <div className="grid grid-cols-1 gap-5 lg:grid-cols-2 lg:items-stretch">
              <div className="space-y-4 lg:col-span-2">
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4 h-full">
                  <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                    <div>
                      <div className="text-xs uppercase tracking-wide text-slate-500">24h PnL</div>
                      <div className="mt-3 text-3xl font-semibold text-slate-900">
                        <span
                          className={
                            typeof current.pnl24hWithOpen === 'number'
                              ? current.pnl24hWithOpen >= 0
                                ? 'text-emerald-600'
                                : 'text-rose-600'
                              : 'text-slate-500'
                          }
                        >
                          {typeof current.pnl24hWithOpen === 'number'
                            ? `${current.pnl24hWithOpen.toFixed(2)}%`
                            : typeof current.pnl24h === 'number'
                            ? `${current.pnl24h.toFixed(2)}%`
                            : '—'}
                          {typeof current.pnl24hNet === 'number' ? (
                            <span className="ml-1 align-middle text-sm font-medium text-slate-500">
                              ({current.pnl24hNet >= 0 ? '+' : ''}
                              {formatUsd(current.pnl24hNet)})
                            </span>
                          ) : null}
                        </span>
                      </div>
                      <p className="mt-1 text-xs text-slate-500">
                        from {current.pnl24hTrades ?? 0} {current.pnl24hTrades === 1 ? 'trade' : 'trades'}
                        {typeof current.openPnl === 'number' ? ' + open position' : ''}
                      </p>
                      <div className="mt-1 text-[11px] text-slate-500">
                        {typeof current.pnl24hGross === 'number' || typeof current.pnl24h === 'number' ? (
                          <>
                            gross vs net:{' '}
                            <span className="font-semibold text-slate-700">
                              {typeof current.pnl24hGross === 'number' ? current.pnl24hGross.toFixed(2) : '—'}%
                            </span>{' '}
                            /{' '}
                            <span className="font-semibold text-slate-700">
                              {typeof current.pnl24h === 'number' ? current.pnl24h.toFixed(2) : '—'}%
                            </span>
                          </>
                        ) : null}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs uppercase tracking-wide text-slate-500">Last PNL</div>
                      <div className="mt-3 text-3xl font-semibold text-slate-900">
                        <span
                          className={
                            typeof current.lastPositionPnl === 'number'
                              ? current.lastPositionPnl >= 0
                                ? 'text-emerald-600'
                                : 'text-rose-600'
                              : 'text-slate-500'
                          }
                        >
                          {typeof current.lastPositionPnl === 'number'
                            ? `${current.lastPositionPnl.toFixed(2)}%`
                            : '—'}
                        </span>
                      </div>
                      <p className="mt-1 text-xs text-slate-500">
                        {typeof current.lastPositionPnl === 'number' ? (
                          <span className="flex flex-wrap items-center gap-2">
                            <span className="flex items-center gap-1">
                              direction –
                              {current.lastPositionDirection ? (
                                <span
                                  className={`${
                                    current.lastPositionDirection === 'long' ? 'text-emerald-600' : 'text-rose-600'
                                  }`}
                                >
                                  {current.lastPositionDirection}
                                </span>
                              ) : null}
                            </span>
                            {typeof current.lastPositionLeverage === 'number' ? (
                              <span className="rounded-full bg-slate-100 px-2 py-0.5 text-[11px] font-semibold text-slate-700">
                                {current.lastPositionLeverage.toFixed(0)}x
                              </span>
                            ) : null}
                          </span>
                        ) : (
                          'no recent positions'
                        )}
                      </p>
                      {typeof current.winRate === 'number' || typeof current.avgWinPct === 'number' || typeof current.avgLossPct === 'number' ? (
                        <div className="mt-2 text-[11px] text-slate-500">
                          {typeof current.winRate === 'number' ? `Win rate: ${current.winRate.toFixed(0)}%` : ''}
                          {typeof current.avgWinPct === 'number' ? ` · Avg win: ${current.avgWinPct.toFixed(2)}%` : ''}
                          {typeof current.avgLossPct === 'number' ? ` · Avg loss: ${current.avgLossPct.toFixed(2)}%` : ''}
                        </div>
                      ) : null}
                    </div>
                    <div>
                      <div className="text-xs uppercase tracking-wide text-slate-500">Open PNL</div>
                      <div className="mt-3 text-3xl font-semibold text-slate-900">
                        <span
                          className={
                            typeof current.openPnl === 'number'
                              ? current.openPnl >= 0
                                ? 'text-emerald-600'
                                : 'text-rose-600'
                              : 'text-slate-500'
                          }
                        >
                          {typeof current.openPnl === 'number' ? `${current.openPnl.toFixed(2)}%` : '—'}
                        </span>
                      </div>
                      <p className="mt-1 text-xs text-slate-500">
                        {typeof current.openPnl === 'number' ? (
                          <span className="flex flex-wrap items-center gap-2">
                            <span className="flex items-center gap-1">
                              direction –
                              {current.openDirection ? (
                                <span className={current.openDirection === 'long' ? 'text-emerald-600' : 'text-rose-600'}>
                                  {current.openDirection}
                                </span>
                              ) : null}
                            </span>
                            {typeof current.openLeverage === 'number' ? (
                              <span className="rounded-full bg-slate-100 px-2 py-0.5 text-[11px] font-semibold text-slate-700">
                                {current.openLeverage.toFixed(0)}x
                              </span>
                            ) : null}
                          </span>
                        ) : (
                          'no open position'
                        )}
                      </p>
                    </div>
                  </div>
                </div>

              </div>

              {(chartLoading || chartData.length > 0) && (
                <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
                  <div className="flex items-center justify-between">
                    <div className="text-xs uppercase tracking-wide text-slate-500">24h Price</div>
                    <div className="text-xs text-slate-400">15m bars</div>
                  </div>
                  <div className="relative mt-3 h-[260px] w-full" style={{ minHeight: 260 }}>
                    {chartLoading ? (
                      <div className="flex h-full w-full items-center justify-center rounded-xl bg-slate-50 text-sm font-semibold text-slate-500">
                        Loading chart...
                      </div>
                    ) : (
                      <div ref={chartContainerRef} className="h-full w-full" style={{ minHeight: 260 }} />
                    )}
                    {!chartLoading && (
                      <>
                        <div
                          ref={overlayLayerRef}
                          className="pointer-events-none absolute inset-0"
                          style={{ right: 56 }}
                        >
                          {renderedOverlays.map((pos) => {
                            const profitable = typeof pos.pnlPct === 'number' ? pos.pnlPct >= 0 : null;
                            const pnlLabel = typeof pos.pnlPct === 'number' ? `${pos.pnlPct.toFixed(1)}%` : null;
                            const leverageLabel = typeof pos.leverage === 'number' ? `${pos.leverage.toFixed(0)}x` : null;
                            const fill =
                              profitable === null
                                ? 'rgba(148,163,184,0.08)'
                                : profitable
                                ? 'rgba(16,185,129,0.12)'
                                : 'rgba(248,113,113,0.12)';
                            const stroke =
                              profitable === null
                                ? 'rgba(100,116,139,0.4)'
                                : profitable
                                ? 'rgba(16,185,129,0.9)'
                                : 'rgba(239,68,68,0.9)';
                            return (
                              <div
                                key={pos.id}
                                className="absolute inset-y-3 rounded-md shadow-[inset_0_0_0_1px_rgba(0,0,0,0.04)]"
                                style={{ left: pos.left, width: pos.width, background: fill, pointerEvents: 'auto' }}
                                onMouseEnter={() => {
                                  setHoveredOverlay(pos);
                                  setHoverX(pos.left + pos.width / 2);
                                }}
                                onMouseLeave={() => {
                                  setHoveredOverlay(null);
                                  setHoverX(null);
                                }}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setHoveredOverlay((cur) => (cur?.id === pos.id ? null : pos));
                                  setHoverX(pos.left + pos.width / 2);
                                }}
                              >
                                {pos.showEntryWall && (
                                  <div
                                    className="absolute top-0 bottom-0 w-[2px]"
                                    style={{ left: 0, backgroundColor: stroke }}
                                  />
                                )}
                                {pos.status === 'closed' ? (
                                  <div
                                    className="absolute top-0 bottom-0 w-[2px]"
                                    style={{ right: 0, backgroundColor: stroke }}
                                  />
                                ) : (
                                  <div
                                    className="absolute top-0 bottom-0 w-[2px]"
                                    style={{ right: 0, backgroundColor: 'rgba(148,163,184,0.8)' }}
                                  />
                                )}
                                <div className="pointer-events-none absolute right-1 top-1 rounded-full bg-white/90 p-2 shadow-sm">
                                  {pos.side === 'long' ? (
                                    <ArrowUpRight className="h-3.5 w-3.5 text-emerald-600" aria-hidden="true" />
                                  ) : pos.side === 'short' ? (
                                    <ArrowDownRight className="h-3.5 w-3.5 text-rose-600" aria-hidden="true" />
                                  ) : null}
                                </div>
                                {pnlLabel && (
                                  <div className="pointer-events-none absolute right-1 top-10 rounded-full bg-white/90 px-2 py-0.5 text-[10px] font-semibold text-slate-700 shadow-sm">
                                    {pnlLabel}
                                  </div>
                                )}
                                {leverageLabel ? (
                                  <div className="pointer-events-none absolute right-9 top-3.5 rounded-full bg-white/90 px-2 py-0.5 text-[10px] font-semibold text-slate-700 shadow-sm">
                                    {leverageLabel}
                                  </div>
                                ) : null}
                              </div>
                            );
                          })}
                        </div>
                        {hoveredOverlay && hoverX !== null && (
                          <div
                            className="absolute z-20"
                            style={{
                              left: Math.min(
                                Math.max(hoverX - 130, 8),
                                (overlayLayerRef.current?.clientWidth || 280) - 220
                              ),
                              top: 10,
                            }}
                          >
                            <div className="pointer-events-none rounded-xl border border-slate-200 bg-white/95 px-3 py-2 text-[11px] text-slate-700 shadow-lg backdrop-blur">
                              <div className="flex items-center justify-between gap-3">
                                <span className="font-semibold text-slate-900">
                                  {hoveredOverlay.status === 'open' ? 'Open position' : 'Closed position'}
                                </span>
                                <span
                                  className={
                                    typeof hoveredOverlay.pnlPct === 'number'
                                      ? hoveredOverlay.pnlPct >= 0
                                        ? 'font-semibold text-emerald-600'
                                        : 'font-semibold text-rose-600'
                                      : 'text-slate-500'
                                  }
                                >
                                  {typeof hoveredOverlay.pnlPct === 'number'
                                    ? `${hoveredOverlay.pnlPct.toFixed(2)}%`
                                    : '—'}
                                </span>
                              </div>
                              <div className="mt-1 text-[10px] uppercase tracking-wide text-slate-500">
                                {hoveredOverlay.side || 'position'} · entry {formatOverlayTime(hoveredOverlay.entryTime)}
                                {hoveredOverlay.exitTime ? ` · exit ${formatOverlayTime(hoveredOverlay.exitTime)}` : ''}
                              </div>
                              <div className="mt-2 flex flex-wrap items-center gap-3 text-[11px] text-slate-600">
                                {typeof hoveredOverlay.leverage === 'number' ? (
                                  <span className="font-semibold text-slate-800">
                                    {hoveredOverlay.leverage.toFixed(0)}x
                                  </span>
                                ) : null}
                                {typeof hoveredOverlay.entryPrice === 'number' ? (
                                  <span>Entry {hoveredOverlay.entryPrice.toFixed(2)}</span>
                                ) : null}
                                {typeof hoveredOverlay.exitPrice === 'number' ? (
                                  <span>Exit {hoveredOverlay.exitPrice.toFixed(2)}</span>
                                ) : null}
                              </div>

                              {hoveredOverlay.entryDecision && (
                                <div className="mt-2 space-y-0.5 rounded-lg bg-slate-50/80 p-2">
                                  <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                    Entry AI decision
                                  </div>
                                  <div className="text-[11px] text-slate-800">
                                    <span className="font-semibold text-sky-700">
                                      {hoveredOverlay.entryDecision.action || 'Decision'}
                                    </span>
                                    {hoveredOverlay.entryDecision.summary
                                      ? ` · ${hoveredOverlay.entryDecision.summary}`
                                      : ''}
                                  </div>
                                  <div className="text-[10px] text-slate-500">
                                    {formatOverlayDecisionTs(hoveredOverlay.entryDecision.timestamp || null)}
                                  </div>
                                </div>
                              )}

                              {hoveredOverlay.exitDecision && (
                                <div className="mt-2 space-y-0.5 rounded-lg bg-slate-50/80 p-2">
                                  <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                    Exit AI decision
                                  </div>
                                  <div className="text-[11px] text-slate-800">
                                    <span className="font-semibold text-sky-700">
                                      {hoveredOverlay.exitDecision.action || 'Decision'}
                                    </span>
                                    {hoveredOverlay.exitDecision.summary
                                      ? ` · ${hoveredOverlay.exitDecision.summary}`
                                      : ''}
                                  </div>
                                  <div className="text-[10px] text-slate-500">
                                    {formatOverlayDecisionTs(hoveredOverlay.exitDecision.timestamp || null)}
                                  </div>
                                </div>
                              )}
                            </div>
                          </div>
                        )}
                      </>
                    )}
                  </div>
                </div>
              )}

              {hasLastDecision && (
                <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div className="flex items-center gap-2 text-xs uppercase tracking-wide text-slate-500">
                      <span>Latest Decision</span>
                      {current.lastDecisionTs ? (
                        <span className="lowercase text-slate-400">
                          {formatDecisionTime(current.lastDecisionTs)}
                        </span>
                      ) : null}
                    </div>
                    <div className="flex items-center gap-2">
                      {current.lastDecision?.signal_strength && (
                        <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-semibold text-slate-700">
                          Strength: {current.lastDecision.signal_strength}
                        </span>
                      )}
                    </div>
                  </div>
                  <div className="mt-3 text-sm text-slate-800">
                    Action:{' '}
                    <span className="font-semibold text-sky-700">
                      {((current.lastDecision as any)?.action || '').toString() || '—'}
                    </span>
                    {(current.lastDecision as any)?.summary ? ` · ${(current.lastDecision as any).summary}` : ''}
                  </div>
                  {(current.lastDecision as any)?.reason ? (
                    <p className="mt-2 text-sm text-slate-700 whitespace-pre-wrap">
                      <span className="font-semibold text-slate-800">Reason: </span>
                      {(current.lastDecision as any).reason}
                    </p>
                  ) : null}
                  <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-4">
                    {biasOrder.map(({ key, label }) => {
                      const raw = (current.lastDecision as any)?.[key];
                      const val = typeof raw === 'string' ? raw.toUpperCase() : raw;
                      const tfLabel = current.lastBiasTimeframes?.[key.replace('_bias', '')] || null;
                      const displayLabel = tfLabel ? `${label} (${tfLabel})` : label;
                      const meta =
                        val === 'UP'
                          ? { color: 'text-emerald-600', Icon: ArrowUpRight }
                          : val === 'DOWN'
                          ? { color: 'text-rose-600', Icon: ArrowDownRight }
                          : { color: 'text-slate-500', Icon: Circle };
                      const Icon = meta.Icon;
                      return (
                        <div
                          key={key}
                          className="flex items-center justify-between rounded-lg border border-slate-100 bg-slate-50 px-3 py-2"
                        >
                          <span className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                            {displayLabel}
                          </span>
                          <span className={`flex items-center gap-1 text-sm font-semibold ${meta.color}`}>
                            <Icon className="h-4 w-4" />
                            {val || '—'}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                  <div className="mt-3">
                    <button
                      onClick={() => setShowPrompt((prev) => !prev)}
                      className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 transition hover:border-slate-300"
                    >
                      {showPrompt ? 'Hide prompt' : 'Show prompt'}
                    </button>
                  </div>
                  {showPrompt && (
                    <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                        <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">System</div>
                        <div className="mt-2">{renderPromptContent(current.lastPrompt?.system)}</div>
                      </div>
                      <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                        <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">User</div>
                        <div className="mt-2">{renderPromptContent(current.lastPrompt?.user)}</div>
                      </div>
                    </div>
                  )}
                </div>
              )}

                <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2 text-xs uppercase tracking-wide text-slate-500">
                    <span>Latest Evaluation</span>
                    {current.evaluationTs ? (
                      <span className="lowercase text-slate-400">{formatDecisionTime(current.evaluationTs)}</span>
                    ) : null}
                  </div>
                  {current?.evaluation?.confidence && (
                    <div className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
                      Confidence: {current.evaluation.confidence}
                    </div>
                  )}
                </div>
                <div className="mt-2 text-lg font-semibold text-slate-900 flex items-center gap-3 flex-wrap">
                  <span>
                    Rating: <span className="text-sky-600">{current?.evaluation?.overall_rating ?? '—'}</span>
                  </span>
                  <div className="flex flex-wrap items-center gap-1">
                    {Array.from({ length: 10 }).map((_, idx) => {
                      const ratingVal = Number(current?.evaluation?.overall_rating ?? 0);
                      const filled = ratingVal >= idx + 1;
                      const colorClass =
                        ratingVal >= 9
                          ? 'text-emerald-500 fill-emerald-500'
                          : ratingVal >= 8
                          ? 'text-emerald-400 fill-emerald-400'
                          : ratingVal >= 6
                          ? 'text-lime-400 fill-lime-400'
                          : ratingVal >= 5
                          ? 'text-amber-400 fill-amber-400'
                          : ratingVal >= 3
                          ? 'text-orange-400 fill-orange-400'
                          : 'text-rose-500 fill-rose-500';
                      return (
                        <Star
                          key={idx}
                          className={`h-4 w-4 ${filled ? colorClass : 'stroke-slate-300 text-slate-300'}`}
                        />
                      );
                    })}
                  </div>
                </div>
                <p className="mt-3 text-sm text-slate-700">
                  {current?.evaluation?.overview || 'No overview provided.'}
                </p>
                {((current?.evaluation?.aspects ?? null) || hasDetails) && (
                  <div className="mt-4 space-y-4">
                    {current?.evaluation?.aspects && (
                      <>
                        <div className="flex flex-wrap items-center gap-2">
                          <button
                            onClick={() => setShowAspects((prev) => !prev)}
                            className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 transition hover:border-slate-300"
                          >
                            {showAspects ? 'Hide aspect ratings' : 'Show aspect ratings'}
                          </button>
                          <button
                            onClick={() => setShowRawEvaluation((prev) => !prev)}
                            className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 transition hover:border-slate-300"
                          >
                            {showRawEvaluation ? 'Hide raw JSON' : 'Show raw JSON'}
                          </button>
                        </div>
                        {showRawEvaluation && (
                          <pre className="overflow-auto rounded-xl border border-slate-800 bg-slate-900/95 p-3 font-mono text-[11px] leading-snug text-slate-100">
                            {JSON.stringify(current.evaluation, null, 2)}
                          </pre>
                        )}
                        {showAspects && (
                          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                            {Object.entries(current.evaluation.aspects).map(([key, val]) => (
                              <div
                                key={key}
                                className="rounded-xl border border-slate-200 bg-slate-50 p-3 shadow-inner shadow-slate-100"
                              >
                                <div className="flex items-center justify-between">
                                  <div className="flex items-center gap-2">
                                    {(() => {
                                      const meta = aspectMeta[key] || {
                                        Icon: Circle,
                                        color: 'text-slate-600',
                                        bg: 'bg-slate-100',
                                      };
                                      const Icon = meta.Icon;
                                      return (
                                        <span className="flex h-9 w-9 items-center justify-center rounded-full bg-slate-100 text-slate-600">
                                          <Icon className="h-4 w-4" />
                                        </span>
                                      );
                                    })()}
                                      <div className="text-sm font-semibold text-slate-900">
                                        {formatLabel(key)}
                                      </div>
                                    </div>
                                    <div className="text-lg font-semibold text-sky-700">{val?.rating ?? '—'}</div>
                                  </div>
                                <p className="mt-2 text-xs text-slate-600">{val?.comment || 'No comment'}</p>
                                {(val?.checks?.length || val?.improvements?.length || val?.findings?.length) && (
                                  <div className="mt-3 space-y-2">
                                    {val?.checks?.length ? (
                                      <div>
                                        <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                          Checks
                                        </div>
                                        <ul className="mt-1 list-disc space-y-1 pl-4 text-xs text-slate-700">
                                          {val.checks.map((item, idx) => (
                                            <li key={idx}>{item}</li>
                                          ))}
                                        </ul>
                                      </div>
                                    ) : null}
                                    {val?.improvements?.length ? (
                                      <div>
                                        <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                          Improvements
                                        </div>
                                        <ul className="mt-1 list-disc space-y-1 pl-4 text-xs text-amber-800">
                                          {val.improvements.map((item, idx) => (
                                            <li key={idx}>{item}</li>
                                          ))}
                                        </ul>
                                      </div>
                                    ) : null}
                                    {val?.findings?.length ? (
                                      <div>
                                        <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                          Findings
                                        </div>
                                        <ul className="mt-1 list-disc space-y-1 pl-4 text-xs text-rose-800">
                                          {val.findings.map((item, idx) => (
                                            <li key={idx}>{item}</li>
                                          ))}
                                        </ul>
                                      </div>
                                    ) : null}
                                  </div>
                                )}
                              </div>
                            ))}
                          </div>
                        )}
                      </>
                    )}

                    {((current?.evaluation?.aspects && showAspects) || !current?.evaluation?.aspects) && hasDetails && (
                      <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3">
                        <div className="text-xs uppercase tracking-wide text-slate-500">Details</div>
                        <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-3">
                          {current.evaluation.what_went_well?.length ? (
                            <div className="rounded-xl border border-emerald-100 bg-emerald-50 p-3">
                              <div className="text-sm font-semibold text-emerald-800">What went well</div>
                              <ul className="mt-2 list-disc space-y-1 pl-4 text-sm text-slate-800">
                                {current.evaluation.what_went_well.map((item, idx) => (
                                  <li key={idx}>{item}</li>
                                ))}
                              </ul>
                            </div>
                          ) : null}
                          {current.evaluation.issues?.length ? (
                            <div className="rounded-xl border border-rose-100 bg-rose-50 p-3">
                              <div className="text-sm font-semibold text-rose-800">Issues</div>
                              <ul className="mt-2 list-disc space-y-1 pl-4 text-sm text-slate-800">
                                {current.evaluation.issues.map((item, idx) => (
                                  <li key={idx}>{item}</li>
                                ))}
                              </ul>
                            </div>
                          ) : null}
                          {current.evaluation.improvements?.length ? (
                            <div className="rounded-xl border border-amber-100 bg-amber-50 p-3">
                              <div className="text-sm font-semibold text-amber-800">Improvements</div>
                              <ul className="mt-2 list-disc space-y-1 pl-4 text-sm text-slate-800">
                                {current.evaluation.improvements.map((item, idx) => (
                                  <li key={idx}>{item}</li>
                                ))}
                              </ul>
                            </div>
                          ) : null}
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
          ) : (
            <div className="flex items-center justify-center py-12 text-sm font-semibold text-slate-500">
              Loading...
            </div>
          )}
        </div>
      </div>
      </div>
    </>
  );
}
