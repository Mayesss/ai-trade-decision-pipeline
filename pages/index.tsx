import React, { useEffect, useRef, useState } from 'react';
import Head from 'next/head';
import dynamic from 'next/dynamic';
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
  pnl7d?: number | null;
  pnl7dWithOpen?: number | null;
  pnl7dNet?: number | null;
  pnl7dGross?: number | null;
  pnl7dTrades?: number | null;
  pnlSpark?: number[] | null;
  openPnl?: number | null;
  openDirection?: 'long' | 'short' | null;
  openLeverage?: number | null;
  openEntryPrice?: number | null;
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
const BITGET_PUBLIC_WS_URL = 'wss://ws.bitget.com/v2/ws/public';
const WS_RECONNECT_MS = 1500;
const WS_PING_MS = 25_000;

const ADMIN_SECRET_STORAGE_KEY = 'admin_access_secret';
const ADMIN_AUTH_TIMEOUT_MS = 4000;

const ChartPanel = dynamic(() => import('../components/ChartPanel'), {
  ssr: false,
  loading: () => (
    <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
      <div className="flex items-center justify-between">
        <div className="text-xs uppercase tracking-wide text-slate-500">7D Price</div>
        <div className="text-xs text-slate-400">1H bars · 7D window</div>
      </div>
      <div className="relative mt-3 h-[260px] w-full" style={{ minHeight: 260 }}>
        <div className="h-full w-full rounded-xl border border-slate-200 bg-slate-50/80 p-4">
          <div className="flex h-full w-full animate-pulse flex-col justify-between">
            <div className="h-3 w-28 rounded-full bg-slate-200" />
            <div className="space-y-2">
              <div className="h-2.5 w-full rounded-full bg-slate-200" />
              <div className="h-2.5 w-11/12 rounded-full bg-slate-200" />
              <div className="h-2.5 w-10/12 rounded-full bg-slate-200" />
            </div>
            <div className="h-3 w-40 rounded-full bg-slate-200" />
          </div>
        </div>
      </div>
    </div>
  ),
});

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
  const [evaluateJobs, setEvaluateJobs] = useState<Record<string, EvaluateJobRecord>>({});
  const [evaluateSubmittingSymbol, setEvaluateSubmittingSymbol] = useState<string | null>(null);
  const [livePriceNow, setLivePriceNow] = useState<number | null>(null);
  const [livePriceTs, setLivePriceTs] = useState<number | null>(null);
  const [livePriceConnected, setLivePriceConnected] = useState(false);
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
    const symbol = symbols[active] || null;
    if (!adminGranted || !symbol) {
      setLivePriceNow(null);
      setLivePriceTs(null);
      setLivePriceConnected(false);
      return;
    }

    let closed = false;
    let ws: WebSocket | null = null;
    let pingTimer: number | null = null;
    let reconnectTimer: number | null = null;

    const clearTimers = () => {
      if (pingTimer) {
        window.clearInterval(pingTimer);
        pingTimer = null;
      }
      if (reconnectTimer) {
        window.clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
    };

    const scheduleReconnect = () => {
      if (closed) return;
      if (reconnectTimer) window.clearTimeout(reconnectTimer);
      reconnectTimer = window.setTimeout(() => {
        if (closed) return;
        connect();
      }, WS_RECONNECT_MS);
    };

    const connect = () => {
      try {
        ws = new WebSocket(BITGET_PUBLIC_WS_URL);
      } catch {
        scheduleReconnect();
        return;
      }

      ws.onopen = () => {
        if (closed || !ws) return;
        setLivePriceConnected(true);
        try {
          ws.send(
            JSON.stringify({
              op: 'subscribe',
              args: [{ instType: 'USDT-FUTURES', channel: 'ticker', instId: symbol }],
            }),
          );
        } catch {}

        pingTimer = window.setInterval(() => {
          if (!ws || ws.readyState !== WebSocket.OPEN) return;
          try {
            ws.send('ping');
          } catch {}
        }, WS_PING_MS);
      };

      ws.onmessage = (event) => {
        if (closed) return;
        const raw = String(event.data ?? '');
        if (!raw || raw === 'pong' || raw === 'ping') return;
        let parsed: any = null;
        try {
          parsed = JSON.parse(raw);
        } catch {
          return;
        }
        const rows = Array.isArray(parsed?.data) ? parsed.data : [];
        for (const row of rows) {
          const px = Number(row?.lastPr ?? row?.last ?? row?.price);
          if (!Number.isFinite(px) || px <= 0) continue;
          const ts = Number(row?.ts ?? parsed?.ts ?? Date.now());
          setLivePriceNow(px);
          setLivePriceTs(Number.isFinite(ts) ? ts : Date.now());
          break;
        }
      };

      ws.onerror = () => {
        if (closed) return;
        setLivePriceConnected(false);
      };

      ws.onclose = () => {
        if (closed) return;
        setLivePriceConnected(false);
        clearTimers();
        scheduleReconnect();
      };
    };

    setLivePriceNow(null);
    setLivePriceTs(null);
    setLivePriceConnected(false);
    connect();

    return () => {
      closed = true;
      clearTimers();
      setLivePriceConnected(false);
      if (ws && ws.readyState === WebSocket.OPEN) {
        try {
          ws.close();
        } catch {}
      }
      ws = null;
    };
  }, [adminGranted, symbols, active]);

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
  const liveOpenPnl =
    current &&
    typeof livePriceNow === 'number' &&
    Number.isFinite(livePriceNow) &&
    typeof current.openEntryPrice === 'number' &&
    Number.isFinite(current.openEntryPrice) &&
    current.openEntryPrice > 0 &&
    (current.openDirection === 'long' || current.openDirection === 'short')
      ? (((livePriceNow - current.openEntryPrice) / current.openEntryPrice) *
          (current.openDirection === 'long' ? 1 : -1) *
          (typeof current.openLeverage === 'number' && current.openLeverage > 0 ? current.openLeverage : 1) *
          100)
      : null;
  const effectiveOpenPnl =
    typeof liveOpenPnl === 'number'
      ? liveOpenPnl
      : current && typeof current.openPnl === 'number'
      ? current.openPnl
      : null;
  const effectivePnl7dWithOpen =
    current && typeof current.pnl7d === 'number' && typeof effectiveOpenPnl === 'number'
      ? current.pnl7d + effectiveOpenPnl
      : current && typeof current.pnl7d === 'number'
      ? current.pnl7d
      : typeof effectiveOpenPnl === 'number'
      ? effectiveOpenPnl
      : current && typeof current.pnl7dWithOpen === 'number'
      ? current.pnl7dWithOpen
      : null;
  const openPnlIsLive = typeof liveOpenPnl === 'number';
  const showChartPanel = Boolean(adminGranted && activeSymbol);
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
            {activeSymbol ? (
              <p className="mt-1 text-xs text-slate-500">
                Live price:{' '}
                <span className={livePriceConnected ? 'font-semibold text-emerald-700' : 'font-semibold text-slate-600'}>
                  {livePriceConnected ? 'connected' : 'connecting'}
                </span>
                {typeof livePriceNow === 'number' ? ` · ${livePriceNow.toFixed(2)}` : ''}
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
              const pnl7dValue =
                typeof tab?.pnl7dWithOpen === 'number'
                  ? tab.pnl7dWithOpen
                  : typeof tab?.pnl7d === 'number'
                  ? tab.pnl7d
                  : null;
              const pnlTone =
                typeof pnl7dValue === 'number' ? (pnl7dValue < 0 ? 'negative' : 'positive') : 'neutral';
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
                      <div className="text-xs uppercase tracking-wide text-slate-500">7D PnL</div>
                      <div className="mt-3 text-3xl font-semibold text-slate-900">
                        <span
                          className={
                            typeof effectivePnl7dWithOpen === 'number'
                              ? effectivePnl7dWithOpen >= 0
                                ? 'text-emerald-600'
                                : 'text-rose-600'
                              : 'text-slate-500'
                          }
                        >
                          {typeof effectivePnl7dWithOpen === 'number'
                            ? `${effectivePnl7dWithOpen.toFixed(2)}%`
                            : typeof current.pnl7d === 'number'
                            ? `${current.pnl7d.toFixed(2)}%`
                            : '—'}
                          {typeof current.pnl7dNet === 'number' ? (
                            <span className="ml-1 align-middle text-sm font-medium text-slate-500">
                              ({current.pnl7dNet >= 0 ? '+' : ''}
                              {formatUsd(current.pnl7dNet)})
                            </span>
                          ) : null}
                        </span>
                      </div>
                      <p className="mt-1 text-xs text-slate-500">
                        from {current.pnl7dTrades ?? 0} {current.pnl7dTrades === 1 ? 'trade' : 'trades'} in last 7d
                        {typeof effectiveOpenPnl === 'number' ? ' + open position' : ''}
                      </p>
                      <div className="mt-1 text-[11px] text-slate-500">
                        {typeof current.pnl7dGross === 'number' || typeof current.pnl7d === 'number' ? (
                          <>
                            gross vs net:{' '}
                            <span className="font-semibold text-slate-700">
                              {typeof current.pnl7dGross === 'number' ? current.pnl7dGross.toFixed(2) : '—'}%
                            </span>{' '}
                            /{' '}
                            <span className="font-semibold text-slate-700">
                              {typeof current.pnl7d === 'number' ? current.pnl7d.toFixed(2) : '—'}%
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
                            typeof effectiveOpenPnl === 'number'
                              ? effectiveOpenPnl >= 0
                                ? 'text-emerald-600'
                                : 'text-rose-600'
                              : 'text-slate-500'
                          }
                        >
                          {typeof effectiveOpenPnl === 'number' ? `${effectiveOpenPnl.toFixed(2)}%` : '—'}
                        </span>
                      </div>
                      <p className="mt-1 text-xs text-slate-500">
                        {typeof effectiveOpenPnl === 'number' ? (
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
                            {openPnlIsLive ? (
                              <span className="rounded-full bg-emerald-50 px-2 py-0.5 text-[11px] font-semibold text-emerald-700">
                                live
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

              {showChartPanel ? (
                <ChartPanel
                  symbol={activeSymbol}
                  adminSecret={adminSecret}
                  adminGranted={adminGranted}
                  timeframe="1H"
                  limit={168}
                  livePrice={livePriceNow}
                  liveTimestamp={livePriceTs}
                  liveConnected={livePriceConnected}
                />
              ) : null}

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
