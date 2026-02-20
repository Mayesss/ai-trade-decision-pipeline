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
  Moon,
  Sun,
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
  lastPlatform?: string | null;
  lastNewsSource?: string | null;
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

type DashboardSymbolRow = {
  symbol: string;
  platform?: string | null;
  newsSource?: string | null;
  schedule?: string | null;
  decisionPolicy?: string | null;
};

type DashboardSymbolsResponse = {
  symbols: string[];
  data: DashboardSymbolRow[];
};

type DashboardSummaryRow = {
  symbol: string;
  lastPlatform?: string | null;
  lastNewsSource?: string | null;
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
  winRate?: number | null;
  avgWinPct?: number | null;
  avgLossPct?: number | null;
};

type DashboardSummaryResponse = {
  symbols: string[];
  data: DashboardSummaryRow[];
  range?: DashboardRangeKey;
};

type ForexSummaryPair = {
  pair: string;
  eligible: boolean;
  rank: number;
  score: number;
  reasons: string[];
  metrics?: {
    spreadPips?: number;
    spreadToAtr1h?: number;
    atr1hPercent?: number;
    sessionTag?: string;
    [key: string]: any;
  } | null;
  packet?: {
    regime?: string;
    permission?: string;
    allowed_modules?: string[];
    risk_state?: string;
    confidence?: number;
    [key: string]: any;
  } | null;
  gate?: {
    allowNewEntries?: boolean;
    blockNewEntries?: boolean;
    staleData?: boolean;
    reasonCodes?: string[];
    [key: string]: any;
  } | null;
  journalCount24h?: number;
  lastExecutionAtMs?: number | null;
  lastExecutionReasonCodes?: string[];
  latestExecution?: {
    pair?: string | null;
    timestampMs?: number | null;
    status?: string | null;
    attempted?: boolean;
    placed?: boolean;
    dryRun?: boolean;
    module?: string | null;
    action?: string | null;
    summary?: string | null;
    reason?: string | null;
    orderId?: string | null;
    clientOid?: string | null;
    reasonCodes?: string[];
  } | null;
  openPosition?: {
    isOpen?: boolean;
    epic?: string | null;
    dealId?: string | null;
    side?: 'long' | 'short' | null;
    entryPrice?: number | null;
    leverage?: number | null;
    size?: number | null;
    pnlPct?: number | null;
    updatedAtMs?: number | null;
  } | null;
};

type ForexSummaryResponse = {
  mode?: 'forex';
  generatedAtMs?: number;
  scanGeneratedAtMs?: number | null;
  packetsGeneratedAtMs?: number | null;
  staleEvents?: boolean;
  latestExecution?: ForexSummaryPair['latestExecution'];
  pairs?: ForexSummaryPair[];
};

type DashboardDecisionResponse = {
  symbol: string;
  platform?: string | null;
  lastDecisionTs?: number | null;
  lastDecision?: EvaluationEntry['lastDecision'];
  lastPrompt?: { system?: string; user?: string } | null;
  lastMetrics?: Record<string, any> | null;
  lastBiasTimeframes?: Record<string, string | undefined> | null;
  lastNewsSource?: string | null;
};

type DashboardEvaluationResponse = {
  symbol: string;
  evaluation: Evaluation;
  evaluationTs?: number | null;
};

type EvaluateJobStatus = 'queued' | 'running' | 'succeeded' | 'failed';

type EvaluateJobRecord = {
  id: string;
  status: EvaluateJobStatus;
  updatedAt?: number;
  error?: string;
};

type DashboardRangeKey = '7D' | '30D' | '6M';
type ThemePreference = 'system' | 'light' | 'dark';
type ResolvedTheme = 'light' | 'dark';
type StrategyMode = 'swing' | 'forex';

const CURRENCY_SYMBOL = '₮'; // Tether-style symbol
const THEME_PREFERENCE_STORAGE_KEY = 'dashboard_theme_preference';
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
const CAPITAL_LIVE_POLL_MS = 3000;
const FOREX_LIVE_POLL_MS = 3000;

const ADMIN_SECRET_STORAGE_KEY = 'admin_access_secret';
const ADMIN_AUTH_TIMEOUT_MS = 4000;
const STRATEGY_MODE_STORAGE_KEY = 'strategy_mode';

const ChartPanel = dynamic(() => import('../components/ChartPanel'), {
  ssr: false,
  loading: () => (
    <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
      <div className="flex items-center justify-between">
        <div className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 p-1 text-[11px] font-semibold text-slate-500">
          <span className="rounded-full bg-slate-200 px-2.5 py-1 text-slate-700">7D</span>
          <span className="px-2.5 py-1">30D</span>
          <span className="px-2.5 py-1">6M</span>
        </div>
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
  const [forexExecuteSubmitting, setForexExecuteSubmitting] = useState(false);
  const [dashboardRange, setDashboardRange] = useState<DashboardRangeKey>('7D');
  const [strategyMode, setStrategyMode] = useState<StrategyMode>('swing');
  const [forexSummary, setForexSummary] = useState<ForexSummaryResponse | null>(null);
  const [livePriceNow, setLivePriceNow] = useState<number | null>(null);
  const [livePriceTs, setLivePriceTs] = useState<number | null>(null);
  const [livePriceConnected, setLivePriceConnected] = useState(false);
  const [themePreference, setThemePreference] = useState<ThemePreference>('system');
  const [resolvedTheme, setResolvedTheme] = useState<ResolvedTheme>('light');
  const evaluatePollTimersRef = useRef<Record<string, number>>({});

  const readStoredAdminSecret = () => {
    if (typeof window === 'undefined') return null;
    const stored = window.localStorage.getItem(ADMIN_SECRET_STORAGE_KEY);
    const normalized = typeof stored === 'string' ? stored.trim() : '';
    return normalized || null;
  };

  const resolveAdminSecret = () => {
    const inMemory = typeof adminSecret === 'string' ? adminSecret.trim() : '';
    if (inMemory) return inMemory;
    return readStoredAdminSecret();
  };

  const buildAdminHeaders = () => {
    const secret = resolveAdminSecret();
    return secret ? { 'x-admin-access-secret': secret } : undefined;
  };

  const resolveSystemTheme = (): ResolvedTheme => {
    if (typeof window === 'undefined') return 'light';
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  };

  const handleAuthExpired = (message?: string) => {
    if (typeof window !== 'undefined') {
      window.localStorage.removeItem(ADMIN_SECRET_STORAGE_KEY);
    }
    setAdminSecret(null);
    setAdminGranted(false);
    setAdminInput('');
    setAdminError(message || 'Admin session expired. Enter ADMIN_ACCESS_SECRET again.');
  };

  const validateAdminAccess = async (secret: string | null) => {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), ADMIN_AUTH_TIMEOUT_MS);
    const normalizedSecret = typeof secret === 'string' ? secret.trim() : '';
    try {
      const res = await fetch('/api/admin-auth', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ secret: normalizedSecret }),
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
    const normalizedInput = adminInput.trim();
    const result = await validateAdminAccess(normalizedInput);
    if (result.ok) {
      if (result.required) {
        window.localStorage.setItem(ADMIN_SECRET_STORAGE_KEY, normalizedInput);
        setAdminSecret(normalizedInput);
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
  const mergeTabPatch = (symbol: string, patch: Partial<EvaluationEntry>) => {
    setTabData((prev) => {
      const current = prev[symbol] || { symbol, evaluation: {} };
      const nextEvaluation = patch.evaluation ?? current.evaluation ?? {};
      return {
        ...prev,
        [symbol]: {
          ...current,
          ...patch,
          symbol,
          evaluation: nextEvaluation,
        },
      };
    });
  };

  const loadSymbolDecision = async (symbol: string, platform?: string | null) => {
    if (!symbol) return;
    const params = new URLSearchParams({ symbol });
    if (platform) params.set('platform', platform);
    const res = await fetch(`/api/swing/dashboard/decision?${params.toString()}`, {
      headers: buildAdminHeaders(),
      cache: 'no-store',
    });
    if (res.status === 401) {
      handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
      throw new Error('Unauthorized');
    }
    if (!res.ok) {
      throw new Error(`Failed to load decision (${res.status})`);
    }
    const json: DashboardDecisionResponse = await res.json();
    mergeTabPatch(symbol, {
      lastPlatform: json.platform ?? platform ?? null,
      lastNewsSource: json.lastNewsSource ?? null,
      lastDecisionTs: json.lastDecisionTs ?? null,
      lastDecision: json.lastDecision ?? null,
      lastPrompt: json.lastPrompt ?? null,
      lastMetrics: json.lastMetrics ?? null,
      lastBiasTimeframes: json.lastBiasTimeframes ?? null,
    });
  };

  const loadSymbolEvaluation = async (symbol: string) => {
    if (!symbol) return;
    const params = new URLSearchParams({ symbol });
    const res = await fetch(`/api/swing/dashboard/evaluation?${params.toString()}`, {
      headers: buildAdminHeaders(),
      cache: 'no-store',
    });
    if (res.status === 401) {
      handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
      throw new Error('Unauthorized');
    }
    if (!res.ok) {
      throw new Error(`Failed to load evaluation (${res.status})`);
    }
    const json: DashboardEvaluationResponse = await res.json();
    mergeTabPatch(symbol, {
      evaluation: json.evaluation || {},
      evaluationTs: json.evaluationTs ?? null,
    });
  };

  const loadDashboard = async () => {
    setLoading(true);
    try {
      let summaryError: string | null = null;
      const symbolsRes = await fetch('/api/swing/dashboard/symbols', {
        headers: buildAdminHeaders(),
        cache: 'no-store',
      });
      if (!symbolsRes.ok) {
        if (symbolsRes.status === 401) {
          handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
        }
        throw new Error(`Failed to load symbols (${symbolsRes.status})`);
      }
      const symbolsJson: DashboardSymbolsResponse = await symbolsRes.json();
      const orderedSymbols = symbolsJson.symbols || [];
      const symbolMeta = new Map<string, DashboardSymbolRow>();
      for (const row of symbolsJson.data || []) {
        if (!row?.symbol) continue;
        symbolMeta.set(row.symbol.toUpperCase(), row);
      }

      const activeSymbolBefore = symbols[active] || null;
      setSymbols(orderedSymbols);
      setActive(() => {
        if (!activeSymbolBefore) return 0;
        const nextIdx = orderedSymbols.findIndex((s) => s === activeSymbolBefore);
        return nextIdx >= 0 ? nextIdx : 0;
      });

      setTabData((prev) => {
        const next: Record<string, EvaluationEntry> = {};
        for (const symbol of orderedSymbols) {
          const key = symbol.toUpperCase();
          const meta = symbolMeta.get(key);
          const existing = prev[key] || prev[symbol] || { symbol: key, evaluation: {} };
          next[key] = {
            ...existing,
            symbol: key,
            evaluation: existing.evaluation || {},
            lastPlatform: meta?.platform ?? existing.lastPlatform ?? null,
            lastNewsSource: meta?.newsSource ?? existing.lastNewsSource ?? null,
          };
        }
        return next;
      });

      try {
        const summaryParams = new URLSearchParams({ range: dashboardRange });
        const summaryRes = await fetch(`/api/swing/dashboard/summary?${summaryParams.toString()}`, {
          headers: buildAdminHeaders(),
          cache: 'no-store',
        });
        if (summaryRes.status === 401) {
          handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
          throw new Error('Unauthorized');
        }
        if (!summaryRes.ok) {
          throw new Error(`Failed to load summary (${summaryRes.status})`);
        }
        const summaryJson: DashboardSummaryResponse = await summaryRes.json();
        const summaryRows = Array.isArray(summaryJson.data) ? summaryJson.data : [];
        setTabData((prev) => {
          const next = { ...prev };
          for (const row of summaryRows) {
            if (!row?.symbol) continue;
            const key = row.symbol.toUpperCase();
            const existing = next[key] || { symbol: key, evaluation: {} };
            next[key] = {
              ...existing,
              ...row,
              symbol: key,
              evaluation: existing.evaluation || {},
            };
          }
          return next;
        });
      } catch (summaryErr: any) {
        summaryError = summaryErr?.message || 'Failed to load dashboard summary';
      }

      setError(summaryError);
    } catch (err: any) {
      setError(err?.message || 'Failed to load dashboard');
    } finally {
      setLoading(false);
    }
  };

  const loadForexDashboard = async (opts: { silent?: boolean } = {}) => {
    const silent = opts.silent === true;
    if (!silent) setLoading(true);
    try {
      const summaryRes = await fetch('/api/forex/dashboard/summary', {
        headers: buildAdminHeaders(),
        cache: 'no-store',
      });
      if (summaryRes.status === 401) {
        handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
        throw new Error('Unauthorized');
      }
      if (!summaryRes.ok) {
        throw new Error(`Failed to load forex summary (${summaryRes.status})`);
      }
      const summaryJson: ForexSummaryResponse = await summaryRes.json();
      setForexSummary(summaryJson);
      setError(null);
    } catch (err: any) {
      setError(err?.message || 'Failed to load forex dashboard');
    } finally {
      if (!silent) setLoading(false);
    }
  };

  const runForexExecute = async () => {
    if (forexExecuteSubmitting) return;
    setForexExecuteSubmitting(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        dryRun: 'false',
        notional: '100',
        t: String(Date.now()),
      });
      const executeRes = await fetch(`/api/forex/cron/execute?${params.toString()}`, {
        headers: buildAdminHeaders(),
        cache: 'no-store',
      });
      if (executeRes.status === 401) {
        handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
        throw new Error('Unauthorized');
      }
      if (!executeRes.ok) {
        let message = `Failed to run forex execute (${executeRes.status})`;
        try {
          const body = await executeRes.json();
          if (body?.error) message = `${message}: ${String(body.error)}`;
        } catch {}
        throw new Error(message);
      }
      await loadForexDashboard();
    } catch (err: any) {
      setError(err?.message || 'Failed to run forex execute');
    } finally {
      setForexExecuteSubmitting(false);
    }
  };

  const clearEvaluatePollTimer = (symbol: string) => {
    const timerId = evaluatePollTimersRef.current[symbol];
    if (timerId) {
      window.clearInterval(timerId);
      delete evaluatePollTimersRef.current[symbol];
    }
  };
  const pollEvaluationJob = async (symbol: string, jobId: string) => {
    try {
      const params = new URLSearchParams({
        jobId,
        t: String(Date.now()),
      });
      const res = await fetch(`/api/swing/evaluate?${params.toString()}`, {
        headers: buildAdminHeaders(),
        cache: 'no-store',
      });
      if (res.status === 401) {
        clearEvaluatePollTimer(symbol);
        handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
        setError('Evaluation polling unauthorized (401). Re-enter admin access secret.');
        return;
      }
      if (res.status === 304) return;
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
          try {
            await loadSymbolEvaluation(symbol);
          } catch {}
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
      const res = await fetch(`/api/swing/evaluate?${params.toString()}`, {
        headers: buildAdminHeaders(),
        cache: 'no-store',
      });
      if (!res.ok) {
        if (res.status === 401) {
          handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
        }
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

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const stored = readStoredAdminSecret();
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
    if (typeof window === 'undefined') return;
    const stored = window.localStorage.getItem(THEME_PREFERENCE_STORAGE_KEY);
    const normalizedThemePreference: ThemePreference =
      stored === 'light' || stored === 'dark' || stored === 'system' ? stored : 'system';
    setThemePreference(normalizedThemePreference);
    if (normalizedThemePreference === 'system') {
      setResolvedTheme(resolveSystemTheme());
      return;
    }
    setResolvedTheme(normalizedThemePreference);
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const stored = window.localStorage.getItem(STRATEGY_MODE_STORAGE_KEY);
    if (stored === 'forex' || stored === 'swing') {
      setStrategyMode(stored);
    }
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (themePreference !== 'system') return;
    const media = window.matchMedia('(prefers-color-scheme: dark)');
    const handleThemeChange = () => {
      setResolvedTheme(media.matches ? 'dark' : 'light');
    };
    handleThemeChange();
    if (typeof media.addEventListener === 'function') {
      media.addEventListener('change', handleThemeChange);
      return () => media.removeEventListener('change', handleThemeChange);
    }
    media.addListener(handleThemeChange);
    return () => media.removeListener(handleThemeChange);
  }, [themePreference]);

  useEffect(() => {
    if (typeof document === 'undefined') return;
    document.documentElement.style.colorScheme = resolvedTheme;
  }, [resolvedTheme]);

  useEffect(() => {
    if (!adminGranted) return;
    if (strategyMode === 'forex') {
      loadForexDashboard();
      return;
    }
    loadDashboard();
  }, [adminGranted, dashboardRange, strategyMode]);

  useEffect(() => {
    if (!adminGranted || strategyMode !== 'forex') return;
    const timerId = window.setInterval(() => {
      void loadForexDashboard({ silent: true });
    }, FOREX_LIVE_POLL_MS);
    return () => window.clearInterval(timerId);
  }, [adminGranted, strategyMode, adminSecret]);

  useEffect(() => {
    const symbol = symbols[active] || null;
    if (strategyMode !== 'swing') return;
    if (!adminGranted || !symbol) return;
    const platform = tabData[symbol]?.lastPlatform ?? null;
    let cancelled = false;
    (async () => {
      try {
        await Promise.all([loadSymbolDecision(symbol, platform), loadSymbolEvaluation(symbol)]);
        if (!cancelled) setError(null);
      } catch (err: any) {
        if (cancelled) return;
        setError(err?.message || `Failed to load details for ${symbol}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [adminGranted, symbols, active, strategyMode]);

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
    if (strategyMode !== 'swing') {
      setLivePriceNow(null);
      setLivePriceTs(null);
      setLivePriceConnected(false);
      return;
    }
    const symbol = symbols[active] || null;
    const platform = symbol ? String(tabData[symbol]?.lastPlatform || '').toLowerCase() : '';
    if (!adminGranted || !symbol) {
      setLivePriceNow(null);
      setLivePriceTs(null);
      setLivePriceConnected(false);
      return;
    }

    if (platform === 'capital') {
      let closed = false;
      let pollTimer: number | null = null;
      let inFlight: AbortController | null = null;

      const clearPoll = () => {
        if (pollTimer) {
          window.clearTimeout(pollTimer);
          pollTimer = null;
        }
        if (inFlight) {
          inFlight.abort();
          inFlight = null;
        }
      };

      const schedulePoll = () => {
        if (closed) return;
        pollTimer = window.setTimeout(() => {
          if (closed) return;
          void poll();
        }, CAPITAL_LIVE_POLL_MS);
      };

      const poll = async () => {
        if (closed) return;
        inFlight = new AbortController();
        try {
          const params = new URLSearchParams({
            symbol,
            platform: 'capital',
            t: String(Date.now()),
          });
          const res = await fetch(`/api/swing/dashboard/live-price?${params.toString()}`, {
            headers: buildAdminHeaders(),
            cache: 'no-store',
            signal: inFlight.signal,
          });
          if (res.status === 401) {
            closed = true;
            clearPoll();
            setLivePriceConnected(false);
            handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
            return;
          }
          if (!res.ok) {
            throw new Error(`Capital live price failed (${res.status})`);
          }
          const payload = await res.json();
          const px = Number(payload?.price);
          const ts = Number(payload?.ts);
          if (Number.isFinite(px) && px > 0) {
            setLivePriceNow(px);
            setLivePriceTs(Number.isFinite(ts) ? ts : Date.now());
            setLivePriceConnected(true);
          } else {
            setLivePriceConnected(false);
          }
        } catch (err: any) {
          if (err?.name !== 'AbortError') {
            setLivePriceConnected(false);
          }
        } finally {
          inFlight = null;
          schedulePoll();
        }
      };

      setLivePriceNow(null);
      setLivePriceTs(null);
      setLivePriceConnected(false);
      void poll();

      return () => {
        closed = true;
        clearPoll();
        setLivePriceConnected(false);
      };
    }

    if (platform && platform !== 'bitget') {
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
  }, [adminGranted, symbols, active, tabData, adminSecret, strategyMode]);

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
  const activePlatform = current?.lastPlatform?.toLowerCase() === 'capital' ? 'capital' : 'bitget';
  const activePlatformLogo = activePlatform === 'capital' ? '/capital.svg' : '/bitget.svg';
  const dashboardRangeText = dashboardRange === '6M' ? '6m' : dashboardRange.toLowerCase();
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
  const isInitialLoading = loading && !symbols.length;
  const loadingLabel = strategyMode === 'forex'
    ? 'Loading forex dashboard...'
    : !symbols.length
    ? 'Loading evaluations...'
    : activeSymbol
    ? `Loading ${activeSymbol}...`
    : 'Loading selected symbol...';
  const forexTotalOpenPnl = (() => {
    const rows = Array.isArray(forexSummary?.pairs) ? forexSummary.pairs : [];
    const openRows = rows.filter(
      (row) => row.openPosition?.isOpen && typeof row.openPosition?.pnlPct === 'number',
    );
    if (!openRows.length) {
      return { totalPct: null as number | null, openCount: 0 };
    }
    const totalPct = openRows.reduce((sum, row) => sum + Number(row.openPosition?.pnlPct || 0), 0);
    return { totalPct, openCount: openRows.length };
  })();

  const renderDashboardSkeleton = () => (
    <div className="grid grid-cols-1 gap-5 lg:grid-cols-2 lg:items-stretch">
      <div className="space-y-4 lg:col-span-2">
        <div className="h-full rounded-2xl border border-slate-200 bg-slate-50 p-4">
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
            {Array.from({ length: 3 }).map((_, idx) => (
              <div key={`summary-skeleton-${idx}`} className="animate-pulse space-y-2">
                <div className="h-3 w-20 rounded-full bg-slate-200" />
                <div className="h-8 w-28 rounded-lg bg-slate-200" />
                <div className="h-3 w-full max-w-[200px] rounded-full bg-slate-200" />
                <div className="h-3 w-full max-w-[150px] rounded-full bg-slate-200" />
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
        <div className="animate-pulse">
          <div className="h-3 w-24 rounded-full bg-slate-200" />
          <div className="mt-2 h-3 w-44 rounded-full bg-slate-200" />
          <div className="mt-3 h-[260px] w-full rounded-xl border border-slate-200 bg-slate-50/80 p-4">
            <div className="flex h-full w-full flex-col justify-between">
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

      <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
        <div className="animate-pulse">
          <div className="h-3 w-32 rounded-full bg-slate-200" />
          <div className="mt-3 h-4 w-3/4 rounded-full bg-slate-200" />
          <div className="mt-2 h-4 w-2/3 rounded-full bg-slate-200" />
          <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
            {Array.from({ length: 4 }).map((_, idx) => (
              <div key={`bias-skeleton-${idx}`} className="h-12 rounded-lg border border-slate-200 bg-slate-50" />
            ))}
          </div>
        </div>
      </div>

      <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
        <div className="animate-pulse">
          <div className="h-3 w-36 rounded-full bg-slate-200" />
          <div className="mt-3 h-5 w-52 rounded-full bg-slate-200" />
          <div className="mt-3 space-y-2">
            <div className="h-3 w-full rounded-full bg-slate-200" />
            <div className="h-3 w-11/12 rounded-full bg-slate-200" />
            <div className="h-3 w-10/12 rounded-full bg-slate-200" />
          </div>
        </div>
      </div>
    </div>
  );

  const handleThemeToggle = () => {
    const nextTheme: ThemePreference = resolvedTheme === 'dark' ? 'light' : 'dark';
    setThemePreference(nextTheme);
    setResolvedTheme(nextTheme);
    if (typeof window !== 'undefined') {
      window.localStorage.setItem(THEME_PREFERENCE_STORAGE_KEY, nextTheme);
    }
  };

  const handleStrategyModeChange = (mode: StrategyMode) => {
    setStrategyMode(mode);
    setError(null);
    if (typeof window !== 'undefined') {
      window.localStorage.setItem(STRATEGY_MODE_STORAGE_KEY, mode);
    }
  };

  return (
    <>
      <Head>
        <title>AI Trade Dashboard</title>
        <link rel="icon" href="/favicon.svg" type="image/svg+xml" />
      </Head>
      <div
        className={`min-h-screen px-4 py-6 relative sm:px-6 lg:px-8 ${
          resolvedTheme === 'dark'
            ? 'theme-dark bg-slate-950 text-slate-100'
            : 'theme-light bg-slate-50 text-slate-900'
        }`}
      >
        <button
          type="button"
          onClick={handleThemeToggle}
          className={`fixed right-4 top-4 z-[60] inline-flex h-10 w-10 items-center justify-center rounded-full border shadow-sm backdrop-blur transition ${
            resolvedTheme === 'dark'
              ? 'border-slate-700 bg-slate-900/90 text-slate-100 hover:border-sky-600 hover:text-sky-300'
              : 'border-slate-200 bg-white/90 text-slate-700 hover:border-sky-300 hover:text-sky-700'
          }`}
          aria-label={resolvedTheme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
          title={resolvedTheme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
        >
          {resolvedTheme === 'dark' ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
        </button>
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
        <div className="w-full">
        <div className="flex items-center justify-between gap-4 rounded-2xl border border-slate-200 bg-white px-6 py-5 shadow-sm">
          <div>
            <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Performance</p>
            <h1 className="text-3xl font-semibold leading-tight text-slate-900">AI Trade Dashboard</h1>
            <div className="mt-2 inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 p-1">
              <button
                type="button"
                onClick={() => handleStrategyModeChange('swing')}
                className={`rounded-full px-3 py-1 text-[11px] font-semibold transition ${
                  strategyMode === 'swing'
                    ? 'bg-sky-600 text-white shadow-sm'
                    : 'text-slate-600 hover:bg-slate-200/70 hover:text-slate-900'
                }`}
              >
                Swing
              </button>
              <button
                type="button"
                onClick={() => handleStrategyModeChange('forex')}
                className={`rounded-full px-3 py-1 text-[11px] font-semibold transition ${
                  strategyMode === 'forex'
                    ? 'bg-sky-600 text-white shadow-sm'
                    : 'text-slate-600 hover:bg-slate-200/70 hover:text-slate-900'
                }`}
              >
                Forex
              </button>
            </div>
            {strategyMode === 'swing' && activeSymbol && currentEvalJob ? (
              <p className="mt-1 text-xs text-slate-500">
                Eval job for {activeSymbol}:{' '}
                <span className="font-semibold text-slate-700">{currentEvalJob.status}</span>
                {currentEvalJob.error ? ` (${currentEvalJob.error})` : ''}
              </p>
            ) : null}
            {strategyMode === 'swing' && activeSymbol ? (
              <p className="mt-1 text-xs text-slate-500">
                Live price:{' '}
                <span className={livePriceConnected ? 'font-semibold text-emerald-700' : 'font-semibold text-slate-600'}>
                  {livePriceConnected ? 'connected' : 'connecting'}
                </span>
                {typeof livePriceNow === 'number' ? ` · ${livePriceNow.toFixed(2)}` : ''}
              </p>
            ) : null}
            {loading ? <p className="mt-1 text-xs text-slate-500">{loadingLabel}</p> : null}
          </div>
          <div className="flex items-center gap-2">
            {strategyMode === 'swing' ? (
              <button
                onClick={() => (activeSymbol ? triggerEvaluation(activeSymbol) : undefined)}
                disabled={!adminGranted || !activeSymbol || !!evaluateSubmittingSymbol || evaluateRunning}
                className="rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-800 transition hover:border-emerald-300 hover:bg-emerald-50 hover:text-emerald-800 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {evaluateSubmittingSymbol ? 'Queueing…' : evaluateRunning ? 'Evaluating…' : 'Run Evaluation'}
              </button>
            ) : null}
            {strategyMode === 'forex' ? (
              <button
                onClick={runForexExecute}
                disabled={!adminGranted || forexExecuteSubmitting}
                className="rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-800 transition hover:border-emerald-300 hover:bg-emerald-50 hover:text-emerald-800 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {forexExecuteSubmitting ? 'Running Execute…' : 'Run Execute (Live)'}
              </button>
            ) : null}
            <button
              onClick={() => (strategyMode === 'forex' ? loadForexDashboard() : loadDashboard())}
              disabled={!adminGranted}
              className="rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-800 transition hover:border-sky-300 hover:bg-sky-50 hover:text-sky-800 disabled:cursor-not-allowed disabled:opacity-60"
            >
              Refresh
            </button>
          </div>
        </div>

        {error && (
          <div className="mt-4 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm font-semibold text-rose-700">
            Could not load dashboard data: {error}
          </div>
        )}

        {strategyMode === 'swing' && !error && (
          <div className="mt-4 flex flex-wrap items-center gap-2">
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
            {isInitialLoading &&
              Array.from({ length: 4 }).map((_, idx) => (
                <span
                  key={`tab-skeleton-${idx}`}
                  className="h-9 w-24 animate-pulse rounded-full border border-slate-200 bg-slate-100"
                />
              ))}
          </div>
        )}

        <div className="mt-4 pb-8">
          {strategyMode === 'forex' ? (
            loading ? (
              <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
                <div className="animate-pulse space-y-3">
                  <div className="h-4 w-52 rounded-full bg-slate-200" />
                  <div className="h-3 w-72 rounded-full bg-slate-200" />
                  <div className="h-40 rounded-xl border border-slate-200 bg-slate-50" />
                </div>
              </div>
            ) : !forexSummary?.pairs?.length ? (
              <div className="flex items-center justify-center py-12 text-sm font-semibold text-slate-500">
                No forex scan data yet. Trigger `/api/forex/cron/scan` then refresh.
              </div>
            ) : (
              <div className="space-y-4">
                <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
                  <div className="text-xs uppercase tracking-wide text-slate-500">Forex Overview</div>
                  <div className="mt-2 text-sm text-slate-700">
                    Scan: {forexSummary.scanGeneratedAtMs ? formatDecisionTime(forexSummary.scanGeneratedAtMs) : 'n/a'} ·
                    Packets: {forexSummary.packetsGeneratedAtMs ? formatDecisionTime(forexSummary.packetsGeneratedAtMs) : 'n/a'} ·
                    Events: {forexSummary.staleEvents ? 'stale' : 'fresh'}
                  </div>
                  <div className="mt-2 text-sm text-slate-700">
                    Latest regime:{' '}
                    {forexSummary.packetsGeneratedAtMs ? formatDecisionTime(forexSummary.packetsGeneratedAtMs) : 'n/a'}
                    {' · '}
                    Total open PnL:{' '}
                    <span
                      className={
                        typeof forexTotalOpenPnl.totalPct === 'number'
                          ? forexTotalOpenPnl.totalPct >= 0
                            ? 'text-emerald-600'
                            : 'text-rose-600'
                          : 'text-slate-700'
                      }
                    >
                      {typeof forexTotalOpenPnl.totalPct === 'number'
                        ? `${forexTotalOpenPnl.totalPct.toFixed(2)}%`
                        : '—'}
                    </span>
                    {forexTotalOpenPnl.openCount > 0 ? ` (${forexTotalOpenPnl.openCount} open)` : ''}
                  </div>
                </div>
                <div className="overflow-auto rounded-2xl border border-slate-200 bg-white shadow-sm">
                  <table className="min-w-full text-left text-sm">
                    <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
                      <tr>
                        <th className="px-3 py-2">Pair</th>
                        <th className="px-3 py-2">Eligible</th>
                        <th className="px-3 py-2">Score</th>
                        <th className="px-3 py-2">Regime</th>
                        <th className="px-3 py-2">Permission</th>
                        <th className="px-3 py-2">Risk</th>
                        <th className="px-3 py-2">Event Gate</th>
                        <th className="px-3 py-2">Spread/ATR</th>
                        <th className="px-3 py-2">Open PnL</th>
                        <th className="px-3 py-2">Latest Exec</th>
                      </tr>
                    </thead>
                    <tbody>
                      {forexSummary.pairs.map((row) => (
                        <tr key={row.pair} className="border-t border-slate-100">
                          <td className="px-3 py-2 font-semibold text-slate-800">{row.pair}</td>
                          <td className="px-3 py-2">
                            <span
                              className={`rounded-full px-2 py-0.5 text-xs font-semibold ${
                                row.eligible ? 'bg-emerald-50 text-emerald-700' : 'bg-rose-50 text-rose-700'
                              }`}
                            >
                              {row.eligible ? 'yes' : 'no'}
                            </span>
                          </td>
                          <td className="px-3 py-2 text-slate-700">{Number(row.score || 0).toFixed(3)}</td>
                          <td className="px-3 py-2 text-slate-700">{row.packet?.regime || '—'}</td>
                          <td className="px-3 py-2 text-slate-700">{row.packet?.permission || '—'}</td>
                          <td className="px-3 py-2 text-slate-700">{row.packet?.risk_state || '—'}</td>
                          <td className="px-3 py-2 text-slate-700">
                            {row.gate?.allowNewEntries ? 'open' : 'blocked'}
                            {row.gate?.staleData ? ' (stale feed)' : ''}
                          </td>
                          <td className="px-3 py-2 text-slate-700">
                            {typeof row.metrics?.spreadPips === 'number' ? row.metrics.spreadPips.toFixed(2) : '—'} /{' '}
                            {typeof row.metrics?.spreadToAtr1h === 'number'
                              ? row.metrics.spreadToAtr1h.toFixed(3)
                              : '—'}
                          </td>
                          <td className="px-3 py-2 text-slate-700">
                            {row.openPosition?.isOpen ? (
                              <div className="space-y-0.5">
                                <div
                                  className={`font-semibold ${
                                    typeof row.openPosition?.pnlPct === 'number'
                                      ? row.openPosition.pnlPct >= 0
                                        ? 'text-emerald-600'
                                        : 'text-rose-600'
                                      : 'text-slate-800'
                                  }`}
                                >
                                  {typeof row.openPosition?.pnlPct === 'number'
                                    ? `${row.openPosition.pnlPct.toFixed(2)}%`
                                    : 'open'}
                                </div>
                                <div className="text-xs text-slate-600">
                                  {row.openPosition?.side || '—'}
                                  {typeof row.openPosition?.leverage === 'number'
                                    ? ` · ${row.openPosition.leverage.toFixed(0)}x`
                                    : ''}
                                </div>
                              </div>
                            ) : (
                              '—'
                            )}
                          </td>
                          <td className="px-3 py-2 text-slate-700">
                            {row.latestExecution?.timestampMs ? (
                              <div className="space-y-0.5">
                                <div className="font-semibold text-slate-800">
                                  {formatDecisionTime(row.latestExecution.timestampMs)}
                                </div>
                                <div className="text-xs text-slate-600">
                                  {row.latestExecution.module || '—'}
                                  {row.latestExecution.action ? ` · ${row.latestExecution.action}` : ''}
                                  {row.latestExecution.status ? ` · ${row.latestExecution.status}` : ''}
                                </div>
                              </div>
                            ) : (
                              '—'
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )
          ) : isInitialLoading ? (
            renderDashboardSkeleton()
          ) : !symbols.length ? (
            <div className="flex items-center justify-center py-12 text-sm font-semibold text-slate-500">
              No evaluations found.
            </div>
          ) : current ? (
            <div className="grid grid-cols-1 gap-5 lg:grid-cols-2 lg:items-stretch">
              <div className="space-y-4 lg:col-span-2">
                <div className="relative rounded-2xl border border-slate-200 bg-slate-50 p-4 h-full">
                  <div className="absolute right-4 top-4">
                    <img
                      src={activePlatformLogo}
                      alt={`${activePlatform} platform`}
                      className="h-5 w-auto opacity-80"
                    />
                  </div>
                  <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                    <div>
                      <div className="text-xs uppercase tracking-wide text-slate-500">{dashboardRange} PnL</div>
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
                        from {current.pnl7dTrades ?? 0} {current.pnl7dTrades === 1 ? 'trade' : 'trades'} in last{' '}
                        {dashboardRangeText}
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
                  key={activeSymbol}
                  symbol={activeSymbol}
                  platform={current?.lastPlatform || null}
                  adminSecret={resolveAdminSecret()}
                  adminGranted={adminGranted}
                  isDark={resolvedTheme === 'dark'}
                  rangeKey={dashboardRange}
                  onRangeChange={setDashboardRange}
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
            renderDashboardSkeleton()
          )}
        </div>
      </div>
      </div>
    </>
  );
}
