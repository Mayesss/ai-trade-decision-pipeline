import Head from 'next/head';
import {
  Activity,
  ArrowUpRight,
  BrainCircuit,
  CandlestickChart,
  CheckCircle2,
  Clock,
  Loader2,
  PauseCircle,
  Radar,
  RefreshCw,
  Search,
  ShieldCheck,
  TriangleAlert,
  Zap,
} from 'lucide-react';
import { useCallback, useEffect, useId, useState } from 'react';

/* ------------------------------------------------------------------ */
/*  Types matching /api/scalp/v2/dashboard/summary response            */
/* ------------------------------------------------------------------ */

type JobRow = {
  jobKind: string;
  status: string;
  updatedAt?: string;
  lockedAt?: string;
  attempts?: number;
};

type CandidateRow = {
  id: number;
  venue: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: string;
  score: number;
  status: string;
  metadata?: Record<string, any>;
};

type DeploymentRow = {
  deploymentId: string;
  venue: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: string;
  enabled: boolean;
  liveMode: string;
  promotionGate?: Record<string, any>;
  riskProfile?: Record<string, any>;
};

type CursorRow = {
  cursorKey: string;
  venue: string;
  symbol: string;
  entrySessionProfile: string;
  phase: string;
  lastCandidateOffset: number;
  lastWeekStartMs: number | null;
  progress?: Record<string, any>;
};

type HighlightRow = {
  candidateId: string;
  venue: string;
  symbol: string;
  entrySessionProfile: string;
  score: number;
  trades12w: number;
  winningWeeks12w: number;
  consecutiveWinningWeeks: number;
  robustness?: Record<string, any>;
  dsl?: Record<string, any>;
  remarkable: boolean;
};

type LedgerRow = {
  deploymentId: string;
  venue: string;
  symbol: string;
  rMultiple: number;
  closeType: string;
  tsExitMs: number;
};

type RuntimeConfig = {
  enabled: boolean;
  liveEnabled: boolean;
  dryRunDefault: boolean;
  supportedVenues: string[];
  supportedSessions: string[];
  seedSymbolsByVenue: Record<string, string[]>;
  budgets: {
    maxCandidatesTotal: number;
    maxCandidatesPerSymbol: number;
    maxEnabledDeployments: number;
  };
  riskProfile: {
    riskPerTradePct: number;
    maxOpenPositionsPerSymbol: number;
    autoPauseDailyR: number;
    autoPause30dR: number;
  };
};

type SummaryResponse = {
  ok: boolean;
  runtime: RuntimeConfig;
  summary: {
    candidates: number;
    deployments: number;
    enabledDeployments: number;
    events24h: number;
    ledgerRows30d: number;
    netR30d: number | null;
  };
  deployments: DeploymentRow[];
  events: any[];
  ledger: LedgerRow[];
  jobs: JobRow[];
  candidates: CandidateRow[];
  researchCursors: CursorRow[];
  researchHighlights: HighlightRow[];
};

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

type Tone = 'positive' | 'warning' | 'critical' | 'neutral';

function toneBg(tone: Tone): string {
  if (tone === 'positive') return 'from-emerald-400 to-teal-300';
  if (tone === 'warning') return 'from-amber-300 to-orange-300';
  if (tone === 'critical') return 'from-rose-400 to-pink-400';
  return 'from-sky-400 to-cyan-300';
}

function toneTxt(tone: Tone): string {
  if (tone === 'positive') return 'text-emerald-100';
  if (tone === 'warning') return 'text-amber-100';
  if (tone === 'critical') return 'text-rose-100';
  return 'text-sky-100';
}

function badge(tone: Tone): string {
  if (tone === 'positive') return 'border-emerald-300/40 bg-emerald-300/20 text-emerald-100';
  if (tone === 'warning') return 'border-amber-300/40 bg-amber-300/20 text-amber-100';
  if (tone === 'critical') return 'border-rose-300/45 bg-rose-300/20 text-rose-100';
  return 'border-sky-300/40 bg-sky-300/20 text-sky-100';
}

function jobTone(status: string): Tone {
  if (status === 'succeeded') return 'positive';
  if (status === 'running') return 'warning';
  if (status === 'failed') return 'critical';
  return 'neutral';
}

function timeAgo(dateStr?: string): string {
  if (!dateStr) return '--';
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

function Sparkline({ points, tone }: { points: number[]; tone: Tone }) {
  const gradientId = useId().replace(/:/g, '');
  if (points.length < 2) return null;
  const width = 176;
  const height = 48;
  const min = Math.min(...points);
  const max = Math.max(...points);
  const range = Math.max(1, max - min);
  const coords = points.map((v, i) => ({
    x: (i / Math.max(1, points.length - 1)) * width,
    y: height - ((v - min) / range) * height,
  }));
  const line = coords.map((c) => `${c.x.toFixed(2)},${c.y.toFixed(2)}`).join(' ');
  const area = `${line} ${width},${height} 0,${height}`;
  const stroke =
    tone === 'positive' ? '#34d399' : tone === 'warning' ? '#fbbf24' : tone === 'critical' ? '#fb7185' : '#38bdf8';
  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="h-12 w-full">
      <defs>
        <linearGradient id={`sf-${gradientId}`} x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor={stroke} stopOpacity="0.42" />
          <stop offset="100%" stopColor={stroke} stopOpacity="0" />
        </linearGradient>
      </defs>
      <polyline points={area} fill={`url(#sf-${gradientId})`} />
      <polyline points={line} fill="none" stroke={stroke} strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function FunnelBar({ label, count, total, tone }: { label: string; count: number; total: number; tone: Tone }) {
  const pct = total > 0 ? Math.round((count / total) * 100) : 0;
  const barColor =
    tone === 'positive' ? 'bg-emerald-400' : tone === 'warning' ? 'bg-amber-300' : tone === 'critical' ? 'bg-rose-400' : 'bg-sky-400';
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between text-xs">
        <span className="text-slate-300">{label}</span>
        <span className="text-slate-400">
          {count} <span className="text-slate-500">({pct}%)</span>
        </span>
      </div>
      <div className="h-2 overflow-hidden rounded-full bg-slate-800">
        <div className={`h-full rounded-full ${barColor}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function RingChart({ slices }: { slices: { id: string; label: string; pct: number; tone: Tone }[] }) {
  const size = 170;
  const stroke = 22;
  const radius = (size - stroke) / 2;
  const circumference = 2 * Math.PI * radius;
  let progress = 0;
  return (
    <svg viewBox={`0 0 ${size} ${size}`} className="h-44 w-44">
      <circle cx={size / 2} cy={size / 2} r={radius} fill="none" stroke="#112236" strokeWidth={stroke} />
      {slices.map((slice) => {
        const ratio = slice.pct / 100;
        const dash = circumference * ratio;
        const gap = circumference - dash;
        const rotation = (progress / 100) * 360 - 90;
        progress += slice.pct;
        const color = slice.tone === 'positive' ? '#34d399' : slice.tone === 'warning' ? '#fbbf24' : slice.tone === 'critical' ? '#fb7185' : '#38bdf8';
        return (
          <circle key={slice.id} cx={size / 2} cy={size / 2} r={radius} fill="none"
            stroke={color} strokeWidth={stroke} strokeDasharray={`${dash} ${gap}`}
            transform={`rotate(${rotation} ${size / 2} ${size / 2})`} strokeLinecap="round" />
        );
      })}
    </svg>
  );
}

/* ------------------------------------------------------------------ */
/*  Main page component                                                */
/* ------------------------------------------------------------------ */

export default function ScalpUiRedesignPage() {
  const [data, setData] = useState<SummaryResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastFetch, setLastFetch] = useState<number>(0);

  const fetchData = useCallback(async () => {
    try {
      setLoading(true);
      const res = await fetch('/api/scalp/v2/dashboard/summary');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
      setError(null);
      setLastFetch(Date.now());
    } catch (err: any) {
      setError(err?.message || 'Failed to fetch');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Derived metrics
  const candidates = data?.candidates || [];
  const deployments = data?.deployments || [];
  const jobs = data?.jobs || [];
  const cursors = data?.researchCursors || [];
  const highlights = data?.researchHighlights || [];
  const ledger = data?.ledger || [];
  const runtime = data?.runtime;
  const summary = data?.summary;

  const candidatesByStatus = candidates.reduce<Record<string, number>>((acc, c) => {
    acc[c.status] = (acc[c.status] || 0) + 1;
    return acc;
  }, {});

  const enabledDeploys = deployments.filter((d) => d.enabled);
  const liveDeploys = enabledDeploys.filter((d) => d.liveMode === 'live');

  const jobByKind = new Map<string, JobRow>();
  for (const j of jobs) {
    const existing = jobByKind.get(j.jobKind);
    if (!existing || (j.updatedAt && (!existing.updatedAt || j.updatedAt > existing.updatedAt))) {
      jobByKind.set(j.jobKind, j);
    }
  }

  // Cumulative ledger PnL sparkline
  const ledgerSorted = [...ledger].sort((a, b) => a.tsExitMs - b.tsExitMs);
  const pnlSpark: number[] = [];
  let cumR = 0;
  for (const row of ledgerSorted) {
    cumR += row.rMultiple || 0;
    pnlSpark.push(cumR);
  }

  // Research funnel slices for ring chart
  const totalCandidates = candidates.length;
  const evaluatedCount = candidatesByStatus['evaluated'] || 0;
  const promotedCount = candidatesByStatus['promoted'] || 0;
  const rejectedCount = candidatesByStatus['rejected'] || 0;
  const otherCount = Math.max(0, totalCandidates - evaluatedCount - promotedCount - rejectedCount);
  const funnelSlices = [
    { id: 'promoted', label: 'Promoted', pct: totalCandidates > 0 ? Math.round((promotedCount / totalCandidates) * 100) : 0, tone: 'positive' as Tone },
    { id: 'evaluated', label: 'Evaluated', pct: totalCandidates > 0 ? Math.round((evaluatedCount / totalCandidates) * 100) : 0, tone: 'neutral' as Tone },
    { id: 'rejected', label: 'Rejected', pct: totalCandidates > 0 ? Math.round((rejectedCount / totalCandidates) * 100) : 0, tone: 'critical' as Tone },
    { id: 'other', label: 'Other', pct: totalCandidates > 0 ? Math.max(0, 100 - Math.round((promotedCount / totalCandidates) * 100) - Math.round((evaluatedCount / totalCandidates) * 100) - Math.round((rejectedCount / totalCandidates) * 100)) : 0, tone: 'warning' as Tone },
  ].filter((s) => s.pct > 0);

  const FONT = { fontFamily: 'Sora, sans-serif' };
  const MONO = { fontFamily: 'IBM Plex Mono, monospace' };

  return (
    <>
      <Head>
        <title>Scalp V2 Pipeline</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="" />
        <link href="https://fonts.googleapis.com/css2?family=Sora:wght@500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet" />
      </Head>

      <main className="min-h-screen bg-[#060d14] text-slate-100">
        <div className="mx-auto max-w-[1450px] px-4 py-6 sm:px-6 lg:px-8">

          {/* ---- Header ---- */}
          <section className="relative overflow-hidden rounded-[28px] border border-cyan-300/25 bg-[radial-gradient(circle_at_0%_10%,rgba(56,189,248,0.32),transparent_45%),radial-gradient(circle_at_100%_0%,rgba(16,185,129,0.2),transparent_40%),linear-gradient(145deg,#091421,#0d1d2e_45%,#071019)] p-5 sm:p-6">
            <div className="absolute right-4 top-4 flex items-center gap-2">
              {runtime && (
                <span className={`rounded-full border px-3 py-1 text-[11px] ${runtime.liveEnabled ? badge('positive') : badge('warning')}`}>
                  {runtime.liveEnabled ? 'LIVE' : 'DRY RUN'}
                </span>
              )}
              <button onClick={fetchData} disabled={loading} className="rounded-full border border-cyan-200/35 bg-cyan-200/10 p-1.5 text-cyan-100 hover:bg-cyan-200/20 disabled:opacity-50">
                {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
              </button>
            </div>
            <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/80" style={MONO}>Scalp V2 Pipeline</p>
            <h1 className="mt-2 max-w-3xl text-2xl sm:text-3xl" style={FONT}>
              Tree-adaptive pattern research &amp; live execution
            </h1>

            {error && (
              <div className="mt-3 rounded-xl border border-rose-400/30 bg-rose-400/10 px-4 py-2 text-sm text-rose-200">
                {error}
              </div>
            )}

            {/* ---- Pulse cards ---- */}
            <div className="mt-5 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
              {[
                {
                  id: 'candidates', label: 'Candidates', icon: Search,
                  value: String(totalCandidates),
                  delta: `${promotedCount} promoted`,
                  tone: (promotedCount > 0 ? 'positive' : totalCandidates > 0 ? 'neutral' : 'warning') as Tone,
                },
                {
                  id: 'deployments', label: 'Deployments', icon: Zap,
                  value: `${enabledDeploys.length} enabled`,
                  delta: `${liveDeploys.length} live`,
                  tone: (liveDeploys.length > 0 ? 'positive' : enabledDeploys.length > 0 ? 'warning' : 'neutral') as Tone,
                },
                {
                  id: 'trades', label: 'Trades (30d)', icon: CandlestickChart,
                  value: String(summary?.ledgerRows30d || 0),
                  delta: `${(summary?.netR30d || 0).toFixed(1)}R net`,
                  tone: ((summary?.netR30d || 0) > 0 ? 'positive' : (summary?.netR30d || 0) < 0 ? 'critical' : 'neutral') as Tone,
                },
                {
                  id: 'highlights', label: 'Highlights', icon: CheckCircle2,
                  value: String(highlights.length),
                  delta: `${highlights.filter((h) => h.remarkable).length} remarkable`,
                  tone: (highlights.length > 0 ? 'positive' : 'neutral') as Tone,
                },
              ].map((card) => {
                const Icon = card.icon;
                return (
                  <article key={card.id} className="rounded-2xl border border-white/10 bg-slate-900/55 p-3">
                    <div className="flex items-center justify-between">
                      <div className={`inline-flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br ${toneBg(card.tone)}`}>
                        <Icon className="h-4 w-4 text-[#031018]" />
                      </div>
                      <span className={`text-[11px] ${toneTxt(card.tone)}`}>{card.delta}</span>
                    </div>
                    <p className="mt-3 text-xs text-slate-300">{card.label}</p>
                    <p className="text-xl font-semibold" style={FONT}>{card.value}</p>
                  </article>
                );
              })}
            </div>
          </section>

          {/* ---- Pipeline Jobs + Research Funnel ---- */}
          <section className="mt-5 grid grid-cols-1 gap-5 xl:grid-cols-[1.3fr_1fr]">

            {/* Jobs */}
            <article className="rounded-3xl border border-slate-700/60 bg-[#0b1623] p-4 sm:p-5">
              <div className="mb-4 flex items-center justify-between">
                <h2 className="text-lg sm:text-xl" style={FONT}>Pipeline Jobs</h2>
                <span className="rounded-full border border-sky-300/30 bg-sky-300/15 px-2.5 py-1 text-xs text-sky-100">
                  {jobs.length} jobs
                </span>
              </div>
              <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
                {(['research', 'promote', 'execute', 'reconcile'] as const).map((kind) => {
                  const job = jobByKind.get(kind);
                  const status = job?.status || 'pending';
                  const tone = jobTone(status);
                  const icons: Record<string, any> = {
                    research: BrainCircuit, promote: ArrowUpRight,
                    execute: CandlestickChart, reconcile: Activity,
                  };
                  const Icon = icons[kind] || Radar;
                  return (
                    <div key={kind} className="rounded-2xl border border-white/10 bg-slate-900/60 p-3">
                      <div className="flex items-start justify-between">
                        <div className="inline-flex h-9 w-9 items-center justify-center rounded-xl border border-slate-700/80 bg-[#0b1320]">
                          <Icon className="h-4 w-4 text-slate-100" />
                        </div>
                        <span className={`rounded-full border px-2 py-0.5 text-[10px] ${badge(tone)}`}>{status}</span>
                      </div>
                      <p className="mt-3 text-sm text-slate-200 capitalize">{kind}</p>
                      <p className="mt-1 text-[10px] text-slate-500">{timeAgo(job?.updatedAt)}</p>
                      <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-slate-800">
                        <div className={`h-full w-full ${tone === 'positive' ? 'bg-emerald-400' : tone === 'warning' ? 'bg-amber-300' : tone === 'critical' ? 'bg-rose-400' : 'bg-sky-400'}`} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </article>

            {/* Research funnel */}
            <article className="rounded-3xl border border-slate-700/60 bg-[#0b1623] p-4 sm:p-5">
              <h2 className="text-lg sm:text-xl" style={FONT}>Research Funnel</h2>
              {totalCandidates > 0 ? (
                <div className="mt-3 flex flex-col items-center gap-3">
                  <RingChart slices={funnelSlices} />
                  <div className="grid w-full grid-cols-2 gap-2">
                    {funnelSlices.map((s) => (
                      <div key={s.id} className="rounded-xl border border-white/10 bg-slate-900/60 px-3 py-2">
                        <p className="text-xs text-slate-300">{s.label}</p>
                        <p className={`text-sm font-semibold ${toneTxt(s.tone)}`}>{s.pct}%</p>
                      </div>
                    ))}
                  </div>
                </div>
              ) : (
                <div className="mt-8 flex flex-col items-center gap-2 text-slate-500">
                  <Search className="h-8 w-8" />
                  <p className="text-sm">No candidates yet — research pipeline starting</p>
                </div>
              )}
            </article>
          </section>

          {/* ---- Research Cursors ---- */}
          <section className="mt-5 rounded-3xl border border-slate-700/60 bg-[#0b1623] p-4 sm:p-5">
            <div className="mb-3 flex items-center justify-between">
              <h2 className="text-lg sm:text-xl" style={FONT}>Research Cursors</h2>
              <span className="rounded-full border border-slate-600 bg-slate-800/80 px-2.5 py-1 text-xs text-slate-300">
                {cursors.length} scopes
              </span>
            </div>
            {cursors.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead>
                    <tr className="border-b border-slate-700/60 text-xs text-slate-400">
                      <th className="pb-2 pr-4">Venue</th>
                      <th className="pb-2 pr-4">Symbol</th>
                      <th className="pb-2 pr-4">Session</th>
                      <th className="pb-2 pr-4">Phase</th>
                      <th className="pb-2 pr-4">Offset</th>
                      <th className="pb-2">Week Start</th>
                    </tr>
                  </thead>
                  <tbody>
                    {cursors.map((c) => (
                      <tr key={c.cursorKey} className="border-b border-slate-800/50">
                        <td className="py-2 pr-4 text-slate-300">{c.venue}</td>
                        <td className="py-2 pr-4 font-medium" style={MONO}>{c.symbol}</td>
                        <td className="py-2 pr-4 text-slate-300">{c.entrySessionProfile}</td>
                        <td className="py-2 pr-4">
                          <span className={`rounded-full border px-2 py-0.5 text-[10px] ${badge(c.phase === 'promote' ? 'positive' : c.phase === 'validate' ? 'warning' : 'neutral')}`}>
                            {c.phase}
                          </span>
                        </td>
                        <td className="py-2 pr-4 text-slate-400" style={MONO}>{c.lastCandidateOffset}</td>
                        <td className="py-2 text-slate-400" style={MONO}>
                          {c.lastWeekStartMs ? new Date(c.lastWeekStartMs).toISOString().slice(0, 10) : '--'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="py-6 text-center text-sm text-slate-500">No research cursors — waiting for first cycle</p>
            )}
          </section>

          {/* ---- Deployments ---- */}
          <section className="mt-5 rounded-3xl border border-slate-700/60 bg-[#0b1623] p-4 sm:p-5">
            <div className="mb-3 flex items-center justify-between">
              <h2 className="text-lg sm:text-xl" style={FONT}>Deployments</h2>
              <span className={`rounded-full border px-2.5 py-1 text-xs ${badge(enabledDeploys.length > 0 ? 'positive' : 'neutral')}`}>
                {enabledDeploys.length} / {runtime?.budgets.maxEnabledDeployments || '?'} slots
              </span>
            </div>
            {deployments.length > 0 ? (
              <div className="grid grid-cols-1 gap-3 lg:grid-cols-2 2xl:grid-cols-3">
                {deployments.map((d) => {
                  const tone: Tone = d.enabled && d.liveMode === 'live' ? 'positive' : d.enabled ? 'warning' : 'neutral';
                  return (
                    <article key={d.deploymentId} className="rounded-2xl border border-white/10 bg-slate-900/60 p-3">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <div className={`inline-flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br ${toneBg(tone)}`}>
                            <CandlestickChart className="h-4 w-4 text-[#031018]" />
                          </div>
                          <div>
                            <p className="text-sm font-semibold" style={FONT}>{d.symbol}</p>
                            <p className="text-[11px] text-slate-400">{d.venue} / {d.entrySessionProfile}</p>
                          </div>
                        </div>
                        <span className={`rounded-full border px-2 py-0.5 text-[10px] ${badge(tone)}`}>
                          {d.enabled ? d.liveMode : 'disabled'}
                        </span>
                      </div>
                      <div className="mt-2 flex items-center gap-3 text-[11px] text-slate-400" style={MONO}>
                        <span>tune: {d.tuneId}</span>
                        {d.promotionGate?.reason && (
                          <span className="truncate text-slate-500">{String(d.promotionGate.reason)}</span>
                        )}
                      </div>
                    </article>
                  );
                })}
              </div>
            ) : (
              <div className="py-8 text-center">
                <PauseCircle className="mx-auto h-8 w-8 text-slate-600" />
                <p className="mt-2 text-sm text-slate-500">No deployments — stage-C pass required for promotion</p>
              </div>
            )}
          </section>

          {/* ---- Highlights ---- */}
          {highlights.length > 0 && (
            <section className="mt-5 rounded-3xl border border-slate-700/60 bg-[#0b1623] p-4 sm:p-5">
              <div className="mb-3 flex items-center justify-between">
                <h2 className="text-lg sm:text-xl" style={FONT}>Research Highlights</h2>
                <span className="rounded-full border border-emerald-300/35 bg-emerald-300/15 px-2.5 py-1 text-xs text-emerald-100">
                  {highlights.filter((h) => h.remarkable).length} remarkable
                </span>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead>
                    <tr className="border-b border-slate-700/60 text-xs text-slate-400">
                      <th className="pb-2 pr-4">Symbol</th>
                      <th className="pb-2 pr-4">Session</th>
                      <th className="pb-2 pr-4">Score</th>
                      <th className="pb-2 pr-4">12w Trades</th>
                      <th className="pb-2 pr-4">Win Weeks</th>
                      <th className="pb-2 pr-4">Consec.</th>
                      <th className="pb-2"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {highlights.slice(0, 20).map((h, idx) => (
                      <tr key={`${h.candidateId}-${idx}`} className="border-b border-slate-800/50">
                        <td className="py-2 pr-4 font-medium" style={MONO}>{h.symbol}</td>
                        <td className="py-2 pr-4 text-slate-300">{h.entrySessionProfile}</td>
                        <td className="py-2 pr-4 text-slate-200" style={MONO}>{h.score.toFixed(1)}</td>
                        <td className="py-2 pr-4 text-slate-300" style={MONO}>{h.trades12w}</td>
                        <td className="py-2 pr-4 text-slate-300" style={MONO}>{h.winningWeeks12w}</td>
                        <td className="py-2 pr-4 text-slate-300" style={MONO}>{h.consecutiveWinningWeeks}</td>
                        <td className="py-2">
                          {h.remarkable && <CheckCircle2 className="h-4 w-4 text-emerald-400" />}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}

          {/* ---- PnL Curve ---- */}
          {pnlSpark.length > 1 && (
            <section className="mt-5 rounded-3xl border border-slate-700/60 bg-[#0b1623] p-4 sm:p-5">
              <div className="mb-3 flex items-center justify-between">
                <h2 className="text-lg sm:text-xl" style={FONT}>Cumulative PnL (R)</h2>
                <span className={`rounded-full border px-2.5 py-1 text-xs ${badge(cumR >= 0 ? 'positive' : 'critical')}`}>
                  {cumR >= 0 ? '+' : ''}{cumR.toFixed(1)}R
                </span>
              </div>
              <Sparkline points={pnlSpark} tone={cumR >= 0 ? 'positive' : 'critical'} />
            </section>
          )}

          {/* ---- Risk Profile ---- */}
          {runtime && (
            <section className="mt-5 rounded-3xl border border-slate-700/60 bg-[#0b1623] p-4 sm:p-5">
              <h2 className="mb-3 text-lg sm:text-xl" style={FONT}>Runtime Config</h2>
              <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
                {[
                  { label: 'Risk/Trade', value: `${runtime.riskProfile.riskPerTradePct}%` },
                  { label: 'Max Pos/Symbol', value: String(runtime.riskProfile.maxOpenPositionsPerSymbol) },
                  { label: 'Daily R Pause', value: `${runtime.riskProfile.autoPauseDailyR}R` },
                  { label: '30d R Pause', value: `${runtime.riskProfile.autoPause30dR}R` },
                  { label: 'Max Candidates', value: String(runtime.budgets.maxCandidatesTotal) },
                  { label: 'Max Deployments', value: String(runtime.budgets.maxEnabledDeployments) },
                ].map((item) => (
                  <div key={item.label} className="rounded-xl border border-white/10 bg-slate-900/60 px-3 py-2">
                    <p className="text-[10px] text-slate-400">{item.label}</p>
                    <p className="text-sm font-semibold" style={MONO}>{item.value}</p>
                  </div>
                ))}
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                {runtime.supportedVenues.map((v) => (
                  <span key={v} className="rounded-full border border-slate-600 bg-slate-800/80 px-2.5 py-1 text-[11px] text-slate-300">
                    {v}: {(runtime.seedSymbolsByVenue[v] || []).join(', ') || 'none'}
                  </span>
                ))}
              </div>
            </section>
          )}

          {/* ---- Footer ---- */}
          <section className="mt-5 flex items-center justify-between">
            <p className="text-[11px] text-slate-500" style={MONO}>
              {lastFetch > 0 ? `Last fetch: ${new Date(lastFetch).toLocaleTimeString()}` : ''}
            </p>
            <p className="rounded-full border border-slate-700 bg-slate-900/60 px-3 py-1 text-[11px] text-slate-400">
              auto-refresh 30s
            </p>
          </section>
        </div>
      </main>
    </>
  );
}
