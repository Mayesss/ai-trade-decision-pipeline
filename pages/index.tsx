import React, { useEffect, useState } from 'react';
import {
  Activity,
  BarChart3,
  BookOpen,
  Circle,
  Cpu,
  ArrowUp,
  ArrowDown,
  Minus,
  Database,
  Layers3,
  PenTool,
  Repeat,
  ShieldCheck,
  Zap,
  type LucideIcon,
} from 'lucide-react';
type Evaluation = {
  overall_rating?: number;
  overview?: string;
  what_went_well?: string[];
  issues?: string[];
  improvements?: string[];
  confidence?: string;
  aspects?: Record<
    string,
    {
      rating?: number;
      comment?: string;
    }
  >;
};

type EvaluationEntry = {
  symbol: string;
  evaluation: Evaluation;
  pnl24h?: number | null;
  pnl24hTrades?: number | null;
  openPnl?: number | null;
  lastDecisionTs?: number | null;
  lastDecision?: {
    action?: string;
    summary?: string;
    reason?: string;
    signal_strength?: string;
    [key: string]: any;
  } | null;
  lastMetrics?: Record<string, any> | null;
};

type EvaluationsResponse = {
  symbols: string[];
  data: EvaluationEntry[];
};

export default function Home() {
  const [symbols, setSymbols] = useState<string[]>([]);
  const [active, setActive] = useState(0);
  const [tabData, setTabData] = useState<Record<string, EvaluationEntry>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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
  };

  const formatLabel = (key: string) => key.replace(/_/g, ' ');
  const renderBias = (label: string, bias?: string | null) => {
    if (!bias) return null;
    const norm = String(bias || '').toUpperCase();
    const meta =
      norm === 'UP'
        ? { Icon: ArrowUp, color: 'text-emerald-600', bg: 'bg-emerald-50' }
        : norm === 'DOWN'
        ? { Icon: ArrowDown, color: 'text-rose-600', bg: 'bg-rose-50' }
        : { Icon: Minus, color: 'text-slate-600', bg: 'bg-slate-100' };
    const Icon = meta.Icon;
    return (
      <div className="flex items-center gap-2">
        <span className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">{label}</span>
        <span className={`flex h-8 w-8 items-center justify-center rounded-lg border border-slate-200 ${meta.bg}`}>
          <Icon className={`h-4 w-4 ${meta.color}`} />
        </span>
      </div>
    );
  };

  const loadEvaluations = async () => {
    try {
      setLoading(true);
      const res = await fetch('/api/evaluations');
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
    loadEvaluations();
  }, []);

  const current = symbols[active] ? tabData[symbols[active]] : null;

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900 flex items-center justify-center px-4 py-10">
      <div className="w-full max-w-6xl rounded-3xl border border-slate-200 bg-white shadow-xl">
        <div className="flex items-center justify-between gap-4 border-b border-slate-200 px-6 py-5">
          <div>
            <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Performance</p>
            <h1 className="text-3xl font-semibold leading-tight text-slate-900">AI Trade Dashboard</h1>
          </div>
          <button
            onClick={loadEvaluations}
            className="rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-800 transition hover:border-sky-300 hover:bg-sky-50 hover:text-sky-800"
          >
            Refresh
          </button>
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
              return (
                <button
                  key={sym}
                  onClick={() => setActive(i)}
                  className={`rounded-full border px-4 py-2 text-sm font-semibold transition ${
                    isActive
                      ? 'border-sky-400 bg-sky-100 text-sky-800 shadow-sm'
                      : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:text-slate-800'
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
            <div className="grid grid-cols-1 gap-5 lg:grid-cols-3">
              <div className="space-y-4 lg:col-span-1">
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                    <div>
                      <div className="text-xs uppercase tracking-wide text-slate-500">24h PnL</div>
                      <div className="mt-3 text-3xl font-semibold text-slate-900">
                        <span
                          className={
                            typeof current.pnl24h === 'number'
                              ? current.pnl24h >= 0
                                ? 'text-emerald-600'
                                : 'text-rose-600'
                              : 'text-slate-500'
                          }
                        >
                          {typeof current.pnl24h === 'number' ? `${current.pnl24h.toFixed(2)}%` : '—'}
                        </span>
                      </div>
                      <p className="mt-1 text-xs text-slate-500">
                        from {current.pnl24hTrades ?? 0} {current.pnl24hTrades === 1 ? 'trade' : 'trades'}
                      </p>
                    </div>
                    <div>
                      <div className="text-xs uppercase tracking-wide text-slate-500">Open PnL</div>
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
                      <p className="mt-1 text-xs text-slate-500">current position</p>
                    </div>
                  </div>
                </div>

                <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
                  <div className="flex items-center justify-between">
                    <div className="text-xs uppercase tracking-wide text-slate-500">Latest Evaluation</div>
                    {current.evaluation.confidence && (
                      <div className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
                        Confidence: {current.evaluation.confidence}
                      </div>
                    )}
                  </div>
                  <div className="mt-2 text-lg font-semibold text-slate-900">
                    Rating: <span className="text-sky-600">{current.evaluation.overall_rating ?? '—'}</span>
                  </div>
                  <p className="mt-3 text-sm text-slate-700">
                    {current.evaluation.overview || 'No overview provided.'}
                  </p>
                </div>

                {current.lastDecision && (
                  <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
                    <div className="flex items-center justify-between">
                      <div className="text-xs uppercase tracking-wide text-slate-500">Latest Decision</div>
                      {current.lastDecision.signal_strength && (
                        <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-semibold text-slate-700">
                          Strength: {current.lastDecision.signal_strength}
                        </span>
                      )}
                    </div>
                    <div className="mt-3 text-sm text-slate-800">
                      Action:{' '}
                      <span className="font-semibold text-sky-700">
                        {(current.lastDecision.action || '').toString() || '—'}
                      </span>
                      {current.lastDecision.summary ? ` · ${current.lastDecision.summary}` : ''}
                    </div>
                    {(current.lastDecision.micro_bias ||
                      current.lastDecision.macro_bias ||
                      current.lastDecision.primary_bias ||
                      current.lastDecision.bias) && (
                      <div className="mt-3 flex flex-wrap items-center gap-3">
                        {renderBias('Micro', current.lastDecision.micro_bias)}
                        {renderBias('Primary', current.lastDecision.primary_bias || current.lastDecision.bias)}
                        {renderBias('Macro', current.lastDecision.macro_bias)}
                      </div>
                    )}
                    {current.lastDecisionTs && (
                      <div className="mt-1 text-xs text-slate-500">
                        Decided at {new Date(current.lastDecisionTs).toLocaleString('de-DE')}
                      </div>
                    )}
                  </div>
                )}
              </div>

              <div className="space-y-4 lg:col-span-2">
                {current.evaluation.aspects && (
                  <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
                    <div className="text-xs uppercase tracking-wide text-slate-500">Aspect Ratings</div>
                    <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2">
                      {Object.entries(current.evaluation.aspects).map(([key, val]) => (
                        <div
                          key={key}
                          className="rounded-xl border border-slate-200 bg-slate-50 p-3 shadow-inner shadow-slate-100"
                        >
                          <div className="flex items-center justify-between">
                            <div className="flex items-center gap-2">
                              {(() => {
                                const meta = aspectMeta[key] || { Icon: Circle, color: 'text-slate-600', bg: 'bg-slate-100' };
                                const Icon = meta.Icon;
                                return (
                                  <span
                                    className={`flex h-9 w-9 items-center justify-center rounded-full ${meta.bg} ${meta.color}`}
                                  >
                                    <Icon className="h-4 w-4" />
                                  </span>
                                );
                              })()}
                              <div className="text-sm font-semibold text-slate-900">
                                {formatLabel(key)}
                              </div>
                            </div>
                            <div className="flex items-center gap-3">
                              <div className="flex h-3 w-20 items-center rounded-full bg-slate-100">
                                <div
                                  className="h-full rounded-full bg-gradient-to-r from-rose-500 via-amber-400 to-emerald-500"
                                  style={{
                                    width: `${Math.max(0, Math.min(10, Number(val?.rating ?? 0))) * 10}%`,
                                  }}
                                />
                              </div>
                              <div className="text-lg font-semibold text-sky-700">{val?.rating ?? '—'}</div>
                            </div>
                          </div>
                          <p className="mt-2 text-xs text-slate-600">{val?.comment || 'No comment'}</p>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>

              {(current.evaluation.what_went_well?.length ||
                current.evaluation.issues?.length ||
                current.evaluation.improvements?.length) && (
                <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-3">
                  <div className="text-xs uppercase tracking-wide text-slate-500">Details</div>
                  <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-3">
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
          ) : (
            <div className="flex items-center justify-center py-12 text-sm font-semibold text-slate-500">
              Loading...
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
