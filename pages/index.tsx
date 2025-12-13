import React, { useEffect, useRef, useState } from 'react';
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
  Star,
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
  openDirection?: 'long' | 'short' | null;
  lastPositionPnl?: number | null;
  lastPositionDirection?: 'long' | 'short' | null;
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
  const [showAspects, setShowAspects] = useState(false);
  const [chartData, setChartData] = useState<{ time: number; value: number }[]>([]);
  const [chartMarkers, setChartMarkers] = useState<any[]>([]);
  const chartContainerRef = useRef<HTMLDivElement | null>(null);
  const chartInstanceRef = useRef<any>(null);

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
        ? { Icon: ArrowUp, color: 'text-emerald-600' }
        : norm === 'DOWN'
        ? { Icon: ArrowDown, color: 'text-rose-600' }
        : { Icon: Minus, color: 'text-slate-600' };
    const Icon = meta.Icon;
    return (
      <div className="flex items-center justify-center gap-2 flex-1">
        <Icon className={`h-4 w-4 ${meta.color}`} />
        <span className="text-[11px] font-semibold uppercase tracking-wide text-slate-600">{label}</span>
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

  useEffect(() => {
    setShowAspects(false);
  }, [active, symbols]);

  useEffect(() => {
    const fetchChart = async () => {
      if (!symbols[active]) return;
      try {
        const res = await fetch(`/api/chart?symbol=${symbols[active]}&timeframe=15m`);
        if (!res.ok) throw new Error('Failed to load chart');
        const json = await res.json();
        const mapped = (json.candles || []).map((c: any) => ({ time: c.time, value: c.close }));
        setChartData(mapped);
        setChartMarkers(json.markers || []);
        console.log('chart loaded', symbols[active], { candles: mapped.length, markers: (json.markers || []).length });
      } catch {
        console.warn('chart data load failed');
        setChartData([]);
        setChartMarkers([]);
      }
    };
    fetchChart();
  }, [active, symbols]);

  useEffect(() => {
    if (!chartContainerRef.current || !chartData.length) return;
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

      chart = createChart(chartContainerRef.current, {
        width: chartContainerRef.current.clientWidth,
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
        },
        timeScale: {
          borderVisible: false,
          fixLeftEdge: true,
          fixRightEdge: true,
          timeVisible: true,
          secondsVisible: false,
        },
      });

      chartInstanceRef.current = chart;

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

  const formatDecisionTime = (ts?: number | null) => {
    if (!ts) return '';
    const d = new Date(ts);
    const now = new Date();
    const sameDay =
      d.getFullYear() === now.getFullYear() &&
      d.getMonth() === now.getMonth() &&
      d.getDate() === now.getDate();
    const time = d.toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' });
    if (sameDay) return `– ${time}`;
    const date = d.toLocaleDateString('de-DE');
    return `– ${date} ${time}`;
  };

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
            <div className="grid grid-cols-1 gap-5 lg:grid-cols-2 lg:items-stretch">
              <div className="space-y-4 lg:col-span-1">
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4 h-full">
                  <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
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
                        ) : (
                          'no recent positions'
                        )}
                      </p>
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
                          <span className="flex items-center gap-1">
                            direction –
                            {current.openDirection ? (
                              <span className={current.openDirection === 'long' ? 'text-emerald-600' : 'text-rose-600'}>
                                {current.openDirection}
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

              {current.lastDecision && (
                <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm h-full">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2 text-xs uppercase tracking-wide text-slate-500">
                      <span>Latest Decision</span>
                      {current.lastDecisionTs ? (
                        <span className="lowercase text-slate-400">
                          {formatDecisionTime(current.lastDecisionTs)}
                        </span>
                      ) : null}
                    </div>
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
                    <div className="mt-3 grid grid-cols-1 gap-2 sm:grid-cols-3">
                      {renderBias('Macro', current.lastDecision.macro_bias)}
                      {renderBias('Primary', current.lastDecision.primary_bias || current.lastDecision.bias)}
                      {renderBias('Micro', current.lastDecision.micro_bias)}
                    </div>
                  )}
                </div>
              )}

              {chartData.length > 0 && (
                <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
                  <div className="flex items-center justify-between">
                    <div className="text-xs uppercase tracking-wide text-slate-500">24h Price</div>
                    <div className="text-xs text-slate-400">15m bars</div>
                  </div>
                  <div
                    ref={chartContainerRef}
                    className="mt-3 h-[260px] w-full"
                    style={{ minHeight: 260 }}
                  />
                </div>
              )}

              <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
                <div className="flex items-center justify-between">
                  <div className="text-xs uppercase tracking-wide text-slate-500">Latest Evaluation</div>
                  {current.evaluation.confidence && (
                    <div className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
                      Confidence: {current.evaluation.confidence}
                    </div>
                  )}
                </div>
                <div className="mt-2 text-lg font-semibold text-slate-900 flex items-center gap-3 flex-wrap">
                  <span>
                    Rating: <span className="text-sky-600">{current.evaluation.overall_rating ?? '—'}</span>
                  </span>
                  <div className="flex flex-wrap items-center gap-1">
                    {Array.from({ length: 10 }).map((_, idx) => {
                      const ratingVal = Number(current.evaluation.overall_rating ?? 0);
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
                  {current.evaluation.overview || 'No overview provided.'}
                </p>
                {current.evaluation.aspects && (
                  <div className="mt-4">
                    <button
                      onClick={() => setShowAspects((prev) => !prev)}
                      className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 transition hover:border-slate-300"
                    >
                      {showAspects ? 'Hide aspect ratings' : 'Show aspect ratings'}
                    </button>
                    {showAspects && (
                      <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2">
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
                              <div className="text-lg font-semibold text-sky-700">{val?.rating ?? '—'}</div>
                            </div>
                            <p className="mt-2 text-xs text-slate-600">{val?.comment || 'No comment'}</p>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>

              {(current.evaluation.what_went_well?.length ||
                current.evaluation.issues?.length ||
                current.evaluation.improvements?.length) && (
                <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
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
