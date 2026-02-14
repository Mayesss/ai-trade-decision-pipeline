import React, { useEffect, useRef, useState } from 'react';
import { ArrowDownRight, ArrowUpRight } from 'lucide-react';

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

type ChartApiResponse = {
  candles?: Array<{ time: number; close: number }>;
  markers?: any[];
  positions?: PositionOverlay[];
};

type CachedChartEntry = {
  payload: ChartApiResponse;
  fetchedAt: number;
};

type ChartPanelProps = {
  symbol: string | null;
  adminSecret: string | null;
  adminGranted: boolean;
  isDark?: boolean;
  timeframe?: string;
  limit?: number;
  livePrice?: number | null;
  liveTimestamp?: number | null;
  liveConnected?: boolean;
};

const BERLIN_TZ = 'Europe/Berlin';
const CHART_CACHE_TTL_MS = 60_000;
const CHART_FETCH_DEFER_MS = 120;
const DEFAULT_WINDOW_DAYS = 7;

const timeframeToSeconds = (tf: string): number => {
  const match = /^(\d+)([smhd])$/i.exec(String(tf || '').trim());
  if (!match) return 60;
  const value = Number(match[1]);
  if (!Number.isFinite(value) || value <= 0) return 60;
  const unit = match[2].toLowerCase();
  if (unit === 's') return value;
  if (unit === 'm') return value * 60;
  if (unit === 'h') return value * 60 * 60;
  if (unit === 'd') return value * 24 * 60 * 60;
  return value * 60;
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

export default function ChartPanel(props: ChartPanelProps) {
  const {
    symbol,
    adminSecret,
    adminGranted,
    isDark = false,
    timeframe = '1H',
    limit,
    livePrice = null,
    liveTimestamp = null,
    liveConnected = false,
  } = props;

  const [chartData, setChartData] = useState<{ time: number; value: number }[]>([]);
  const [chartMarkers, setChartMarkers] = useState<any[]>([]);
  const [positionOverlays, setPositionOverlays] = useState<PositionOverlay[]>([]);
  const [chartLoading, setChartLoading] = useState(false);
  const [chartAttempted, setChartAttempted] = useState(false);
  const [renderedOverlays, setRenderedOverlays] = useState<RenderedOverlay[]>([]);
  const [hoveredOverlay, setHoveredOverlay] = useState<RenderedOverlay | null>(null);
  const [hoverX, setHoverX] = useState<number | null>(null);
  const [livePulseY, setLivePulseY] = useState<number | null>(null);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [chartInitToken, setChartInitToken] = useState(0);

  const panelRef = useRef<HTMLDivElement | null>(null);
  const chartContainerRef = useRef<HTMLDivElement | null>(null);
  const overlayLayerRef = useRef<HTMLDivElement | null>(null);
  const chartInstanceRef = useRef<any>(null);
  const chartSeriesRef = useRef<any>(null);
  const chartCacheRef = useRef<Map<string, CachedChartEntry>>(new Map());
  const timeframeSeconds = timeframeToSeconds(timeframe);
  const resolvedLimit = Math.max(
    32,
    Math.floor(limit ?? (DEFAULT_WINDOW_DAYS * 24 * 60 * 60) / Math.max(1, timeframeSeconds)),
  );
  const windowDays = (resolvedLimit * Math.max(1, timeframeSeconds)) / (24 * 60 * 60);
  const chartTextColor = isDark ? '#cbd5e1' : '#0f172a';
  const chartGridColor = isDark ? '#334155' : '#e2e8f0';
  const chartLineColor = isDark ? '#38bdf8' : '#0ea5e9';
  const chartAreaTopColor = isDark ? 'rgba(56,189,248,0.26)' : 'rgba(14,165,233,0.3)';
  const chartAreaBottomColor = isDark ? 'rgba(14,165,233,0.08)' : 'rgba(14,165,233,0.05)';

  const applyPayload = (payload: ChartApiResponse) => {
    const mapped = (payload.candles || []).map((c: any) => ({ time: Number(c.time), value: Number(c.close) }));
    setChartData(mapped.filter((c) => Number.isFinite(c.time) && Number.isFinite(c.value)));
    setChartMarkers(Array.isArray(payload.markers) ? payload.markers : []);
    setPositionOverlays(Array.isArray(payload.positions) ? payload.positions : []);
  };

  useEffect(() => {
    if (!adminGranted || !symbol) {
      setIsFullscreen(false);
      setChartLoading(false);
      setChartAttempted(false);
      setChartData([]);
      setChartMarkers([]);
      setPositionOverlays([]);
      setRenderedOverlays([]);
      setHoveredOverlay(null);
      setHoverX(null);
      return;
    }

    let cancelled = false;
    const controller = new AbortController();
    let timeoutId: number | null = null;
    const cacheKey = `${symbol}|${timeframe}|${resolvedLimit}`;
    const cached = chartCacheRef.current.get(cacheKey);
    const hasFresh = !!cached && Date.now() - cached.fetchedAt <= CHART_CACHE_TTL_MS;

    setHoveredOverlay(null);
    setHoverX(null);

    if (cached) {
      applyPayload(cached.payload);
      setChartAttempted(true);
      setChartLoading(!hasFresh);
    } else {
      setChartData([]);
      setChartMarkers([]);
      setPositionOverlays([]);
      setChartLoading(true);
      setChartAttempted(false);
    }

    const fetchChart = async (limit: number): Promise<ChartApiResponse> => {
      const res = await fetch(
        `/api/chart?symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(timeframe)}&limit=${limit}`,
        {
          headers: adminSecret ? { 'x-admin-access-secret': adminSecret } : undefined,
          signal: controller.signal,
        },
      );
      if (!res.ok) throw new Error(`Failed to load chart (${res.status})`);
      return res.json();
    };

    const run = async () => {
      try {
        if (hasFresh) {
          setChartLoading(false);
          return;
        }

        const payload = await fetchChart(resolvedLimit);
        if (cancelled) return;
        chartCacheRef.current.set(cacheKey, {
          payload,
          fetchedAt: Date.now(),
        });
        applyPayload(payload);
      } catch (err: any) {
        if (cancelled || err?.name === 'AbortError') return;
        if (!cached) {
          setChartData([]);
          setChartMarkers([]);
          setPositionOverlays([]);
        }
      } finally {
        if (!cancelled) setChartLoading(false);
      }
    };

    timeoutId = window.setTimeout(() => {
      if (cancelled) return;
      setChartAttempted(true);
      void run();
    }, CHART_FETCH_DEFER_MS);

    return () => {
      cancelled = true;
      controller.abort();
      if (timeoutId) window.clearTimeout(timeoutId);
    };
  }, [symbol, timeframe, adminSecret, adminGranted, resolvedLimit]);

  useEffect(() => {
    if (!isFullscreen) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setIsFullscreen(false);
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [isFullscreen]);

  useEffect(() => {
    if (!adminGranted || !symbol) return;
    const price = Number(livePrice);
    if (!Number.isFinite(price) || price <= 0) return;
    const interval = Math.max(1, timeframeSeconds);
    const tsMs = Number.isFinite(liveTimestamp as number) ? Number(liveTimestamp) : Date.now();
    const barTime = Math.floor(Math.floor(tsMs / 1000) / interval) * interval;

    setChartData((prev) => {
      if (!prev.length || barTime < prev[0].time) return prev;
      const last = prev[prev.length - 1];
      if (!last) return prev;
      if (barTime === last.time) {
        if (Math.abs(last.value - price) < 1e-9) return prev;
        const next = prev.slice();
        next[next.length - 1] = { time: last.time, value: price };
        return next;
      }
      if (barTime > last.time) {
        const next = [...prev, { time: barTime, value: price }];
        if (next.length > resolvedLimit) {
          next.splice(0, next.length - resolvedLimit);
        }
        return next;
      }
      return prev;
    });

    setPositionOverlays((prev) =>
      prev.map((pos) => {
        if (
          pos.status !== 'open' ||
          typeof pos.entryPrice !== 'number' ||
          !Number.isFinite(pos.entryPrice) ||
          pos.entryPrice <= 0 ||
          (pos.side !== 'long' && pos.side !== 'short')
        ) {
          return pos;
        }
        const lev = typeof pos.leverage === 'number' && pos.leverage > 0 ? pos.leverage : 1;
        const sideSign = pos.side === 'long' ? 1 : -1;
        const nextPnlPct = ((price - pos.entryPrice) / pos.entryPrice) * sideSign * lev * 100;
        if (!Number.isFinite(nextPnlPct)) return pos;
        if (typeof pos.pnlPct === 'number' && Math.abs(pos.pnlPct - nextPnlPct) < 0.01) return pos;
        return { ...pos, pnlPct: nextPnlPct };
      }),
    );
  }, [livePrice, liveTimestamp, timeframeSeconds, resolvedLimit, adminGranted, symbol]);

  useEffect(() => {
    const container = chartContainerRef.current;
    if (!container || !chartData.length) return;
    let chart: any;
    let disposed = false;

    (async () => {
      const lw = await import('lightweight-charts');
      const createChart =
        typeof lw.createChart === 'function'
          ? lw.createChart
          : typeof (lw as any)?.default?.createChart === 'function'
          ? (lw as any).default.createChart
          : null;
      if (!createChart || disposed) return;

      const priceScaleWidth = 56;
      const initialHeight = Math.max(260, Math.floor(container.clientHeight || 260));
      chart = createChart(container, {
        width: container.clientWidth,
        height: initialHeight,
        handleScroll: false,
        handleScale: false,
        layout: {
          background: { type: 'solid', color: 'transparent' },
          textColor: chartTextColor,
        },
        grid: {
          vertLines: { color: chartGridColor },
          horzLines: { color: chartGridColor },
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
              lineColor: chartLineColor,
              topColor: chartAreaTopColor,
              bottomColor: chartAreaBottomColor,
            })
          : typeof chart.addSeries === 'function' && AreaSeriesCtor
          ? chart.addSeries(AreaSeriesCtor, {
              lineColor: chartLineColor,
              topColor: chartAreaTopColor,
              bottomColor: chartAreaBottomColor,
            })
          : typeof chart.addLineSeries === 'function'
          ? chart.addLineSeries({ color: chartLineColor, lineWidth: 2 })
          : typeof chart.addSeries === 'function' && LineSeriesCtor
          ? chart.addSeries(LineSeriesCtor, { color: chartLineColor, lineWidth: 2 })
          : null;

      if (!series) return;

      chartSeriesRef.current = series;
      series.setData(chartData);
      if (Array.isArray(chartMarkers) && chartMarkers.length && typeof series.setMarkers === 'function') {
        series.setMarkers(chartMarkers);
      }
      chart.timeScale().fitContent();
      setChartInitToken((t) => t + 1);
    })();

    const handleResize = () => {
      if (chart && chartContainerRef.current) {
        chart.applyOptions({
          width: chartContainerRef.current.clientWidth,
          height: Math.max(260, Math.floor(chartContainerRef.current.clientHeight || 260)),
        });
      }
    };

    window.addEventListener('resize', handleResize);
    return () => {
      disposed = true;
      window.removeEventListener('resize', handleResize);
      if (chart) {
        chart.remove();
        chartInstanceRef.current = null;
      }
      chartSeriesRef.current = null;
    };
  }, [symbol, timeframe, chartData.length, isFullscreen, chartAreaBottomColor, chartAreaTopColor, chartGridColor, chartLineColor, chartTextColor]);

  useEffect(() => {
    const series = chartSeriesRef.current;
    if (!series || !chartData.length) return;
    series.setData(chartData);
    if (Array.isArray(chartMarkers) && typeof series.setMarkers === 'function') {
      series.setMarkers(chartMarkers);
    }
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

  useEffect(() => {
    const recalcLivePulse = () => {
      if (!liveConnected) {
        setLivePulseY(null);
        return;
      }
      const series = chartSeriesRef.current;
      const layer = overlayLayerRef.current;
      if (!series || !layer || !chartData.length || typeof series.priceToCoordinate !== 'function') {
        setLivePulseY(null);
        return;
      }
      const fallback = chartData[chartData.length - 1]?.value;
      const price = typeof livePrice === 'number' ? livePrice : fallback;
      if (!Number.isFinite(price)) {
        setLivePulseY(null);
        return;
      }
      const y = Number(series.priceToCoordinate(price));
      if (!Number.isFinite(y)) {
        setLivePulseY(null);
        return;
      }
      const clampedY = Math.min(Math.max(y, 6), Math.max(6, layer.clientHeight - 6));
      setLivePulseY(clampedY);
    };

    recalcLivePulse();
    window.addEventListener('resize', recalcLivePulse);
    return () => window.removeEventListener('resize', recalcLivePulse);
  }, [chartData, livePrice, liveConnected, chartInitToken]);

  if (!adminGranted || !symbol) return null;

  const hasChartData = chartData.length > 0;
  const showSkeleton = (chartLoading && !hasChartData) || !chartAttempted;
  const showEmpty = !chartLoading && chartAttempted && !hasChartData;

  return (
    <div
      ref={panelRef}
      className={`bg-white p-4 ${
        isFullscreen
          ? 'fixed inset-0 z-[90] rounded-none border-0 shadow-none'
          : 'rounded-2xl border border-slate-200 shadow-sm lg:col-span-2'
      }`}
    >
      <div className="flex items-center justify-between">
        <div className="text-xs uppercase tracking-wide text-slate-500">7D Price</div>
        <div className="text-xs text-slate-400">
          {timeframe} bars · {windowDays.toFixed(0)}D window
          {liveConnected ? ' · live' : ''}
          {typeof livePrice === 'number' ? ` · ${livePrice.toFixed(2)}` : ''}
          {chartLoading && hasChartData ? ' · updating…' : ''}
        </div>
      </div>
      <div
        className={`relative mt-3 w-full ${isFullscreen ? 'h-[calc(100vh-96px)]' : 'h-[260px]'}`}
        style={{ minHeight: 260 }}
      >
        <button
          type="button"
          onClick={() => setIsFullscreen((prev) => !prev)}
          className="absolute bottom-0 right-3 z-30 inline-flex h-8 w-8 items-center justify-center rounded-md border border-slate-200 bg-white/95 text-slate-600 shadow-sm backdrop-blur transition hover:border-sky-300 hover:text-sky-700"
          aria-label={isFullscreen ? 'Exit fullscreen chart' : 'Enter fullscreen chart'}
          title={isFullscreen ? 'Exit fullscreen' : 'Fullscreen'}
        >
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.9"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="h-4 w-4"
            aria-hidden="true"
          >
            <path d="M4 9V4h5" />
            <path d="M15 4h5v5" />
            <path d="M20 15v5h-5" />
            <path d="M9 20H4v-5" />
          </svg>
        </button>
        {showSkeleton ? (
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
        ) : hasChartData ? (
          <div ref={chartContainerRef} className="h-full w-full" style={{ minHeight: 260 }} />
        ) : showEmpty ? (
          <div className="flex h-full w-full items-center justify-center rounded-xl border border-slate-200 bg-slate-50 text-sm font-semibold text-slate-500">
            Chart unavailable right now. Try refresh.
          </div>
        ) : null}
        {hasChartData && (
          <>
            <div ref={overlayLayerRef} className="pointer-events-none absolute inset-0" style={{ right: 56 }}>
              {liveConnected && livePulseY !== null ? (
                <div className="pointer-events-none absolute" style={{ top: livePulseY - 14, right: 1 }}>
                  <span className="relative inline-flex h-2 w-2">
                    <span className="absolute inset-0 animate-ping rounded-full bg-sky-400/80" />
                    <span className="relative inline-flex h-2 w-2 rounded-full bg-sky-500 ring-1 ring-white/90 shadow-sm" />
                  </span>
                </div>
              ) : null}
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
                      <div className="absolute top-0 bottom-0 w-[2px]" style={{ left: 0, backgroundColor: stroke }} />
                    )}
                    {pos.status === 'closed' ? (
                      <div className="absolute top-0 bottom-0 w-[2px]" style={{ right: 0, backgroundColor: stroke }} />
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
                  left: Math.min(Math.max(hoverX - 130, 8), (overlayLayerRef.current?.clientWidth || 280) - 220),
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
                      {typeof hoveredOverlay.pnlPct === 'number' ? `${hoveredOverlay.pnlPct.toFixed(2)}%` : '—'}
                    </span>
                  </div>
                  <div className="mt-1 text-[10px] uppercase tracking-wide text-slate-500">
                    {hoveredOverlay.side || 'position'} · entry {formatOverlayTime(hoveredOverlay.entryTime)}
                    {hoveredOverlay.exitTime ? ` · exit ${formatOverlayTime(hoveredOverlay.exitTime)}` : ''}
                  </div>
                  <div className="mt-2 flex flex-wrap items-center gap-3 text-[11px] text-slate-600">
                    {typeof hoveredOverlay.leverage === 'number' ? (
                      <span className="font-semibold text-slate-800">{hoveredOverlay.leverage.toFixed(0)}x</span>
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
                        {hoveredOverlay.entryDecision.summary ? ` · ${hoveredOverlay.entryDecision.summary}` : ''}
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
                        {hoveredOverlay.exitDecision.summary ? ` · ${hoveredOverlay.exitDecision.summary}` : ''}
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
  );
}
