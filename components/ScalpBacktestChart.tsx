import React, { useEffect, useMemo, useRef, useState } from 'react';

export type ScalpBacktestChartCandle = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
};

export type ScalpBacktestChartMarker = {
  time: number;
  position: 'aboveBar' | 'belowBar' | 'inBar';
  color: string;
  shape: 'arrowUp' | 'arrowDown' | 'circle' | 'square';
  text?: string;
};

export type ScalpBacktestTradeSegment = {
  id: string;
  side: 'BUY' | 'SELL';
  entryTime: number;
  exitTime: number;
  entryPrice: number;
  exitPrice: number;
  stopPrice: number;
  takeProfitPrice: number;
  rMultiple: number;
  pnlUsd: number;
  exitReason: string;
  holdMinutes: number;
  color?: string;
  lineStyle?: number;
  runLabel?: string;
};

type ScalpBacktestChartProps = {
  candles: ScalpBacktestChartCandle[];
  markers: ScalpBacktestChartMarker[];
  trades: ScalpBacktestTradeSegment[];
  className?: string;
};

export default function ScalpBacktestChart(props: ScalpBacktestChartProps) {
  const { candles, markers, trades, className } = props;
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<any>(null);
  const chartLibRef = useRef<any>(null);
  const candleSeriesRef = useRef<any>(null);
  const markerSeriesRef = useRef<any>(null);
  const tradeSeriesRef = useRef<any[]>([]);
  const resizeObserverRef = useRef<ResizeObserver | null>(null);
  const [chartError, setChartError] = useState<string | null>(null);
  const [chartReady, setChartReady] = useState(false);

  const pricePrecision = useMemo(() => {
    const sample = candles[0]?.close;
    if (!Number.isFinite(sample)) return 5;
    const asString = String(sample);
    const idx = asString.indexOf('.');
    if (idx < 0) return 2;
    return Math.max(2, Math.min(6, asString.length - idx - 1));
  }, [candles]);

  useEffect(() => {
    let cancelled = false;
    setChartError(null);
    setChartReady(false);

    (async () => {
      const root = containerRef.current;
      if (!root) return;
      try {
        const chartLib = await import('lightweight-charts');
        if (cancelled || !containerRef.current) return;
        chartLibRef.current = chartLib;

        const chart: any = chartLib.createChart(root, {
          width: Math.max(400, root.clientWidth),
          height: 480,
          layout: {
            background: { type: chartLib.ColorType.Solid, color: '#0b1220' },
            textColor: '#cbd5e1',
            fontFamily: 'ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif',
            fontSize: 12,
          },
          grid: {
            vertLines: { color: 'rgba(148, 163, 184, 0.18)' },
            horzLines: { color: 'rgba(148, 163, 184, 0.18)' },
          },
          rightPriceScale: {
            borderColor: 'rgba(148, 163, 184, 0.35)',
            autoScale: true,
          },
          timeScale: {
            borderColor: 'rgba(148, 163, 184, 0.35)',
            timeVisible: true,
            secondsVisible: false,
          },
          crosshair: {
            mode: chartLib.CrosshairMode.Normal,
            vertLine: { color: 'rgba(56, 189, 248, 0.65)' },
            horzLine: { color: 'rgba(56, 189, 248, 0.65)' },
          },
          handleScale: true,
          handleScroll: true,
        });
        const CandlestickSeriesCtor = (chartLib as any).CandlestickSeries || (chartLib as any)?.default?.CandlestickSeries;
        const candleSeries =
          typeof chart.addSeries === 'function' && CandlestickSeriesCtor
            ? chart.addSeries(CandlestickSeriesCtor, {
                upColor: '#16a34a',
                downColor: '#dc2626',
                wickUpColor: '#4ade80',
                wickDownColor: '#f87171',
                borderVisible: false,
                priceFormat: {
                  type: 'price',
                  precision: pricePrecision,
                  minMove: Math.pow(10, -1 * pricePrecision),
                },
              })
            : typeof chart.addCandlestickSeries === 'function'
            ? chart.addCandlestickSeries({
                upColor: '#16a34a',
                downColor: '#dc2626',
                wickUpColor: '#4ade80',
                wickDownColor: '#f87171',
                borderVisible: false,
                priceFormat: {
                  type: 'price',
                  precision: pricePrecision,
                  minMove: Math.pow(10, -1 * pricePrecision),
                },
              })
            : null;
        if (!candleSeries) {
          setChartError('Chart init failed: candlestick series API unavailable.');
          setChartReady(false);
          return;
        }

        chartRef.current = chart;
        candleSeriesRef.current = candleSeries;
        markerSeriesRef.current =
          typeof chartLib.createSeriesMarkers === 'function'
            ? chartLib.createSeriesMarkers(candleSeries, [])
            : null;

        resizeObserverRef.current = new ResizeObserver((entries) => {
          const entry = entries[0];
          if (!entry || !chartRef.current) return;
          const width = Math.floor(entry.contentRect.width);
          if (width <= 0) return;
          chartRef.current.applyOptions({ width });
        });
        resizeObserverRef.current.observe(root);
        setChartReady(true);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err || 'unknown chart init error');
        setChartError(`Chart init error: ${msg}`);
        setChartReady(false);
        console.error('scalp chart init failed', err);
      }
    })();

    return () => {
      cancelled = true;
      if (resizeObserverRef.current) {
        resizeObserverRef.current.disconnect();
        resizeObserverRef.current = null;
      }
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }
      chartLibRef.current = null;
      candleSeriesRef.current = null;
      if (markerSeriesRef.current && typeof markerSeriesRef.current.detach === 'function') {
        try {
          markerSeriesRef.current.detach();
        } catch {
          // noop
        }
      }
      markerSeriesRef.current = null;
      tradeSeriesRef.current = [];
      setChartReady(false);
    };
  }, [pricePrecision]);

  useEffect(() => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    if (!chartReady || !chart || !candleSeries) return;

    const setMarkers = (rows: ScalpBacktestChartMarker[]) => {
      const markerSeries = markerSeriesRef.current;
      if (markerSeries && typeof markerSeries.setMarkers === 'function') {
        markerSeries.setMarkers(rows as any);
        return;
      }
      if (typeof candleSeries.setMarkers === 'function') {
        candleSeries.setMarkers(rows as any);
      }
    };

    tradeSeriesRef.current.forEach((series) => {
      try {
        chart.removeSeries(series);
      } catch {
        // noop
      }
    });
    tradeSeriesRef.current = [];

    if (!candles.length) {
      candleSeries.setData([]);
      setMarkers([]);
      return;
    }

    try {
      candleSeries.setData(candles as any);
      setMarkers(markers);
      setChartError(null);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err || 'unknown setData error');
      setChartError(`Chart data error: ${msg}`);
      console.error('scalp chart setData/setMarkers failed', err);
      return;
    }

    trades.forEach((trade) => {
      const chartLib = chartLibRef.current;
      const LineSeriesCtor = chartLib?.LineSeries || chartLib?.default?.LineSeries;
      const tradeSeries =
        typeof chart.addLineSeries === 'function'
          ? chart.addLineSeries({
              color: trade.color || (trade.rMultiple >= 0 ? '#14b8a6' : '#f97316'),
              lineWidth: 2,
              lineStyle: typeof trade.lineStyle === 'number' ? trade.lineStyle : 0,
              lastValueVisible: false,
              priceLineVisible: false,
            })
          : typeof chart.addSeries === 'function' && LineSeriesCtor
          ? chart.addSeries(LineSeriesCtor, {
              color: trade.color || (trade.rMultiple >= 0 ? '#14b8a6' : '#f97316'),
              lineWidth: 2,
              lineStyle: typeof trade.lineStyle === 'number' ? trade.lineStyle : 0,
              lastValueVisible: false,
              priceLineVisible: false,
            })
          : null;
      if (!tradeSeries) return;
      tradeSeries.setData([
        { time: trade.entryTime, value: trade.entryPrice },
        { time: trade.exitTime, value: trade.exitPrice },
      ]);
      tradeSeriesRef.current.push(tradeSeries);
    });

    chart.timeScale().fitContent();
  }, [candles, markers, trades, chartReady]);

  return (
    <div className={className || ''}>
      <div className="rounded-2xl border border-slate-700 bg-[#0b1220] p-3 shadow-2xl shadow-slate-900/40">
        <div className="mb-2 flex items-center justify-between text-xs text-slate-300">
          <span className="rounded-full border border-slate-600 bg-slate-800 px-2 py-1">Scalp Replay Chart</span>
          <span className="text-slate-400">Candles: {candles.length} | Trades: {trades.length}</span>
        </div>
        {chartError ? <p className="mb-2 text-xs text-rose-300">{chartError}</p> : null}
        <div ref={containerRef} className="h-[480px] w-full overflow-hidden rounded-xl border border-slate-800 bg-[#0a1020]" />
      </div>
    </div>
  );
}
