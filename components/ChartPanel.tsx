import React, { useEffect, useRef, useState } from 'react';

type DecisionBrief = {
  timestamp?: number | null;
  action?: string;
  summary?: string;
  reason?: string;
};

type PartialCloseBrief = DecisionBrief & {
  closePct?: number | null;
  size?: number | null;
};

type PositionOverlay = {
  id: string;
  status: 'open' | 'closed';
  side?: 'long' | 'short' | null;
  entryTime: number | null;
  exitTime?: number | null;
  pnlPct?: number | null;
  pnlNet?: number | null;
  leverage?: number | null;
  entryPrice?: number | null;
  exitPrice?: number | null;
  entryDecision?: DecisionBrief | null;
  exitDecision?: DecisionBrief | null;
  partialCloses?: PartialCloseBrief[];
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

export type ChartRangeKey = '1D' | '7D' | '30D' | '6M';

type ChartPanelProps = {
  symbol: string | null;
  platform?: string | null;
  adminSecret: string | null;
  adminGranted: boolean;
  isDark?: boolean;
  rangeKey: ChartRangeKey;
  onRangeChange: (nextRange: ChartRangeKey) => void;
  // Optional compact PnL stats rendered in the header: beside the range
  // switches on desktop, and in place of the "bars · window" caption on mobile.
  statsSlot?: React.ReactNode;
  livePrice?: number | null;
  liveTimestamp?: number | null;
  liveConnected?: boolean;
  onOpenPositionChange?: (position: {
    pnlPct: number | null;
    side: 'long' | 'short' | null;
    leverage: number | null;
    entryPrice: number | null;
  } | null) => void;
  onPositionSummaryChange?: (summary: {
    closedPnlPct: number | null;
    closedPnlNet: number | null;
    closedCount: number;
    lastPnlPct: number | null;
    lastSide: 'long' | 'short' | null;
    lastLeverage: number | null;
    openPnlPct: number | null;
    openSide: 'long' | 'short' | null;
    openLeverage: number | null;
    openEntryPrice: number | null;
  }) => void;
};

const BERLIN_TZ = 'Europe/Berlin';
const CHART_CACHE_TTL_MS = 60_000;
const CHART_FETCH_DEFER_MS = 120;
const CHART_RANGE_ORDER: ChartRangeKey[] = ['1D', '7D', '30D', '6M'];
const CHART_RANGE_PRESETS: Record<ChartRangeKey, { timeframe: string; limit: number }> = {
  '1D': { timeframe: '15m', limit: 96 },
  '7D': { timeframe: '1H', limit: 168 },
  '30D': { timeframe: '4H', limit: 180 },
  '6M': { timeframe: '1D', limit: 183 },
};

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

const toUnixSeconds = (time: any): number => {
  const seconds =
    typeof time === 'number'
      ? time
      : typeof time === 'object' && time !== null && 'timestamp' in time
      ? Number((time as any).timestamp)
      : Number(time);
  return Number.isFinite(seconds) ? seconds : NaN;
};

const formatBerlinTime = (time: any, opts: Intl.DateTimeFormatOptions = {}) => {
  const seconds = toUnixSeconds(time);
  if (!Number.isFinite(seconds)) return '';
  const date = new Date(seconds * 1000);
  return new Intl.DateTimeFormat('de-DE', {
    timeZone: BERLIN_TZ,
    hour: '2-digit',
    minute: '2-digit',
    ...opts,
  }).format(date);
};

const formatAxisTick = (time: any, rangeKey: ChartRangeKey) => {
  const seconds = toUnixSeconds(time);
  if (!Number.isFinite(seconds)) return '';
  const date = new Date(seconds * 1000);
  if (rangeKey === '6M') {
    return new Intl.DateTimeFormat('de-DE', {
      timeZone: BERLIN_TZ,
      month: 'short',
      year: '2-digit',
    }).format(date);
  }
  if (rangeKey === '30D') {
    return new Intl.DateTimeFormat('de-DE', {
      timeZone: BERLIN_TZ,
      day: '2-digit',
      month: '2-digit',
    }).format(date);
  }
  if (rangeKey === '1D') {
    return new Intl.DateTimeFormat('de-DE', {
      timeZone: BERLIN_TZ,
      hour: '2-digit',
      minute: '2-digit',
    }).format(date);
  }
  return new Intl.DateTimeFormat('de-DE', {
    timeZone: BERLIN_TZ,
    day: '2-digit',
    month: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date);
};

const formatCrosshairTime = (time: any, rangeKey: ChartRangeKey) => {
  const seconds = toUnixSeconds(time);
  if (!Number.isFinite(seconds)) return '';
  const date = new Date(seconds * 1000);
  if (rangeKey === '6M') {
    return new Intl.DateTimeFormat('de-DE', {
      timeZone: BERLIN_TZ,
      day: '2-digit',
      month: '2-digit',
      year: '2-digit',
    }).format(date);
  }
  return new Intl.DateTimeFormat('de-DE', {
    timeZone: BERLIN_TZ,
    day: '2-digit',
    month: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date);
};

const formatOverlayTime = (tsSeconds?: number | null) => {
  if (!tsSeconds) return '—';
  const d = new Date(tsSeconds * 1000);
  return d.toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit', timeZone: BERLIN_TZ });
};

const formatOverlayPnl = (pos: PositionOverlay) => {
  if (typeof pos.pnlPct === 'number') return `${pos.pnlPct.toFixed(1)}%`;
  if (typeof pos.pnlNet === 'number') return `${pos.pnlNet >= 0 ? '+' : ''}${pos.pnlNet.toFixed(2)}`;
  return null;
};

const getOverlayPnlValue = (pos: PositionOverlay) => {
  if (typeof pos.pnlPct === 'number') return pos.pnlPct;
  if (typeof pos.pnlNet === 'number') return pos.pnlNet;
  return null;
};

const actionPillToneClass = (action?: string | null, pnlValue?: number | null) => {
  const normalized = String(action || '').trim().toUpperCase();
  if (normalized === 'BUY') return 'border-emerald-200 bg-emerald-100 text-emerald-800';
  if (normalized === 'SELL') return 'border-rose-200 bg-rose-100 text-rose-800';
  if (normalized === 'CLOSE') {
    if (typeof pnlValue === 'number') {
      return pnlValue >= 0
        ? 'border-emerald-200 bg-emerald-100 text-emerald-800'
        : 'border-rose-200 bg-rose-100 text-rose-800';
    }
    return 'neutral-highlight';
  }
  return 'neutral-highlight';
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

// ── Position overlays as a canvas primitive ───────────────────────────────
// Drawing the shaded position rectangles, entry/exit walls and the leverage /
// PnL badge inside the chart canvas (rather than as absolutely-positioned HTML)
// means the chart itself repaints them in lockstep with its own coordinate
// system on every zoom/resize frame — so they can never drift or lag behind the
// candles. The rich hover tooltip stays HTML and is driven by crosshair events.

type OverlayTone = 'up' | 'down' | 'neutral';

type OverlayTheme = {
  fillUp: string;
  fillDown: string;
  fillNeutral: string;
  strokeUp: string;
  strokeDown: string;
  strokeNeutral: string;
  openWall: string;
  badgeBg: string;
  leverageText: string;
  upText: string;
  downText: string;
  neutralText: string;
  partialStroke: string;
  partialText: string;
};

const buildOverlayTheme = (isDark: boolean): OverlayTheme => ({
  fillUp: 'rgba(16,185,129,0.12)',
  fillDown: 'rgba(248,113,113,0.12)',
  fillNeutral: 'rgba(161,161,170,0.08)',
  strokeUp: 'rgba(16,185,129,0.9)',
  strokeDown: 'rgba(239,68,68,0.9)',
  strokeNeutral: 'rgba(113,113,122,0.5)',
  openWall: 'rgba(161,161,170,0.8)',
  badgeBg: isDark ? 'rgba(24,24,27,0.9)' : 'rgba(255,255,255,0.9)',
  leverageText: isDark ? 'rgb(212,212,216)' : 'rgb(51,65,85)',
  upText: isDark ? 'rgb(52,211,153)' : 'rgb(4,120,87)',
  downText: isDark ? 'rgb(251,113,133)' : 'rgb(190,18,60)',
  neutralText: isDark ? 'rgb(212,212,216)' : 'rgb(51,65,85)',
  partialStroke: isDark ? 'rgba(251,191,36,0.95)' : 'rgba(217,119,6,0.95)',
  partialText: isDark ? 'rgb(253,230,138)' : 'rgb(146,64,14)',
});

type OverlayPrimitiveDatum = {
  id: string;
  entryTime: number;
  exitTime: number;
  showEntryWall: boolean;
  closed: boolean;
  side: 'long' | 'short' | null;
  tone: OverlayTone;
  leverageLabel: string | null;
  pnlLabel: string | null;
  partialLabel: string | null;
  partialTimes: number[];
};

const OVERLAY_INSET_Y = 12;
const OVERLAY_RADIUS = 6;

const overlayFill = (tone: OverlayTone, theme: OverlayTheme) =>
  tone === 'up' ? theme.fillUp : tone === 'down' ? theme.fillDown : theme.fillNeutral;
const overlayStroke = (tone: OverlayTone, theme: OverlayTheme) =>
  tone === 'up' ? theme.strokeUp : tone === 'down' ? theme.strokeDown : theme.strokeNeutral;
const overlayText = (tone: OverlayTone, theme: OverlayTheme) =>
  tone === 'up' ? theme.upText : tone === 'down' ? theme.downText : theme.neutralText;

const roundRectPath = (
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  w: number,
  h: number,
  r: number,
) => {
  const radius = Math.max(0, Math.min(r, w / 2, h / 2));
  ctx.beginPath();
  ctx.moveTo(x + radius, y);
  ctx.arcTo(x + w, y, x + w, y + h, radius);
  ctx.arcTo(x + w, y + h, x, y + h, radius);
  ctx.arcTo(x, y + h, x, y, radius);
  ctx.arcTo(x, y, x + w, y, radius);
  ctx.closePath();
};

const drawOverlayBadge = (
  ctx: CanvasRenderingContext2D,
  rightX: number,
  topY: number,
  datum: OverlayPrimitiveDatum,
  theme: OverlayTheme,
) => {
  const hasArrow = datum.side === 'long' || datum.side === 'short';
  const parts: { text: string; color: string }[] = [];
  if (datum.leverageLabel) parts.push({ text: datum.leverageLabel, color: theme.leverageText });
  if (datum.pnlLabel) parts.push({ text: datum.pnlLabel, color: overlayText(datum.tone, theme) });
  if (datum.partialLabel) parts.push({ text: datum.partialLabel, color: theme.partialText });
  if (!hasArrow && !parts.length) return;

  ctx.font = '600 10px system-ui, -apple-system, sans-serif';
  ctx.textBaseline = 'middle';
  const padX = 5;
  const gap = 4;
  const arrowW = hasArrow ? 8 : 0;
  const textWidths = parts.map((p) => ctx.measureText(p.text).width);
  const gaps = Math.max(0, (hasArrow ? 1 : 0) + parts.length - 1) * gap;
  const contentW = arrowW + textWidths.reduce((a, b) => a + b, 0) + gaps;
  const badgeW = contentW + padX * 2;
  const badgeH = 15;
  const bx = rightX - badgeW - 2;
  const by = topY + 2;
  const cy = by + badgeH / 2;

  ctx.fillStyle = theme.badgeBg;
  roundRectPath(ctx, bx, by, badgeW, badgeH, 3);
  ctx.fill();

  let cx = bx + padX;
  if (hasArrow) {
    const s = 4;
    ctx.fillStyle = datum.side === 'long' ? theme.upText : theme.downText;
    ctx.beginPath();
    if (datum.side === 'long') {
      ctx.moveTo(cx, cy + s);
      ctx.lineTo(cx + s, cy - s);
      ctx.lineTo(cx + 2 * s, cy + s);
    } else {
      ctx.moveTo(cx, cy - s);
      ctx.lineTo(cx + s, cy + s);
      ctx.lineTo(cx + 2 * s, cy - s);
    }
    ctx.closePath();
    ctx.fill();
    cx += arrowW + gap;
  }
  parts.forEach((p, i) => {
    ctx.fillStyle = p.color;
    ctx.fillText(p.text, cx, cy);
    cx += textWidths[i] + gap;
  });
};

class PositionOverlayRenderer {
  constructor(
    private readonly items: { left: number; right: number; partials: number[]; datum: OverlayPrimitiveDatum }[],
    private readonly theme: OverlayTheme,
  ) {}
  draw(target: any) {
    target.useMediaCoordinateSpace((scope: any) => {
      const ctx = scope.context as CanvasRenderingContext2D;
      const paneHeight = scope.mediaSize.height as number;
      const top = OVERLAY_INSET_Y;
      const height = Math.max(0, paneHeight - OVERLAY_INSET_Y * 2);
      if (height <= 0) return;
      for (const { left, right, partials, datum } of this.items) {
        const width = Math.max(4, right - left);
        ctx.fillStyle = overlayFill(datum.tone, this.theme);
        roundRectPath(ctx, left, top, width, height, OVERLAY_RADIUS);
        ctx.fill();
        const stroke = overlayStroke(datum.tone, this.theme);
        if (datum.showEntryWall) {
          ctx.fillStyle = stroke;
          ctx.fillRect(left, top, 1, height);
        }
        if (partials.length) {
          ctx.fillStyle = this.theme.partialStroke;
          for (const x of partials) {
            if (x < left || x > right) continue;
            const markerX = Math.round(x);
            ctx.fillRect(markerX - 1, top + 3, 2, Math.max(4, height - 6));
          }
        }
        ctx.fillStyle = datum.closed ? stroke : this.theme.openWall;
        ctx.fillRect(right - 1, top, 1, height);
        drawOverlayBadge(ctx, right, top, datum, this.theme);
      }
    });
  }
}

class PositionOverlayPaneView {
  private items: { left: number; right: number; partials: number[]; datum: OverlayPrimitiveDatum }[] = [];
  constructor(private readonly source: PositionOverlayPrimitive) {}
  update() {
    const chart = this.source.chart;
    const data = this.source.data;
    if (!chart || !data.length) {
      this.items = [];
      return;
    }
    const timeScale = chart.timeScale();
    this.items = data
      .map((datum) => {
        const x1 = timeScale.timeToCoordinate(datum.entryTime);
        const x2 = timeScale.timeToCoordinate(datum.exitTime);
        if (x1 === null || x2 === null || !Number.isFinite(x1) || !Number.isFinite(x2)) {
          return null;
        }
        const left = Math.min(x1, x2);
        const right = Math.max(x1, x2);
        const partials = datum.partialTimes
          .map((time) => timeScale.timeToCoordinate(time))
          .filter((x): x is number => x !== null && Number.isFinite(x) && x >= left && x <= right);
        return { left, right, partials, datum };
      })
      .filter(Boolean) as { left: number; right: number; partials: number[]; datum: OverlayPrimitiveDatum }[];
  }
  renderer() {
    return new PositionOverlayRenderer(this.items, this.source.theme);
  }
}

class PositionOverlayPrimitive {
  chart: any = null;
  data: OverlayPrimitiveDatum[] = [];
  theme: OverlayTheme;
  private requestUpdate: (() => void) | null = null;
  private readonly paneView: PositionOverlayPaneView;
  constructor(theme: OverlayTheme) {
    this.theme = theme;
    this.paneView = new PositionOverlayPaneView(this);
  }
  attached(param: any) {
    this.chart = param.chart;
    this.requestUpdate = param.requestUpdate;
  }
  detached() {
    this.chart = null;
    this.requestUpdate = null;
  }
  updateAllViews() {
    this.paneView.update();
  }
  paneViews() {
    return [this.paneView];
  }
  setData(data: OverlayPrimitiveDatum[]) {
    this.data = data;
    this.requestUpdate?.();
  }
  setTheme(theme: OverlayTheme) {
    this.theme = theme;
    this.requestUpdate?.();
  }
}

// Snap each overlay's entry/exit to the nearest candle and package it (with the
// badge labels) for the primitive. Depends only on the data, never the zoom.
type SnappedOverlay = { pos: PositionOverlay; entryTime: number; exitTime: number };

const buildOverlayPrimitiveData = (
  chartData: { time: number; value: number }[],
  positionOverlays: PositionOverlay[],
): { data: OverlayPrimitiveDatum[]; snapped: SnappedOverlay[] } => {
  if (!chartData.length || !positionOverlays.length) return { data: [], snapped: [] };
  const minTime = chartData[0].time;
  const maxTime = chartData[chartData.length - 1].time;
  const candleTimes = chartData.map((c) => c.time);
  const nearestTime = (target: number) =>
    candleTimes.reduce(
      (prev, curr) => (Math.abs(curr - target) < Math.abs(prev - target) ? curr : prev),
      candleTimes[0],
    );
  const data: OverlayPrimitiveDatum[] = [];
  const snapped: SnappedOverlay[] = [];
  for (const pos of positionOverlays) {
    const boundedEntry = Math.min(Math.max(pos.entryTime ?? minTime, minTime), maxTime);
    const boundedExit = pos.exitTime ? Math.min(Math.max(pos.exitTime, minTime), maxTime) : maxTime;
    const entryTime = nearestTime(boundedEntry);
    const exitTime = pos.exitTime ? nearestTime(boundedExit) : maxTime;
    const pnlValue = getOverlayPnlValue(pos);
    const tone: OverlayTone =
      pnlValue === null ? 'neutral' : pnlValue >= 0 ? 'up' : 'down';
    const partialTimes = (pos.partialCloses || [])
      .map((partial) => {
        const raw = Number(partial.timestamp);
        if (!Number.isFinite(raw) || raw <= 0) return null;
        return nearestTime(Math.floor(raw / 1000));
      })
      .filter((time): time is number => time !== null && time >= entryTime && time <= exitTime);
    const partialLabel =
      partialTimes.length === 1 ? '1 trim' : partialTimes.length > 1 ? `${partialTimes.length} trims` : null;
    data.push({
      id: pos.id,
      entryTime,
      exitTime,
      showEntryWall: pos.entryTime !== null && pos.entryTime >= minTime,
      closed: pos.status === 'closed',
      side: pos.side ?? null,
      tone,
      leverageLabel: typeof pos.leverage === 'number' ? `${pos.leverage.toFixed(0)}x` : null,
      pnlLabel: formatOverlayPnl(pos),
      partialLabel,
      partialTimes,
    });
    snapped.push({ pos, entryTime, exitTime });
  }
  return { data, snapped };
};

export default function ChartPanel(props: ChartPanelProps) {
  const {
    symbol,
    platform = null,
    adminSecret,
    adminGranted,
    isDark = false,
    rangeKey,
    onRangeChange,
    statsSlot = null,
    livePrice = null,
    liveTimestamp = null,
    liveConnected = false,
    onOpenPositionChange,
    onPositionSummaryChange,
  } = props;

  const [chartData, setChartData] = useState<{ time: number; value: number }[]>([]);
  const [positionOverlays, setPositionOverlays] = useState<PositionOverlay[]>([]);
  const [chartLoading, setChartLoading] = useState(false);
  const [chartAttempted, setChartAttempted] = useState(false);
  const [hoveredOverlay, setHoveredOverlay] = useState<PositionOverlay | null>(null);
  const [hoverX, setHoverX] = useState<number | null>(null);
  const [livePulseY, setLivePulseY] = useState<number | null>(null);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [chartInitToken, setChartInitToken] = useState(0);

  const panelRef = useRef<HTMLDivElement | null>(null);
  const chartContainerRef = useRef<HTMLDivElement | null>(null);
  const overlayLayerRef = useRef<HTMLDivElement | null>(null);
  const chartInstanceRef = useRef<any>(null);
  const chartSeriesRef = useRef<any>(null);
  const overlayPrimitiveRef = useRef<PositionOverlayPrimitive | null>(null);
  const snappedOverlaysRef = useRef<SnappedOverlay[]>([]);
  const pinnedOverlayIdRef = useRef<string | null>(null);
  const chartCacheRef = useRef<Map<string, CachedChartEntry>>(new Map());
  const rangePreset = CHART_RANGE_PRESETS[rangeKey];
  const timeframe = rangePreset.timeframe;
  const timeframeSeconds = timeframeToSeconds(timeframe);
  const resolvedLimit = Math.max(32, rangePreset.limit);
  const chartTextColor = isDark ? '#d4d4d8' : '#0f172a';
  const chartGridColor = isDark ? '#3f3f46' : '#e2e8f0';
  const chartLineColor = isDark ? '#d4d4d8' : '#475569';
  const chartAreaTopColor = isDark ? 'rgba(212,212,216,0.3)' : 'rgba(71,85,105,0.3)';
  const chartAreaBottomColor = isDark ? 'rgba(161,161,170,0.09)' : 'rgba(71,85,105,0.07)';
  const hasChartData = chartData.length > 0;
  const closeOverlayTooltip = () => {
    pinnedOverlayIdRef.current = null;
    setHoveredOverlay(null);
    setHoverX(null);
  };

  const applyPayload = (payload: ChartApiResponse) => {
    const mapped = (payload.candles || []).map((c: any) => ({ time: Number(c.time), value: Number(c.close) }));
    setChartData(mapped.filter((c) => Number.isFinite(c.time) && Number.isFinite(c.value)));
    const nextPositions = Array.isArray(payload.positions) ? payload.positions : [];
    setPositionOverlays(nextPositions);
    const openPosition = nextPositions.find((pos) => pos?.status === 'open') ?? null;
    const closedPositions = nextPositions.filter((pos) => pos?.status === 'closed');
    const closedPcts = closedPositions
      .map((pos) => (typeof pos.pnlPct === 'number' ? pos.pnlPct : null))
      .filter((value): value is number => typeof value === 'number');
    const closedNet = closedPositions
      .map((pos) => (typeof pos.pnlNet === 'number' ? pos.pnlNet : null))
      .filter((value): value is number => typeof value === 'number');
    const lastClosed = closedPositions
      .slice()
      .sort((a, b) => Number(a.exitTime ?? a.entryTime ?? 0) - Number(b.exitTime ?? b.entryTime ?? 0))
      .at(-1);
    onOpenPositionChange?.(
      openPosition
        ? {
            pnlPct: typeof openPosition.pnlPct === 'number' ? openPosition.pnlPct : null,
            side: openPosition.side === 'long' || openPosition.side === 'short' ? openPosition.side : null,
            leverage: typeof openPosition.leverage === 'number' ? openPosition.leverage : null,
            entryPrice: typeof openPosition.entryPrice === 'number' ? openPosition.entryPrice : null,
          }
        : null,
    );
    onPositionSummaryChange?.({
      closedPnlPct: closedPcts.length ? closedPcts.reduce((sum, value) => sum + value, 0) : null,
      closedPnlNet: closedNet.length ? closedNet.reduce((sum, value) => sum + value, 0) : null,
      closedCount: closedPositions.length,
      lastPnlPct: typeof lastClosed?.pnlPct === 'number' ? lastClosed.pnlPct : null,
      lastSide: lastClosed?.side === 'long' || lastClosed?.side === 'short' ? lastClosed.side : null,
      lastLeverage: typeof lastClosed?.leverage === 'number' ? lastClosed.leverage : null,
      openPnlPct: typeof openPosition?.pnlPct === 'number' ? openPosition.pnlPct : null,
      openSide: openPosition?.side === 'long' || openPosition?.side === 'short' ? openPosition.side : null,
      openLeverage: typeof openPosition?.leverage === 'number' ? openPosition.leverage : null,
      openEntryPrice: typeof openPosition?.entryPrice === 'number' ? openPosition.entryPrice : null,
    });
  };

  useEffect(() => {
    if (!adminGranted || !symbol) {
      setIsFullscreen(false);
      setChartLoading(false);
      setChartAttempted(false);
      setChartData([]);
      setPositionOverlays([]);
      overlayPrimitiveRef.current?.setData([]);
      snappedOverlaysRef.current = [];
      pinnedOverlayIdRef.current = null;
      setHoveredOverlay(null);
      setHoverX(null);
      return;
    }

    let cancelled = false;
    const controller = new AbortController();
    let timeoutId: number | null = null;
    const cacheKey = `${symbol}|${platform || 'bitget'}|${timeframe}|${resolvedLimit}`;
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
      setPositionOverlays([]);
      setChartLoading(true);
      setChartAttempted(false);
    }

    const fetchChart = async (limit: number): Promise<ChartApiResponse> => {
      const params = new URLSearchParams({
        symbol,
        timeframe,
        limit: String(limit),
      });
      if (platform) params.set('platform', platform);
      const res = await fetch(
        `/api/swing/chart?${params.toString()}`,
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
  }, [symbol, platform, timeframe, adminSecret, adminGranted, resolvedLimit]);

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
    if (!container || !hasChartData) return;
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
          timeFormatter: (time: any) => formatCrosshairTime(time, rangeKey),
        },
      });

      chartInstanceRef.current = chart;
      chart.timeScale().applyOptions({
        tickMarkFormatter: (time: any) => formatAxisTick(time, rangeKey),
      });

      const AreaSeriesCtor = (lw as any).AreaSeries || (lw as any)?.default?.AreaSeries;
      const LineSeriesCtor = (lw as any).LineSeries || (lw as any)?.default?.LineSeries;

      const series =
        typeof chart.addAreaSeries === 'function'
          ? chart.addAreaSeries({
              lineColor: chartLineColor,
              topColor: chartAreaTopColor,
              bottomColor: chartAreaBottomColor,
              lineWidth: 1,
            })
          : typeof chart.addSeries === 'function' && AreaSeriesCtor
          ? chart.addSeries(AreaSeriesCtor, {
              lineColor: chartLineColor,
              topColor: chartAreaTopColor,
              bottomColor: chartAreaBottomColor,
              lineWidth: 1,
            })
          : typeof chart.addLineSeries === 'function'
          ? chart.addLineSeries({ color: chartLineColor, lineWidth: 1 })
          : typeof chart.addSeries === 'function' && LineSeriesCtor
          ? chart.addSeries(LineSeriesCtor, { color: chartLineColor, lineWidth: 1 })
          : null;

      if (!series) return;

      chartSeriesRef.current = series;
      series.setData(chartData);

      // Attach the position-overlay canvas primitive so the chart paints the
      // overlays in lockstep with its own coordinate system — no layout drift
      // on zoom/resize, no per-event React state churn.
      try {
        const primitive = new PositionOverlayPrimitive(buildOverlayTheme(isDark));
        series.attachPrimitive?.(primitive);
        overlayPrimitiveRef.current = primitive;
        const { data, snapped } = buildOverlayPrimitiveData(chartData, positionOverlays);
        snappedOverlaysRef.current = snapped;
        primitive.setData(data);
      } catch (err) {
        console.warn('[chart] failed to attach position overlays primitive', err);
      }

      // Click/tap drives the HTML tooltip. Hover is intentionally ignored so the
      // overlay does not flash open while scanning the chart on desktop.
      const handleClick = (param: any) => {
        const point = param?.point;
        const time = param?.time;
        if (!point || time == null) {
          pinnedOverlayIdRef.current = null;
          setHoveredOverlay(null);
          setHoverX(null);
          return;
        }
        const t = Number(time);
        const hit = snappedOverlaysRef.current.find((o) => t >= o.entryTime && t <= o.exitTime);
        if (!hit) {
          pinnedOverlayIdRef.current = null;
          setHoveredOverlay(null);
          setHoverX(null);
          return;
        }
        if (pinnedOverlayIdRef.current === hit.pos.id) {
          pinnedOverlayIdRef.current = null;
        } else {
          pinnedOverlayIdRef.current = hit.pos.id;
          setHoveredOverlay(hit.pos);
          setHoverX(point.x);
        }
      };
      chart.subscribeClick(handleClick);

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
      overlayPrimitiveRef.current = null;
      pinnedOverlayIdRef.current = null;
    };
  }, [
    symbol,
    timeframe,
    rangeKey,
    hasChartData,
    isFullscreen,
    chartAreaBottomColor,
    chartAreaTopColor,
    chartGridColor,
    chartLineColor,
    chartTextColor,
  ]);

  useEffect(() => {
    const series = chartSeriesRef.current;
    if (!series || !chartData.length) return;
    series.setData(chartData);
  }, [chartData]);

  // Feed overlays to the canvas primitive. Snapping to candles depends only on
  // the data, so it happens here (once per data change); the chart itself
  // repaints the primitive on every zoom/resize frame — no manual projection,
  // no drift. Clearing the hover if the pinned/hovered overlay disappears.
  useEffect(() => {
    const { data, snapped } = buildOverlayPrimitiveData(chartData, positionOverlays);
    snappedOverlaysRef.current = snapped;
    overlayPrimitiveRef.current?.setData(data);
    const stillExists = (id: string | null) =>
      id !== null && snapped.some((o) => o.pos.id === id);
    if (!stillExists(pinnedOverlayIdRef.current)) pinnedOverlayIdRef.current = null;
    setHoveredOverlay((cur) => (cur && stillExists(cur.id) ? cur : null));
  }, [positionOverlays, chartData, chartInitToken]);

  useEffect(() => {
    overlayPrimitiveRef.current?.setTheme(buildOverlayTheme(isDark));
  }, [isDark]);

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

    // Same rAF-batched + observer approach as the position overlays so the
    // pulse marker tracks the chart on resize/zoom without event thrash.
    let raf = 0;
    const schedule = () => {
      if (raf) return;
      raf = window.requestAnimationFrame(() => {
        raf = 0;
        recalcLivePulse();
      });
    };

    recalcLivePulse();

    const container = chartContainerRef.current;
    const timeScale = chartInstanceRef.current?.timeScale?.();
    timeScale?.subscribeVisibleLogicalRangeChange?.(schedule);
    window.addEventListener('resize', schedule);
    let resizeObserver: ResizeObserver | null = null;
    if (container && typeof ResizeObserver !== 'undefined') {
      resizeObserver = new ResizeObserver(schedule);
      resizeObserver.observe(container);
    }

    return () => {
      if (raf) window.cancelAnimationFrame(raf);
      timeScale?.unsubscribeVisibleLogicalRangeChange?.(schedule);
      window.removeEventListener('resize', schedule);
      resizeObserver?.disconnect();
    };
  }, [chartData, livePrice, liveConnected, chartInitToken]);

  if (!adminGranted || !symbol) return null;

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
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2 sm:gap-3">
          <div className="inline-flex shrink-0 items-center gap-1 rounded-full border border-slate-200 bg-slate-50 p-1">
            {CHART_RANGE_ORDER.map((key) => {
              const active = key === rangeKey;
              return (
                <button
                  key={key}
                  type="button"
                  onClick={() => onRangeChange(key)}
                  className={`rounded-full px-2.5 py-1 text-[11px] font-semibold transition ${
                    active
                      ? 'neutral-highlight shadow-sm'
                      : 'text-slate-600 hover:bg-slate-200/70 hover:text-slate-900'
                  }`}
                  aria-pressed={active}
                  aria-label={`Switch chart range to ${key}`}
                >
                  {key}
                </button>
              );
            })}
          </div>
          {statsSlot ? (
            <div className="hidden min-w-0 sm:block">{statsSlot}</div>
          ) : null}
        </div>
        {statsSlot ? (
          <div className="min-w-0 sm:hidden">{statsSlot}</div>
        ) : null}
        <div
          className={`shrink-0 text-xs text-slate-400 ${statsSlot ? 'hidden sm:block' : ''}`}
        >
          {timeframe} bars · {rangeKey} window
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
            </div>
            {hoveredOverlay && hoverX !== null && (
              <div
                className="pointer-events-none absolute z-40"
                style={{
                  left: Math.min(Math.max(hoverX - 140, 8), (overlayLayerRef.current?.clientWidth || 320) - 288),
                  top: 10,
                }}
              >
                <div
                  role="button"
                  tabIndex={0}
                  aria-label="Close position tooltip"
                  className="pointer-events-auto max-h-[min(70vh,360px)] w-[280px] cursor-pointer overflow-y-auto overscroll-contain rounded-xl border border-slate-200 bg-white/95 px-3 py-2 text-[11px] text-slate-700 shadow-lg backdrop-blur"
                  onClick={(event) => {
                    event.stopPropagation();
                    closeOverlayTooltip();
                  }}
                  onKeyDown={(event) => {
                    if (event.key !== 'Enter' && event.key !== ' ' && event.key !== 'Escape') return;
                    event.preventDefault();
                    event.stopPropagation();
                    closeOverlayTooltip();
                  }}
                >
                  <div className="flex items-center justify-between gap-3">
                    <span className="font-semibold text-slate-900">
                      {hoveredOverlay.status === 'open' ? 'Open position' : 'Closed position'}
                    </span>
                    <span
                      className={
                        getOverlayPnlValue(hoveredOverlay) !== null
                          ? (getOverlayPnlValue(hoveredOverlay) as number) >= 0
                            ? 'font-semibold text-emerald-600'
                            : 'font-semibold text-rose-600'
                          : 'text-slate-500'
                      }
                    >
                      {formatOverlayPnl(hoveredOverlay) || '—'}
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
                        <span
                          className={`inline-flex rounded border px-1.5 py-0.5 font-semibold ${actionPillToneClass(
                            hoveredOverlay.entryDecision.action,
                          )}`}
                        >
                          {hoveredOverlay.entryDecision.action || 'Decision'}
                        </span>
                        {hoveredOverlay.entryDecision.summary ? ` · ${hoveredOverlay.entryDecision.summary}` : ''}
                      </div>
                      <div className="text-[10px] text-slate-500">
                        {formatOverlayDecisionTs(hoveredOverlay.entryDecision.timestamp || null)}
                      </div>
                    </div>
                  )}

                  {hoveredOverlay.partialCloses?.map((partial, idx) => (
                    <div
                      key={`${partial.timestamp ?? idx}-${partial.closePct ?? 'trim'}`}
                      className="mt-2 space-y-0.5 rounded-lg bg-slate-50/80 p-2"
                    >
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                        Partial close AI decision
                      </div>
                      <div className="text-[11px] text-slate-800">
                        <span
                          className={`inline-flex rounded border px-1.5 py-0.5 font-semibold ${actionPillToneClass(
                            partial.action || 'CLOSE',
                            getOverlayPnlValue(hoveredOverlay),
                          )}`}
                        >
                          {typeof partial.closePct === 'number'
                            ? `${partial.closePct.toFixed(0)}% ${partial.action || 'CLOSE'}`
                            : partial.action || 'CLOSE'}
                        </span>
                        {partial.summary ? ` · ${partial.summary}` : ''}
                      </div>
                      <div className="text-[10px] text-slate-500">
                        {formatOverlayDecisionTs(partial.timestamp || null)}
                      </div>
                    </div>
                  ))}

                  {hoveredOverlay.exitDecision && (
                    <div className="mt-2 space-y-0.5 rounded-lg bg-slate-50/80 p-2">
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                        Exit AI decision
                      </div>
                      <div className="text-[11px] text-slate-800">
                        <span
                          className={`inline-flex rounded border px-1.5 py-0.5 font-semibold ${actionPillToneClass(
                            hoveredOverlay.exitDecision.action,
                            getOverlayPnlValue(hoveredOverlay),
                          )}`}
                        >
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
