import React, { useCallback, useEffect, useRef, useState } from 'react';
import { ChartSkeleton, TimelineSkeleton } from './ChartSkeleton';

type DecisionBrief = {
  timestamp?: number | null;
  action?: string;
  summary?: string;
  reason?: string;
  closePct?: number | null;
};

type PartialCloseBrief = DecisionBrief & {
  closePct?: number | null;
  size?: number | null;
  // Venue cash this trim realized (matched server-side from the position's
  // folded trim chunks) — absent when no transaction row matched.
  pnlNet?: number | null;
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
  // Standing exchange-side bracket of the OPEN position (null/absent = no
  // resting order on that leg). Drawn as thin horizontal price lines.
  takeProfitPrice?: number | null;
  stopLossPrice?: number | null;
  // Inferred close cause for CLOSED positions: exchange-side bracket hit
  // ('tp'/'sl') vs. AI-driven close (null). Server-side inference.
  closeReason?: 'tp' | 'sl' | null;
  // AI post-mortem of this CLOSED position (losses by default) — verdict +
  // distilled lesson for the tooltip, violet exit accent on the overlay.
  postmortem?: {
    id: number;
    status: string;
    verdict?: string | null;
    lesson?: string | null;
  } | null;
};


type ChartApiResponse = {
  candles?: Array<{ time: number; close: number }>;
  markers?: any[];
  positions?: PositionOverlay[];
  pendingOrders?: PendingOrderLine[];
  cooldowns?: CooldownBandSegment[];
  limitOrders?: LimitOrderSegment[];
};

// An AI flat-HOLD cooldown window with its wake band levels — drawn as gray
// dashed horizontal segments spanning exactly the cooldown (times in epoch s).
type CooldownBandSegment = {
  fromTime: number;
  toTime: number;
  wakeAbove?: number | null;
  wakeBelow?: number | null;
};

// A pullback limit entry's resting window — drawn as a side-colored dashed
// segment at the limit price (green BUY / red SELL), with a dot when it filled.
type LimitOrderSegment = {
  side: 'buy' | 'sell';
  price: number;
  fromTime: number;
  toTime: number;
  filled?: boolean;
};

// A resting pullback limit entry (Bitget normal order / Capital working order),
// drawn as a dotted entry-level line on the chart.
type PendingOrderLine = {
  side: 'buy' | 'sell' | null;
  price: number;
  size?: string | null;
};

// A decision-timeline tick rendered as a time-aligned dot under the chart's
// time axis. Structurally compatible with the dashboard's TimelineTickUi.
export type ChartTimelineTick = {
  ts: number;
  hourly: boolean;
  kind: 'action' | 'ai_call' | 'gate_skip' | 'scan_skip' | 'scan' | 'postmortem';
  action?: string;
  stage?: string;
  reason?: string;
  // Post-mortem ticks (violet, at the position's exit time): status + the
  // verdict/lesson once the analysis succeeded.
  postmortemId?: number;
  postmortemStatus?: string;
  verdict?: string;
  lesson?: string;
  // AI-requested flat cooldown armed by this decision (flat HOLD only) —
  // appended to the tooltip label ("AI HOLD + CD 2h (↑x ↓y)"); the dot keeps
  // the plain ai_call fill.
  cooldownMinutes?: number;
  cooldownWakeAbove?: number;
  cooldownWakeBelow?: number;
  // Responses-API conversation chain: `previousResponseId` marks a context AI
  // call (in-position tick / post-fill management of a pullback limit) chained
  // onto the tick whose `responseId` matches — linked with a full-contrast
  // connector segment.
  responseId?: string;
  previousResponseId?: string;
};

const timelineDotFillClass = (tick: ChartTimelineTick): string =>
  tick.kind === 'action'
    ? tick.action === 'BUY'
      ? 'timeline-dot-buy'
      : tick.action === 'SELL'
        ? 'timeline-dot-sell'
        : 'timeline-dot-trim'
    : tick.kind === 'postmortem'
      ? 'timeline-dot-postmortem'
      : tick.kind === 'ai_call'
        ? 'timeline-dot-ai'
        : 'timeline-dot-skip';

// "HOLD + CD 2h (↑51,200 ↓49,700)" suffix for ticks that armed a flat cooldown.
const timelineTickCooldownSuffix = (tick: ChartTimelineTick): string => {
  const minutes = Number(tick.cooldownMinutes);
  if (!Number.isFinite(minutes) || minutes <= 0) return '';
  const m = Math.max(1, Math.round(minutes));
  const duration = m < 60 ? `${m}m` : m % 60 ? `${Math.floor(m / 60)}h${m % 60}m` : `${m / 60}h`;
  const fmt = (v: number) =>
    v.toLocaleString('en-US', { maximumFractionDigits: Math.abs(v) >= 1000 ? 0 : Math.abs(v) >= 10 ? 2 : 4 });
  const bands = [
    Number.isFinite(Number(tick.cooldownWakeAbove)) && Number(tick.cooldownWakeAbove) > 0
      ? `↑${fmt(Number(tick.cooldownWakeAbove))}`
      : null,
    Number.isFinite(Number(tick.cooldownWakeBelow)) && Number(tick.cooldownWakeBelow) > 0
      ? `↓${fmt(Number(tick.cooldownWakeBelow))}`
      : null,
  ].filter(Boolean);
  return ` + CD ${duration}${bands.length ? ` (${bands.join(' ')})` : ''}`;
};

const timelineTickLabel = (tick: ChartTimelineTick): string => {
  const time = new Intl.DateTimeFormat('de-DE', {
    timeZone: BERLIN_TZ,
    day: '2-digit',
    month: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(new Date(tick.ts));
  return `${time}${
    tick.kind === 'action'
      ? ` · ${tick.action}`
      : tick.kind === 'postmortem'
        ? ` · post-mortem${
            tick.verdict ? `: ${tick.verdict}` : tick.postmortemStatus ? ` (${tick.postmortemStatus})` : ''
          }`
        : tick.kind === 'ai_call'
          ? ` · AI ${tick.action || 'decision'}${timelineTickCooldownSuffix(tick)}`
          : tick.stage
            ? ` · skipped: ${tick.reason || tick.stage}`
            : ' · scanned'
  }`;
};

// Minimum px between dot centers before lower-priority dots get culled on
// wide zooms. Priority: selected > BUY/SELL/trim > AI call > hourly skip >
// quarter skip.
const TIMELINE_MIN_GAP_PX = 14;

type CachedChartEntry = {
  payload: ChartApiResponse;
  fetchedAt: number;
};

export type ChartRangeKey = '4H' | '1D' | '7D' | '30D' | '6M';

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
  // Vertical marker for the decision-timeline selection (ms epoch); null hides it.
  highlightTimeMs?: number | null;
  // Chart click → nearest-tick selection on the decision timeline (ms epoch).
  onTimeSelect?: (tsMs: number) => void;
  // Decision-timeline ticks rendered time-aligned under the chart's time axis.
  timelineTicks?: ChartTimelineTick[];
  // True while the parent is still fetching the ticks — shows the dot skeleton
  // instead of collapsing the strip.
  timelineLoading?: boolean;
  // Tick that renders with the full-contrast selection stroke.
  selectedTimelineTs?: number | null;
  // Click on a timeline dot (ms epoch of that tick).
  onTimelineTickSelect?: (tsMs: number) => void;
};

const BERLIN_TZ = 'Europe/Berlin';
const CHART_CACHE_TTL_MS = 60_000;
const CHART_FETCH_DEFER_MS = 120;
const CHART_RANGE_ORDER: ChartRangeKey[] = ['4H', '1D', '7D', '30D', '6M'];
const CHART_RANGE_PRESETS: Record<ChartRangeKey, { timeframe: string; limit: number }> = {
  '4H': { timeframe: '5m', limit: 48 },
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
  if (rangeKey === '1D' || rangeKey === '4H') {
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

// Render a decision's action label, annotating a partial close as e.g. "30% CLOSE"
// so a trim isn't shown as a bare "Close" (mirrors the partial-close pill format
// and the AI-prompt-feed annotation).
const formatDecisionActionLabel = (decision?: DecisionBrief | null): string => {
  const action = decision?.action;
  if (action === 'CLOSE' && typeof decision?.closePct === 'number') {
    return `${decision.closePct.toFixed(0)}% ${action}`;
  }
  return action || 'Decision';
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
  cooldownBand: string;
  cooldownFill: string;
  limitBuy: string;
  limitSell: string;
  postmortem: string;
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
  partialStroke: isDark ? 'rgba(251,191,36,0.95)' : 'rgba(245,158,11,0.9)',
  partialText: isDark ? 'rgb(253,230,138)' : 'rgb(146,64,14)',
  cooldownBand: isDark ? 'rgba(161,161,170,0.65)' : 'rgba(113,113,122,0.6)',
  cooldownFill: isDark ? 'rgba(161,161,170,0.12)' : 'rgba(113,113,122,0.1)',
  limitBuy: isDark ? 'rgba(52,211,153,0.85)' : 'rgba(5,150,105,0.8)',
  limitSell: isDark ? 'rgba(251,113,133,0.85)' : 'rgba(225,29,72,0.8)',
  postmortem: isDark ? 'rgba(167,139,250,0.95)' : 'rgba(124,58,237,0.9)',
});

type OverlayPrimitiveDatum = {
  id: string;
  entryTime: number;
  exitTime: number;
  showEntryWall: boolean;
  closed: boolean;
  closeReason: 'tp' | 'sl' | null;
  hasPostmortem: boolean;
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
    private readonly bandItems: { left: number; right: number; yAbove: number | null; yBelow: number | null }[],
    private readonly limitItems: { left: number; right: number; y: number; side: 'buy' | 'sell'; filled: boolean }[],
    private readonly theme: OverlayTheme,
  ) {}
  draw(target: any) {
    target.useMediaCoordinateSpace((scope: any) => {
      const ctx = scope.context as CanvasRenderingContext2D;
      const paneHeight = scope.mediaSize.height as number;
      // Cooldown wake zones: a translucent gray box between the two wake bands
      // over exactly the cooldown window, with dashed edges — reads as "the AI
      // sleeps while price stays inside this range", visually distinct from the
      // full-width TP/SL price lines. A one-sided band degrades to its dashed
      // edge only. Drawn before the position boxes so those stay on top.
      if (this.bandItems.length) {
        for (const band of this.bandItems) {
          const left = band.left;
          const right = Math.max(band.right, band.left + 2);
          if (band.yAbove !== null && band.yBelow !== null) {
            const zoneTop = Math.min(band.yAbove, band.yBelow);
            const zoneHeight = Math.abs(band.yBelow - band.yAbove);
            ctx.fillStyle = this.theme.cooldownFill;
            ctx.fillRect(left, zoneTop, right - left, Math.max(1, zoneHeight));
          }
          ctx.strokeStyle = this.theme.cooldownBand;
          ctx.lineWidth = 1;
          ctx.setLineDash([4, 3]);
          for (const yEdge of [band.yAbove, band.yBelow]) {
            if (yEdge === null) continue;
            const y = Math.round(yEdge) + 0.5;
            ctx.beginPath();
            ctx.moveTo(left, y);
            ctx.lineTo(right, y);
            ctx.stroke();
          }
          ctx.setLineDash([]);
        }
      }
      // Resting limit windows: side-colored dashed segments at the limit price
      // (green BUY / red SELL) spanning the time the order actually rested; a
      // filled dot marks the fill moment at the segment end.
      if (this.limitItems.length) {
        ctx.lineWidth = 1;
        for (const order of this.limitItems) {
          const color = order.side === 'buy' ? this.theme.limitBuy : this.theme.limitSell;
          const y = Math.round(order.y) + 0.5;
          const right = Math.max(order.right, order.left + 2);
          ctx.strokeStyle = color;
          ctx.setLineDash([6, 4]);
          ctx.beginPath();
          ctx.moveTo(order.left, y);
          ctx.lineTo(right, y);
          ctx.stroke();
          ctx.setLineDash([]);
          if (order.filled) {
            ctx.fillStyle = color;
            ctx.beginPath();
            ctx.arc(right, y, 2.5, 0, Math.PI * 2);
            ctx.fill();
          }
        }
      }
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
            ctx.fillRect(markerX, top + 3, 1, Math.max(4, height - 6));
          }
        }
        if (datum.closeReason) {
          // Bracket-hit exit: a stronger, TP/SL-colored vertical line so the
          // "hit" moment stands out from AI-driven closes.
          ctx.fillStyle =
            datum.closeReason === 'tp' ? 'rgba(16,185,129,0.95)' : 'rgba(239,68,68,0.95)';
          ctx.fillRect(right - 2, top, 2, height);
        } else {
          ctx.fillStyle = datum.closed ? stroke : this.theme.openWall;
          ctx.fillRect(right - 1, top, 1, height);
        }
        if (datum.hasPostmortem) {
          // Analyzed close: a violet dot at the foot of the exit wall — "this
          // loss has been autopsied", without recoloring the TP/SL-hit wall.
          ctx.fillStyle = this.theme.postmortem;
          ctx.beginPath();
          ctx.arc(right - 1, top + height - 5, 3, 0, Math.PI * 2);
          ctx.fill();
        }
        drawOverlayBadge(ctx, right, top, datum, this.theme);
      }
    });
  }
}

class PositionOverlayPaneView {
  private items: { left: number; right: number; partials: number[]; datum: OverlayPrimitiveDatum }[] = [];
  private bandItems: { left: number; right: number; yAbove: number | null; yBelow: number | null }[] = [];
  private limitItems: { left: number; right: number; y: number; side: 'buy' | 'sell'; filled: boolean }[] = [];
  constructor(private readonly source: PositionOverlayPrimitive) {}
  update() {
    const chart = this.source.chart;
    const data = this.source.data;
    if (!chart) {
      this.items = [];
      this.bandItems = [];
      this.limitItems = [];
      return;
    }
    const timeScale = chart.timeScale();
    const series = this.source.series;
    this.limitItems =
      series && typeof series.priceToCoordinate === 'function'
        ? (this.source.limitOrders
            .map((order) => {
              const x1 = timeScale.timeToCoordinate(order.fromTime);
              const x2 = timeScale.timeToCoordinate(order.toTime);
              const y = series.priceToCoordinate(order.price);
              if (
                x1 === null ||
                x2 === null ||
                y === null ||
                !Number.isFinite(x1) ||
                !Number.isFinite(x2) ||
                !Number.isFinite(y)
              ) {
                return null;
              }
              return { left: Math.min(x1, x2), right: Math.max(x1, x2), y, side: order.side, filled: order.filled };
            })
            .filter(Boolean) as { left: number; right: number; y: number; side: 'buy' | 'sell'; filled: boolean }[])
        : [];
    this.bandItems =
      series && typeof series.priceToCoordinate === 'function'
        ? (this.source.bands
            .map((band) => {
              const x1 = timeScale.timeToCoordinate(band.fromTime);
              const x2 = timeScale.timeToCoordinate(band.toTime);
              if (x1 === null || x2 === null || !Number.isFinite(x1) || !Number.isFinite(x2)) {
                return null;
              }
              const toY = (level: number | null): number | null => {
                if (level === null) return null;
                const y = series.priceToCoordinate(level);
                return y !== null && Number.isFinite(y) ? y : null;
              };
              const yAbove = toY(band.above);
              const yBelow = toY(band.below);
              if (yAbove === null && yBelow === null) return null;
              return { left: Math.min(x1, x2), right: Math.max(x1, x2), yAbove, yBelow };
            })
            .filter(Boolean) as { left: number; right: number; yAbove: number | null; yBelow: number | null }[])
        : [];
    if (!data.length) {
      this.items = [];
      return;
    }
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
    return new PositionOverlayRenderer(this.items, this.bandItems, this.limitItems, this.source.theme);
  }
}

// A cooldown window with its wake levels, candle-snapped. Rendered as a shaded
// gray zone between the bands (dashed edges) so it reads as a quiet range over
// a time window, not as another TP/SL-style price level line.
type CooldownBandItem = { fromTime: number; toTime: number; above: number | null; below: number | null };

// A resting limit's window, candle-snapped: dashed side-colored segment at the
// limit price; `filled` draws a dot at the segment end (the fill moment).
type LimitOrderItem = { fromTime: number; toTime: number; price: number; side: 'buy' | 'sell'; filled: boolean };

class PositionOverlayPrimitive {
  chart: any = null;
  series: any = null;
  data: OverlayPrimitiveDatum[] = [];
  bands: CooldownBandItem[] = [];
  limitOrders: LimitOrderItem[] = [];
  theme: OverlayTheme;
  private requestUpdate: (() => void) | null = null;
  private readonly paneView: PositionOverlayPaneView;
  constructor(theme: OverlayTheme) {
    this.theme = theme;
    this.paneView = new PositionOverlayPaneView(this);
  }
  attached(param: any) {
    this.chart = param.chart;
    this.series = param.series ?? null;
    this.requestUpdate = param.requestUpdate;
  }
  detached() {
    this.chart = null;
    this.series = null;
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
  setBands(bands: CooldownBandItem[]) {
    this.bands = bands;
    this.requestUpdate?.();
  }
  setLimitOrders(limitOrders: LimitOrderItem[]) {
    this.limitOrders = limitOrders;
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
      closeReason: pos.status === 'closed' ? pos.closeReason ?? null : null,
      hasPostmortem: pos.status === 'closed' && pos.postmortem?.status === 'succeeded',
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

// Snap each cooldown window to candle times (timeToCoordinate needs exact bar
// times), keeping the wake_above/wake_below pair together so the renderer can
// shade the zone between them. Windows fully outside the loaded candles are
// dropped; ones sticking out are clamped, so a still-running cooldown ends at
// the last bar.
const buildCooldownBandItems = (
  chartData: { time: number; value: number }[],
  bands: CooldownBandSegment[],
): CooldownBandItem[] => {
  if (!chartData.length || !bands.length) return [];
  const minTime = chartData[0].time;
  const maxTime = chartData[chartData.length - 1].time;
  const candleTimes = chartData.map((c) => c.time);
  const nearestTime = (target: number) =>
    candleTimes.reduce(
      (prev, curr) => (Math.abs(curr - target) < Math.abs(prev - target) ? curr : prev),
      candleTimes[0],
    );
  const positiveLevel = (level: unknown): number | null => {
    const value = Number(level);
    return Number.isFinite(value) && value > 0 ? value : null;
  };
  const items: CooldownBandItem[] = [];
  for (const band of bands) {
    const fromRaw = Number(band.fromTime);
    const toRaw = Number(band.toTime);
    if (!Number.isFinite(fromRaw) || !Number.isFinite(toRaw) || toRaw <= fromRaw) continue;
    if (toRaw < minTime || fromRaw > maxTime) continue;
    const above = positiveLevel(band.wakeAbove);
    const below = positiveLevel(band.wakeBelow);
    if (above === null && below === null) continue;
    items.push({
      fromTime: nearestTime(Math.min(Math.max(fromRaw, minTime), maxTime)),
      toTime: nearestTime(Math.min(Math.max(toRaw, minTime), maxTime)),
      above,
      below,
    });
  }
  return items;
};

// Same snap/clamp treatment for resting-limit windows (see buildCooldownBandItems).
const buildLimitOrderItems = (
  chartData: { time: number; value: number }[],
  orders: LimitOrderSegment[],
): LimitOrderItem[] => {
  if (!chartData.length || !orders.length) return [];
  const minTime = chartData[0].time;
  const maxTime = chartData[chartData.length - 1].time;
  const candleTimes = chartData.map((c) => c.time);
  const nearestTime = (target: number) =>
    candleTimes.reduce(
      (prev, curr) => (Math.abs(curr - target) < Math.abs(prev - target) ? curr : prev),
      candleTimes[0],
    );
  const items: LimitOrderItem[] = [];
  for (const order of orders) {
    const fromRaw = Number(order.fromTime);
    const toRaw = Number(order.toTime);
    const price = Number(order.price);
    if (!Number.isFinite(fromRaw) || !Number.isFinite(toRaw) || toRaw <= fromRaw) continue;
    if (!Number.isFinite(price) || price <= 0) continue;
    if (toRaw < minTime || fromRaw > maxTime) continue;
    if (order.side !== 'buy' && order.side !== 'sell') continue;
    items.push({
      fromTime: nearestTime(Math.min(Math.max(fromRaw, minTime), maxTime)),
      toTime: nearestTime(Math.min(Math.max(toRaw, minTime), maxTime)),
      price,
      side: order.side,
      filled: order.filled === true,
    });
  }
  return items;
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
    onOpenPositionChange,
    onPositionSummaryChange,
    highlightTimeMs = null,
    onTimeSelect,
    timelineTicks,
    timelineLoading = false,
    selectedTimelineTs = null,
    onTimelineTickSelect,
  } = props;

  const [chartData, setChartData] = useState<{ time: number; value: number }[]>([]);
  const [positionOverlays, setPositionOverlays] = useState<PositionOverlay[]>([]);
  const [pendingOrders, setPendingOrders] = useState<PendingOrderLine[]>([]);
  const [cooldownBands, setCooldownBands] = useState<CooldownBandSegment[]>([]);
  const [limitOrderSegments, setLimitOrderSegments] = useState<LimitOrderSegment[]>([]);
  const [chartLoading, setChartLoading] = useState(false);
  const [chartAttempted, setChartAttempted] = useState(false);
  const [hoveredOverlay, setHoveredOverlay] = useState<PositionOverlay | null>(null);
  const [hoverX, setHoverX] = useState<number | null>(null);
  const [highlightX, setHighlightX] = useState<number | null>(null);
  const [timelineDots, setTimelineDots] = useState<
    Array<{ x: number; tick: ChartTimelineTick }>
  >([]);
  // Full-contrast connector segments linking the decisions of one AI
  // conversation (context calls) — px ranges over the timeline strip.
  const [threadSegments, setThreadSegments] = useState<
    Array<{ left: number; width: number }>
  >([]);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [chartInitToken, setChartInitToken] = useState(0);

  const panelRef = useRef<HTMLDivElement | null>(null);
  const chartContainerRef = useRef<HTMLDivElement | null>(null);
  const overlayLayerRef = useRef<HTMLDivElement | null>(null);
  const chartInstanceRef = useRef<any>(null);
  const chartSeriesRef = useRef<any>(null);
  const overlayPrimitiveRef = useRef<PositionOverlayPrimitive | null>(null);
  const bracketPriceLinesRef = useRef<any[]>([]);
  const pendingOrderLinesRef = useRef<any[]>([]);
  // Identity of the currently-rendered dataset — dataset swaps re-fit the
  // time scale, live appends don't (see the setData effect).
  const lastDatasetRef = useRef<{ first: number; length: number } | null>(null);
  const snappedOverlaysRef = useRef<SnappedOverlay[]>([]);
  const pinnedOverlayIdRef = useRef<string | null>(null);
  // Prop callbacks used inside chart event handlers registered once at init —
  // read through a ref so the handlers never close over stale props.
  const onTimeSelectRef = useRef<typeof onTimeSelect>(onTimeSelect);
  onTimeSelectRef.current = onTimeSelect;
  const chartCacheRef = useRef<Map<string, CachedChartEntry>>(new Map());
  const rangePreset = CHART_RANGE_PRESETS[rangeKey];
  const timeframe = rangePreset.timeframe;
  const timeframeSeconds = timeframeToSeconds(timeframe);
  const resolvedLimit = Math.max(32, rangePreset.limit);
  // Right price-scale width: tighter on phones to win horizontal chart space
  // (it's a minimum — the scale still grows if a label needs more). The same
  // value insets the overlay layer and the timeline strip so they stay aligned
  // with the pane.
  const priceScaleWidth =
    typeof window !== 'undefined' && window.matchMedia('(max-width: 640px)').matches
      ? 40
      : 56;
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
    setPendingOrders(Array.isArray(payload.pendingOrders) ? payload.pendingOrders : []);
    setCooldownBands(Array.isArray(payload.cooldowns) ? payload.cooldowns : []);
    setLimitOrderSegments(Array.isArray(payload.limitOrders) ? payload.limitOrders : []);
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
      setCooldownBands([]);
      setLimitOrderSegments([]);
      overlayPrimitiveRef.current?.setData([]);
      overlayPrimitiveRef.current?.setBands([]);
      overlayPrimitiveRef.current?.setLimitOrders([]);
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

  // Fullscreen must be escapable on mobile too: lock the page scroll behind the
  // overlay and let the browser/hardware back button exit fullscreen instead of
  // leaving the dashboard (pushState on enter, popstate exits).
  useEffect(() => {
    if (!isFullscreen || typeof window === 'undefined') return;
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    window.history.pushState({ ...(window.history.state ?? {}), chartFullscreen: true }, '');
    const onPopState = () => setIsFullscreen(false);
    window.addEventListener('popstate', onPopState);
    return () => {
      document.body.style.overflow = prevOverflow;
      window.removeEventListener('popstate', onPopState);
      // Exit via button/Escape leaves our pushed entry on the stack — pop it so
      // the next back press doesn't need a no-op step. When the exit itself came
      // from popstate the flag is already gone.
      if ((window.history.state as any)?.chartFullscreen) {
        window.history.back();
      }
    };
  }, [isFullscreen]);

  // Reset any pan/zoom back to the full loaded window.
  const resetChartView = useCallback(() => {
    try {
      chartInstanceRef.current?.timeScale?.()?.fitContent?.();
    } catch {
      // best-effort — a disposed chart just ignores the reset
    }
  }, []);

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

      const initialHeight = Math.max(260, Math.floor(container.clientHeight || 260));
      chart = createChart(container, {
        width: container.clientWidth,
        height: initialHeight,
        // Interactive pan/zoom: wheel + pinch zoom, drag to pan. Vertical touch
        // drag stays with the page so mobile scrolling over the chart works.
        handleScroll: {
          pressedMouseMove: true,
          mouseWheel: false,
          horzTouchDrag: true,
          vertTouchDrag: false,
        },
        handleScale: {
          mouseWheel: true,
          pinch: true,
          axisPressedMouseMove: true,
          axisDoubleClickReset: true,
        },
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
        // Chart click drives the decision timeline too: hand the clicked bar
        // time to the parent so it can select the nearest tick.
        if (Number.isFinite(t)) onTimeSelectRef.current?.(t * 1000);
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
    // With interactive zoom enabled the visible logical range survives
    // setData, so a dataset SWAP (range/timeframe switch, symbol change)
    // would keep showing the old window width. Re-fit only when the data
    // identity really changed — live tick appends (and the occasional
    // ring-buffer head trim) must NOT reset the user's zoom.
    const first = chartData[0]?.time ?? 0;
    const prev = lastDatasetRef.current;
    const swapped =
      !prev ||
      Math.abs(prev.first - first) > timeframeSeconds * 2 ||
      Math.abs(prev.length - chartData.length) > 2;
    lastDatasetRef.current = { first, length: chartData.length };
    if (swapped) {
      try {
        chartInstanceRef.current?.timeScale?.()?.fitContent?.();
      } catch {
        /* chart mid-teardown */
      }
    }
  }, [chartData, timeframeSeconds]);

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

  // Feed cooldown wake bands to the same canvas primitive — gray dashed
  // horizontal segments spanning each cooldown window. Same pattern as the
  // position overlays above: snapping depends only on the data; the chart
  // repaints the primitive on zoom/resize itself.
  useEffect(() => {
    overlayPrimitiveRef.current?.setBands(buildCooldownBandItems(chartData, cooldownBands));
  }, [cooldownBands, chartData, chartInitToken]);

  // Resting-limit windows: same feed pattern as the cooldown bands above.
  useEffect(() => {
    overlayPrimitiveRef.current?.setLimitOrders(buildLimitOrderItems(chartData, limitOrderSegments));
  }, [limitOrderSegments, chartData, chartInitToken]);

  // Standing exchange-side bracket of the open position: thin horizontal price
  // lines — TP green, SL red. Recreated whenever the overlay payload or the
  // chart instance changes (chartInitToken bumps after series init); the lines
  // die with the series on re-init, so removal is best-effort.
  useEffect(() => {
    const series = chartSeriesRef.current;
    if (!series) return;
    for (const line of bracketPriceLinesRef.current) {
      try {
        series.removePriceLine?.(line);
      } catch {
        /* series/line already disposed */
      }
    }
    bracketPriceLinesRef.current = [];
    const open = positionOverlays.find((pos) => pos?.status === 'open');
    if (!open || typeof series.createPriceLine !== 'function') return;
    const drawLine = (price: unknown, color: string, title: string) => {
      const value = Number(price);
      if (!Number.isFinite(value) || value <= 0) return;
      try {
        bracketPriceLinesRef.current.push(
          series.createPriceLine({
            price: value,
            color,
            lineWidth: 1,
            lineStyle: 2, // dashed — reads as a level, not a price path
            axisLabelVisible: true,
            title,
          }),
        );
      } catch (err) {
        console.warn('[chart] failed to draw bracket price line', err);
      }
    };
    drawLine(open.takeProfitPrice, 'rgba(16,185,129,0.9)', 'TP');
    drawLine(open.stopLossPrice, 'rgba(239,68,68,0.9)', 'SL');
  }, [positionOverlays, chartInitToken]);

  // Resting pullback limit entries as dotted entry-level lines — side-colored
  // so a waiting BUY reads green, a waiting SELL red.
  useEffect(() => {
    const series = chartSeriesRef.current;
    if (!series) return;
    for (const line of pendingOrderLinesRef.current) {
      try {
        series.removePriceLine?.(line);
      } catch {
        /* series/line already disposed */
      }
    }
    pendingOrderLinesRef.current = [];
    if (!pendingOrders.length || typeof series.createPriceLine !== 'function') return;
    for (const order of pendingOrders) {
      if (!Number.isFinite(order.price) || order.price <= 0) continue;
      try {
        pendingOrderLinesRef.current.push(
          series.createPriceLine({
            price: order.price,
            color: order.side === 'sell' ? 'rgba(225,29,72,0.75)' : 'rgba(5,150,105,0.75)',
            lineWidth: 1,
            lineStyle: 1, // dotted — a waiting order, not an active level
            axisLabelVisible: true,
            title: `${(order.side || 'entry').toUpperCase()} LIMIT`,
          }),
        );
      } catch (err) {
        console.warn('[chart] failed to draw pending order line', err);
      }
    }
  }, [pendingOrders, chartInitToken]);

  useEffect(() => {
    // Vertical marker at the decision-timeline selection: snap the selected
    // tick's time to the nearest bar and project it to an X coordinate.
    const recalcHighlightX = () => {
      const timeScaleNow = chartInstanceRef.current?.timeScale?.();
      if (
        highlightTimeMs == null ||
        !timeScaleNow ||
        !chartData.length ||
        typeof timeScaleNow.timeToCoordinate !== 'function'
      ) {
        setHighlightX(null);
        return;
      }
      const targetSec = highlightTimeMs / 1000;
      let nearest = chartData[0].time;
      let bestDiff = Math.abs(chartData[0].time - targetSec);
      for (const bar of chartData) {
        const diff = Math.abs(bar.time - targetSec);
        if (diff < bestDiff) {
          bestDiff = diff;
          nearest = bar.time;
        }
      }
      const x = Number(timeScaleNow.timeToCoordinate(nearest));
      setHighlightX(Number.isFinite(x) ? x : null);
    };

    // Snap a ms timestamp to the nearest bar time (chartData is ascending).
    const nearestBarTime = (tsMs: number): number => {
      const target = tsMs / 1000;
      let lo = 0;
      let hi = chartData.length - 1;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (chartData[mid].time < target) lo = mid + 1;
        else hi = mid;
      }
      const below = Math.max(0, lo - 1);
      return Math.abs(chartData[below].time - target) <=
        Math.abs(chartData[lo].time - target)
        ? chartData[below].time
        : chartData[lo].time;
    };

    // Project timeline ticks onto the time axis. Zoom/pan re-runs this via the
    // shared schedule, so the strip stretches with the chart; on wide zooms,
    // lower-priority dots inside TIMELINE_MIN_GAP_PX of a kept dot are culled.
    const recalcTimelineDots = () => {
      const timeScaleNow = chartInstanceRef.current?.timeScale?.();
      if (
        !timelineTicks?.length ||
        !timeScaleNow ||
        !chartData.length ||
        typeof timeScaleNow.timeToCoordinate !== 'function'
      ) {
        setTimelineDots([]);
        setThreadSegments([]);
        return;
      }
      const paneWidth = overlayLayerRef.current?.clientWidth ?? 0;
      const projected: Array<{ x: number; tick: ChartTimelineTick }> = [];
      for (const tick of timelineTicks) {
        const x = Number(timeScaleNow.timeToCoordinate(nearestBarTime(tick.ts)));
        if (!Number.isFinite(x) || x < 0 || (paneWidth > 0 && x > paneWidth)) continue;
        projected.push({ x, tick });
      }

      // Full-contrast segments linking context AI calls: each tick that chained
      // onto a previous response gets a segment back to the tick that produced
      // that response (the resting period of a pullback limit is bridged — the
      // fill's management tick chains straight onto the entry decision). A
      // predecessor missing from the window clamps to the strip's left edge:
      // the conversation extends into the past. Culling never drops segments —
      // they're computed from ALL ticks, not the kept dots.
      const tsByResponseId = new Map<string, number>();
      for (const tick of timelineTicks) {
        if (tick.responseId) tsByResponseId.set(tick.responseId, tick.ts);
      }
      const clampedX = (tsMs: number): number => {
        const x = Number(timeScaleNow.timeToCoordinate(nearestBarTime(tsMs)));
        if (!Number.isFinite(x)) return NaN;
        return Math.min(Math.max(x, 0), paneWidth > 0 ? paneWidth : x);
      };
      const segments: Array<{ left: number; width: number }> = [];
      for (const tick of timelineTicks) {
        if (!tick.previousResponseId) continue;
        const endX = clampedX(tick.ts);
        if (!Number.isFinite(endX)) continue;
        const prevTs = tsByResponseId.get(tick.previousResponseId);
        const startX = prevTs != null ? clampedX(prevTs) : 0;
        if (!Number.isFinite(startX) || endX - startX < 1) continue;
        segments.push({ left: startX, width: endX - startX });
      }
      // A live position keeps the decision context active even when later
      // quarter ticks are flat gate skips and therefore have no response ids.
      // Extend the bright thread from entry through the latest observed tick.
      const openPosition = positionOverlays.find((pos) => pos.status === 'open');
      const openEntryMs = Number(openPosition?.entryTime) * 1000;
      const latestTickTs = timelineTicks.reduce(
        (latest, tick) => Math.max(latest, tick.ts),
        0,
      );
      if (
        Number.isFinite(openEntryMs) &&
        openEntryMs > 0 &&
        latestTickTs > openEntryMs
      ) {
        const startX = clampedX(openEntryMs);
        const endX = clampedX(latestTickTs);
        if (
          Number.isFinite(startX) &&
          Number.isFinite(endX) &&
          endX - startX >= 1
        ) {
          segments.push({ left: startX, width: endX - startX });
        }
      }
      setThreadSegments(segments);
      const kindPriority = (tick: ChartTimelineTick): number =>
        // Post-mortems rank with actions: one dot per analyzed trade, and it
        // must survive culling next to the exit-adjacent decision dots.
        tick.kind === 'action' || tick.kind === 'postmortem'
          ? 0
          : tick.kind === 'ai_call'
            ? 1
            : tick.hourly
              ? 2
              : 3;
      // Selection floats a dot ahead of its own kind, but a selected non-action
      // dot never outranks BUY/SELL/trim dots: the default "live" selection is
      // often a scan tick newer than the last candle, which snaps onto the
      // latest decision's bar — and must not cull its action dot.
      const priority = (tick: ChartTimelineTick): number =>
        tick.ts === selectedTimelineTs
          ? kindPriority(tick) === 0
            ? -1
            : 0.5
          : kindPriority(tick);
      projected.sort(
        (a, b) => priority(a.tick) - priority(b.tick) || b.tick.ts - a.tick.ts,
      );
      const kept: Array<{ x: number; tick: ChartTimelineTick }> = [];
      for (const candidate of projected) {
        if (kept.some((k) => Math.abs(k.x - candidate.x) < TIMELINE_MIN_GAP_PX)) continue;
        kept.push(candidate);
      }
      kept.sort((a, b) => a.x - b.x);
      setTimelineDots(kept);
    };

    // Same rAF-batched + observer approach as the position overlays so the
    // markers track the chart on resize/zoom without event thrash.
    let raf = 0;
    const schedule = () => {
      if (raf) return;
      raf = window.requestAnimationFrame(() => {
        raf = 0;
        recalcHighlightX();
        recalcTimelineDots();
      });
    };

    recalcHighlightX();
    recalcTimelineDots();

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
  }, [
    chartData,
    chartInitToken,
    highlightTimeMs,
    timelineTicks,
    selectedTimelineTs,
    positionOverlays,
  ]);

  if (!adminGranted || !symbol) return null;

  const showSkeleton = (chartLoading && !hasChartData) || !chartAttempted;
  const showEmpty = !chartLoading && chartAttempted && !hasChartData;

  return (
    <div
      ref={panelRef}
      className={`bg-white px-2 py-4 sm:p-4 ${
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
          {typeof livePrice === 'number' ? ` · ${livePrice.toFixed(2)}` : ''}
          {chartLoading && hasChartData ? ' · updating…' : ''}
        </div>
      </div>
      <div
        className={`relative mt-3 w-full ${isFullscreen ? 'h-[calc(100dvh-96px)]' : 'h-[260px]'}`}
        style={{ minHeight: 260 }}
        onDoubleClick={resetChartView}
      >
        {isFullscreen ? (
          <button
            type="button"
            onClick={() => setIsFullscreen(false)}
            className="absolute right-3 top-0 z-30 inline-flex h-8 w-8 items-center justify-center rounded-md border border-slate-200 bg-white/95 text-slate-600 shadow-sm backdrop-blur transition hover:border-rose-300 hover:text-rose-600"
            aria-label="Exit fullscreen chart"
            title="Exit fullscreen"
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
              <path d="M6 6l12 12" />
              <path d="M18 6L6 18" />
            </svg>
          </button>
        ) : null}
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
          <ChartSkeleton />
        ) : hasChartData ? (
          <div ref={chartContainerRef} className="h-full w-full" style={{ minHeight: 260 }} />
        ) : showEmpty ? (
          <div className="flex h-full w-full items-center justify-center rounded-xl border border-slate-200 bg-slate-50 text-sm font-semibold text-slate-500">
            Chart unavailable right now. Try refresh.
          </div>
        ) : null}
        {hasChartData && (
          <>
            <div ref={overlayLayerRef} className="pointer-events-none absolute inset-0" style={{ right: priceScaleWidth }}>
              {highlightX !== null ? (
                // Time marker for the selected decision-timeline tick.
                <div
                  className="pointer-events-none absolute bottom-0 top-0 w-px"
                  style={{
                    left: highlightX,
                    backgroundColor: isDark ? 'rgba(255,255,255,0.4)' : 'rgba(15,23,42,0.35)',
                  }}
                />
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

                  {hoveredOverlay.status === 'closed' && hoveredOverlay.closeReason ? (
                    <div className="mt-2 rounded-lg bg-slate-50/80 p-2 text-[11px]">
                      <span
                        className={`font-semibold ${
                          hoveredOverlay.closeReason === 'tp' ? 'text-emerald-600' : 'text-rose-600'
                        }`}
                      >
                        {hoveredOverlay.closeReason === 'tp' ? 'Take-profit hit' : 'Stop-loss hit'}
                      </span>
                      <span className="text-slate-500"> · closed by exchange-side bracket</span>
                    </div>
                  ) : null}

                  {hoveredOverlay.status === 'closed' && hoveredOverlay.postmortem?.status === 'succeeded' ? (
                    <div className="mt-2 space-y-0.5 rounded-lg bg-slate-50/80 p-2">
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                        Post-mortem
                        {hoveredOverlay.postmortem.verdict ? (
                          <span className="postmortem-chip ml-1.5 inline-flex rounded border px-1.5 py-0.5 normal-case">
                            {hoveredOverlay.postmortem.verdict.replace(/_/g, ' ')}
                          </span>
                        ) : null}
                      </div>
                      {hoveredOverlay.postmortem.lesson ? (
                        <div className="text-[11px] italic text-slate-700">
                          {hoveredOverlay.postmortem.lesson}
                        </div>
                      ) : null}
                    </div>
                  ) : null}

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
                          {formatDecisionActionLabel(hoveredOverlay.entryDecision)}
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
                        {typeof partial.pnlNet === 'number'
                          ? ` · ${partial.pnlNet >= 0 ? '+' : ''}${partial.pnlNet.toFixed(2)} realized`
                          : ''}
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
                          {formatDecisionActionLabel(hoveredOverlay.exitDecision)}
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
      {showSkeleton || (timelineLoading && !timelineDots.length) ? (
        <TimelineSkeleton rightInset={priceScaleWidth} />
      ) : hasChartData && timelineDots.length > 0 ? (
        // Decision timeline, time-aligned under the chart's own time axis.
        // Dots sit at their tick's bar position and stretch with zoom/pan;
        // wide zooms cull the least important overlapping dots.
        <div className="relative mt-1 h-6" aria-label="Decision timeline">
          <div
            className="timeline-connector absolute top-1/2 h-[2px] -translate-y-1/2"
            style={{ left: 0, right: priceScaleWidth }}
          />
          {/* Full-contrast overlay on the connector: decisions sharing one AI
              conversation (context calls while in position / managing a filled
              pullback limit) read as a linked chain. */}
          {threadSegments.map((segment, idx) => (
            <div
              key={`${segment.left}-${segment.width}-${idx}`}
              className="timeline-connector-thread absolute top-1/2 h-[2px] -translate-y-1/2"
              style={{ left: segment.left, width: segment.width }}
            />
          ))}
          {timelineDots.map(({ x, tick }) => {
            const isSelected = tick.ts === selectedTimelineTs;
            const isContextSkip =
              tick.kind !== 'action' &&
              tick.kind !== 'ai_call' &&
              threadSegments.some(
                (segment) => x >= segment.left && x <= segment.left + segment.width,
              );
            const label = timelineTickLabel(tick);
            return (
              <button
                key={tick.ts}
                type="button"
                onClick={() => onTimelineTickSelect?.(tick.ts)}
                title={label}
                aria-label={label}
                aria-pressed={isSelected}
                className="timeline-tick absolute top-1/2 flex h-6 w-6 -translate-x-1/2 -translate-y-1/2 items-center justify-center"
                style={{ left: x }}
              >
                <span
                  className={`timeline-dot ${tick.hourly ? 'h-3.5 w-3.5' : 'h-2 w-2'} rounded-full ${timelineDotFillClass(
                    tick,
                  )} ${isContextSkip ? 'timeline-dot-context-skip' : ''} ${
                    isSelected ? 'timeline-dot-selected' : ''
                  }`}
                />
              </button>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}
