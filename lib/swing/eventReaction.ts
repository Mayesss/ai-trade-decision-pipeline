// Post-event reaction measurements for the swing prompt: WHAT did price do
// since a high-impact release? The event calendar (market.forex_events) says a
// release just happened; this module quantifies the reaction so the model can
// weigh the post-announcement state instead of guessing it from raw candles.
// Measured basis (Jun 2024–Jun 2026, 1m study over CPI/NFP/FOMC): the direction
// of the first ~45min reaction persisted over the following ~4h on gold/EUR
// (gold big-reaction: +29bp mean @ 65% by 4h), decayed by 24h — while the
// pre-release drift direction was a coin flip. Measurements only, no verdicts
// (no bias/strength fields) — the model does the reasoning. Pure functions,
// no I/O; tolerant of both venues' candle shapes ([ts,o,h,l,c,...] arrays or
// objects, ts in seconds or ms).

import type { ForexCompactEvent } from './forexEvents';

export type EventReactionMeasurement = {
  event_name: string;
  currency: string;
  impact: string;
  release_ts_utc: string;
  minutes_since_release: number;
  // Close-to-close move from the last bar that fully closed BEFORE the release
  // to the latest bar, in basis points. Positive = up since release.
  ret_since_release_bp: number | null;
  // (max high − min low) over all bars covering [release, now], vs the
  // pre-release anchor close, in basis points. Reaction size incl. whipsaw.
  range_since_release_bp: number | null;
  // How much of the post-release push has been given back: 0 = price sits at
  // the reaction extreme, 1 = fully retraced to the pre-release anchor, >1 =
  // overshot beyond it. Null when the reaction is too small to measure.
  retrace_pct: number | null;
};

type Bar = { ts: number; open: number; high: number; low: number; close: number };

const DEFAULT_BAR_MS = 15 * 60_000;
// Below this reaction size the retrace ratio is numerically meaningless.
const MIN_PUSH_BP = 2;

export function swingEventReactionEnabled(): boolean {
  const raw = String(process.env.SWING_EVENT_REACTION_ENABLED ?? '')
    .trim()
    .toLowerCase();
  if (['false', '0', 'no', 'off'].includes(raw)) return false;
  return true;
}

function normalizeBars(raw: unknown): Bar[] {
  return (Array.isArray(raw) ? raw : [])
    .map((c: any): Bar | null => {
      const tsRaw = Number(Array.isArray(c) ? c[0] : (c?.ts ?? c?.timestamp ?? c?.time));
      const open = Number(Array.isArray(c) ? c[1] : c?.open);
      const high = Number(Array.isArray(c) ? c[2] : c?.high);
      const low = Number(Array.isArray(c) ? c[3] : c?.low);
      const close = Number(Array.isArray(c) ? c[4] : c?.close);
      if (![tsRaw, open, high, low, close].every(Number.isFinite)) return null;
      const ts = tsRaw > 1e12 ? tsRaw : tsRaw * 1000;
      return { ts, open, high, low, close };
    })
    .filter((b): b is Bar => b !== null)
    .sort((a, b) => a.ts - b.ts);
}

// Bar duration from the median timestamp gap — candle rows carry the bar OPEN
// time, and "closed before the release" needs the close time.
function inferBarMs(bars: Bar[]): number {
  if (bars.length < 2) return DEFAULT_BAR_MS;
  const gaps = bars
    .slice(1)
    .map((b, i) => b.ts - bars[i].ts)
    .filter((gap) => gap > 0)
    .sort((a, b) => a - b);
  return gaps.length ? gaps[Math.floor(gaps.length / 2)] : DEFAULT_BAR_MS;
}

function round1(x: number): number {
  return Number(x.toFixed(1));
}

export function measureEventReaction(params: {
  event: ForexCompactEvent;
  bars: Bar[];
  barMs: number;
  nowMs: number;
}): EventReactionMeasurement | null {
  const { event, bars, barMs, nowMs } = params;
  const releaseMs = Date.parse(event.timestamp_utc);
  if (!Number.isFinite(releaseMs) || releaseMs > nowMs) return null;

  // Anchor: last bar that fully closed before the release. The bar containing
  // the release already includes the reaction, so its close would contaminate
  // the baseline.
  const anchor = [...bars].reverse().find((b) => b.ts + barMs <= releaseMs);
  const last = bars[bars.length - 1];
  if (!anchor || !last || anchor.close <= 0 || last.ts < releaseMs - barMs) return null;

  const retBp = (last.close / anchor.close - 1) * 1e4;

  // Bars covering [release, now]: everything that was open at or after release.
  const post = bars.filter((b) => b.ts + barMs > releaseMs);
  let rangeBp: number | null = null;
  let retracePct: number | null = null;
  if (post.length) {
    const maxHigh = Math.max(...post.map((b) => b.high));
    const minLow = Math.min(...post.map((b) => b.low));
    rangeBp = round1(((maxHigh - minLow) / anchor.close) * 1e4);

    // Reaction push = the larger excursion from the anchor; retrace = how far
    // back from that extreme price has come, as a fraction of the push.
    const upPush = maxHigh - anchor.close;
    const downPush = anchor.close - minLow;
    const pushAbs = Math.max(upPush, downPush);
    if ((pushAbs / anchor.close) * 1e4 >= MIN_PUSH_BP) {
      const extreme = upPush >= downPush ? maxHigh : minLow;
      retracePct = Number((Math.abs(extreme - last.close) / pushAbs).toFixed(2));
    }
  }

  return {
    event_name: event.event_name,
    currency: event.currency,
    impact: event.impact,
    release_ts_utc: event.timestamp_utc,
    minutes_since_release: Math.round((nowMs - releaseMs) / 60_000),
    ret_since_release_bp: round1(retBp),
    range_since_release_bp: rangeBp,
    retrace_pct: retracePct,
  };
}

// Entry point for /api/analyze: measurements for every recent event the
// candles can cover (nano 15m bundle spans ~27h — always covers the 3h
// lookback). Returns null when there is nothing to report so the prompt
// block stays absent instead of empty.
export function buildEventReactionContext(params: {
  recentEvents: ForexCompactEvent[] | null | undefined;
  candles: unknown;
  nowMs?: number;
}): EventReactionMeasurement[] | null {
  const events = Array.isArray(params.recentEvents) ? params.recentEvents : [];
  if (!events.length) return null;
  const bars = normalizeBars(params.candles);
  if (bars.length < 3) return null;
  const barMs = inferBarMs(bars);
  const nowMs = Number.isFinite(params.nowMs as number) ? Number(params.nowMs) : Date.now();

  const measurements = events
    .slice(0, 2)
    .map((event) => measureEventReaction({ event, bars, barMs, nowMs }))
    .filter((m): m is EventReactionMeasurement => m !== null);
  return measurements.length ? measurements : null;
}
