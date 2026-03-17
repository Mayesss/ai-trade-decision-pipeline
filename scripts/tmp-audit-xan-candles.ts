import { scalpPrisma } from '../lib/scalp/pg/client';

const DAY_MS = 24 * 60 * 60_000;
const WEEK_MS = 7 * DAY_MS;

function weekStartMondayUtc(tsMs: number): number {
  const d = new Date(tsMs);
  const dayStart = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
  const day = new Date(dayStart).getUTCDay();
  const sinceMonday = (day + 6) % 7;
  return dayStart - sinceMonday * DAY_MS;
}

async function main() {
  const db = scalpPrisma();
  const rows = await db.$queryRawUnsafe<Array<{
    weekStart: Date;
    candles: unknown;
  }>>(`
    SELECT week_start AS "weekStart", candles_json AS "candles"
    FROM scalp_candle_history_weeks
    WHERE symbol = 'XANUSDT'
      AND timeframe = '1m'
      AND week_start >= TIMESTAMPTZ '2025-12-22T00:00:00Z'
      AND week_start <= TIMESTAMPTZ '2026-03-16T00:00:00Z'
    ORDER BY week_start ASC;
  `);

  const summary = rows.map((row) => {
    const candles = Array.isArray(row.candles) ? row.candles as unknown[] : [];
    let count = 0;
    let minLow = Number.POSITIVE_INFINITY;
    let maxHigh = Number.NEGATIVE_INFINITY;
    let firstClose: number | null = null;
    let lastClose: number | null = null;
    let maxBarAbsRetPct = 0;
    let maxBarRetPct = Number.NEGATIVE_INFINITY;
    let minBarRetPct = Number.POSITIVE_INFINITY;
    let prevClose: number | null = null;
    for (const c of candles) {
      if (!Array.isArray(c)) continue;
      const open = Number(c[1]);
      const high = Number(c[2]);
      const low = Number(c[3]);
      const close = Number(c[4]);
      if (![open, high, low, close].every((n) => Number.isFinite(n) && n > 0)) continue;
      count += 1;
      if (low < minLow) minLow = low;
      if (high > maxHigh) maxHigh = high;
      if (firstClose === null) firstClose = close;
      lastClose = close;
      if (prevClose !== null && prevClose > 0) {
        const retPct = ((close - prevClose) / prevClose) * 100;
        if (Math.abs(retPct) > maxBarAbsRetPct) maxBarAbsRetPct = Math.abs(retPct);
        if (retPct > maxBarRetPct) maxBarRetPct = retPct;
        if (retPct < minBarRetPct) minBarRetPct = retPct;
      }
      prevClose = close;
    }
    const weeklyRetPct = firstClose && lastClose ? ((lastClose - firstClose) / firstClose) * 100 : null;
    return {
      weekStartIso: row.weekStart?.toISOString?.() || String(row.weekStart),
      candles: count,
      minLow: Number.isFinite(minLow) ? minLow : null,
      maxHigh: Number.isFinite(maxHigh) ? maxHigh : null,
      rangePct: Number.isFinite(minLow) && Number.isFinite(maxHigh) && minLow > 0 ? ((maxHigh - minLow) / minLow) * 100 : null,
      firstClose,
      lastClose,
      weeklyRetPct,
      maxBarAbsRetPct,
      maxBarRetPct: Number.isFinite(maxBarRetPct) ? maxBarRetPct : null,
      minBarRetPct: Number.isFinite(minBarRetPct) ? minBarRetPct : null,
    };
  });

  console.log(JSON.stringify({ summary }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
