import { scalpPrisma } from '../lib/scalp/pg/client';
import { defaultScalpReplayConfig, runScalpReplay } from '../lib/scalp/replay/harness';
import { resolveScalpDeployment } from '../lib/scalp/deployments';
import { buildScalpReplayRuntimeFromDeployment } from '../lib/scalp/replay/runtimeConfig';
import { pipSizeForScalpSymbol } from '../lib/scalp/marketData';

function toReplayCandles(rows: Array<[number, number, number, number, number, number]>, spreadPips: number) {
  return rows.map((row) => ({
    ts: Number(row[0]),
    open: Number(row[1]),
    high: Number(row[2]),
    low: Number(row[3]),
    close: Number(row[4]),
    volume: Number(row[5] ?? 0),
    spreadPips,
  }));
}

async function runWindow(symbol: string, strategyId: string, tuneId: string, weekStartIso: string) {
  const db = scalpPrisma();
  const weekStart = new Date(weekStartIso);
  const rows = await db.$queryRawUnsafe<Array<{ candles: unknown }>>(`
    SELECT candles_json AS "candles"
    FROM scalp_candle_history_weeks
    WHERE symbol = '${symbol}'
      AND timeframe = '1m'
      AND week_start = TIMESTAMPTZ '${weekStart.toISOString()}';
  `);
  const raw = rows[0]?.candles;
  const candles = (Array.isArray(raw) ? raw : [])
    .filter((c): c is [number, number, number, number, number, number] => Array.isArray(c) && c.length >= 5)
    .map((c) => [
      Number(c[0]),
      Number(c[1]),
      Number(c[2]),
      Number(c[3]),
      Number(c[4]),
      Number(c[5] ?? 0),
    ] as [number, number, number, number, number, number])
    .filter((c) => [c[0], c[1], c[2], c[3], c[4]].every((n) => Number.isFinite(n) && n > 0))
    .sort((a, b) => a[0] - b[0]);

  const base = defaultScalpReplayConfig(symbol);
  const deployment = resolveScalpDeployment({ symbol, strategyId, tuneId });
  const runtime = buildScalpReplayRuntimeFromDeployment({ deployment, configOverride: null, baseRuntime: base });

  const replay = await runScalpReplay({
    candles: toReplayCandles(candles, runtime.defaultSpreadPips),
    pipSize: pipSizeForScalpSymbol(symbol, null),
    config: runtime,
    captureTimeline: false,
    symbolMeta: null,
  });

  const trades = replay.trades
    .map((t) => ({
      id: t.id,
      side: t.side,
      entryTs: new Date(t.entryTs).toISOString(),
      exitTs: new Date(t.exitTs).toISOString(),
      holdMinutes: t.holdMinutes,
      entryPrice: t.entryPrice,
      stopPrice: t.stopPrice,
      exitPrice: t.exitPrice,
      rMultiple: t.rMultiple,
      exitReason: t.exitReason,
    }))
    .sort((a, b) => b.rMultiple - a.rMultiple);

  return {
    weekStartIso: weekStart.toISOString(),
    candleCount: candles.length,
    summary: replay.summary,
    trades,
  };
}

async function main() {
  const symbol = 'XANUSDT';
  const strategyId = 'trend_day_reacceleration_m15_m3';
  const tuneId = 'default';

  const jan26 = await runWindow(symbol, strategyId, tuneId, '2026-01-26T00:00:00.000Z');
  const mar02 = await runWindow(symbol, strategyId, tuneId, '2026-03-02T00:00:00.000Z');

  console.log(JSON.stringify({ jan26, mar02 }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
