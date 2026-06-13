/**
 * Timeframe probe — tests the hypothesis that stepping the scalp band up from
 * M15 to M30/H1 (with a correspondingly longer lookback) improves NET R/trade
 * because the fixed venue fee becomes a smaller fraction of a larger 1R.
 *
 * For each (strategy x timeframe-pair) it runs the real replay harness over N
 * weeks of stored 1m candles and reports, pooled across symbols:
 *   trades, gross R/trade, net R/trade, fee R/trade, fee drag %, net R/week,
 *   win%, lowerBoundR (mean - 1.64*stderr, matching v5 evidence), and the
 *   t-stat (sqrt(n)*mean/sd) as a significance proxy.
 *
 * Decisive read: does net R/trade rise and fee-drag% fall as the timeframe
 * climbs, while retaining enough trades for significance?
 *
 * Usage:
 *   node scripts/with-db-env.mjs node --import tsx scripts/scalp-tf-probe.ts \
 *     [--symbols BTCUSDT,ETHUSDT,SOLUSDT] [--weeks 26] \
 *     [--strategies regime_pullback_m15_m3,compression_breakout_pullback_m15_m3] \
 *     [--tfs m15_m3,m30_m5,h1_m15]
 */
import { loadScalpCandleHistoryRange } from '../lib/scalp/candleHistory';
import { defaultScalpReplayConfig, runScalpReplay } from '../lib/scalp/replay/harness';
import type { ScalpReplayCandle } from '../lib/scalp/replay/types';
import type { ScalpBaseTimeframe, ScalpConfirmTimeframe, ScalpCandle } from '../lib/scalp/types';
import { pipSizeForScalpSymbol } from '../lib/scalp/symbolInfo';
import { isScalpPgConfigured } from '../lib/scalp/pg/client';
import { getScalpStrategyById } from '../lib/scalp/strategies/registry';

// Strategies hard-gate entries on market.baseTf === requiredBaseTf, so each
// timeframe must use the TF-matched registered variant (e.g.
// regime_pullback_h1_m15), not the M15 singleton fed higher-TF candles.
function resolveVariantId(baseStrategyId: string, tfLabel: string): string {
  return baseStrategyId.replace(/_[mh]\d+_[mh]\d+$/, `_${tfLabel}`);
}

type TfPair = { label: string; baseTf: ScalpBaseTimeframe; confirmTf: ScalpConfirmTimeframe };

const TF_PAIRS: Record<string, TfPair> = {
  m15_m3: { label: 'm15_m3', baseTf: 'M15', confirmTf: 'M3' },
  m30_m5: { label: 'm30_m5', baseTf: 'M30', confirmTf: 'M5' },
  h1_m15: { label: 'h1_m15', baseTf: 'H1', confirmTf: 'M15' },
};

const TF_MINUTES: Record<string, number> = { M1: 1, M3: 3, M5: 5, M15: 15, M30: 30, H1: 60 };

function arg(name: string, fallback: string): string {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? String(process.argv[i + 1]) : fallback;
}

const SYMBOLS = arg('symbols', 'BTCUSDT,ETHUSDT,SOLUSDT').split(',').map((s) => s.trim().toUpperCase()).filter(Boolean);
const WEEKS = Math.max(4, Math.floor(Number(arg('weeks', '26'))));
const STRATEGIES = arg(
  'strategies',
  'regime_pullback_m15_m3,compression_breakout_pullback_m15_m3',
).split(',').map((s) => s.trim()).filter(Boolean);
const TFS = arg('tfs', 'm15_m3,m30_m5,h1_m15').split(',').map((s) => s.trim()).filter((t) => TF_PAIRS[t]);
const NOW = Number(arg('nowMs', String(Date.UTC(2026, 5, 1)))); // default anchor: 2026-06-01 (last full data week)

function toReplayCandles(rows: ScalpCandle[]): ScalpReplayCandle[] {
  const out: ScalpReplayCandle[] = [];
  for (const r of rows) {
    const [ts, open, high, low, close, volume] = r;
    if (![ts, open, high, low, close].every((v) => Number.isFinite(v))) continue;
    out.push({ ts, open, high, low, close, volume: Number(volume) || 0, spreadPips: NaN });
  }
  return out.sort((a, b) => a.ts - b.ts);
}

function stats(xs: number[]) {
  const n = xs.length;
  if (!n) return { n: 0, mean: 0, sd: 0, stderr: 0 };
  const mean = xs.reduce((a, b) => a + b, 0) / n;
  const variance = n > 1 ? xs.reduce((a, b) => a + (b - mean) * (b - mean), 0) / (n - 1) : 0;
  const sd = Math.sqrt(variance);
  return { n, mean, sd, stderr: n > 0 ? sd / Math.sqrt(n) : 0 };
}

type Cell = {
  strategy: string;
  tf: string;
  netR: number[];
  grossR: number[];
  feeR: number[];
  symbols: Set<string>;
  // entry timestamps per symbol, for stage-window viability counting
  entryTsBySymbol: Map<string, number[]>;
};

// Stage base windows (legacy M15 calibration) + the trade-count bars each must
// clear. Windows scale by k = baseTfMinutes/15; thresholds stay fixed.
const STAGE_DEFS = [
  { id: 'A', baseWeeks: 4, minTrades: 4 },
  { id: 'B', baseWeeks: 6, minTrades: 14 },
  { id: 'C', baseWeeks: 12, minTrades: 24 },
  { id: 'promote', baseWeeks: 12, minTrades: 40 },
] as const;

async function loadSymbol1m(symbol: string, fromMs: number, toMs: number): Promise<ScalpReplayCandle[]> {
  const res = await loadScalpCandleHistoryRange(symbol, '1m', fromMs, toMs, {
    readOrder: ['pg'],
    requireCoverageRatio: 0,
  });
  return toReplayCandles(res.record?.candles || []);
}

async function main() {
  if (!isScalpPgConfigured()) {
    console.error('PG not configured');
    process.exit(1);
  }
  const toMs = NOW;
  const fromMs = toMs - WEEKS * 7 * 24 * 60 * 60 * 1000;
  console.log(
    `TF probe | weeks=${WEEKS} | window=${new Date(fromMs).toISOString().slice(0, 10)}..${new Date(toMs).toISOString().slice(0, 10)}`,
  );
  console.log(`symbols=${SYMBOLS.join(',')}`);
  console.log(`strategies=${STRATEGIES.join(',')}`);
  console.log(`tfs=${TFS.join(',')}\n`);

  // Preload candles per symbol once (expensive).
  const candlesBySymbol = new Map<string, ScalpReplayCandle[]>();
  for (const symbol of SYMBOLS) {
    const c = await loadSymbol1m(symbol, fromMs, toMs);
    candlesBySymbol.set(symbol, c);
    console.log(`  loaded ${symbol}: ${c.length} 1m candles`);
  }
  console.log('');

  const cells = new Map<string, Cell>();
  const keyOf = (strategy: string, tf: string) => `${strategy}::${tf}`;

  let run = 0;
  const totalRuns = STRATEGIES.length * TFS.length * SYMBOLS.length;
  for (const strategy of STRATEGIES) {
    for (const tf of TFS) {
      const pair = TF_PAIRS[tf]!;
      const k = TF_MINUTES[pair.baseTf]! / 15; // scale time-denominated windows relative to M15 baseline
      const cell: Cell = { strategy, tf, netR: [], grossR: [], feeR: [], symbols: new Set(), entryTsBySymbol: new Map() };
      cells.set(keyOf(strategy, tf), cell);
      for (const symbol of SYMBOLS) {
        run += 1;
        const candles = candlesBySymbol.get(symbol) || [];
        if (candles.length < 500) {
          console.log(`  [${run}/${totalRuns}] skip ${symbol} ${strategy} ${tf} (insufficient candles)`);
          continue;
        }
        const variantId = resolveVariantId(strategy, tf);
        if (!getScalpStrategyById(variantId)) {
          console.log(`  [${run}/${totalRuns}] skip ${symbol} ${strategy} ${tf}: no registered variant ${variantId}`);
          continue;
        }
        const cfg = defaultScalpReplayConfig(symbol);
        cfg.strategyId = variantId;
        cfg.tuneId = `tfprobe_${tf}`;
        cfg.deploymentId = `tfprobe_${symbol}_${strategy}_${tf}`;
        cfg.strategy.asiaBaseTf = pair.baseTf;
        cfg.strategy.confirmTf = pair.confirmTf;
        // Scale minute-denominated confirmation/entry windows so they remain a
        // comparable number of *base bars* across timeframes (else at H1 a
        // 45-min confirm TTL is < 1 bar and nothing ever triggers).
        cfg.strategy.confirmTtlMinutes = Math.round(cfg.strategy.confirmTtlMinutes * k);
        cfg.strategy.ifvgTtlMinutes = Math.round(cfg.strategy.ifvgTtlMinutes * k);
        let result;
        try {
          result = await runScalpReplay({ candles, pipSize: pipSizeForScalpSymbol(symbol), config: cfg });
        } catch (e) {
          console.log(`  [${run}/${totalRuns}] ERR ${symbol} ${strategy} ${tf}: ${(e as Error).message}`);
          continue;
        }
        const entryTs: number[] = [];
        for (const t of result.trades) {
          const net = Number(t.rMultiple);
          if (!Number.isFinite(net)) continue;
          cell.netR.push(net);
          cell.grossR.push(Number.isFinite(Number(t.grossRMultiple)) ? Number(t.grossRMultiple) : net);
          cell.feeR.push(Number.isFinite(Number(t.feeR)) ? Number(t.feeR) : 0);
          if (Number.isFinite(Number(t.entryTs))) entryTs.push(Number(t.entryTs));
        }
        cell.entryTsBySymbol.set(symbol, entryTs);
        if (result.trades.length) cell.symbols.add(symbol);
        console.log(
          `  [${run}/${totalRuns}] ${symbol} ${strategy} ${tf}: ${result.trades.length} trades, netR=${result.summary.netR.toFixed(2)}`,
        );
      }
    }
  }

  // Report
  console.log('\n================ POOLED RESULTS (across symbols) ================\n');
  const header = [
    'strategy'.padEnd(34),
    'tf'.padEnd(8),
    'trades'.padStart(7),
    'netR/t'.padStart(8),
    'grossR/t'.padStart(9),
    'feeR/t'.padStart(8),
    'feeDrag%'.padStart(9),
    'netR/wk'.padStart(8),
    'win%'.padStart(6),
    'lowBndR'.padStart(8),
    'tstat'.padStart(7),
  ].join(' ');
  console.log(header);
  console.log('-'.repeat(header.length));
  for (const strategy of STRATEGIES) {
    for (const tf of TFS) {
      const cell = cells.get(keyOf(strategy, tf));
      if (!cell) continue;
      const s = stats(cell.netR);
      const gross = stats(cell.grossR).mean;
      const fee = stats(cell.feeR).mean;
      const wins = cell.netR.filter((r) => r > 0).length;
      const winPct = s.n ? (100 * wins) / s.n : 0;
      const netPerWk = (cell.netR.reduce((a, b) => a + b, 0)) / WEEKS;
      const lowBnd = s.mean - 1.64 * s.stderr;
      const tstat = s.stderr > 0 ? s.mean / s.stderr : 0;
      const feeDragPct = Math.abs(gross) > 1e-9 ? (100 * fee) / Math.abs(gross) : 0;
      console.log(
        [
          strategy.padEnd(34),
          tf.padEnd(8),
          String(s.n).padStart(7),
          s.mean.toFixed(3).padStart(8),
          gross.toFixed(3).padStart(9),
          fee.toFixed(3).padStart(8),
          feeDragPct.toFixed(1).padStart(9),
          netPerWk.toFixed(2).padStart(8),
          winPct.toFixed(1).padStart(6),
          lowBnd.toFixed(3).padStart(8),
          tstat.toFixed(2).padStart(7),
        ].join(' '),
      );
    }
    console.log('-'.repeat(header.length));
  }
  console.log('\nReads: netR/t up + feeDrag% down as tf climbs => fee-domination hypothesis holds.');
  console.log('lowBndR>0 and |tstat|>~2 => the edge survives sampling noise at that tf.');

  // ---- Stage-window viability (per symbol) ----
  // The composer gates PER SYMBOL per candidate. A stage is viable only if the
  // candidate accumulates >= minTrades inside its (TF-scaled) calendar window.
  // We count entries in the trailing baseWeeks*k window ending at the anchor.
  console.log('\n================ STAGE VIABILITY (per symbol, TF-scaled windows) ================\n');
  console.log('Trades inside each stage window; (PASS/FAIL vs minTrades). k = baseTfMinutes/15.\n');
  const sHeader = [
    'strategy'.padEnd(34),
    'tf'.padEnd(8),
    'symbol'.padEnd(9),
    'k'.padStart(2),
    `A(${STAGE_DEFS[0].minTrades})`.padStart(9),
    `B(${STAGE_DEFS[1].minTrades})`.padStart(10),
    `C(${STAGE_DEFS[2].minTrades})`.padStart(10),
    `promo(${STAGE_DEFS[3].minTrades})`.padStart(12),
  ].join(' ');
  console.log(sHeader);
  console.log('-'.repeat(sHeader.length));
  for (const strategy of STRATEGIES) {
    for (const tf of TFS) {
      const cell = cells.get(keyOf(strategy, tf));
      if (!cell) continue;
      const pair = TF_PAIRS[tf]!;
      const k = TF_MINUTES[pair.baseTf]! / 15;
      for (const symbol of SYMBOLS) {
        const ts = cell.entryTsBySymbol.get(symbol);
        if (!ts) continue;
        const cellTxt = STAGE_DEFS.map((st) => {
          const fromTs = toMs - st.baseWeeks * k * 7 * 24 * 60 * 60 * 1000;
          const cnt = ts.filter((t) => t >= fromTs && t <= toMs).length;
          const wk = st.baseWeeks * k;
          return `${cnt}/${wk}w ${cnt >= st.minTrades ? 'P' : 'F'}`;
        });
        console.log(
          [
            strategy.padEnd(34),
            tf.padEnd(8),
            symbol.padEnd(9),
            String(k).padStart(2),
            cellTxt[0]!.padStart(9),
            cellTxt[1]!.padStart(10),
            cellTxt[2]!.padStart(10),
            cellTxt[3]!.padStart(12),
          ].join(' '),
        );
      }
    }
    console.log('-'.repeat(sHeader.length));
  }
  console.log('\nViable funnel = stage C reaches 24 trades and promotion reaches 40 in their windows.');
  console.log('If H1 per-symbol counts fall short even at 52wk, pool symbols or relax the H1 trade bars.');
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
