import { loadScalpCandleHistoryInRange } from "../scalp/candleHistory";
import { pipSizeForScalpSymbol } from "../scalp/marketData";
import { loadScalpSymbolMarketMetadataBulk } from "../scalp/symbolMarketMetadataStore";
import type { ScalpSymbolMarketMetadata } from "../scalp/symbolMarketMetadata";
import {
  defaultScalpReplayConfig,
  runScalpReplay,
} from "../scalp/replay/harness";
import type {
  ScalpReplayCandle,
  ScalpReplayTrade,
} from "../scalp/replay/types";

import {
  parseEntryTriggerFromTuneId,
  parseExitRuleFromTuneId,
  parseRegimeGateFromTuneId,
  parseRiskRuleFromTuneId,
  parseStateMachineFromTuneId,
} from "./composerExecution";
import { upsertScalpV2WorkerStageWeeklyCache, paginateScalpV2Candidates } from "./db";
import {
  resolveEntryTriggerOverrides,
  type EntryTriggerBlockId,
} from "./entryTriggerPresets";
import {
  resolveExitRuleOverrides,
  type ExitRuleBlockId,
} from "./exitRulePresets";
import {
  resolveRiskRuleReplayOverrides,
  type RiskRuleBlockId,
} from "./riskRulePresets";
import {
  resolveStateMachineReplayOverrides,
  type StateMachineBlockId,
} from "./stateMachinePresets";
import { inferScalpV2AssetCategory, minSpreadPipsForCategory } from "./symbolInfo";
import type {
  ScalpV2CandidateStatus,
  ScalpV2Session,
  ScalpV2Venue,
  ScalpV2WorkerStageId,
  ScalpV2WorkerStageWeeklyMetrics,
} from "./types";
import { resolveScalpV2CompletedWeekWindowToUtc } from "./weekWindows";

const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;
const PAGE_SIZE = 500;

export type ScalpV2WorkerWeeklyCacheBackfillOptions = {
  dryRun: boolean;
  verbose: boolean;
  limit: number;
  offset: number;
  venue: ScalpV2Venue | null;
  session: ScalpV2Session | null;
  statuses: Set<ScalpV2CandidateStatus>;
  symbolFilter: Set<string>;
  windowToTs: number;
  stageAWeeks: number;
  stageBWeeks: number;
  stageCWeeks: number;
  minCandles: number;
  upsertBatchSize: number;
  cacheVersion: string;
};

export type ScalpV2WorkerWeeklyCacheBackfillStats = {
  scannedCandidates: number;
  matchedCandidates: number;
  selectedCandidates: number;
  skippedNoResearchDsl: number;
  skippedNoCandles: number;
  skippedStageCandles: number;
  replayRuns: number;
  stageRowsPrepared: number;
  stageRowsUpserted: number;
};

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function toPositiveInt(value: unknown, fallback: number, max = 1_000_000): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.min(max, n));
}

function normalizeStatuses(
  raw: Iterable<string> | string | undefined,
): Set<ScalpV2CandidateStatus> {
  const allowed: ScalpV2CandidateStatus[] = [
    "discovered",
    "evaluated",
    "promoted",
    "rejected",
    "shadow",
  ];
  const out = new Set<ScalpV2CandidateStatus>();
  const values =
    typeof raw === "string"
      ? raw.split(",")
      : raw
        ? Array.from(raw)
        : [];
  for (const entry of values) {
    const normalized = String(entry || "").trim().toLowerCase();
    if (allowed.includes(normalized as ScalpV2CandidateStatus)) {
      out.add(normalized as ScalpV2CandidateStatus);
    }
  }
  if (!out.size) {
    out.add("evaluated");
    out.add("promoted");
    out.add("shadow");
  }
  return out;
}

function normalizeSymbols(raw: Iterable<string> | string | undefined): Set<string> {
  const out = new Set<string>();
  const values =
    typeof raw === "string"
      ? raw.split(",")
      : raw
        ? Array.from(raw)
        : [];
  for (const entry of values) {
    const symbol = String(entry || "").trim().toUpperCase();
    if (symbol) out.add(symbol);
  }
  return out;
}

export function resolveScalpV2WorkerWeeklyCacheBackfillOptions(
  input: Partial<
    Omit<ScalpV2WorkerWeeklyCacheBackfillOptions, "statuses" | "symbolFilter"> & {
      statuses: Iterable<string> | string;
      symbols: Iterable<string> | string;
    }
  > = {},
): ScalpV2WorkerWeeklyCacheBackfillOptions {
  return {
    dryRun: input.dryRun ?? true,
    verbose: input.verbose ?? false,
    limit: toPositiveInt(
      input.limit ?? process.env.SCALP_V2_BACKFILL_CACHE_LIMIT,
      500,
      200_000,
    ),
    offset: Math.max(0, Math.floor(Number(input.offset ?? 0) || 0)),
    venue:
      input.venue === "bitget" || input.venue === "capital"
        ? input.venue
        : null,
    session:
      input.session === "tokyo" ||
      input.session === "berlin" ||
      input.session === "newyork" ||
      input.session === "pacific" ||
      input.session === "sydney"
        ? input.session
        : null,
    statuses: normalizeStatuses(
      input.statuses ?? process.env.SCALP_V2_BACKFILL_CACHE_STATUSES,
    ),
    symbolFilter: normalizeSymbols(
      input.symbols ?? process.env.SCALP_V2_BACKFILL_CACHE_SYMBOLS,
    ),
    windowToTs: Math.floor(
      Number(input.windowToTs) || resolveScalpV2CompletedWeekWindowToUtc(Date.now()),
    ),
    stageAWeeks: toPositiveInt(input.stageAWeeks, 4, 24),
    stageBWeeks: toPositiveInt(input.stageBWeeks, 6, 24),
    stageCWeeks: toPositiveInt(input.stageCWeeks, 12, 52),
    minCandles: Math.max(
      120,
      toPositiveInt(
        input.minCandles ?? process.env.SCALP_V2_WORKER_MIN_CANDLES,
        8_000,
        2_000_000,
      ),
    ),
    upsertBatchSize: toPositiveInt(input.upsertBatchSize, 1200, 20_000),
    cacheVersion:
      String(
        input.cacheVersion ||
          process.env.SCALP_V2_WORKER_WEEKLY_CACHE_VERSION ||
          "v1",
      ).trim() || "v1",
  };
}

function toReplayCandlesFromHistory(
  candles: Array<[number, number, number, number, number, number]>,
  spreadPips: number,
): ScalpReplayCandle[] {
  const out: ScalpReplayCandle[] = [];
  for (const row of candles || []) {
    const ts = Math.floor(Number(row?.[0] || 0));
    const open = Number(row?.[1] || 0);
    const high = Number(row?.[2] || 0);
    const low = Number(row?.[3] || 0);
    const close = Number(row?.[4] || 0);
    const volume = Number(row?.[5] || 0);
    if (
      !Number.isFinite(ts) ||
      ![open, high, low, close].every((value) => Number.isFinite(value) && value > 0)
    ) {
      continue;
    }
    out.push({
      ts,
      open,
      high,
      low,
      close,
      volume: Number.isFinite(volume) ? volume : 0,
      spreadPips,
    });
  }
  return out.sort((a, b) => a.ts - b.ts);
}

function filterSundayReplayCandles(candles: ScalpReplayCandle[]): ScalpReplayCandle[] {
  return (candles || []).filter((row) => new Date(row.ts).getUTCDay() !== 0);
}

function listWeekStarts(params: { fromTs: number; toTs: number }): number[] {
  const fromTs = Math.floor(Number(params.fromTs) || 0);
  const toTs = Math.floor(Number(params.toTs) || 0);
  if (!Number.isFinite(fromTs) || !Number.isFinite(toTs) || toTs <= fromTs) return [];
  const out: number[] = [];
  for (let weekStart = fromTs; weekStart < toTs; weekStart += ONE_WEEK_MS) {
    out.push(weekStart);
  }
  return out;
}

function buildWorkerStageWeeklyMetrics(params: {
  trades: ScalpReplayTrade[];
  weekStartTs: number;
  weekToTs: number;
}): ScalpV2WorkerStageWeeklyMetrics {
  const sortedTrades = [...(params.trades || [])].sort(
    (a, b) => Math.floor(Number(a.exitTs || 0)) - Math.floor(Number(b.exitTs || 0)),
  );
  let trades = 0;
  let wins = 0;
  let netR = 0;
  let grossProfitR = 0;
  let grossLossR = 0;
  let equity = 0;
  let peak = 0;
  let minPrefix = 0;
  let maxDrawdownR = 0;
  let largestTradeR = 0;
  let exitStop = 0;
  let exitTp = 0;
  let exitTimeStop = 0;
  let exitForceClose = 0;

  for (const trade of sortedTrades) {
    const ts = Math.floor(Number(trade.exitTs || 0));
    if (!Number.isFinite(ts) || ts < params.weekStartTs || ts >= params.weekToTs) continue;
    const r = Number(trade.rMultiple || 0);
    if (!Number.isFinite(r)) continue;
    trades += 1;
    if (r > 0) wins += 1;
    netR += r;
    if (r > 0) grossProfitR += r;
    if (r < 0) grossLossR += r;
    equity += r;
    peak = Math.max(peak, equity);
    minPrefix = Math.min(minPrefix, equity);
    maxDrawdownR = Math.max(maxDrawdownR, Math.max(0, peak - equity));
    largestTradeR = Math.max(largestTradeR, Math.abs(r));
    if (
      trade.exitReason === "STOP" ||
      trade.exitReason === "STOP_LOSS" ||
      trade.exitReason === "STOP_BE" ||
      trade.exitReason === "STOP_TRAIL"
    ) {
      exitStop += 1;
    } else if (trade.exitReason === "TP") {
      exitTp += 1;
    } else if (trade.exitReason === "TIME_STOP") {
      exitTimeStop += 1;
    } else if (trade.exitReason === "FORCE_CLOSE") {
      exitForceClose += 1;
    }
  }

  return {
    trades,
    wins,
    netR,
    grossProfitR,
    grossLossR,
    maxDrawdownR,
    maxPrefixR: peak,
    minPrefixR: minPrefix,
    largestTradeR,
    exitStop,
    exitTp,
    exitTimeStop,
    exitForceClose,
  };
}

function buildWorkerStageWeeklyMetricsMap(params: {
  trades: ScalpReplayTrade[];
  fromTs: number;
  toTs: number;
}): Map<number, ScalpV2WorkerStageWeeklyMetrics> {
  const out = new Map<number, ScalpV2WorkerStageWeeklyMetrics>();
  for (const weekStart of listWeekStarts({ fromTs: params.fromTs, toTs: params.toTs })) {
    out.set(
      weekStart,
      buildWorkerStageWeeklyMetrics({
        trades: params.trades,
        weekStartTs: weekStart,
        weekToTs: weekStart + ONE_WEEK_MS,
      }),
    );
  }
  return out;
}

function toStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((row) => String(row || "").trim())
    .filter(Boolean);
}

function normalizeCandidateBlocks(params: {
  metadata: Record<string, unknown>;
  tuneId: string;
}): {
  exitRule: ExitRuleBlockId[];
  entryTrigger: EntryTriggerBlockId[];
  riskRule: RiskRuleBlockId[];
  stateMachine: StateMachineBlockId[];
} {
  const dsl = asRecord(params.metadata.researchDsl);
  const exitRule = toStringList(dsl.exit_rule) as ExitRuleBlockId[];
  const entryTrigger = toStringList(dsl.entry_trigger) as EntryTriggerBlockId[];
  const riskRule = toStringList(dsl.risk_rule) as RiskRuleBlockId[];
  const stateMachine = toStringList(dsl.state_machine) as StateMachineBlockId[];

  if (!exitRule.length) {
    const parsed = parseExitRuleFromTuneId(params.tuneId);
    if (parsed) exitRule.push(parsed);
  }
  if (!entryTrigger.length) {
    const parsed = parseEntryTriggerFromTuneId(params.tuneId);
    if (parsed) entryTrigger.push(parsed);
  }
  if (!riskRule.length) {
    const parsed = parseRiskRuleFromTuneId(params.tuneId);
    if (parsed) riskRule.push(parsed);
  }
  if (!stateMachine.length) {
    const parsed = parseStateMachineFromTuneId(params.tuneId);
    if (parsed) stateMachine.push(parsed);
  }

  parseRegimeGateFromTuneId(params.tuneId);
  return { exitRule, entryTrigger, riskRule, stateMachine };
}

async function loadMatchingCandidates(
  opts: ScalpV2WorkerWeeklyCacheBackfillOptions,
): Promise<{
  totalScanned: number;
  totalMatched: number;
  selected: Awaited<ReturnType<typeof paginateScalpV2Candidates>>["rows"];
}> {
  const selected: Awaited<ReturnType<typeof paginateScalpV2Candidates>>["rows"] = [];
  let totalMatched = 0;
  let totalScanned = 0;
  let pageOffset = 0;

  while (true) {
    const page = await paginateScalpV2Candidates({
      limit: PAGE_SIZE,
      offset: pageOffset,
      venue: opts.venue || undefined,
      session: opts.session || undefined,
    });

    if (!page.rows.length) break;
    totalScanned += page.rows.length;

    for (const candidate of page.rows) {
      if (!opts.statuses.has(candidate.status)) continue;
      if (opts.symbolFilter.size && !opts.symbolFilter.has(candidate.symbol)) continue;
      totalMatched += 1;
      if (totalMatched <= opts.offset) continue;
      if (selected.length < opts.limit) selected.push(candidate);
    }

    if (page.rows.length < PAGE_SIZE) break;
    pageOffset += PAGE_SIZE;
  }

  return { totalScanned, totalMatched, selected };
}

export async function runScalpV2WorkerWeeklyCacheBackfill(
  opts: ScalpV2WorkerWeeklyCacheBackfillOptions,
): Promise<ScalpV2WorkerWeeklyCacheBackfillStats> {
  const stats: ScalpV2WorkerWeeklyCacheBackfillStats = {
    scannedCandidates: 0,
    matchedCandidates: 0,
    selectedCandidates: 0,
    skippedNoResearchDsl: 0,
    skippedNoCandles: 0,
    skippedStageCandles: 0,
    replayRuns: 0,
    stageRowsPrepared: 0,
    stageRowsUpserted: 0,
  };

  const stagePolicies: Array<{ id: ScalpV2WorkerStageId; weeks: number }> = [
    { id: "a", weeks: opts.stageAWeeks },
    { id: "b", weeks: opts.stageBWeeks },
    { id: "c", weeks: opts.stageCWeeks },
  ];

  const loadResult = await loadMatchingCandidates(opts);
  stats.scannedCandidates = loadResult.totalScanned;
  stats.matchedCandidates = loadResult.totalMatched;
  stats.selectedCandidates = loadResult.selected.length;

  if (!loadResult.selected.length) return stats;

  const minWindowFromTs = opts.windowToTs - opts.stageCWeeks * ONE_WEEK_MS;
  const uniqueSymbols = Array.from(new Set(loadResult.selected.map((row) => row.symbol)));
  const symbolMetadataMap = await loadScalpSymbolMarketMetadataBulk(uniqueSymbols).catch(
    () => new Map<string, ScalpSymbolMarketMetadata | null>(),
  );
  const symbolCandleCache = new Map<string, ScalpReplayCandle[]>();

  let pendingRows: Parameters<typeof upsertScalpV2WorkerStageWeeklyCache>[0]["rows"] = [];

  const flush = async () => {
    if (!pendingRows.length || opts.dryRun) {
      pendingRows = [];
      return;
    }
    const upserted = await upsertScalpV2WorkerStageWeeklyCache({ rows: pendingRows });
    stats.stageRowsUpserted += upserted;
    pendingRows = [];
  };

  for (let idx = 0; idx < loadResult.selected.length; idx += 1) {
    const candidate = loadResult.selected[idx]!;
    const candidateMeta = asRecord(candidate.metadata);
    const hasDsl = Object.keys(asRecord(candidateMeta.researchDsl)).length > 0;
    if (!hasDsl) {
      stats.skippedNoResearchDsl += 1;
      continue;
    }

    let symbolCandles = symbolCandleCache.get(candidate.symbol) || null;
    if (!symbolCandles) {
      const history = await loadScalpCandleHistoryInRange(
        candidate.symbol,
        "1m",
        minWindowFromTs,
        opts.windowToTs,
      );
      const meta = symbolMetadataMap.get(candidate.symbol) ?? null;
      const symbolPipSize = pipSizeForScalpSymbol(candidate.symbol, meta ?? undefined);
      const category = inferScalpV2AssetCategory(candidate.symbol);
      const categoryFloor = minSpreadPipsForCategory(category);
      const replayBase = defaultScalpReplayConfig(candidate.symbol);
      const tickSpreadPips = meta?.tickSize ? meta.tickSize / symbolPipSize : 0;
      const spreadPips = Math.max(replayBase.defaultSpreadPips, categoryFloor, tickSpreadPips);
      symbolCandles = filterSundayReplayCandles(
        toReplayCandlesFromHistory(
          (history.record?.candles || []) as Array<
            [number, number, number, number, number, number]
          >,
          spreadPips,
        ),
      );
      symbolCandleCache.set(candidate.symbol, symbolCandles);
    }

    if (symbolCandles.length < opts.minCandles) {
      stats.skippedNoCandles += 1;
      continue;
    }

    const blocks = normalizeCandidateBlocks({
      metadata: candidateMeta,
      tuneId: candidate.tuneId,
    });

    const replayBaseConfig = defaultScalpReplayConfig(candidate.symbol);
    const replayConfig = {
      ...replayBaseConfig,
      symbol: candidate.symbol,
      strategyId: candidate.strategyId,
      tuneId: candidate.tuneId,
      tuneLabel: candidate.tuneId,
      strategy: {
        ...replayBaseConfig.strategy,
        entrySessionProfile: candidate.entrySessionProfile,
        ...resolveExitRuleOverrides(blocks.exitRule),
        ...resolveEntryTriggerOverrides(blocks.entryTrigger),
        ...resolveRiskRuleReplayOverrides(blocks.riskRule),
        ...resolveStateMachineReplayOverrides(blocks.stateMachine),
      },
    };
    const meta = symbolMetadataMap.get(candidate.symbol) ?? null;
    const pipSize = pipSizeForScalpSymbol(candidate.symbol, meta ?? undefined);

    for (const stage of stagePolicies) {
      const fromTs = opts.windowToTs - stage.weeks * ONE_WEEK_MS;
      const stageCandles = symbolCandles.filter(
        (row) => row.ts >= fromTs && row.ts < opts.windowToTs,
      );
      if (stageCandles.length < opts.minCandles) {
        stats.skippedStageCandles += 1;
        continue;
      }

      const replay = await runScalpReplay({
        candles: stageCandles,
        pipSize,
        config: replayConfig,
        captureTimeline: false,
      });
      stats.replayRuns += 1;
      const weekly = buildWorkerStageWeeklyMetricsMap({
        trades: replay.trades,
        fromTs,
        toTs: opts.windowToTs,
      });

      for (const [weekStartTs, metrics] of weekly.entries()) {
        pendingRows.push({
          venue: candidate.venue,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          entrySessionProfile: candidate.entrySessionProfile,
          stageId: stage.id,
          weekStartTs,
          weekToTs: weekStartTs + ONE_WEEK_MS,
          cacheVersion: opts.cacheVersion,
          metrics,
          updatedAtMs: Date.now(),
        });
        stats.stageRowsPrepared += 1;
      }

      if (pendingRows.length >= opts.upsertBatchSize) {
        await flush();
      }
    }

    if (opts.verbose && (idx + 1) % 25 === 0) {
      console.log(
        JSON.stringify(
          {
            progress: {
              processedCandidates: idx + 1,
              selectedCandidates: stats.selectedCandidates,
              replayRuns: stats.replayRuns,
              stageRowsPrepared: stats.stageRowsPrepared,
              stageRowsUpserted: stats.stageRowsUpserted,
            },
          },
          null,
          2,
        ),
      );
    }
  }

  await flush();
  return stats;
}
