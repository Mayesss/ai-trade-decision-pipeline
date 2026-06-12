#!/usr/bin/env node
import nextEnv from "@next/env";

import { loadScalpCandleHistoryInRange } from "../lib/scalp/candleHistory";
import { pipSizeForScalpSymbol } from "../lib/scalp/marketData";
import {
  defaultScalpReplayConfig,
  runScalpReplay,
} from "../lib/scalp/replay/harness";
import type { ScalpReplayCandle } from "../lib/scalp/replay/types";
import type { ScalpCandle } from "../lib/scalp/types";
import { loadScalpSymbolMarketMetadataBulk } from "../lib/scalp/symbolMarketMetadataStore";
import {
  aggregateScalpV2PatternEdges,
  buildScalpV2PatternKey,
  extractScalpV2PatternTradeVectors,
  SCALP_V2_PATTERN_EVIDENCE_POPULATION_STAGE_C_PASSED,
  selectScalpV2PatternRepresentativeCandidates,
  type ScalpV2PatternCandidateSummary,
} from "../lib/scalp-v2/patternEvidence";
import {
  listScalpV2PatternEvidenceBackfillCandidates,
  loadScalpV2PatternTradeVectors,
  replaceScalpV2PatternTradeVectors,
  upsertScalpV2PatternEdges,
} from "../lib/scalp-v2/db";
import { resolveEntryTriggerOverrides } from "../lib/scalp-v2/entryTriggerPresets";
import { resolveExitRuleOverrides } from "../lib/scalp-v2/exitRulePresets";
import { toDeploymentId } from "../lib/scalp-v2/logic";
import { isScalpPgConfigured } from "../lib/scalp-v2/pg";
import { resolveRiskRuleReplayOverrides } from "../lib/scalp-v2/riskRulePresets";
import { parseSessionStructureComposerTuneId } from "../lib/scalp-v2/sessionStructureComposer";
import { inferScalpV2AssetCategory, minSpreadPipsForCategory } from "../lib/scalp-v2/symbolInfo";
import { resolveStateMachineReplayOverrides } from "../lib/scalp-v2/stateMachinePresets";
import type { ScalpV2Candidate, ScalpV2Session, ScalpV2Venue } from "../lib/scalp-v2/types";
import { startOfScalpV2WeekMondayUtc } from "../lib/scalp-v2/weekWindows";

const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

if (process.env.SCALP_PG_USE_HTTP === undefined) {
  process.env.SCALP_PG_USE_HTTP = "1";
}

type Args = Record<string, string | boolean>;

function parseArgs(argv: string[]): Args {
  const out: Args = {};
  for (let idx = 0; idx < argv.length; idx += 1) {
    const token = argv[idx]!;
    if (!token.startsWith("--")) continue;
    const eqIdx = token.indexOf("=");
    if (eqIdx > 2) {
      out[token.slice(2, eqIdx)] = token.slice(eqIdx + 1);
      continue;
    }
    const key = token.slice(2);
    const next = argv[idx + 1];
    if (!next || next.startsWith("--")) out[key] = true;
    else {
      out[key] = next;
      idx += 1;
    }
  }
  return out;
}

function boolArg(args: Args, key: string, fallback: boolean): boolean {
  const raw = args[key];
  if (raw === undefined) return fallback;
  if (typeof raw === "boolean") return raw;
  const s = String(raw).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(s)) return true;
  if (["0", "false", "no", "off"].includes(s)) return false;
  return fallback;
}

function intArg(args: Args, key: string, fallback: number, min: number, max: number): number {
  const n = Math.floor(Number(args[key]));
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function num(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function strArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.map((row) => String(row || "").trim()).filter(Boolean)
    : [];
}

function toReplayCandlesFromHistory(candles: ScalpCandle[], spreadPips: number): ScalpReplayCandle[] {
  const out: ScalpReplayCandle[] = [];
  for (const row of candles || []) {
    const tuple = row as [number, number, number, number, number, number];
    const ts = Math.floor(Number(tuple?.[0] || 0));
    const open = Number(tuple?.[1] || 0);
    const high = Number(tuple?.[2] || 0);
    const low = Number(tuple?.[3] || 0);
    const close = Number(tuple?.[4] || 0);
    const volume = Number(tuple?.[5] || 0);
    if (!Number.isFinite(ts) || ![open, high, low, close].every((v) => Number.isFinite(v) && v > 0)) {
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
  const byWeek = new Map<number, { nonSunday: ScalpReplayCandle[]; sunday: ScalpReplayCandle[] }>();
  for (const row of candles || []) {
    const weekStart = startOfScalpV2WeekMondayUtc(row.ts);
    const bucket = byWeek.get(weekStart) || { nonSunday: [], sunday: [] };
    if (new Date(row.ts).getUTCDay() === 0) bucket.sunday.push(row);
    else bucket.nonSunday.push(row);
    byWeek.set(weekStart, bucket);
  }
  const out: ScalpReplayCandle[] = [];
  for (const weekStart of Array.from(byWeek.keys()).sort((a, b) => a - b)) {
    const bucket = byWeek.get(weekStart)!;
    out.push(...(bucket.nonSunday.length ? bucket.nonSunday : bucket.sunday));
  }
  return out.sort((a, b) => a.ts - b.ts);
}

function behaviorFingerprintForCandidate(candidate: ScalpV2Candidate): string {
  const meta = asRecord(candidate.metadata);
  const existing = String(meta.sessionComposerBehaviorFingerprint || "").trim();
  if (existing) return existing;
  const plan = asRecord(meta.sessionComposerPlan);
  const fromPlan = [
    plan.contextId,
    plan.levelId,
    plan.triggerId,
    plan.confirmationId,
    plan.managementId,
  ]
    .map((row) => String(row || "").trim())
    .filter(Boolean);
  if (fromPlan.length === 5) return fromPlan.join("|");
  const parsed = parseSessionStructureComposerTuneId(candidate.tuneId);
  return [
    parsed.contextId,
    parsed.levelId,
    parsed.triggerId,
    parsed.confirmationId,
    parsed.managementId,
  ].join("|");
}

function summaryForCandidate(candidate: ScalpV2Candidate): ScalpV2PatternCandidateSummary {
  const meta = asRecord(candidate.metadata);
  const worker = asRecord(meta.worker);
  const stageC = asRecord(worker.stageC);
  const v3Ranking = asRecord(meta.v3Ranking);
  const v3StageC = asRecord(v3Ranking.stageC);
  const stats = asRecord(v3StageC.stats);
  return {
    candidateId: candidate.id,
    venue: candidate.venue,
    symbol: candidate.symbol,
    session: candidate.entrySessionProfile,
    behaviorFingerprint: behaviorFingerprintForCandidate(candidate),
    windowToTs: Math.floor(num(worker.windowToTs)),
    stageCLowerBoundR: Number.isFinite(Number(stats.lowerBoundR))
      ? Number(stats.lowerBoundR)
      : null,
    stageCNetR: num(stageC.netR),
    stageCTrades: Math.max(0, Math.floor(num(stageC.trades))),
  };
}

function countByPattern(rows: ScalpV2PatternCandidateSummary[]): Map<string, number> {
  const out = new Map<string, number>();
  for (const row of rows) {
    const key = buildScalpV2PatternKey({
      venue: row.venue,
      session: row.session,
      behaviorFingerprint: row.behaviorFingerprint,
    });
    out.set(key, (out.get(key) || 0) + 1);
  }
  return out;
}

function legacyBlocks(candidate: ScalpV2Candidate): {
  entry_trigger: string[];
  exit_rule: string[];
  risk_rule: string[];
  state_machine: string[];
} {
  const meta = asRecord(candidate.metadata);
  const legacy = asRecord(meta.legacyResearchDsl);
  const fallback = asRecord(meta.researchDsl);
  return {
    entry_trigger: strArray(legacy.entry_trigger ?? fallback.entry_trigger),
    exit_rule: strArray(legacy.exit_rule ?? fallback.exit_rule),
    risk_rule: strArray(legacy.risk_rule ?? fallback.risk_rule),
    state_machine: strArray(legacy.state_machine ?? fallback.state_machine),
  };
}

async function runWithConcurrency<T>(
  rows: T[],
  concurrency: number,
  fn: (row: T, index: number) => Promise<void>,
): Promise<void> {
  let next = 0;
  const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
    while (next < rows.length) {
      const index = next;
      next += 1;
      await fn(rows[index]!, index);
    }
  });
  await Promise.all(workers);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const explicitBackfill = args.backfill !== undefined;
  const explicitAggregate = args.aggregate !== undefined;
  const backfill = boolArg(args, "backfill", !explicitBackfill && !explicitAggregate ? true : false);
  const aggregate = boolArg(args, "aggregate", !explicitBackfill && !explicitAggregate ? true : false);
  const dryRun = boolArg(args, "dryRun", false);
  const bucketMinutes = intArg(args, "bucketMinutes", 60, 1, 24 * 60);
  const limit = intArg(args, "limit", 1_000, 1, 50_000);
  const concurrency = intArg(args, "concurrency", 4, 1, 16);
  const windowArg = args.window === undefined ? "latest" : String(args.window);
  const windowToTs = windowArg === "latest" ? "latest" : Math.floor(Number(windowArg));

  if (!isScalpPgConfigured()) {
    console.error("Scalp PG is not configured.");
    process.exitCode = 1;
    return;
  }

  const candidates = await listScalpV2PatternEvidenceBackfillCandidates({
    windowToTs: windowToTs === "latest" ? "latest" : windowToTs,
    limit,
  });
  const summaries = candidates
    .map(summaryForCandidate)
    .filter((row) => row.windowToTs > 0 && row.behaviorFingerprint);
  const representatives = selectScalpV2PatternRepresentativeCandidates(summaries);
  const representativeIds = new Set(representatives.map((row) => row.candidateId).filter((id): id is number => id !== null));
  const representativeCandidates = candidates.filter((row) => representativeIds.has(row.id));
  const effectiveWindowToTs = representatives[0]?.windowToTs || 0;

  console.log(
    `pattern evidence: candidates=${candidates.length} representatives=${representativeCandidates.length} windowToTs=${effectiveWindowToTs || "none"} bucket=${bucketMinutes}m dryRun=${dryRun ? "yes" : "no"}`,
  );

  if (backfill && representativeCandidates.length > 0) {
    const symbols = Array.from(new Set(representativeCandidates.map((row) => row.symbol)));
    const metadata = await loadScalpSymbolMarketMetadataBulk(symbols).catch(() => new Map());
    const candleCache = new Map<string, ScalpReplayCandle[]>();
    let writtenTrades = 0;
    let replayed = 0;
    let errors = 0;

    await runWithConcurrency(representativeCandidates, concurrency, async (candidate, index) => {
      try {
        const meta = asRecord(candidate.metadata);
        const worker = asRecord(meta.worker);
        const stageC = asRecord(worker.stageC);
        const fromTs = Math.floor(num(stageC.fromTs));
        const toTs = Math.floor(num(stageC.toTs, num(worker.windowToTs)));
        if (fromTs <= 0 || toTs <= fromTs) throw new Error("missing stage-C window");
        const symbolMeta = metadata.get(candidate.symbol) || null;
        const pipSize = pipSizeForScalpSymbol(candidate.symbol, symbolMeta || undefined);
        let candles = candleCache.get(candidate.symbol);
        if (!candles) {
          const category = inferScalpV2AssetCategory(candidate.symbol);
          const baseReplayConfig = defaultScalpReplayConfig(candidate.symbol);
          const tickSpreadPips = symbolMeta?.tickSize ? symbolMeta.tickSize / pipSize : 0;
          const spreadPips = Math.max(
            baseReplayConfig.defaultSpreadPips,
            minSpreadPipsForCategory(category),
            tickSpreadPips,
          );
          const history = await loadScalpCandleHistoryInRange(
            candidate.symbol,
            "1m",
            fromTs,
            toTs,
          );
          candles = filterSundayReplayCandles(
            toReplayCandlesFromHistory(
              (history.record?.candles || []) as ScalpCandle[],
              spreadPips,
            ),
          );
          candleCache.set(candidate.symbol, candles);
        }
        const deploymentId = toDeploymentId({
          venue: candidate.venue,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          session: candidate.entrySessionProfile,
        });
        const base = defaultScalpReplayConfig(candidate.symbol);
        const blocks = legacyBlocks(candidate);
        const replayConfig = {
          ...base,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          deploymentId,
          tuneLabel: candidate.tuneId,
          strategy: {
            ...base.strategy,
            entrySessionProfile: candidate.entrySessionProfile,
            ...resolveExitRuleOverrides(blocks.exit_rule),
            ...resolveEntryTriggerOverrides(blocks.entry_trigger),
            ...resolveRiskRuleReplayOverrides(blocks.risk_rule),
            ...resolveStateMachineReplayOverrides(blocks.state_machine),
          },
        };
        const replay = await runScalpReplay({
          candles,
          pipSize,
          config: replayConfig,
          captureTimeline: false,
          symbolMeta: symbolMeta || null,
        });
        const behaviorFingerprint = behaviorFingerprintForCandidate(candidate);
        const rows = extractScalpV2PatternTradeVectors({
          candidateId: candidate.id,
          venue: candidate.venue as ScalpV2Venue,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          session: candidate.entrySessionProfile as ScalpV2Session,
          windowToTs: Math.floor(num(worker.windowToTs, toTs)),
          behaviorFingerprint,
          trades: replay.trades,
          bucketMinutes,
        });
        if (!dryRun) {
          await replaceScalpV2PatternTradeVectors({
            identity: {
              venue: candidate.venue,
              symbol: candidate.symbol,
              strategyId: candidate.strategyId,
              tuneId: candidate.tuneId,
              session: candidate.entrySessionProfile,
              windowToTs: Math.floor(num(worker.windowToTs, toTs)),
              stageId: "c",
            },
            rows,
          });
        }
        writtenTrades += rows.length;
        replayed += 1;
        if ((index + 1) % 25 === 0 || index + 1 === representativeCandidates.length) {
          console.log(`  backfill progress ${index + 1}/${representativeCandidates.length} replayed=${replayed} trades=${writtenTrades} errors=${errors}`);
        }
      } catch (err: any) {
        errors += 1;
        console.warn(`  backfill failed candidate=${candidate.id} ${candidate.symbol} ${candidate.tuneId}: ${err?.message || err}`);
      }
    });
    console.log(`backfill done: replayed=${replayed} trades=${writtenTrades} errors=${errors}`);
  }

  if (aggregate && effectiveWindowToTs > 0) {
    const allVectors = await loadScalpV2PatternTradeVectors({
      windowToTs: effectiveWindowToTs,
      bucketMinutes,
      populationScope: SCALP_V2_PATTERN_EVIDENCE_POPULATION_STAGE_C_PASSED,
    });
    const filteredVectors = allVectors.filter((row) => row.candidateId !== null && representativeIds.has(row.candidateId));
    const candidateCountsByPattern = countByPattern(summaries);
    const representativeCountsByPattern = countByPattern(representatives);
    const edges = aggregateScalpV2PatternEdges({
      trades: filteredVectors,
      candidateCount: summaries.length,
      representativeCandidateCount: representatives.length,
      candidateCountsByPattern,
      representativeCandidateCountsByPattern: representativeCountsByPattern,
      bucketMinutes,
      populationScope: SCALP_V2_PATTERN_EVIDENCE_POPULATION_STAGE_C_PASSED,
    });
    if (!dryRun) {
      await upsertScalpV2PatternEdges({ edges });
    }
    console.log(`aggregate done: vectors=${filteredVectors.length}/${allVectors.length} edges=${edges.length}`);
    for (const edge of edges.slice(0, 10)) {
      console.log(
        `  ${edge.patternKey} bucketLB=${edge.bucketLowerBoundR.toFixed(4)} bucketNet=${edge.bucketNetR.toFixed(2)} buckets=${edge.bucketCount} symbols=${edge.positiveSymbolCount}/${edge.symbolCount} looLB=${edge.leaveOneSymbolOutBucketLowerBoundR?.toFixed(4) ?? "n/a"}`,
      );
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
