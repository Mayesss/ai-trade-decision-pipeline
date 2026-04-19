import {
  listScalpV2Deployments,
  updateScalpV2CandidateStatuses,
  upsertScalpV2Deployments,
} from "../lib/scalp-v2/db";
import { scalpPrisma } from "../lib/scalp-v2/pg";
import type {
  ScalpV2RiskProfile,
  ScalpV2Session,
  ScalpV2Venue,
} from "../lib/scalp-v2/types";
import { resolveScalpV2CompletedWeekWindowToUtc } from "../lib/scalp-v2/weekWindows";

type Options = {
  apply: boolean;
  requiredWeeks: number;
  minNonZeroWeeks: number;
  limit: number;
};

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function toNonNegativeInt(value: unknown, fallback: number, max = 100_000): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n < 0) return fallback;
  return Math.max(0, Math.min(max, n));
}

function parseCli(argv: string[]): Options {
  let apply = false;
  let requiredWeeks = 12;
  let minNonZeroWeeks = 0;
  let limit = 10_000;

  for (const arg of argv) {
    const raw = String(arg || "").trim();
    if (!raw) continue;
    if (raw === "--apply") {
      apply = true;
      continue;
    }
    if (raw === "--dry-run") {
      apply = false;
      continue;
    }
    if (raw.startsWith("--weeks=")) {
      requiredWeeks = toNonNegativeInt(raw.split("=")[1], requiredWeeks, 52);
      continue;
    }
    if (raw.startsWith("--min-non-zero-weeks=")) {
      minNonZeroWeeks = toNonNegativeInt(
        raw.split("=")[1],
        minNonZeroWeeks,
        52,
      );
      continue;
    }
    if (raw.startsWith("--limit=")) {
      limit = Math.max(1, toNonNegativeInt(raw.split("=")[1], limit, 10_000));
      continue;
    }
  }

  requiredWeeks = Math.max(1, Math.min(52, requiredWeeks));
  minNonZeroWeeks = Math.max(0, Math.min(requiredWeeks, minNonZeroWeeks));
  limit = Math.max(1, Math.min(10_000, limit));

  return {
    apply,
    requiredWeeks,
    minNonZeroWeeks,
    limit,
  };
}

function toIso(ts: number | null): string | null {
  if (!Number.isFinite(Number(ts))) return null;
  return new Date(Number(ts)).toISOString();
}

type Finding = {
  deploymentId: string;
  symbol: string;
  venue: ScalpV2Venue;
  strategyId: string;
  tuneId: string;
  session: ScalpV2Session;
  candidateId: number | null;
  reason:
    | "execute_guard_freshness_not_ready"
    | "execute_guard_freshness_weeks_incomplete"
    | "execute_guard_window_stale"
    | "execute_guard_stage_c_window_stale"
    | "execute_guard_stage_c_weekly_missing"
    | "execute_guard_stage_c_non_zero_weeks_below_threshold";
  requiredWeeks: number;
  completedWeeks: number;
  weeklyWeeks: number;
  nonZeroWeeks: number;
  freshnessWindowToTs: number | null;
  freshnessWindowToIso: string | null;
  stageCToTs: number | null;
  stageCToIso: string | null;
  expectedWindowToTs: number;
  expectedWindowToIso: string;
  promotionGate: Record<string, unknown>;
  riskProfile: ScalpV2RiskProfile;
};

async function main() {
  const options = parseCli(process.argv.slice(2));
  const nowTs = Date.now();
  const expectedWindowToTs = resolveScalpV2CompletedWeekWindowToUtc(nowTs);
  const rows = await listScalpV2Deployments({
    enabledOnly: true,
    limit: options.limit,
  });

  const findings: Finding[] = [];

  for (const row of rows) {
    const gate = asRecord(row.promotionGate);
    const freshness = asRecord(gate.freshness);
    const worker = asRecord(gate.worker);
    const stageC = asRecord(worker.stageC);
    const weeklyNetR = asRecord(stageC.weeklyNetR);

    const freshnessReady = freshness.ready === true;
    const completedWeeks = Math.max(
      0,
      Math.floor(Number(freshness.completedWeeks) || 0),
    );
    const freshnessWindowToTsRaw = Math.floor(Number(freshness.windowToTs) || 0);
    const freshnessWindowToTs =
      freshnessWindowToTsRaw > 0 ? freshnessWindowToTsRaw : null;
    const stageCToTs = Number(stageC.toTs);

    const weekKeys = Object.keys(weeklyNetR)
      .map((k) => Number(k))
      .filter((n) => Number.isFinite(n) && n > 0)
      .sort((a, b) => a - b);

    const nonZeroWeeks = weekKeys.filter((k) => {
      const v = Number(weeklyNetR[String(k)]);
      return Number.isFinite(v) && Math.abs(v) > 1e-9;
    }).length;

    const totalWeeks = weekKeys.length;

    let reason: Finding["reason"] | null = null;
    if (!freshnessReady) {
      reason = "execute_guard_freshness_not_ready";
    } else if (completedWeeks < options.requiredWeeks) {
      reason = "execute_guard_freshness_weeks_incomplete";
    } else if (
      !Number.isFinite(Number(freshnessWindowToTs)) ||
      freshnessWindowToTs !== expectedWindowToTs
    ) {
      reason = "execute_guard_window_stale";
    } else if (
      Number.isFinite(stageCToTs) &&
      Number(stageCToTs) > 0 &&
      Number(stageCToTs) !== expectedWindowToTs
    ) {
      reason = "execute_guard_stage_c_window_stale";
    } else if (totalWeeks < options.requiredWeeks) {
      reason = "execute_guard_stage_c_weekly_missing";
    } else if (
      options.minNonZeroWeeks > 0 &&
      nonZeroWeeks < options.minNonZeroWeeks
    ) {
      reason = "execute_guard_stage_c_non_zero_weeks_below_threshold";
    }

    if (reason) {
      findings.push({
        deploymentId: row.deploymentId,
        symbol: row.symbol,
        venue: row.venue,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        session: row.entrySessionProfile,
        candidateId: row.candidateId === null ? null : Number(row.candidateId),
        reason,
        requiredWeeks: options.requiredWeeks,
        completedWeeks,
        weeklyWeeks: totalWeeks,
        nonZeroWeeks,
        freshnessWindowToTs,
        freshnessWindowToIso: toIso(freshnessWindowToTs),
        stageCToTs: Number.isFinite(stageCToTs) ? stageCToTs : null,
        stageCToIso: Number.isFinite(stageCToTs) ? toIso(stageCToTs) : null,
        expectedWindowToTs,
        expectedWindowToIso: toIso(expectedWindowToTs) || "",
        promotionGate: gate,
        riskProfile: row.riskProfile,
      });
    }
  }

  const reasonCounts = findings.reduce<Record<string, number>>((acc, row) => {
    const reason = String(row.reason || "unknown");
    acc[reason] = (acc[reason] || 0) + 1;
    return acc;
  }, {});

  let demotedDeployments = 0;
  let requeuedCandidates = 0;
  if (options.apply && findings.length > 0) {
    demotedDeployments = await upsertScalpV2Deployments({
      rows: findings.map((row) => ({
        candidateId: row.candidateId,
        venue: row.venue,
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        entrySessionProfile: row.session,
        enabled: false,
        liveMode: "shadow",
        promotionGate: {
          ...row.promotionGate,
          eligible: false,
          shadowEligible: false,
          reason: row.reason,
          source: "manual_enabled_freshness_remediation",
          evaluatedAtMs: nowTs,
          executeGuard: {
            checkedAtMs: nowTs,
            requiredWeeks: row.requiredWeeks,
            completedWeeks: row.completedWeeks,
            freshnessWindowToTs: row.freshnessWindowToTs,
            stageCToTs: row.stageCToTs,
            weeklyWeeks: row.weeklyWeeks,
            nonZeroWeeks: row.nonZeroWeeks,
            expectedWindowToTs: row.expectedWindowToTs,
            reason: row.reason,
          },
        },
        riskProfile: row.riskProfile,
      })),
    });

    const candidateIds = Array.from(
      new Set(
        findings
          .map((row) => Math.floor(Number(row.candidateId) || 0))
          .filter((id) => id > 0),
      ),
    );
    if (candidateIds.length > 0) {
      requeuedCandidates = await updateScalpV2CandidateStatuses({
        ids: candidateIds,
        status: "discovered",
        metadataPatch: {
          requeue: {
            triggeredAtMs: nowTs,
            trigger: "manual_enabled_freshness_remediation",
            windowToTs: expectedWindowToTs,
          },
        },
      });
    }
  }

  console.log(JSON.stringify({
    apply: options.apply,
    requiredWeeks: options.requiredWeeks,
    minNonZeroWeeks: options.minNonZeroWeeks,
    nowTs,
    windowToTs: expectedWindowToTs,
    windowToIso: toIso(expectedWindowToTs),
    enabledDeployments: rows.length,
    failingEnabledDeployments: findings.length,
    reasonCounts,
    demotedDeployments,
    requeuedCandidates,
    findingsSample: findings.slice(0, 120).map((row) => ({
      deploymentId: row.deploymentId,
      symbol: row.symbol,
      venue: row.venue,
      strategyId: row.strategyId,
      tuneId: row.tuneId,
      session: row.session,
      candidateId: row.candidateId,
      reason: row.reason,
      requiredWeeks: row.requiredWeeks,
      completedWeeks: row.completedWeeks,
      weeklyWeeks: row.weeklyWeeks,
      nonZeroWeeks: row.nonZeroWeeks,
      freshnessWindowToTs: row.freshnessWindowToTs,
      freshnessWindowToIso: row.freshnessWindowToIso,
      stageCToTs: row.stageCToTs,
      stageCToIso: row.stageCToIso,
      expectedWindowToTs: row.expectedWindowToTs,
      expectedWindowToIso: row.expectedWindowToIso,
    })),
  }, null, 2));

  await scalpPrisma().$disconnect();
}

main().catch(async (err) => {
  console.error(String(err?.stack || err?.message || err));
  try {
    await scalpPrisma().$disconnect();
  } catch {
    // noop
  }
  process.exitCode = 1;
});
