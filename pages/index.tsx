import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Head from "next/head";
import {
  AdminSecretPanel,
  DashboardHeader,
  PageShell,
  SectionHeader,
  Skeleton,
  V5_DECISION_COLOR,
  V5_DECISION_GLYPH,
  V5_DECISION_LABEL,
  type V5GateDecision,
  abbrCell,
  bar,
  fetchOne,
  fmtAgo,
  fmtClock,
  fmtEta,
  sparkline,
  useAdminSecretLoader,
} from "../components/scalp/shared";
import { WeeklyNetRTrack } from "../components/scalp/WeeklyNetRTrack";

// ─── types (mirror the four /api/scalp/v5/* endpoints + /api/scalp/v4/recent)

interface CoverageResp {
  ok: boolean;
  classifierVersion: string;
  v5Enabled: boolean;
  v5HardGateEnabled: boolean;
  config: { holdoutWeeks: number; minTradesPerCell: number };
  nowMs: number;
  evaluator: { latestEvaluationMs: number | null; oldestEvaluationMs: number | null };
  coverage: {
    totalDeployments: number;
    enabledDeployments: number;
    evaluated: number;
    missingEvidence: number;
    stale: number;
    staleThresholdMs: number;
  };
  progress: {
    weekStartMs: number;
    evaluatedThisWeek: number;
    remainingThisWeek: number;
    lastHour: number;
    buckets12h: number[];
    ratePerHour: number;
    etaHours: number | null;
  };
  candleHealth: {
    fresh: number;
    lagging: number;
    broken: number;
    missing: number;
    worstSymbols: Array<{ venue: string; symbol: string; ageMinutes: number | null }>;
  };
}

interface GateStateResp {
  ok: boolean;
  classifierVersion: string;
  nowMs: number;
  stateNow: Record<V5GateDecision, number>;
  stateNowEnabled: Record<V5GateDecision, number>;
  stateNowLive?: Record<V5GateDecision, number>;
}

interface V5CellRow {
  cellId: string;
  trades: number;
  netR: number;
  expectancyR: number;
  wins: number;
  losses: number;
  weeklyNetR: number[];
  isCurrent: boolean;
}

interface V5DeploymentRow {
  deploymentId: string;
  venue: string;
  symbol: string;
  session: string;
  strategyId: string;
  tuneId: string;
  enabled: boolean;
  liveMode: string | null;
  v5Enabled: boolean;
  v5EvaluatedAtMs: number | null;
  currentCell: { cellId: string | null; stale: boolean; updatedAtMs: number | null };
  gate: {
    decision: V5GateDecision;
    currentCellStat: { trades: number; expectancyR: number; netR: number; wins: number; losses: number } | null;
    eligibleCells: string[];
  };
  holdoutWindow: { fromMs: number; toMs: number } | null;
  totalTrades: number;
  totalNetR: number;
  cells: V5CellRow[];
}

interface DeploymentsResp {
  ok: boolean;
  classifierVersion: string;
  nowMs: number;
  page: {
    scope: DeploymentScope;
    limit: number;
    offset: number;
    returned: number;
    totalMatching: number;
    hasMore: boolean;
    includeInactive: boolean;
  };
  diagnostics?: { payloadClass?: string };
  deployments: V5DeploymentRow[];
}

interface TradeRow {
  tsMs: number;
  deploymentId: string | null;
  venue: string | null;
  symbol: string | null;
  reasonCodes: string[];
  eventKind: "trade" | "trade_open" | "trade_close" | "entry_error" | "entry_skipped" | "state_change";
  state: string;
  stateChanged: boolean;
  tradeEventOccurred: boolean;
  rMultiple: number | null;
  summary: string;
}

interface DailyNetRRow {
  dayKey: string;
  dayStartMs: number;
  trades: number;
  wins: number;
  losses: number;
  netR: number;
  pnlUsd: number;
}

interface RecentResp {
  ok: boolean;
  classifierVersion: string;
  dailyNetR: DailyNetRRow[];
  recentTrades: TradeRow[];
}

interface NeonUsageResp {
  ok: boolean;
  configured: boolean;
  generatedAtMs: number;
  message?: string;
  requiredEnv?: {
    NEON_API_KEY: boolean;
    NEON_PROJECT_ID: boolean;
    NEON_ORG_ID: boolean;
  };
  project?: {
    id: string;
    name: string | null;
    dataTransferBytes: number;
    allowanceBytes: number;
    allowanceGb: number;
    allowanceUsedPct: number | null;
  };
  branch?: {
    id: string;
    name: string | null;
    dataTransferBytes: number | null;
  } | null;
  consumption24h?: {
    available: boolean;
    error: string | null;
    from: string;
    to: string;
    granularity: string;
    publicNetworkTransferBytes: number;
    privateNetworkTransferBytes: number;
    totalNetworkTransferBytes: number;
    hourly: Array<{
      timeframeStart: string | null;
      timeframeEnd: string | null;
      publicNetworkTransferBytes: number;
      privateNetworkTransferBytes: number;
      totalNetworkTransferBytes: number;
    }>;
  };
}

interface ResearchHealthResp {
  ok: boolean;
  mode: "scalp_v2";
  nowMs: number;
  staleLockMinutes: number;
  health: {
    staleThresholdMs: number;
    stale: boolean;
    approachingStale: boolean;
    lockAgeMs: number | null;
    heartbeatAgeMs: number | null;
  };
  queue?: {
    total: number;
    processed: number;
    discovered: number;
    evaluated: number;
    promoted: number;
    rejected: number;
  };
  robustness?: {
    stageCPassed: number;
    missing: number;
    passed: number;
    failed: number;
  };
  job: {
    status: string;
    attempts: number;
    locked: boolean;
    lockedAtMs: number | null;
    updatedAtMs: number | null;
    nextRunAtMs: number | null;
    phase: string | null;
    reason: string | null;
    progress: {
      processedSoFar: number;
      totalSelected: number;
      selectedTotal: number;
      discoveredTotal: number;
      workerStage: string | null;
      workerStageProcessed: number;
      workerStageTotal: number;
      skippedByCache: number;
      skippedByClearFail: number;
      skippedByNetRPreFilter: number;
      smartSkippedPersisted: number;
      surrogateSkippedPersisted: number;
      stageAPass: number;
      stageAFail: number;
      stageBPass: number;
      stageBFail: number;
      stageCPass: number;
      stageCFail: number;
      persisted: number;
      replayErrors: number;
      persistErrors: number;
      stage0Replays: number;
      stage0Skipped: number;
      incrementalStageReplays: number;
      fullStageReplays: number;
      earlyAbortedStageReplays: number;
      cachedStageReuses: number;
      newestWeekReplayReuses: number;
      stageBCacheHits: number;
      stageCCacheHits: number;
      deferredByCandleCoverage: number;
      finalizedCoverageDeferrals: number;
      pendingAfter: number;
      remaining: number;
      timeBudgetExhausted: boolean;
    };
    log: unknown[];
  } | null;
  hint: {
    tone: "ok" | "warn" | "critical" | "info";
    label: string;
    detail: string | null;
  };
}

type DeploymentScope = "live" | "enabled" | "inactive" | "all";

const DECISION_ORDER: V5GateDecision[] = [
  "allow",
  "block_negative",
  "block_unseen",
  "block_insufficient_trades",
  "block_stale",
  "block_evaluator_pending",
];

// ─── page ────────────────────────────────────────────────────────────────────

export default function ScalpV5Dashboard() {
  // Four independent slices so a slow endpoint doesn't gate the others.
  const [coverage, setCoverage] = useState<CoverageResp | null>(null);
  const [gateState, setGateState] = useState<GateStateResp | null>(null);
  const [deployments, setDeployments] = useState<DeploymentsResp | null>(null);
  const [recent, setRecent] = useState<RecentResp | null>(null);
  const [neonUsage, setNeonUsage] = useState<NeonUsageResp | null>(null);
  const [researchHealth, setResearchHealth] = useState<ResearchHealthResp | null>(null);

  const [expandedDeployments, setExpandedDeployments] = useState<Set<string>>(new Set());
  const [decisionFilter, setDecisionFilter] = useState<V5GateDecision | null>(null);
  const [deploymentScope, setDeploymentScope] = useState<DeploymentScope>("live");
  const [deploymentOffset, setDeploymentOffset] = useState(0);
  const [deploymentsLoading, setDeploymentsLoading] = useState(false);
  const [deploymentsError, setDeploymentsError] = useState<string | null>(null);
  const deploymentsInitialLoaded = useRef(false);

  // On first gate-state load, default the filter to "allow" so the deployments
  // section opens scoped to the live-trading rows. Only fires once — user
  // clears/switches stick after.
  const defaultFilterApplied = useRef(false);
  useEffect(() => {
    if (defaultFilterApplied.current || !gateState) return;
    defaultFilterApplied.current = true;
    if ((gateState.stateNowEnabled.allow || 0) > 0) {
      setDecisionFilter("allow");
    }
  }, [gateState]);

  const toggleExpanded = useCallback((id: string) => {
    setExpandedDeployments((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  // Fire small endpoints in parallel. Deployments are intentionally excluded:
  // that endpoint can carry large evidence payloads and is loaded on demand.
  const loader = useCallback(async (headers: Record<string, string>) => {
    const [c, g, rc, nu, rh] = await Promise.all([
      fetchOne<CoverageResp>("/api/scalp/v5/coverage", headers, setCoverage),
      fetchOne<GateStateResp>("/api/scalp/v5/gate-state", headers, setGateState),
      fetchOne<RecentResp>("/api/scalp/v4/recent", headers, setRecent),
      fetchOne<NeonUsageResp>("/api/scalp/ops/neon-usage", headers, setNeonUsage),
      fetchOne<ResearchHealthResp>("/api/scalp/v2/ops/research-health", headers, setResearchHealth),
    ]);
    if (c.unauthorized || g.unauthorized || rc.unauthorized || nu.unauthorized || rh.unauthorized) {
      return { unauthorized: true };
    }
    return { error: c.error || g.error || rc.error || nu.error || rh.error };
  }, []);

  const access = useAdminSecretLoader(loader);

  const loadDeployments = useCallback(
    async (scope: DeploymentScope = deploymentScope, offset = deploymentOffset) => {
      const headers: Record<string, string> = {};
      if (access.adminSecret) headers["x-admin-access-secret"] = access.adminSecret;
      const limit = scope === "live" ? 100 : 50;
      setDeploymentsLoading(true);
      setDeploymentsError(null);
      const attempt = await fetchOne<DeploymentsResp>(
        `/api/scalp/v5/deployments?scope=${encodeURIComponent(scope)}&limit=${limit}&offset=${offset}`,
        headers,
        setDeployments,
      );
      setDeploymentsLoading(false);
      if (attempt.unauthorized) {
        deploymentsInitialLoaded.current = false;
        access.setShowSecretPanel(true);
        setDeploymentsError("Unauthorized — admin secret missing or invalid.");
        return;
      }
      if (attempt.error) {
        setDeploymentsError(attempt.error);
        return;
      }
      setDeploymentScope(scope);
      setDeploymentOffset(offset);
    },
    [access.adminSecret, access.setShowSecretPanel, deploymentOffset, deploymentScope],
  );

  useEffect(() => {
    if (deploymentsInitialLoaded.current || !access.secretHydrated || access.unauthorized) return;
    deploymentsInitialLoaded.current = true;
    loadDeployments("live", 0);
  }, [access.secretHydrated, access.unauthorized, loadDeployments]);

  const deploymentRows = useMemo(() => (deployments ? deployments.deployments : []), [deployments]);
  const filteredDeploymentRows = useMemo(() => {
    if (!decisionFilter) return deploymentRows;
    return deploymentRows.filter((r) => r.gate.decision === decisionFilter);
  }, [deploymentRows, decisionFilter]);

  const activityRows = useMemo(() => (recent ? recent.recentTrades.slice(0, 10) : []), [recent]);

  // Hide evaluator section once this week's evaluation pass is complete —
  // the panel is only useful while there's still work to do.
  const evaluatorComplete = useMemo(() => {
    if (!coverage) return false;
    const total = coverage.coverage.totalDeployments;
    if (total === 0) return false;
    return coverage.progress.evaluatedThisWeek >= total;
  }, [coverage]);

  const showResearchProgress = Boolean(
    coverage &&
      coverage.coverage.totalDeployments === 0 &&
      researchHealth &&
      ((researchHealth.queue?.total || 0) > 0 || researchHealth.job),
  );

  const sessionCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const row of deploymentRows) {
      if (!row.enabled || row.liveMode !== "live") continue;
      if (row.gate.decision !== "allow") continue;
      const key = (row.session || "").toLowerCase();
      counts[key] = (counts[key] || 0) + 1;
    }
    return counts;
  }, [deploymentRows]);

  // Tick once per 30s so session highlighting follows the wall clock.
  const [nowMs, setNowMs] = useState<number>(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 30_000);
    return () => clearInterval(id);
  }, []);

  const activeSessions = useMemo(() => {
    const set = new Set<string>();
    for (const p of SESSION_PROFILES) {
      const localMoD = minuteOfDayInTz(nowMs, p.timeZone);
      const inWindow =
        p.endMin > p.startMin
          ? localMoD >= p.startMin && localMoD < p.endMin
          : localMoD >= p.startMin || localMoD < p.endMin;
      if (inWindow) set.add(p.name);
    }
    return set;
  }, [nowMs]);

  // Header classifierVersion: prefer whichever endpoint has loaded first.
  const classifierVersion =
    coverage?.classifierVersion ||
    gateState?.classifierVersion ||
    deployments?.classifierVersion ||
    null;

  return (
    <PageShell>
      <Head>
        <title>scalp v5 · cell gate</title>
      </Head>
      <DashboardHeader
        title="SCALP V5 · CELL GATE"
        classifierVersion={classifierVersion}
        loadedAt={access.loadedAt}
        autoRefresh={access.autoRefresh}
        setAutoRefresh={access.setAutoRefresh}
        load={access.load}
        adminSecret={access.adminSecret}
        onToggleSecret={() => {
          access.setSecretInput(access.adminSecret);
          access.setShowSecretPanel(!access.showSecretPanel);
        }}
        navLinks={[
          { href: "/v4-pipeline", label: "v4 pipeline" },
          { href: "/legacy", label: "legacy" },
        ]}
      />

      <AdminSecretPanel
        show={access.showSecretPanel}
        unauthorized={access.unauthorized}
        adminSecret={access.adminSecret}
        secretInput={access.secretInput}
        setSecretInput={access.setSecretInput}
        saveSecret={access.saveSecret}
        load={access.load}
        dismiss={() => access.setShowSecretPanel(false)}
      />

      {access.error && !access.unauthorized ? (
        <pre className="mt-3 whitespace-pre-wrap text-rose-400">⚠ {access.error}</pre>
      ) : null}

      {evaluatorComplete ? null : showResearchProgress ? (
        <>
          <SectionHeader title="research · session composer backtests" />
          {researchHealth ? <ResearchProgress data={researchHealth} /> : <Skeleton label="loading research progress" />}
        </>
      ) : (
        <>
          <SectionHeader
            title={`evaluator · holdout=${coverage?.config.holdoutWeeks ?? "—"}w  minTrades/cell=${coverage?.config.minTradesPerCell ?? "—"}`}
          />
          {coverage ? (
            <>
              <EvaluatorStrip data={coverage} />
              <EvaluatorProgress data={coverage} />
              <CandleHealth data={coverage} />
            </>
          ) : (
            <Skeleton label="loading evaluator" />
          )}
        </>
      )}

      <WeekNetRTrack dailyNetR={recent?.dailyNetR ?? null} recentTrades={recent?.recentTrades ?? null} />

      <SectionHeader title="neon usage · network transfer" />
      {neonUsage ? <NeonUsageCompact data={neonUsage} /> : <Skeleton label="loading neon usage" />}

      <SessionTimeline sessionCounts={sessionCounts} />

      <SectionHeader title="deployments · live cell evidence" />
      {gateState ? (
        <GateChipBar
          stateNowEnabled={gateState.stateNowLive ?? gateState.stateNowEnabled}
          activeFilter={decisionFilter}
          onPick={(d) => setDecisionFilter((cur) => (cur === d ? null : d))}
        />
      ) : (
        <Skeleton label="loading gate state" />
      )}
      <DeploymentScopeControls
        scope={deploymentScope}
        page={deployments?.page ?? null}
        loading={deploymentsLoading}
        error={deploymentsError}
        onScope={(scope) => loadDeployments(scope, 0)}
        onRefresh={() => loadDeployments(deploymentScope, deploymentOffset)}
        onPrev={() => loadDeployments(deploymentScope, Math.max(0, deploymentOffset - (deployments?.page.limit ?? 50)))}
        onNext={() => loadDeployments(deploymentScope, deploymentOffset + (deployments?.page.limit ?? 50))}
      />
      {deployments ? (
        deployments.deployments.length === 0 ? (
          <div className="pl-2 mt-1 text-zinc-500">(no {deploymentScope} deployments on this page)</div>
        ) : (
          <div className="mt-1 pl-2 space-y-0.5">
            {filteredDeploymentRows.length > 0 ? (
              filteredDeploymentRows.map((dep) => (
                <V5DeploymentRowView
                  key={dep.deploymentId}
                  row={dep}
                  expanded={expandedDeployments.has(dep.deploymentId)}
                  onToggle={() => toggleExpanded(dep.deploymentId)}
                  sessionActive={activeSessions.has((dep.session || "").toLowerCase())}
                />
              ))
            ) : (
              <div className="text-zinc-500 text-[12px]">
                (no {deploymentScope} deployments{decisionFilter ? ` with decision=${V5_DECISION_LABEL[decisionFilter]}` : ""})
              </div>
            )}
          </div>
        )
      ) : (
        <div className="pl-2 mt-1 text-zinc-600">deployments load on demand; live page loads once</div>
      )}

      <SectionHeader title={`activity · last ${activityRows.length} trade events`} />
      {recent ? (
        activityRows.length === 0 ? (
          <div className="pl-2 text-zinc-500">(no recent trade events)</div>
        ) : (
          <div className="mt-1 pl-2 space-y-0.5">
            {activityRows.map((event, idx) => (
              <V5ActivityRow key={`a:${idx}`} event={event} />
            ))}
          </div>
        )
      ) : (
        <Skeleton label="loading activity" />
      )}

      <div className="border-t border-zinc-800 pt-2 mt-6 text-zinc-600">
        sources /api/scalp/v5/coverage · /gate-state · /deployments · /api/scalp/v4/recent · /api/scalp/v2/ops/research-health · /api/scalp/ops/neon-usage
      </div>
    </PageShell>
  );
}

function fmtBytes(bytes: number | null | undefined): string {
  const n = Number(bytes || 0);
  if (!Number.isFinite(n) || n <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = n;
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  const digits = idx <= 1 ? 0 : value < 10 ? 2 : value < 100 ? 1 : 0;
  return `${value.toFixed(digits)} ${units[idx]}`;
}

function NeonUsageCompact({ data }: { data: NeonUsageResp }) {
  if (!data.configured) {
    return (
      <div className="mt-1 pl-2 text-zinc-500">
        not configured · set <span className="text-zinc-400">NEON_API_KEY</span> and{" "}
        <span className="text-zinc-400">NEON_PROJECT_ID</span>
      </div>
    );
  }
  const project = data.project;
  const usedPct = project?.allowanceUsedPct ?? null;
  const pctLabel = usedPct === null ? "—" : `${usedPct.toFixed(1)}%`;
  const pctClass =
    usedPct === null
      ? "text-zinc-500"
      : usedPct >= 100
        ? "text-rose-400"
        : usedPct >= 80
          ? "text-amber-400"
          : "text-emerald-400";
  const hourlyTotals = data.consumption24h?.hourly.map((row) => row.totalNetworkTransferBytes) || [];
  const hourlyUnavailableLabel = data.consumption24h?.error
    ? "hourly unavailable on this Neon API plan/key"
    : "set NEON_ORG_ID for hourly breakdown";
  return (
    <div className="mt-1 pl-2 grid grid-cols-[auto_1fr] md:grid-cols-[10rem_1fr] gap-x-3 gap-y-0.5 items-baseline">
      <span className="text-zinc-500">billing period</span>
      <span className="flex flex-wrap gap-x-3">
        <span>
          <span className="text-zinc-100">{fmtBytes(project?.dataTransferBytes)}</span>
          <span className="text-zinc-500"> / {project?.allowanceGb?.toFixed(0) ?? "—"} GB</span>
        </span>
        <span className={pctClass}>{pctLabel}</span>
        <span className="text-zinc-500">{project?.name || project?.id || "project"}</span>
        {data.branch?.dataTransferBytes !== null && data.branch?.dataTransferBytes !== undefined ? (
          <span>
            <span className="text-zinc-500">branch </span>
            <span className="text-zinc-300">{fmtBytes(data.branch.dataTransferBytes)}</span>
          </span>
        ) : null}
      </span>
      <span className="text-zinc-500">last 24h</span>
      <span className="flex flex-wrap gap-x-3">
        {data.consumption24h?.available ? (
          <>
            <span>
              <span className="text-zinc-100">{fmtBytes(data.consumption24h.totalNetworkTransferBytes)}</span>
              <span className="text-zinc-500"> total</span>
            </span>
            <span>
              <span className="text-sky-300">{fmtBytes(data.consumption24h.publicNetworkTransferBytes)}</span>
              <span className="text-zinc-500"> public</span>
            </span>
            <span>
              <span className="text-zinc-300">{fmtBytes(data.consumption24h.privateNetworkTransferBytes)}</span>
              <span className="text-zinc-500"> private</span>
            </span>
            <span className="text-emerald-400 font-mono whitespace-pre" title={hourlyTotals.map(fmtBytes).join(" / ")}>
              {sparkline(hourlyTotals)}
            </span>
          </>
        ) : (
          <span className="text-zinc-500" title={data.consumption24h?.error || undefined}>
            {hourlyUnavailableLabel}
          </span>
        )}
      </span>
    </div>
  );
}

// ─── gate chip bar (inline filter strip under deployments header) ────────────

const DEPLOYMENT_SCOPE_LABEL: Record<DeploymentScope, string> = {
  live: "live",
  enabled: "enabled",
  inactive: "inactive",
  all: "all",
};

function DeploymentScopeControls({
  scope,
  page,
  loading,
  error,
  onScope,
  onRefresh,
  onPrev,
  onNext,
}: {
  scope: DeploymentScope;
  page: DeploymentsResp["page"] | null;
  loading: boolean;
  error: string | null;
  onScope: (scope: DeploymentScope) => void;
  onRefresh: () => void;
  onPrev: () => void;
  onNext: () => void;
}) {
  const pageStart = page ? page.offset + 1 : 0;
  const pageEnd = page ? page.offset + page.returned : 0;
  return (
    <div className="mt-1 pl-2 flex flex-wrap items-baseline gap-x-3 gap-y-1 text-[12px]">
      {(["live", "enabled", "inactive", "all"] as DeploymentScope[]).map((s) => (
        <button
          key={s}
          onClick={() => onScope(s)}
          disabled={loading}
          className={`${scope === s ? "text-zinc-100 underline underline-offset-4" : "text-zinc-500 hover:text-zinc-300"} disabled:opacity-50`}
        >
          {DEPLOYMENT_SCOPE_LABEL[s]}
        </button>
      ))}
      <button onClick={onRefresh} disabled={loading} className="text-zinc-500 hover:text-zinc-300 disabled:opacity-50">
        {loading ? "loading" : "refresh deployments"}
      </button>
      {page ? (
        <>
          <span className="text-zinc-600">
            {page.totalMatching === 0 ? "0" : `${pageStart}-${pageEnd}`} / {page.totalMatching}
          </span>
          <button
            onClick={onPrev}
            disabled={loading || page.offset <= 0}
            className="text-zinc-500 hover:text-zinc-300 disabled:opacity-30"
          >
            prev
          </button>
          <button
            onClick={onNext}
            disabled={loading || !page.hasMore}
            className="text-zinc-500 hover:text-zinc-300 disabled:opacity-30"
          >
            next
          </button>
          <span className="text-zinc-600">{page.scope} · limit {page.limit}</span>
        </>
      ) : null}
      {error ? <span className="text-amber-400">{error}</span> : null}
    </div>
  );
}

function GateChipBar({
  stateNowEnabled,
  activeFilter,
  onPick,
}: {
  stateNowEnabled: Record<V5GateDecision, number>;
  activeFilter: V5GateDecision | null;
  onPick: (d: V5GateDecision) => void;
}) {
  return (
    <div className="mt-1 pl-2 flex flex-wrap items-baseline gap-x-4 gap-y-0.5 text-[12px]">
      {DECISION_ORDER.map((decision) => {
        const value = stateNowEnabled[decision] || 0;
        const isActive = activeFilter === decision;
        const dim = activeFilter && !isActive ? "opacity-40" : "";
        return (
          <button
            key={decision}
            onClick={() => onPick(decision)}
            className={`${dim} hover:opacity-100 transition-opacity ${isActive ? "underline underline-offset-4" : ""}`}
            title={`click to ${isActive ? "clear filter" : `filter to ${V5_DECISION_LABEL[decision]}`}`}
          >
            <span className={V5_DECISION_COLOR[decision]}>{V5_DECISION_GLYPH[decision]} {V5_DECISION_LABEL[decision]}</span>
            <span className="text-zinc-100 ml-1">{value}</span>
          </button>
        );
      })}
      {activeFilter ? (
        <button onClick={() => onPick(activeFilter)} className="text-zinc-500 hover:text-zinc-300">
          ↑ clear
        </button>
      ) : null}
    </div>
  );
}

function EvaluatorStrip({ data }: { data: CoverageResp }) {
  const cov = data.coverage;
  const latest = data.evaluator.latestEvaluationMs ? fmtAgo(data.evaluator.latestEvaluationMs) : null;
  const oldest = data.evaluator.oldestEvaluationMs ? fmtAgo(data.evaluator.oldestEvaluationMs) : null;
  return (
    <div className="mt-1 grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 pl-2 items-baseline">
      <span className="text-zinc-500">coverage</span>
      <span className="flex flex-wrap gap-x-3">
        <span>
          <span className="text-zinc-100">{cov.evaluated}</span>
          <span className="text-zinc-500">/</span>
          <span className="text-zinc-100">{cov.totalDeployments}</span>
          <span className="text-zinc-500"> evaluated · </span>
          <span className="text-emerald-400">{cov.enabledDeployments}</span>
          <span className="text-zinc-500"> enabled · </span>
          <span className="text-zinc-400">{cov.totalDeployments - cov.enabledDeployments}</span>
          <span className="text-zinc-500"> inactive</span>
        </span>
        <span>
          <span className={cov.missingEvidence > 0 ? "text-sky-400" : "text-zinc-500"}>{cov.missingEvidence}</span>
          <span className="text-zinc-500"> missing evidence</span>
        </span>
        <span>
          <span className={cov.stale > 0 ? "text-amber-400" : "text-zinc-500"}>{cov.stale}</span>
          <span className="text-zinc-500"> stale</span>
        </span>
      </span>
      <span className="text-zinc-500">runs</span>
      <span className="flex flex-wrap gap-x-3">
        <span>
          <span className="text-zinc-500">last </span>
          <span className="text-zinc-100">{latest ? `${latest} ago` : "—"}</span>
        </span>
        <span>
          <span className="text-zinc-500">oldest </span>
          <span className="text-zinc-100">{oldest ? `${oldest} ago` : "—"}</span>
        </span>
        <span>
          <span className="text-zinc-500">hard gate </span>
          <span className={data.v5HardGateEnabled ? "text-emerald-400" : "text-amber-400"}>
            {data.v5HardGateEnabled ? "ON" : "OFF"}
          </span>
        </span>
      </span>
    </div>
  );
}

// Weekly evaluator progress: bar showing evaluatedThisWeek / total, the
// hourly throughput (with a 12h sparkline), and an ETA until every row is
// re-evaluated this week. Re-evaluation cadence matches the regime week —
// every Monday rollover the staleness threshold kicks back and the worker
// chews through the pool again.
function EvaluatorProgress({ data }: { data: CoverageResp }) {
  const p = data.progress;
  const total = data.coverage.totalDeployments;
  const done = p.evaluatedThisWeek;
  const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;
  const idle = p.ratePerHour === 0;
  const weekLabel = new Date(p.weekStartMs).toISOString().slice(0, 10);
  return (
    <div className="mt-2 pl-2 grid grid-cols-[auto_1fr] md:grid-cols-[10rem_1fr] gap-x-3 items-baseline">
      <span className="text-zinc-500">this week</span>
      <span className="flex flex-wrap items-baseline gap-x-2">
        <span>
          <span className={pct >= 100 ? "text-emerald-400" : "text-zinc-100"}>{done}</span>
          <span className="text-zinc-500">/{total}</span>
        </span>
        <span className={`font-mono whitespace-pre ${pct >= 100 ? "text-emerald-400" : "text-sky-400"}`}>
          {bar(done, total || 1, 30)}
        </span>
        <span className="text-zinc-500">{pct}%</span>
        {pct >= 100 ? <span className="text-emerald-400">✓</span> : null}
        <span className="text-zinc-500 text-[12px]">since {weekLabel}</span>
      </span>
      <span className="text-zinc-500">throughput</span>
      <span className="flex flex-wrap items-baseline gap-x-3">
        <span>
          <span className="text-zinc-100">{p.lastHour}</span>
          <span className="text-zinc-500">/hr last 1h</span>
        </span>
        <span className="text-zinc-500">avg </span>
        <span className="text-zinc-100">{p.ratePerHour.toFixed(1)}</span>
        <span className="text-zinc-500">/hr</span>
        <span className="text-zinc-500">12h </span>
        <span
          className="text-emerald-400 font-mono whitespace-pre"
          title={p.buckets12h.join(" / ")}
        >
          {sparkline(p.buckets12h)}
        </span>
      </span>
      <span className="text-zinc-500">eta</span>
      <span>
        {idle ? (
          <>
            <span className="text-amber-400">idle</span>
            <span className="text-zinc-500"> · {p.remainingThisWeek} deployments still to evaluate this week</span>
          </>
        ) : p.remainingThisWeek === 0 ? (
          <span className="text-emerald-400">done · all deployments evaluated this week</span>
        ) : (
          <>
            <span className="text-zinc-100">{fmtEta(p.etaHours)}</span>
            <span className="text-zinc-500"> at {p.ratePerHour.toFixed(1)}/hr · {p.remainingThisWeek} remaining</span>
          </>
        )}
      </span>
    </div>
  );
}

function fmtDurationMs(ms: number | null | undefined): string {
  const n = Number(ms);
  if (ms === null || ms === undefined || !Number.isFinite(n) || n < 0) return "—";
  if (n < 60_000) return `${Math.max(0, Math.floor(n / 1000))}s`;
  const minutes = Math.floor(n / 60_000);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours}h`;
  return `${Math.floor(hours / 24)}d`;
}

function ResearchProgress({ data }: { data: ResearchHealthResp }) {
  const n = (value: unknown) => Math.max(0, Math.floor(Number(value) || 0));
  const q = data.queue || {
    total: 0,
    processed: 0,
    discovered: 0,
    evaluated: 0,
    promoted: 0,
    rejected: 0,
  };
  const total = Math.max(0, Math.floor(Number(q.total || 0)));
  const done = Math.max(0, Math.floor(Number(q.processed || 0)));
  const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;
  const job = data.job;
  const batch = job?.progress || null;
  const batchTotal = n(batch?.totalSelected);
  const batchDone = n(batch?.processedSoFar);
  const batchPct = batchTotal > 0 ? Math.min(100, Math.round((batchDone / batchTotal) * 100)) : 0;
  const stageTotal = n(batch?.workerStageTotal);
  const stageDone = n(batch?.workerStageProcessed);
  const stagePct = stageTotal > 0 ? Math.min(100, Math.round((stageDone / stageTotal) * 100)) : 0;
  const skipped =
    n(batch?.skippedByCache) +
    n(batch?.skippedByClearFail) +
    n(batch?.skippedByNetRPreFilter) +
    n(batch?.smartSkippedPersisted) +
    n(batch?.surrogateSkippedPersisted);
  const replayTotal =
    n(batch?.fullStageReplays) +
    n(batch?.incrementalStageReplays) +
    n(batch?.cachedStageReuses) +
    n(batch?.newestWeekReplayReuses);
  const cacheTotal = n(batch?.stageBCacheHits) + n(batch?.stageCCacheHits);
  const errors = n(batch?.replayErrors) + n(batch?.persistErrors);
  const status = String(job?.status || "unknown").toLowerCase();
  const statusClass =
    data.health.stale || data.hint.tone === "critical"
      ? "text-rose-400"
      : data.health.approachingStale || data.hint.tone === "warn"
        ? "text-amber-400"
        : status === "running" || data.hint.tone === "ok"
          ? "text-emerald-400"
          : "text-zinc-400";
  return (
    <div className="mt-2 pl-2 grid grid-cols-[auto_1fr] md:grid-cols-[10rem_1fr] gap-x-3 items-baseline">
      <span className="text-zinc-500">queue</span>
      <span className="flex flex-wrap items-baseline gap-x-2">
        <span>
          <span className={pct >= 100 ? "text-emerald-400" : "text-zinc-100"}>{done}</span>
          <span className="text-zinc-500">/{total}</span>
        </span>
        <span className={`font-mono whitespace-pre ${pct >= 100 ? "text-emerald-400" : "text-sky-400"}`}>
          {bar(done, total || 1, 30)}
        </span>
        <span className="text-zinc-500">{pct}%</span>
        <span className="text-zinc-500">pending </span>
        <span className="text-zinc-100">{q.discovered}</span>
        <span className="text-zinc-500">rejected </span>
        <span className="text-zinc-100">{q.rejected}</span>
        <span className="text-zinc-500">evaluated </span>
        <span className="text-zinc-100">{q.evaluated}</span>
        <span className="text-zinc-500">promoted </span>
        <span className="text-zinc-100">{q.promoted}</span>
      </span>

      <span className="text-zinc-500">worker</span>
      <span className="flex flex-wrap items-baseline gap-x-3">
        <span className={statusClass}>{status}</span>
        <span>
          <span className="text-zinc-500">phase </span>
          <span className="text-zinc-100">{job?.phase || "—"}</span>
        </span>
        <span>
          <span className="text-zinc-500">reason </span>
          <span className="text-zinc-100">{job?.reason || "—"}</span>
        </span>
        <span>
          <span className="text-zinc-500">updated </span>
          <span className="text-zinc-100">{fmtAgo(job?.updatedAtMs)}</span>
        </span>
      </span>

      <span className="text-zinc-500">batch</span>
      <span className="flex flex-wrap items-baseline gap-x-2">
        {batch ? (
          <>
            <span>
              <span className={batchPct >= 100 ? "text-emerald-400" : "text-zinc-100"}>{batchDone}</span>
              <span className="text-zinc-500">/{batchTotal}</span>
            </span>
            <span className={`font-mono whitespace-pre ${batchPct >= 100 ? "text-emerald-400" : "text-sky-400"}`}>
              {bar(batchDone, batchTotal || 1, 20)}
            </span>
            <span className="text-zinc-500">{batchPct}%</span>
            {batch.workerStage ? (
              <>
                <span className="text-zinc-500">stage </span>
                <span className="text-zinc-100">{batch.workerStage}</span>
                <span className="text-zinc-500">{stageDone}/{stageTotal}</span>
                <span className="text-zinc-500">{stagePct}%</span>
              </>
            ) : null}
            <span className="text-zinc-500">persisted </span>
            <span className="text-zinc-100">{batch.persisted}</span>
            <span className={errors > 0 ? "text-rose-400" : "text-zinc-500"}>
              errors {errors}
            </span>
            {batch.timeBudgetExhausted ? <span className="text-amber-400">budget hit</span> : null}
            {skipped > 0 ? (
              <>
                <span className="text-zinc-500">skipped </span>
                <span className="text-zinc-100">{skipped}</span>
              </>
            ) : null}
          </>
        ) : (
          <span className="text-zinc-500">no current batch</span>
        )}
      </span>

      <span className="text-zinc-500">stage gates</span>
      <span className="flex flex-wrap items-baseline gap-x-3">
        {batch ? (
          <>
            <span>
              <span className="text-zinc-500">A </span>
              <span className="text-emerald-400">{n(batch.stageAPass)}</span>
              <span className="text-zinc-500">/</span>
              <span className={n(batch.stageAFail) > 0 ? "text-rose-400" : "text-zinc-500"}>{n(batch.stageAFail)}</span>
            </span>
            <span>
              <span className="text-zinc-500">B </span>
              <span className="text-emerald-400">{n(batch.stageBPass)}</span>
              <span className="text-zinc-500">/</span>
              <span className={n(batch.stageBFail) > 0 ? "text-rose-400" : "text-zinc-500"}>{n(batch.stageBFail)}</span>
            </span>
            <span>
              <span className="text-zinc-500">C </span>
              <span className="text-emerald-400">{n(batch.stageCPass)}</span>
              <span className="text-zinc-500">/</span>
              <span className={n(batch.stageCFail) > 0 ? "text-rose-400" : "text-zinc-500"}>{n(batch.stageCFail)}</span>
            </span>
            <span className="text-zinc-500">pass/fail</span>
          </>
        ) : (
          <span className="text-zinc-500">no stage data</span>
        )}
      </span>

      <span className="text-zinc-500">replay</span>
      <span className="flex flex-wrap items-baseline gap-x-3">
        {batch ? (
          <>
            <span>
              <span className="text-zinc-500">runs </span>
              <span className="text-zinc-100">{replayTotal}</span>
            </span>
            <span>
              <span className="text-zinc-500">full </span>
              <span className="text-zinc-100">{n(batch.fullStageReplays)}</span>
            </span>
            <span>
              <span className="text-zinc-500">incr </span>
              <span className="text-zinc-100">{n(batch.incrementalStageReplays)}</span>
            </span>
            <span>
              <span className="text-zinc-500">cached </span>
              <span className="text-zinc-100">{n(batch.cachedStageReuses)}</span>
            </span>
            <span>
              <span className="text-zinc-500">stage0 </span>
              <span className="text-zinc-100">{n(batch.stage0Replays)}</span>
              <span className="text-zinc-500">/</span>
              <span className={n(batch.stage0Skipped) > 0 ? "text-amber-400" : "text-zinc-500"}>{n(batch.stage0Skipped)}</span>
            </span>
            <span>
              <span className="text-zinc-500">early abort </span>
              <span className={n(batch.earlyAbortedStageReplays) > 0 ? "text-amber-400" : "text-zinc-500"}>{n(batch.earlyAbortedStageReplays)}</span>
            </span>
          </>
        ) : (
          <span className="text-zinc-500">no replay data</span>
        )}
      </span>

      <span className="text-zinc-500">filters</span>
      <span className="flex flex-wrap items-baseline gap-x-3">
        {batch ? (
          <>
            <span>
              <span className="text-zinc-500">cache </span>
              <span className="text-zinc-100">{n(batch.skippedByCache)}</span>
            </span>
            <span>
              <span className="text-zinc-500">clear-fail </span>
              <span className="text-zinc-100">{n(batch.skippedByClearFail)}</span>
            </span>
            <span>
              <span className="text-zinc-500">netR </span>
              <span className="text-zinc-100">{n(batch.skippedByNetRPreFilter)}</span>
            </span>
            <span>
              <span className="text-zinc-500">smart </span>
              <span className="text-zinc-100">{n(batch.smartSkippedPersisted)}</span>
            </span>
            <span>
              <span className="text-zinc-500">surrogate </span>
              <span className="text-zinc-100">{n(batch.surrogateSkippedPersisted)}</span>
            </span>
            <span>
              <span className="text-zinc-500">cache hits B/C </span>
              <span className="text-zinc-100">{cacheTotal}</span>
            </span>
            <span>
              <span className="text-zinc-500">coverage defers </span>
              <span className={n(batch.deferredByCandleCoverage) > 0 ? "text-amber-400" : "text-zinc-500"}>{n(batch.deferredByCandleCoverage)}</span>
              <span className="text-zinc-500">/</span>
              <span className={n(batch.finalizedCoverageDeferrals) > 0 ? "text-amber-400" : "text-zinc-500"}>{n(batch.finalizedCoverageDeferrals)}</span>
            </span>
          </>
        ) : (
          <span className="text-zinc-500">no filter data</span>
        )}
      </span>

      <span className="text-zinc-500">health</span>
      <span className="flex flex-wrap items-baseline gap-x-3" title={data.hint.detail || undefined}>
        <span className={statusClass}>{data.hint.label}</span>
        <span>
          <span className="text-zinc-500">heartbeat </span>
          <span className="text-zinc-100">{fmtDurationMs(data.health.heartbeatAgeMs)}</span>
        </span>
        <span>
          <span className="text-zinc-500">lock </span>
          <span className="text-zinc-100">{fmtDurationMs(data.health.lockAgeMs)}</span>
        </span>
        {data.hint.detail ? <span className="text-zinc-500">{data.hint.detail}</span> : null}
      </span>

      <span className="text-zinc-500">robustness</span>
      <span className="flex flex-wrap items-baseline gap-x-3">
        <span>
          <span className="text-zinc-100">{data.robustness?.stageCPassed ?? 0}</span>
          <span className="text-zinc-500"> stageC finalists</span>
        </span>
        <span>
          <span className={(data.robustness?.missing ?? 0) > 0 ? "text-amber-400" : "text-zinc-500"}>
            {data.robustness?.missing ?? 0}
          </span>
          <span className="text-zinc-500"> pending</span>
        </span>
        <span>
          <span className="text-emerald-400">{data.robustness?.passed ?? 0}</span>
          <span className="text-zinc-500"> passed</span>
        </span>
        <span>
          <span className={(data.robustness?.failed ?? 0) > 0 ? "text-rose-400" : "text-zinc-500"}>
            {data.robustness?.failed ?? 0}
          </span>
          <span className="text-zinc-500"> failed</span>
        </span>
      </span>
    </div>
  );
}

// Candle freshness panel: surfaces symbols whose 1m candle history hasn't
// been updated recently by the load-candles cron. Cron runs every 2h so
// "fresh" is <4h since last write; 4-12h is "lagging" (one missed tick);
// >12h is "broken" (cron is failing on this symbol). Missing means no
// candles at all — usually a new symbol that hasn't been backfilled yet.
function CandleHealth({ data }: { data: CoverageResp }) {
  const ch = data.candleHealth;
  const anyProblem = ch.lagging + ch.broken + ch.missing > 0;
  const worstTooltip = ch.worstSymbols.length === 0
    ? "all enabled deployments have fresh candles"
    : ch.worstSymbols
        .map((s) => `${s.venue}/${s.symbol}: ${s.ageMinutes === null ? "no candles" : `${s.ageMinutes}m ago`}`)
        .join("\n");
  return (
    <div className="mt-2 pl-2 grid grid-cols-[auto_1fr] md:grid-cols-[10rem_1fr] gap-x-3 items-baseline">
      <span className="text-zinc-500">candle health</span>
      <span className="flex flex-wrap items-baseline gap-x-3" title={worstTooltip}>
        <span>
          <span className="text-emerald-400">{ch.fresh}</span>
          <span className="text-zinc-500"> fresh</span>
        </span>
        <span>
          <span className={ch.lagging > 0 ? "text-amber-400" : "text-zinc-500"}>{ch.lagging}</span>
          <span className="text-zinc-500"> lagging (4-12h)</span>
        </span>
        <span>
          <span className={ch.broken > 0 ? "text-rose-400" : "text-zinc-500"}>{ch.broken}</span>
          <span className="text-zinc-500"> broken (&gt;12h)</span>
        </span>
        <span>
          <span className={ch.missing > 0 ? "text-rose-400" : "text-zinc-500"}>{ch.missing}</span>
          <span className="text-zinc-500"> missing</span>
        </span>
        {anyProblem && ch.worstSymbols.length > 0 ? (
          <span className="text-zinc-500 text-[12px]">
            worst:{" "}
            <span className="text-zinc-300">
              {ch.worstSymbols
                .slice(0, 3)
                .map((s) => `${s.symbol}${s.ageMinutes === null ? "(none)" : `(${s.ageMinutes}m)`}`)
                .join(", ")}
              {ch.worstSymbols.length > 3 ? ` +${ch.worstSymbols.length - 3}` : ""}
            </span>
          </span>
        ) : null}
      </span>
    </div>
  );
}

// ─── deployment row ──────────────────────────────────────────────────────────

function V5DeploymentRowView({
  row,
  expanded,
  onToggle,
  sessionActive,
}: {
  row: V5DeploymentRow;
  expanded: boolean;
  onToggle: () => void;
  sessionActive: boolean;
}) {
  const family = row.tuneId.split("_").slice(0, 4).join("_");
  const decision = row.gate.decision;
  const sessionClass = sessionActive ? "text-emerald-400" : "text-zinc-500";
  const currentR = row.gate.currentCellStat?.expectancyR ?? null;
  const currentN = row.gate.currentCellStat?.trades ?? null;
  const currentExpStr =
    currentR === null ? "—" : `${currentR >= 0 ? "+" : ""}${currentR.toFixed(2)}R${currentN !== null ? `(${currentN}t)` : ""}`;
  const netRStr = `${row.totalNetR >= 0 ? "+" : ""}${row.totalNetR.toFixed(2)}R`;
  const netRColor =
    row.totalNetR > 0 ? "text-emerald-400" : row.totalNetR < 0 ? "text-rose-400" : "text-zinc-500";
  const decisionGlyph = V5_DECISION_GLYPH[decision];
  const decisionLabel = V5_DECISION_LABEL[decision];
  const decisionColor = V5_DECISION_COLOR[decision];
  const dim = !row.enabled ? "opacity-60" : "";
  const enabledMark = row.enabled ? (
    <span className="text-emerald-400" title="enabled">●</span>
  ) : (
    <span className="text-zinc-600" title="inactive">○</span>
  );
  const maxAbs = useMemo(() => {
    let m = 0;
    for (const c of row.cells) {
      for (const v of c.weeklyNetR) {
        const abs = Math.abs(v);
        if (abs > m) m = abs;
      }
    }
    return m;
  }, [row.cells]);
  // Deployment-wide running NetR across all cells — overlaid as a single
  // trajectory line on every cell's track so the chart tells one coherent
  // story instead of N independent per-cell trajectories.
  const deploymentCumulative = useMemo(() => {
    const weekCount = row.cells.reduce((m, c) => Math.max(m, c.weeklyNetR.length), 0);
    const weeklySum = new Array<number>(weekCount).fill(0);
    for (const c of row.cells) {
      for (let i = 0; i < c.weeklyNetR.length; i++) {
        const v = c.weeklyNetR[i];
        weeklySum[i] += Number.isFinite(v) ? v : 0;
      }
    }
    const cum: number[] = [];
    let running = 0;
    for (const v of weeklySum) {
      running += v;
      cum.push(running);
    }
    return cum;
  }, [row.cells]);

  return (
    <div className={dim}>
      <button
        onClick={onToggle}
        className="w-full text-left hover:bg-zinc-900/40 -mx-2 px-2 py-0.5 rounded-sm transition-colors"
        title={expanded ? "click to collapse" : "click to expand cell detail"}
      >
        <div className="md:hidden">
          <div className="flex items-baseline gap-x-2">
            {enabledMark}
            <span className={decisionColor}>{decisionGlyph}</span>
            <span className="text-zinc-100 truncate">
              <span className="text-zinc-500">{row.venue}/</span>
              {row.symbol}
            </span>
            <span className={`${sessionClass} text-[12px]`} title={sessionActive ? "session active now" : undefined}>{row.session}</span>
            <span className={`${decisionColor} text-[12px]`}>{decisionLabel}</span>
            <span className={`${netRColor} text-[12px] ml-auto`} title="total 12w netR across all cells">
              {netRStr}
            </span>
            <span className="text-zinc-500">{expanded ? "▾" : "▸"}</span>
          </div>
          <div className="pl-5 text-[12px] text-zinc-400 truncate" title={row.tuneId}>
            {family}
          </div>
          <div className="pl-5 text-[12px]">
            <span className="text-zinc-500">curr </span>
            <span className="text-amber-300">{abbrCell(row.currentCell.cellId)}</span>
            <span className="text-zinc-500"> · E </span>
            <span className={currentR === null ? "text-zinc-500" : currentR >= 0 ? "text-emerald-400" : "text-rose-400"}>
              {currentExpStr}
            </span>
          </div>
        </div>
        <div className="hidden md:grid md:grid-cols-[1rem_1.25rem_4rem_8rem_5rem_5rem_6rem_7rem_1fr_1rem] md:gap-x-2 md:items-baseline">
          {enabledMark}
          <span className={decisionColor}>{decisionGlyph}</span>
          <span className="text-zinc-500">{row.venue}</span>
          <span className="text-zinc-100 truncate">{row.symbol}</span>
          <span className={sessionClass} title={sessionActive ? "session active now" : undefined}>{row.session}</span>
          <span className={`${decisionColor} text-[13px] truncate`} title={decision}>
            {decisionLabel}
          </span>
          <span
            className={`${netRColor} text-right`}
            title="total 12w netR across all cells"
          >
            {netRStr}
          </span>
          <span
            className={currentR === null ? "text-zinc-500" : currentR >= 0 ? "text-emerald-400" : "text-rose-400"}
            title={`current cell expectancy${currentN !== null ? ` · ${currentN} trades` : ""}`}
          >
            {currentExpStr}
          </span>
          <span className="text-amber-300 truncate" title={row.currentCell.cellId || "no cell"}>
            {abbrCell(row.currentCell.cellId)}
          </span>
          <span className="text-zinc-500 text-right">{expanded ? "▾" : "▸"}</span>
        </div>
      </button>
      {expanded ? (
        <div className="pl-5 md:pl-7 mt-1 mb-3 border-l border-zinc-800/60 ml-2 md:ml-3 pl-3 space-y-2">
          <div className="text-[12px] text-zinc-500 flex flex-wrap gap-x-3">
            <span>
              evaluated <span className="text-zinc-300">{row.v5EvaluatedAtMs ? fmtAgo(row.v5EvaluatedAtMs) + " ago" : "—"}</span>
            </span>
            <span>
              trades <span className="text-zinc-300">{row.totalTrades}</span>
            </span>
            <span>
              cells <span className="text-zinc-300">{row.cells.length}</span>
            </span>
            {row.holdoutWindow ? (
              <span>
                holdout{" "}
                <span className="text-zinc-300">
                  {new Date(row.holdoutWindow.fromMs).toISOString().slice(0, 10)}..
                  {new Date(row.holdoutWindow.toMs).toISOString().slice(0, 10)}
                </span>
              </span>
            ) : null}
            <span className="truncate" title={row.deploymentId}>
              id <span className="text-zinc-400">{row.deploymentId}</span>
            </span>
          </div>
          {row.cells.length === 0 ? (
            <div className="text-zinc-500 text-[12px]">(no cell evidence — evaluator hasn&apos;t run on this row)</div>
          ) : (
            <div className="space-y-1.5">
              {row.cells.map((cell) => (
                <CellEvidenceRow
                  key={cell.cellId}
                  cell={cell}
                  maxAbs={maxAbs}
                  holdoutFromMs={row.holdoutWindow?.fromMs ?? null}
                  deploymentCumulative={deploymentCumulative}
                />
              ))}
            </div>
          )}
          {row.gate.eligibleCells.length > 0 ? (
            <div className="text-[12px] text-zinc-500">
              eligible cells: <span className="text-emerald-400">{row.gate.eligibleCells.map(abbrCell).join(", ")}</span>
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

function CellEvidenceRow({
  cell,
  maxAbs,
  holdoutFromMs,
  deploymentCumulative,
}: {
  cell: V5CellRow;
  maxAbs: number;
  holdoutFromMs: number | null;
  deploymentCumulative: number[];
}) {
  const expR = cell.expectancyR;
  const expColor = expR > 0 ? "text-emerald-400" : expR < 0 ? "text-rose-400" : "text-zinc-300";
  const netColor = cell.netR > 0 ? "text-emerald-400" : cell.netR < 0 ? "text-rose-400" : "text-zinc-300";
  const ONE_WEEK = 7 * 24 * 60 * 60_000;
  const weekLabel = useCallback(
    (idx: number, value: number) => {
      if (holdoutFromMs === null) {
        return `w${idx + 1}: ${value >= 0 ? "+" : ""}${value.toFixed(2)}R`;
      }
      const weekStart = new Date(holdoutFromMs + idx * ONE_WEEK).toISOString().slice(0, 10);
      return `${weekStart}: ${value >= 0 ? "+" : ""}${value.toFixed(2)}R`;
    },
    [holdoutFromMs],
  );
  return (
    <div className="grid grid-cols-[1rem_1fr] md:grid-cols-[1rem_18rem_1fr] gap-x-2 items-start">
      <span className={cell.isCurrent ? "text-amber-300 mt-1" : "text-zinc-500 mt-1"}>
        {cell.isCurrent ? "▶" : "·"}
      </span>
      <div className="flex flex-col gap-y-0.5">
        <span
          className={cell.isCurrent ? "text-amber-300 truncate text-[13px]" : "text-zinc-300 truncate text-[13px]"}
          title={cell.cellId}
        >
          {abbrCell(cell.cellId)}
          {cell.isCurrent ? <span className="ml-1 text-[11px] text-amber-400/70">CURRENT</span> : null}
        </span>
        <div className="flex flex-wrap gap-x-2 text-[12px] text-zinc-500">
          <span>
            trades <span className="text-zinc-300">{cell.trades}</span>
          </span>
          <span>
            E/tr{" "}
            <span className={expColor}>
              {expR >= 0 ? "+" : ""}
              {expR.toFixed(3)}R
            </span>
          </span>
          <span>
            netR{" "}
            <span className={netColor}>
              {cell.netR >= 0 ? "+" : ""}
              {cell.netR.toFixed(2)}R
            </span>
          </span>
          <span>
            w/l <span className="text-zinc-300">{cell.wins}</span>/<span className="text-zinc-300">{cell.losses}</span>
          </span>
        </div>
      </div>
      <div className="col-span-2 md:col-span-1 md:max-w-[420px]">
        <WeeklyNetRTrack
          values={cell.weeklyNetR}
          globalMaxAbs={maxAbs}
          cumulativeOverride={deploymentCumulative}
          weekLabel={weekLabel}
        />
      </div>
    </div>
  );
}

function V5ActivityRow({ event }: { event: TradeRow }) {
  const timeMs = event.tsMs;
  const symbol = `${event.venue || "?"}/${event.symbol || "?"}`;
  const v5Reason = event.reasonCodes.find((c) => c.startsWith("V5_"));
  let marker: string;
  let markerClass: string;
  let detail: string;
  let detailClass: string;
  let tail = "";
  let tailClass = "text-zinc-500";

  if (event.eventKind === "trade" || event.eventKind === "trade_open" || event.eventKind === "trade_close") {
    marker =
      event.eventKind === "trade_open"
        ? "OPEN"
        : event.eventKind === "trade_close"
          ? "CLOSE"
          : "TRADE";
    const tone =
      event.rMultiple !== null ? (event.rMultiple > 0 ? "text-emerald-400" : "text-rose-400") : "text-zinc-200";
    markerClass = tone;
    detail = event.summary || event.state;
    detailClass = tone;
    tail = event.rMultiple !== null ? (event.rMultiple > 0 ? `+${event.rMultiple.toFixed(2)}R` : `${event.rMultiple.toFixed(2)}R`) : "";
    tailClass = tone;
  } else if (event.eventKind === "entry_error") {
    marker = "ENTRY✗";
    markerClass = "text-rose-400";
    detail = "execution error";
    detailClass = "text-rose-400";
  } else if (event.eventKind === "entry_skipped") {
    let decision: V5GateDecision | null = null;
    if (v5Reason === "V5_CELL_NEGATIVE_EXPECTANCY") decision = "block_negative";
    else if (v5Reason === "V5_CELL_NOT_IN_EVIDENCE") decision = "block_unseen";
    else if (v5Reason === "V5_CELL_DATA_STALE") decision = "block_stale";
    else if (v5Reason === "V5_CELL_EVIDENCE_MISSING") decision = "block_evaluator_pending";
    else if (v5Reason === "V5_CELL_INSUFFICIENT_TRADES") decision = "block_insufficient_trades";
    const tone = decision ? V5_DECISION_COLOR[decision] : "text-amber-400";
    const tag = decision ? V5_DECISION_GLYPH[decision] + " " + V5_DECISION_LABEL[decision] : "skipped";
    marker = "ENTRY-";
    markerClass = tone;
    detail = tag;
    detailClass = tone;
    if (v5Reason) tail = v5Reason.replace(/^V5_/, "");
  } else {
    marker = "STATE";
    markerClass = "text-zinc-400";
    detail = event.state || "state change";
    detailClass = "text-zinc-400";
  }
  return (
    <div>
      <div className="md:hidden">
        <div className="flex items-baseline gap-x-2">
          <span className="text-zinc-500 w-[2.75rem] shrink-0">{fmtClock(timeMs)}</span>
          <span className={`${markerClass} w-[3.5rem] shrink-0`}>{marker}</span>
          <span className="text-zinc-100 truncate">{symbol}</span>
        </div>
        <div className={`pl-[6.5rem] text-[12px] truncate ${detailClass}`} title={detail}>
          {detail}
          {tail ? <span className={`ml-2 ${tailClass}`}>{tail}</span> : null}
        </div>
      </div>
      <div className="hidden md:grid md:grid-cols-[3rem_3.5rem_14rem_1fr_8rem] md:gap-x-2 md:items-baseline">
        <span className="text-zinc-500">{fmtClock(timeMs)}</span>
        <span className={markerClass}>{marker}</span>
        <span className="text-zinc-100 truncate">{symbol}</span>
        <span className={`${detailClass} truncate text-[13px]`}>{detail}</span>
        <span className={`${tailClass} text-[13px] truncate text-right`} title={tail}>
          {tail}
        </span>
      </div>
    </div>
  );
}

// ─── session timeline ────────────────────────────────────────────────────────

// Sessions are 4h windows defined in local time (DST-aware via Intl). UTC
// window labels are derived per render so they stay correct across DST shifts.
// Kept in sync with lib/scalp/sessions.ts — five 4h profiles.
type SessionName = "tokyo" | "berlin" | "newyork" | "pacific" | "sydney";

const SESSION_PROFILES: ReadonlyArray<{
  name: SessionName;
  timeZone: string;
  startMin: number;
  endMin: number;
}> = [
  { name: "tokyo", timeZone: "Asia/Tokyo", startMin: 9 * 60, endMin: 13 * 60 },
  { name: "berlin", timeZone: "Europe/Berlin", startMin: 8 * 60, endMin: 12 * 60 },
  { name: "newyork", timeZone: "America/New_York", startMin: 8 * 60, endMin: 12 * 60 },
  { name: "pacific", timeZone: "America/Los_Angeles", startMin: 10 * 60, endMin: 14 * 60 },
  { name: "sydney", timeZone: "Australia/Sydney", startMin: 8 * 60, endMin: 12 * 60 },
];

function minuteOfDayInTz(tsMs: number, tz: string): number {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: tz,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(new Date(tsMs));
  const hh = Number(parts.find((p) => p.type === "hour")?.value ?? 0);
  const mm = Number(parts.find((p) => p.type === "minute")?.value ?? 0);
  return hh * 60 + mm;
}

function fmtClockHM(min: number): string {
  const norm = ((min % 1440) + 1440) % 1440;
  const hh = Math.floor(norm / 60);
  const mm = norm % 60;
  return `${String(hh).padStart(2, "0")}:${String(mm).padStart(2, "0")}`;
}

function fmtDurShort(min: number): string {
  if (min <= 0) return "0m";
  if (min < 60) return `${min}m`;
  const h = Math.floor(min / 60);
  const m = min % 60;
  return m === 0 ? `${h}h` : `${h}h${String(m).padStart(2, "0")}m`;
}

interface SessionRowState {
  name: SessionName;
  utcLabel: string;
  active: boolean;
  // Status frames the most relevant time fact: time left in active session,
  // "in X" for the next open, or "closed Xm ago" if it just ended.
  status: string;
  statusTone: "active" | "soon" | "just-closed" | "later";
}

function computeSessionRow(
  profile: { name: SessionName; timeZone: string; startMin: number; endMin: number },
  nowMs: number,
): SessionRowState {
  const localMoD = minuteOfDayInTz(nowMs, profile.timeZone);
  const utcMoD =
    new Date(nowMs).getUTCHours() * 60 + new Date(nowMs).getUTCMinutes();
  let offset = localMoD - utcMoD;
  if (offset > 720) offset -= 1440;
  if (offset < -720) offset += 1440;
  const utcStart = ((profile.startMin - offset) % 1440 + 1440) % 1440;
  const utcEnd = ((profile.endMin - offset) % 1440 + 1440) % 1440;
  const utcLabel = `${fmtClockHM(utcStart)}–${fmtClockHM(utcEnd)} UTC`;

  const inWindow =
    profile.endMin > profile.startMin
      ? localMoD >= profile.startMin && localMoD < profile.endMin
      : localMoD >= profile.startMin || localMoD < profile.endMin;

  if (inWindow) {
    const minutesLeft =
      profile.endMin > profile.startMin
        ? profile.endMin - localMoD
        : localMoD >= profile.startMin
          ? 1440 - localMoD + profile.endMin
          : profile.endMin - localMoD;
    return {
      name: profile.name,
      utcLabel,
      active: true,
      status: `${fmtDurShort(minutesLeft)} left`,
      statusTone: "active",
    };
  }

  let minutesUntilOpen: number;
  let minutesSinceClose: number;
  if (localMoD < profile.startMin) {
    minutesUntilOpen = profile.startMin - localMoD;
    minutesSinceClose = 1440 - profile.endMin + localMoD;
  } else {
    minutesUntilOpen = 1440 - localMoD + profile.startMin;
    minutesSinceClose = localMoD - profile.endMin;
  }

  if (minutesSinceClose < 240) {
    return {
      name: profile.name,
      utcLabel,
      active: false,
      status: `closed ${fmtDurShort(minutesSinceClose)} ago`,
      statusTone: "just-closed",
    };
  }
  return {
    name: profile.name,
    utcLabel,
    active: false,
    status: `in ${fmtDurShort(minutesUntilOpen)}`,
    statusTone: minutesUntilOpen < 120 ? "soon" : "later",
  };
}

// Axis is centered on NOW with ±12h on each side (24h total span). Each
// session's nearest occurrence is clipped to this window and rendered as a
// percentage offset from the left edge.
const SPAN_MINUTES = 1440;

function computeRelativeBands(
  profile: { startMin: number; endMin: number; timeZone: string },
  nowMs: number,
): Array<{ leftPct: number; widthPct: number }> {
  const localMoD = minuteOfDayInTz(nowMs, profile.timeZone);
  const duration =
    profile.endMin > profile.startMin
      ? profile.endMin - profile.startMin
      : 1440 - profile.startMin + profile.endMin;
  const deltaToStart = profile.startMin - localMoD;
  const todayStartMs = nowMs + deltaToStart * 60_000;
  const windowStartMs = nowMs - (SPAN_MINUTES / 2) * 60_000;
  const windowEndMs = nowMs + (SPAN_MINUTES / 2) * 60_000;
  const spanMs = windowEndMs - windowStartMs;

  const bands: Array<{ leftPct: number; widthPct: number }> = [];
  for (const shiftDays of [-1, 0, 1]) {
    const candStart = todayStartMs + shiftDays * 24 * 60 * 60_000;
    const candEnd = candStart + duration * 60_000;
    if (candEnd <= windowStartMs || candStart >= windowEndMs) continue;
    const clippedStart = Math.max(candStart, windowStartMs);
    const clippedEnd = Math.min(candEnd, windowEndMs);
    bands.push({
      leftPct: ((clippedStart - windowStartMs) / spanMs) * 100,
      widthPct: ((clippedEnd - clippedStart) / spanMs) * 100,
    });
  }
  return bands;
}

// ─── week NetR strip ─────────────────────────────────────────────────────────

// Sunday is the rollover/evaluation day — no live trading happens, so the
// week strip shows Mon–Sat with a 7th cell that rolls the week total up.
const WEEK_DOW_LABELS = ["mon", "tue", "wed", "thu", "fri", "sat"] as const;
const DAY_MS = 24 * 60 * 60_000;

function WeekNetRTrack({
  dailyNetR,
  recentTrades,
}: {
  dailyNetR: DailyNetRRow[] | null;
  recentTrades: TradeRow[] | null;
}) {
  const [now, setNow] = useState<number>(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 60_000);
    return () => clearInterval(id);
  }, []);

  const days = useMemo(() => {
    const today = new Date(now);
    const utcDow = today.getUTCDay(); // 0=Sun..6=Sat
    const daysSinceMon = (utcDow + 6) % 7;
    const mondayMidnightMs = Date.UTC(
      today.getUTCFullYear(),
      today.getUTCMonth(),
      today.getUTCDate() - daysSinceMon,
    );
    return Array.from({ length: 6 }, (_, i) => {
      const startMs = mondayMidnightMs + i * DAY_MS;
      return {
        startMs,
        endMs: startMs + DAY_MS,
        dow: WEEK_DOW_LABELS[i],
        date: new Date(startMs).getUTCDate(),
        isToday: now >= startMs && now < startMs + DAY_MS,
        isFuture: startMs > now,
      };
    });
  }, [now]);

  const aggByDay = useMemo(() => {
    const byDay = new Map<number, { netR: number; trades: number }>();
    if (dailyNetR && dailyNetR.length > 0) {
      for (const row of dailyNetR) {
        const dayStart =
          Number.isFinite(row.dayStartMs) && row.dayStartMs > 0
            ? Math.floor(row.dayStartMs / DAY_MS) * DAY_MS
            : Date.parse(`${row.dayKey}T00:00:00.000Z`);
        if (!Number.isFinite(dayStart)) continue;
        byDay.set(dayStart, {
          netR: Number.isFinite(row.netR) ? row.netR : 0,
          trades: Number.isFinite(row.trades) ? row.trades : 0,
        });
      }
    }
    if (!recentTrades) return byDay;
    const recentByDay = new Map<number, { netR: number; trades: number }>();
    for (const t of recentTrades) {
      // The ledger-sourced closes carry the realized rMultiple as "trade_close".
      // Journal "trade" rows are usually opens (rMultiple null) but accept them
      // too if they happen to carry a value, so we don't silently drop data.
      if (t.eventKind !== "trade_close" && t.eventKind !== "trade") continue;
      if (t.rMultiple == null || !Number.isFinite(t.rMultiple)) continue;
      const dayStart = Math.floor(t.tsMs / DAY_MS) * DAY_MS;
      const prev = recentByDay.get(dayStart) || { netR: 0, trades: 0 };
      recentByDay.set(dayStart, { netR: prev.netR + t.rMultiple, trades: prev.trades + 1 });
    }
    for (const [dayStart, recentAgg] of recentByDay) {
      const dailyAgg = byDay.get(dayStart);
      if (!dailyAgg || recentAgg.trades > dailyAgg.trades) {
        byDay.set(dayStart, recentAgg);
      }
    }
    return byDay;
  }, [dailyNetR, recentTrades]);

  const weekTotal = days.reduce(
    (acc, d) => {
      const a = aggByDay.get(d.startMs);
      if (!a) return acc;
      return {
        netR: acc.netR + a.netR,
        trades: acc.trades + a.trades,
        daysActive: acc.daysActive + 1,
      };
    },
    { netR: 0, trades: 0, daysActive: 0 },
  );
  const totalNetRColor =
    weekTotal.trades === 0
      ? "text-zinc-600"
      : weekTotal.netR > 0
        ? "text-emerald-400"
        : weekTotal.netR < 0
          ? "text-rose-400"
          : "text-zinc-300";
  return (
    <>
      <SectionHeader title="this week · live netR" />
      <div className="mt-1 pl-2 grid grid-cols-7 gap-1">
        {days.map((d) => {
          const agg = aggByDay.get(d.startMs);
          const hasData = !!agg && agg.trades > 0;
          const netRColor = !hasData
            ? "text-zinc-600"
            : agg!.netR > 0
              ? "text-emerald-400"
              : agg!.netR < 0
                ? "text-rose-400"
                : "text-zinc-300";
          const cellBg = d.isToday
            ? "border-emerald-500/50 bg-emerald-500/5"
            : hasData
              ? "border-zinc-700/60 bg-zinc-900/40"
              : "border-zinc-800/60 bg-zinc-900/20";
          const dim = d.isFuture ? "opacity-40" : "";
          return (
            <div
              key={d.startMs}
              className={`border rounded-sm px-1.5 py-1 ${cellBg} ${dim}`}
              title={new Date(d.startMs).toISOString().slice(0, 10)}
            >
              <div className="flex items-baseline justify-between text-[10px] text-zinc-500 leading-none">
                <span className={d.isToday ? "text-emerald-400" : ""}>{d.dow}</span>
                <span className="text-zinc-400">{d.date}</span>
              </div>
              <div className={`${netRColor} text-[12px] mt-1 leading-none whitespace-nowrap`}>
                {hasData ? `${agg!.netR >= 0 ? "+" : ""}${agg!.netR.toFixed(2)}R` : "—"}
              </div>
              <div className="text-zinc-600 text-[10px] mt-0.5 leading-none">
                {hasData ? `${agg!.trades} trade${agg!.trades === 1 ? "" : "s"}` : " "}
              </div>
            </div>
          );
        })}
        <div
          className="border border-zinc-600/70 bg-zinc-800/40 rounded-sm px-1.5 py-1"
          title={`week total · ${weekTotal.trades} trade${weekTotal.trades === 1 ? "" : "s"} across ${weekTotal.daysActive} day${weekTotal.daysActive === 1 ? "" : "s"}`}
        >
          <div className="flex items-baseline justify-between text-[10px] text-zinc-500 leading-none">
            <span>week</span>
            <span className="text-zinc-400">Σ</span>
          </div>
          <div className={`${totalNetRColor} text-[12px] mt-1 leading-none whitespace-nowrap`}>
            {weekTotal.trades > 0
              ? `${weekTotal.netR >= 0 ? "+" : ""}${weekTotal.netR.toFixed(2)}R`
              : "—"}
          </div>
          <div className="text-zinc-600 text-[10px] mt-0.5 leading-none">
            {weekTotal.daysActive > 0
              ? `${weekTotal.daysActive} day${weekTotal.daysActive === 1 ? "" : "s"}`
              : " "}
          </div>
        </div>
      </div>
      {dailyNetR === null && recentTrades === null ? (
        <div className="pl-2 mt-1 text-zinc-600 text-[11px]">loading daily NetR…</div>
      ) : null}
    </>
  );
}

function SessionTimeline({ sessionCounts }: { sessionCounts: Record<string, number> }) {
  const [now, setNow] = useState<number>(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 30_000);
    return () => clearInterval(id);
  }, []);

  const nowLabel = fmtClockHM(new Date(now).getUTCHours() * 60 + new Date(now).getUTCMinutes());
  const NOW_PCT = 50;

  const rows = SESSION_PROFILES.map((p) => ({
    profile: p,
    state: computeSessionRow(p, now),
    bands: computeRelativeBands(p, now),
    count: sessionCounts[p.name] || 0,
  }));

  return (
    <>
      <SectionHeader title={`sessions · now ${nowLabel} UTC`} />
      <div className="mt-1 pl-2">
        <SessionAxis nowMs={now} nowPct={NOW_PCT} />
        <div className="space-y-0.5">
          {rows.map(({ profile, state, bands, count }) => (
            <SessionLane key={profile.name} state={state} bands={bands} count={count} nowPct={NOW_PCT} />
          ))}
        </div>
      </div>
    </>
  );
}

function computeUtcHourTicks(nowMs: number): Array<{ pct: number; label: string }> {
  const windowStartMs = nowMs - 12 * 60 * 60_000;
  const windowEndMs = nowMs + 12 * 60 * 60_000;
  const spanMs = windowEndMs - windowStartMs;
  const nowDate = new Date(nowMs);
  const midnightUtcMs = Date.UTC(nowDate.getUTCFullYear(), nowDate.getUTCMonth(), nowDate.getUTCDate());
  const ticks: Array<{ pct: number; label: string }> = [];
  for (const h of [0, 4, 8, 12, 16, 20]) {
    for (const shiftDays of [-1, 0, 1]) {
      const ts = midnightUtcMs + (shiftDays * 24 + h) * 60 * 60_000;
      if (ts >= windowStartMs && ts <= windowEndMs) {
        ticks.push({
          pct: ((ts - windowStartMs) / spanMs) * 100,
          label: String(h).padStart(2, "0"),
        });
        break;
      }
    }
  }
  return ticks;
}

function SessionAxis({ nowMs, nowPct }: { nowMs: number; nowPct: number }) {
  const ticks = computeUtcHourTicks(nowMs);
  return (
    <div className="grid grid-cols-[5rem_1fr_11rem] gap-x-3 items-end mb-1">
      <span />
      <div className="relative h-4 text-[10px] text-zinc-600">
        {ticks.map((t) => (
          <span
            key={`${t.label}-${t.pct.toFixed(2)}`}
            className="absolute top-1"
            style={{ left: `${t.pct}%`, transform: "translateX(-50%)" }}
          >
            {t.label}
          </span>
        ))}
        <span
          className="absolute top-0 text-amber-400 text-[11px] leading-none"
          style={{ left: `${nowPct}%`, transform: "translateX(-50%)" }}
        >
          ▼
        </span>
      </div>
      <span />
    </div>
  );
}

function SessionLane({
  state,
  bands,
  count,
  nowPct,
}: {
  state: SessionRowState;
  bands: Array<{ leftPct: number; widthPct: number }>;
  count: number;
  nowPct: number;
}) {
  const nameClass = state.active ? "text-zinc-100" : "text-zinc-400";
  const statusClass =
    state.statusTone === "active"
      ? "text-emerald-400"
      : state.statusTone === "soon"
        ? "text-sky-400"
        : "text-zinc-500";
  const barClass = state.active
    ? "bg-emerald-500/60"
    : state.statusTone === "soon"
      ? "bg-sky-500/25"
      : "bg-zinc-500/20";
  const countClass = count === 0 ? "text-zinc-600" : state.active ? "text-emerald-400" : "text-zinc-300";
  return (
    <div className="grid grid-cols-[5rem_1fr_11rem] gap-x-3 items-center text-[12px]">
      <span className={nameClass}>{state.name}</span>
      <div className="relative h-2 bg-zinc-900/40 rounded-[1px]">
        {bands.map((b, i) => (
          <div
            key={i}
            className={`absolute inset-y-0 ${barClass} rounded-[1px]`}
            style={{ left: `${b.leftPct}%`, width: `${b.widthPct}%` }}
            title={state.utcLabel}
          />
        ))}
        <div
          className="absolute inset-y-[-2px] border-l border-amber-400/60"
          style={{ left: `${nowPct}%` }}
        />
      </div>
      <span className="text-right whitespace-nowrap">
        <span className={countClass}>{count} {count === 1 ? "dep" : "deps"}</span>
        <span className="text-zinc-600"> · </span>
        <span className={statusClass}>{state.status}</span>
      </span>
    </div>
  );
}
