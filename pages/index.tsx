import { useCallback, useMemo, useState } from "react";
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
}

interface SymbolRegimeBucket {
  venue: string;
  symbol: string;
  cellId: string | null;
  stale: boolean;
  allowCount: number;
  blockCount: number;
  pendingCount: number;
  totalEnabled: number;
}

interface RegimesResp {
  ok: boolean;
  classifierVersion: string;
  nowMs: number;
  regimes: SymbolRegimeBucket[];
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
  deployments: V5DeploymentRow[];
}

interface TradeRow {
  tsMs: number;
  deploymentId: string | null;
  venue: string | null;
  symbol: string | null;
  reasonCodes: string[];
  eventKind: "trade" | "entry_error" | "entry_skipped" | "state_change";
  state: string;
  stateChanged: boolean;
  tradeEventOccurred: boolean;
  rMultiple: number | null;
  summary: string;
}

interface RecentResp {
  ok: boolean;
  classifierVersion: string;
  recentTrades: TradeRow[];
}

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
  const [regimes, setRegimes] = useState<RegimesResp | null>(null);
  const [deployments, setDeployments] = useState<DeploymentsResp | null>(null);
  const [recent, setRecent] = useState<RecentResp | null>(null);

  const [expandedDeployments, setExpandedDeployments] = useState<Set<string>>(new Set());
  const [showAllInactive, setShowAllInactive] = useState(false);
  const [decisionFilter, setDecisionFilter] = useState<V5GateDecision | null>(null);

  const toggleExpanded = useCallback((id: string) => {
    setExpandedDeployments((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  // Fire all five endpoints in parallel. Each one calls its own setState as it
  // resolves — coverage typically lands first (~50ms), deployments last (the
  // heavy one). UI sections render as data arrives.
  const loader = useCallback(async (headers: Record<string, string>) => {
    const [c, g, r, d, rc] = await Promise.all([
      fetchOne<CoverageResp>("/api/scalp/v5/coverage", headers, setCoverage),
      fetchOne<GateStateResp>("/api/scalp/v5/gate-state", headers, setGateState),
      fetchOne<RegimesResp>("/api/scalp/v5/regimes", headers, setRegimes),
      fetchOne<DeploymentsResp>("/api/scalp/v5/deployments", headers, setDeployments),
      fetchOne<RecentResp>("/api/scalp/v4/recent", headers, setRecent),
    ]);
    if (c.unauthorized || g.unauthorized || r.unauthorized || d.unauthorized || rc.unauthorized) {
      return { unauthorized: true };
    }
    return { error: c.error || g.error || r.error || d.error || rc.error };
  }, []);

  const access = useAdminSecretLoader(loader);

  const enabledRows = useMemo(
    () => (deployments ? deployments.deployments.filter((d) => d.enabled) : []),
    [deployments],
  );
  const inactiveRows = useMemo(
    () => (deployments ? deployments.deployments.filter((d) => !d.enabled) : []),
    [deployments],
  );
  const filteredEnabled = useMemo(() => {
    if (!decisionFilter) return enabledRows;
    return enabledRows.filter((r) => r.gate.decision === decisionFilter);
  }, [enabledRows, decisionFilter]);

  const activityRows = useMemo(() => (recent ? recent.recentTrades.slice(0, 40) : []), [recent]);

  // Hide evaluator section once this week's evaluation pass is complete —
  // the panel is only useful while there's still work to do.
  const evaluatorComplete = useMemo(() => {
    if (!coverage) return false;
    const total = coverage.coverage.totalDeployments;
    if (total === 0) return false;
    return coverage.progress.evaluatedThisWeek >= total;
  }, [coverage]);

  // Header classifierVersion: prefer whichever endpoint has loaded first.
  const classifierVersion =
    coverage?.classifierVersion ||
    gateState?.classifierVersion ||
    regimes?.classifierVersion ||
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

      {evaluatorComplete ? null : (
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

      <SectionHeader title="deployments · live cell evidence" />
      {gateState ? (
        <GateChipBar
          stateNowEnabled={gateState.stateNowEnabled}
          activeFilter={decisionFilter}
          onPick={(d) => setDecisionFilter((cur) => (cur === d ? null : d))}
        />
      ) : (
        <Skeleton label="loading gate state" />
      )}
      {deployments ? (
        deployments.deployments.length === 0 ? (
          <div className="pl-2 mt-1 text-zinc-500">(no deployments — nothing v3-promoted yet)</div>
        ) : (
          <div className="mt-1 pl-2 space-y-0.5">
            {filteredEnabled.length > 0 ? (
              filteredEnabled.map((dep) => (
                <V5DeploymentRowView
                  key={dep.deploymentId}
                  row={dep}
                  expanded={expandedDeployments.has(dep.deploymentId)}
                  onToggle={() => toggleExpanded(dep.deploymentId)}
                />
              ))
            ) : (
              <div className="text-zinc-500 text-[12px]">
                (no enabled deployments{decisionFilter ? ` with decision=${V5_DECISION_LABEL[decisionFilter]}` : ""})
              </div>
            )}
            {inactiveRows.length > 0 && !decisionFilter ? (
              <>
                <div className="mt-3 text-zinc-500 text-[11px] uppercase tracking-wider">
                  ─── inactive ({inactiveRows.length}) · evidence pre-staged, not enabled
                </div>
                {(showAllInactive ? inactiveRows : inactiveRows.slice(0, 12)).map((dep) => (
                  <V5DeploymentRowView
                    key={dep.deploymentId}
                    row={dep}
                    expanded={expandedDeployments.has(dep.deploymentId)}
                    onToggle={() => toggleExpanded(dep.deploymentId)}
                  />
                ))}
                {inactiveRows.length > 12 ? (
                  <button
                    onClick={() => setShowAllInactive((v) => !v)}
                    className="text-zinc-500 hover:text-zinc-300"
                  >
                    {showAllInactive ? "↑ collapse inactive" : `… show all ${inactiveRows.length} inactive`}
                  </button>
                ) : null}
              </>
            ) : null}
          </div>
        )
      ) : (
        <Skeleton label="loading deployments" />
      )}

      <SectionHeader title="symbol regimes · this week" />
      {regimes ? (
        regimes.regimes.length === 0 ? (
          <div className="pl-2 text-zinc-500">(no enabled symbols)</div>
        ) : (
          <div className="mt-1 pl-2 space-y-0.5">
            {regimes.regimes.map((row) => (
              <SymbolRegimeRow key={`${row.venue}:${row.symbol}`} row={row} />
            ))}
          </div>
        )
      ) : (
        <Skeleton label="loading regimes" />
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
        sources /api/scalp/v5/coverage · /gate-state · /regimes · /deployments · /api/scalp/v4/recent
      </div>
    </PageShell>
  );
}

// ─── gate chip bar (inline filter strip under deployments header) ────────────

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
}: {
  row: V5DeploymentRow;
  expanded: boolean;
  onToggle: () => void;
}) {
  const family = row.tuneId.split("_").slice(0, 4).join("_");
  const decision = row.gate.decision;
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
            <span className="text-zinc-500 text-[12px]">{row.session}</span>
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
          <span className="text-zinc-500">{row.session}</span>
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
                <CellEvidenceRow key={cell.cellId} cell={cell} maxAbs={maxAbs} holdoutFromMs={row.holdoutWindow?.fromMs ?? null} />
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
}: {
  cell: V5CellRow;
  maxAbs: number;
  holdoutFromMs: number | null;
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
        <WeeklyNetRTrack values={cell.weeklyNetR} globalMaxAbs={maxAbs} weekLabel={weekLabel} heightPx={36} />
      </div>
    </div>
  );
}

function SymbolRegimeRow({ row }: { row: SymbolRegimeBucket }) {
  return (
    <div>
      <div className="md:hidden">
        <div className="flex items-baseline gap-x-2">
          <span className="text-zinc-100 truncate">
            <span className="text-zinc-500">{row.venue}/</span>
            {row.symbol}
          </span>
          {row.stale ? <span className="text-amber-300 text-[11px]">stale</span> : null}
        </div>
        <div
          className={`pl-2 text-[12px] truncate ${row.cellId ? "text-zinc-300" : "text-zinc-600"}`}
          title={row.cellId || ""}
        >
          {abbrCell(row.cellId)}
        </div>
        <div className="pl-2 text-[12px] text-zinc-500">
          <span className="text-emerald-400">{row.allowCount} allow</span>
          {row.blockCount > 0 ? <span className="text-rose-400"> · {row.blockCount} block</span> : null}
          {row.pendingCount > 0 ? <span className="text-sky-400"> · {row.pendingCount} pending</span> : null}
          <span> / {row.totalEnabled} deps</span>
        </div>
      </div>
      <div className="hidden md:grid md:grid-cols-[5rem_8rem_1fr_4rem_1fr] md:gap-x-2 md:items-baseline">
        <span className="text-zinc-500">{row.venue}</span>
        <span className="text-zinc-100 truncate">{row.symbol}</span>
        <span className={row.cellId ? "text-zinc-300 truncate" : "text-zinc-600"} title={row.cellId || ""}>
          {abbrCell(row.cellId)}
        </span>
        <span className={`text-[12px] ${row.stale ? "text-amber-300" : "text-zinc-600"}`}>
          {row.stale ? "stale" : ""}
        </span>
        <span className="text-[13px] text-zinc-500">
          <span className="text-emerald-400">{row.allowCount} allow</span>
          {row.blockCount > 0 ? <span className="text-rose-400"> · {row.blockCount} block</span> : null}
          {row.pendingCount > 0 ? <span className="text-sky-400"> · {row.pendingCount} pending</span> : null}
          <span> / {row.totalEnabled} deps</span>
        </span>
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

  if (event.eventKind === "trade") {
    marker = "TRADE";
    const tone =
      event.rMultiple !== null ? (event.rMultiple > 0 ? "text-emerald-400" : "text-rose-400") : "text-zinc-200";
    markerClass = tone;
    detail = event.state || event.summary;
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
