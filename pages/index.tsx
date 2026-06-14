import { useCallback, useEffect, useState } from "react";
import Head from "next/head";
import {
  AdminSecretPanel,
  DashboardHeader,
  PageShell,
  SectionHeader,
  Skeleton,
  bar,
  fetchOne,
  fmtAgo,
  fmtClock,
  sparkline,
  useAdminSecretLoader,
} from "../components/scalp/shared";

// ─── types ───────────────────────────────────────────────────────────────────
// The system was stripped down to a single composer (session_structure_composer_v1).
// This dashboard reads the live composer backend only:
//   /api/scalp/composer/dashboard/summary   — deployments, events, runtime, jobs
//   /api/scalp/composer/ops/research-health — research/backtest progress
//   /api/scalp/ops/neon-usage               — Neon data-transfer budget

interface DeploymentRow {
  deploymentId?: string;
  venue?: string;
  symbol?: string;
  strategyId?: string;
  tuneId?: string;
  entrySessionProfile?: string;
  enabled?: boolean;
  liveMode?: string;
  score?: number | null;
  updatedAtMs?: number | null;
  promotionGate?: {
    eligible?: boolean;
    reason?: string;
    [k: string]: unknown;
  } | null;
}

interface EventRow {
  tsMs?: number;
  venue?: string;
  symbol?: string;
  type?: string;
  level?: string;
  reasonCodes?: string[];
}

interface RuntimeConfig {
  liveEnabled?: boolean;
  killSwitch?: boolean;
  enabledSymbolScope?: unknown;
  [k: string]: unknown;
}

interface SummaryResp {
  ok: boolean;
  mode?: string;
  runtime?: RuntimeConfig | null;
  summary?: {
    candidates?: number;
    deployments?: number;
    enabledDeployments?: number;
    events24h?: number;
    ledgerRows30d?: number;
    netR30d?: number;
    [k: string]: unknown;
  } | null;
  events?: EventRow[];
}

type DeploymentScope = "live" | "enabled" | "inactive" | "all";

interface DeploymentsFeedResp {
  ok: boolean;
  scope: string;
  hasMore: boolean;
  offset: number;
  deployments: DeploymentRow[];
}

const DEPLOYMENT_SCOPES: DeploymentScope[] = ["live", "enabled", "inactive", "all"];
const DEPLOYMENT_PAGE_SIZE = 25;

interface NeonUsageResp {
  ok: boolean;
  configured: boolean;
  generatedAtMs: number;
  message?: string;
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
    publicNetworkTransferBytes: number;
    privateNetworkTransferBytes: number;
    totalNetworkTransferBytes: number;
    hourly: Array<{ totalNetworkTransferBytes: number }>;
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
    phase: string | null;
    reason: string | null;
    updatedAtMs: number | null;
    progress: {
      processedSoFar: number;
      totalSelected: number;
      workerStage: string | null;
      workerStageProcessed: number;
      workerStageTotal: number;
      skippedByCache: number;
      skippedByClearFail: number;
      skippedByNetRPreFilter: number;
      smartSkippedPersisted: number;
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
      timeBudgetExhausted: boolean;
    };
  } | null;
  hint: {
    tone: "ok" | "warn" | "critical" | "info";
    label: string;
    detail: string | null;
  };
}

export default function ScalpComposerDashboard() {
  const [summary, setSummary] = useState<SummaryResp | null>(null);
  const [neonUsage, setNeonUsage] = useState<NeonUsageResp | null>(null);
  const [researchHealth, setResearchHealth] = useState<ResearchHealthResp | null>(null);

  const loader = useCallback(async (headers: Record<string, string>) => {
    // The roster is loaded lazily/paged via /dashboard/deployments, so this
    // "loads once" summary keeps deploymentLimit minimal and trims events.
    const attempts = await Promise.all([
      fetchOne<SummaryResp>(
        "/api/scalp/composer/dashboard/summary?deploymentLimit=1&eventLimit=15",
        headers,
        setSummary,
      ),
      fetchOne<NeonUsageResp>("/api/scalp/ops/neon-usage", headers, setNeonUsage),
      fetchOne<ResearchHealthResp>("/api/scalp/composer/ops/research-health", headers, setResearchHealth),
    ]);
    const unauthorized = attempts.find((a) => a.unauthorized);
    if (unauthorized) return unauthorized;
    const errored = attempts.find((a) => a.error);
    return errored || {};
  }, []);

  const {
    adminSecret,
    secretInput,
    setSecretInput,
    showSecretPanel,
    setShowSecretPanel,
    unauthorized,
    secretHydrated,
    error,
    loadedAt,
    autoRefresh,
    setAutoRefresh,
    load,
    saveSecret,
  } = useAdminSecretLoader(loader);

  // ─── lazy / paged / scoped deployments feed ─────────────────────────────────
  const [depScope, setDepScope] = useState<DeploymentScope>("live");
  const [depVenue, setDepVenue] = useState<"" | "bitget" | "capital">("");
  const [depOffset, setDepOffset] = useState(0);
  const [depRows, setDepRows] = useState<DeploymentRow[]>([]);
  const [depHasMore, setDepHasMore] = useState(false);
  const [depLoading, setDepLoading] = useState(false);
  const [depError, setDepError] = useState<string | null>(null);

  useEffect(() => {
    if (!secretHydrated || unauthorized) return;
    let cancelled = false;
    const run = async () => {
      setDepLoading(true);
      setDepError(null);
      const headers: Record<string, string> = {};
      if (adminSecret) headers["x-admin-access-secret"] = adminSecret;
      const params = new URLSearchParams({
        scope: depScope,
        limit: String(DEPLOYMENT_PAGE_SIZE),
        offset: String(depOffset),
      });
      if (depVenue) params.set("venue", depVenue);
      try {
        const res = await fetch(`/api/scalp/composer/dashboard/deployments?${params.toString()}`, {
          headers,
          credentials: "include",
        });
        if (res.status === 401) {
          if (!cancelled) {
            setShowSecretPanel(true);
            setDepError("Unauthorized — admin secret missing or invalid.");
          }
          return;
        }
        const data = (await res.json()) as DeploymentsFeedResp & { error?: string };
        if (!res.ok || !data.ok) {
          if (!cancelled) setDepError(data.error || `HTTP ${res.status}`);
          return;
        }
        if (!cancelled) {
          setDepRows(data.deployments || []);
          setDepHasMore(Boolean(data.hasMore));
        }
      } catch (e) {
        if (!cancelled) setDepError((e as Error)?.message || String(e));
      } finally {
        if (!cancelled) setDepLoading(false);
      }
    };
    run();
    return () => {
      cancelled = true;
    };
  }, [adminSecret, secretHydrated, unauthorized, depScope, depVenue, depOffset, setShowSecretPanel]);

  const setScope = (s: DeploymentScope) => {
    setDepScope(s);
    setDepOffset(0);
  };
  const setVenueFilter = (v: "" | "bitget" | "capital") => {
    setDepVenue(v);
    setDepOffset(0);
  };

  return (
    <PageShell>
      <Head>
        <title>scalp · composer</title>
      </Head>
      <DashboardHeader
        title="scalp · composer"
        classifierVersion="session_structure_composer_v1"
        loadedAt={loadedAt}
        autoRefresh={autoRefresh}
        setAutoRefresh={setAutoRefresh}
        load={load}
        adminSecret={adminSecret}
        onToggleSecret={() => setShowSecretPanel((v) => !v)}
        navLinks={[
          { href: "/legacy", label: "legacy", active: false },
          { href: "/scalp-backtest", label: "backtest", active: false },
        ]}
      />

      <AdminSecretPanel
        show={showSecretPanel}
        unauthorized={unauthorized}
        adminSecret={adminSecret}
        secretInput={secretInput}
        setSecretInput={setSecretInput}
        saveSecret={saveSecret}
        load={load}
        dismiss={() => setShowSecretPanel(false)}
      />

      {error ? <div className="mt-2 text-rose-400">{error}</div> : null}

      <SectionHeader title="overview" />
      {summary ? (
        <div className="mt-1 pl-2 flex flex-wrap gap-x-6 gap-y-0.5">
          <span>
            <span className="text-zinc-500">deployments </span>
            <span className="text-zinc-100">{Number(summary.summary?.deployments ?? 0)}</span>
          </span>
          <span>
            <span className="text-zinc-500">enabled </span>
            <span className="text-emerald-400">{Number(summary.summary?.enabledDeployments ?? 0)}</span>
          </span>
          <span>
            <span className="text-zinc-500">candidates </span>
            <span className="text-zinc-100">{Number(summary.summary?.candidates ?? 0)}</span>
          </span>
          <span>
            <span className="text-zinc-500">events 24h </span>
            <span className="text-zinc-100">{Number(summary.summary?.events24h ?? 0)}</span>
          </span>
          <span>
            <span className="text-zinc-500">netR 30d </span>
            <span className={Number(summary.summary?.netR30d ?? 0) >= 0 ? "text-emerald-400" : "text-rose-400"}>
              {Number(summary.summary?.netR30d ?? 0).toFixed(2)}
            </span>
          </span>
          <span>
            <span className="text-zinc-500">live engine </span>
            <span className={summary.runtime?.liveEnabled ? "text-emerald-400" : "text-amber-400"}>
              {summary.runtime?.liveEnabled ? "ON" : "OFF"}
            </span>
          </span>
          {summary.runtime?.killSwitch ? <span className="text-rose-400">KILL SWITCH</span> : null}
        </div>
      ) : (
        <Skeleton label="loading composer summary" />
      )}

      <SectionHeader title="research · session composer backtests" />
      {researchHealth ? <ResearchProgress data={researchHealth} /> : <Skeleton label="loading research progress" />}

      <SectionHeader title="deployments" />
      <div className="mt-1 pl-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-zinc-500">
        <span>scope</span>
        {DEPLOYMENT_SCOPES.map((s) => (
          <button
            key={s}
            onClick={() => setScope(s)}
            className={depScope === s ? "text-zinc-100" : "text-zinc-500 hover:text-zinc-300"}
          >
            [{s}]
          </button>
        ))}
        <span className="text-zinc-700">·</span>
        <span>venue</span>
        {(["", "bitget", "capital"] as const).map((v) => (
          <button
            key={v || "all"}
            onClick={() => setVenueFilter(v)}
            className={depVenue === v ? "text-zinc-100" : "text-zinc-500 hover:text-zinc-300"}
          >
            [{v || "all"}]
          </button>
        ))}
        {depLoading ? <span className="text-sky-400">loading…</span> : null}
      </div>
      {depError ? <div className="pl-2 mt-1 text-rose-400">{depError}</div> : null}
      {depRows.length > 0 ? (
        <div className="mt-1 overflow-x-auto">
          <table className="w-full text-left">
            <thead className="text-zinc-500">
              <tr>
                <th className="pr-3 font-normal">symbol</th>
                <th className="pr-3 font-normal">session</th>
                <th className="pr-3 font-normal">venue</th>
                <th className="pr-3 font-normal">mode</th>
                <th className="pr-3 font-normal">score</th>
                <th className="pr-3 font-normal">gate</th>
                <th className="pr-3 font-normal">updated</th>
              </tr>
            </thead>
            <tbody>
              {depRows.map((d, i) => {
                const gate = d.promotionGate || {};
                const eligible = Boolean(gate.eligible);
                const reason = String(gate.reason || "—");
                const scoreRaw = (gate as { score?: unknown }).score;
                const liveMode = String(d.liveMode || "—");
                return (
                  <tr key={d.deploymentId || i} className="border-t border-zinc-900">
                    <td className="pr-3 text-zinc-100">{d.symbol || "—"}</td>
                    <td className="pr-3 text-zinc-400">{d.entrySessionProfile || "—"}</td>
                    <td className="pr-3 text-zinc-500">{d.venue || "—"}</td>
                    <td className="pr-3">
                      <span
                        className={
                          d.enabled && liveMode === "live"
                            ? "text-emerald-400"
                            : liveMode === "shadow"
                              ? "text-sky-400"
                              : "text-zinc-500"
                        }
                      >
                        {d.enabled ? liveMode : "off"}
                      </span>
                    </td>
                    <td className="pr-3 text-zinc-300">
                      {scoreRaw === null || scoreRaw === undefined || !Number.isFinite(Number(scoreRaw))
                        ? "—"
                        : Number(scoreRaw).toFixed(1)}
                    </td>
                    <td className="pr-3">
                      <span className={eligible ? "text-emerald-400" : "text-zinc-500"}>{reason}</span>
                    </td>
                    <td className="pr-3 text-zinc-500">{fmtAgo(d.updatedAtMs)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          <div className="pl-2 mt-1 flex items-center gap-3 text-zinc-500">
            <button
              onClick={() => setDepOffset((o) => Math.max(0, o - DEPLOYMENT_PAGE_SIZE))}
              disabled={depOffset === 0}
              className={depOffset === 0 ? "text-zinc-700" : "text-zinc-300 hover:text-white"}
            >
              [prev]
            </button>
            <span>
              {depOffset + 1}–{depOffset + depRows.length}
            </span>
            <button
              onClick={() => setDepOffset((o) => o + DEPLOYMENT_PAGE_SIZE)}
              disabled={!depHasMore}
              className={!depHasMore ? "text-zinc-700" : "text-zinc-300 hover:text-white"}
            >
              [next]
            </button>
          </div>
        </div>
      ) : (
        <div className="pl-2 mt-1 text-zinc-500">{depLoading ? "loading…" : `(no ${depScope} deployments)`}</div>
      )}

      <SectionHeader title="recent execution events" />
      {summary ? (
        (summary.events || []).length > 0 ? (
          <div className="mt-1 pl-2 flex flex-col gap-0.5">
            {(summary.events || []).slice(0, 15).map((e, i) => (
              <div key={i} className="flex flex-wrap items-baseline gap-x-3">
                <span className="text-zinc-500">{fmtClock(e.tsMs)}</span>
                <span className="text-zinc-300">{e.symbol || "—"}</span>
                <span className="text-zinc-600">{e.venue || ""}</span>
                <span className={e.level === "error" ? "text-rose-400" : e.level === "warn" ? "text-amber-400" : "text-zinc-500"}>
                  {e.type || ""}
                </span>
                <span className="text-zinc-500">{(e.reasonCodes || []).join(" ")}</span>
              </div>
            ))}
          </div>
        ) : (
          <div className="pl-2 mt-1 text-zinc-500">(no recent execution events)</div>
        )
      ) : (
        <Skeleton label="loading events" />
      )}

      <SectionHeader title="neon data transfer" />
      {neonUsage ? <NeonUsageCompact data={neonUsage} /> : <Skeleton label="loading neon usage" />}

      <div className="border-t border-zinc-800 pt-2 mt-6 text-zinc-600">
        sources /api/scalp/composer/dashboard/summary · /composer/ops/research-health · /api/scalp/ops/neon-usage
      </div>
    </PageShell>
  );
}

// ─── neon usage (ported from the prior dashboard) ────────────────────────────

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

// ─── research progress (ported; surrogate field dropped in the strip-down) ───

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
    n(batch?.smartSkippedPersisted);
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
            <span className={errors > 0 ? "text-rose-400" : "text-zinc-500"}>errors {errors}</span>
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
