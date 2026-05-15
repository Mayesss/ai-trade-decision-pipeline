import { useCallback, useEffect, useMemo, useState } from "react";
import Head from "next/head";
import Link from "next/link";
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  CircleSlash,
  Clock,
  History,
  KeyRound,
  Layers,
  Moon,
  RefreshCw,
  Sun,
  TrendingUp,
} from "lucide-react";

// Same storage key the legacy page uses — secrets entered on either page
// transfer automatically.
const ADMIN_SECRET_STORAGE_KEY = "admin_access_secret";

function readStoredAdminSecret(): string {
  if (typeof window === "undefined") return "";
  try {
    return (window.localStorage.getItem(ADMIN_SECRET_STORAGE_KEY) || "").trim();
  } catch {
    return "";
  }
}

function persistAdminSecret(value: string): void {
  if (typeof window === "undefined") return;
  try {
    const normalized = value.trim();
    if (normalized) window.localStorage.setItem(ADMIN_SECRET_STORAGE_KEY, normalized);
    else window.localStorage.removeItem(ADMIN_SECRET_STORAGE_KEY);
  } catch {
    // ignore quota / private-mode failures
  }
}

type V4Status =
  | "trading"
  | "dormant_wrong_regime"
  | "dormant_no_regime"
  | "pending_walkforward"
  | "eligible_not_promoted"
  | "failed_walkforward"
  | "disabled";

interface DeploymentRow {
  deploymentId: string;
  venue: string;
  symbol: string;
  session: string;
  strategyId: string;
  tuneId: string;
  enabled: boolean;
  liveMode: string;
  v4Status: V4Status;
  envelope: {
    eligible: boolean;
    status: string | null;
    allowedCells: string[];
    occupiedCells: number;
    strictPassingCells: number;
  };
  currentRegime: { cellId: string | null; stale: boolean; updatedAtMs: number | null };
  reason: string | null;
  score: number | null;
}

interface WalkforwardRow {
  deploymentId: string;
  venue: string;
  symbol: string;
  status: string;
  eligible: boolean;
  allowedCells: string[];
  strictPassingCells: number;
  occupiedCells: number;
  evaluatedAtMs: number;
  durationMs: number | null;
}

interface TransitionRow {
  venue: string;
  symbol: string;
  transitionWeekStartMs: number;
  fromCellId: string | null;
  toCellId: string;
}

interface HealthResp {
  ok: boolean;
  classifierVersion: string;
  v4Enabled: boolean;
  v4HardGateEnabled: boolean;
  stageCSurvivors: number;
  walkforwardCounts: Record<string, number>;
  walkforwardTotal: number;
  pendingWalkforward: number;
  regimeBuild: { symbolsCovered: number; latestWeekStartMs: number | null };
}

interface DeploymentsResp {
  ok: boolean;
  classifierVersion: string;
  deployments: DeploymentRow[];
  statusHistogram: Record<V4Status, number>;
}

interface RecentResp {
  ok: boolean;
  classifierVersion: string;
  recentWalkforward: WalkforwardRow[];
  recentTransitions: TransitionRow[];
}

const STATUS_META: Record<V4Status, { label: string; cls: string; emoji: string }> = {
  trading: { label: "Trading", cls: "bg-emerald-500/20 text-emerald-300 border-emerald-500/40", emoji: "🟢" },
  dormant_wrong_regime: { label: "Dormant (wrong regime)", cls: "bg-amber-500/20 text-amber-300 border-amber-500/40", emoji: "🟡" },
  dormant_no_regime: { label: "Dormant (no regime data)", cls: "bg-amber-500/10 text-amber-200 border-amber-500/30", emoji: "⏸" },
  pending_walkforward: { label: "Pending v4", cls: "bg-orange-500/20 text-orange-300 border-orange-500/40", emoji: "🟠" },
  eligible_not_promoted: { label: "Eligible (awaiting promotion)", cls: "bg-sky-500/20 text-sky-300 border-sky-500/40", emoji: "🔵" },
  failed_walkforward: { label: "Failed v4", cls: "bg-rose-500/20 text-rose-300 border-rose-500/40", emoji: "🔴" },
  disabled: { label: "Disabled", cls: "bg-zinc-700/30 text-zinc-400 border-zinc-700/50", emoji: "⚪" },
};

const STATUS_ORDER: V4Status[] = [
  "trading",
  "dormant_wrong_regime",
  "dormant_no_regime",
  "eligible_not_promoted",
  "pending_walkforward",
  "failed_walkforward",
  "disabled",
];

function fmtAgo(ms: number | null | undefined): string {
  if (!ms) return "—";
  const diff = Math.max(0, Date.now() - ms);
  const sec = Math.floor(diff / 1000);
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 48) return `${hr}h ago`;
  return `${Math.floor(hr / 24)}d ago`;
}

function fmtDate(ms: number | null | undefined): string {
  if (!ms) return "—";
  const d = new Date(ms);
  return `${d.toISOString().slice(0, 16).replace("T", " ")} UTC`;
}

function fmtDuration(ms: number | null): string {
  if (!ms) return "—";
  if (ms < 1000) return `${ms}ms`;
  const sec = Math.floor(ms / 1000);
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  return `${min}m ${sec % 60}s`;
}

function CellChip({ cellId, highlight = false }: { cellId: string; highlight?: boolean }) {
  return (
    <span
      className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-mono ${
        highlight ? "bg-emerald-500/30 text-emerald-200 border border-emerald-400/40" : "bg-zinc-800/80 text-zinc-300 border border-zinc-700"
      }`}
      title={cellId}
    >
      {cellId.replace(/vol=|trend=|risk=/g, "").replace(/\|/g, "/")}
    </span>
  );
}

function StatBox({ label, value, sub, accent }: { label: string; value: number | string; sub?: string; accent?: string }) {
  return (
    <div className={`flex flex-col rounded-lg border p-3 ${accent || "border-zinc-800 bg-zinc-900/50"}`}>
      <span className="text-[11px] uppercase tracking-wide text-zinc-400">{label}</span>
      <span className="mt-0.5 text-2xl font-semibold leading-tight">{value}</span>
      {sub ? <span className="text-[11px] text-zinc-500">{sub}</span> : null}
    </div>
  );
}

export default function ScalpV4Dashboard() {
  const [health, setHealth] = useState<HealthResp | null>(null);
  const [deploymentsResp, setDeploymentsResp] = useState<DeploymentsResp | null>(null);
  const [recent, setRecent] = useState<RecentResp | null>(null);
  const [healthLoading, setHealthLoading] = useState(false);
  const [deploymentsLoading, setDeploymentsLoading] = useState(false);
  const [recentLoading, setRecentLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loadedAt, setLoadedAt] = useState<number | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [dark, setDark] = useState(true);
  const [statusFilter, setStatusFilter] = useState<V4Status | "all">("all");
  const [search, setSearch] = useState("");
  const [adminSecret, setAdminSecret] = useState("");
  const [secretInput, setSecretInput] = useState("");
  const [showSecretPanel, setShowSecretPanel] = useState(false);
  const [unauthorized, setUnauthorized] = useState(false);

  // Hydrate the admin secret from localStorage on first client render.
  useEffect(() => {
    const stored = readStoredAdminSecret();
    if (stored) {
      setAdminSecret(stored);
      setSecretInput(stored);
    } else {
      setShowSecretPanel(true);
    }
  }, []);

  const load = useCallback(async () => {
    const headers: Record<string, string> = {};
    if (adminSecret) headers["x-admin-access-secret"] = adminSecret;

    async function fetchSection<T extends { ok: boolean }>(
      url: string,
      setData: (data: T) => void,
      setLoading: (loading: boolean) => void,
    ): Promise<{ unauthorized?: boolean; error?: string }> {
      setLoading(true);
      try {
        const res = await fetch(url, { headers, credentials: "include" });
        if (res.status === 401) return { unauthorized: true };
        if (!res.ok) return { error: `HTTP ${res.status}` };
        const data = (await res.json()) as T;
        if (!data.ok) return { error: (data as any).error || "request_failed" };
        setData(data);
        return {};
      } catch (err) {
        return { error: (err as Error)?.message || String(err) };
      } finally {
        setLoading(false);
      }
    }

    // Fire all three in parallel — each writes its own state independently.
    const [hRes, dRes, rRes] = await Promise.all([
      fetchSection<HealthResp>("/api/scalp/v4/health", setHealth, setHealthLoading),
      fetchSection<DeploymentsResp>("/api/scalp/v4/deployments", setDeploymentsResp, setDeploymentsLoading),
      fetchSection<RecentResp>("/api/scalp/v4/recent", setRecent, setRecentLoading),
    ]);

    const anyUnauth = hRes.unauthorized || dRes.unauthorized || rRes.unauthorized;
    if (anyUnauth) {
      setUnauthorized(true);
      setShowSecretPanel(true);
      setError("Unauthorized — admin secret missing or invalid.");
      return;
    }
    const firstError = hRes.error || dRes.error || rRes.error || null;
    setError(firstError);
    setUnauthorized(false);
    setLoadedAt(Date.now());
  }, [adminSecret]);

  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    if (!autoRefresh || unauthorized) return;
    const t = setInterval(load, 30_000);
    return () => clearInterval(t);
  }, [autoRefresh, load, unauthorized]);

  const saveSecret = useCallback(
    (next: string) => {
      const value = next.trim();
      persistAdminSecret(value);
      setAdminSecret(value);
      setUnauthorized(false);
      setShowSecretPanel(false);
    },
    [],
  );

  const filteredDeployments = useMemo(() => {
    if (!deploymentsResp) return [];
    const needle = search.trim().toLowerCase();
    return deploymentsResp.deployments
      .filter((row: DeploymentRow) => statusFilter === "all" || row.v4Status === statusFilter)
      .filter((row: DeploymentRow) => {
        if (!needle) return true;
        return (
          row.symbol.toLowerCase().includes(needle) ||
          row.venue.toLowerCase().includes(needle) ||
          row.session.toLowerCase().includes(needle) ||
          row.strategyId.toLowerCase().includes(needle) ||
          row.tuneId.toLowerCase().includes(needle)
        );
      })
      .sort(
        (a: DeploymentRow, b: DeploymentRow) =>
          STATUS_ORDER.indexOf(a.v4Status) - STATUS_ORDER.indexOf(b.v4Status),
      );
  }, [deploymentsResp, statusFilter, search]);

  const totalDeployments = deploymentsResp?.deployments.length || 0;
  const classifierAge = health?.regimeBuild.latestWeekStartMs
    ? fmtAgo(health.regimeBuild.latestWeekStartMs + 7 * 24 * 60 * 60_000)
    : "—";

  return (
    <div className={dark ? "dark min-h-screen bg-zinc-950 text-zinc-100" : "min-h-screen bg-zinc-50 text-zinc-900"}>
      <Head>
        <title>Scalp V4 · Macro-Regime Control</title>
      </Head>

      <header className="border-b border-zinc-800 bg-zinc-900/40 px-6 py-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <Activity className="h-5 w-5 text-emerald-400" />
            <h1 className="text-lg font-semibold">Scalp v4 · Macro-Regime Control</h1>
            <span className="ml-2 rounded bg-zinc-800/80 px-2 py-0.5 text-[11px] text-zinc-300">
              {health?.classifierVersion || "—"}
            </span>
          </div>
          <div className="flex items-center gap-3 text-sm text-zinc-400">
            <span>Updated {loadedAt ? fmtAgo(loadedAt) : "—"}</span>
            <button
              onClick={load}
              className="inline-flex items-center gap-1 rounded border border-zinc-700 px-2 py-1 hover:bg-zinc-800"
            >
              <RefreshCw className="h-3.5 w-3.5" /> Refresh
            </button>
            <label className="flex items-center gap-1">
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
              />
              Auto 30s
            </label>
            <button
              onClick={() => {
                setSecretInput(adminSecret);
                setShowSecretPanel((s) => !s);
              }}
              className={`inline-flex items-center gap-1 rounded border px-2 py-1 ${
                adminSecret
                  ? "border-zinc-700 text-zinc-400 hover:bg-zinc-800"
                  : "border-amber-500/50 bg-amber-500/10 text-amber-300 hover:bg-amber-500/20"
              }`}
              aria-label="admin secret"
              title={adminSecret ? "Admin secret set — click to change" : "Set admin secret"}
            >
              <KeyRound className="h-3.5 w-3.5" />
              {adminSecret ? "secret" : "set secret"}
            </button>
            <button
              onClick={() => setDark((d) => !d)}
              className="inline-flex items-center rounded border border-zinc-700 p-1 hover:bg-zinc-800"
              aria-label="toggle theme"
            >
              {dark ? <Sun className="h-3.5 w-3.5" /> : <Moon className="h-3.5 w-3.5" />}
            </button>
            <Link href="/legacy" className="text-xs text-zinc-500 hover:text-zinc-300">
              legacy dashboard →
            </Link>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-[1400px] space-y-6 px-6 py-6">
        {showSecretPanel ? (
          <div className={`rounded-lg border p-4 ${
            unauthorized
              ? "border-rose-500/40 bg-rose-500/10"
              : "border-amber-500/40 bg-amber-500/10"
          }`}>
            <div className="mb-2 flex items-center gap-2 text-sm">
              <KeyRound className="h-4 w-4" />
              <span className="font-medium">
                {unauthorized ? "Admin access required" : "Set admin access secret"}
              </span>
              <span className="text-xs text-zinc-400">
                stored in localStorage as <code className="font-mono">{ADMIN_SECRET_STORAGE_KEY}</code>
              </span>
            </div>
            <form
              onSubmit={(e) => {
                e.preventDefault();
                saveSecret(secretInput);
                setTimeout(load, 0);
              }}
              className="flex flex-wrap items-center gap-2"
            >
              <input
                type="password"
                placeholder="ADMIN_ACCESS_SECRET"
                value={secretInput}
                onChange={(e) => setSecretInput(e.target.value)}
                className="w-80 rounded border border-zinc-700 bg-zinc-900 px-2 py-1 text-xs text-zinc-100 placeholder-zinc-500 focus:outline-none focus:ring-1 focus:ring-zinc-500"
                autoFocus
              />
              <button
                type="submit"
                className="rounded border border-emerald-500/50 bg-emerald-500/20 px-3 py-1 text-xs text-emerald-200 hover:bg-emerald-500/30"
              >
                Save & retry
              </button>
              {adminSecret ? (
                <button
                  type="button"
                  onClick={() => {
                    saveSecret("");
                    setSecretInput("");
                  }}
                  className="rounded border border-zinc-700 px-3 py-1 text-xs text-zinc-400 hover:bg-zinc-800"
                >
                  Clear
                </button>
              ) : null}
              <button
                type="button"
                onClick={() => setShowSecretPanel(false)}
                className="rounded border border-zinc-700 px-3 py-1 text-xs text-zinc-400 hover:bg-zinc-800"
              >
                Dismiss
              </button>
            </form>
          </div>
        ) : null}

        {error ? (
          <div className="rounded border border-rose-500/40 bg-rose-500/10 p-3 text-sm text-rose-200">
            <AlertTriangle className="mr-1 inline h-4 w-4" /> {error}
          </div>
        ) : null}

        {/* --- System health --- */}
        <section>
          <h2 className="mb-2 text-sm uppercase tracking-wide text-zinc-400">System health</h2>
          <div className="grid grid-cols-2 gap-3 md:grid-cols-4 lg:grid-cols-7">
            <StatBox
              label="V4 hard gate"
              value={health?.v4HardGateEnabled ? "ON" : "OFF"}
              sub={health?.v4Enabled ? "v4 enabled" : "v4 disabled"}
              accent={
                health?.v4HardGateEnabled
                  ? "border-emerald-500/40 bg-emerald-500/10"
                  : "border-amber-500/40 bg-amber-500/10"
              }
            />
            <StatBox
              label="Symbols covered"
              value={health?.regimeBuild.symbolsCovered ?? (healthLoading ? "…" : "—")}
              sub={`latest build ${classifierAge}`}
            />
            <StatBox
              label="Stage-C survivors"
              value={health?.stageCSurvivors ?? (healthLoading ? "…" : "—")}
            />
            <StatBox
              label="WF eligible"
              value={health?.walkforwardCounts.eligible || 0}
              accent="border-emerald-500/40 bg-emerald-500/10"
            />
            <StatBox
              label="WF in progress"
              value={health?.walkforwardCounts.in_progress || 0}
              accent="border-sky-500/40 bg-sky-500/10"
            />
            <StatBox
              label="WF no passing cells"
              value={health?.walkforwardCounts.no_passing_cells || 0}
              accent="border-rose-500/40 bg-rose-500/10"
            />
            <StatBox
              label="WF cluster-capped"
              value={health?.walkforwardCounts.cluster_cap_exceeded || 0}
              sub="skipped — same bet"
              accent="border-amber-500/40 bg-amber-500/10"
            />
            <StatBox
              label="WF pending"
              value={health?.pendingWalkforward ?? (healthLoading ? "…" : "—")}
              sub={`${health?.walkforwardTotal ?? 0} done`}
            />
          </div>
        </section>

        {/* --- Status histogram --- */}
        <section>
          <h2 className="mb-2 text-sm uppercase tracking-wide text-zinc-400">
            Deployment v4 status · {totalDeployments} tracked
            {deploymentsLoading ? <span className="ml-2 text-xs text-zinc-500">loading…</span> : null}
          </h2>
          <div className="flex flex-wrap gap-2">
            <button
              onClick={() => setStatusFilter("all")}
              className={`rounded border px-2.5 py-1 text-xs ${
                statusFilter === "all"
                  ? "border-zinc-300 bg-zinc-800 text-white"
                  : "border-zinc-700 text-zinc-400 hover:bg-zinc-800"
              }`}
            >
              All · {totalDeployments}
            </button>
            {STATUS_ORDER.map((status) => {
              const meta = STATUS_META[status];
              const count = deploymentsResp?.statusHistogram[status] || 0;
              const active = statusFilter === status;
              return (
                <button
                  key={status}
                  onClick={() => setStatusFilter(active ? "all" : status)}
                  className={`rounded border px-2.5 py-1 text-xs ${active ? "ring-2 ring-offset-1 ring-offset-zinc-950" : ""} ${meta.cls}`}
                  title={meta.label}
                >
                  {meta.emoji} {meta.label} · {count}
                </button>
              );
            })}
          </div>
        </section>

        {/* --- Deployments table --- */}
        <section>
          <div className="mb-2 flex items-center justify-between gap-2">
            <h2 className="text-sm uppercase tracking-wide text-zinc-400">Deployments</h2>
            <input
              type="text"
              placeholder="filter symbol, venue, session, strategy…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="w-72 rounded border border-zinc-700 bg-zinc-900 px-2 py-1 text-xs text-zinc-100 placeholder-zinc-500 focus:outline-none focus:ring-1 focus:ring-zinc-500"
            />
          </div>
          <div className="overflow-x-auto rounded-lg border border-zinc-800">
            <table className="w-full min-w-[720px] text-xs">
              <thead className="bg-zinc-900 text-zinc-400">
                <tr>
                  <th className="px-3 py-2 text-left">Status</th>
                  <th className="px-3 py-2 text-left">Symbol</th>
                  <th className="px-3 py-2 text-left">Session</th>
                  <th className="px-3 py-2 text-left">Strategy</th>
                  <th className="px-3 py-2 text-left">Current regime</th>
                  <th className="px-3 py-2 text-left">Allowed cells</th>
                  <th className="px-3 py-2 text-left">Reason</th>
                </tr>
              </thead>
              <tbody>
                {filteredDeployments.slice(0, 200).map((row) => {
                  const meta = STATUS_META[row.v4Status];
                  return (
                    <tr key={row.deploymentId} className="border-t border-zinc-800/80 hover:bg-zinc-900/50">
                      <td className="px-3 py-1.5">
                        <span className={`inline-block rounded border px-1.5 py-0.5 text-[10px] ${meta.cls}`}>
                          {meta.emoji} {meta.label}
                        </span>
                      </td>
                      <td className="px-3 py-1.5 font-mono">
                        <span className="text-zinc-500">{row.venue}/</span>
                        {row.symbol}
                      </td>
                      <td className="px-3 py-1.5">{row.session}</td>
                      <td className="px-3 py-1.5 font-mono text-zinc-400" title={row.tuneId}>
                        {row.strategyId}
                      </td>
                      <td className="px-3 py-1.5">
                        {row.currentRegime.cellId ? (
                          <CellChip
                            cellId={row.currentRegime.cellId}
                            highlight={row.envelope.allowedCells.includes(row.currentRegime.cellId)}
                          />
                        ) : (
                          <span className="text-zinc-500">no regime</span>
                        )}
                      </td>
                      <td className="px-3 py-1.5">
                        <div className="flex flex-wrap gap-1">
                          {row.envelope.allowedCells.length === 0 ? (
                            <span className="text-zinc-500">—</span>
                          ) : (
                            row.envelope.allowedCells.map((cell) => (
                              <CellChip
                                key={cell}
                                cellId={cell}
                                highlight={row.currentRegime.cellId === cell}
                              />
                            ))
                          )}
                        </div>
                      </td>
                      <td className="px-3 py-1.5 text-zinc-400" title={row.reason || ""}>
                        {row.reason ? <span className="font-mono">{row.reason}</span> : "—"}
                      </td>
                    </tr>
                  );
                })}
                {filteredDeployments.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="px-3 py-6 text-center text-zinc-500">
                      <CircleSlash className="mr-1 inline h-3.5 w-3.5" /> No deployments match the current filter.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
            {filteredDeployments.length > 200 ? (
              <div className="border-t border-zinc-800 px-3 py-1.5 text-[11px] text-zinc-500">
                Showing 200 of {filteredDeployments.length} — refine the filter to see more.
              </div>
            ) : null}
          </div>
        </section>

        {/* --- Recent walk-forwards + transitions side by side on wide screens --- */}
        <section className="grid gap-6 lg:grid-cols-2">
          <div>
            <h2 className="mb-2 flex items-center gap-1 text-sm uppercase tracking-wide text-zinc-400">
              <TrendingUp className="h-4 w-4" /> Recent walk-forward results
              {recentLoading ? <span className="ml-2 text-xs text-zinc-500">loading…</span> : null}
            </h2>
            <div className="overflow-hidden rounded-lg border border-zinc-800">
              <table className="w-full text-xs">
                <thead className="bg-zinc-900 text-zinc-400">
                  <tr>
                    <th className="px-3 py-2 text-left">When</th>
                    <th className="px-3 py-2 text-left">Symbol</th>
                    <th className="px-3 py-2 text-left">Status</th>
                    <th className="px-3 py-2 text-left">Cells</th>
                    <th className="px-3 py-2 text-left">Duration</th>
                  </tr>
                </thead>
                <tbody>
                  {(recent?.recentWalkforward || []).slice(0, 30).map((row, idx) => {
                    const isEligible = row.eligible;
                    const isInProgress = row.status === "in_progress";
                    const isClusterCapped = row.status === "cluster_cap_exceeded";
                    const isOverbroad =
                      row.status === "regime_overbroad_pending_review" ||
                      row.status === "regime_overbroad_auto_rejected";
                    return (
                      <tr key={`${row.deploymentId}-${idx}`} className="border-t border-zinc-800/80">
                        <td className="px-3 py-1.5 text-zinc-400">{fmtAgo(row.evaluatedAtMs)}</td>
                        <td className="px-3 py-1.5 font-mono">
                          <span className="text-zinc-500">{row.venue}/</span>
                          {row.symbol}
                        </td>
                        <td className="px-3 py-1.5">
                          <span
                            className={`rounded border px-1.5 py-0.5 text-[10px] ${
                              isEligible
                                ? "border-emerald-500/40 bg-emerald-500/20 text-emerald-300"
                                : isInProgress
                                  ? "border-sky-500/40 bg-sky-500/20 text-sky-300"
                                  : isClusterCapped
                                    ? "border-amber-500/40 bg-amber-500/20 text-amber-300"
                                    : isOverbroad
                                      ? "border-purple-500/40 bg-purple-500/20 text-purple-300"
                                      : "border-rose-500/40 bg-rose-500/20 text-rose-300"
                            }`}
                            title={
                              isClusterCapped
                                ? "Skipped — same-cluster cap reached (top 2 per cluster kept)"
                                : undefined
                            }
                          >
                            {row.status}
                          </span>
                        </td>
                        <td className="px-3 py-1.5">
                          <span className="text-emerald-300">{row.strictPassingCells}</span>
                          <span className="text-zinc-500"> / {row.occupiedCells}</span>
                        </td>
                        <td className="px-3 py-1.5 text-zinc-400">{fmtDuration(row.durationMs)}</td>
                      </tr>
                    );
                  })}
                  {!recent?.recentWalkforward.length ? (
                    <tr>
                      <td colSpan={5} className="px-3 py-6 text-center text-zinc-500">
                        No walk-forward results yet.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </div>

          <div>
            <h2 className="mb-2 flex items-center gap-1 text-sm uppercase tracking-wide text-zinc-400">
              <History className="h-4 w-4" /> Recent regime transitions
              {recentLoading ? <span className="ml-2 text-xs text-zinc-500">loading…</span> : null}
            </h2>
            <div className="overflow-hidden rounded-lg border border-zinc-800">
              <table className="w-full text-xs">
                <thead className="bg-zinc-900 text-zinc-400">
                  <tr>
                    <th className="px-3 py-2 text-left">Week</th>
                    <th className="px-3 py-2 text-left">Symbol</th>
                    <th className="px-3 py-2 text-left">From</th>
                    <th className="px-3 py-2 text-left">To</th>
                  </tr>
                </thead>
                <tbody>
                  {(recent?.recentTransitions || []).slice(0, 30).map((row, idx) => (
                    <tr key={`${row.venue}-${row.symbol}-${row.transitionWeekStartMs}-${idx}`} className="border-t border-zinc-800/80">
                      <td className="px-3 py-1.5 text-zinc-400">
                        <Clock className="mr-1 inline h-3 w-3" />
                        {fmtDate(row.transitionWeekStartMs)}
                      </td>
                      <td className="px-3 py-1.5 font-mono">
                        <span className="text-zinc-500">{row.venue}/</span>
                        {row.symbol}
                      </td>
                      <td className="px-3 py-1.5">
                        {row.fromCellId ? <CellChip cellId={row.fromCellId} /> : <span className="text-zinc-500">—</span>}
                      </td>
                      <td className="px-3 py-1.5">
                        <CellChip cellId={row.toCellId} highlight />
                      </td>
                    </tr>
                  ))}
                  {!recent?.recentTransitions.length ? (
                    <tr>
                      <td colSpan={4} className="px-3 py-6 text-center text-zinc-500">
                        No transitions recorded.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </div>
        </section>

        <footer className="flex items-center justify-between border-t border-zinc-800 pt-4 text-[11px] text-zinc-500">
          <span className="inline-flex items-center gap-1">
            <Layers className="h-3 w-3" /> Sources:{" "}
            <code>/api/scalp/v4/health</code> · <code>/deployments</code> · <code>/recent</code>
          </span>
          <span>
            <CheckCircle2 className="mr-1 inline h-3 w-3 text-emerald-500" /> v4 dashboard · macro-regime view
          </span>
        </footer>
      </main>
    </div>
  );
}
