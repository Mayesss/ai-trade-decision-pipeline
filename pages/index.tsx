import { useCallback, useEffect, useMemo, useState } from "react";
import Head from "next/head";
import Link from "next/link";

// Storage key shared with the legacy page so the admin secret transfers freely.
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
    /* ignore */
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

interface CellDetail {
  cellId: string;
  windows: number;
  trades: number;
  distinctEpochCount: number;
  netR: number;
  expectancyR: number;
  positiveWindowPct: number;
  p25ExpectancyR: number;
  maxDrawdownR: number;
  crossRegimeTradePct: number;
  bootstrapP05ExpectancyR: number | null;
  sharpe: number | null;
  deflatedScore: number | null;
  strictPassed: boolean;
  relaxedPassed: boolean;
  reason: string | null;
  windowExpectancyR: number[] | null;
}

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
    cells?: CellDetail[];
  };
  currentRegime: { cellId: string | null; stale: boolean; updatedAtMs: number | null; weeksInCell: number | null };
  lastEntryAtMs: number | null;
  openPositionCount: number;
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
  throughput: {
    lastHour: number;
    buckets12h: number[];
    etaHours: number | null;
  };
  rollover: {
    currentWeekStartMs: number;
    previousWeekStartMs: number;
    walkedThisWeek: number;
    walkedLastWeek: number;
    regimesBuiltThisWeek: number;
    regimesExpected: number;
    newSurvivorsDiscovered: number;
    newSurvivorsWalked: number;
  };
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
  recentTrades: TradeRow[];
}

// ─── formatting helpers ───────────────────────────────────────────────────────

function fmtAgo(ms: number | null | undefined): string {
  if (!ms) return "—";
  const diff = Math.max(0, Date.now() - ms);
  const sec = Math.floor(diff / 1000);
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m`;
  const hr = Math.floor(min / 60);
  if (hr < 48) return `${hr}h`;
  return `${Math.floor(hr / 24)}d`;
}

function fmtClock(ms: number | null | undefined): string {
  if (!ms) return "—";
  const d = new Date(ms);
  return `${String(d.getUTCHours()).padStart(2, "0")}:${String(d.getUTCMinutes()).padStart(2, "0")}`;
}

function fmtWeek(ms: number | null | undefined): string {
  if (!ms) return "—";
  const d = new Date(ms);
  const year = d.getUTCFullYear();
  const start = Date.UTC(year, 0, 1);
  const week = Math.floor((d.getTime() - start) / (7 * 24 * 60 * 60_000)) + 1;
  return `W${String(week).padStart(2, "0")}`;
}

// Unicode block sparkline — 8 levels. Renders the array left-to-right as-given;
// callers should pass values in oldest→newest order (natural time flow).
// Handles negative values by anchoring the bottom of the scale at min(0, min)
// and the top at max(0, max) so positive bars grow up and negative bars stay
// near the baseline.
function sparkline(values: number[]): string {
  const chars = ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"];
  if (values.length === 0) return "";
  const min = Math.min(0, ...values);
  const max = Math.max(0, ...values);
  const range = max - min;
  return values
    .map((n) => {
      if (range === 0) return " ";
      const norm = (n - min) / range;
      const idx = Math.max(0, Math.min(chars.length - 1, Math.floor(norm * chars.length)));
      return chars[idx];
    })
    .join("");
}

function fmtEta(hours: number | null): string {
  if (hours === null || !Number.isFinite(hours)) return "—";
  if (hours < 1) return `${Math.max(1, Math.round(hours * 60))}m`;
  if (hours < 24) return `${hours.toFixed(1)}h`;
  const d = Math.floor(hours / 24);
  const h = Math.round(hours - d * 24);
  return h > 0 ? `${d}d ${h}h` : `${d}d`;
}

function abbrCell(cellId: string | null): string {
  if (!cellId || cellId === "unknown") return "unknown";
  // vol=mid|trend=trending_up|risk=risk_on  ->  vol=mid|tr=up|risk=on
  return cellId
    .replace(/trend=trending_up/g, "tr=up")
    .replace(/trend=trending_down/g, "tr=dn")
    .replace(/trend=choppy/g, "tr=chop")
    .replace(/risk=risk_on/g, "risk=on")
    .replace(/risk=risk_off/g, "risk=off")
    .replace(/risk=neutral/g, "risk=neu");
}

// Minimalist loading placeholder — three dots that pulse, matches text rhythm.
function Skeleton({ label }: { label: string }) {
  return (
    <div className="text-zinc-600 animate-pulse">{"  "}{label} <span className="inline-block">· · ·</span></div>
  );
}

// One-liner section header — uses pseudo-rule via `flex-1` border so it grows
// with the viewport width and stays readable on mobile.
function SectionHeader({ title }: { title: string }) {
  return (
    <div className="mt-5 flex items-center gap-2 text-zinc-500">
      <span className="text-zinc-500">══</span>
      <span className="whitespace-nowrap text-zinc-400 uppercase tracking-wider text-[12px]">{title}</span>
      <span className="flex-1 border-t border-zinc-800/80" />
    </div>
  );
}

function bar(value: number, max: number, width = 28): string {
  if (!max || max <= 0) return " ".repeat(width);
  const filled = Math.max(0, Math.min(width, Math.round((value / max) * width)));
  return "█".repeat(filled) + "░".repeat(width - filled);
}

// ─── component ────────────────────────────────────────────────────────────────

export default function ScalpV4Dashboard() {
  const [health, setHealth] = useState<HealthResp | null>(null);
  const [deploymentsResp, setDeploymentsResp] = useState<DeploymentsResp | null>(null);
  const [recent, setRecent] = useState<RecentResp | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loadedAt, setLoadedAt] = useState<number | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [adminSecret, setAdminSecret] = useState("");
  const [secretInput, setSecretInput] = useState("");
  const [showSecretPanel, setShowSecretPanel] = useState(false);
  const [unauthorized, setUnauthorized] = useState(false);
  const [showAllRegimes, setShowAllRegimes] = useState(false);
  const [showAllActiveDeployments, setShowAllActiveDeployments] = useState(false);
  // Click-to-expand: deployment IDs whose per-cell detail is currently visible.
  const [expandedDeployments, setExpandedDeployments] = useState<Set<string>>(new Set());
  const toggleExpanded = useCallback((id: string) => {
    setExpandedDeployments((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);
  // Tracks whether we've finished reading localStorage for the admin secret.
  // load() must wait for this — otherwise the first call fires with an empty
  // secret (because the hydration effect hasn't run yet), gets a 401, and
  // opens the secret panel even though a valid secret is stored.
  const [secretHydrated, setSecretHydrated] = useState(false);

  useEffect(() => {
    const stored = readStoredAdminSecret();
    if (stored) {
      setAdminSecret(stored);
      setSecretInput(stored);
    } else {
      setShowSecretPanel(true);
    }
    setSecretHydrated(true);
  }, []);

  const load = useCallback(async () => {
    const headers: Record<string, string> = {};
    if (adminSecret) headers["x-admin-access-secret"] = adminSecret;
    // Each fetch fires in parallel AND calls setState the moment it resolves.
    // The slowest endpoint no longer gates the faster ones, so health renders
    // first, then deployments, then recent (in whatever order they complete).
    async function fetchOne<T extends { ok: boolean }>(
      url: string,
      apply: (data: T) => void,
    ): Promise<{ unauthorized?: boolean; error?: string }> {
      try {
        const res = await fetch(url, { headers, credentials: "include" });
        if (res.status === 401) return { unauthorized: true };
        if (!res.ok) return { error: `${url} → HTTP ${res.status}` };
        const data = (await res.json()) as T;
        if (!data.ok) return { error: (data as any).error || `${url} → request_failed` };
        apply(data);
        return {};
      } catch (err) {
        return { error: (err as Error)?.message || String(err) };
      }
    }
    const [h, d, r] = await Promise.all([
      fetchOne<HealthResp>("/api/scalp/v4/health", setHealth),
      fetchOne<DeploymentsResp>("/api/scalp/v4/deployments", setDeploymentsResp),
      fetchOne<RecentResp>("/api/scalp/v4/recent", setRecent),
    ]);
    if (h.unauthorized || d.unauthorized || r.unauthorized) {
      setUnauthorized(true);
      setShowSecretPanel(true);
      setError("Unauthorized — admin secret missing or invalid.");
      return;
    }
    setError(h.error || d.error || r.error || null);
    setUnauthorized(false);
    // Successful load means the secret is valid — auto-close the panel if it
    // was open from a previous 401.
    setShowSecretPanel(false);
    setLoadedAt(Date.now());
  }, [adminSecret]);

  useEffect(() => {
    // Wait for localStorage hydration before the first load() — otherwise the
    // first call fires with an empty adminSecret, gets 401, and pops the
    // panel even when a valid secret is stored.
    if (!secretHydrated) return;
    load();
  }, [load, secretHydrated]);

  useEffect(() => {
    if (!autoRefresh || unauthorized || !secretHydrated) return;
    const t = setInterval(load, 30_000);
    return () => clearInterval(t);
  }, [autoRefresh, load, unauthorized, secretHydrated]);

  const saveSecret = useCallback((next: string) => {
    const value = next.trim();
    persistAdminSecret(value);
    setAdminSecret(value);
    setUnauthorized(false);
    setShowSecretPanel(false);
  }, []);

  // ─── derived: split deployments by status for the sections ──────────────────
  const trading = useMemo(() => {
    if (!deploymentsResp) return [];
    return deploymentsResp.deployments
      .filter((r) => r.v4Status === "trading")
      .sort((a, b) => (b.lastEntryAtMs || 0) - (a.lastEntryAtMs || 0));
  }, [deploymentsResp]);
  const dormant = useMemo(() => {
    if (!deploymentsResp) return [];
    return deploymentsResp.deployments
      .filter((r) => r.v4Status === "dormant_wrong_regime" || r.v4Status === "dormant_no_regime")
      .sort((a, b) => `${a.venue}/${a.symbol}`.localeCompare(`${b.venue}/${b.symbol}`));
  }, [deploymentsResp]);
  const eligibleNotPromoted = useMemo(() => {
    if (!deploymentsResp) return [];
    return deploymentsResp.deployments
      .filter((r) => r.v4Status === "eligible_not_promoted")
      .sort((a, b) => (b.envelope.strictPassingCells - a.envelope.strictPassingCells));
  }, [deploymentsResp]);
  // current-regime overview: enabled deployments' symbols, deduped by (venue, symbol)
  const regimeRows = useMemo(() => {
    if (!deploymentsResp) return [] as Array<{
      venue: string;
      symbol: string;
      cellId: string | null;
      weeksInCell: number | null;
      matches: number;
    }>;
    const seen = new Map<string, { venue: string; symbol: string; cellId: string | null; weeksInCell: number | null; matches: number }>();
    for (const row of deploymentsResp.deployments) {
      if (!row.enabled) continue;
      const key = `${row.venue}:${row.symbol}`;
      const matchesThis = row.currentRegime.cellId && row.envelope.allowedCells.includes(row.currentRegime.cellId) ? 1 : 0;
      const prev = seen.get(key);
      if (prev) {
        prev.matches += matchesThis;
      } else {
        seen.set(key, {
          venue: row.venue,
          symbol: row.symbol,
          cellId: row.currentRegime.cellId,
          weeksInCell: row.currentRegime.weeksInCell,
          matches: matchesThis,
        });
      }
    }
    return Array.from(seen.values()).sort((a, b) => {
      if (a.venue !== b.venue) return a.venue.localeCompare(b.venue);
      return a.symbol.localeCompare(b.symbol);
    });
  }, [deploymentsResp]);

  // Combined activity feed
  const activity = useMemo(() => {
    type Event = { tsMs: number; kind: "wf" | "regime" | "trade"; payload: any };
    const out: Event[] = [];
    if (recent) {
      for (const r of recent.recentWalkforward) out.push({ tsMs: r.evaluatedAtMs, kind: "wf", payload: r });
      for (const r of recent.recentTransitions) out.push({ tsMs: r.transitionWeekStartMs, kind: "regime", payload: r });
      for (const r of recent.recentTrades) out.push({ tsMs: r.tsMs, kind: "trade", payload: r });
    }
    return out.sort((a, b) => b.tsMs - a.tsMs).slice(0, 30);
  }, [recent]);

  // ─── render ──────────────────────────────────────────────────────────────────
  const totalDone = (health?.walkforwardTotal ?? 0);
  const totalPlanned = (health?.stageCSurvivors ?? 0);
  const pctDone = totalPlanned > 0 ? Math.round((totalDone / totalPlanned) * 100) : 0;
  const counts = health?.walkforwardCounts ?? {};
  const maxCount = Math.max(1, counts.eligible || 0, counts.no_passing_cells || 0, counts.cluster_cap_exceeded || 0, counts.in_progress || 0, counts.regime_overbroad_pending_review || 0);

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-200 font-mono text-[13px] leading-[1.45]">
      <Head>
        <title>scalp v4 · operator</title>
      </Head>
      <div className="mx-auto max-w-[1200px] px-4 py-4">
        {/* header */}
        <div className="flex flex-wrap items-baseline justify-between gap-2 border-b border-zinc-800 pb-2">
          <div className="flex items-baseline gap-3">
            <span className="text-zinc-100">SCALP V4</span>
            <span className="text-zinc-500">·</span>
            <span className="text-zinc-400">{new Date().toISOString().slice(0, 16).replace("T", " ")} UTC</span>
            <span className="text-zinc-500">·</span>
            <span className="text-zinc-500">classifier {health?.classifierVersion || "—"}</span>
          </div>
          <div className="flex items-center gap-3 text-zinc-500">
            <span>refresh 30s · last {loadedAt ? `${fmtAgo(loadedAt)} ago` : "—"}</span>
            <label className="flex items-center gap-1 cursor-pointer">
              <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />auto
            </label>
            <button onClick={load} className="text-zinc-300 hover:text-white">[refresh]</button>
            <button
              onClick={() => {
                setSecretInput(adminSecret);
                setShowSecretPanel((s) => !s);
              }}
              className={adminSecret ? "text-zinc-500 hover:text-zinc-300" : "text-amber-300 hover:text-amber-200"}
            >
              [{adminSecret ? "secret" : "set secret"}]
            </button>
            <Link href="/legacy" className="text-zinc-500 hover:text-zinc-300">[legacy]</Link>
          </div>
        </div>

        {/* admin secret panel */}
        {showSecretPanel ? (
          <div className={`mt-3 border ${unauthorized ? "border-rose-500/40 bg-rose-500/10" : "border-amber-500/40 bg-amber-500/10"} p-3`}>
            <div className="text-zinc-300">
              {unauthorized ? "admin access required" : "set admin access secret"} · key <span className="text-zinc-500">{ADMIN_SECRET_STORAGE_KEY}</span>
            </div>
            <form
              onSubmit={(e) => {
                e.preventDefault();
                saveSecret(secretInput);
                setTimeout(load, 0);
              }}
              className="mt-2 flex flex-wrap items-center gap-2"
            >
              <input
                type="password"
                placeholder="ADMIN_ACCESS_SECRET"
                value={secretInput}
                onChange={(e) => setSecretInput(e.target.value)}
                className="w-80 border border-zinc-700 bg-zinc-900 px-2 py-1 text-zinc-100 focus:outline-none focus:border-zinc-500"
                autoFocus
              />
              <button type="submit" className="border border-emerald-500/50 bg-emerald-500/20 px-3 py-1 text-emerald-200 hover:bg-emerald-500/30">save &amp; retry</button>
              {adminSecret ? (
                <button
                  type="button"
                  onClick={() => {
                    saveSecret("");
                    setSecretInput("");
                  }}
                  className="text-zinc-500 hover:text-zinc-300"
                >clear</button>
              ) : null}
              <button type="button" onClick={() => setShowSecretPanel(false)} className="text-zinc-500 hover:text-zinc-300">dismiss</button>
            </form>
          </div>
        ) : null}

        {error && !unauthorized ? (
          <pre className="mt-3 whitespace-pre-wrap text-rose-400">⚠ {error}</pre>
        ) : null}

        {/* HEALTH */}
        {/* HEALTH */}
        <SectionHeader title="health" />
        {health ? (
          <div className="mt-1 flex flex-wrap gap-x-6 gap-y-0.5 pl-2">
            <span>
              <span className="text-zinc-500">hard gate </span>
              <span className={health.v4HardGateEnabled ? "text-emerald-400" : "text-amber-400"}>{health.v4HardGateEnabled ? "ON" : "OFF"}</span>
            </span>
            <span>
              <span className="text-zinc-500">v4 </span>
              <span className={health.v4Enabled ? "text-emerald-400" : "text-amber-400"}>{health.v4Enabled ? "enabled" : "disabled"}</span>
            </span>
            <span>
              <span className="text-zinc-500">freeze </span>
              <span className="text-emerald-400">inactive</span>
            </span>
            <span>
              <span className="text-zinc-500">regimes </span>
              <span className="text-zinc-100">{health.regimeBuild.symbolsCovered}</span>
              <span className="text-zinc-500"> symbols valid</span>
            </span>
            <span>
              <span className="text-zinc-500">last build </span>
              <span className="text-zinc-100">{health.regimeBuild.latestWeekStartMs ? fmtAgo(health.regimeBuild.latestWeekStartMs + 7 * 24 * 60 * 60_000) + " ago" : "—"}</span>
            </span>
          </div>
        ) : (
          <Skeleton label="loading health" />
        )}

        {/* PIPELINE */}
        <SectionHeader title={`pipeline · ${fmtWeek(Date.now())}`} />
        {health ? (
          <div className="mt-1 pl-2">
            <div className="grid grid-cols-[auto_1fr] md:grid-cols-[10rem_1fr] gap-x-3 items-baseline">
              <span className="text-zinc-500">stage-C survivors</span>
              <span>
                <span className="text-zinc-100">{totalPlanned}</span>
                <span className="text-zinc-500">  progress </span>
                <span className="text-emerald-400 font-mono whitespace-pre">{bar(totalDone, totalPlanned)}</span>
                <span className="text-zinc-500"> {pctDone}%</span>
              </span>
              <span className="text-zinc-500">walk-forward</span>
              <span className="flex flex-wrap gap-x-3">
                <span><span className="text-zinc-100">{totalDone}</span><span className="text-zinc-500"> done</span></span>
                <span><span className="text-sky-400">{counts.in_progress || 0}</span><span className="text-zinc-500"> running</span></span>
                <span><span className="text-zinc-100">{health.pendingWalkforward}</span><span className="text-zinc-500"> pending</span></span>
              </span>
              <span className="text-zinc-500">throughput</span>
              <span className="flex flex-wrap items-baseline gap-x-3">
                <span><span className="text-zinc-100">{health.throughput.lastHour}</span><span className="text-zinc-500">/hr last 1h</span></span>
                <span className="text-zinc-500">12h </span>
                <span className="text-emerald-400 font-mono whitespace-pre" title={health.throughput.buckets12h.slice().reverse().join(" / ")}>
                  {sparkline([...health.throughput.buckets12h].reverse())}
                </span>
              </span>
              <span className="text-zinc-500">eta</span>
              <span>
                <span className="text-zinc-100">{fmtEta(health.throughput.etaHours)}</span>
                <span className="text-zinc-500"> at current rate</span>
                {health.throughput.lastHour === 0 && health.pendingWalkforward > 0 ? (
                  <span className="text-amber-400/80"> · idle</span>
                ) : null}
              </span>
            </div>
            <div className="mt-2 space-y-0.5">
              <CountLineGrid label="● eligible" value={counts.eligible || 0} max={maxCount} colorClass="text-emerald-400" />
              <CountLineGrid label="○ no_passing" value={counts.no_passing_cells || 0} max={maxCount} colorClass="text-rose-400" />
              <CountLineGrid label="◐ cluster_capped" value={counts.cluster_cap_exceeded || 0} max={maxCount} colorClass="text-amber-400" />
              <CountLineGrid label="◔ in_progress" value={counts.in_progress || 0} max={maxCount} colorClass="text-sky-400" />
              <CountLineGrid label="· overbroad" value={counts.regime_overbroad_pending_review || 0} max={maxCount} colorClass="text-zinc-400" />
            </div>
          </div>
        ) : (
          <Skeleton label="loading pipeline" />
        )}

        {/* ROLLOVER */}
        <SectionHeader title={`rollover · week of ${health ? new Date(health.rollover.currentWeekStartMs).toISOString().slice(0, 10) : "—"}`} />
        {health ? (
          <RolloverProgress rollover={health.rollover} />
        ) : (
          <Skeleton label="loading rollover" />
        )}

        {/* LIVE summary */}
        <SectionHeader title="live" />
        {deploymentsResp ? (
          <div className="mt-1 grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 pl-2 items-baseline">
            <span className="text-zinc-500">trading now</span>
            <span>
              <span className="text-emerald-400">{trading.length}</span>
              <span className="text-zinc-500"> deployments · </span>
              <span className="text-zinc-100">{trading.reduce((acc, r) => acc + r.openPositionCount, 0)}</span>
              <span className="text-zinc-500"> open position(s)</span>
            </span>
            <span className="text-zinc-500">dormant by regime</span>
            <span>
              <span className="text-amber-400">{dormant.length}</span>
              <span className="text-zinc-500"> deployments · awaiting regime match</span>
            </span>
            <span className="text-zinc-500">awaiting promote</span>
            <span>
              <span className="text-sky-400">{eligibleNotPromoted.length}</span>
              <span className="text-zinc-500"> candidates · next promote cycle imminent</span>
            </span>
          </div>
        ) : (
          <Skeleton label="loading live state" />
        )}

        {/* ACTIVE DEPLOYMENTS */}
        <SectionHeader title="active deployments · live=● dormant=○" />
        {deploymentsResp ? (
          (trading.length === 0 && dormant.length === 0) ? (
            <div className="pl-2 text-zinc-500">(no enabled deployments)</div>
          ) : (
            <div className="mt-1 pl-2 space-y-0.5">
              {trading.map((row) => <DeploymentRowView key={row.deploymentId} row={row} kind="trading" />)}
              {(showAllActiveDeployments ? dormant : dormant.slice(0, 8)).map((row) => (
                <DeploymentRowView key={row.deploymentId} row={row} kind="dormant" />
              ))}
              {dormant.length > 8 && !showAllActiveDeployments ? (
                <button
                  onClick={() => setShowAllActiveDeployments(true)}
                  className="text-zinc-500 hover:text-zinc-300"
                >
                  … show all {dormant.length} dormant
                </button>
              ) : null}
              {dormant.length > 8 && showAllActiveDeployments ? (
                <button
                  onClick={() => setShowAllActiveDeployments(false)}
                  className="text-zinc-500 hover:text-zinc-300"
                >
                  ↑ collapse
                </button>
              ) : null}
            </div>
          )
        ) : (
          <Skeleton label="loading deployments" />
        )}

        {/* ELIGIBLE NOT PROMOTED */}
        <SectionHeader title={`eligible · not yet promoted (${eligibleNotPromoted.length})`} />
        {deploymentsResp ? (
          eligibleNotPromoted.length === 0 ? (
            <div className="pl-2 text-zinc-500">(none)</div>
          ) : (
            <div className="mt-1 pl-2 space-y-0.5">
              {eligibleNotPromoted.slice(0, 30).map((row) => (
                <EligibleRowView
                  key={row.deploymentId}
                  row={row}
                  expanded={expandedDeployments.has(row.deploymentId)}
                  onToggle={() => toggleExpanded(row.deploymentId)}
                />
              ))}
              {eligibleNotPromoted.length > 30 ? (
                <div className="text-zinc-500">… {eligibleNotPromoted.length - 30} more</div>
              ) : null}
            </div>
          )
        ) : (
          <Skeleton label="loading eligibles" />
        )}

        {/* REGIMES */}
        <SectionHeader title="regimes · current week" />
        {deploymentsResp ? (
          regimeRows.length === 0 ? (
            <div className="pl-2 text-zinc-500">(no enabled symbols)</div>
          ) : (
            <div className="mt-1 pl-2 space-y-0.5">
              {(showAllRegimes ? regimeRows : regimeRows.slice(0, 15)).map((row) => (
                <RegimeRowView key={`${row.venue}:${row.symbol}`} row={row} />
              ))}
              {regimeRows.length > 15 ? (
                <button
                  onClick={() => setShowAllRegimes((s) => !s)}
                  className="text-zinc-500 hover:text-zinc-300"
                >
                  {showAllRegimes ? "↑ collapse" : `… show all ${regimeRows.length}`}
                </button>
              ) : null}
            </div>
          )
        ) : (
          <Skeleton label="loading regimes" />
        )}

        {/* ACTIVITY */}
        <SectionHeader title={`activity · last ${activity.length}`} />
        {recent ? (
          activity.length === 0 ? (
            <div className="pl-2 text-zinc-500">(no recent events)</div>
          ) : (
            <div className="mt-1 pl-2 space-y-0.5">
              {activity.map((event, idx) => <ActivityRowView key={`a:${idx}`} event={event} />)}
            </div>
          )
        ) : (
          <Skeleton label="loading activity" />
        )}

        <div className="border-t border-zinc-800 pt-2 text-zinc-600">
          sources  /api/scalp/v4/health · /deployments · /recent
        </div>
      </div>
    </div>
  );
}

// ─── reusable row views ─────────────────────────────────────────────────────
// All rows use CSS grid for column alignment so long symbols (PENDLEUSDT) don't
// shift the layout. On mobile (< md) the grid collapses to flex-wrap and the
// less-critical columns wrap onto new lines.

function CountLineGrid({
  label,
  value,
  max,
  colorClass,
}: {
  label: string;
  value: number;
  max: number;
  colorClass: string;
}) {
  return (
    <div className="grid grid-cols-[10rem_3rem_1fr] gap-x-3 items-baseline">
      <span className={colorClass}>{label}</span>
      <span className="text-zinc-100 text-right">{value}</span>
      <span className="text-zinc-500 font-mono whitespace-pre overflow-hidden">{bar(value, max, 30)}</span>
    </div>
  );
}

function DeploymentRowView({ row, kind }: { row: DeploymentRow; kind: "trading" | "dormant" }) {
  const isTrading = kind === "trading";
  const family = row.tuneId.split("_").slice(0, 4).join("_");
  const dot = isTrading ? "●" : "○";
  const dotClass = isTrading ? "text-emerald-400" : "text-amber-400";
  const meta = isTrading ? (
    <>
      <span className="text-zinc-500">last </span>
      <span className="text-zinc-200">{row.lastEntryAtMs ? fmtAgo(row.lastEntryAtMs) : "never"}</span>
      <span className="text-zinc-500"> · open </span>
      <span className="text-zinc-100">{row.openPositionCount}</span>
    </>
  ) : (
    <span className="text-amber-400/80">
      blocked: {row.v4Status === "dormant_no_regime" ? "no regime data" : "regime mismatch"}
    </span>
  );
  return (
    <div>
      {/* mobile */}
      <div className="md:hidden">
        <div className="flex items-baseline gap-x-2">
          <span className={dotClass}>{dot}</span>
          <span className="text-zinc-100 truncate"><span className="text-zinc-500">{row.venue}/</span>{row.symbol}</span>
          <span className="text-zinc-500 text-[12px]">{row.session}</span>
        </div>
        <div className="pl-5 text-[12px] text-zinc-400 truncate" title={row.tuneId}>{family}</div>
        <div className="pl-5 text-[12px]">{meta}</div>
      </div>
      {/* desktop */}
      <div className="hidden md:grid md:grid-cols-[1.25rem_5rem_8rem_6rem_14rem_1fr] md:gap-x-2 md:items-baseline">
        <span className={dotClass}>{dot}</span>
        <span className="text-zinc-500">{row.venue}</span>
        <span className="text-zinc-100 truncate">{row.symbol}</span>
        <span className="text-zinc-500">{row.session}</span>
        <span className="text-zinc-400 truncate" title={row.tuneId}>{family}</span>
        <span className="text-zinc-500 text-[13px]">{meta}</span>
      </div>
      {!isTrading ? (
        <div className="pl-5 md:ml-6 mt-0.5 text-[11px] md:text-[12px] text-zinc-500 flex flex-wrap gap-x-3">
          <span>
            <span>allowed </span>
            <span className="text-zinc-400">[{row.envelope.allowedCells.map(abbrCell).join(", ") || "none"}]</span>
          </span>
          <span>
            <span>current </span>
            <span className="text-amber-300">{abbrCell(row.currentRegime.cellId)}</span>
          </span>
        </div>
      ) : null}
    </div>
  );
}

function EligibleRowView({
  row,
  expanded,
  onToggle,
}: {
  row: DeploymentRow;
  expanded: boolean;
  onToggle: () => void;
}) {
  const family = row.tuneId.split("_").slice(0, 4).join("_");
  const allowedHead = row.envelope.allowedCells.slice(0, 2).map(abbrCell).join(", ");
  const more = row.envelope.allowedCells.length > 2 ? "…" : "";
  const meta = (
    <>
      <span className="text-zinc-500">cells </span>
      <span className="text-emerald-400">{row.envelope.strictPassingCells}</span>
      <span className="text-zinc-500">/{row.envelope.occupiedCells} </span>
      <span className="text-zinc-500">[</span>
      <span className="text-zinc-300">{allowedHead}{more}</span>
      <span className="text-zinc-500">]</span>
      <span className="ml-2 text-zinc-500">{expanded ? "▾" : "▸"}</span>
    </>
  );
  const cells = row.envelope.cells || [];
  return (
    <div>
      <button
        onClick={onToggle}
        className="w-full text-left hover:bg-zinc-900/40 -mx-2 px-2 py-0.5 rounded-sm transition-colors"
        title={expanded ? "click to collapse" : "click to expand cell detail"}
      >
        {/* mobile */}
        <div className="md:hidden">
          <div className="flex items-baseline gap-x-2">
            <span className="text-sky-400">▸</span>
            <span className="text-zinc-100 truncate"><span className="text-zinc-500">{row.venue}/</span>{row.symbol}</span>
            <span className="text-zinc-500 text-[12px]">{row.session}</span>
          </div>
          <div className="pl-5 text-[12px] text-zinc-400 truncate" title={row.tuneId}>{family}</div>
          <div className="pl-5 text-[12px] truncate">{meta}</div>
        </div>
        {/* desktop */}
        <div className="hidden md:grid md:grid-cols-[1.25rem_5rem_8rem_6rem_14rem_1fr] md:gap-x-2 md:items-baseline">
          <span className="text-sky-400">▸</span>
          <span className="text-zinc-500">{row.venue}</span>
          <span className="text-zinc-100 truncate">{row.symbol}</span>
          <span className="text-zinc-500">{row.session}</span>
          <span className="text-zinc-400 truncate" title={row.tuneId}>{family}</span>
          <span className="text-[13px] truncate">{meta}</span>
        </div>
      </button>
      {expanded ? (
        <div className="pl-5 md:pl-7 mt-1 mb-2 border-l border-zinc-800/60 ml-2 md:ml-3 pl-3 space-y-2">
          {cells.length === 0 ? (
            <div className="text-zinc-500 text-[12px]">(no per-cell detail available — older eligible row before backfill)</div>
          ) : (
            cells.map((cell) => <CellDetailView key={cell.cellId} cell={cell} />)
          )}
        </div>
      ) : null}
    </div>
  );
}

function CellDetailView({ cell }: { cell: CellDetail }) {
  const passed = cell.strictPassed;
  const headerColor = passed ? "text-emerald-300" : "text-zinc-500";
  const labelColor = "text-zinc-500";
  const numClass = passed ? "text-zinc-200" : "text-zinc-400";
  const expR = cell.expectancyR;
  const expColor = expR > 0 ? "text-emerald-400" : expR < 0 ? "text-rose-400" : "text-zinc-300";
  const p05 = cell.bootstrapP05ExpectancyR;
  const p05Color = p05 === null ? "text-zinc-500" : p05 > 0 ? "text-emerald-400" : "text-rose-400";
  const winSpark = cell.windowExpectancyR;
  // Range for the sparkline tooltip
  const winRange = winSpark && winSpark.length > 0
    ? `${Math.min(...winSpark).toFixed(2)} .. ${Math.max(...winSpark).toFixed(2)}`
    : "—";
  return (
    <div className="text-[12px] md:text-[13px]">
      <div className={`${headerColor} flex flex-wrap items-baseline gap-x-2`}>
        <span>{passed ? "✓ cell" : "· cell"}</span>
        <span className="font-mono text-zinc-300 truncate" title={cell.cellId}>{abbrCell(cell.cellId)}</span>
        {passed ? <span className="text-emerald-400">strict pass</span> : (
          <span className="text-zinc-500">fail: {cell.reason || "—"}</span>
        )}
      </div>
      <div className="pl-3 mt-0.5 grid grid-cols-2 md:grid-cols-4 gap-x-3 gap-y-0.5">
        <span><span className={labelColor}>trades </span><span className={numClass}>{cell.trades}</span></span>
        <span><span className={labelColor}>netR </span><span className={expR > 0 ? "text-emerald-400" : "text-rose-400"}>{expR >= 0 ? "+" : ""}{cell.netR.toFixed(2)}R</span></span>
        <span><span className={labelColor}>exp/trade </span><span className={expColor}>{expR >= 0 ? "+" : ""}{expR.toFixed(3)}R</span></span>
        <span><span className={labelColor}>p05 </span><span className={p05Color}>{p05 === null ? "—" : `${p05 >= 0 ? "+" : ""}${p05.toFixed(3)}R`}</span></span>
        <span><span className={labelColor}>windows </span><span className={numClass}>{cell.windows}</span></span>
        <span><span className={labelColor}>pos% </span><span className={cell.positiveWindowPct >= 70 ? "text-emerald-400" : "text-zinc-400"}>{cell.positiveWindowPct.toFixed(0)}%</span></span>
        <span><span className={labelColor}>epochs </span><span className={numClass}>{cell.distinctEpochCount}</span></span>
        <span><span className={labelColor}>maxDD </span><span className="text-zinc-400">{cell.maxDrawdownR.toFixed(2)}R</span></span>
        <span><span className={labelColor}>p25 </span><span className={numClass}>{cell.p25ExpectancyR >= 0 ? "+" : ""}{cell.p25ExpectancyR.toFixed(3)}R</span></span>
        <span><span className={labelColor}>crossR </span><span className="text-zinc-400">{cell.crossRegimeTradePct.toFixed(0)}%</span></span>
        <span><span className={labelColor}>sharpe </span><span className={numClass}>{cell.sharpe === null ? "—" : cell.sharpe.toFixed(2)}</span></span>
        <span><span className={labelColor}>deflated </span><span className={numClass}>{cell.deflatedScore === null ? "—" : cell.deflatedScore.toFixed(2)}</span></span>
      </div>
      {winSpark && winSpark.length > 0 ? (
        <div className="pl-3 mt-1 flex items-baseline gap-2">
          <span className={labelColor}>per-window R</span>
          <span
            className={`font-mono whitespace-pre ${passed ? "text-emerald-400" : "text-zinc-500"}`}
            title={`${winSpark.length} windows · range ${winRange}\nvalues: ${winSpark.map((v) => v.toFixed(2)).join(", ")}`}
          >
            {sparkline(winSpark)}
          </span>
          <span className={`${labelColor} text-[11px]`}>{winSpark.length}w · range {winRange}</span>
        </div>
      ) : null}
    </div>
  );
}

// Sunday rollover progress — shows how the new week's data is filling in:
//   1. regimes built for new week (denominator = symbols across deployments)
//   2. walk-forwards for new week (denominator = last week's deployment count
//      — anything walked last week should be carried forward incrementally)
//   3. new stage-C survivors discovered this week + how many walked
function RolloverProgress({
  rollover,
}: {
  rollover: {
    currentWeekStartMs: number;
    previousWeekStartMs: number;
    walkedThisWeek: number;
    walkedLastWeek: number;
    regimesBuiltThisWeek: number;
    regimesExpected: number;
    newSurvivorsDiscovered: number;
    newSurvivorsWalked: number;
  };
}) {
  const regimePct = rollover.regimesExpected > 0
    ? Math.round((rollover.regimesBuiltThisWeek / rollover.regimesExpected) * 100)
    : 0;
  // Use last week's distinct deployments as the target — that's the set we
  // should be carrying forward incrementally this week.
  const walkTarget = Math.max(rollover.walkedLastWeek, rollover.walkedThisWeek);
  const walkPct = walkTarget > 0
    ? Math.round((rollover.walkedThisWeek / walkTarget) * 100)
    : 0;
  const newWalkPct = rollover.newSurvivorsDiscovered > 0
    ? Math.round((rollover.newSurvivorsWalked / rollover.newSurvivorsDiscovered) * 100)
    : 0;
  return (
    <div className="mt-1 pl-2 grid grid-cols-[auto_1fr] md:grid-cols-[14rem_1fr] gap-x-3 gap-y-0.5 items-baseline">
      <span className="text-zinc-500">regimes built</span>
      <span className="flex flex-wrap items-baseline gap-x-2">
        <span>
          <span className={regimePct >= 100 ? "text-emerald-400" : "text-zinc-100"}>{rollover.regimesBuiltThisWeek}</span>
          <span className="text-zinc-500">/{rollover.regimesExpected}</span>
        </span>
        <span className={`font-mono whitespace-pre ${regimePct >= 100 ? "text-emerald-400" : "text-amber-400"}`}>
          {bar(rollover.regimesBuiltThisWeek, rollover.regimesExpected, 20)}
        </span>
        <span className="text-zinc-500">{regimePct}%</span>
        {regimePct >= 100 ? <span className="text-emerald-400">✓</span> : null}
      </span>

      <span className="text-zinc-500">incremental walks</span>
      <span className="flex flex-wrap items-baseline gap-x-2">
        <span>
          <span className={walkPct >= 100 ? "text-emerald-400" : "text-zinc-100"}>{rollover.walkedThisWeek}</span>
          <span className="text-zinc-500">/{walkTarget}</span>
        </span>
        <span className={`font-mono whitespace-pre ${walkPct >= 100 ? "text-emerald-400" : "text-sky-400"}`}>
          {bar(rollover.walkedThisWeek, walkTarget, 20)}
        </span>
        <span className="text-zinc-500">{walkPct}%</span>
        <span className="text-zinc-500 text-[12px]">· carryover from last week</span>
      </span>

      <span className="text-zinc-500">new survivors</span>
      <span className="flex flex-wrap items-baseline gap-x-2">
        <span>
          <span className="text-zinc-100">{rollover.newSurvivorsWalked}</span>
          <span className="text-zinc-500">/{rollover.newSurvivorsDiscovered}</span>
        </span>
        {rollover.newSurvivorsDiscovered > 0 ? (
          <>
            <span className="font-mono whitespace-pre text-sky-400">
              {bar(rollover.newSurvivorsWalked, rollover.newSurvivorsDiscovered, 20)}
            </span>
            <span className="text-zinc-500">{newWalkPct}%</span>
          </>
        ) : (
          <span className="text-zinc-500 text-[12px]">no new stage-C survivors this week yet</span>
        )}
      </span>

      <span className="text-zinc-500">next rollover</span>
      <span className="text-zinc-300">
        {(() => {
          const nextMs = rollover.currentWeekStartMs + 7 * 24 * 60 * 60_000;
          const diff = nextMs - Date.now();
          if (diff <= 0) return <span className="text-amber-400">now (rollover overdue)</span>;
          const d = Math.floor(diff / (24 * 60 * 60_000));
          const h = Math.floor((diff % (24 * 60 * 60_000)) / (60 * 60_000));
          return <span>in {d}d {h}h <span className="text-zinc-500">· {new Date(nextMs).toISOString().slice(0, 10)} UTC</span></span>;
        })()}
      </span>
    </div>
  );
}

function RegimeRowView({
  row,
}: {
  row: {
    venue: string;
    symbol: string;
    cellId: string | null;
    weeksInCell: number | null;
    matches: number;
  };
}) {
  return (
    <div>
      {/* mobile */}
      <div className="md:hidden">
        <div className="flex items-baseline gap-x-2">
          <span className="text-zinc-100 truncate"><span className="text-zinc-500">{row.venue}/</span>{row.symbol}</span>
          <span className="text-zinc-500 text-[12px]">
            {row.weeksInCell ? `${row.weeksInCell}w` : "—"}
            {row.matches > 0 ? <span className="text-emerald-400"> · {row.matches} strat</span> : null}
          </span>
        </div>
        <div className={`pl-2 text-[12px] truncate ${row.cellId ? "text-zinc-300" : "text-zinc-600"}`} title={row.cellId || ""}>
          {abbrCell(row.cellId)}
        </div>
      </div>
      {/* desktop */}
      <div className="hidden md:grid md:grid-cols-[5rem_8rem_1fr_6rem_auto] md:gap-x-2 md:items-baseline">
        <span className="text-zinc-500">{row.venue}</span>
        <span className="text-zinc-100 truncate">{row.symbol}</span>
        <span className={row.cellId ? "text-zinc-300 truncate" : "text-zinc-600"} title={row.cellId || ""}>{abbrCell(row.cellId)}</span>
        <span className="text-zinc-500 text-[13px]">{row.weeksInCell ? `${row.weeksInCell}w in cell` : "—"}</span>
        {row.matches > 0 ? <span className="text-emerald-400 text-[13px]">matches: {row.matches} strat</span> : <span />}
      </div>
    </div>
  );
}

type ActivityEvent = { tsMs: number; kind: "wf" | "regime" | "trade"; payload: WalkforwardRow | TransitionRow | TradeRow };

function ActivityRowView({ event }: { event: ActivityEvent }) {
  let timeMs: number;
  let marker: string;
  let markerClass: string;
  let symbol: string;
  let detail: string;
  let detailClass: string;
  let tail = ""; // duration / R-multiple shown on the right on desktop, after detail on mobile
  let tailClass = "text-zinc-500";

  if (event.kind === "wf") {
    const p = event.payload as WalkforwardRow;
    timeMs = p.evaluatedAtMs;
    symbol = `${p.venue}/${p.symbol}`;
    marker = p.eligible ? "WF ✓" : p.status === "in_progress" ? "WF ◔" : p.status === "cluster_cap_exceeded" ? "WF ◐" : "WF ✗";
    markerClass =
      p.eligible ? "text-emerald-400"
      : p.status === "cluster_cap_exceeded" ? "text-amber-400"
      : p.status === "in_progress" ? "text-sky-400"
      : "text-rose-400";
    detail = p.eligible ? `cells:${p.strictPassingCells}/${p.occupiedCells}` : p.status;
    detailClass = "text-zinc-400";
    tail = p.durationMs ? `${Math.round(p.durationMs / 60_000)}m` : "";
  } else if (event.kind === "regime") {
    const p = event.payload as TransitionRow;
    timeMs = p.transitionWeekStartMs;
    symbol = `${p.venue}/${p.symbol}`;
    marker = "REGIME";
    markerClass = "text-purple-400";
    detail = `${abbrCell(p.fromCellId)} → ${abbrCell(p.toCellId)}`;
    detailClass = "text-zinc-400";
  } else {
    const p = event.payload as TradeRow;
    timeMs = p.tsMs;
    symbol = `${p.venue || "?"}/${p.symbol || "?"}`;
    marker =
      p.eventKind === "trade" ? "TRADE"
      : p.eventKind === "entry_error" ? "ENTRY✗"
      : p.eventKind === "entry_skipped" ? "ENTRY-"
      : "STATE";
    const tone =
      p.eventKind === "trade" ? (p.rMultiple !== null ? (p.rMultiple > 0 ? "text-emerald-400" : "text-rose-400") : "text-zinc-200")
      : p.eventKind === "entry_error" ? "text-rose-400"
      : p.eventKind === "entry_skipped" ? "text-amber-400"
      : "text-zinc-400";
    markerClass = tone;
    detail =
      p.eventKind === "trade" ? (p.state || p.summary)
      : p.eventKind === "entry_error" ? "execution error"
      : p.eventKind === "entry_skipped" ? "entry skipped"
      : p.state || "state change";
    detailClass = tone;
    tail = p.rMultiple !== null ? (p.rMultiple > 0 ? `+${p.rMultiple.toFixed(2)}R` : `${p.rMultiple.toFixed(2)}R`) : "";
    tailClass = tone;
  }

  return (
    <div>
      {/* mobile: 2 lines — primary identifier on line 1, detail indented on line 2 */}
      <div className="md:hidden">
        <div className="flex items-baseline gap-x-2">
          <span className="text-zinc-500 w-[2.75rem] shrink-0">{fmtClock(timeMs)}</span>
          <span className={`${markerClass} w-[3.5rem] shrink-0`}>{marker}</span>
          <span className="text-zinc-100 truncate">{symbol}</span>
        </div>
        <div className={`pl-[6.5rem] text-[12px] truncate ${detailClass}`} title={detail}>
          {detail}{tail ? <span className={`ml-2 ${tailClass}`}>{tail}</span> : null}
        </div>
      </div>
      {/* desktop: single line grid */}
      <div className="hidden md:grid md:grid-cols-[3rem_3.5rem_14rem_1fr_3rem] md:gap-x-2 md:items-baseline">
        <span className="text-zinc-500">{fmtClock(timeMs)}</span>
        <span className={markerClass}>{marker}</span>
        <span className="text-zinc-100 truncate">{symbol}</span>
        <span className={`${detailClass} truncate text-[13px]`}>{detail}</span>
        <span className={`${tailClass} text-[13px]`}>{tail}</span>
      </div>
    </div>
  );
}
