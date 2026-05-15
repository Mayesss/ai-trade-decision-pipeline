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
  summary: string;
  rMultiple: number | null;
  phase: string;
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

function fmtPad(s: string | number, width: number, align: "L" | "R" = "L"): string {
  const str = String(s);
  if (str.length >= width) return str;
  const pad = " ".repeat(width - str.length);
  return align === "L" ? str + pad : pad + str;
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
    async function fetchOne<T extends { ok: boolean }>(url: string): Promise<{ data?: T; unauthorized?: boolean; error?: string }> {
      try {
        const res = await fetch(url, { headers, credentials: "include" });
        if (res.status === 401) return { unauthorized: true };
        if (!res.ok) return { error: `${url} → HTTP ${res.status}` };
        const data = (await res.json()) as T;
        if (!data.ok) return { error: (data as any).error || `${url} → request_failed` };
        return { data };
      } catch (err) {
        return { error: (err as Error)?.message || String(err) };
      }
    }
    const [h, d, r] = await Promise.all([
      fetchOne<HealthResp>("/api/scalp/v4/health"),
      fetchOne<DeploymentsResp>("/api/scalp/v4/deployments"),
      fetchOne<RecentResp>("/api/scalp/v4/recent"),
    ]);
    if (h.unauthorized || d.unauthorized || r.unauthorized) {
      setUnauthorized(true);
      setShowSecretPanel(true);
      setError("Unauthorized — admin secret missing or invalid.");
      return;
    }
    if (h.data) setHealth(h.data);
    if (d.data) setDeploymentsResp(d.data);
    if (r.data) setRecent(r.data);
    setError(h.error || d.error || r.error || null);
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
        <section className="mt-4">
          <div className="text-zinc-500">═══════════ HEALTH ════════════════════════════════════════════════════════════</div>
          <div className="text-zinc-300">
            {"  hard gate   "}
            <span className={health?.v4HardGateEnabled ? "text-emerald-400" : "text-amber-400"}>{health?.v4HardGateEnabled ? "ON " : "OFF"}</span>
            {"       v4  "}<span className={health?.v4Enabled ? "text-emerald-400" : "text-amber-400"}>{health?.v4Enabled ? "enabled" : "disabled"}</span>
            {"       freeze  "}<span className="text-emerald-400">inactive</span>
          </div>
          <div className="text-zinc-300">
            {"  regimes     "}<span className="text-zinc-100">{health?.regimeBuild.symbolsCovered ?? "—"}</span>
            <span className="text-zinc-500"> symbols valid</span>
            {"     last build  "}<span className="text-zinc-100">{health?.regimeBuild.latestWeekStartMs ? fmtAgo(health.regimeBuild.latestWeekStartMs + 7 * 24 * 60 * 60_000) + " ago" : "—"}</span>
          </div>
        </section>

        {/* PIPELINE */}
        <section className="mt-4">
          <div className="text-zinc-500">═══════════ PIPELINE  ·  {fmtWeek(Date.now())} ════════════════════════════════════════════════</div>
          <div className="text-zinc-300">
            {"  stage-C survivors  "}<span className="text-zinc-100">{fmtPad(totalPlanned, 6, "R")}</span>
            <span className="text-zinc-500">  progress </span>
            <span className="text-emerald-400">{bar(totalDone, totalPlanned)}</span>
            <span className="text-zinc-500"> {pctDone}%</span>
          </div>
          <div className="text-zinc-300">
            {"  walk-forward       "}<span className="text-zinc-100">{fmtPad(totalDone, 6, "R")}</span>
            <span className="text-zinc-500"> done    </span>
            <span className="text-sky-400">{fmtPad(counts.in_progress || 0, 4, "R")}</span>
            <span className="text-zinc-500"> running   </span>
            <span className="text-zinc-100">{fmtPad(health?.pendingWalkforward ?? "—", 4, "R")}</span>
            <span className="text-zinc-500"> pending</span>
          </div>
          <CountLine label="● eligible        " value={counts.eligible || 0} max={maxCount} colorClass="text-emerald-400" />
          <CountLine label="○ no_passing      " value={counts.no_passing_cells || 0} max={maxCount} colorClass="text-rose-400" />
          <CountLine label="◐ cluster_capped  " value={counts.cluster_cap_exceeded || 0} max={maxCount} colorClass="text-amber-400" />
          <CountLine label="◔ in_progress     " value={counts.in_progress || 0} max={maxCount} colorClass="text-sky-400" />
          <CountLine label="· overbroad       " value={counts.regime_overbroad_pending_review || 0} max={maxCount} colorClass="text-zinc-400" />
        </section>

        {/* LIVE summary */}
        <section className="mt-4">
          <div className="text-zinc-500">═══════════ LIVE ══════════════════════════════════════════════════════════════</div>
          <div className="text-zinc-300">
            {"  trading now           "}<span className="text-emerald-400">{fmtPad(trading.length, 3, "R")}</span><span className="text-zinc-500"> deployments  ·  </span>
            <span className="text-zinc-100">{trading.reduce((acc, r) => acc + r.openPositionCount, 0)}</span><span className="text-zinc-500"> open position(s)</span>
          </div>
          <div className="text-zinc-300">
            {"  dormant by regime    "}<span className="text-amber-400">{fmtPad(dormant.length, 3, "R")}</span><span className="text-zinc-500"> deployments  ·  awaiting regime match</span>
          </div>
          <div className="text-zinc-300">
            {"  awaiting promote     "}<span className="text-sky-400">{fmtPad(eligibleNotPromoted.length, 3, "R")}</span><span className="text-zinc-500"> candidates    ·  next promote cycle imminent</span>
          </div>
        </section>

        {/* ACTIVE DEPLOYMENTS */}
        <section className="mt-4">
          <div className="text-zinc-500">═══════════ ACTIVE DEPLOYMENTS  ·  live=● dormant=○ ═════════════════════════════</div>
          {trading.length === 0 && dormant.length === 0 ? (
            <div className="text-zinc-500">  (no enabled deployments)</div>
          ) : null}
          {trading.map((row) => (
            <div key={row.deploymentId} className="text-zinc-300">
              <span className="text-emerald-400">{"  ● "}</span>
              <span className="text-zinc-500">{fmtPad(row.venue, 7)}</span>
              <span className="text-zinc-100">{fmtPad(row.symbol, 11)}</span>
              <span className="text-zinc-500">{fmtPad(row.session, 9)}</span>
              <span className="text-zinc-400">{fmtPad(row.tuneId.split("_").slice(0, 4).join("_"), 24)}</span>
              <span className="text-zinc-500">  last </span><span className="text-zinc-200">{fmtPad(row.lastEntryAtMs ? fmtAgo(row.lastEntryAtMs) : "never", 6)}</span>
              <span className="text-zinc-500">open </span><span className="text-zinc-100">{row.openPositionCount}</span>
            </div>
          ))}
          {dormant.map((row) => (
            <div key={row.deploymentId} className="text-zinc-300">
              <span className="text-amber-400">{"  ○ "}</span>
              <span className="text-zinc-500">{fmtPad(row.venue, 7)}</span>
              <span className="text-zinc-100">{fmtPad(row.symbol, 11)}</span>
              <span className="text-zinc-500">{fmtPad(row.session, 9)}</span>
              <span className="text-zinc-400">{fmtPad(row.tuneId.split("_").slice(0, 4).join("_"), 24)}</span>
              <span className="text-amber-400/80">  blocked: {row.v4Status === "dormant_no_regime" ? "no regime data" : "regime mismatch"}</span>
            </div>
          ))}
          {dormant.length > 0 ? (
            <div className="mt-1 pl-6 text-zinc-500">
              {dormant.slice(0, 5).map((row) => (
                <div key={`mm:${row.deploymentId}`}>
                  <span className="text-zinc-500">    {fmtPad(row.symbol, 11)} </span>
                  <span className="text-zinc-500">allowed </span><span className="text-zinc-400">[{row.envelope.allowedCells.map(abbrCell).join(", ") || "none"}]</span>
                  <span className="text-zinc-500">  current </span><span className="text-amber-300">{abbrCell(row.currentRegime.cellId)}</span>
                </div>
              ))}
              {dormant.length > 5 ? <div>    … {dormant.length - 5} more</div> : null}
            </div>
          ) : null}
        </section>

        {/* ELIGIBLE NOT PROMOTED */}
        <section className="mt-4">
          <div className="text-zinc-500">═══════════ ELIGIBLE  ·  not yet promoted ({eligibleNotPromoted.length}) ════════════════════════</div>
          {eligibleNotPromoted.length === 0 ? <div className="text-zinc-500">  (none)</div> : null}
          {eligibleNotPromoted.slice(0, 30).map((row) => (
            <div key={row.deploymentId} className="text-zinc-300">
              <span className="text-sky-400">{"  ▸ "}</span>
              <span className="text-zinc-500">{fmtPad(row.venue, 7)}</span>
              <span className="text-zinc-100">{fmtPad(row.symbol, 11)}</span>
              <span className="text-zinc-500">{fmtPad(row.session, 9)}</span>
              <span className="text-zinc-400">{fmtPad(row.tuneId.split("_").slice(0, 4).join("_"), 24)}</span>
              <span className="text-zinc-500">cells </span>
              <span className="text-emerald-400">{row.envelope.strictPassingCells}</span>
              <span className="text-zinc-500">/{row.envelope.occupiedCells}</span>
              <span className="text-zinc-500">  allowed </span>
              <span className="text-zinc-300">[{row.envelope.allowedCells.slice(0, 2).map(abbrCell).join(", ")}{row.envelope.allowedCells.length > 2 ? "…" : ""}]</span>
            </div>
          ))}
          {eligibleNotPromoted.length > 30 ? <div className="text-zinc-500">  … {eligibleNotPromoted.length - 30} more</div> : null}
        </section>

        {/* REGIMES */}
        <section className="mt-4">
          <div className="text-zinc-500">═══════════ REGIMES  ·  current week ═══════════════════════════════════════════</div>
          {regimeRows.length === 0 ? <div className="text-zinc-500">  (no enabled symbols)</div> : null}
          {regimeRows.map((row) => (
            <div key={`${row.venue}:${row.symbol}`} className="text-zinc-300">
              <span className="text-zinc-500">  {fmtPad(row.venue, 7)}</span>
              <span className="text-zinc-100">{fmtPad(row.symbol, 11)}</span>
              <span className={row.cellId ? "text-zinc-300" : "text-zinc-600"}>{fmtPad(abbrCell(row.cellId), 34)}</span>
              <span className="text-zinc-500">{fmtPad(row.weeksInCell ? `${row.weeksInCell}w in cell` : "—", 14)}</span>
              {row.matches > 0 ? <span className="text-emerald-400">matches: {row.matches} strat</span> : null}
            </div>
          ))}
        </section>

        {/* ACTIVITY */}
        <section className="mt-4 mb-6">
          <div className="text-zinc-500">═══════════ ACTIVITY  ·  last {activity.length} ═══════════════════════════════════════════════</div>
          {activity.length === 0 ? <div className="text-zinc-500">  (no recent events)</div> : null}
          {activity.map((event, idx) => {
            if (event.kind === "wf") {
              const p = event.payload as WalkforwardRow;
              const symbol = `${p.venue}/${p.symbol}`;
              const marker = p.eligible ? "WF ✓" : p.status === "in_progress" ? "WF ◔" : p.status === "cluster_cap_exceeded" ? "WF ◐" : "WF ✗";
              const colorClass = p.eligible ? "text-emerald-400" : p.status === "cluster_cap_exceeded" ? "text-amber-400" : p.status === "in_progress" ? "text-sky-400" : "text-rose-400";
              return (
                <div key={`wf:${idx}`} className="text-zinc-300">
                  <span className="text-zinc-500">  {fmtClock(p.evaluatedAtMs)}  </span>
                  <span className={colorClass}>{fmtPad(marker, 7)}</span>
                  <span className="text-zinc-100">{fmtPad(symbol, 28)}</span>
                  <span className="text-zinc-400">{fmtPad(p.eligible ? `cells:${p.strictPassingCells}/${p.occupiedCells}` : p.status, 22)}</span>
                  <span className="text-zinc-500">{p.durationMs ? `${Math.round(p.durationMs / 60_000)}m` : ""}</span>
                </div>
              );
            }
            if (event.kind === "regime") {
              const p = event.payload as TransitionRow;
              const symbol = `${p.venue}/${p.symbol}`;
              return (
                <div key={`r:${idx}`} className="text-zinc-300">
                  <span className="text-zinc-500">  {fmtClock(p.transitionWeekStartMs)}  </span>
                  <span className="text-purple-400">{fmtPad("REGIME", 7)}</span>
                  <span className="text-zinc-100">{fmtPad(symbol, 28)}</span>
                  <span className="text-zinc-400">{abbrCell(p.fromCellId)} → {abbrCell(p.toCellId)}</span>
                </div>
              );
            }
            const p = event.payload as TradeRow;
            const symbol = `${p.venue || "?"}/${p.symbol || "?"}`;
            const r = p.rMultiple !== null ? (p.rMultiple > 0 ? `+${p.rMultiple.toFixed(2)}R` : `${p.rMultiple.toFixed(2)}R`) : "";
            const colorClass = p.rMultiple !== null ? (p.rMultiple > 0 ? "text-emerald-400" : "text-rose-400") : "text-zinc-400";
            return (
              <div key={`t:${idx}`} className="text-zinc-300">
                <span className="text-zinc-500">  {fmtClock(p.tsMs)}  </span>
                <span className="text-zinc-200">{fmtPad("TRADE", 7)}</span>
                <span className="text-zinc-100">{fmtPad(symbol, 28)}</span>
                <span className={colorClass}>{fmtPad(p.phase || p.summary, 22)}</span>
                <span className={colorClass}>{r}</span>
              </div>
            );
          })}
        </section>

        <div className="border-t border-zinc-800 pt-2 text-zinc-600">
          sources  /api/scalp/v4/health · /deployments · /recent
        </div>
      </div>
    </div>
  );
}

function CountLine({
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
    <div className="text-zinc-300">
      <span className="text-zinc-500">{"                      "}</span>
      <span className={colorClass}>{label}</span>
      <span className="text-zinc-100">{fmtPad(value, 4, "R")}</span>
      <span className="text-zinc-500">   {bar(value, max, 30)}</span>
    </div>
  );
}
