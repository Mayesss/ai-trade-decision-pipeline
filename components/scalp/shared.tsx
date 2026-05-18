import { useCallback, useEffect, useState } from "react";
import Link from "next/link";

// ─── admin secret ────────────────────────────────────────────────────────────

export const ADMIN_SECRET_STORAGE_KEY = "admin_access_secret";

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

export interface FetchAttempt {
  unauthorized?: boolean;
  error?: string;
}

// Encapsulates the localStorage hydration / 401 panel state machine used by
// both the v5 dashboard and the v4 pipeline page. Callers provide a loader
// function that returns one FetchAttempt summary; the hook handles auto-refresh,
// visibility-driven refresh, and the secret panel toggle.
export function useAdminSecretLoader(
  loader: (headers: Record<string, string>) => Promise<FetchAttempt>,
) {
  const [adminSecret, setAdminSecret] = useState("");
  const [secretInput, setSecretInput] = useState("");
  const [showSecretPanel, setShowSecretPanel] = useState(false);
  const [unauthorized, setUnauthorized] = useState(false);
  const [secretHydrated, setSecretHydrated] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loadedAt, setLoadedAt] = useState<number | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);

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
    const result = await loader(headers);
    if (result.unauthorized) {
      setUnauthorized(true);
      setShowSecretPanel(true);
      setError("Unauthorized — admin secret missing or invalid.");
      return;
    }
    setError(result.error || null);
    setUnauthorized(false);
    setShowSecretPanel(false);
    setLoadedAt(Date.now());
  }, [adminSecret, loader]);

  useEffect(() => {
    if (!secretHydrated) return;
    load();
  }, [load, secretHydrated]);

  useEffect(() => {
    if (!autoRefresh || unauthorized || !secretHydrated) return;
    const t = setInterval(load, 30_000);
    return () => clearInterval(t);
  }, [autoRefresh, load, unauthorized, secretHydrated]);

  // Background tabs throttle setInterval (~1/min); refetch on visibility return.
  useEffect(() => {
    if (!secretHydrated || unauthorized) return;
    const onVisible = () => {
      if (typeof document !== "undefined" && document.visibilityState === "visible") {
        load();
      }
    };
    if (typeof document !== "undefined") {
      document.addEventListener("visibilitychange", onVisible);
      window.addEventListener("focus", onVisible);
    }
    return () => {
      if (typeof document !== "undefined") {
        document.removeEventListener("visibilitychange", onVisible);
        window.removeEventListener("focus", onVisible);
      }
    };
  }, [load, secretHydrated, unauthorized]);

  // Tick to make "X ago" labels update without refetching.
  const [, setTick] = useState(0);
  useEffect(() => {
    const t = setInterval(() => setTick((n) => n + 1), 5_000);
    return () => clearInterval(t);
  }, []);

  const saveSecret = useCallback((next: string) => {
    const value = next.trim();
    persistAdminSecret(value);
    setAdminSecret(value);
    setUnauthorized(false);
    setShowSecretPanel(false);
  }, []);

  return {
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
  };
}

export async function fetchOne<T extends { ok: boolean }>(
  url: string,
  headers: Record<string, string>,
  apply: (data: T) => void,
): Promise<FetchAttempt> {
  try {
    const res = await fetch(url, { headers, credentials: "include" });
    if (res.status === 401) return { unauthorized: true };
    if (!res.ok) return { error: `${url} → HTTP ${res.status}` };
    const data = (await res.json()) as T;
    if (!data.ok) return { error: (data as { error?: string }).error || `${url} → request_failed` };
    apply(data);
    return {};
  } catch (err) {
    return { error: (err as Error)?.message || String(err) };
  }
}

// ─── formatters ──────────────────────────────────────────────────────────────

export function fmtAgo(ms: number | null | undefined): string {
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

export function fmtClock(ms: number | null | undefined): string {
  if (!ms) return "—";
  const d = new Date(ms);
  return `${String(d.getUTCHours()).padStart(2, "0")}:${String(d.getUTCMinutes()).padStart(2, "0")}`;
}

export function fmtWeek(ms: number | null | undefined): string {
  if (!ms) return "—";
  const d = new Date(ms);
  const year = d.getUTCFullYear();
  const start = Date.UTC(year, 0, 1);
  const week = Math.floor((d.getTime() - start) / (7 * 24 * 60 * 60_000)) + 1;
  return `W${String(week).padStart(2, "0")}`;
}

export function fmtEta(hours: number | null): string {
  if (hours === null || !Number.isFinite(hours)) return "—";
  if (hours < 1) return `${Math.max(1, Math.round(hours * 60))}m`;
  if (hours < 24) return `${hours.toFixed(1)}h`;
  const d = Math.floor(hours / 24);
  const h = Math.round(hours - d * 24);
  return h > 0 ? `${d}d ${h}h` : `${d}d`;
}

export function abbrCell(cellId: string | null): string {
  if (!cellId || cellId === "unknown") return "unknown";
  return cellId
    .replace(/trend=trending_up/g, "tr=up")
    .replace(/trend=trending_down/g, "tr=dn")
    .replace(/trend=choppy/g, "tr=chop")
    .replace(/risk=risk_on/g, "risk=on")
    .replace(/risk=risk_off/g, "risk=off")
    .replace(/risk=neutral/g, "risk=neu");
}

// Unicode block sparkline — 8 levels, oldest→newest order.
export function sparkline(values: number[]): string {
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

export function bar(value: number, max: number, width = 28): string {
  if (!max || max <= 0) return " ".repeat(width);
  const filled = Math.max(0, Math.min(width, Math.round((value / max) * width)));
  return "█".repeat(filled) + "░".repeat(width - filled);
}

// ─── shared UI primitives ────────────────────────────────────────────────────

export function Skeleton({ label }: { label: string }) {
  return (
    <div className="text-zinc-600 animate-pulse">
      {"  "}
      {label} <span className="inline-block">· · ·</span>
    </div>
  );
}

export function SectionHeader({ title }: { title: string }) {
  return (
    <div className="mt-5 flex items-center gap-2 text-zinc-500">
      <span className="text-zinc-500">══</span>
      <span className="whitespace-nowrap text-zinc-400 uppercase tracking-wider text-[12px]">{title}</span>
      <span className="flex-1 border-t border-zinc-800/80" />
    </div>
  );
}

export function PageShell({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-200 font-mono text-[13px] leading-[1.45]">
      <div className="mx-auto max-w-[1200px] px-4 py-4">{children}</div>
    </div>
  );
}

export interface NavLink {
  href: string;
  label: string;
  active?: boolean;
}

export function DashboardHeader({
  title,
  classifierVersion,
  loadedAt,
  autoRefresh,
  setAutoRefresh,
  load,
  adminSecret,
  onToggleSecret,
  navLinks,
}: {
  title: string;
  classifierVersion: string | null;
  loadedAt: number | null;
  autoRefresh: boolean;
  setAutoRefresh: (v: boolean) => void;
  load: () => void;
  adminSecret: string;
  onToggleSecret: () => void;
  navLinks: NavLink[];
}) {
  return (
    <div className="flex flex-wrap items-baseline justify-between gap-2 border-b border-zinc-800 pb-2">
      <div className="flex items-baseline gap-3">
        <span className="text-zinc-100">{title}</span>
        <span className="text-zinc-500">·</span>
        <span className="text-zinc-400">{new Date().toISOString().slice(0, 16).replace("T", " ")} UTC</span>
        <span className="text-zinc-500">·</span>
        <span className="text-zinc-500">classifier {classifierVersion || "—"}</span>
      </div>
      <div className="flex items-center gap-3 text-zinc-500">
        <span>
          refresh 30s · last{" "}
          {loadedAt
            ? (() => {
                const ageSec = Math.floor((Date.now() - loadedAt) / 1000);
                const cls =
                  ageSec > 5 * 60 ? "text-rose-400" : ageSec > 90 ? "text-amber-400" : "text-zinc-400";
                return <span className={cls}>{fmtAgo(loadedAt)} ago</span>;
              })()
            : "—"}
        </span>
        <label className="flex items-center gap-1 cursor-pointer">
          <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />
          auto
        </label>
        <button onClick={load} className="text-zinc-300 hover:text-white">
          [refresh]
        </button>
        <button
          onClick={onToggleSecret}
          className={adminSecret ? "text-zinc-500 hover:text-zinc-300" : "text-amber-300 hover:text-amber-200"}
        >
          [{adminSecret ? "secret" : "set secret"}]
        </button>
        {navLinks.map((link) => (
          <Link
            key={link.href}
            href={link.href}
            className={link.active ? "text-zinc-300" : "text-zinc-500 hover:text-zinc-300"}
          >
            [{link.label}]
          </Link>
        ))}
      </div>
    </div>
  );
}

export function AdminSecretPanel({
  show,
  unauthorized,
  adminSecret,
  secretInput,
  setSecretInput,
  saveSecret,
  load,
  dismiss,
}: {
  show: boolean;
  unauthorized: boolean;
  adminSecret: string;
  secretInput: string;
  setSecretInput: (v: string) => void;
  saveSecret: (v: string) => void;
  load: () => void;
  dismiss: () => void;
}) {
  if (!show) return null;
  return (
    <div className={`mt-3 border ${unauthorized ? "border-rose-500/40 bg-rose-500/10" : "border-amber-500/40 bg-amber-500/10"} p-3`}>
      <div className="text-zinc-300">
        {unauthorized ? "admin access required" : "set admin access secret"} · key{" "}
        <span className="text-zinc-500">{ADMIN_SECRET_STORAGE_KEY}</span>
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
        <button
          type="submit"
          className="border border-emerald-500/50 bg-emerald-500/20 px-3 py-1 text-emerald-200 hover:bg-emerald-500/30"
        >
          save &amp; retry
        </button>
        {adminSecret ? (
          <button
            type="button"
            onClick={() => {
              saveSecret("");
              setSecretInput("");
            }}
            className="text-zinc-500 hover:text-zinc-300"
          >
            clear
          </button>
        ) : null}
        <button type="button" onClick={dismiss} className="text-zinc-500 hover:text-zinc-300">
          dismiss
        </button>
      </form>
    </div>
  );
}

// ─── v5 decision typing (shared so both pages can colour-code activity) ──────

export type V5GateDecision =
  | "allow"
  | "block_negative"
  | "block_unseen"
  | "block_stale"
  | "block_evaluator_pending"
  | "block_insufficient_trades";

export const V5_DECISION_LABEL: Record<V5GateDecision, string> = {
  allow: "allow",
  block_negative: "neg",
  block_unseen: "unseen",
  block_stale: "stale",
  block_evaluator_pending: "pending",
  block_insufficient_trades: "thin",
};

export const V5_DECISION_COLOR: Record<V5GateDecision, string> = {
  allow: "text-emerald-400",
  block_negative: "text-rose-400",
  block_unseen: "text-amber-300",
  block_stale: "text-amber-300",
  block_evaluator_pending: "text-sky-400",
  block_insufficient_trades: "text-amber-300",
};

export const V5_DECISION_GLYPH: Record<V5GateDecision, string> = {
  allow: "✓",
  block_negative: "✗",
  block_unseen: "✗",
  block_stale: "⊘",
  block_evaluator_pending: "○",
  block_insufficient_trades: "◐",
};
