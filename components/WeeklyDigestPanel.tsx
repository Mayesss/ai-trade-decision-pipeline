// Weekly system digest panel (dashboard, under Latest Decision). Renders
// /api/swing/weekly-digest — live compute or a stored Sunday-cron snapshot —
// as compact cards with a copy-markdown button so the digest can be pasted
// straight into the interactive judgment session. Collapsed by default and
// lazily fetched on first expand: the digest fans out ~15 aggregate queries,
// which shouldn't ride along on every dashboard load.
import { ChevronDown, ClipboardCopy } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import type {
  StoredDigestMeta,
  SwingWeeklyDigest,
} from "../lib/swing/weeklyDigest";

const PANEL_OPEN_STORAGE_KEY = "weekly_digest_panel_open";

type ApiResponse = {
  ok: boolean;
  note?: string;
  digest?: SwingWeeklyDigest;
  markdown?: string;
};

const usd = (n: number | null | undefined) =>
  n === null || n === undefined ? "n/a" : `$${n.toFixed(2)}`;
const pct = (n: number | null | undefined) =>
  n === null || n === undefined ? "n/a" : `${(n * 100).toFixed(0)}%`;
const day = (ms: number) => new Date(ms).toISOString().slice(0, 10);

const pnlClass = (n: number | null | undefined) =>
  n === null || n === undefined || n === 0
    ? "text-slate-900"
    : n > 0
      ? "text-emerald-600"
      : "text-rose-600";

function Stat(props: {
  label: string;
  value: string;
  valueClass?: string;
}) {
  return (
    <div className="min-w-0">
      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
        {props.label}
      </div>
      <div
        className={`truncate text-sm font-semibold sm:text-base ${props.valueClass ?? "text-slate-900"}`}
      >
        {props.value}
      </div>
    </div>
  );
}

function Section(props: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-xl border border-slate-100 bg-slate-50 p-3">
      <div className="text-[10px] font-semibold uppercase tracking-[0.15em] text-slate-500">
        {props.title}
      </div>
      <div className="mt-2">{props.children}</div>
    </div>
  );
}

export default function WeeklyDigestPanel(props: {
  getAdminHeaders: () => Record<string, string> | undefined;
  onUnauthorized?: () => void;
}) {
  const { getAdminHeaders, onUnauthorized } = props;
  const [open, setOpen] = useState(false);
  const [days, setDays] = useState(7);
  const [storedList, setStoredList] = useState<StoredDigestMeta[]>([]);
  const [selectedStoredId, setSelectedStoredId] = useState<number | null>(null);
  const [data, setData] = useState<ApiResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const fetchedOnceRef = useRef(false);

  useEffect(() => {
    if (typeof window === "undefined") return;
    if (window.localStorage.getItem(PANEL_OPEN_STORAGE_KEY) === "1") {
      setOpen(true);
    }
  }, []);

  const fetchDigest = useCallback(
    async (storedId: number | null, liveDays: number) => {
      setLoading(true);
      setError(null);
      try {
        const qs = storedId ? `id=${storedId}` : `days=${liveDays}`;
        const res = await fetch(`/api/swing/weekly-digest?${qs}`, {
          headers: getAdminHeaders(),
        });
        if (res.status === 401) {
          setData(null);
          setError("unauthorized — unlock the dashboard first");
          onUnauthorized?.();
          return;
        }
        const json = (await res.json()) as ApiResponse;
        if (!json.ok) {
          setError(json.note ?? "digest unavailable");
          setData(null);
          return;
        }
        setData(json);
      } catch (err) {
        setError(String(err));
      } finally {
        setLoading(false);
      }
    },
    [getAdminHeaders, onUnauthorized],
  );

  // Lazy load on first expand; later expands keep the cached digest.
  useEffect(() => {
    if (!open || fetchedOnceRef.current) return;
    fetchedOnceRef.current = true;
    void fetchDigest(selectedStoredId, days);
    void (async () => {
      try {
        const res = await fetch("/api/swing/weekly-digest?list=1", {
          headers: getAdminHeaders(),
        });
        if (!res.ok) return;
        const json = await res.json();
        if (Array.isArray(json?.digests)) setStoredList(json.digests);
      } catch {
        /* history picker is optional */
      }
    })();
  }, [open, fetchDigest, selectedStoredId, days, getAdminHeaders]);

  const toggleOpen = () => {
    const next = !open;
    setOpen(next);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(PANEL_OPEN_STORAGE_KEY, next ? "1" : "0");
    }
  };

  const handleWindowChange = (value: string) => {
    if (value.startsWith("live-")) {
      const nextDays = Number(value.slice(5)) || 7;
      setSelectedStoredId(null);
      setDays(nextDays);
      void fetchDigest(null, nextDays);
    } else {
      const id = Number(value);
      setSelectedStoredId(id);
      void fetchDigest(id, days);
    }
  };

  const copyMarkdown = async () => {
    if (!data?.markdown) return;
    try {
      await navigator.clipboard.writeText(data.markdown);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      /* clipboard unavailable (non-secure context) */
    }
  };

  const d = data?.digest ?? null;
  const t = d?.trades ?? null;
  const p = d?.prevWindowTrades ?? null;

  return (
    <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
      <button
        type="button"
        onClick={toggleOpen}
        aria-expanded={open}
        className="flex w-full items-center justify-between gap-2 text-left"
      >
        <div className="flex items-center gap-2 text-xs uppercase tracking-wide text-slate-500">
          <span>Weekly Digest</span>
          {d ? (
            <span className="lowercase text-slate-400">
              {day(d.window.fromMs)} → {day(d.window.toMs)}
            </span>
          ) : null}
        </div>
        <ChevronDown
          className={`h-4 w-4 shrink-0 text-slate-500 transition-transform ${open ? "rotate-180" : ""}`}
        />
      </button>

      {open && (
        <div className="mt-3">
          <div className="flex flex-wrap items-center gap-2">
            <select
              value={selectedStoredId ?? `live-${days}`}
              onChange={(event) => handleWindowChange(event.target.value)}
              className="max-w-full rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-600 shadow-sm"
            >
              <option value="live-7">Live · last 7 days</option>
              <option value="live-14">Live · last 14 days</option>
              <option value="live-30">Live · last 30 days</option>
              {storedList.map((s) => (
                <option key={s.id} value={s.id}>
                  Stored · {day(s.windowFromMs)} → {day(s.windowToMs)}
                </option>
              ))}
            </select>
            <button
              type="button"
              onClick={copyMarkdown}
              disabled={!data?.markdown}
              className="inline-flex items-center gap-1.5 rounded-full bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-400"
            >
              <ClipboardCopy className="h-3.5 w-3.5" />
              {copied ? "Copied!" : "Copy markdown"}
            </button>
            {loading ? (
              <span className="text-xs font-semibold text-slate-400">
                loading…
              </span>
            ) : null}
          </div>

          {error && (
            <div className="mt-3 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-xs font-semibold text-rose-700">
              Could not load digest: {error}
            </div>
          )}

          {d && t && p && (
            <div className="mt-3 space-y-3">
              <Section title="Trades (closed)">
                <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
                  <Stat label="Closed" value={`${t.closed} (prev ${p.closed})`} />
                  <Stat
                    label="Win rate"
                    value={`${pct(t.winRate)} (prev ${pct(p.winRate)})`}
                  />
                  <Stat
                    label="PnL net"
                    value={usd(t.pnlNetTotal)}
                    valueClass={pnlClass(t.pnlNetTotal)}
                  />
                  <Stat label="Fees" value={usd(t.feesPaid)} />
                  <Stat
                    label="Expectancy"
                    value={usd(t.expectancyUsd)}
                    valueClass={pnlClass(t.expectancyUsd)}
                  />
                  <Stat
                    label="Avg hold"
                    value={
                      t.avgHoldingHours === null
                        ? "n/a"
                        : `${t.avgHoldingHours.toFixed(1)}h`
                    }
                  />
                </div>
                <div className="mt-3 flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-600">
                  <span>
                    avg win {usd(t.avgWinUsd)} vs avg loss {usd(t.avgLossUsd)}
                  </span>
                  {t.largestWin && (
                    <span>
                      best {t.largestWin.symbol} {usd(t.largestWin.pnlNet)}
                    </span>
                  )}
                  {t.largestLoss && (
                    <span>
                      worst {t.largestLoss.symbol} {usd(t.largestLoss.pnlNet)}
                    </span>
                  )}
                  {t.bySide.map((s) => (
                    <span key={s.side}>
                      {s.side}: {s.closed} closed, {s.wins}W, {usd(s.pnlNetTotal)}
                    </span>
                  ))}
                  {t.byPlatform.map((s) => (
                    <span key={s.platform}>
                      {s.platform}: {s.closed} closed, {s.wins}W,{" "}
                      {usd(s.pnlNetTotal)}
                    </span>
                  ))}
                </div>
              </Section>

              <div className="grid gap-3 lg:grid-cols-2">
                <Section title="Symbols (by |PnL|)">
                  {t.bySymbol.length ? (
                    <ul className="space-y-1">
                      {t.bySymbol.map((s) => (
                        <li
                          key={`${s.platform}-${s.symbol}`}
                          className="flex flex-wrap items-baseline gap-x-2 border-b border-slate-100 py-1 text-xs last:border-0 sm:text-sm"
                        >
                          <span className="font-semibold">{s.symbol}</span>
                          <span className="text-slate-400">{s.platform}</span>
                          <span className="text-slate-500">
                            {s.closed} closed · {s.wins}W
                          </span>
                          <span
                            className={`ml-auto font-semibold ${pnlClass(s.pnlNetTotal)}`}
                          >
                            {usd(s.pnlNetTotal)}
                          </span>
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <div className="text-xs text-slate-500">
                      No closed trades.
                    </div>
                  )}
                </Section>

                <div className="space-y-3">
                  <Section title="Equity">
                    {d.equity.length ? (
                      <div className="space-y-1.5 text-xs sm:text-sm">
                        {d.equity.map((e) => (
                          <div
                            key={e.platform}
                            className="flex flex-wrap items-baseline gap-x-2"
                          >
                            <span className="font-semibold">{e.platform}</span>
                            <span>
                              {usd(e.firstEquity)} → {usd(e.lastEquity)}
                            </span>
                            <span
                              className={`font-semibold ${
                                (e.changePct ?? 0) > 0
                                  ? "text-emerald-600"
                                  : (e.changePct ?? 0) < 0
                                    ? "text-rose-600"
                                    : ""
                              }`}
                            >
                              {e.changePct === null
                                ? "n/a"
                                : `${e.changePct >= 0 ? "+" : ""}${e.changePct.toFixed(1)}%`}
                            </span>
                            <span className="text-slate-500">
                              range {usd(e.minEquity)}–{usd(e.maxEquity)} ·{" "}
                              {e.readings} readings
                            </span>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="text-xs text-slate-500">
                        No equity readings in window.
                      </div>
                    )}
                  </Section>

                  <Section title="Post-mortems">
                    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs sm:text-sm">
                      <span>
                        {d.postmortems.total} enqueued
                        {d.postmortems.unfinished > 0
                          ? ` (${d.postmortems.unfinished} unfinished)`
                          : ""}
                      </span>
                      <span
                        className={`font-semibold ${
                          (d.postmortems.processFaultShare ?? 0) >= 0.7
                            ? "text-rose-600"
                            : "text-slate-900"
                        }`}
                      >
                        process-fault share {pct(d.postmortems.processFaultShare)}
                      </span>
                    </div>
                    <div className="mt-2 flex flex-wrap gap-1.5">
                      {d.postmortems.byVerdict.map((v) => (
                        <span
                          key={v.verdict}
                          className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold sm:text-xs ${
                            v.verdict === "bad_luck"
                              ? "border-slate-200 bg-white text-slate-600"
                              : "border-amber-200 bg-amber-50 text-amber-700"
                          }`}
                        >
                          {v.verdict} · {v.count}
                        </span>
                      ))}
                    </div>
                  </Section>
                </div>
              </div>

              <Section title="Lessons">
                <div className="text-xs text-slate-600 sm:text-sm">
                  Library:{" "}
                  {d.lessons.activeByScope
                    .map(
                      (s) =>
                        `${s.scope} ${s.count} (avg conf ${s.avgConfidence.toFixed(2)})`,
                    )
                    .join(", ") || "empty"}
                  {" · "}this window: {d.lessons.addedInWindow.length} added,{" "}
                  {d.lessons.reinforcedInWindow} reinforced,{" "}
                  {d.lessons.retiredInWindow} retired
                </div>
                {d.lessons.addedInWindow.length > 0 && (
                  <ul className="mt-2 space-y-1.5">
                    {d.lessons.addedInWindow.map((l, i) => (
                      <li
                        key={i}
                        className="rounded-lg border border-slate-100 bg-white px-2.5 py-1.5 text-xs sm:text-sm"
                      >
                        <span className="mr-2 inline-block rounded-full bg-slate-200 px-1.5 py-0.5 text-[10px] font-semibold text-slate-600">
                          {l.scope} · {l.confidence.toFixed(2)}
                        </span>
                        {l.lesson}
                      </li>
                    ))}
                  </ul>
                )}
              </Section>

              <div className="grid gap-3 lg:grid-cols-2">
                <Section title="Ticks & gates">
                  <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
                    <Stat label="Ticks" value={String(d.ticks.total)} />
                    <Stat label="AI calls" value={String(d.ticks.aiCalls)} />
                    <Stat
                      label="Handler errors"
                      value={String(d.ticks.handlerErrors)}
                      valueClass={
                        d.ticks.handlerErrors > 0
                          ? "text-rose-600"
                          : "text-slate-900"
                      }
                    />
                    <Stat
                      label="AI unavailable"
                      value={String(d.ticks.aiUnavailable)}
                      valueClass={
                        d.ticks.aiUnavailable > 0
                          ? "text-rose-600"
                          : "text-slate-900"
                      }
                    />
                  </div>
                  <div className="mt-2 text-xs text-slate-600">
                    top skips:{" "}
                    {d.ticks.topSkipStages
                      .slice(0, 5)
                      .map((s) => `${s.stage} ${s.count}`)
                      .join(", ") || "none"}
                  </div>
                  <div className="mt-1 text-xs text-slate-600">
                    decisions:{" "}
                    {d.decisions.map((a) => `${a.action} ${a.count}`).join(", ") ||
                      "none"}
                  </div>
                </Section>

                <Section title="Open now">
                  {d.openNow.length ? (
                    <ul className="space-y-1 text-xs sm:text-sm">
                      {d.openNow.map((o) => (
                        <li
                          key={`${o.platform}-${o.symbol}`}
                          className="flex flex-wrap items-center gap-x-2 gap-y-0.5"
                        >
                          <span className="font-semibold">{o.symbol}</span>
                          <span className="text-slate-400">{o.platform}</span>
                          <span
                            className={`rounded-full px-1.5 py-0.5 text-[10px] font-semibold ${
                              o.status === "in_position"
                                ? "bg-emerald-50 text-emerald-700"
                                : "bg-amber-50 text-amber-700"
                            }`}
                          >
                            {o.status}
                          </span>
                          <span className="text-slate-500">
                            {o.turns} AI turns
                          </span>
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <div className="text-xs text-slate-500">Flat everywhere.</div>
                  )}
                </Section>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
