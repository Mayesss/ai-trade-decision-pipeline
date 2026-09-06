import React, {
  useEffect,
  useEffectEvent,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import Head from "next/head";
import dynamic from "next/dynamic";
import { ChartSkeleton, TimelineSkeleton } from "../components/ChartSkeleton";
import { NANO_TIMEFRAME } from "../lib/constants";
import WeeklyDigestPanel from "../components/WeeklyDigestPanel";
import {
  Circle,
  ShieldCheck,
  Moon,
  Sun,
  ArrowUpRight,
  ArrowDownRight,
  AlertTriangle,
  X,
} from "lucide-react";

type AspectEvaluation = {
  rating?: number;
  comment?: string;
  improvements?: string[];
  checks?: string[];
  findings?: string[];
};

type Evaluation = {
  overall_rating?: number;
  overview?: string;
  what_went_well?: string[];
  issues?: string[];
  improvements?: string[];
  confidence?: string;
  aspects?: Record<string, AspectEvaluation>;
};

type EvaluationEntry = {
  symbol: string;
  category?: string | null;
  evaluation: Evaluation;
  evaluationTs?: number | null;
  lastBiasTimeframes?: Record<string, string | undefined> | null;
  lastPlatform?: string | null;
  lastNewsSource?: string | null;
  pnl7d?: number | null;
  pnl7dWithOpen?: number | null;
  pnl7dNet?: number | null;
  pnl7dGross?: number | null;
  pnl7dTrades?: number | null;
  pnlSpark?: number[] | null;
  pnlDaily?: Array<{ day: string; net: number | null; trades: number }> | null;
  pendingEntry?: boolean;
  openPnl?: number | null;
  // Open PnL in venue cash + the margin behind it. The money calendar adds
  // today's open exposure to the day's realized net, and margin is what lets
  // the live quote rescale that cash between summary builds.
  openPnlCash?: number | null;
  openMargin?: number | null;
  openDirection?: "long" | "short" | null;
  openLeverage?: number | null;
  openEntryPrice?: number | null;
  lastPositionPnl?: number | null;
  lastPositionDirection?: "long" | "short" | null;
  lastPositionLeverage?: number | null;
  lastWasAiCall?: boolean;
  lastAiDecisionTs?: number | null;
  lastAiDecisionAction?: string | null;
  lastAiDecisionCooldownMinutes?: number | null;
  marketClosed?: boolean;
  lastScanAt?: number | null;
  lastScanStage?: string | null;
  lastScanReason?: string | null;
  lastDecisionTs?: number | null;
  lastDecision?: {
    action?: string;
    summary?: string;
    reason?: string;
    signal_strength?: string;
    [key: string]: unknown;
  } | null;
  lastPrompt?: { system?: string; user?: string } | null;
  lastMetrics?: Record<string, unknown> | null;
  winRate?: number | null;
  avgWinPct?: number | null;
  avgLossPct?: number | null;
};

type DashboardSymbolRow = {
  symbol: string;
  platform?: string | null;
  newsSource?: string | null;
  category?: string | null;
  schedule?: string | null;
  decisionPolicy?: string | null;
};

type DashboardSymbolsResponse = {
  symbols: string[];
  data: DashboardSymbolRow[];
};

type DashboardSummaryRow = {
  symbol: string;
  category?: string | null;
  lastPlatform?: string | null;
  lastNewsSource?: string | null;
  pnl7d?: number | null;
  pnl7dWithOpen?: number | null;
  pnl7dNet?: number | null;
  pnl7dGross?: number | null;
  pnl7dTrades?: number | null;
  pnlSpark?: number[] | null;
  // Per-Berlin-day closed nets in venue cash (mirrors the summary API) —
  // folded across symbols into the header week-calendar strip.
  pnlDaily?: Array<{ day: string; net: number | null; trades: number }> | null;
  // A pullback entry limit is resting on the venue — ranks the pill between
  // open positions and fresh AI decisions.
  pendingEntry?: boolean;
  openPnl?: number | null;
  // Open PnL in venue cash + the margin behind it. The money calendar adds
  // today's open exposure to the day's realized net, and margin is what lets
  // the live quote rescale that cash between summary builds.
  openPnlCash?: number | null;
  openMargin?: number | null;
  openDirection?: "long" | "short" | null;
  openLeverage?: number | null;
  openEntryPrice?: number | null;
  lastPositionPnl?: number | null;
  lastPositionDirection?: "long" | "short" | null;
  lastPositionLeverage?: number | null;
  lastWasAiCall?: boolean;
  // Freshest real AI call in the history window — drives the recency-sorted
  // pill order (pre-AI skips don't count).
  lastAiDecisionTs?: number | null;
  // Its action (BUY/SELL/CLOSE/HOLD/…) — colors the pill's decision dot.
  lastAiDecisionAction?: string | null;
  // Flat-HOLD cooldown that call armed, if any — clock hands inside the dot.
  lastAiDecisionCooldownMinutes?: number | null;
  marketClosed?: boolean;
  lastScanAt?: number | null;
  lastScanStage?: string | null;
  lastScanReason?: string | null;
  winRate?: number | null;
  avgWinPct?: number | null;
  avgLossPct?: number | null;
};

type DashboardSummaryResponse = {
  symbols: string[];
  data: DashboardSummaryRow[];
  range?: DashboardRangeKey;
};

type DashboardDecisionResponse = {
  symbol: string;
  category?: string | null;
  platform?: string | null;
  lastDecisionTs?: number | null;
  lastDecision?: EvaluationEntry["lastDecision"];
  lastPrompt?: { system?: string; user?: string } | null;
  lastMetrics?: Record<string, unknown> | null;
  lastBiasTimeframes?: Record<string, string | undefined> | null;
  lastNewsSource?: string | null;
};

type DashboardEvaluationResponse = {
  symbol: string;
  evaluation: Evaluation;
  evaluationTs?: number | null;
};

// One tick on the swing decision timeline (mirrors /api/dashboard/timeline).
// `hasDetails` ticks have a persisted decision row fetchable by exact ts;
// scan-only ticks carry their gate stage/reason inline (quarter-tick skips are
// never persisted as decision rows).
type TimelineTickUi = {
  ts: number;
  source: "decision" | "scan" | "postmortem";
  hourly: boolean;
  kind: "action" | "ai_call" | "gate_skip" | "scan_skip" | "scan" | "postmortem";
  action?: string;
  summary?: string;
  stage?: string;
  reason?: string;
  // Post-mortem ticks: row id (full report via /api/swing/dashboard/postmortem),
  // worker status, analyst family, and — once succeeded — verdict + lesson.
  postmortemId?: number;
  postmortemStatus?: string;
  analysisKind?: "postmortem" | "investigation" | "win_evaluation";
  verdict?: string;
  lesson?: string;
  // AI-requested flat cooldown armed by this decision (flat HOLD only).
  cooldownMinutes?: number;
  cooldownWakeAbove?: number;
  cooldownWakeBelow?: number;
  // Responses-API conversation chain (context AI calls): links chained
  // decisions on the timeline with a full-contrast connector segment.
  responseId?: string;
  previousResponseId?: string;
  hasDetails: boolean;
};

type DashboardTimelineResponse = {
  symbol: string;
  platform?: string | null;
  hours?: number;
  ticks?: TimelineTickUi[];
};

type DashboardRangeKey = "1D" | "7D" | "30D" | "6M";
type ThemePreference = "system" | "light" | "dark";
type ResolvedTheme = "light" | "dark";
const THEME_PREFERENCE_STORAGE_KEY = "dashboard_theme_preference";
// Fallback EURUSD rate for the header rollup when the live quote isn't
// available — the strip is explicitly approximate, so a ballpark is fine.
const EUR_USD_FALLBACK_RATE = 1.1;
// Cash PnL is venue-denominated: Bitget settles in USDT (shown as $), the
// Capital.com ledger is in the account currency, EUR. Symbol picked per
// platform — never sum across the two.
const platformCurrencySymbol = (platform?: string | null): "$" | "€" =>
  String(platform || "").toLowerCase() === "capital" ? "€" : "$";
// The money calendar renders as many trailing days as the panel is wide, so it
// reads its daily buckets from the 30D blob and never from a hardcoded count.
const CALENDAR_SOURCE_RANGE = "30D" as const;
// One cell holds "31 · SUN · +€12" at its natural size — cells keep this width
// and the ROW shows however many of them fit. Widening the cells instead would
// just make seven days take up the whole screen.
const CALENDAR_CELL_PX = 54;
const CALENDAR_CELL_GAP_PX = 4;
// The 30D blob is the deepest daily source, so it is also the ceiling.
const CALENDAR_MAX_DAYS = 30;
const CALENDAR_MIN_DAYS = 3;

const formatCash = (value: number, currencySymbol: "$" | "€" = "$") => {
  const abs = Math.abs(value);
  const sign = value < 0 ? "-" : "";
  const v = Math.abs(value);
  if (abs >= 1_000_000)
    return `${sign}${currencySymbol}${(v / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000)
    return `${sign}${currencySymbol}${(v / 1_000).toFixed(1)}K`;
  return `${sign}${currencySymbol}${v.toFixed(0)}`;
};
// `err?.message || fallback` for unknown catch variables — returns the message
// only when it is a non-empty string, so the caller's fallback chain is intact.
const errMsg = (err: unknown): string | undefined => {
  const message = (err as { message?: unknown } | null | undefined)?.message;
  return typeof message === "string" && message ? message : undefined;
};

// External-store bindings (useSyncExternalStore): the theme preference lives
// in localStorage, the resolved system theme and the phone layout in media
// queries — all client-only, so the server snapshots pin SSR markup to the
// pre-hydration defaults (dark theme, desktop layout) and React swaps in the
// client value right after hydration, exactly like the old mount effects did.
const THEME_PREFERENCE_CHANGE_EVENT = "dashboard-theme-preference-change";
const subscribeThemePreference = (onChange: () => void) => {
  window.addEventListener("storage", onChange);
  window.addEventListener(THEME_PREFERENCE_CHANGE_EVENT, onChange);
  return () => {
    window.removeEventListener("storage", onChange);
    window.removeEventListener(THEME_PREFERENCE_CHANGE_EVENT, onChange);
  };
};
const readStoredThemePreference = (): ThemePreference => {
  const stored = window.localStorage.getItem(THEME_PREFERENCE_STORAGE_KEY);
  return stored === "light" || stored === "dark" || stored === "system"
    ? stored
    : "system";
};
const subscribeSystemTheme = (onChange: () => void) => {
  const media = window.matchMedia("(prefers-color-scheme: dark)");
  if (typeof media.addEventListener === "function") {
    media.addEventListener("change", onChange);
    return () => media.removeEventListener("change", onChange);
  }
  media.addListener(onChange);
  return () => media.removeListener(onChange);
};
const readSystemTheme = (): ResolvedTheme =>
  window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
const subscribePhoneLayout = (onChange: () => void) => {
  const media = window.matchMedia("(max-width: 640px)");
  media.addEventListener("change", onChange);
  return () => media.removeEventListener("change", onChange);
};
const readPhoneLayout = () => window.matchMedia("(max-width: 640px)").matches;

const actionPillToneClass = (action?: string | null, pnlValue?: number | null) => {
  const normalized = String(action || '').trim().toUpperCase();
  if (normalized === "BUY") return "border-emerald-200 bg-emerald-100 text-emerald-800";
  if (normalized === "SELL") return "border-rose-200 bg-rose-100 text-rose-800";
  if (normalized === "CLOSE") {
    if (typeof pnlValue === "number") {
      return pnlValue >= 0
        ? "border-emerald-200 bg-emerald-100 text-emerald-800"
        : "border-rose-200 bg-rose-100 text-rose-800";
    }
    return "neutral-highlight";
  }
  return "neutral-highlight";
};

const BERLIN_TZ = "Europe/Berlin";
// Week-calendar strip formatters — Berlin calendar days throughout; en-CA
// renders YYYY-MM-DD, matching the summary API's pnlDaily keys.
const BERLIN_DAY_KEY_FORMAT = new Intl.DateTimeFormat("en-CA", {
  timeZone: BERLIN_TZ,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});
const BERLIN_DAY_NUM_FORMAT = new Intl.DateTimeFormat("en-GB", {
  timeZone: BERLIN_TZ,
  day: "numeric",
});
const BERLIN_WEEKDAY_FORMAT = new Intl.DateTimeFormat("en-GB", {
  timeZone: BERLIN_TZ,
  weekday: "short",
});
const BERLIN_MONTH_FORMAT = new Intl.DateTimeFormat("en-GB", {
  timeZone: BERLIN_TZ,
  month: "short",
});
const ADMIN_SECRET_STORAGE_KEY = "admin_access_secret";
const ADMIN_AUTH_TIMEOUT_MS = 4000;
type ChartRangeKey = import("../components/ChartPanel").ChartRangeKey;

const ChartPanel = dynamic(() => import("../components/ChartPanel"), {
  ssr: false,
  loading: () => (
    <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
      <div className="flex items-center justify-between">
        <div className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 p-1 text-[11px] font-semibold text-slate-500">
          <span className="px-2.5 py-1">4H</span>
          <span className="rounded-full bg-slate-200 px-2.5 py-1 text-slate-700">
            1D
          </span>
          <span className="px-2.5 py-1">7D</span>
          <span className="px-2.5 py-1">30D</span>
          <span className="px-2.5 py-1">6M</span>
        </div>
        <div className="text-xs text-slate-400">15m bars · 1D window</div>
      </div>
      <div
        className="relative mt-3 h-[260px] w-full"
        style={{ minHeight: 260 }}
      >
        <ChartSkeleton />
      </div>
      <TimelineSkeleton />
    </div>
  ),
});

export default function Home() {
  const [adminReady, setAdminReady] = useState(false);
  const [adminGranted, setAdminGranted] = useState(false);
  const [adminSecret, setAdminSecret] = useState<string | null>(null);
  const [adminInput, setAdminInput] = useState("");
  const [adminError, setAdminError] = useState<string | null>(null);
  const [adminSubmitting, setAdminSubmitting] = useState(false);
  const [symbols, setSymbols] = useState<string[]>([]);
  const [active, setActive] = useState(0);
  const [tabData, setTabData] = useState<Record<string, EvaluationEntry>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showPrompt, setShowPrompt] = useState(false);
  const [showRawResponse, setShowRawResponse] = useState(false);
  const [dashboardRange, setDashboardRange] = useState<DashboardRangeKey>("1D");
  // Trailing-7-day per-day closed nets for the header week-calendar strip,
  // folded across symbols. Kept in both venue currencies (Bitget USDT ≈ $,
  // Capital €) and converted to one € figure at render, like the old rollup.
  const [swingWeekDaily, setSwingWeekDaily] = useState<Record<
    string,
    { netUsd: number | null; netEur: number | null; trades: number }
  > | null>(null);
  // "Now" for the calendar strip, stamped when its data lands — render code
  // must stay pure, and day boundaries only need to be as fresh as the data.
  const [swingWeekLoadedAtMs, setSwingWeekLoadedAtMs] = useState<number | null>(
    null,
  );
  // Chart-only range: superset of DashboardRangeKey ("4H" shows 5m bars but the
  // summary pipeline only warms 1D/7D/30D/6M caches, so 4H maps to 1D for PnL).
  // On phones, the chart defaults to the 4H range (desktop keeps 1D; PnL stays
  // on the 1D summary since 4H is chart-only). Derived, so an explicit pick
  // always wins and the default never overwrites it.
  const [chartRangeChoice, setChartRangeChoice] =
    useState<ChartRangeKey | null>(null);
  const isPhoneLayout = useSyncExternalStore(
    subscribePhoneLayout,
    readPhoneLayout,
    () => false,
  );
  const chartRange: ChartRangeKey =
    chartRangeChoice ?? (isPhoneLayout ? "4H" : "1D");
  // Live EURUSD used to fold the Bitget USDT net into the € header rollup.
  // Fetched once per session; EUR_USD_FALLBACK_RATE covers the gap.
  const [eurUsdRate, setEurUsdRate] = useState<number | null>(null);
  // Decision timeline (per symbol): recent hourly + quarter ticks. Selecting an
  // older tick loads that decision into the card; null = newest (live) tick.
  const [symbolTimelines, setSymbolTimelines] = useState<
    Record<string, TimelineTickUi[]>
  >({});
  const [selectedTickTs, setSelectedTickTs] = useState<number | null>(null);
  const [selectedTickDecision, setSelectedTickDecision] =
    useState<DashboardDecisionResponse | null>(null);
  const [selectedTickLoading, setSelectedTickLoading] = useState(false);
  // Fetched historical decisions, keyed `${symbol}:${ts}` — clicking back and
  // forth on the timeline shouldn't refetch.
  const tickDecisionCacheRef = useRef<Map<string, DashboardDecisionResponse>>(
    new Map(),
  );
  // Latest tick the user asked for; a slower earlier fetch must not overwrite
  // a newer selection.
  const tickSelectionSeqRef = useRef(0);
  const [swingSummaryRange, setSwingSummaryRange] =
    useState<DashboardRangeKey | null>(null);
  const swingDashboardRequestIdRef = useRef(0);
  // True once the user clicks a symbol pill — from then on, dashboard reloads
  // keep their selection instead of re-defaulting to the top-ranked pill.
  const userPickedSymbolRef = useRef(false);
  // AI health (swing:ai:health:v1) — rides on the warm-status poll.
  // degraded with kind billing/config means AI calls fail until a human acts
  // (pay the subscription / fix the key); open positions run on their
  // exchange-side TP/SL bracket only.
  const [swingAiHealth, setSwingAiHealth] = useState<{
    degraded: boolean;
    dialect: string | null;
    kind: string | null;
    reason: string | null;
    sinceMs: number | null;
  } | null>(null);
  // "×"-dismissed key of the outage banner (kind + streak start). Keyed, not
  // boolean: a NEW outage (different sinceMs) re-shows the banner even after
  // an earlier one was dismissed. Resets on reload — deliberate for an alert.
  const [swingAiBannerDismissedKey, setSwingAiBannerDismissedKey] = useState<
    string | null
  >(null);
  // Live quotes keyed by symbol: the viewed one (drives the chart's live
  // candle) plus every symbol holding an open position (drives its pill's
  // PnL). Keyed rather than a single value, so a quote can never paint the
  // wrong symbol — the active reading is just a lookup that is absent until
  // this symbol's own quote lands.
  const [livePrices, setLivePrices] = useState<
    Record<string, { price: number; ts: number }>
  >({});
  const themePreference = useSyncExternalStore(
    subscribeThemePreference,
    readStoredThemePreference,
    (): ThemePreference => "dark",
  );
  const systemTheme = useSyncExternalStore(
    subscribeSystemTheme,
    readSystemTheme,
    (): ResolvedTheme => "dark",
  );
  const resolvedTheme: ResolvedTheme =
    themePreference === "system" ? systemTheme : themePreference;
  const readStoredAdminSecret = () => {
    if (typeof window === "undefined") return null;
    const stored = window.localStorage.getItem(ADMIN_SECRET_STORAGE_KEY);
    const normalized = typeof stored === "string" ? stored.trim() : "";
    return normalized || null;
  };

  const resolveAdminSecret = () => {
    const inMemory = typeof adminSecret === "string" ? adminSecret.trim() : "";
    if (inMemory) return inMemory;
    return readStoredAdminSecret();
  };

  const buildAdminHeaders = () => {
    const secret = resolveAdminSecret();
    return secret ? { "x-admin-access-secret": secret } : undefined;
  };
  // Effect events: effects read the latest admin secret / symbol platform
  // through these without widening their dependency arrays — re-subscription
  // stays keyed to the deps that matter, not to every value the body touches.
  const fetchAdminHeaders = useEffectEvent(buildAdminHeaders);
  // Symbols holding an open position, as a stable string: the quote poll below
  // keys off it so it re-subscribes when a position opens or closes, not on
  // every unrelated summary refresh.
  const openPositionSymbolKey = symbols
    .filter((sym) => {
      const direction = tabData[sym]?.openDirection;
      return direction === "long" || direction === "short";
    })
    .join(",");

  // Per-symbol view state resets the moment the viewed symbol context
  // changes — the render-time adjustment pattern (react.dev: "Adjusting
  // state when a prop changes"), replacing reset-inside-effect versions.
  const [prevViewKey, setPrevViewKey] = useState<
    readonly [boolean, string[], number] | null
  >(null);
  if (
    prevViewKey === null ||
    prevViewKey[0] !== adminGranted ||
    prevViewKey[1] !== symbols ||
    prevViewKey[2] !== active
  ) {
    setPrevViewKey([adminGranted, symbols, active]);
    if (prevViewKey !== null) {
      // Quotes are keyed by symbol, so nothing to reset here — a symbol the
      // poll has not answered for yet simply has no entry.
      setShowPrompt(false);
      setShowRawResponse(false);
      if (adminGranted && (symbols[active] || null)) {
        // Switching symbols returns the decision card to its live "latest" view.
        setSelectedTickTs(null);
        setSelectedTickDecision(null);
      }
    }
  }
  // EURUSD quote for the header rollup's $→€ conversion. Once per session —
  // the figure is approximate by design, so no polling.
  useEffect(() => {
    if (!adminGranted || eurUsdRate !== null) return;
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(
          "/api/swing/dashboard/live-price?symbol=EURUSD&platform=capital",
          { headers: fetchAdminHeaders(), cache: "no-store" },
        );
        if (!res.ok) return;
        const json = await res.json();
        const price = Number(json?.price);
        // Sanity band so a bad quote can't nuke the rollup.
        if (!cancelled && Number.isFinite(price) && price > 0.5 && price < 2) {
          setEurUsdRate(price);
        }
      } catch {
        // fallback rate covers it
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [adminGranted, eurUsdRate]);

  // Live price for the active symbol: REST polling every 3s (no venue
  // websockets — Capital would need a Lightstreamer session; the REST quote
  // covers both platforms uniformly). Feeds the chart's live candle and the
  // live open-PnL. Skips ticks while the tab is hidden or a request is still
  // in flight; the render-time reset above clears state on symbol switch so a
  // stale quote from the previous symbol never paints the new chart.
  useEffect(() => {
    const symbol = symbols[active] || null;
    if (!adminGranted || !symbol) return;
    // The viewed symbol plus every symbol carrying an open position: those
    // pills show a PnL that moves with price, and a summary-cadence number
    // there is stale by minutes. Everything else on the pill row is a settled
    // figure that price cannot change, so it stays off the poll.
    const wanted = Array.from(
      new Set([symbol, ...openPositionSymbolKey.split(",").filter(Boolean)]),
    );
    let cancelled = false;
    let inFlight = false;
    const tick = async () => {
      if (cancelled || inFlight || document.hidden) return;
      inFlight = true;
      try {
        const params = new URLSearchParams({ symbols: wanted.join(",") });
        const res = await fetch(
          `/api/swing/dashboard/live-price?${params.toString()}`,
          { headers: fetchAdminHeaders(), cache: "no-store" },
        );
        if (cancelled) return;
        if (!res.ok) return;
        const json = await res.json();
        const quotes = Array.isArray(json?.quotes) ? json.quotes : [];
        const next: Record<string, { price: number; ts: number }> = {};
        for (const quote of quotes) {
          const price = Number(quote?.price);
          const sym = String(quote?.symbol || "").toUpperCase();
          if (!sym || !Number.isFinite(price) || price <= 0) continue;
          const ts = Number(quote?.ts);
          next[sym] = { price, ts: Number.isFinite(ts) && ts > 0 ? ts : Date.now() };
        }
        // Merge, never replace: a symbol the venue failed on this tick keeps
        // its previous quote instead of blanking.
        if (!cancelled && Object.keys(next).length) {
          setLivePrices((prev) => ({ ...prev, ...next }));
        }
      } catch {
        // transient poll failure — keep the last quotes
      } finally {
        inFlight = false;
      }
    };
    void tick();
    const id = window.setInterval(tick, 3000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [adminGranted, symbols, active, openPositionSymbolKey]);

  const handleAuthExpired = (message?: string) => {
    if (typeof window !== "undefined") {
      window.localStorage.removeItem(ADMIN_SECRET_STORAGE_KEY);
    }
    setAdminSecret(null);
    setAdminGranted(false);
    setAdminInput("");
    setAdminError(
      message || "Admin session expired. Enter ADMIN_ACCESS_SECRET again.",
    );
  };

  const validateAdminAccess = async (secret: string | null) => {
    const controller = new AbortController();
    const timeout = window.setTimeout(
      () => controller.abort(),
      ADMIN_AUTH_TIMEOUT_MS,
    );
    const normalizedSecret = typeof secret === "string" ? secret.trim() : "";
    try {
      const res = await fetch("/api/admin-auth", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ secret: normalizedSecret }),
        signal: controller.signal,
      });
      const json = await res.json().catch(() => null);
      const required = json?.required !== false;
      const ok = Boolean(json?.ok);
      return { ok: res.ok && ok, required };
    } catch {
      return { ok: false, required: true };
    } finally {
      window.clearTimeout(timeout);
    }
  };

  const handleAdminSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setAdminError(null);
    setAdminSubmitting(true);
    const normalizedInput = adminInput.trim();
    const result = await validateAdminAccess(normalizedInput);
    if (result.ok) {
      if (result.required) {
        window.localStorage.setItem(ADMIN_SECRET_STORAGE_KEY, normalizedInput);
        setAdminSecret(normalizedInput);
      }
      setAdminGranted(true);
      setAdminInput("");
    } else {
      setAdminError("Invalid access secret.");
    }
    setAdminSubmitting(false);
  };

  const mergeTabPatch = (symbol: string, patch: Partial<EvaluationEntry>) => {
    setTabData((prev) => {
      const current = prev[symbol] || { symbol, evaluation: {} };
      const nextEvaluation = patch.evaluation ?? current.evaluation ?? {};
      return {
        ...prev,
        [symbol]: {
          ...current,
          ...patch,
          symbol,
          evaluation: nextEvaluation,
        },
      };
    });
  };

  const loadSymbolDecision = async (
    symbol: string,
    platform?: string | null,
  ) => {
    if (!symbol) return;
    const params = new URLSearchParams({ symbol });
    if (platform) params.set("platform", platform);
    const res = await fetch(
      `/api/swing/dashboard/decision?${params.toString()}`,
      {
        headers: buildAdminHeaders(),
        cache: "no-store",
      },
    );
    if (res.status === 401) {
      handleAuthExpired("Admin session expired. Re-enter ADMIN_ACCESS_SECRET.");
      throw new Error("Unauthorized");
    }
    if (!res.ok) {
      throw new Error(`Failed to load decision (${res.status})`);
    }
    const json: DashboardDecisionResponse = await res.json();
    mergeTabPatch(symbol, {
      category: json.category ?? null,
      lastPlatform: json.platform ?? platform ?? null,
      lastNewsSource: json.lastNewsSource ?? null,
      lastDecisionTs: json.lastDecisionTs ?? null,
      lastDecision: json.lastDecision ?? null,
      lastPrompt: json.lastPrompt ?? null,
      lastMetrics: json.lastMetrics ?? null,
      lastBiasTimeframes: json.lastBiasTimeframes ?? null,
    });
  };

  const loadSymbolTimeline = async (
    symbol: string,
    platform?: string | null,
  ) => {
    if (!symbol) return;
    const params = new URLSearchParams({ symbol });
    if (platform) params.set("platform", platform);
    const res = await fetch(
      `/api/swing/dashboard/timeline?${params.toString()}`,
      {
        headers: buildAdminHeaders(),
        cache: "no-store",
      },
    );
    if (res.status === 401) {
      handleAuthExpired("Admin session expired. Re-enter ADMIN_ACCESS_SECRET.");
      throw new Error("Unauthorized");
    }
    if (!res.ok) {
      throw new Error(`Failed to load timeline (${res.status})`);
    }
    const json: DashboardTimelineResponse = await res.json();
    setSymbolTimelines((prev) => ({
      ...prev,
      [symbol]: Array.isArray(json.ticks) ? json.ticks : [],
    }));
  };

  // Timeline tick click: the newest persisted decision returns to the live
  // "Latest Decision" view. Scan-only ticks, including the newest quarter
  // tick, remain selectable because their gate stage/reason lives inline.
  const handleTimelineTickSelect = async (
    symbol: string,
    tick: TimelineTickUi,
    isNewest: boolean,
  ) => {
    const seq = ++tickSelectionSeqRef.current;
    if (isNewest && tick.hasDetails) {
      setSelectedTickTs(null);
      setSelectedTickDecision(null);
      return;
    }
    setSelectedTickTs(tick.ts);
    if (!tick.hasDetails) {
      setSelectedTickLoading(false);
      setSelectedTickDecision(null);
      return;
    }
    const cacheKey = `${symbol}:${tick.ts}`;
    const cached = tickDecisionCacheRef.current.get(cacheKey);
    if (cached) {
      setSelectedTickDecision(cached);
      return;
    }
    setSelectedTickLoading(true);
    setSelectedTickDecision(null);
    try {
      const params = new URLSearchParams({ symbol, ts: String(tick.ts) });
      const platform = tabData[symbol]?.lastPlatform;
      if (platform) params.set("platform", platform);
      const res = await fetch(
        `/api/swing/dashboard/decision?${params.toString()}`,
        {
          headers: buildAdminHeaders(),
          cache: "no-store",
        },
      );
      if (res.status === 401) {
        handleAuthExpired(
          "Admin session expired. Re-enter ADMIN_ACCESS_SECRET.",
        );
        return;
      }
      if (!res.ok) return;
      const json: DashboardDecisionResponse = await res.json();
      tickDecisionCacheRef.current.set(cacheKey, json);
      // Only apply if this is still the tick the user is looking at.
      if (tickSelectionSeqRef.current === seq) setSelectedTickDecision(json);
    } catch {
      // tick stays selected with an empty body; the timeline itself is intact
    } finally {
      if (tickSelectionSeqRef.current === seq) setSelectedTickLoading(false);
    }
  };

  const loadSymbolEvaluation = async (symbol: string) => {
    if (!symbol) return;
    const params = new URLSearchParams({ symbol });
    const res = await fetch(
      `/api/swing/dashboard/evaluation?${params.toString()}`,
      {
        headers: buildAdminHeaders(),
        cache: "no-store",
      },
    );
    if (res.status === 401) {
      handleAuthExpired("Admin session expired. Re-enter ADMIN_ACCESS_SECRET.");
      throw new Error("Unauthorized");
    }
    if (!res.ok) {
      throw new Error(`Failed to load evaluation (${res.status})`);
    }
    const json: DashboardEvaluationResponse = await res.json();
    mergeTabPatch(symbol, {
      evaluation: json.evaluation || {},
      evaluationTs: json.evaluationTs ?? null,
    });
  };

  // background: refresh the data in place without the loading skeleton — used
  // by the warm-status poll when a new analyze cycle's summary lands.
  const loadDashboard = async (opts?: {
    background?: boolean;
    range?: DashboardRangeKey;
  }) => {
    const requestId = swingDashboardRequestIdRef.current + 1;
    swingDashboardRequestIdRef.current = requestId;
    const requestedRange = opts?.range ?? dashboardRange;
    setSwingSummaryRange((prev) => (prev === requestedRange ? prev : null));
    if (!opts?.background) setLoading(true);
    try {
      let summaryError: string | null = null;
      const symbolsRes = await fetch("/api/swing/dashboard/symbols", {
        headers: buildAdminHeaders(),
        cache: "no-store",
      });
      if (!symbolsRes.ok) {
        if (symbolsRes.status === 401) {
          handleAuthExpired(
            "Admin session expired. Re-enter ADMIN_ACCESS_SECRET.",
          );
        }
        throw new Error(`Failed to load symbols (${symbolsRes.status})`);
      }
      const symbolsJson: DashboardSymbolsResponse = await symbolsRes.json();
      const orderedSymbols = symbolsJson.symbols || [];
      const symbolMeta = new Map<string, DashboardSymbolRow>();
      for (const row of symbolsJson.data || []) {
        if (!row?.symbol) continue;
        symbolMeta.set(row.symbol.toUpperCase(), row);
      }

      const activeSymbolBefore = symbols[active] || null;
      setSymbols(orderedSymbols);
      setActive(() => {
        if (!activeSymbolBefore) return 0;
        const nextIdx = orderedSymbols.findIndex(
          (s) => s === activeSymbolBefore,
        );
        return nextIdx >= 0 ? nextIdx : 0;
      });

      setTabData((prev) => {
        const next: Record<string, EvaluationEntry> = {};
        for (const symbol of orderedSymbols) {
          const key = symbol.toUpperCase();
          const meta = symbolMeta.get(key);
          const existing = prev[key] ||
            prev[symbol] || { symbol: key, evaluation: {} };
          next[key] = {
            ...existing,
            symbol: key,
            evaluation: existing.evaluation || {},
            category: meta?.category ?? existing.category ?? null,
            lastPlatform: meta?.platform ?? existing.lastPlatform ?? null,
            lastNewsSource: meta?.newsSource ?? existing.lastNewsSource ?? null,
          };
        }
        return next;
      });

      try {
        const summaryParams = new URLSearchParams({ range: requestedRange });
        const summaryRes = await fetch(
          `/api/swing/dashboard/summary?${summaryParams.toString()}`,
          {
            headers: buildAdminHeaders(),
            cache: "no-store",
          },
        );
        if (summaryRes.status === 401) {
          handleAuthExpired(
            "Admin session expired. Re-enter ADMIN_ACCESS_SECRET.",
          );
          throw new Error("Unauthorized");
        }
        if (!summaryRes.ok) {
          throw new Error(`Failed to load summary (${summaryRes.status})`);
        }
        const summaryJson: DashboardSummaryResponse = await summaryRes.json();
        if (requestId !== swingDashboardRequestIdRef.current) return;
        const summaryRows = Array.isArray(summaryJson.data)
          ? summaryJson.data
          : [];
        const resolvedSummaryRange = summaryJson.range || requestedRange;
        setTabData((prev) => {
          const next = { ...prev };
          for (const row of summaryRows) {
            if (!row?.symbol) continue;
            const key = row.symbol.toUpperCase();
            const existing = next[key] || { symbol: key, evaluation: {} };
            next[key] = {
              ...existing,
              ...row,
              symbol: key,
              evaluation: existing.evaluation || {},
            };
          }
          return next;
        });
        setSwingSummaryRange(resolvedSummaryRange);

        // Default selection: the attention-sorted leftmost pill (open position
        // → resting limit → fresh AI decision → |range pnl|) — computable only
        // now that the summary is in. Skipped once the user picks a pill
        // themselves.
        if (!userPickedSymbolRef.current && orderedSymbols.length) {
          const rowBySymbol = new Map(
            summaryRows
              .filter((row) => row?.symbol)
              .map((row) => [String(row.symbol).toUpperCase(), row] as const),
          );
          // [rank, tiebreak] — lower wins; mirrors orderedSymbolPills:
          // open position → resting limit → AI-decision recency → closed.
          const rankOf = (sym: string): [number, number] => {
            const row = rowBySymbol.get(sym.toUpperCase());
            if (!row) return [2, 0];
            const aiRecency =
              typeof row.lastAiDecisionTs === "number" &&
              row.lastAiDecisionTs > 0
                ? -row.lastAiDecisionTs
                : 0;
            if (row.marketClosed === true) return [3, aiRecency];
            if (row.openDirection === "long" || row.openDirection === "short")
              return [0, aiRecency];
            if (row.pendingEntry === true) return [1, aiRecency];
            return [2, aiRecency];
          };
          let bestIdx = 0;
          let bestRank: [number, number] = [Infinity, 0];
          orderedSymbols.forEach((sym, idx) => {
            const rank = rankOf(sym);
            if (
              rank[0] < bestRank[0] ||
              (rank[0] === bestRank[0] && rank[1] < bestRank[1])
            ) {
              bestRank = rank;
              bestIdx = idx;
            }
          });
          setActive(bestIdx);
        }

        // Money-calendar strip: fold each symbol's per-day closed nets into one
        // day → {USD, EUR, trades} map. It renders as many trailing days as the
        // panel is wide, so the source is the 30D blob rather than 7D — same
        // one KV read (every range is cron-warmed), just enough days to fill a
        // wide screen. Non-fatal: a failure keeps the strip's previous data.
        try {
          let weekRows: DashboardSummaryRow[] = summaryRows;
          if (resolvedSummaryRange !== CALENDAR_SOURCE_RANGE) {
            const weekRes = await fetch(
              `/api/swing/dashboard/summary?range=${CALENDAR_SOURCE_RANGE}`,
              { headers: buildAdminHeaders(), cache: "no-store" },
            );
            if (!weekRes.ok) {
              throw new Error(
                `Failed to load ${CALENDAR_SOURCE_RANGE} summary (${weekRes.status})`,
              );
            }
            const weekJson: DashboardSummaryResponse = await weekRes.json();
            weekRows = Array.isArray(weekJson.data) ? weekJson.data : [];
          }
          if (requestId !== swingDashboardRequestIdRef.current) return;
          const byDay: Record<
            string,
            { netUsd: number | null; netEur: number | null; trades: number }
          > = {};
          for (const row of weekRows) {
            for (const bucket of row?.pnlDaily ?? []) {
              if (!bucket?.day) continue;
              const slot = (byDay[bucket.day] ??= {
                netUsd: null,
                netEur: null,
                trades: 0,
              });
              slot.trades += bucket.trades || 0;
              if (typeof bucket.net === "number") {
                if (platformCurrencySymbol(row.lastPlatform) === "€") {
                  slot.netEur = (slot.netEur ?? 0) + bucket.net;
                } else {
                  slot.netUsd = (slot.netUsd ?? 0) + bucket.net;
                }
              }
            }
          }
          setSwingWeekDaily(byDay);
          setSwingWeekLoadedAtMs(Date.now());
        } catch (weekErr) {
          console.warn("week-calendar summary load failed:", weekErr);
        }
      } catch (summaryErr) {
        if (requestId === swingDashboardRequestIdRef.current) {
          setSwingSummaryRange(null);
        }
        summaryError =
          errMsg(summaryErr) || "Failed to load dashboard summary";
      }

      setError(summaryError);
    } catch (err) {
      setError(errMsg(err) || "Failed to load dashboard");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (typeof window === "undefined") return;
    const stored = readStoredAdminSecret();
    (async () => {
      let result = { ok: false, required: true };
      try {
        result = await validateAdminAccess(stored);
      } catch {
        result = { ok: false, required: true };
      }
      if (result.ok) {
        if (result.required && stored) {
          setAdminSecret(stored);
        }
        setAdminGranted(true);
      } else if (stored) {
        window.localStorage.removeItem(ADMIN_SECRET_STORAGE_KEY);
      }
      setAdminReady(true);
      if (!result.ok) {
        setLoading(false);
      }
    })();
  }, []);

  useEffect(() => {
    if (typeof document === "undefined") return;
    document.documentElement.style.colorScheme = resolvedTheme;
  }, [resolvedTheme]);
  // The range keys the effect below; the event reads all other state (tabs,
  // request ids) at call time.
  const refreshDashboard = useEffectEvent((range: DashboardRangeKey) => {
    void loadDashboard({ range });
  });
  useEffect(() => {
    if (!adminGranted) return;
    // Canonical fetch-effect: loadDashboard flips its own loading state
    // synchronously before fetching — there is no rule-clean formulation of
    // "fetch on auth/range change with a spinner" that isn't worse code.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    refreshDashboard(dashboardRange);
  }, [adminGranted, dashboardRange]);
  const loadActiveSymbolDetails = useEffectEvent(
    async (symbol: string, isCancelled: () => boolean) => {
      const platform = tabData[symbol]?.lastPlatform ?? null;
      try {
        await Promise.all([
          loadSymbolDecision(symbol, platform),
          loadSymbolEvaluation(symbol),
          // Timeline is decoration around the decision card — a failure there
          // must not blank the whole card.
          loadSymbolTimeline(symbol, platform).catch(() => undefined),
        ]);
        if (!isCancelled()) setError(null);
      } catch (err) {
        if (isCancelled()) return;
        setError(errMsg(err) || `Failed to load details for ${symbol}`);
      }
    },
  );
  useEffect(() => {
    const symbol = symbols[active] || null;
    if (!adminGranted || !symbol) return;
    let cancelled = false;
    // Canonical fetch-effect: the loaders merge fetched data into state; the
    // sync prefix the rule flags is their request bookkeeping, not derived
    // state that could move to render.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    void loadActiveSymbolDetails(symbol, () => cancelled);
    return () => {
      cancelled = true;
    };
  }, [adminGranted, symbols, active]);

  // Cycle-aware refresh: /api/analyze's warm latch stamps swing:warm:last the
  // moment the LAST analyze cron of a 15-minute cycle finished rebuilding the
  // summary blobs (the fallback warm stamps it too). Polling that tiny status
  // endpoint (one KV read) tells an open dashboard exactly when new decisions
  // and timeline ticks are queryable — no full-summary polling on a timer.
  // The refresher is an effect event so the poll effect keeps stable deps
  // while still seeing the current range/symbol/platform.
  const swingWarmSeenMsRef = useRef<number | null>(null);
  const [chartRefreshToken, setChartRefreshToken] = useState(0);
  const onSwingWarm = useEffectEvent(() => {
    void loadDashboard({ background: true });
    // Chart overlays too: the candles ride the live-price feed, but the
    // resting-entry windows, cooldown bands, position boxes and markers only
    // change when the chart payload is re-read. Without this the price line
    // advances for hours over overlays frozen at the last fetch.
    setChartRefreshToken((prev) => prev + 1);
    const symbol = symbols[active] || null;
    if (!symbol) return;
    const platform = tabData[symbol]?.lastPlatform ?? null;
    // Decision card + timeline dots for the visible symbol; both merge into
    // existing state, so a user inspecting an older tick isn't disturbed.
    void loadSymbolDecision(symbol, platform).catch(() => undefined);
    void loadSymbolTimeline(symbol, platform).catch(() => undefined);
  });

  useEffect(() => {
    if (!adminGranted) return;
    if (typeof window === "undefined") return;
    let cancelled = false;
    let inFlight = false;
    const tick = async () => {
      if (cancelled || inFlight || document.hidden) return;
      inFlight = true;
      try {
        const res = await fetch("/api/swing/dashboard/warm-status", {
          headers: fetchAdminHeaders(),
          cache: "no-store",
        });
        if (!res.ok || cancelled) return;
        const json = await res.json();
        const health = json?.aiHealth;
        if (health && typeof health === "object") {
          setSwingAiHealth({
            degraded: health.degraded === true,
            dialect: typeof health.dialect === "string" ? health.dialect : null,
            kind: typeof health.kind === "string" ? health.kind : null,
            reason: typeof health.reason === "string" ? health.reason : null,
            sinceMs:
              Number.isFinite(Number(health.sinceMs)) && Number(health.sinceMs) > 0
                ? Number(health.sinceMs)
                : null,
          });
        }
        const warmedAtMs = Number(json?.warmedAtMs);
        if (!Number.isFinite(warmedAtMs) || warmedAtMs <= 0) return;
        const seen = swingWarmSeenMsRef.current;
        swingWarmSeenMsRef.current = warmedAtMs;
        // The first sample is only a baseline — the page just loaded fresh
        // data anyway. Refresh when a NEWER warm lands after that.
        if (seen !== null && warmedAtMs > seen && !cancelled) {
          onSwingWarm();
        }
      } catch {
        // transient poll failure — retry on the next tick
      } finally {
        inFlight = false;
      }
    };
    void tick();
    const id = window.setInterval(tick, 20_000);
    // Background tabs throttle setInterval (~1/min); catch up immediately when
    // the tab becomes visible again.
    const onVisible = () => {
      if (!document.hidden) void tick();
    };
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      cancelled = true;
      window.clearInterval(id);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [adminGranted]);

  // Decision prices span BTC (~118,000) to forex (~1.08) — scale the decimals
  // to the magnitude instead of one fixed precision.
  const formatDecisionPrice = (value: number): string => {
    const abs = Math.abs(value);
    const maxDecimals = abs >= 1000 ? 0 : abs >= 10 ? 2 : 4;
    return value.toLocaleString("en-US", { maximumFractionDigits: maxDecimals });
  };

  // "90" → "1h30m", "45" → "45m", "360" → "6h" — for the cooldown suffix.
  const formatCooldownDuration = (minutes: number): string => {
    const m = Math.max(1, Math.round(minutes));
    if (m < 60) return `${m}m`;
    const h = Math.floor(m / 60);
    const rest = m % 60;
    return rest ? `${h}h${rest}m` : `${h}h`;
  };

  // "HOLD + CD 2h (↑51,200 ↓49,700)" — the armed quiet period and the wake
  // bands that end it early. Empty when the decision carries no cooldown.
  const formatCooldownSuffix = (
    decision: Record<string, unknown> | null | undefined,
  ): string => {
    const minutes = Number(decision?.cooldown_minutes ?? decision?.cooldownMinutes);
    if (!Number.isFinite(minutes) || minutes <= 0) return "";
    const above = Number(decision?.cooldown_wake_above ?? decision?.cooldownWakeAbove);
    const below = Number(decision?.cooldown_wake_below ?? decision?.cooldownWakeBelow);
    const bands = [
      Number.isFinite(above) && above > 0 ? `↑${formatDecisionPrice(above)}` : null,
      Number.isFinite(below) && below > 0 ? `↓${formatDecisionPrice(below)}` : null,
    ].filter(Boolean);
    return ` + CD ${formatCooldownDuration(minutes)}${bands.length ? ` (${bands.join(" ")})` : ""}`;
  };

  // Action label for the Latest Decision pill: a partial CLOSE (trim) shows its
  // size, e.g. "CLOSE 40%"; a full close (pct absent or 100) stays "CLOSE"; a
  // resting entry shows its resting price and which kind rests there, e.g.
  // "BUY @ 6.34" (limit) or "SELL stop @ 2,426" (a market entry stays bare
  // "BUY"/"SELL"); a flat HOLD that armed a cooldown shows the quiet period +
  // wake bands, e.g. "HOLD + CD 2h (↑51,200 ↓49,700)".
  const formatLastDecisionAction = (
    decision: Record<string, unknown> | null | undefined,
  ): string => {
    const action = String(decision?.action || "");
    if (action === "BUY" || action === "SELL") {
      // Both resting legs carry a price and exactly one is ever set (analyze
      // rewrites the pair off the sanitized kind). Reading only the limit leg
      // made every STOP entry read as a bare market "SELL".
      const limit = Number(decision?.entry_limit_price);
      if (Number.isFinite(limit) && limit > 0) {
        return `${action} @ ${formatDecisionPrice(limit)}`;
      }
      const stop = Number(decision?.entry_stop_price);
      if (Number.isFinite(stop) && stop > 0) {
        return `${action} stop @ ${formatDecisionPrice(stop)}`;
      }
      return action;
    }
    if (action === "HOLD") return `${action}${formatCooldownSuffix(decision)}`;
    if (action !== "CLOSE") return action;
    const rawPct =
      decision?.exit_size_pct ?? decision?.close_size_pct ?? decision?.partial_close_pct;
    const pct = Number(rawPct);
    if (Number.isFinite(pct) && pct > 0 && pct < 100) return `CLOSE ${Math.round(pct)}%`;
    return action;
  };

  const formatDecisionTime = (ts?: number | null) => {
    if (!ts) return "";
    const d = new Date(ts);
    const now = new Date();
    const sameDay =
      d.getFullYear() === now.getFullYear() &&
      d.getMonth() === now.getMonth() &&
      d.getDate() === now.getDate();
    const time = d.toLocaleTimeString("de-DE", {
      hour: "2-digit",
      minute: "2-digit",
      timeZone: BERLIN_TZ,
    });
    if (sameDay) return `– ${time}`;
    const date = d.toLocaleDateString("de-DE", { timeZone: BERLIN_TZ });
    return `– ${date} ${time}`;
  };

  const renderPromptContent = (text?: string | null) => {
    if (!text?.trim()) {
      return <span className="text-[11px] text-slate-500">Not available</span>;
    }
    const blocks = text.split(/\n\s*\n/);
    const rendered = blocks
      .map((block, idx) => {
        const trimmed = block.trim();
        if (!trimmed) return null;
        const looksJson =
          (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
          (trimmed.startsWith("[") && trimmed.endsWith("]"));
        if (looksJson) {
          try {
            const parsed = JSON.parse(trimmed);
            return (
              <pre
                key={`json-${idx}`}
                className="overflow-auto rounded-lg border border-slate-800 bg-slate-900/95 px-3 py-2 font-mono text-[11px] leading-snug text-slate-100 shadow-sm"
              >
                {JSON.stringify(parsed, null, 2)}
              </pre>
            );
          } catch {
            // fall through to raw text
          }
        }
        return (
          <pre
            key={`txt-${idx}`}
            className="whitespace-pre-wrap break-words rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 font-mono text-[11px] leading-snug text-slate-800"
          >
            {trimmed}
          </pre>
        );
      })
      .filter(Boolean);

    if (!rendered.length) {
      return <span className="text-[11px] text-slate-500">Not available</span>;
    }
    return <div className="space-y-2">{rendered}</div>;
  };

  // Open PnL on margin from a live quote — price move × side × leverage. One
  // formula for the header/chart reading and for every pill, so a position's
  // number cannot differ depending on where you look at it.
  const liveOpenPnlPct = (
    tab: (typeof tabData)[string] | null | undefined,
    price: number | null,
  ): number | null => {
    const entry = Number(tab?.openEntryPrice);
    const direction = tab?.openDirection;
    if (price === null || !Number.isFinite(price) || price <= 0) return null;
    if (!Number.isFinite(entry) || entry <= 0) return null;
    if (direction !== "long" && direction !== "short") return null;
    const leverage =
      typeof tab?.openLeverage === "number" && tab.openLeverage > 0
        ? tab.openLeverage
        : 1;
    return (
      ((price - entry) / entry) * (direction === "long" ? 1 : -1) * leverage * 100
    );
  };

  const current = symbols[active] ? tabData[symbols[active]] : null;
  const activeSymbol = symbols[active] || null;
  const livePriceNow = activeSymbol
    ? (livePrices[activeSymbol]?.price ?? null)
    : null;
  const livePriceTs = activeSymbol ? (livePrices[activeSymbol]?.ts ?? null) : null;
  const swingSummaryMatchesRange = swingSummaryRange === dashboardRange;
  const liveOpenPnl = liveOpenPnlPct(current, livePriceNow);
  const effectiveOpenPnl =
    typeof liveOpenPnl === "number"
      ? liveOpenPnl
      : current && typeof current.openPnl === "number"
        ? current.openPnl
        : null;
  const effectiveRangePnlWithOpen =
    swingSummaryMatchesRange &&
    current &&
    typeof current.pnl7d === "number" &&
    typeof effectiveOpenPnl === "number"
      ? current.pnl7d + effectiveOpenPnl
      : swingSummaryMatchesRange && current && typeof current.pnl7d === "number"
        ? current.pnl7d
        : swingSummaryMatchesRange && typeof effectiveOpenPnl === "number"
          ? effectiveOpenPnl
          : swingSummaryMatchesRange &&
              current &&
              typeof current.pnl7dWithOpen === "number"
            ? current.pnl7dWithOpen
            : null;
  const effectiveRangeCashPnl =
    swingSummaryMatchesRange && current && typeof current.pnl7dNet === "number"
      ? current.pnl7dNet
      : null;
  const rangePnlToneValue =
    typeof effectiveRangePnlWithOpen === "number"
      ? effectiveRangePnlWithOpen
      : typeof effectiveRangeCashPnl === "number"
        ? effectiveRangeCashPnl
        : null;
  const rangePnlLabel =
    typeof effectiveRangePnlWithOpen === "number"
      ? `${effectiveRangePnlWithOpen.toFixed(2)}%`
      : null;
  // Attention-first pill ordering: the header row gets scanned for "what's up"
  // on every visit — open positions first, then resting entry limits, then
  // everything else by AI-decision recency (freshest real AI call first, so an
  // hourly-tick decision naturally outranks stale ones; symbols the AI never
  // looked at trail the bucket), market-closed at the very end. Ties keep the
  // original symbol order, and clicks keep working because each pill carries
  // its original index into `symbols`.
  const rangePnlForPill = (tab?: EvaluationEntry): number | null => {
    if (!swingSummaryMatchesRange || !tab) return null;
    if (typeof tab.pnl7dWithOpen === "number") return tab.pnl7dWithOpen;
    if (typeof tab.pnl7d === "number") return tab.pnl7d;
    return null;
  };
  const aiRecencyForPill = (tab?: EvaluationEntry): number | null =>
    typeof tab?.lastAiDecisionTs === "number" && tab.lastAiDecisionTs > 0
      ? tab.lastAiDecisionTs
      : null;
  const orderedSymbolPills = symbols
    .map((sym, index) => {
      const tab = tabData[sym];
      const marketClosed = tab?.marketClosed === true;
      const openDirection =
        !marketClosed &&
        (tab?.openDirection === "long" || tab?.openDirection === "short")
          ? tab.openDirection
          : null;
      const pnl = rangePnlForPill(tab);
      const aiTs = aiRecencyForPill(tab);
      const rank = marketClosed
        ? 3
        : openDirection
          ? 0
          : tab?.pendingEntry === true
            ? 1
            : 2;
      return { sym, index, tab, marketClosed, openDirection, pnl, aiTs, rank };
    })
    .sort((a, b) => {
      if (a.rank !== b.rank) return a.rank - b.rank;
      // Within every bucket (including market-closed): fresher AI decision
      // first; never-AI-called symbols (aiTs null → 0) fall to the tail.
      const tsDiff = (b.aiTs ?? 0) - (a.aiTs ?? 0);
      if (tsDiff !== 0) return tsDiff;
      return a.index - b.index;
    });
  // How many days the strip shows: whatever fits the panel, measured. It used
  // to be a fixed 7 with the two oldest hidden under `sm:` — a breakpoint guess
  // that left a wide screen half empty and a narrow one scrolling.
  const [calendarWidthPx, setCalendarWidthPx] = useState(0);
  const calendarDayCount = (() => {
    if (calendarWidthPx <= 0) return 7;
    const step = CALENDAR_CELL_PX + CALENDAR_CELL_GAP_PX;
    const fits = Math.floor((calendarWidthPx + CALENDAR_CELL_GAP_PX) / step);
    return Math.max(CALENDAR_MIN_DAYS, Math.min(CALENDAR_MAX_DAYS, fits));
  })();
  // Live open money, folded to € across every symbol holding a position. Cash
  // rather than percent: this is the one place the dashboard shows real money,
  // and an open position's money is only real once you count it. Preference
  // order per symbol: margin x the live percent (moves with the 3s quote),
  // else the venue's own cash figure from the last summary build.
  const liveOpenCashEur = (() => {
    const rate = eurUsdRate ?? EUR_USD_FALLBACK_RATE;
    let total = 0;
    let live = false;
    let counted = 0;
    for (const sym of symbols) {
      const tab = tabData[sym];
      if (tab?.openDirection !== "long" && tab?.openDirection !== "short") continue;
      const margin = typeof tab?.openMargin === "number" ? tab.openMargin : null;
      const livePct = liveOpenPnlPct(tab, livePrices[sym]?.price ?? null);
      const fromLive = margin !== null && livePct !== null ? (margin * livePct) / 100 : null;
      const cash = fromLive ?? (typeof tab?.openPnlCash === "number" ? tab.openPnlCash : null);
      if (cash === null) continue;
      if (fromLive !== null) live = true;
      counted += 1;
      total += platformCurrencySymbol(tab?.lastPlatform) === "€" ? cash : cash / rate;
    }
    return counted ? { eur: total, live, positions: counted } : null;
  })();
  // Money calendar: the trailing Berlin days that FIT (oldest → today), each
  // cell carrying that day's all-symbols closed net in €. The USDT net is
  // folded in at the live EURUSD rate (fallback approximation when the quote
  // hasn't loaded) — cells with a conversion are marked ≈ in their tooltip.
  // Today's cell also carries the open positions' live money.
  const swingWeekCalendar = (() => {
    if (!swingWeekDaily || swingWeekLoadedAtMs === null) return null;
    const rate = eurUsdRate ?? EUR_USD_FALLBACK_RATE;
    const cells: Array<{
      key: string;
      dayNum: string;
      weekday: string;
      month: string;
      showMonth: boolean;
      isToday: boolean;
      net: number | null;
      approximate: boolean;
      trades: number;
      // Today only: the open positions' unrealized € already folded into `net`.
      openEur: number | null;
      openLive: boolean;
      openPositions: number;
    }> = [];
    for (let daysBack = calendarDayCount - 1; daysBack >= 0; daysBack--) {
      const date = new Date(swingWeekLoadedAtMs - daysBack * 24 * 60 * 60 * 1000);
      const key = BERLIN_DAY_KEY_FORMAT.format(date);
      const slot = swingWeekDaily[key];
      const hasNet =
        typeof slot?.netUsd === "number" || typeof slot?.netEur === "number";
      const month = BERLIN_MONTH_FORMAT.format(date);
      const isToday = daysBack === 0;
      const realized = hasNet
        ? (slot?.netEur ?? 0) +
          (typeof slot?.netUsd === "number" ? slot.netUsd / rate : 0)
        : null;
      const open = isToday ? liveOpenCashEur : null;
      cells.push({
        key,
        dayNum: BERLIN_DAY_NUM_FORMAT.format(date),
        weekday: BERLIN_WEEKDAY_FORMAT.format(date),
        month,
        // Month label only where it changes along the row (and on the first
        // cell) — keeps the strip narrow.
        showMonth: cells.length === 0 || cells[cells.length - 1].month !== month,
        isToday,
        // Realized + what is still on the table. A day with no closes but an
        // open position is a number, not a dash.
        net: open ? (realized ?? 0) + open.eur : realized,
        approximate: typeof slot?.netUsd === "number" && slot.netUsd !== 0,
        trades: slot?.trades ?? 0,
        openEur: open?.eur ?? null,
        openLive: open?.live ?? false,
        openPositions: open?.positions ?? 0,
      });
    }
    return cells;
  })();
  // The panel's headline: today's money, realized + still open. Same number as
  // the calendar's last cell — a summary and its breakdown, deliberately — but
  // sitting where the eye lands first instead of at the far end of a 24-cell
  // row. Everything else in the panel is context for this.
  const todayMoney = (() => {
    const today = swingWeekCalendar?.[swingWeekCalendar.length - 1];
    if (!today?.isToday) return null;
    const realized = today.openEur !== null ? (today.net ?? 0) - today.openEur : today.net;
    return {
      net: today.net,
      openEur: today.openEur,
      title: [
        `${today.key} · ${today.trades} trade${today.trades === 1 ? "" : "s"}`,
        realized !== null
          ? `booked ${realized >= 0 ? "+" : ""}${formatCash(realized, "€")}`
          : "nothing booked yet",
        today.openEur !== null
          ? `open ${today.openPositions} position${
              today.openPositions === 1 ? "" : "s"
            }: ${today.openEur >= 0 ? "+" : ""}${formatCash(today.openEur, "€")}${
              today.openLive ? " (live)" : ""
            }`
          : null,
      ]
        .filter(Boolean)
        .join(" · "),
    };
  })();
  // The pill row is one horizontally-scrollable line; keep the active pill in
  // view when selection changes (not on every render — live ticks re-render
  // constantly and must not fight the user's scroll position).
  const pillRowRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    const row = pillRowRef.current;
    if (!row) return;
    const activePill = row.querySelector<HTMLElement>('[data-active-pill="true"]');
    activePill?.scrollIntoView({ block: "nearest", inline: "nearest" });
  }, [active]);
  // Money-calendar strip: measure the row so the day count follows the actual
  // width, at every size, instead of a breakpoint guess.
  //
  // The observer watches the row WRAPPER, which is always mounted. Watching the
  // calendar element itself did nothing: on mount that node does not exist yet
  // (the skeleton is up until the summary lands), so a one-shot effect found a
  // null ref, never attached, and the count sat on its 7-day fallback forever.
  const weekCalendarRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    const row = weekCalendarRef.current;
    if (!row) return;
    const apply = (width: number) =>
      setCalendarWidthPx((prev) => (Math.abs(prev - width) < 1 ? prev : width));
    if (typeof ResizeObserver === "undefined") {
      apply(row.clientWidth);
      return;
    }
    const observer = new ResizeObserver((entries) => {
      apply(entries[0]?.contentRect.width ?? 0);
    });
    observer.observe(row);
    return () => observer.disconnect();
  }, []);
  const showChartPanel = Boolean(adminGranted && activeSymbol);
  // Compact PnL stats (range / open) that ride alongside the chart's range
  // switches. Rendered inside ChartPanel so it no longer costs its own header
  // line; on mobile it takes the "1H bars · window" slot. Each block only
  // renders when it actually has data — no "—" placeholders.
  const rangeStatHasData = Boolean(rangePnlLabel);
  const openStatHasData = Boolean(
    current &&
      (current.openDirection || typeof effectiveOpenPnl === "number"),
  );
  const swingChartStats =
    current &&
    (rangeStatHasData || openStatHasData) ? (
      <div className="flex flex-nowrap items-center gap-x-2 overflow-x-auto text-[12px] tabular-nums sm:gap-x-3 sm:text-[13px]">
        {rangeStatHasData ? (
          <span className="flex shrink-0 items-baseline gap-1">
            <span className="text-[10px] uppercase tracking-wide text-slate-500">
              {dashboardRange}
            </span>
            <span
              className={`font-semibold ${
                typeof rangePnlToneValue === "number"
                  ? rangePnlToneValue >= 0
                    ? "text-emerald-600"
                    : "text-rose-600"
                  : "text-slate-500"
              }`}
            >
              {rangePnlLabel}
            </span>
            {swingSummaryMatchesRange &&
            typeof current.pnl7dNet === "number" ? (
              <span className="hidden text-[11px] text-slate-500 sm:inline">
                {current.pnl7dNet >= 0 ? "+" : ""}
                {formatCash(
                  current.pnl7dNet,
                  platformCurrencySymbol(current.lastPlatform),
                )}
              </span>
            ) : null}
            {swingSummaryMatchesRange && current.pnl7dTrades ? (
              <span className="hidden text-[11px] text-slate-400 sm:inline">
                ·{current.pnl7dTrades}t
              </span>
            ) : null}
          </span>
        ) : null}
        {rangeStatHasData &&
        swingSummaryMatchesRange &&
        Array.isArray(current.pnlSpark) &&
        current.pnlSpark.length > 1 ? (
          <span className="hidden h-4 items-end gap-[2px] rounded bg-slate-100 px-1 py-[3px] sm:inline-flex">
            {current.pnlSpark.map((v, i) => {
              const arr = current.pnlSpark as number[];
              const max = Math.max(...arr.map((n) => Math.abs(n)), 1e-9);
              const isLatest = i === arr.length - 1;
              const h = Math.max(2, Math.round((Math.abs(v) / max) * 10));
              return (
                <span
                  key={i}
                  className={`rounded-full shadow-[0_0_0_1px_rgba(15,23,42,0.04)] ${
                    isLatest ? "w-[4px]" : "w-[3px]"
                  } ${
                    v >= 0
                      ? isLatest
                        ? "bg-emerald-500"
                        : "bg-emerald-500/80"
                      : isLatest
                        ? "bg-rose-500"
                        : "bg-rose-500/80"
                  }`}
                  style={{ height: `${h}px` }}
                />
              );
            })}
          </span>
        ) : null}
        {openStatHasData ? (
          <>
            {rangeStatHasData ? (
              <span className="shrink-0 text-slate-300">·</span>
            ) : null}
            <span className="flex shrink-0 items-baseline gap-1">
              <span className="text-[10px] uppercase tracking-wide text-slate-500">
                open
              </span>
              <span
                className={`font-semibold ${
                  typeof effectiveOpenPnl === "number"
                    ? effectiveOpenPnl >= 0
                      ? "text-emerald-600"
                      : "text-rose-600"
                    : "text-slate-500"
                }`}
              >
                {typeof effectiveOpenPnl === "number"
                  ? `${effectiveOpenPnl.toFixed(2)}%`
                  : "—"}
              </span>
              {current.openDirection ? (
                <span
                  className={`text-[10px] ${current.openDirection === "long" ? "text-emerald-600" : "text-rose-600"}`}
                >
                  {current.openDirection === "long" ? "L" : "S"}
                  {typeof current.openLeverage === "number"
                    ? `${current.openLeverage.toFixed(0)}x`
                    : ""}
                </span>
              ) : null}
            </span>
          </>
        ) : null}
      </div>
    ) : null;
  const handleChartOpenPositionChange = (
    symbol: string | null,
    position: {
      pnlPct: number | null;
      side: "long" | "short" | null;
      leverage: number | null;
      entryPrice: number | null;
    } | null,
  ) => {
    if (!symbol) return;
    const key = symbol.toUpperCase();
    setTabData((prev) => {
      const existing = prev[key];
      if (!existing) return prev;
      const nextOpenPnl = position?.pnlPct ?? null;
      const nextOpenDirection = position?.side ?? null;
      const nextOpenLeverage = position?.leverage ?? null;
      const nextOpenEntryPrice = position?.entryPrice ?? null;
      if (
        existing.openPnl === nextOpenPnl &&
        existing.openDirection === nextOpenDirection &&
        existing.openLeverage === nextOpenLeverage &&
        existing.openEntryPrice === nextOpenEntryPrice
      ) {
        return prev;
      }
      return {
        ...prev,
        [key]: {
          ...existing,
          openPnl: nextOpenPnl,
          openDirection: nextOpenDirection,
          openLeverage: nextOpenLeverage,
          openEntryPrice: nextOpenEntryPrice,
        },
      };
    });
  };
  const handleChartPositionSummaryChange = (
    symbol: string | null,
    summary: {
      closedPnlPct: number | null;
      closedPnlNet: number | null;
      closedCount: number;
      lastPnlPct: number | null;
      lastSide: "long" | "short" | null;
      lastLeverage: number | null;
      openPnlPct: number | null;
      openSide: "long" | "short" | null;
      openLeverage: number | null;
      openEntryPrice: number | null;
    },
  ) => {
    if (!symbol || !swingSummaryMatchesRange) return;
    // 4H is a chart-only range (the dashboard stays on 1D): its overlay
    // window covers just 4 hours, so a chart-derived closed-PnL rollup would
    // overwrite the pill's 1D figures with 4-hour ones on every chart load.
    if (chartRange === "4H") return;
    const key = symbol.toUpperCase();
    setTabData((prev) => {
      const existing = prev[key];
      if (!existing) return prev;
      const openPnl = summary.openPnlPct;
      const closedPnl = summary.closedPnlPct;
      const pnlWithOpen =
        typeof closedPnl === "number" && typeof openPnl === "number"
          ? closedPnl + openPnl
          : typeof closedPnl === "number"
            ? closedPnl
            : typeof openPnl === "number"
              ? openPnl
              : null;
      return {
        ...prev,
        [key]: {
          ...existing,
          pnl7d: closedPnl,
          pnl7dWithOpen: pnlWithOpen,
          pnl7dNet: summary.closedPnlNet ?? existing.pnl7dNet ?? null,
          pnl7dTrades: summary.closedCount,
          lastPositionPnl: summary.lastPnlPct,
          lastPositionDirection: summary.lastSide,
          lastPositionLeverage: summary.lastLeverage,
          openPnl,
          openDirection: summary.openSide,
          openLeverage: summary.openLeverage,
          openEntryPrice: summary.openEntryPrice,
        },
      };
    });
  };
  const hasLastDecision = !!(
    current &&
    ("lastDecision" in current ||
      "lastDecisionTs" in current ||
      "lastPrompt" in current ||
      "lastMetrics" in current ||
      "lastBiasTimeframes" in current)
  );
  // Decision timeline for the active symbol + what the card body shows: the
  // live latest decision by default, or the selected tick — a fetched decision
  // row for `hasDetails` ticks, the tick's own gate stage/reason for
  // quarter-tick scans (those were never persisted as decision rows).
  const activeTimeline = activeSymbol
    ? symbolTimelines[activeSymbol] ?? []
    : [];
  // Decision-lane ticks only — analysis reports (kind 'postmortem') live on
  // their own lane with their own selection.
  const activeDecisionTimeline = activeTimeline.filter(
    (tick) => tick.kind !== "postmortem",
  );
  const selectedTick =
    selectedTickTs !== null
      ? activeDecisionTimeline.find((tick) => tick.ts === selectedTickTs) ??
        null
      : null;
  const displayDecision = selectedTick
    ? selectedTick.hasDetails
      ? selectedTickDecision?.lastDecision ?? null
      : null
    : current?.lastDecision ?? null;
  const displayDecisionTs = selectedTick
    ? selectedTick.ts
    : current?.lastDecisionTs ?? null;
  const displayPrompt = selectedTick
    ? selectedTickDecision?.lastPrompt ?? null
    : current?.lastPrompt ?? null;
  const displayBiasTimeframes = selectedTick
    ? selectedTickDecision?.lastBiasTimeframes ?? null
    : current?.lastBiasTimeframes ?? null;
  // Partial close (e.g. CLOSE 50%): the action pill goes amber — trims carry
  // the same yellow as their timeline dot and chart marker.
  const displayIsTrim = (() => {
    const d = displayDecision;
    if (!d || String(d.action || "").toUpperCase() !== "CLOSE") return false;
    const pct = Number(
      d.exit_size_pct ?? d.close_size_pct ?? d.partial_close_pct,
    );
    return Number.isFinite(pct) && pct > 0 && pct < 100;
  })();
  const biasOrder = [
    { key: "context_bias", label: "Context" },
    { key: "macro_bias", label: "Macro" },
    { key: "primary_bias", label: "Primary" },
    { key: "micro_bias", label: "Micro" },
    // Nano (15m) wave/entry-timing bias — measured only on real AI calls, so
    // skip decisions render it as "—" like any missing bias.
    { key: "nano_bias", label: "Nano" },
  ] as const;
  const isInitialLoading = loading && !symbols.length;
  // Mirrors the real decision-card BODY (action row, reason lines, bias grid,
  // prompt button). The bias cells copy the real cells' box model exactly —
  // same border/padding/inner sizes — so nothing jumps when data lands.
  const renderDecisionBodySkeleton = () => (
    <div className="skeleton-shimmer">
      <div className="mt-3 flex items-center gap-2">
        <div className="h-3 w-12 rounded-full bg-slate-200" />
        <div className="h-5 w-16 rounded bg-slate-200" />
        <div className="h-3 w-2/5 rounded-full bg-slate-100" />
      </div>
      <div className="mt-3 space-y-2">
        <div className="h-3 w-full rounded-full bg-slate-100" />
        <div className="h-3 w-11/12 rounded-full bg-slate-100" />
      </div>
      <div className="mt-3 grid grid-cols-5 gap-1.5 sm:gap-2">
        {Array.from({ length: 5 }).map((_, idx) => (
          <div
            key={`bias-skeleton-${idx}`}
            className="flex items-center justify-between gap-0.5 rounded-lg border border-slate-100 bg-slate-50 px-1 py-1 sm:gap-0 sm:px-3 sm:py-2"
          >
            <div className="h-2.5 w-10 rounded-full bg-slate-200" />
            <div className="h-3 w-3 rounded-full bg-slate-200 sm:h-5 sm:w-9" />
          </div>
        ))}
      </div>
      <div className="mt-3 h-7 w-28 rounded-full bg-slate-100" />
    </div>
  );

  // Full decision card skeleton: header (title + strength pill) + body.
  const renderDecisionCardSkeleton = () => (
    <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
      <div className="skeleton-shimmer flex items-center justify-between gap-3">
        <div className="h-3 w-40 rounded-full bg-slate-200" />
        <div className="h-6 w-28 rounded-full bg-slate-100" />
      </div>
      {renderDecisionBodySkeleton()}
    </div>
  );

  // Full-page loading state, shaped like the real layout: chart panel (range
  // chips + charty skeleton + timeline dots) above the decision card.
  const renderDashboardSkeleton = () => (
    <div className="grid grid-cols-1 gap-5 lg:grid-cols-2 lg:items-stretch">
      <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
        <div className="flex items-center justify-between">
          <div className="skeleton-shimmer inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 p-1">
            {Array.from({ length: 5 }).map((_, idx) => (
              <div
                key={`range-skeleton-${idx}`}
                className={`h-5 w-8 rounded-full ${idx === 1 ? "bg-slate-200" : "bg-slate-100"}`}
              />
            ))}
          </div>
          <div className="skeleton-shimmer h-3 w-36 rounded-full bg-slate-200" />
        </div>
        <div
          className="relative mt-3 h-[260px] w-full"
          style={{ minHeight: 260 }}
        >
          <ChartSkeleton />
        </div>
        <TimelineSkeleton />
      </div>
      {renderDecisionCardSkeleton()}
    </div>
  );

  const handleThemeToggle = () => {
    const nextTheme: ThemePreference =
      resolvedTheme === "dark" ? "light" : "dark";
    if (typeof window !== "undefined") {
      window.localStorage.setItem(THEME_PREFERENCE_STORAGE_KEY, nextTheme);
      window.dispatchEvent(new Event(THEME_PREFERENCE_CHANGE_EVENT));
    }
  };
  return (
    <>
      <Head>
        <title>AI Trade Dashboard</title>
        <link rel="icon" href="/favicon.svg" type="image/svg+xml" />
      </Head>
      <div
        className={`relative min-h-screen overflow-x-hidden px-0 py-6 sm:px-6 lg:px-8 ${
          resolvedTheme === "dark"
            ? "theme-dark bg-slate-950 text-slate-100"
            : "theme-light bg-slate-50 text-slate-900"
        }`}
      >
        {adminReady && !adminGranted && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-sm px-4">
            <div className="w-full max-w-md rounded-2xl border border-slate-200 bg-white p-6 shadow-2xl pointer-events-auto">
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 items-center justify-center rounded-full bg-slate-100">
                  <ShieldCheck className="h-5 w-5 text-slate-700" />
                </div>
                <div>
                  <div className="text-xs uppercase tracking-[0.2em] text-slate-500">
                    Admin Access
                  </div>
                  <h2 className="text-xl font-semibold text-slate-900">
                    Enter access secret
                  </h2>
                </div>
              </div>
              <form className="mt-5 space-y-3" onSubmit={handleAdminSubmit}>
                <input
                  type="password"
                  autoComplete="current-password"
                  autoFocus
                  value={adminInput}
                  onChange={(event) => setAdminInput(event.target.value)}
                  placeholder="ADMIN_ACCESS_SECRET"
                  className="w-full rounded-xl border border-slate-200 px-4 py-3 text-sm font-semibold text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none"
                />
                {adminError && (
                  <div className="text-sm font-semibold text-rose-600">
                    {adminError}
                  </div>
                )}
                <button
                  type="submit"
                  disabled={adminSubmitting || !adminInput.trim()}
                  className="w-full rounded-xl bg-slate-900 px-4 py-3 text-sm font-semibold text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-400"
                >
                  {adminSubmitting ? "Checking…" : "Unlock dashboard"}
                </button>
              </form>
            </div>
          </div>
        )}
        <div className="w-full">
          {/* AI outage banner (top of page, ×-dismissible): billing/config
              failures don't self-heal — decisions, in-position management
              and postmortems are all stopped until a human pays the bill /
              fixes the key. Full-strength bg-*-50 on purpose: opacity
              variants miss the .theme-dark remap. */}
          {swingAiHealth?.degraded &&
          swingAiBannerDismissedKey !==
            `${swingAiHealth.kind}:${swingAiHealth.sinceMs}` ? (
            <div
              role="alert"
              className={`mb-3 flex items-start gap-2.5 rounded-2xl border px-4 py-3 shadow-sm ${
                swingAiHealth.kind === "transient"
                  ? "border-amber-300 bg-amber-50 text-amber-800"
                  : "border-rose-300 bg-rose-50 text-rose-800"
              }`}
            >
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
              <div className="min-w-0 flex-1 text-sm">
                <span className="font-semibold">
                  {swingAiHealth.kind === "billing"
                    ? "AI unavailable — billing/quota"
                    : swingAiHealth.kind === "config"
                      ? "AI unavailable — API key/config"
                      : "AI degraded — repeated call failures"}
                  {swingAiHealth.sinceMs
                    ? ` since ${new Date(swingAiHealth.sinceMs).toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })}`
                    : ""}
                  .
                </span>{" "}
                {swingAiHealth.kind === "billing" ||
                swingAiHealth.kind === "config" ? (
                  <span>
                    Decisions, in-position management and post-mortems are
                    stopped; open positions are protected by their
                    exchange-side TP/SL bracket only.
                  </span>
                ) : null}
                {swingAiHealth.reason ? (
                  <div className="mt-0.5 truncate text-xs opacity-80" title={swingAiHealth.reason}>
                    {swingAiHealth.reason}
                  </div>
                ) : null}
              </div>
              <button
                type="button"
                aria-label="Dismiss AI outage banner"
                onClick={() =>
                  setSwingAiBannerDismissedKey(
                    `${swingAiHealth.kind}:${swingAiHealth.sinceMs}`,
                  )
                }
                className="rounded-full p-1 transition hover:bg-black/5"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          ) : null}
          <div className="relative rounded-2xl border border-slate-200 bg-white px-4 py-3 shadow-sm">
            {/* Theme: pinned inside the panel's top-right corner, OUT of the
                flow — as a flex child it stole a column's worth of width from
                every row under it, days and symbol pills alike. Only the
                calendar row keeps clearance for it; the pill row runs the full
                width. The cron kill switch used to sit here too; it is gone,
                and /api/swing/ops/cron-control owns that now. */}
            <button
              type="button"
              onClick={handleThemeToggle}
              aria-label={
                resolvedTheme === "dark" ? "Switch to light mode" : "Switch to dark mode"
              }
              title={
                resolvedTheme === "dark" ? "Switch to light mode" : "Switch to dark mode"
              }
              className="absolute right-1.5 top-3 z-10 rounded-full p-1.5 text-slate-400 transition hover:bg-slate-100 hover:text-slate-700"
            >
              {resolvedTheme === "dark" ? (
                <Sun className="h-3.5 w-3.5" />
              ) : (
                <Moon className="h-3.5 w-3.5" />
              )}
            </button>
            <div className="flex items-start">
              <div className="min-w-0 flex-1">
                {/* Top-left is where the eye lands, so it holds the one number
                    that matters: today's money. The calendar to its right is
                    that number's history — same row, so the panel keeps its
                    height. pr-4 is EXACTLY the theme icon's reach into this
                    row: the icon is 26px wide at right-1.5 (6px), and the
                    panel's own px-4 already holds 16px of that. */}
                <div className="flex flex-nowrap items-center gap-3 pr-4">
                  {todayMoney ? (
                    <div
                      className="flex shrink-0 flex-col justify-center"
                      title={todayMoney.title}
                    >
                      <span className="text-[8px] font-semibold uppercase leading-none tracking-wide text-slate-400">
                        {/* Just "Today". The open/booked split lives in the
                            tooltip: a "· live" suffix made this block wide
                            enough to cost a whole day off the strip, which is
                            a bad trade for a word most days do not need. */}
                        Today
                      </span>
                      <span
                        className={`mt-[3px] text-[15px] font-semibold leading-none tabular-nums ${
                          todayMoney.net === null
                            ? "text-slate-300"
                            : todayMoney.net >= 0
                              ? "text-emerald-600"
                              : "text-rose-600"
                        }`}
                      >
                        {todayMoney.net === null
                          ? "–"
                          : `${todayMoney.net >= 0 ? "+" : ""}${formatCash(todayMoney.net, "€")}`}
                      </span>
                    </div>
                  ) : loading && !error ? (
                    <span className="skeleton-shimmer h-[26px] w-16 shrink-0 rounded bg-slate-200" />
                  ) : null}
                  {/* Measured separately from the headline, so the day count
                      uses the space actually left for cells. */}
                  <div
                    ref={weekCalendarRef}
                    className="flex min-w-0 flex-1 flex-wrap items-center gap-x-3 gap-y-1"
                  >
                    {!swingWeekCalendar &&
                    loading &&
                    !error ? (
                      // Money-calendar skeleton — covers the initial load and the
                      // gap while a range switch refetches the summary.
                      <span className="skeleton-shimmer flex w-full items-center gap-1 overflow-hidden">
                        {Array.from({ length: calendarDayCount }, (_, i) => (
                          <span
                            key={i}
                            className="h-[18px] w-[54px] shrink-0 rounded bg-slate-200"
                          />
                        ))}
                      </span>
                    ) : null}
                    {swingWeekCalendar ? (
                      // Money calendar: per-day all-symbols net in € (USDT folded
                      // in at the EURUSD rate — tooltip carries the ≈ note), one
                      // line, cells sharing the full width out to the right edge.
                      // Today also carries the open positions' live money.
                      <div
                        className="flex w-full min-w-0 flex-nowrap items-center gap-1 overflow-hidden"
                        aria-label={`Daily net, last ${calendarDayCount} days`}
                      >
                        {swingWeekCalendar.map((cell) => (
                          <div
                            key={cell.key}
                            title={[
                              `${cell.key} · ${cell.trades} trade${cell.trades === 1 ? "" : "s"}`,
                              cell.openEur !== null
                                ? `open ${cell.openPositions} position${
                                    cell.openPositions === 1 ? "" : "s"
                                  }: ${cell.openEur >= 0 ? "+" : ""}${formatCash(cell.openEur, "€")}${
                                    cell.openLive ? " (live)" : ""
                                  } — included above, not booked yet`
                                : null,
                              cell.approximate
                                ? `includes USDT converted at EURUSD ${(eurUsdRate ?? EUR_USD_FALLBACK_RATE).toFixed(4)}`
                                : null,
                            ]
                              .filter(Boolean)
                              .join(" · ")}
                            className={`flex w-[54px] shrink-0 items-center gap-1 rounded px-1 ${
                              // Today terminates the row the way the chart's
                              // last-price label terminates its axis: a ring, not
                              // just a tint, so the right edge reads as "now"
                              // instead of as one more chip.
                              cell.isToday ? "ring-1 ring-slate-300" : ""
                            }`}
                          >
                            <span
                              className={`text-[15px] leading-none tabular-nums ${
                                cell.isToday
                                  ? "font-bold text-slate-900"
                                  : "font-semibold text-slate-700"
                              }`}
                            >
                              {cell.dayNum}
                            </span>
                            <span className="flex flex-col justify-center gap-[1px]">
                              <span className="text-[8px] font-normal uppercase leading-none tracking-wide text-slate-400">
                                {cell.weekday}
                                {cell.showMonth ? ` ${cell.month}` : ""}
                              </span>
                              <span
                                className={`text-[9px] font-semibold leading-none tabular-nums ${
                                  cell.net === null
                                    ? "text-slate-300"
                                    : cell.net >= 0
                                      ? "text-emerald-600"
                                      : "text-rose-600"
                                }`}
                              >
                                {cell.net === null
                                  ? "–"
                                  : `${cell.net >= 0 ? "+" : ""}${formatCash(cell.net, "€")}`}
                              </span>
                            </span>
                          </div>
                          ))}
                        </div>
                      ) : null}
                  </div>
                </div>
                {!error &&
                (symbols.length || isInitialLoading) ? (
                  // One always-single-line, horizontally scrollable pill row —
                  // attention-sorted (open → fresh AI → |pnl| → idle → closed),
                  // so "what's up" sits at the left edge without hunting.
                  <div
                    ref={pillRowRef}
                    className="scrollbar-none mt-1.5 flex flex-nowrap items-center gap-1 overflow-x-auto"
                  >
                    {orderedSymbolPills.map(
                      ({ sym, index, tab, marketClosed, openDirection, pnl }) => {
                        const isActive = index === active;
                        // Decision dot on the symbol segment: shown when the
                        // AI decided within the last hour (skips don't light
                        // it) or a pullback limit is resting. Color = the last
                        // AI action, using the timeline's palette; a HOLLOW
                        // ring means the order is a resting limit (not yet
                        // filled), filled means executed/hold.
                        const aiDecisionRecent = tab?.lastWasAiCall === true;
                        const pendingLimit = tab?.pendingEntry === true;
                        const lastAiAction = String(
                          tab?.lastAiDecisionAction || "",
                        ).toUpperCase();
                        const decisionDotClass =
                          lastAiAction === "BUY"
                            ? "timeline-dot-buy"
                            : lastAiAction === "SELL"
                              ? "timeline-dot-sell"
                              : lastAiAction === "CLOSE" ||
                                  lastAiAction === "REVERSE"
                                ? "timeline-dot-trim"
                                : "timeline-dot-ai";
                        // A flat HOLD that armed a cooldown gets clock hands
                        // inside the dot instead of a plain filled circle.
                        const holdCooldown =
                          lastAiAction === "HOLD" &&
                          Number(tab?.lastAiDecisionCooldownMinutes) > 0;
                        // Live quote first, summary snapshot as the floor: an
                        // open position's PnL moves with price, and the summary
                        // figure is minutes old by the time it is read.
                        const openPnlValue = openDirection
                          ? (liveOpenPnlPct(tab, livePrices[sym]?.price ?? null) ??
                            (typeof tab?.openPnl === "number" ? tab.openPnl : null))
                          : null;
                        // Split pill: neutral symbol segment + one signal
                        // segment carrying the most important number. Color
                        // lives in the segment, not the whole pill, so the row
                        // reads calmer while still scannable.
                        const containerClass = marketClosed
                          ? `border-dashed border-slate-200 grayscale ${isActive ? "opacity-70" : "opacity-45"}`
                          : isActive
                            ? "border-slate-400 shadow-sm"
                            : "border-slate-200 hover:border-slate-300";
                        const symbolSegClass = isActive
                          ? "bg-slate-100 text-slate-900"
                          : "text-slate-600 hover:text-slate-900";
                        let signalSegClass = "border-slate-100 text-slate-400";
                        let signalContent: React.ReactNode = "–";
                        if (marketClosed) {
                          signalContent = (
                            <Moon className="h-2.5 w-2.5" aria-hidden="true" />
                          );
                        } else if (openDirection) {
                          // Arrow shape = side (▲ long / ▼ short); segment tone
                          // = open PnL sign.
                          signalSegClass =
                            (openPnlValue ?? 0) >= 0
                              ? "border-emerald-200 bg-emerald-50 text-emerald-700"
                              : "border-rose-200 bg-rose-50 text-rose-700";
                          signalContent = (
                            <>
                              {typeof openPnlValue === "number"
                                ? `${openPnlValue >= 0 ? "+" : ""}${openPnlValue.toFixed(1)}%`
                                : "open"}
                              <span className="text-[9px] leading-none">
                                {openDirection === "long" ? "▲" : "▼"}
                              </span>
                            </>
                          );
                        } else if (typeof pnl === "number") {
                          // Full-strength bg-emerald-50/rose-50 (no /60): the
                          // opacity variants aren't covered by the .theme-dark
                          // remap and would render as light-mode mint/pink on
                          // the dark background.
                          signalSegClass =
                            pnl >= 0
                              ? "border-emerald-100 bg-emerald-50 text-emerald-700"
                              : "border-rose-100 bg-rose-50 text-rose-700";
                          signalContent = `${pnl >= 0 ? "+" : ""}${pnl.toFixed(1)}%`;
                        }
                        return (
                          <button
                            key={sym}
                            data-active-pill={isActive ? "true" : undefined}
                            onClick={() => {
                              userPickedSymbolRef.current = true;
                              setActive(index);
                            }}
                            title={
                              [
                                marketClosed
                                  ? `${sym} — market closed`
                                  : openDirection
                                    ? `${sym} — open ${openDirection}`
                                    : pendingLimit
                                      ? `${sym} — resting limit entry${
                                          lastAiAction === "BUY" || lastAiAction === "SELL"
                                            ? ` (${lastAiAction.toLowerCase()})`
                                            : ""
                                        }`
                                      : null,
                                // Cron liveness: quarter-tick scans don't write
                                // decision rows, so the KV last-scan marker is
                                // the only evidence the 15m cadence ran — plus
                                // which gate stopped it and why, when it skipped.
                                typeof tab?.lastScanAt === "number" && tab.lastScanAt > 0
                                  ? `last scan ${new Date(tab.lastScanAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}${
                                      tab?.lastScanStage
                                        ? ` — skipped: ${tab.lastScanReason || tab.lastScanStage}`
                                        : ""
                                    }`
                                  : null,
                              ]
                                .filter(Boolean)
                                .join(" · ") || undefined
                            }
                            className={`inline-flex shrink-0 items-stretch overflow-hidden rounded-full border text-[11px] font-semibold transition ${containerClass}`}
                          >
                            <span
                              className={`flex items-center gap-1 px-2 py-0.5 ${symbolSegClass}`}
                            >
                              {aiDecisionRecent || pendingLimit ? (
                                <span
                                  className={`pill-decision-dot h-2 w-2 shrink-0 ${
                                    holdCooldown ? "timeline-dot-clock" : ""
                                  } ${decisionDotClass} ${
                                    pendingLimit ? "pill-dot-hollow" : ""
                                  }`}
                                />
                              ) : null}
                              {sym}
                            </span>
                            <span
                              className={`flex items-center gap-0.5 border-l px-1.5 py-0.5 text-[10px] tabular-nums ${signalSegClass}`}
                            >
                              {signalContent}
                            </span>
                          </button>
                        );
                      },
                    )}
                    {isInitialLoading &&
                      Array.from({ length: 3 }).map((_, idx) => (
                        <span
                          key={`tab-skeleton-${idx}`}
                          className="skeleton-shimmer h-5 w-16 shrink-0 rounded-full border border-slate-200 bg-slate-100"
                        />
                      ))}
                  </div>
                ) : null}
              </div>
            </div>
          </div>

          {error && (
            <div className="mt-4 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm font-semibold text-rose-700">
              Could not load dashboard data: {error}
            </div>
          )}

          <div className="mt-4 pb-8">
            {isInitialLoading ? (
              renderDashboardSkeleton()
            ) : !symbols.length ? (
              <div className="flex items-center justify-center py-12 text-sm font-semibold text-slate-500">
                No evaluations found.
              </div>
            ) : current ? (
              <div className="grid grid-cols-1 gap-5 lg:grid-cols-2 lg:items-stretch">

                {showChartPanel ? (
                  <ChartPanel
                    key={activeSymbol}
                    symbol={activeSymbol}
                    platform={current?.lastPlatform || null}
                    adminSecret={resolveAdminSecret()}
                    adminGranted={adminGranted}
                    isDark={resolvedTheme === "dark"}
                    rangeKey={chartRange}
                    onRangeChange={(next) => {
                      setChartRangeChoice(next);
                      // 4H is chart-only; PnL/summary ranges stay at 1D.
                      setDashboardRange(next === "4H" ? "1D" : next);
                    }}
                    statsSlot={swingChartStats}
                    livePrice={livePriceNow}
                    liveTimestamp={livePriceTs}
                    refreshToken={chartRefreshToken}
                    onOpenPositionChange={(position) =>
                      handleChartOpenPositionChange(activeSymbol, position)
                    }
                    onPositionSummaryChange={(summary) =>
                      handleChartPositionSummaryChange(activeSymbol, summary)
                    }
                    highlightTimeMs={selectedTick ? selectedTick.ts : null}
                    timelineTicks={activeTimeline}
                    timelineLoading={
                      !!activeSymbol && !(activeSymbol in symbolTimelines)
                    }
                    selectedTimelineTs={
                      // Default ring on the newest DECISION tick — that's what
                      // the Latest Decision panel below is showing. The very
                      // newest tick is often just a scan marker.
                      selectedTick
                        ? selectedTick.ts
                        : activeDecisionTimeline.find((t) => t.hasDetails)
                            ?.ts ??
                          activeDecisionTimeline[0]?.ts ??
                          null
                    }
                    onTimelineTickSelect={(ts) => {
                      if (!activeSymbol) return;
                      const tick = activeTimeline.find(
                        (t) => t.kind !== "postmortem" && t.ts === ts,
                      );
                      if (!tick) return;
                      void handleTimelineTickSelect(
                        activeSymbol,
                        tick,
                        tick.ts === activeDecisionTimeline[0]?.ts,
                      );
                    }}
                    onTimeSelect={(tsMs) => {
                      // Chart click → nearest decision-timeline tick (analysis
                      // dots are click-only, never chart-click targets).
                      // Clicking near the newest tick returns to the live view.
                      if (!activeSymbol || !activeDecisionTimeline.length)
                        return;
                      let nearest = activeDecisionTimeline[0];
                      let bestDiff = Math.abs(nearest.ts - tsMs);
                      for (const tick of activeDecisionTimeline) {
                        const diff = Math.abs(tick.ts - tsMs);
                        if (diff < bestDiff) {
                          bestDiff = diff;
                          nearest = tick;
                        }
                      }
                      void handleTimelineTickSelect(
                        activeSymbol,
                        nearest,
                        nearest.ts === activeDecisionTimeline[0].ts,
                      );
                    }}
                  />
                ) : null}

                {hasLastDecision || activeTimeline.length > 0 ? (
                  <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
                    {/* The tick timeline lives INSIDE the chart panel
                        (time-aligned under the axis); this is the classic
                        title/time + strength header. */}
                    <div className="flex flex-wrap items-center justify-between gap-3">
                      <div className="flex items-center gap-2 text-xs uppercase tracking-wide text-slate-500">
                        <span>
                          {selectedTick ? "Decision" : "Latest Decision"}
                        </span>
                        {displayDecisionTs ? (
                          <span className="lowercase text-slate-400">
                            {formatDecisionTime(displayDecisionTs)}
                          </span>
                        ) : null}
                      </div>
                      <div className="flex items-center gap-2">
                        {displayDecision?.signal_strength && (
                          <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-semibold text-slate-700">
                            Strength: {displayDecision.signal_strength}
                          </span>
                        )}
                      </div>
                    </div>
                    {selectedTick && !selectedTick.hasDetails ? (
                      // Quarter-tick scan: never persisted as a decision row —
                      // the gate verdict travels on the tick itself.
                      <div className="mt-3 rounded-xl border border-slate-100 bg-slate-50 p-3 text-sm text-slate-700">
                        <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                          Quarter-tick scan
                        </div>
                        <div className="mt-1">
                          {selectedTick.stage ? (
                            <>
                              <span className="inline-flex rounded border border-slate-200 bg-white px-1.5 py-0.5 font-semibold text-slate-700">
                                {selectedTick.stage}
                              </span>
                              {selectedTick.reason &&
                              selectedTick.reason !== selectedTick.stage
                                ? ` · ${selectedTick.reason}`
                                : ""}
                            </>
                          ) : (
                            "Scanned — no gate skip recorded (tick proceeded or ended without a persisted verdict)."
                          )}
                        </div>
                      </div>
                    ) : selectedTickLoading ? (
                      // Full body skeleton so the bias grid & co. hold their
                      // place instead of collapsing and jumping back in.
                      renderDecisionBodySkeleton()
                    ) : selectedTick && !selectedTickDecision ? (
                      <div className="mt-3 text-sm text-slate-500">
                        Decision details unavailable (expired from the 7-day
                        history window).
                      </div>
                    ) : (
                      <>
                        <div className="mt-3 text-sm text-slate-800">
                          Action:{" "}
                          <span
                            className={`inline-flex rounded border px-1.5 py-0.5 font-semibold ${
                              displayIsTrim
                                ? "action-pill-trim"
                                : actionPillToneClass(
                                    displayDecision?.action,
                                    current.lastPositionPnl,
                                  )
                            }`}
                          >
                            {formatLastDecisionAction(displayDecision) || "—"}
                          </span>
                          {displayDecision?.summary
                            ? ` · ${displayDecision.summary}`
                            : ""}
                        </div>
                        {displayDecision?.reason ? (
                          <p className="mt-2 text-sm text-slate-700 whitespace-pre-wrap">
                            <span className="font-semibold text-slate-800">
                              Reason:{" "}
                            </span>
                            {displayDecision.reason}
                          </p>
                        ) : null}
                        <div className="mt-3 grid grid-cols-5 gap-1.5 sm:gap-2">
                          {biasOrder.map(({ key, label }) => {
                            const raw = displayDecision?.[key];
                            const val =
                              typeof raw === "string"
                                ? raw.toUpperCase()
                                : raw != null
                                  ? String(raw)
                                  : null;
                            const tfLabel =
                              displayBiasTimeframes?.[
                                key.replace("_bias", "")
                              ] || (key === "nano_bias" ? NANO_TIMEFRAME : null);
                            const displayLabel = tfLabel
                              ? `${label} (${tfLabel})`
                              : label;
                            const meta =
                              val === "UP"
                                ? { color: "text-emerald-600", Icon: ArrowUpRight }
                                : val === "DOWN"
                                  ? { color: "text-rose-600", Icon: ArrowDownRight }
                                  : { color: "text-slate-500", Icon: Circle };
                            const Icon = meta.Icon;
                            return (
                              <div
                                key={key}
                                className="flex items-center justify-between gap-0.5 rounded-lg border border-slate-100 bg-slate-50 px-1 py-1 sm:gap-0 sm:px-3 sm:py-2"
                              >
                                <span className="whitespace-nowrap text-[8px] font-semibold uppercase leading-tight tracking-tight text-slate-500 sm:text-[10px] sm:tracking-wide">
                                  {displayLabel}
                                </span>
                                <span
                                  className={`flex items-center gap-0.5 text-xs font-semibold sm:gap-1 sm:text-sm ${meta.color}`}
                                >
                                  <Icon className="h-3 w-3 sm:h-4 sm:w-4" />
                                  <span className="hidden sm:inline">
                                    {val || "—"}
                                  </span>
                                </span>
                              </div>
                            );
                          })}
                        </div>
                        <div className="mt-3 flex flex-wrap items-center gap-2">
                          <button
                            onClick={() => setShowPrompt((prev) => !prev)}
                            className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 transition hover:border-slate-300"
                          >
                            {showPrompt ? "Hide prompt" : "Show prompt"}
                          </button>
                          <button
                            onClick={() => setShowRawResponse((prev) => !prev)}
                            className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 transition hover:border-slate-300"
                          >
                            {showRawResponse ? "Hide raw response" : "Show raw response"}
                          </button>
                        </div>
                        {showPrompt && (
                          <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2">
                            <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                              <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                System
                              </div>
                              <div className="mt-2">
                                {renderPromptContent(displayPrompt?.system)}
                              </div>
                            </div>
                            <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                              <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                User
                              </div>
                              <div className="mt-2">
                                {renderPromptContent(displayPrompt?.user)}
                              </div>
                            </div>
                          </div>
                        )}
                        {showRawResponse && (
                          <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50 p-3">
                            <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                              Raw response
                            </div>
                            <pre className="mt-2 whitespace-pre-wrap break-words font-mono text-[11px] leading-relaxed text-slate-700">
                              {displayDecision
                                ? JSON.stringify(displayDecision, null, 2)
                                : "—"}
                            </pre>
                          </div>
                        )}
                      </>
                    )}
                  </div>
                ) : (
                  // Decision data for this symbol hasn't arrived yet — hold
                  // the card's shape instead of collapsing the layout.
                  renderDecisionCardSkeleton()
                )}

                <WeeklyDigestPanel
                  getAdminHeaders={buildAdminHeaders}
                  onUnauthorized={() => handleAuthExpired()}
                />
              </div>
            ) : (
              renderDashboardSkeleton()
            )}
          </div>
        </div>
      </div>
    </>
  );
}
