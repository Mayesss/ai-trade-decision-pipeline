import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Head from "next/head";
import dynamic from "next/dynamic";
import {
  AllCommunityModule,
  ModuleRegistry,
  type ColDef,
} from "ag-grid-community";
import vercelConfig from "../vercel.json";
import { inScalpEntrySessionProfileWindow } from "../lib/scalp/sessions";
import {
  Activity,
  BarChart3,
  Bot,
  BookOpen,
  CandlestickChart,
  ShieldPlus,
  Wand2,
  Circle,
  Cpu,
  Database,
  Globe2,
  ListChecks,
  Braces,
  Layers3,
  PauseCircle,
  PenTool,
  Radar,
  Repeat,
  ShieldCheck,
  Moon,
  Sun,
  TimerReset,
  Zap,
  Star,
  ArrowUpRight,
  ArrowDownRight,
  Copy,
  CheckCircle2,
  type LucideIcon,
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
  openPnl?: number | null;
  openDirection?: "long" | "short" | null;
  openLeverage?: number | null;
  openEntryPrice?: number | null;
  lastPositionPnl?: number | null;
  lastPositionDirection?: "long" | "short" | null;
  lastPositionLeverage?: number | null;
  lastDecisionTs?: number | null;
  lastDecision?: {
    action?: string;
    summary?: string;
    reason?: string;
    signal_strength?: string;
    [key: string]: any;
  } | null;
  lastPrompt?: { system?: string; user?: string } | null;
  lastMetrics?: Record<string, any> | null;
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
  openPnl?: number | null;
  openDirection?: "long" | "short" | null;
  openLeverage?: number | null;
  openEntryPrice?: number | null;
  lastPositionPnl?: number | null;
  lastPositionDirection?: "long" | "short" | null;
  lastPositionLeverage?: number | null;
  winRate?: number | null;
  avgWinPct?: number | null;
  avgLossPct?: number | null;
};

type DashboardSummaryResponse = {
  symbols: string[];
  data: DashboardSummaryRow[];
  range?: DashboardRangeKey;
};

type SwingCronControlState = {
  hardDeactivated?: boolean;
  reason?: string | null;
  updatedAtMs?: number | null;
  updatedBy?: string | null;
};

type ScalpDashboardSymbol = {
  symbol: string;
  strategyId: string;
  tuneId: string;
  deploymentId: string;
  entrySessionProfile?: ScalpEntrySessionProfileUi | string | null;
  enabled?: boolean;
  tune: string;
  cronSchedule?: string | null;
  cronRoute?: "execute-deployments" | string;
  cronPath?: string | null;
  dayKey: string;
  state: string | null;
  updatedAtMs?: number | null;
  lastRunAtMs?: number | null;
  dryRunLast?: boolean | null;
  tradesPlaced: number;
  wins: number;
  losses: number;
  inTrade: boolean;
  tradeSide: "BUY" | "SELL" | null;
  dealReference: string | null;
  reasonCodes?: string[];
  netR?: number | null;
  maxDrawdownR?: number | null;
  promotionEligible?: boolean;
  promotionReason?: string | null;
  forwardValidation?: ScalpForwardValidation | null;
};

type ScalpHistoryDiscoveryRow = {
  symbol?: string;
  candles?: number | null;
  depthDays?: number | null;
  barsPerDay?: number | null;
  coveragePct?: number | null;
  fromTsMs?: number | null;
  toTsMs?: number | null;
  updatedAtMs?: number | null;
};

type ScalpHistoryDiscoverySnapshot = {
  timeframe?: string | null;
  backend?: "file" | "kv" | "unknown" | string;
  generatedAtMs?: number | null;
  symbolCount?: number | null;
  scannedCount?: number | null;
  scannedLimit?: number | null;
  previewLimit?: number | null;
  previewCount?: number | null;
  truncated?: boolean;
  nonEmptyCount?: number | null;
  emptyCount?: number | null;
  totalCandles?: number | null;
  avgCandles?: number | null;
  medianCandles?: number | null;
  minCandles?: number | null;
  maxCandles?: number | null;
  avgDepthDays?: number | null;
  medianDepthDays?: number | null;
  minDepthDays?: number | null;
  maxDepthDays?: number | null;
  oldestCandleAtMs?: number | null;
  newestCandleAtMs?: number | null;
  rows?: ScalpHistoryDiscoveryRow[];
};

type ScalpStrategyControl = {
  strategyId?: string;
  shortName?: string;
  longName?: string;
  enabled?: boolean;
  envEnabled?: boolean;
  kvEnabled?: boolean | null;
  updatedAtMs?: number | null;
  updatedBy?: string | null;
};

type ScalpPipelineStepState =
  | "pending"
  | "running"
  | "success"
  | "failed"
  | "blocked";
type ScalpPipelineStatus =
  | "idle"
  | "running"
  | "completed"
  | "failed"
  | "blocked";

type ScalpPipelineStatusPanel = {
  status?: ScalpPipelineStatus;
  label?: string;
  detail?: string | null;
  updatedAtMs?: number | null;
  progressPct?: number | null;
  steps?: Array<{
    id?: string;
    label?: string;
    state?: ScalpPipelineStepState;
    detail?: string | null;
  }>;
};

type ScalpPipelineJobSummary = {
  jobKind?: string;
  status?: string;
  locked?: boolean;
  runningSinceAtMs?: number | null;
  runningDurationMs?: number | null;
  lastRunAtMs?: number | null;
  lastDurationMs?: number | null;
  lastSuccessAtMs?: number | null;
  nextRunAtMs?: number | null;
  lastError?: string | null;
  progressLabel?: string | null;
  progress?: Record<string, unknown> | null;
  queue?: {
    pending?: number;
    running?: number;
    retryWait?: number;
    failed?: number;
    succeeded?: number;
  } | null;
};

type ScalpSummaryDeployment = {
  deploymentId?: string;
  symbol?: string;
  strategyId?: string;
  tuneId?: string;
  entrySessionProfile?: ScalpEntrySessionProfileUi | string | null;
  source?: string;
  enabled?: boolean;
  inUniverse?: boolean | null;
  lifecycleState?:
    | "candidate"
    | "incumbent_refresh"
    | "graduated"
    | "suspended"
    | "retired"
    | null;
  promotionEligible?: boolean | null;
  promotionReason?: string | null;
  forwardValidation?: ScalpForwardValidation | null;
  promotionGate?: Record<string, any> | null;
  updatedAtMs?: number | null;
};

type ScalpSummaryWorkerRow = {
  deploymentId?: string;
  symbol?: string;
  strategyId?: string;
  tuneId?: string;
  entrySessionProfile?: ScalpEntrySessionProfileUi | string | null;
  workerId?: string | null;
  weekStartMs?: number | null;
  weekEndMs?: number | null;
  status?: string;
  attempts?: number | null;
  startedAtMs?: number | null;
  finishedAtMs?: number | null;
  durationMs?: number | null;
  errorCode?: string | null;
  errorMessage?: string | null;
  trades?: number | null;
  netR?: number | null;
  expectancyR?: number | null;
  profitFactor?: number | null;
  maxDrawdownR?: number | null;
};

type ScalpSummaryResponse = {
  mode?: "scalp";
  generatedAtMs?: number;
  range?: DashboardRangeKey;
  dayKey?: string;
  clockMode?: "LONDON_TZ" | "UTC_FIXED" | string;
  entrySessionProfile?: ScalpEntrySessionProfileUi | string;
  source?: "deployment_registry" | "cron_symbols" | string;
  strategyId?: string;
  defaultStrategyId?: string;
  strategy?: ScalpStrategyControl;
  strategies?: ScalpStrategyControl[];
  summary?: {
    symbols?: number;
    openCount?: number;
    runCount?: number;
    dryRunCount?: number;
    totalTradesPlaced?: number;
    stateCounts?: Record<string, number>;
  };
  deployments?: ScalpSummaryDeployment[];
  jobs?: ScalpPipelineJobSummary[];
  workerRows?: ScalpSummaryWorkerRow[];
  panicStop?: {
    enabled?: boolean;
    reason?: string | null;
    updatedAtMs?: number | null;
    updatedBy?: string | null;
  } | null;
  pipeline?: {
    panicStop?: {
      enabled?: boolean;
      reason?: string | null;
      updatedAtMs?: number | null;
      updatedBy?: string | null;
    } | null;
    queue?: {
      pending?: number | null;
      running?: number | null;
      outstanding?: number | null;
    } | null;
    statusPanel?: ScalpPipelineStatusPanel | null;
  } | null;
  symbols?: ScalpDashboardSymbol[];
  history?: ScalpHistoryDiscoverySnapshot;
  latestExecutionByDeploymentId?: Record<string, Record<string, any>>;
  latestExecutionBySymbol?: Record<string, Record<string, any>>;
  journal?: Array<{
    id?: string;
    timestampMs?: number;
    type?: string;
    level?: "info" | "warn" | "error" | string;
    symbol?: string | null;
    dayKey?: string | null;
    reasonCodes?: string[];
    payload?: Record<string, any>;
  }>;
  researchSummary?: {
    totalCandidates: number;
    stageCPass: number;
    stageCFail: number;
    stageBPass: number;
    stageAPass: number;
    uniqueSymbols: number;
    uniqueSessions: string[];
    avgNetR: number | null;
    avgExpR: number | null;
  } | null;
  researchCursors?: Array<{
    cursorKey?: string;
    venue?: string;
    symbol?: string;
    entrySessionProfile?: string;
    phase?: string;
    lastCandidateOffset?: number;
    progress?: Record<string, unknown>;
    updatedAtMs?: number;
  }>;
  researchHighlights?: Array<{
    id?: number;
    candidateId?: string;
    venue?: string;
    symbol?: string;
    entrySessionProfile?: string;
    score?: number;
    trades12w?: number;
    winningWeeks12w?: number;
    consecutiveWinningWeeks?: number;
    remarkable?: boolean;
    createdAtMs?: number;
  }>;
};

type ScalpJournalRow = NonNullable<ScalpSummaryResponse["journal"]>[number];

type ScalpForwardValidation = {
  rollCount?: number;
  profitableWindowPct?: number;
  meanExpectancyR?: number;
  meanProfitFactor?: number | null;
  maxDrawdownR?: number | null;
  minTradesPerWindow?: number | null;
  selectionWindowDays?: number | null;
  forwardWindowDays?: number | null;
  weeklySlices?: number | null;
  weeklyProfitablePct?: number | null;
  weeklyMeanExpectancyR?: number | null;
  weeklyMedianExpectancyR?: number | null;
  weeklyWorstNetR?: number | null;
  weeklyTopWeekPnlConcentrationPct?: number | null;
  weeklyEvaluatedAtMs?: number | null;
  confirmationWindowDays?: number | null;
  confirmationForwardWindowDays?: number | null;
  confirmationRollCount?: number | null;
  confirmationProfitableWindowPct?: number | null;
  confirmationMeanExpectancyR?: number | null;
  confirmationMeanProfitFactor?: number | null;
  confirmationMaxDrawdownR?: number | null;
  confirmationMinTradesPerWindow?: number | null;
  confirmationTotalTrades?: number | null;
  confirmationEvaluatedAtMs?: number | null;
};

type ScalpWorkerTaskResult = {
  windowFromTs?: number;
  windowToTs?: number;
  tuneId?: string;
  trades?: number | null;
  netR?: number | null;
  expectancyR?: number | null;
  profitFactor?: number | null;
  maxDrawdownR?: number | null;
};

type ScalpWorkerTask = {
  taskId?: string;
  symbol?: string;
  strategyId?: string;
  tuneId?: string;
  entrySessionProfile?: ScalpEntrySessionProfileUi | null;
  deploymentId?: string | null;
  workerId?: string | null;
  windowFromTs?: number;
  windowToTs?: number;
  startedAtMs?: number | null;
  finishedAtMs?: number | null;
  status?: "pending" | "running" | "completed" | "failed" | string;
  configOverride?: Record<string, unknown> | null;
  deployed?: boolean | null;
  deploymentEnabled?: boolean | null;
  promotionEligible?: boolean | null;
  promotionReason?: string | null;
  errorCode?: string | null;
  errorMessage?: string | null;
  result?: ScalpWorkerTaskResult | null;
};

type ScalpOpsDeploymentRow = {
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpEntrySessionProfileUi | null;
  source: string;
  enabled: boolean;
  inUniverse: boolean | null;
  lifecycleState:
    | "candidate"
    | "incumbent_refresh"
    | "graduated"
    | "suspended"
    | "retired"
    | null;
  promotionEligible: boolean;
  promotionReason: string | null;
  forwardValidation: ScalpForwardValidation | null;
  perf30dTrades: number | null;
  perf30dExpectancyR: number | null;
  perf30dNetR: number | null;
  perf30dMaxDrawdownR: number | null;
  runtime: ScalpDashboardSymbol | null;
  /** Raw v2 promotion gate — carries stage-C backtest metrics. */
  promotionGate: Record<string, any> | null;
};

type ScalpOpsCronStatus = "healthy" | "lagging" | "unknown";
type ScalpOpsCronDetailTone = "neutral" | "positive" | "warning" | "critical";
type ScalpOpsCronDetail = {
  label: string;
  value: string;
  tone?: ScalpOpsCronDetailTone;
};
type ScalpOpsCronVisualMetric = {
  label: string;
  valueLabel: string;
  pct: number | null;
  tone?: ScalpOpsCronDetailTone;
};
type ScalpOpsCronRow = {
  id: string;
  cadence: string;
  cronExpression: string | null;
  nextRunAtMs: number | null;
  invokePath: string | null;
  role: string;
  status: ScalpOpsCronStatus;
  lastRunAtMs: number | null;
  lastDurationMs: number | null;
  details: ScalpOpsCronDetail[];
  visualMetrics?: ScalpOpsCronVisualMetric[];
  resultPreview?: Record<string, unknown> | null;
};

type ScalpWorkerSortKey =
  | "symbol"
  | "strategyId"
  | "tuneId"
  | "whyNotPromoted"
  | "windowToTs"
  | "status"
  | "trades"
  | "netR"
  | "expectancyR"
  | "profitFactor"
  | "maxDrawdownR";

type ScalpWorkerSortDirection = "asc" | "desc";

type ScalpWorkerSortState = {
  key: ScalpWorkerSortKey;
  direction: ScalpWorkerSortDirection;
};

type ScalpWorkerJobGridRow = {
  rowId: string;
  deploymentId: string | null;
  entrySessionProfile?: ScalpEntrySessionProfileUi | null;
  workerOnly?: boolean;
  symbol: string;
  strategyId: string;
  tuneId: string;
  inUniverse?: boolean | null;
  lifecycleState?:
    | "candidate"
    | "incumbent_refresh"
    | "graduated"
    | "suspended"
    | "retired"
    | null;
  forwardValidation: ScalpForwardValidation | null;
  deployed: boolean;
  deploymentEnabled: boolean | null;
  promotionEligible: boolean | null;
  reason: string;
  status: string;
  windowCount: number;
  windowsResults: string;
  windowNetRs: Array<{
    sortTs: number | null;
    value: number | null;
    display: string;
    tooltip: string;
  }>;
  trades: number | null;
  netR: number | null;
  totalNetR: number | null;
  expectancyR: number | null;
  profitFactor: number | null;
  maxDrawdownR: number | null;
  totalMaxDrawdownR: number | null;
  maxWeeklyNetR: number | null;
  largestTradeR: number | null;
  exitReasons: { stop: number; tp: number; timeStop: number; forceClose: number } | null;
  errorCodes: string | null;
};

type ScalpUniversePipelineRow = {
  symbol: string;
  discovered: boolean;
  importStatus: "seeded" | "skipped" | "failed" | "not_run";
  importReason: string | null;
  importAddedCount: number | null;
  evaluated: boolean;
  eligible: boolean | null;
  score: number | null;
  reasons: string[];
};

type DashboardDecisionResponse = {
  symbol: string;
  category?: string | null;
  platform?: string | null;
  lastDecisionTs?: number | null;
  lastDecision?: EvaluationEntry["lastDecision"];
  lastPrompt?: { system?: string; user?: string } | null;
  lastMetrics?: Record<string, any> | null;
  lastBiasTimeframes?: Record<string, string | undefined> | null;
  lastNewsSource?: string | null;
};

type DashboardEvaluationResponse = {
  symbol: string;
  evaluation: Evaluation;
  evaluationTs?: number | null;
};

type EvaluateJobStatus = "queued" | "running" | "succeeded" | "failed";

type EvaluateJobRecord = {
  id: string;
  status: EvaluateJobStatus;
  updatedAt?: number;
  error?: string;
};

type DashboardRangeKey = "7D" | "30D" | "6M";
type ThemePreference = "system" | "light" | "dark";
type ResolvedTheme = "light" | "dark";
type StrategyMode = "swing" | "scalp";
type ScalpEntrySessionProfileUi =
  | "berlin"
  | "tokyo"
  | "newyork"
  | "sydney";

const CURRENCY_SYMBOL = "₮"; // Tether-style symbol
const THEME_PREFERENCE_STORAGE_KEY = "dashboard_theme_preference";
const formatUsd = (value: number) => {
  const abs = Math.abs(value);
  const sign = value < 0 ? "-" : "";
  const v = Math.abs(value);
  if (abs >= 1_000_000)
    return `${sign}${CURRENCY_SYMBOL}${(v / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000)
    return `${sign}${CURRENCY_SYMBOL}${(v / 1_000).toFixed(1)}K`;
  return `${sign}${CURRENCY_SYMBOL}${v.toFixed(0)}`;
};
const formatSignedR = (value: number): string =>
  `${value >= 0 ? "+" : ""}${value.toFixed(2)}R`;

const BERLIN_TZ = "Europe/Berlin";
const BITGET_PUBLIC_WS_URL = "wss://ws.bitget.com/v2/ws/public";
const WS_RECONNECT_MS = 1500;
const WS_PING_MS = 25_000;
const CAPITAL_LIVE_POLL_MS = 3000;
const SCALP_LIVE_POLL_VISIBLE_MS = 10_000;
const SCALP_LIVE_POLL_HIDDEN_MS = 60_000;
const SCALP_LIVE_POLL_ERROR_BACKOFF_MS = 120_000;
const SCALP_MIN_REFRESH_GAP_MS = 8_000;
const SCALP_WORKER_TASK_LIMIT_FULL = 5_000;
const SCALP_GRID_LOAD_BATCH = 60;
type ScalpVenueUi = "capital" | "bitget";
const SCALP_VENUE_ICON_SRC: Record<ScalpVenueUi, string> = {
  capital: "/capital.svg",
  bitget: "/bitget.svg",
};

function resolveScalpVenueUiFromDeploymentId(
  deploymentId: string | null | undefined,
): ScalpVenueUi {
  const raw = String(deploymentId || "")
    .trim()
    .toLowerCase();
  if (raw.startsWith("bitget:")) return "bitget";
  if (raw.startsWith("capital:")) return "capital";
  // Infer from symbol: crypto pairs (ending in USDT/BUSD/BTC) are Bitget
  const symbolPart = raw.split("~")[0] || raw.split(":")[0] || "";
  if (/usdt|busd|btc$/i.test(symbolPart)) return "bitget";
  return "capital";
}

function stripScalpVenuePrefixFromDeploymentId(
  deploymentId: string | null | undefined,
): string {
  const raw = String(deploymentId || "").trim();
  if (!raw) return "";
  const withoutVenue = raw.replace(/^(bitget|capital):/i, "").trim();
  const sessionMatch = withoutVenue.match(/__sp_([a-z0-9_-]+)$/i);
  if (!sessionMatch) return withoutVenue;
  const session = String(sessionMatch[1] || "")
    .trim()
    .toLowerCase();
  const base = withoutVenue.slice(0, sessionMatch.index).trim();
  if (!session) return base || withoutVenue;
  return base ? `${base} · ${session}` : session;
}

function normalizeScalpEntrySessionProfileUi(
  value: unknown,
): ScalpEntrySessionProfileUi | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (
    normalized === "berlin" ||
    normalized === "tokyo" ||
    normalized === "newyork" ||
    normalized === "sydney"
  ) {
    return normalized;
  }
  return null;
}

function extractScalpEntrySessionProfileFromDeploymentId(
  deploymentId: string | null | undefined,
): ScalpEntrySessionProfileUi | null {
  const raw = String(deploymentId || "").trim();
  if (!raw) return null;
  const withoutVenue = raw.replace(/^(bitget|capital):/i, "").trim();
  const sessionMatch = withoutVenue.match(/__sp_([a-z0-9_-]+)$/i);
  if (!sessionMatch) return null;
  return normalizeScalpEntrySessionProfileUi(sessionMatch[1]);
}

function buildScalpCandidateSessionKey(params: {
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpEntrySessionProfileUi | null;
}): string {
  const symbol = String(params.symbol || "")
    .trim()
    .toUpperCase();
  const strategyId = String(params.strategyId || "")
    .trim()
    .toLowerCase();
  const tuneId = String(params.tuneId || "")
    .trim()
    .toLowerCase();
  const session =
    normalizeScalpEntrySessionProfileUi(params.entrySessionProfile) || "unknown";
  return `${symbol}~${strategyId}~${tuneId}~${session}`;
}

const ADMIN_SECRET_STORAGE_KEY = "admin_access_secret";
const ADMIN_AUTH_TIMEOUT_MS = 4000;
const STRATEGY_MODE_STORAGE_KEY = "strategy_mode";
const SCALP_ENTRY_SESSION_STORAGE_KEY = "scalp_entry_session_profile";
type ScalpSessionTimelineColorMeta = {
  id: ScalpEntrySessionProfileUi;
  label: string;
  lightFill: string;
  darkFill: string;
  lightBorder: string;
  darkBorder: string;
};
const SCALP_SESSION_TIMELINE_COLORS: ScalpSessionTimelineColorMeta[] = [
  {
    id: "tokyo",
    label: "Tokyo",
    lightFill: "rgba(14, 165, 233, 0.35)",
    darkFill: "rgba(56, 189, 248, 0.28)",
    lightBorder: "rgba(2, 132, 199, 0.72)",
    darkBorder: "rgba(56, 189, 248, 0.62)",
  },
  {
    id: "berlin",
    label: "Berlin",
    lightFill: "rgba(16, 185, 129, 0.3)",
    darkFill: "rgba(52, 211, 153, 0.24)",
    lightBorder: "rgba(5, 150, 105, 0.7)",
    darkBorder: "rgba(52, 211, 153, 0.58)",
  },
  {
    id: "newyork",
    label: "New York",
    lightFill: "rgba(244, 63, 94, 0.28)",
    darkFill: "rgba(251, 113, 133, 0.24)",
    lightBorder: "rgba(225, 29, 72, 0.7)",
    darkBorder: "rgba(251, 113, 133, 0.56)",
  },
  {
    id: "sydney",
    label: "Sydney",
    lightFill: "rgba(249, 115, 22, 0.28)",
    darkFill: "rgba(251, 146, 60, 0.24)",
    lightBorder: "rgba(234, 88, 12, 0.7)",
    darkBorder: "rgba(251, 146, 60, 0.56)",
  },
];
const SCALP_SESSION_TIMELINE_TICK_MINUTES = [0, 360, 720, 1080, 1440];

function asPlainObject(value: unknown): Record<string, any> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, any>;
}

function asFiniteOrNull(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function asBoolOrNull(value: unknown): boolean | null {
  if (typeof value === "boolean") return value;
  return null;
}

function toDayKeyFromMs(tsMs: number): string {
  return new Date(tsMs).toISOString().slice(0, 10);
}

function toUiScalpSummaryFromV2(
  payloadRaw: unknown,
  opts: { range: DashboardRangeKey; session: ScalpEntrySessionProfileUi },
): ScalpSummaryResponse {
  const payload = asPlainObject(payloadRaw);
  const runtime = asPlainObject(payload.runtime);
  const summary = asPlainObject(payload.summary);
  const deploymentsRaw = Array.isArray(payload.deployments) ? payload.deployments : [];
  const eventsRaw = Array.isArray(payload.events) ? payload.events : [];
  const ledgerRaw = Array.isArray(payload.ledger) ? payload.ledger : [];
  const researchCursorsRaw = Array.isArray(payload.researchCursors) ? payload.researchCursors : [];
  const researchHighlightsRaw = Array.isArray(payload.researchHighlights) ? payload.researchHighlights : [];
  const jobsRaw = Array.isArray(payload.jobs) ? payload.jobs : [];
  const candidatesRaw = Array.isArray(payload.candidates) ? payload.candidates : [];
  const generatedAtMs = asFiniteOrNull(summary.generatedAtMs) ?? Date.now();
  const dayKey = toDayKeyFromMs(generatedAtMs);

  const ledgerByDeployment = new Map<
    string,
    { trades: number; wins: number; losses: number; netR: number }
  >();
  for (const rowRaw of ledgerRaw) {
    const row = asPlainObject(rowRaw);
    const deploymentId = String(row.deploymentId || "").trim();
    if (!deploymentId) continue;
    const rMultiple = asFiniteOrNull(row.rMultiple) ?? 0;
    const agg = ledgerByDeployment.get(deploymentId) || {
      trades: 0,
      wins: 0,
      losses: 0,
      netR: 0,
    };
    agg.trades += 1;
    if (rMultiple > 0) agg.wins += 1;
    else if (rMultiple < 0) agg.losses += 1;
    agg.netR += rMultiple;
    ledgerByDeployment.set(deploymentId, agg);
  }

  const latestByDeployment = new Map<string, Record<string, any>>();
  const latestExecutionByDeploymentId: Record<string, Record<string, any>> = {};
  const latestExecutionBySymbol: Record<string, Record<string, any>> = {};
  const journal: NonNullable<ScalpSummaryResponse["journal"]> = [];

  for (const eventRaw of eventsRaw) {
    const event = asPlainObject(eventRaw);
    const deploymentId = String(event.deploymentId || "").trim();
    const symbol = String(event.symbol || "")
      .trim()
      .toUpperCase();
    const tsMs = asFiniteOrNull(event.tsMs) ?? Date.now();
    const eventType = String(event.eventType || "")
      .trim()
      .toLowerCase();
    const reasonCodes = Array.isArray(event.reasonCodes)
      ? event.reasonCodes
          .map((code) => String(code || "").trim())
          .filter((code) => code.length > 0)
      : [];
    const rawPayload = asPlainObject(event.rawPayload);
    const executionPayload = {
      ...rawPayload,
      timestampMs: tsMs,
      eventType,
      reasonCodes,
      sourceOfTruth: String(event.sourceOfTruth || "").trim().toLowerCase(),
    };

    if (deploymentId && !latestByDeployment.has(deploymentId)) {
      latestByDeployment.set(deploymentId, executionPayload);
      latestExecutionByDeploymentId[deploymentId] = executionPayload;
    }
    if (symbol && !latestExecutionBySymbol[symbol]) {
      latestExecutionBySymbol[symbol] = executionPayload;
    }

    const level =
      eventType === "order_rejected" || eventType === "liquidation"
        ? "error"
        : eventType === "stop_loss" || eventType === "reconcile_close"
          ? "warn"
          : "info";
    journal.push({
      id: String(event.id || ""),
      timestampMs: tsMs,
      type: eventType || "event",
      level,
      symbol: symbol || null,
      reasonCodes,
      payload: executionPayload,
    });
  }

  const symbols: ScalpDashboardSymbol[] = deploymentsRaw.map((deploymentRaw) => {
    const deployment = asPlainObject(deploymentRaw);
    const deploymentId = String(deployment.deploymentId || "").trim();
    const symbol = String(deployment.symbol || "")
      .trim()
      .toUpperCase();
    const strategyId = String(deployment.strategyId || "")
      .trim()
      .toLowerCase();
    const tuneId = String(deployment.tuneId || "")
      .trim()
      .toLowerCase();
    const enabled = Boolean(deployment.enabled);
    const entrySessionProfile = normalizeScalpEntrySessionProfileUi(
      deployment.entrySessionProfile,
    );
    const promotionGate = asPlainObject(deployment.promotionGate);
    const promotionEligible =
      typeof promotionGate.eligible === "boolean" ? promotionGate.eligible : enabled;
    const promotionReason =
      String(promotionGate.reason || "").trim() ||
      (enabled ? "enabled" : "shadow");
    const latest = latestByDeployment.get(deploymentId) || {};
    const latestState = asPlainObject(asPlainObject(latest).state);
    const latestTrade = asPlainObject(latestState.trade);
    const sideRaw = String(latestTrade.side || latestState.side || "")
      .trim()
      .toUpperCase();
    const tradeSide: "BUY" | "SELL" | null =
      sideRaw === "BUY" || sideRaw === "SELL" ? sideRaw : null;
    const inTrade =
      asBoolOrNull(latestState.inTrade) === true ||
      (String(latestTrade.dealReference || "").trim().length > 0 &&
        tradeSide !== null);
    const stats = ledgerByDeployment.get(deploymentId) || null;
    return {
      symbol,
      strategyId,
      tuneId,
      deploymentId,
      entrySessionProfile,
      enabled,
      tune: tuneId || "default",
      cronSchedule: null,
      cronRoute: "execute-deployments",
      cronPath: "/api/scalp/v2/cron/execute?dryRun=false",
      dayKey,
      state:
        String(latestState.state || "").trim() ||
        String(asPlainObject(latest).eventType || "").trim() ||
        null,
      updatedAtMs: asFiniteOrNull(deployment.updatedAtMs),
      lastRunAtMs: asFiniteOrNull(asPlainObject(latest).timestampMs),
      dryRunLast: asBoolOrNull(asPlainObject(latest).dryRun),
      tradesPlaced: stats?.trades || 0,
      wins: stats?.wins || 0,
      losses: stats?.losses || 0,
      inTrade,
      tradeSide,
      dealReference:
        String(latestTrade.dealReference || "").trim() ||
        String(asPlainObject(latest).brokerRef || "").trim() ||
        null,
      reasonCodes: Array.isArray(asPlainObject(latest).reasonCodes)
        ? asPlainObject(latest).reasonCodes
        : [],
      netR: stats ? stats.netR : null,
      maxDrawdownR: null,
      promotionEligible,
      promotionReason,
      forwardValidation: null,
    };
  });

  const stateCounts = symbols.reduce<Record<string, number>>((acc, row) => {
    const state = String(row.state || "idle")
      .trim()
      .toLowerCase();
    if (!state) return acc;
    acc[state] = (acc[state] || 0) + 1;
    return acc;
  }, {});
  const totalTradesPlaced = symbols.reduce(
    (acc, row) => acc + Math.max(0, Math.floor(Number(row.tradesPlaced || 0))),
    0,
  );
  const openCount = symbols.filter((row) => row.inTrade).length;

  const deployments: ScalpSummaryDeployment[] = deploymentsRaw.map((deploymentRaw) => {
    const row = asPlainObject(deploymentRaw);
    const enabled = Boolean(row.enabled);
    const promotionGate = asPlainObject(row.promotionGate);
    const lifecycle = asPlainObject(promotionGate.lifecycle);
    const promotionEligible =
      typeof promotionGate.eligible === "boolean" ? promotionGate.eligible : enabled;
    const promotionReason =
      String(promotionGate.reason || "").trim() ||
      (enabled ? "enabled" : "not_promoted");
    const lifecycleStateRaw = String(lifecycle.state || "").trim().toLowerCase();
    const lifecycleState =
      lifecycleStateRaw === "graduated" || lifecycleStateRaw === "suspended" || lifecycleStateRaw === "retired"
        ? lifecycleStateRaw
        : enabled ? "graduated" : "candidate";
    return {
      deploymentId: String(row.deploymentId || "").trim(),
      symbol: String(row.symbol || "")
        .trim()
        .toUpperCase(),
      strategyId: String(row.strategyId || "")
        .trim()
        .toLowerCase(),
      tuneId: String(row.tuneId || "")
        .trim()
        .toLowerCase(),
      entrySessionProfile: normalizeScalpEntrySessionProfileUi(
        row.entrySessionProfile,
      ),
      source: "scalp_v2",
      enabled,
      inUniverse: true,
      lifecycleState,
      promotionEligible,
      promotionReason,
      promotionGate: Object.keys(promotionGate).length ? promotionGate : null,
      forwardValidation: null,
      updatedAtMs: asFiniteOrNull(row.updatedAtMs),
    };
  });

  const jobs: ScalpPipelineJobSummary[] = (
    jobsRaw.length
      ? jobsRaw
      : [
          { jobKind: "research" },
          { jobKind: "promote" },
          { jobKind: "execute" },
          { jobKind: "reconcile" },
          { jobKind: "cycle" },
        ]
  ).map((jobRaw) => {
    const row = asPlainObject(jobRaw);
    const status = String(row.status || "pending")
      .trim()
      .toLowerCase();
    const updatedAtMs = asFiniteOrNull(row.updatedAtMs);
    const payload = asPlainObject(row.payload);
    return {
      jobKind: String(row.jobKind || "").trim().toLowerCase(),
      status,
      locked: status === "running" || String(row.lockedBy || "").trim().length > 0,
      runningSinceAtMs: asFiniteOrNull(row.lockedAtMs),
      runningDurationMs: null,
      lastRunAtMs: updatedAtMs,
      lastDurationMs: null,
      lastSuccessAtMs: status === "succeeded" ? updatedAtMs : null,
      nextRunAtMs: asFiniteOrNull(row.nextRunAtMs),
      lastError: String(payload.error || payload.message || "").trim() || null,
      progressLabel: null,
      progress: payload,
      queue: {
        pending: status === "pending" ? 1 : 0,
        running: status === "running" ? 1 : 0,
        retryWait: 0,
        failed: status === "failed" ? 1 : 0,
        succeeded: status === "succeeded" ? 1 : 0,
      },
    };
  });

  const deploymentIdByCandidateKey = new Map<string, string>();
  for (const deploymentRaw of deploymentsRaw) {
    const row = asPlainObject(deploymentRaw);
    const deploymentId = String(row.deploymentId || "").trim();
    const symbol = String(row.symbol || "")
      .trim()
      .toUpperCase();
    const strategyId = String(row.strategyId || "")
      .trim()
      .toLowerCase();
    const tuneId = String(row.tuneId || "")
      .trim()
      .toLowerCase();
    const entrySessionProfile = normalizeScalpEntrySessionProfileUi(
      row.entrySessionProfile,
    );
    if (!deploymentId || !symbol || !strategyId || !tuneId || !entrySessionProfile)
      continue;
    deploymentIdByCandidateKey.set(
      buildScalpCandidateSessionKey({
        symbol,
        strategyId,
        tuneId,
        entrySessionProfile,
      }),
      deploymentId,
    );
  }

  const selectedSession = String(opts.session || "")
    .trim()
    .toLowerCase();
  const workerRows = candidatesRaw
    .filter((candidateRaw) => {
      if (!selectedSession) return true;
      const candidate = asPlainObject(candidateRaw);
      const candidateSession = String(candidate.entrySessionProfile || "")
        .trim()
        .toLowerCase();
      return !candidateSession || candidateSession === selectedSession;
    })
    .flatMap((candidateRaw) => {
      const candidate = asPlainObject(candidateRaw);
      const metadata = asPlainObject(candidate.metadata);
      const worker = asPlainObject(metadata.worker);
      if (!Object.keys(worker).length) return [];

      const evaluatedAtMs =
        asFiniteOrNull(worker.evaluatedAtMs) ??
        asFiniteOrNull(candidate.updatedAtMs) ??
        Date.now();

      const symbol = String(candidate.symbol || "")
        .trim()
        .toUpperCase();
      const strategyId = String(candidate.strategyId || "")
        .trim()
        .toLowerCase();
      const tuneId = String(candidate.tuneId || "")
        .trim()
        .toLowerCase();
      const entrySessionProfile = normalizeScalpEntrySessionProfileUi(
        candidate.entrySessionProfile,
      );
      const venue = String(candidate.venue || "").trim().toLowerCase();
      const deploymentId =
        deploymentIdByCandidateKey.get(
          buildScalpCandidateSessionKey({
            symbol,
            strategyId,
            tuneId,
            entrySessionProfile,
          }),
        ) || (venue && symbol && strategyId && tuneId && entrySessionProfile
          ? `${venue}:${symbol}~${strategyId}~${tuneId}__sp_${entrySessionProfile}`
          : "");

      // Emit one row per week from the longest executed stage's weeklyNetR.
      // This gives the grid individual weekly sticks instead of one per stage.
      const ONE_WEEK = 7 * 24 * 60 * 60 * 1000;
      const stageC = asPlainObject(worker.stageC);
      const stageB = asPlainObject(worker.stageB);
      const stageA = asPlainObject(worker.stageA);
      const finalPass = worker.finalPass === true || stageC.passed === true;

      // Pick the longest executed stage for the weekly breakdown.
      // Prefer the stage with weeklyNetR data — weekly bars are more
      // informative than per-stage aggregates.
      const bestStageWithWeekly = (
        [stageC, stageB, stageA] as Record<string, unknown>[]
      ).find(
        (s) =>
          s.executed &&
          Object.keys(asPlainObject(s.weeklyNetR)).length > 0,
      );
      const primaryStage = bestStageWithWeekly || (stageC.executed ? stageC : stageB.executed ? stageB : stageA);
      const weeklyNetR = asPlainObject(primaryStage.weeklyNetR);
      const stageFromTs = asFiniteOrNull(primaryStage.fromTs);
      const stageToTs = asFiniteOrNull(primaryStage.toTs);

      const weekKeys = Object.keys(weeklyNetR)
        .map((k) => Number(k))
        .filter((v) => Number.isFinite(v))
        .sort((a, b) => a - b);

      // If weeklyNetR is missing (old data), fall back to one row per stage
      if (!weekKeys.length) {
        const stages = [
          { id: "a", data: stageA },
          { id: "b", data: stageB },
          { id: "c", data: stageC },
        ] as const;
        const fallbackRows: ScalpSummaryWorkerRow[] = [];
        for (const stage of stages) {
          if (!stage.data.executed) continue;
          const fromTs = asFiniteOrNull(stage.data.fromTs);
          const toTs = asFiniteOrNull(stage.data.toTs);
          if (fromTs === null || toTs === null) continue;
          const passed = stage.data.passed === true;
          fallbackRows.push({
            deploymentId,
            symbol,
            strategyId,
            tuneId,
            entrySessionProfile:
              normalizeScalpEntrySessionProfileUi(entrySessionProfile),
            workerId: `v2_research_stage_${stage.id}`,
            weekStartMs: fromTs,
            weekEndMs: toTs,
            status: passed ? "succeeded" : "failed",
            attempts: 1,
            startedAtMs: evaluatedAtMs,
            finishedAtMs: evaluatedAtMs,
            durationMs: 0,
            errorCode: passed
              ? null
              : String(stage.data.reason || "").trim() || `stage_${stage.id}_failed`,
            errorMessage: passed
              ? null
              : String(stage.data.reason || "").trim() || null,
            trades: asFiniteOrNull(stage.data.trades),
            netR: asFiniteOrNull(stage.data.netR),
            expectancyR: asFiniteOrNull(stage.data.expectancyR),
            profitFactor: asFiniteOrNull(stage.data.profitFactor),
            maxDrawdownR: asFiniteOrNull(stage.data.maxDrawdownR),
          });
        }
        return fallbackRows;
      }

      // Distribute aggregate stage trades evenly across weeks for display
      const stageTrades = asFiniteOrNull(primaryStage.trades);
      const tradesPerWeek = stageTrades !== null && weekKeys.length > 0
        ? Math.round(stageTrades / weekKeys.length)
        : null;

      const rows: ScalpSummaryWorkerRow[] = [];
      for (const weekStart of weekKeys) {
        const netR = Number(weeklyNetR[String(weekStart)] || 0);
        const weekEnd = weekStart + ONE_WEEK;
        rows.push({
          deploymentId,
          symbol,
          strategyId,
          tuneId,
          entrySessionProfile:
            normalizeScalpEntrySessionProfileUi(entrySessionProfile),
          workerId: `v2_research_week`,
          weekStartMs: weekStart,
          weekEndMs: weekEnd,
          status: finalPass ? "succeeded" : "failed",
          attempts: 1,
          startedAtMs: evaluatedAtMs,
          finishedAtMs: evaluatedAtMs,
          durationMs: 0,
          errorCode: null,
          errorMessage: null,
          trades: tradesPerWeek,
          netR,
          expectancyR: null,
          profitFactor: null,
          maxDrawdownR: null,
        });
      }
      return rows;
    })
    .sort((a, b) => {
      const aTs = asFiniteOrNull(a.finishedAtMs) ?? asFiniteOrNull(a.weekEndMs) ?? 0;
      const bTs = asFiniteOrNull(b.finishedAtMs) ?? asFiniteOrNull(b.weekEndMs) ?? 0;
      if (bTs !== aTs) return bTs - aTs;
      const aSymbol = String(a.symbol || "");
      const bSymbol = String(b.symbol || "");
      if (aSymbol !== bSymbol) return aSymbol.localeCompare(bSymbol);
      const aStrategy = String(a.strategyId || "");
      const bStrategy = String(b.strategyId || "");
      if (aStrategy !== bStrategy) return aStrategy.localeCompare(bStrategy);
      return String(a.tuneId || "").localeCompare(String(b.tuneId || ""));
    }) as ScalpSummaryWorkerRow[];

  // Research summary from candidates
  const researchSummary = (() => {
    if (!candidatesRaw.length) return null;
    let totalCandidates = 0;
    let stageCPass = 0;
    let stageCFail = 0;
    let stageBPass = 0;
    let stageAPass = 0;
    let netRSum = 0;
    let netRCount = 0;
    let expSum = 0;
    let expCount = 0;
    const symbolSet = new Set<string>();
    const sessionSet = new Set<string>();
    for (const raw of candidatesRaw) {
      const c = asPlainObject(raw);
      const worker = asPlainObject(asPlainObject(c.metadata).worker);
      if (!Object.keys(worker).length) continue;
      totalCandidates += 1;
      const sym = String(c.symbol || "").trim().toUpperCase();
      const sess = String(c.entrySessionProfile || "").trim().toLowerCase();
      if (sym) symbolSet.add(sym);
      if (sess) sessionSet.add(sess);
      const sA = asPlainObject(worker.stageA);
      const sB = asPlainObject(worker.stageB);
      const sC = asPlainObject(worker.stageC);
      if (sA.passed === true) stageAPass += 1;
      if (sB.passed === true) stageBPass += 1;
      if (sC.passed === true) stageCPass += 1;
      else if (sC.executed === true) stageCFail += 1;
      const nr = asFiniteOrNull(sC.netR) ?? asFiniteOrNull(sB.netR) ?? asFiniteOrNull(sA.netR);
      if (nr !== null) { netRSum += nr; netRCount += 1; }
      const er = asFiniteOrNull(sC.expectancyR) ?? asFiniteOrNull(sB.expectancyR) ?? asFiniteOrNull(sA.expectancyR);
      if (er !== null) { expSum += er; expCount += 1; }
    }
    return {
      totalCandidates,
      stageCPass,
      stageCFail,
      stageBPass,
      stageAPass,
      uniqueSymbols: symbolSet.size,
      uniqueSessions: Array.from(sessionSet).sort(),
      avgNetR: netRCount > 0 ? netRSum / netRCount : null,
      avgExpR: expCount > 0 ? expSum / expCount : null,
    };
  })();

  const panicStopRaw = asPlainObject(runtime.panicStop);
  const panicStopEnabled =
    panicStopRaw.enabled === true ||
    runtime.enabled === false ||
    runtime.liveEnabled === false;
  const panicStopReason =
    String(panicStopRaw.reason || "").trim() ||
    (runtime.enabled === false
      ? "runtime disabled"
      : runtime.liveEnabled === false
        ? "live disabled"
        : null);
  const panicStopUpdatedAtMs =
    asFiniteOrNull(panicStopRaw.updatedAtMs) ?? asFiniteOrNull(summary.generatedAtMs);
  const panicStopUpdatedBy =
    String(panicStopRaw.updatedBy || "").trim() || null;
  const panicStop = {
    enabled: panicStopEnabled,
    reason: panicStopReason,
    updatedAtMs: panicStopUpdatedAtMs,
    updatedBy: panicStopUpdatedBy,
  };

  return {
    mode: "scalp",
    generatedAtMs,
    range: opts.range,
    dayKey,
    entrySessionProfile: opts.session,
    source: "deployment_registry",
    strategyId: String(runtime.defaultStrategyId || "").trim().toLowerCase(),
    defaultStrategyId: String(runtime.defaultStrategyId || "")
      .trim()
      .toLowerCase(),
    summary: {
      symbols: symbols.length,
      openCount,
      runCount: eventsRaw.length,
      dryRunCount: eventsRaw.filter((row) => asPlainObject(row).rawPayload?.dryRun === true)
        .length,
      totalTradesPlaced,
      stateCounts,
    },
    deployments,
    jobs,
    workerRows,
    panicStop,
    pipeline: {
      panicStop,
      queue: null,
      statusPanel: null,
    },
    symbols,
    history: undefined,
    latestExecutionByDeploymentId,
    latestExecutionBySymbol,
    journal,
    researchSummary,
    researchCursors: researchCursorsRaw.map((raw: unknown) => {
      const c = asPlainObject(raw);
      return {
        cursorKey: String(c.cursorKey || ""),
        venue: String(c.venue || ""),
        symbol: String(c.symbol || ""),
        entrySessionProfile: String(c.entrySessionProfile || ""),
        phase: String(c.phase || "scan"),
        lastCandidateOffset: Math.max(0, Math.floor(Number(c.lastCandidateOffset || 0))),
        progress: asPlainObject(c.progress),
        updatedAtMs: asFiniteOrNull(c.updatedAtMs) ?? 0,
      };
    }),
    researchHighlights: researchHighlightsRaw.map((raw: unknown) => {
      const h = asPlainObject(raw);
      return {
        id: Math.floor(Number(h.id || 0)),
        candidateId: String(h.candidateId || ""),
        venue: String(h.venue || ""),
        symbol: String(h.symbol || ""),
        entrySessionProfile: String(h.entrySessionProfile || ""),
        score: asFiniteOrNull(h.score) ?? 0,
        trades12w: Math.floor(Number(h.trades12w || 0)),
        winningWeeks12w: Math.floor(Number(h.winningWeeks12w || 0)),
        consecutiveWinningWeeks: Math.floor(Number(h.consecutiveWinningWeeks || 0)),
        remarkable: h.remarkable === true,
        createdAtMs: asFiniteOrNull(h.createdAtMs) ?? 0,
      };
    }),
  };
}

type TimeZoneClockParts = {
  y: number;
  m: number;
  d: number;
  hh: number;
  mm: number;
};

function readClockPartsInTimeZone(
  tsMs: number,
  timeZone: string,
): TimeZoneClockParts {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const parts = fmt.formatToParts(new Date(tsMs));
  const read = (type: Intl.DateTimeFormatPartTypes, fallback: number) => {
    const raw = parts.find((part) => part.type === type)?.value;
    const n = Number(raw);
    return Number.isFinite(n) ? n : fallback;
  };
  const hourRaw = read("hour", 0);
  return {
    y: read("year", 1970),
    m: read("month", 1),
    d: read("day", 1),
    hh: hourRaw === 24 ? 0 : hourRaw,
    mm: read("minute", 0),
  };
}

function formatDayKeyFromClockParts(parts: TimeZoneClockParts): string {
  return `${String(parts.y).padStart(4, "0")}-${String(parts.m).padStart(2, "0")}-${String(parts.d).padStart(2, "0")}`;
}

function parseTimelineDayKey(dayKey: string): { y: number; m: number; d: number } | null {
  const match = String(dayKey || "")
    .trim()
    .match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  return {
    y: Number(match[1]),
    m: Number(match[2]),
    d: Number(match[3]),
  };
}

function parseTimelineClock(
  clock: string,
): { hh: number; mm: number } | null {
  const match = String(clock || "")
    .trim()
    .match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
  if (!match) return null;
  return { hh: Number(match[1]), mm: Number(match[2]) };
}

function utcMsFromZonedDayClock(
  dayKey: string,
  clock: string,
  timeZone: string,
): number | null {
  const parsedDay = parseTimelineDayKey(dayKey);
  const parsedClock = parseTimelineClock(clock);
  if (!parsedDay || !parsedClock) return null;
  let guessMs = Date.UTC(
    parsedDay.y,
    parsedDay.m - 1,
    parsedDay.d,
    parsedClock.hh,
    parsedClock.mm,
    0,
    0,
  );
  const targetDayInt = parsedDay.y * 10_000 + parsedDay.m * 100 + parsedDay.d;
  const targetMinuteOfDay = parsedClock.hh * 60 + parsedClock.mm;
  for (let i = 0; i < 6; i += 1) {
    const local = readClockPartsInTimeZone(guessMs, timeZone);
    const localDayInt = local.y * 10_000 + local.m * 100 + local.d;
    const dayDelta =
      localDayInt === targetDayInt ? 0 : localDayInt < targetDayInt ? 1 : -1;
    const localMinuteOfDay = local.hh * 60 + local.mm;
    const deltaMinutes = dayDelta * 1440 + (targetMinuteOfDay - localMinuteOfDay);
    if (deltaMinutes === 0) break;
    guessMs += deltaMinutes * 60_000;
  }
  return guessMs;
}

function formatTimelineMinuteLabel(minuteOfDay: number): string {
  const safeMinute = Math.max(0, Math.min(1440, Math.floor(minuteOfDay)));
  const normalizedMinute = safeMinute === 1440 ? 0 : safeMinute;
  const hh = Math.floor(normalizedMinute / 60);
  const mm = normalizedMinute % 60;
  return `${String(hh).padStart(2, "0")}:${String(mm).padStart(2, "0")}`;
}

type VercelCronEntry = {
  path?: string;
  schedule?: string;
};

type ScalpCronPipelineDefinition = {
  primaryPathname: string;
  matchPathnames: string[];
  fallbackInvokePath: string;
};

type ScalpCronRuntimeMeta = {
  expressions: string[];
  expressionLabel: string | null;
  nextRunAtMs: number | null;
  invokePath: string | null;
};

const SCALP_CRON_PIPELINE_DEFINITIONS: Record<
  string,
  ScalpCronPipelineDefinition
> = {
  scalp_research: {
    primaryPathname: "/api/scalp/v2/cron/research",
    matchPathnames: ["/api/scalp/v2/cron/research"],
    fallbackInvokePath: "/api/scalp/v2/cron/research?batchSize=50",
  },
  scalp_promote: {
    primaryPathname: "/api/scalp/v2/cron/promote",
    matchPathnames: ["/api/scalp/v2/cron/promote"],
    fallbackInvokePath: "/api/scalp/v2/cron/promote?dryRun=false",
  },
  scalp_execute: {
    primaryPathname: "/api/scalp/v2/cron/execute",
    matchPathnames: ["/api/scalp/v2/cron/execute"],
    fallbackInvokePath: "/api/scalp/v2/cron/execute?dryRun=false",
  },
  scalp_reconcile: {
    primaryPathname: "/api/scalp/v2/cron/reconcile",
    matchPathnames: ["/api/scalp/v2/cron/reconcile"],
    fallbackInvokePath: "/api/scalp/v2/cron/reconcile",
  },
  scalp_cycle: {
    primaryPathname: "/api/scalp/v2/cron/cycle",
    matchPathnames: ["/api/scalp/v2/cron/cycle"],
    fallbackInvokePath:
      "/api/scalp/v2/cron/cycle?dryRun=false",
  },
};

const scalpParsedCronCache = new Map<string, ParsedCronSchedule | null>();

function parseCronPathname(rawPath: unknown): string | null {
  const value = String(rawPath || "").trim();
  if (!value) return null;
  try {
    return new URL(value, "http://localhost").pathname;
  } catch {
    return null;
  }
}

function parseEntrySessionProfileFromCronPath(
  rawPath: unknown,
): ScalpEntrySessionProfileUi | null {
  const value = String(rawPath || "").trim();
  if (!value) return null;
  try {
    const parsed = new URL(value, "http://localhost");
    const raw = String(parsed.searchParams.get("session") || "")
      .trim()
      .toLowerCase();
    if (
      raw === "berlin" ||
      raw === "tokyo" ||
      raw === "newyork" ||
      raw === "sydney"
    ) {
      return raw;
    }
    return null;
  } catch {
    return null;
  }
}

function normalizeInvokePathForScalpCronNow(
  rowId: string,
  rawInvokePath: string,
  entrySessionProfile: ScalpEntrySessionProfileUi,
): string {
  void entrySessionProfile;
  const value = String(rawInvokePath || "").trim();
  if (!value) return "";
  const pathname = parseCronPathname(value);
  const isDiscoverCron =
    rowId === "scalp_discover" ||
    pathname === "/api/scalp/v2/cron/discover";
  const isDryRunOverridableCron =
    rowId === "scalp_discover" ||
    rowId === "scalp_evaluate" ||
    rowId === "scalp_promote" ||
    rowId === "scalp_execute" ||
    rowId === "scalp_cycle" ||
    pathname === "/api/scalp/v2/cron/discover" ||
    pathname === "/api/scalp/v2/cron/evaluate" ||
    pathname === "/api/scalp/v2/cron/promote" ||
    pathname === "/api/scalp/v2/cron/execute" ||
    pathname === "/api/scalp/v2/cron/cycle";
  if (!isDiscoverCron && !isDryRunOverridableCron) return value;
  try {
    const absolute = /^https?:\/\//i.test(value);
    const parsed = new URL(value, "http://localhost");
    if (isDiscoverCron || isDryRunOverridableCron)
      parsed.searchParams.set("dryRun", "false");
    if (absolute) return `${parsed.origin}${parsed.pathname}${parsed.search}`;
    return `${parsed.pathname}${parsed.search}`;
  } catch {
    let next = value;
    if (isDiscoverCron || isDryRunOverridableCron) {
      if (/([?&])dryRun=/i.test(next)) {
        next = next.replace(/([?&])dryRun=[^&#]*/i, "$1dryRun=false");
      } else {
        next = `${next}${next.includes("?") ? "&" : "?"}dryRun=false`;
      }
    }
    return next;
  }
}

function dedupeStrings(rows: string[]): string[] {
  return Array.from(
    new Set(
      rows
        .map((row) => String(row || "").trim())
        .filter((row) => row.length > 0),
    ),
  );
}

type ParsedCronSchedule = {
  minute: Set<number>;
  hour: Set<number>;
  dayOfMonth: Set<number>;
  month: Set<number>;
  dayOfWeek: Set<number>;
  dayOfMonthWildcard: boolean;
  dayOfWeekWildcard: boolean;
};

function normalizeCronNumberForDayOfWeek(value: number): number {
  if (value === 7) return 0;
  return value;
}

function parseCronFieldSegment(
  segmentRaw: string,
  min: number,
  max: number,
  normalizer?: (value: number) => number,
): number[] | null {
  const segment = segmentRaw.trim();
  if (!segment) return null;

  const [baseRaw, stepRaw] = segment.split("/");
  const base = (baseRaw || "").trim();
  const step = stepRaw === undefined ? 1 : Math.floor(Number(stepRaw));
  if (!Number.isFinite(step) || step <= 0) return null;

  const parseNumber = (value: string): number | null => {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n)) return null;
    const normalized = normalizer ? normalizer(n) : n;
    if (normalized < min || normalized > max) return null;
    return normalized;
  };

  let start = min;
  let end = max;
  if (base !== "*" && base.length > 0) {
    if (base.includes("-")) {
      const [startRaw, endRaw] = base.split("-");
      const parsedStart = parseNumber(startRaw || "");
      const parsedEnd = parseNumber(endRaw || "");
      if (parsedStart === null || parsedEnd === null || parsedEnd < parsedStart)
        return null;
      start = parsedStart;
      end = parsedEnd;
    } else {
      const parsed = parseNumber(base);
      if (parsed === null) return null;
      start = parsed;
      end = parsed;
    }
  }

  const out: number[] = [];
  for (let v = start; v <= end; v += step) {
    out.push(v);
  }
  return out;
}

function parseCronField(
  fieldRaw: string,
  min: number,
  max: number,
  normalizer?: (value: number) => number,
): { values: Set<number>; wildcard: boolean } | null {
  const field = fieldRaw.trim();
  if (!field) return null;
  const wildcard = field === "*";
  const out = new Set<number>();
  const segments = field.split(",");
  for (const segmentRaw of segments) {
    const values = parseCronFieldSegment(segmentRaw, min, max, normalizer);
    if (!values) return null;
    for (const value of values) out.add(value);
  }
  return { values: out, wildcard };
}

function parseCronSchedule(expressionRaw: string): ParsedCronSchedule | null {
  const expression = String(expressionRaw || "").trim();
  if (!expression) return null;
  if (scalpParsedCronCache.has(expression)) {
    return scalpParsedCronCache.get(expression) || null;
  }

  const parts = expression.split(/\s+/).filter((part) => part.length > 0);
  if (parts.length !== 5) {
    scalpParsedCronCache.set(expression, null);
    return null;
  }

  const minute = parseCronField(parts[0] || "", 0, 59);
  const hour = parseCronField(parts[1] || "", 0, 23);
  const dayOfMonth = parseCronField(parts[2] || "", 1, 31);
  const month = parseCronField(parts[3] || "", 1, 12);
  const dayOfWeek = parseCronField(
    parts[4] || "",
    0,
    6,
    normalizeCronNumberForDayOfWeek,
  );
  if (!minute || !hour || !dayOfMonth || !month || !dayOfWeek) {
    scalpParsedCronCache.set(expression, null);
    return null;
  }

  const parsed: ParsedCronSchedule = {
    minute: minute.values,
    hour: hour.values,
    dayOfMonth: dayOfMonth.values,
    month: month.values,
    dayOfWeek: dayOfWeek.values,
    dayOfMonthWildcard: dayOfMonth.wildcard,
    dayOfWeekWildcard: dayOfWeek.wildcard,
  };
  scalpParsedCronCache.set(expression, parsed);
  return parsed;
}

function cronMatchesUtcMinute(
  tsMs: number,
  parsed: ParsedCronSchedule,
): boolean {
  const date = new Date(tsMs);
  const minute = date.getUTCMinutes();
  const hour = date.getUTCHours();
  const dayOfMonth = date.getUTCDate();
  const month = date.getUTCMonth() + 1;
  const dayOfWeek = date.getUTCDay();

  if (!parsed.minute.has(minute)) return false;
  if (!parsed.hour.has(hour)) return false;
  if (!parsed.month.has(month)) return false;

  const domMatch = parsed.dayOfMonth.has(dayOfMonth);
  const dowMatch = parsed.dayOfWeek.has(dayOfWeek);
  if (parsed.dayOfMonthWildcard && parsed.dayOfWeekWildcard) return true;
  if (parsed.dayOfMonthWildcard) return dowMatch;
  if (parsed.dayOfWeekWildcard) return domMatch;
  return domMatch || dowMatch;
}

function nextCronRunAtMs(expressionRaw: string, nowMs: number): number | null {
  const parsed = parseCronSchedule(expressionRaw);
  if (!parsed) return null;
  const minuteMs = 60_000;
  const firstTs = Math.floor(nowMs / minuteMs) * minuteMs + minuteMs;
  const maxLookAheadMinutes = 370 * 24 * 60;
  for (let step = 0; step <= maxLookAheadMinutes; step += 1) {
    const candidate = firstTs + step * minuteMs;
    if (cronMatchesUtcMinute(candidate, parsed)) return candidate;
  }
  return null;
}

function formatScalpNextRunIn(
  nextRunAtMs: number | null,
  nowMs: number,
): string {
  if (nextRunAtMs === null) return "—";
  const diffMs = nextRunAtMs - nowMs;
  if (!Number.isFinite(diffMs)) return "—";
  if (diffMs <= 0) return "now";
  if (diffMs < 60_000) return `in ${Math.max(1, Math.ceil(diffMs / 1_000))}s`;
  const totalMinutes = Math.max(1, Math.ceil(diffMs / 60_000));
  const days = Math.floor(totalMinutes / (24 * 60));
  const hours = Math.floor((totalMinutes % (24 * 60)) / 60);
  const minutes = totalMinutes % 60;
  if (days > 0) return `in ${days}d ${hours}h`;
  if (hours > 0) return `in ${hours}h ${minutes}m`;
  return `in ${minutes}m`;
}

function listVercelCronRows(): Array<{
  path: string;
  schedule: string;
  pathname: string | null;
}> {
  const crons: VercelCronEntry[] = Array.isArray((vercelConfig as any)?.crons)
    ? (vercelConfig as any).crons
    : [];
  return crons.map((row) => {
    const path = String(row?.path || "").trim();
    const schedule = String(row?.schedule || "").trim();
    const pathname = parseCronPathname(path);
    return { path, schedule, pathname };
  });
}

function buildScalpCronRuntimeMap(
  nowMs: number,
  entrySessionProfile: ScalpEntrySessionProfileUi,
): Record<string, ScalpCronRuntimeMeta> {
  const crons = listVercelCronRows();
  const out: Record<string, ScalpCronRuntimeMeta> = {};

  for (const [id, def] of Object.entries(SCALP_CRON_PIPELINE_DEFINITIONS)) {
    const baseRows = crons.filter(
      (row) =>
        row.pathname !== null && def.matchPathnames.includes(row.pathname),
    );
    const isSessionScopedCron = false;
    const rows = isSessionScopedCron
      ? baseRows.filter((row) => {
          const profile = parseEntrySessionProfileFromCronPath(row.path);
          return profile === null || profile === entrySessionProfile;
        })
      : baseRows;

    const expressions = dedupeStrings(rows.map((row) => row.schedule));
    const nextRunCandidates = expressions
      .map((expression) => nextCronRunAtMs(expression, nowMs))
      .filter(
        (value): value is number =>
          typeof value === "number" && Number.isFinite(value),
      );
    const nextRunAtMs = nextRunCandidates.length
      ? Math.min(...nextRunCandidates)
      : null;
    const invokePath =
      rows.find((row) => row.pathname === def.primaryPathname)?.path ||
      rows[0]?.path ||
      def.fallbackInvokePath ||
      null;

    out[id] = {
      expressions,
      expressionLabel: expressions.length ? expressions.join(" | ") : null,
      nextRunAtMs,
      invokePath,
    };
  }

  return out;
}

ModuleRegistry.registerModules([AllCommunityModule]);

const AgGridReact = dynamic(
  () =>
    import("ag-grid-react").then(
      (mod) => mod.AgGridReact as React.ComponentType<Record<string, unknown>>,
    ),
  { ssr: false },
);

const ChartPanel = dynamic(() => import("../components/ChartPanel"), {
  ssr: false,
  loading: () => (
    <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
      <div className="flex items-center justify-between">
        <div className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 p-1 text-[11px] font-semibold text-slate-500">
          <span className="rounded-full bg-slate-200 px-2.5 py-1 text-slate-700">
            7D
          </span>
          <span className="px-2.5 py-1">30D</span>
          <span className="px-2.5 py-1">6M</span>
        </div>
        <div className="text-xs text-slate-400">1H bars · 7D window</div>
      </div>
      <div
        className="relative mt-3 h-[260px] w-full"
        style={{ minHeight: 260 }}
      >
        <div className="h-full w-full rounded-xl border border-slate-200 bg-slate-50/80 p-4">
          <div className="flex h-full w-full animate-pulse flex-col justify-between">
            <div className="h-3 w-28 rounded-full bg-slate-200" />
            <div className="space-y-2">
              <div className="h-2.5 w-full rounded-full bg-slate-200" />
              <div className="h-2.5 w-11/12 rounded-full bg-slate-200" />
              <div className="h-2.5 w-10/12 rounded-full bg-slate-200" />
            </div>
            <div className="h-3 w-40 rounded-full bg-slate-200" />
          </div>
        </div>
      </div>
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
  const [showAspects, setShowAspects] = useState(false);
  const [showRawEvaluation, setShowRawEvaluation] = useState(false);
  const [showPrompt, setShowPrompt] = useState(false);
  const [evaluateJobs, setEvaluateJobs] = useState<
    Record<string, EvaluateJobRecord>
  >({});
  const [evaluateSubmittingSymbol, setEvaluateSubmittingSymbol] = useState<
    string | null
  >(null);
  const [dashboardRange, setDashboardRange] = useState<DashboardRangeKey>("7D");
  const [strategyMode, setStrategyMode] = useState<StrategyMode>("swing");
  const [scalpSession, setScalpSession] =
    useState<ScalpEntrySessionProfileUi>("berlin");
  const [scalpSummary, setScalpSummary] = useState<ScalpSummaryResponse | null>(
    null,
  );
  const [scalpActiveDeploymentId, setScalpActiveDeploymentId] = useState<
    string | null
  >(null);
  const [scalpExpandedCronId, setScalpExpandedCronId] = useState<string | null>(
    null,
  );
  const [scalpEnabledFilter, setScalpEnabledFilter] = useState<
    "enabled" | "disabled"
  >("enabled");
  const [scalpCopiedDeploymentId, setScalpCopiedDeploymentId] = useState<
    string | null
  >(null);
  const [scalpIsMobileViewport, setScalpIsMobileViewport] =
    useState<boolean>(false);
  const [scalpGridLoadedRows, setScalpGridLoadedRows] = useState<number>(
    SCALP_GRID_LOAD_BATCH,
  );
  const [scalpWorkerSort, setScalpWorkerSort] = useState<ScalpWorkerSortState>({
    key: "windowToTs",
    direction: "desc",
  });
  const [scalpCronNowMs, setScalpCronNowMs] = useState<number>(() =>
    Date.now(),
  );
  const [scalpCronInvokeStateById, setScalpCronInvokeStateById] = useState<
    Record<
      string,
      {
        running: boolean;
        atMs: number | null;
        ok: boolean | null;
        status: number | null;
        durationMs: number | null;
        message: string | null;
      }
    >
  >({});
  const [scalpPanicStopUpdating, setScalpPanicStopUpdating] = useState(false);
  const [swingCronControl, setSwingCronControl] =
    useState<SwingCronControlState | null>(null);
  const [swingCronControlUpdating, setSwingCronControlUpdating] =
    useState(false);
  const [livePriceNow, setLivePriceNow] = useState<number | null>(null);
  const [livePriceTs, setLivePriceTs] = useState<number | null>(null);
  const [livePriceConnected, setLivePriceConnected] = useState(false);
  const [themePreference, setThemePreference] =
    useState<ThemePreference>("system");
  const [resolvedTheme, setResolvedTheme] = useState<ResolvedTheme>("light");
  const evaluatePollTimersRef = useRef<Record<string, number>>({});
  const scalpSummaryFetchedAtMsRef = useRef<number>(0);
  const scalpSummaryErrorCountRef = useRef<number>(0);
  const scalpCopyFeedbackTimerRef = useRef<number | null>(null);

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

  const copyScalpDeploymentLabel = async (
    event: React.MouseEvent<HTMLButtonElement>,
    deploymentId: string,
    label: string,
  ) => {
    event.preventDefault();
    event.stopPropagation();
    const text = String(label || "").trim();
    if (!text) return;
    let copied = false;
    if (typeof navigator !== "undefined" && navigator.clipboard?.writeText) {
      try {
        await navigator.clipboard.writeText(text);
        copied = true;
      } catch {
        copied = false;
      }
    }
    if (!copied && typeof document !== "undefined") {
      const textarea = document.createElement("textarea");
      textarea.value = text;
      textarea.style.position = "fixed";
      textarea.style.opacity = "0";
      document.body.appendChild(textarea);
      textarea.focus();
      textarea.select();
      try {
        copied = document.execCommand("copy");
      } catch {
        copied = false;
      } finally {
        document.body.removeChild(textarea);
      }
    }
    if (!copied) return;
    setScalpCopiedDeploymentId(deploymentId);
    if (scalpCopyFeedbackTimerRef.current) {
      window.clearTimeout(scalpCopyFeedbackTimerRef.current);
      scalpCopyFeedbackTimerRef.current = null;
    }
    scalpCopyFeedbackTimerRef.current = window.setTimeout(() => {
      setScalpCopiedDeploymentId((prev) =>
        prev === deploymentId ? null : prev,
      );
      scalpCopyFeedbackTimerRef.current = null;
    }, 950);
  };

  useEffect(() => {
    const timerId = window.setInterval(() => {
      setScalpCronNowMs(Date.now());
    }, 10_000);
    return () => window.clearInterval(timerId);
  }, []);

  useEffect(() => {
    return () => {
      if (scalpCopyFeedbackTimerRef.current) {
        window.clearTimeout(scalpCopyFeedbackTimerRef.current);
      }
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const mobileMedia = window.matchMedia("(max-width: 640px)");
    const applyViewport = (matches: boolean) => {
      setScalpIsMobileViewport(matches);
    };
    applyViewport(mobileMedia.matches);
    const onViewportChange = (event: MediaQueryListEvent) => {
      applyViewport(event.matches);
    };
    if (typeof mobileMedia.addEventListener === "function") {
      mobileMedia.addEventListener("change", onViewportChange);
      return () => {
        mobileMedia.removeEventListener("change", onViewportChange);
      };
    }
    mobileMedia.addListener(onViewportChange);
    return () => {
      mobileMedia.removeListener(onViewportChange);
    };
  }, []);

  const resolveSystemTheme = (): ResolvedTheme => {
    if (typeof window === "undefined") return "light";
    return window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  };

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

  const aspectMeta: Record<
    string,
    { Icon: LucideIcon; color: string; bg: string }
  > = {
    data_quality: { Icon: Database, color: "text-sky-700", bg: "bg-sky-100" },
    data_quantity: { Icon: Layers3, color: "text-cyan-700", bg: "bg-cyan-100" },
    ai_performance: {
      Icon: Cpu,
      color: "text-indigo-700",
      bg: "bg-indigo-100",
    },
    strategy_performance: {
      Icon: BarChart3,
      color: "text-emerald-700",
      bg: "bg-emerald-100",
    },
    signal_strength_clarity: {
      Icon: Activity,
      color: "text-amber-700",
      bg: "bg-amber-100",
    },
    risk_management: {
      Icon: ShieldCheck,
      color: "text-rose-700",
      bg: "bg-rose-100",
    },
    consistency: { Icon: Repeat, color: "text-blue-700", bg: "bg-blue-100" },
    explainability: {
      Icon: BookOpen,
      color: "text-purple-700",
      bg: "bg-purple-100",
    },
    responsiveness: { Icon: Zap, color: "text-teal-700", bg: "bg-teal-100" },
    prompt_engineering: {
      Icon: PenTool,
      color: "text-fuchsia-700",
      bg: "bg-fuchsia-100",
    },
    prompt_consistency: {
      Icon: ListChecks,
      color: "text-lime-700",
      bg: "bg-lime-100",
    },
    action_logic: {
      Icon: Braces,
      color: "text-orange-700",
      bg: "bg-orange-100",
    },
    ai_freedom: { Icon: Wand2, color: "text-indigo-700", bg: "bg-indigo-100" },
    guardrail_coverage: {
      Icon: ShieldPlus,
      color: "text-rose-700",
      bg: "bg-rose-100",
    },
  };

  const formatLabel = (key: string) => key.replace(/_/g, " ");
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

  const loadSwingCronControl = async () => {
    const res = await fetch("/api/swing/ops/cron-control", {
      headers: buildAdminHeaders(),
      cache: "no-store",
    });
    if (res.status === 401) {
      handleAuthExpired("Admin session expired. Re-enter ADMIN_ACCESS_SECRET.");
      throw new Error("Unauthorized");
    }
    if (!res.ok) {
      throw new Error(`Failed to load swing cron control (${res.status})`);
    }
    const json = await res.json();
    setSwingCronControl((json?.cronControl || null) as SwingCronControlState);
  };

  const loadDashboard = async () => {
    setLoading(true);
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
        const summaryParams = new URLSearchParams({ range: dashboardRange });
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
        const summaryRows = Array.isArray(summaryJson.data)
          ? summaryJson.data
          : [];
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
      } catch (summaryErr: any) {
        summaryError =
          summaryErr?.message || "Failed to load dashboard summary";
      }

      try {
        await loadSwingCronControl();
      } catch (controlErr: any) {
        summaryError =
          summaryError ||
          controlErr?.message ||
          "Failed to load swing cron control";
      }

      setError(summaryError);
    } catch (err: any) {
      setError(err?.message || "Failed to load dashboard");
    } finally {
      setLoading(false);
    }
  };

  const loadScalpDashboard = async (
    opts: { silent?: boolean; force?: boolean } = {},
  ) => {
    const silent = opts.silent === true;
    const force = opts.force === true;
    const nowMs = Date.now();
    if (
      !force &&
      silent &&
      nowMs - scalpSummaryFetchedAtMsRef.current < SCALP_MIN_REFRESH_GAP_MS
    ) {
      return;
    }
    if (!silent) setLoading(true);
    try {
      const params = new URLSearchParams({
        range: dashboardRange,
        session: scalpSession,
        eventLimit: "240",
        ledgerLimit: "300",
        deploymentLimit: "2000",
        jobLimit: "20",
      });
      if (!silent || force) {
        params.set("fresh", "true");
      }
      const summaryRes = await fetch(
        `/api/scalp/v2/dashboard/summary?${params.toString()}`,
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
        throw new Error(`Failed to load scalp summary (${summaryRes.status})`);
      }
      const summaryRaw = await summaryRes.json();
      const summaryJson: ScalpSummaryResponse = toUiScalpSummaryFromV2(
        summaryRaw,
        {
          range: dashboardRange,
          session: scalpSession,
        },
      );
      setScalpSummary(summaryJson);
      scalpSummaryFetchedAtMsRef.current = nowMs;
      scalpSummaryErrorCountRef.current = 0;
      setError(null);
    } catch (err: any) {
      scalpSummaryErrorCountRef.current += 1;
      setError(err?.message || "Failed to load scalp dashboard");
    } finally {
      if (!silent) setLoading(false);
    }
  };

  const invokeScalpCronNow = async (row: ScalpOpsCronRow) => {
    const invokePath = normalizeInvokePathForScalpCronNow(
      row.id,
      String(row.invokePath || "").trim(),
      scalpSession,
    );
    if (!invokePath) {
      setScalpCronInvokeStateById((prev) => ({
        ...prev,
        [row.id]: {
          running: false,
          atMs: Date.now(),
          ok: false,
          status: null,
          durationMs: null,
          message: "No invoke path configured",
        },
      }));
      return;
    }

    const invokeStartedAtMs = Date.now();
    setScalpCronInvokeStateById((prev) => ({
      ...prev,
      [row.id]: {
        ...(prev[row.id] || {
          atMs: null,
          ok: null,
          status: null,
          durationMs: null,
          message: null,
        }),
        running: true,
        message: null,
      },
    }));

    try {
      const res = await fetch(invokePath, {
        headers: buildAdminHeaders(),
        cache: "no-store",
      });
      const payload = await res.json().catch(() => null);
      if (res.status === 401) {
        handleAuthExpired(
          "Admin session expired. Re-enter ADMIN_ACCESS_SECRET.",
        );
        throw new Error("Unauthorized");
      }
      if (!res.ok) {
        const msg =
          String(payload?.message || payload?.error || "").trim() ||
          `Invoke failed (${res.status})`;
        setScalpCronInvokeStateById((prev) => ({
          ...prev,
          [row.id]: {
            running: false,
            atMs: Date.now(),
            ok: false,
            status: res.status,
            durationMs: Math.max(0, Date.now() - invokeStartedAtMs),
            message: msg,
          },
        }));
        return;
      }

      const okMsg = String(payload?.message || "").trim() || "Invoked";
      const workerDurationMs = asFiniteNumber(
        payload?.job?.diagnostics?.durationMs,
      );
      const durationMs =
        workerDurationMs !== null
          ? Math.max(0, Math.floor(workerDurationMs))
          : Math.max(0, Date.now() - invokeStartedAtMs);
      setScalpCronInvokeStateById((prev) => ({
        ...prev,
        [row.id]: {
          running: false,
          atMs: Date.now(),
          ok: true,
          status: res.status,
          durationMs,
          message: okMsg,
        },
      }));
      await loadScalpDashboard({ silent: true, force: true });
    } catch (err: any) {
      const msg =
        String(err?.message || "Invoke failed").trim() || "Invoke failed";
      setScalpCronInvokeStateById((prev) => ({
        ...prev,
        [row.id]: {
          running: false,
          atMs: Date.now(),
          ok: false,
          status: prev[row.id]?.status ?? null,
          durationMs: Math.max(0, Date.now() - invokeStartedAtMs),
          message: msg,
        },
      }));
    }
  };

  const setScalpPanicStop = async (enabled: boolean) => {
    if (scalpPanicStopUpdating) return;
    setScalpPanicStopUpdating(true);
    try {
      const reason = enabled
        ? "manual_panic_stop_from_ui"
        : "manual_panic_stop_release_from_ui";
      const updatedBy = "ui:panic-stop";
      const res = await fetch("/api/scalp/v2/control", {
        method: "POST",
        headers: {
          ...(buildAdminHeaders() || {}),
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          enabled: !enabled,
          liveEnabled: !enabled,
          panicStop: {
            enabled,
            reason,
            updatedBy,
            updatedAtMs: Date.now(),
          },
        }),
        cache: "no-store",
      });
      if (res.status === 401) {
        handleAuthExpired(
          "Admin session expired. Re-enter ADMIN_ACCESS_SECRET.",
        );
        return;
      }
      if (!res.ok) {
        throw new Error(`panic_stop_update_failed (${res.status})`);
      }
      await loadScalpDashboard({ silent: true, force: true });
    } catch (err: any) {
      setError(err?.message || "Failed to update panic stop");
    } finally {
      setScalpPanicStopUpdating(false);
    }
  };

  const setSwingCronHardDeactivate = async (hardDeactivated: boolean) => {
    if (swingCronControlUpdating) return;
    setSwingCronControlUpdating(true);
    try {
      const reason = hardDeactivated
        ? "manual_hard_deactivate_from_ui"
        : "manual_reactivate_from_ui";
      const updatedBy = "ui:swing-cron-control";
      const params = new URLSearchParams({
        hardDeactivated: hardDeactivated ? "true" : "false",
        reason,
        updatedBy,
      });
      const res = await fetch(`/api/swing/ops/cron-control?${params.toString()}`, {
        method: "POST",
        headers: buildAdminHeaders(),
        cache: "no-store",
      });
      if (res.status === 401) {
        handleAuthExpired(
          "Admin session expired. Re-enter ADMIN_ACCESS_SECRET.",
        );
        return;
      }
      if (!res.ok) {
        throw new Error(`swing_cron_control_update_failed (${res.status})`);
      }
      const json = await res.json().catch(() => null);
      if (json?.cronControl) {
        setSwingCronControl(json.cronControl as SwingCronControlState);
      } else {
        await loadSwingCronControl();
      }
    } catch (err: any) {
      setError(err?.message || "Failed to update swing cron control");
    } finally {
      setSwingCronControlUpdating(false);
    }
  };

  const clearEvaluatePollTimer = (symbol: string) => {
    const timerId = evaluatePollTimersRef.current[symbol];
    if (timerId) {
      window.clearInterval(timerId);
      delete evaluatePollTimersRef.current[symbol];
    }
  };
  const pollEvaluationJob = async (symbol: string, jobId: string) => {
    try {
      const params = new URLSearchParams({
        jobId,
        t: String(Date.now()),
      });
      const res = await fetch(`/api/swing/evaluate?${params.toString()}`, {
        headers: buildAdminHeaders(),
        cache: "no-store",
      });
      if (res.status === 401) {
        clearEvaluatePollTimer(symbol);
        handleAuthExpired(
          "Admin session expired. Re-enter ADMIN_ACCESS_SECRET.",
        );
        setError(
          "Evaluation polling unauthorized (401). Re-enter admin access secret.",
        );
        return;
      }
      if (res.status === 304) return;
      if (!res.ok) return;
      const json = await res.json();
      const status = String(json?.status || "") as EvaluateJobStatus;
      if (!status) return;
      setEvaluateJobs((prev) => ({
        ...prev,
        [symbol]: {
          id: jobId,
          status,
          updatedAt: Number(json?.updatedAt) || Date.now(),
          error: typeof json?.error === "string" ? json.error : undefined,
        },
      }));

      if (status === "succeeded" || status === "failed") {
        clearEvaluatePollTimer(symbol);
        if (status === "succeeded") {
          try {
            await loadSymbolEvaluation(symbol);
          } catch {}
        }
      }
    } catch {
      // keep polling on transient fetch issues
    }
  };
  const triggerEvaluation = async (symbol: string) => {
    if (!symbol || evaluateSubmittingSymbol) return;
    setEvaluateSubmittingSymbol(symbol);
    setError(null);
    try {
      const params = new URLSearchParams({
        symbol,
        async: "true",
      });
      const res = await fetch(`/api/swing/evaluate?${params.toString()}`, {
        headers: buildAdminHeaders(),
        cache: "no-store",
      });
      if (!res.ok) {
        if (res.status === 401) {
          handleAuthExpired(
            "Admin session expired. Re-enter ADMIN_ACCESS_SECRET.",
          );
        }
        let msg = `Failed to queue evaluation (${res.status})`;
        try {
          const body = await res.json();
          msg = body?.error ? `${msg}: ${String(body.error)}` : msg;
        } catch {}
        throw new Error(msg);
      }
      const json = await res.json();
      const jobId = String(json?.jobId || "");
      if (!jobId) throw new Error("Missing evaluation job ID");
      setEvaluateJobs((prev) => ({
        ...prev,
        [symbol]: { id: jobId, status: "queued", updatedAt: Date.now() },
      }));
      clearEvaluatePollTimer(symbol);
      void pollEvaluationJob(symbol, jobId);
      evaluatePollTimersRef.current[symbol] = window.setInterval(() => {
        void pollEvaluationJob(symbol, jobId);
      }, 5000);
    } catch (err: any) {
      setError(err?.message || "Failed to queue evaluation");
    } finally {
      setEvaluateSubmittingSymbol(null);
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
    if (typeof window === "undefined") return;
    const stored = window.localStorage.getItem(THEME_PREFERENCE_STORAGE_KEY);
    const normalizedThemePreference: ThemePreference =
      stored === "light" || stored === "dark" || stored === "system"
        ? stored
        : "system";
    setThemePreference(normalizedThemePreference);
    if (normalizedThemePreference === "system") {
      setResolvedTheme(resolveSystemTheme());
      return;
    }
    setResolvedTheme(normalizedThemePreference);
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const stored = window.localStorage.getItem(STRATEGY_MODE_STORAGE_KEY);
    if (stored === "swing" || stored === "scalp") {
      setStrategyMode(stored);
      return;
    }
    if (stored === "forex") {
      window.localStorage.setItem(STRATEGY_MODE_STORAGE_KEY, "swing");
    }
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const stored = String(
      window.localStorage.getItem(SCALP_ENTRY_SESSION_STORAGE_KEY) || "",
    )
      .trim()
      .toLowerCase();
    if (
      stored === "berlin" ||
      stored === "tokyo" ||
      stored === "newyork" ||
      stored === "sydney"
    ) {
      setScalpSession(stored);
    }
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    if (themePreference !== "system") return;
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const handleThemeChange = () => {
      setResolvedTheme(media.matches ? "dark" : "light");
    };
    handleThemeChange();
    if (typeof media.addEventListener === "function") {
      media.addEventListener("change", handleThemeChange);
      return () => media.removeEventListener("change", handleThemeChange);
    }
    media.addListener(handleThemeChange);
    return () => media.removeListener(handleThemeChange);
  }, [themePreference]);

  useEffect(() => {
    if (typeof document === "undefined") return;
    document.documentElement.style.colorScheme = resolvedTheme;
  }, [resolvedTheme]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(SCALP_ENTRY_SESSION_STORAGE_KEY, scalpSession);
  }, [scalpSession]);

  useEffect(() => {
    if (!adminGranted) return;
    if (strategyMode === "scalp") {
      scalpSummaryFetchedAtMsRef.current = 0;
      loadScalpDashboard();
      return;
    }
    loadDashboard();
  }, [adminGranted, dashboardRange, strategyMode, scalpSession]);

  useEffect(() => {
    if (!adminGranted || strategyMode !== "scalp") return;
    let cancelled = false;
    let timerId: number | null = null;

    const scheduleNextPoll = () => {
      if (cancelled) return;
      const hidden =
        typeof document !== "undefined" &&
        document.visibilityState !== "visible";
      const hadRecentErrors = scalpSummaryErrorCountRef.current > 0;
      const intervalMs = hadRecentErrors
        ? SCALP_LIVE_POLL_ERROR_BACKOFF_MS
        : hidden
          ? SCALP_LIVE_POLL_HIDDEN_MS
          : SCALP_LIVE_POLL_VISIBLE_MS;
      timerId = window.setTimeout(async () => {
        await loadScalpDashboard({ silent: true });
        scheduleNextPoll();
      }, intervalMs);
    };

    scheduleNextPoll();
    return () => {
      cancelled = true;
      if (timerId) window.clearTimeout(timerId);
    };
  }, [adminGranted, strategyMode, adminSecret, dashboardRange, scalpSession]);

  useEffect(() => {
    const rows = Array.isArray(scalpSummary?.symbols)
      ? scalpSummary.symbols
      : [];
    if (!rows.length) {
      setScalpActiveDeploymentId(null);
      return;
    }
    setScalpActiveDeploymentId((prev) => {
      if (prev && rows.some((row) => row.deploymentId === prev)) return prev;
      return rows[0]?.deploymentId || null;
    });
  }, [scalpSummary]);

  useEffect(() => {
    const symbol = symbols[active] || null;
    if (strategyMode !== "swing") return;
    if (!adminGranted || !symbol) return;
    const platform = tabData[symbol]?.lastPlatform ?? null;
    let cancelled = false;
    (async () => {
      try {
        await Promise.all([
          loadSymbolDecision(symbol, platform),
          loadSymbolEvaluation(symbol),
        ]);
        if (!cancelled) setError(null);
      } catch (err: any) {
        if (cancelled) return;
        setError(err?.message || `Failed to load details for ${symbol}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [adminGranted, symbols, active, strategyMode]);

  useEffect(() => {
    return () => {
      Object.keys(evaluatePollTimersRef.current).forEach((symbol) => {
        clearEvaluatePollTimer(symbol);
      });
    };
  }, []);

  useEffect(() => {
    setShowAspects(false);
    setShowRawEvaluation(false);
    setShowPrompt(false);
  }, [active, symbols]);

  useEffect(() => {
    if (strategyMode !== "swing") {
      setLivePriceNow(null);
      setLivePriceTs(null);
      setLivePriceConnected(false);
      return;
    }
    const symbol = symbols[active] || null;
    const platform = symbol
      ? String(tabData[symbol]?.lastPlatform || "").toLowerCase()
      : "";
    if (!adminGranted || !symbol) {
      setLivePriceNow(null);
      setLivePriceTs(null);
      setLivePriceConnected(false);
      return;
    }

    if (platform === "capital") {
      let closed = false;
      let pollTimer: number | null = null;
      let inFlight: AbortController | null = null;

      const clearPoll = () => {
        if (pollTimer) {
          window.clearTimeout(pollTimer);
          pollTimer = null;
        }
        if (inFlight) {
          inFlight.abort();
          inFlight = null;
        }
      };

      const schedulePoll = () => {
        if (closed) return;
        pollTimer = window.setTimeout(() => {
          if (closed) return;
          void poll();
        }, CAPITAL_LIVE_POLL_MS);
      };

      const poll = async () => {
        if (closed) return;
        inFlight = new AbortController();
        try {
          const params = new URLSearchParams({
            symbol,
            platform: "capital",
            t: String(Date.now()),
          });
          const res = await fetch(
            `/api/swing/dashboard/live-price?${params.toString()}`,
            {
              headers: buildAdminHeaders(),
              cache: "no-store",
              signal: inFlight.signal,
            },
          );
          if (res.status === 401) {
            closed = true;
            clearPoll();
            setLivePriceConnected(false);
            handleAuthExpired(
              "Admin session expired. Re-enter ADMIN_ACCESS_SECRET.",
            );
            return;
          }
          if (!res.ok) {
            throw new Error(`Capital live price failed (${res.status})`);
          }
          const payload = await res.json();
          const px = Number(payload?.price);
          const ts = Number(payload?.ts);
          if (Number.isFinite(px) && px > 0) {
            setLivePriceNow(px);
            setLivePriceTs(Number.isFinite(ts) ? ts : Date.now());
            setLivePriceConnected(true);
          } else {
            setLivePriceConnected(false);
          }
        } catch (err: any) {
          if (err?.name !== "AbortError") {
            setLivePriceConnected(false);
          }
        } finally {
          inFlight = null;
          schedulePoll();
        }
      };

      setLivePriceNow(null);
      setLivePriceTs(null);
      setLivePriceConnected(false);
      void poll();

      return () => {
        closed = true;
        clearPoll();
        setLivePriceConnected(false);
      };
    }

    if (platform && platform !== "bitget") {
      setLivePriceNow(null);
      setLivePriceTs(null);
      setLivePriceConnected(false);
      return;
    }

    let closed = false;
    let ws: WebSocket | null = null;
    let pingTimer: number | null = null;
    let reconnectTimer: number | null = null;

    const clearTimers = () => {
      if (pingTimer) {
        window.clearInterval(pingTimer);
        pingTimer = null;
      }
      if (reconnectTimer) {
        window.clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
    };

    const scheduleReconnect = () => {
      if (closed) return;
      if (reconnectTimer) window.clearTimeout(reconnectTimer);
      reconnectTimer = window.setTimeout(() => {
        if (closed) return;
        connect();
      }, WS_RECONNECT_MS);
    };

    const connect = () => {
      try {
        ws = new WebSocket(BITGET_PUBLIC_WS_URL);
      } catch {
        scheduleReconnect();
        return;
      }

      ws.onopen = () => {
        if (closed || !ws) return;
        setLivePriceConnected(true);
        try {
          ws.send(
            JSON.stringify({
              op: "subscribe",
              args: [
                { instType: "USDT-FUTURES", channel: "ticker", instId: symbol },
              ],
            }),
          );
        } catch {}

        pingTimer = window.setInterval(() => {
          if (!ws || ws.readyState !== WebSocket.OPEN) return;
          try {
            ws.send("ping");
          } catch {}
        }, WS_PING_MS);
      };

      ws.onmessage = (event) => {
        if (closed) return;
        const raw = String(event.data ?? "");
        if (!raw || raw === "pong" || raw === "ping") return;
        let parsed: any = null;
        try {
          parsed = JSON.parse(raw);
        } catch {
          return;
        }
        const rows = Array.isArray(parsed?.data) ? parsed.data : [];
        for (const row of rows) {
          const px = Number(row?.lastPr ?? row?.last ?? row?.price);
          if (!Number.isFinite(px) || px <= 0) continue;
          const ts = Number(row?.ts ?? parsed?.ts ?? Date.now());
          setLivePriceNow(px);
          setLivePriceTs(Number.isFinite(ts) ? ts : Date.now());
          break;
        }
      };

      ws.onerror = () => {
        if (closed) return;
        setLivePriceConnected(false);
      };

      ws.onclose = () => {
        if (closed) return;
        setLivePriceConnected(false);
        clearTimers();
        scheduleReconnect();
      };
    };

    setLivePriceNow(null);
    setLivePriceTs(null);
    setLivePriceConnected(false);
    connect();

    return () => {
      closed = true;
      clearTimers();
      setLivePriceConnected(false);
      if (ws && ws.readyState === WebSocket.OPEN) {
        try {
          ws.close();
        } catch {}
      }
      ws = null;
    };
  }, [adminGranted, symbols, active, tabData, adminSecret, strategyMode]);

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

  const current = symbols[active] ? tabData[symbols[active]] : null;
  const activeSymbol = symbols[active] || null;
  const activePlatform =
    current?.lastPlatform?.toLowerCase() === "capital" ? "capital" : "bitget";
  const activePlatformLogo =
    activePlatform === "capital" ? "/capital.svg" : "/bitget.svg";
  const dashboardRangeText =
    dashboardRange === "6M" ? "6m" : dashboardRange.toLowerCase();
  const liveOpenPnl =
    current &&
    typeof livePriceNow === "number" &&
    Number.isFinite(livePriceNow) &&
    typeof current.openEntryPrice === "number" &&
    Number.isFinite(current.openEntryPrice) &&
    current.openEntryPrice > 0 &&
    (current.openDirection === "long" || current.openDirection === "short")
      ? ((livePriceNow - current.openEntryPrice) / current.openEntryPrice) *
        (current.openDirection === "long" ? 1 : -1) *
        (typeof current.openLeverage === "number" && current.openLeverage > 0
          ? current.openLeverage
          : 1) *
        100
      : null;
  const effectiveOpenPnl =
    typeof liveOpenPnl === "number"
      ? liveOpenPnl
      : current && typeof current.openPnl === "number"
        ? current.openPnl
        : null;
  const effectivePnl7dWithOpen =
    current &&
    typeof current.pnl7d === "number" &&
    typeof effectiveOpenPnl === "number"
      ? current.pnl7d + effectiveOpenPnl
      : current && typeof current.pnl7d === "number"
        ? current.pnl7d
        : typeof effectiveOpenPnl === "number"
          ? effectiveOpenPnl
          : current && typeof current.pnl7dWithOpen === "number"
            ? current.pnl7dWithOpen
            : null;
  const openPnlIsLive = typeof liveOpenPnl === "number";
  const showChartPanel = Boolean(adminGranted && activeSymbol);
  const currentEvalJob = activeSymbol ? evaluateJobs[activeSymbol] : null;
  const evaluateRunning = Boolean(
    activeSymbol &&
    currentEvalJob &&
    (currentEvalJob.status === "queued" || currentEvalJob.status === "running"),
  );
  const swingCronControlLoaded = swingCronControl !== null;
  const swingCronHardDeactivated = swingCronControl?.hardDeactivated === true;
  const swingCronReason =
    typeof swingCronControl?.reason === "string"
      ? swingCronControl.reason.trim()
      : "";
  const hasLastDecision = !!(
    current &&
    ("lastDecision" in current ||
      "lastDecisionTs" in current ||
      "lastPrompt" in current ||
      "lastMetrics" in current ||
      "lastBiasTimeframes" in current)
  );
  const hasDetails = !!(
    current?.evaluation?.what_went_well?.length ||
    current?.evaluation?.issues?.length ||
    current?.evaluation?.improvements?.length
  );
  const biasOrder = [
    { key: "context_bias", label: "Context" },
    { key: "macro_bias", label: "Macro" },
    { key: "primary_bias", label: "Primary" },
    { key: "micro_bias", label: "Micro" },
  ] as const;
  const isInitialLoading = loading && !symbols.length;
  const loadingLabel =
    strategyMode === "scalp"
      ? "Loading scalp dashboard..."
      : !symbols.length
        ? "Loading evaluations..."
        : activeSymbol
          ? `Loading ${activeSymbol}...`
          : "Loading selected symbol...";
  const scalpRows = Array.isArray(scalpSummary?.symbols)
    ? scalpSummary.symbols
    : [];
  const scalpSelectedDeploymentId =
    String(scalpActiveDeploymentId || "").trim() || null;
  const scalpActiveRow = scalpSelectedDeploymentId
    ? scalpRows.find((row) => row.deploymentId === scalpSelectedDeploymentId) ||
      null
    : scalpRows[0] || null;
  const scalpActiveJournal = Array.isArray(scalpSummary?.journal)
    ? scalpSummary.journal.filter((entry) => {
        const payload =
          entry.payload && typeof entry.payload === "object"
            ? entry.payload
            : {};
        const deploymentId = String(
          (payload as Record<string, unknown>).deploymentId || "",
        ).trim();
        if (scalpSelectedDeploymentId) {
          if (deploymentId) return deploymentId === scalpSelectedDeploymentId;
          if (!scalpActiveRow) return false;
        } else if (!scalpActiveRow) {
          return true;
        }
        if (deploymentId) return deploymentId === scalpActiveRow.deploymentId;
        const strategyId = String(
          (payload as Record<string, unknown>).strategyId || "",
        )
          .trim()
          .toLowerCase();
        return (
          String(entry.symbol || "")
            .trim()
            .toUpperCase() === scalpActiveRow.symbol.toUpperCase() &&
          (!strategyId ||
            strategyId ===
              String(scalpActiveRow.strategyId || "")
                .trim()
                .toLowerCase())
        );
      })
    : [];
  const scalpLatestExecutionByDeploymentId =
    scalpSummary?.latestExecutionByDeploymentId &&
    typeof scalpSummary.latestExecutionByDeploymentId === "object"
      ? scalpSummary.latestExecutionByDeploymentId
      : {};
  const scalpLatestExecutionBySymbol =
    scalpSummary?.latestExecutionBySymbol &&
    typeof scalpSummary.latestExecutionBySymbol === "object"
      ? scalpSummary.latestExecutionBySymbol
      : {};
  const scalpActiveExecution = scalpActiveRow
    ? (scalpLatestExecutionByDeploymentId[scalpActiveRow.deploymentId] ??
      scalpLatestExecutionBySymbol[scalpActiveRow.symbol] ??
      scalpLatestExecutionBySymbol[scalpActiveRow.symbol.toUpperCase()] ??
      null)
    : null;
  const scalpActiveReasonCodesRaw = (
    Array.isArray(scalpActiveRow?.reasonCodes)
      ? scalpActiveRow?.reasonCodes
      : []
  ) as string[];
  const scalpActiveReasonCodes = (() => {
    if (!scalpActiveReasonCodesRaw.length) return [];
    const nonGeneric = scalpActiveReasonCodesRaw.filter((code) => {
      const upper = String(code || "")
        .trim()
        .toUpperCase();
      return upper !== "SCALP_PHASE3_EXECUTION" && upper !== "NO_STATE_CHANGE";
    });
    return nonGeneric.length ? nonGeneric : scalpActiveReasonCodesRaw;
  })();
  const scalpReasonSnapshotState = !scalpActiveRow
    ? "none"
    : scalpActiveReasonCodesRaw.length
      ? "fresh"
      : "none";
  const scalpTopStates = Object.entries(
    scalpSummary?.summary?.stateCounts ?? {},
  )
    .sort((a, b) => b[1] - a[1])
    .slice(0, 4);
  const scalpActiveWinRatePct =
    scalpActiveRow && scalpActiveRow.tradesPlaced > 0
      ? (scalpActiveRow.wins / scalpActiveRow.tradesPlaced) * 100
      : null;
  const scalpActiveNetR =
    scalpActiveRow &&
    typeof scalpActiveRow.netR === "number" &&
    Number.isFinite(scalpActiveRow.netR)
      ? scalpActiveRow.netR
      : null;
  const scalpActiveMaxDdR =
    scalpActiveRow &&
    typeof scalpActiveRow.maxDrawdownR === "number" &&
    Number.isFinite(scalpActiveRow.maxDrawdownR)
      ? scalpActiveRow.maxDrawdownR
      : null;
  const asFiniteNumber = (value: unknown): number | null => {
    if (value === null || value === undefined) return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  };
  const normalizeScalpLifecycleState = (
    value: unknown,
  ):
    | "candidate"
    | "incumbent_refresh"
    | "graduated"
    | "suspended"
    | "retired"
    | null => {
    const normalized = String(value || "")
      .trim()
      .toLowerCase();
    if (
      normalized === "candidate" ||
      normalized === "incumbent_refresh" ||
      normalized === "graduated" ||
      normalized === "suspended" ||
      normalized === "retired"
    ) {
      return normalized;
    }
    return null;
  };
  const scalpOpsDeployments: ScalpOpsDeploymentRow[] = scalpRows.map((row) => {
    const runtimePerf30dExpectancyR =
      row.tradesPlaced > 0 &&
      typeof row.netR === "number" &&
      Number.isFinite(row.netR)
        ? row.netR / row.tradesPlaced
        : null;
    const entrySessionProfile =
      normalizeScalpEntrySessionProfileUi(row.entrySessionProfile) ||
      extractScalpEntrySessionProfileFromDeploymentId(row.deploymentId) ||
      normalizeScalpEntrySessionProfileUi(scalpSummary?.entrySessionProfile) ||
      null;

    return {
      deploymentId: row.deploymentId,
      symbol: row.symbol,
      strategyId: row.strategyId,
      tuneId: String(row.tuneId || row.tune || "default"),
      entrySessionProfile,
      source: "runtime",
      enabled: row.enabled === true,
      inUniverse: null,
      lifecycleState: null,
      promotionEligible:
        typeof row.promotionEligible === "boolean"
          ? row.promotionEligible
          : false,
      promotionReason: row.promotionReason || null,
      forwardValidation: row.forwardValidation || null,
      perf30dTrades: asFiniteNumber(row.tradesPlaced),
      perf30dExpectancyR: runtimePerf30dExpectancyR,
      perf30dNetR: asFiniteNumber(row.netR),
      perf30dMaxDrawdownR: asFiniteNumber(row.maxDrawdownR),
      runtime: row,
      promotionGate: null,
    };
  });
  const scalpRegistryDeployments = useMemo<ScalpOpsDeploymentRow[]>(() => {
    const rows = (
      Array.isArray(scalpSummary?.deployments) ? scalpSummary.deployments : []
    ).reduce<ScalpOpsDeploymentRow[]>((out, row) => {
      const deploymentId = String(row?.deploymentId || "").trim();
      const symbol = String(row?.symbol || "")
        .trim()
        .toUpperCase();
      const strategyId = String(row?.strategyId || "").trim();
      if (!deploymentId || !symbol || !strategyId) return out;
      const entrySessionProfile =
        normalizeScalpEntrySessionProfileUi(row?.entrySessionProfile) ||
        extractScalpEntrySessionProfileFromDeploymentId(deploymentId) ||
        null;
      out.push({
        deploymentId,
        symbol,
        strategyId,
        lifecycleState: normalizeScalpLifecycleState(row?.lifecycleState),
        tuneId: String(row?.tuneId || "").trim() || "default",
        entrySessionProfile,
        source: String(row?.source || "").trim() || "registry",
        enabled: row?.enabled === true,
        inUniverse:
          typeof row?.inUniverse === "boolean" ? row.inUniverse : null,
        promotionEligible:
          typeof row?.promotionEligible === "boolean"
            ? row.promotionEligible
            : false,
        promotionReason: String(row?.promotionReason || "").trim() || null,
        forwardValidation: row?.forwardValidation || null,
        perf30dTrades: null,
        perf30dExpectancyR: null,
        perf30dNetR: null,
        perf30dMaxDrawdownR: null,
        runtime: null,
        promotionGate: row?.promotionGate || null,
      });
      return out;
    }, []);
    return rows.sort((a, b) => {
      if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
      if (a.strategyId !== b.strategyId)
        return a.strategyId.localeCompare(b.strategyId);
      return a.tuneId.localeCompare(b.tuneId);
    });
  }, [scalpSummary?.deployments]);

  const scalpActiveOpsRow =
    (scalpActiveDeploymentId
      ? scalpOpsDeployments.find(
          (row) => row.deploymentId === scalpActiveDeploymentId,
        )
      : null) ||
    scalpOpsDeployments[0] ||
    null;
  const scalpEnabledDeploymentCount = scalpOpsDeployments.filter(
    (row) => row.enabled,
  ).length;
  const scalpPromotionEligibleCount = scalpOpsDeployments.filter(
    (row) => row.enabled && row.promotionEligible,
  ).length;
  const scalpForwardExpectancyRows = scalpOpsDeployments
    .map((row) => asFiniteNumber(row.forwardValidation?.meanExpectancyR))
    .filter((row): row is number => row !== null);
  const scalpForwardProfitablePctRows = scalpOpsDeployments
    .map((row) => asFiniteNumber(row.forwardValidation?.profitableWindowPct))
    .filter((row): row is number => row !== null);
  const scalpMeanForwardExpectancyR = scalpForwardExpectancyRows.length
    ? scalpForwardExpectancyRows.reduce((acc, row) => acc + row, 0) /
      scalpForwardExpectancyRows.length
    : null;
  const scalpMeanForwardProfitablePct = scalpForwardProfitablePctRows.length
    ? scalpForwardProfitablePctRows.reduce((acc, row) => acc + row, 0) /
      scalpForwardProfitablePctRows.length
    : null;
  const scalpOpsByCandidateKey = useMemo(
    () => {
      const map = new Map<string, ScalpOpsDeploymentRow>();
      for (const row of scalpOpsDeployments) {
        const entrySessionProfile =
          row.entrySessionProfile ||
          extractScalpEntrySessionProfileFromDeploymentId(row.deploymentId) ||
          null;
        map.set(
          buildScalpCandidateSessionKey({
            symbol: row.symbol,
            strategyId: row.strategyId,
            tuneId: row.tuneId,
            entrySessionProfile,
          }),
          row,
        );
        const legacyKey = `${String(row.symbol || "")
          .trim()
          .toUpperCase()}~${String(row.strategyId || "")
          .trim()
          .toLowerCase()}~${String(row.tuneId || "")
          .trim()
          .toLowerCase()}`;
        if (!map.has(legacyKey)) map.set(legacyKey, row);
      }
      return map;
    },
    [scalpOpsDeployments],
  );
  const scalpPipelineJobs = (
    Array.isArray(scalpSummary?.jobs) ? scalpSummary.jobs : []
  ).filter((job) => {
    const kind = String(job?.jobKind || "")
      .trim()
      .toLowerCase();
    return (
      kind !== "discover" &&
      kind !== "evaluate" &&
      kind !== "worker" &&
      kind !== "load_candles" &&
      kind !== "prepare" &&
      kind !== "promotion"
    );
  });
  const scalpPanicStop =
    scalpSummary?.panicStop || scalpSummary?.pipeline?.panicStop || null;
  const scalpPanicStopEnabled = scalpPanicStop?.enabled === true;
  const scalpPanicStopReason =
    String(scalpPanicStop?.reason || "").trim() || null;
  const scalpPanicStopUpdatedAtMs = asFiniteNumber(scalpPanicStop?.updatedAtMs);
  const scalpPipelineStatusPanel = (() => {
    const explicit = scalpSummary?.pipeline?.statusPanel || null;
    if (explicit) return explicit;
    const orderedKinds = ["research"];
    const labelsByKind: Record<string, string> = {
      research: "Research",
    };
    const byKind = new Map(
      scalpPipelineJobs.map((job) => [
        String(job?.jobKind || "")
          .trim()
          .toLowerCase(),
        job,
      ]),
    );
    const steps = orderedKinds.map((kind) => {
      const job = byKind.get(kind) || null;
      const status = String(job?.status || "")
        .trim()
        .toLowerCase();
      const queuePending = Math.max(
        0,
        Math.floor(
          Number(job?.queue?.pending || 0) + Number(job?.queue?.retryWait || 0),
        ),
      );
      const queueRunning = Math.max(
        0,
        Math.floor(Number(job?.queue?.running || 0)),
      );
      const queueFailed = Math.max(
        0,
        Math.floor(Number(job?.queue?.failed || 0)),
      );
      const queueSucceeded = Math.max(
        0,
        Math.floor(Number(job?.queue?.succeeded || 0)),
      );
      const hasError =
        status === "failed" || queueFailed > 0 || Boolean(job?.lastError);
      const isRunning =
        Boolean(job?.locked) || status === "running" || queueRunning > 0;
      const isSuccess =
        !hasError &&
        !isRunning &&
        queuePending <= 0 &&
        (queueSucceeded > 0 || asFiniteNumber(job?.lastSuccessAtMs) !== null);
      const state: ScalpPipelineStepState =
        scalpPanicStopEnabled && !isSuccess
          ? "blocked"
          : hasError
            ? "failed"
            : isRunning
              ? "running"
              : isSuccess
                ? "success"
                : "pending";
      const queueLabel = `${queuePending} pending · ${queueRunning} running`;
      return {
        id: kind,
        label: labelsByKind[kind] || kind,
        state,
        detail:
          String(job?.progressLabel || "").trim() ||
          String(job?.lastError || "").trim() ||
          queueLabel,
      };
    });
    const firstFailed = steps.find((step) => step.state === "failed") || null;
    const firstRunning = steps.find((step) => step.state === "running") || null;
    const allSuccess =
      steps.length > 0 && steps.every((step) => step.state === "success");
    const hasPending = steps.some((step) => step.state === "pending");
    const blocked = scalpPanicStopEnabled && !allSuccess && !firstRunning;
    const completedCount = steps.filter(
      (step) => step.state === "success",
    ).length;
    const runningBump = firstRunning ? 0.5 : 0;
    const progressPct =
      steps.length > 0
        ? Math.max(
            0,
            Math.min(
              100,
              ((completedCount + runningBump) / steps.length) * 100,
            ),
          )
        : null;
    if (blocked) {
      return {
        status: "blocked",
        label: "Pipeline paused",
        detail: scalpPanicStopReason || "panic stop enabled",
        updatedAtMs: scalpPanicStopUpdatedAtMs,
        progressPct,
        steps,
      };
    }
    if (firstFailed) {
      return {
        status: "failed",
        label: `${firstFailed.label} failed`,
        detail: firstFailed.detail,
        updatedAtMs: asFiniteNumber(Date.now()),
        progressPct,
        steps,
      };
    }
    if (firstRunning) {
      return {
        status: "running",
        label: `${firstRunning.label} in progress`,
        detail: firstRunning.detail,
        updatedAtMs: asFiniteNumber(Date.now()),
        progressPct,
        steps,
      };
    }
    if (allSuccess) {
      return {
        status: "completed",
        label: "All jobs healthy",
        detail: "No pending backlog.",
        updatedAtMs: asFiniteNumber(Date.now()),
        progressPct: 100,
        steps,
      };
    }
    return {
      status: hasPending ? "idle" : "completed",
      label: hasPending ? "Awaiting pending work" : "All jobs healthy",
      detail: hasPending ? null : "No pending backlog.",
      updatedAtMs: asFiniteNumber(Date.now()),
      progressPct,
      steps,
    };
  })();
  const scalpPipelineStatus = String(scalpPipelineStatusPanel?.status || "idle")
    .trim()
    .toLowerCase() as ScalpPipelineStatus;
  const scalpSummaryWorkerRows = Array.isArray(scalpSummary?.workerRows)
    ? scalpSummary.workerRows
    : [];
  const scalpWorkerTasks = useMemo<ScalpWorkerTask[]>(() => {
    const tasks: ScalpWorkerTask[] = [];
    for (const row of scalpSummaryWorkerRows) {
      const symbol = String(row?.symbol || "")
        .trim()
        .toUpperCase();
      const strategyId = String(row?.strategyId || "").trim();
      if (!symbol || !strategyId) continue;
      const tuneId = String(row?.tuneId || "").trim() || "default";
      const windowFromTs = asFiniteNumber(row?.weekStartMs);
      const windowToTs = asFiniteNumber(row?.weekEndMs);
      if (windowFromTs === null || windowToTs === null) continue;
      const deploymentIdRaw = String(row?.deploymentId || "").trim();
      const entrySessionProfile =
        normalizeScalpEntrySessionProfileUi(row?.entrySessionProfile) ||
        extractScalpEntrySessionProfileFromDeploymentId(deploymentIdRaw) ||
        normalizeScalpEntrySessionProfileUi(scalpSummary?.entrySessionProfile) ||
        normalizeScalpEntrySessionProfileUi(scalpSession) ||
        null;
      const deploymentId =
        deploymentIdRaw ||
        (entrySessionProfile
          ? `${symbol}~${strategyId.toLowerCase()}~${tuneId.toLowerCase()}__sp_${entrySessionProfile}`
          : `${symbol}~${strategyId.toLowerCase()}~${tuneId.toLowerCase()}`);
      if (!symbol || !strategyId) continue;
      const rawStatus = String(row?.status || "pending")
        .trim()
        .toLowerCase();
      const normalizedStatus =
        rawStatus === "succeeded"
          ? "completed"
          : rawStatus === "retry_wait"
            ? "pending"
            : rawStatus;
      tasks.push({
        taskId: `${deploymentId}:${windowFromTs}`,
        symbol,
        strategyId,
        tuneId,
        entrySessionProfile,
        deploymentId,
        workerId: String(row?.workerId || "").trim() || null,
        windowFromTs,
        windowToTs,
        startedAtMs: asFiniteNumber(row?.startedAtMs),
        finishedAtMs: asFiniteNumber(row?.finishedAtMs),
        status: normalizedStatus,
        errorCode: String(row?.errorCode || "").trim() || null,
        errorMessage: String(row?.errorMessage || "").trim() || null,
        result: {
          windowFromTs,
          windowToTs,
          tuneId,
          trades: asFiniteNumber(row?.trades),
          netR: asFiniteNumber(row?.netR),
          expectancyR: asFiniteNumber(row?.expectancyR),
          profitFactor: asFiniteNumber(row?.profitFactor),
          maxDrawdownR: asFiniteNumber(row?.maxDrawdownR),
        },
      });
    }
    return tasks;
  }, [scalpSummaryWorkerRows, scalpSummary?.entrySessionProfile, scalpSession]);
  const scalpWorkerGridTasks = scalpWorkerTasks;
  const scalpWorkerRunningStaleAfterMs = 20 * 60_000;
  const scalpWorkerNowMs = Date.now();
  const scalpWorkerTaskStatusTotals = scalpWorkerTasks.reduce(
    (acc, task) => {
      const status = String(task?.status || "pending")
        .trim()
        .toLowerCase();
      acc.tasks += 1;
      if (status === "completed") acc.completed += 1;
      else if (status === "failed") acc.failed += 1;
      else if (status === "running") {
        const startedAtMs = asFiniteNumber(task?.startedAtMs);
        const staleRunning =
          startedAtMs === null ||
          scalpWorkerNowMs - startedAtMs >= scalpWorkerRunningStaleAfterMs;
        if (staleRunning) {
          acc.pending += 1;
          if (startedAtMs === null) acc.runningMissingStartedAt += 1;
          else acc.runningStale += 1;
        } else {
          acc.running += 1;
        }
      } else acc.pending += 1;
      return acc;
    },
    {
      tasks: 0,
      pending: 0,
      running: 0,
      completed: 0,
      failed: 0,
      runningStale: 0,
      runningMissingStartedAt: 0,
    },
  );
  const scalpWorkerTaskTotalsAvailable = scalpWorkerTaskStatusTotals.tasks > 0;
  const scalpCycleTasks = scalpWorkerTaskTotalsAvailable
    ? scalpWorkerTaskStatusTotals.tasks
    : null;
  const scalpCyclePending = scalpWorkerTaskTotalsAvailable
    ? scalpWorkerTaskStatusTotals.pending
    : null;
  const scalpCycleRunning = scalpWorkerTaskTotalsAvailable
    ? scalpWorkerTaskStatusTotals.running
    : null;
  const scalpCycleCompleted = scalpWorkerTaskTotalsAvailable
    ? scalpWorkerTaskStatusTotals.completed
    : null;
  const scalpCycleFailed = scalpWorkerTaskTotalsAvailable
    ? scalpWorkerTaskStatusTotals.failed
    : null;
  const scalpCycleProgressFromChosenTotals =
    scalpCycleTasks !== null &&
    scalpCycleTasks > 0 &&
    scalpCycleCompleted !== null &&
    scalpCycleFailed !== null
      ? ((scalpCycleCompleted + scalpCycleFailed) / scalpCycleTasks) * 100
      : null;
  const scalpCycleProgressPct = scalpCycleProgressFromChosenTotals;
  const scalpWorkerFinishedTasks =
    scalpCycleCompleted !== null && scalpCycleFailed !== null
      ? scalpCycleCompleted + scalpCycleFailed
      : null;
  const scalpWorkerSuccessRatePct =
    scalpWorkerFinishedTasks !== null &&
    scalpWorkerFinishedTasks > 0 &&
    scalpCycleCompleted !== null
      ? (scalpCycleCompleted / scalpWorkerFinishedTasks) * 100
      : null;
  const scalpUniverseSelectedCount = null;
  const scalpUniverseCandidatesEvaluated = null;

  const scalpJournalRows = Array.isArray(scalpSummary?.journal)
    ? scalpSummary.journal
    : [];
  const formatScalpTime = (ts?: number | null) => {
    const raw = formatDecisionTime(ts);
    return raw ? raw.replace(/^–\s*/, "") : "—";
  };
  const formatScalpDuration = (durationMs?: number | null): string => {
    const ms =
      typeof durationMs === "number" && Number.isFinite(durationMs)
        ? Math.max(0, Math.floor(durationMs))
        : null;
    if (ms === null) return "—";
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60_000) return `${(ms / 1000).toFixed(ms >= 10_000 ? 1 : 2)}s`;
    if (ms < 3_600_000)
      return `${(ms / 60_000).toFixed(ms >= 600_000 ? 1 : 2)}m`;
    return `${(ms / 3_600_000).toFixed(2)}h`;
  };
  const formatScalpCount = (value: number | null): string =>
    value === null ? "—" : `${Math.max(0, Math.floor(value))}`;
  const formatScalpPct = (value: number | null, digits = 0): string =>
    value === null ? "—" : `${value.toFixed(digits)}%`;
  const formatScalpSignedR = (value: number | null): string =>
    value === null ? "—" : `${value >= 0 ? "+" : ""}${value.toFixed(2)}R`;
  const compareScalpWorkerOptionalNumber = (
    a: number | null | undefined,
    b: number | null | undefined,
  ): number => {
    const av = typeof a === "number" && Number.isFinite(a) ? a : null;
    const bv = typeof b === "number" && Number.isFinite(b) ? b : null;
    if (av === null && bv === null) return 0;
    if (av === null) return 1;
    if (bv === null) return -1;
    return av - bv;
  };
  const compareScalpWorkerText = (a: string, b: string): number =>
    String(a || "").localeCompare(String(b || ""), undefined, {
      numeric: true,
      sensitivity: "base",
    });
  const scalpWorkerTaskRows = useMemo(() => {
    const rows = scalpWorkerGridTasks
      .map((task) => {
        const symbol = String(task?.symbol || "")
          .trim()
          .toUpperCase();
        const strategyId = String(task?.strategyId || "").trim();
        const tuneId = String(
          task?.tuneId || task?.result?.tuneId || "default",
        ).trim();
        const taskDeploymentId =
          String(task?.deploymentId || "").trim() || null;
        const taskDeployed =
          typeof task?.deployed === "boolean" ? task.deployed : null;
        const taskDeploymentEnabled =
          typeof task?.deploymentEnabled === "boolean"
            ? task.deploymentEnabled
            : null;
        const taskPromotionEligible =
          typeof task?.promotionEligible === "boolean"
            ? task.promotionEligible
            : null;
        const taskPromotionReason =
          String(task?.promotionReason || "").trim() || null;
        const status = String(task?.status || "pending")
          .trim()
          .toLowerCase();
        const hasCompletedResult = status === "completed" || status === "failed";
        const fromTs =
          asFiniteNumber(task?.windowFromTs) ??
          asFiniteNumber(task?.result?.windowFromTs);
        const toTs =
          asFiniteNumber(task?.windowToTs) ??
          asFiniteNumber(task?.result?.windowToTs);
        const entrySessionProfile =
          normalizeScalpEntrySessionProfileUi(task?.entrySessionProfile) ||
          extractScalpEntrySessionProfileFromDeploymentId(taskDeploymentId) ||
          normalizeScalpEntrySessionProfileUi(scalpSession) ||
          null;
        const candidateSessionKey = buildScalpCandidateSessionKey({
          symbol,
          strategyId,
          tuneId,
          entrySessionProfile,
        });
        const legacyCandidateKey = `${symbol}~${strategyId}~${tuneId}`;
        const deploymentRow =
          scalpOpsByCandidateKey.get(candidateSessionKey) ||
          scalpOpsByCandidateKey.get(legacyCandidateKey) ||
          null;
        const forwardValidation = deploymentRow?.forwardValidation || null;
        const deploymentId =
          taskDeploymentId || deploymentRow?.deploymentId || null;
        const deployed = taskDeployed ?? Boolean(deploymentId);
        const deploymentEnabled =
          taskDeploymentEnabled ??
          (deploymentRow ? Boolean(deploymentRow.enabled) : null);
        const promotionEligible =
          taskPromotionEligible ??
          deploymentRow?.promotionEligible ??
          null;
        const whyNotPromoted =
          promotionEligible === true
            ? "eligible"
            : taskPromotionReason ||
              deploymentRow?.promotionReason ||
              (deployed ? "promotion_unknown" : "not_deployed");
        return {
          taskId: String(task?.taskId || "").trim(),
          symbol,
          strategyId,
          tuneId,
          entrySessionProfile,
          deploymentId,
          forwardValidation,
          deployed,
          deploymentEnabled,
          promotionEligible,
          whyNotPromoted,
          status,
          windowFromTs: fromTs,
          windowToTs: toTs,
          trades: hasCompletedResult
            ? asFiniteNumber(task?.result?.trades)
            : null,
          netR: hasCompletedResult ? asFiniteNumber(task?.result?.netR) : null,
          expectancyR: hasCompletedResult
            ? asFiniteNumber(task?.result?.expectancyR)
            : null,
          profitFactor: hasCompletedResult
            ? asFiniteNumber(task?.result?.profitFactor)
            : null,
          maxDrawdownR: hasCompletedResult
            ? asFiniteNumber(task?.result?.maxDrawdownR)
            : null,
          errorCode: String(task?.errorCode || "").trim() || null,
          errorMessage: String(task?.errorMessage || "").trim() || null,
        };
      })
      .filter((row) => row.symbol && row.strategyId);
    return rows.slice().sort((a, b) => {
      let cmp = 0;
      switch (scalpWorkerSort.key) {
        case "symbol":
          cmp = compareScalpWorkerText(a.symbol, b.symbol);
          break;
        case "strategyId":
          cmp = compareScalpWorkerText(a.strategyId, b.strategyId);
          break;
        case "tuneId":
          cmp = compareScalpWorkerText(a.tuneId, b.tuneId);
          break;
        case "whyNotPromoted":
          cmp = compareScalpWorkerText(a.whyNotPromoted, b.whyNotPromoted);
          break;
        case "windowToTs":
          cmp = compareScalpWorkerOptionalNumber(a.windowToTs, b.windowToTs);
          break;
        case "status":
          cmp = compareScalpWorkerText(a.status, b.status);
          break;
        case "trades":
          cmp = compareScalpWorkerOptionalNumber(a.trades, b.trades);
          break;
        case "netR":
          cmp = compareScalpWorkerOptionalNumber(a.netR, b.netR);
          break;
        case "expectancyR":
          cmp = compareScalpWorkerOptionalNumber(a.expectancyR, b.expectancyR);
          break;
        case "profitFactor":
          cmp = compareScalpWorkerOptionalNumber(
            a.profitFactor,
            b.profitFactor,
          );
          break;
        case "maxDrawdownR":
          cmp = compareScalpWorkerOptionalNumber(
            a.maxDrawdownR,
            b.maxDrawdownR,
          );
          break;
        default:
          cmp = 0;
          break;
      }
      if (cmp !== 0) {
        return scalpWorkerSort.direction === "asc" ? cmp : -cmp;
      }
      const aTo = a.windowToTs ?? 0;
      const bTo = b.windowToTs ?? 0;
      if (bTo !== aTo) return bTo - aTo;
      if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
      if (a.strategyId !== b.strategyId)
        return a.strategyId.localeCompare(b.strategyId);
      return a.tuneId.localeCompare(b.tuneId);
    });
  }, [
    scalpWorkerGridTasks,
    scalpOpsByCandidateKey,
    scalpSession,
    scalpWorkerSort.key,
    scalpWorkerSort.direction,
  ]);
  const scalpWorkerLatestRunSummary = (() => {
    const completedTaskRuns = scalpWorkerTasks
      .map((task) => {
        const workerId = String(task?.workerId || "").trim();
        const symbol = String(task?.symbol || "")
          .trim()
          .toUpperCase();
        const startedAtMs = asFiniteNumber(task?.startedAtMs);
        const finishedAtMs = asFiniteNumber(task?.finishedAtMs);
        if (
          !workerId ||
          !symbol ||
          startedAtMs === null ||
          finishedAtMs === null ||
          finishedAtMs < startedAtMs
        ) {
          return null;
        }
        return { workerId, symbol, startedAtMs, finishedAtMs };
      })
      .filter(
        (
          row,
        ): row is {
          workerId: string;
          symbol: string;
          startedAtMs: number;
          finishedAtMs: number;
        } => row !== null,
      );
    if (!completedTaskRuns.length) return null;

    const latestTask =
      completedTaskRuns
        .slice()
        .sort((a, b) => b.finishedAtMs - a.finishedAtMs)[0] || null;
    if (!latestTask) return null;

    const sameRunTasks = completedTaskRuns.filter(
      (row) => row.workerId === latestTask.workerId,
    );
    if (!sameRunTasks.length) return null;

    const runStartedAtMs = Math.min(
      ...sameRunTasks.map((row) => row.startedAtMs),
    );
    const runFinishedAtMs = Math.max(
      ...sameRunTasks.map((row) => row.finishedAtMs),
    );
    if (
      !Number.isFinite(runStartedAtMs) ||
      !Number.isFinite(runFinishedAtMs) ||
      runFinishedAtMs < runStartedAtMs
    ) {
      return null;
    }
    const durationMs = runFinishedAtMs - runStartedAtMs;
    const symbols = new Set(
      sameRunTasks.map((row) => String(row.symbol || "").trim().toUpperCase()),
    );
    return {
      durationMs,
      taskCount: sameRunTasks.length,
      symbolCount: Array.from(symbols).filter((symbol) => Boolean(symbol)).length,
      finishedAtMs: runFinishedAtMs,
    };
  })();
  const scalpWorkerLastDurationFromTasksMs =
    scalpWorkerLatestRunSummary?.durationMs ?? null;
  const scalpWorkerLatestThroughputTasksPerMin =
    scalpWorkerLatestRunSummary &&
    scalpWorkerLatestRunSummary.durationMs > 0 &&
    scalpWorkerLatestRunSummary.taskCount > 0
      ? scalpWorkerLatestRunSummary.taskCount /
        (scalpWorkerLatestRunSummary.durationMs / 60_000)
      : null;
  const scalpWorkerPerformanceSummary = useMemo(() => {
    const processedSymbols = new Set<string>();
    let completedRows = 0;
    let netRSum = 0;
    let netRCount = 0;
    let expectancySum = 0;
    let expectancyCount = 0;
    for (const row of scalpWorkerTaskRows) {
      const status = String(row?.status || "")
        .trim()
        .toLowerCase();
      const symbol = String(row?.symbol || "")
        .trim()
        .toUpperCase();
      if ((status === "completed" || status === "failed") && symbol) {
        processedSymbols.add(symbol);
      }
      if (status !== "completed") continue;
      completedRows += 1;
      if (typeof row?.netR === "number" && Number.isFinite(row.netR)) {
        netRSum += row.netR;
        netRCount += 1;
      }
      if (
        typeof row?.expectancyR === "number" &&
        Number.isFinite(row.expectancyR)
      ) {
        expectancySum += row.expectancyR;
        expectancyCount += 1;
      }
    }
    return {
      processedSymbolCount: processedSymbols.size,
      completedTaskRows: completedRows,
      avgNetRPerCompletedTask: netRCount > 0 ? netRSum / netRCount : null,
      avgExpectancyPerCompletedTask:
        expectancyCount > 0 ? expectancySum / expectancyCount : null,
    };
  }, [scalpWorkerTaskRows]);
  const scalpResearchCursors = Array.isArray(scalpSummary?.researchCursors)
    ? scalpSummary.researchCursors
    : [];
  const scalpResearchHighlights = Array.isArray(scalpSummary?.researchHighlights)
    ? scalpSummary.researchHighlights
    : [];
  const scalpResearchCursorTotalOffset = scalpResearchCursors.reduce(
    (sum, c) => sum + Math.max(0, Math.floor(Number(c?.lastCandidateOffset || 0))),
    0,
  );
  const scalpResearchCursorCount = scalpResearchCursors.length;
  const scalpResearchHighlightCount = scalpResearchHighlights.filter(
    (h) => h?.remarkable === true,
  ).length;
  const scalpResearchLatestCursorAtMs = scalpResearchCursors.reduce(
    (max, c) => Math.max(max, Number(c?.updatedAtMs || 0)),
    0,
  ) || null;
  const scalpWorkerCompactStats = [
    {
      id: "cursors",
      label: "scopes",
      value: formatScalpCount(scalpResearchCursorCount),
    },
    {
      id: "explored",
      label: "explored",
      value: formatScalpCount(scalpResearchCursorTotalOffset),
    },
    {
      id: "candidates",
      label: "candidates",
      value: formatScalpCount(scalpWorkerPerformanceSummary.processedSymbolCount),
    },
    {
      id: "stageC",
      label: "stage C pass",
      value: formatScalpPct(scalpWorkerSuccessRatePct, 0),
    },
    {
      id: "highlights",
      label: "highlights",
      value: formatScalpCount(scalpResearchHighlightCount),
    },
    {
      id: "netr",
      label: "avg netR",
      value: formatScalpSignedR(
        scalpWorkerPerformanceSummary.avgNetRPerCompletedTask,
      ),
    },
    {
      id: "exp",
      label: "avg exp",
      value: formatScalpSignedR(
        scalpWorkerPerformanceSummary.avgExpectancyPerCompletedTask,
      ),
    },
  ] as const;
  const scalpWorkerCompactStatusLine = `scopes ${formatScalpCount(
    scalpResearchCursorCount,
  )} · explored ${formatScalpCount(
    scalpResearchCursorTotalOffset,
  )} · highlights ${formatScalpCount(scalpResearchHighlightCount)} · weeks ${formatScalpCount(
    scalpWorkerTaskStatusTotals.completed,
  )} pass / ${formatScalpCount(scalpWorkerTaskStatusTotals.failed)} fail`;
  const formatScalpWindowIso = (
    fromTs: number | null,
    toTs: number | null,
  ): string => {
    if (fromTs === null || toTs === null) return "—";
    const fromIso = new Date(fromTs).toISOString().slice(0, 10);
    const toIso = new Date(toTs).toISOString().slice(0, 10);
    return `${fromIso} → ${toIso}`;
  };
  const scalpCronRuntimeById: Record<string, ScalpCronRuntimeMeta> =
    buildScalpCronRuntimeMap(scalpCronNowMs, scalpSession);
  const scalpCronRuntimeMeta = (id: string): ScalpCronRuntimeMeta =>
    scalpCronRuntimeById[id] || {
      expressions: [],
      expressionLabel: null,
      nextRunAtMs: null,
      invokePath: null,
    };
  const scalpCronRowsFromJobs: ScalpOpsCronRow[] = scalpPipelineJobs.map((job) => {
        const rawKind = String(job?.jobKind || "")
          .trim()
          .toLowerCase();
        const rowId =
          rawKind === "research"
            ? "scalp_research"
            : rawKind === "promote"
              ? "scalp_promote"
              : rawKind === "execute"
                ? "scalp_execute"
                : rawKind === "reconcile"
                  ? "scalp_reconcile"
                  : rawKind === "cycle"
                    ? "scalp_cycle"
                    : `scalp_${rawKind || "job"}`;
        const queuePending = Math.max(
          0,
          Math.floor(Number(job?.queue?.pending || 0)),
        );
        const queueRunning = Math.max(
          0,
          Math.floor(Number(job?.queue?.running || 0)),
        );
        const queueRetry = Math.max(
          0,
          Math.floor(Number(job?.queue?.retryWait || 0)),
        );
        const queueFailed = Math.max(
          0,
          Math.floor(Number(job?.queue?.failed || 0)),
        );
        const queueSucceeded = Math.max(
          0,
          Math.floor(Number(job?.queue?.succeeded || 0)),
        );
        const jobNextRunAtMs = asFiniteNumber(job?.nextRunAtMs);
        const cronNextRunAtMs = scalpCronRuntimeMeta(rowId).nextRunAtMs;
        const invokePath = scalpCronRuntimeMeta(rowId).invokePath;
        const statusRaw = String(job?.status || "")
          .trim()
          .toLowerCase();
        const jobRunningNow =
          statusRaw === "running" || Boolean(job?.locked) || queueRunning > 0;
        const nextRunAtMs =
          !jobRunningNow &&
          jobNextRunAtMs !== null &&
          jobNextRunAtMs > scalpCronNowMs
            ? jobNextRunAtMs
            : cronNextRunAtMs;
        const status: ScalpOpsCronStatus =
          statusRaw === "running" || Boolean(job?.locked)
            ? "healthy"
            : statusRaw === "failed"
              ? "lagging"
              : "unknown";
        const queueTotal =
          queuePending +
          queueRunning +
          queueRetry +
          queueFailed +
          queueSucceeded;
        const queueExecutionTotal =
          queueRunning + queuePending + queueRetry + queueFailed;
        const completedWork = queueSucceeded + queueFailed;
        const progressDenominator =
          completedWork + queuePending + queueRunning + queueRetry;
        const progressPct =
          progressDenominator > 0
            ? (completedWork / progressDenominator) * 100
            : null;
        const successDenominator = queueSucceeded + queueFailed;
        const successRatePct =
          successDenominator > 0
            ? (queueSucceeded / successDenominator) * 100
            : null;
        return {
          id: rowId,
          cadence: "State-driven",
          cronExpression: scalpCronRuntimeMeta(rowId).expressionLabel,
          nextRunAtMs,
          invokePath,
          role: rawKind.replace(/_/g, " "),
          status,
          lastRunAtMs: asFiniteNumber(job?.lastRunAtMs),
          lastDurationMs: null,
          details: [
            {
              label: "Status",
              value: statusRaw || "idle",
              tone:
                statusRaw === "failed"
                  ? "critical"
                  : statusRaw === "running"
                    ? "warning"
                    : "neutral",
            },
            {
              label: "Queue",
              value: `${queuePending} pending · ${queueRunning} running · ${queueRetry} retry`,
              tone:
                queueFailed > 0
                  ? "warning"
                  : queueRunning > 0
                    ? "neutral"
                    : "positive",
            },
            {
              label: "Failed",
              value: `${queueFailed}`,
              tone: queueFailed > 0 ? "critical" : "neutral",
            },
            {
              label: "Succeeded",
              value: `${queueSucceeded}`,
              tone: queueSucceeded > 0 ? "positive" : "neutral",
            },
            {
              label: "Last Run",
              value: formatScalpTime(asFiniteNumber(job?.lastRunAtMs)),
              tone: "neutral",
            },
            {
              label: "Next Run",
              value: formatScalpNextRunIn(nextRunAtMs, scalpCronNowMs),
              tone: "neutral",
            },
            {
              label: "Error",
              value: String(job?.lastError || "").trim() || "none",
              tone: job?.lastError ? "critical" : "neutral",
            },
          ],
          visualMetrics: [
            {
              label: "Progress",
              valueLabel:
                progressPct === null ? "—" : `${progressPct.toFixed(0)}%`,
              pct: progressPct,
              tone:
                progressPct === null
                  ? "neutral"
                  : progressPct >= 100
                    ? "positive"
                    : queueRunning > 0
                      ? "warning"
                      : "neutral",
            },
            {
              label: "Success Rate",
              valueLabel:
                successRatePct === null ? "—" : `${successRatePct.toFixed(0)}%`,
              pct: successRatePct,
              tone:
                successRatePct === null
                  ? "neutral"
                  : successRatePct >= 70
                    ? "positive"
                    : successRatePct >= 40
                      ? "warning"
                      : "critical",
            },
            {
              label: "Failed",
              valueLabel: `${queueFailed}`,
              pct: queueTotal > 0 ? (queueFailed / queueTotal) * 100 : null,
              tone: queueFailed > 0 ? "critical" : "neutral",
            },
          ],
          resultPreview: {
            progressLabel: job?.progressLabel || null,
            progress: job?.progress || null,
            queue: job?.queue || null,
          },
        } satisfies ScalpOpsCronRow;
      });
  const scalpCronRows: ScalpOpsCronRow[] = (() => {
    // Filter out legacy job kinds that no longer have pipeline definitions
    const activeJobRows = scalpCronRowsFromJobs.filter(
      (row) => row.id in SCALP_CRON_PIPELINE_DEFINITIONS,
    );
    const byId = new Map<string, ScalpOpsCronRow>(
      activeJobRows.map((row) => [row.id, row] as const),
    );
    const rows = [...activeJobRows];
    for (const [id] of Object.entries(SCALP_CRON_PIPELINE_DEFINITIONS)) {
      if (byId.has(id)) continue;
      const runtimeMeta = scalpCronRuntimeMeta(id);
      rows.push({
        id,
        cadence: "Schedule-driven",
        cronExpression: runtimeMeta.expressionLabel,
        nextRunAtMs: runtimeMeta.nextRunAtMs,
        invokePath: runtimeMeta.invokePath,
        role: id.replace(/^scalp_/, "").replace(/_/g, " "),
        status: "unknown",
        lastRunAtMs: null,
        lastDurationMs: null,
        details: [
          {
            label: "Status",
            value: "not observed",
            tone: "neutral",
          },
          {
            label: "Next Run",
            value: formatScalpNextRunIn(runtimeMeta.nextRunAtMs, scalpCronNowMs),
            tone: "neutral",
          },
        ],
        visualMetrics: [],
        resultPreview: null,
      });
    }
    return rows;
  })();
  const scalpRunningJobRowIds = new Set(
    scalpPipelineJobs
      .filter((job) => {
        const status = String(job?.status || "")
          .trim()
          .toLowerCase();
        return (
          Boolean(job?.locked) ||
          status === "running" ||
          Math.max(0, Math.floor(Number(job?.queue?.running || 0))) > 0
        );
      })
      .map((job) => {
        const kind = String(job?.jobKind || "")
          .trim()
          .toLowerCase();
        if (kind === "research") return "scalp_research";
        if (kind === "promote") return "scalp_promote";
        if (kind === "execute") return "scalp_execute";
        if (kind === "reconcile") return "scalp_reconcile";
        if (kind === "cycle") return "scalp_cycle";
        return `scalp_${kind || "job"}`;
      }),
  );
  const scalpIsCronRowInProgress = (rowId: string): boolean => {
    const invokeRunning = Boolean(scalpCronInvokeStateById[rowId]?.running);
    if (invokeRunning) return true;
    if (scalpRunningJobRowIds.has(rowId)) return true;
    return false;
  };
  const scalpInProgressCronRows = scalpCronRows.filter((row) =>
    scalpIsCronRowInProgress(row.id),
  );
  const scalpInProgressCronLabel =
    scalpInProgressCronRows.length > 0
      ? scalpInProgressCronRows.map((row) => row.id).join(", ")
      : "none";
  const scalpExpandedCronRow =
    (scalpExpandedCronId
      ? scalpCronRows.find((row) => row.id === scalpExpandedCronId)
      : null) || null;
  const scalpCronRowForStep = (stepId?: string | null): ScalpOpsCronRow | null => {
    const normalized = String(stepId || "")
      .trim()
      .toLowerCase();
    if (!normalized) return null;
    if (normalized === "research")
      return scalpCronRows.find((row) => row.id.includes("scalp_research")) || null;
    if (normalized === "promote" || normalized === "promotion")
      return scalpCronRows.find((row) => row.id.includes("scalp_promote")) || null;
    if (normalized.includes("execute"))
      return scalpCronRows.find((row) => row.id.includes("scalp_execute")) || null;
    if (normalized.includes("reconcile"))
      return (
        scalpCronRows.find((row) => row.id.includes("scalp_reconcile")) || null
      );
    if (normalized.includes("cycle"))
      return scalpCronRows.find((row) => row.id.includes("scalp_cycle")) || null;
    return (
      scalpCronRows.find((row) => row.id.includes(normalized)) ||
      scalpCronRows.find((row) => row.role.includes(normalized)) ||
      null
    );
  };
  const scalpActiveExecutionTs =
    scalpActiveExecution && typeof scalpActiveExecution.timestampMs === "number"
      ? scalpActiveExecution.timestampMs
      : null;
  const scalpActiveRuntimeRow =
    scalpActiveRow || scalpActiveOpsRow?.runtime || null;
  const scalpDarkMode = resolvedTheme === "dark";
  const scalpSectionShellClass = scalpDarkMode
    ? "rounded-3xl border border-zinc-700 bg-zinc-900 text-zinc-100"
    : "rounded-3xl border border-slate-200 bg-white text-slate-900";
  const scalpCardClass = scalpDarkMode
    ? "rounded-2xl border border-zinc-700 bg-zinc-950/70 p-3"
    : "rounded-2xl border border-slate-200 bg-white p-3";
  const scalpTextPrimaryClass = scalpDarkMode
    ? "text-zinc-100"
    : "text-slate-900";
  const scalpTextSecondaryClass = scalpDarkMode
    ? "text-zinc-300"
    : "text-slate-600";
  const scalpTextMutedClass = scalpDarkMode
    ? "text-zinc-400"
    : "text-slate-500";
  const scalpTableHeaderClass = scalpDarkMode
    ? "text-zinc-400"
    : "text-slate-500";
  const scalpTableRowClass = scalpDarkMode
    ? "bg-zinc-950/85 hover:bg-zinc-800/85"
    : "bg-slate-50 hover:bg-slate-100";
  const scalpTagNeutralClass = scalpDarkMode
    ? "rounded-full border border-zinc-600 bg-zinc-800 px-2 py-1 text-[11px] text-zinc-200"
    : "rounded-full border border-slate-300 bg-white px-2 py-1 text-[11px] text-slate-600";
  const scalpCronExpandedPanelClass = scalpDarkMode
    ? "mx-2 mb-2 rounded-2xl border border-zinc-700 bg-zinc-900/95 p-3"
    : "mx-2 mb-2 rounded-2xl border border-slate-200 bg-white p-3";
  const scalpCronPreviewClass = scalpDarkMode
    ? "max-h-64 overflow-auto rounded-xl border border-zinc-700 bg-zinc-950/80 p-2 font-mono text-[11px] text-zinc-300"
    : "max-h-64 overflow-auto rounded-xl border border-slate-200 bg-slate-50 p-2 font-mono text-[11px] text-slate-700";
  const scalpWorkerJobsGridThemeClass = scalpDarkMode
    ? "ag-theme-quartz-dark scalp-grid-muted-dark"
    : "ag-theme-quartz";
  const scalpBerlinClockParts = useMemo(
    () => readClockPartsInTimeZone(scalpCronNowMs, BERLIN_TZ),
    [scalpCronNowMs],
  );
  const scalpBerlinDayKey = useMemo(
    () => formatDayKeyFromClockParts(scalpBerlinClockParts),
    [scalpBerlinClockParts],
  );
  const scalpBerlinNowMinuteOfDay = useMemo(
    () =>
      Math.max(0, Math.min(1439, scalpBerlinClockParts.hh * 60 + scalpBerlinClockParts.mm)),
    [scalpBerlinClockParts],
  );
  const scalpBerlinNowLabel = useMemo(
    () =>
      `${String(scalpBerlinClockParts.hh).padStart(2, "0")}:${String(
        scalpBerlinClockParts.mm,
      ).padStart(2, "0")}`,
    [scalpBerlinClockParts],
  );
  const scalpBerlinDayStartMs = useMemo(() => {
    const fromTz = utcMsFromZonedDayClock(scalpBerlinDayKey, "00:00", BERLIN_TZ);
    if (typeof fromTz === "number" && Number.isFinite(fromTz)) return fromTz;
    return scalpCronNowMs - scalpBerlinNowMinuteOfDay * 60_000;
  }, [scalpBerlinDayKey, scalpCronNowMs, scalpBerlinNowMinuteOfDay]);
  const scalpSessionTimelineTracks = useMemo(
    () =>
      SCALP_SESSION_TIMELINE_COLORS.map((meta) => {
        const segments: Array<{ startMinute: number; endMinute: number }> = [];
        let activeStart: number | null = null;
        for (let minute = 0; minute < 1440; minute += 1) {
          const tsMs = scalpBerlinDayStartMs + minute * 60_000;
          const active = inScalpEntrySessionProfileWindow(tsMs, meta.id);
          if (active && activeStart === null) {
            activeStart = minute;
            continue;
          }
          if (!active && activeStart !== null) {
            segments.push({ startMinute: activeStart, endMinute: minute });
            activeStart = null;
          }
        }
        if (activeStart !== null) {
          segments.push({ startMinute: activeStart, endMinute: 1440 });
        }
        return {
          ...meta,
          segments,
        };
      }),
    [scalpBerlinDayStartMs],
  );
  const scalpSessionTimelineNowPct = useMemo(
    () => (scalpBerlinNowMinuteOfDay / 1440) * 100,
    [scalpBerlinNowMinuteOfDay],
  );
  const scalpPipelineStepIcon = (id?: string | null): LucideIcon => {
    const normalized = String(id || "")
      .trim()
      .toLowerCase();
    if (normalized.includes("research")) return Radar;
    if (normalized.includes("load")) return Globe2;
    if (normalized.includes("execute")) return CandlestickChart;
    if (normalized.includes("monitor")) return Activity;
    if (normalized.includes("panic") || normalized.includes("stop"))
      return PauseCircle;
    return TimerReset;
  };
  const scalpPipelineFlowSteps = (
    Array.isArray(scalpPipelineStatusPanel?.steps)
      ? scalpPipelineStatusPanel.steps
      : []
  ).filter((step) => {
    const id = String(step?.id || "")
      .trim()
      .toLowerCase();
    return (
      id !== "discover" &&
      id !== "evaluate" &&
      id !== "worker" &&
      id !== "load_candles" &&
      id !== "prepare" &&
      id !== "promotion"
    );
  });
  const scalpPipelineStepVisualMeta = (state?: ScalpPipelineStepState) => {
    if (state === "success") {
      return {
        badge: scalpCronDetailToneMeta("positive"),
        fill: scalpDarkMode ? "bg-emerald-400" : "bg-emerald-500",
        label: "ok",
      };
    }
    if (state === "running") {
      return {
        badge: scalpCronDetailToneMeta("warning"),
        fill: scalpDarkMode ? "bg-amber-400" : "bg-amber-500",
        label: "run",
      };
    }
    if (state === "failed" || state === "blocked") {
      return {
        badge: scalpCronDetailToneMeta("critical"),
        fill: scalpDarkMode ? "bg-rose-400" : "bg-rose-500",
        label: state === "blocked" ? "halt" : "fail",
      };
    }
    return {
      badge: scalpCronDetailToneMeta("neutral"),
      fill: scalpDarkMode ? "bg-zinc-400" : "bg-slate-500",
      label: "wait",
    };
  };
  const scalpWorkerJobRankScore = (row: ScalpWorkerJobGridRow): number => {
    const forwardValidation = row.forwardValidation || null;
    const primaryExpectancy =
      asFiniteNumber(forwardValidation?.meanExpectancyR) ??
      asFiniteNumber(row.expectancyR) ??
      0;
    const primaryMedianExpectancy =
      asFiniteNumber(forwardValidation?.weeklyMedianExpectancyR) ??
      primaryExpectancy;
    const profitablePct =
      asFiniteNumber(forwardValidation?.profitableWindowPct) ?? 0;
    const concentrationPct = Math.max(
      0,
      (asFiniteNumber(forwardValidation?.weeklyTopWeekPnlConcentrationPct) ??
        0) - 50,
    );
    const profitFactor =
      asFiniteNumber(forwardValidation?.meanProfitFactor) ??
      asFiniteNumber(row.profitFactor) ??
      0;
    const maxDrawdown =
      asFiniteNumber(forwardValidation?.maxDrawdownR) ??
      asFiniteNumber(row.totalMaxDrawdownR) ??
      0;
    const smoothedExpectancy =
      (primaryExpectancy + primaryMedianExpectancy) / 2;
    let score = smoothedExpectancy * (1 - concentrationPct / 100);
    score += profitablePct / 100;
    score += Math.min(profitFactor, 25) * 0.02;
    score -= maxDrawdown * 0.08;
    return score;
  };
  const compareScalpWorkerJobGridRows = (
    a: ScalpWorkerJobGridRow,
    b: ScalpWorkerJobGridRow,
  ): number => {
    const aScore = scalpWorkerJobRankScore(a);
    const bScore = scalpWorkerJobRankScore(b);
    if (bScore !== aScore) return bScore - aScore;

    const aProfitablePct =
      asFiniteNumber(a.forwardValidation?.profitableWindowPct) ?? 0;
    const bProfitablePct =
      asFiniteNumber(b.forwardValidation?.profitableWindowPct) ?? 0;
    if (bProfitablePct !== aProfitablePct)
      return bProfitablePct - aProfitablePct;

    const aProfitFactor =
      asFiniteNumber(a.forwardValidation?.meanProfitFactor) ??
      asFiniteNumber(a.profitFactor) ??
      Number.NEGATIVE_INFINITY;
    const bProfitFactor =
      asFiniteNumber(b.forwardValidation?.meanProfitFactor) ??
      asFiniteNumber(b.profitFactor) ??
      Number.NEGATIVE_INFINITY;
    if (bProfitFactor !== aProfitFactor) return bProfitFactor - aProfitFactor;

    const aDrawdown =
      asFiniteNumber(a.forwardValidation?.maxDrawdownR) ??
      asFiniteNumber(a.totalMaxDrawdownR) ??
      Number.POSITIVE_INFINITY;
    const bDrawdown =
      asFiniteNumber(b.forwardValidation?.maxDrawdownR) ??
      asFiniteNumber(b.totalMaxDrawdownR) ??
      Number.POSITIVE_INFINITY;
    if (aDrawdown !== bDrawdown) return aDrawdown - bDrawdown;

    const aNetR = asFiniteNumber(a.totalNetR) ?? Number.NEGATIVE_INFINITY;
    const bNetR = asFiniteNumber(b.totalNetR) ?? Number.NEGATIVE_INFINITY;
    if (bNetR !== aNetR) return bNetR - aNetR;

    if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
    if (a.strategyId !== b.strategyId)
      return a.strategyId.localeCompare(b.strategyId);
    if (a.tuneId !== b.tuneId) return a.tuneId.localeCompare(b.tuneId);
    return String(a.deploymentId || "").localeCompare(
      String(b.deploymentId || ""),
    );
  };
  const scalpWorkerJobsGridRows = useMemo<ScalpWorkerJobGridRow[]>(() => {
    type MutableGridRow = ScalpWorkerJobGridRow & {
      statusCounts: {
        completed: number;
        failed: number;
        running: number;
        pending: number;
      };
      windows: Array<{
        sortTs: number;
        netRValue: number | null;
        netRDisplay: string;
        tooltipText: string;
      }>;
      expectancyWeightedSum: number;
      expectancyWeightedTrades: number;
      expectancySum: number;
      expectancyCount: number;
      profitFactorSum: number;
      profitFactorCount: number;
      errorCodeSet: Set<string>;
    };

    const byKey = new Map<string, MutableGridRow>();
    for (const row of scalpWorkerTaskRows) {
      const entrySessionProfile =
        normalizeScalpEntrySessionProfileUi(row.entrySessionProfile) ||
        extractScalpEntrySessionProfileFromDeploymentId(row.deploymentId) ||
        normalizeScalpEntrySessionProfileUi(scalpSession) ||
        null;
      const candidateSessionKey = buildScalpCandidateSessionKey({
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        entrySessionProfile,
      });
      const legacyCandidateKey = `${String(row.symbol || "")
        .trim()
        .toUpperCase()}~${String(row.strategyId || "")
        .trim()
        .toLowerCase()}~${String(row.tuneId || "")
        .trim()
        .toLowerCase()}`;
      const key = row.deploymentId || candidateSessionKey || legacyCandidateKey;
      const windowLabel =
        row.windowFromTs === null || row.windowToTs === null
          ? "—"
          : `${new Date(row.windowFromTs).toISOString().slice(0, 10)} → ${new Date(
              Math.max(row.windowFromTs, row.windowToTs - 1),
            )
              .toISOString()
              .slice(0, 10)}`;
      const netRValue = row.netR;
      const netRDisplay =
        netRValue === null
          ? "—"
          : `${netRValue >= 0 ? "+" : ""}${netRValue.toFixed(2)}R`;
      const tooltipText = [
        `Window:${windowLabel}`,
        row.trades !== null ? `T:${Math.max(0, Math.floor(row.trades))}` : null,
        row.netR !== null
          ? `Net:${row.netR >= 0 ? "+" : ""}${row.netR.toFixed(2)}R`
          : null,
        row.expectancyR !== null
          ? `Exp:${row.expectancyR >= 0 ? "+" : ""}${row.expectancyR.toFixed(3)}R`
          : null,
        row.profitFactor !== null ? `PF:${row.profitFactor.toFixed(2)}` : null,
        row.maxDrawdownR !== null ? `DD:${row.maxDrawdownR.toFixed(2)}R` : null,
        row.errorCode ? `ERR:${row.errorCode}` : null,
      ]
        .filter((part): part is string => Boolean(part))
        .join(" | ");

      const current = byKey.get(key);
      if (!current) {
        const status = String(row.status || "pending")
          .trim()
          .toLowerCase();
        const statusCounts = {
          completed: status === "completed" ? 1 : 0,
          failed: status === "failed" ? 1 : 0,
          running: status === "running" ? 1 : 0,
          pending:
            status !== "completed" &&
            status !== "failed" &&
            status !== "running"
              ? 1
              : 0,
        };
        const errorCodeSet = new Set<string>();
        if (row.errorCode) errorCodeSet.add(row.errorCode);
        byKey.set(key, {
          rowId: key,
          deploymentId: row.deploymentId || null,
          entrySessionProfile,
          workerOnly: false,
          symbol: row.symbol,
          strategyId: row.strategyId,
          tuneId: row.tuneId,
          forwardValidation: row.forwardValidation,
          deployed: row.deployed,
          deploymentEnabled: row.deploymentEnabled,
          promotionEligible: row.promotionEligible,
          reason: row.whyNotPromoted,
          status: "",
          windowCount: 1,
          windowsResults: "",
          windowNetRs: [],
          trades: row.trades,
          netR: row.netR,
          totalNetR: row.netR,
          expectancyR: row.expectancyR,
          profitFactor: row.profitFactor,
          maxDrawdownR: row.maxDrawdownR,
          totalMaxDrawdownR: row.maxDrawdownR,
          maxWeeklyNetR: null,
          largestTradeR: null,
          exitReasons: null,
          errorCodes: null,
          statusCounts,
          windows: [
            {
              sortTs: row.windowToTs ?? row.windowFromTs ?? 0,
              netRValue,
              netRDisplay,
              tooltipText,
            },
          ],
          expectancyWeightedSum:
            row.expectancyR !== null && row.trades !== null && row.trades > 0
              ? row.expectancyR * row.trades
              : 0,
          expectancyWeightedTrades:
            row.trades !== null && row.trades > 0 ? row.trades : 0,
          expectancySum: row.expectancyR ?? 0,
          expectancyCount: row.expectancyR !== null ? 1 : 0,
          profitFactorSum: row.profitFactor ?? 0,
          profitFactorCount: row.profitFactor !== null ? 1 : 0,
          errorCodeSet,
        });
        continue;
      }

      current.deployed = current.deployed || row.deployed;
      if (!current.deploymentId && row.deploymentId)
        current.deploymentId = row.deploymentId;
      if (!current.entrySessionProfile && entrySessionProfile)
        current.entrySessionProfile = entrySessionProfile;
      if (!current.forwardValidation && row.forwardValidation)
        current.forwardValidation = row.forwardValidation;
      if (current.deploymentEnabled === null && row.deploymentEnabled !== null)
        current.deploymentEnabled = row.deploymentEnabled;
      if (current.promotionEligible === null && row.promotionEligible !== null)
        current.promotionEligible = row.promotionEligible;
      if (current.reason === "eligible" && row.whyNotPromoted !== "eligible")
        current.reason = row.whyNotPromoted;

      const status = String(row.status || "pending")
        .trim()
        .toLowerCase();
      if (status === "completed") current.statusCounts.completed += 1;
      else if (status === "failed") current.statusCounts.failed += 1;
      else if (status === "running") current.statusCounts.running += 1;
      else current.statusCounts.pending += 1;

      current.windowCount += 1;
      current.windows.push({
        sortTs: row.windowToTs ?? row.windowFromTs ?? 0,
        netRValue,
        netRDisplay,
        tooltipText,
      });
      if (row.trades !== null)
        current.trades = (current.trades ?? 0) + row.trades;
      if (row.netR !== null) current.netR = (current.netR ?? 0) + row.netR;
      if (row.expectancyR !== null) {
        current.expectancySum += row.expectancyR;
        current.expectancyCount += 1;
        if (row.trades !== null && row.trades > 0) {
          current.expectancyWeightedSum += row.expectancyR * row.trades;
          current.expectancyWeightedTrades += row.trades;
        }
      }
      if (row.profitFactor !== null) {
        current.profitFactorSum += row.profitFactor;
        current.profitFactorCount += 1;
      }
      if (row.maxDrawdownR !== null) {
        current.maxDrawdownR =
          current.maxDrawdownR === null
            ? row.maxDrawdownR
            : Math.max(current.maxDrawdownR, row.maxDrawdownR);
      }
      if (row.errorCode) current.errorCodeSet.add(row.errorCode);
    }

    const out: ScalpWorkerJobGridRow[] = [];
    for (const row of byKey.values()) {
      const sortedWindows = row.windows
        .slice()
        .sort((a, b) => a.sortTs - b.sortTs);
      const windowRows = sortedWindows.map((entry) => entry.netRDisplay);
      const expectation =
        row.expectancyWeightedTrades > 0
          ? row.expectancyWeightedSum / row.expectancyWeightedTrades
          : row.expectancyCount > 0
            ? row.expectancySum / row.expectancyCount
            : null;
      out.push({
        rowId: row.rowId,
        deploymentId: row.deploymentId,
        entrySessionProfile: row.entrySessionProfile,
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        forwardValidation: row.forwardValidation,
        deployed: row.deployed,
        deploymentEnabled: row.deploymentEnabled,
        promotionEligible: row.promotionEligible,
        reason: row.reason,
        status: `C:${row.statusCounts.completed} F:${row.statusCounts.failed} R:${row.statusCounts.running} P:${row.statusCounts.pending}`,
        windowCount: row.windowCount,
        windowsResults: windowRows.join(" | "),
        windowNetRs: sortedWindows.map((entry) => ({
          sortTs: entry.sortTs || null,
          value: entry.netRValue,
          display: entry.netRDisplay,
          tooltip: entry.tooltipText,
        })),
        trades: row.trades,
        netR: row.netR,
        totalNetR: row.netR,
        expectancyR: expectation,
        profitFactor:
          row.profitFactorCount > 0
            ? row.profitFactorSum / row.profitFactorCount
            : null,
        maxDrawdownR: row.maxDrawdownR,
        totalMaxDrawdownR: row.maxDrawdownR,
        maxWeeklyNetR: null,
        largestTradeR: null,
        exitReasons: null,
        errorCodes: row.errorCodeSet.size
          ? Array.from(row.errorCodeSet).join(", ")
          : null,
      });
    }
    return out.sort(compareScalpWorkerJobGridRows);
  }, [scalpWorkerTaskRows, scalpSession]);
  const scalpAllDeploymentsGridRows = useMemo<ScalpWorkerJobGridRow[]>(() => {
    const workerMetricsByDeploymentId = new Map(
      scalpWorkerJobsGridRows
        .map((row) =>
          row.deploymentId ? ([row.deploymentId, row] as const) : null,
        )
        .filter((entry): entry is readonly [string, ScalpWorkerJobGridRow] =>
          Boolean(entry),
        ),
    );
    const registryDeploymentIds = new Set(
      scalpRegistryDeployments.map((deployment) => deployment.deploymentId),
    );
    const out = scalpRegistryDeployments.map((deployment) => {
      const workerMetrics = workerMetricsByDeploymentId.get(
        deployment.deploymentId,
      );
      if (workerMetrics) {
        // Backfill new metrics from promotionGate when the worker row
        // doesn't have them (worker rows are built from weekly breakdowns
        // that don't carry per-stage aggregate metrics).
        const pg = asPlainObject(deployment.promotionGate);
        const pgWorker = asPlainObject(pg.worker || pg);
        const pgBestStage = (() => {
          for (const key of ["stageC", "stageB", "stageA"] as const) {
            const s = asPlainObject(pgWorker[key]);
            if (s.executed) return s;
          }
          return {};
        })();
        const pgStageC = pgBestStage;
        return {
          ...workerMetrics,
          rowId: `deployment:${deployment.deploymentId}`,
          deploymentId: deployment.deploymentId,
          entrySessionProfile:
            deployment.entrySessionProfile ||
            workerMetrics.entrySessionProfile ||
            null,
          workerOnly: false,
          forwardValidation:
            deployment.forwardValidation || workerMetrics.forwardValidation,
          deployed: true,
          deploymentEnabled: deployment.enabled,
          inUniverse: deployment.inUniverse,
          lifecycleState: deployment.lifecycleState,
          promotionEligible: deployment.promotionEligible,
          reason:
            deployment.promotionReason ||
            (deployment.promotionEligible ? "eligible" : workerMetrics.reason),
          expectancyR: workerMetrics.expectancyR ?? asFiniteNumber(pgStageC.expectancyR),
          profitFactor: workerMetrics.profitFactor ?? asFiniteNumber(pgStageC.profitFactor),
          maxDrawdownR: workerMetrics.maxDrawdownR ?? asFiniteNumber(pgStageC.maxDrawdownR),
          totalMaxDrawdownR: workerMetrics.totalMaxDrawdownR ?? asFiniteNumber(pgStageC.maxDrawdownR),
          maxWeeklyNetR: workerMetrics.maxWeeklyNetR ?? asFiniteNumber(pgStageC.maxWeeklyNetR),
          largestTradeR: workerMetrics.largestTradeR ?? asFiniteNumber(pgStageC.largestTradeR),
          exitReasons: workerMetrics.exitReasons ?? (
            pgStageC.exitReasons && typeof pgStageC.exitReasons === "object"
              ? {
                  stop: Number((pgStageC.exitReasons as any).stop || 0),
                  tp: Number((pgStageC.exitReasons as any).tp || 0),
                  timeStop: Number((pgStageC.exitReasons as any).timeStop || 0),
                  forceClose: Number((pgStageC.exitReasons as any).forceClose || 0),
                }
              : null
          ),
        } satisfies ScalpWorkerJobGridRow;
      }
      const forwardValidation = deployment.forwardValidation || null;
      // Extract best-available stage metrics from promotionGate when worker rows are absent.
      // Prefer stage C, fall back to B then A when prior stages blocked execution.
      const gate = deployment.promotionGate || {};
      const gateWorker = (gate.worker || gate) as Record<string, any>;
      const gateBestStage = (() => {
        for (const key of ["stageC", "stageB", "stageA"] as const) {
          const raw = (gateWorker[key] || {}) as Record<string, any>;
          if (raw.executed) return { data: raw, label: key.replace("stage", "").toUpperCase() };
        }
        return null;
      })();
      const gateStageC = gateBestStage?.data ?? null;
      const gateStageLabel = gateBestStage?.label ?? "C";
      const gateTrades = asFiniteNumber(gateStageC?.trades);
      const gateNetR = asFiniteNumber(gateStageC?.netR);
      const gateExpR = asFiniteNumber(gateStageC?.expectancyR);
      const gatePF = asFiniteNumber(gateStageC?.profitFactor);
      const gateDD = asFiniteNumber(gateStageC?.maxDrawdownR);

      // Build weekly window bars from stage C weeklyNetR stored on deployment
      const gateWeeklyNetR = (gateStageC?.weeklyNetR || {}) as Record<string, unknown>;
      const gateWeekKeys = Object.keys(gateWeeklyNetR)
        .map((k) => Number(k))
        .filter((v) => Number.isFinite(v))
        .sort((a, b) => a - b);
      const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;
      const gateWindowNetRs = gateWeekKeys.map((weekStart) => {
        const netRValue = Number(gateWeeklyNetR[String(weekStart)] || 0);
        const netRDisplay = `${netRValue >= 0 ? "+" : ""}${netRValue.toFixed(2)}R`;
        const weekEnd = weekStart + ONE_WEEK_MS;
        const windowLabel = `${new Date(weekStart).toISOString().slice(0, 10)} → ${new Date(Math.max(weekStart, weekEnd - 1)).toISOString().slice(0, 10)}`;
        return {
          sortTs: weekEnd,
          value: netRValue,
          display: netRDisplay,
          tooltip: `Window:${windowLabel} | Net:${netRDisplay}`,
        };
      });
      const gateWindowTotalNetR = gateWindowNetRs.reduce(
        (acc, w) => acc + (w.value || 0),
        0,
      );

      return {
        rowId: `deployment:${deployment.deploymentId}`,
        deploymentId: deployment.deploymentId,
        entrySessionProfile: deployment.entrySessionProfile || null,
        workerOnly: false,
        symbol: deployment.symbol,
        strategyId: deployment.strategyId,
        tuneId: deployment.tuneId,
        forwardValidation,
        deployed: true,
        deploymentEnabled: deployment.enabled,
        inUniverse: deployment.inUniverse,
        lifecycleState: deployment.lifecycleState,
        promotionEligible: deployment.promotionEligible,
        reason:
          deployment.promotionReason ||
          (deployment.promotionEligible ? "eligible" : "not_evaluated"),
        status: gateWindowNetRs.length > 0 ? `${gateStageLabel}:${gateWindowNetRs.length}` : "registry",
        windowCount: gateWindowNetRs.length,
        windowsResults: gateWindowNetRs.length > 0
          ? gateWindowNetRs.map((w) => w.display).join(" | ")
          : "—",
        windowNetRs: gateWindowNetRs,
        trades: gateTrades,
        netR: gateWindowNetRs.length > 0 ? gateWindowTotalNetR : gateNetR,
        totalNetR: gateWindowNetRs.length > 0 ? gateWindowTotalNetR : gateNetR,
        expectancyR: gateExpR ?? asFiniteNumber(forwardValidation?.meanExpectancyR),
        profitFactor: gatePF ?? asFiniteNumber(forwardValidation?.meanProfitFactor),
        maxDrawdownR: gateDD,
        totalMaxDrawdownR: gateDD,
        maxWeeklyNetR: asFiniteNumber(gateStageC?.maxWeeklyNetR),
        largestTradeR: asFiniteNumber(gateStageC?.largestTradeR),
        exitReasons: gateStageC?.exitReasons && typeof gateStageC.exitReasons === "object"
          ? {
              stop: Number(gateStageC.exitReasons.stop || 0),
              tp: Number(gateStageC.exitReasons.tp || 0),
              timeStop: Number(gateStageC.exitReasons.timeStop || 0),
              forceClose: Number(gateStageC.exitReasons.forceClose || 0),
            }
          : null,
        errorCodes: null,
      } satisfies ScalpWorkerJobGridRow;
    });

    return out.sort((a, b) => {
      const aNetR = a.totalNetR ?? Number.NEGATIVE_INFINITY;
      const bNetR = b.totalNetR ?? Number.NEGATIVE_INFINITY;
      return bNetR - aNetR;
    });
  }, [scalpRegistryDeployments, scalpWorkerJobsGridRows]);
  const scalpSelectedWorkerGridRows = useMemo<ScalpWorkerJobGridRow[]>(() => {
    return scalpAllDeploymentsGridRows.filter((row) => {
      if (scalpEnabledFilter === "enabled") {
        return row.deploymentEnabled === true;
      }
      return row.deploymentEnabled !== true;
    });
  }, [scalpAllDeploymentsGridRows, scalpEnabledFilter]);
  useEffect(() => {
    if (scalpEnabledFilter !== "enabled") return;
    const hasEnabledRows = scalpAllDeploymentsGridRows.some(
      (row) => row.deploymentEnabled === true,
    );
    if (hasEnabledRows) return;
    const hasNonEnabledRows = scalpAllDeploymentsGridRows.some(
      (row) => row.deploymentEnabled !== true,
    );
    if (hasNonEnabledRows) {
      setScalpEnabledFilter("disabled");
    }
  }, [scalpEnabledFilter, scalpAllDeploymentsGridRows]);
  useEffect(() => {
    const total = scalpSelectedWorkerGridRows.length;
    setScalpGridLoadedRows(
      total > 0 ? Math.min(SCALP_GRID_LOAD_BATCH, total) : 0,
    );
  }, [scalpEnabledFilter, scalpSelectedWorkerGridRows.length]);
  const scalpVisibleWorkerGridRows = useMemo<ScalpWorkerJobGridRow[]>(() => {
    if (!scalpSelectedWorkerGridRows.length) return [];
    const cappedCount = Math.max(
      0,
      Math.min(scalpGridLoadedRows, scalpSelectedWorkerGridRows.length),
    );
    return scalpSelectedWorkerGridRows.slice(0, cappedCount);
  }, [scalpSelectedWorkerGridRows, scalpGridLoadedRows]);
  const scalpWindowsResultsGlobalMaxAbs = useMemo(() => {
    let maxAbs = 0;
    for (const row of scalpSelectedWorkerGridRows) {
      const entries = Array.isArray(row.windowNetRs) ? row.windowNetRs : [];
      for (const entry of entries) {
        if (typeof entry?.value !== "number" || !Number.isFinite(entry.value))
          continue;
        const abs = Math.abs(entry.value);
        if (abs > maxAbs) maxAbs = abs;
      }
    }
    return maxAbs > 0 ? maxAbs : 1;
  }, [scalpSelectedWorkerGridRows]);
  const scalpWindowsResultWeekSlots = useMemo<number[]>(() => {
    const uniqueWeekTs = new Set<number>();
    for (const row of scalpSelectedWorkerGridRows) {
      const entries = Array.isArray(row.windowNetRs) ? row.windowNetRs : [];
      for (const entry of entries) {
        const ts =
          typeof entry?.sortTs === "number" && Number.isFinite(entry.sortTs)
            ? Math.floor(entry.sortTs)
            : null;
        if (ts !== null && ts > 0) uniqueWeekTs.add(ts);
      }
    }
    return Array.from(uniqueWeekTs).sort((a, b) => a - b);
  }, [scalpSelectedWorkerGridRows]);
  const scalpWorkerJobsGridDefaultColDef = useMemo<
    ColDef<ScalpWorkerJobGridRow>
  >(
    () => ({
      sortable: true,
      filter: true,
      resizable: true,
      minWidth: 120,
    }),
    [],
  );
  const scalpWorkerJobsGridColumnDefs = useMemo<
    ColDef<ScalpWorkerJobGridRow>[]
  >(
    () => [
      {
        headerName: "Deployment",
        field: "deploymentId",
        pinned: "left",
        minWidth: scalpIsMobileViewport ? 150 : 220,
        initialWidth: scalpIsMobileViewport ? 168 : 480,
        cellRenderer: (params: any) => {
          const deploymentId = String(params?.value || "").trim();
          const row = params?.data || {};
          const fallbackLabel =
            [String(row?.symbol || "").trim(), String(row?.strategyId || "").trim(), String(row?.tuneId || "").trim()]
              .filter((part) => Boolean(part))
              .join(" · ") || "—";
          if (!deploymentId) {
            return (
              <div className="flex min-w-0 items-center gap-2">
                <span
                  className={scalpDarkMode ? "text-zinc-300" : "text-slate-600"}
                  title={fallbackLabel}
                >
                  {fallbackLabel}
                </span>
              </div>
            );
          }
          const venue = resolveScalpVenueUiFromDeploymentId(deploymentId);
          const iconSrc = SCALP_VENUE_ICON_SRC[venue];
          const displayLabel =
            stripScalpVenuePrefixFromDeploymentId(deploymentId) || deploymentId;
          const copied = scalpCopiedDeploymentId === deploymentId;
          return (
            <div className="flex min-w-0 items-center gap-2">
              <img
                src={iconSrc}
                alt={`${venue} venue`}
                className="h-3.5 w-auto opacity-80"
              />
              <button
                type="button"
                title={
                  copied
                    ? `Copied: ${displayLabel}`
                    : `Copy deployment name: ${displayLabel}`
                }
                className={`inline-flex min-w-0 max-w-full items-center gap-1.5 cursor-copy text-left hover:underline ${
                  copied
                    ? "text-emerald-500"
                    : scalpDarkMode
                      ? "text-zinc-100"
                      : "text-slate-900"
                }`}
                onClick={(event) =>
                  void copyScalpDeploymentLabel(
                    event,
                    deploymentId,
                    displayLabel,
                  )
                }
              >
                <span className="truncate">{displayLabel}</span>
                {copied ? (
                  <CheckCircle2 className="h-3.5 w-3.5" />
                ) : (
                  <Copy className="h-3.5 w-3.5 opacity-70" />
                )}
              </button>
            </div>
          );
        },
      },
      {
        headerName: "Windows Results",
        field: "windowsResults",
        minWidth: 180,
        cellStyle: {
          paddingTop: "0px",
          paddingBottom: "0px",
        },
        cellRenderer: (params: any) => {
          const entries = Array.isArray(params?.data?.windowNetRs)
            ? params.data.windowNetRs
            : [];
          if (!entries.length || !scalpWindowsResultWeekSlots.length) return "—";
          const rowEntryByWeekTs = new Map<
            number,
            {
              sortTs: number | null;
              value: number | null;
              display: string;
              tooltip: string;
            }
          >();
          for (const entry of entries) {
            const ts =
              typeof entry?.sortTs === "number" && Number.isFinite(entry.sortTs)
                ? Math.floor(entry.sortTs)
                : null;
            if (ts !== null && ts > 0) {
              rowEntryByWeekTs.set(ts, entry);
            }
          }
          const trackClass = scalpDarkMode ? "bg-zinc-800" : "bg-slate-200";
          return (
            <div className="h-full min-h-[48px] w-full py-1">
              <div
                className={`flex h-full w-full items-end justify-end gap-1 overflow-hidden rounded-md px-1 ${trackClass}`}
              >
                {scalpWindowsResultWeekSlots.map((slotWeekTs, idx) => {
                  const entry = rowEntryByWeekTs.get(slotWeekTs) || null;
                  if (!entry) {
                    return (
                      <span
                        key={`win-bar-empty-${slotWeekTs}-${idx}`}
                        className="w-1.5 rounded-sm bg-transparent"
                        style={{ height: "100%" }}
                      />
                    );
                  }
                  const value = entry.value;
                  const normalized =
                    value === null || !Number.isFinite(value)
                      ? 20
                      : Math.max(
                          6,
                          Math.round(
                            (Math.abs(value) / scalpWindowsResultsGlobalMaxAbs) *
                              100,
                          ),
                        );
                  const toneClass =
                    value === null || value === 0
                      ? scalpDarkMode
                        ? "bg-zinc-500/80"
                        : "bg-slate-400"
                      : value > 0
                        ? scalpDarkMode
                          ? "bg-emerald-400/90"
                          : "bg-emerald-500"
                        : scalpDarkMode
                          ? "bg-rose-400/90"
                          : "bg-rose-500";
                  return (
                    <span
                      key={`win-bar-${slotWeekTs}-${idx}`}
                      className={`w-1.5 rounded-sm ${toneClass}`}
                      style={{ height: `${Math.min(100, normalized)}%` }}
                      title={entry.tooltip}
                    />
                  );
                })}
              </div>
            </div>
          );
        },
      },
      {
        headerName: "Reason",
        field: "reason",
        hide: scalpEnabledFilter === "enabled",
        minWidth: 210,
        valueFormatter: (params) =>
          String(params.value || "unknown").replace(/_/g, " "),
      },
      // {
      //   headerName: "Lifecycle",
      //   field: "lifecycleState",
      //   minWidth: 150,
      //   valueFormatter: (params) => {
      //     const value = String(params.value || "")
      //       .trim()
      //       .toLowerCase();
      //     if (!value) return "unknown";
      //     return value.replace(/_/g, " ");
      //   },
      //   cellRenderer: (params: any) => {
      //     const value = String(params?.value || "")
      //       .trim()
      //       .toLowerCase();
      //     if (!value) {
      //       return (
      //         <span className={scalpDarkMode ? "text-zinc-500" : "text-slate-400"}>
      //           unknown
      //         </span>
      //       );
      //     }
      //     const toneClass =
      //       value === "graduated"
      //         ? scalpDarkMode
      //           ? "text-emerald-300"
      //           : "text-emerald-700"
      //         : value === "incumbent_refresh"
      //           ? scalpDarkMode
      //             ? "text-sky-300"
      //             : "text-sky-700"
      //           : value === "candidate"
      //             ? scalpDarkMode
      //               ? "text-zinc-300"
      //               : "text-slate-700"
      //             : value === "suspended"
      //               ? scalpDarkMode
      //                 ? "text-amber-300"
      //                 : "text-amber-700"
      //               : value === "retired"
      //                 ? scalpDarkMode
      //                   ? "text-rose-300"
      //                   : "text-rose-700"
      //                 : scalpDarkMode
      //                   ? "text-zinc-300"
      //                   : "text-slate-700";
      //     return (
      //       <span className={toneClass}>{value.replace(/_/g, " ")}</span>
      //     );
      //   },
      // },
      {
        headerName: "Trades",
        field: "trades",
        width: 40,
        valueFormatter: (params) =>
          typeof params.value === "number" && Number.isFinite(params.value)
            ? Math.floor(params.value).toString()
            : "—",
      },
      {
        headerName: "Total Net R",
        field: "totalNetR",
        sort: "desc",
        width: 80,
        cellRenderer: (params: any) => {
          const value =
            typeof params?.value === "number" && Number.isFinite(params.value)
              ? params.value
              : null;
          if (value === null) {
            return (
              <span className={scalpDarkMode ? "text-zinc-500" : "text-slate-400"}>
                —
              </span>
            );
          }
          return (
            <span
              className={
                value >= 0
                  ? "text-emerald-500"
                  : scalpDarkMode
                    ? "text-rose-400"
                    : "text-rose-700"
              }
            >
              {`${value >= 0 ? "+" : ""}${value.toFixed(2)}`}
            </span>
          );
        },
      },
      // {
      //   headerName: "Expectancy",
      //   field: "expectancyR",
      //   width: 50,
      //   valueFormatter: (params) =>
      //     typeof params.value === "number" && Number.isFinite(params.value)
      //       ? `${params.value >= 0 ? "+" : ""}${params.value.toFixed(3)}`
      //       : "—",
      // },
      {
        headerName: "PF",
        field: "profitFactor",
        width: 50,
        valueFormatter: (params) =>
          typeof params.value === "number" && Number.isFinite(params.value)
            ? params.value.toFixed(2)
            : "—",
      },
      {
        headerName: "Max Weekly DD",
        field: "maxDrawdownR",
        width: 50,
        valueFormatter: (params) =>
          typeof params.value === "number" && Number.isFinite(params.value)
            ? `${params.value.toFixed(2)}R`
            : "—",
      },
      {
        headerName: "Max Week R",
        field: "maxWeeklyNetR",
        width: 60,
        cellRenderer: (params: any) => {
          const value = typeof params?.value === "number" && Number.isFinite(params.value) ? params.value : null;
          if (value === null) return <span className={scalpDarkMode ? "text-zinc-500" : "text-slate-400"}>—</span>;
          return (
            <span className={value >= 0 ? "text-emerald-500" : scalpDarkMode ? "text-rose-400" : "text-rose-700"}>
              {`${value >= 0 ? "+" : ""}${value.toFixed(2)}`}
            </span>
          );
        },
      },
      {
        headerName: "Largest R",
        field: "largestTradeR",
        width: 55,
        valueFormatter: (params) =>
          typeof params.value === "number" && Number.isFinite(params.value)
            ? `${params.value.toFixed(2)}R`
            : "—",
      },
      {
        headerName: "Exits",
        field: "exitReasons",
        minWidth: 180,
        valueFormatter: (params) => {
          const v = params.value;
          if (!v || typeof v !== "object") return "—";
          const parts: string[] = [];
          if (v.stop) parts.push(`SL:${v.stop}`);
          if (v.tp) parts.push(`TP:${v.tp}`);
          if (v.timeStop) parts.push(`TS:${v.timeStop}`);
          if (v.forceClose) parts.push(`FC:${v.forceClose}`);
          return parts.length ? parts.join(" ") : "—";
        },
      },
      {
        headerName: "Errors",
        field: "errorCodes",
        hide: scalpEnabledFilter === "enabled",
        minWidth: 220,
        valueFormatter: (params) => String(params.value || "—"),
      },
      // {
      //   headerName: "Universe",
      //   field: "inUniverse",
      //   hide: scalpEnabledFilter === "enabled",
      //   minWidth: 140,
      //   cellRenderer: (params: any) => {
      //     const value =
      //       typeof params?.value === "boolean" ? params.value : null;
      //     if (value === null) {
      //       return (
      //         <span className={scalpDarkMode ? "text-zinc-500" : "text-slate-400"}>
      //           unknown
      //         </span>
      //       );
      //     }
      //     return (
      //       <span
      //         className={
      //           value
      //             ? scalpDarkMode
      //               ? "text-emerald-300"
      //               : "text-emerald-700"
      //             : scalpDarkMode
      //               ? "text-amber-300"
      //               : "text-amber-700"
      //         }
      //       >
      //         {value ? "active" : "inactive"}
      //       </span>
      //     );
      //   },
      // },
    ],
    [
      scalpDarkMode,
      scalpEnabledFilter,
      scalpWindowsResultsGlobalMaxAbs,
      scalpWindowsResultWeekSlots,
      scalpCopiedDeploymentId,
      scalpIsMobileViewport,
    ],
  );
  const handleScalpGridGetRowId = useCallback(
    (params: any) => String(params?.data?.rowId || ""),
    [],
  );
  const handleScalpGridBodyScrollEnd = useCallback(
    (event: any) => {
      if (
        event?.direction &&
        String(event.direction).toLowerCase() !== "vertical"
      ) {
        return;
      }
      const totalRows = scalpSelectedWorkerGridRows.length;
      const loadedRows = scalpVisibleWorkerGridRows.length;
      if (!totalRows || loadedRows >= totalRows) return;
      const range = event?.api?.getVerticalPixelRange?.();
      const displayedRows = event?.api?.getDisplayedRowCount?.() || 0;
      if (!range || displayedRows <= 0) return;
      const nearBottom = range.bottom >= displayedRows * 54 - 108;
      if (!nearBottom) return;
      setScalpGridLoadedRows((prev) =>
        Math.min(totalRows, prev + SCALP_GRID_LOAD_BATCH),
      );
    },
    [scalpSelectedWorkerGridRows.length, scalpVisibleWorkerGridRows.length],
  );
  const handleScalpGridRowClicked = useCallback((event: any) => {
    const deploymentId = String(event?.data?.deploymentId || "").trim();
    if (deploymentId) {
      setScalpActiveDeploymentId(deploymentId);
    }
  }, []);

  const scalpStateMeta = (state?: string | null) => {
    const normalized = String(state || "MISSING")
      .trim()
      .toUpperCase();
    if (normalized === "IN_TRADE") {
      return {
        label: "IN TRADE",
        className: "border-emerald-200 bg-emerald-100 text-emerald-800",
        Icon: ArrowUpRight,
      };
    }
    if (normalized === "DONE") {
      return {
        label: "DONE",
        className:
          resolvedTheme === "dark"
            ? "border-sky-500/60 bg-sky-500/20 text-sky-100"
            : "border-sky-200 bg-sky-50 text-sky-700",
        Icon: Circle,
      };
    }
    if (
      normalized.includes("ERROR") ||
      normalized.includes("BLOCK") ||
      normalized === "MISSING"
    ) {
      return {
        label: normalized,
        className: "border-rose-200 bg-rose-100 text-rose-800",
        Icon: ShieldPlus,
      };
    }
    if (
      normalized.includes("WAIT") ||
      normalized.includes("IDLE") ||
      normalized.includes("COOLDOWN")
    ) {
      return {
        label: normalized,
        className: "border-amber-200 bg-amber-100 text-amber-800",
        Icon: Repeat,
      };
    }
    return {
      label: normalized,
      className: "border-sky-200 bg-sky-100 text-sky-800",
      Icon: Circle,
    };
  };

  const scalpModeMeta = (dryRunLast?: boolean | null) => {
    if (dryRunLast === true) {
      return {
        label: "DRY",
        className: "border-amber-200 bg-amber-100 text-amber-800",
      };
    }
    if (dryRunLast === false) {
      return {
        label: "LIVE",
        className: "border-rose-200 bg-rose-100 text-rose-800",
      };
    }
    return {
      label: "UNKNOWN",
      className: "border-slate-200 bg-slate-100 text-slate-600",
    };
  };

  const scalpReasonMeta = (code: string) => {
    const upper = code.toUpperCase();
    if (/ERROR|FAIL|REJECT|INVALID|BLOCK/.test(upper)) {
      return {
        className: "border-rose-200 bg-rose-50 text-rose-700",
        Icon: ShieldPlus,
      };
    }
    if (/WARMUP|LOW_SAMPLE|SAMPLE_30D/.test(upper)) {
      return {
        className: "border-sky-200 bg-sky-50 text-sky-700",
        Icon: Repeat,
      };
    }
    if (/ENTRY|EXEC|OPEN|CLOSE|BUY|SELL|TP|SL|TRAIL/.test(upper)) {
      return {
        className: "border-emerald-200 bg-emerald-50 text-emerald-700",
        Icon: Zap,
      };
    }
    if (/WAIT|COOLDOWN|PAUSE|HOLD|IDLE/.test(upper)) {
      return {
        className: "border-amber-200 bg-amber-50 text-amber-700",
        Icon: Activity,
      };
    }
    return {
      className: "border-slate-200 bg-slate-100 text-slate-700",
      Icon: Circle,
    };
  };

  const scalpJournalMeta = (entry: { type?: string; level?: string }) => {
    const type = String(entry.type || "")
      .trim()
      .toUpperCase();
    const level = String(entry.level || "")
      .trim()
      .toUpperCase();
    if (level === "ERROR" || type === "ERROR") {
      return {
        className: scalpDarkMode
          ? "border-rose-500/35 bg-rose-500/12 text-rose-200"
          : "border-rose-200 bg-rose-50 text-rose-700",
        Icon: ShieldPlus,
      };
    }
    if (level === "WARN") {
      return {
        className: scalpDarkMode
          ? "border-amber-500/35 bg-amber-500/12 text-amber-200"
          : "border-amber-200 bg-amber-50 text-amber-700",
        Icon: Activity,
      };
    }
    if (type === "EXECUTION") {
      return {
        className: scalpDarkMode
          ? "border-emerald-500/35 bg-emerald-500/12 text-emerald-200"
          : "border-emerald-200 bg-emerald-50 text-emerald-700",
        Icon: Zap,
      };
    }
    if (type === "STATE") {
      return {
        className: scalpDarkMode
          ? "border-sky-500/35 bg-sky-500/12 text-sky-200"
          : "border-sky-200 bg-sky-50 text-sky-700",
        Icon: Repeat,
      };
    }
    return {
      className: scalpDarkMode
        ? "border-zinc-700 bg-zinc-900/80 text-zinc-200"
        : "border-slate-200 bg-slate-100 text-slate-700",
      Icon: BookOpen,
    };
  };

  const scalpCronStatusMeta = (status: ScalpOpsCronStatus) => {
    if (status === "healthy") {
      return resolvedTheme === "dark"
        ? "border-emerald-300/50 bg-emerald-400/15 text-emerald-200"
        : "border-emerald-200 bg-emerald-50 text-emerald-700";
    }
    if (status === "lagging") {
      return resolvedTheme === "dark"
        ? "border-amber-300/50 bg-amber-400/15 text-amber-200"
        : "border-amber-200 bg-amber-50 text-amber-700";
    }
    return resolvedTheme === "dark"
      ? "border-zinc-500/60 bg-zinc-500/15 text-zinc-200"
      : "border-slate-200 bg-slate-100 text-slate-600";
  };
  const scalpCronDetailToneMeta = (tone?: ScalpOpsCronDetailTone) => {
    if (tone === "positive") {
      return resolvedTheme === "dark"
        ? "border-emerald-400/40 bg-emerald-500/10 text-emerald-200"
        : "border-emerald-200 bg-emerald-50 text-emerald-700";
    }
    if (tone === "warning") {
      return resolvedTheme === "dark"
        ? "border-amber-400/40 bg-amber-500/10 text-amber-200"
        : "border-amber-200 bg-amber-50 text-amber-700";
    }
    if (tone === "critical") {
      return resolvedTheme === "dark"
        ? "border-rose-400/40 bg-rose-500/10 text-rose-200"
        : "border-rose-200 bg-rose-50 text-rose-700";
    }
    return resolvedTheme === "dark"
      ? "border-zinc-600 bg-zinc-800/70 text-zinc-200"
      : "border-slate-200 bg-slate-50 text-slate-700";
  };
  const scalpWorkerTaskStatusMeta = (status: string) => {
    const normalized = String(status || "")
      .trim()
      .toLowerCase();
    if (normalized === "completed") {
      return resolvedTheme === "dark"
        ? "border-emerald-400/40 bg-emerald-500/10 text-emerald-200"
        : "border-emerald-200 bg-emerald-50 text-emerald-700";
    }
    if (normalized === "failed") {
      return resolvedTheme === "dark"
        ? "border-rose-400/40 bg-rose-500/10 text-rose-200"
        : "border-rose-200 bg-rose-50 text-rose-700";
    }
    if (normalized === "running") {
      return resolvedTheme === "dark"
        ? "border-amber-400/40 bg-amber-500/10 text-amber-200"
        : "border-amber-200 bg-amber-50 text-amber-700";
    }
    return resolvedTheme === "dark"
      ? "border-zinc-600 bg-zinc-800/70 text-zinc-300"
      : "border-slate-200 bg-slate-100 text-slate-600";
  };
  const scalpVisualMetricFillMeta = (tone?: ScalpOpsCronDetailTone) => {
    if (tone === "positive")
      return resolvedTheme === "dark" ? "bg-emerald-400/80" : "bg-emerald-500";
    if (tone === "warning")
      return resolvedTheme === "dark" ? "bg-amber-400/80" : "bg-amber-500";
    if (tone === "critical")
      return resolvedTheme === "dark" ? "bg-rose-400/80" : "bg-rose-500";
    return resolvedTheme === "dark" ? "bg-zinc-400/80" : "bg-slate-500";
  };
  const scalpVisualMetricTrackClass =
    resolvedTheme === "dark" ? "bg-zinc-800" : "bg-slate-200";
  const scalpImportStatusMeta = (
    status: ScalpUniversePipelineRow["importStatus"],
  ) => {
    if (status === "seeded") {
      return resolvedTheme === "dark"
        ? "border-emerald-400/40 bg-emerald-500/10 text-emerald-200"
        : "border-emerald-200 bg-emerald-50 text-emerald-700";
    }
    if (status === "failed") {
      return resolvedTheme === "dark"
        ? "border-rose-400/40 bg-rose-500/10 text-rose-200"
        : "border-rose-200 bg-rose-50 text-rose-700";
    }
    if (status === "skipped") {
      return resolvedTheme === "dark"
        ? "border-amber-400/40 bg-amber-500/10 text-amber-200"
        : "border-amber-200 bg-amber-50 text-amber-700";
    }
    return resolvedTheme === "dark"
      ? "border-zinc-600 bg-zinc-800/70 text-zinc-300"
      : "border-slate-200 bg-slate-100 text-slate-600";
  };
  const scalpEvaluationMeta = (eligible: boolean | null) => {
    if (eligible === true) {
      return resolvedTheme === "dark"
        ? "border-emerald-400/40 bg-emerald-500/10 text-emerald-200"
        : "border-emerald-200 bg-emerald-50 text-emerald-700";
    }
    if (eligible === false) {
      return resolvedTheme === "dark"
        ? "border-rose-400/40 bg-rose-500/10 text-rose-200"
        : "border-rose-200 bg-rose-50 text-rose-700";
    }
    return resolvedTheme === "dark"
      ? "border-zinc-600 bg-zinc-800/70 text-zinc-300"
      : "border-slate-200 bg-slate-100 text-slate-600";
  };
  const scalpWorkerSortButtonClass = (active: boolean) =>
    active
      ? resolvedTheme === "dark"
        ? "rounded border border-zinc-300 bg-zinc-100 px-1 text-[10px] leading-4 text-zinc-900"
        : "rounded border border-slate-500 bg-slate-700 px-1 text-[10px] leading-4 text-white"
      : resolvedTheme === "dark"
        ? "rounded border border-zinc-600 bg-zinc-800 px-1 text-[10px] leading-4 text-zinc-300 hover:border-zinc-400 hover:text-zinc-100"
        : "rounded border border-slate-300 bg-white px-1 text-[10px] leading-4 text-slate-500 hover:border-slate-500 hover:text-slate-700";
  const setScalpWorkerSortColumn = (
    key: ScalpWorkerSortKey,
    direction: ScalpWorkerSortDirection,
  ) => {
    setScalpWorkerSort((prev) => {
      if (prev.key === key && prev.direction === direction) return prev;
      return { key, direction };
    });
  };
  const renderScalpWorkerSortableHeader = (
    label: string,
    key: ScalpWorkerSortKey,
  ) => {
    const ascActive =
      scalpWorkerSort.key === key && scalpWorkerSort.direction === "asc";
    const descActive =
      scalpWorkerSort.key === key && scalpWorkerSort.direction === "desc";
    return (
      <div className="inline-flex items-center gap-1">
        <span>{label}</span>
        <div className="inline-flex items-center gap-0.5">
          <button
            type="button"
            aria-label={`Sort ${label} ascending`}
            className={scalpWorkerSortButtonClass(ascActive)}
            onClick={() => setScalpWorkerSortColumn(key, "asc")}
          >
            ↑
          </button>
          <button
            type="button"
            aria-label={`Sort ${label} descending`}
            className={scalpWorkerSortButtonClass(descActive)}
            onClick={() => setScalpWorkerSortColumn(key, "desc")}
          >
            ↓
          </button>
        </div>
      </div>
    );
  };

  const renderDashboardSkeleton = () => (
    <div className="grid grid-cols-1 gap-5 lg:grid-cols-2 lg:items-stretch">
      <div className="space-y-4 lg:col-span-2">
        <div className="h-full rounded-2xl border border-slate-200 bg-slate-50 p-4">
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
            {Array.from({ length: 3 }).map((_, idx) => (
              <div
                key={`summary-skeleton-${idx}`}
                className="animate-pulse space-y-2"
              >
                <div className="h-3 w-20 rounded-full bg-slate-200" />
                <div className="h-8 w-28 rounded-lg bg-slate-200" />
                <div className="h-3 w-full max-w-[200px] rounded-full bg-slate-200" />
                <div className="h-3 w-full max-w-[150px] rounded-full bg-slate-200" />
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
        <div className="animate-pulse">
          <div className="h-3 w-24 rounded-full bg-slate-200" />
          <div className="mt-2 h-3 w-44 rounded-full bg-slate-200" />
          <div className="mt-3 h-[260px] w-full rounded-xl border border-slate-200 bg-slate-50/80 p-4">
            <div className="flex h-full w-full flex-col justify-between">
              <div className="h-3 w-28 rounded-full bg-slate-200" />
              <div className="space-y-2">
                <div className="h-2.5 w-full rounded-full bg-slate-200" />
                <div className="h-2.5 w-11/12 rounded-full bg-slate-200" />
                <div className="h-2.5 w-10/12 rounded-full bg-slate-200" />
              </div>
              <div className="h-3 w-40 rounded-full bg-slate-200" />
            </div>
          </div>
        </div>
      </div>

      <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
        <div className="animate-pulse">
          <div className="h-3 w-32 rounded-full bg-slate-200" />
          <div className="mt-3 h-4 w-3/4 rounded-full bg-slate-200" />
          <div className="mt-2 h-4 w-2/3 rounded-full bg-slate-200" />
          <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
            {Array.from({ length: 4 }).map((_, idx) => (
              <div
                key={`bias-skeleton-${idx}`}
                className="h-12 rounded-lg border border-slate-200 bg-slate-50"
              />
            ))}
          </div>
        </div>
      </div>

      <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
        <div className="animate-pulse">
          <div className="h-3 w-36 rounded-full bg-slate-200" />
          <div className="mt-3 h-5 w-52 rounded-full bg-slate-200" />
          <div className="mt-3 space-y-2">
            <div className="h-3 w-full rounded-full bg-slate-200" />
            <div className="h-3 w-11/12 rounded-full bg-slate-200" />
            <div className="h-3 w-10/12 rounded-full bg-slate-200" />
          </div>
        </div>
      </div>
    </div>
  );

  const handleThemeToggle = () => {
    const nextTheme: ThemePreference =
      resolvedTheme === "dark" ? "light" : "dark";
    setThemePreference(nextTheme);
    setResolvedTheme(nextTheme);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(THEME_PREFERENCE_STORAGE_KEY, nextTheme);
    }
  };

  const handleStrategyModeChange = (mode: StrategyMode) => {
    setStrategyMode(mode);
    setError(null);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(STRATEGY_MODE_STORAGE_KEY, mode);
    }
  };

  return (
    <>
      <Head>
        <title>AI Trade Dashboard</title>
        <link rel="icon" href="/favicon.svg" type="image/svg+xml" />
      </Head>
      <div
        className={`relative min-h-screen overflow-x-hidden px-4 py-6 sm:px-6 lg:px-8 ${
          resolvedTheme === "dark"
            ? "theme-dark bg-slate-950 text-slate-100"
            : "theme-light bg-slate-50 text-slate-900"
        }`}
      >
        <button
          type="button"
          onClick={handleThemeToggle}
          className={`fixed right-4 top-4 z-[60] inline-flex h-10 w-10 items-center justify-center rounded-full border shadow-sm transition focus:outline-none focus-visible:ring-2 ${
            resolvedTheme === "dark"
              ? "border-zinc-700 bg-zinc-900 text-zinc-100 hover:border-zinc-500 hover:bg-zinc-800 hover:text-zinc-50 focus-visible:ring-zinc-500 focus-visible:ring-offset-2 focus-visible:ring-offset-zinc-950"
              : "border-slate-300 bg-white text-slate-700 hover:border-slate-500 hover:bg-slate-50 hover:text-slate-900 focus-visible:ring-slate-400 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-50"
          }`}
          aria-label={
            resolvedTheme === "dark"
              ? "Switch to light mode"
              : "Switch to dark mode"
          }
          title={
            resolvedTheme === "dark"
              ? "Switch to light mode"
              : "Switch to dark mode"
          }
        >
          {resolvedTheme === "dark" ? (
            <Sun className="h-4 w-4" />
          ) : (
            <Moon className="h-4 w-4" />
          )}
        </button>
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
          <div className="rounded-2xl border border-slate-200 bg-white px-6 py-5 shadow-sm">
            <div className="flex items-center justify-between gap-4">
              <div>
              <p className="text-xs uppercase tracking-[0.2em] text-slate-500">
                Performance
              </p>
              <h1 className="text-3xl font-semibold leading-tight text-slate-900">
                AI Trade Dashboard
              </h1>
              <div className="mt-2 inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 p-1">
                <button
                  type="button"
                  onClick={() => handleStrategyModeChange("swing")}
                  className={`rounded-full px-3 py-1 text-[11px] font-semibold transition ${
                    strategyMode === "swing"
                      ? "bg-sky-600 text-white shadow-sm"
                      : "text-slate-600 hover:bg-slate-200/70 hover:text-slate-900"
                  }`}
                >
                  Swing
                </button>
                <button
                  type="button"
                  onClick={() => handleStrategyModeChange("scalp")}
                  className={`rounded-full px-3 py-1 text-[11px] font-semibold transition ${
                    strategyMode === "scalp"
                      ? "bg-sky-600 text-white shadow-sm"
                      : "text-slate-600 hover:bg-slate-200/70 hover:text-slate-900"
                  }`}
                >
                  Scalp
                </button>
              </div>
              {strategyMode === "swing" && activeSymbol && currentEvalJob ? (
                <p className="mt-1 text-xs text-slate-500">
                  Eval job for {activeSymbol}:{" "}
                  <span className="font-semibold text-slate-700">
                    {currentEvalJob.status}
                  </span>
                  {currentEvalJob.error ? ` (${currentEvalJob.error})` : ""}
                </p>
              ) : null}
              {strategyMode === "swing" && activeSymbol ? (
                <p className="mt-1 text-xs text-slate-500">
                  Live price:{" "}
                  <span
                    className={
                      livePriceConnected
                        ? "font-semibold text-emerald-700"
                        : "font-semibold text-slate-600"
                    }
                  >
                    {livePriceConnected ? "connected" : "connecting"}
                  </span>
                  {typeof livePriceNow === "number"
                    ? ` · ${livePriceNow.toFixed(2)}`
                    : ""}
                </p>
              ) : null}
              {strategyMode === "swing" ? (
                <p className="mt-1 text-xs text-slate-500">
                  Swing cron:{" "}
                  <span
                    className={
                      !swingCronControlLoaded
                        ? "font-semibold text-slate-600"
                        : swingCronHardDeactivated
                          ? "font-semibold text-rose-700"
                          : "font-semibold text-emerald-700"
                    }
                  >
                    {!swingCronControlLoaded
                      ? "loading"
                      : swingCronHardDeactivated
                        ? "hard-deactivated"
                        : "active"}
                  </span>
                  {swingCronReason ? ` · ${swingCronReason}` : ""}
                </p>
              ) : null}
              {loading ? (
                <p className="mt-1 text-xs text-slate-500">{loadingLabel}</p>
              ) : null}
              </div>
              <div className="flex items-center gap-2">
              {strategyMode === "swing" ? (
                <button
                  onClick={() =>
                    activeSymbol ? triggerEvaluation(activeSymbol) : undefined
                  }
                  disabled={
                    !adminGranted ||
                    !activeSymbol ||
                    !!evaluateSubmittingSymbol ||
                    evaluateRunning
                  }
                  className="rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-800 transition hover:border-emerald-300 hover:bg-emerald-50 hover:text-emerald-800 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {evaluateSubmittingSymbol
                    ? "Queueing…"
                    : evaluateRunning
                      ? "Evaluating…"
                      : "Run Evaluation"}
                </button>
              ) : null}
              {strategyMode === "swing" ? (
                <button
                  type="button"
                  onClick={() => {
                    void setSwingCronHardDeactivate(!swingCronHardDeactivated);
                  }}
                  disabled={
                    !adminGranted ||
                    swingCronControlUpdating ||
                    !swingCronControlLoaded
                  }
                  className={`rounded-full border px-4 py-2 text-sm font-semibold transition disabled:cursor-not-allowed disabled:opacity-60 ${
                    swingCronHardDeactivated
                      ? "border-rose-300 bg-rose-50 text-rose-700 hover:border-rose-400 hover:bg-rose-100"
                      : "border-emerald-300 bg-emerald-50 text-emerald-700 hover:border-emerald-400 hover:bg-emerald-100"
                  }`}
                  title={
                    swingCronHardDeactivated
                      ? "Re-enable swing cron analyze execution"
                      : "Hard-deactivate swing cron analyze execution"
                  }
                >
                  {swingCronControlUpdating
                    ? "Updating…"
                    : !swingCronControlLoaded
                      ? "Swing Cron: ..."
                      : swingCronHardDeactivated
                        ? "Swing Cron: OFF"
                        : "Swing Cron: ON"}
                </button>
              ) : null}
                <button
                  onClick={() => {
                    if (strategyMode === "scalp") {
                      loadScalpDashboard();
                      return;
                    }
                    loadDashboard();
                  }}
                  disabled={!adminGranted}
                  className="rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-800 transition hover:border-sky-300 hover:bg-sky-50 hover:text-sky-800 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  Refresh
                </button>
              </div>
            </div>
            {strategyMode === "scalp" ? (
              <div
                className={`mt-3 w-full rounded-xl border p-2.5 ${
                  resolvedTheme === "dark"
                    ? "border-zinc-700 bg-zinc-900/85"
                    : "border-slate-200 bg-slate-50"
                }`}
              >
                <div
                  className={`relative h-14 overflow-hidden rounded-lg border ${
                    resolvedTheme === "dark"
                      ? "border-zinc-700 bg-zinc-950"
                      : "border-slate-300 bg-white"
                  }`}
                >
                  {SCALP_SESSION_TIMELINE_TICK_MINUTES.map((minute) => (
                    <div
                      key={`header-timeline-grid-${minute}`}
                      className={`pointer-events-none absolute inset-y-0 w-px ${
                        resolvedTheme === "dark" ? "bg-zinc-700/90" : "bg-slate-200"
                      }`}
                      style={{ left: `${(minute / 1440) * 100}%` }}
                    />
                  ))}
                  {scalpSessionTimelineTracks.map((track) =>
                    track.segments.map((segment, segmentIndex) => {
                      const startPct = (segment.startMinute / 1440) * 100;
                      const widthPct =
                        ((segment.endMinute - segment.startMinute) / 1440) * 100;
                      const isActiveSession = scalpSession === track.id;
                      return (
                        <button
                          type="button"
                          key={`header-timeline-segment-${track.id}-${segmentIndex}`}
                          onClick={() => {
                            setScalpSession(track.id);
                            scalpSummaryFetchedAtMsRef.current = 0;
                          }}
                          className={`absolute rounded-md border px-1.5 text-[10px] font-semibold transition ${
                            resolvedTheme === "dark"
                              ? "text-zinc-100 hover:brightness-110"
                              : "text-slate-900 hover:brightness-95"
                          } ${
                            isActiveSession
                              ? resolvedTheme === "dark"
                                ? "z-10 ring-2 ring-white/90"
                                : "z-10 ring-2 ring-black/85"
                              : ""
                          }`}
                          style={{
                            top: "20%",
                            bottom: "20%",
                            left: `${startPct}%`,
                            width: `${widthPct}%`,
                            backgroundColor:
                              resolvedTheme === "dark"
                                ? track.darkFill
                                : track.lightFill,
                            borderColor:
                              resolvedTheme === "dark"
                                ? track.darkBorder
                                : track.lightBorder,
                          }}
                          aria-label={`Select ${track.label} session`}
                          aria-pressed={isActiveSession}
                          title={`${track.label}: ${formatTimelineMinuteLabel(
                            segment.startMinute,
                          )} - ${formatTimelineMinuteLabel(segment.endMinute)}`}
                        >
                          <span className="inline-flex h-full items-center truncate">
                            {track.label}
                          </span>
                        </button>
                      );
                    }),
                  )}
                  <div
                    className="pointer-events-none absolute inset-y-0 w-[2px]"
                    style={{
                      left: `calc(${scalpSessionTimelineNowPct}% - 1px)`,
                      backgroundColor:
                        resolvedTheme === "dark"
                          ? "rgba(255, 255, 255, 0.96)"
                          : "rgba(0, 0, 0, 0.94)",
                      boxShadow:
                        resolvedTheme === "dark"
                          ? "0 0 0 1px rgba(255,255,255,0.2)"
                          : "0 0 0 1px rgba(0,0,0,0.18)",
                    }}
                  />
                </div>
                <div
                  className={`mt-1.5 grid grid-cols-5 text-[10px] ${
                    resolvedTheme === "dark" ? "text-zinc-400" : "text-slate-500"
                  }`}
                >
                  {SCALP_SESSION_TIMELINE_TICK_MINUTES.map((minute, idx) => (
                    <span
                      key={`header-timeline-tick-${minute}`}
                      className={
                        idx === 0
                          ? "text-left"
                          : idx === SCALP_SESSION_TIMELINE_TICK_MINUTES.length - 1
                            ? "text-right"
                            : "text-center"
                      }
                    >
                      {formatTimelineMinuteLabel(minute)}
                    </span>
                  ))}
                </div>
              </div>
            ) : null}
          </div>

          {error && (
            <div className="mt-4 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm font-semibold text-rose-700">
              Could not load dashboard data: {error}
            </div>
          )}

          {strategyMode === "swing" && !error && (
            <div className="mt-4 flex flex-wrap items-center gap-2">
              {symbols.map((sym, i) => {
                const isActive = i === active;
                const tab = tabData[sym];
                const pnl7dValue =
                  typeof tab?.pnl7dWithOpen === "number"
                    ? tab.pnl7dWithOpen
                    : typeof tab?.pnl7d === "number"
                      ? tab.pnl7d
                      : null;
                const pnlTone =
                  typeof pnl7dValue === "number"
                    ? pnl7dValue < 0
                      ? "negative"
                      : "positive"
                    : "neutral";
                return (
                  <button
                    key={sym}
                    onClick={() => setActive(i)}
                    className={`rounded-full border px-4 py-2 text-sm font-semibold transition ${
                      pnlTone === "positive"
                        ? "border-emerald-200 bg-emerald-50 text-emerald-700 hover:border-emerald-300 hover:text-emerald-800"
                        : pnlTone === "negative"
                          ? "border-rose-200 bg-rose-50 text-rose-700 hover:border-rose-300 hover:text-rose-800"
                          : "border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:text-slate-800"
                    } ${
                      isActive
                        ? "shadow-md ring-2 ring-slate-400/70 outline outline-2 outline-offset-2 outline-slate-200/80"
                        : ""
                    }`}
                  >
                    {sym}
                  </button>
                );
              })}
              {isInitialLoading &&
                Array.from({ length: 4 }).map((_, idx) => (
                  <span
                    key={`tab-skeleton-${idx}`}
                    className="h-9 w-24 animate-pulse rounded-full border border-slate-200 bg-slate-100"
                  />
                ))}
            </div>
          )}

          <div className="mt-4 pb-8">
            {strategyMode === "scalp" ? (
              loading ? (
                <div className={`${scalpSectionShellClass} p-4 shadow-sm`}>
                  <div className="animate-pulse space-y-3">
                    <div
                      className={`h-4 w-44 rounded-full ${scalpDarkMode ? "bg-zinc-600" : "bg-slate-200"}`}
                    />
                    <div
                      className={`h-3 w-64 rounded-full ${scalpDarkMode ? "bg-zinc-700" : "bg-slate-200"}`}
                    />
                    <div
                      className={`h-40 rounded-xl border ${scalpDarkMode ? "border-zinc-700 bg-zinc-800" : "border-slate-200 bg-slate-50"}`}
                    />
                  </div>
                </div>
              ) : (
                <div className="space-y-5">
                  {!scalpOpsDeployments.length ? (
                    <div
                      className={`rounded-xl border px-4 py-3 text-sm ${
                        scalpDarkMode
                          ? "border-zinc-700 bg-zinc-900/60 text-zinc-300"
                          : "border-slate-200 bg-slate-50 text-slate-600"
                      }`}
                    >
                      No enabled scalp deployments right now. Job health and
                      cron telemetry are still available below.
                    </div>
                  ) : null}
                  {/* Pipeline Flow panel removed */}
                  <section className="hidden">
                    <article className={`${scalpSectionShellClass} p-4`}>
                      <div className="flex flex-wrap items-start gap-3">
                        <div className="min-w-0 flex-1">
                          {scalpPipelineFlowSteps.length ? (
                            <div className="mt-3 grid grid-cols-[repeat(auto-fit,minmax(220px,1fr))] gap-3">
                              {scalpPipelineFlowSteps.map((step) => {
                                const Icon = scalpPipelineStepIcon(step.id);
                                const visual = scalpPipelineStepVisualMeta(
                                  step.state,
                                );
                                const normalizedStepId = String(step.id || "")
                                  .trim()
                                  .toLowerCase();
                                const isWorkerStep =
                                  normalizedStepId === "research" ||
                                  normalizedStepId.includes("research");
                                const row = scalpCronRowForStep(step.id);
                                const rowInProgress = row
                                  ? scalpIsCronRowInProgress(row.id)
                                  : false;
                                const invokeState = row
                                  ? scalpCronInvokeStateById[row.id] || null
                                  : null;
                                const metricsOpen = row
                                  ? scalpExpandedCronId === row.id
                                  : false;
                                const invokeDisabled =
                                  !row?.invokePath ||
                                  Boolean(invokeState?.running);
                                const rowQueue = (row?.resultPreview?.queue ||
                                  {}) as Record<string, unknown>;
                                const rowRunningCount = Math.max(
                                  0,
                                  Math.floor(Number(rowQueue.running || 0)),
                                );
                                const rowQueuedCount = Math.max(
                                  0,
                                  Math.floor(
                                    Number(rowQueue.pending || 0) +
                                      Number(rowQueue.retryWait || 0),
                                  ),
                                );
                                return (
                                  <div
                                    key={`pipeline-step-${step.id || step.label}`}
                                    className={`rounded-xl border px-3 py-2.5 ${
                                      scalpDarkMode
                                        ? "border-zinc-700/80 bg-zinc-950/60"
                                        : "border-slate-200 bg-white"
                                    }`}
                                    title={String(row?.id || step.detail || "")
                                      .trim() || undefined}
                                  >
                                    <div className="flex items-start justify-between gap-2">
                                      <div className="flex min-w-0 flex-1 items-start gap-2">
                                        <span
                                          className={`inline-flex h-7 w-7 items-center justify-center rounded-lg border ${
                                            scalpDarkMode
                                              ? "border-zinc-600 bg-zinc-800"
                                              : "border-slate-300 bg-white"
                                          }`}
                                        >
                                          <Icon className="h-3.5 w-3.5" />
                                        </span>
                                        {isWorkerStep ? (
                                          <div className="flex min-w-0 flex-wrap gap-1">
                                            {scalpWorkerCompactStats.map((stat) => (
                                              <span
                                                key={`worker-step-stat-${step.id || "worker"}-${stat.id}`}
                                                className={`inline-flex items-center gap-1 rounded-md border px-1.5 py-0.5 text-[10px] ${
                                                  scalpDarkMode
                                                    ? "border-zinc-600 bg-zinc-900/80 text-zinc-200"
                                                    : "border-slate-300 bg-slate-50 text-slate-700"
                                                }`}
                                              >
                                                <span className="opacity-70">
                                                  {stat.label}
                                                </span>
                                                <span className="font-semibold">
                                                  {stat.value}
                                                </span>
                                              </span>
                                            ))}
                                          </div>
                                        ) : null}
                                      </div>
                                      <span
                                        className={`rounded-full border px-1.5 py-0.5 text-[10px] uppercase tracking-[0.12em] ${visual.badge}`}
                                      >
                                        {rowInProgress ? "run" : visual.label}
                                      </span>
                                    </div>
                                    <div
                                      className={`mt-2 truncate text-sm font-semibold ${scalpTextPrimaryClass}`}
                                    >
                                      {step.label || step.id || "step"}
                                    </div>
                                    <div className={`mt-1 text-[11px] ${scalpTextSecondaryClass}`}>
                                      {row
                                        ? `last ${formatScalpTime(row.lastRunAtMs)} · next ${formatScalpNextRunIn(
                                            row.nextRunAtMs,
                                            scalpCronNowMs,
                                          )} · ${rowRunningCount} running · ${rowQueuedCount} queued`
                                        : String(step.detail || "—")}
                                    </div>
                                    {(row?.visualMetrics || []).length ? (
                                      <div className="mt-2 space-y-1.5">
                                        {(row?.visualMetrics || [])
                                          .slice(0, 3)
                                          .map((metric) => {
                                            const pct =
                                              metric.pct === null ||
                                              !Number.isFinite(metric.pct)
                                                ? 0
                                                : Math.max(
                                                    0,
                                                    Math.min(100, metric.pct),
                                                  );
                                            return (
                                              <div
                                                key={`${row?.id || step.id}-metric-${metric.label}`}
                                                className="space-y-1"
                                              >
                                                <div
                                                  className={`flex items-center justify-between text-[10px] ${scalpTextSecondaryClass}`}
                                                >
                                                  <span>{metric.label}</span>
                                                  <span className="font-semibold">
                                                    {metric.valueLabel}
                                                  </span>
                                                </div>
                                                <div
                                                  className={`h-1.5 overflow-hidden rounded-full ${scalpVisualMetricTrackClass}`}
                                                >
                                                  <div
                                                    className={`h-full ${scalpVisualMetricFillMeta(metric.tone)}`}
                                                    style={{
                                                      width: `${Math.max(
                                                        5,
                                                        pct,
                                                      )}%`,
                                                    }}
                                                  />
                                                </div>
                                              </div>
                                            );
                                          })}
                                      </div>
                                    ) : (
                                      <div
                                        className={`mt-1 h-1.5 overflow-hidden rounded-full ${
                                          scalpDarkMode
                                            ? "bg-zinc-800"
                                            : "bg-slate-200"
                                        }`}
                                      >
                                        <div
                                          className={`h-full w-full ${visual.fill}`}
                                        />
                                      </div>
                                    )}
                                    {row ? (
                                      <div className="mt-3 flex flex-wrap items-center gap-2">
                                        <button
                                          type="button"
                                          onClick={(event) => {
                                            event.stopPropagation();
                                            void invokeScalpCronNow(row);
                                          }}
                                          disabled={invokeDisabled}
                                          className={`rounded-full border px-2.5 py-1 text-[11px] font-medium transition ${
                                            scalpDarkMode
                                              ? "border-zinc-600 bg-zinc-800 text-zinc-200 hover:bg-zinc-700 disabled:cursor-not-allowed disabled:opacity-50"
                                              : "border-slate-300 bg-white text-slate-700 hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-50"
                                          }`}
                                        >
                                          {invokeState?.running
                                            ? "invoking..."
                                            : "Invoke now()"}
                                        </button>
                                        <button
                                          type="button"
                                          onClick={(event) => {
                                            event.stopPropagation();
                                            setScalpExpandedCronId((prev) =>
                                              prev === row.id ? null : row.id,
                                            );
                                          }}
                                          className={`rounded-full border px-2.5 py-1 text-[11px] font-medium transition ${
                                            metricsOpen
                                              ? scalpDarkMode
                                                ? "border-sky-400/70 bg-sky-500/15 text-sky-200"
                                                : "border-sky-300 bg-sky-50 text-sky-700"
                                              : scalpDarkMode
                                                ? "border-zinc-600 bg-zinc-800 text-zinc-200 hover:bg-zinc-700"
                                                : "border-slate-300 bg-white text-slate-700 hover:bg-slate-100"
                                          }`}
                                        >
                                          {metricsOpen
                                            ? "Hide metrics"
                                            : "Open metrics"}
                                        </button>
                                      </div>
                                    ) : null}
                                    {isWorkerStep ? (
                                      <div
                                        className={`mt-2 border-t pt-1.5 text-[10px] ${
                                          scalpDarkMode
                                            ? "border-zinc-700/80 text-zinc-400"
                                            : "border-slate-200 text-slate-500"
                                        }`}
                                      >
                                        {`${scalpWorkerCompactStatusLine} · last scan ${formatScalpTime(
                                          scalpResearchLatestCursorAtMs,
                                        )}`}
                                      </div>
                                    ) : null}
                                  </div>
                                );
                              })}
                            </div>
                          ) : null}
                        </div>
                      </div>

                      {!scalpCronRows.length ? (
                        <div
                          className={`mt-4 rounded-xl border px-3 py-4 text-sm ${
                            scalpDarkMode
                              ? "border-zinc-700/60 text-zinc-300"
                              : "border-slate-200 text-slate-600"
                          }`}
                        >
                          No pipeline job rows are available yet.
                        </div>
                      ) : null}
                      {scalpExpandedCronRow ? (
                        <article
                          className={`mt-4 rounded-2xl border p-3 ${
                            scalpDarkMode
                              ? "border-zinc-700 bg-zinc-950/70"
                              : "border-slate-200 bg-slate-50"
                          }`}
                        >
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <h4
                              className={`text-sm font-semibold ${scalpTextPrimaryClass}`}
                            >
                              {`Detailed metrics · ${scalpExpandedCronRow.id}`}
                            </h4>
                            <button
                              type="button"
                              onClick={() => setScalpExpandedCronId(null)}
                              className={`rounded-full border px-2.5 py-1 text-[11px] font-medium ${
                                scalpDarkMode
                                  ? "border-zinc-600 bg-zinc-800 text-zinc-200 hover:bg-zinc-700"
                                  : "border-slate-300 bg-white text-slate-700 hover:bg-slate-100"
                              }`}
                            >
                              Close
                            </button>
                          </div>
                          <div className="mt-3 grid grid-cols-1 gap-2 md:grid-cols-2 xl:grid-cols-4">
                            {scalpExpandedCronRow.details.map((detail) => (
                              <div
                                key={`${scalpExpandedCronRow.id}-detail-${detail.label}`}
                                className={`rounded-lg border px-2 py-1.5 ${scalpCronDetailToneMeta(
                                  detail.tone,
                                )}`}
                              >
                                <div className="uppercase tracking-[0.14em] opacity-75 text-[10px]">
                                  {detail.label}
                                </div>
                                <div className="mt-0.5 text-xs font-semibold">
                                  {detail.value}
                                </div>
                              </div>
                            ))}
                          </div>
                          {(scalpExpandedCronRow.visualMetrics || []).length ? (
                            <div className="mt-3 grid grid-cols-1 gap-2 md:grid-cols-2">
                              {(scalpExpandedCronRow.visualMetrics || []).map(
                                (metric) => {
                                  const pct =
                                    metric.pct === null ||
                                    !Number.isFinite(metric.pct)
                                      ? 0
                                      : Math.max(0, Math.min(100, metric.pct));
                                  return (
                                    <div
                                      key={`${scalpExpandedCronRow.id}-visual-${metric.label}`}
                                      className={`rounded-lg border px-2 py-2 ${
                                        scalpDarkMode
                                          ? "border-zinc-700 bg-zinc-900/70"
                                          : "border-slate-200 bg-white"
                                      }`}
                                    >
                                      <div
                                        className={`flex items-center justify-between text-[11px] ${scalpTextSecondaryClass}`}
                                      >
                                        <span>{metric.label}</span>
                                        <span className="font-semibold">
                                          {metric.valueLabel}
                                        </span>
                                      </div>
                                      <div
                                        className={`mt-1.5 h-2 overflow-hidden rounded-full ${scalpVisualMetricTrackClass}`}
                                      >
                                        <div
                                          className={`h-full ${scalpVisualMetricFillMeta(metric.tone)}`}
                                          style={{
                                            width: `${Math.max(5, pct)}%`,
                                          }}
                                        />
                                      </div>
                                    </div>
                                  );
                                },
                              )}
                            </div>
                          ) : null}
                        </article>
                      ) : null}
                      {scalpExpandedCronId ? (
                        <div className="mt-4">
                          <h4 className={`text-sm font-semibold ${scalpTextPrimaryClass}`}>
                            Research Overview
                          </h4>
                          <div className="mt-2 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-5">
                            {[
                              {
                                label: "Candidates",
                                value: scalpSummary?.researchSummary?.totalCandidates ?? 0,
                              },
                              {
                                label: "Stage A pass",
                                value: scalpSummary?.researchSummary?.stageAPass ?? 0,
                              },
                              {
                                label: "Stage B pass",
                                value: scalpSummary?.researchSummary?.stageBPass ?? 0,
                              },
                              {
                                label: "Stage C pass",
                                value: scalpSummary?.researchSummary?.stageCPass ?? 0,
                                tone: (scalpSummary?.researchSummary?.stageCPass ?? 0) > 0
                                  ? "positive" as const
                                  : "neutral" as const,
                              },
                              {
                                label: "Stage C fail",
                                value: scalpSummary?.researchSummary?.stageCFail ?? 0,
                                tone: (scalpSummary?.researchSummary?.stageCFail ?? 0) > 0
                                  ? "critical" as const
                                  : "neutral" as const,
                              },
                              {
                                label: "Symbols",
                                value: scalpSummary?.researchSummary?.uniqueSymbols ?? 0,
                              },
                              {
                                label: "Sessions",
                                value: (scalpSummary?.researchSummary?.uniqueSessions ?? []).join(", ") || "—",
                              },
                              {
                                label: "Avg netR",
                                value:
                                  scalpSummary?.researchSummary?.avgNetR != null
                                    ? `${scalpSummary.researchSummary.avgNetR >= 0 ? "+" : ""}${scalpSummary.researchSummary.avgNetR.toFixed(2)}R`
                                    : "—",
                              },
                              {
                                label: "Avg expectancy",
                                value:
                                  scalpSummary?.researchSummary?.avgExpR != null
                                    ? `${scalpSummary.researchSummary.avgExpR >= 0 ? "+" : ""}${scalpSummary.researchSummary.avgExpR.toFixed(3)}R`
                                    : "—",
                              },
                              {
                                label: "Highlights",
                                value: scalpResearchHighlightCount,
                                tone: scalpResearchHighlightCount > 0
                                  ? "positive" as const
                                  : "neutral" as const,
                              },
                            ].map((stat) => (
                              <div
                                key={`research-stat-${stat.label}`}
                                className={`rounded-lg border px-2.5 py-2 ${
                                  scalpDarkMode
                                    ? "border-zinc-700/80 bg-zinc-900/60"
                                    : "border-slate-200 bg-slate-50"
                                }`}
                              >
                                <div
                                  className={`text-[10px] uppercase tracking-[0.12em] ${scalpTextMutedClass}`}
                                >
                                  {stat.label}
                                </div>
                                <div
                                  className={`mt-0.5 text-sm font-semibold ${
                                    "tone" in stat && stat.tone === "positive"
                                      ? scalpDarkMode
                                        ? "text-emerald-300"
                                        : "text-emerald-600"
                                      : "tone" in stat && stat.tone === "critical"
                                        ? scalpDarkMode
                                          ? "text-rose-300"
                                          : "text-rose-600"
                                        : scalpTextPrimaryClass
                                  }`}
                                >
                                  {stat.value}
                                </div>
                              </div>
                            ))}
                          </div>
                          {scalpResearchCursors.length > 0 ? (
                            <div className="mt-3">
                              <h4
                                className={`text-sm font-semibold ${scalpTextPrimaryClass}`}
                              >
                                {`Cursors (${scalpResearchCursors.length})`}
                              </h4>
                              <div
                                className={`mt-2 overflow-x-auto rounded-lg border ${
                                  scalpDarkMode
                                    ? "border-zinc-700/80"
                                    : "border-slate-200"
                                }`}
                              >
                          <table
                            className={`w-full text-xs ${
                              scalpDarkMode ? "text-zinc-200" : "text-slate-700"
                            }`}
                          >
                            <thead>
                              <tr
                                className={
                                  scalpDarkMode
                                    ? "bg-zinc-800/80"
                                    : "bg-slate-50"
                                }
                              >
                                <th className="px-2 py-1.5 text-left font-semibold">
                                  Symbol
                                </th>
                                <th className="px-2 py-1.5 text-left font-semibold">
                                  Session
                                </th>
                                <th className="px-2 py-1.5 text-right font-semibold">
                                  Offset
                                </th>
                                <th className="px-2 py-1.5 text-left font-semibold">
                                  Phase
                                </th>
                                <th className="px-2 py-1.5 text-left font-semibold">
                                  Updated
                                </th>
                              </tr>
                            </thead>
                            <tbody>
                              {scalpResearchCursors.map((cursor) => (
                                <tr
                                  key={`cursor-${cursor.cursorKey || cursor.symbol}`}
                                  className={
                                    scalpDarkMode
                                      ? "border-t border-zinc-700/60"
                                      : "border-t border-slate-100"
                                  }
                                >
                                  <td className="px-2 py-1 font-medium">
                                    {cursor.symbol || "—"}
                                  </td>
                                  <td className="px-2 py-1">
                                    {cursor.entrySessionProfile || "—"}
                                  </td>
                                  <td className="px-2 py-1 text-right tabular-nums">
                                    {cursor.lastCandidateOffset ?? 0}
                                  </td>
                                  <td className="px-2 py-1">
                                    <span
                                      className={`inline-flex rounded-full border px-1.5 py-0.5 text-[10px] ${
                                        cursor.phase === "promote"
                                          ? scalpDarkMode
                                            ? "border-emerald-500/50 bg-emerald-500/10 text-emerald-300"
                                            : "border-emerald-300 bg-emerald-50 text-emerald-700"
                                          : scalpDarkMode
                                            ? "border-zinc-600 bg-zinc-800 text-zinc-300"
                                            : "border-slate-300 bg-slate-100 text-slate-600"
                                      }`}
                                    >
                                      {cursor.phase || "scan"}
                                    </span>
                                  </td>
                                  <td
                                    className={`px-2 py-1 ${scalpTextMutedClass}`}
                                  >
                                    {cursor.updatedAtMs
                                      ? new Date(cursor.updatedAtMs)
                                          .toISOString()
                                          .slice(0, 16)
                                          .replace("T", " ")
                                      : "—"}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    ) : null}
                        </div>
                      ) : null}
                    </article>
                  </section>

                  <section className={`${scalpSectionShellClass} p-4`}>
                    <div className="flex items-center justify-between">
                      <h3
                        className={`text-lg font-semibold ${scalpTextPrimaryClass}`}
                      >
                        Deployment Coverage
                      </h3>
                      <div className="flex items-center gap-2">
                        <span className={`text-xs ${scalpTextMutedClass}`}>
                          Enabled
                        </span>
                        <button
                          type="button"
                          role="switch"
                          aria-checked={scalpEnabledFilter === "enabled"}
                          aria-label="Toggle enabled filter"
                          onClick={() =>
                            setScalpEnabledFilter((prev) =>
                              prev === "enabled" ? "disabled" : "enabled",
                            )
                          }
                          className={`relative inline-flex h-6 w-11 items-center rounded-full border transition ${
                            scalpDarkMode
                              ? scalpEnabledFilter === "enabled"
                                ? "border-emerald-500/60 bg-emerald-500/20"
                                : "border-zinc-600 bg-zinc-800"
                              : scalpEnabledFilter === "enabled"
                                ? "border-emerald-300 bg-emerald-100"
                                : "border-slate-300 bg-slate-100"
                          }`}
                        >
                          <span
                            className={`inline-block h-4 w-4 transform rounded-full transition ${
                              scalpDarkMode
                                ? "bg-zinc-100"
                                : "bg-white shadow-sm"
                            } ${
                              scalpEnabledFilter === "enabled"
                                ? "translate-x-6"
                                : "translate-x-1"
                            }`}
                          />
                        </button>
                        <span
                          className={`text-xs font-semibold ${
                            scalpEnabledFilter === "enabled"
                              ? scalpDarkMode
                                ? "text-emerald-300"
                                : "text-emerald-700"
                              : scalpDarkMode
                                ? "text-rose-300"
                                : "text-rose-700"
                          }`}
                        >
                          {scalpEnabledFilter === "enabled" ? "yes" : "no"}
                        </span>
                        <span className={scalpTagNeutralClass}>
                          {`${scalpSelectedWorkerGridRows.length}/${scalpAllDeploymentsGridRows.length}`}
                        </span>
                      </div>
                    </div>
                    <div className={`mt-2 text-xs ${scalpTextSecondaryClass}`}>
                      One row per deployment in the registry. Weekly windows
                      are shown when worker history exists for that deployment;
                      otherwise the row still appears with registry-level
                      status and forward-validation state.
                    </div>
                    {scalpSelectedWorkerGridRows.length ? (
                      <div
                        className={`mt-4 h-[420px] w-full overflow-hidden rounded-xl border ${
                          scalpDarkMode
                            ? "border-zinc-700/60"
                            : "border-slate-200"
                        } ${scalpWorkerJobsGridThemeClass}`}
                      >
                        <AgGridReact
                          theme="legacy"
                          rowData={scalpVisibleWorkerGridRows}
                          columnDefs={scalpWorkerJobsGridColumnDefs}
                          defaultColDef={scalpWorkerJobsGridDefaultColDef}
                          rowHeight={54}
                          headerHeight={40}
                          immutableData
                          suppressScrollOnNewData
                          getRowId={handleScalpGridGetRowId}
                          animateRows
                          onBodyScrollEnd={handleScalpGridBodyScrollEnd}
                          onRowClicked={handleScalpGridRowClicked}
                        />
                      </div>
                    ) : (
                      <div
                        className={`mt-4 rounded-xl border px-3 py-4 text-sm ${
                          scalpDarkMode
                            ? "border-zinc-700/60 text-zinc-300"
                            : "border-slate-200 text-slate-600"
                        }`}
                      >
                        {scalpAllDeploymentsGridRows.length
                          ? "No deployments match the enabled filter."
                          : "No deployment registry rows are available yet."}
                      </div>
                    )}
                    <div className={`mt-2 text-xs ${scalpTextMutedClass}`}>
                      {`loaded ${scalpVisibleWorkerGridRows.length} rows of ${scalpSelectedWorkerGridRows.length}`}
                    </div>
                  </section>

                  <section className="grid grid-cols-1 gap-5">
                    <article className={`${scalpSectionShellClass} p-4`}>
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        {(() => {
                          const activeDeploymentId = String(
                            scalpActiveRuntimeRow?.deploymentId ||
                              scalpActiveOpsRow?.deploymentId ||
                              "",
                          ).trim();
                          const venue =
                            resolveScalpVenueUiFromDeploymentId(activeDeploymentId);
                          const iconSrc = SCALP_VENUE_ICON_SRC[venue];
                          const displayLabel =
                            stripScalpVenuePrefixFromDeploymentId(
                              activeDeploymentId,
                            ) ||
                            activeDeploymentId ||
                            "Selected Deployment";
                          return (
                            <h3
                              className={`min-w-0 flex-1 inline-flex items-center gap-2 text-lg font-semibold ${scalpTextPrimaryClass}`}
                            >
                              {activeDeploymentId ? (
                                <img
                                  src={iconSrc}
                                  alt={`${venue} venue`}
                                  className="h-4 w-auto opacity-85"
                                />
                              ) : null}
                              <span className="min-w-0 truncate" title={displayLabel}>
                                {displayLabel}
                              </span>
                            </h3>
                          );
                        })()}
                        <span
                          className={`shrink-0 text-xs ${scalpTextSecondaryClass}`}
                        >
                          {scalpActiveRuntimeRow
                            ? scalpActiveRuntimeRow.symbol
                            : scalpActiveOpsRow?.symbol || "—"}
                        </span>
                      </div>
                      <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-2">
                        <div className="space-y-3">
                          {scalpActiveRuntimeRow ? (
                            <>
                              <div className="flex flex-wrap items-center gap-2">
                                {(() => {
                                  const state = scalpStateMeta(
                                    scalpActiveRuntimeRow.state,
                                  );
                                  const StateIcon = state.Icon;
                                  const mode = scalpModeMeta(
                                    scalpActiveRuntimeRow.dryRunLast,
                                  );
                                  return (
                                    <>
                                      <span
                                        className={`inline-flex items-center gap-1 rounded-full border px-2 py-1 text-xs font-semibold ${state.className}`}
                                      >
                                        <StateIcon className="h-3.5 w-3.5" />
                                        {state.label}
                                      </span>
                                      <span
                                        className={`rounded-full border px-2 py-1 text-xs font-semibold ${mode.className}`}
                                      >
                                        {mode.label}
                                      </span>
                                    </>
                                  );
                                })()}
                                <span
                                  className={`${scalpTagNeutralClass} inline-flex items-center gap-1`}
                                >
                                  <TimerReset className="h-3.5 w-3.5" />
                                  {scalpActiveRuntimeRow.cronSchedule ||
                                    scalpActiveRuntimeRow.cronRoute ||
                                    "no schedule"}
                                </span>
                                <span
                                  className={`${scalpTagNeutralClass} inline-flex items-center gap-1`}
                                >
                                  <Activity className="h-3.5 w-3.5" />
                                  {formatScalpTime(
                                    scalpActiveExecutionTs ??
                                      scalpActiveRuntimeRow.lastRunAtMs,
                                  )}
                                </span>
                                <span
                                  className={`${scalpTagNeutralClass} inline-flex items-center gap-1`}
                                >
                                  <ArrowDownRight className="h-3.5 w-3.5" />
                                  {scalpActiveMaxDdR === null
                                    ? "DD —"
                                    : `DD ${Math.abs(scalpActiveMaxDdR).toFixed(2)}R`}
                                </span>
                              </div>
                              {(() => {
                                const trades = Math.max(
                                  0,
                                  Number(scalpActiveRuntimeRow.tradesPlaced || 0),
                                );
                                const wins = Math.max(
                                  0,
                                  Number(scalpActiveRuntimeRow.wins || 0),
                                );
                                const losses = Math.max(
                                  0,
                                  Number(scalpActiveRuntimeRow.losses || 0),
                                );
                                const closedTrades = wins + losses;
                                const tradesPct = Math.max(
                                  0,
                                  Math.min(100, trades * 2),
                                );
                                const winPct =
                                  scalpActiveWinRatePct === null
                                    ? null
                                    : Math.max(
                                        0,
                                        Math.min(100, scalpActiveWinRatePct),
                                      );
                                const netRPct =
                                  scalpActiveNetR === null
                                    ? null
                                    : Math.max(
                                        0,
                                        Math.min(100, 50 + scalpActiveNetR * 12.5),
                                      );
                                const winSplitPct =
                                  closedTrades > 0 ? (wins / closedTrades) * 100 : 0;
                                const lossSplitPct =
                                  closedTrades > 0
                                    ? (losses / closedTrades) * 100
                                    : 0;
                                const compactCardClass = scalpDarkMode
                                  ? "rounded-xl border border-zinc-700 bg-zinc-950/70 px-2.5 py-2"
                                  : "rounded-xl border border-slate-200 bg-slate-50 px-2.5 py-2";
                                return (
                                  <div className="grid grid-cols-2 gap-2 xl:grid-cols-4">
                                    <div className={compactCardClass}>
                                      <div
                                        className={`flex items-center justify-between text-[11px] ${scalpTextMutedClass}`}
                                      >
                                        <span className="inline-flex items-center gap-1">
                                          <ListChecks className="h-3.5 w-3.5" />
                                          Trades
                                        </span>
                                        <span
                                          className={`text-sm font-semibold ${scalpTextPrimaryClass}`}
                                        >
                                          {trades}
                                        </span>
                                      </div>
                                      <div
                                        className={`mt-1.5 h-1.5 overflow-hidden rounded-full ${scalpVisualMetricTrackClass}`}
                                      >
                                        <div
                                          className="h-full bg-sky-500"
                                          style={{
                                            width: `${Math.max(6, tradesPct)}%`,
                                          }}
                                        />
                                      </div>
                                    </div>
                                    <div className={compactCardClass}>
                                      <div
                                        className={`flex items-center justify-between text-[11px] ${scalpTextMutedClass}`}
                                      >
                                        <span className="inline-flex items-center gap-1">
                                          <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                                          Win
                                        </span>
                                        <span className="text-sm font-semibold text-emerald-500">
                                          {winPct === null
                                            ? "—"
                                            : `${winPct.toFixed(0)}%`}
                                        </span>
                                      </div>
                                      <div
                                        className={`mt-1.5 h-1.5 overflow-hidden rounded-full ${scalpVisualMetricTrackClass}`}
                                      >
                                        <div
                                          className="h-full bg-emerald-500"
                                          style={{
                                            width: `${Math.max(6, winPct || 0)}%`,
                                          }}
                                        />
                                      </div>
                                    </div>
                                    <div className={compactCardClass}>
                                      <div
                                        className={`flex items-center justify-between text-[11px] ${scalpTextMutedClass}`}
                                      >
                                        <span className="inline-flex items-center gap-1">
                                          <BarChart3 className="h-3.5 w-3.5" />
                                          Net R
                                        </span>
                                        <span
                                          className={`text-sm font-semibold ${
                                            scalpActiveNetR === null
                                              ? scalpTextPrimaryClass
                                              : scalpActiveNetR >= 0
                                                ? "text-emerald-500"
                                                : "text-rose-500"
                                          }`}
                                        >
                                          {scalpActiveNetR === null
                                            ? "—"
                                            : formatSignedR(scalpActiveNetR)}
                                        </span>
                                      </div>
                                      <div
                                        className={`mt-1.5 h-1.5 overflow-hidden rounded-full ${scalpVisualMetricTrackClass}`}
                                      >
                                        <div
                                          className={`h-full ${
                                            scalpActiveNetR === null
                                              ? scalpDarkMode
                                                ? "bg-zinc-400/80"
                                                : "bg-slate-500"
                                              : scalpActiveNetR >= 0
                                                ? "bg-emerald-500"
                                                : "bg-rose-500"
                                          }`}
                                          style={{
                                            width: `${Math.max(6, netRPct || 0)}%`,
                                          }}
                                        />
                                      </div>
                                    </div>
                                    <div className={compactCardClass}>
                                      <div
                                        className={`flex items-center justify-between text-[11px] ${scalpTextMutedClass}`}
                                      >
                                        <span className="inline-flex items-center gap-1">
                                          <Activity className="h-3.5 w-3.5" />
                                          W/L
                                        </span>
                                        <span
                                          className={`text-sm font-semibold ${scalpTextPrimaryClass}`}
                                        >
                                          {`${wins}/${losses}`}
                                        </span>
                                      </div>
                                      <div
                                        className={`mt-1.5 flex h-1.5 overflow-hidden rounded-full ${scalpVisualMetricTrackClass}`}
                                      >
                                        <div
                                          className="bg-emerald-500"
                                          style={{
                                            width: `${Math.max(0, winSplitPct)}%`,
                                          }}
                                        />
                                        <div
                                          className="bg-rose-500"
                                          style={{
                                            width: `${Math.max(0, lossSplitPct)}%`,
                                          }}
                                        />
                                      </div>
                                    </div>
                                  </div>
                                );
                              })()}
                              <div
                                className={`flex items-center gap-1.5 text-[11px] ${scalpTextMutedClass}`}
                              >
                                <Database className="h-3.5 w-3.5" />
                                <span
                                  className={`truncate font-mono ${scalpTextSecondaryClass}`}
                                >
                                  {scalpActiveRuntimeRow.deploymentId}
                                </span>
                              </div>
                            </>
                          ) : (
                            <div
                              className={`mt-1 text-sm ${scalpTextSecondaryClass}`}
                            >
                              No runtime state found for the selected deployment.
                            </div>
                          )}
                          <div
                            className={`rounded-xl border p-2.5 ${
                              scalpDarkMode
                                ? "border-zinc-700 bg-zinc-950/60"
                                : "border-slate-200 bg-slate-50"
                            }`}
                          >
                            <div className="flex flex-wrap items-center justify-between gap-2">
                              <div
                                className={`text-xs uppercase tracking-[0.16em] ${scalpTextMutedClass}`}
                              >
                                Reason Snapshot
                              </div>
                              <div className={`text-xs ${scalpTextMutedClass}`}>
                                {scalpReasonSnapshotState === "fresh"
                                  ? `${scalpActiveReasonCodes.length} shown`
                                  : "none"}
                              </div>
                            </div>
                            {scalpActiveReasonCodes.length ? (
                              <div className="mt-2 flex flex-wrap gap-1.5">
                                {scalpActiveReasonCodes.map((code, idx) => {
                                  const meta = scalpReasonMeta(code);
                                  const Icon = meta.Icon;
                                  return (
                                    <span
                                      key={`${code}-${idx}`}
                                      className={`inline-flex items-center gap-1 rounded-full border px-2 py-1 text-[11px] font-semibold ${meta.className}`}
                                    >
                                      <Icon className="h-3.5 w-3.5" />
                                      {code.replace(/_/g, " ")}
                                    </span>
                                  );
                                })}
                              </div>
                            ) : (
                              <div
                                className={`mt-2 text-sm ${scalpTextSecondaryClass}`}
                              >
                                No reason codes recorded for this deployment.
                              </div>
                            )}
                          </div>
                        </div>

                        <div
                          className={`rounded-xl border p-2.5 ${
                            scalpDarkMode
                              ? "border-zinc-700 bg-zinc-950/60"
                              : "border-slate-200 bg-slate-50"
                          }`}
                        >
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <div
                              className={`text-xs uppercase tracking-[0.16em] ${scalpTextMutedClass}`}
                            >
                              Journal Snapshot
                            </div>
                            <div className={`text-xs ${scalpTextMutedClass}`}>
                              {scalpActiveJournal.length
                                ? `${Math.min(scalpActiveJournal.length, 4)} events`
                                : "empty"}
                            </div>
                          </div>
                          {scalpActiveJournal.length ? (
                            <div className="mt-2 space-y-1.5">
                              {scalpActiveJournal.slice(0, 4).map((entry) => {
                                const meta = scalpJournalMeta({
                                  type: String(entry.type || ""),
                                  level: String(entry.level || ""),
                                });
                                const Icon = meta.Icon;
                                return (
                                  <div
                                    key={
                                      entry.id ||
                                      `${entry.timestampMs}-${entry.symbol || "na"}`
                                    }
                                    className={`rounded-xl border px-2.5 py-1.5 text-xs ${meta.className}`}
                                  >
                                    <div className="flex flex-wrap items-center justify-between gap-2">
                                      <div className="inline-flex items-center gap-1.5 font-semibold">
                                        <Icon className="h-3.5 w-3.5" />
                                        {String(
                                          entry.type || "event",
                                        ).toUpperCase()}
                                      </div>
                                      <div>{formatScalpTime(entry.timestampMs)}</div>
                                    </div>
                                    {(entry.reasonCodes || []).length ? (
                                      <div className="mt-1 flex flex-wrap gap-1">
                                        {(entry.reasonCodes || [])
                                          .slice(0, 3)
                                          .map((code, idx) => (
                                            <span
                                              key={`${entry.id || entry.timestampMs || idx}-${code}-${idx}`}
                                              className={`rounded-full border px-1.5 py-0.5 text-[10px] font-semibold ${
                                                scalpDarkMode
                                                  ? "border-current/20 bg-black/20"
                                                  : "border-current/30 bg-white/50"
                                              }`}
                                            >
                                              {code.replace(/_/g, " ")}
                                            </span>
                                          ))}
                                      </div>
                                    ) : null}
                                  </div>
                                );
                              })}
                            </div>
                          ) : (
                            <div
                              className={`mt-2 text-sm ${scalpTextSecondaryClass}`}
                            >
                              No journal events for this deployment yet.
                            </div>
                          )}
                        </div>
                      </div>
                    </article>
                  </section>
                </div>
              )
            ) : isInitialLoading ? (
              renderDashboardSkeleton()
            ) : !symbols.length ? (
              <div className="flex items-center justify-center py-12 text-sm font-semibold text-slate-500">
                No evaluations found.
              </div>
            ) : current ? (
              <div className="grid grid-cols-1 gap-5 lg:grid-cols-2 lg:items-stretch">
                <div className="space-y-4 lg:col-span-2">
                  <div className="relative rounded-2xl border border-slate-200 bg-slate-50 p-4 h-full">
                    <div className="absolute right-4 top-4">
                      <img
                        src={activePlatformLogo}
                        alt={`${activePlatform} platform`}
                        className="h-5 w-auto opacity-80"
                      />
                    </div>
                    <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                      <div>
                        <div className="text-xs uppercase tracking-wide text-slate-500">
                          {dashboardRange} PnL
                        </div>
                        <div className="mt-3 text-3xl font-semibold text-slate-900">
                          <span
                            className={
                              typeof effectivePnl7dWithOpen === "number"
                                ? effectivePnl7dWithOpen >= 0
                                  ? "text-emerald-600"
                                  : "text-rose-600"
                                : "text-slate-500"
                            }
                          >
                            {typeof effectivePnl7dWithOpen === "number"
                              ? `${effectivePnl7dWithOpen.toFixed(2)}%`
                              : typeof current.pnl7d === "number"
                                ? `${current.pnl7d.toFixed(2)}%`
                                : "—"}
                            {typeof current.pnl7dNet === "number" ? (
                              <span className="ml-1 align-middle text-sm font-medium text-slate-500">
                                ({current.pnl7dNet >= 0 ? "+" : ""}
                                {formatUsd(current.pnl7dNet)})
                              </span>
                            ) : null}
                          </span>
                        </div>
                        <p className="mt-1 text-xs text-slate-500">
                          from {current.pnl7dTrades ?? 0}{" "}
                          {current.pnl7dTrades === 1 ? "trade" : "trades"} in
                          last {dashboardRangeText}
                          {typeof effectiveOpenPnl === "number"
                            ? " + open position"
                            : ""}
                        </p>
                        <div className="mt-1 text-[11px] text-slate-500">
                          {typeof current.pnl7dGross === "number" ||
                          typeof current.pnl7d === "number" ? (
                            <>
                              gross vs net:{" "}
                              <span className="font-semibold text-slate-700">
                                {typeof current.pnl7dGross === "number"
                                  ? current.pnl7dGross.toFixed(2)
                                  : "—"}
                                %
                              </span>{" "}
                              /{" "}
                              <span className="font-semibold text-slate-700">
                                {typeof current.pnl7d === "number"
                                  ? current.pnl7d.toFixed(2)
                                  : "—"}
                                %
                              </span>
                            </>
                          ) : null}
                        </div>
                      </div>
                      <div>
                        <div className="text-xs uppercase tracking-wide text-slate-500">
                          Last PNL
                        </div>
                        <div className="mt-3 text-3xl font-semibold text-slate-900">
                          <span
                            className={
                              typeof current.lastPositionPnl === "number"
                                ? current.lastPositionPnl >= 0
                                  ? "text-emerald-600"
                                  : "text-rose-600"
                                : "text-slate-500"
                            }
                          >
                            {typeof current.lastPositionPnl === "number"
                              ? `${current.lastPositionPnl.toFixed(2)}%`
                              : "—"}
                          </span>
                        </div>
                        <p className="mt-1 text-xs text-slate-500">
                          {typeof current.lastPositionPnl === "number" ? (
                            <span className="flex flex-wrap items-center gap-2">
                              <span className="flex items-center gap-1">
                                direction –
                                {current.lastPositionDirection ? (
                                  <span
                                    className={`${
                                      current.lastPositionDirection === "long"
                                        ? "text-emerald-600"
                                        : "text-rose-600"
                                    }`}
                                  >
                                    {current.lastPositionDirection}
                                  </span>
                                ) : null}
                              </span>
                              {typeof current.lastPositionLeverage ===
                              "number" ? (
                                <span className="rounded-full bg-slate-100 px-2 py-0.5 text-[11px] font-semibold text-slate-700">
                                  {current.lastPositionLeverage.toFixed(0)}x
                                </span>
                              ) : null}
                            </span>
                          ) : (
                            "no recent positions"
                          )}
                        </p>
                        {typeof current.winRate === "number" ||
                        typeof current.avgWinPct === "number" ||
                        typeof current.avgLossPct === "number" ? (
                          <div className="mt-2 text-[11px] text-slate-500">
                            {typeof current.winRate === "number"
                              ? `Win rate: ${current.winRate.toFixed(0)}%`
                              : ""}
                            {typeof current.avgWinPct === "number"
                              ? ` · Avg win: ${current.avgWinPct.toFixed(2)}%`
                              : ""}
                            {typeof current.avgLossPct === "number"
                              ? ` · Avg loss: ${current.avgLossPct.toFixed(2)}%`
                              : ""}
                          </div>
                        ) : null}
                      </div>
                      <div>
                        <div className="text-xs uppercase tracking-wide text-slate-500">
                          Open PNL
                        </div>
                        <div className="mt-3 text-3xl font-semibold text-slate-900">
                          <span
                            className={
                              typeof effectiveOpenPnl === "number"
                                ? effectiveOpenPnl >= 0
                                  ? "text-emerald-600"
                                  : "text-rose-600"
                                : "text-slate-500"
                            }
                          >
                            {typeof effectiveOpenPnl === "number"
                              ? `${effectiveOpenPnl.toFixed(2)}%`
                              : "—"}
                          </span>
                        </div>
                        <p className="mt-1 text-xs text-slate-500">
                          {typeof effectiveOpenPnl === "number" ? (
                            <span className="flex flex-wrap items-center gap-2">
                              <span className="flex items-center gap-1">
                                direction –
                                {current.openDirection ? (
                                  <span
                                    className={
                                      current.openDirection === "long"
                                        ? "text-emerald-600"
                                        : "text-rose-600"
                                    }
                                  >
                                    {current.openDirection}
                                  </span>
                                ) : null}
                              </span>
                              {typeof current.openLeverage === "number" ? (
                                <span className="rounded-full bg-slate-100 px-2 py-0.5 text-[11px] font-semibold text-slate-700">
                                  {current.openLeverage.toFixed(0)}x
                                </span>
                              ) : null}
                              {openPnlIsLive ? (
                                <span className="rounded-full bg-emerald-50 px-2 py-0.5 text-[11px] font-semibold text-emerald-700">
                                  live
                                </span>
                              ) : null}
                            </span>
                          ) : (
                            "no open position"
                          )}
                        </p>
                      </div>
                    </div>
                  </div>
                </div>

                {showChartPanel ? (
                  <ChartPanel
                    key={activeSymbol}
                    symbol={activeSymbol}
                    platform={current?.lastPlatform || null}
                    adminSecret={resolveAdminSecret()}
                    adminGranted={adminGranted}
                    isDark={resolvedTheme === "dark"}
                    rangeKey={dashboardRange}
                    onRangeChange={setDashboardRange}
                    livePrice={livePriceNow}
                    liveTimestamp={livePriceTs}
                    liveConnected={livePriceConnected}
                  />
                ) : null}

                {hasLastDecision && (
                  <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
                    <div className="flex flex-wrap items-center justify-between gap-3">
                      <div className="flex items-center gap-2 text-xs uppercase tracking-wide text-slate-500">
                        <span>Latest Decision</span>
                        {current.lastDecisionTs ? (
                          <span className="lowercase text-slate-400">
                            {formatDecisionTime(current.lastDecisionTs)}
                          </span>
                        ) : null}
                      </div>
                      <div className="flex items-center gap-2">
                        {current.lastDecision?.signal_strength && (
                          <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-semibold text-slate-700">
                            Strength: {current.lastDecision.signal_strength}
                          </span>
                        )}
                      </div>
                    </div>
                    <div className="mt-3 text-sm text-slate-800">
                      Action:{" "}
                      <span className="font-semibold text-sky-700">
                        {(
                          (current.lastDecision as any)?.action || ""
                        ).toString() || "—"}
                      </span>
                      {(current.lastDecision as any)?.summary
                        ? ` · ${(current.lastDecision as any).summary}`
                        : ""}
                    </div>
                    {(current.lastDecision as any)?.reason ? (
                      <p className="mt-2 text-sm text-slate-700 whitespace-pre-wrap">
                        <span className="font-semibold text-slate-800">
                          Reason:{" "}
                        </span>
                        {(current.lastDecision as any).reason}
                      </p>
                    ) : null}
                    <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-4">
                      {biasOrder.map(({ key, label }) => {
                        const raw = (current.lastDecision as any)?.[key];
                        const val =
                          typeof raw === "string" ? raw.toUpperCase() : raw;
                        const tfLabel =
                          current.lastBiasTimeframes?.[
                            key.replace("_bias", "")
                          ] || null;
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
                            className="flex items-center justify-between rounded-lg border border-slate-100 bg-slate-50 px-3 py-2"
                          >
                            <span className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                              {displayLabel}
                            </span>
                            <span
                              className={`flex items-center gap-1 text-sm font-semibold ${meta.color}`}
                            >
                              <Icon className="h-4 w-4" />
                              {val || "—"}
                            </span>
                          </div>
                        );
                      })}
                    </div>
                    <div className="mt-3">
                      <button
                        onClick={() => setShowPrompt((prev) => !prev)}
                        className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 transition hover:border-slate-300"
                      >
                        {showPrompt ? "Hide prompt" : "Show prompt"}
                      </button>
                    </div>
                    {showPrompt && (
                      <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2">
                        <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                          <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                            System
                          </div>
                          <div className="mt-2">
                            {renderPromptContent(current.lastPrompt?.system)}
                          </div>
                        </div>
                        <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                          <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                            User
                          </div>
                          <div className="mt-2">
                            {renderPromptContent(current.lastPrompt?.user)}
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                )}

                <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2 text-xs uppercase tracking-wide text-slate-500">
                      <span>Latest Evaluation</span>
                      {current.evaluationTs ? (
                        <span className="lowercase text-slate-400">
                          {formatDecisionTime(current.evaluationTs)}
                        </span>
                      ) : null}
                    </div>
                    {current?.evaluation?.confidence && (
                      <div className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
                        Confidence: {current.evaluation.confidence}
                      </div>
                    )}
                  </div>
                  <div className="mt-2 text-lg font-semibold text-slate-900 flex items-center gap-3 flex-wrap">
                    <span>
                      Rating:{" "}
                      <span className="text-sky-600">
                        {current?.evaluation?.overall_rating ?? "—"}
                      </span>
                    </span>
                    <div className="flex flex-wrap items-center gap-1">
                      {Array.from({ length: 10 }).map((_, idx) => {
                        const ratingVal = Number(
                          current?.evaluation?.overall_rating ?? 0,
                        );
                        const filled = ratingVal >= idx + 1;
                        const colorClass =
                          ratingVal >= 9
                            ? "text-emerald-500 fill-emerald-500"
                            : ratingVal >= 8
                              ? "text-emerald-400 fill-emerald-400"
                              : ratingVal >= 6
                                ? "text-lime-400 fill-lime-400"
                                : ratingVal >= 5
                                  ? "text-amber-400 fill-amber-400"
                                  : ratingVal >= 3
                                    ? "text-orange-400 fill-orange-400"
                                    : "text-rose-500 fill-rose-500";
                        return (
                          <Star
                            key={idx}
                            className={`h-4 w-4 ${filled ? colorClass : "stroke-slate-300 text-slate-300"}`}
                          />
                        );
                      })}
                    </div>
                  </div>
                  <p className="mt-3 text-sm text-slate-700">
                    {current?.evaluation?.overview || "No overview provided."}
                  </p>
                  {((current?.evaluation?.aspects ?? null) || hasDetails) && (
                    <div className="mt-4 space-y-4">
                      {current?.evaluation?.aspects && (
                        <>
                          <div className="flex flex-wrap items-center gap-2">
                            <button
                              onClick={() => setShowAspects((prev) => !prev)}
                              className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 transition hover:border-slate-300"
                            >
                              {showAspects
                                ? "Hide aspect ratings"
                                : "Show aspect ratings"}
                            </button>
                            <button
                              onClick={() =>
                                setShowRawEvaluation((prev) => !prev)
                              }
                              className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 transition hover:border-slate-300"
                            >
                              {showRawEvaluation
                                ? "Hide raw JSON"
                                : "Show raw JSON"}
                            </button>
                          </div>
                          {showRawEvaluation && (
                            <pre className="overflow-auto rounded-xl border border-slate-800 bg-slate-900/95 p-3 font-mono text-[11px] leading-snug text-slate-100">
                              {JSON.stringify(current.evaluation, null, 2)}
                            </pre>
                          )}
                          {showAspects && (
                            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                              {Object.entries(current.evaluation.aspects).map(
                                ([key, val]) => (
                                  <div
                                    key={key}
                                    className="rounded-xl border border-slate-200 bg-slate-50 p-3 shadow-inner shadow-slate-100"
                                  >
                                    <div className="flex items-center justify-between">
                                      <div className="flex items-center gap-2">
                                        {(() => {
                                          const meta = aspectMeta[key] || {
                                            Icon: Circle,
                                            color: "text-slate-600",
                                            bg: "bg-slate-100",
                                          };
                                          const Icon = meta.Icon;
                                          return (
                                            <span className="flex h-9 w-9 items-center justify-center rounded-full bg-slate-100 text-slate-600">
                                              <Icon className="h-4 w-4" />
                                            </span>
                                          );
                                        })()}
                                        <div className="text-sm font-semibold text-slate-900">
                                          {formatLabel(key)}
                                        </div>
                                      </div>
                                      <div className="text-lg font-semibold text-sky-700">
                                        {val?.rating ?? "—"}
                                      </div>
                                    </div>
                                    <p className="mt-2 text-xs text-slate-600">
                                      {val?.comment || "No comment"}
                                    </p>
                                    {(val?.checks?.length ||
                                      val?.improvements?.length ||
                                      val?.findings?.length) && (
                                      <div className="mt-3 space-y-2">
                                        {val?.checks?.length ? (
                                          <div>
                                            <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                              Checks
                                            </div>
                                            <ul className="mt-1 list-disc space-y-1 pl-4 text-xs text-slate-700">
                                              {val.checks.map((item, idx) => (
                                                <li key={idx}>{item}</li>
                                              ))}
                                            </ul>
                                          </div>
                                        ) : null}
                                        {val?.improvements?.length ? (
                                          <div>
                                            <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                              Improvements
                                            </div>
                                            <ul className="mt-1 list-disc space-y-1 pl-4 text-xs text-amber-800">
                                              {val.improvements.map(
                                                (item, idx) => (
                                                  <li key={idx}>{item}</li>
                                                ),
                                              )}
                                            </ul>
                                          </div>
                                        ) : null}
                                        {val?.findings?.length ? (
                                          <div>
                                            <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                              Findings
                                            </div>
                                            <ul className="mt-1 list-disc space-y-1 pl-4 text-xs text-rose-800">
                                              {val.findings.map((item, idx) => (
                                                <li key={idx}>{item}</li>
                                              ))}
                                            </ul>
                                          </div>
                                        ) : null}
                                      </div>
                                    )}
                                  </div>
                                ),
                              )}
                            </div>
                          )}
                        </>
                      )}

                      {((current?.evaluation?.aspects && showAspects) ||
                        !current?.evaluation?.aspects) &&
                        hasDetails && (
                          <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3">
                            <div className="text-xs uppercase tracking-wide text-slate-500">
                              Details
                            </div>
                            <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-3">
                              {current.evaluation.what_went_well?.length ? (
                                <div className="rounded-xl border border-emerald-100 bg-emerald-50 p-3">
                                  <div className="text-sm font-semibold text-emerald-800">
                                    What went well
                                  </div>
                                  <ul className="mt-2 list-disc space-y-1 pl-4 text-sm text-slate-800">
                                    {current.evaluation.what_went_well.map(
                                      (item, idx) => (
                                        <li key={idx}>{item}</li>
                                      ),
                                    )}
                                  </ul>
                                </div>
                              ) : null}
                              {current.evaluation.issues?.length ? (
                                <div className="rounded-xl border border-rose-100 bg-rose-50 p-3">
                                  <div className="text-sm font-semibold text-rose-800">
                                    Issues
                                  </div>
                                  <ul className="mt-2 list-disc space-y-1 pl-4 text-sm text-slate-800">
                                    {current.evaluation.issues.map(
                                      (item, idx) => (
                                        <li key={idx}>{item}</li>
                                      ),
                                    )}
                                  </ul>
                                </div>
                              ) : null}
                              {current.evaluation.improvements?.length ? (
                                <div className="rounded-xl border border-amber-100 bg-amber-50 p-3">
                                  <div className="text-sm font-semibold text-amber-800">
                                    Improvements
                                  </div>
                                  <ul className="mt-2 list-disc space-y-1 pl-4 text-sm text-slate-800">
                                    {current.evaluation.improvements.map(
                                      (item, idx) => (
                                        <li key={idx}>{item}</li>
                                      ),
                                    )}
                                  </ul>
                                </div>
                              ) : null}
                            </div>
                          </div>
                        )}
                    </div>
                  )}
                </div>
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
