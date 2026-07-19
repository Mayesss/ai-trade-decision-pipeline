import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Head from "next/head";
import dynamic from "next/dynamic";
import { ChartSkeleton, TimelineSkeleton } from "../components/ChartSkeleton";
import {
  AllCommunityModule,
  ModuleRegistry,
  type ColDef,
} from "ag-grid-community";
import vercelConfig from "../vercel.json";
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
  AlertTriangle,
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
  pnlDaily?: Array<{ day: string; net: number | null; trades: number }> | null;
  pendingEntry?: boolean;
  openPnl?: number | null;
  openDirection?: "long" | "short" | null;
  openLeverage?: number | null;
  openEntryPrice?: number | null;
  lastPositionPnl?: number | null;
  lastPositionDirection?: "long" | "short" | null;
  lastPositionLeverage?: number | null;
  lastWasAiCall?: boolean;
  lastAiDecisionTs?: number | null;
  lastAiDecisionAction?: string | null;
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
  // Per-Berlin-day closed nets in venue cash (mirrors the summary API) —
  // folded across symbols into the header week-calendar strip.
  pnlDaily?: Array<{ day: string; net: number | null; trades: number }> | null;
  // A pullback entry limit is resting on the venue — ranks the pill between
  // open positions and fresh AI decisions.
  pendingEntry?: boolean;
  openPnl?: number | null;
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

type ScalpResearchHealthResponse = {
  ok?: boolean;
  mode?: "scalp_v2" | string;
  nowMs?: number | null;
  staleLockMinutes?: number | null;
  health?: {
    staleThresholdMs?: number | null;
    stale?: boolean;
    approachingStale?: boolean;
    lockAgeMs?: number | null;
    heartbeatAgeMs?: number | null;
  } | null;
  job?: {
    status?: string;
    attempts?: number | null;
    locked?: boolean;
    lockedAtMs?: number | null;
    updatedAtMs?: number | null;
    nextRunAtMs?: number | null;
    phase?: string | null;
    reason?: string | null;
    progress?: Record<string, unknown> | null;
  } | null;
  hint?: {
    tone?: "ok" | "warn" | "critical" | "info" | string;
    label?: string;
    detail?: string | null;
  } | null;
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
    totalDeployments?: number;
    totalCandidates?: number;
    candidateStatusCounts?: Partial<Record<ScalpCandidateStatusUi, number>>;
    symbolCoverage?: Array<{ symbol: string; candidates: number; deployments: number }>;
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
  _stageMaxWeeklyNetR?: number | null;
  _stageLargestR?: number | null;
  _stageExitReasons?: Record<string, unknown> | null;
  _stageReason?: string | null;
  _stageWindowKind?: string | null;
  _workerVersion?: string | null;
  _workerWindowToTs?: number | null;
  _holdoutFromTs?: number | null;
  _holdoutToTs?: number | null;
  _holdoutPassed?: boolean | null;
  _holdoutReason?: string | null;
  _holdoutTrades?: number | null;
  _holdoutNetR?: number | null;
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

type ScalpCandidateStatusUi =
  | "discovered"
  | "evaluated"
  | "promoted"
  | "rejected";

type ScalpCandidateGridStateUi = ScalpCandidateStatusUi | "enabled" | "all";
type ScalpCandidatesPageRequest = {
  offset: number;
  session: ScalpEntrySessionFilterUi;
  stateFilter: ScalpCandidateGridStateUi;
  reset?: boolean;
};

type ScalpWorkerJobGridRow = {
  rowId: string;
  candidateId: number | null;
  deploymentId: string | null;
  candidateStatus: ScalpCandidateStatusUi;
  candidateState: Exclude<ScalpCandidateGridStateUi, "all">;
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
    kind?: "training" | "window";
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
  exitReasons: { stop: number; stopLoss: number; stopBe: number; stopTrail: number; tp: number; timeStop: number; forceClose: number } | null;
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
  // worker status, and — once succeeded — verdict + distilled lesson.
  postmortemId?: number;
  postmortemStatus?: string;
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

// Full post-mortem row (mirrors lib/swing/pg SwingPostmortemRow — the fields
// the panel renders; report/dossier stay loosely typed).
type PostmortemUi = {
  id: number;
  platform: string;
  symbol: string;
  positionKey: string;
  status: "queued" | "running" | "succeeded" | "failed";
  trigger: string;
  side: string | null;
  entryTsMs: number | null;
  exitTsMs: number | null;
  entryPrice: number | null;
  exitPrice: number | null;
  pnlPct: number | null;
  pnlNet: number | null;
  verdict: string | null;
  lesson: string | null;
  report: {
    confidence?: number;
    timeline_analysis?: string;
    what_went_wrong?: string[];
    missed_signals?: Array<{
      ts_utc?: string;
      description?: string;
      visible_in?: string;
    }>;
    gate_impact?: string | null;
    suggestions?: string[];
    lesson_adherence?: string | null;
    lesson_action?: string;
  } | null;
  dossier: Record<string, any> | null;
  model: string | null;
  usage: Record<string, any> | null;
  error: string | null;
  attempts: number;
};

type EvaluateJobStatus = "queued" | "running" | "succeeded" | "failed";

type EvaluateJobRecord = {
  id: string;
  status: EvaluateJobStatus;
  updatedAt?: number;
  error?: string;
};

type DashboardRangeKey = "1D" | "7D" | "30D" | "6M";
type ThemePreference = "system" | "light" | "dark";
type ResolvedTheme = "light" | "dark";
type StrategyMode = "swing" | "scalp";
type ScalpEntrySessionProfileUi =
  | "berlin"
  | "tokyo"
  | "newyork"
  | "pacific"
  | "sydney";
type ScalpEntrySessionFilterUi = "all" | ScalpEntrySessionProfileUi;

const THEME_PREFERENCE_STORAGE_KEY = "dashboard_theme_preference";
// Fallback EURUSD rate for the header rollup when the live quote isn't
// available — the strip is explicitly approximate, so a ballpark is fine.
const EUR_USD_FALLBACK_RATE = 1.1;
// Cash PnL is venue-denominated: Bitget settles in USDT (shown as $), the
// Capital.com ledger is in the account currency, EUR. Symbol picked per
// platform — never sum across the two.
const platformCurrencySymbol = (platform?: string | null): "$" | "€" =>
  String(platform || "").toLowerCase() === "capital" ? "€" : "$";
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
const formatSignedR = (value: number): string =>
  `${value >= 0 ? "+" : ""}${value.toFixed(2)}R`;

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
const SCALP_MIN_REFRESH_GAP_MS = 25_000;
const SCALP_WORKER_TASK_LIMIT_FULL = 5_000;
const SCALP_GRID_LOAD_BATCH = 60;
const SCALP_CANDIDATE_GRID_STATES: ScalpCandidateGridStateUi[] = [
  "enabled",
  "all",
  "evaluated",
  "rejected",
  "promoted",
  "discovered",
];
const createScalpCandidateTotalsByState = (): Record<
  ScalpCandidateGridStateUi,
  number
> => ({
  enabled: 0,
  all: 0,
  evaluated: 0,
  rejected: 0,
  promoted: 0,
  discovered: 0,
});
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
    normalized === "pacific" ||
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

function normalizeScalpCandidateStatusUi(value: unknown): ScalpCandidateStatusUi {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "evaluated") return "evaluated";
  if (normalized === "promoted") return "promoted";
  if (normalized === "rejected") return "rejected";
  return "discovered";
}

const ADMIN_SECRET_STORAGE_KEY = "admin_access_secret";
const ADMIN_AUTH_TIMEOUT_MS = 4000;
const STRATEGY_MODE_STORAGE_KEY = "strategy_mode";
const SCALP_ENTRY_SESSION_STORAGE_KEY = "scalp_entry_session_filter_v2";
const SCALP_ENTRY_SESSION_FILTER_OPTIONS: Array<{
  id: ScalpEntrySessionFilterUi;
  label: string;
}> = [
  { id: "all", label: "All" },
  { id: "tokyo", label: "Tokyo" },
  { id: "berlin", label: "Berlin" },
  { id: "newyork", label: "New York" },
  { id: "pacific", label: "Pacific" },
  { id: "sydney", label: "Sydney" },
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

type ScalpBrokerSeatUiStatus =
  | "winner"
  | "management_only"
  | "excluded"
  | "legacy"
  | "disabled";

type ScalpBrokerTimelineTone =
  | "active"
  | "management"
  | "blocked"
  | "legacy"
  | "disabled";

type ScalpBrokerTimelineBlock = {
  id: string;
  venue: ScalpVenueUi;
  symbol: string;
  session: ScalpEntrySessionProfileUi;
  deploymentId: string;
  label: string;
  detail: string;
  temporalLabel: string;
  status: ScalpBrokerSeatUiStatus;
  tone: ScalpBrokerTimelineTone;
  startMinute: number;
  endMinute: number;
  leftPct: number;
  widthPct: number;
  lane: number;
};

type ScalpBrokerTimeline = {
  blocks: ScalpBrokerTimelineBlock[];
  laneCount: number;
  activeCount: number;
  managementCount: number;
  blockedCount: number;
};

const SCALP_V3_SESSION_TIME_ZONE: Record<ScalpEntrySessionProfileUi, string> = {
  tokyo: "Asia/Tokyo",
  berlin: "Europe/Berlin",
  newyork: "America/New_York",
  pacific: "America/Los_Angeles",
  sydney: "Australia/Sydney",
};

const SCALP_V3_SESSION_START_MINUTE: Record<ScalpEntrySessionProfileUi, number> = {
  tokyo: 9 * 60,
  berlin: 8 * 60,
  newyork: 8 * 60,
  pacific: 10 * 60,
  sydney: 8 * 60,
};

const SCALP_V3_SESSION_DURATION_MINUTES = 4 * 60;
const SCALP_V3_TIMELINE_LOOKAHEAD_DAYS = 14;
const SCALP_V3_WEEKDAY_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

function normalizeNumberArrayUi(value: unknown): number[] | null {
  if (!Array.isArray(value)) return null;
  const out = value
    .map((item) => Number(item))
    .filter((item) => Number.isInteger(item));
  return out.length ? Array.from(new Set(out)).sort((a, b) => a - b) : null;
}

function scalpLocalWeekdayFromParts(parts: TimeZoneClockParts): number {
  return new Date(Date.UTC(parts.y, parts.m - 1, parts.d)).getUTCDay();
}

function normalizeScalpV3TemporalFilterUi(
  row: ScalpOpsDeploymentRow,
): Record<string, any> {
  const gate = asPlainObject(row.promotionGate);
  const direct = asPlainObject(gate.v3TemporalFilter);
  const metadata = asPlainObject(gate.metadata);
  const nested = asPlainObject(metadata.v3TemporalFilter);
  return Object.keys(direct).length ? direct : nested;
}

function scalpBrokerSeatUi(row: ScalpOpsDeploymentRow): Record<string, any> {
  const gate = asPlainObject(row.promotionGate);
  const direct = asPlainObject(gate.brokerSeat);
  if (Object.keys(direct).length) return direct;
  return asPlainObject(asPlainObject(gate.metadata).brokerSeat);
}

function scalpEntryBlockReasonCodesUi(row: ScalpOpsDeploymentRow): string[] {
  const gate = asPlainObject(row.promotionGate);
  const raw = Array.isArray(gate.entryBlockReasonCodes)
    ? gate.entryBlockReasonCodes
    : Array.isArray(asPlainObject(gate.metadata).entryBlockReasonCodes)
      ? asPlainObject(gate.metadata).entryBlockReasonCodes
      : [];
  return raw
    .map((code: unknown) => String(code || "").trim())
    .filter(Boolean);
}

function scalpBrokerSeatStatusUi(row: ScalpOpsDeploymentRow): ScalpBrokerSeatUiStatus {
  const brokerSeat = scalpBrokerSeatUi(row);
  const rawStatus = String(brokerSeat.status || "").trim().toLowerCase();
  const reason = String(row.promotionReason || "").trim().toLowerCase();
  const reasonCodes = scalpEntryBlockReasonCodesUi(row);
  const blockedBySeat = reasonCodes.includes("V3_BROKER_SEAT_ENTRY_BLOCKED");
  if (rawStatus === "management_only" || blockedBySeat) return "management_only";
  if (
    rawStatus === "excluded" ||
    rawStatus === "blocked" ||
    reason === "broker_entry_window_overlap_demoted"
  ) {
    return "excluded";
  }
  if (rawStatus === "winner") return "winner";
  if (row.enabled) return "legacy";
  return "disabled";
}

function scalpBrokerTimelineTone(status: ScalpBrokerSeatUiStatus): ScalpBrokerTimelineTone {
  if (status === "winner") return "active";
  if (status === "management_only") return "management";
  if (status === "excluded") return "blocked";
  if (status === "legacy") return "legacy";
  return "disabled";
}

function formatScalpV3TemporalLabel(
  filter: Record<string, any>,
  session: ScalpEntrySessionProfileUi,
): string {
  const slotMinutes = Math.max(
    5,
    Math.floor(Number(filter.sessionSlotMinutes || 30)) || 30,
  );
  const sessionStartMinute = SCALP_V3_SESSION_START_MINUTE[session] ?? 8 * 60;
  const parts: string[] = [];
  const slots = normalizeNumberArrayUi(filter.allowedSessionWindowSlots);
  const weekdays = normalizeNumberArrayUi(filter.allowedWeekdaysLocal);
  const utcHours = normalizeNumberArrayUi(filter.allowedUtcHours);
  if (slots?.length) {
    parts.push(
      slots
        .slice(0, 3)
        .map((slot) => {
          const start = sessionStartMinute + slot * slotMinutes;
          const end = start + slotMinutes;
          return `${formatTimelineMinuteLabel(start)}-${formatTimelineMinuteLabel(end)} local`;
        })
        .join(", "),
    );
  }
  if (weekdays?.length) {
    parts.push(
      weekdays
        .slice(0, 4)
        .map((day) => SCALP_V3_WEEKDAY_LABELS[((day % 7) + 7) % 7])
        .join("/"),
    );
  }
  if (utcHours?.length) {
    parts.push(
      utcHours
        .slice(0, 4)
        .map((hour) => `${String(((hour % 24) + 24) % 24).padStart(2, "0")}Z`)
        .join("/"),
    );
  }
  return parts.length ? parts.join(" · ") : "full session";
}

function collectScalpV3EntryMinutesForBerlinUi(params: {
  session: ScalpEntrySessionProfileUi;
  filter: Record<string, any>;
}): { minutes: number[]; stepMinutes: number } {
  const timeZone = SCALP_V3_SESSION_TIME_ZONE[params.session] || BERLIN_TZ;
  const sessionStartMinute =
    SCALP_V3_SESSION_START_MINUTE[params.session] ?? 8 * 60;
  const filter = params.filter;
  const slotMinutes = Math.max(
    5,
    Math.floor(Number(filter.sessionSlotMinutes || 30)) || 30,
  );
  const stepMinutes = Math.max(5, Math.min(15, slotMinutes));
  const allowedSlots = normalizeNumberArrayUi(filter.allowedSessionWindowSlots);
  const allowedWeekdays = normalizeNumberArrayUi(filter.allowedWeekdaysLocal);
  const allowedUtcHours = normalizeNumberArrayUi(filter.allowedUtcHours);
  const startUtcMs = Date.UTC(
    new Date().getUTCFullYear(),
    new Date().getUTCMonth(),
    new Date().getUTCDate(),
    0,
    0,
    0,
    0,
  );
  const minutes = new Set<number>();
  for (let day = 0; day < SCALP_V3_TIMELINE_LOOKAHEAD_DAYS; day += 1) {
    for (let minute = 0; minute < 1440; minute += stepMinutes) {
      const tsMs = startUtcMs + (day * 1440 + minute) * 60_000;
      const local = readClockPartsInTimeZone(tsMs, timeZone);
      const localMinuteOfDay = local.hh * 60 + local.mm;
      const minuteOffset = localMinuteOfDay - sessionStartMinute;
      if (
        minuteOffset < 0 ||
        minuteOffset >= SCALP_V3_SESSION_DURATION_MINUTES
      ) {
        continue;
      }
      if (allowedWeekdays?.length) {
        const weekday = scalpLocalWeekdayFromParts(local);
        if (!allowedWeekdays.includes(weekday)) continue;
      }
      if (allowedUtcHours?.length) {
        const utcHour = Math.floor(minute / 60);
        if (!allowedUtcHours.includes(utcHour)) continue;
      }
      if (allowedSlots?.length) {
        const slotIndex = Math.floor(minuteOffset / slotMinutes);
        if (!allowedSlots.includes(slotIndex)) continue;
      }
      const berlin = readClockPartsInTimeZone(tsMs, BERLIN_TZ);
      minutes.add(berlin.hh * 60 + berlin.mm);
    }
  }
  return { minutes: Array.from(minutes).sort((a, b) => a - b), stepMinutes };
}

function groupScalpTimelineMinutes(
  minutes: number[],
  stepMinutes: number,
): Array<{ startMinute: number; endMinute: number }> {
  if (!minutes.length) return [];
  const groups: Array<{ startMinute: number; endMinute: number }> = [];
  let startMinute = minutes[0];
  let previousMinute = minutes[0];
  for (const minute of minutes.slice(1)) {
    if (minute <= previousMinute + stepMinutes) {
      previousMinute = minute;
      continue;
    }
    groups.push({
      startMinute,
      endMinute: Math.min(1440, previousMinute + stepMinutes),
    });
    startMinute = minute;
    previousMinute = minute;
  }
  groups.push({
    startMinute,
    endMinute: Math.min(1440, previousMinute + stepMinutes),
  });
  return groups.filter((group) => group.endMinute > group.startMinute);
}

function assignScalpTimelineLanes(
  blocks: Omit<ScalpBrokerTimelineBlock, "lane">[],
): ScalpBrokerTimelineBlock[] {
  const laneEnds: number[] = [];
  return blocks
    .slice()
    .sort((a, b) => {
      if (a.startMinute !== b.startMinute) return a.startMinute - b.startMinute;
      return b.endMinute - a.endMinute;
    })
    .map((block) => {
      let lane = laneEnds.findIndex((endMinute) => endMinute <= block.startMinute);
      if (lane < 0) {
        lane = laneEnds.length;
        laneEnds.push(block.endMinute);
      } else {
        laneEnds[lane] = block.endMinute;
      }
      return { ...block, lane };
    });
}

function buildScalpBrokerTimeline(
  deployments: ScalpOpsDeploymentRow[],
): ScalpBrokerTimeline | null {
  const rawBlocks: Array<Omit<ScalpBrokerTimelineBlock, "lane">> = [];
  for (const row of deployments) {
    const session =
      normalizeScalpEntrySessionProfileUi(row.entrySessionProfile) ||
      extractScalpEntrySessionProfileFromDeploymentId(row.deploymentId);
    if (!session) continue;
    const filter = normalizeScalpV3TemporalFilterUi(row);
    const brokerSeat = scalpBrokerSeatUi(row);
    const hasV3Metadata =
      Object.keys(filter).length > 0 || Object.keys(brokerSeat).length > 0;
    if (!row.enabled && !row.promotionEligible && !hasV3Metadata) continue;
    const venue = resolveScalpVenueUiFromDeploymentId(row.deploymentId);
    const status = scalpBrokerSeatStatusUi(row);
    const tone = scalpBrokerTimelineTone(status);
    const reasonCodes = scalpEntryBlockReasonCodesUi(row);
    const { minutes, stepMinutes } = collectScalpV3EntryMinutesForBerlinUi({
      session,
      filter,
    });
    const groups = groupScalpTimelineMinutes(minutes, stepMinutes);
    const temporalLabel = formatScalpV3TemporalLabel(filter, session);
    const blocks = groups.map((group, idx) => {
      const label = `${row.symbol} · ${session}`;
      const statusLabel =
        status === "management_only"
          ? "management only"
          : status === "winner"
            ? "entry winner"
            : status === "excluded"
              ? "entry blocked"
              : status;
      const detailParts = [
        row.deploymentId,
        statusLabel,
        temporalLabel,
        reasonCodes.length ? reasonCodes.join(", ") : null,
      ].filter(Boolean);
      return {
        id: `${row.deploymentId}:${idx}:${group.startMinute}`,
        venue,
        symbol: row.symbol,
        session,
        deploymentId: row.deploymentId,
        label,
        detail: detailParts.join(" · "),
        temporalLabel,
        status,
        tone,
        startMinute: group.startMinute,
        endMinute: group.endMinute,
        leftPct: (group.startMinute / 1440) * 100,
        widthPct: Math.max(1.2, ((group.endMinute - group.startMinute) / 1440) * 100),
      };
    });
    rawBlocks.push(...blocks);
  }
  const blocks = assignScalpTimelineLanes(rawBlocks);
  if (!blocks.length) return null;
  const laneCount = blocks.reduce(
    (maxLane, block) => Math.max(maxLane, block.lane + 1),
    0,
  );
  return {
    blocks,
    laneCount,
    activeCount: blocks.filter((block) => block.tone === "active").length,
    managementCount: blocks.filter((block) => block.tone === "management").length,
    blockedCount: blocks.filter((block) => block.tone === "blocked").length,
  };
}

function toDayKeyFromMs(tsMs: number): string {
  return new Date(tsMs).toISOString().slice(0, 10);
}

function toUiScalpSummaryFromV2(
  payloadRaw: unknown,
  opts: { range: DashboardRangeKey; session: ScalpEntrySessionFilterUi },
): ScalpSummaryResponse {
  const payload = asPlainObject(payloadRaw);
  const runtime = asPlainObject(payload.runtime);
  const summary = asPlainObject(payload.summary);
  const deploymentsRaw = Array.isArray(payload.deployments) ? payload.deployments : [];
  const eventsRaw = Array.isArray(payload.events) ? payload.events : [];
  const sessionsRaw = Array.isArray(payload.sessions) ? payload.sessions : [];
  const journalRaw = Array.isArray(payload.journal) ? payload.journal : [];
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

  const latestSessionByDeploymentId = new Map<
    string,
    {
      state: Record<string, any>;
      lastReasonCodes: string[];
      updatedAtMs: number | null;
      dayKey: string | null;
    }
  >();
  for (const rowRaw of sessionsRaw) {
    const row = asPlainObject(rowRaw);
    const deploymentId = String(row.deploymentId || "").trim();
    if (!deploymentId || latestSessionByDeploymentId.has(deploymentId)) continue;
    const state = asPlainObject(row.state);
    const lastReasonCodes = Array.isArray(row.lastReasonCodes)
      ? row.lastReasonCodes
          .map((code: unknown) => String(code || "").trim())
          .filter((code: string) => code.length > 0)
      : [];
    latestSessionByDeploymentId.set(deploymentId, {
      state,
      lastReasonCodes,
      updatedAtMs: asFiniteOrNull(row.updatedAtMs),
      dayKey: String(row.dayKey || "").trim() || null,
    });
  }

  const latestByDeployment = new Map<string, Record<string, any>>();
  const latestExecutionByDeploymentId: Record<string, Record<string, any>> = {};
  const latestExecutionBySymbol: Record<string, Record<string, any>> = {};
  const journalFromEvents: NonNullable<ScalpSummaryResponse["journal"]> = [];
  const journalFromPayload: NonNullable<ScalpSummaryResponse["journal"]> =
    journalRaw
      .map((rowRaw, index) => {
        const row = asPlainObject(rowRaw);
        const tsMs =
          asFiniteOrNull(row.tsMs) ??
          asFiniteOrNull(row.timestampMs) ??
          null;
        if (tsMs === null) return null;
        const type =
          String(row.type || row.eventType || "")
            .trim()
            .toLowerCase() || "event";
        const levelRaw = String(row.level || "")
          .trim()
          .toLowerCase();
        const level =
          levelRaw === "warn" || levelRaw === "error" ? levelRaw : "info";
        const reasonCodes = Array.isArray(row.reasonCodes)
          ? row.reasonCodes
              .map((code: unknown) => String(code || "").trim())
              .filter((code: string) => code.length > 0)
          : [];
        const symbol = String(row.symbol || "")
          .trim()
          .toUpperCase();
        return {
          id: String(row.id || `${type}_${tsMs}_${index}`),
          timestampMs: tsMs,
          type,
          level,
          symbol: symbol || null,
          dayKey: String(row.dayKey || "").trim() || null,
          reasonCodes,
          payload: asPlainObject(row.payload),
        } as ScalpJournalRow;
      })
      .filter((row): row is ScalpJournalRow => row !== null);

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
          .map((code: unknown) => String(code || "").trim())
          .filter((code: string) => code.length > 0)
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
    journalFromEvents.push({
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
    const sessionSnapshot = latestSessionByDeploymentId.get(deploymentId) || null;
    const sessionState = asPlainObject(sessionSnapshot?.state);
    const sessionRun = asPlainObject(sessionState.run);
    const sessionStats = asPlainObject(sessionState.stats);
    const sessionTrade = asPlainObject(sessionState.trade);
    const latest = latestByDeployment.get(deploymentId) || {};
    const latestState = asPlainObject(asPlainObject(latest).state);
    const latestTrade = asPlainObject(latestState.trade);
    const sideRaw = String(
      sessionTrade.side || latestTrade.side || sessionState.side || latestState.side || "",
    )
      .trim()
      .toUpperCase();
    const tradeSide: "BUY" | "SELL" | null =
      sideRaw === "BUY" || sideRaw === "SELL" ? sideRaw : null;
    const sessionStateRaw = String(sessionState.state || "")
      .trim()
      .toUpperCase();
    const inTrade =
      sessionStateRaw === "IN_TRADE" ||
      asBoolOrNull(latestState.inTrade) === true ||
      (String(sessionTrade.dealReference || "").trim().length > 0 &&
        tradeSide !== null) ||
      (String(latestTrade.dealReference || "").trim().length > 0 &&
        tradeSide !== null);
    const stats = ledgerByDeployment.get(deploymentId) || null;
    const sessionTrades = asFiniteOrNull(sessionStats.tradesPlaced);
    const sessionWins = asFiniteOrNull(sessionStats.wins);
    const sessionLosses = asFiniteOrNull(sessionStats.losses);
    const sessionNetR = asFiniteOrNull(sessionStats.realizedR);
    const reasonCodesFromSessionRun = Array.isArray(sessionRun.lastReasonCodes)
      ? sessionRun.lastReasonCodes
          .map((code: unknown) => String(code || "").trim())
          .filter((code: string) => code.length > 0)
      : [];
    const reasonCodesFromLatest = Array.isArray(asPlainObject(latest).reasonCodes)
      ? asPlainObject(latest).reasonCodes
          .map((code: unknown) => String(code || "").trim())
          .filter((code: string) => code.length > 0)
      : [];
    const forwardValidationRaw = asPlainObject(
      deployment.forwardValidation || promotionGate.forwardValidation,
    );
    const forwardValidation = Object.keys(forwardValidationRaw).length
      ? (forwardValidationRaw as ScalpForwardValidation)
      : null;
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
      cronPath: "/api/scalp/composer/cron/execute?dryRun=false",
      dayKey,
      state:
        String(sessionState.state || "").trim() ||
        String(latestState.state || "").trim() ||
        String(asPlainObject(latest).eventType || "").trim() ||
        null,
      updatedAtMs:
        sessionSnapshot?.updatedAtMs ??
        asFiniteOrNull(deployment.updatedAtMs),
      lastRunAtMs:
        asFiniteOrNull(sessionRun.lastRunAtMs) ??
        asFiniteOrNull(asPlainObject(latest).timestampMs),
      dryRunLast:
        asBoolOrNull(sessionRun.dryRunLast) ??
        asBoolOrNull(asPlainObject(latest).dryRun),
      tradesPlaced:
        stats?.trades ??
        (sessionTrades !== null ? Math.max(0, Math.floor(sessionTrades)) : 0),
      wins:
        stats?.wins ??
        (sessionWins !== null ? Math.max(0, Math.floor(sessionWins)) : 0),
      losses:
        stats?.losses ??
        (sessionLosses !== null ? Math.max(0, Math.floor(sessionLosses)) : 0),
      inTrade,
      tradeSide,
      dealReference:
        String(sessionTrade.dealReference || "").trim() ||
        String(latestTrade.dealReference || "").trim() ||
        String(asPlainObject(latest).brokerRef || "").trim() ||
        null,
      reasonCodes:
        reasonCodesFromSessionRun.length
          ? reasonCodesFromSessionRun
          : sessionSnapshot?.lastReasonCodes?.length
            ? sessionSnapshot.lastReasonCodes
            : reasonCodesFromLatest,
      netR: stats ? stats.netR : sessionNetR,
      maxDrawdownR: null,
      promotionEligible,
      promotionReason,
      forwardValidation,
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
    const forwardValidationRaw = asPlainObject(
      row.forwardValidation || promotionGate.forwardValidation,
    );
    const forwardValidation = Object.keys(forwardValidationRaw).length
      ? (forwardValidationRaw as ScalpForwardValidation)
      : null;
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
      forwardValidation,
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

  const selectedSessionRaw = String(opts.session || "")
    .trim()
    .toLowerCase();
  const selectedSession = selectedSessionRaw === "all" ? "" : selectedSessionRaw;
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
      const workerVersion = String(worker.version || "").trim();
      const workerWindowToTs = asFiniteOrNull(worker.windowToTs);
      const holdout = asPlainObject(worker.holdout || stageC.v3Holdout);
      const holdoutFromTs = asFiniteOrNull(holdout.fromTs);
      const holdoutToTs = asFiniteOrNull(holdout.toTs);
      const hasHoldoutWindow = holdoutFromTs !== null && holdoutToTs !== null;
      const displaysTrainingWeeks =
        workerVersion.startsWith("v3_") && hasHoldoutWindow;
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

      // Extract stage aggregates for grid columns
      const stageExpR = asFiniteOrNull(primaryStage.expectancyR);
      const stagePF = asFiniteOrNull(primaryStage.profitFactor);
      const stageMDD = asFiniteOrNull(primaryStage.maxDrawdownR);
      const stageMaxWeeklyNetR = asFiniteOrNull(primaryStage.maxWeeklyNetR);
      const stageLargestR = asFiniteOrNull(primaryStage.largestTradeR);
      const stageExitReasons = asPlainObject(primaryStage.exitReasons);
      const stageReason = String(
        worker.reason || primaryStage.reason || (finalPass ? "stage_c_passed" : "worker_stage_c_failed"),
      ).trim();
      const stageWindowKind = displaysTrainingWeeks ? "training" : "window";

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
          expectancyR: stageExpR,
          profitFactor: stagePF,
          maxDrawdownR: stageMDD,
          // Extra stage fields attached for grid builder
          _stageMaxWeeklyNetR: stageMaxWeeklyNetR,
          _stageLargestR: stageLargestR,
          _stageExitReasons: stageExitReasons,
          _stageReason: stageReason,
          _stageWindowKind: stageWindowKind,
          _workerVersion: workerVersion,
          _workerWindowToTs: workerWindowToTs,
          _holdoutFromTs: holdoutFromTs,
          _holdoutToTs: holdoutToTs,
          _holdoutPassed: holdout.passed === true,
          _holdoutReason: String(holdout.reason || "").trim() || null,
          _holdoutTrades: asFiniteOrNull(holdout.trades),
          _holdoutNetR: asFiniteOrNull(holdout.netR),
        } as ScalpSummaryWorkerRow);
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
  const journal =
    journalFromPayload.length > 0 ? journalFromPayload : journalFromEvents;

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
      totalDeployments: asFiniteOrNull(summary.deployments) ?? deploymentsRaw.length,
      totalCandidates: asFiniteOrNull(summary.candidates) ?? candidatesRaw.length,
      candidateStatusCounts: (() => {
        const rawCounts = asPlainObject(summary.candidateStatusCounts);
        const discovered =
          asFiniteOrNull(rawCounts.discovered) ??
          asFiniteOrNull(summary.discoveredCandidates) ??
          0;
        const evaluated =
          asFiniteOrNull(rawCounts.evaluated) ??
          asFiniteOrNull(summary.evaluatedCandidates) ??
          0;
        const promoted =
          asFiniteOrNull(rawCounts.promoted) ??
          asFiniteOrNull(summary.promotedCandidates) ??
          0;
        const rejected =
          asFiniteOrNull(rawCounts.rejected) ??
          asFiniteOrNull(summary.rejectedCandidates) ??
          0;
        return {
          discovered: Math.max(0, Math.floor(discovered)),
          evaluated: Math.max(0, Math.floor(evaluated)),
          promoted: Math.max(0, Math.floor(promoted)),
          rejected: Math.max(0, Math.floor(rejected)),
        };
      })(),
      symbolCoverage: Array.isArray(summary.symbolCoverage)
        ? (summary.symbolCoverage as Array<Record<string, unknown>>).map((r) => ({
            symbol: String(r.symbol || ""),
            candidates: Math.max(0, Math.floor(Number(r.candidates) || 0)),
            deployments: Math.max(0, Math.floor(Number(r.deployments) || 0)),
          }))
        : undefined,
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
    primaryPathname: "/api/scalp/composer/cron/research",
    matchPathnames: ["/api/scalp/composer/cron/research"],
    fallbackInvokePath: "/api/scalp/composer/cron/research?batchSize=100",
  },
  scalp_promote: {
    primaryPathname: "/api/scalp/composer/cron/promote",
    matchPathnames: ["/api/scalp/composer/cron/promote"],
    fallbackInvokePath: "/api/scalp/composer/cron/promote?dryRun=false",
  },
  scalp_execute: {
    primaryPathname: "/api/scalp/composer/cron/execute",
    matchPathnames: ["/api/scalp/composer/cron/execute"],
    fallbackInvokePath: "/api/scalp/composer/cron/execute?dryRun=false",
  },
  scalp_reconcile: {
    primaryPathname: "/api/scalp/composer/cron/reconcile",
    matchPathnames: ["/api/scalp/composer/cron/reconcile"],
    fallbackInvokePath: "/api/scalp/composer/cron/reconcile",
  },
  scalp_cycle: {
    primaryPathname: "/api/scalp/composer/cron/cycle",
    matchPathnames: ["/api/scalp/composer/cron/cycle"],
    fallbackInvokePath:
      "/api/scalp/composer/cron/cycle?dryRun=false",
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
      raw === "pacific" ||
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
    pathname === "/api/scalp/composer/cron/discover";
  const isDryRunOverridableCron =
    rowId === "scalp_discover" ||
    rowId === "scalp_evaluate" ||
    rowId === "scalp_promote" ||
    rowId === "scalp_execute" ||
    rowId === "scalp_cycle" ||
    pathname === "/api/scalp/composer/cron/discover" ||
    pathname === "/api/scalp/composer/cron/evaluate" ||
    pathname === "/api/scalp/composer/cron/promote" ||
    pathname === "/api/scalp/composer/cron/execute" ||
    pathname === "/api/scalp/composer/cron/cycle";
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
  const [cronConfirmOpen, setCronConfirmOpen] = useState(false);
  const [evaluateJobs, setEvaluateJobs] = useState<
    Record<string, EvaluateJobRecord>
  >({});
  const [evaluateSubmittingSymbol, setEvaluateSubmittingSymbol] = useState<
    string | null
  >(null);
  const [dashboardRange, setDashboardRange] = useState<DashboardRangeKey>("1D");
  // Trailing-7-day per-day closed nets for the header week-calendar strip,
  // folded across symbols. Kept in both venue currencies (Bitget USDT ≈ $,
  // Capital €) and converted to one € figure at render, like the old rollup.
  const [swingWeekDaily, setSwingWeekDaily] = useState<Record<
    string,
    { netUsd: number | null; netEur: number | null; trades: number }
  > | null>(null);
  // Chart-only range: superset of DashboardRangeKey ("4H" shows 5m bars but the
  // summary pipeline only warms 1D/7D/30D/6M caches, so 4H maps to 1D for PnL).
  // Desktop defaults to 1D; phones drop to 4H on mount (effect below).
  const [chartRange, setChartRange] = useState<ChartRangeKey>("1D");
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
  // Selected post-mortem tick's full row (report + dossier) — the decision
  // card renders the post-mortem panel instead of a decision body then.
  const [selectedPostmortem, setSelectedPostmortem] =
    useState<PostmortemUi | null>(null);
  const [showPostmortemAnalysis, setShowPostmortemAnalysis] = useState(false);
  const [showPostmortemDossier, setShowPostmortemDossier] = useState(false);
  // Fetched historical decisions, keyed `${symbol}:${ts}` — clicking back and
  // forth on the timeline shouldn't refetch.
  const tickDecisionCacheRef = useRef<Map<string, DashboardDecisionResponse>>(
    new Map(),
  );
  const postmortemCacheRef = useRef<Map<number, PostmortemUi>>(new Map());
  // Latest tick the user asked for; a slower earlier fetch must not overwrite
  // a newer selection.
  const tickSelectionSeqRef = useRef(0);
  const [swingSummaryRange, setSwingSummaryRange] =
    useState<DashboardRangeKey | null>(null);
  const [strategyMode, setStrategyMode] = useState<StrategyMode>("swing");
  const [scalpSession, setScalpSession] =
    useState<ScalpEntrySessionFilterUi>("all");
  const [scalpSummary, setScalpSummary] = useState<ScalpSummaryResponse | null>(
    null,
  );
  const [scalpResearchHealth, setScalpResearchHealth] =
    useState<ScalpResearchHealthResponse | null>(null);
  const [scalpPaginatedCandidates, setScalpPaginatedCandidates] = useState<
    Array<Record<string, unknown>>
  >([]);
  const [scalpCandidatesTotal, setScalpCandidatesTotal] = useState(0);
  const [scalpCandidateTotalsByState, setScalpCandidateTotalsByState] =
    useState<Record<ScalpCandidateGridStateUi, number>>(
      createScalpCandidateTotalsByState,
    );
  const [scalpCandidatesSessionTotal, setScalpCandidatesSessionTotal] = useState(0);
  const scalpCandidatesLoadingRef = useRef(false);
  const scalpCandidatesPendingRequestRef =
    useRef<ScalpCandidatesPageRequest | null>(null);
  const scalpCandidatesSessionRef = useRef<ScalpEntrySessionFilterUi>("all");
  const scalpCandidateTotalsRequestIdRef = useRef(0);
  const scalpCandidatesStateFilterRef = useRef<ScalpCandidateGridStateUi>("evaluated");
  const scalpSummaryRawRef = useRef<Record<string, unknown> | null>(null);
  const swingDashboardRequestIdRef = useRef(0);
  // True once the user clicks a symbol pill — from then on, dashboard reloads
  // keep their selection instead of re-defaulting to the top-ranked pill.
  const userPickedSymbolRef = useRef(false);
  const [scalpActiveDeploymentId, setScalpActiveDeploymentId] = useState<
    string | null
  >(null);
  const [scalpCandidateStateFilter, setScalpCandidateStateFilter] =
    useState<ScalpCandidateGridStateUi>("evaluated");
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
  const [themePreference, setThemePreference] =
    useState<ThemePreference>("dark");
  const [resolvedTheme, setResolvedTheme] = useState<ResolvedTheme>("dark");
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
    setScalpCronNowMs(Date.now());
  }, []);

  useEffect(() => {
    scalpCandidatesSessionRef.current = scalpSession;
  }, [scalpSession]);

  useEffect(() => {
    scalpCandidatesStateFilterRef.current = scalpCandidateStateFilter;
  }, [scalpCandidateStateFilter]);

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

  // On phones, default the chart to the 4H range (desktop keeps 1D; PnL stays
  // on the 1D summary since 4H is chart-only). Runs once on mount so it never
  // overrides a range the user picks afterward.
  useEffect(() => {
    if (typeof window === "undefined") return;
    if (window.matchMedia("(max-width: 640px)").matches) {
      setDashboardRange("1D");
      setChartRange("4H");
    }
  }, []);

  // EURUSD quote for the header rollup's $→€ conversion. Once per session —
  // the figure is approximate by design, so no polling.
  useEffect(() => {
    if (!adminGranted || strategyMode !== "swing" || eurUsdRate !== null) return;
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(
          "/api/swing/dashboard/live-price?symbol=EURUSD&platform=capital",
          { headers: buildAdminHeaders(), cache: "no-store" },
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
  }, [adminGranted, strategyMode, eurUsdRate]);

  // Live price for the active symbol: REST polling every 3s (no venue
  // websockets — Capital would need a Lightstreamer session; the REST quote
  // covers both platforms uniformly). Feeds the chart's live candle and the
  // live open-PnL. Skips ticks while the tab is hidden or a request is still
  // in flight; state resets on symbol switch so a stale quote from the
  // previous symbol never paints the new chart.
  useEffect(() => {
    setLivePriceNow(null);
    setLivePriceTs(null);
    const symbol = strategyMode === "swing" ? symbols[active] || null : null;
    if (!adminGranted || !symbol) return;
    // Captured once — a symbol's platform doesn't change within its lifetime.
    const platform = tabData[symbol]?.lastPlatform ?? null;
    let cancelled = false;
    let inFlight = false;
    const tick = async () => {
      if (cancelled || inFlight || document.hidden) return;
      inFlight = true;
      try {
        const params = new URLSearchParams({ symbol });
        if (platform) params.set("platform", platform);
        const res = await fetch(
          `/api/swing/dashboard/live-price?${params.toString()}`,
          { headers: buildAdminHeaders(), cache: "no-store" },
        );
        if (cancelled) return;
        if (!res.ok) return;
        const json = await res.json();
        const price = Number(json?.price);
        if (!cancelled && Number.isFinite(price) && price > 0) {
          setLivePriceNow(price);
          const ts = Number(json?.ts);
          setLivePriceTs(Number.isFinite(ts) && ts > 0 ? ts : Date.now());
        }
      } catch {
        // transient poll failure — keep the last quote
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
  }, [adminGranted, strategyMode, symbols, active]);

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
    if (tick.kind !== "postmortem") setSelectedPostmortem(null);
    if (isNewest && tick.hasDetails) {
      setSelectedTickTs(null);
      setSelectedTickDecision(null);
      return;
    }
    setSelectedTickTs(tick.ts);
    if (tick.kind === "postmortem" && tick.postmortemId) {
      // Post-mortem tick: the card renders the forensic report, not a
      // decision. Collapsibles reset so each report opens compact.
      setSelectedTickDecision(null);
      setShowPostmortemAnalysis(false);
      setShowPostmortemDossier(false);
      const cached = postmortemCacheRef.current.get(tick.postmortemId);
      if (cached) {
        setSelectedTickLoading(false);
        setSelectedPostmortem(cached);
        return;
      }
      setSelectedTickLoading(true);
      try {
        const res = await fetch(
          `/api/swing/dashboard/postmortem?id=${tick.postmortemId}`,
          { headers: buildAdminHeaders(), cache: "no-store" },
        );
        if (res.status === 401) {
          handleAuthExpired(
            "Admin session expired. Re-enter ADMIN_ACCESS_SECRET.",
          );
          return;
        }
        if (!res.ok) return;
        const json = await res.json();
        const row = (json?.postmortem ?? null) as PostmortemUi | null;
        if (row) postmortemCacheRef.current.set(tick.postmortemId, row);
        if (tickSelectionSeqRef.current === seq) setSelectedPostmortem(row);
      } catch {
        // tick stays selected; panel shows the unavailable note
      } finally {
        if (tickSelectionSeqRef.current === seq) setSelectedTickLoading(false);
      }
      return;
    }
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

  // background: refresh the data in place without the loading skeleton — used
  // by the warm-status poll when a new analyze cycle's summary lands.
  const loadDashboard = async (opts?: { background?: boolean }) => {
    const requestId = swingDashboardRequestIdRef.current + 1;
    swingDashboardRequestIdRef.current = requestId;
    const requestedRange = dashboardRange;
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

        // Week-calendar strip: fold each symbol's per-day closed nets into one
        // day → {USD, EUR, trades} map. The strip always shows the trailing 7
        // days, so on other ranges fetch the 7D blob too (KV-cached and
        // cron-warmed — no extra fan-out). Non-fatal: a failure just keeps the
        // strip's previous data.
        try {
          let weekRows: DashboardSummaryRow[] = summaryRows;
          if (resolvedSummaryRange !== "7D") {
            const weekRes = await fetch(
              "/api/swing/dashboard/summary?range=7D",
              { headers: buildAdminHeaders(), cache: "no-store" },
            );
            if (!weekRes.ok) {
              throw new Error(`Failed to load 7D summary (${weekRes.status})`);
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
        } catch (weekErr) {
          console.warn("week-calendar summary load failed:", weekErr);
        }
      } catch (summaryErr: any) {
        if (requestId === swingDashboardRequestIdRef.current) {
          setSwingSummaryRange(null);
        }
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

  const loadScalpCandidatesPage = async (
    offset: number,
    session: ScalpEntrySessionFilterUi,
    stateFilter: ScalpCandidateGridStateUi,
    reset?: boolean,
  ) => {
    if (scalpCandidatesLoadingRef.current) {
      scalpCandidatesPendingRequestRef.current = {
        offset,
        session,
        stateFilter,
        reset,
      };
      return;
    }
    scalpCandidatesLoadingRef.current = true;
    scalpCandidatesPendingRequestRef.current = null;
    try {
      const params = new URLSearchParams({
        state: stateFilter,
        offset: String(offset),
        limit: "100",
      });
      if (session !== "all") params.set("session", session);
      const res = await fetch(
        `/api/scalp/composer/dashboard/candidates?${params.toString()}`,
        { headers: buildAdminHeaders(), cache: "no-store" },
      );
      if (!res.ok) return;
      const json = await res.json();
      const rows = Array.isArray(json.rows) ? json.rows : [];
      const total = Math.max(0, Math.floor(Number(json.total) || 0));
      if (
        session !== scalpCandidatesSessionRef.current ||
        stateFilter !== scalpCandidatesStateFilterRef.current
      ) {
        return;
      }
      setScalpCandidatesTotal(total);
      setScalpCandidateTotalsByState((prev) => ({
        ...prev,
        [stateFilter]: total,
      }));
      if (stateFilter === "all") {
        setScalpCandidatesSessionTotal(total);
      }
      setScalpPaginatedCandidates((prev) => (reset ? rows : [...prev, ...rows]));
    } catch {
      // Non-fatal — grid just shows fewer rows
    } finally {
      scalpCandidatesLoadingRef.current = false;
      const pendingRequest = scalpCandidatesPendingRequestRef.current;
      if (pendingRequest) {
        scalpCandidatesPendingRequestRef.current = null;
        const {
          offset: nextOffset,
          session: nextSession,
          stateFilter: nextStateFilter,
          reset: nextReset,
        } = pendingRequest;
        void loadScalpCandidatesPage(
          nextOffset,
          nextSession,
          nextStateFilter,
          nextReset,
        );
      }
    }
  };

  const loadScalpCandidateTotalsByState = async (
    session: ScalpEntrySessionFilterUi,
  ) => {
    const requestId = scalpCandidateTotalsRequestIdRef.current + 1;
    scalpCandidateTotalsRequestIdRef.current = requestId;
    const updates: Partial<Record<ScalpCandidateGridStateUi, number>> = {};
    try {
      await Promise.all(
        SCALP_CANDIDATE_GRID_STATES.map(async (state) => {
          try {
            const params = new URLSearchParams({
              state,
              offset: "0",
              limit: "1",
            });
            if (session !== "all") params.set("session", session);
            const res = await fetch(
              `/api/scalp/composer/dashboard/candidates?${params.toString()}`,
              { headers: buildAdminHeaders(), cache: "no-store" },
            );
            if (!res.ok) return;
            const json = await res.json();
            const total = Math.max(0, Math.floor(Number(json.total) || 0));
            updates[state] = total;
          } catch {
            // Non-fatal — leave previous count for this state.
          }
        }),
      );
      if (session !== scalpCandidatesSessionRef.current) return;
      if (requestId !== scalpCandidateTotalsRequestIdRef.current) return;
      const updateEntries = Object.entries(updates);
      if (!updateEntries.length) return;
      setScalpCandidateTotalsByState((prev) => ({
        ...prev,
        ...updates,
      }));
      const allTotal = updates.all;
      if (typeof allTotal === "number") {
        setScalpCandidatesSessionTotal(allTotal);
      }
    } catch {
      // Non-fatal — denominator can fall back to the filtered total.
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
        eventLimit: "240",
        ledgerLimit: "300",
        deploymentLimit: "500",
        runtimeDeploymentLimit: "80",
        compactDeployments: "true",
        jobLimit: "20",
      });
      if (scalpSession !== "all") params.set("session", scalpSession);
      if (!silent || force) {
        params.set("fresh", "true");
      }
      const summaryRes = await fetch(
        `/api/scalp/composer/dashboard/summary?${params.toString()}`,
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
      scalpSummaryRawRef.current = summaryRaw;
      const summaryJson: ScalpSummaryResponse = toUiScalpSummaryFromV2(
        summaryRaw,
        {
          range: dashboardRange,
          session: scalpSession,
        },
      );
      setScalpSummary(summaryJson);
      try {
        const researchHealthRes = await fetch(
          "/api/scalp/composer/ops/research-health",
          {
            headers: buildAdminHeaders(),
            cache: "no-store",
          },
        );
        if (researchHealthRes.status === 401) {
          handleAuthExpired(
            "Admin session expired. Re-enter ADMIN_ACCESS_SECRET.",
          );
          throw new Error("Unauthorized");
        }
        if (researchHealthRes.ok) {
          const researchHealthRaw = await researchHealthRes.json();
          setScalpResearchHealth(
            researchHealthRaw as ScalpResearchHealthResponse,
          );
        } else {
          setScalpResearchHealth(null);
        }
      } catch {
        setScalpResearchHealth(null);
      }
      scalpSummaryFetchedAtMsRef.current = nowMs;
      scalpSummaryErrorCountRef.current = 0;
      setError(null);
      // Load first page of candidates
      setScalpPaginatedCandidates([]);
      setScalpCandidatesTotal(0);
      setScalpCandidateTotalsByState(createScalpCandidateTotalsByState());
      setScalpCandidatesSessionTotal(0);
      loadScalpCandidatesPage(0, scalpSession, scalpCandidateStateFilter, true);
      loadScalpCandidateTotalsByState(scalpSession);
    } catch (err: any) {
      scalpSummaryErrorCountRef.current += 1;
      setError(err?.message || "Failed to load scalp dashboard");
    } finally {
      if (!silent) setLoading(false);
    }
  };

  // Re-map summary whenever paginated candidates arrive
  useEffect(() => {
    const raw = scalpSummaryRawRef.current;
    if (!raw || !scalpPaginatedCandidates.length) return;
    const patched = { ...raw, candidates: scalpPaginatedCandidates };
    const remapped = toUiScalpSummaryFromV2(patched, {
      range: dashboardRange,
      session: scalpSession,
    });
    setScalpSummary(remapped);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scalpPaginatedCandidates]);

  useEffect(() => {
    if (strategyMode !== "scalp") return;
    if (!adminGranted) return;
    if (!scalpSummaryRawRef.current) return;
    setScalpPaginatedCandidates([]);
    setScalpCandidatesTotal(0);
    loadScalpCandidatesPage(0, scalpSession, scalpCandidateStateFilter, true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scalpCandidateStateFilter]);

  const invokeScalpCronNow = async (row: ScalpOpsCronRow) => {
    const invokePath = normalizeInvokePathForScalpCronNow(
      row.id,
      String(row.invokePath || "").trim(),
      scalpSession === "all" ? "berlin" : scalpSession,
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
      const res = await fetch("/api/scalp/composer/control", {
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
      stored === "all" ||
      stored === "berlin" ||
      stored === "tokyo" ||
      stored === "newyork" ||
      stored === "pacific" ||
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
    // Switching symbols returns the decision card to its live "latest" view.
    setSelectedTickTs(null);
    setSelectedTickDecision(null);
    setSelectedPostmortem(null);
    let cancelled = false;
    (async () => {
      try {
        await Promise.all([
          loadSymbolDecision(symbol, platform),
          loadSymbolEvaluation(symbol),
          // Timeline is decoration around the decision card — a failure there
          // must not blank the whole card.
          loadSymbolTimeline(symbol, platform).catch(() => undefined),
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

  // Cycle-aware refresh: /api/analyze's warm latch stamps swing:warm:last the
  // moment the LAST analyze cron of a 15-minute cycle finished rebuilding the
  // summary blobs (the fallback warm stamps it too). Polling that tiny status
  // endpoint (one KV read) tells an open dashboard exactly when new decisions
  // and timeline ticks are queryable — no full-summary polling on a timer.
  // The refresher lives in a ref so the poll effect keeps stable deps while
  // still seeing the current range/symbol/platform.
  const swingWarmSeenMsRef = useRef<number | null>(null);
  const swingWarmRefreshRef = useRef<() => void>(() => {});
  swingWarmRefreshRef.current = () => {
    void loadDashboard({ background: true });
    const symbol = symbols[active] || null;
    if (!symbol) return;
    const platform = tabData[symbol]?.lastPlatform ?? null;
    // Decision card + timeline dots for the visible symbol; both merge into
    // existing state, so a user inspecting an older tick isn't disturbed.
    void loadSymbolDecision(symbol, platform).catch(() => undefined);
    void loadSymbolTimeline(symbol, platform).catch(() => undefined);
  };

  useEffect(() => {
    if (!adminGranted || strategyMode !== "swing") return;
    if (typeof window === "undefined") return;
    let cancelled = false;
    let inFlight = false;
    const tick = async () => {
      if (cancelled || inFlight || document.hidden) return;
      inFlight = true;
      try {
        const res = await fetch("/api/swing/dashboard/warm-status", {
          headers: buildAdminHeaders(),
          cache: "no-store",
        });
        if (!res.ok || cancelled) return;
        const json = await res.json();
        const warmedAtMs = Number(json?.warmedAtMs);
        if (!Number.isFinite(warmedAtMs) || warmedAtMs <= 0) return;
        const seen = swingWarmSeenMsRef.current;
        swingWarmSeenMsRef.current = warmedAtMs;
        // The first sample is only a baseline — the page just loaded fresh
        // data anyway. Refresh when a NEWER warm lands after that.
        if (seen !== null && warmedAtMs > seen && !cancelled) {
          swingWarmRefreshRef.current();
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
  }, [adminGranted, strategyMode]);

  useEffect(() => {
    return () => {
      Object.keys(evaluatePollTimersRef.current).forEach((symbol) => {
        clearEvaluatePollTimer(symbol);
      });
    };
  }, []);

  useEffect(() => {
    setShowPrompt(false);
  }, [active, symbols]);

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
  const formatCooldownSuffix = (decision: any): string => {
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
  // pullback-limit entry shows its resting price, e.g. "BUY @ 6.34" (a market
  // entry stays bare "BUY"/"SELL"); a flat HOLD that armed a cooldown shows the
  // quiet period + wake bands, e.g. "HOLD + CD 2h (↑51,200 ↓49,700)".
  const formatLastDecisionAction = (decision: any): string => {
    const action = String(decision?.action || "");
    if (action === "BUY" || action === "SELL") {
      const limit = Number(decision?.entry_limit_price);
      if (Number.isFinite(limit) && limit > 0) {
        return `${action} @ ${formatDecisionPrice(limit)}`;
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

  const current = symbols[active] ? tabData[symbols[active]] : null;
  const activeSymbol = symbols[active] || null;
  const activePlatform =
    current?.lastPlatform?.toLowerCase() === "capital" ? "capital" : "bitget";
  const activePlatformLogo =
    activePlatform === "capital" ? "/capital.svg" : "/bitget.svg";
  const dashboardRangeText =
    dashboardRange === "6M" ? "6m" : dashboardRange.toLowerCase();
  const swingSummaryMatchesRange = swingSummaryRange === dashboardRange;
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
  // Header week-calendar strip: the trailing 7 Berlin days (oldest → today),
  // each cell carrying that day's all-symbols closed net in €. The USDT net is
  // folded in at the live EURUSD rate (fallback approximation when the quote
  // hasn't loaded) — cells with a conversion are marked ≈ in their tooltip.
  const swingWeekCalendar = (() => {
    if (strategyMode !== "swing" || !swingWeekDaily) return null;
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
    }> = [];
    for (let daysBack = 6; daysBack >= 0; daysBack--) {
      const date = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000);
      const key = BERLIN_DAY_KEY_FORMAT.format(date);
      const slot = swingWeekDaily[key];
      const hasNet =
        typeof slot?.netUsd === "number" || typeof slot?.netEur === "number";
      const month = BERLIN_MONTH_FORMAT.format(date);
      cells.push({
        key,
        dayNum: BERLIN_DAY_NUM_FORMAT.format(date),
        weekday: BERLIN_WEEKDAY_FORMAT.format(date),
        month,
        // Month label only where it changes along the row (and on the first
        // cell) — keeps the strip narrow.
        showMonth: cells.length === 0 || cells[cells.length - 1].month !== month,
        isToday: daysBack === 0,
        net: hasNet
          ? (slot?.netEur ?? 0) +
            (typeof slot?.netUsd === "number" ? slot.netUsd / rate : 0)
          : null,
        approximate: typeof slot?.netUsd === "number" && slot.netUsd !== 0,
        trades: slot?.trades ?? 0,
      });
    }
    return cells;
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
  }, [active, strategyMode]);
  // Week-calendar strip: days run oldest → today, so on narrow screens pin the
  // scroll to the right edge — today stays in view, the past scrolls away.
  const weekCalendarRef = useRef<HTMLDivElement | null>(null);
  const weekCalendarHasData = Boolean(swingWeekCalendar);
  useEffect(() => {
    const row = weekCalendarRef.current;
    if (!row) return;
    row.scrollLeft = row.scrollWidth;
  }, [weekCalendarHasData, strategyMode]);
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
    strategyMode === "swing" &&
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
  // Decision timeline for the active symbol + what the card body shows: the
  // live latest decision by default, or the selected tick — a fetched decision
  // row for `hasDetails` ticks, the tick's own gate stage/reason for
  // quarter-tick scans (those were never persisted as decision rows).
  const activeTimeline = activeSymbol
    ? symbolTimelines[activeSymbol] ?? []
    : [];
  const selectedTick =
    selectedTickTs !== null
      ? activeTimeline.find((tick) => tick.ts === selectedTickTs) ?? null
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
    const d = displayDecision as any;
    if (!d || String(d.action || "").toUpperCase() !== "CLOSE") return false;
    const pct = Number(
      d.exit_size_pct ?? d.close_size_pct ?? d.partial_close_pct,
    );
    return Number.isFinite(pct) && pct > 0 && pct < 100;
  })();
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
    // Nano (15m) wave/entry-timing bias — measured only on real AI calls, so
    // skip decisions render it as "—" like any missing bias.
    { key: "nano_bias", label: "Nano" },
  ] as const;
  const isInitialLoading = loading && !symbols.length;
  // Swing mode replaces loading text with per-section skeletons; only the
  // scalp view still announces its load in the header.
  const loadingLabel = "Loading scalp dashboard...";
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
  const scalpBrokerTimeline = useMemo(
    () => buildScalpBrokerTimeline(scalpRegistryDeployments),
    [scalpRegistryDeployments],
  );

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
        _stageMaxWeeklyNetR: asFiniteNumber((row as any)?._stageMaxWeeklyNetR),
        _stageLargestR: asFiniteNumber((row as any)?._stageLargestR),
        _stageExitReasons: (row as any)?._stageExitReasons ?? null,
        _stageReason: (row as any)?._stageReason ?? null,
        _stageWindowKind: (row as any)?._stageWindowKind ?? null,
        _workerVersion: (row as any)?._workerVersion ?? null,
        _workerWindowToTs: asFiniteNumber((row as any)?._workerWindowToTs),
        _holdoutFromTs: asFiniteNumber((row as any)?._holdoutFromTs),
        _holdoutToTs: asFiniteNumber((row as any)?._holdoutToTs),
        _holdoutPassed:
          typeof (row as any)?._holdoutPassed === "boolean"
            ? (row as any)._holdoutPassed
            : null,
        _holdoutReason: (row as any)?._holdoutReason ?? null,
        _holdoutTrades: asFiniteNumber((row as any)?._holdoutTrades),
        _holdoutNetR: asFiniteNumber((row as any)?._holdoutNetR),
      } as ScalpWorkerTask);
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
        const deploymentRow = scalpOpsByCandidateKey.get(candidateSessionKey) || null;
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
          _stageMaxWeeklyNetR: asFiniteNumber(task?._stageMaxWeeklyNetR),
          _stageLargestR: asFiniteNumber(task?._stageLargestR),
          _stageExitReasons: task?._stageExitReasons || null,
          _stageReason: String(task?._stageReason || "").trim() || null,
          _stageWindowKind: String(task?._stageWindowKind || "").trim() || null,
          _workerVersion: String(task?._workerVersion || "").trim() || null,
          _workerWindowToTs: asFiniteNumber(task?._workerWindowToTs),
          _holdoutFromTs: asFiniteNumber(task?._holdoutFromTs),
          _holdoutToTs: asFiniteNumber(task?._holdoutToTs),
          _holdoutPassed:
            typeof task?._holdoutPassed === "boolean" ? task._holdoutPassed : null,
          _holdoutReason: String(task?._holdoutReason || "").trim() || null,
          _holdoutTrades: asFiniteNumber(task?._holdoutTrades),
          _holdoutNetR: asFiniteNumber(task?._holdoutNetR),
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
  const scalpCandidateStatusProgress = useMemo(() => {
    const summaryCountsRaw = asPlainObject(
      scalpSummary?.summary?.candidateStatusCounts,
    );
    const summaryTotal = Math.max(
      0,
      Math.floor(Number(scalpSummary?.summary?.totalCandidates) || 0),
    );
    const summaryDiscovered = Math.max(
      0,
      Math.floor(Number(summaryCountsRaw.discovered) || 0),
    );
    const summaryEvaluated = Math.max(
      0,
      Math.floor(Number(summaryCountsRaw.evaluated) || 0),
    );
    const summaryPromoted = Math.max(
      0,
      Math.floor(Number(summaryCountsRaw.promoted) || 0),
    );
    const summaryRejected = Math.max(
      0,
      Math.floor(Number(summaryCountsRaw.rejected) || 0),
    );
    const hasSummaryBreakdown =
      summaryTotal > 0 &&
      summaryDiscovered + summaryEvaluated + summaryPromoted + summaryRejected >
        0;
    if (hasSummaryBreakdown) {
      const done = summaryEvaluated + summaryPromoted + summaryRejected;
      return {
        total: summaryTotal,
        discovered: summaryDiscovered,
        evaluated: summaryEvaluated,
        promoted: summaryPromoted,
        rejected: summaryRejected,
        done,
        donePct: summaryTotal > 0 ? Math.min(100, (done / summaryTotal) * 100) : 0,
      };
    }

    const totals = {
      discovered: 0,
      evaluated: 0,
      promoted: 0,
      rejected: 0,
    };
    for (const raw of scalpPaginatedCandidates) {
      const row = asPlainObject(raw);
      const status = normalizeScalpCandidateStatusUi(row.status);
      totals[status] += 1;
    }
    const total = Math.max(
      summaryTotal,
      totals.discovered + totals.evaluated + totals.promoted + totals.rejected,
    );
    const done = totals.evaluated + totals.promoted + totals.rejected;
    return {
      total,
      discovered: totals.discovered,
      evaluated: totals.evaluated,
      promoted: totals.promoted,
      rejected: totals.rejected,
      done,
      donePct: total > 0 ? Math.min(100, (done / total) * 100) : 0,
    };
  }, [
    scalpSummary?.summary?.candidateStatusCounts,
    scalpSummary?.summary?.totalCandidates,
    scalpPaginatedCandidates,
  ]);

  // --- Research progress bar data ---
  const scalpResearchProgress = (() => {
    const researchJob = scalpPipelineJobs.find(
      (j) => String(j?.jobKind || "").trim().toLowerCase() === "research",
    );
    const p = researchJob?.progress || {};
    const hb = asPlainObject((p as any)?.progress);
    const isRunning = researchJob?.status === "running";
    const phase = String((p as any)?.phase || "").replace(/_/g, " ").trim() || null;

    // Collect numbers from whichever source has them (heartbeat fields, result fields, or health API)
    const healthJob = scalpResearchHealth?.job || null;
    const healthProg = healthJob?.progress || {};
    const n = (a: unknown, b: unknown, c: unknown) => {
      const v = Number(a) || Number(b) || Number(c) || 0;
      return Math.max(0, Math.floor(v));
    };

    // Primary progress model: processed / discovered_total.
    const discoveredTotal = n(hb.discoveredTotal, p.discoveredTotal, null);
    // Fallback legacy model: weeklyTotal / weeklyEvaluated.
    const weeklyTotal = n(hb.weeklyTotal, p.weeklyTotal, null);
    const weeklyEvaluated = n(hb.weeklyEvaluated, p.weeklyEvaluated, null);
    const stageCPass = n(hb.stageCPass, healthProg.stageCPass, p.stageCPass);
    const symbolsThisRun = n(p.symbolsThisRun, null, null);
    const symbolsTotal = n(p.symbolsTotal, null, null);
    const processedSoFar = n(
      hb.processedSoFar,
      (p as any)?.processedSoFar,
      (healthProg as any)?.processedSoFar,
    );

    const total = discoveredTotal > 0 ? discoveredTotal : weeklyTotal;
    const doneConfirmed =
      discoveredTotal > 0
        ? Math.min(total, processedSoFar)
        : weeklyEvaluated;
    const doneLive = discoveredTotal > 0
      ? doneConfirmed
      : total > 0
          ? Math.min(
              total,
              Math.max(
                doneConfirmed,
                doneConfirmed + (isRunning ? processedSoFar : 0),
              ),
            )
          : doneConfirmed;
    const pct = total > 0 ? Math.min(100, (doneLive / total) * 100) : 0;

    let statusLabel: string | null = null;
    if (symbolsTotal > 0) {
      const symsDone = symbolsTotal - (n(null, null, (p as any)?.discoveredSymbols) || symbolsTotal);
      statusLabel = symsDone > 0 ? `${symsDone}/${symbolsTotal} symbols` : `${symbolsTotal} symbols`;
    }

    return {
      totalCandidates: total,
      done: doneLive,
      doneConfirmed,
      processedSoFar,
      pct,
      phase,
      statusLabel,
      stageCPass,
      isRunning,
    };
  })();
  const scalpResearchHealthHint = (() => {
    const hint = scalpResearchHealth?.hint || null;
    const label = String(hint?.label || "").trim();
    if (!label) return null;
    const toneRaw = String(hint?.tone || "info")
      .trim()
      .toLowerCase();
    const tone: "ok" | "warn" | "critical" | "info" =
      toneRaw === "ok" || toneRaw === "warn" || toneRaw === "critical"
        ? toneRaw
        : "info";
    const detail = String(hint?.detail || "").trim() || null;
    const phase = String(scalpResearchHealth?.job?.phase || "").trim() || null;
    return {
      tone,
      label,
      detail,
      phase,
    };
  })();

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
    buildScalpCronRuntimeMap(
      scalpCronNowMs,
      scalpSession === "all" ? "berlin" : scalpSession,
    );
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
    const aNetR = asFiniteNumber(a.totalNetR) ?? Number.NEGATIVE_INFINITY;
    const bNetR = asFiniteNumber(b.totalNetR) ?? Number.NEGATIVE_INFINITY;
    if (bNetR !== aNetR) return bNetR - aNetR;

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
        kind?: "training" | "window";
      }>;
      expectancyWeightedSum: number;
      expectancyWeightedTrades: number;
      expectancySum: number;
      expectancyCount: number;
      profitFactorSum: number;
      profitFactorCount: number;
      maxWeeklyNetR: number | null;
      largestTradeR: number | null;
      exitReasons: ScalpWorkerJobGridRow["exitReasons"];
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
      const key = row.deploymentId || candidateSessionKey;
      const windowLabel =
        row.windowFromTs === null || row.windowToTs === null
          ? "—"
          : `${new Date(row.windowFromTs).toISOString().slice(0, 10)} → ${new Date(
              Math.max(row.windowFromTs, row.windowToTs - 1),
            )
              .toISOString()
              .slice(0, 10)}`;
      const stageWindowKind = String((row as any)._stageWindowKind || "") === "training"
        ? "training"
        : "window";
      const workerWindowToTs = asFiniteNumber((row as any)._workerWindowToTs);
      const holdoutFromTs = asFiniteNumber((row as any)._holdoutFromTs);
      const holdoutToTs = asFiniteNumber((row as any)._holdoutToTs);
      const holdoutPassed =
        typeof (row as any)._holdoutPassed === "boolean"
          ? ((row as any)._holdoutPassed as boolean)
          : null;
      const holdoutNetR = asFiniteNumber((row as any)._holdoutNetR);
      const holdoutTrades = asFiniteNumber((row as any)._holdoutTrades);
      const holdoutReason = String((row as any)._holdoutReason || "").trim();
      const formatInclusiveWindow = (fromTs: number | null, toTs: number | null) =>
        fromTs === null || toTs === null
          ? null
          : `${new Date(fromTs).toISOString().slice(0, 10)} → ${new Date(
              Math.max(fromTs, toTs - 1),
            )
              .toISOString()
              .slice(0, 10)}`;
      const holdoutLabel = formatInclusiveWindow(holdoutFromTs, holdoutToTs);
      const workerThroughLabel =
        workerWindowToTs === null
          ? null
          : new Date(Math.max(0, workerWindowToTs - 1))
              .toISOString()
              .slice(0, 10);
      const netRValue = row.netR;
      const netRDisplay =
        netRValue === null
          ? "—"
          : `${netRValue >= 0 ? "+" : ""}${netRValue.toFixed(2)}R`;
      const tooltipText = [
        `${stageWindowKind === "training" ? "Training week" : "Window"}:${windowLabel}`,
        stageWindowKind === "training" && workerThroughLabel
          ? `Evaluation through:${workerThroughLabel}`
          : null,
        stageWindowKind === "training" && holdoutLabel
          ? `Holdout:${holdoutLabel}`
          : null,
        stageWindowKind === "training" && holdoutPassed !== null
          ? `Holdout pass:${holdoutPassed ? "yes" : "no"}`
          : null,
        stageWindowKind === "training" && holdoutNetR !== null
          ? `Holdout Net:${holdoutNetR >= 0 ? "+" : ""}${holdoutNetR.toFixed(2)}R`
          : null,
        stageWindowKind === "training" && holdoutTrades !== null
          ? `Holdout T:${Math.max(0, Math.floor(holdoutTrades))}`
          : null,
        stageWindowKind === "training" && holdoutReason
          ? `Holdout reason:${holdoutReason}`
          : null,
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
          candidateId: null,
          candidateStatus: "evaluated",
          candidateState:
            row.deploymentEnabled === true ? "enabled" : "evaluated",
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
          maxWeeklyNetR: asFiniteNumber((row as any)._stageMaxWeeklyNetR),
          largestTradeR: asFiniteNumber((row as any)._stageLargestR),
          exitReasons: (() => {
            const er = (row as any)._stageExitReasons;
            if (!er || typeof er !== "object") return null;
            return {
              stop: Number(er.stop || 0),
              stopLoss: Number(er.stopLoss || 0),
              stopBe: Number(er.stopBe || 0),
              stopTrail: Number(er.stopTrail || 0),
              tp: Number(er.tp || 0),
              timeStop: Number(er.timeStop || 0),
              forceClose: Number(er.forceClose || 0),
            };
          })(),
          errorCodes: null,
          statusCounts,
          windows: [
            {
              sortTs: row.windowToTs ?? row.windowFromTs ?? 0,
              netRValue,
              netRDisplay,
              tooltipText,
              kind: stageWindowKind,
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
        kind: stageWindowKind,
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
      // Pick up stage aggregates from first row that has them
      const stageMaxW = asFiniteNumber((row as any)._stageMaxWeeklyNetR);
      if (current.maxWeeklyNetR === null && stageMaxW !== null) current.maxWeeklyNetR = stageMaxW;
      const stageLR = asFiniteNumber((row as any)._stageLargestR);
      if (current.largestTradeR === null && stageLR !== null) current.largestTradeR = stageLR;
      if (current.exitReasons === null) {
        const er = (row as any)._stageExitReasons;
        if (er && typeof er === "object") {
          current.exitReasons = {
            stop: Number(er.stop || 0), stopLoss: Number(er.stopLoss || 0),
            stopBe: Number(er.stopBe || 0), stopTrail: Number(er.stopTrail || 0),
            tp: Number(er.tp || 0), timeStop: Number(er.timeStop || 0),
            forceClose: Number(er.forceClose || 0),
          };
        }
      }
      const sr = (row as any)._stageReason;
      if (sr && typeof sr === "string") current.reason = sr;
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
        candidateId: null,
        candidateStatus: "evaluated",
        candidateState:
          row.deploymentEnabled === true ? "enabled" : "evaluated",
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
          kind: entry.kind,
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
        maxWeeklyNetR: row.maxWeeklyNetR ?? null,
        largestTradeR: row.largestTradeR ?? null,
        exitReasons: row.exitReasons ?? null,
        errorCodes: row.errorCodeSet.size
          ? Array.from(row.errorCodeSet).join(", ")
          : null,
      });
    }
    return out.sort(compareScalpWorkerJobGridRows);
  }, [scalpWorkerTaskRows, scalpSession]);
  const scalpAllCandidatesGridRows = useMemo<ScalpWorkerJobGridRow[]>(() => {
    const workerMetricsByCandidateKey = new Map<string, ScalpWorkerJobGridRow>();
    const workerMetricsByDeploymentId = new Map<string, ScalpWorkerJobGridRow>();
    for (const row of scalpWorkerJobsGridRows) {
      const candidateKey = buildScalpCandidateSessionKey({
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        entrySessionProfile: row.entrySessionProfile || null,
      });
      workerMetricsByCandidateKey.set(candidateKey, row);
      if (row.deploymentId) {
        workerMetricsByDeploymentId.set(row.deploymentId, row);
      }
    }

    const deploymentByCandidateKey = new Map<string, ScalpOpsDeploymentRow>();
    for (const row of scalpRegistryDeployments) {
      deploymentByCandidateKey.set(
        buildScalpCandidateSessionKey({
          symbol: row.symbol,
          strategyId: row.strategyId,
          tuneId: row.tuneId,
          entrySessionProfile: row.entrySessionProfile,
        }),
        row,
      );
    }
    for (const row of scalpOpsDeployments) {
      const key = buildScalpCandidateSessionKey({
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        entrySessionProfile: row.entrySessionProfile,
      });
      if (!deploymentByCandidateKey.has(key)) {
        deploymentByCandidateKey.set(key, row);
      }
    }

    const rows: ScalpWorkerJobGridRow[] = [];
    for (const raw of scalpPaginatedCandidates) {
      const candidate = asPlainObject(raw);
      const symbol = String(candidate.symbol || "")
        .trim()
        .toUpperCase();
      const strategyId = String(candidate.strategyId || "")
        .trim()
        .toLowerCase();
      if (!symbol || !strategyId) continue;
      const tuneId = String(candidate.tuneId || "default")
        .trim()
        .toLowerCase();
      const entrySessionProfile =
        normalizeScalpEntrySessionProfileUi(candidate.entrySessionProfile) ||
        normalizeScalpEntrySessionProfileUi(scalpSession) ||
        null;
      const candidateId = asFiniteNumber(candidate.id);
      const candidateStatus = normalizeScalpCandidateStatusUi(candidate.status);
      const candidateReasonCodes = Array.isArray(candidate.reasonCodes)
        ? candidate.reasonCodes
            .map((code: unknown) => String(code || "").trim())
            .filter((code: string) => code.length > 0)
        : [];
      const candidateKey = buildScalpCandidateSessionKey({
        symbol,
        strategyId,
        tuneId,
        entrySessionProfile,
      });
      const deploymentRow = deploymentByCandidateKey.get(candidateKey) || null;
      const deploymentIdRaw = String(candidate.deploymentId || "").trim();
      const deploymentEnabledRaw =
        typeof candidate.deploymentEnabled === "boolean"
          ? candidate.deploymentEnabled
          : null;
      const workerMetrics =
        workerMetricsByCandidateKey.get(candidateKey) ||
        (deploymentIdRaw
          ? workerMetricsByDeploymentId.get(deploymentIdRaw) || null
          : null);
      const deploymentId =
        deploymentIdRaw ||
        deploymentRow?.deploymentId ||
        workerMetrics?.deploymentId ||
        null;
      const deploymentEnabled =
        deploymentEnabledRaw ??
        (deploymentRow ? deploymentRow.enabled : workerMetrics?.deploymentEnabled ?? null);
      const deployed = Boolean(deploymentId);
      const candidateState: ScalpWorkerJobGridRow["candidateState"] =
        deploymentEnabled === true ? "enabled" : candidateStatus;
      const reason =
        candidateStatus === "rejected"
          ? candidateReasonCodes.join(", ") || workerMetrics?.reason || "rejected"
          : workerMetrics?.reason ||
            candidateReasonCodes[0] ||
            (candidateState === "enabled" ? "enabled" : candidateStatus);

      rows.push({
        rowId:
          candidateId !== null
            ? `candidate:${Math.floor(candidateId)}`
            : `candidate:${candidateKey}`,
        candidateId: candidateId !== null ? Math.floor(candidateId) : null,
        candidateStatus,
        candidateState,
        deploymentId,
        entrySessionProfile:
          entrySessionProfile || workerMetrics?.entrySessionProfile || null,
        workerOnly: workerMetrics?.workerOnly || false,
        symbol,
        strategyId,
        tuneId,
        inUniverse:
          deploymentRow?.inUniverse ??
          workerMetrics?.inUniverse ??
          null,
        lifecycleState:
          deploymentRow?.lifecycleState ??
          workerMetrics?.lifecycleState ??
          null,
        forwardValidation:
          deploymentRow?.forwardValidation ||
          workerMetrics?.forwardValidation ||
          null,
        deployed,
        deploymentEnabled,
        promotionEligible:
          deploymentRow?.promotionEligible ??
          workerMetrics?.promotionEligible ??
          (candidateStatus === "promoted" ? true : null),
        reason,
        status: workerMetrics?.status || candidateStatus,
        windowCount: workerMetrics?.windowCount ?? 0,
        windowsResults: workerMetrics?.windowsResults || "—",
        windowNetRs: Array.isArray(workerMetrics?.windowNetRs)
          ? workerMetrics?.windowNetRs
          : [],
        trades: workerMetrics?.trades ?? null,
        netR: workerMetrics?.netR ?? null,
        totalNetR: workerMetrics?.totalNetR ?? workerMetrics?.netR ?? null,
        expectancyR: workerMetrics?.expectancyR ?? null,
        profitFactor: workerMetrics?.profitFactor ?? null,
        maxDrawdownR: workerMetrics?.maxDrawdownR ?? null,
        totalMaxDrawdownR:
          workerMetrics?.totalMaxDrawdownR ??
          workerMetrics?.maxDrawdownR ??
          null,
        maxWeeklyNetR: workerMetrics?.maxWeeklyNetR ?? null,
        largestTradeR: workerMetrics?.largestTradeR ?? null,
        exitReasons: workerMetrics?.exitReasons ?? null,
        errorCodes: workerMetrics?.errorCodes ?? null,
      });
    }

    return rows.sort(compareScalpWorkerJobGridRows);
  }, [
    scalpWorkerJobsGridRows,
    scalpRegistryDeployments,
    scalpOpsDeployments,
    scalpPaginatedCandidates,
    scalpSession,
  ]);
  const scalpSelectedWorkerGridRows = useMemo<ScalpWorkerJobGridRow[]>(() => {
    if (scalpCandidateStateFilter === "all") return scalpAllCandidatesGridRows;
    return scalpAllCandidatesGridRows.filter(
      (row) => row.candidateState === scalpCandidateStateFilter,
    );
  }, [scalpAllCandidatesGridRows, scalpCandidateStateFilter]);
  useEffect(() => {
    const total = scalpSelectedWorkerGridRows.length;
    setScalpGridLoadedRows(
      total > 0 ? Math.min(SCALP_GRID_LOAD_BATCH, total) : 0,
    );
  }, [scalpCandidateStateFilter, scalpSelectedWorkerGridRows.length]);
  useEffect(() => {
    // If the current filter has no matches in loaded pages yet, keep
    // prefetching candidate pages until we either find matches or exhaust total.
    if (scalpSelectedWorkerGridRows.length > 0) return;
    if (scalpCandidatesLoadingRef.current) return;
    const fetchedCount = scalpPaginatedCandidates.length;
    if (fetchedCount >= scalpCandidatesTotal) return;
    loadScalpCandidatesPage(
      fetchedCount,
      scalpSession,
      scalpCandidateStateFilter,
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    scalpSelectedWorkerGridRows.length,
    scalpPaginatedCandidates.length,
    scalpCandidatesTotal,
    scalpSession,
    scalpCandidateStateFilter,
  ]);
  const scalpVisibleWorkerGridRows = useMemo<ScalpWorkerJobGridRow[]>(() => {
    if (!scalpSelectedWorkerGridRows.length) return [];
    const cappedCount = Math.max(
      0,
      Math.min(scalpGridLoadedRows, scalpSelectedWorkerGridRows.length),
    );
    return scalpSelectedWorkerGridRows.slice(0, cappedCount);
  }, [scalpSelectedWorkerGridRows, scalpGridLoadedRows]);
  const scalpSessionCandidatesTotalForStateChip = useMemo(
    () =>
      scalpCandidatesSessionTotal > 0
        ? scalpCandidatesSessionTotal
        : scalpCandidatesTotal,
    [scalpCandidatesSessionTotal, scalpCandidatesTotal],
  );
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
        headerName: "Candidate",
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
        headerName: "State",
        field: "candidateState",
        minWidth: 140,
        valueFormatter: (params) =>
          String(params.value || "discovered").replace(/_/g, " "),
        cellRenderer: (params: any) => {
          const value = String(params?.value || "discovered")
            .trim()
            .toLowerCase() as ScalpWorkerJobGridRow["candidateState"];
          const className =
            value === "enabled"
              ? scalpDarkMode
                ? "text-emerald-300"
                : "text-emerald-700"
              : value === "promoted"
                ? scalpDarkMode
                  ? "text-emerald-300"
                  : "text-emerald-700"
                : value === "evaluated"
                  ? scalpDarkMode
                    ? "text-amber-300"
                    : "text-amber-700"
                  : value === "rejected"
                    ? scalpDarkMode
                      ? "text-rose-300"
                      : "text-rose-700"
                    : scalpDarkMode
                      ? "text-zinc-300"
                      : "text-slate-700";
          return <span className={className}>{value.replace(/_/g, " ")}</span>;
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
              kind?: "training" | "window";
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
          if (v.stopLoss) parts.push(`SL:${v.stopLoss}`);
          if (v.stopBe) parts.push(`BE:${v.stopBe}`);
          if (v.stopTrail) parts.push(`TR:${v.stopTrail}`);
          if (v.stop && !v.stopLoss && !v.stopBe && !v.stopTrail) parts.push(`SL:${v.stop}`);
          if (v.tp) parts.push(`TP:${v.tp}`);
          if (v.timeStop) parts.push(`TS:${v.timeStop}`);
          if (v.forceClose) parts.push(`FC:${v.forceClose}`);
          return parts.length ? parts.join(" ") : "—";
        },
      },
      {
        headerName: "Errors",
        field: "errorCodes",
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
      const fetchedCount = scalpPaginatedCandidates.length;
      const canFetchMore = fetchedCount < scalpCandidatesTotal;
      if ((!totalRows || loadedRows >= totalRows) && !canFetchMore) return;
      const range = event?.api?.getVerticalPixelRange?.();
      const displayedRows = event?.api?.getDisplayedRowCount?.() || 0;
      if (!range || displayedRows <= 0) return;
      const nearBottom = range.bottom >= displayedRows * 54 - 108;
      if (!nearBottom) return;
      if (loadedRows < totalRows) {
        setScalpGridLoadedRows((prev) =>
          Math.min(totalRows, prev + SCALP_GRID_LOAD_BATCH),
        );
      }
      if (canFetchMore) {
        loadScalpCandidatesPage(
          fetchedCount,
          scalpSession,
          scalpCandidateStateFilter,
        );
      }
    },
    [
      scalpSelectedWorkerGridRows.length,
      scalpVisibleWorkerGridRows.length,
      scalpPaginatedCandidates.length,
      scalpCandidatesTotal,
      scalpSession,
      scalpCandidateStateFilter,
    ],
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
          <div className="rounded-2xl border border-slate-200 bg-white px-4 py-3 shadow-sm">
            <div className="flex items-start justify-between gap-4">
              <div className="min-w-0 flex-1">
                <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                  {strategyMode === "swing" && currentEvalJob ? (
                    <span className="text-[11px] text-slate-500">
                      eval:{" "}
                      <span className="font-semibold text-slate-700">
                        {currentEvalJob.status}
                      </span>
                    </span>
                  ) : null}
                  {strategyMode === "scalp" && loading ? (
                    <span className="text-[11px] text-slate-500">
                      {loadingLabel}
                    </span>
                  ) : null}
                  {strategyMode === "swing" &&
                  !swingWeekCalendar &&
                  loading &&
                  !error ? (
                    // Week-calendar skeleton — covers the initial load and the
                    // gap while a range switch refetches the summary.
                    <span className="skeleton-shimmer flex items-center gap-1">
                      {Array.from({ length: 7 }, (_, i) => (
                        <span
                          key={i}
                          className={`h-[18px] w-10 rounded bg-slate-200 ${
                            i < 2 ? "hidden sm:block" : ""
                          }`}
                        />
                      ))}
                    </span>
                  ) : null}
                  {swingWeekCalendar ? (
                    // Trailing-7-day calendar: per-day all-symbols closed net
                    // in € (USDT folded in at the EURUSD rate — tooltip carries
                    // the ≈ note). One-line height; scrolls horizontally on
                    // narrow screens with today pinned at the right edge.
                    <div
                      ref={weekCalendarRef}
                      className="scrollbar-none flex min-w-0 max-w-full flex-nowrap items-center gap-2 overflow-x-auto"
                      aria-label="Daily net, last 7 days"
                    >
                      {swingWeekCalendar.map((cell, index) => (
                        <div
                          key={cell.key}
                          title={`${cell.key} · ${cell.trades} trade${
                            cell.trades === 1 ? "" : "s"
                          }${
                            cell.approximate
                              ? ` · includes USDT converted at EURUSD ${(eurUsdRate ?? EUR_USD_FALLBACK_RATE).toFixed(4)}`
                              : ""
                          }`}
                          className={`shrink-0 items-center gap-1 rounded px-1 ${
                            index < 2 ? "hidden sm:flex" : "flex"
                          } ${
                            cell.isToday ? "bg-slate-100" : ""
                          }`}
                        >
                          <span
                            className={`text-[15px] font-semibold leading-none tabular-nums ${
                              cell.isToday ? "text-slate-900" : "text-slate-700"
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
                {strategyMode === "swing" &&
                !error &&
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
                        const openPnlValue =
                          openDirection && typeof tab?.openPnl === "number"
                            ? tab.openPnl
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
                                  className={`pill-decision-dot h-2 w-2 shrink-0 ${decisionDotClass} ${
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
              {/* Cron + theme switches: always stacked, one per line. */}
              <div className="flex flex-col items-end gap-2">
              {strategyMode === "swing" ? (
                <div className="relative flex items-center gap-2">
                  <span className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                    {swingCronControlUpdating
                      ? "…"
                      : !swingCronControlLoaded
                        ? "cron"
                        : swingCronHardDeactivated
                          ? "cron off"
                          : "cron on"}
                  </span>
                  <button
                    type="button"
                    role="switch"
                    aria-checked={!swingCronHardDeactivated}
                    onClick={() => setCronConfirmOpen((v) => !v)}
                    disabled={
                      !adminGranted ||
                      swingCronControlUpdating ||
                      !swingCronControlLoaded
                    }
                    title={
                      !swingCronControlLoaded
                        ? "loading"
                        : swingCronHardDeactivated
                          ? "swing cron OFF — click to re-enable"
                          : "swing cron ON — click to disable"
                    }
                    className={`relative inline-flex h-5 w-9 shrink-0 items-center rounded-full border transition disabled:cursor-not-allowed disabled:opacity-60 ${
                      swingCronControlUpdating || !swingCronControlLoaded
                        ? "border-slate-300 bg-slate-200"
                        : swingCronHardDeactivated
                          ? "border-rose-400 bg-rose-500/80"
                          : "neutral-highlight"
                    }`}
                  >
                    <span
                      className={`inline-block h-3.5 w-3.5 rounded-full bg-white transition-transform ${
                        swingCronHardDeactivated
                          ? "translate-x-0.5"
                          : "translate-x-4"
                      }`}
                    />
                  </button>
                  {cronConfirmOpen ? (
                    <div className="absolute right-0 top-full z-50 mt-2 w-64 rounded-xl border border-amber-300 bg-white p-3 text-left shadow-lg">
                      <div className="flex items-start gap-2">
                        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-500" />
                        <div className="text-xs text-slate-700">
                          {swingCronHardDeactivated
                            ? "Re-enable the swing cron? It resumes analyzing and executing swing trades automatically."
                            : "Disable the swing cron? This hard-deactivates automated swing analysis and execution."}
                        </div>
                      </div>
                      <div className="mt-3 flex justify-end gap-2">
                        <button
                          type="button"
                          onClick={() => setCronConfirmOpen(false)}
                          className="rounded-full border border-slate-200 px-3 py-1 text-xs font-semibold text-slate-600 transition hover:bg-slate-50"
                        >
                          Cancel
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            void setSwingCronHardDeactivate(
                              !swingCronHardDeactivated,
                            );
                            setCronConfirmOpen(false);
                          }}
                          className={`rounded-full px-3 py-1 text-xs font-semibold text-white transition ${
                            swingCronHardDeactivated
                              ? "bg-emerald-600 hover:bg-emerald-700"
                              : "bg-rose-600 hover:bg-rose-700"
                          }`}
                        >
                          {swingCronHardDeactivated ? "Enable" : "Disable"}
                        </button>
                      </div>
                    </div>
                  ) : null}
                </div>
              ) : null}
                <div className="flex items-center gap-2">
                  <span className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                    {resolvedTheme === "dark" ? "dark" : "light"}
                  </span>
                  <button
                    type="button"
                    role="switch"
                    aria-checked={resolvedTheme === "dark"}
                    onClick={handleThemeToggle}
                    title={
                      resolvedTheme === "dark"
                        ? "Switch to light mode"
                        : "Switch to dark mode"
                    }
                    className="relative inline-flex h-5 w-9 shrink-0 items-center rounded-full border border-slate-400 bg-slate-200 transition"
                  >
                    <span
                      className={`inline-flex h-3.5 w-3.5 items-center justify-center rounded-full bg-white transition-transform ${
                        resolvedTheme === "dark"
                          ? "translate-x-4"
                          : "translate-x-0.5"
                      }`}
                    >
                      {resolvedTheme === "dark" ? (
                        <Moon className="h-2.5 w-2.5 text-slate-700" />
                      ) : (
                        <Sun className="h-2.5 w-2.5 text-slate-700" />
                      )}
                    </span>
                  </button>
                </div>
              </div>
            </div>
          </div>

          {error && (
            <div className="mt-4 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm font-semibold text-rose-700">
              Could not load dashboard data: {error}
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

                  {scalpBrokerTimeline ? (
                    <section className={`${scalpSectionShellClass} p-4`}>
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div>
                          <h3 className={`text-lg font-semibold ${scalpTextPrimaryClass}`}>
                            Broker Entry Seats
                          </h3>
                        </div>
                        <div className="flex flex-wrap items-center gap-2 text-[11px]">
                          <span className={scalpTagNeutralClass}>
                            Berlin time
                          </span>
                          <span className={scalpTagNeutralClass}>
                            {scalpBrokerTimeline.activeCount} entry blocks
                          </span>
                          {scalpBrokerTimeline.managementCount ? (
                            <span className={scalpTagNeutralClass}>
                              {scalpBrokerTimeline.managementCount} management
                            </span>
                          ) : null}
                          {scalpBrokerTimeline.blockedCount ? (
                            <span className={scalpTagNeutralClass}>
                              {scalpBrokerTimeline.blockedCount} blocked
                            </span>
                          ) : null}
                          {[
                            ["bg-emerald-500", "entry"],
                            ["bg-amber-400", "management"],
                            ["bg-rose-500", "blocked"],
                            ["bg-sky-400", "legacy"],
                          ].map(([dotClass, label]) => (
                            <span
                              key={label}
                              className={`inline-flex items-center gap-1.5 rounded-full border px-2 py-1 ${
                                scalpDarkMode
                                  ? "border-zinc-700 text-zinc-300"
                                  : "border-slate-200 text-slate-600"
                              }`}
                            >
                              <span className={`h-2 w-2 rounded-full ${dotClass}`} />
                              {label}
                            </span>
                          ))}
                        </div>
                      </div>
                      <div
                        className={`relative mt-4 overflow-hidden rounded-lg border ${
                          scalpDarkMode
                            ? "border-zinc-700 bg-zinc-950/60"
                            : "border-slate-200 bg-white"
                        }`}
                        style={{
                          height: `${Math.max(94, 52 + scalpBrokerTimeline.laneCount * 30)}px`,
                        }}
                      >
                        {SCALP_SESSION_TIMELINE_TICK_MINUTES.map((minute) => (
                          <div
                            key={`broker-seat-tick-${minute}`}
                            className={`absolute top-0 h-full border-l ${
                              scalpDarkMode ? "border-zinc-800" : "border-slate-100"
                            }`}
                            style={{
                              left:
                                minute === 1440
                                  ? "calc(100% - 1px)"
                                  : `${(minute / 1440) * 100}%`,
                            }}
                          >
                            <span
                              className={`absolute top-1 translate-x-1 text-[10px] ${scalpTextMutedClass}`}
                            >
                              {formatTimelineMinuteLabel(minute)}
                            </span>
                          </div>
                        ))}
                        <div
                          className={`absolute left-0 right-0 top-7 border-t ${
                            scalpDarkMode ? "border-zinc-800" : "border-slate-100"
                          }`}
                        />
                        {scalpBrokerTimeline.blocks.map((block) => {
                          const blockVenueIconSrc =
                            SCALP_VENUE_ICON_SRC[block.venue];
                          const blockClass = (() => {
                            if (block.tone === "active") {
                              return scalpDarkMode
                                ? "border-emerald-400/60 bg-emerald-500/20 text-emerald-100"
                                : "border-emerald-300 bg-emerald-100 text-emerald-800";
                            }
                            if (block.tone === "management") {
                              return scalpDarkMode
                                ? "border-amber-300/60 bg-amber-400/20 text-amber-100"
                                : "border-amber-300 bg-amber-100 text-amber-800";
                            }
                            if (block.tone === "blocked") {
                              return scalpDarkMode
                                ? "border-rose-400/60 bg-rose-500/20 text-rose-100"
                                : "border-rose-300 bg-rose-100 text-rose-800";
                            }
                            if (block.tone === "legacy") {
                              return scalpDarkMode
                                ? "border-sky-400/60 bg-sky-500/20 text-sky-100"
                                : "border-sky-300 bg-sky-100 text-sky-800";
                            }
                            return scalpDarkMode
                              ? "border-zinc-600 bg-zinc-800 text-zinc-300"
                              : "border-slate-300 bg-slate-100 text-slate-500";
                          })();
                          return (
                            <div
                              key={block.id}
                              title={`${formatTimelineMinuteLabel(block.startMinute)}-${formatTimelineMinuteLabel(block.endMinute)} Berlin · ${block.detail}`}
                              className={`absolute overflow-hidden rounded-md border px-2 py-1 text-[10px] leading-tight shadow-sm ${blockClass}`}
                              style={{
                                left: `${block.leftPct}%`,
                                width: `${block.widthPct}%`,
                                top: `${34 + block.lane * 30}px`,
                              }}
                            >
                              <div className="flex min-w-0 items-center gap-1 font-semibold">
                                <img
                                  src={blockVenueIconSrc}
                                  alt={`${block.venue} venue`}
                                  className="h-3 w-auto shrink-0"
                                />
                                <span className="min-w-0 truncate">
                                  {block.label}
                                </span>
                              </div>
                              <div className="truncate opacity-75">
                                {block.temporalLabel}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    </section>
                  ) : null}

                  <section className={`${scalpSectionShellClass} overflow-hidden`}>
                    {/* Research progress bar — thin strip at top of panel */}
                    {(scalpCandidateStatusProgress.total > 0 || scalpResearchProgress.isRunning || scalpResearchHealthHint) && (
                      <div className="relative">
                        {(() => {
                          const total = Math.max(0, scalpCandidateStatusProgress.total);
                          const promotedPct =
                            total > 0
                              ? Math.min(
                                  100,
                                  (scalpCandidateStatusProgress.promoted / total) *
                                    100,
                                )
                              : 0;
                          const evaluatedPct =
                            total > 0
                              ? Math.min(
                                  100,
                                  (scalpCandidateStatusProgress.evaluated / total) *
                                    100,
                                )
                              : 0;
                          const rejectedPct =
                            total > 0
                              ? Math.min(
                                  100,
                                  (scalpCandidateStatusProgress.rejected / total) *
                                    100,
                                )
                              : 0;
                          return (
                            <div
                              className={`flex h-2 w-full overflow-hidden rounded-full ${scalpDarkMode ? "bg-zinc-700" : "bg-slate-200"}`}
                            >
                              {promotedPct > 0 ? (
                                <div
                                  className="h-full bg-emerald-500 transition-all duration-700 ease-out"
                                  style={{ width: `${promotedPct}%` }}
                                />
                              ) : null}
                              {evaluatedPct > 0 ? (
                                <div
                                  className="h-full bg-amber-400 transition-all duration-700 ease-out"
                                  style={{ width: `${evaluatedPct}%` }}
                                />
                              ) : null}
                              {rejectedPct > 0 ? (
                                <div
                                  className="h-full bg-rose-500 transition-all duration-700 ease-out"
                                  style={{ width: `${rejectedPct}%` }}
                                />
                              ) : null}
                            </div>
                          );
                        })()}
                        {(() => {
                          const total = Math.max(0, scalpCandidateStatusProgress.total);
                          const evaluatedPct =
                            total > 0
                              ? Math.min(
                                  100,
                                  (scalpCandidateStatusProgress.evaluated / total) *
                                    100,
                                )
                              : 0;
                          const rejectedPct =
                            total > 0
                              ? Math.min(
                                  100,
                                  (scalpCandidateStatusProgress.rejected / total) *
                                    100,
                                )
                              : 0;
                          return (
                            <div
                              className={`flex items-center justify-between px-4 pt-1.5 pb-0 text-[11px] ${scalpTextMutedClass}`}
                            >
                              <span className="flex items-center gap-2">
                                <span>
                                  Research{" "}
                                  <span className={scalpTextSecondaryClass}>
                                    {scalpCandidateStatusProgress.done}/
                                    {scalpCandidateStatusProgress.total}
                                  </span>
                                  {" "}
                                  <span className="opacity-60">
                                    ({scalpCandidateStatusProgress.donePct < 1 &&
                                    scalpCandidateStatusProgress.donePct > 0
                                      ? scalpCandidateStatusProgress.donePct.toFixed(1)
                                      : Math.round(scalpCandidateStatusProgress.donePct)}
                                    %)
                                  </span>
                                </span>
                                <span className="text-amber-400">
                                  {scalpCandidateStatusProgress.evaluated} evaluated{" "}
                                  <span className="opacity-60">
                                    ({evaluatedPct < 1 && evaluatedPct > 0
                                      ? evaluatedPct.toFixed(1)
                                      : Math.round(evaluatedPct)}
                                    %)
                                  </span>
                                </span>
                                <span className="text-rose-400">
                                  {scalpCandidateStatusProgress.rejected} rejected{" "}
                                  <span className="opacity-60">
                                    ({rejectedPct < 1 && rejectedPct > 0
                                      ? rejectedPct.toFixed(1)
                                      : Math.round(rejectedPct)}
                                    %)
                                  </span>
                                </span>
                                <span className="text-emerald-400">
                                  {scalpCandidateStatusProgress.promoted} promoted
                                </span>
                                {scalpResearchProgress.isRunning &&
                                  scalpResearchProgress.doneConfirmed <
                                    scalpResearchProgress.done && (
                                    <span className="opacity-60">
                                      confirmed {scalpResearchProgress.doneConfirmed}
                                    </span>
                                  )}
                                {scalpResearchProgress.phase && scalpResearchProgress.isRunning && (
                                  <span className="opacity-50">{scalpResearchProgress.phase}</span>
                                )}
                              </span>
                              <span className="flex items-center gap-2">
                                {scalpResearchProgress.statusLabel && (
                                  <span className="opacity-70">{scalpResearchProgress.statusLabel}</span>
                                )}
                                {scalpResearchHealthHint && (
                                  <span className="relative group">
                                    <span className={`inline-block h-2.5 w-2.5 rounded-full cursor-help ${
                                      scalpResearchHealthHint.tone === "critical" ? "bg-rose-500" :
                                      scalpResearchHealthHint.tone === "warn" ? "bg-amber-400" :
                                      scalpResearchHealthHint.tone === "ok" ? "bg-emerald-400" :
                                      "bg-slate-400"
                                    }`} />
                                    <span className={`pointer-events-none absolute right-0 top-full mt-2 z-50 w-80 rounded-lg border px-3 py-2 text-[10px] leading-relaxed opacity-0 shadow-xl transition-opacity group-hover:opacity-100 ${
                                      scalpDarkMode
                                        ? "border-zinc-600 bg-zinc-800 text-zinc-200"
                                        : "border-slate-200 bg-white text-slate-700"
                                    }`}>
                                      <div className="font-medium">{scalpResearchHealthHint.label}</div>
                                      {scalpResearchHealthHint.detail && (
                                        <div className="mt-0.5 opacity-70">{scalpResearchHealthHint.detail}</div>
                                      )}
                                      {(() => {
                                        const log = (scalpResearchHealth?.job as any)?.log;
                                        if (!Array.isArray(log) || !log.length) return null;
                                        return (
                                          <div className="mt-1.5 border-t border-current/20 pt-1.5 max-h-48 overflow-y-auto font-mono">
                                            {log.slice(-10).map((entry: any, i: number) => (
                                              <div key={i} className="opacity-60">{entry.t}s {entry.p}{entry.d ? ` · ${entry.d}` : ''}</div>
                                            ))}
                                          </div>
                                        );
                                      })()}
                                    </span>
                                  </span>
                                )}
                              </span>
                            </div>
                          );
                        })()}
                      </div>
                    )}
                    {/* Health hint is shown as a dot inside the progress row — see below */}
                    {/* Per-symbol coverage gaps — only show incomplete symbols */}
                    {(() => {
                      const gaps = (scalpSummary?.summary?.symbolCoverage || [])
                        .filter((s) => s.candidates > s.deployments)
                        .sort((a, b) => (a.deployments / a.candidates) - (b.deployments / b.candidates));
                      if (!gaps.length) return null;
                      return (
                        <div className={`flex flex-wrap items-center gap-1.5 px-4 pt-1.5 pb-1 text-[10px] ${scalpTextMutedClass}`}>
                          {gaps.map((s) => (
                            <span
                              key={s.symbol}
                              className={`inline-flex items-center gap-0.5 rounded px-1.5 py-0.5 font-mono ${
                                scalpDarkMode ? "bg-zinc-800" : "bg-slate-100"
                              }`}
                            >
                              <span className={scalpTextSecondaryClass}>{s.symbol}</span>
                              {" "}
                              {s.deployments}/{s.candidates}
                            </span>
                          ))}
                        </div>
                      );
                    })()}
                    <div className="flex items-center justify-between p-4 pt-2">
                      <h3
                        className={`text-lg font-semibold ${scalpTextPrimaryClass}`}
                      >
                        Candidate Coverage
                      </h3>
                      <div className="flex flex-wrap items-center justify-end gap-2">
                        <span className={`text-xs ${scalpTextMutedClass}`}>
                          Session
                        </span>
                        {SCALP_ENTRY_SESSION_FILTER_OPTIONS.map((option) => {
                          const active = scalpSession === option.id;
                          return (
                            <button
                              key={`candidate-session-filter-${option.id}`}
                              type="button"
                              onClick={() => {
                                setScalpSession(option.id);
                                scalpSummaryFetchedAtMsRef.current = 0;
                              }}
                              className={`rounded-full border px-2.5 py-1 text-[11px] font-semibold transition ${
                                active
                                  ? scalpDarkMode
                                    ? "border-emerald-400/70 bg-emerald-500/20 text-emerald-200"
                                    : "border-emerald-300 bg-emerald-100 text-emerald-700"
                                  : scalpDarkMode
                                    ? "border-zinc-600 bg-zinc-800 text-zinc-300 hover:border-zinc-500"
                                    : "border-slate-300 bg-white text-slate-600 hover:border-slate-400"
                              }`}
                            >
                              {option.label}
                            </button>
                          );
                        })}
                        <span className={`text-xs ${scalpTextMutedClass}`}>
                          State
                        </span>
                        {SCALP_CANDIDATE_GRID_STATES.map((state) => {
                          const active = scalpCandidateStateFilter === state;
                          const stateTotal = Math.max(
                            0,
                            Math.floor(Number(scalpCandidateTotalsByState[state]) || 0),
                          );
                          return (
                            <button
                              key={`candidate-filter-${state}`}
                              type="button"
                              onClick={() => setScalpCandidateStateFilter(state)}
                              className={`rounded-full border px-2.5 py-1 text-[11px] font-semibold transition ${
                                active
                                  ? scalpDarkMode
                                    ? "border-sky-400/70 bg-sky-500/20 text-sky-200"
                                    : "border-sky-300 bg-sky-100 text-sky-700"
                                  : scalpDarkMode
                                    ? "border-zinc-600 bg-zinc-800 text-zinc-300 hover:border-zinc-500"
                                    : "border-slate-300 bg-white text-slate-600 hover:border-slate-400"
                              }`}
                            >
                              <span className="inline-flex items-center gap-1">
                                <span>{state}</span>
                                <span className="opacity-70">{stateTotal.toLocaleString()}</span>
                              </span>
                            </button>
                          );
                        })}
                        <span className={scalpTagNeutralClass}>
                          {`${scalpCandidatesTotal}/${scalpSessionCandidatesTotalForStateChip}`}
                        </span>
                      </div>
                    </div>
                    {scalpSelectedWorkerGridRows.length && scalpPaginatedCandidates.length > 0 ? (
                      <div
                        className={`mt-4 mx-4 mb-4 h-[420px] overflow-hidden rounded-xl border ${
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
                        {scalpAllCandidatesGridRows.length
                          ? "No candidates match the selected state filter."
                          : "No candidate rows are available yet."}
                      </div>
                    )}
                    <div className={`mt-2 px-4 pb-4 text-xs ${scalpTextMutedClass}`}>
                      {`loaded ${scalpVisibleWorkerGridRows.length} of ${scalpCandidatesTotal}`}
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
                      setChartRange(next);
                      // 4H is chart-only; PnL/summary ranges stay at 1D.
                      setDashboardRange(next === "4H" ? "1D" : next);
                    }}
                    statsSlot={swingChartStats}
                    livePrice={livePriceNow}
                    liveTimestamp={livePriceTs}
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
                        : activeTimeline.find((t) => t.hasDetails)?.ts ??
                          activeTimeline[0]?.ts ??
                          null
                    }
                    onTimelineTickSelect={(ts) => {
                      if (!activeSymbol) return;
                      const tick = activeTimeline.find((t) => t.ts === ts);
                      if (!tick) return;
                      void handleTimelineTickSelect(
                        activeSymbol,
                        tick,
                        tick.ts === activeTimeline[0].ts,
                      );
                    }}
                    onTimeSelect={(tsMs) => {
                      // Chart click → nearest decision-timeline tick. Clicking
                      // near the newest tick returns to the live view.
                      if (!activeSymbol || !activeTimeline.length) return;
                      let nearest = activeTimeline[0];
                      let bestDiff = Math.abs(nearest.ts - tsMs);
                      for (const tick of activeTimeline) {
                        const diff = Math.abs(tick.ts - tsMs);
                        if (diff < bestDiff) {
                          bestDiff = diff;
                          nearest = tick;
                        }
                      }
                      void handleTimelineTickSelect(
                        activeSymbol,
                        nearest,
                        nearest.ts === activeTimeline[0].ts,
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
                          {selectedTick?.kind === "postmortem"
                            ? "Post-mortem"
                            : selectedTick
                              ? "Decision"
                              : "Latest Decision"}
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
                    {selectedTick?.kind === "postmortem" ? (
                      selectedTickLoading ? (
                        renderDecisionBodySkeleton()
                      ) : selectedPostmortem ? (
                        <div className="mt-3">
                          <div className="flex flex-wrap items-center gap-2 text-sm">
                            <span className="postmortem-chip inline-flex rounded border px-1.5 py-0.5 font-semibold">
                              {(
                                selectedPostmortem.verdict ||
                                selectedPostmortem.status
                              ).replace(/_/g, " ")}
                            </span>
                            {typeof selectedPostmortem.pnlPct === "number" ? (
                              <span
                                className={`font-semibold ${
                                  selectedPostmortem.pnlPct >= 0
                                    ? "text-emerald-600"
                                    : "text-rose-600"
                                }`}
                              >
                                {selectedPostmortem.pnlPct >= 0 ? "+" : ""}
                                {selectedPostmortem.pnlPct.toFixed(2)}%
                              </span>
                            ) : null}
                            {typeof selectedPostmortem.report?.confidence ===
                            "number" ? (
                              <span className="text-xs text-slate-500">
                                confidence{" "}
                                {Math.round(
                                  selectedPostmortem.report.confidence * 100,
                                )}
                                %
                              </span>
                            ) : null}
                          </div>
                          {selectedPostmortem.status === "succeeded" &&
                          selectedPostmortem.lesson ? (
                            // The lesson IS the compact face of the report —
                            // everything else sits behind the disclosure.
                            <p className="mt-2 rounded-xl border border-slate-100 bg-slate-50 p-3 text-sm italic text-slate-800">
                              {selectedPostmortem.lesson}
                            </p>
                          ) : selectedPostmortem.status === "succeeded" ? (
                            // Deliberate no-lesson outcome: bad luck, or the
                            // library already covers this failure mode.
                            <p className="mt-2 text-sm text-slate-500">
                              No new lesson —{" "}
                              {selectedPostmortem.report?.lesson_action ===
                              "reinforce"
                                ? "an existing library lesson covers this case (reinforced)."
                                : "nothing generalizable to teach (see analysis)."}
                            </p>
                          ) : selectedPostmortem.status === "failed" ? (
                            <p className="mt-2 text-sm text-rose-600">
                              Analysis failed:{" "}
                              {selectedPostmortem.error || "unknown error"}
                            </p>
                          ) : (
                            <p className="mt-2 text-sm text-slate-500">
                              Analysis {selectedPostmortem.status}…
                            </p>
                          )}
                          {selectedPostmortem.status === "succeeded" &&
                          selectedPostmortem.report ? (
                            <>
                              <div className="mt-3">
                                <button
                                  onClick={() =>
                                    setShowPostmortemAnalysis((prev) => !prev)
                                  }
                                  className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 transition hover:border-slate-300"
                                >
                                  {showPostmortemAnalysis
                                    ? "Hide analysis"
                                    : "Show analysis"}
                                </button>
                              </div>
                              {showPostmortemAnalysis && (
                                <div className="mt-3 space-y-3 text-sm text-slate-700">
                                  {selectedPostmortem.report
                                    .timeline_analysis ? (
                                    <div>
                                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                        Timeline
                                      </div>
                                      <p className="mt-1 whitespace-pre-wrap">
                                        {
                                          selectedPostmortem.report
                                            .timeline_analysis
                                        }
                                      </p>
                                    </div>
                                  ) : null}
                                  {selectedPostmortem.report
                                    .lesson_adherence ? (
                                    <div>
                                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                        Lesson adherence
                                      </div>
                                      <p className="mt-1 whitespace-pre-wrap">
                                        {
                                          selectedPostmortem.report
                                            .lesson_adherence
                                        }
                                      </p>
                                    </div>
                                  ) : null}
                                  {selectedPostmortem.report.what_went_wrong
                                    ?.length ? (
                                    <div>
                                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                        What went wrong
                                      </div>
                                      <ul className="mt-1 list-disc space-y-1 pl-4">
                                        {selectedPostmortem.report.what_went_wrong.map(
                                          (item, idx) => (
                                            <li key={idx}>{item}</li>
                                          ),
                                        )}
                                      </ul>
                                    </div>
                                  ) : null}
                                  {selectedPostmortem.report.missed_signals
                                    ?.length ? (
                                    <div>
                                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                        Missed signals
                                      </div>
                                      <ul className="mt-1 space-y-1.5">
                                        {selectedPostmortem.report.missed_signals.map(
                                          (signal, idx) => (
                                            <li
                                              key={idx}
                                              className="rounded-lg border border-slate-100 bg-slate-50 p-2"
                                            >
                                              <div className="flex flex-wrap items-center gap-2 text-[10px] text-slate-500">
                                                {signal.ts_utc ? (
                                                  <span>
                                                    {Number.isFinite(
                                                      Date.parse(signal.ts_utc),
                                                    )
                                                      ? formatDecisionTime(
                                                          Date.parse(
                                                            signal.ts_utc,
                                                          ),
                                                        )
                                                      : signal.ts_utc}
                                                  </span>
                                                ) : null}
                                                {signal.visible_in ? (
                                                  <span className="inline-flex rounded border border-slate-200 bg-white px-1 py-0.5 font-semibold uppercase tracking-wide">
                                                    {signal.visible_in.replace(
                                                      /_/g,
                                                      " ",
                                                    )}
                                                  </span>
                                                ) : null}
                                              </div>
                                              <div className="mt-1">
                                                {signal.description}
                                              </div>
                                            </li>
                                          ),
                                        )}
                                      </ul>
                                    </div>
                                  ) : null}
                                  {selectedPostmortem.report.gate_impact ? (
                                    <div>
                                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                        Gate impact (skipped ticks)
                                      </div>
                                      <p className="mt-1 whitespace-pre-wrap">
                                        {selectedPostmortem.report.gate_impact}
                                      </p>
                                    </div>
                                  ) : null}
                                  {selectedPostmortem.report.suggestions
                                    ?.length ? (
                                    <div>
                                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                        Suggestions
                                      </div>
                                      <ul className="mt-1 list-disc space-y-1 pl-4">
                                        {selectedPostmortem.report.suggestions.map(
                                          (item, idx) => (
                                            <li key={idx}>{item}</li>
                                          ),
                                        )}
                                      </ul>
                                    </div>
                                  ) : null}
                                  <div>
                                    <button
                                      onClick={() =>
                                        setShowPostmortemDossier(
                                          (prev) => !prev,
                                        )
                                      }
                                      className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 transition hover:border-slate-300"
                                    >
                                      {showPostmortemDossier
                                        ? "Hide debug dossier"
                                        : "Show debug dossier"}
                                    </button>
                                  </div>
                                  {showPostmortemDossier && (
                                    <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                                        {selectedPostmortem.model || "model n/a"}
                                        {selectedPostmortem.usage
                                          ? ` · ${Number(selectedPostmortem.usage.input_tokens ?? 0).toLocaleString()} in / ${Number(selectedPostmortem.usage.output_tokens ?? 0).toLocaleString()} out tokens`
                                          : ""}
                                        {" · "}
                                        {selectedPostmortem.positionKey}
                                      </div>
                                      <pre className="mt-2 max-h-80 overflow-auto whitespace-pre-wrap break-words text-[10px] leading-relaxed text-slate-600">
                                        {JSON.stringify(
                                          selectedPostmortem.dossier,
                                          null,
                                          1,
                                        )}
                                      </pre>
                                    </div>
                                  )}
                                </div>
                              )}
                            </>
                          ) : null}
                        </div>
                      ) : (
                        <div className="mt-3 text-sm text-slate-500">
                          Post-mortem details unavailable.
                        </div>
                      )
                    ) : selectedTick && !selectedTick.hasDetails ? (
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
                                    (displayDecision as any)?.action,
                                    current.lastPositionPnl,
                                  )
                            }`}
                          >
                            {formatLastDecisionAction(displayDecision) || "—"}
                          </span>
                          {(displayDecision as any)?.summary
                            ? ` · ${(displayDecision as any).summary}`
                            : ""}
                        </div>
                        {(displayDecision as any)?.reason ? (
                          <p className="mt-2 text-sm text-slate-700 whitespace-pre-wrap">
                            <span className="font-semibold text-slate-800">
                              Reason:{" "}
                            </span>
                            {(displayDecision as any).reason}
                          </p>
                        ) : null}
                        <div className="mt-3 grid grid-cols-5 gap-1.5 sm:gap-2">
                          {biasOrder.map(({ key, label }) => {
                            const raw = (displayDecision as any)?.[key];
                            const val =
                              typeof raw === "string" ? raw.toUpperCase() : raw;
                            const tfLabel =
                              displayBiasTimeframes?.[
                                key.replace("_bias", "")
                              ] || (key === "nano_bias" ? "15m" : null);
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
                      </>
                    )}
                  </div>
                ) : (
                  // Decision data for this symbol hasn't arrived yet — hold
                  // the card's shape instead of collapsing the layout.
                  renderDecisionCardSkeleton()
                )}

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
