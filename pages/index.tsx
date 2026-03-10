import React, { useEffect, useRef, useState } from 'react';
import Head from 'next/head';
import dynamic from 'next/dynamic';
import vercelConfig from '../vercel.json';
import {
  Activity,
  BarChart3,
  BookOpen,
  ShieldPlus,
  Wand2,
  Circle,
  Cpu,
  Database,
  ListChecks,
  Braces,
  Layers3,
  PenTool,
  Repeat,
  ShieldCheck,
  Moon,
  Sun,
  Zap,
  Star,
  ArrowUpRight,
  ArrowDownRight,
  type LucideIcon,
} from 'lucide-react';

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
  openDirection?: 'long' | 'short' | null;
  openLeverage?: number | null;
  openEntryPrice?: number | null;
  lastPositionPnl?: number | null;
  lastPositionDirection?: 'long' | 'short' | null;
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
  openDirection?: 'long' | 'short' | null;
  openLeverage?: number | null;
  openEntryPrice?: number | null;
  lastPositionPnl?: number | null;
  lastPositionDirection?: 'long' | 'short' | null;
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

type ScalpDashboardSymbol = {
  symbol: string;
  strategyId: string;
  tuneId: string;
  deploymentId: string;
  tune: string;
  cronSchedule?: string | null;
  cronRoute?: 'execute-deployments' | string;
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
  tradeSide: 'BUY' | 'SELL' | null;
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
  backend?: 'file' | 'kv' | 'unknown' | string;
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

type ScalpSummaryResponse = {
  mode?: 'scalp';
  generatedAtMs?: number;
  range?: DashboardRangeKey;
  dayKey?: string;
  clockMode?: 'LONDON_TZ' | 'UTC_FIXED' | string;
  source?: 'deployment_registry' | 'cron_symbols' | string;
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
  symbols?: ScalpDashboardSymbol[];
  history?: ScalpHistoryDiscoverySnapshot;
  latestExecutionByDeploymentId?: Record<string, Record<string, any>>;
  latestExecutionBySymbol?: Record<string, Record<string, any>>;
  journal?: Array<{
    id?: string;
    timestampMs?: number;
    type?: string;
    level?: 'info' | 'warn' | 'error' | string;
    symbol?: string | null;
    dayKey?: string | null;
    reasonCodes?: string[];
    payload?: Record<string, any>;
  }>;
};

type ScalpJournalRow = NonNullable<ScalpSummaryResponse['journal']>[number];

type ScalpForwardValidation = {
  rollCount?: number;
  profitableWindowPct?: number;
  meanExpectancyR?: number;
  meanProfitFactor?: number | null;
  maxDrawdownR?: number | null;
  minTradesPerWindow?: number | null;
  selectionWindowDays?: number | null;
  forwardWindowDays?: number | null;
};

type ScalpResearchReportDeployment = {
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  source?: string;
  enabled?: boolean;
  promotionEligible?: boolean;
  promotionReason?: string | null;
  forwardValidation?: ScalpForwardValidation | null;
  perf30d?: {
    trades?: number;
    wins?: number;
    losses?: number;
    netR?: number;
    expectancyR?: number;
    winRatePct?: number;
    maxDrawdownR?: number;
  };
  perf90d?: {
    trades?: number;
    wins?: number;
    losses?: number;
    netR?: number;
    expectancyR?: number;
    winRatePct?: number;
    maxDrawdownR?: number;
  };
};

type ScalpResearchReportSnapshot = {
  generatedAtMs?: number;
  generatedAtIso?: string;
  cycle?: {
    cycleId?: string | null;
    status?: string | null;
    progressPct?: number | null;
    tasks?: number | null;
    completed?: number | null;
    failed?: number | null;
    candidateCount?: number | null;
  };
  summary?: {
    deploymentsTotal?: number;
    deploymentsEnabled?: number;
    enabledPromotionEligible?: number;
    enabledPromotionIneligible?: number;
    enabledWithoutGate?: number;
    enabledSymbols?: number;
    avgAbsPairCorrelation?: number | null;
  };
  deployments?: ScalpResearchReportDeployment[];
};

type ScalpResearchReportResponse = {
  ok?: boolean;
  snapshot?: ScalpResearchReportSnapshot;
};

type ScalpResearchCycleTaskResult = {
  windowFromTs?: number;
  windowToTs?: number;
  tuneId?: string;
  trades?: number;
  netR?: number;
  expectancyR?: number;
  profitFactor?: number | null;
  maxDrawdownR?: number;
};

type ScalpResearchCycleTask = {
  taskId?: string;
  symbol?: string;
  strategyId?: string;
  tuneId?: string;
  workerId?: string | null;
  windowFromTs?: number;
  windowToTs?: number;
  startedAtMs?: number | null;
  finishedAtMs?: number | null;
  status?: 'pending' | 'running' | 'completed' | 'failed' | string;
  result?: ScalpResearchCycleTaskResult | null;
  configOverride?: Record<string, unknown> | null;
  errorCode?: string | null;
  errorMessage?: string | null;
};

type ScalpResearchCycleResponse = {
  ok?: boolean;
  cycleId?: string;
  cycleSource?: 'requested' | 'active' | 'latest_completed_fallback' | 'none' | string;
  cycle?: {
    cycleId?: string;
    status?: string;
    createdAtMs?: number;
    updatedAtMs?: number;
    params?: {
      runningStaleAfterMs?: number;
    };
  };
  summary?: {
    status?: string;
    progressPct?: number;
    generatedAtMs?: number;
    totals?: {
      tasks?: number;
      pending?: number;
      running?: number;
      completed?: number;
      failed?: number;
    };
  } | null;
  workerHeartbeat?: {
    status?: 'started' | 'completed' | 'failed' | 'no_cycle' | 'cycle_not_found' | 'cycle_not_running' | string;
    workerId?: string;
    updatedAtMs?: number;
    startedAtMs?: number | null;
    finishedAtMs?: number | null;
    durationMs?: number | null;
    maxRuns?: number;
    concurrency?: number;
    maxDurationMs?: number;
    attemptedRuns?: number;
    completedRuns?: number;
    failedRuns?: number;
    stoppedByDurationBudget?: boolean;
  } | null;
  tasks?: ScalpResearchCycleTask[];
  taskCountReturned?: number;
  taskLimit?: number;
  includeTasks?: boolean;
};

type ScalpPromotionSyncMaterializationRow = {
  deploymentId?: string;
  symbol?: string;
  strategyId?: string;
  tuneId?: string;
  source?: 'matrix' | 'backtest' | string;
  exists?: boolean;
  created?: boolean;
};

type ScalpPromotionSyncMaterialization = {
  enabled?: boolean;
  source?: 'matrix' | 'backtest' | string;
  topKPerSymbol?: number;
  shortlistedCandidates?: number;
  missingCandidates?: number;
  createdCandidates?: number;
  rows?: ScalpPromotionSyncMaterializationRow[];
};

type ScalpPromotionSyncWeeklyPolicy = {
  enabled?: boolean;
  topKPerSymbol?: number;
  lookbackDays?: number;
  minCandlesPerSlice?: number;
  requireWinnerShortlist?: boolean;
  minSlices?: number;
  minProfitablePct?: number;
  minMedianExpectancyR?: number;
  maxTopWeekPnlConcentrationPct?: number;
};

type ScalpPromotionSyncPreviewResponse = {
  ok?: boolean;
  cycleId?: string | null;
  cycleStatus?: string | null;
  reason?: string | null;
  dryRun?: boolean;
  requireCompletedCycle?: boolean;
  weeklyPolicy?: ScalpPromotionSyncWeeklyPolicy | null;
  deploymentsConsidered?: number;
  deploymentsMatched?: number;
  deploymentsUpdated?: number;
  materialization?: ScalpPromotionSyncMaterialization | null;
  rows?: Array<{
    symbol?: string;
    strategyId?: string;
    tuneId?: string;
    weeklyGateReason?: string | null;
    previousGate?: {
      reason?: string | null;
      eligible?: boolean;
    } | null;
    nextGate?: {
      reason?: string | null;
      eligible?: boolean;
    } | null;
  }>;
};

type ScalpPromotionSyncSnapshot = ScalpPromotionSyncPreviewResponse & {
  fetchedAtMs: number;
};

type ScalpResearchUniverseCandidateRow = {
  symbol?: string;
  eligible?: boolean;
  score?: number;
  reasons?: string[];
  recommendedStrategyIds?: string[];
};

type ScalpResearchUniverseSeedResult = {
  symbol?: string;
  status?: 'seeded' | 'skipped' | 'failed' | string;
  reason?: string;
  addedCount?: number;
};

type ScalpResearchUniverseSeedSummary = {
  processedSymbols?: number;
  seededSymbols?: number;
  skippedSymbols?: number;
  failedSymbols?: number;
  results?: ScalpResearchUniverseSeedResult[];
};

type ScalpResearchUniverseResponse = {
  ok?: boolean;
  selectedCount?: number;
  candidatesEvaluated?: number;
  generatedAtIso?: string;
  snapshot?: {
    selectedSymbols?: string[];
    candidatesEvaluated?: number;
    generatedAtIso?: string;
    selectedRows?: ScalpResearchUniverseCandidateRow[];
    topRejectedRows?: ScalpResearchUniverseCandidateRow[];
    seedSummary?: ScalpResearchUniverseSeedSummary | null;
  };
};

type ScalpOpsDeploymentRow = {
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  source: string;
  enabled: boolean;
  promotionEligible: boolean;
  promotionReason: string | null;
  forwardValidation: ScalpForwardValidation | null;
  perf30dTrades: number | null;
  perf30dExpectancyR: number | null;
  perf30dNetR: number | null;
  perf30dMaxDrawdownR: number | null;
  runtime: ScalpDashboardSymbol | null;
};

type ScalpOpsCronStatus = 'healthy' | 'lagging' | 'unknown';
type ScalpOpsCronDetailTone = 'neutral' | 'positive' | 'warning' | 'critical';
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
  | 'symbol'
  | 'strategyId'
  | 'tuneId'
  | 'whyNotPromoted'
  | 'windowToTs'
  | 'status'
  | 'trades'
  | 'netR'
  | 'expectancyR'
  | 'profitFactor'
  | 'maxDrawdownR';

type ScalpWorkerSortDirection = 'asc' | 'desc';

type ScalpWorkerSortState = {
  key: ScalpWorkerSortKey;
  direction: ScalpWorkerSortDirection;
};

type ScalpUniversePipelineRow = {
  symbol: string;
  discovered: boolean;
  importStatus: 'seeded' | 'skipped' | 'failed' | 'not_run';
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
  lastDecision?: EvaluationEntry['lastDecision'];
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

type EvaluateJobStatus = 'queued' | 'running' | 'succeeded' | 'failed';

type EvaluateJobRecord = {
  id: string;
  status: EvaluateJobStatus;
  updatedAt?: number;
  error?: string;
};

type DashboardRangeKey = '7D' | '30D' | '6M';
type ThemePreference = 'system' | 'light' | 'dark';
type ResolvedTheme = 'light' | 'dark';
type StrategyMode = 'swing' | 'scalp';

const CURRENCY_SYMBOL = '₮'; // Tether-style symbol
const THEME_PREFERENCE_STORAGE_KEY = 'dashboard_theme_preference';
const formatUsd = (value: number) => {
  const abs = Math.abs(value);
  const sign = value < 0 ? '-' : '';
  const v = Math.abs(value);
  if (abs >= 1_000_000) return `${sign}${CURRENCY_SYMBOL}${(v / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${sign}${CURRENCY_SYMBOL}${(v / 1_000).toFixed(1)}K`;
  return `${sign}${CURRENCY_SYMBOL}${v.toFixed(0)}`;
};
const formatSignedR = (value: number): string => `${value >= 0 ? '+' : ''}${value.toFixed(2)}R`;

const BERLIN_TZ = 'Europe/Berlin';
const BITGET_PUBLIC_WS_URL = 'wss://ws.bitget.com/v2/ws/public';
const WS_RECONNECT_MS = 1500;
const WS_PING_MS = 25_000;
const CAPITAL_LIVE_POLL_MS = 3000;
const SCALP_LIVE_POLL_VISIBLE_MS = 15_000;
const SCALP_LIVE_POLL_HIDDEN_MS = 60_000;
const SCALP_LIVE_POLL_ERROR_BACKOFF_MS = 120_000;
const SCALP_MIN_REFRESH_GAP_MS = 8_000;
const SCALP_RESEARCH_REFRESH_MS = 45_000;
const SCALP_PROMOTION_SYNC_REFRESH_MS = 5 * 60_000;
const SCALP_WORKER_TASK_LIMIT_PREVIEW = 100;
const SCALP_WORKER_TASK_LIMIT_FULL = 5_000;

const ADMIN_SECRET_STORAGE_KEY = 'admin_access_secret';
const ADMIN_AUTH_TIMEOUT_MS = 4000;
const STRATEGY_MODE_STORAGE_KEY = 'strategy_mode';

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

const SCALP_CRON_PIPELINE_DEFINITIONS: Record<string, ScalpCronPipelineDefinition> = {
  scalp_discover_symbols: {
    primaryPathname: '/api/scalp/cron/discover-symbols',
    matchPathnames: ['/api/scalp/cron/discover-symbols'],
    fallbackInvokePath:
      '/api/scalp/cron/discover-symbols?dryRun=false&includeLiveQuotes=true&seedTopSymbols=10&seedChunkDays=5&seedTargetHistoryDays=90&seedMaxHistoryDays=95&seedMaxRequestsPerSymbol=30&seedMaxSymbolsPerRun=10',
  },
  scalp_cycle_start: {
    primaryPathname: '/api/scalp/cron/research-cycle-start',
    matchPathnames: ['/api/scalp/cron/research-cycle-start'],
    fallbackInvokePath: '/api/scalp/cron/research-cycle-start?dryRun=false&force=false',
  },
  scalp_cycle_worker: {
    primaryPathname: '/api/scalp/cron/research-cycle-worker',
    matchPathnames: ['/api/scalp/cron/research-cycle-worker'],
    fallbackInvokePath:
      '/api/scalp/cron/research-cycle-worker?maxRuns=40&concurrency=4&aggregateAfter=false&finalizeWhenDone=true&syncPromotionGates=false&requireCompletedCycleForSync=false',
  },
  scalp_cycle_aggregate: {
    primaryPathname: '/api/scalp/cron/research-cycle-aggregate',
    matchPathnames: ['/api/scalp/cron/research-cycle-aggregate'],
    fallbackInvokePath: '/api/scalp/cron/research-cycle-aggregate?finalizeWhenDone=true',
  },
  scalp_promotion_gate_apply: {
    primaryPathname: '/api/scalp/cron/research-cycle-sync-gates',
    matchPathnames: ['/api/scalp/cron/research-cycle-sync-gates'],
    fallbackInvokePath:
      '/api/scalp/cron/research-cycle-sync-gates?dryRun=false&requireCompletedCycle=true&materializeEnabled=true',
  },
  scalp_execute_deployments: {
    primaryPathname: '/api/scalp/cron/execute-deployments',
    matchPathnames: ['/api/scalp/cron/execute-deployments'],
    fallbackInvokePath: '/api/scalp/cron/execute-deployments?all=true&dryRun=false&requirePromotionEligible=true',
  },
  scalp_live_guardrail_monitor: {
    primaryPathname: '/api/scalp/cron/live-guardrail-monitor',
    matchPathnames: ['/api/scalp/cron/live-guardrail-monitor'],
    fallbackInvokePath: '/api/scalp/cron/live-guardrail-monitor?dryRun=false&autoPause=true',
  },
  scalp_housekeeping: {
    primaryPathname: '/api/scalp/cron/housekeeping',
    matchPathnames: ['/api/scalp/cron/housekeeping'],
    fallbackInvokePath: '/api/scalp/cron/housekeeping?dryRun=false&refreshReport=true',
  },
};

const scalpParsedCronCache = new Map<string, ParsedCronSchedule | null>();

function parseCronPathname(rawPath: unknown): string | null {
  const value = String(rawPath || '').trim();
  if (!value) return null;
  try {
    return new URL(value, 'http://localhost').pathname;
  } catch {
    return null;
  }
}

function dedupeStrings(rows: string[]): string[] {
  return Array.from(new Set(rows.map((row) => String(row || '').trim()).filter((row) => row.length > 0)));
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

  const [baseRaw, stepRaw] = segment.split('/');
  const base = (baseRaw || '').trim();
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
  if (base !== '*' && base.length > 0) {
    if (base.includes('-')) {
      const [startRaw, endRaw] = base.split('-');
      const parsedStart = parseNumber(startRaw || '');
      const parsedEnd = parseNumber(endRaw || '');
      if (parsedStart === null || parsedEnd === null || parsedEnd < parsedStart) return null;
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
  const wildcard = field === '*';
  const out = new Set<number>();
  const segments = field.split(',');
  for (const segmentRaw of segments) {
    const values = parseCronFieldSegment(segmentRaw, min, max, normalizer);
    if (!values) return null;
    for (const value of values) out.add(value);
  }
  return { values: out, wildcard };
}

function parseCronSchedule(expressionRaw: string): ParsedCronSchedule | null {
  const expression = String(expressionRaw || '').trim();
  if (!expression) return null;
  if (scalpParsedCronCache.has(expression)) {
    return scalpParsedCronCache.get(expression) || null;
  }

  const parts = expression.split(/\s+/).filter((part) => part.length > 0);
  if (parts.length !== 5) {
    scalpParsedCronCache.set(expression, null);
    return null;
  }

  const minute = parseCronField(parts[0] || '', 0, 59);
  const hour = parseCronField(parts[1] || '', 0, 23);
  const dayOfMonth = parseCronField(parts[2] || '', 1, 31);
  const month = parseCronField(parts[3] || '', 1, 12);
  const dayOfWeek = parseCronField(parts[4] || '', 0, 6, normalizeCronNumberForDayOfWeek);
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

function cronMatchesUtcMinute(tsMs: number, parsed: ParsedCronSchedule): boolean {
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

function formatScalpNextRunIn(nextRunAtMs: number | null, nowMs: number): string {
  if (nextRunAtMs === null) return '—';
  const diffMs = nextRunAtMs - nowMs;
  if (!Number.isFinite(diffMs)) return '—';
  if (diffMs <= 0) return 'now';
  if (diffMs < 60_000) return `in ${Math.max(1, Math.ceil(diffMs / 1_000))}s`;
  const totalMinutes = Math.max(1, Math.ceil(diffMs / 60_000));
  const days = Math.floor(totalMinutes / (24 * 60));
  const hours = Math.floor((totalMinutes % (24 * 60)) / 60);
  const minutes = totalMinutes % 60;
  if (days > 0) return `in ${days}d ${hours}h`;
  if (hours > 0) return `in ${hours}h ${minutes}m`;
  return `in ${minutes}m`;
}

function buildScalpCronRuntimeMap(nowMs: number): Record<string, ScalpCronRuntimeMeta> {
  const crons: VercelCronEntry[] = Array.isArray((vercelConfig as any)?.crons) ? (vercelConfig as any).crons : [];
  const out: Record<string, ScalpCronRuntimeMeta> = {};

  for (const [id, def] of Object.entries(SCALP_CRON_PIPELINE_DEFINITIONS)) {
    const rows = crons
      .map((row) => {
        const path = String(row?.path || '').trim();
        const schedule = String(row?.schedule || '').trim();
        const pathname = parseCronPathname(path);
        return { path, schedule, pathname };
      })
      .filter((row) => row.pathname !== null && def.matchPathnames.includes(row.pathname));

    const expressions = dedupeStrings(rows.map((row) => row.schedule));
    const nextRunCandidates = expressions
      .map((expression) => nextCronRunAtMs(expression, nowMs))
      .filter((value): value is number => typeof value === 'number' && Number.isFinite(value));
    const nextRunAtMs = nextRunCandidates.length ? Math.min(...nextRunCandidates) : null;
    const invokePath =
      rows.find((row) => row.pathname === def.primaryPathname)?.path ||
      rows[0]?.path ||
      def.fallbackInvokePath ||
      null;

    out[id] = {
      expressions,
      expressionLabel: expressions.length ? expressions.join(' | ') : null,
      nextRunAtMs,
      invokePath,
    };
  }

  return out;
}

const ChartPanel = dynamic(() => import('../components/ChartPanel'), {
  ssr: false,
  loading: () => (
    <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm lg:col-span-2">
      <div className="flex items-center justify-between">
        <div className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 p-1 text-[11px] font-semibold text-slate-500">
          <span className="rounded-full bg-slate-200 px-2.5 py-1 text-slate-700">7D</span>
          <span className="px-2.5 py-1">30D</span>
          <span className="px-2.5 py-1">6M</span>
        </div>
        <div className="text-xs text-slate-400">1H bars · 7D window</div>
      </div>
      <div className="relative mt-3 h-[260px] w-full" style={{ minHeight: 260 }}>
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
  const [adminInput, setAdminInput] = useState('');
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
  const [evaluateJobs, setEvaluateJobs] = useState<Record<string, EvaluateJobRecord>>({});
  const [evaluateSubmittingSymbol, setEvaluateSubmittingSymbol] = useState<string | null>(null);
  const [dashboardRange, setDashboardRange] = useState<DashboardRangeKey>('7D');
  const [strategyMode, setStrategyMode] = useState<StrategyMode>('swing');
  const [scalpSummary, setScalpSummary] = useState<ScalpSummaryResponse | null>(null);
  const [scalpResearchCycle, setScalpResearchCycle] = useState<ScalpResearchCycleResponse | null>(null);
  const [scalpResearchReport, setScalpResearchReport] = useState<ScalpResearchReportSnapshot | null>(null);
  const [scalpResearchUniverse, setScalpResearchUniverse] = useState<ScalpResearchUniverseResponse | null>(null);
  const [scalpPromotionSyncSnapshot, setScalpPromotionSyncSnapshot] =
    useState<ScalpPromotionSyncSnapshot | null>(null);
  const [scalpActiveDeploymentId, setScalpActiveDeploymentId] = useState<string | null>(null);
  const [scalpExpandedCronId, setScalpExpandedCronId] = useState<string | null>(null);
  const [scalpWorkerTasksLoadingFull, setScalpWorkerTasksLoadingFull] = useState(false);
  const [scalpWorkerSort, setScalpWorkerSort] = useState<ScalpWorkerSortState>({
    key: 'windowToTs',
    direction: 'desc',
  });
  const [scalpCronNowMs, setScalpCronNowMs] = useState<number>(() => Date.now());
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
  const [scalpWorkerRetryStateByTaskId, setScalpWorkerRetryStateByTaskId] = useState<
    Record<
      string,
      {
        running: boolean;
        atMs: number | null;
        ok: boolean | null;
        message: string | null;
      }
    >
  >({});
  const [livePriceNow, setLivePriceNow] = useState<number | null>(null);
  const [livePriceTs, setLivePriceTs] = useState<number | null>(null);
  const [livePriceConnected, setLivePriceConnected] = useState(false);
  const [themePreference, setThemePreference] = useState<ThemePreference>('system');
  const [resolvedTheme, setResolvedTheme] = useState<ResolvedTheme>('light');
  const evaluatePollTimersRef = useRef<Record<string, number>>({});
  const scalpResearchFetchedAtMsRef = useRef<number>(0);
  const scalpPromotionSyncFetchedAtMsRef = useRef<number>(0);
  const scalpSummaryFetchedAtMsRef = useRef<number>(0);
  const scalpSummaryErrorCountRef = useRef<number>(0);

  const readStoredAdminSecret = () => {
    if (typeof window === 'undefined') return null;
    const stored = window.localStorage.getItem(ADMIN_SECRET_STORAGE_KEY);
    const normalized = typeof stored === 'string' ? stored.trim() : '';
    return normalized || null;
  };

  const resolveAdminSecret = () => {
    const inMemory = typeof adminSecret === 'string' ? adminSecret.trim() : '';
    if (inMemory) return inMemory;
    return readStoredAdminSecret();
  };

  const buildAdminHeaders = () => {
    const secret = resolveAdminSecret();
    return secret ? { 'x-admin-access-secret': secret } : undefined;
  };

  useEffect(() => {
    const timerId = window.setInterval(() => {
      setScalpCronNowMs(Date.now());
    }, 1_000);
    return () => window.clearInterval(timerId);
  }, []);

  const resolveSystemTheme = (): ResolvedTheme => {
    if (typeof window === 'undefined') return 'light';
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  };

  const handleAuthExpired = (message?: string) => {
    if (typeof window !== 'undefined') {
      window.localStorage.removeItem(ADMIN_SECRET_STORAGE_KEY);
    }
    setAdminSecret(null);
    setAdminGranted(false);
    setAdminInput('');
    setAdminError(message || 'Admin session expired. Enter ADMIN_ACCESS_SECRET again.');
  };

  const validateAdminAccess = async (secret: string | null) => {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), ADMIN_AUTH_TIMEOUT_MS);
    const normalizedSecret = typeof secret === 'string' ? secret.trim() : '';
    try {
      const res = await fetch('/api/admin-auth', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
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
      setAdminInput('');
    } else {
      setAdminError('Invalid access secret.');
    }
    setAdminSubmitting(false);
  };

  const aspectMeta: Record<string, { Icon: LucideIcon; color: string; bg: string }> = {
    data_quality: { Icon: Database, color: 'text-sky-700', bg: 'bg-sky-100' },
    data_quantity: { Icon: Layers3, color: 'text-cyan-700', bg: 'bg-cyan-100' },
    ai_performance: { Icon: Cpu, color: 'text-indigo-700', bg: 'bg-indigo-100' },
    strategy_performance: { Icon: BarChart3, color: 'text-emerald-700', bg: 'bg-emerald-100' },
    signal_strength_clarity: { Icon: Activity, color: 'text-amber-700', bg: 'bg-amber-100' },
    risk_management: { Icon: ShieldCheck, color: 'text-rose-700', bg: 'bg-rose-100' },
    consistency: { Icon: Repeat, color: 'text-blue-700', bg: 'bg-blue-100' },
    explainability: { Icon: BookOpen, color: 'text-purple-700', bg: 'bg-purple-100' },
    responsiveness: { Icon: Zap, color: 'text-teal-700', bg: 'bg-teal-100' },
    prompt_engineering: { Icon: PenTool, color: 'text-fuchsia-700', bg: 'bg-fuchsia-100' },
    prompt_consistency: { Icon: ListChecks, color: 'text-lime-700', bg: 'bg-lime-100' },
    action_logic: { Icon: Braces, color: 'text-orange-700', bg: 'bg-orange-100' },
    ai_freedom: { Icon: Wand2, color: 'text-indigo-700', bg: 'bg-indigo-100' },
    guardrail_coverage: { Icon: ShieldPlus, color: 'text-rose-700', bg: 'bg-rose-100' },
  };

  const formatLabel = (key: string) => key.replace(/_/g, ' ');
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

  const loadSymbolDecision = async (symbol: string, platform?: string | null) => {
    if (!symbol) return;
    const params = new URLSearchParams({ symbol });
    if (platform) params.set('platform', platform);
    const res = await fetch(`/api/swing/dashboard/decision?${params.toString()}`, {
      headers: buildAdminHeaders(),
      cache: 'no-store',
    });
    if (res.status === 401) {
      handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
      throw new Error('Unauthorized');
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
    const res = await fetch(`/api/swing/dashboard/evaluation?${params.toString()}`, {
      headers: buildAdminHeaders(),
      cache: 'no-store',
    });
    if (res.status === 401) {
      handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
      throw new Error('Unauthorized');
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

  const loadDashboard = async () => {
    setLoading(true);
    try {
      let summaryError: string | null = null;
      const symbolsRes = await fetch('/api/swing/dashboard/symbols', {
        headers: buildAdminHeaders(),
        cache: 'no-store',
      });
      if (!symbolsRes.ok) {
        if (symbolsRes.status === 401) {
          handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
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
        const nextIdx = orderedSymbols.findIndex((s) => s === activeSymbolBefore);
        return nextIdx >= 0 ? nextIdx : 0;
      });

      setTabData((prev) => {
        const next: Record<string, EvaluationEntry> = {};
        for (const symbol of orderedSymbols) {
          const key = symbol.toUpperCase();
          const meta = symbolMeta.get(key);
          const existing = prev[key] || prev[symbol] || { symbol: key, evaluation: {} };
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
        const summaryRes = await fetch(`/api/swing/dashboard/summary?${summaryParams.toString()}`, {
          headers: buildAdminHeaders(),
          cache: 'no-store',
        });
        if (summaryRes.status === 401) {
          handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
          throw new Error('Unauthorized');
        }
        if (!summaryRes.ok) {
          throw new Error(`Failed to load summary (${summaryRes.status})`);
        }
        const summaryJson: DashboardSummaryResponse = await summaryRes.json();
        const summaryRows = Array.isArray(summaryJson.data) ? summaryJson.data : [];
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
        summaryError = summaryErr?.message || 'Failed to load dashboard summary';
      }

      setError(summaryError);
    } catch (err: any) {
      setError(err?.message || 'Failed to load dashboard');
    } finally {
      setLoading(false);
    }
  };

  const loadScalpDashboard = async (opts: { silent?: boolean; force?: boolean } = {}) => {
    const silent = opts.silent === true;
    const force = opts.force === true;
    const nowMs = Date.now();
    if (!force && silent && nowMs - scalpSummaryFetchedAtMsRef.current < SCALP_MIN_REFRESH_GAP_MS) {
      return;
    }
    if (!silent) setLoading(true);
    try {
      const params = new URLSearchParams({
        useDeploymentRegistry: 'true',
        range: dashboardRange,
      });
      if (!silent || force) {
        params.set('fresh', 'true');
      }
      const summaryRes = await fetch(`/api/scalp/dashboard/summary?${params.toString()}`, {
        headers: buildAdminHeaders(),
        cache: 'no-store',
      });
      if (summaryRes.status === 401) {
        handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
        throw new Error('Unauthorized');
      }
      if (!summaryRes.ok) {
        throw new Error(`Failed to load scalp summary (${summaryRes.status})`);
      }
      const summaryJson: ScalpSummaryResponse = await summaryRes.json();
      setScalpSummary(summaryJson);
      scalpSummaryFetchedAtMsRef.current = nowMs;
      scalpSummaryErrorCountRef.current = 0;
      const shouldRefreshResearch =
        !silent || nowMs - scalpResearchFetchedAtMsRef.current >= SCALP_RESEARCH_REFRESH_MS;
      if (shouldRefreshResearch) {
        const fetchScalpEndpoint = async (url: string): Promise<{ status: number; json: any | null }> => {
          const res = await fetch(url, {
            headers: buildAdminHeaders(),
            cache: 'no-store',
          });
          if (res.status === 401) {
            handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
            throw new Error('Unauthorized');
          }
          if (res.status === 404) {
            return { status: 404, json: null };
          }
          if (!res.ok) {
            return { status: res.status, json: null };
          }
          const json = await res.json().catch(() => null);
          return { status: res.status, json };
        };

        const shouldRefreshPromotionSync =
          !silent ||
          nowMs - scalpPromotionSyncFetchedAtMsRef.current >= SCALP_PROMOTION_SYNC_REFRESH_MS;

        const [cycleResult, reportResult, universeResult, promotionSyncResult] = await Promise.all([
          fetchScalpEndpoint(
            `/api/scalp/research/cycle?includeTasks=true&taskLimit=${SCALP_WORKER_TASK_LIMIT_PREVIEW}`,
          ),
          fetchScalpEndpoint('/api/scalp/research/report'),
          fetchScalpEndpoint('/api/scalp/research/universe'),
          shouldRefreshPromotionSync
            ? fetchScalpEndpoint(
                '/api/scalp/cron/research-cycle-sync-gates?dryRun=true&weeklyRobustnessEnabled=false&requireCompletedCycle=true',
              )
            : Promise.resolve({ status: 0, json: null }),
        ]);

        if (cycleResult.status === 200 && cycleResult.json) {
          setScalpResearchCycle(cycleResult.json as ScalpResearchCycleResponse);
        } else if (cycleResult.status === 404) {
          setScalpResearchCycle(null);
        }

        if (reportResult.status === 200 && reportResult.json) {
          const reportJson = reportResult.json as ScalpResearchReportResponse;
          setScalpResearchReport(reportJson.snapshot || null);
        } else if (reportResult.status === 404) {
          setScalpResearchReport(null);
        }

        if (universeResult.status === 200 && universeResult.json) {
          setScalpResearchUniverse(universeResult.json as ScalpResearchUniverseResponse);
        } else if (universeResult.status === 404) {
          setScalpResearchUniverse(null);
        }

        if (shouldRefreshPromotionSync) {
          if (promotionSyncResult.status === 200 && promotionSyncResult.json) {
            setScalpPromotionSyncSnapshot({
              ...(promotionSyncResult.json as ScalpPromotionSyncPreviewResponse),
              fetchedAtMs: nowMs,
            });
          } else if (promotionSyncResult.status === 404) {
            setScalpPromotionSyncSnapshot(null);
          }
          scalpPromotionSyncFetchedAtMsRef.current = nowMs;
        }

        scalpResearchFetchedAtMsRef.current = nowMs;
      }
      setError(null);
    } catch (err: any) {
      scalpSummaryErrorCountRef.current += 1;
      setError(err?.message || 'Failed to load scalp dashboard');
    } finally {
      if (!silent) setLoading(false);
    }
  };

  const scalpResearchCycleNeedsFullTasks = (cycle: ScalpResearchCycleResponse | null): boolean => {
    if (!cycle) return false;
    const taskLimit = Number(cycle.taskLimit);
    const taskCountReturned = Number(cycle.taskCountReturned);
    const totalTasks = Number(cycle.summary?.totals?.tasks);
    const safeTaskLimit = Number.isFinite(taskLimit) && taskLimit > 0 ? Math.floor(taskLimit) : null;
    const safeTaskCountReturned =
      Number.isFinite(taskCountReturned) && taskCountReturned >= 0 ? Math.floor(taskCountReturned) : null;
    const safeTotalTasks = Number.isFinite(totalTasks) && totalTasks >= 0 ? Math.floor(totalTasks) : null;
    if (safeTaskLimit !== null && safeTaskLimit >= SCALP_WORKER_TASK_LIMIT_FULL) return false;
    if (safeTotalTasks !== null && safeTaskCountReturned !== null) return safeTotalTasks > safeTaskCountReturned;
    if (safeTaskCountReturned !== null && safeTaskLimit !== null) return safeTaskCountReturned >= safeTaskLimit;
    return false;
  };

  const loadScalpWorkerTasksFull = async () => {
    if (scalpWorkerTasksLoadingFull) return;
    if (!scalpResearchCycleNeedsFullTasks(scalpResearchCycle)) return;

    setScalpWorkerTasksLoadingFull(true);
    try {
      const res = await fetch(
        `/api/scalp/research/cycle?includeTasks=true&taskLimit=${SCALP_WORKER_TASK_LIMIT_FULL}`,
        {
          headers: buildAdminHeaders(),
          cache: 'no-store',
        },
      );
      if (res.status === 401) {
        handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
        return;
      }
      if (res.status === 404) {
        setScalpResearchCycle(null);
        return;
      }
      if (!res.ok) return;
      const json = await res.json().catch(() => null);
      if (json) {
        setScalpResearchCycle(json as ScalpResearchCycleResponse);
      }
    } catch (err) {
      console.warn('Failed to lazy-load full scalp worker tasks:', err);
    } finally {
      setScalpWorkerTasksLoadingFull(false);
    }
  };

  const invokeScalpCronNow = async (row: ScalpOpsCronRow) => {
    const invokePath = String(row.invokePath || '').trim();
    if (!invokePath) {
      setScalpCronInvokeStateById((prev) => ({
        ...prev,
        [row.id]: {
          running: false,
          atMs: Date.now(),
          ok: false,
          status: null,
          durationMs: null,
          message: 'No invoke path configured',
        },
      }));
      return;
    }

    const invokeStartedAtMs = Date.now();
    setScalpCronInvokeStateById((prev) => ({
      ...prev,
      [row.id]: {
        ...(prev[row.id] || { atMs: null, ok: null, status: null, durationMs: null, message: null }),
        running: true,
        message: null,
      },
    }));

    try {
      const res = await fetch(invokePath, {
        headers: buildAdminHeaders(),
        cache: 'no-store',
      });
      const payload = await res.json().catch(() => null);
      if (res.status === 401) {
        handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
        throw new Error('Unauthorized');
      }
      if (!res.ok) {
        const msg = String(payload?.message || payload?.error || '').trim() || `Invoke failed (${res.status})`;
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

      const okMsg = String(payload?.message || '').trim() || 'Invoked';
      const workerDurationMs = asFiniteNumber(payload?.worker?.diagnostics?.durationMs);
      const durationMs =
        workerDurationMs !== null ? Math.max(0, Math.floor(workerDurationMs)) : Math.max(0, Date.now() - invokeStartedAtMs);
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
      const msg = String(err?.message || 'Invoke failed').trim() || 'Invoke failed';
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

  const retryScalpWorkerTask = async (taskIdRaw: string) => {
    const taskId = String(taskIdRaw || '').trim();
    const cycleId = String(scalpResearchCycle?.cycleId || scalpResearchCycle?.cycle?.cycleId || '').trim();
    const cycleStatus = String(
      scalpResearchCycle?.cycle?.status || scalpResearchCycle?.summary?.status || '',
    )
      .trim()
      .toLowerCase();
    if (!taskId || !cycleId || cycleStatus !== 'running') {
      setScalpWorkerRetryStateByTaskId((prev) => ({
        ...prev,
        [taskId || 'unknown']: {
          running: false,
          atMs: Date.now(),
          ok: false,
          message: 'Retry unavailable unless cycle is active and running.',
        },
      }));
      return;
    }

    setScalpWorkerRetryStateByTaskId((prev) => ({
      ...prev,
      [taskId]: {
        running: true,
        atMs: null,
        ok: null,
        message: null,
      },
    }));

    try {
      const res = await fetch('/api/scalp/research/cycle', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(buildAdminHeaders() || {}),
        },
        cache: 'no-store',
        body: JSON.stringify({
          action: 'retryTask',
          cycleId,
          taskId,
        }),
      });
      const payload = await res.json().catch(() => null);
      if (res.status === 401) {
        handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
        throw new Error('Unauthorized');
      }
      if (!res.ok) {
        const msg =
          String(payload?.message || payload?.error || '').trim() || `Retry failed (${res.status})`;
        setScalpWorkerRetryStateByTaskId((prev) => ({
          ...prev,
          [taskId]: {
            running: false,
            atMs: Date.now(),
            ok: false,
            message: msg,
          },
        }));
        return;
      }

      setScalpWorkerRetryStateByTaskId((prev) => ({
        ...prev,
        [taskId]: {
          running: false,
          atMs: Date.now(),
          ok: true,
          message: 'Requeued',
        },
      }));
      await loadScalpDashboard({ silent: true, force: true });
    } catch (err: any) {
      const msg = String(err?.message || 'Retry failed').trim() || 'Retry failed';
      setScalpWorkerRetryStateByTaskId((prev) => ({
        ...prev,
        [taskId]: {
          running: false,
          atMs: Date.now(),
          ok: false,
          message: msg,
        },
      }));
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
        cache: 'no-store',
      });
      if (res.status === 401) {
        clearEvaluatePollTimer(symbol);
        handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
        setError('Evaluation polling unauthorized (401). Re-enter admin access secret.');
        return;
      }
      if (res.status === 304) return;
      if (!res.ok) return;
      const json = await res.json();
      const status = String(json?.status || '') as EvaluateJobStatus;
      if (!status) return;
      setEvaluateJobs((prev) => ({
        ...prev,
        [symbol]: {
          id: jobId,
          status,
          updatedAt: Number(json?.updatedAt) || Date.now(),
          error: typeof json?.error === 'string' ? json.error : undefined,
        },
      }));

      if (status === 'succeeded' || status === 'failed') {
        clearEvaluatePollTimer(symbol);
        if (status === 'succeeded') {
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
        async: 'true',
      });
      const res = await fetch(`/api/swing/evaluate?${params.toString()}`, {
        headers: buildAdminHeaders(),
        cache: 'no-store',
      });
      if (!res.ok) {
        if (res.status === 401) {
          handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
        }
        let msg = `Failed to queue evaluation (${res.status})`;
        try {
          const body = await res.json();
          msg = body?.error ? `${msg}: ${String(body.error)}` : msg;
        } catch {}
        throw new Error(msg);
      }
      const json = await res.json();
      const jobId = String(json?.jobId || '');
      if (!jobId) throw new Error('Missing evaluation job ID');
      setEvaluateJobs((prev) => ({
        ...prev,
        [symbol]: { id: jobId, status: 'queued', updatedAt: Date.now() },
      }));
      clearEvaluatePollTimer(symbol);
      void pollEvaluationJob(symbol, jobId);
      evaluatePollTimersRef.current[symbol] = window.setInterval(() => {
        void pollEvaluationJob(symbol, jobId);
      }, 5000);
    } catch (err: any) {
      setError(err?.message || 'Failed to queue evaluation');
    } finally {
      setEvaluateSubmittingSymbol(null);
    }
  };

  useEffect(() => {
    if (typeof window === 'undefined') return;
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
    if (typeof window === 'undefined') return;
    const stored = window.localStorage.getItem(THEME_PREFERENCE_STORAGE_KEY);
    const normalizedThemePreference: ThemePreference =
      stored === 'light' || stored === 'dark' || stored === 'system' ? stored : 'system';
    setThemePreference(normalizedThemePreference);
    if (normalizedThemePreference === 'system') {
      setResolvedTheme(resolveSystemTheme());
      return;
    }
    setResolvedTheme(normalizedThemePreference);
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const stored = window.localStorage.getItem(STRATEGY_MODE_STORAGE_KEY);
    if (stored === 'swing' || stored === 'scalp') {
      setStrategyMode(stored);
      return;
    }
    if (stored === 'forex') {
      window.localStorage.setItem(STRATEGY_MODE_STORAGE_KEY, 'swing');
    }
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (themePreference !== 'system') return;
    const media = window.matchMedia('(prefers-color-scheme: dark)');
    const handleThemeChange = () => {
      setResolvedTheme(media.matches ? 'dark' : 'light');
    };
    handleThemeChange();
    if (typeof media.addEventListener === 'function') {
      media.addEventListener('change', handleThemeChange);
      return () => media.removeEventListener('change', handleThemeChange);
    }
    media.addListener(handleThemeChange);
    return () => media.removeListener(handleThemeChange);
  }, [themePreference]);

  useEffect(() => {
    if (typeof document === 'undefined') return;
    document.documentElement.style.colorScheme = resolvedTheme;
  }, [resolvedTheme]);

  useEffect(() => {
    if (!adminGranted) return;
    if (strategyMode === 'scalp') {
      loadScalpDashboard();
      return;
    }
    loadDashboard();
  }, [adminGranted, dashboardRange, strategyMode]);

  useEffect(() => {
    if (!adminGranted || strategyMode !== 'scalp') return;
    let cancelled = false;
    let timerId: number | null = null;

    const scheduleNextPoll = () => {
      if (cancelled) return;
      const hidden = typeof document !== 'undefined' && document.visibilityState !== 'visible';
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
  }, [adminGranted, strategyMode, adminSecret, dashboardRange]);

  useEffect(() => {
    const rows = Array.isArray(scalpSummary?.symbols) ? scalpSummary.symbols : [];
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
    if (strategyMode !== 'swing') return;
    if (!adminGranted || !symbol) return;
    const platform = tabData[symbol]?.lastPlatform ?? null;
    let cancelled = false;
    (async () => {
      try {
        await Promise.all([loadSymbolDecision(symbol, platform), loadSymbolEvaluation(symbol)]);
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
    if (strategyMode !== 'swing') {
      setLivePriceNow(null);
      setLivePriceTs(null);
      setLivePriceConnected(false);
      return;
    }
    const symbol = symbols[active] || null;
    const platform = symbol ? String(tabData[symbol]?.lastPlatform || '').toLowerCase() : '';
    if (!adminGranted || !symbol) {
      setLivePriceNow(null);
      setLivePriceTs(null);
      setLivePriceConnected(false);
      return;
    }

    if (platform === 'capital') {
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
            platform: 'capital',
            t: String(Date.now()),
          });
          const res = await fetch(`/api/swing/dashboard/live-price?${params.toString()}`, {
            headers: buildAdminHeaders(),
            cache: 'no-store',
            signal: inFlight.signal,
          });
          if (res.status === 401) {
            closed = true;
            clearPoll();
            setLivePriceConnected(false);
            handleAuthExpired('Admin session expired. Re-enter ADMIN_ACCESS_SECRET.');
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
          if (err?.name !== 'AbortError') {
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

    if (platform && platform !== 'bitget') {
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
              op: 'subscribe',
              args: [{ instType: 'USDT-FUTURES', channel: 'ticker', instId: symbol }],
            }),
          );
        } catch {}

        pingTimer = window.setInterval(() => {
          if (!ws || ws.readyState !== WebSocket.OPEN) return;
          try {
            ws.send('ping');
          } catch {}
        }, WS_PING_MS);
      };

      ws.onmessage = (event) => {
        if (closed) return;
        const raw = String(event.data ?? '');
        if (!raw || raw === 'pong' || raw === 'ping') return;
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
    if (!ts) return '';
    const d = new Date(ts);
    const now = new Date();
    const sameDay =
      d.getFullYear() === now.getFullYear() &&
      d.getMonth() === now.getMonth() &&
      d.getDate() === now.getDate();
    const time = d.toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit', timeZone: BERLIN_TZ });
    if (sameDay) return `– ${time}`;
    const date = d.toLocaleDateString('de-DE', { timeZone: BERLIN_TZ });
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
          (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
          (trimmed.startsWith('[') && trimmed.endsWith(']'));
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
  const activePlatform = current?.lastPlatform?.toLowerCase() === 'capital' ? 'capital' : 'bitget';
  const activePlatformLogo = activePlatform === 'capital' ? '/capital.svg' : '/bitget.svg';
  const dashboardRangeText = dashboardRange === '6M' ? '6m' : dashboardRange.toLowerCase();
  const liveOpenPnl =
    current &&
    typeof livePriceNow === 'number' &&
    Number.isFinite(livePriceNow) &&
    typeof current.openEntryPrice === 'number' &&
    Number.isFinite(current.openEntryPrice) &&
    current.openEntryPrice > 0 &&
    (current.openDirection === 'long' || current.openDirection === 'short')
      ? (((livePriceNow - current.openEntryPrice) / current.openEntryPrice) *
          (current.openDirection === 'long' ? 1 : -1) *
          (typeof current.openLeverage === 'number' && current.openLeverage > 0 ? current.openLeverage : 1) *
          100)
      : null;
  const effectiveOpenPnl =
    typeof liveOpenPnl === 'number'
      ? liveOpenPnl
      : current && typeof current.openPnl === 'number'
      ? current.openPnl
      : null;
  const effectivePnl7dWithOpen =
    current && typeof current.pnl7d === 'number' && typeof effectiveOpenPnl === 'number'
      ? current.pnl7d + effectiveOpenPnl
      : current && typeof current.pnl7d === 'number'
      ? current.pnl7d
      : typeof effectiveOpenPnl === 'number'
      ? effectiveOpenPnl
      : current && typeof current.pnl7dWithOpen === 'number'
      ? current.pnl7dWithOpen
      : null;
  const openPnlIsLive = typeof liveOpenPnl === 'number';
  const showChartPanel = Boolean(adminGranted && activeSymbol);
  const currentEvalJob = activeSymbol ? evaluateJobs[activeSymbol] : null;
  const evaluateRunning = Boolean(
    activeSymbol &&
      currentEvalJob &&
      (currentEvalJob.status === 'queued' || currentEvalJob.status === 'running'),
  );
  const hasLastDecision =
    !!(
      current &&
      ('lastDecision' in current ||
        'lastDecisionTs' in current ||
        'lastPrompt' in current ||
        'lastMetrics' in current ||
        'lastBiasTimeframes' in current)
    );
  const hasDetails =
    !!(
      current?.evaluation?.what_went_well?.length ||
      current?.evaluation?.issues?.length ||
      current?.evaluation?.improvements?.length
    );
  const biasOrder = [
    { key: 'context_bias', label: 'Context' },
    { key: 'macro_bias', label: 'Macro' },
    { key: 'primary_bias', label: 'Primary' },
    { key: 'micro_bias', label: 'Micro' },
  ] as const;
  const isInitialLoading = loading && !symbols.length;
  const loadingLabel = strategyMode === 'scalp'
    ? 'Loading scalp dashboard...'
    : !symbols.length
    ? 'Loading evaluations...'
    : activeSymbol
    ? `Loading ${activeSymbol}...`
    : 'Loading selected symbol...';
  const scalpRows = Array.isArray(scalpSummary?.symbols) ? scalpSummary.symbols : [];
  const scalpActiveRow =
    (scalpActiveDeploymentId ? scalpRows.find((row) => row.deploymentId === scalpActiveDeploymentId) : null) || scalpRows[0] || null;
  const scalpActiveJournal = Array.isArray(scalpSummary?.journal)
    ? scalpSummary.journal.filter((entry) => {
        if (!scalpActiveRow) return true;
        const payload = entry.payload && typeof entry.payload === 'object' ? entry.payload : {};
        const deploymentId = String((payload as Record<string, unknown>).deploymentId || '').trim();
        if (deploymentId) return deploymentId === scalpActiveRow.deploymentId;
        const strategyId = String((payload as Record<string, unknown>).strategyId || '').trim().toLowerCase();
        return (
          String(entry.symbol || '').trim().toUpperCase() === scalpActiveRow.symbol.toUpperCase() &&
          (!strategyId || strategyId === String(scalpActiveRow.strategyId || '').trim().toLowerCase())
        );
      })
    : [];
  const scalpLatestExecutionByDeploymentId =
    scalpSummary?.latestExecutionByDeploymentId && typeof scalpSummary.latestExecutionByDeploymentId === 'object'
      ? scalpSummary.latestExecutionByDeploymentId
      : {};
  const scalpLatestExecutionBySymbol =
    scalpSummary?.latestExecutionBySymbol && typeof scalpSummary.latestExecutionBySymbol === 'object'
      ? scalpSummary.latestExecutionBySymbol
      : {};
  const scalpActiveExecution = scalpActiveRow
    ? (scalpLatestExecutionByDeploymentId[scalpActiveRow.deploymentId] ??
        scalpLatestExecutionBySymbol[scalpActiveRow.symbol] ??
        scalpLatestExecutionBySymbol[scalpActiveRow.symbol.toUpperCase()] ??
        null)
    : null;
  const scalpActiveReasonCodesRaw = (Array.isArray(scalpActiveRow?.reasonCodes)
    ? scalpActiveRow?.reasonCodes
    : []) as string[];
  const scalpActiveReasonCodes = (() => {
    if (!scalpActiveReasonCodesRaw.length) return [];
    const nonGeneric = scalpActiveReasonCodesRaw.filter((code) => {
      const upper = String(code || '').trim().toUpperCase();
      return upper !== 'SCALP_PHASE3_EXECUTION' && upper !== 'NO_STATE_CHANGE';
    });
    return nonGeneric.length ? nonGeneric : scalpActiveReasonCodesRaw;
  })();
  const scalpReasonSnapshotState = !scalpActiveRow
    ? 'none'
    : scalpActiveReasonCodesRaw.length
      ? 'fresh'
      : 'none';
  const scalpTopStates = Object.entries(scalpSummary?.summary?.stateCounts ?? {})
    .sort((a, b) => b[1] - a[1])
    .slice(0, 4);
  const scalpActiveWinRatePct =
    scalpActiveRow && scalpActiveRow.tradesPlaced > 0
      ? (scalpActiveRow.wins / scalpActiveRow.tradesPlaced) * 100
      : null;
  const scalpActiveNetR =
    scalpActiveRow && typeof scalpActiveRow.netR === 'number' && Number.isFinite(scalpActiveRow.netR)
      ? scalpActiveRow.netR
      : null;
  const scalpActiveMaxDdR =
    scalpActiveRow && typeof scalpActiveRow.maxDrawdownR === 'number' && Number.isFinite(scalpActiveRow.maxDrawdownR)
      ? scalpActiveRow.maxDrawdownR
      : null;
  const asFiniteNumber = (value: unknown): number | null => {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  };
  const scalpReportDeployments = Array.isArray(scalpResearchReport?.deployments)
    ? scalpResearchReport.deployments
    : [];
  const scalpRuntimeByDeploymentId = new Map<string, ScalpDashboardSymbol>(
    scalpRows.map((row) => [row.deploymentId, row]),
  );
  const scalpOpsDeployments: ScalpOpsDeploymentRow[] =
    scalpReportDeployments.length > 0
      ? scalpReportDeployments.map((row) => {
          const runtime = scalpRuntimeByDeploymentId.get(row.deploymentId) || null;
          const runtimePromotionEligible = typeof runtime?.promotionEligible === 'boolean' ? runtime.promotionEligible : null;
          const mergedPromotionEligible =
            typeof row.promotionEligible === 'boolean'
              ? row.promotionEligible
              : runtimePromotionEligible !== null
                ? runtimePromotionEligible
                : false;
          return {
            deploymentId: row.deploymentId,
            symbol: row.symbol,
            strategyId: row.strategyId,
            tuneId: String(row.tuneId || runtime?.tuneId || runtime?.tune || 'default'),
            source: String(row.source || 'report'),
            enabled: row.enabled !== false,
            promotionEligible: mergedPromotionEligible,
            promotionReason: row.promotionReason || runtime?.promotionReason || null,
            forwardValidation: row.forwardValidation || runtime?.forwardValidation || null,
            perf30dTrades: asFiniteNumber(row.perf30d?.trades),
            perf30dExpectancyR: asFiniteNumber(row.perf30d?.expectancyR),
            perf30dNetR: asFiniteNumber(row.perf30d?.netR),
            perf30dMaxDrawdownR: asFiniteNumber(row.perf30d?.maxDrawdownR),
            runtime,
          };
        })
      : scalpRows.map((row) => ({
          deploymentId: row.deploymentId,
          symbol: row.symbol,
          strategyId: row.strategyId,
          tuneId: String(row.tuneId || row.tune || 'default'),
          source: 'runtime',
          enabled: true,
          promotionEligible: typeof row.promotionEligible === 'boolean' ? row.promotionEligible : false,
          promotionReason: row.promotionReason || null,
          forwardValidation: row.forwardValidation || null,
          perf30dTrades: asFiniteNumber(row.tradesPlaced),
          perf30dExpectancyR:
            row.tradesPlaced > 0 && typeof row.netR === 'number' && Number.isFinite(row.netR)
              ? row.netR / row.tradesPlaced
              : null,
          perf30dNetR: asFiniteNumber(row.netR),
          perf30dMaxDrawdownR: asFiniteNumber(row.maxDrawdownR),
          runtime: row,
        }));

  const scalpActiveOpsRow =
    (scalpActiveDeploymentId
      ? scalpOpsDeployments.find((row) => row.deploymentId === scalpActiveDeploymentId)
      : null) ||
    scalpOpsDeployments[0] ||
    null;
  const scalpEnabledDeploymentCount = scalpOpsDeployments.filter((row) => row.enabled).length;
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
    ? scalpForwardExpectancyRows.reduce((acc, row) => acc + row, 0) / scalpForwardExpectancyRows.length
    : null;
  const scalpMeanForwardProfitablePct = scalpForwardProfitablePctRows.length
    ? scalpForwardProfitablePctRows.reduce((acc, row) => acc + row, 0) / scalpForwardProfitablePctRows.length
    : null;
  const scalpPromotionSyncCycleId = String(scalpPromotionSyncSnapshot?.cycleId || '').trim() || null;
  const scalpPromotionSyncStatusRaw = String(scalpPromotionSyncSnapshot?.cycleStatus || '').trim().toUpperCase();
  const scalpPromotionSyncReason =
    String(scalpPromotionSyncSnapshot?.reason || '').trim() || null;
  const scalpPromotionSyncStatusTone: ScalpOpsCronDetailTone = scalpPromotionSyncReason
    ? 'warning'
    : scalpPromotionSyncStatusRaw.includes('FAILED')
      ? 'critical'
      : scalpPromotionSyncStatusRaw.includes('RUNNING') || scalpPromotionSyncStatusRaw.includes('PENDING')
        ? 'warning'
        : scalpPromotionSyncStatusRaw.includes('COMPLETE')
          ? 'positive'
          : scalpPromotionSyncStatusRaw
            ? 'neutral'
            : 'warning';
  const scalpPromotionSyncFetchedAtMs = asFiniteNumber(scalpPromotionSyncSnapshot?.fetchedAtMs);
  const scalpPromotionSyncConsidered = asFiniteNumber(
    scalpPromotionSyncSnapshot?.deploymentsConsidered,
  );
  const scalpPromotionSyncMatched = asFiniteNumber(scalpPromotionSyncSnapshot?.deploymentsMatched);
  const scalpPromotionSyncUpdated = asFiniteNumber(scalpPromotionSyncSnapshot?.deploymentsUpdated);
  const scalpMaterializationShortlisted = asFiniteNumber(
    scalpPromotionSyncSnapshot?.materialization?.shortlistedCandidates,
  );
  const scalpMaterializationMissing = asFiniteNumber(
    scalpPromotionSyncSnapshot?.materialization?.missingCandidates,
  );
  const scalpMaterializationCreated = asFiniteNumber(
    scalpPromotionSyncSnapshot?.materialization?.createdCandidates,
  );
  const scalpMaterializationSource =
    String(scalpPromotionSyncSnapshot?.materialization?.source || '').trim() || null;
  const scalpMaterializationTopKPerSymbol = asFiniteNumber(
    scalpPromotionSyncSnapshot?.materialization?.topKPerSymbol,
  );
  const scalpMaterializationRows = Array.isArray(scalpPromotionSyncSnapshot?.materialization?.rows)
    ? scalpPromotionSyncSnapshot.materialization.rows
        .map((row) => ({
          deploymentId: String(row?.deploymentId || '').trim(),
          symbol: String(row?.symbol || '').trim().toUpperCase(),
          strategyId: String(row?.strategyId || '').trim(),
          tuneId: String(row?.tuneId || '').trim() || 'default',
          source: String(row?.source || '').trim().toLowerCase() || null,
          exists: Boolean(row?.exists),
          created: Boolean(row?.created),
        }))
        .filter((row) => row.symbol && row.strategyId && row.deploymentId)
        .sort((a, b) => {
          if (a.exists !== b.exists) return a.exists ? -1 : 1;
          if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
          if (a.strategyId !== b.strategyId) return a.strategyId.localeCompare(b.strategyId);
          return a.tuneId.localeCompare(b.tuneId);
        })
    : [];
  const scalpMaterializationExistingCount = scalpMaterializationRows.reduce(
    (acc, row) => acc + (row.exists ? 1 : 0),
    0,
  );
  const scalpMaterializationPreviewRows = scalpMaterializationRows.slice(0, 40);
  const scalpMaterializationKeySet = new Set<string>(
    scalpMaterializationRows.map((row) => `${row.symbol}~${row.strategyId}~${row.tuneId}`),
  );
  const scalpPromotionSyncRowEntries: Array<[string, { syncReason: string | null; eligibleFromSync: boolean | null }]> = [];
  for (const row of Array.isArray(scalpPromotionSyncSnapshot?.rows) ? scalpPromotionSyncSnapshot.rows : []) {
    const symbol = String(row?.symbol || '').trim().toUpperCase();
    const strategyId = String(row?.strategyId || '').trim();
    const tuneId = String(row?.tuneId || '').trim() || 'default';
    if (!symbol || !strategyId) continue;
    const syncReason =
      String(row?.weeklyGateReason || '').trim() ||
      String(row?.nextGate?.reason || '').trim() ||
      String(row?.previousGate?.reason || '').trim() ||
      null;
    const eligibleFromSync =
      typeof row?.nextGate?.eligible === 'boolean'
        ? row.nextGate.eligible
        : typeof row?.previousGate?.eligible === 'boolean'
          ? row.previousGate.eligible
          : null;
    scalpPromotionSyncRowEntries.push([`${symbol}~${strategyId}~${tuneId}`, { syncReason, eligibleFromSync }]);
  }
  const scalpPromotionSyncRowMap = new Map<string, { syncReason: string | null; eligibleFromSync: boolean | null }>(
    scalpPromotionSyncRowEntries,
  );
  const scalpOpsByCandidateKey = new Map<string, ScalpOpsDeploymentRow>(
    scalpOpsDeployments.map((row) => [`${row.symbol}~${row.strategyId}~${row.tuneId}`, row] as const),
  );
  const scalpAvgAbsPairCorrelation = asFiniteNumber(scalpResearchReport?.summary?.avgAbsPairCorrelation);
  const scalpWorkerTasks = Array.isArray(scalpResearchCycle?.tasks) ? scalpResearchCycle.tasks : [];
  const scalpWorkerRunningStaleAfterMs =
    asFiniteNumber(scalpResearchCycle?.cycle?.params?.runningStaleAfterMs) ?? 20 * 60_000;
  const scalpWorkerNowMs = Date.now();
  const scalpWorkerTaskStatusTotals = scalpWorkerTasks.reduce(
    (acc, task) => {
      const status = String(task?.status || 'pending')
        .trim()
        .toLowerCase();
      acc.tasks += 1;
      if (status === 'completed') acc.completed += 1;
      else if (status === 'failed') acc.failed += 1;
      else if (status === 'running') {
        const startedAtMs = asFiniteNumber(task?.startedAtMs);
        const staleRunning =
          startedAtMs === null || scalpWorkerNowMs - startedAtMs >= scalpWorkerRunningStaleAfterMs;
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
    { tasks: 0, pending: 0, running: 0, completed: 0, failed: 0, runningStale: 0, runningMissingStartedAt: 0 },
  );
  const scalpWorkerTaskTotalsAvailable = scalpWorkerTaskStatusTotals.tasks > 0;
  const scalpSummaryTaskTotals = scalpResearchCycle?.summary?.totals || null;
  const scalpSummaryTaskCount = asFiniteNumber(scalpSummaryTaskTotals?.tasks);
  const scalpWorkerTaskCountReturned = asFiniteNumber(scalpResearchCycle?.taskCountReturned);
  const scalpWorkerTasksCoverAll =
    Boolean(scalpResearchCycle?.includeTasks) &&
    scalpWorkerTaskTotalsAvailable &&
    scalpWorkerTaskCountReturned !== null &&
    (scalpSummaryTaskCount !== null
      ? scalpWorkerTaskCountReturned >= scalpSummaryTaskCount
      : scalpWorkerTaskCountReturned >= scalpWorkerTaskStatusTotals.tasks);
  const scalpCycleTotalsSource: 'task_list_full' | 'summary_cache' | 'task_list_partial' | 'report_fallback' =
    scalpWorkerTasksCoverAll
      ? 'task_list_full'
      : scalpSummaryTaskCount !== null
        ? 'summary_cache'
        : scalpWorkerTaskTotalsAvailable
          ? 'task_list_partial'
          : 'report_fallback';
  const scalpCycleTasks = scalpWorkerTasksCoverAll
    ? scalpWorkerTaskStatusTotals.tasks
    : scalpSummaryTaskCount ??
      (scalpWorkerTaskTotalsAvailable
        ? scalpWorkerTaskStatusTotals.tasks
        : asFiniteNumber(scalpResearchReport?.cycle?.tasks));
  const scalpCyclePending = scalpWorkerTasksCoverAll
    ? scalpWorkerTaskStatusTotals.pending
    : asFiniteNumber(scalpSummaryTaskTotals?.pending) ??
      (scalpWorkerTaskTotalsAvailable ? scalpWorkerTaskStatusTotals.pending : null);
  const scalpCycleRunning = scalpWorkerTasksCoverAll
    ? scalpWorkerTaskStatusTotals.running
    : asFiniteNumber(scalpSummaryTaskTotals?.running) ??
      (scalpWorkerTaskTotalsAvailable ? scalpWorkerTaskStatusTotals.running : null);
  const scalpCycleCompleted = scalpWorkerTasksCoverAll
    ? scalpWorkerTaskStatusTotals.completed
    : asFiniteNumber(scalpSummaryTaskTotals?.completed) ??
      (scalpWorkerTaskTotalsAvailable
        ? scalpWorkerTaskStatusTotals.completed
        : asFiniteNumber(scalpResearchReport?.cycle?.completed));
  const scalpCycleFailed = scalpWorkerTasksCoverAll
    ? scalpWorkerTaskStatusTotals.failed
    : asFiniteNumber(scalpSummaryTaskTotals?.failed) ??
      (scalpWorkerTaskTotalsAvailable
        ? scalpWorkerTaskStatusTotals.failed
        : asFiniteNumber(scalpResearchReport?.cycle?.failed));
  const scalpCycleRunningStaleAsPending = scalpWorkerTasksCoverAll
    ? scalpWorkerTaskStatusTotals.runningStale + scalpWorkerTaskStatusTotals.runningMissingStartedAt
    : null;
  const scalpCycleProgressFromChosenTotals =
    scalpCycleTasks !== null &&
    scalpCycleTasks > 0 &&
    scalpCycleCompleted !== null &&
    scalpCycleFailed !== null
      ? ((scalpCycleCompleted + scalpCycleFailed) / scalpCycleTasks) * 100
      : null;
  const scalpCycleProgressPct = scalpWorkerTasksCoverAll
    ? scalpCycleProgressFromChosenTotals ??
      asFiniteNumber(scalpResearchCycle?.summary?.progressPct) ??
      asFiniteNumber(scalpResearchReport?.cycle?.progressPct)
    : asFiniteNumber(scalpResearchCycle?.summary?.progressPct) ??
      (scalpWorkerTaskTotalsAvailable
        ? ((scalpWorkerTaskStatusTotals.completed + scalpWorkerTaskStatusTotals.failed) /
            scalpWorkerTaskStatusTotals.tasks) *
          100
        : asFiniteNumber(scalpResearchReport?.cycle?.progressPct));
  const scalpUniverseSelectedCount =
    asFiniteNumber(scalpResearchUniverse?.selectedCount) ??
    asFiniteNumber(scalpResearchUniverse?.snapshot?.selectedSymbols?.length);
  const scalpUniverseCandidatesEvaluated =
    asFiniteNumber(scalpResearchUniverse?.candidatesEvaluated) ??
    asFiniteNumber(scalpResearchUniverse?.snapshot?.candidatesEvaluated);

  const scalpJournalRows = Array.isArray(scalpSummary?.journal) ? scalpSummary.journal : [];
  const formatScalpTime = (ts?: number | null) => {
    const raw = formatDecisionTime(ts);
    return raw ? raw.replace(/^–\s*/, '') : '—';
  };
  const formatScalpDuration = (durationMs?: number | null): string => {
    const ms = typeof durationMs === 'number' && Number.isFinite(durationMs) ? Math.max(0, Math.floor(durationMs)) : null;
    if (ms === null) return '—';
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60_000) return `${(ms / 1000).toFixed(ms >= 10_000 ? 1 : 2)}s`;
    if (ms < 3_600_000) return `${(ms / 60_000).toFixed(ms >= 600_000 ? 1 : 2)}m`;
    return `${(ms / 3_600_000).toFixed(2)}h`;
  };
  const formatScalpCount = (value: number | null): string =>
    value === null ? '—' : `${Math.max(0, Math.floor(value))}`;
  const formatScalpPct = (value: number | null, digits = 0): string =>
    value === null ? '—' : `${value.toFixed(digits)}%`;
  const formatScalpSignedR = (value: number | null): string =>
    value === null ? '—' : `${value >= 0 ? '+' : ''}${value.toFixed(2)}R`;
  const scalpCycleId = String(
    scalpResearchCycle?.cycleId || scalpResearchCycle?.cycle?.cycleId || scalpResearchReport?.cycle?.cycleId || '',
  ).trim();
  const scalpCycleStatusRaw = String(
    scalpResearchCycle?.summary?.status ||
      scalpResearchCycle?.cycle?.status ||
      scalpResearchReport?.cycle?.status ||
      'unknown',
  )
    .trim()
    .toUpperCase();
  const scalpCycleStatusTone: ScalpOpsCronDetailTone = scalpCycleStatusRaw.includes('FAILED')
    ? 'critical'
    : scalpCycleStatusRaw.includes('RUNNING') || scalpCycleStatusRaw.includes('PENDING')
      ? 'warning'
      : scalpCycleStatusRaw.includes('COMPLETE')
        ? 'positive'
        : 'neutral';
  const scalpWorkerCycleSource = String(scalpResearchCycle?.cycleSource || '').trim() || 'none';
  const scalpWorkerRetryCycleReady =
    Boolean(scalpCycleId) &&
    scalpCycleStatusRaw.includes('RUNNING') &&
    (scalpWorkerCycleSource === 'active' || scalpWorkerCycleSource === 'requested');
  const compareScalpWorkerOptionalNumber = (
    a: number | null | undefined,
    b: number | null | undefined,
  ): number => {
    const av = typeof a === 'number' && Number.isFinite(a) ? a : null;
    const bv = typeof b === 'number' && Number.isFinite(b) ? b : null;
    if (av === null && bv === null) return 0;
    if (av === null) return 1;
    if (bv === null) return -1;
    return av - bv;
  };
  const compareScalpWorkerText = (a: string, b: string): number =>
    String(a || '').localeCompare(String(b || ''), undefined, {
      numeric: true,
      sensitivity: 'base',
    });
  const scalpWorkerTaskRowsRaw = scalpWorkerTasks
    .map((task) => {
      const symbol = String(task?.symbol || '').trim().toUpperCase();
      const strategyId = String(task?.strategyId || '').trim();
      const tuneId = String(task?.tuneId || task?.result?.tuneId || 'default').trim();
      const status = String(task?.status || 'pending')
        .trim()
        .toLowerCase();
      const fromTs =
        asFiniteNumber(task?.windowFromTs) ??
        asFiniteNumber(task?.result?.windowFromTs);
      const toTs =
        asFiniteNumber(task?.windowToTs) ??
        asFiniteNumber(task?.result?.windowToTs);
      const candidateKey = `${symbol}~${strategyId}~${tuneId}`;
      const deploymentRow = scalpOpsByCandidateKey.get(candidateKey) || null;
      const syncRow = scalpPromotionSyncRowMap.get(candidateKey) || null;
      const inShortlist = scalpMaterializationKeySet.has(candidateKey);
      const whyNotPromoted =
        deploymentRow?.promotionEligible
          ? 'eligible'
          : deploymentRow?.promotionReason || syncRow?.syncReason || (inShortlist ? 'shortlisted_pending_gate' : 'not_in_shortlist');
      return {
        taskId: String(task?.taskId || '').trim(),
        symbol,
        strategyId,
        tuneId,
        whyNotPromoted,
        status,
        windowFromTs: fromTs,
        windowToTs: toTs,
        trades: asFiniteNumber(task?.result?.trades),
        netR: asFiniteNumber(task?.result?.netR),
        expectancyR: asFiniteNumber(task?.result?.expectancyR),
        profitFactor: asFiniteNumber(task?.result?.profitFactor),
        maxDrawdownR: asFiniteNumber(task?.result?.maxDrawdownR),
        errorCode: String(task?.errorCode || '').trim() || null,
      };
    })
    .filter((row) => row.symbol && row.strategyId);
  const scalpWorkerTaskRows = scalpWorkerTaskRowsRaw.slice().sort((a, b) => {
    let cmp = 0;
    switch (scalpWorkerSort.key) {
      case 'symbol':
        cmp = compareScalpWorkerText(a.symbol, b.symbol);
        break;
      case 'strategyId':
        cmp = compareScalpWorkerText(a.strategyId, b.strategyId);
        break;
      case 'tuneId':
        cmp = compareScalpWorkerText(a.tuneId, b.tuneId);
        break;
      case 'whyNotPromoted':
        cmp = compareScalpWorkerText(a.whyNotPromoted, b.whyNotPromoted);
        break;
      case 'windowToTs':
        cmp = compareScalpWorkerOptionalNumber(a.windowToTs, b.windowToTs);
        break;
      case 'status':
        cmp = compareScalpWorkerText(a.status, b.status);
        break;
      case 'trades':
        cmp = compareScalpWorkerOptionalNumber(a.trades, b.trades);
        break;
      case 'netR':
        cmp = compareScalpWorkerOptionalNumber(a.netR, b.netR);
        break;
      case 'expectancyR':
        cmp = compareScalpWorkerOptionalNumber(a.expectancyR, b.expectancyR);
        break;
      case 'profitFactor':
        cmp = compareScalpWorkerOptionalNumber(a.profitFactor, b.profitFactor);
        break;
      case 'maxDrawdownR':
        cmp = compareScalpWorkerOptionalNumber(a.maxDrawdownR, b.maxDrawdownR);
        break;
      default:
        cmp = 0;
        break;
    }
    if (cmp !== 0) {
      return scalpWorkerSort.direction === 'asc' ? cmp : -cmp;
    }
    const aTo = a.windowToTs ?? 0;
    const bTo = b.windowToTs ?? 0;
    if (bTo !== aTo) return bTo - aTo;
    if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
    if (a.strategyId !== b.strategyId) return a.strategyId.localeCompare(b.strategyId);
    return a.tuneId.localeCompare(b.tuneId);
  });
  const scalpWorkerLastDurationFromTasksMs = (() => {
    const completedTaskRuns = scalpWorkerTasks
      .map((task) => {
        const workerId = String(task?.workerId || '').trim();
        const startedAtMs = asFiniteNumber(task?.startedAtMs);
        const finishedAtMs = asFiniteNumber(task?.finishedAtMs);
        if (!workerId || startedAtMs === null || finishedAtMs === null || finishedAtMs < startedAtMs) {
          return null;
        }
        return { workerId, startedAtMs, finishedAtMs };
      })
      .filter((row): row is { workerId: string; startedAtMs: number; finishedAtMs: number } => row !== null);
    if (!completedTaskRuns.length) return null;

    const latestTask = completedTaskRuns.slice().sort((a, b) => b.finishedAtMs - a.finishedAtMs)[0] || null;
    if (!latestTask) return null;

    const sameRunTasks = completedTaskRuns.filter((row) => row.workerId === latestTask.workerId);
    if (!sameRunTasks.length) return null;

    const runStartedAtMs = Math.min(...sameRunTasks.map((row) => row.startedAtMs));
    const runFinishedAtMs = Math.max(...sameRunTasks.map((row) => row.finishedAtMs));
    if (!Number.isFinite(runStartedAtMs) || !Number.isFinite(runFinishedAtMs) || runFinishedAtMs < runStartedAtMs) {
      return null;
    }
    return runFinishedAtMs - runStartedAtMs;
  })();
  const formatScalpWindowIso = (fromTs: number | null, toTs: number | null): string => {
    if (fromTs === null || toTs === null) return '—';
    const fromIso = new Date(fromTs).toISOString().slice(0, 10);
    const toIso = new Date(toTs).toISOString().slice(0, 10);
    return `${fromIso} → ${toIso}`;
  };
  const normalizeScalpUniverseSymbol = (value: unknown): string =>
    String(value || '')
      .trim()
      .toUpperCase();
  const scalpUniverseSymbols = Array.isArray(scalpResearchUniverse?.snapshot?.selectedSymbols)
    ? scalpResearchUniverse.snapshot.selectedSymbols
        .map((symbol) => normalizeScalpUniverseSymbol(symbol))
        .filter(Boolean)
    : [];
  const scalpUniverseSelectedRows = Array.isArray(scalpResearchUniverse?.snapshot?.selectedRows)
    ? scalpResearchUniverse.snapshot.selectedRows
    : [];
  const scalpUniverseRejectedRows = Array.isArray(scalpResearchUniverse?.snapshot?.topRejectedRows)
    ? scalpResearchUniverse.snapshot.topRejectedRows
    : [];
  const scalpUniverseSeedSummary = scalpResearchUniverse?.snapshot?.seedSummary ?? null;
  const scalpUniverseSeedResults = Array.isArray(scalpUniverseSeedSummary?.results)
    ? scalpUniverseSeedSummary.results
    : [];
  const scalpUniverseSeededSymbols = scalpUniverseSeedResults
    .filter((row) => String(row?.status || '').trim().toLowerCase() === 'seeded')
    .map((row) => normalizeScalpUniverseSymbol(row?.symbol))
    .filter(Boolean);
  const scalpUniverseSeededCount =
    asFiniteNumber(scalpUniverseSeedSummary?.seededSymbols) ?? scalpUniverseSeededSymbols.length;
  const scalpUniverseSeedProcessedCount =
    asFiniteNumber(scalpUniverseSeedSummary?.processedSymbols) ?? scalpUniverseSeedResults.length;
  const scalpUniverseGeneratedAtMs = (() => {
    const iso = String(
      scalpResearchUniverse?.generatedAtIso || scalpResearchUniverse?.snapshot?.generatedAtIso || '',
    ).trim();
    if (!iso) return null;
    const parsed = Date.parse(iso);
    return Number.isFinite(parsed) ? parsed : null;
  })();
  const scalpUniverseEvaluationMap = new Map<string, ScalpResearchUniverseCandidateRow>();
  for (const row of scalpUniverseSelectedRows) {
    const symbol = normalizeScalpUniverseSymbol(row?.symbol);
    if (!symbol || scalpUniverseEvaluationMap.has(symbol)) continue;
    scalpUniverseEvaluationMap.set(symbol, row);
  }
  for (const row of scalpUniverseRejectedRows) {
    const symbol = normalizeScalpUniverseSymbol(row?.symbol);
    if (!symbol || scalpUniverseEvaluationMap.has(symbol)) continue;
    scalpUniverseEvaluationMap.set(symbol, row);
  }
  const scalpUniverseEvaluatedRows = Array.from(scalpUniverseEvaluationMap.entries()).map(([symbol, row]) => ({
    symbol,
    eligible: Boolean(row?.eligible),
    score: asFiniteNumber(row?.score),
    reasons: Array.isArray(row?.reasons) ? row.reasons.map((reason) => String(reason || '')).filter(Boolean) : [],
    recommendedStrategyIds: Array.isArray(row?.recommendedStrategyIds)
      ? row.recommendedStrategyIds.map((strategyId) => String(strategyId || '')).filter(Boolean)
      : [],
  }));
  const scalpUniverseEvaluatedCount =
    asFiniteNumber(scalpResearchUniverse?.candidatesEvaluated) ??
    asFiniteNumber(scalpResearchUniverse?.snapshot?.candidatesEvaluated) ??
    scalpUniverseEvaluatedRows.length;
  const scalpUniverseEvaluatedSymbols = scalpUniverseEvaluatedRows
    .map((row) => row.symbol)
    .filter(Boolean);
  const scalpUniverseEligibleEvaluatedCount = scalpUniverseEvaluatedRows.reduce(
    (acc, row) => acc + (row.eligible ? 1 : 0),
    0,
  );
  const scalpUniversePipelineMap = new Map<string, ScalpUniversePipelineRow>();
  const ensurePipelineRow = (symbol: string): ScalpUniversePipelineRow => {
    const existing = scalpUniversePipelineMap.get(symbol);
    if (existing) return existing;
    const created: ScalpUniversePipelineRow = {
      symbol,
      discovered: false,
      importStatus: 'not_run',
      importReason: null,
      importAddedCount: null,
      evaluated: false,
      eligible: null,
      score: null,
      reasons: [],
    };
    scalpUniversePipelineMap.set(symbol, created);
    return created;
  };
  for (const symbol of scalpUniverseSymbols) {
    if (!symbol) continue;
    const row = ensurePipelineRow(symbol);
    row.discovered = true;
  }
  for (const result of scalpUniverseSeedResults) {
    const symbol = normalizeScalpUniverseSymbol(result?.symbol);
    if (!symbol) continue;
    const row = ensurePipelineRow(symbol);
    const status = String(result?.status || '')
      .trim()
      .toLowerCase();
    if (status === 'seeded' || status === 'skipped' || status === 'failed') {
      row.importStatus = status;
    }
    row.importReason = typeof result?.reason === 'string' && result.reason.trim() ? result.reason.trim() : null;
    row.importAddedCount = asFiniteNumber(result?.addedCount);
  }
  for (const evaluation of scalpUniverseEvaluatedRows) {
    if (!evaluation.symbol) continue;
    const row = ensurePipelineRow(evaluation.symbol);
    row.evaluated = true;
    row.eligible = evaluation.eligible;
    row.score = evaluation.score;
    row.reasons = evaluation.reasons;
  }
  const scalpUniversePipelineRows = Array.from(scalpUniversePipelineMap.values()).sort((a, b) => {
    if (a.discovered !== b.discovered) return a.discovered ? -1 : 1;
    const importRank = (status: ScalpUniversePipelineRow['importStatus']): number => {
      if (status === 'seeded') return 0;
      if (status === 'skipped') return 1;
      if (status === 'failed') return 2;
      return 3;
    };
    const importDelta = importRank(a.importStatus) - importRank(b.importStatus);
    if (importDelta !== 0) return importDelta;
    if (a.evaluated !== b.evaluated) return a.evaluated ? -1 : 1;
    return a.symbol.localeCompare(b.symbol);
  });
  const scalpUniverseDiscoveredPreview = scalpUniverseSymbols.slice(0, 8);
  const scalpUniverseImportedPreview = scalpUniverseSeededSymbols.slice(0, 8);
  const scalpUniverseEvaluatedPreview = scalpUniverseEvaluatedSymbols.slice(0, 8);
  const scalpHistorySnapshot = scalpSummary?.history || null;
  const scalpHistoryTimeframe = String(scalpHistorySnapshot?.timeframe || '').trim() || '1m';
  const scalpHistoryBackend = String(scalpHistorySnapshot?.backend || '').trim() || 'unknown';
  const scalpHistorySymbolCount = asFiniteNumber(scalpHistorySnapshot?.symbolCount);
  const scalpHistoryScannedCount = asFiniteNumber(scalpHistorySnapshot?.scannedCount);
  const scalpHistoryScannedLimit = asFiniteNumber(scalpHistorySnapshot?.scannedLimit);
  const scalpHistoryNonEmptyCount = asFiniteNumber(scalpHistorySnapshot?.nonEmptyCount);
  const scalpHistoryTotalCandles = asFiniteNumber(scalpHistorySnapshot?.totalCandles);
  const scalpHistoryMedianCandles = asFiniteNumber(scalpHistorySnapshot?.medianCandles);
  const scalpHistoryAvgCandles = asFiniteNumber(scalpHistorySnapshot?.avgCandles);
  const scalpHistoryMedianDepthDays = asFiniteNumber(scalpHistorySnapshot?.medianDepthDays);
  const scalpHistoryAvgDepthDays = asFiniteNumber(scalpHistorySnapshot?.avgDepthDays);
  const scalpHistoryNewestCandleAtMs = asFiniteNumber(scalpHistorySnapshot?.newestCandleAtMs);
  const scalpHistoryOldestCandleAtMs = asFiniteNumber(scalpHistorySnapshot?.oldestCandleAtMs);
  const scalpHistoryTruncated = Boolean(scalpHistorySnapshot?.truncated);
  const scalpHistoryRows = (Array.isArray(scalpHistorySnapshot?.rows) ? scalpHistorySnapshot.rows : [])
    .map((row) => {
      const symbol = String(row?.symbol || '')
        .trim()
        .toUpperCase();
      const candlesRaw = asFiniteNumber(row?.candles);
      const candles = candlesRaw === null ? null : Math.max(0, Math.floor(candlesRaw));
      const depthDays = asFiniteNumber(row?.depthDays);
      const barsPerDay = asFiniteNumber(row?.barsPerDay);
      const coveragePct = asFiniteNumber(row?.coveragePct);
      const fromTsMs = asFiniteNumber(row?.fromTsMs);
      const toTsMs = asFiniteNumber(row?.toTsMs);
      const updatedAtMs = asFiniteNumber(row?.updatedAtMs);
      return {
        symbol,
        candles,
        depthDays,
        barsPerDay,
        coveragePct,
        fromTsMs,
        toTsMs,
        updatedAtMs,
      };
    })
    .filter((row) => Boolean(row.symbol));
  const scalpHistoryPreviewRows = scalpHistoryRows.slice(0, 12);
  const isScalpGuardrailJournalEntry = (entry: ScalpJournalRow): boolean => {
    const type = String(entry.type || '').trim().toUpperCase();
    if (type === 'RISK') return true;
    if (!Array.isArray(entry.reasonCodes)) return false;
    return entry.reasonCodes.some((code) => {
      const normalized = String(code || '').trim().toUpperCase();
      return (
        normalized.includes('GUARDRAIL') ||
        normalized.includes('BREACH') ||
        normalized.includes('PAUSE')
      );
    });
  };
  const isScalpExecutionJournalEntry = (entry: ScalpJournalRow): boolean => {
    const type = String(entry.type || '').trim().toUpperCase();
    if (type === 'EXECUTION') return true;
    if (!Array.isArray(entry.reasonCodes)) return false;
    return entry.reasonCodes.some((code) =>
      String(code || '').trim().toUpperCase().includes('SCALP_PHASE3_EXECUTION'),
    );
  };
  const scalpLatestJournalEntryBy = (
    predicate: (entry: ScalpJournalRow) => boolean,
  ): ScalpJournalRow | null =>
    scalpJournalRows.reduce<ScalpJournalRow | null>((acc, entry) => {
      if (!predicate(entry)) return acc;
      const timestampMs =
        typeof entry.timestampMs === 'number' && Number.isFinite(entry.timestampMs)
          ? entry.timestampMs
          : null;
      if (timestampMs === null) return acc;
      if (!acc) return entry;
      const accTs =
        typeof acc.timestampMs === 'number' && Number.isFinite(acc.timestampMs)
          ? acc.timestampMs
          : null;
      if (accTs === null || timestampMs > accTs) return entry;
      return acc;
    }, null);
  const scalpLastExecuteRunAtMs = scalpRows.reduce<number | null>((acc, row) => {
    if (typeof row.lastRunAtMs !== 'number' || !Number.isFinite(row.lastRunAtMs)) return acc;
    if (acc === null) return row.lastRunAtMs;
    return Math.max(acc, row.lastRunAtMs);
  }, null);
  const scalpLatestExecutionJournalEntry = scalpLatestJournalEntryBy(isScalpExecutionJournalEntry);
  const scalpLatestGuardrailEntry = scalpLatestJournalEntryBy(isScalpGuardrailJournalEntry);
  const scalpLastGuardrailAtMs = asFiniteNumber(scalpLatestGuardrailEntry?.timestampMs);
  const scalpWorkerHeartbeatUpdatedAtMs =
    asFiniteNumber(scalpResearchCycle?.workerHeartbeat?.updatedAtMs) ??
    asFiniteNumber(scalpResearchCycle?.workerHeartbeat?.finishedAtMs) ??
    asFiniteNumber(scalpResearchCycle?.workerHeartbeat?.startedAtMs);
  const scalpWorkerHeartbeatDurationMs = asFiniteNumber(scalpResearchCycle?.workerHeartbeat?.durationMs);
  const scalpWorkerLastDurationMs = scalpWorkerHeartbeatDurationMs ?? scalpWorkerLastDurationFromTasksMs;
  const scalpCycleUpdatedAtMs =
    asFiniteNumber(scalpResearchCycle?.cycle?.updatedAtMs) ??
    asFiniteNumber(scalpResearchCycle?.summary?.generatedAtMs) ??
    asFiniteNumber(scalpResearchReport?.generatedAtMs);
  const scalpWorkerLastRunAtMs = scalpWorkerHeartbeatUpdatedAtMs ?? scalpCycleUpdatedAtMs;
  const scalpReportGeneratedAtMs = asFiniteNumber(scalpResearchReport?.generatedAtMs);
  const scalpDashboardGeneratedAtMs = asFiniteNumber(scalpSummary?.generatedAtMs);
  const scalpSummaryRunCount = asFiniteNumber(scalpSummary?.summary?.runCount);
  const scalpSummaryDryRunCount = asFiniteNumber(scalpSummary?.summary?.dryRunCount);
  const scalpSummaryOpenCount = asFiniteNumber(scalpSummary?.summary?.openCount);
  const scalpSummaryTradesPlaced = asFiniteNumber(scalpSummary?.summary?.totalTradesPlaced);
  const scalpLatestExecutionPayload =
    scalpLatestExecutionJournalEntry?.payload &&
    typeof scalpLatestExecutionJournalEntry.payload === 'object'
      ? (scalpLatestExecutionJournalEntry.payload as Record<string, unknown>)
      : null;
  const scalpLatestGuardrailPayload =
    scalpLatestGuardrailEntry?.payload && typeof scalpLatestGuardrailEntry.payload === 'object'
      ? (scalpLatestGuardrailEntry.payload as Record<string, unknown>)
      : null;
  const scalpLatestExecutionDeploymentId = String(
    scalpLatestExecutionPayload?.deploymentId || '',
  ).trim();
  const scalpLatestExecutionSymbol = String(scalpLatestExecutionPayload?.symbol || '').trim().toUpperCase();
  const scalpLatestGuardrailDeploymentId = String(
    scalpLatestGuardrailPayload?.deploymentId || '',
  ).trim();
  const scalpLatestGuardrailSymbol = String(scalpLatestGuardrailEntry?.symbol || '')
    .trim()
    .toUpperCase();
  const scalpIneligibleCount = Math.max(0, scalpEnabledDeploymentCount - scalpPromotionEligibleCount);
  const toScalpRatioPct = (numerator: number | null, denominator: number | null): number | null => {
    if (numerator === null || denominator === null || denominator <= 0) return null;
    const raw = (numerator / denominator) * 100;
    if (!Number.isFinite(raw)) return null;
    return Math.max(0, Math.min(100, raw));
  };
  const scalpDiscoverySelectionPct = toScalpRatioPct(
    scalpUniverseSelectedCount,
    scalpUniverseCandidatesEvaluated ?? scalpUniverseEvaluatedCount,
  );
  const scalpImportCoveragePct = toScalpRatioPct(scalpUniverseSeededCount, scalpUniverseSelectedCount);
  const scalpEvaluationCoveragePct = toScalpRatioPct(
    scalpUniverseEvaluatedCount,
    scalpUniverseCandidatesEvaluated ?? scalpUniverseEvaluatedCount,
  );
  const scalpEligibleSharePct = toScalpRatioPct(scalpUniverseEligibleEvaluatedCount, scalpUniverseEvaluatedCount);
  const scalpHistoryCoveragePct = toScalpRatioPct(
    scalpHistoryNonEmptyCount,
    scalpHistoryScannedCount ?? scalpHistorySymbolCount,
  );
  const scalpCycleCompletedPct = toScalpRatioPct(scalpCycleCompleted, scalpCycleTasks);
  const scalpCycleFailedPct = toScalpRatioPct(scalpCycleFailed, scalpCycleTasks);
  const scalpCycleQueuePct = toScalpRatioPct(
    (scalpCyclePending ?? 0) + (scalpCycleRunning ?? 0),
    scalpCycleTasks,
  );
  const scalpGateEligiblePct = toScalpRatioPct(scalpPromotionEligibleCount, scalpEnabledDeploymentCount);
  const scalpGateBlockedPct = toScalpRatioPct(scalpIneligibleCount, scalpEnabledDeploymentCount);
  const scalpMaterializationCoveragePct = toScalpRatioPct(
    scalpMaterializationExistingCount,
    scalpMaterializationShortlisted,
  );
  const scalpMaterializationCreatedFromMissingPct = toScalpRatioPct(
    scalpMaterializationCreated,
    scalpMaterializationMissing,
  );
  const scalpStatusFromTs = (ts: number | null, staleMs: number): ScalpOpsCronStatus => {
    if (typeof ts !== 'number' || !Number.isFinite(ts)) return 'unknown';
    return Date.now() - ts <= staleMs ? 'healthy' : 'lagging';
  };
  const scalpCronRuntimeById = buildScalpCronRuntimeMap(scalpCronNowMs);
  const scalpCronRuntimeMeta = (id: string): ScalpCronRuntimeMeta =>
    scalpCronRuntimeById[id] || {
      expressions: [],
      expressionLabel: null,
      nextRunAtMs: null,
      invokePath: null,
    };
  const scalpCronRows: ScalpOpsCronRow[] = [
    {
      id: 'scalp_discover_symbols',
      cadence: 'Daily + intraweek',
      cronExpression: scalpCronRuntimeMeta('scalp_discover_symbols').expressionLabel,
      nextRunAtMs: scalpCronRuntimeMeta('scalp_discover_symbols').nextRunAtMs,
      invokePath: scalpCronRuntimeMeta('scalp_discover_symbols').invokePath,
      role: 'Discover symbols and seed candle history',
      status: scalpStatusFromTs(scalpUniverseGeneratedAtMs, 36 * 60 * 60_000),
      lastRunAtMs: scalpUniverseGeneratedAtMs,
      lastDurationMs: null,
      details: [
        {
          label: 'Cron',
          value: scalpCronRuntimeMeta('scalp_discover_symbols').expressionLabel || 'not configured',
          tone: scalpCronRuntimeMeta('scalp_discover_symbols').expressionLabel ? 'neutral' : 'warning',
        },
        {
          label: 'Next Run',
          value: formatScalpNextRunIn(scalpCronRuntimeMeta('scalp_discover_symbols').nextRunAtMs, scalpCronNowMs),
          tone: 'neutral',
        },
        {
          label: 'Snapshot',
          value: formatScalpTime(scalpUniverseGeneratedAtMs),
          tone: scalpUniverseGeneratedAtMs ? 'neutral' : 'warning',
        },
        { label: 'Selected', value: formatScalpCount(scalpUniverseSelectedCount), tone: 'neutral' },
        { label: 'Candidates', value: formatScalpCount(scalpUniverseCandidatesEvaluated), tone: 'neutral' },
        { label: 'Imported', value: formatScalpCount(scalpUniverseSeededCount), tone: 'neutral' },
        {
          label: 'History',
          value: `${formatScalpCount(scalpHistoryNonEmptyCount)} / ${formatScalpCount(
            scalpHistoryScannedCount ?? scalpHistorySymbolCount,
          )}`,
          tone: scalpHistoryCoveragePct !== null && scalpHistoryCoveragePct >= 80 ? 'positive' : 'warning',
        },
        {
          label: 'Median Depth',
          value: scalpHistoryMedianDepthDays === null ? '—' : `${scalpHistoryMedianDepthDays.toFixed(1)}d`,
          tone:
            scalpHistoryMedianDepthDays === null
              ? 'neutral'
              : scalpHistoryMedianDepthDays >= 90
                ? 'positive'
                : 'warning',
        },
      ],
      visualMetrics: [
        {
          label: 'Selection Rate',
          valueLabel:
            scalpDiscoverySelectionPct === null
              ? '—'
              : `${scalpDiscoverySelectionPct.toFixed(1)}%`,
          pct: scalpDiscoverySelectionPct,
          tone: 'neutral',
        },
        {
          label: 'Import Coverage',
          valueLabel:
            scalpImportCoveragePct === null
              ? '—'
              : `${scalpImportCoveragePct.toFixed(1)}%`,
          pct: scalpImportCoveragePct,
          tone: scalpImportCoveragePct !== null && scalpImportCoveragePct >= 75 ? 'positive' : 'warning',
        },
        {
          label: 'History Coverage',
          valueLabel:
            scalpHistoryCoveragePct === null
              ? '—'
              : `${scalpHistoryCoveragePct.toFixed(1)}%`,
          pct: scalpHistoryCoveragePct,
          tone: scalpHistoryCoveragePct !== null && scalpHistoryCoveragePct >= 80 ? 'positive' : 'warning',
        },
      ],
      resultPreview: {
        generatedAtIso: scalpResearchUniverse?.generatedAtIso || scalpResearchUniverse?.snapshot?.generatedAtIso || null,
        selectedSymbolsPreview: scalpUniverseSymbols.slice(0, 10),
        selectedSymbolsRemaining: Math.max(0, scalpUniverseSymbols.length - 10),
        candidatesEvaluated: scalpUniverseCandidatesEvaluated ?? scalpUniverseEvaluatedCount,
        seedSummary: scalpUniverseSeedSummary || null,
        history: {
          timeframe: scalpHistoryTimeframe,
          backend: scalpHistoryBackend,
          symbolCount: scalpHistorySymbolCount,
          scannedCount: scalpHistoryScannedCount,
          nonEmptyCount: scalpHistoryNonEmptyCount,
          totalCandles: scalpHistoryTotalCandles,
          medianCandles: scalpHistoryMedianCandles,
          medianDepthDays: scalpHistoryMedianDepthDays,
          oldestCandleAtMs: scalpHistoryOldestCandleAtMs,
          newestCandleAtMs: scalpHistoryNewestCandleAtMs,
          rowsPreview: scalpHistoryPreviewRows,
          rowsRemaining: Math.max(0, scalpHistoryRows.length - scalpHistoryPreviewRows.length),
        },
      },
    },
    {
      id: 'scalp_cycle_start',
      cadence: 'Twice daily',
      cronExpression: scalpCronRuntimeMeta('scalp_cycle_start').expressionLabel,
      nextRunAtMs: scalpCronRuntimeMeta('scalp_cycle_start').nextRunAtMs,
      invokePath: scalpCronRuntimeMeta('scalp_cycle_start').invokePath,
      role: 'Freeze universe and emit task manifest',
      status: scalpStatusFromTs(scalpCycleUpdatedAtMs, 36 * 60 * 60_000),
      lastRunAtMs: scalpCycleUpdatedAtMs,
      lastDurationMs: null,
      details: [
        {
          label: 'Cron',
          value: scalpCronRuntimeMeta('scalp_cycle_start').expressionLabel || 'not configured',
          tone: scalpCronRuntimeMeta('scalp_cycle_start').expressionLabel ? 'neutral' : 'warning',
        },
        {
          label: 'Next Run',
          value: formatScalpNextRunIn(scalpCronRuntimeMeta('scalp_cycle_start').nextRunAtMs, scalpCronNowMs),
          tone: 'neutral',
        },
        { label: 'Cycle', value: scalpCycleId || 'pending', tone: scalpCycleId ? 'neutral' : 'warning' },
        { label: 'Status', value: scalpCycleStatusRaw.replace(/_/g, ' '), tone: scalpCycleStatusTone },
        { label: 'Discovered', value: formatScalpCount(scalpUniverseSelectedCount), tone: 'neutral' },
        { label: 'Imported', value: formatScalpCount(scalpUniverseSeededCount), tone: 'neutral' },
        { label: 'Evaluated', value: formatScalpCount(scalpUniverseEvaluatedCount), tone: 'neutral' },
        { label: 'Eligible', value: formatScalpCount(scalpUniverseEligibleEvaluatedCount), tone: 'positive' },
      ],
      visualMetrics: [
        {
          label: 'Selection Rate',
          valueLabel:
            scalpDiscoverySelectionPct === null
              ? '—'
              : `${scalpDiscoverySelectionPct.toFixed(1)}%`,
          pct: scalpDiscoverySelectionPct,
          tone: 'neutral',
        },
        {
          label: 'Import Coverage',
          valueLabel:
            scalpImportCoveragePct === null
              ? '—'
              : `${scalpImportCoveragePct.toFixed(1)}%`,
          pct: scalpImportCoveragePct,
          tone: scalpImportCoveragePct !== null && scalpImportCoveragePct >= 75 ? 'positive' : 'warning',
        },
        {
          label: 'Evaluation Coverage',
          valueLabel:
            scalpEvaluationCoveragePct === null
              ? '—'
              : `${scalpEvaluationCoveragePct.toFixed(1)}%`,
          pct: scalpEvaluationCoveragePct,
          tone: scalpEvaluationCoveragePct !== null && scalpEvaluationCoveragePct >= 90 ? 'positive' : 'warning',
        },
        {
          label: 'Eligible Share',
          valueLabel:
            scalpEligibleSharePct === null
              ? '—'
              : `${scalpEligibleSharePct.toFixed(1)}%`,
          pct: scalpEligibleSharePct,
          tone: scalpEligibleSharePct !== null && scalpEligibleSharePct >= 20 ? 'positive' : 'neutral',
        },
      ],
      resultPreview: {
        cycleId: scalpCycleId || null,
        status: scalpCycleStatusRaw || null,
        discoveredSymbolsPreview: scalpUniverseSymbols.slice(0, 10),
        discoveredSymbolsRemaining: Math.max(0, scalpUniverseSymbols.length - 10),
        importedSymbolsPreview: scalpUniverseSeededSymbols.slice(0, 10),
        importedSymbolsRemaining: Math.max(0, scalpUniverseSeededSymbols.length - 10),
        evaluatedSymbolsPreview: scalpUniverseEvaluatedSymbols.slice(0, 10),
        evaluatedSymbolsRemaining: Math.max(0, scalpUniverseEvaluatedSymbols.length - 10),
        candidatesEvaluated: scalpUniverseCandidatesEvaluated ?? scalpUniverseEvaluatedCount,
        seedProcessedSymbols: scalpUniverseSeedProcessedCount,
        seedSummary: scalpUniverseSeedSummary || null,
        topEvaluationRows: scalpUniverseEvaluatedRows.slice(0, 8).map((row) => ({
          symbol: row.symbol,
          eligible: row.eligible,
          score: row.score,
          reasons: row.reasons,
        })),
        generatedAtIso: scalpResearchUniverse?.generatedAtIso || scalpResearchUniverse?.snapshot?.generatedAtIso || null,
      },
    },
    {
      id: 'scalp_cycle_worker',
      cadence: 'Every 1-2m',
      cronExpression: scalpCronRuntimeMeta('scalp_cycle_worker').expressionLabel,
      nextRunAtMs: scalpCronRuntimeMeta('scalp_cycle_worker').nextRunAtMs,
      invokePath: scalpCronRuntimeMeta('scalp_cycle_worker').invokePath,
      role: 'Claim and execute replay chunks',
      status: scalpStatusFromTs(scalpWorkerLastRunAtMs, 20 * 60_000),
      lastRunAtMs: scalpWorkerLastRunAtMs,
      lastDurationMs: scalpWorkerLastDurationMs,
      details: [
        {
          label: 'Cron',
          value: scalpCronRuntimeMeta('scalp_cycle_worker').expressionLabel || 'not configured',
          tone: scalpCronRuntimeMeta('scalp_cycle_worker').expressionLabel ? 'neutral' : 'warning',
        },
        {
          label: 'Next Run',
          value: formatScalpNextRunIn(scalpCronRuntimeMeta('scalp_cycle_worker').nextRunAtMs, scalpCronNowMs),
          tone: 'neutral',
        },
        {
          label: 'Worker Status',
          value: String(scalpResearchCycle?.workerHeartbeat?.status || 'n/a').trim() || 'n/a',
          tone:
            scalpResearchCycle?.workerHeartbeat?.status === 'failed'
              ? 'critical'
              : scalpResearchCycle?.workerHeartbeat?.status
                ? 'neutral'
                : 'warning',
        },
        {
          label: 'Budget Stop',
          value: scalpResearchCycle?.workerHeartbeat?.stoppedByDurationBudget ? 'yes' : 'no',
          tone: scalpResearchCycle?.workerHeartbeat?.stoppedByDurationBudget ? 'warning' : 'neutral',
        },
        { label: 'Progress', value: formatScalpPct(scalpCycleProgressPct), tone: 'neutral' },
        { label: 'Tasks', value: formatScalpCount(scalpCycleTasks), tone: 'neutral' },
        { label: 'Pending', value: formatScalpCount(scalpCyclePending), tone: 'warning' },
        { label: 'Running', value: formatScalpCount(scalpCycleRunning), tone: 'warning' },
        {
          label: 'Stale→Pending',
          value: formatScalpCount(scalpCycleRunningStaleAsPending),
          tone: scalpCycleRunningStaleAsPending ? 'warning' : 'neutral',
        },
        { label: 'Failed', value: formatScalpCount(scalpCycleFailed), tone: scalpCycleFailed ? 'critical' : 'positive' },
      ],
      visualMetrics: [
        {
          label: 'Progress',
          valueLabel:
            scalpCycleProgressPct === null
              ? '—'
              : `${scalpCycleProgressPct.toFixed(1)}%`,
          pct: scalpCycleProgressPct,
          tone: scalpCycleProgressPct !== null && scalpCycleProgressPct >= 99.9 ? 'positive' : 'neutral',
        },
        {
          label: 'Completed Share',
          valueLabel:
            scalpCycleCompletedPct === null
              ? '—'
              : `${scalpCycleCompletedPct.toFixed(1)}%`,
          pct: scalpCycleCompletedPct,
          tone: scalpCycleCompletedPct !== null && scalpCycleCompletedPct >= 70 ? 'positive' : 'neutral',
        },
        {
          label: 'Queue Share',
          valueLabel:
            scalpCycleQueuePct === null
              ? '—'
              : `${scalpCycleQueuePct.toFixed(1)}%`,
          pct: scalpCycleQueuePct,
          tone: scalpCycleQueuePct !== null && scalpCycleQueuePct > 30 ? 'warning' : 'neutral',
        },
        {
          label: 'Failure Share',
          valueLabel:
            scalpCycleFailedPct === null
              ? '—'
              : `${scalpCycleFailedPct.toFixed(1)}%`,
          pct: scalpCycleFailedPct,
          tone: scalpCycleFailedPct !== null && scalpCycleFailedPct > 0 ? 'critical' : 'positive',
        },
      ],
      resultPreview: {
        cycleId: scalpCycleId || null,
        status: scalpCycleStatusRaw || null,
        progressPct: scalpCycleProgressPct,
        totals: {
          tasks: scalpCycleTasks,
          pending: scalpCyclePending,
          running: scalpCycleRunning,
          completed: scalpCycleCompleted,
          failed: scalpCycleFailed,
        },
        totalsSource: scalpCycleTotalsSource,
        tasksLoaded: scalpWorkerTaskRows.length,
        includeTasks: scalpResearchCycle?.includeTasks ?? null,
        taskLimit: scalpResearchCycle?.taskLimit ?? null,
      },
    },
    {
      id: 'scalp_cycle_aggregate',
      cadence: 'Every 10m',
      cronExpression: scalpCronRuntimeMeta('scalp_cycle_aggregate').expressionLabel,
      nextRunAtMs: scalpCronRuntimeMeta('scalp_cycle_aggregate').nextRunAtMs,
      invokePath: scalpCronRuntimeMeta('scalp_cycle_aggregate').invokePath,
      role: 'Compute candidate/forward summary',
      status: scalpStatusFromTs(scalpCycleUpdatedAtMs, 45 * 60_000),
      lastRunAtMs: scalpCycleUpdatedAtMs,
      lastDurationMs: null,
      details: [
        {
          label: 'Cron',
          value: scalpCronRuntimeMeta('scalp_cycle_aggregate').expressionLabel || 'not configured',
          tone: scalpCronRuntimeMeta('scalp_cycle_aggregate').expressionLabel ? 'neutral' : 'warning',
        },
        {
          label: 'Next Run',
          value: formatScalpNextRunIn(scalpCronRuntimeMeta('scalp_cycle_aggregate').nextRunAtMs, scalpCronNowMs),
          tone: 'neutral',
        },
        {
          label: 'Completed',
          value: formatScalpCount(scalpCycleCompleted),
          tone: 'positive',
        },
        {
          label: 'Pending',
          value: formatScalpCount(scalpCyclePending),
          tone: (scalpCyclePending ?? 0) > 0 ? 'warning' : 'neutral',
        },
        {
          label: 'Candidates',
          value: formatScalpCount(asFiniteNumber(scalpResearchReport?.cycle?.candidateCount)),
          tone: 'neutral',
        },
        {
          label: 'Deployments',
          value: formatScalpCount(asFiniteNumber(scalpResearchReport?.summary?.deploymentsTotal)),
          tone: 'neutral',
        },
        {
          label: 'Enabled',
          value: formatScalpCount(asFiniteNumber(scalpResearchReport?.summary?.deploymentsEnabled)),
          tone: 'neutral',
        },
        {
          label: 'Abs Corr',
          value: scalpAvgAbsPairCorrelation === null ? '—' : scalpAvgAbsPairCorrelation.toFixed(2),
          tone: scalpAvgAbsPairCorrelation !== null && scalpAvgAbsPairCorrelation >= 0.7 ? 'warning' : 'neutral',
        },
      ],
      resultPreview: {
        cycle: scalpResearchReport?.cycle || null,
        summary: scalpResearchReport?.summary || null,
        generatedAtMs: scalpReportGeneratedAtMs,
      },
    },
    {
      id: 'scalp_promotion_gate_apply',
      cadence: 'Daily',
      cronExpression: scalpCronRuntimeMeta('scalp_promotion_gate_apply').expressionLabel,
      nextRunAtMs: scalpCronRuntimeMeta('scalp_promotion_gate_apply').nextRunAtMs,
      invokePath: scalpCronRuntimeMeta('scalp_promotion_gate_apply').invokePath,
      role: 'Apply forward validation gate',
      status: scalpStatusFromTs(
        scalpPromotionSyncFetchedAtMs ?? scalpReportGeneratedAtMs,
        36 * 60 * 60_000,
      ),
      lastRunAtMs: scalpPromotionSyncFetchedAtMs ?? scalpReportGeneratedAtMs,
      lastDurationMs: null,
      details: [
        {
          label: 'Cron',
          value: scalpCronRuntimeMeta('scalp_promotion_gate_apply').expressionLabel || 'not configured',
          tone: scalpCronRuntimeMeta('scalp_promotion_gate_apply').expressionLabel ? 'neutral' : 'warning',
        },
        {
          label: 'Next Run',
          value: formatScalpNextRunIn(
            scalpCronRuntimeMeta('scalp_promotion_gate_apply').nextRunAtMs,
            scalpCronNowMs,
          ),
          tone: 'neutral',
        },
        {
          label: 'Cycle',
          value: scalpPromotionSyncCycleId || scalpCycleId || 'pending',
          tone: scalpPromotionSyncCycleId || scalpCycleId ? 'neutral' : 'warning',
        },
        {
          label: 'Sync',
          value: scalpPromotionSyncStatusRaw || 'unknown',
          tone: scalpPromotionSyncStatusTone,
        },
        { label: 'Enabled', value: formatScalpCount(scalpEnabledDeploymentCount), tone: 'neutral' },
        { label: 'Eligible', value: formatScalpCount(scalpPromotionEligibleCount), tone: 'positive' },
        {
          label: 'Blocked',
          value: formatScalpCount(scalpIneligibleCount),
          tone: scalpIneligibleCount > 0 ? 'warning' : 'positive',
        },
        {
          label: 'Shortlisted',
          value: formatScalpCount(scalpMaterializationShortlisted),
          tone: 'neutral',
        },
        {
          label: 'Missing',
          value: formatScalpCount(scalpMaterializationMissing),
          tone:
            scalpMaterializationMissing === null
              ? 'neutral'
              : scalpMaterializationMissing > 0
                ? 'warning'
                : 'positive',
        },
        {
          label: 'Forward Exp',
          value: formatScalpSignedR(scalpMeanForwardExpectancyR),
          tone:
            scalpMeanForwardExpectancyR === null
              ? 'neutral'
              : scalpMeanForwardExpectancyR >= 0
                ? 'positive'
                : 'critical',
        },
        {
          label: 'Profitable',
          value: formatScalpPct(scalpMeanForwardProfitablePct, 1),
          tone:
            scalpMeanForwardProfitablePct === null
              ? 'neutral'
              : scalpMeanForwardProfitablePct >= 50
                ? 'positive'
                : 'warning',
        },
      ],
      visualMetrics: [
        {
          label: 'Eligible Share',
          valueLabel: scalpGateEligiblePct === null ? '—' : `${scalpGateEligiblePct.toFixed(1)}%`,
          pct: scalpGateEligiblePct,
          tone: scalpGateEligiblePct !== null && scalpGateEligiblePct >= 60 ? 'positive' : 'warning',
        },
        {
          label: 'Blocked Share',
          valueLabel: scalpGateBlockedPct === null ? '—' : `${scalpGateBlockedPct.toFixed(1)}%`,
          pct: scalpGateBlockedPct,
          tone: scalpGateBlockedPct !== null && scalpGateBlockedPct > 40 ? 'warning' : 'neutral',
        },
        {
          label: 'Profitable Forward',
          valueLabel:
            scalpMeanForwardProfitablePct === null
              ? '—'
              : `${scalpMeanForwardProfitablePct.toFixed(1)}%`,
          pct: scalpMeanForwardProfitablePct,
          tone:
            scalpMeanForwardProfitablePct === null
              ? 'neutral'
              : scalpMeanForwardProfitablePct >= 50
                ? 'positive'
                : 'warning',
        },
        {
          label: 'Shortlist Coverage',
          valueLabel:
            scalpMaterializationCoveragePct === null
              ? '—'
              : `${scalpMaterializationCoveragePct.toFixed(1)}%`,
          pct: scalpMaterializationCoveragePct,
          tone:
            scalpMaterializationCoveragePct === null
              ? 'neutral'
              : scalpMaterializationCoveragePct >= 80
                ? 'positive'
                : 'warning',
        },
        {
          label: 'Created from Missing',
          valueLabel:
            scalpMaterializationCreatedFromMissingPct === null
              ? '—'
              : `${scalpMaterializationCreatedFromMissingPct.toFixed(1)}%`,
          pct: scalpMaterializationCreatedFromMissingPct,
          tone:
            scalpMaterializationCreatedFromMissingPct === null
              ? 'neutral'
              : scalpMaterializationCreatedFromMissingPct >= 80
                ? 'positive'
                : 'warning',
        },
      ],
      resultPreview: {
        syncPreview: {
          fetchedAtMs: scalpPromotionSyncFetchedAtMs,
          cycleId: scalpPromotionSyncCycleId,
          cycleStatus: scalpPromotionSyncStatusRaw || null,
          reason: scalpPromotionSyncReason,
          deploymentsConsidered: scalpPromotionSyncConsidered,
          deploymentsMatched: scalpPromotionSyncMatched,
          deploymentsUpdated: scalpPromotionSyncUpdated,
          weeklyPolicy: scalpPromotionSyncSnapshot?.weeklyPolicy || null,
        },
        materialization: {
          source: scalpMaterializationSource,
          topKPerSymbol: scalpMaterializationTopKPerSymbol,
          shortlistedCandidates: scalpMaterializationShortlisted,
          existingCandidates: scalpMaterializationExistingCount,
          missingCandidates: scalpMaterializationMissing,
          createdCandidates: scalpMaterializationCreated,
          rowsPreview: scalpMaterializationPreviewRows.slice(0, 12),
          rowsRemaining: Math.max(0, scalpMaterializationRows.length - 12),
        },
        enabledDeployments: scalpEnabledDeploymentCount,
        promotionEligible: scalpPromotionEligibleCount,
        ineligible: scalpIneligibleCount,
        meanForwardExpectancyR: scalpMeanForwardExpectancyR,
        meanForwardProfitableWindowPct: scalpMeanForwardProfitablePct,
        reportGeneratedAtMs: scalpReportGeneratedAtMs,
      },
    },
    {
      id: 'scalp_execute_deployments',
      cadence: 'Every 1m',
      cronExpression: scalpCronRuntimeMeta('scalp_execute_deployments').expressionLabel,
      nextRunAtMs: scalpCronRuntimeMeta('scalp_execute_deployments').nextRunAtMs,
      invokePath: scalpCronRuntimeMeta('scalp_execute_deployments').invokePath,
      role: 'Run enabled and gate-eligible deployments',
      status: scalpStatusFromTs(scalpLastExecuteRunAtMs, 10 * 60_000),
      lastRunAtMs: scalpLastExecuteRunAtMs,
      lastDurationMs: null,
      details: [
        {
          label: 'Cron',
          value: scalpCronRuntimeMeta('scalp_execute_deployments').expressionLabel || 'not configured',
          tone: scalpCronRuntimeMeta('scalp_execute_deployments').expressionLabel ? 'neutral' : 'warning',
        },
        {
          label: 'Next Run',
          value: formatScalpNextRunIn(
            scalpCronRuntimeMeta('scalp_execute_deployments').nextRunAtMs,
            scalpCronNowMs,
          ),
          tone: 'neutral',
        },
        { label: 'Run Count', value: formatScalpCount(scalpSummaryRunCount), tone: 'neutral' },
        { label: 'Dry Runs', value: formatScalpCount(scalpSummaryDryRunCount), tone: 'neutral' },
        { label: 'Open', value: formatScalpCount(scalpSummaryOpenCount), tone: 'neutral' },
        { label: 'Trades', value: formatScalpCount(scalpSummaryTradesPlaced), tone: 'neutral' },
        {
          label: 'Last Deployment',
          value: scalpLatestExecutionDeploymentId || '—',
          tone: scalpLatestExecutionDeploymentId ? 'neutral' : 'warning',
        },
      ],
      resultPreview: {
        summary: scalpSummary?.summary || null,
        latestExecution: {
          atMs: scalpLatestExecutionJournalEntry?.timestampMs ?? null,
          symbol: scalpLatestExecutionSymbol || null,
          deploymentId: scalpLatestExecutionDeploymentId || null,
          reasonCodes: Array.isArray(scalpLatestExecutionJournalEntry?.reasonCodes)
            ? scalpLatestExecutionJournalEntry?.reasonCodes
            : [],
          payload: scalpLatestExecutionPayload || null,
        },
      },
    },
    {
      id: 'scalp_live_guardrail_monitor',
      cadence: 'Every 5-15m',
      cronExpression: scalpCronRuntimeMeta('scalp_live_guardrail_monitor').expressionLabel,
      nextRunAtMs: scalpCronRuntimeMeta('scalp_live_guardrail_monitor').nextRunAtMs,
      invokePath: scalpCronRuntimeMeta('scalp_live_guardrail_monitor').invokePath,
      role: 'Pause hard-breach deployments',
      status: scalpStatusFromTs(
        scalpLastGuardrailAtMs ?? scalpReportGeneratedAtMs,
        60 * 60_000,
      ),
      lastRunAtMs: scalpLastGuardrailAtMs ?? scalpReportGeneratedAtMs,
      lastDurationMs: null,
      details: [
        {
          label: 'Cron',
          value: scalpCronRuntimeMeta('scalp_live_guardrail_monitor').expressionLabel || 'not configured',
          tone: scalpCronRuntimeMeta('scalp_live_guardrail_monitor').expressionLabel ? 'neutral' : 'warning',
        },
        {
          label: 'Next Run',
          value: formatScalpNextRunIn(
            scalpCronRuntimeMeta('scalp_live_guardrail_monitor').nextRunAtMs,
            scalpCronNowMs,
          ),
          tone: 'neutral',
        },
        {
          label: 'Last Event',
          value: formatScalpTime(scalpLastGuardrailAtMs ?? scalpReportGeneratedAtMs),
          tone: scalpLastGuardrailAtMs ? 'warning' : 'neutral',
        },
        {
          label: 'Symbol',
          value: scalpLatestGuardrailSymbol || '—',
          tone: scalpLatestGuardrailSymbol ? 'warning' : 'neutral',
        },
        {
          label: 'Deployment',
          value: scalpLatestGuardrailDeploymentId || '—',
          tone: scalpLatestGuardrailDeploymentId ? 'warning' : 'neutral',
        },
        {
          label: 'Reason',
          value: Array.isArray(scalpLatestGuardrailEntry?.reasonCodes) && scalpLatestGuardrailEntry.reasonCodes.length
            ? String(scalpLatestGuardrailEntry.reasonCodes[0] || '').replace(/_/g, ' ')
            : 'none',
          tone: scalpLatestGuardrailEntry ? 'warning' : 'neutral',
        },
      ],
      resultPreview: scalpLatestGuardrailEntry
        ? {
            id: scalpLatestGuardrailEntry.id || null,
            timestampMs: scalpLatestGuardrailEntry.timestampMs || null,
            type: scalpLatestGuardrailEntry.type || null,
            level: scalpLatestGuardrailEntry.level || null,
            symbol: scalpLatestGuardrailSymbol || null,
            deploymentId: scalpLatestGuardrailDeploymentId || null,
            reasonCodes: scalpLatestGuardrailEntry.reasonCodes || [],
            payload: scalpLatestGuardrailPayload || null,
          }
        : {
            message: 'No guardrail event found in current journal window.',
            journalWindowCount: scalpJournalRows.length,
          },
    },
    {
      id: 'scalp_housekeeping',
      cadence: 'Hourly',
      cronExpression: scalpCronRuntimeMeta('scalp_housekeeping').expressionLabel,
      nextRunAtMs: scalpCronRuntimeMeta('scalp_housekeeping').nextRunAtMs,
      invokePath: scalpCronRuntimeMeta('scalp_housekeeping').invokePath,
      role: 'Prune stale locks and compact retention',
      status: scalpStatusFromTs(
        scalpDashboardGeneratedAtMs ?? scalpReportGeneratedAtMs,
        2 * 60 * 60_000,
      ),
      lastRunAtMs: scalpDashboardGeneratedAtMs ?? scalpReportGeneratedAtMs,
      lastDurationMs: null,
      details: [
        {
          label: 'Cron',
          value: scalpCronRuntimeMeta('scalp_housekeeping').expressionLabel || 'not configured',
          tone: scalpCronRuntimeMeta('scalp_housekeeping').expressionLabel ? 'neutral' : 'warning',
        },
        {
          label: 'Next Run',
          value: formatScalpNextRunIn(scalpCronRuntimeMeta('scalp_housekeeping').nextRunAtMs, scalpCronNowMs),
          tone: 'neutral',
        },
        {
          label: 'Dashboard Snapshot',
          value: formatScalpTime(scalpDashboardGeneratedAtMs),
          tone: scalpDashboardGeneratedAtMs ? 'neutral' : 'warning',
        },
        {
          label: 'Report Snapshot',
          value: formatScalpTime(scalpReportGeneratedAtMs),
          tone: scalpReportGeneratedAtMs ? 'neutral' : 'warning',
        },
        {
          label: 'Cycle Snapshot',
          value: formatScalpTime(scalpCycleUpdatedAtMs),
          tone: scalpCycleUpdatedAtMs ? 'neutral' : 'warning',
        },
        {
          label: 'Journal Rows',
          value: formatScalpCount(asFiniteNumber(scalpJournalRows.length)),
          tone: 'neutral',
        },
      ],
      resultPreview: {
        source: scalpSummary?.source || null,
        generatedAtMs: scalpDashboardGeneratedAtMs,
        reportGeneratedAtMs: scalpReportGeneratedAtMs,
        cycleUpdatedAtMs: scalpCycleUpdatedAtMs,
        journalRowsLoaded: scalpJournalRows.length,
      },
    },
  ];
  const scalpActiveExecutionTs =
    scalpActiveExecution && typeof scalpActiveExecution.timestampMs === 'number'
      ? scalpActiveExecution.timestampMs
      : null;
  const scalpActiveRuntimeRow = scalpActiveRow || scalpActiveOpsRow?.runtime || null;
  const scalpDarkMode = resolvedTheme === 'dark';
  const scalpSectionShellClass = scalpDarkMode
    ? 'rounded-3xl border border-zinc-700 bg-zinc-900 text-zinc-100'
    : 'rounded-3xl border border-slate-200 bg-white text-slate-900';
  const scalpCardClass = scalpDarkMode
    ? 'rounded-2xl border border-zinc-700 bg-zinc-950/70 p-3'
    : 'rounded-2xl border border-slate-200 bg-white p-3';
  const scalpHeroClass = scalpDarkMode
    ? 'relative overflow-hidden rounded-3xl border border-zinc-700 bg-gradient-to-br from-zinc-900 via-zinc-900 to-zinc-800 p-5 text-zinc-100'
    : 'relative overflow-hidden rounded-3xl border border-slate-200 bg-gradient-to-br from-slate-100 via-white to-slate-50 p-5 text-slate-900';
  const scalpHeroBadgeClass = scalpDarkMode
    ? 'absolute right-5 top-5 rounded-full border border-zinc-500/70 bg-zinc-800 px-3 py-1 text-[11px] font-medium tracking-wider text-zinc-200'
    : 'absolute right-5 top-5 rounded-full border border-slate-300 bg-white/90 px-3 py-1 text-[11px] font-medium tracking-wider text-slate-600';
  const scalpTextPrimaryClass = scalpDarkMode ? 'text-zinc-100' : 'text-slate-900';
  const scalpTextSecondaryClass = scalpDarkMode ? 'text-zinc-300' : 'text-slate-600';
  const scalpTextMutedClass = scalpDarkMode ? 'text-zinc-400' : 'text-slate-500';
  const scalpTableHeaderClass = scalpDarkMode ? 'text-zinc-400' : 'text-slate-500';
  const scalpTableRowClass = scalpDarkMode ? 'bg-zinc-950/85 hover:bg-zinc-800/85' : 'bg-slate-50 hover:bg-slate-100';
  const scalpTagNeutralClass = scalpDarkMode
    ? 'rounded-full border border-zinc-600 bg-zinc-800 px-2 py-1 text-[11px] text-zinc-200'
    : 'rounded-full border border-slate-300 bg-white px-2 py-1 text-[11px] text-slate-600';
  const scalpCronExpandedPanelClass = scalpDarkMode
    ? 'mx-2 mb-2 rounded-2xl border border-zinc-700 bg-zinc-900/95 p-3'
    : 'mx-2 mb-2 rounded-2xl border border-slate-200 bg-white p-3';
  const scalpCronPreviewClass = scalpDarkMode
    ? 'max-h-64 overflow-auto rounded-xl border border-zinc-700 bg-zinc-950/80 p-2 font-mono text-[11px] text-zinc-300'
    : 'max-h-64 overflow-auto rounded-xl border border-slate-200 bg-slate-50 p-2 font-mono text-[11px] text-slate-700';

  const scalpStateMeta = (state?: string | null) => {
    const normalized = String(state || 'MISSING').trim().toUpperCase();
    if (normalized === 'IN_TRADE') {
      return {
        label: 'IN TRADE',
        className: 'border-emerald-200 bg-emerald-100 text-emerald-800',
        Icon: ArrowUpRight,
      };
    }
    if (normalized === 'DONE') {
      return {
        label: 'DONE',
        className:
          resolvedTheme === 'dark'
            ? 'border-sky-500/60 bg-sky-500/20 text-sky-100'
            : 'border-sky-200 bg-sky-50 text-sky-700',
        Icon: Circle,
      };
    }
    if (normalized.includes('ERROR') || normalized.includes('BLOCK') || normalized === 'MISSING') {
      return {
        label: normalized,
        className: 'border-rose-200 bg-rose-100 text-rose-800',
        Icon: ShieldPlus,
      };
    }
    if (normalized.includes('WAIT') || normalized.includes('IDLE') || normalized.includes('COOLDOWN')) {
      return {
        label: normalized,
        className: 'border-amber-200 bg-amber-100 text-amber-800',
        Icon: Repeat,
      };
    }
    return {
      label: normalized,
      className: 'border-sky-200 bg-sky-100 text-sky-800',
      Icon: Circle,
    };
  };

  const scalpModeMeta = (dryRunLast?: boolean | null) => {
    if (dryRunLast === true) {
      return {
        label: 'DRY',
        className: 'border-amber-200 bg-amber-100 text-amber-800',
      };
    }
    if (dryRunLast === false) {
      return {
        label: 'LIVE',
        className: 'border-rose-200 bg-rose-100 text-rose-800',
      };
    }
    return {
      label: 'UNKNOWN',
      className: 'border-slate-200 bg-slate-100 text-slate-600',
    };
  };

  const scalpReasonMeta = (code: string) => {
    const upper = code.toUpperCase();
    if (/ERROR|FAIL|REJECT|INVALID|BLOCK/.test(upper)) {
      return {
        className: 'border-rose-200 bg-rose-50 text-rose-700',
        Icon: ShieldPlus,
      };
    }
    if (/ENTRY|EXEC|OPEN|CLOSE|BUY|SELL|TP|SL|TRAIL/.test(upper)) {
      return {
        className: 'border-emerald-200 bg-emerald-50 text-emerald-700',
        Icon: Zap,
      };
    }
    if (/WAIT|COOLDOWN|PAUSE|HOLD|IDLE/.test(upper)) {
      return {
        className: 'border-amber-200 bg-amber-50 text-amber-700',
        Icon: Activity,
      };
    }
    return {
      className: 'border-slate-200 bg-slate-100 text-slate-700',
      Icon: Circle,
    };
  };

  const scalpJournalMeta = (entry: { type?: string; level?: string }) => {
    const type = String(entry.type || '').trim().toUpperCase();
    const level = String(entry.level || '').trim().toUpperCase();
    if (level === 'ERROR' || type === 'ERROR') {
      return {
        className: 'border-rose-200 bg-rose-50 text-rose-700',
        Icon: ShieldPlus,
      };
    }
    if (level === 'WARN') {
      return {
        className: 'border-amber-200 bg-amber-50 text-amber-700',
        Icon: Activity,
      };
    }
    if (type === 'EXECUTION') {
      return {
        className: 'border-emerald-200 bg-emerald-50 text-emerald-700',
        Icon: Zap,
      };
    }
    if (type === 'STATE') {
      return {
        className: 'border-sky-200 bg-sky-50 text-sky-700',
        Icon: Repeat,
      };
    }
    return {
      className: 'border-slate-200 bg-slate-100 text-slate-700',
      Icon: BookOpen,
    };
  };

  const scalpCronStatusMeta = (status: ScalpOpsCronStatus) => {
    if (status === 'healthy') {
      return resolvedTheme === 'dark'
        ? 'border-emerald-300/50 bg-emerald-400/15 text-emerald-200'
        : 'border-emerald-200 bg-emerald-50 text-emerald-700';
    }
    if (status === 'lagging') {
      return resolvedTheme === 'dark'
        ? 'border-amber-300/50 bg-amber-400/15 text-amber-200'
        : 'border-amber-200 bg-amber-50 text-amber-700';
    }
    return resolvedTheme === 'dark'
      ? 'border-zinc-500/60 bg-zinc-500/15 text-zinc-200'
      : 'border-slate-200 bg-slate-100 text-slate-600';
  };
  const scalpCronDetailToneMeta = (tone?: ScalpOpsCronDetailTone) => {
    if (tone === 'positive') {
      return resolvedTheme === 'dark'
        ? 'border-emerald-400/40 bg-emerald-500/10 text-emerald-200'
        : 'border-emerald-200 bg-emerald-50 text-emerald-700';
    }
    if (tone === 'warning') {
      return resolvedTheme === 'dark'
        ? 'border-amber-400/40 bg-amber-500/10 text-amber-200'
        : 'border-amber-200 bg-amber-50 text-amber-700';
    }
    if (tone === 'critical') {
      return resolvedTheme === 'dark'
        ? 'border-rose-400/40 bg-rose-500/10 text-rose-200'
        : 'border-rose-200 bg-rose-50 text-rose-700';
    }
    return resolvedTheme === 'dark'
      ? 'border-zinc-600 bg-zinc-800/70 text-zinc-200'
      : 'border-slate-200 bg-slate-50 text-slate-700';
  };
  const scalpWorkerTaskStatusMeta = (status: string) => {
    const normalized = String(status || '').trim().toLowerCase();
    if (normalized === 'completed') {
      return resolvedTheme === 'dark'
        ? 'border-emerald-400/40 bg-emerald-500/10 text-emerald-200'
        : 'border-emerald-200 bg-emerald-50 text-emerald-700';
    }
    if (normalized === 'failed') {
      return resolvedTheme === 'dark'
        ? 'border-rose-400/40 bg-rose-500/10 text-rose-200'
        : 'border-rose-200 bg-rose-50 text-rose-700';
    }
    if (normalized === 'running') {
      return resolvedTheme === 'dark'
        ? 'border-amber-400/40 bg-amber-500/10 text-amber-200'
        : 'border-amber-200 bg-amber-50 text-amber-700';
    }
    return resolvedTheme === 'dark'
      ? 'border-zinc-600 bg-zinc-800/70 text-zinc-300'
      : 'border-slate-200 bg-slate-100 text-slate-600';
  };
  const scalpVisualMetricFillMeta = (tone?: ScalpOpsCronDetailTone) => {
    if (tone === 'positive') return resolvedTheme === 'dark' ? 'bg-emerald-400/80' : 'bg-emerald-500';
    if (tone === 'warning') return resolvedTheme === 'dark' ? 'bg-amber-400/80' : 'bg-amber-500';
    if (tone === 'critical') return resolvedTheme === 'dark' ? 'bg-rose-400/80' : 'bg-rose-500';
    return resolvedTheme === 'dark' ? 'bg-zinc-400/80' : 'bg-slate-500';
  };
  const scalpVisualMetricTrackClass = resolvedTheme === 'dark' ? 'bg-zinc-800' : 'bg-slate-200';
  const scalpImportStatusMeta = (status: ScalpUniversePipelineRow['importStatus']) => {
    if (status === 'seeded') {
      return resolvedTheme === 'dark'
        ? 'border-emerald-400/40 bg-emerald-500/10 text-emerald-200'
        : 'border-emerald-200 bg-emerald-50 text-emerald-700';
    }
    if (status === 'failed') {
      return resolvedTheme === 'dark'
        ? 'border-rose-400/40 bg-rose-500/10 text-rose-200'
        : 'border-rose-200 bg-rose-50 text-rose-700';
    }
    if (status === 'skipped') {
      return resolvedTheme === 'dark'
        ? 'border-amber-400/40 bg-amber-500/10 text-amber-200'
        : 'border-amber-200 bg-amber-50 text-amber-700';
    }
    return resolvedTheme === 'dark'
      ? 'border-zinc-600 bg-zinc-800/70 text-zinc-300'
      : 'border-slate-200 bg-slate-100 text-slate-600';
  };
  const scalpEvaluationMeta = (eligible: boolean | null) => {
    if (eligible === true) {
      return resolvedTheme === 'dark'
        ? 'border-emerald-400/40 bg-emerald-500/10 text-emerald-200'
        : 'border-emerald-200 bg-emerald-50 text-emerald-700';
    }
    if (eligible === false) {
      return resolvedTheme === 'dark'
        ? 'border-rose-400/40 bg-rose-500/10 text-rose-200'
        : 'border-rose-200 bg-rose-50 text-rose-700';
    }
    return resolvedTheme === 'dark'
      ? 'border-zinc-600 bg-zinc-800/70 text-zinc-300'
      : 'border-slate-200 bg-slate-100 text-slate-600';
  };
  const scalpWorkerSortButtonClass = (active: boolean) =>
    active
      ? resolvedTheme === 'dark'
        ? 'rounded border border-zinc-300 bg-zinc-100 px-1 text-[10px] leading-4 text-zinc-900'
        : 'rounded border border-slate-500 bg-slate-700 px-1 text-[10px] leading-4 text-white'
      : resolvedTheme === 'dark'
        ? 'rounded border border-zinc-600 bg-zinc-800 px-1 text-[10px] leading-4 text-zinc-300 hover:border-zinc-400 hover:text-zinc-100'
        : 'rounded border border-slate-300 bg-white px-1 text-[10px] leading-4 text-slate-500 hover:border-slate-500 hover:text-slate-700';
  const setScalpWorkerSortColumn = (key: ScalpWorkerSortKey, direction: ScalpWorkerSortDirection) => {
    setScalpWorkerSort((prev) => {
      if (prev.key === key && prev.direction === direction) return prev;
      return { key, direction };
    });
  };
  const renderScalpWorkerSortableHeader = (label: string, key: ScalpWorkerSortKey) => {
    const ascActive = scalpWorkerSort.key === key && scalpWorkerSort.direction === 'asc';
    const descActive = scalpWorkerSort.key === key && scalpWorkerSort.direction === 'desc';
    return (
      <div className="inline-flex items-center gap-1">
        <span>{label}</span>
        <div className="inline-flex items-center gap-0.5">
          <button
            type="button"
            aria-label={`Sort ${label} ascending`}
            className={scalpWorkerSortButtonClass(ascActive)}
            onClick={() => setScalpWorkerSortColumn(key, 'asc')}
          >
            ↑
          </button>
          <button
            type="button"
            aria-label={`Sort ${label} descending`}
            className={scalpWorkerSortButtonClass(descActive)}
            onClick={() => setScalpWorkerSortColumn(key, 'desc')}
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
              <div key={`summary-skeleton-${idx}`} className="animate-pulse space-y-2">
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
              <div key={`bias-skeleton-${idx}`} className="h-12 rounded-lg border border-slate-200 bg-slate-50" />
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
    const nextTheme: ThemePreference = resolvedTheme === 'dark' ? 'light' : 'dark';
    setThemePreference(nextTheme);
    setResolvedTheme(nextTheme);
    if (typeof window !== 'undefined') {
      window.localStorage.setItem(THEME_PREFERENCE_STORAGE_KEY, nextTheme);
    }
  };

  const handleStrategyModeChange = (mode: StrategyMode) => {
    setStrategyMode(mode);
    setError(null);
    if (typeof window !== 'undefined') {
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
        className={`min-h-screen px-4 py-6 relative sm:px-6 lg:px-8 ${
          resolvedTheme === 'dark'
            ? 'theme-dark bg-slate-950 text-slate-100'
            : 'theme-light bg-slate-50 text-slate-900'
        }`}
      >
        <button
          type="button"
          onClick={handleThemeToggle}
          className={`fixed right-4 top-4 z-[60] inline-flex h-10 w-10 items-center justify-center rounded-full border shadow-sm transition focus:outline-none focus-visible:ring-2 ${
            resolvedTheme === 'dark'
              ? 'border-zinc-700 bg-zinc-900 text-zinc-100 hover:border-zinc-500 hover:bg-zinc-800 hover:text-zinc-50 focus-visible:ring-zinc-500 focus-visible:ring-offset-2 focus-visible:ring-offset-zinc-950'
              : 'border-slate-300 bg-white text-slate-700 hover:border-slate-500 hover:bg-slate-50 hover:text-slate-900 focus-visible:ring-slate-400 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-50'
          }`}
          aria-label={resolvedTheme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
          title={resolvedTheme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
        >
          {resolvedTheme === 'dark' ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
        </button>
        {adminReady && !adminGranted && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-sm px-4">
            <div className="w-full max-w-md rounded-2xl border border-slate-200 bg-white p-6 shadow-2xl pointer-events-auto">
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 items-center justify-center rounded-full bg-slate-100">
                  <ShieldCheck className="h-5 w-5 text-slate-700" />
                </div>
                <div>
                  <div className="text-xs uppercase tracking-[0.2em] text-slate-500">Admin Access</div>
                  <h2 className="text-xl font-semibold text-slate-900">Enter access secret</h2>
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
                {adminError && <div className="text-sm font-semibold text-rose-600">{adminError}</div>}
                <button
                  type="submit"
                  disabled={adminSubmitting || !adminInput.trim()}
                  className="w-full rounded-xl bg-slate-900 px-4 py-3 text-sm font-semibold text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-400"
                >
                  {adminSubmitting ? 'Checking…' : 'Unlock dashboard'}
                </button>
              </form>
            </div>
          </div>
        )}
        <div className="w-full">
        <div className="flex items-center justify-between gap-4 rounded-2xl border border-slate-200 bg-white px-6 py-5 shadow-sm">
          <div>
            <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Performance</p>
            <h1 className="text-3xl font-semibold leading-tight text-slate-900">AI Trade Dashboard</h1>
            <div className="mt-2 inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 p-1">
              <button
                type="button"
                onClick={() => handleStrategyModeChange('swing')}
                className={`rounded-full px-3 py-1 text-[11px] font-semibold transition ${
                  strategyMode === 'swing'
                    ? 'bg-sky-600 text-white shadow-sm'
                    : 'text-slate-600 hover:bg-slate-200/70 hover:text-slate-900'
                }`}
              >
                Swing
              </button>
              <button
                type="button"
                onClick={() => handleStrategyModeChange('scalp')}
                className={`rounded-full px-3 py-1 text-[11px] font-semibold transition ${
                  strategyMode === 'scalp'
                    ? 'bg-sky-600 text-white shadow-sm'
                    : 'text-slate-600 hover:bg-slate-200/70 hover:text-slate-900'
                }`}
              >
                Scalp
              </button>
            </div>
            {strategyMode === 'swing' && activeSymbol && currentEvalJob ? (
              <p className="mt-1 text-xs text-slate-500">
                Eval job for {activeSymbol}:{' '}
                <span className="font-semibold text-slate-700">{currentEvalJob.status}</span>
                {currentEvalJob.error ? ` (${currentEvalJob.error})` : ''}
              </p>
            ) : null}
            {strategyMode === 'swing' && activeSymbol ? (
              <p className="mt-1 text-xs text-slate-500">
                Live price:{' '}
                <span className={livePriceConnected ? 'font-semibold text-emerald-700' : 'font-semibold text-slate-600'}>
                  {livePriceConnected ? 'connected' : 'connecting'}
                </span>
                {typeof livePriceNow === 'number' ? ` · ${livePriceNow.toFixed(2)}` : ''}
              </p>
            ) : null}
            {loading ? <p className="mt-1 text-xs text-slate-500">{loadingLabel}</p> : null}
          </div>
          <div className="flex items-center gap-2">
            {strategyMode === 'swing' ? (
              <button
                onClick={() => (activeSymbol ? triggerEvaluation(activeSymbol) : undefined)}
                disabled={!adminGranted || !activeSymbol || !!evaluateSubmittingSymbol || evaluateRunning}
                className="rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-800 transition hover:border-emerald-300 hover:bg-emerald-50 hover:text-emerald-800 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {evaluateSubmittingSymbol ? 'Queueing…' : evaluateRunning ? 'Evaluating…' : 'Run Evaluation'}
              </button>
            ) : null}
            <button
              onClick={() => {
                if (strategyMode === 'scalp') {
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

        {error && (
          <div className="mt-4 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm font-semibold text-rose-700">
            Could not load dashboard data: {error}
          </div>
        )}

        {strategyMode === 'swing' && !error && (
          <div className="mt-4 flex flex-wrap items-center gap-2">
            {symbols.map((sym, i) => {
              const isActive = i === active;
              const tab = tabData[sym];
              const pnl7dValue =
                typeof tab?.pnl7dWithOpen === 'number'
                  ? tab.pnl7dWithOpen
                  : typeof tab?.pnl7d === 'number'
                  ? tab.pnl7d
                  : null;
              const pnlTone =
                typeof pnl7dValue === 'number' ? (pnl7dValue < 0 ? 'negative' : 'positive') : 'neutral';
              return (
                <button
                  key={sym}
                  onClick={() => setActive(i)}
                  className={`rounded-full border px-4 py-2 text-sm font-semibold transition ${
                    pnlTone === 'positive'
                      ? 'border-emerald-200 bg-emerald-50 text-emerald-700 hover:border-emerald-300 hover:text-emerald-800'
                      : pnlTone === 'negative'
                      ? 'border-rose-200 bg-rose-50 text-rose-700 hover:border-rose-300 hover:text-rose-800'
                      : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:text-slate-800'
                  } ${
                    isActive
                      ? 'shadow-md ring-2 ring-slate-400/70 outline outline-2 outline-offset-2 outline-slate-200/80'
                      : ''
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
          {strategyMode === 'scalp' ? (
            loading ? (
              <div className={`${scalpSectionShellClass} p-4 shadow-sm`}>
                <div className="animate-pulse space-y-3">
                  <div className={`h-4 w-44 rounded-full ${scalpDarkMode ? 'bg-zinc-600' : 'bg-slate-200'}`} />
                  <div className={`h-3 w-64 rounded-full ${scalpDarkMode ? 'bg-zinc-700' : 'bg-slate-200'}`} />
                  <div className={`h-40 rounded-xl border ${scalpDarkMode ? 'border-zinc-700 bg-zinc-800' : 'border-slate-200 bg-slate-50'}`} />
                </div>
              </div>
            ) : !scalpOpsDeployments.length ? (
              <div className="flex items-center justify-center py-12 text-sm font-semibold text-slate-500">
                No scalp deployments yet. Add enabled deployments to the registry, run one cycle, then refresh.
              </div>
            ) : (
              <div className="space-y-5">
                <section className={scalpHeroClass}>
                  <div className={scalpHeroBadgeClass}>
                    {scalpResearchCycle?.cycleId || scalpResearchReport?.cycle?.cycleId || 'cycle_pending'}
                  </div>
                  <p className={`text-[11px] uppercase tracking-[0.24em] ${scalpTextMutedClass}`}>Scalp Ops Console</p>
                  <h2 className={`mt-2 text-2xl font-semibold ${scalpTextPrimaryClass}`}>
                    Deploy only what passes forward evidence.
                  </h2>
                  <p className={`mt-2 max-w-4xl text-sm ${scalpTextSecondaryClass}`}>
                    Live view combines cycle progress, promotion gate outcomes, deployment-level forward validation, and
                    runtime execution signals.
                  </p>
                  <div className="mt-5 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
                    <article className={scalpCardClass}>
                      <p className={`text-[11px] uppercase tracking-[0.16em] ${scalpTextMutedClass}`}>Cycle Progress</p>
                      <p className={`mt-2 text-2xl font-semibold ${scalpTextPrimaryClass}`}>
                        {scalpCycleProgressPct === null ? '—' : `${scalpCycleProgressPct.toFixed(0)}%`}
                      </p>
                      <p className={`mt-1 text-xs ${scalpTextSecondaryClass}`}>
                        {scalpCycleCompleted !== null && scalpCycleTasks !== null
                          ? `${Math.floor(scalpCycleCompleted)} / ${Math.floor(scalpCycleTasks)} tasks`
                          : 'Awaiting cycle summary'}
                      </p>
                    </article>
                    <article className={scalpCardClass}>
                      <p className={`text-[11px] uppercase tracking-[0.16em] ${scalpTextMutedClass}`}>Forward Expectancy</p>
                      <p className={`mt-2 text-2xl font-semibold ${scalpMeanForwardExpectancyR === null ? scalpTextPrimaryClass : scalpMeanForwardExpectancyR >= 0 ? 'text-emerald-500' : 'text-rose-500'}`}>
                        {scalpMeanForwardExpectancyR === null
                          ? '—'
                          : `${scalpMeanForwardExpectancyR >= 0 ? '+' : ''}${scalpMeanForwardExpectancyR.toFixed(2)}R`}
                      </p>
                      <p className={`mt-1 text-xs ${scalpTextSecondaryClass}`}>
                        mean across {scalpForwardExpectancyRows.length} validated deployment(s)
                      </p>
                    </article>
                    <article className={scalpCardClass}>
                      <p className={`text-[11px] uppercase tracking-[0.16em] ${scalpTextMutedClass}`}>Promotion Eligible</p>
                      <p className={`mt-2 text-2xl font-semibold ${scalpTextPrimaryClass}`}>
                        {scalpPromotionEligibleCount} / {scalpEnabledDeploymentCount || scalpOpsDeployments.length}
                      </p>
                      <p className={`mt-1 text-xs ${scalpTextSecondaryClass}`}>
                        enabled deployments passing forward gate
                      </p>
                    </article>
                    <article className={scalpCardClass}>
                      <p className={`text-[11px] uppercase tracking-[0.16em] ${scalpTextMutedClass}`}>Abs Correlation</p>
                      <p className={`mt-2 text-2xl font-semibold ${scalpTextPrimaryClass}`}>
                        {scalpAvgAbsPairCorrelation === null ? '—' : scalpAvgAbsPairCorrelation.toFixed(2)}
                      </p>
                      <p className={`mt-1 text-xs ${scalpTextSecondaryClass}`}>
                        portfolio overlap from research report
                      </p>
                    </article>
                  </div>
                </section>

                <section className="grid grid-cols-1 gap-5 2xl:grid-cols-[1.15fr_1fr]">
                  <article className={`${scalpSectionShellClass} p-4`}>
                    <div className="flex items-center justify-between">
                      <h3 className={`text-lg font-semibold ${scalpTextPrimaryClass}`}>Cron Execution Pipeline</h3>
                      <span className={scalpTagNeutralClass}>timeout-safe chunks</span>
                    </div>
                    <div className="mt-4 overflow-x-auto">
                      <table className="w-full min-w-[1140px] border-separate border-spacing-y-2 text-left text-sm">
                        <thead className={`text-[11px] uppercase tracking-[0.16em] ${scalpTableHeaderClass}`}>
                          <tr>
                            <th className="px-3 py-1">Cron</th>
                            <th className="px-3 py-1">Expression</th>
                            <th className="px-3 py-1">Next Run</th>
                            <th className="px-3 py-1">Role</th>
                            <th className="px-3 py-1">Last</th>
                            <th className="px-3 py-1">Last Duration</th>
                            <th className="px-3 py-1">Status</th>
                            <th className="px-3 py-1">Action</th>
                          </tr>
                        </thead>
                        <tbody>
                          {scalpCronRows.map((row) => {
                            const isExpanded = scalpExpandedCronId === row.id;
                            const invokeState = scalpCronInvokeStateById[row.id] || null;
                            const invokeButtonDisabled = !row.invokePath || Boolean(invokeState?.running);
                            const rowLastDurationMs =
                              (typeof invokeState?.durationMs === 'number' && Number.isFinite(invokeState.durationMs)
                                ? invokeState.durationMs
                                : null) ??
                              row.lastDurationMs;
                            return (
                              <React.Fragment key={row.id}>
                                <tr
                                  onClick={() => {
                                    const nextExpandedId = scalpExpandedCronId === row.id ? null : row.id;
                                    setScalpExpandedCronId(nextExpandedId);
                                    if (nextExpandedId === 'scalp_cycle_worker') {
                                      void loadScalpWorkerTasksFull();
                                    }
                                  }}
                                  className={`cursor-pointer transition ${scalpTableRowClass}`}
                                >
                                  <td className={`rounded-l-xl px-3 py-3 font-medium ${scalpTextPrimaryClass}`}>
                                    <div className="flex items-center gap-2">
                                      <span>{row.id}</span>
                                      <span
                                        className={`rounded-full border px-1.5 py-0.5 text-[10px] ${
                                          scalpDarkMode
                                            ? 'border-zinc-600 bg-zinc-800 text-zinc-300'
                                            : 'border-slate-200 bg-white text-slate-500'
                                        }`}
                                      >
                                        {isExpanded ? 'hide' : 'open'}
                                      </span>
                                    </div>
                                  </td>
                                  <td className={`px-3 py-3 font-mono text-xs ${scalpTextSecondaryClass}`}>
                                    {row.cronExpression || row.cadence}
                                  </td>
                                  <td className={`px-3 py-3 ${scalpTextSecondaryClass}`}>
                                    <span title={formatScalpTime(row.nextRunAtMs)}>
                                      {formatScalpNextRunIn(row.nextRunAtMs, scalpCronNowMs)}
                                    </span>
                                  </td>
                                  <td className={`px-3 py-3 ${scalpTextSecondaryClass}`}>{row.role}</td>
                                  <td className={`px-3 py-3 ${scalpTextSecondaryClass}`}>
                                    {formatScalpTime(row.lastRunAtMs)}
                                  </td>
                                  <td className={`px-3 py-3 font-mono text-xs ${scalpTextSecondaryClass}`}>
                                    {formatScalpDuration(rowLastDurationMs)}
                                  </td>
                                  <td className="px-3 py-3">
                                    <span
                                      className={`rounded-full border px-2 py-1 text-[11px] ${scalpCronStatusMeta(
                                        row.status,
                                      )}`}
                                    >
                                      {row.status}
                                    </span>
                                  </td>
                                  <td className="rounded-r-xl px-3 py-3">
                                    <button
                                      type="button"
                                      onClick={(event) => {
                                        event.stopPropagation();
                                        void invokeScalpCronNow(row);
                                      }}
                                      disabled={invokeButtonDisabled}
                                      className={`rounded-full border px-2.5 py-1 text-[11px] font-medium transition ${
                                        scalpDarkMode
                                          ? 'border-zinc-600 bg-zinc-800 text-zinc-200 hover:bg-zinc-700 disabled:cursor-not-allowed disabled:opacity-50'
                                          : 'border-slate-300 bg-white text-slate-700 hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-50'
                                      }`}
                                      title={row.invokePath || 'No invoke path configured'}
                                    >
                                      {invokeState?.running ? 'invoking...' : 'Invoke now()'}
                                    </button>
                                  </td>
                                </tr>
                                {isExpanded ? (
                                  <tr>
                                    <td colSpan={8} className="px-0 pt-0">
                                      <div className={scalpCronExpandedPanelClass}>
                                        <div
                                          className={`mb-2 text-[11px] uppercase tracking-[0.14em] ${scalpTextMutedClass}`}
                                        >
                                          Last execution snapshot
                                        </div>
                                        {invokeState ? (
                                          <div
                                            className={`mb-3 rounded-xl border px-2.5 py-2 text-xs ${
                                              invokeState.ok === true
                                                ? scalpCronDetailToneMeta('positive')
                                                : invokeState.ok === false
                                                  ? scalpCronDetailToneMeta('critical')
                                                  : scalpCronDetailToneMeta('neutral')
                                            }`}
                                          >
                                            <span className="font-semibold">Manual invoke:</span>{' '}
                                            {invokeState.ok === null
                                              ? 'not run'
                                              : invokeState.ok
                                                ? `ok (${invokeState.status || '200'})`
                                                : `failed (${invokeState.status || 'n/a'})`}
                                            {invokeState.durationMs !== null
                                              ? ` · ${formatScalpDuration(invokeState.durationMs)}`
                                              : ''}
                                            {invokeState.message ? ` · ${invokeState.message}` : ''}
                                            {invokeState.atMs ? ` · ${formatScalpTime(invokeState.atMs)}` : ''}
                                          </div>
                                        ) : null}
                                        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-5">
                                          {row.details.map((detail) => (
                                            <div
                                              key={`${row.id}-${detail.label}`}
                                              className={`rounded-xl border px-2.5 py-2 ${scalpCronDetailToneMeta(
                                                detail.tone,
                                              )}`}
                                            >
                                              <div className="text-[10px] uppercase tracking-[0.14em]">
                                                {detail.label}
                                              </div>
                                              <div className="mt-1 text-xs font-semibold">{detail.value}</div>
                                            </div>
                                          ))}
                                        </div>
                                        {row.visualMetrics && row.visualMetrics.length ? (
                                          <div className="mt-3">
                                            <div
                                              className={`mb-1 text-[10px] uppercase tracking-[0.14em] ${scalpTextMutedClass}`}
                                            >
                                              Visual metrics
                                            </div>
                                            <div className="grid grid-cols-1 gap-2 xl:grid-cols-2">
                                              {row.visualMetrics.map((metric) => {
                                                const pct = metric.pct === null ? 0 : Math.max(0, Math.min(100, metric.pct));
                                                return (
                                                  <div
                                                    key={`${row.id}-visual-${metric.label}`}
                                                    className={`rounded-xl border px-2.5 py-2 ${scalpCronDetailToneMeta(metric.tone)}`}
                                                  >
                                                    <div className="flex items-center justify-between text-[11px]">
                                                      <span className="uppercase tracking-[0.12em]">{metric.label}</span>
                                                      <span className="font-semibold">{metric.valueLabel}</span>
                                                    </div>
                                                    <div className={`mt-2 h-1.5 overflow-hidden rounded-full ${scalpVisualMetricTrackClass}`}>
                                                      <div
                                                        className={`h-full rounded-full ${scalpVisualMetricFillMeta(metric.tone)}`}
                                                        style={{ width: `${pct}%` }}
                                                      />
                                                    </div>
                                                  </div>
                                                );
                                              })}
                                            </div>
                                          </div>
                                        ) : null}
                                        {row.id === 'scalp_cycle_worker' ? (
                                          <div className="mt-3">
                                            <div
                                              className={`mb-1 text-[10px] uppercase tracking-[0.14em] ${scalpTextMutedClass}`}
                                            >
                                              Task metrics by symbol, strategy, and window ({scalpWorkerTaskRows.length} shown)
                                            </div>
                                            <div className={`mb-2 text-[11px] ${scalpTextSecondaryClass}`}>
                                              Source: {scalpWorkerCycleSource.replace(/_/g, ' ')} • totals:{' '}
                                              {scalpCycleTotalsSource.replace(/_/g, ' ')}
                                              {scalpWorkerTasksLoadingFull
                                                ? ' • loading full task list...'
                                                : scalpResearchCycleNeedsFullTasks(scalpResearchCycle)
                                                  ? ` • preview capped at ${SCALP_WORKER_TASK_LIMIT_PREVIEW}`
                                                  : ''}
                                            </div>
                                            <div
                                              className={`max-h-80 overflow-auto rounded-xl border ${
                                                scalpDarkMode ? 'border-zinc-700/40' : 'border-slate-200'
                                              }`}
                                            >
                                              <table className="w-full min-w-[1380px] border-separate border-spacing-y-1.5 text-left text-sm">
                                                <thead className={`text-[10px] uppercase tracking-[0.14em] ${scalpTableHeaderClass}`}>
                                                  <tr>
                                                    <th className="px-3 py-1">{renderScalpWorkerSortableHeader('Symbol', 'symbol')}</th>
                                                    <th className="px-3 py-1">{renderScalpWorkerSortableHeader('Strategy', 'strategyId')}</th>
                                                    <th className="px-3 py-1">{renderScalpWorkerSortableHeader('Tune', 'tuneId')}</th>
                                                    <th className="px-3 py-1">{renderScalpWorkerSortableHeader('Why not promoted', 'whyNotPromoted')}</th>
                                                    <th className="px-3 py-1">{renderScalpWorkerSortableHeader('Window', 'windowToTs')}</th>
                                                    <th className="px-3 py-1">{renderScalpWorkerSortableHeader('Status', 'status')}</th>
                                                    <th className="px-3 py-1">{renderScalpWorkerSortableHeader('Trades', 'trades')}</th>
                                                    <th className="px-3 py-1">{renderScalpWorkerSortableHeader('Net R', 'netR')}</th>
                                                    <th className="px-3 py-1">{renderScalpWorkerSortableHeader('Expectancy', 'expectancyR')}</th>
                                                    <th className="px-3 py-1">{renderScalpWorkerSortableHeader('PF', 'profitFactor')}</th>
                                                    <th className="px-3 py-1">{renderScalpWorkerSortableHeader('Max DD', 'maxDrawdownR')}</th>
                                                    <th className="px-3 py-1">Action</th>
                                                  </tr>
                                                </thead>
                                                <tbody>
                                                  {scalpWorkerTaskRows.length ? (
                                                    scalpWorkerTaskRows.map((taskRow) => {
                                                      const retryState =
                                                        scalpWorkerRetryStateByTaskId[taskRow.taskId] || null;
                                                      const canRetryTask =
                                                        taskRow.status === 'failed' &&
                                                        Boolean(taskRow.taskId) &&
                                                        scalpWorkerRetryCycleReady;
                                                      return (
                                                        <tr key={`worker-task-${taskRow.taskId}`} className={scalpTableRowClass}>
                                                          <td className={`rounded-l-xl px-3 py-2.5 font-medium ${scalpTextPrimaryClass}`}>
                                                            {taskRow.symbol}
                                                          </td>
                                                          <td className={`px-3 py-2.5 ${scalpTextSecondaryClass}`}>
                                                            {taskRow.strategyId}
                                                          </td>
                                                          <td className={`px-3 py-2.5 ${scalpTextSecondaryClass}`}>
                                                            {taskRow.tuneId}
                                                          </td>
                                                          <td className={`px-3 py-2.5 ${scalpTextSecondaryClass}`}>
                                                            {String(taskRow.whyNotPromoted || 'unknown').replace(/_/g, ' ')}
                                                          </td>
                                                          <td className={`px-3 py-2.5 ${scalpTextSecondaryClass}`}>
                                                            {formatScalpWindowIso(taskRow.windowFromTs, taskRow.windowToTs)}
                                                          </td>
                                                          <td className={`px-3 py-2.5 ${scalpTextSecondaryClass}`}>
                                                            <span
                                                              className={`rounded-full border px-2 py-1 text-[11px] ${scalpWorkerTaskStatusMeta(
                                                                taskRow.status,
                                                              )}`}
                                                            >
                                                              {taskRow.status || 'pending'}
                                                            </span>
                                                          </td>
                                                          <td className={`px-3 py-2.5 ${scalpTextSecondaryClass}`}>
                                                            {taskRow.trades === null ? '—' : Math.floor(taskRow.trades)}
                                                          </td>
                                                          <td
                                                            className={`px-3 py-2.5 ${
                                                              taskRow.netR === null
                                                                ? scalpTextMutedClass
                                                                : taskRow.netR >= 0
                                                                  ? 'text-emerald-500'
                                                                  : 'text-rose-500'
                                                            }`}
                                                          >
                                                            {taskRow.netR === null
                                                              ? '—'
                                                              : `${taskRow.netR >= 0 ? '+' : ''}${taskRow.netR.toFixed(2)}`}
                                                          </td>
                                                          <td
                                                            className={`px-3 py-2.5 ${
                                                              taskRow.expectancyR === null
                                                                ? scalpTextMutedClass
                                                                : taskRow.expectancyR >= 0
                                                                  ? 'text-emerald-500'
                                                                  : 'text-rose-500'
                                                            }`}
                                                          >
                                                            {taskRow.expectancyR === null
                                                              ? '—'
                                                              : `${taskRow.expectancyR >= 0 ? '+' : ''}${taskRow.expectancyR.toFixed(3)}`}
                                                          </td>
                                                          <td className={`px-3 py-2.5 ${scalpTextSecondaryClass}`}>
                                                            {taskRow.profitFactor === null ? '—' : taskRow.profitFactor.toFixed(2)}
                                                          </td>
                                                          <td className={`px-3 py-2.5 ${scalpTextSecondaryClass}`}>
                                                            {taskRow.maxDrawdownR === null ? '—' : `${taskRow.maxDrawdownR.toFixed(2)}R`}
                                                          </td>
                                                          <td className={`rounded-r-xl px-3 py-2.5 ${scalpTextSecondaryClass}`}>
                                                            {taskRow.status === 'failed' ? (
                                                              <div className="flex items-center gap-2">
                                                                <button
                                                                  type="button"
                                                                  onClick={() => retryScalpWorkerTask(taskRow.taskId)}
                                                                  disabled={!canRetryTask || retryState?.running === true}
                                                                  className={`rounded-md border px-2 py-1 text-[11px] font-medium transition ${
                                                                    !canRetryTask || retryState?.running === true
                                                                      ? scalpDarkMode
                                                                        ? 'cursor-not-allowed border-zinc-700 text-zinc-500'
                                                                        : 'cursor-not-allowed border-slate-200 text-slate-400'
                                                                      : scalpDarkMode
                                                                        ? 'border-amber-500/50 bg-amber-500/10 text-amber-200 hover:bg-amber-500/20'
                                                                        : 'border-amber-300 bg-amber-50 text-amber-700 hover:bg-amber-100'
                                                                  }`}
                                                                >
                                                                  {retryState?.running ? 'Retrying...' : 'Retry'}
                                                                </button>
                                                                {retryState?.ok === false && retryState?.message ? (
                                                                  <span className="max-w-[160px] truncate text-[10px] text-rose-500">
                                                                    {retryState.message}
                                                                  </span>
                                                                ) : null}
                                                                {retryState?.ok === true && retryState?.message ? (
                                                                  <span className="text-[10px] text-emerald-500">
                                                                    {retryState.message}
                                                                  </span>
                                                                ) : null}
                                                              </div>
                                                            ) : (
                                                              '—'
                                                            )}
                                                          </td>
                                                        </tr>
                                                      );
                                                    })
                                                  ) : (
                                                    <tr className={scalpTableRowClass}>
                                                      <td colSpan={12} className={`rounded-xl px-3 py-4 text-sm ${scalpTextSecondaryClass}`}>
                                                        {scalpResearchCycle
                                                          ? 'Cycle loaded, but no tasks returned yet.'
                                                          : 'No active or completed research cycle found yet. Run scalp_cycle_start first.'}
                                                      </td>
                                                    </tr>
                                                  )}
                                                </tbody>
                                              </table>
                                            </div>
                                          </div>
                                        ) : null}
                                        {row.id === 'scalp_promotion_gate_apply' ? (
                                          <div className="mt-3">
                                            <div
                                              className={`mb-1 text-[10px] uppercase tracking-[0.14em] ${scalpTextMutedClass}`}
                                            >
                                              Materialization shortlist ({scalpMaterializationPreviewRows.length} shown)
                                            </div>
                                            <div className={`mb-2 text-[11px] ${scalpTextSecondaryClass}`}>
                                              Source: {scalpMaterializationSource || '—'} • top K per symbol:{' '}
                                              {scalpMaterializationTopKPerSymbol === null
                                                ? '—'
                                                : Math.max(1, Math.floor(scalpMaterializationTopKPerSymbol))}{' '}
                                              • reason: {scalpPromotionSyncReason || 'none'}
                                            </div>
                                            <div
                                              className={`max-h-80 overflow-auto rounded-xl border ${
                                                scalpDarkMode ? 'border-zinc-700/40' : 'border-slate-200'
                                              }`}
                                            >
                                              <table className="w-full min-w-[1100px] border-separate border-spacing-y-1.5 text-left text-sm">
                                                <thead className={`text-[10px] uppercase tracking-[0.14em] ${scalpTableHeaderClass}`}>
                                                  <tr>
                                                    <th className="px-3 py-1">Symbol</th>
                                                    <th className="px-3 py-1">Strategy</th>
                                                    <th className="px-3 py-1">Tune</th>
                                                    <th className="px-3 py-1">Deployment</th>
                                                    <th className="px-3 py-1">In Registry</th>
                                                    <th className="px-3 py-1">Created</th>
                                                    <th className="px-3 py-1">Source</th>
                                                  </tr>
                                                </thead>
                                                <tbody>
                                                  {scalpMaterializationPreviewRows.length ? (
                                                    scalpMaterializationPreviewRows.map((materializedRow) => (
                                                      <tr
                                                        key={`materialization-row-${materializedRow.deploymentId}`}
                                                        className={scalpTableRowClass}
                                                      >
                                                        <td className={`rounded-l-xl px-3 py-2.5 font-medium ${scalpTextPrimaryClass}`}>
                                                          {materializedRow.symbol}
                                                        </td>
                                                        <td className={`px-3 py-2.5 ${scalpTextSecondaryClass}`}>
                                                          {materializedRow.strategyId}
                                                        </td>
                                                        <td className={`px-3 py-2.5 ${scalpTextSecondaryClass}`}>
                                                          {materializedRow.tuneId}
                                                        </td>
                                                        <td className={`px-3 py-2.5 font-mono text-[11px] ${scalpTextSecondaryClass}`}>
                                                          {materializedRow.deploymentId}
                                                        </td>
                                                        <td className={`px-3 py-2.5 ${scalpTextSecondaryClass}`}>
                                                          <span
                                                            className={`rounded-full border px-2 py-1 text-[11px] ${
                                                              materializedRow.exists
                                                                ? scalpDarkMode
                                                                  ? 'border-emerald-400/40 bg-emerald-500/10 text-emerald-200'
                                                                  : 'border-emerald-200 bg-emerald-50 text-emerald-700'
                                                                : scalpDarkMode
                                                                  ? 'border-amber-400/40 bg-amber-500/10 text-amber-200'
                                                                  : 'border-amber-200 bg-amber-50 text-amber-700'
                                                            }`}
                                                          >
                                                            {materializedRow.exists ? 'yes' : 'missing'}
                                                          </span>
                                                        </td>
                                                        <td className={`px-3 py-2.5 ${scalpTextSecondaryClass}`}>
                                                          <span
                                                            className={`rounded-full border px-2 py-1 text-[11px] ${
                                                              materializedRow.created
                                                                ? scalpDarkMode
                                                                  ? 'border-emerald-400/40 bg-emerald-500/10 text-emerald-200'
                                                                  : 'border-emerald-200 bg-emerald-50 text-emerald-700'
                                                                : scalpDarkMode
                                                                  ? 'border-zinc-600 bg-zinc-800 text-zinc-300'
                                                                  : 'border-slate-200 bg-slate-100 text-slate-600'
                                                            }`}
                                                          >
                                                            {materializedRow.created ? 'yes' : 'no'}
                                                          </span>
                                                        </td>
                                                        <td className={`rounded-r-xl px-3 py-2.5 ${scalpTextSecondaryClass}`}>
                                                          {materializedRow.source || '—'}
                                                        </td>
                                                      </tr>
                                                    ))
                                                  ) : (
                                                    <tr className={scalpTableRowClass}>
                                                      <td colSpan={7} className={`rounded-xl px-3 py-4 text-sm ${scalpTextSecondaryClass}`}>
                                                        {scalpPromotionSyncSnapshot
                                                          ? 'No shortlist rows returned yet for the latest sync preview.'
                                                          : 'No sync preview loaded yet. Wait for the next research refresh cycle.'}
                                                      </td>
                                                    </tr>
                                                  )}
                                                </tbody>
                                              </table>
                                            </div>
                                          </div>
                                        ) : null}
                                        {row.resultPreview ? (
                                          <div className="mt-3">
                                            <div
                                              className={`mb-1 text-[10px] uppercase tracking-[0.14em] ${scalpTextMutedClass}`}
                                            >
                                              Result payload
                                            </div>
                                            <pre className={scalpCronPreviewClass}>
                                              {JSON.stringify(row.resultPreview, null, 2)}
                                            </pre>
                                          </div>
                                        ) : null}
                                        {row.id === 'scalp_discover_symbols' ? (
                                          <div className="mt-3">
                                            <div
                                              className={`mb-1 text-[10px] uppercase tracking-[0.14em] ${scalpTextMutedClass}`}
                                            >
                                              Symbol discovery flow
                                            </div>
                                            <div className="overflow-x-auto">
                                              <table className="w-full min-w-[860px] border-separate border-spacing-y-2 text-left text-sm">
                                                <thead
                                                  className={`text-[11px] uppercase tracking-[0.16em] ${scalpTableHeaderClass}`}
                                                >
                                                  <tr>
                                                    <th className="px-3 py-1">Symbol</th>
                                                    <th className="px-3 py-1">Discovered</th>
                                                    <th className="px-3 py-1">Data Import</th>
                                                    <th className="px-3 py-1">Evaluation</th>
                                                    <th className="px-3 py-1">Notes</th>
                                                  </tr>
                                                </thead>
                                                <tbody>
                                                  {scalpUniversePipelineRows.length ? (
                                                    scalpUniversePipelineRows.map((pipelineRow) => (
                                                      <tr key={`universe-flow-${pipelineRow.symbol}`} className={scalpTableRowClass}>
                                                        <td className={`rounded-l-xl px-3 py-3 font-medium ${scalpTextPrimaryClass}`}>
                                                          {pipelineRow.symbol}
                                                        </td>
                                                        <td className={`px-3 py-3 ${scalpTextSecondaryClass}`}>
                                                          <span
                                                            className={`rounded-full border px-2 py-1 text-[11px] ${
                                                              pipelineRow.discovered
                                                                ? scalpDarkMode
                                                                  ? 'border-emerald-400/40 bg-emerald-500/10 text-emerald-200'
                                                                  : 'border-emerald-200 bg-emerald-50 text-emerald-700'
                                                                : scalpDarkMode
                                                                ? 'border-zinc-600 bg-zinc-800/70 text-zinc-300'
                                                                : 'border-slate-200 bg-slate-100 text-slate-600'
                                                            }`}
                                                          >
                                                            {pipelineRow.discovered ? 'selected' : 'not selected'}
                                                          </span>
                                                        </td>
                                                        <td className={`px-3 py-3 ${scalpTextSecondaryClass}`}>
                                                          <div className="flex flex-wrap items-center gap-2">
                                                            <span
                                                              className={`rounded-full border px-2 py-1 text-[11px] ${scalpImportStatusMeta(
                                                                pipelineRow.importStatus,
                                                              )}`}
                                                            >
                                                              {pipelineRow.importStatus}
                                                            </span>
                                                            {typeof pipelineRow.importAddedCount === 'number' &&
                                                            pipelineRow.importAddedCount > 0 ? (
                                                              <span className={scalpTagNeutralClass}>
                                                                {`+${Math.floor(pipelineRow.importAddedCount)} bars`}
                                                              </span>
                                                            ) : null}
                                                          </div>
                                                        </td>
                                                        <td className={`px-3 py-3 ${scalpTextSecondaryClass}`}>
                                                          <div className="flex flex-wrap items-center gap-2">
                                                            <span
                                                              className={`rounded-full border px-2 py-1 text-[11px] ${scalpEvaluationMeta(
                                                                pipelineRow.eligible,
                                                              )}`}
                                                            >
                                                              {!pipelineRow.evaluated
                                                                ? 'pending'
                                                                : pipelineRow.eligible
                                                                  ? 'eligible'
                                                                  : 'ineligible'}
                                                            </span>
                                                            <span className={scalpTagNeutralClass}>
                                                              {pipelineRow.score === null
                                                                ? 'score —'
                                                                : `score ${pipelineRow.score.toFixed(2)}`}
                                                            </span>
                                                          </div>
                                                        </td>
                                                        <td className={`rounded-r-xl px-3 py-3 text-xs ${scalpTextSecondaryClass}`}>
                                                          {pipelineRow.reasons.length
                                                            ? pipelineRow.reasons.slice(0, 3).join(', ')
                                                            : pipelineRow.importReason ||
                                                              (pipelineRow.evaluated
                                                                ? 'passed filters'
                                                                : 'awaiting evaluation')}
                                                        </td>
                                                      </tr>
                                                    ))
                                                  ) : (
                                                    <tr className={scalpTableRowClass}>
                                                      <td
                                                        colSpan={5}
                                                        className={`rounded-xl px-3 py-4 text-sm ${scalpTextSecondaryClass}`}
                                                      >
                                                        No symbol discovery snapshot loaded yet.
                                                      </td>
                                                    </tr>
                                                  )}
                                                </tbody>
                                              </table>
                                            </div>
                                            <div className="mt-3">
                                              <div
                                                className={`mb-1 text-[10px] uppercase tracking-[0.14em] ${scalpTextMutedClass}`}
                                              >
                                                History Rows ({scalpHistoryPreviewRows.length})
                                              </div>
                                              <div className={`mb-2 text-[11px] ${scalpTextSecondaryClass}`}>
                                                oldest: {formatScalpTime(scalpHistoryOldestCandleAtMs)} • newest:{' '}
                                                {formatScalpTime(scalpHistoryNewestCandleAtMs)}
                                                {scalpHistoryAvgCandles === null || scalpHistoryAvgDepthDays === null
                                                  ? ''
                                                  : ` • avg ${scalpHistoryAvgCandles.toFixed(0)} bars / ${scalpHistoryAvgDepthDays.toFixed(1)}d`}
                                                {scalpHistoryTruncated
                                                  ? ` • scanned first ${formatScalpCount(
                                                      scalpHistoryScannedLimit,
                                                    )} symbols from history store`
                                                  : ''}
                                              </div>
                                              <div className="overflow-x-auto">
                                                <table className="w-full min-w-[760px] border-separate border-spacing-y-1 text-left text-xs">
                                                  <thead
                                                    className={`text-[10px] uppercase tracking-[0.14em] ${scalpTableHeaderClass}`}
                                                  >
                                                    <tr>
                                                      <th className="px-2 py-1">Symbol</th>
                                                      <th className="px-2 py-1">Candles</th>
                                                      <th className="px-2 py-1">Depth</th>
                                                      <th className="px-2 py-1">Density</th>
                                                      <th className="px-2 py-1">Coverage</th>
                                                      <th className="px-2 py-1">Last Candle</th>
                                                      <th className="px-2 py-1">Updated</th>
                                                    </tr>
                                                  </thead>
                                                  <tbody>
                                                    {scalpHistoryPreviewRows.length ? (
                                                      scalpHistoryPreviewRows.map((historyRow) => (
                                                        <tr
                                                          key={`history-row-${historyRow.symbol}`}
                                                          className={scalpTableRowClass}
                                                        >
                                                          <td className={`rounded-l-xl px-2 py-2 font-medium ${scalpTextPrimaryClass}`}>
                                                            {historyRow.symbol}
                                                          </td>
                                                          <td className={`px-2 py-2 ${scalpTextSecondaryClass}`}>
                                                            {formatScalpCount(historyRow.candles)}
                                                          </td>
                                                          <td className={`px-2 py-2 ${scalpTextSecondaryClass}`}>
                                                            {historyRow.depthDays === null ? '—' : `${historyRow.depthDays.toFixed(1)}d`}
                                                          </td>
                                                          <td className={`px-2 py-2 ${scalpTextSecondaryClass}`}>
                                                            {historyRow.barsPerDay === null ? '—' : `${historyRow.barsPerDay.toFixed(0)}/d`}
                                                          </td>
                                                          <td
                                                            className={`px-2 py-2 ${
                                                              historyRow.coveragePct === null
                                                                ? scalpTextMutedClass
                                                                : historyRow.coveragePct >= 95
                                                                  ? 'text-emerald-500'
                                                                  : historyRow.coveragePct >= 80
                                                                    ? 'text-amber-500'
                                                                    : 'text-rose-500'
                                                            }`}
                                                          >
                                                            {formatScalpPct(historyRow.coveragePct, 1)}
                                                          </td>
                                                          <td className={`px-2 py-2 ${scalpTextSecondaryClass}`}>
                                                            {formatScalpTime(historyRow.toTsMs)}
                                                          </td>
                                                          <td className={`rounded-r-xl px-2 py-2 ${scalpTextSecondaryClass}`}>
                                                            {formatScalpTime(historyRow.updatedAtMs)}
                                                          </td>
                                                        </tr>
                                                      ))
                                                    ) : (
                                                      <tr className={scalpTableRowClass}>
                                                        <td colSpan={7} className={`rounded-xl px-2 py-3 ${scalpTextSecondaryClass}`}>
                                                          No candle-history rows found yet for the selected timeframe.
                                                        </td>
                                                      </tr>
                                                    )}
                                                  </tbody>
                                                </table>
                                              </div>
                                            </div>
                                          </div>
                                        ) : null}
                                      </div>
                                    </td>
                                  </tr>
                                ) : null}
                              </React.Fragment>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  </article>

                  <article className={`${scalpSectionShellClass} p-4`}>
                    <div className="flex items-center justify-between">
                      <h3 className={`text-lg font-semibold ${scalpTextPrimaryClass}`}>Gate Health</h3>
                      <span className={scalpTagNeutralClass}>strict mode</span>
                    </div>
                    <div className="mt-4 space-y-3">
                      <div className={scalpCardClass}>
                        <p className={`text-[11px] uppercase tracking-[0.16em] ${scalpTextMutedClass}`}>Forward Profitable Windows</p>
                        <p className={`mt-2 text-2xl font-semibold ${scalpTextPrimaryClass}`}>
                          {scalpMeanForwardProfitablePct === null
                            ? '—'
                            : `${scalpMeanForwardProfitablePct.toFixed(1)}%`}
                        </p>
                        <p className={`mt-1 text-xs ${scalpTextSecondaryClass}`}>
                          average profitable-window percentage across validated deployments
                        </p>
                      </div>
                      <div className={scalpCardClass}>
                        <p className={`text-[11px] uppercase tracking-[0.16em] ${scalpTextMutedClass}`}>Cycle Status</p>
                        <p className={`mt-2 text-xl font-semibold ${scalpTextPrimaryClass}`}>
                          {String(
                            scalpResearchCycle?.summary?.status ||
                              scalpResearchCycle?.cycle?.status ||
                              scalpResearchReport?.cycle?.status ||
                              'unknown',
                          )
                            .replace(/_/g, ' ')
                            .toUpperCase()}
                        </p>
                        <p className={`mt-1 text-xs ${scalpTextSecondaryClass}`}>
                          {scalpCycleFailed === null ? 'no failure count reported' : `${Math.floor(scalpCycleFailed)} failed task(s)`}
                        </p>
                      </div>
                      <div className={scalpCardClass}>
                        <p className={`text-[11px] uppercase tracking-[0.16em] ${scalpTextMutedClass}`}>Universe Snapshot</p>
                        <p className={`mt-2 text-xl font-semibold ${scalpTextPrimaryClass}`}>
                          {scalpUniverseSelectedCount === null ? '—' : Math.floor(scalpUniverseSelectedCount)} selected
                        </p>
                        <p className={`mt-1 text-xs ${scalpTextSecondaryClass}`}>
                          {scalpUniverseCandidatesEvaluated === null
                            ? 'candidates evaluated unavailable'
                            : `${Math.floor(scalpUniverseCandidatesEvaluated)} candidates evaluated`}
                        </p>
                        <p className={`mt-1 text-xs ${scalpTextSecondaryClass}`}>
                          {`${formatScalpCount(scalpUniverseSeededCount)} imported • ${formatScalpCount(
                            scalpUniverseEvaluatedCount,
                          )} evaluated`}
                        </p>
                        <div className="mt-2 space-y-1">
                          <p className={`text-[10px] ${scalpTextMutedClass}`}>
                            discovered: {scalpUniverseDiscoveredPreview.length ? scalpUniverseDiscoveredPreview.join(', ') : '—'}
                          </p>
                          <p className={`text-[10px] ${scalpTextMutedClass}`}>
                            imported: {scalpUniverseImportedPreview.length ? scalpUniverseImportedPreview.join(', ') : '—'}
                          </p>
                          <p className={`text-[10px] ${scalpTextMutedClass}`}>
                            evaluated: {scalpUniverseEvaluatedPreview.length ? scalpUniverseEvaluatedPreview.join(', ') : '—'}
                          </p>
                        </div>
                      </div>
                      <div className={scalpCardClass}>
                        <p className={`text-[11px] uppercase tracking-[0.16em] ${scalpTextMutedClass}`}>History Discovery</p>
                        <p className={`mt-2 text-xl font-semibold ${scalpTextPrimaryClass}`}>
                          {formatScalpCount(scalpHistoryNonEmptyCount)} /{' '}
                          {formatScalpCount(scalpHistoryScannedCount ?? scalpHistorySymbolCount)} symbols
                        </p>
                        <p className={`mt-1 text-xs ${scalpTextSecondaryClass}`}>
                          {`${formatScalpCount(scalpHistoryTotalCandles)} candles • median ${formatScalpCount(
                            scalpHistoryMedianCandles,
                          )} • tf ${scalpHistoryTimeframe} • ${scalpHistoryBackend}`}
                        </p>
                        <p className={`mt-1 text-xs ${scalpTextSecondaryClass}`}>
                          {`median depth ${
                            scalpHistoryMedianDepthDays === null ? '—' : `${scalpHistoryMedianDepthDays.toFixed(1)}d`
                          } • coverage ${formatScalpPct(scalpHistoryCoveragePct, 1)}`}
                        </p>
                        {scalpHistoryTruncated ? (
                          <p className={`mt-1 text-[10px] ${scalpTextMutedClass}`}>
                            scanned first {formatScalpCount(scalpHistoryScannedLimit)} symbols from history store
                          </p>
                        ) : null}
                      </div>
                    </div>
                  </article>
                </section>

                <section className={`${scalpSectionShellClass} p-4`}>
                  <div className="flex items-center justify-between">
                    <h3 className={`text-lg font-semibold ${scalpTextPrimaryClass}`}>Deployment Registry and Forward Validation</h3>
                    <span className={scalpTagNeutralClass}>click row to focus</span>
                  </div>
                  <div className="mt-4 overflow-x-auto">
                    <table className="w-full min-w-[980px] border-separate border-spacing-y-2 text-left text-sm">
                      <thead className={`text-[11px] uppercase tracking-[0.16em] ${scalpTableHeaderClass}`}>
                        <tr>
                          <th className="px-3 py-1">Deployment</th>
                          <th className="px-3 py-1">Symbol</th>
                          <th className="px-3 py-1">Strategy</th>
                          <th className="px-3 py-1">Tune</th>
                          <th className="px-3 py-1">Forward Exp</th>
                          <th className="px-3 py-1">Profitable %</th>
                          <th className="px-3 py-1">Max DD</th>
                          <th className="px-3 py-1">Guardrail</th>
                          <th className="px-3 py-1">Promotion</th>
                        </tr>
                      </thead>
                      <tbody>
                        {scalpOpsDeployments.map((row) => {
                          const isActive = scalpActiveOpsRow?.deploymentId === row.deploymentId;
                          const forwardExp = asFiniteNumber(row.forwardValidation?.meanExpectancyR);
                          const profitablePct = asFiniteNumber(row.forwardValidation?.profitableWindowPct);
                          const maxDd =
                            asFiniteNumber(row.forwardValidation?.maxDrawdownR) ?? row.perf30dMaxDrawdownR;
                          const guardrail = row.promotionReason || 'none';
                          return (
                            <tr
                              key={row.deploymentId}
                              onClick={() => setScalpActiveDeploymentId(row.deploymentId)}
                              className={`cursor-pointer transition ${scalpTableRowClass} ${
                                isActive ? (scalpDarkMode ? 'ring-2 ring-zinc-500/70' : 'ring-2 ring-slate-300') : ''
                              }`}
                            >
                              <td className={`rounded-l-xl px-3 py-3 font-medium ${scalpTextPrimaryClass}`}>{row.deploymentId}</td>
                              <td className={`px-3 py-3 ${scalpTextSecondaryClass}`}>{row.symbol}</td>
                              <td className={`px-3 py-3 ${scalpTextSecondaryClass}`}>{row.strategyId}</td>
                              <td className={`px-3 py-3 ${scalpTextSecondaryClass}`}>{row.tuneId}</td>
                              <td className={`px-3 py-3 ${forwardExp === null ? scalpTextMutedClass : forwardExp >= 0 ? 'text-emerald-500' : 'text-rose-500'}`}>
                                {forwardExp === null ? '—' : `${forwardExp >= 0 ? '+' : ''}${forwardExp.toFixed(2)}`}
                              </td>
                              <td className={`px-3 py-3 ${scalpTextSecondaryClass}`}>
                                {profitablePct === null ? '—' : `${profitablePct.toFixed(1)}%`}
                              </td>
                              <td className={`px-3 py-3 ${scalpTextSecondaryClass}`}>
                                {maxDd === null ? '—' : `${maxDd.toFixed(2)}R`}
                              </td>
                              <td className={`px-3 py-3 ${scalpTextSecondaryClass}`}>{guardrail}</td>
                              <td className="rounded-r-xl px-3 py-3">
                                <span
                                  className={`rounded-full border px-2 py-1 text-[11px] ${
                                    row.promotionEligible
                                      ? scalpDarkMode
                                        ? 'border-emerald-300/40 bg-emerald-300/15 text-emerald-200'
                                        : 'border-emerald-200 bg-emerald-50 text-emerald-700'
                                      : scalpDarkMode
                                      ? 'border-rose-300/40 bg-rose-300/15 text-rose-200'
                                      : 'border-rose-200 bg-rose-50 text-rose-700'
                                  }`}
                                >
                                  {row.promotionEligible ? 'eligible' : 'blocked'}
                                </span>
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </section>

                <section className="grid grid-cols-1 gap-5 2xl:grid-cols-[1.15fr_1fr]">
                  <article className={`${scalpSectionShellClass} p-4`}>
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <h3 className={`text-lg font-semibold ${scalpTextPrimaryClass}`}>Active Deployment Focus</h3>
                      <span className={`text-xs ${scalpTextSecondaryClass}`}>
                        {scalpActiveRuntimeRow ? scalpActiveRuntimeRow.symbol : scalpActiveOpsRow?.symbol || '—'}
                      </span>
                    </div>
                    {scalpActiveRuntimeRow ? (
                      <>
                        <div className="mt-3 flex flex-wrap items-center gap-2">
                          {(() => {
                            const state = scalpStateMeta(scalpActiveRuntimeRow.state);
                            const StateIcon = state.Icon;
                            const mode = scalpModeMeta(scalpActiveRuntimeRow.dryRunLast);
                            return (
                              <>
                                <span className={`inline-flex items-center gap-1 rounded-full border px-2 py-1 text-xs font-semibold ${state.className}`}>
                                  <StateIcon className="h-3.5 w-3.5" />
                                  {state.label}
                                </span>
                                <span className={`rounded-full border px-2 py-1 text-xs font-semibold ${mode.className}`}>
                                  {mode.label}
                                </span>
                              </>
                            );
                          })()}
                          <span className={scalpTagNeutralClass}>
                            {scalpActiveRuntimeRow.cronSchedule || scalpActiveRuntimeRow.cronRoute || 'no schedule'}
                          </span>
                        </div>
                        <div className="mt-4 grid grid-cols-2 gap-3 md:grid-cols-4">
                          <div className={scalpCardClass}>
                            <div className={`text-[11px] uppercase tracking-[0.14em] ${scalpTextMutedClass}`}>Trades</div>
                            <div className={`mt-1 text-xl font-semibold ${scalpTextPrimaryClass}`}>{scalpActiveRuntimeRow.tradesPlaced}</div>
                          </div>
                          <div className={scalpCardClass}>
                            <div className={`text-[11px] uppercase tracking-[0.14em] ${scalpTextMutedClass}`}>Win Rate</div>
                            <div className="mt-1 text-xl font-semibold text-emerald-500">
                              {scalpActiveWinRatePct === null ? '—' : `${scalpActiveWinRatePct.toFixed(0)}%`}
                            </div>
                          </div>
                          <div className={scalpCardClass}>
                            <div className={`text-[11px] uppercase tracking-[0.14em] ${scalpTextMutedClass}`}>Net R</div>
                            <div className={`mt-1 text-xl font-semibold ${scalpActiveNetR === null ? scalpTextPrimaryClass : scalpActiveNetR >= 0 ? 'text-emerald-500' : 'text-rose-500'}`}>
                              {scalpActiveNetR === null ? '—' : formatSignedR(scalpActiveNetR)}
                            </div>
                          </div>
                          <div className={scalpCardClass}>
                            <div className={`text-[11px] uppercase tracking-[0.14em] ${scalpTextMutedClass}`}>Last Run</div>
                            <div className={`mt-1 text-xl font-semibold ${scalpTextPrimaryClass}`}>
                              {formatScalpTime(scalpActiveExecutionTs ?? scalpActiveRuntimeRow.lastRunAtMs)}
                            </div>
                          </div>
                        </div>
                        <div className={`mt-3 text-xs ${scalpTextSecondaryClass}`}>
                          Deployment ID: <span className="font-mono">{scalpActiveRuntimeRow.deploymentId}</span>
                        </div>
                      </>
                    ) : (
                      <div className={`mt-4 text-sm ${scalpTextSecondaryClass}`}>No runtime state found for the selected deployment.</div>
                    )}
                  </article>

                  <article className="space-y-4">
                    <div className={`${scalpSectionShellClass} p-4`}>
                      <div className="flex items-center justify-between">
                        <div className={`text-xs uppercase tracking-[0.16em] ${scalpTextMutedClass}`}>
                          Reason Snapshot{scalpActiveRuntimeRow ? ` · ${scalpActiveRuntimeRow.symbol}` : ''}
                        </div>
                        <div className={`text-xs ${scalpTextMutedClass}`}>
                          {scalpReasonSnapshotState === 'fresh'
                            ? `${Math.min(scalpActiveReasonCodes.length, 8)} shown`
                            : 'none'}
                        </div>
                      </div>
                      {scalpActiveReasonCodes.length ? (
                        <div className="mt-3 flex flex-wrap gap-2">
                          {scalpActiveReasonCodes.slice(0, 8).map((code, idx) => {
                            const meta = scalpReasonMeta(code);
                            const Icon = meta.Icon;
                            return (
                              <span
                                key={`${code}-${idx}`}
                                className={`inline-flex items-center gap-1 rounded-full border px-2 py-1 text-[11px] font-semibold ${meta.className}`}
                              >
                                <Icon className="h-3.5 w-3.5" />
                                {code.replace(/_/g, ' ')}
                              </span>
                            );
                          })}
                        </div>
                      ) : (
                        <div className={`mt-3 text-sm ${scalpTextSecondaryClass}`}>No reason codes recorded for this deployment.</div>
                      )}
                    </div>

                    <div className={`${scalpSectionShellClass} p-4`}>
                      <div className="flex items-center justify-between">
                        <div className={`text-xs uppercase tracking-[0.16em] ${scalpTextMutedClass}`}>
                          Journal Snapshot{scalpActiveRuntimeRow ? ` · ${scalpActiveRuntimeRow.symbol}` : ''}
                        </div>
                        <div className={`text-xs ${scalpTextMutedClass}`}>
                          {scalpActiveJournal.length ? `${Math.min(scalpActiveJournal.length, 8)} events` : 'empty'}
                        </div>
                      </div>
                      {scalpActiveJournal.length ? (
                        <div className="mt-3 space-y-2">
                          {scalpActiveJournal.slice(0, 8).map((entry) => {
                            const meta = scalpJournalMeta({
                              type: String(entry.type || ''),
                              level: String(entry.level || ''),
                            });
                            const Icon = meta.Icon;
                            return (
                              <div
                                key={entry.id || `${entry.timestampMs}-${entry.symbol || 'na'}`}
                                className={`rounded-xl border px-3 py-2 text-xs ${meta.className}`}
                              >
                                <div className="flex flex-wrap items-center justify-between gap-2">
                                  <div className="inline-flex items-center gap-1.5 font-semibold">
                                    <Icon className="h-3.5 w-3.5" />
                                    {String(entry.type || 'event').toUpperCase()}
                                  </div>
                                  <div>{formatScalpTime(entry.timestampMs)}</div>
                                </div>
                                {(entry.reasonCodes || []).length ? (
                                  <div className="mt-1 flex flex-wrap gap-1">
                                    {(entry.reasonCodes || []).slice(0, 3).map((code, idx) => (
                                      <span
                                        key={`${entry.id || entry.timestampMs || idx}-${code}-${idx}`}
                                        className="rounded-full border border-current/30 bg-white/50 px-1.5 py-0.5 text-[10px] font-semibold"
                                      >
                                        {code.replace(/_/g, ' ')}
                                      </span>
                                    ))}
                                  </div>
                                ) : null}
                              </div>
                            );
                          })}
                        </div>
                      ) : (
                        <div className={`mt-3 text-sm ${scalpTextSecondaryClass}`}>No journal events for this deployment yet.</div>
                      )}
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
                      <div className="text-xs uppercase tracking-wide text-slate-500">{dashboardRange} PnL</div>
                      <div className="mt-3 text-3xl font-semibold text-slate-900">
                        <span
                          className={
                            typeof effectivePnl7dWithOpen === 'number'
                              ? effectivePnl7dWithOpen >= 0
                                ? 'text-emerald-600'
                                : 'text-rose-600'
                              : 'text-slate-500'
                          }
                        >
                          {typeof effectivePnl7dWithOpen === 'number'
                            ? `${effectivePnl7dWithOpen.toFixed(2)}%`
                            : typeof current.pnl7d === 'number'
                            ? `${current.pnl7d.toFixed(2)}%`
                            : '—'}
                          {typeof current.pnl7dNet === 'number' ? (
                            <span className="ml-1 align-middle text-sm font-medium text-slate-500">
                              ({current.pnl7dNet >= 0 ? '+' : ''}
                              {formatUsd(current.pnl7dNet)})
                            </span>
                          ) : null}
                        </span>
                      </div>
                      <p className="mt-1 text-xs text-slate-500">
                        from {current.pnl7dTrades ?? 0} {current.pnl7dTrades === 1 ? 'trade' : 'trades'} in last{' '}
                        {dashboardRangeText}
                        {typeof effectiveOpenPnl === 'number' ? ' + open position' : ''}
                      </p>
                      <div className="mt-1 text-[11px] text-slate-500">
                        {typeof current.pnl7dGross === 'number' || typeof current.pnl7d === 'number' ? (
                          <>
                            gross vs net:{' '}
                            <span className="font-semibold text-slate-700">
                              {typeof current.pnl7dGross === 'number' ? current.pnl7dGross.toFixed(2) : '—'}%
                            </span>{' '}
                            /{' '}
                            <span className="font-semibold text-slate-700">
                              {typeof current.pnl7d === 'number' ? current.pnl7d.toFixed(2) : '—'}%
                            </span>
                          </>
                        ) : null}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs uppercase tracking-wide text-slate-500">Last PNL</div>
                      <div className="mt-3 text-3xl font-semibold text-slate-900">
                        <span
                          className={
                            typeof current.lastPositionPnl === 'number'
                              ? current.lastPositionPnl >= 0
                                ? 'text-emerald-600'
                                : 'text-rose-600'
                              : 'text-slate-500'
                          }
                        >
                          {typeof current.lastPositionPnl === 'number'
                            ? `${current.lastPositionPnl.toFixed(2)}%`
                            : '—'}
                        </span>
                      </div>
                      <p className="mt-1 text-xs text-slate-500">
                        {typeof current.lastPositionPnl === 'number' ? (
                          <span className="flex flex-wrap items-center gap-2">
                            <span className="flex items-center gap-1">
                              direction –
                              {current.lastPositionDirection ? (
                                <span
                                  className={`${
                                    current.lastPositionDirection === 'long' ? 'text-emerald-600' : 'text-rose-600'
                                  }`}
                                >
                                  {current.lastPositionDirection}
                                </span>
                              ) : null}
                            </span>
                            {typeof current.lastPositionLeverage === 'number' ? (
                              <span className="rounded-full bg-slate-100 px-2 py-0.5 text-[11px] font-semibold text-slate-700">
                                {current.lastPositionLeverage.toFixed(0)}x
                              </span>
                            ) : null}
                          </span>
                        ) : (
                          'no recent positions'
                        )}
                      </p>
                      {typeof current.winRate === 'number' || typeof current.avgWinPct === 'number' || typeof current.avgLossPct === 'number' ? (
                        <div className="mt-2 text-[11px] text-slate-500">
                          {typeof current.winRate === 'number' ? `Win rate: ${current.winRate.toFixed(0)}%` : ''}
                          {typeof current.avgWinPct === 'number' ? ` · Avg win: ${current.avgWinPct.toFixed(2)}%` : ''}
                          {typeof current.avgLossPct === 'number' ? ` · Avg loss: ${current.avgLossPct.toFixed(2)}%` : ''}
                        </div>
                      ) : null}
                    </div>
                    <div>
                      <div className="text-xs uppercase tracking-wide text-slate-500">Open PNL</div>
                      <div className="mt-3 text-3xl font-semibold text-slate-900">
                        <span
                          className={
                            typeof effectiveOpenPnl === 'number'
                              ? effectiveOpenPnl >= 0
                                ? 'text-emerald-600'
                                : 'text-rose-600'
                              : 'text-slate-500'
                          }
                        >
                          {typeof effectiveOpenPnl === 'number' ? `${effectiveOpenPnl.toFixed(2)}%` : '—'}
                        </span>
                      </div>
                      <p className="mt-1 text-xs text-slate-500">
                        {typeof effectiveOpenPnl === 'number' ? (
                          <span className="flex flex-wrap items-center gap-2">
                            <span className="flex items-center gap-1">
                              direction –
                              {current.openDirection ? (
                                <span className={current.openDirection === 'long' ? 'text-emerald-600' : 'text-rose-600'}>
                                  {current.openDirection}
                                </span>
                              ) : null}
                            </span>
                            {typeof current.openLeverage === 'number' ? (
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
                          'no open position'
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
                  isDark={resolvedTheme === 'dark'}
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
                    Action:{' '}
                    <span className="font-semibold text-sky-700">
                      {((current.lastDecision as any)?.action || '').toString() || '—'}
                    </span>
                    {(current.lastDecision as any)?.summary ? ` · ${(current.lastDecision as any).summary}` : ''}
                  </div>
                  {(current.lastDecision as any)?.reason ? (
                    <p className="mt-2 text-sm text-slate-700 whitespace-pre-wrap">
                      <span className="font-semibold text-slate-800">Reason: </span>
                      {(current.lastDecision as any).reason}
                    </p>
                  ) : null}
                  <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-4">
                    {biasOrder.map(({ key, label }) => {
                      const raw = (current.lastDecision as any)?.[key];
                      const val = typeof raw === 'string' ? raw.toUpperCase() : raw;
                      const tfLabel = current.lastBiasTimeframes?.[key.replace('_bias', '')] || null;
                      const displayLabel = tfLabel ? `${label} (${tfLabel})` : label;
                      const meta =
                        val === 'UP'
                          ? { color: 'text-emerald-600', Icon: ArrowUpRight }
                          : val === 'DOWN'
                          ? { color: 'text-rose-600', Icon: ArrowDownRight }
                          : { color: 'text-slate-500', Icon: Circle };
                      const Icon = meta.Icon;
                      return (
                        <div
                          key={key}
                          className="flex items-center justify-between rounded-lg border border-slate-100 bg-slate-50 px-3 py-2"
                        >
                          <span className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                            {displayLabel}
                          </span>
                          <span className={`flex items-center gap-1 text-sm font-semibold ${meta.color}`}>
                            <Icon className="h-4 w-4" />
                            {val || '—'}
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
                      {showPrompt ? 'Hide prompt' : 'Show prompt'}
                    </button>
                  </div>
                  {showPrompt && (
                    <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                        <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">System</div>
                        <div className="mt-2">{renderPromptContent(current.lastPrompt?.system)}</div>
                      </div>
                      <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                        <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">User</div>
                        <div className="mt-2">{renderPromptContent(current.lastPrompt?.user)}</div>
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
                      <span className="lowercase text-slate-400">{formatDecisionTime(current.evaluationTs)}</span>
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
                    Rating: <span className="text-sky-600">{current?.evaluation?.overall_rating ?? '—'}</span>
                  </span>
                  <div className="flex flex-wrap items-center gap-1">
                    {Array.from({ length: 10 }).map((_, idx) => {
                      const ratingVal = Number(current?.evaluation?.overall_rating ?? 0);
                      const filled = ratingVal >= idx + 1;
                      const colorClass =
                        ratingVal >= 9
                          ? 'text-emerald-500 fill-emerald-500'
                          : ratingVal >= 8
                          ? 'text-emerald-400 fill-emerald-400'
                          : ratingVal >= 6
                          ? 'text-lime-400 fill-lime-400'
                          : ratingVal >= 5
                          ? 'text-amber-400 fill-amber-400'
                          : ratingVal >= 3
                          ? 'text-orange-400 fill-orange-400'
                          : 'text-rose-500 fill-rose-500';
                      return (
                        <Star
                          key={idx}
                          className={`h-4 w-4 ${filled ? colorClass : 'stroke-slate-300 text-slate-300'}`}
                        />
                      );
                    })}
                  </div>
                </div>
                <p className="mt-3 text-sm text-slate-700">
                  {current?.evaluation?.overview || 'No overview provided.'}
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
                            {showAspects ? 'Hide aspect ratings' : 'Show aspect ratings'}
                          </button>
                          <button
                            onClick={() => setShowRawEvaluation((prev) => !prev)}
                            className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 transition hover:border-slate-300"
                          >
                            {showRawEvaluation ? 'Hide raw JSON' : 'Show raw JSON'}
                          </button>
                        </div>
                        {showRawEvaluation && (
                          <pre className="overflow-auto rounded-xl border border-slate-800 bg-slate-900/95 p-3 font-mono text-[11px] leading-snug text-slate-100">
                            {JSON.stringify(current.evaluation, null, 2)}
                          </pre>
                        )}
                        {showAspects && (
                          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                            {Object.entries(current.evaluation.aspects).map(([key, val]) => (
                              <div
                                key={key}
                                className="rounded-xl border border-slate-200 bg-slate-50 p-3 shadow-inner shadow-slate-100"
                              >
                                <div className="flex items-center justify-between">
                                  <div className="flex items-center gap-2">
                                    {(() => {
                                      const meta = aspectMeta[key] || {
                                        Icon: Circle,
                                        color: 'text-slate-600',
                                        bg: 'bg-slate-100',
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
                                    <div className="text-lg font-semibold text-sky-700">{val?.rating ?? '—'}</div>
                                  </div>
                                <p className="mt-2 text-xs text-slate-600">{val?.comment || 'No comment'}</p>
                                {(val?.checks?.length || val?.improvements?.length || val?.findings?.length) && (
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
                                          {val.improvements.map((item, idx) => (
                                            <li key={idx}>{item}</li>
                                          ))}
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
                            ))}
                          </div>
                        )}
                      </>
                    )}

                    {((current?.evaluation?.aspects && showAspects) || !current?.evaluation?.aspects) && hasDetails && (
                      <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3">
                        <div className="text-xs uppercase tracking-wide text-slate-500">Details</div>
                        <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-3">
                          {current.evaluation.what_went_well?.length ? (
                            <div className="rounded-xl border border-emerald-100 bg-emerald-50 p-3">
                              <div className="text-sm font-semibold text-emerald-800">What went well</div>
                              <ul className="mt-2 list-disc space-y-1 pl-4 text-sm text-slate-800">
                                {current.evaluation.what_went_well.map((item, idx) => (
                                  <li key={idx}>{item}</li>
                                ))}
                              </ul>
                            </div>
                          ) : null}
                          {current.evaluation.issues?.length ? (
                            <div className="rounded-xl border border-rose-100 bg-rose-50 p-3">
                              <div className="text-sm font-semibold text-rose-800">Issues</div>
                              <ul className="mt-2 list-disc space-y-1 pl-4 text-sm text-slate-800">
                                {current.evaluation.issues.map((item, idx) => (
                                  <li key={idx}>{item}</li>
                                ))}
                              </ul>
                            </div>
                          ) : null}
                          {current.evaluation.improvements?.length ? (
                            <div className="rounded-xl border border-amber-100 bg-amber-50 p-3">
                              <div className="text-sm font-semibold text-amber-800">Improvements</div>
                              <ul className="mt-2 list-disc space-y-1 pl-4 text-sm text-slate-800">
                                {current.evaluation.improvements.map((item, idx) => (
                                  <li key={idx}>{item}</li>
                                ))}
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
