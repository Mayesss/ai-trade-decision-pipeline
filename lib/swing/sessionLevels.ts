type SessionName = 'asia' | 'london' | 'new_york';

type CandleRow = [number, number, number, number, number, number?];

type SessionSummary = {
  name: SessionName;
  date: string;
  startUtc: string;
  endUtc: string;
  open: number;
  high: number;
  low: number;
  close: number;
  candleCount: number;
};

export type ForexSessionLevelsContext = {
  sourceTimeframe: string;
  sessionsUtc: Record<SessionName, string>;
  currentSession: (SessionSummary & {
    sweptLastSessionHigh: boolean;
    sweptLastSessionLow: boolean;
    reclaimedLastSessionHigh: boolean;
    reclaimedLastSessionLow: boolean;
  }) | null;
  lastCompletedSession: SessionSummary | null;
  latestCompletedBySession: Partial<Record<SessionName, SessionSummary>>;
  priorDay: {
    date: string;
    open: number;
    high: number;
    low: number;
    close: number;
    sweptHigh: boolean;
    sweptLow: boolean;
    reclaimedHigh: boolean;
    reclaimedLow: boolean;
  } | null;
  currentDay: {
    date: string;
    open: number;
    high: number;
    low: number;
    close: number;
  } | null;
  weeklyOpen: number | null;
  nearestLevels: {
    lastSessionHighPips: number | null;
    lastSessionLowPips: number | null;
    priorDayHighPips: number | null;
    priorDayLowPips: number | null;
    weeklyOpenPips: number | null;
  };
  signals: {
    sweptLastSessionHigh: boolean;
    sweptLastSessionLow: boolean;
    sweptPriorDayHigh: boolean;
    sweptPriorDayLow: boolean;
    bullishLiquidityReclaim: boolean;
    bearishLiquidityRejection: boolean;
    midSessionRange: boolean;
  };
};

const SESSION_WINDOWS: Record<SessionName, { startHour: number; endHour: number; label: string }> = {
  asia: { startHour: 0, endHour: 7, label: '00:00-07:00' },
  london: { startHour: 7, endHour: 13, label: '07:00-13:00' },
  new_york: { startHour: 13, endHour: 21, label: '13:00-21:00' },
};

function round(value: number | null | undefined, decimals = 5): number | null {
  if (!Number.isFinite(value as number)) return null;
  const factor = 10 ** decimals;
  return Math.round((value as number) * factor) / factor;
}

function roundPrice(value: number): number {
  return round(value, Math.abs(value) >= 10 ? 3 : 5) ?? value;
}

function isoDate(ms: number): string {
  return new Date(ms).toISOString().slice(0, 10);
}

function dayStartMs(ms: number): number {
  const d = new Date(ms);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

function weekStartMs(ms: number): number {
  const start = dayStartMs(ms);
  const day = new Date(start).getUTCDay();
  const daysSinceMonday = (day + 6) % 7;
  return start - daysSinceMonday * 24 * 60 * 60 * 1000;
}

function normalizeCandle(row: any): CandleRow | null {
  if (!Array.isArray(row)) return null;
  const tsRaw = Number(row[0]);
  const ts = tsRaw > 1e12 ? tsRaw : tsRaw * 1000;
  const open = Number(row[1]);
  const high = Number(row[2]);
  const low = Number(row[3]);
  const close = Number(row[4]);
  const volume = Number(row[5] ?? 0);
  if (![ts, open, high, low, close].every(Number.isFinite)) return null;
  return [ts, open, high, low, close, Number.isFinite(volume) ? volume : 0];
}

function sessionForHour(hour: number): SessionName | null {
  for (const [name, window] of Object.entries(SESSION_WINDOWS) as Array<[SessionName, typeof SESSION_WINDOWS[SessionName]]>) {
    if (hour >= window.startHour && hour < window.endHour) return name;
  }
  return null;
}

function sessionBoundsMs(dateMs: number, session: SessionName) {
  const day = dayStartMs(dateMs);
  const window = SESSION_WINDOWS[session];
  return {
    startMs: day + window.startHour * 60 * 60 * 1000,
    endMs: day + window.endHour * 60 * 60 * 1000,
  };
}

function pipSizeForForexPair(symbol: string): number {
  const upper = String(symbol || '').toUpperCase().replace(/[^A-Z]/g, '');
  return upper.includes('JPY') ? 0.01 : 0.0001;
}

function summarizeCandles(candles: CandleRow[]): Omit<SessionSummary, 'name' | 'date' | 'startUtc' | 'endUtc'> | null {
  if (!candles.length) return null;
  return {
    open: roundPrice(candles[0][1]),
    high: roundPrice(Math.max(...candles.map((c) => c[2]))),
    low: roundPrice(Math.min(...candles.map((c) => c[3]))),
    close: roundPrice(candles[candles.length - 1][4]),
    candleCount: candles.length,
  };
}

function daySummary(candles: CandleRow[], date: string) {
  const summary = summarizeCandles(candles);
  if (!summary) return null;
  return {
    date,
    open: summary.open,
    high: summary.high,
    low: summary.low,
    close: summary.close,
  };
}

function pipsBetween(price: number | null | undefined, level: number | null | undefined, pipSize: number): number | null {
  if (!Number.isFinite(price as number) || !Number.isFinite(level as number) || pipSize <= 0) return null;
  return round(((price as number) - (level as number)) / pipSize, 1);
}

export function buildForexSessionLevelsContext(params: {
  symbol: string;
  candles: any[];
  nowMs?: number;
  sourceTimeframe?: string;
}): ForexSessionLevelsContext | null {
  const nowMs = Number.isFinite(params.nowMs as number) ? Number(params.nowMs) : Date.now();
  const candles = (Array.isArray(params.candles) ? params.candles : [])
    .map(normalizeCandle)
    .filter((row): row is CandleRow => Array.isArray(row))
    .sort((a, b) => a[0] - b[0]);
  if (candles.length < 12) return null;

  const bySession = new Map<string, { session: SessionName; date: string; startMs: number; endMs: number; candles: CandleRow[] }>();
  const byDay = new Map<string, CandleRow[]>();

  for (const candle of candles) {
    const ts = candle[0];
    const date = isoDate(ts);
    const dayRows = byDay.get(date) ?? [];
    dayRows.push(candle);
    byDay.set(date, dayRows);

    const hour = new Date(ts).getUTCHours();
    const session = sessionForHour(hour);
    if (!session) continue;
    const bounds = sessionBoundsMs(ts, session);
    const key = `${date}:${session}`;
    const row = bySession.get(key) ?? {
      session,
      date,
      startMs: bounds.startMs,
      endMs: bounds.endMs,
      candles: [],
    };
    row.candles.push(candle);
    bySession.set(key, row);
  }

  const sessions = Array.from(bySession.values())
    .map((row) => {
      const summary = summarizeCandles(row.candles);
      if (!summary) return null;
      return {
        name: row.session,
        date: row.date,
        startUtc: new Date(row.startMs).toISOString(),
        endUtc: new Date(row.endMs).toISOString(),
        ...summary,
        startMs: row.startMs,
        endMs: row.endMs,
      };
    })
    .filter((row): row is SessionSummary & { startMs: number; endMs: number } => Boolean(row))
    .sort((a, b) => a.startMs - b.startMs);

  if (!sessions.length) return null;

  const completed = sessions.filter((s) => s.endMs <= nowMs);
  const lastCompletedRaw = completed.at(-1) ?? null;
  const currentSessionName = sessionForHour(new Date(nowMs).getUTCHours());
  const currentDate = isoDate(nowMs);
  const currentRaw = currentSessionName
    ? sessions.find((s) => s.name === currentSessionName && s.date === currentDate) ?? null
    : null;

  const stripBounds = (s: (SessionSummary & { startMs: number; endMs: number }) | null): SessionSummary | null => {
    if (!s) return null;
    return {
      name: s.name,
      date: s.date,
      startUtc: s.startUtc,
      endUtc: s.endUtc,
      open: s.open,
      high: s.high,
      low: s.low,
      close: s.close,
      candleCount: s.candleCount,
    };
  };

  const latestCompletedBySession: Partial<Record<SessionName, SessionSummary>> = {};
  for (const name of Object.keys(SESSION_WINDOWS) as SessionName[]) {
    const latest = completed.filter((s) => s.name === name).at(-1) ?? null;
    const stripped = stripBounds(latest);
    if (stripped) latestCompletedBySession[name] = stripped;
  }

  const lastCompleted = stripBounds(lastCompletedRaw);
  const currentBase = stripBounds(currentRaw);
  const lastHigh = lastCompleted?.high ?? null;
  const lastLow = lastCompleted?.low ?? null;
  const sweptLastSessionHigh = Boolean(currentBase && Number.isFinite(lastHigh as number) && currentBase.high > (lastHigh as number));
  const sweptLastSessionLow = Boolean(currentBase && Number.isFinite(lastLow as number) && currentBase.low < (lastLow as number));
  const reclaimedLastSessionHigh = Boolean(sweptLastSessionHigh && currentBase && currentBase.close < (lastHigh as number));
  const reclaimedLastSessionLow = Boolean(sweptLastSessionLow && currentBase && currentBase.close > (lastLow as number));

  const currentSession = currentBase
    ? {
        ...currentBase,
        sweptLastSessionHigh,
        sweptLastSessionLow,
        reclaimedLastSessionHigh,
        reclaimedLastSessionLow,
      }
    : null;

  const currentDayDate = isoDate(nowMs);
  const priorDayDate = isoDate(dayStartMs(nowMs) - 24 * 60 * 60 * 1000);
  const currentDay = daySummary(byDay.get(currentDayDate) ?? [], currentDayDate);
  const priorDayBase = daySummary(byDay.get(priorDayDate) ?? [], priorDayDate);
  const sweptPriorDayHigh = Boolean(currentDay && priorDayBase && currentDay.high > priorDayBase.high);
  const sweptPriorDayLow = Boolean(currentDay && priorDayBase && currentDay.low < priorDayBase.low);
  const reclaimedPriorDayHigh = Boolean(sweptPriorDayHigh && currentDay && priorDayBase && currentDay.close < priorDayBase.high);
  const reclaimedPriorDayLow = Boolean(sweptPriorDayLow && currentDay && priorDayBase && currentDay.close > priorDayBase.low);
  const priorDay = priorDayBase
    ? {
        ...priorDayBase,
        sweptHigh: sweptPriorDayHigh,
        sweptLow: sweptPriorDayLow,
        reclaimedHigh: reclaimedPriorDayHigh,
        reclaimedLow: reclaimedPriorDayLow,
      }
    : null;

  const weekStart = weekStartMs(nowMs);
  const weeklyOpenCandle = candles.find((c) => c[0] >= weekStart);
  const weeklyOpen = weeklyOpenCandle ? roundPrice(weeklyOpenCandle[1]) : null;
  const latestClose = roundPrice(candles.at(-1)?.[4] ?? NaN);
  const pipSize = pipSizeForForexPair(params.symbol);
  const sessionRange = currentSession ? Math.max(0, currentSession.high - currentSession.low) : 0;
  const currentMid = currentSession ? (currentSession.high + currentSession.low) / 2 : NaN;
  const midSessionRange =
    Boolean(currentSession) &&
    sessionRange > 0 &&
    Math.abs(latestClose - currentMid) <= sessionRange * 0.2;

  return {
    sourceTimeframe: params.sourceTimeframe || '1H',
    sessionsUtc: {
      asia: SESSION_WINDOWS.asia.label,
      london: SESSION_WINDOWS.london.label,
      new_york: SESSION_WINDOWS.new_york.label,
    },
    currentSession,
    lastCompletedSession: lastCompleted,
    latestCompletedBySession,
    priorDay,
    currentDay,
    weeklyOpen,
    nearestLevels: {
      lastSessionHighPips: pipsBetween(latestClose, lastCompleted?.high, pipSize),
      lastSessionLowPips: pipsBetween(latestClose, lastCompleted?.low, pipSize),
      priorDayHighPips: pipsBetween(latestClose, priorDayBase?.high, pipSize),
      priorDayLowPips: pipsBetween(latestClose, priorDayBase?.low, pipSize),
      weeklyOpenPips: pipsBetween(latestClose, weeklyOpen, pipSize),
    },
    signals: {
      sweptLastSessionHigh,
      sweptLastSessionLow,
      sweptPriorDayHigh,
      sweptPriorDayLow,
      bullishLiquidityReclaim: reclaimedLastSessionLow || reclaimedPriorDayLow,
      bearishLiquidityRejection: reclaimedLastSessionHigh || reclaimedPriorDayHigh,
      midSessionRange,
    },
  };
}
