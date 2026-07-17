import crypto from 'crypto';

import { kvGetJson, kvSetJson } from '../kv';

const SWING_FOREX_EVENTS_SNAPSHOT_KEY = 'swing:forex:events:snapshot:v1';
const SWING_FOREX_EVENTS_META_KEY = 'swing:forex:events:meta:v1';
const SWING_FOREX_EVENTS_TTL_SECONDS = 14 * 24 * 60 * 60;
const SWING_FOREX_DEFAULT_CALENDAR_URL = 'https://nfs.faireconomy.media/ff_calendar_thisweek.json';

const MAJOR_FX_CURRENCIES = new Set(['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD']);
const COUNTRY_TO_CURRENCY: Record<string, string> = {
  US: 'USD',
  USA: 'USD',
  UNITEDSTATES: 'USD',
  EUR: 'EUR',
  EU: 'EUR',
  EMU: 'EUR',
  EUROAREA: 'EUR',
  EUROZONE: 'EUR',
  GB: 'GBP',
  GBR: 'GBP',
  UNITEDKINGDOM: 'GBP',
  UK: 'GBP',
  JP: 'JPY',
  JPN: 'JPY',
  JAPAN: 'JPY',
  CH: 'CHF',
  CHE: 'CHF',
  SWITZERLAND: 'CHF',
  CA: 'CAD',
  CAN: 'CAD',
  CANADA: 'CAD',
  AU: 'AUD',
  AUS: 'AUD',
  AUSTRALIA: 'AUD',
  NZ: 'NZD',
  NZL: 'NZD',
  NEWZEALAND: 'NZD',
};

type ForexEventImpact = 'LOW' | 'MEDIUM' | 'HIGH' | 'UNKNOWN';

export type ForexEconomicEvent = {
  id: string;
  timestamp_utc: string;
  currency: string;
  impact: ForexEventImpact;
  event_name: string;
  actual?: string | number | null;
  forecast?: string | number | null;
  previous?: string | number | null;
  source: 'forexfactory';
};

type ForexEventsSnapshot = {
  source: 'forexfactory';
  fetchedAtMs: number;
  fromDate: string;
  toDate: string;
  events: ForexEconomicEvent[];
};

type ForexEventsMeta = {
  lastFetchAttemptAtMs: number | null;
  lastSuccessAtMs: number | null;
  lastFailureAtMs: number | null;
  lastError: string | null;
};

export type ForexEventsState = {
  snapshot: ForexEventsSnapshot | null;
  meta: ForexEventsMeta;
  stale: boolean;
  staleMinutes: number;
  refreshMinutes: number;
};

type ForexEventMatch = {
  event: ForexEconomicEvent;
  activeWindow: boolean;
  msToEvent: number;
};

export type ForexCompactEvent = {
  id: string;
  timestamp_utc: string;
  currency: string;
  impact: ForexEventImpact;
  event_name: string;
  minutesToEvent: number;
  activeWindow: boolean;
};

export type ForexEventContext = {
  source: 'forexfactory';
  pair: string | null;
  status: 'clear' | 'active' | 'stale';
  staleData: boolean;
  reasonCodes: string[];
  generatedAtMs: number;
  activeEvents: ForexCompactEvent[];
  nextEvents: ForexCompactEvent[];
  // Events that already RELEASED within the recent lookback and whose blackout
  // window has passed (minutesToEvent is negative). Carries the post-event
  // drift context: the calendar snapshot spans now-24h, so just-released
  // events are available — they were previously dropped from the context.
  // Never feeds the blackout gate (status stays derived from activeEvents only).
  recentEvents: ForexCompactEvent[];
};

function toPositiveInt(value: string | undefined, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

function toNonNegativeInt(value: string | undefined, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return fallback;
  return Math.floor(n);
}

function parseImpacts(raw: string | undefined, fallback: string[]): string[] {
  if (!raw) return fallback.slice();
  const values = String(raw)
    .split(',')
    .map((item) => item.trim().toUpperCase())
    .filter((item) => item.length > 0);
  return values.length ? values : fallback.slice();
}

function normalizeImpact(value: unknown): ForexEventImpact {
  const raw = String(value ?? '')
    .trim()
    .toUpperCase();
  if (raw.includes('HIGH')) return 'HIGH';
  if (raw.includes('MEDIUM')) return 'MEDIUM';
  if (raw.includes('LOW')) return 'LOW';
  return 'UNKNOWN';
}

function formatUtcDate(date: Date): string {
  const year = date.getUTCFullYear();
  const month = `${date.getUTCMonth() + 1}`.padStart(2, '0');
  const day = `${date.getUTCDate()}`.padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function normalizeCalendarUrl(raw: string): string {
  const value = String(raw || '').trim();
  if (!value) return SWING_FOREX_DEFAULT_CALENDAR_URL;
  if (value.startsWith('http://') || value.startsWith('https://')) return value;
  return `https://${value.replace(/^\/+/, '')}`;
}

function getForexEventConfig() {
  const calendarUrl = String(
    process.env.SWING_FOREX_EVENT_CALENDAR_URL ||
      process.env.FOREX_FACTORY_CALENDAR_URL ||
      process.env.FOREX_EVENT_CALENDAR_URL ||
      SWING_FOREX_DEFAULT_CALENDAR_URL,
  ).trim();
  return {
    calendarUrl: normalizeCalendarUrl(calendarUrl),
    refreshMinutes: toPositiveInt(process.env.SWING_FOREX_EVENT_REFRESH_MINUTES, 15),
    staleMinutes: toPositiveInt(process.env.SWING_FOREX_EVENT_STALE_MINUTES, 45),
    preEventBlockMinutes: toNonNegativeInt(process.env.SWING_FOREX_EVENT_PRE_BLOCK_MINUTES, 30),
    postEventBlockMinutes: toNonNegativeInt(process.env.SWING_FOREX_EVENT_POST_BLOCK_MINUTES, 15),
    blockImpacts: parseImpacts(process.env.SWING_FOREX_EVENT_BLOCK_IMPACTS, ['HIGH', 'MEDIUM']),
    // recentEvents window: how long after release an event stays in the context
    // (default 180min — measured post-announcement drift on gold/EUR persists ~4h
    // from release, decayed by 24h) and which impacts qualify (HIGH only: the
    // drift edge was measured on tier-1 releases; MEDIUM would mostly add noise).
    recentLookbackMinutes: toNonNegativeInt(process.env.SWING_FOREX_EVENT_RECENT_LOOKBACK_MINUTES, 180),
    recentImpacts: parseImpacts(process.env.SWING_FOREX_EVENT_RECENT_IMPACTS, ['HIGH']),
  };
}

function defaultMeta(): ForexEventsMeta {
  return {
    lastFetchAttemptAtMs: null,
    lastSuccessAtMs: null,
    lastFailureAtMs: null,
    lastError: null,
  };
}

function isSnapshotStale(lastSuccessAtMs: number | null, staleMinutes: number, nowMs: number): boolean {
  if (!Number.isFinite(lastSuccessAtMs as number) || (lastSuccessAtMs as number) <= 0) return true;
  return nowMs - Number(lastSuccessAtMs) > staleMinutes * 60_000;
}

function safeRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function normalizeMetricValue(value: unknown): string | number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const raw = String(value).trim();
  if (!raw) return null;
  const normalized = raw.replace(/,/g, '');
  if (/^[+-]?\d*\.?\d+$/.test(normalized)) {
    const numeric = Number(normalized);
    if (Number.isFinite(numeric)) return numeric;
  }
  return raw;
}

function resolveCountryCurrency(value: unknown): string | null {
  const raw = String(value ?? '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z]/g, '');
  if (!raw) return null;
  return COUNTRY_TO_CURRENCY[raw] ?? null;
}

function resolveCurrency(row: Record<string, unknown>): string | null {
  const direct = String(row.currency ?? row.ccy ?? row.country ?? '')
    .trim()
    .toUpperCase();
  if (/^[A-Z]{3}$/.test(direct)) return direct;
  const fromCountry = resolveCountryCurrency(row.country ?? row.countryCode);
  return fromCountry;
}

function parseEventTimestampUtc(rawValue: unknown): string | null {
  const raw = String(rawValue ?? '').trim();
  if (!raw) return null;
  const withT = raw.includes('T') ? raw : raw.replace(' ', 'T');
  const withZone = /[zZ]$|[+\-]\d{2}:?\d{2}$/.test(withT) ? withT : `${withT}Z`;
  const ts = Date.parse(withZone);
  if (!Number.isFinite(ts)) return null;
  return new Date(ts).toISOString();
}

function buildEventId(currency: string, timestampUtc: string, eventName: string): string {
  const base = `${currency}|${timestampUtc}|${eventName.toUpperCase()}`;
  return crypto.createHash('sha1').update(base).digest('hex').slice(0, 20);
}

function normalizeForexFactoryEventRow(value: unknown): ForexEconomicEvent | null {
  const row = safeRecord(value);
  const currency = resolveCurrency(row);
  if (!currency) return null;
  const timestampUtc = parseEventTimestampUtc(row.date ?? row.dateUtc ?? row.timestamp ?? row.time);
  if (!timestampUtc) return null;
  const eventName = String(row.event ?? row.name ?? row.title ?? '')
    .trim()
    .slice(0, 160);
  if (!eventName) return null;
  return {
    id: buildEventId(currency, timestampUtc, eventName),
    timestamp_utc: timestampUtc,
    currency,
    impact: normalizeImpact(row.impact ?? row.importance ?? row.priority),
    event_name: eventName,
    actual: normalizeMetricValue(row.actual),
    forecast: normalizeMetricValue(row.forecast ?? row.estimate),
    previous: normalizeMetricValue(row.previous),
    source: 'forexfactory',
  };
}

function timestampMs(value: string): number {
  const ts = Date.parse(value);
  return Number.isFinite(ts) ? ts : NaN;
}

function isWithinEventWindow(
  eventIsoTimestamp: string,
  nowMs: number,
  window: { preEventBlockMinutes: number; postEventBlockMinutes: number },
): boolean {
  const eventMs = timestampMs(eventIsoTimestamp);
  if (!Number.isFinite(eventMs)) return false;
  const startMs = eventMs - window.preEventBlockMinutes * 60_000;
  const endMs = eventMs + window.postEventBlockMinutes * 60_000;
  return nowMs >= startMs && nowMs <= endMs;
}

async function loadSnapshot(): Promise<ForexEventsSnapshot | null> {
  return kvGetJson<ForexEventsSnapshot>(SWING_FOREX_EVENTS_SNAPSHOT_KEY);
}

async function loadMeta(): Promise<ForexEventsMeta> {
  const meta = await kvGetJson<ForexEventsMeta>(SWING_FOREX_EVENTS_META_KEY);
  return meta || defaultMeta();
}

export async function getForexEventsState(nowMs = Date.now()): Promise<ForexEventsState> {
  const cfg = getForexEventConfig();
  const [snapshot, meta] = await Promise.all([loadSnapshot(), loadMeta()]);
  return {
    snapshot,
    meta,
    stale: isSnapshotStale(meta.lastSuccessAtMs, cfg.staleMinutes, nowMs),
    staleMinutes: cfg.staleMinutes,
    refreshMinutes: cfg.refreshMinutes,
  };
}

async function fetchForexFactoryCalendar(fromDate: string, toDate: string): Promise<ForexEconomicEvent[]> {
  const cfg = getForexEventConfig();
  const res = await fetch(cfg.calendarUrl, { method: 'GET' });
  if (!res.ok) {
    const message = await res.text().catch(() => '');
    throw new Error(`Forex event calendar error ${res.status}: ${message || res.statusText}`);
  }
  const payload: unknown = await res.json();
  if (!Array.isArray(payload)) {
    throw new Error('Forex event calendar returned invalid payload');
  }
  const fromMs = Date.parse(`${fromDate}T00:00:00.000Z`);
  const toMs = Date.parse(`${toDate}T23:59:59.999Z`);
  const deduped = new Map<string, ForexEconomicEvent>();
  for (const row of payload) {
    const normalized = normalizeForexFactoryEventRow(row);
    if (!normalized) continue;
    const ts = timestampMs(normalized.timestamp_utc);
    if (!Number.isFinite(ts)) continue;
    if (ts < fromMs || ts > toMs) continue;
    deduped.set(normalized.id, normalized);
  }
  return Array.from(deduped.values()).sort((a, b) => Date.parse(a.timestamp_utc) - Date.parse(b.timestamp_utc));
}

export async function refreshForexEvents(opts: { force?: boolean; nowMs?: number } = {}) {
  const nowMs = Number.isFinite(opts.nowMs as number) ? Number(opts.nowMs) : Date.now();
  const force = Boolean(opts.force);
  const cfg = getForexEventConfig();
  const fromDate = formatUtcDate(new Date(nowMs - 24 * 60 * 60 * 1000));
  const toDate = formatUtcDate(new Date(nowMs + 7 * 24 * 60 * 60 * 1000));

  const stateBefore = await getForexEventsState(nowMs);
  const recentlyRefreshed =
    Number.isFinite(stateBefore.meta.lastSuccessAtMs as number) &&
    nowMs - Number(stateBefore.meta.lastSuccessAtMs) < cfg.refreshMinutes * 60_000;

  if (!force && recentlyRefreshed) {
    return {
      ok: true,
      refreshed: false,
      skipped: true,
      reason: 'within_refresh_interval',
      state: stateBefore,
    };
  }

  const attemptMeta: ForexEventsMeta = {
    ...stateBefore.meta,
    lastFetchAttemptAtMs: nowMs,
  };
  await kvSetJson(SWING_FOREX_EVENTS_META_KEY, attemptMeta, SWING_FOREX_EVENTS_TTL_SECONDS);

  try {
    const events = await fetchForexFactoryCalendar(fromDate, toDate);
    const snapshot: ForexEventsSnapshot = {
      source: 'forexfactory',
      fetchedAtMs: nowMs,
      fromDate,
      toDate,
      events,
    };
    const successMeta: ForexEventsMeta = {
      ...attemptMeta,
      lastSuccessAtMs: nowMs,
      lastFailureAtMs: null,
      lastError: null,
    };
    await Promise.all([
      kvSetJson(SWING_FOREX_EVENTS_SNAPSHOT_KEY, snapshot, SWING_FOREX_EVENTS_TTL_SECONDS),
      kvSetJson(SWING_FOREX_EVENTS_META_KEY, successMeta, SWING_FOREX_EVENTS_TTL_SECONDS),
    ]);
    return {
      ok: true,
      refreshed: true,
      skipped: false,
      reason: null,
      state: await getForexEventsState(nowMs),
    };
  } catch (err) {
    const failureMeta: ForexEventsMeta = {
      ...attemptMeta,
      lastFailureAtMs: nowMs,
      lastError: err instanceof Error ? err.message : String(err),
    };
    await kvSetJson(SWING_FOREX_EVENTS_META_KEY, failureMeta, SWING_FOREX_EVENTS_TTL_SECONDS);
    return {
      ok: false,
      refreshed: false,
      skipped: false,
      reason: failureMeta.lastError,
      state: await getForexEventsState(nowMs),
    };
  }
}

export async function ensureForexEventsState(nowMs = Date.now()): Promise<ForexEventsState> {
  const state = await getForexEventsState(nowMs);
  const lastAttemptAt = Number(state.meta.lastFetchAttemptAtMs || 0);
  const refreshWindowMs = state.refreshMinutes * 60_000;
  const shouldTryRefresh =
    !state.snapshot || state.stale || !Number.isFinite(lastAttemptAt) || nowMs - lastAttemptAt >= refreshWindowMs;

  if (!shouldTryRefresh) return state;
  try {
    const refreshed = await refreshForexEvents({ nowMs, force: state.stale || !state.snapshot });
    return refreshed.state;
  } catch (err) {
    console.warn('Forex event refresh failed, using current state:', err);
    return state;
  }
}

function toComparable(value: string): string {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z]/g, '');
}

function extractMajorForexPair(value: string): string | null {
  const normalized = toComparable(value);
  if (!normalized) return null;
  for (let i = 0; i <= normalized.length - 6; i += 1) {
    const candidate = normalized.slice(i, i + 6);
    const base = candidate.slice(0, 3);
    const quote = candidate.slice(3, 6);
    if (MAJOR_FX_CURRENCIES.has(base) && MAJOR_FX_CURRENCIES.has(quote) && base !== quote) {
      return candidate;
    }
  }
  return null;
}

export function resolveForexPair(params: { symbol: string; instrumentId?: string | null }): string | null {
  const candidates = [params.instrumentId, params.symbol]
    .map((value) => String(value || '').trim())
    .filter((value) => value.length > 0);

  for (const raw of candidates) {
    const withoutPrefix = raw.includes(':') ? raw.split(':').slice(1).join(':') : raw;
    const pair = extractMajorForexPair(withoutPrefix);
    if (pair) return pair;
  }
  return null;
}

function pairCurrencies(pair: string): string[] {
  const normalized = toComparable(pair);
  if (normalized.length < 6) return [];
  return [normalized.slice(0, 3), normalized.slice(3, 6)];
}

// Non-FX instruments still key off a dominant macro currency for the economic
// calendar: most commodities are USD-quoted (metals, oil), and each index tracks its
// home economy. This gives metals/indices the same event awareness as FX pairs.
const COMMODITY_USD_HINTS = [
  'GOLD', 'SILVER', 'XAU', 'XAG', 'WTI', 'BRENT', 'CRUDE', 'OIL', 'NGAS', 'NATGAS', 'COPPER', 'PLATINUM', 'PALLADIUM',
];
const INDEX_CURRENCY_HINTS: Array<[string, string]> = [
  ['US500', 'USD'], ['US100', 'USD'], ['NAS100', 'USD'], ['SPX', 'USD'], ['US30', 'USD'], ['DJ30', 'USD'], ['US2000', 'USD'], ['RUSSELL', 'USD'],
  ['TLT', 'USD'], // US Treasury ETF — rates driven by the USD macro calendar (FOMC/CPI/NFP)
  ['GER40', 'EUR'], ['GER30', 'EUR'], ['DAX', 'EUR'], ['FR40', 'EUR'], ['CAC', 'EUR'], ['EU50', 'EUR'], ['STOXX', 'EUR'], ['ESP35', 'EUR'], ['IT40', 'EUR'],
  ['UK100', 'GBP'], ['FTSE', 'GBP'],
  ['JP225', 'JPY'], ['NIKKEI', 'JPY'],
  ['AUS200', 'AUD'], ['ASX', 'AUD'],
  ['SWI20', 'CHF'], ['SMI', 'CHF'],
];

// Currencies whose calendar events are relevant to an instrument. FX pairs use both
// legs; commodities/indices use their macro currency (by symbol hint, then category);
// crypto always uses the USD calendar.
export function resolveEventCurrencies(params: {
  symbol: string;
  instrumentId?: string | null;
  category?: string | null;
}): string[] {
  const pair = resolveForexPair({ symbol: params.symbol, instrumentId: params.instrumentId });
  if (pair) return pairCurrencies(pair);

  // Non-FX resolution is OPT-IN via category, so callers that don't pass one
  // (scalp evidence, dashboard) keep their original FX-pair-only behavior.
  const category = String(params.category || '').trim().toLowerCase();
  // Crypto trades 24/7 but reacts to the USD macro calendar (CPI/FOMC/NFP)
  // like any USD-denominated risk asset — no per-symbol hints needed.
  if (category === 'crypto') return ['USD'];
  if (category !== 'commodity' && category !== 'index') return [];

  const haystack = [params.instrumentId, params.symbol].map((v) => String(v || '').toUpperCase()).join(' ');
  const indexHit = INDEX_CURRENCY_HINTS.find(([hint]) => haystack.includes(hint));
  if (indexHit) return [indexHit[1]];
  if (COMMODITY_USD_HINTS.some((hint) => haystack.includes(hint))) return ['USD'];

  // Commodity/index with no recognizable hint → default to the USD macro calendar.
  return ['USD'];
}

function listCurrencyEventMatches(params: {
  currencies: string[];
  events: ForexEconomicEvent[];
  nowMs: number;
  blockedImpacts: string[];
  preEventBlockMinutes: number;
  postEventBlockMinutes: number;
}): ForexEventMatch[] {
  const currencies = new Set(params.currencies.map((c) => String(c || '').toUpperCase()).filter(Boolean));
  if (!currencies.size) return [];

  const blocked = params.blockedImpacts.map((impact) => impact.toUpperCase());
  return (params.events || [])
    .filter((event) => currencies.has(String(event.currency || '').toUpperCase()))
    .filter((event) => blocked.includes(String(event.impact || '').toUpperCase()))
    .map((event) => {
      const eventMs = timestampMs(event.timestamp_utc);
      return {
        event,
        activeWindow: isWithinEventWindow(event.timestamp_utc, params.nowMs, {
          preEventBlockMinutes: params.preEventBlockMinutes,
          postEventBlockMinutes: params.postEventBlockMinutes,
        }),
        msToEvent: Number.isFinite(eventMs) ? eventMs - params.nowMs : Number.POSITIVE_INFINITY,
      };
    })
    .sort((a, b) => Math.abs(a.msToEvent) - Math.abs(b.msToEvent));
}

function toCompactEvent(match: ForexEventMatch): ForexCompactEvent {
  return {
    id: match.event.id,
    timestamp_utc: match.event.timestamp_utc,
    currency: match.event.currency,
    impact: match.event.impact,
    event_name: match.event.event_name,
    minutesToEvent: Number.isFinite(match.msToEvent) ? Math.round(match.msToEvent / 60_000) : 0,
    activeWindow: match.activeWindow,
  };
}

export function buildForexEventContext(params: {
  symbol: string;
  instrumentId?: string | null;
  category?: string | null;
  state: ForexEventsState;
  nowMs?: number;
}): ForexEventContext {
  const cfg = getForexEventConfig();
  const nowMs = Number.isFinite(params.nowMs as number) ? Number(params.nowMs) : Date.now();
  const pair = resolveForexPair({ symbol: params.symbol, instrumentId: params.instrumentId });
  const currencies = resolveEventCurrencies({
    symbol: params.symbol,
    instrumentId: params.instrumentId,
    category: params.category,
  });
  // Label the context with the FX pair when there is one, else the macro currency
  // (e.g. "USD" for gold) so the prompt can see what calendar is being applied.
  const label = pair ?? (currencies.length ? currencies.join('/') : null);

  if (!currencies.length) {
    return {
      source: params.state.snapshot?.source ?? 'forexfactory',
      pair: null,
      status: params.state.stale ? 'stale' : 'clear',
      staleData: params.state.stale,
      reasonCodes: params.state.stale ? ['FOREX_EVENT_DATA_STALE', 'FOREX_EVENT_PAIR_UNRESOLVED'] : ['FOREX_EVENT_PAIR_UNRESOLVED'],
      generatedAtMs: nowMs,
      activeEvents: [],
      nextEvents: [],
      recentEvents: [],
    };
  }

  const matches = listCurrencyEventMatches({
    currencies,
    events: params.state.snapshot?.events ?? [],
    nowMs,
    blockedImpacts: cfg.blockImpacts,
    preEventBlockMinutes: cfg.preEventBlockMinutes,
    postEventBlockMinutes: cfg.postEventBlockMinutes,
  });

  const activeMatches = matches.filter((match) => match.activeWindow).slice(0, 2);
  const nextMatches = matches.filter((match) => match.msToEvent >= 0).slice(0, 2);
  // Already-released events within the recent lookback, past their blackout
  // window. Separate match pass: recentImpacts (HIGH only by default) is
  // narrower than blockImpacts, and the blackout selection above must not
  // change. Sorted by |msToEvent| already → most recent release first.
  const recentMatches = listCurrencyEventMatches({
    currencies,
    events: params.state.snapshot?.events ?? [],
    nowMs,
    blockedImpacts: cfg.recentImpacts,
    preEventBlockMinutes: cfg.preEventBlockMinutes,
    postEventBlockMinutes: cfg.postEventBlockMinutes,
  })
    .filter(
      (match) =>
        !match.activeWindow &&
        match.msToEvent < 0 &&
        -match.msToEvent <= cfg.recentLookbackMinutes * 60_000,
    )
    .slice(0, 2);
  // status derives from activeMatches ONLY — recentEvents must never extend
  // the event-blackout gate in /api/analyze.
  const status: ForexEventContext['status'] = params.state.stale ? 'stale' : activeMatches.length ? 'active' : 'clear';
  const reasonCodes = params.state.stale
    ? ['FOREX_EVENT_DATA_STALE']
    : activeMatches.length
      ? ['FOREX_EVENT_WINDOW_ACTIVE']
      : ['FOREX_EVENT_WINDOW_CLEAR'];

  return {
    source: params.state.snapshot?.source ?? 'forexfactory',
    pair: label,
    status,
    staleData: params.state.stale,
    reasonCodes,
    generatedAtMs: nowMs,
    activeEvents: activeMatches.map(toCompactEvent),
    nextEvents: nextMatches.map(toCompactEvent),
    recentEvents: recentMatches.map(toCompactEvent),
  };
}

export async function loadForexEventContext(params: {
  symbol: string;
  instrumentId?: string | null;
  category?: string | null;
  nowMs?: number;
}): Promise<ForexEventContext> {
  const nowMs = Number.isFinite(params.nowMs as number) ? Number(params.nowMs) : Date.now();
  const state = await ensureForexEventsState(nowMs);
  return buildForexEventContext({
    symbol: params.symbol,
    instrumentId: params.instrumentId,
    category: params.category,
    state,
    nowMs,
  });
}
