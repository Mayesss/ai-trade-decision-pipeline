import crypto from 'crypto';

import type { PositionInfo } from './analytics';
import type { MultiTFIndicators, IndicatorTimeframeOptions } from './indicators';
import { computeATR, computeEMA, computeRSI_Wilder, computeSMA, computeVWAP, slopePct } from './indicators';
import { CONTEXT_TIMEFRAME, MACRO_TIMEFRAME, MICRO_TIMEFRAME, PRIMARY_TIMEFRAME, TRADE_WINDOW_MINUTES } from './constants';
import type { TradeDecision } from './trading';

import defaultTickerEpicMap from '../data/capitalTickerMap.json';

type CapitalMethod = 'GET' | 'POST' | 'PUT' | 'DELETE';

type SessionState = {
    cst: string;
    securityToken: string;
    expiresAtMs: number;
};

type CapitalTickerMap = Record<string, string>;

type CapitalPositionRow = {
    market?: {
        epic?: string;
        bid?: number | string;
        offer?: number | string;
    };
    position?: {
        dealId?: string;
        direction?: string;
        size?: number | string;
        level?: number | string;
        openLevel?: number | string;
        createdDateUTC?: string;
        createdDate?: string;
        leverage?: number | string;
        unrealisedProfitLoss?: number | string;
        upl?: number | string;
        profit?: number | string;
    };
    dealId?: string;
    direction?: string;
    size?: number | string;
    level?: number | string;
    openLevel?: number | string;
};

type BundleOpts = {
    includeTrades?: boolean;
    tradeMinutes?: number;
    tradeMaxMs?: number;
    tradeMaxPages?: number;
    tradeMaxTrades?: number;
    candleLimit?: number;
};

type MarketDetails = {
    bid: number | null;
    offer: number | null;
    minDealSize: number | null;
    sizeDecimals: number;
    epic: string;
};

type ResolveEpicResult = {
    ticker: string;
    epic: string;
    source: 'env' | 'default' | 'passthrough' | 'discovered';
};

const CAPITAL_API_BASE = (process.env.CAPITAL_API_BASE || 'https://api-capital.backend-capital.com').replace(/\/+$/, '');
const SESSION_TTL_MS = 10 * 60 * 1000;

let cachedSession: SessionState | null = null;
const resolvedEpicCache = new Map<string, ResolveEpicResult>();

const QUOTE_SUFFIXES = ['USDT', 'USDC', 'USD', 'EUR', 'GBP', 'PERP'];

function parseEnvTickerMap(): CapitalTickerMap {
    const raw = process.env.CAPITAL_TICKER_EPIC_MAP;
    if (!raw) return {};
    try {
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};
        const out: CapitalTickerMap = {};
        for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
            if (typeof v !== 'string') continue;
            out[String(k).toUpperCase()] = v.trim();
        }
        return out;
    } catch {
        return {};
    }
}

function ensureCapitalConfig() {
    if (!process.env.CAPITAL_API_KEY) throw new Error('Missing CAPITAL_API_KEY');
    if (!process.env.CAPITAL_IDENTIFIER) throw new Error('Missing CAPITAL_IDENTIFIER');
    if (!process.env.CAPITAL_PASSWORD) throw new Error('Missing CAPITAL_PASSWORD');
}

function buildQuery(params: Record<string, string | number | undefined>) {
    return Object.entries(params)
        .filter(([, v]) => v !== undefined && v !== null && v !== '')
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
        .join('&');
}

function safeNumber(value: unknown, fallback = 0): number {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
}

function normalizeTicker(symbol: string): string {
    return String(symbol || '').trim().toUpperCase();
}

function baseFromTicker(symbol: string): string {
    const ticker = normalizeTicker(symbol);
    for (const suffix of QUOTE_SUFFIXES) {
        if (ticker.endsWith(suffix) && ticker.length > suffix.length) {
            return ticker.slice(0, ticker.length - suffix.length).replace(/[-_]/g, '');
        }
    }
    return ticker.replace(/[-_]/g, '');
}

function isCapitalEpicNotFoundError(err: unknown): boolean {
    const msg = err instanceof Error ? err.message : String(err);
    return msg.includes('error.not-found.epic') || (msg.includes('Capital API error 404') && msg.toLowerCase().includes('epic'));
}

function midFromQuote(value: any): number | null {
    if (value === null || value === undefined) return null;
    if (typeof value === 'number') return Number.isFinite(value) ? value : null;
    if (typeof value === 'string') {
        const n = Number(value);
        return Number.isFinite(n) ? n : null;
    }
    const bid = safeNumber(value?.bid, NaN);
    const ask = safeNumber(value?.ask, NaN);
    const last = safeNumber(value?.lastTraded, NaN);
    if (Number.isFinite(last)) return last;
    if (Number.isFinite(bid) && Number.isFinite(ask)) return (bid + ask) / 2;
    if (Number.isFinite(bid)) return bid;
    if (Number.isFinite(ask)) return ask;
    return null;
}

function toIsoTimestampMs(raw: unknown): number | null {
    if (raw === null || raw === undefined) return null;
    const ts = Date.parse(String(raw));
    return Number.isFinite(ts) ? ts : null;
}

function timeframeToMinutes(tf: string): number {
    const match = String(tf).trim().toLowerCase().match(/^(\d+)([mhdw])$/);
    if (!match) return 60;
    const value = Number(match[1]);
    if (!Number.isFinite(value) || value <= 0) return 60;
    const unit = match[2];
    if (unit === 'm') return value;
    if (unit === 'h') return value * 60;
    if (unit === 'd') return value * 60 * 24;
    return value * 60 * 24 * 7;
}

function toCapitalResolution(tf: string): string {
    const normalized = String(tf || '').trim();
    const match = normalized.match(/^(\d+)([mMhHdDwW])$/);
    if (!match) return 'HOUR';
    const value = Number(match[1]);
    const unit = match[2].toLowerCase();
    if (!Number.isFinite(value) || value <= 0) return 'HOUR';
    if (unit === 'm') return value === 1 ? 'MINUTE' : `MINUTE_${value}`;
    if (unit === 'h') return value === 1 ? 'HOUR' : `HOUR_${value}`;
    if (unit === 'd') return value === 1 ? 'DAY' : `DAY_${value}`;
    return value === 1 ? 'WEEK' : `WEEK_${value}`;
}

function normalizeTimeframe(tf: string): string {
    if (tf === '4D') return '1W';
    return tf;
}

function getSessionExpired() {
    if (!cachedSession) return true;
    return Date.now() >= cachedSession.expiresAtMs;
}

async function parseResponsePayload(res: Response): Promise<any> {
    const text = await res.text();
    if (!text) return null;
    try {
        return JSON.parse(text);
    } catch {
        return { raw: text };
    }
}

async function createSession(forceRefresh = false): Promise<SessionState> {
    if (!forceRefresh && !getSessionExpired() && cachedSession) return cachedSession;
    ensureCapitalConfig();

    const url = `${CAPITAL_API_BASE}/api/v1/session`;
    const res = await fetch(url, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CAP-API-KEY': process.env.CAPITAL_API_KEY ?? '',
        },
        body: JSON.stringify({
            identifier: process.env.CAPITAL_IDENTIFIER,
            password: process.env.CAPITAL_PASSWORD,
        }),
    });

    const payload = await parseResponsePayload(res);
    if (!res.ok) {
        const message = payload?.errorCode || payload?.message || res.statusText;
        throw new Error(`Capital session error ${res.status}: ${message}`);
    }

    const cst = res.headers.get('CST') || res.headers.get('cst');
    const securityToken = res.headers.get('X-SECURITY-TOKEN') || res.headers.get('x-security-token');
    if (!cst || !securityToken) {
        throw new Error('Capital session missing CST/X-SECURITY-TOKEN headers');
    }

    cachedSession = {
        cst,
        securityToken,
        expiresAtMs: Date.now() + SESSION_TTL_MS,
    };
    return cachedSession;
}

async function capitalFetch(
    method: CapitalMethod,
    path: string,
    params: Record<string, string | number | undefined> = {},
    body?: unknown,
    auth = true,
    retryAuth = true,
) {
    if (!process.env.CAPITAL_API_KEY) throw new Error('Missing CAPITAL_API_KEY');
    const query = buildQuery(params);
    const url = `${CAPITAL_API_BASE}${path}${query ? `?${query}` : ''}`;

    const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        'X-CAP-API-KEY': process.env.CAPITAL_API_KEY,
    };

    if (auth) {
        const session = await createSession(false);
        headers.CST = session.cst;
        headers['X-SECURITY-TOKEN'] = session.securityToken;
    }

    const res = await fetch(url, {
        method,
        headers,
        body: body !== undefined ? JSON.stringify(body) : undefined,
    });

    if (res.status === 401 && auth && retryAuth) {
        cachedSession = null;
        await createSession(true);
        return capitalFetch(method, path, params, body, auth, false);
    }

    const payload = await parseResponsePayload(res);
    if (!res.ok) {
        const message = payload?.errorCode || payload?.message || payload?.raw || res.statusText;
        throw new Error(`Capital API error ${res.status}: ${message}`);
    }

    return payload;
}

function extractRows<T>(payload: any, keys: string[]): T[] {
    for (const key of keys) {
        const value = payload?.[key];
        if (Array.isArray(value)) return value as T[];
    }
    if (Array.isArray(payload)) return payload as T[];
    if (Array.isArray(payload?.data)) return payload.data as T[];
    return [];
}

function parseCapitalCandles(payload: any): any[] {
    const prices = extractRows<any>(payload, ['prices', 'Price', 'data']);
    const candles = prices
        .map((p) => {
            const tsRaw = p?.snapshotTimeUTC ?? p?.snapshotTime ?? p?.time ?? p?.timestamp;
            const tsMs = toIsoTimestampMs(tsRaw);
            const open = midFromQuote(p?.openPrice) ?? safeNumber(p?.open, NaN);
            const high = midFromQuote(p?.highPrice) ?? safeNumber(p?.high, NaN);
            const low = midFromQuote(p?.lowPrice) ?? safeNumber(p?.low, NaN);
            const close = midFromQuote(p?.closePrice) ?? safeNumber(p?.close, NaN);
            const volume = safeNumber(p?.lastTradedVolume ?? p?.volume, 0);
            if (!Number.isFinite(tsMs as number)) return null;
            if (![open, high, low, close].every((v) => Number.isFinite(v))) return null;
            return [tsMs, open, high, low, close, volume];
        })
        .filter((row): row is [number, number, number, number, number, number] => Array.isArray(row))
        .sort((a, b) => Number(a[0]) - Number(b[0]));

    return candles;
}

function formatSummary(candles: any[]): string {
    if (!Array.isArray(candles) || candles.length < 5) {
        return 'VWAP=0.00, RSI=50.0, trend=down, ATR=0.00, EMA9=0.00, EMA21=0.00, EMA20=0.00, EMA50=0.00, SMA200=0.00, slopeEMA21_10=0.000%/bar';
    }
    const closes = candles.map((c) => Number(c?.[4])).filter((v) => Number.isFinite(v));
    if (closes.length < 5) {
        return 'VWAP=0.00, RSI=50.0, trend=down, ATR=0.00, EMA9=0.00, EMA21=0.00, EMA20=0.00, EMA50=0.00, SMA200=0.00, slopeEMA21_10=0.000%/bar';
    }

    const vwap = computeVWAP(candles);
    const rsi = computeRSI_Wilder(closes, 14);
    const ema9 = computeEMA(closes, 9);
    const ema21 = computeEMA(closes, 21);
    const ema20 = computeEMA(closes, 20);
    const ema50 = computeEMA(closes, 50);
    const sma200 = computeSMA(closes, 200);
    const atr = computeATR(candles, 14);

    const e9 = ema9.at(-1) ?? closes.at(-1) ?? 0;
    const e21 = ema21.at(-1) ?? closes.at(-1) ?? 0;
    const e20 = ema20.at(-1) ?? closes.at(-1) ?? 0;
    const e50 = ema50.at(-1) ?? closes.at(-1) ?? 0;
    const s200 = sma200.at(-1) ?? closes.at(-1) ?? 0;
    const trend = e20 >= e50 ? 'up' : 'down';
    const momSlope = slopePct(ema21, 10);

    return `VWAP=${vwap.toFixed(2)}, RSI=${rsi.toFixed(1)}, trend=${trend}, ATR=${atr.toFixed(2)}, EMA9=${e9.toFixed(
        2,
    )}, EMA21=${e21.toFixed(2)}, EMA20=${e20.toFixed(2)}, EMA50=${e50.toFixed(2)}, SMA200=${s200.toFixed(
        2,
    )}, slopeEMA21_10=${momSlope.toFixed(3)}%/bar`;
}

function buildSyntheticOrderbook(last: number) {
    const normalizedLast = Number.isFinite(last) && last > 0 ? last : 1;
    const spread = Math.max(normalizedLast * 0.0002, normalizedLast * 0.00001);
    const bid = normalizedLast - spread / 2;
    const ask = normalizedLast + spread / 2;
    const notionalDepth = 2_000_000;
    const size = notionalDepth / normalizedLast;
    return {
        bids: [[bid, size]],
        asks: [[ask, size]],
    };
}

function buildCapitalTicker(candles: any[], timeframe: string) {
    const last = safeNumber(candles.at(-1)?.[4], NaN);
    if (!Number.isFinite(last)) return { last: 0, lastPr: 0, close: 0, change24h: 0 };
    const tfMinutes = Math.max(1, timeframeToMinutes(timeframe));
    const bars24h = Math.max(1, Math.round((24 * 60) / tfMinutes));
    const ref = safeNumber(candles.at(-1 - bars24h)?.[4], safeNumber(candles.at(0)?.[4], last));
    const change24h = ref > 0 ? ((last - ref) / ref) * 100 : 0;
    return {
        last,
        lastPr: last,
        close: last,
        change24h,
    };
}

function clampLeverage(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n)) return null;
    const rounded = Math.round(n);
    const clamped = Math.max(1, Math.min(5, rounded));
    return clamped;
}

function deriveLeverage(decision: TradeDecision): number | null {
    const explicit = clampLeverage((decision as any)?.leverage);
    if (explicit) return explicit;
    const raw = (decision as any)?.signal_strength;
    const numericStrength = Number(raw);
    if (Number.isFinite(numericStrength)) {
        const mapped = clampLeverage(numericStrength);
        if (mapped) return mapped;
    }
    const strength = String(raw ?? '').toUpperCase();
    if (decision.action === 'BUY' || decision.action === 'SELL' || decision.action === 'REVERSE') {
        if (strength === 'HIGH') return 4;
        if (strength === 'MEDIUM') return 3;
        if (strength === 'LOW') return 1;
    }
    return null;
}

function normalizeClosePct(pct: unknown) {
    const n = Number(pct);
    if (!Number.isFinite(n)) return null;
    const clamped = Math.max(0, Math.min(100, n));
    return clamped > 0 ? clamped : null;
}

function numberOfDecimals(value: number): number {
    if (!Number.isFinite(value) || value <= 0) return 4;
    const asString = String(value);
    const idx = asString.indexOf('.');
    if (idx < 0) return 0;
    return Math.max(0, asString.length - idx - 1);
}

function quantizeSize(rawSize: number, minDealSize: number | null, sizeDecimals = 4) {
    if (!Number.isFinite(rawSize) || rawSize <= 0) return 0;
    const min = Number.isFinite(minDealSize as number) && (minDealSize as number) > 0 ? (minDealSize as number) : 0.0001;
    const step = min > 0 ? min : Math.pow(10, -1 * sizeDecimals);
    const roundedDown = Math.floor(rawSize / step) * step;
    const finalSize = Math.max(roundedDown, min);
    return Number(finalSize.toFixed(Math.max(0, sizeDecimals)));
}

function extractPositionRows(payload: any): CapitalPositionRow[] {
    return extractRows<CapitalPositionRow>(payload, ['positions', 'data']);
}

export function resolveCapitalEpic(symbol: string): ResolveEpicResult {
    const ticker = normalizeTicker(symbol);
    const envMap = parseEnvTickerMap();
    const defaultMap = defaultTickerEpicMap as CapitalTickerMap;

    const envEpic = envMap[ticker];
    if (envEpic) return { ticker, epic: envEpic, source: 'env' };

    const defaultEpic = defaultMap[ticker];
    if (defaultEpic) return { ticker, epic: defaultEpic, source: 'default' };

    return { ticker, epic: ticker, source: 'passthrough' };
}

type CapitalMarketSearchRow = {
    epic?: string;
    symbol?: string;
    marketName?: string;
    instrumentName?: string;
    displayName?: string;
    status?: string;
    snapshot?: {
        marketStatus?: string;
    };
};

function scoreMarketCandidate(row: CapitalMarketSearchRow, term: string, ticker: string, base: string): number {
    const epic = normalizeTicker(row?.epic || '');
    const symbol = normalizeTicker(row?.symbol || '');
    const marketName = normalizeTicker(row?.marketName || row?.instrumentName || row?.displayName || '');
    const normalizedTerm = normalizeTicker(term);

    let score = 0;
    if (!epic) return -1;
    if (epic === ticker) score += 120;
    if (epic === normalizedTerm) score += 115;
    if (symbol === ticker) score += 100;
    if (symbol === normalizedTerm) score += 95;
    if (epic.includes(ticker)) score += 75;
    if (epic.includes(base)) score += 50;
    if (symbol.includes(base)) score += 40;
    if (marketName.includes(base)) score += 35;
    if (marketName.includes(normalizedTerm)) score += 45;
    if (row?.status === 'TRADEABLE' || row?.snapshot?.marketStatus === 'TRADEABLE') score += 20;
    return score;
}

async function discoverCapitalEpic(symbol: string, candidateEpics: string[]): Promise<ResolveEpicResult | null> {
    const ticker = normalizeTicker(symbol);
    const base = baseFromTicker(ticker);
    const terms = Array.from(
        new Set(
            [
                ticker,
                base,
                ...candidateEpics,
                ...candidateEpics.map((c) => baseFromTicker(c)),
            ]
                .map((v) => normalizeTicker(v))
                .filter((v) => v.length > 0),
        ),
    );

    let best: { epic: string; score: number } | null = null;
    for (const term of terms) {
        let payload: any;
        try {
            payload = await capitalFetch('GET', '/api/v1/markets', { searchTerm: term, pageSize: 50 }, undefined, true);
        } catch {
            continue;
        }
        const rows = extractRows<CapitalMarketSearchRow>(payload, ['markets', 'data']);
        for (const row of rows) {
            const epic = normalizeTicker(row?.epic || '');
            if (!epic) continue;
            const score = scoreMarketCandidate(row, term, ticker, base);
            if (!best || score > best.score) {
                best = { epic, score };
            }
        }
    }

    if (!best) return null;
    return {
        ticker,
        epic: best.epic,
        source: 'discovered',
    };
}

export async function resolveCapitalEpicRuntime(symbol: string): Promise<ResolveEpicResult> {
    const ticker = normalizeTicker(symbol);
    const cached = resolvedEpicCache.get(ticker);
    if (cached) return cached;

    const preferred = resolveCapitalEpic(ticker);
    const fallbackBase = baseFromTicker(ticker);
    const candidates = Array.from(
        new Set(
            [preferred.epic, ticker, fallbackBase]
                .map((v) => normalizeTicker(v))
                .filter((v) => v.length > 0),
        ),
    );

    for (const epic of candidates) {
        try {
            await capitalFetch('GET', `/api/v1/markets/${encodeURIComponent(epic)}`, {}, undefined, true);
            const result: ResolveEpicResult = {
                ticker,
                epic,
                source: epic === preferred.epic ? preferred.source : 'discovered',
            };
            resolvedEpicCache.set(ticker, result);
            return result;
        } catch (err) {
            if (isCapitalEpicNotFoundError(err)) continue;
            throw err;
        }
    }

    const discovered = await discoverCapitalEpic(ticker, candidates);
    if (discovered) {
        resolvedEpicCache.set(ticker, discovered);
        return discovered;
    }

    throw new Error(
        `Capital epic resolution failed for ${ticker}. Set CAPITAL_TICKER_EPIC_MAP for this symbol.`,
    );
}

export async function fetchCapitalCandlesByEpic(epic: string, timeframe: string, limit = 200): Promise<any[]> {
    const resolution = toCapitalResolution(timeframe);
    const payload = await capitalFetch(
        'GET',
        `/api/v1/prices/${encodeURIComponent(epic)}`,
        {
            resolution,
            max: Math.max(20, Math.min(limit, 1000)),
        },
        undefined,
        true,
    );
    return parseCapitalCandles(payload);
}

export async function fetchCapitalMarketBundle(symbol: string, bundleTimeFrame: string, opts: BundleOpts = {}) {
    const { includeTrades = true, tradeMinutes = Number(TRADE_WINDOW_MINUTES || 60), candleLimit = 30 } = opts;
    const resolved = await resolveCapitalEpicRuntime(symbol);
    const candles = await fetchCapitalCandlesByEpic(resolved.epic, bundleTimeFrame, candleLimit + 10);
    const ticker = buildCapitalTicker(candles, bundleTimeFrame);
    const last = safeNumber(ticker.last, 0);
    const orderbook = buildSyntheticOrderbook(last);

    let trades: any[] = [];
    if (includeTrades) {
        const tfMinutes = Math.max(1, timeframeToMinutes(bundleTimeFrame));
        const bars = Math.max(1, Math.ceil(tradeMinutes / tfMinutes));
        trades = candles.slice(-bars).map((c) => ({
            ts: Number(c?.[0]),
            price: Number(c?.[4]),
            size: Number(c?.[5]) || 1,
        }));
    }

    return {
        ticker,
        candles,
        trades,
        orderbook,
        funding: null,
        fundingHistory: null,
        oi: null,
        productType: 'capital-cfd',
        epic: resolved.epic,
        mappingSource: resolved.source,
    };
}

async function loadMarketDetails(epic: string): Promise<MarketDetails> {
    const payload = await capitalFetch('GET', `/api/v1/markets/${encodeURIComponent(epic)}`, {}, undefined, true);
    const market = payload?.market ?? payload?.data ?? payload;
    const bid = safeNumber(market?.snapshot?.bid ?? market?.bid, NaN);
    const offer = safeNumber(market?.snapshot?.offer ?? market?.offer, NaN);
    const minDealSize = safeNumber(market?.dealingRules?.minDealSize?.value, NaN);
    const minDealSizeSafe = Number.isFinite(minDealSize) && minDealSize > 0 ? minDealSize : null;
    const sizeDecimals = numberOfDecimals(minDealSizeSafe ?? 0.0001);
    return {
        bid: Number.isFinite(bid) ? bid : null,
        offer: Number.isFinite(offer) ? offer : null,
        minDealSize: minDealSizeSafe,
        sizeDecimals,
        epic: String(market?.epic ?? epic),
    };
}

function extractDirection(position: CapitalPositionRow): 'long' | 'short' | null {
    const raw = String(position?.position?.direction ?? position?.direction ?? '').toUpperCase();
    if (raw === 'BUY' || raw === 'LONG') return 'long';
    if (raw === 'SELL' || raw === 'SHORT') return 'short';
    return null;
}

function extractEntryPrice(position: CapitalPositionRow): number | null {
    const value = safeNumber(position?.position?.level ?? position?.position?.openLevel ?? position?.level ?? position?.openLevel, NaN);
    return Number.isFinite(value) && value > 0 ? value : null;
}

function extractLeverage(position: CapitalPositionRow): number | null {
    const value = safeNumber(position?.position?.leverage, NaN);
    return Number.isFinite(value) && value > 0 ? value : null;
}

function extractPositionSize(position: CapitalPositionRow): number | null {
    const value = safeNumber(position?.position?.size ?? position?.size, NaN);
    return Number.isFinite(value) && value > 0 ? value : null;
}

function extractEntryTimestamp(position: CapitalPositionRow): number | undefined {
    const ts = toIsoTimestampMs(position?.position?.createdDateUTC ?? position?.position?.createdDate);
    if (!Number.isFinite(ts as number)) return undefined;
    return Number(ts);
}

function extractDealId(position: CapitalPositionRow): string | null {
    const id = position?.position?.dealId ?? position?.dealId;
    if (!id) return null;
    return String(id);
}

async function listOpenCapitalPositions(): Promise<CapitalPositionRow[]> {
    const payload = await capitalFetch('GET', '/api/v1/positions', {}, undefined, true);
    return extractPositionRows(payload);
}

async function findOpenCapitalPositionByEpic(epic: string): Promise<CapitalPositionRow | null> {
    const rows = await listOpenCapitalPositions();
    const match = rows.find((row) => String(row?.market?.epic || '').toUpperCase() === normalizeTicker(epic));
    return match ?? null;
}

function computeOpenPnlPct(position: CapitalPositionRow, details: MarketDetails | null): number | null {
    const side = extractDirection(position);
    const entry = extractEntryPrice(position);
    if (!side || !entry || entry <= 0) return null;

    const bid = safeNumber(details?.bid, NaN);
    const offer = safeNumber(details?.offer, NaN);
    const mark = Number.isFinite(bid) && Number.isFinite(offer) ? (bid + offer) / 2 : Number.isFinite(bid) ? bid : offer;
    if (!Number.isFinite(mark) || mark <= 0) return null;

    const lev = extractLeverage(position) ?? 1;
    const sideSign = side === 'long' ? 1 : -1;
    const pct = ((mark - entry) / entry) * sideSign * lev * 100;
    return Number.isFinite(pct) ? pct : null;
}

export async function fetchCapitalPositionInfo(symbol: string): Promise<PositionInfo> {
    const resolved = await resolveCapitalEpicRuntime(symbol);
    const open = await findOpenCapitalPositionByEpic(resolved.epic);
    if (!open) return { status: 'none' };

    let details: MarketDetails | null = null;
    try {
        details = await loadMarketDetails(resolved.epic);
    } catch {
        details = null;
    }

    const side = extractDirection(open);
    const entryPrice = extractEntryPrice(open);
    if (!side || !entryPrice) return { status: 'none' };

    const pnlPct = computeOpenPnlPct(open, details);
    return {
        status: 'open',
        symbol: symbol.toUpperCase(),
        holdSide: side,
        entryPrice: entryPrice.toString(),
        entryTimestamp: extractEntryTimestamp(open),
        currentPnl: Number.isFinite(pnlPct as number) ? `${Number(pnlPct).toFixed(2)}%` : '0.00%',
        leverage: extractLeverage(open),
    };
}

export async function fetchCapitalRealizedRoi(_symbol: string, _hours = 24) {
    return {
        roi: null,
        count: 0,
        sumPct: null,
        last: null,
        lastNet: null,
        lastNetPct: null,
        lastSide: null as 'long' | 'short' | null,
    };
}

function buildSimpleMetrics(candles: any[]) {
    const closes = candles.map((c) => Number(c?.[4])).filter((v) => Number.isFinite(v));
    const last = closes.at(-1);
    const prev = closes.at(-2);
    const prev2 = closes.at(-3);
    let structure: 'bull' | 'bear' | 'range' = 'range';
    if (
        Number.isFinite(last as number) &&
        Number.isFinite(prev as number) &&
        Number.isFinite(prev2 as number) &&
        (last as number) > (prev as number) &&
        (prev as number) > (prev2 as number)
    ) {
        structure = 'bull';
    } else if (
        Number.isFinite(last as number) &&
        Number.isFinite(prev as number) &&
        Number.isFinite(prev2 as number) &&
        (last as number) < (prev as number) &&
        (prev as number) < (prev2 as number)
    ) {
        structure = 'bear';
    }
    return {
        structure,
        bos: false,
        bosDir: null as 'up' | 'down' | null,
        structureBreakState: 'inside' as 'above' | 'below' | 'inside',
        choch: false,
        breakoutRetestOk: false,
        breakoutRetestDir: null as 'up' | 'down' | null,
    };
}

export async function calculateCapitalMultiTFIndicators(
    symbol: string,
    opts: IndicatorTimeframeOptions = {},
): Promise<MultiTFIndicators> {
    const microTF = normalizeTimeframe(opts.micro || MICRO_TIMEFRAME);
    const macroTF = normalizeTimeframe(opts.macro || MACRO_TIMEFRAME);
    const primaryTF = normalizeTimeframe(opts.primary || PRIMARY_TIMEFRAME);
    const contextTF = normalizeTimeframe(opts.context || CONTEXT_TIMEFRAME);
    const epic = (await resolveCapitalEpicRuntime(symbol)).epic;

    const byTf = new Map<string, any[]>();
    const tfs = Array.from(new Set([microTF, macroTF, primaryTF, contextTF]));
    await Promise.all(
        tfs.map(async (tf) => {
            const candles = await fetchCapitalCandlesByEpic(epic, tf, 200);
            byTf.set(tf, candles);
        }),
    );

    const microCandles = byTf.get(microTF) ?? [];
    const macroCandles = byTf.get(macroTF) ?? [];
    const primaryCandles = byTf.get(primaryTF) ?? [];
    const contextCandles = byTf.get(contextTF) ?? [];

    const out: MultiTFIndicators = {
        micro: formatSummary(microCandles),
        macro: formatSummary(macroCandles),
        microTimeFrame: microTF,
        macroTimeFrame: macroTF,
        contextTimeFrame: contextTF,
        primary: {
            timeframe: primaryTF,
            summary: formatSummary(primaryCandles),
        },
        context: {
            timeframe: contextTF,
            summary: formatSummary(contextCandles),
        },
        candleDepth: {
            [microTF]: microCandles.length,
            [macroTF]: macroCandles.length,
            [primaryTF]: primaryCandles.length,
            [contextTF]: contextCandles.length,
        },
        sr: {},
        metrics: {
            [microTF]: buildSimpleMetrics(microCandles),
            [macroTF]: buildSimpleMetrics(macroCandles),
            [primaryTF]: buildSimpleMetrics(primaryCandles),
            [contextTF]: buildSimpleMetrics(contextCandles),
        },
    };
    return out;
}

async function openCapitalPosition(params: {
    symbol: string;
    direction: 'BUY' | 'SELL';
    sideSizeUSDT: number;
    leverage: number | null;
    clientOid: string;
}) {
    const { symbol, direction, sideSizeUSDT, leverage, clientOid } = params;
    const resolved = await resolveCapitalEpicRuntime(symbol);
    const details = await loadMarketDetails(resolved.epic);
    const priceCandidate = Number.isFinite(details.bid as number) && Number.isFinite(details.offer as number)
        ? ((details.bid as number) + (details.offer as number)) / 2
        : Number.isFinite(details.bid as number)
          ? (details.bid as number)
          : Number.isFinite(details.offer as number)
            ? (details.offer as number)
            : NaN;
    const fallbackBundle = !Number.isFinite(priceCandidate)
        ? await fetchCapitalMarketBundle(symbol, PRIMARY_TIMEFRAME, { includeTrades: false, candleLimit: 20 })
        : null;
    const referencePrice = Number.isFinite(priceCandidate) && priceCandidate > 0
        ? priceCandidate
        : safeNumber(fallbackBundle?.ticker?.last, 0);
    if (!(referencePrice > 0)) throw new Error(`Cannot derive reference price for ${symbol}`);

    const orderNotional = sideSizeUSDT * (leverage ?? 1);
    const rawSize = orderNotional / referencePrice;
    const size = quantizeSize(rawSize, details.minDealSize, details.sizeDecimals);
    if (!(size > 0)) throw new Error(`Computed non-positive order size for ${symbol}`);

    const body: Record<string, unknown> = {
        epic: details.epic,
        direction,
        size,
        orderType: 'MARKET',
        currencyCode: 'USD',
        forceOpen: true,
        dealReference: clientOid,
    };

    if (leverage) body.leverage = leverage;

    const payload = await capitalFetch('POST', '/api/v1/positions', {}, body, true);
    const orderId = payload?.dealId ?? payload?.dealReference ?? payload?.positionDealId ?? payload?.id ?? null;
    return { payload, orderId, size, epic: details.epic };
}

async function closeCapitalPosition(position: CapitalPositionRow, partialClosePct: number | null, clientOid: string) {
    const dealId = extractDealId(position);
    if (!dealId) throw new Error('Open Capital position missing dealId');

    const fullSize = extractPositionSize(position);
    const side = extractDirection(position);
    const closeDirection = side === 'long' ? 'SELL' : side === 'short' ? 'BUY' : null;
    if (!closeDirection) throw new Error('Cannot resolve close direction for Capital position');
    const requestedSize =
        partialClosePct !== null && partialClosePct < 100 && Number.isFinite(fullSize as number)
            ? Number(fullSize) * (partialClosePct / 100)
            : fullSize;

    try {
        const payload = await capitalFetch('DELETE', `/api/v1/positions/${encodeURIComponent(dealId)}`, {}, undefined, true);
        return { payload, orderId: payload?.dealId ?? payload?.dealReference ?? dealId, partial: false };
    } catch (err) {
        if (!(Number.isFinite(requestedSize as number) && (requestedSize as number) > 0)) throw err;
        const payload = await capitalFetch(
            'DELETE',
            '/api/v1/positions',
            {},
            {
                dealId,
                direction: closeDirection,
                size: requestedSize,
                orderType: 'MARKET',
                dealReference: clientOid,
            },
            true,
        );
        return {
            payload,
            orderId: payload?.dealId ?? payload?.dealReference ?? dealId,
            partial: partialClosePct !== null && partialClosePct < 100,
        };
    }
}

export async function executeCapitalDecision(symbol: string, sideSizeUSDT: number, decision: TradeDecision, dryRun = true) {
    const clientOid = `cap-${crypto.randomUUID()}`;
    const leverage = deriveLeverage(decision);
    const partialClosePct =
        normalizeClosePct((decision as any)?.exit_size_pct) ??
        normalizeClosePct((decision as any)?.close_size_pct) ??
        normalizeClosePct((decision as any)?.partial_close_pct);

    if (decision.action === 'BUY' || decision.action === 'SELL') {
        if (dryRun) return { placed: false, orderId: null, clientOid, leverage };
        const direction = decision.action === 'BUY' ? 'BUY' : 'SELL';
        const opened = await openCapitalPosition({
            symbol,
            direction,
            sideSizeUSDT,
            leverage,
            clientOid,
        });
        return {
            placed: true,
            orderId: opened.orderId,
            clientOid,
            leverage,
            size: opened.size,
            epic: opened.epic,
        };
    }

    if (decision.action === 'CLOSE') {
        if (dryRun) return { placed: false, orderId: null, clientOid, closed: true, partialClosePct };
        const resolved = await resolveCapitalEpicRuntime(symbol);
        const open = await findOpenCapitalPositionByEpic(resolved.epic);
        if (!open) return { placed: false, orderId: null, clientOid, closed: false, note: 'no open position' };
        const closed = await closeCapitalPosition(open, partialClosePct, clientOid);
        return {
            placed: true,
            orderId: closed.orderId,
            clientOid,
            closed: true,
            partial: closed.partial,
            partialClosePct,
        };
    }

    if (decision.action === 'REVERSE') {
        if (dryRun) return { placed: false, orderId: null, clientOid, reversed: true, leverage };
        const resolved = await resolveCapitalEpicRuntime(symbol);
        const open = await findOpenCapitalPositionByEpic(resolved.epic);
        if (!open) return { placed: false, orderId: null, clientOid, reversed: false, note: 'no open position' };
        const side = extractDirection(open);
        if (!side) return { placed: false, orderId: null, clientOid, reversed: false, note: 'unknown position side' };

        const closed = await closeCapitalPosition(open, 100, clientOid);
        const direction = side === 'long' ? 'SELL' : 'BUY';
        const opened = await openCapitalPosition({
            symbol,
            direction,
            sideSizeUSDT,
            leverage,
            clientOid,
        });
        return {
            placed: true,
            orderId: opened.orderId ?? closed.orderId,
            clientOid,
            reversed: true,
            leverage,
            size: opened.size,
            epic: opened.epic,
        };
    }

    return { placed: false, orderId: null, clientOid };
}
