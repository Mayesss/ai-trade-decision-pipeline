// lib/news.ts

import { COINDESK_API_BASE, COINDESK_NEWS_LIST_PATH, MARKETAUX_API_BASE } from './constants';
import type { AnalysisPlatform, NewsSource } from './platform';

// ------------------------------
// KV cache (6h TTL)
// ------------------------------

const upstash_payasyougo_KV_REST_API_URL = (process.env.upstash_payasyougo_KV_REST_API_URL || '').replace(/\/$/, '');
const upstash_payasyougo_KV_REST_API_TOKEN = process.env.upstash_payasyougo_KV_REST_API_TOKEN || '';
const NEWS_CACHE_TTL_SECONDS = 6 * 60 * 60; // 6 hours — swing holds 1–10 days; hourly news freshness is overkill and would blow news-API quotas at hourly cadence
const MARKETAUX_SEARCH_LOOKBACK_DAYS = 7;

type NewsFetchOptions = { source?: NewsSource; platform?: AnalysisPlatform; category?: string | null };

function ensureKvConfig() {
    return upstash_payasyougo_KV_REST_API_URL && upstash_payasyougo_KV_REST_API_TOKEN;
}

async function kvGet(key: string): Promise<string | null> {
    if (!ensureKvConfig()) return null;
    const res = await fetch(`${upstash_payasyougo_KV_REST_API_URL}/GET/${encodeURIComponent(key)}`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${upstash_payasyougo_KV_REST_API_TOKEN}` },
    });
    const data = await res.json();
    if (!res.ok || data.error) return null;
    return data.result ?? null;
}

async function kvSetEx(key: string, ttlSeconds: number, value: string) {
    if (!ensureKvConfig()) return;
    await fetch(`${upstash_payasyougo_KV_REST_API_URL}/SETEX/${encodeURIComponent(key)}/${ttlSeconds}/${encodeURIComponent(value)}`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${upstash_payasyougo_KV_REST_API_TOKEN}` },
    });
}

function newsCacheKey(base: string, source: NewsSource, variant?: string | null) {
    const suffix = variant ? `:${variant}` : '';
    return `news:${source}:${base.toUpperCase()}${suffix}`;
}

function resolveSource(source?: NewsSource, platform?: AnalysisPlatform): NewsSource {
    if (source === 'coindesk' || source === 'marketaux') return source;
    return platform === 'capital' ? 'marketaux' : 'coindesk';
}

// ------------------------------
// CoinDesk API helpers
// ------------------------------

/** Low-level fetch helper for CoinDesk API */
export async function coindeskFetch(path: string, query: Record<string, string | number | undefined> = {}) {
    if (!process.env.COINDESK_API_KEY) throw new Error('Missing COINDESK_API_KEY');

    const qs = Object.entries(query)
        .filter(([, v]) => v !== undefined && v !== '')
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
        .join('&');

    const coindeskBase = (COINDESK_API_BASE || 'https://data-api.coindesk.com').replace(/\/+$/, '');
    const url = `${coindeskBase}${path}${qs ? `?${qs}` : ''}`;

    const res = await fetch(url, {
        headers: { Authorization: `Bearer ${process.env.COINDESK_API_KEY}` },
    });

    if (!res.ok) {
        const text = await res.text().catch(() => '');
        throw new Error(`CoinDesk API error ${res.status}: ${res.statusText} ${text}`);
    }

    return res.json();
}

// ------------------------------
// Marketaux API helpers
// ------------------------------

async function marketauxFetch(path: string, query: Record<string, string | number | undefined> = {}) {
    if (!process.env.MARKETAUX_API_KEY) throw new Error('Missing MARKETAUX_API_KEY');

    const qs = Object.entries({
        ...query,
        api_token: process.env.MARKETAUX_API_KEY,
    })
        .filter(([, v]) => v !== undefined && v !== '')
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
        .join('&');

    const base = (MARKETAUX_API_BASE || 'https://api.marketaux.com/v1').replace(/\/+$/, '');
    const url = `${base}${path}${qs ? `?${qs}` : ''}`;
    const res = await fetch(url);
    if (!res.ok) {
        const text = await res.text().catch(() => '');
        throw new Error(`Marketaux API error ${res.status}: ${res.statusText} ${text}`);
    }
    return res.json();
}

const MARKETAUX_SYMBOL_ALIASES: Record<string, string> = {
    PLATINUM: 'PLAT',
    OIL_CRUDE: 'WTI',
    COFFEEARABICA: 'KC',
};

const MARKETAUX_COMMODITY_SEARCH_TERMS: Record<string, string> = {
    GOLD: 'gold',
    XAU: 'gold',
    SILVER: 'silver',
    XAG: 'silver',
    OIL: 'crude oil',
    OIL_CRUDE: 'crude oil',
    USOIL: 'crude oil',
    WTI: 'wti crude oil',
    BRENT: 'brent crude oil',
    NATURALGAS: 'natural gas',
    NATGAS: 'natural gas',
    NGAS: 'natural gas',
    COPPER: 'copper',
    PLATINUM: 'platinum',
    PALLADIUM: 'palladium',
};

const MARKETAUX_INDEX_SEARCH_TERMS: Record<string, string> = {
    US100: 'Nasdaq 100',
    NAS100: 'Nasdaq 100',
    US500: 'S&P 500',
    SPX: 'S&P 500',
    DJ30: 'Dow Jones Industrial Average',
    HK50: 'Hang Seng Index',
    GER40: 'DAX 40',
    DE40: 'DAX 40',
    UK100: 'FTSE 100',
    JP225: 'Nikkei 225',
    J225: 'Nikkei 225',
    FR40: 'CAC 40',
    EU50: 'Euro Stoxx 50',
};

function resolveMarketauxQuerySymbols(base: string): string {
    const normalized = base.toUpperCase();
    const alias = MARKETAUX_SYMBOL_ALIASES[normalized];
    return alias && alias !== normalized ? `${alias},${normalized}` : normalized;
}

function normalizeMarketauxSearchTerm(base: string): string {
    return String(base || '')
        .trim()
        .toLowerCase()
        .replace(/[_-]+/g, ' ')
        .replace(/\s+/g, ' ');
}

export function resolveMarketauxCommoditySearchTerm(base: string): string {
    const normalized = String(base || '').trim().toUpperCase();
    return MARKETAUX_COMMODITY_SEARCH_TERMS[normalized] || normalizeMarketauxSearchTerm(normalized);
}

export function resolveMarketauxIndexSearchTerm(base: string): string | null {
    const normalized = String(base || '').trim().toUpperCase();
    return MARKETAUX_INDEX_SEARCH_TERMS[normalized] || null;
}

function isCommodityCategory(category?: string | null): boolean {
    return String(category || '').trim().toLowerCase() === 'commodity';
}

function isIndexCategory(category?: string | null): boolean {
    return String(category || '').trim().toLowerCase() === 'index';
}

function resolveMarketauxSearchTerm(base: string, category?: string | null): string | null {
    if (isCommodityCategory(category)) return resolveMarketauxCommoditySearchTerm(base);
    if (isIndexCategory(category)) return resolveMarketauxIndexSearchTerm(base);
    return null;
}

function daysAgoDate(days: number): string {
    return new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
}

/** Extracts the base asset ticker (e.g., BTC from BTCUSDT or ETH-PERP). */
export function baseFromSymbol(symbol: string): string {
    const s = symbol.toUpperCase();
    const QUOTES = ['USDT', 'USDC', 'USD', 'BTC', 'ETH', 'EUR', 'PERP'];
    for (const q of QUOTES) {
        if (s.endsWith(q)) return s.slice(0, s.length - q.length).replace(/[-_]/g, '');
    }
    const alnum = s.replace(/[^A-Z0-9._-]/g, '');
    return alnum || s;
}

// ------------------------------
// Sentiment Aggregation
// ------------------------------

export type Sentiment = 'NEGATIVE' | 'NEUTRAL' | 'POSITIVE';

interface CoinDeskArticle {
    SENTIMENT: Sentiment;
    PUBLISHED_ON: number;
    TITLE: string;
}

interface CoinDeskPayload {
    Data: CoinDeskArticle[];
}

/** Determine the dominant sentiment from a CoinDesk payload. */
export function getDominantSentiment(payload: CoinDeskPayload): Sentiment {
    const sorted = [...payload.Data].sort((a, b) => b.PUBLISHED_ON - a.PUBLISHED_ON);
    const scores: Record<Sentiment, number> = {
        NEGATIVE: 0,
        NEUTRAL: 0,
        POSITIVE: 0,
    };

    const weightStep = 0.1;

    sorted.forEach((item, index) => {
        const weight = Math.max(1 - index * weightStep, 0.1);
        scores[item.SENTIMENT] += weight;
    });

    return Object.entries(scores).reduce((a, b) => (b[1] > a[1] ? b : a))[0] as Sentiment;
}

function scoreToSentiment(score: number): Sentiment {
    if (score >= 0.15) return 'POSITIVE';
    if (score <= -0.15) return 'NEGATIVE';
    return 'NEUTRAL';
}

function sentimentLabelToScore(label: string | undefined): number {
    const normalized = String(label || '').trim().toLowerCase();
    if (!normalized) return 0;
    if (normalized.includes('positive') || normalized === 'bullish') return 1;
    if (normalized.includes('negative') || normalized === 'bearish') return -1;
    return 0;
}

function tsFromAny(value: unknown): number {
    const num = Number(value);
    if (Number.isFinite(num) && num > 0) {
        return num > 1e12 ? num : num * 1000;
    }
    const parsed = Date.parse(String(value ?? ''));
    return Number.isFinite(parsed) ? parsed : 0;
}

function scoreFromMarketauxArticle(article: any): number {
    const entityScores = Array.isArray(article?.entities)
        ? article.entities
              .map((entity: any) => {
                  const score = Number(entity?.sentiment_score);
                  if (Number.isFinite(score)) return score;
                  return sentimentLabelToScore(entity?.sentiment);
              })
              .filter((v: number) => Number.isFinite(v))
        : [];

    if (entityScores.length) {
        return entityScores.reduce((acc: number, v: number) => acc + v, 0) / entityScores.length;
    }

    const articleScore = Number(article?.sentiment_score);
    if (Number.isFinite(articleScore)) return articleScore;
    return sentimentLabelToScore(article?.sentiment);
}

async function fetchCoinDeskNews(base: string): Promise<{ sentiment: Sentiment | null; headlines: string[] }> {
    const listPath = COINDESK_NEWS_LIST_PATH || '/news/v1/article/list';
    const payload = await coindeskFetch(listPath, {
        categories: base,
        limit: 25,
        lang: 'EN',
    });
    const sentiment = getDominantSentiment(payload) || 'NEUTRAL';
    const headlines = Array.isArray(payload?.Data)
        ? payload.Data.sort((a: CoinDeskArticle, b: CoinDeskArticle) => b.PUBLISHED_ON - a.PUBLISHED_ON)
              .slice(0, 5)
              .map((a: CoinDeskArticle) => a.TITLE)
        : [];
    return { sentiment, headlines };
}

async function fetchMarketauxNews(base: string, opts?: { category?: string | null }): Promise<{ sentiment: Sentiment | null; headlines: string[] }> {
    const searchTerm = resolveMarketauxSearchTerm(base, opts?.category);
    const payload = await marketauxFetch('/news/all', {
        ...(searchTerm
            ? {
                  search: searchTerm,
                  published_after: daysAgoDate(MARKETAUX_SEARCH_LOOKBACK_DAYS),
              }
            : {
                  symbols: resolveMarketauxQuerySymbols(base),
              }),
        language: 'en',
        limit: 25,
        filter_entities: 'true',
    });

    const rows = Array.isArray(payload?.data) ? payload.data : [];
    const sorted = rows
        .slice()
        .sort((a: any, b: any) => tsFromAny(b?.published_at ?? b?.published_on) - tsFromAny(a?.published_at ?? a?.published_on));

    let weightedSum = 0;
    let totalWeight = 0;
    sorted.forEach((row: any, index: number) => {
        const weight = Math.max(1 - index * 0.1, 0.1);
        const score = scoreFromMarketauxArticle(row);
        weightedSum += score * weight;
        totalWeight += weight;
    });

    const avgScore = totalWeight > 0 ? weightedSum / totalWeight : 0;
    const sentiment = scoreToSentiment(avgScore);
    const headlines = sorted
        .slice(0, 5)
        .map((row: any) => String(row?.title || '').trim())
        .filter((title: string) => title.length > 0);
    return { sentiment, headlines };
}

export async function fetchNewsSentiment(
    symbolOrBase: string,
    opts?: NewsFetchOptions,
): Promise<Sentiment | null> {
    const { sentiment } = await fetchNewsWithHeadlines(symbolOrBase, opts);
    return sentiment;
}

export async function fetchNewsWithHeadlines(
    symbolOrBase: string,
    opts?: NewsFetchOptions,
): Promise<{ sentiment: Sentiment | null; headlines: string[] }> {
    const base = baseFromSymbol(symbolOrBase);
    const source = resolveSource(opts?.source, opts?.platform);
    const cacheVariant = source === 'marketaux' && resolveMarketauxSearchTerm(base, opts?.category) ? 'search' : null;
    const cacheKey = newsCacheKey(base, source, cacheVariant);

    try {
        const cached = await kvGet(cacheKey);
        if (cached) {
            const parsed = JSON.parse(cached);
            const ageMs = Date.now() - Number(parsed?.timestamp || 0);
            if (Number.isFinite(ageMs) && ageMs < 59 * 60 * 1000) {
                return {
                    sentiment: parsed?.sentiment ?? null,
                    headlines: Array.isArray(parsed?.headlines) ? parsed.headlines : [],
                };
            }
        }
    } catch (err) {
        console.warn('Failed to read news cache:', err);
    }

    try {
        const result =
            source === 'marketaux' ? await fetchMarketauxNews(base, { category: opts?.category }) : await fetchCoinDeskNews(base);
        try {
            await kvSetEx(
                cacheKey,
                NEWS_CACHE_TTL_SECONDS,
                JSON.stringify({
                    timestamp: Date.now(),
                    sentiment: result.sentiment,
                    headlines: result.headlines,
                }),
            );
        } catch (err) {
            console.warn('Failed to write news cache:', err);
        }
        return result;
    } catch (err) {
        console.warn(`Error fetching ${source} news:`, err);
        return { sentiment: null, headlines: [] };
    }
}
