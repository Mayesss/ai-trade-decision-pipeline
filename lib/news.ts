// lib/news.ts

import { COINDESK_API_BASE, COINDESK_NEWS_LIST_PATH, MARKETAUX_API_BASE } from './constants';
import type { AnalysisPlatform, NewsSource } from './platform';

// ------------------------------
// KV cache (1h TTL)
// ------------------------------

const KV_REST_API_URL = (process.env.KV_REST_API_URL || '').replace(/\/$/, '');
const KV_REST_API_TOKEN = process.env.KV_REST_API_TOKEN || '';
const NEWS_CACHE_TTL_SECONDS = 60 * 60; // 1 hour

function ensureKvConfig() {
    return KV_REST_API_URL && KV_REST_API_TOKEN;
}

async function kvGet(key: string): Promise<string | null> {
    if (!ensureKvConfig()) return null;
    const res = await fetch(`${KV_REST_API_URL}/GET/${encodeURIComponent(key)}`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${KV_REST_API_TOKEN}` },
    });
    const data = await res.json();
    if (!res.ok || data.error) return null;
    return data.result ?? null;
}

async function kvSetEx(key: string, ttlSeconds: number, value: string) {
    if (!ensureKvConfig()) return;
    await fetch(`${KV_REST_API_URL}/SETEX/${encodeURIComponent(key)}/${ttlSeconds}/${encodeURIComponent(value)}`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${KV_REST_API_TOKEN}` },
    });
}

function newsCacheKey(base: string, source: NewsSource) {
    return `news:${source}:${base.toUpperCase()}`;
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

function resolveMarketauxQuerySymbols(base: string): string {
    const normalized = base.toUpperCase();
    const alias = MARKETAUX_SYMBOL_ALIASES[normalized];
    return alias && alias !== normalized ? `${alias},${normalized}` : normalized;
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

async function fetchMarketauxNews(base: string): Promise<{ sentiment: Sentiment | null; headlines: string[] }> {
    const querySymbols = resolveMarketauxQuerySymbols(base);
    const payload = await marketauxFetch('/news/all', {
        symbols: querySymbols,
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
    opts?: { source?: NewsSource; platform?: AnalysisPlatform },
): Promise<Sentiment | null> {
    const { sentiment } = await fetchNewsWithHeadlines(symbolOrBase, opts);
    return sentiment;
}

export async function fetchNewsWithHeadlines(
    symbolOrBase: string,
    opts?: { source?: NewsSource; platform?: AnalysisPlatform },
): Promise<{ sentiment: Sentiment | null; headlines: string[] }> {
    const base = baseFromSymbol(symbolOrBase);
    const source = resolveSource(opts?.source, opts?.platform);
    const cacheKey = newsCacheKey(base, source);

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
            source === 'marketaux' ? await fetchMarketauxNews(base) : await fetchCoinDeskNews(base);
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
