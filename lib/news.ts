// lib/news.ts

import { COINDESK_API_BASE, COINDESK_NEWS_LIST_PATH } from './constants';

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

/** Extracts the base asset ticker (e.g., BTC from BTCUSDT or ETH-PERP). */
export function baseFromSymbol(symbol: string): string {
    const s = symbol.toUpperCase();
    const QUOTES = ['USDT', 'USDC', 'USD', 'BTC', 'ETH', 'EUR', 'PERP'];
    for (const q of QUOTES) {
        if (s.endsWith(q)) return s.slice(0, s.length - q.length).replace(/[-_]/g, '');
    }
    return s.replace(/[^A-Z].*$/, '');
}

// ------------------------------
// Sentiment Aggregation
// ------------------------------

export type Sentiment = 'NEGATIVE' | 'NEUTRAL' | 'POSITIVE';

interface Article {
    SENTIMENT: Sentiment;
    PUBLISHED_ON: number;
    TITLE: string;
}

interface Payload {
    Data: Article[];
}

/** Determine the dominant sentiment from a payload of news articles. */
export function getDominantSentiment(payload: Payload): Sentiment {
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

/** Fetch latest CoinDesk news and compute overall sentiment. */
export async function fetchNewsSentiment(symbolOrBase: string): Promise<Sentiment | null> {
    const { sentiment } = await fetchNewsWithHeadlines(symbolOrBase);
    return sentiment;
}

/** Fetch latest CoinDesk news, compute sentiment, and return top 3 titles. */
export async function fetchNewsWithHeadlines(
    symbolOrBase: string,
): Promise<{ sentiment: Sentiment | null; headlines: string[] }> {
    const base = baseFromSymbol(symbolOrBase);
    const listPath = COINDESK_NEWS_LIST_PATH || '/news/v1/article/list';

    const query = {
        categories: base,
        limit: 25,
        lang: 'EN',
    };

    let payload: any;
    try {
        payload = await coindeskFetch(listPath, query);
    } catch (e) {
         console.warn('Error fetching CoinDesk news:', e);
        return { sentiment: null, headlines: [] };
    }

    const sentiment = getDominantSentiment(payload) || 'NEUTRAL';
    const headlines = Array.isArray(payload?.Data)
        ? payload.Data.sort((a: Article, b: Article) => b.PUBLISHED_ON - a.PUBLISHED_ON)
              .slice(0, 3)
              .map((a: Article) => a.TITLE)
        : [];
    return { sentiment, headlines };
}
