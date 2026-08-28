// WORLD BUILDER: news providers — CoinDesk and Marketaux.
//
// CoinDesk (crypto): Bearer-authed GET, payload { Data: [ARTICLE, ...] } with
// SCREAMING_CASE fields (lib/news.ts fetchCoinDeskNews).
// Marketaux (capital markets): api_token QUERY PARAM (no header), payload
// { data: [article, ...] } lowercase (lib/news.ts fetchMarketauxNews).

import { http, HttpResponse } from 'msw';

import type { RequestHandler } from 'msw';

import type { Sentiment } from '../../../lib/news';

export const COINDESK_HOST = 'https://data-api.coindesk.com';
export const MARKETAUX_HOST = 'https://api.marketaux.com';

export interface CoinDeskArticleSeed {
    title: string;
    sentiment?: Sentiment;
    publishedOnMs?: number;
}

export function coindeskNews(articles: CoinDeskArticleSeed[]): RequestHandler {
    return http.get(`${COINDESK_HOST}/news/v1/article/list`, () =>
        HttpResponse.json({
            Data: articles.map((article, index) => ({
                TITLE: article.title,
                SENTIMENT: article.sentiment ?? 'NEUTRAL',
                // Seconds, newest first by default
                PUBLISHED_ON: Math.floor((article.publishedOnMs ?? Date.now() - index * 60_000) / 1000),
            })),
        }),
    );
}

export interface MarketauxArticleSeed {
    title: string;
    publishedAt?: string;
    sentimentScore?: number;
}

export function marketauxNews(articles: MarketauxArticleSeed[]): RequestHandler {
    return http.get(`${MARKETAUX_HOST}/v1/news/all`, () =>
        HttpResponse.json({
            data: articles.map((article, index) => ({
                title: article.title,
                published_at: article.publishedAt ?? new Date(Date.now() - index * 60_000).toISOString(),
                sentiment_score: article.sentimentScore ?? 0,
            })),
        }),
    );
}
