import assert from 'node:assert/strict';
import test from 'node:test';

import { fetchNewsWithHeadlines, resolveMarketauxCommoditySearchTerm, resolveMarketauxIndexSearchTerm } from './news';

test('resolveMarketauxCommoditySearchTerm maps commodity trading symbols to searchable terms', () => {
    assert.equal(resolveMarketauxCommoditySearchTerm('GOLD'), 'gold');
    assert.equal(resolveMarketauxCommoditySearchTerm('OIL_CRUDE'), 'crude oil');
    assert.equal(resolveMarketauxCommoditySearchTerm('NATURALGAS'), 'natural gas');
    assert.equal(resolveMarketauxCommoditySearchTerm('COPPER'), 'copper');
    assert.equal(resolveMarketauxCommoditySearchTerm('WTI'), 'wti crude oil');
});

test('resolveMarketauxIndexSearchTerm maps scoped index CFD aliases only', () => {
    assert.equal(resolveMarketauxIndexSearchTerm('US100'), 'Nasdaq 100');
    assert.equal(resolveMarketauxIndexSearchTerm('HK50'), 'Hang Seng Index');
    assert.equal(resolveMarketauxIndexSearchTerm('TLT'), null);
});

test('Marketaux commodity news uses a single search request', async () => {
    const originalFetch = globalThis.fetch;
    const originalKey = process.env.MARKETAUX_API_KEY;
    const urls: string[] = [];

    process.env.MARKETAUX_API_KEY = 'test-token';
    globalThis.fetch = (async (input: RequestInfo | URL) => {
        urls.push(String(input));
        return new Response(
            JSON.stringify({
                data: [
                    {
                        title: 'Copper market headline',
                        published_at: '2026-06-23T12:25:00.000000Z',
                        sentiment_score: 0.2,
                    },
                ],
            }),
            { status: 200 },
        );
    }) as typeof fetch;

    try {
        const result = await fetchNewsWithHeadlines('COPPER', {
            platform: 'capital',
            source: 'marketaux',
            category: 'commodity',
        });

        assert.deepEqual(result.headlines, ['Copper market headline']);
        assert.equal(urls.length, 1);

        const url = new URL(urls[0]);
        assert.equal(url.searchParams.get('search'), 'copper');
        assert.equal(url.searchParams.has('symbols'), false);
        assert.equal(url.searchParams.get('language'), 'en');
        assert.equal(url.searchParams.has('published_after'), true);
    } finally {
        globalThis.fetch = originalFetch;
        if (originalKey === undefined) {
            delete process.env.MARKETAUX_API_KEY;
        } else {
            process.env.MARKETAUX_API_KEY = originalKey;
        }
    }
});

test('Marketaux mapped index CFD news uses a single search request', async () => {
    const originalFetch = globalThis.fetch;
    const originalKey = process.env.MARKETAUX_API_KEY;
    const urls: string[] = [];

    process.env.MARKETAUX_API_KEY = 'test-token';
    globalThis.fetch = (async (input: RequestInfo | URL) => {
        urls.push(String(input));
        return new Response(
            JSON.stringify({
                data: [
                    {
                        title: 'Nasdaq 100 headline',
                        published_at: '2026-06-24T16:43:39.000000Z',
                        sentiment_score: 0.1,
                    },
                ],
            }),
            { status: 200 },
        );
    }) as typeof fetch;

    try {
        const result = await fetchNewsWithHeadlines('US100', {
            platform: 'capital',
            source: 'marketaux',
            category: 'index',
        });

        assert.deepEqual(result.headlines, ['Nasdaq 100 headline']);
        assert.equal(urls.length, 1);

        const url = new URL(urls[0]);
        assert.equal(url.searchParams.get('search'), 'Nasdaq 100');
        assert.equal(url.searchParams.has('symbols'), false);
        assert.equal(url.searchParams.has('published_after'), true);
    } finally {
        globalThis.fetch = originalFetch;
        if (originalKey === undefined) {
            delete process.env.MARKETAUX_API_KEY;
        } else {
            process.env.MARKETAUX_API_KEY = originalKey;
        }
    }
});

test('Marketaux unmapped index and non-index news keep symbol lookup', async () => {
    const originalFetch = globalThis.fetch;
    const originalKey = process.env.MARKETAUX_API_KEY;
    const urls: string[] = [];

    process.env.MARKETAUX_API_KEY = 'test-token';
    globalThis.fetch = (async (input: RequestInfo | URL) => {
        urls.push(String(input));
        return new Response(
            JSON.stringify({
                data: [
                    {
                        title: 'Equity headline',
                        published_at: '2026-06-23T12:25:00.000000Z',
                        sentiment_score: 0.1,
                    },
                ],
            }),
            { status: 200 },
        );
    }) as typeof fetch;

    try {
        const indexResult = await fetchNewsWithHeadlines('TLT', {
            platform: 'capital',
            source: 'marketaux',
            category: 'index',
        });
        const equityResult = await fetchNewsWithHeadlines('AAPL', {
            platform: 'capital',
            source: 'marketaux',
            category: 'equity',
        });

        assert.deepEqual(indexResult.headlines, ['Equity headline']);
        assert.deepEqual(equityResult.headlines, ['Equity headline']);
        assert.equal(urls.length, 2);

        const url = new URL(urls[0]);
        assert.equal(url.searchParams.get('symbols'), 'TLT');
        assert.equal(url.searchParams.has('search'), false);

        const equityUrl = new URL(urls[1]);
        assert.equal(equityUrl.searchParams.get('symbols'), 'AAPL');
        assert.equal(equityUrl.searchParams.has('search'), false);
    } finally {
        globalThis.fetch = originalFetch;
        if (originalKey === undefined) {
            delete process.env.MARKETAUX_API_KEY;
        } else {
            process.env.MARKETAUX_API_KEY = originalKey;
        }
    }
});
