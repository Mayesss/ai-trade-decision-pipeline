import assert from 'node:assert/strict';
import test from 'node:test';

import { fetchNewsWithHeadlines, resolveMarketauxCommoditySearchTerm } from './news';

test('resolveMarketauxCommoditySearchTerm maps commodity trading symbols to searchable terms', () => {
    assert.equal(resolveMarketauxCommoditySearchTerm('GOLD'), 'gold');
    assert.equal(resolveMarketauxCommoditySearchTerm('OIL_CRUDE'), 'crude oil');
    assert.equal(resolveMarketauxCommoditySearchTerm('NATURALGAS'), 'natural gas');
    assert.equal(resolveMarketauxCommoditySearchTerm('COPPER'), 'copper');
    assert.equal(resolveMarketauxCommoditySearchTerm('WTI'), 'wti crude oil');
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

test('Marketaux non-commodity news keeps symbol lookup', async () => {
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
        const result = await fetchNewsWithHeadlines('AAPL', {
            platform: 'capital',
            source: 'marketaux',
            category: 'equity',
        });

        assert.deepEqual(result.headlines, ['Equity headline']);
        assert.equal(urls.length, 1);

        const url = new URL(urls[0]);
        assert.equal(url.searchParams.get('symbols'), 'AAPL');
        assert.equal(url.searchParams.has('search'), false);
    } finally {
        globalThis.fetch = originalFetch;
        if (originalKey === undefined) {
            delete process.env.MARKETAUX_API_KEY;
        } else {
            process.env.MARKETAUX_API_KEY = originalKey;
        }
    }
});
