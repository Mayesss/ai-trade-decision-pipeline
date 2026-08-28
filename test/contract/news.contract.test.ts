// Contract: lib/news.ts — CoinDesk for crypto, Marketaux for capital markets,
// both behind the path-style KV cache (6h TTL, 59min freshness window).

import { expect, test } from 'vitest';

import { fetchNewsWithHeadlines } from '../../lib/news';
import { conversation, conversationSummary, startBoundary } from '../harness';
import { kvWorld } from '../harness/worlds/kv';
import { coindeskNews, marketauxNews } from '../harness/worlds/news';

startBoundary(() => ({
    http: [
        ...kvWorld(),
        coindeskNews([
            { title: 'Bitcoin holds above 60k', sentiment: 'POSITIVE' },
            { title: 'Miners rotate treasuries', sentiment: 'NEUTRAL' },
        ]),
        marketauxNews([{ title: 'Copper rallies on supply squeeze', sentimentScore: 0.4 }]),
    ],
}));

test('CoinDesk fetch caches in KV; a repeat within the window never leaves the cache', async () => {
    const first = await fetchNewsWithHeadlines('BTCUSDT', { source: 'coindesk' });
    expect(first.headlines).toEqual(['Bitcoin holds above 60k', 'Miners rotate treasuries']);
    expect(first.sentiment).toBe('POSITIVE');

    const second = await fetchNewsWithHeadlines('BTCUSDT', { source: 'coindesk' });
    expect(second).toEqual(first);

    const coindeskCalls = (await conversationSummary()).filter((line) => line.includes('data-api.coindesk.com'));
    expect(coindeskCalls).toHaveLength(1);

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/news-coindesk-cached.txt');
});

test('Marketaux commodity news goes out as a search query with the api_token in the URL', async () => {
    const result = await fetchNewsWithHeadlines('COPPER', {
        platform: 'capital',
        source: 'marketaux',
        category: 'commodity',
    });
    expect(result.headlines).toEqual(['Copper rallies on supply squeeze']);
    expect(result.sentiment).toBe('POSITIVE');

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/news-marketaux-search.txt');
});
