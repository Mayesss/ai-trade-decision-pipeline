// Contract: lib/bitget.ts — every call is signed (ACCESS-* headers) and only
// the { code: "00000" } envelope counts as success; the module returns .data.

import { expect, test } from 'vitest';

import { bitgetFetch } from '../../lib/bitget';
import { conversation, startBoundary } from '../harness';
import { bitgetError, bitgetGet, bitgetPost } from '../harness/worlds/bitget';

const TICKER = [{ symbol: 'BTCUSDT', lastPr: '65000.5', bidPr: '65000', askPr: '65001' }];

const boundary = startBoundary({
    http: [
        bitgetGet('/api/v2/mix/market/ticker', TICKER),
        bitgetPost('/api/v2/mix/order/place-order', { orderId: 'order-1', clientOid: 'client-1' }),
    ],
});

test('signed market GET and order POST unwrap the success envelope', async () => {
    const ticker = await bitgetFetch('GET', '/api/v2/mix/market/ticker', {
        symbol: 'BTCUSDT',
        productType: 'USDT-FUTURES',
    });
    expect(ticker).toEqual(TICKER);

    const order = await bitgetFetch(
        'POST',
        '/api/v2/mix/order/place-order',
        {},
        { symbol: 'BTCUSDT', productType: 'USDT-FUTURES', side: 'buy', orderType: 'market', size: '0.01' },
    );
    expect(order).toEqual({ orderId: 'order-1', clientOid: 'client-1' });

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/bitget-signed-calls.txt');
});

test('a Bitget business error (HTTP 200, code != 00000) throws typed', async () => {
    boundary.use(bitgetGet('/api/v2/mix/market/ticker', () => bitgetError('40034', 'Parameter does not exist')));

    await expect(
        bitgetFetch('GET', '/api/v2/mix/market/ticker', { symbol: 'NOPEUSDT', productType: 'USDT-FUTURES' }),
    ).rejects.toMatchObject({ code: '40034' });
});
