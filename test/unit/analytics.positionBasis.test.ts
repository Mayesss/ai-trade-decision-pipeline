import assert from 'node:assert/strict';
import { test } from 'vitest';

import { describeOpenPositionBasis } from '../../lib/analytics';

// An open position's percent divides by the margin the VENUE actually holds
// (marginSize), not by size x mark / leverage. The two agree while a position
// is untouched and diverge hard after a partial close, because Bitget keeps the
// isolated margin the full-size position posted.

// The real 2026-09-08 ADAUSDT row: 554 ADA opened at 20x, trimmed to 167, so
// 5.51 of margin still sits behind a 38.28 notional (effectively ~6.6x). The
// dashboard read 104% off the size x mark / 20 estimate (1.91) while Bitget
// reported 36% off the 5.51 it was really holding.
const ADA_AFTER_TRIM = {
    total: '167',
    markPrice: '0.2292',
    leverage: '20',
    unrealizedPL: '2.004',
    marginSize: '5.509908333253',
    openPriceAvg: '0.2172',
};

test('trimmed position: percent is on the margin the venue holds, not size x mark / leverage', () => {
    const basis = describeOpenPositionBasis(ADA_AFTER_TRIM);
    assert.equal(basis.pnlPercent, '36.37%');
    assert.equal(basis.marginUsd, 5.509908333253);
    // The estimate this replaced, kept as the thing that must NOT come back.
    assert.notEqual(basis.pnlPercent, '104.71%');
});

test('effective leverage is entry notional / posted margin, not the venue setting', () => {
    const basis = describeOpenPositionBasis(ADA_AFTER_TRIM);
    assert.ok(basis.effectiveLeverage !== null);
    assert.ok(Math.abs((basis.effectiveLeverage as number) - 6.583122223847) < 1e-9);
});

// The client rescales open PnL from its own live quote as
// priceMove x side x effectiveLeverage. Fed the same price the server used, it
// has to land on the server's percent — otherwise header, pill and chart
// disagree about one position.
test('effective leverage reproduces the server percent from a price move', () => {
    for (const row of [
        ADA_AFTER_TRIM,
        // Untouched long: margin == size x entry / leverage, so effective
        // leverage collapses back onto the venue setting.
        { total: '500', markPrice: '0.21', leverage: '10', unrealizedPL: '5', marginSize: '10', openPriceAvg: '0.2' },
        // Short: the sign comes from the side, not the basis.
        { total: '100', markPrice: '95', leverage: '5', unrealizedPL: '500', marginSize: '2000', openPriceAvg: '100' },
    ]) {
        const basis = describeOpenPositionBasis(row);
        const entry = Number(row.openPriceAvg);
        const mark = Number(row.markPrice);
        const side = Number(row.unrealizedPL) >= 0 === mark >= entry ? 1 : -1;
        const client = ((mark - entry) / entry) * side * (basis.effectiveLeverage as number) * 100;
        assert.equal(client.toFixed(2) + '%', basis.pnlPercent);
    }
});

test('untouched position: effective leverage is the venue setting', () => {
    const basis = describeOpenPositionBasis({
        total: '500',
        markPrice: '0.21',
        leverage: '10',
        unrealizedPL: '5',
        marginSize: '10',
        openPriceAvg: '0.2',
    });
    assert.equal(basis.pnlPercent, '50.00%');
    assert.equal(basis.effectiveLeverage, 10);
});

// Venues that report no margin (Capital rows, and the replayed contract
// fixtures) must keep the old derived basis rather than lose their percent.
test('no venue margin: falls back to the size x mark / leverage estimate', () => {
    const basis = describeOpenPositionBasis({
        total: '500',
        markPrice: '0.21',
        leverage: '10',
        unrealizedPL: '5',
        openPriceAvg: '0.2',
    });
    assert.equal(basis.marginUsd, 10.5);
    assert.equal(basis.pnlPercent, '47.62%');
    // Still self-consistent: the client's rescale of the fallback basis lands
    // on the same percent.
    assert.ok(Math.abs((basis.effectiveLeverage as number) - 9.523809523809524) < 1e-9);
});

test('unusable row: no basis, no divide-by-zero percent', () => {
    const basis = describeOpenPositionBasis({ total: '0', unrealizedPL: '0' });
    assert.equal(basis.marginUsd, null);
    assert.equal(basis.pnlPercent, '0.00%');
    assert.equal(basis.effectiveLeverage, null);
});
