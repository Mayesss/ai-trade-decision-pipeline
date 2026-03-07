import assert from 'node:assert/strict';
import test from 'node:test';

import { evaluateExecutionMonotonicity, findExecutionMonotonicityViolations } from './executionMonotonicity';

test('execution monotonicity passes when worse assumptions degrade outcomes', () => {
    const rows = [
        { label: 'baseline', severityRank: 0, netR: 12, expectancyR: 0.3, profitFactor: 1.9, maxDrawdownR: 4.1 },
        { label: 'slippage_x2', severityRank: 1, netR: 10, expectancyR: 0.24, profitFactor: 1.7, maxDrawdownR: 4.5 },
        { label: 'slippage_x2_spread_1.50x', severityRank: 2, netR: 8, expectancyR: 0.18, profitFactor: 1.5, maxDrawdownR: 5.2 },
    ];

    const out = evaluateExecutionMonotonicity(rows);
    assert.equal(out.ok, true);
    assert.equal(out.violations.length, 0);
});

test('execution monotonicity flags expectancy/netR improvements under worse assumptions', () => {
    const rows = [
        { label: 'baseline', severityRank: 0, netR: 12, expectancyR: 0.3, profitFactor: 1.9, maxDrawdownR: 4.1 },
        { label: 'spread_1.50x', severityRank: 1, netR: 12.4, expectancyR: 0.31, profitFactor: 1.95, maxDrawdownR: 4.2 },
    ];

    const violations = findExecutionMonotonicityViolations(rows);
    assert.equal(violations.length, 3);
    assert.deepEqual(
        violations.map((row) => row.metric).sort(),
        ['expectancyR', 'netR', 'profitFactor'],
    );
});

test('execution monotonicity flags drawdown improvement under harsher assumptions', () => {
    const rows = [
        { label: 'baseline', severityRank: 0, netR: 6, expectancyR: 0.12, profitFactor: 1.2, maxDrawdownR: 7.4 },
        { label: 'slippage_x2', severityRank: 1, netR: 5.7, expectancyR: 0.11, profitFactor: 1.18, maxDrawdownR: 6.9 },
    ];

    const violations = findExecutionMonotonicityViolations(rows);
    assert.equal(violations.length, 1);
    assert.equal(violations[0]?.metric, 'maxDrawdownR');
});
