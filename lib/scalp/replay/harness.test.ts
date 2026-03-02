import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

import { defaultScalpReplayConfig, normalizeScalpReplayInput, runScalpReplay } from './harness';
import type { ScalpReplayInputFile } from './types';

test('scalp replay sample fixture produces deterministic non-empty run summary', async () => {
    const here = path.dirname(fileURLToPath(import.meta.url));
    const fixturePath = path.resolve(here, '../../../data/scalp-replay/fixtures/eurusd.sample.json');
    const raw = await readFile(fixturePath, 'utf8');
    const input = normalizeScalpReplayInput(JSON.parse(raw) as ScalpReplayInputFile);
    const config = defaultScalpReplayConfig(input.symbol);

    const result = runScalpReplay({
        candles: input.candles,
        pipSize: input.pipSize,
        config,
    });

    assert.equal(result.summary.symbol, input.symbol);
    assert.ok(result.summary.runs > 0, 'expected replay runs > 0');
    assert.ok(result.timeline.length > 0, 'expected non-empty replay timeline');
    assert.ok(result.summary.trades > 0, 'expected sample fixture to generate at least one trade');
    assert.ok(Number.isFinite(result.summary.expectancyR), 'expectancy must be finite');
    assert.ok(Number.isFinite(result.summary.maxDrawdownR), 'maxDrawdownR must be finite');
});
