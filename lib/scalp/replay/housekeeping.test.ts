import assert from 'node:assert/strict';
import test from 'node:test';

import { shouldPruneResearchCycle } from '../housekeeping';

test('shouldPruneResearchCycle keeps active or fresh cycles', () => {
    const nowMs = Date.UTC(2026, 2, 7, 12, 0, 0, 0);
    const retentionMs = 14 * 24 * 60 * 60_000;

    assert.equal(
        shouldPruneResearchCycle({
            cycle: { cycleId: 'rc_a', status: 'completed', updatedAtMs: nowMs - retentionMs - 1 },
            nowMs,
            activeCycleId: 'rc_a',
            retentionMs,
            cycleIdFromKey: 'rc_a',
        }),
        false,
    );

    assert.equal(
        shouldPruneResearchCycle({
            cycle: { cycleId: 'rc_b', status: 'completed', updatedAtMs: nowMs - 1000 },
            nowMs,
            activeCycleId: null,
            retentionMs,
            cycleIdFromKey: 'rc_b',
        }),
        false,
    );
});

test('shouldPruneResearchCycle prunes stale completed/failed/stalled cycles', () => {
    const nowMs = Date.UTC(2026, 2, 7, 12, 0, 0, 0);
    const retentionMs = 14 * 24 * 60 * 60_000;

    for (const status of ['completed', 'failed', 'stalled']) {
        assert.equal(
            shouldPruneResearchCycle({
                cycle: { cycleId: `rc_${status}`, status, updatedAtMs: nowMs - retentionMs - 1 },
                nowMs,
                activeCycleId: null,
                retentionMs,
                cycleIdFromKey: `rc_${status}`,
            }),
            true,
        );
    }
});

test('shouldPruneResearchCycle prunes stale non-active running cycles only after extended age', () => {
    const nowMs = Date.UTC(2026, 2, 7, 12, 0, 0, 0);
    const retentionMs = 14 * 24 * 60 * 60_000;

    assert.equal(
        shouldPruneResearchCycle({
            cycle: { cycleId: 'rc_running1', status: 'running', updatedAtMs: nowMs - retentionMs - 1 },
            nowMs,
            activeCycleId: null,
            retentionMs,
            cycleIdFromKey: 'rc_running1',
        }),
        false,
    );

    assert.equal(
        shouldPruneResearchCycle({
            cycle: { cycleId: 'rc_running2', status: 'running', updatedAtMs: nowMs - 2 * retentionMs - 1 },
            nowMs,
            activeCycleId: null,
            retentionMs,
            cycleIdFromKey: 'rc_running2',
        }),
        true,
    );
});
