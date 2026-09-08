import assert from 'node:assert/strict';
import { test } from 'vitest';

import { openThreadStartMs } from '../../../components/ChartPanel';

// Where the bright decision-timeline thread starts for an open position. An
// order that rested before filling was DECIDED earlier than it was FILLED, and
// the timeline is the decision chain, so it reaches back to the deciding tick
// (2026-09-08 US100: decided 04:02 UTC, filled ~05:08, and the thread used to
// start at the fill). The position box is unchanged and still starts at the
// fill — that one marks exposure, which genuinely did not exist during the wait.

const FILL_SEC = Math.floor(Date.UTC(2026, 8, 8, 5, 8, 0) / 1000);
const FILL_MS = FILL_SEC * 1000;
const DECIDED_MS = Date.UTC(2026, 8, 8, 4, 2, 54);

test('a resting order that waited starts the thread at the deciding tick', () => {
    assert.equal(openThreadStartMs(FILL_SEC, DECIDED_MS), DECIDED_MS);
});

test('a market entry starts the thread at the fill', () => {
    // Decision and fill within the same moment (280ms apart, as a market entry
    // actually records) — the fill is the start.
    assert.equal(openThreadStartMs(FILL_SEC, FILL_MS - 280), FILL_MS);
});

test('a sub-noise gap starts the thread at the fill, not a hair earlier', () => {
    assert.equal(openThreadStartMs(FILL_SEC, FILL_MS - 2 * 60_000), FILL_MS);
});

test('exactly at the display threshold counts as a wait', () => {
    assert.equal(
        openThreadStartMs(FILL_SEC, FILL_MS - 3 * 60_000),
        FILL_MS - 3 * 60_000,
    );
});

test('no entry decision falls back to the fill', () => {
    assert.equal(openThreadStartMs(FILL_SEC, null), FILL_MS);
    assert.equal(openThreadStartMs(FILL_SEC, undefined), FILL_MS);
});

test('an order still RESTING highlights nothing — the reach back needs a fill', () => {
    // The caller only reaches this for an overlay with status 'open', and an
    // overlay exists solely for a filled position (resting orders are a
    // separate channel — the dashed limit line). A missing fill therefore means
    // "not filled yet", and it must yield no thread at all rather than a span
    // running from the order tick to now.
    assert.equal(openThreadStartMs(null, DECIDED_MS), null);
    assert.equal(openThreadStartMs(undefined, DECIDED_MS), null);
    assert.equal(openThreadStartMs(Number.NaN, DECIDED_MS), null);
    assert.equal(openThreadStartMs(0, DECIDED_MS), null);
});

test('a decision AFTER the fill never pulls the thread forward', () => {
    // Row lag: the decision row can persist just after the fill it caused.
    assert.equal(openThreadStartMs(FILL_SEC, FILL_MS + 90_000), FILL_MS);
});
