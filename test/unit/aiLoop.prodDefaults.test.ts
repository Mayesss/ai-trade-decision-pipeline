// What PRODUCTION runs when no env var is set, for the switches that changed
// on 2026-09-17: the lesson library and the post-mortem analyst, both off. The
// model pair is asserted here too because the pair has an invariant one id
// cannot break alone (exactly one Anthropic id), and because opus-5 was tried
// and reverted the same day on cost — docs/alpha-lab-spec.md §13.

import assert from 'node:assert/strict';
import { test, vi } from 'vitest';

import { dialectForAiModel, vendorForAiModel } from '../../lib/aiModel';
import { DEFAULT_AI_MODEL, FALLBACK_AI_MODEL } from '../../lib/constants';
import { resolveSwingLessonsMode } from '../../lib/swing/lessons';
import { resolveSwingPostmortemMode } from '../../lib/swing/postmortem';

test('the model pair: glm-5.3 decides, opus-4.8 holds the other dialect', () => {
    assert.equal(DEFAULT_AI_MODEL, 'zai/glm-5.3');
    assert.equal(FALLBACK_AI_MODEL, 'anthropic/claude-opus-4.8');
    // The pair must straddle the two dialects — aiModelForDialect returns the
    // fallback for whichever dialect the default does not speak, so two ids on
    // the same side would leave one dialect pointing at the wrong vendor.
    assert.equal(dialectForAiModel(DEFAULT_AI_MODEL), 'responses');
    assert.equal(dialectForAiModel(FALLBACK_AI_MODEL), 'messages');
    assert.equal(vendorForAiModel(DEFAULT_AI_MODEL), 'zai');
});

test('the lesson loop is off unless an env var turns it on', () => {
    vi.stubEnv('SWING_LESSONS_MODE', undefined);
    vi.stubEnv('SWING_POSTMORTEM_MODE', undefined);
    // Injection off: no lesson block reaches the trading prompt.
    assert.equal(resolveSwingLessonsMode(), 'off');
    // Write side off: no analyst call, so no new lessons are minted either.
    assert.equal(resolveSwingPostmortemMode(), 'off');
});

test('both halves are re-enablable without a deploy', () => {
    vi.stubEnv('SWING_LESSONS_MODE', 'on');
    assert.equal(resolveSwingLessonsMode(), 'on');
    vi.stubEnv('SWING_POSTMORTEM_MODE', 'all');
    assert.equal(resolveSwingPostmortemMode(), 'all');
    vi.stubEnv('SWING_POSTMORTEM_MODE', 'loss');
    assert.equal(resolveSwingPostmortemMode(), 'loss');
    // Anything unrecognized stays off rather than falling back to on.
    vi.stubEnv('SWING_LESSONS_MODE', 'yes');
    assert.equal(resolveSwingLessonsMode(), 'off');
});
