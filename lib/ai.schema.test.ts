import assert from 'node:assert/strict';
import test from 'node:test';

import { SWING_DECISION_SCHEMA } from './ai';

// ---------------------------------------------------------------------------
// Minimal JSON-Schema validator covering only the features SWING_DECISION_SCHEMA
// uses (object / required / additionalProperties:false / string+enum /
// number|integer with min/max / boolean / nullable type unions). Enough to
// assert the decision contract without pulling in a runtime dependency.
// ---------------------------------------------------------------------------
function validate(value: any, schema: any): boolean {
    const types: string[] = Array.isArray(schema.type) ? schema.type : [schema.type];

    if (value === null) return types.includes('null');

    if (types.includes('object') && typeof value === 'object' && !Array.isArray(value)) {
        const props = schema.properties ?? {};
        const required: string[] = schema.required ?? [];
        if (schema.additionalProperties === false) {
            for (const key of Object.keys(value)) if (!(key in props)) return false;
        }
        for (const key of required) if (!(key in value)) return false;
        for (const [key, sub] of Object.entries(props)) {
            if (key in value && !validate(value[key], sub)) return false;
        }
        return true;
    }

    if (typeof value === 'string') {
        if (!types.includes('string')) return false;
        if (schema.enum && !schema.enum.includes(value)) return false;
        return true;
    }

    if (typeof value === 'boolean') return types.includes('boolean');

    if (typeof value === 'number') {
        const numericOk = types.includes('number') || (types.includes('integer') && Number.isInteger(value));
        if (!numericOk) return false;
        if (typeof schema.minimum === 'number' && value < schema.minimum) return false;
        if (typeof schema.maximum === 'number' && value > schema.maximum) return false;
        return true;
    }

    return false;
}

test('SWING_DECISION_SCHEMA satisfies OpenAI strict structured-output invariants', () => {
    const schema = SWING_DECISION_SCHEMA.schema as any;
    // strict mode rejects extra keys
    assert.equal(schema.additionalProperties, false);
    // strict mode requires EVERY property to appear in `required`
    const propKeys = Object.keys(schema.properties).sort();
    const requiredKeys = [...schema.required].sort();
    assert.deepEqual(requiredKeys, propKeys);
});

test('valid swing decisions conform to the schema', () => {
    const manageOff = { raise_leverage_to: null, move_stop_to_be: null };
    const noBracket = { take_profit_price: null, stop_loss_price: null, entry_limit_price: null };
    const valid = [
        // entry with a resting exchange-side TP target
        {
            action: 'BUY',
            summary: 'long',
            reason: 'breakout retest',
            exit_size_pct: null,
            leverage: 3,
            ...manageOff,
            take_profit_price: 71250.5,
            stop_loss_price: null,
            entry_limit_price: 70100,
        },
        { action: 'HOLD', summary: 'wait', reason: 'chop', exit_size_pct: null, leverage: null, ...manageOff, ...noBracket },
        // in-position trim that also amends the standing bracket
        {
            action: 'CLOSE',
            summary: 'trim',
            reason: 'into resistance',
            exit_size_pct: 50,
            leverage: null,
            ...manageOff,
            take_profit_price: 72000,
            stop_loss_price: 68000,
            entry_limit_price: null,
        },
        { action: 'REVERSE', summary: 'flip', reason: 'structure flip', exit_size_pct: 100, leverage: 1, ...manageOff, ...noBracket },
        // margin-recycle maneuver: BE stop + leverage raise on an in-profit HOLD
        {
            action: 'HOLD',
            summary: 'lock profit',
            reason: 'cushion, recycle margin',
            exit_size_pct: null,
            leverage: null,
            raise_leverage_to: 50,
            move_stop_to_be: true,
            ...noBracket,
        },
    ];
    for (const d of valid) assert.ok(validate(d, SWING_DECISION_SCHEMA.schema), `expected valid: ${JSON.stringify(d)}`);
});

test('invalid swing decisions are rejected', () => {
    const base = {
        action: 'BUY',
        summary: 's',
        reason: 'r',
        exit_size_pct: null,
        leverage: 2,
        raise_leverage_to: null,
        move_stop_to_be: null,
        take_profit_price: null,
        stop_loss_price: null,
        entry_limit_price: null,
    };
    // the base itself is valid, so each case below fails for its intended reason
    assert.ok(validate(base, SWING_DECISION_SCHEMA.schema));
    // action not in enum
    assert.ok(!validate({ ...base, action: 'WAIT' }, SWING_DECISION_SCHEMA.schema));
    // leverage out of range
    assert.ok(!validate({ ...base, leverage: 9 }, SWING_DECISION_SCHEMA.schema));
    // leverage not an integer
    assert.ok(!validate({ ...base, leverage: 2.5 }, SWING_DECISION_SCHEMA.schema));
    // exit_size_pct out of range
    assert.ok(!validate({ ...base, exit_size_pct: 150 }, SWING_DECISION_SCHEMA.schema));
    // raise_leverage_to above the 125 ceiling / not an integer
    assert.ok(!validate({ ...base, raise_leverage_to: 200 }, SWING_DECISION_SCHEMA.schema));
    assert.ok(!validate({ ...base, raise_leverage_to: 2.5 }, SWING_DECISION_SCHEMA.schema));
    // move_stop_to_be must be boolean or null
    assert.ok(!validate({ ...base, move_stop_to_be: 'yes' }, SWING_DECISION_SCHEMA.schema));
    // bracket prices must be numbers ≥ 0 or null
    assert.ok(!validate({ ...base, take_profit_price: 'above resistance' }, SWING_DECISION_SCHEMA.schema));
    assert.ok(!validate({ ...base, stop_loss_price: -1 }, SWING_DECISION_SCHEMA.schema));
    // missing required key
    const { reason, ...missing } = base;
    assert.ok(!validate(missing, SWING_DECISION_SCHEMA.schema));
    // extra key (additionalProperties: false)
    assert.ok(!validate({ ...base, confidence: 'HIGH' }, SWING_DECISION_SCHEMA.schema));
});
