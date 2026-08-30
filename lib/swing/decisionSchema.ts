// lib/swing/decisionSchema.ts
//
// JSON Schemas for the decision response, handed to whichever provider client
// serves the call (structured outputs). Two variants: the crypto/Bitget one
// carries model-chosen leverage, the Capital one omits it because the broker
// fixes leverage per asset class.

// Strict Structured-Outputs schema for the swing decision. Mirrors the JSON the prompt
// asks for; strict mode requires every property in `required` and additionalProperties:false.
// Nullable fields use a type union (e.g. ['integer','null']).
export const SWING_DECISION_SCHEMA = {
    name: 'swing_decision',
    schema: {
        type: 'object',
        additionalProperties: false,
        required: [
            'action',
            'summary',
            'reason',
            'exit_size_pct',
            'leverage',
            'raise_leverage_to',
            'move_stop_to_be',
            'take_profit_price',
            'stop_loss_price',
            'entry_limit_price',
            'entry_trigger_price',
            'cooldown_minutes',
            'cooldown_wake_above',
            'cooldown_wake_below',
            'cooldown_wake_note',
            'cooldown_wake_sustain_minutes',
        ],
        properties: {
            action: { type: 'string', enum: ['BUY', 'SELL', 'HOLD', 'CLOSE', 'REVERSE'] },
            summary: { type: 'string' },
            reason: { type: 'string' },
            exit_size_pct: { type: ['number', 'null'], minimum: 0, maximum: 100 },
            leverage: { type: ['integer', 'null'], minimum: 5, maximum: 10 },
            // Profit-lock margin-recycle maneuver (crypto only). Execution clamps
            // raise_leverage_to to [current, symbol max]; 125 is a generous ceiling.
            raise_leverage_to: { type: ['integer', 'null'], minimum: 1, maximum: 125 },
            move_stop_to_be: { type: ['boolean', 'null'] },
            // Exchange-side bracket. Entry: take_profit_price is the resting TP
            // attached with the order; stop_loss_price is the structural
            // invalidation stop (null = code-owned 3×ATR catastrophe default).
            // In-position: either field amends the standing bracket (null =
            // leave unchanged; stop amends tighten-only). Price-level sanity
            // (side/distance vs live price+ATR) is enforced in code after parse.
            take_profit_price: { type: ['number', 'null'], minimum: 0 },
            stop_loss_price: { type: ['number', 'null'], minimum: 0 },
            // Pullback limit entry (flat BUY/SELL, only when the
            // SWING_PULLBACK_LIMIT_ENABLED day-trade flag is on): rest a LIMIT
            // at this price instead of entering at market. One-tick TTL —
            // cancelled at the next evaluation if unfilled. null = market;
            // with the flag off (swing default) a non-null value drops the
            // entry (sanitizeEntryLimit).
            entry_limit_price: { type: ['number', 'null'], minimum: 0 },
            // Failed-break watch: on a breakout/breakdown-thesis entry, the
            // trigger level whose break justifies the trade. Code watches it —
            // a later primary bar closing back through it wakes the model with
            // market.failed_break. null for bounce/range/other theses.
            entry_trigger_price: { type: ['number', 'null'], minimum: 0 },
            // Flat-HOLD cooldown: quiet period request (minutes, code-clamped)
            // with optional wake bands that end it early when price crosses
            // them. Only honored when flat + action=HOLD.
            cooldown_minutes: { type: ['integer', 'null'] },
            cooldown_wake_above: { type: ['number', 'null'], minimum: 0 },
            cooldown_wake_below: { type: ['number', 'null'], minimum: 0 },
            // One-line plan the band encodes — persisted with the cooldown row
            // and echoed back as market.cooldown_wake.note when the band fires.
            cooldown_wake_note: { type: ['string', 'null'] },
            // Sustained confirmation for breakout-intent bands: wake only if
            // price still beyond the band this many minutes after first touch
            // (code-clamped; flat bands only). null = instant touch wake.
            cooldown_wake_sustain_minutes: { type: ['integer', 'null'], minimum: 0 },
        },
    },
} as const;

// Capital decides leverage by asset class, so the model is not asked for it and
// the schema omits the field entirely (strict structured-output requires the
// schema to match the prompt's JSON exactly — no leverage key at all).
export const SWING_DECISION_SCHEMA_NO_LEVERAGE = {
    name: 'swing_decision',
    schema: {
        type: 'object',
        additionalProperties: false,
        required: [
            'action',
            'summary',
            'reason',
            'exit_size_pct',
            'take_profit_price',
            'stop_loss_price',
            'entry_limit_price',
            'entry_trigger_price',
            'cooldown_minutes',
            'cooldown_wake_above',
            'cooldown_wake_below',
            'cooldown_wake_note',
            'cooldown_wake_sustain_minutes',
        ],
        properties: {
            action: { type: 'string', enum: ['BUY', 'SELL', 'HOLD', 'CLOSE', 'REVERSE'] },
            summary: { type: 'string' },
            reason: { type: 'string' },
            exit_size_pct: { type: ['number', 'null'], minimum: 0, maximum: 100 },
            // Exchange-side bracket (see SWING_DECISION_SCHEMA).
            take_profit_price: { type: ['number', 'null'], minimum: 0 },
            stop_loss_price: { type: ['number', 'null'], minimum: 0 },
            entry_limit_price: { type: ['number', 'null'], minimum: 0 },
            // Failed-break watch (see SWING_DECISION_SCHEMA).
            entry_trigger_price: { type: ['number', 'null'], minimum: 0 },
            // Flat-HOLD cooldown (see SWING_DECISION_SCHEMA).
            cooldown_minutes: { type: ['integer', 'null'] },
            cooldown_wake_above: { type: ['number', 'null'], minimum: 0 },
            cooldown_wake_below: { type: ['number', 'null'], minimum: 0 },
            cooldown_wake_note: { type: ['string', 'null'] },
            // Sustained confirmation (see SWING_DECISION_SCHEMA).
            cooldown_wake_sustain_minutes: { type: ['integer', 'null'], minimum: 0 },
        },
    },
} as const;

// Transport lives in the provider clients: lib/openAi.ts (Responses API) and
// lib/claudeAi.ts (Messages API), switched by lib/aiProvider.ts. This module is
// the swing-decision DOMAIN — prompt assembly, post-processing and schemas.
