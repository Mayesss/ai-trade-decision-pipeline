// ai-bouncer: a cheap triage model that runs AFTER every deterministic hard
// gate on FLAT entry scans and decides whether the expensive decision call is
// worth making. It can only SKIP work — it never unlocks anything a hard gate
// blocked, and it NEVER runs on open-position or wake-band calls (the caller
// enforces those bypasses; see pages/api/analyze.ts). Named "bouncer" on
// purpose: "nano" is a timeframe in this codebase and "gate" means the
// deterministic stack.
//
// Fail-open by design: any failure (flag off, missing key, HTTP error,
// timeout, unparseable verdict) returns null and the caller proceeds to the
// full call. Deliberately does NOT touch lib/swing/aiHealth.ts — a bouncer
// outage must not latch the global AI-health flag.
//
// Gateway compatibility note: called via the OpenAI-compatible
// /chat/completions endpoint (the /responses path used by lib/ai.ts is
// unverified for spacexai/* ids). Structured output is attempted via
// response_format json_schema; a 400/422 falls back once to instruct-JSON.

import { AI_BASE_URL } from '../constants';
import { resolveAiGatewayKey } from '../aiModel';

// Compact technical snapshot — ONLY values already computed by the tick
// (computeSwingState().context + actionability); building it triggers zero
// extra fetches.
export type AiBouncerInput = {
    symbol: string;
    platform: string;
    category: string | null;
    price: number | null;
    change_24h_pct: number | null;
    signal_strength: string | null;
    micro_bias_calc: string | null;
    primary_bias: string | null;
    macro_bias: string | null;
    context_bias: string | null;
    primary_trend_up: boolean;
    primary_trend_down: boolean;
    primary_breakout_confirmed: boolean;
    primary_breakdown_confirmed: boolean;
    micro_entry_ok: boolean;
    aligned_driver_count: number | null;
    // Regime alignment score in [-1, 1] (computeSwingState).
    regime_alignment: number | null;
    location_confluence_score: number | null;
    micro_extension_atr: number | null;
    primary_extension_atr: number | null;
    breakout_retest_ok_primary: boolean | null;
    breakout_retest_dir_primary: string | null;
    // The actionability branch that admitted this tick (actionability.reason).
    actionability_branch: string | null;
};

export type AiBouncerVerdict = {
    proceed: boolean;
    confidence: number; // clamped 0-1
    reason: string; // ≤140 chars, slugged into the skip reason by the caller
    model: string;
    latencyMs: number;
    usage: { input_tokens: number; output_tokens: number } | null;
};

const flagOn = (raw: unknown) => ['1', 'true', 'yes', 'on'].includes(String(raw ?? '').trim().toLowerCase());

// Opt-in: ships dark; enabled per environment (SWING_AI_BOUNCER_ENABLED=true).
export function swingAiBouncerEnabled(): boolean {
    return flagOn(process.env.SWING_AI_BOUNCER_ENABLED);
}

const BOUNCER_MODEL = String(process.env.SWING_AI_BOUNCER_MODEL || '').trim() || 'spacexai/grok-4.1-fast-reasoning';
const BOUNCER_MAX_TOKENS = 350;
const BOUNCER_TIMEOUT_MS = 15_000;

const BOUNCER_SYSTEM = [
    'You are a triage gate for an expensive swing-trading AI.',
    'You receive a compact technical snapshot for an instrument with NO open position that has already passed every deterministic quality gate (actionable structure, sufficient signal strength, not over-extended).',
    'Decide whether this snapshot is promising enough to justify the expensive full analysis.',
    'Skip (proceed=false) only clearly mediocre setups: conflicting timeframe biases with low confluence, marginal alignment, or extension already eating the room to the next level.',
    'When uncertain, ALWAYS proceed=true — a wasted call is cheap, a missed entry is not.',
    'You can only skip work; you can never authorize a trade.',
    'Respond with strict JSON only: {"proceed":true|false,"confidence":0-1,"reason":"one short line"}',
].join(' ');

const BOUNCER_SCHEMA = {
    type: 'object',
    properties: {
        proceed: { type: 'boolean' },
        confidence: { type: 'number' },
        reason: { type: 'string' },
    },
    required: ['proceed', 'confidence', 'reason'],
    additionalProperties: false,
} as const;

// Tolerant verdict parse: strip markdown fences, take the first {...} span.
// Returns null on anything unparseable (fail open).
export function parseBouncerVerdict(raw: unknown): { proceed: boolean; confidence: number; reason: string } | null {
    if (typeof raw !== 'string' || !raw.trim()) return null;
    const cleaned = raw.replace(/```(?:json)?/gi, '');
    const start = cleaned.indexOf('{');
    const end = cleaned.lastIndexOf('}');
    if (start < 0 || end <= start) return null;
    try {
        const parsed: unknown = JSON.parse(cleaned.slice(start, end + 1));
        if (!parsed || typeof parsed !== 'object') return null;
        const obj = parsed as Record<string, unknown>;
        if (typeof obj.proceed !== 'boolean') return null;
        const confRaw = Number(obj.confidence);
        const confidence = Number.isFinite(confRaw) ? Math.min(1, Math.max(0, confRaw)) : 0;
        const reason = String(obj.reason ?? '').trim().slice(0, 140);
        return { proceed: obj.proceed, confidence, reason };
    } catch {
        return null;
    }
}

async function callBouncerModel(apiKey: string, input: AiBouncerInput, useSchema: boolean): Promise<Response> {
    return fetch(`${AI_BASE_URL}/chat/completions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
        signal: AbortSignal.timeout(BOUNCER_TIMEOUT_MS),
        body: JSON.stringify({
            model: BOUNCER_MODEL,
            messages: [
                { role: 'system', content: BOUNCER_SYSTEM },
                { role: 'user', content: JSON.stringify(input) },
            ],
            max_tokens: BOUNCER_MAX_TOKENS,
            ...(useSchema
                ? {
                      response_format: {
                          type: 'json_schema',
                          json_schema: { name: 'bouncer_verdict', strict: true, schema: BOUNCER_SCHEMA },
                      },
                  }
                : {}),
        }),
    });
}

// null = disabled or ANY failure → the caller proceeds to the full call.
export async function runAiBouncer(input: AiBouncerInput): Promise<AiBouncerVerdict | null> {
    if (!swingAiBouncerEnabled()) return null;
    const startedAt = Date.now();
    try {
        const apiKey = resolveAiGatewayKey('responses');
        let res = await callBouncerModel(apiKey, input, true);
        // Some gateway/provider combos reject response_format for this model
        // family — retry once as plain instruct-JSON (the system prompt already
        // demands strict JSON).
        if (res.status === 400 || res.status === 422) {
            res = await callBouncerModel(apiKey, input, false);
        }
        if (!res.ok) {
            const body = await res.text().catch(() => '');
            console.warn(`ai-bouncer call failed for ${input.symbol}: HTTP ${res.status} ${body.slice(0, 300)}`);
            return null;
        }
        const payload = (await res.json()) as {
            choices?: Array<{ message?: { content?: unknown } }>;
            usage?: { prompt_tokens?: unknown; completion_tokens?: unknown };
        };
        const verdict = parseBouncerVerdict(payload?.choices?.[0]?.message?.content);
        if (!verdict) {
            console.warn(`ai-bouncer verdict unparseable for ${input.symbol} — failing open`);
            return null;
        }
        const inTok = Number(payload?.usage?.prompt_tokens);
        const outTok = Number(payload?.usage?.completion_tokens);
        return {
            ...verdict,
            model: BOUNCER_MODEL,
            latencyMs: Date.now() - startedAt,
            usage:
                Number.isFinite(inTok) && Number.isFinite(outTok)
                    ? { input_tokens: inTok, output_tokens: outTok }
                    : null,
        };
    } catch (err) {
        console.warn(`ai-bouncer failed for ${input.symbol} — failing open:`, err);
        return null;
    }
}
