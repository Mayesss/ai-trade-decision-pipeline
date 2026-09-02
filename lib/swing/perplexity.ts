// Fresh news+social sentiment digest from Perplexity sonar (search-grounded),
// fetched by /api/analyze ONLY after every gate has passed — gated ticks never
// pay for it. The digest complements (never replaces) the structured
// CoinDesk/Marketaux news block: that one is a deterministic sentiment label +
// headlines, this one is grounded prose covering the freshest window including
// social/community mood. Rendered in the USER turn of the decision prompt
// (FRESH SENTIMENT block) so the cached system prefix stays byte-stable.
//
// Fail-open by design: any failure (flag off, missing gateway key, HTTP error,
// timeout, empty text) returns null and the prompt block is simply absent.
// Deliberately does NOT touch lib/swing/aiHealth.ts — a sonar outage must not
// latch the global AI-health flag that guards the main decision path.
//
// Gateway compatibility note: sonar is called via the OpenAI-compatible
// /chat/completions endpoint (the /responses path used by lib/ai.ts is
// unverified for perplexity/* ids). Sonar accepts ONLY max_tokens/temperature/
// stop — no tools, no json_schema — so the output is treated as plain prose.

import { AI_BASE_URL } from '../constants';
import { kvGetJson, kvSetJson } from '../kv';
import { baseFromSymbol } from '../news';
import { resolveAiGatewayKey } from '../aiModel';

export type PerplexityContext = {
    // Trimmed digest prose, hard-capped at PERPLEXITY_TEXT_MAX_CHARS.
    text: string;
    // Model id that produced the digest (env-overridable, recorded for audits).
    model: string;
    // When the digest was GENERATED (not when it was read from cache).
    fetchedAtMs: number;
    // True when served from KV instead of a fresh gateway call.
    fromCache: boolean;
};

const flagOn = (raw: unknown) => ['1', 'true', 'yes', 'on'].includes(String(raw ?? '').trim().toLowerCase());

// Opt-in: the digest costs ~1¢ per uncached call, so it ships dark and is
// enabled per environment (SWING_PERPLEXITY_ENABLED=true).
export function swingPerplexityEnabled(): boolean {
    return flagOn(process.env.SWING_PERPLEXITY_ENABLED);
}

const PERPLEXITY_MODEL = String(process.env.SWING_PERPLEXITY_MODEL || '').trim() || 'perplexity/sonar';

// Cache TTL: decision calls repeat on 4H bar closes (plus rare wake/emergency
// looks), so 45min keeps a same-session re-look (wake fire → hourly tick)
// from paying twice while never serving a digest older than ~1/5 of a bar.
const PERPLEXITY_TTL_SECONDS = (() => {
    const n = Number(process.env.SWING_PERPLEXITY_TTL_SECONDS);
    return Number.isFinite(n) && n >= 300 && n <= 7200 ? Math.round(n) : 2700;
})();

// Detailed-coverage window in hours. Default 6 ≈ one primary (4H) bar plus
// slack: the digest leads with what changed since the model's last look, and
// only carries older (≤24h) items as one-line backdrop — the decision model's
// chained transcript already holds earlier context.
const PERPLEXITY_FRESH_HOURS = (() => {
    const n = Number(process.env.SWING_PERPLEXITY_FRESH_HOURS);
    return Number.isFinite(n) && n >= 2 && n <= 24 ? Math.round(n) : 6;
})();

const PERPLEXITY_MAX_TOKENS = 700;
const PERPLEXITY_TEMPERATURE = 0.2;
// Sonar does a live web search per call and can be slow; the fetch runs inside
// the pre-prompt Promise.all, so it must not hold the tick unboundedly.
const PERPLEXITY_TIMEOUT_MS = 25_000;
const PERPLEXITY_TEXT_MAX_CHARS = 2000;

// v2: digest format changed (fixed aspect first line, citation markers banned)
// — the version bump keeps a stale v1-format digest from serving out of cache
// across the deploy.
const KEY_PREFIX = 'swing:perplexity:v2';

function cacheKey(platform: string, symbol: string): string {
    return `${KEY_PREFIX}:${String(platform || 'bitget').toLowerCase()}:${symbol.toUpperCase()}`;
}

function buildPrompts(symbol: string, category?: string | null): { system: string; user: string } {
    const base = baseFromSymbol(symbol);
    const system = [
        'You are a market-intelligence researcher producing a compact digest for a trading desk.',
        // Fixed aspect line first: the decision model gets a machine-scannable
        // summary in a consistent shape before the prose bullets.
        'Your FIRST line must be exactly this shape and nothing else: "mood: bullish|bearish|mixed | catalyst: yes|no | flows: <≤6 words, or none>" — mood = prevailing retail/social sentiment, catalyst = whether a concrete news/regulatory/macro driver exists for the current move, flows = the most notable flow/on-chain/positioning fact.',
        `Then cover the last ${PERPLEXITY_FRESH_HOURS} hours in detail: major headlines, regulatory or macro items, notable flow/on-chain data, and the retail/social mood behind the label.`,
        'Then AT MOST 2 one-line items for anything older (up to 24h) that is still actively driving the market.',
        'Format: short bullet points, each with a recency marker (e.g. "2h ago"). Flag rumors or unconfirmed items as such.',
        'Plain text only — NEVER emit citation markers, footnote numbers, or bracketed source references like [1]; name a source inline ("per Reuters") only when it matters.',
        'Max ~250 words. Facts only — NO trading advice, NO price predictions.',
    ].join(' ');
    const user = `Asset: ${base}. Asset class: ${String(category || 'crypto')}. Current time: ${new Date().toISOString()}.`;
    return { system, user };
}

// Entry point for /api/analyze. Returns null on flag-off or ANY failure so the
// caller's prompt block is simply absent — never throws.
export async function loadPerplexityContext(
    symbol: string,
    opts: { platform: string; category?: string | null },
): Promise<PerplexityContext | null> {
    if (!swingPerplexityEnabled()) return null;

    const key = cacheKey(opts.platform, symbol);
    try {
        const cached = await kvGetJson<PerplexityContext>(key);
        if (cached && typeof cached.text === 'string' && cached.text.trim()) {
            return { ...cached, fromCache: true };
        }
    } catch (err) {
        console.warn(`Perplexity cache read failed for ${symbol}:`, err);
    }

    try {
        const apiKey = resolveAiGatewayKey('responses');
        const { system, user } = buildPrompts(symbol, opts.category);
        const res = await fetch(`${AI_BASE_URL}/chat/completions`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
            signal: AbortSignal.timeout(PERPLEXITY_TIMEOUT_MS),
            body: JSON.stringify({
                model: PERPLEXITY_MODEL,
                messages: [
                    { role: 'system', content: system },
                    { role: 'user', content: user },
                ],
                max_tokens: PERPLEXITY_MAX_TOKENS,
                temperature: PERPLEXITY_TEMPERATURE,
            }),
        });
        if (!res.ok) {
            const body = await res.text().catch(() => '');
            console.warn(`Perplexity call failed for ${symbol}: HTTP ${res.status} ${body.slice(0, 300)}`);
            return null;
        }
        const payload = (await res.json()) as { choices?: Array<{ message?: { content?: unknown } }> };
        const raw = payload?.choices?.[0]?.message?.content;
        const text = typeof raw === 'string' ? raw.trim().slice(0, PERPLEXITY_TEXT_MAX_CHARS) : '';
        if (!text) return null;

        const ctx: PerplexityContext = {
            text,
            model: PERPLEXITY_MODEL,
            fetchedAtMs: Date.now(),
            fromCache: false,
        };
        try {
            await kvSetJson(key, ctx, PERPLEXITY_TTL_SECONDS);
        } catch (err) {
            console.warn(`Perplexity cache write failed for ${symbol}:`, err);
        }
        return ctx;
    } catch (err) {
        console.warn(`Could not build Perplexity context for ${symbol}:`, err);
        return null;
    }
}
