// lib/aiModel.ts
//
// There is no separate provider setting: the DIALECT is inferred from the
// model id. Two dialects exist on the one gateway — Anthropic Messages for
// 'claude' ids, the OpenAI Responses API for everything else (the gateway
// speaks it for every provider it serves, not just OpenAI's own models), so
// the DEFAULT_AI_MODEL / FALLBACK_AI_MODEL pair in lib/constants.ts must hold
// exactly one Anthropic id and one non-Anthropic id. Swapping either in place
// flips the live model without touching the clients.

import { AiCallError, type AiDialect } from './aiError';
import { DEFAULT_AI_MODEL, FALLBACK_AI_MODEL } from './constants';

// Gateway model ids are `vendor/model` slugs (lib/constants.ts).
const MODEL_SLUG = /^[a-z0-9][\w.-]*\/[\w.-]+$/i;

// Auth for the Vercel AI Gateway (both dialects go through it).
// Priority mirrors the gateway's own: an explicit AI_GATEWAY_API_KEY wins,
// otherwise the Vercel OIDC token (auto-provisioned on Vercel deployments,
// pulled locally via `vercel env pull`). Read per call, never cached — the
// OIDC token rotates.
export function resolveAiGatewayKey(dialect: AiDialect): string {
    const key = String(process.env.AI_GATEWAY_API_KEY || '').trim() || String(process.env.VERCEL_OIDC_TOKEN || '').trim();
    if (!key) {
        throw new AiCallError({
            message: 'Missing AI Gateway auth: set AI_GATEWAY_API_KEY or provide VERCEL_OIDC_TOKEN (vercel env pull)',
            dialect,
            kind: 'config',
        });
    }
    return key;
}

export function dialectForAiModel(model: string): AiDialect {
    if (/claude/i.test(model)) return 'messages';
    // Everything else goes out in the OpenAI Responses dialect. The gateway
    // serves that dialect for every vendor, so the id shape is the only thing
    // worth checking — a bare name is a typo, not a model.
    if (MODEL_SLUG.test(model.trim())) return 'responses';
    throw new Error(`Cannot infer AI dialect from model id "${model}" — expected a gateway "vendor/model" slug`);
}

// WHO serves the model, read off the gateway slug's first segment
// ('zai/glm-5.3' → 'zai'). This is what a decision row records, because the
// dialect alone no longer identifies the vendor.
export function vendorForAiModel(model: string): string {
    const slug = model.trim();
    return MODEL_SLUG.test(slug) ? slug.split('/')[0].toLowerCase() : 'unknown';
}

// Reasoning effort per model, because the scales are NOT shared: gpt-5.6-sol
// runs none|low|medium|high|xhigh|max while zai/glm-5.3 exposes only
// low|high|max, so the literal 'medium' this client used to hardcode is
// rejected outright by GLM. Each entry is the MIDDLE of that model's own
// scale — the operating point sol ran at. It is a cost/quality dial: raising
// it buys benchmark points and pays for them in reasoning tokens, which bill
// as output. An unlisted model sends no reasoning field and takes the
// vendor's own default.
const REASONING_EFFORT: Record<string, string> = {
    'zai/glm-5.3': 'high',
    'openai/gpt-5.6-sol': 'medium',
};

export function reasoningEffortForModel(model: string): string | null {
    return REASONING_EFFORT[model.trim()] ?? null;
}

// The model a given dialect runs: whichever of the default/fallback pair speaks it.
export function aiModelForDialect(dialect: AiDialect): string {
    return dialectForAiModel(DEFAULT_AI_MODEL) === dialect ? DEFAULT_AI_MODEL : FALLBACK_AI_MODEL;
}
