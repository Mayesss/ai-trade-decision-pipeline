// lib/aiModel.ts
//
// There is no separate provider setting: the provider is inferred from the
// model id ('claude' anywhere in it → Anthropic, 'gpt' → OpenAI), so the
// DEFAULT_AI_MODEL / FALLBACK_AI_MODEL pair in lib/constants.ts must each
// contain one of the two markers. Swapping the pair in place flips the live
// provider without touching the clients.

import { AiCallError, type AiCallProvider } from './aiError';
import { DEFAULT_AI_MODEL, FALLBACK_AI_MODEL } from './constants';

// Auth for the Vercel AI Gateway (both provider paths go through it).
// Priority mirrors the gateway's own: an explicit AI_GATEWAY_API_KEY wins,
// otherwise the Vercel OIDC token (auto-provisioned on Vercel deployments,
// pulled locally via `vercel env pull`). Read per call, never cached — the
// OIDC token rotates.
export function resolveAiGatewayKey(provider: AiCallProvider): string {
    const key = String(process.env.AI_GATEWAY_API_KEY || '').trim() || String(process.env.VERCEL_OIDC_TOKEN || '').trim();
    if (!key) {
        throw new AiCallError({
            message: 'Missing AI Gateway auth: set AI_GATEWAY_API_KEY or provide VERCEL_OIDC_TOKEN (vercel env pull)',
            provider,
            kind: 'config',
        });
    }
    return key;
}

export function providerForAiModel(model: string): 'openai' | 'claude' {
    if (/claude/i.test(model)) return 'claude';
    if (/gpt/i.test(model)) return 'openai';
    throw new Error(`Cannot infer AI provider from model id "${model}" — expected 'claude' or 'gpt' in the id`);
}

// The model a given provider runs: whichever of the default/fallback pair belongs to it.
export function aiModelForProvider(provider: 'openai' | 'claude'): string {
    return providerForAiModel(DEFAULT_AI_MODEL) === provider ? DEFAULT_AI_MODEL : FALLBACK_AI_MODEL;
}
