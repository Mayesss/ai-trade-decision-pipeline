// lib/aiModel.ts
//
// There is no separate provider setting: the provider is inferred from the
// model id ('claude' anywhere in it → Anthropic, 'gpt' → OpenAI), so the
// DEFAULT_AI_MODEL / FALLBACK_AI_MODEL pair in lib/constants.ts must each
// contain one of the two markers. Swapping the pair in place flips the live
// provider without touching the clients.

import { DEFAULT_AI_MODEL, FALLBACK_AI_MODEL } from './constants';

export function providerForAiModel(model: string): 'openai' | 'claude' {
    if (/claude/i.test(model)) return 'claude';
    if (/gpt/i.test(model)) return 'openai';
    throw new Error(`Cannot infer AI provider from model id "${model}" — expected 'claude' or 'gpt' in the id`);
}

// The model a given provider runs: whichever of the default/fallback pair belongs to it.
export function aiModelForProvider(provider: 'openai' | 'claude'): string {
    return providerForAiModel(DEFAULT_AI_MODEL) === provider ? DEFAULT_AI_MODEL : FALLBACK_AI_MODEL;
}
