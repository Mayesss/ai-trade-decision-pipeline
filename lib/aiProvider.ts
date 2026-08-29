// lib/aiProvider.ts
//
// Provider switch for every swing AI call. Which model family gets prompted is
// decided by SWING_AI_PROVIDER (env), not by the call sites: 'openai' routes to
// the GPT Responses-API client in lib/ai.ts, 'claude' to the Anthropic Messages
// client in lib/claudeAi.ts (phase 2). Call sites are provider-agnostic — they
// pass system/user/schema plus a thread context and get parsed JSON back.

import { callAIThread } from './ai';
import { coerceAiCallError } from './aiError';
import { callClaudeSwingDecision } from './claudeAi';
import { providerForAiModel } from './aiModel';
import { DEFAULT_AI_MODEL } from './constants';
import { reportSwingAiFailure, reportSwingAiSuccess } from './swing/aiHealth';

export type SwingAiProvider = 'openai' | 'claude';

export function resolveSwingAiProvider(): SwingAiProvider {
    const raw = String(process.env.SWING_AI_PROVIDER || '')
        .trim()
        .toLowerCase();
    if (raw === 'claude') return 'claude';
    if (raw === 'openai') return 'openai';
    // No env override: the provider is whoever owns DEFAULT_AI_MODEL
    // (inferred from the model id — lib/constants.ts).
    return providerForAiModel(DEFAULT_AI_MODEL);
}

// Conversation context for a threaded (per-order) decision call. Both
// providers are stateless through the AI Gateway, so both chain through the
// stored transcript (swing.ai_threads.transcript) resent every tick — Claude
// stores full MessageParam turns (thinking blocks included), OpenAI plain
// {role, content} text turns. Formats differ per provider, so a transcript
// written by the other model family must not be passed here.
export type SwingThreadContext = {
    transcript?: unknown[] | null;
};

export type SwingDecisionCallResult = {
    json: Record<string, unknown>;
    // Provider id of THIS call (OpenAI `resp_...`, Claude `msg_...`) — persisted
    // on the decision row; chained decisions link through it on the dashboard.
    responseId: string | null;
    // Which provider/model actually served the call plus its token accounting
    // (provider-uniform field names) — persisted on the decision row so
    // post-mortems can reconstruct exactly what ran and what it cost.
    provider: SwingAiProvider;
    model: string | null;
    usage: {
        input_tokens: number;
        output_tokens: number;
        cache_creation_input_tokens: number | null;
        cache_read_input_tokens: number | null;
    } | null;
    // The turns this call appends to the stored transcript — the sent user
    // turn plus the assistant response (Claude: full content with thinking
    // blocks, echoed back verbatim next tick; OpenAI: plain text turns).
    appendTurns?: unknown[] | null;
};

export async function callSwingDecision(params: {
    system: string;
    user: string;
    schema?: { name: string; schema: Record<string, unknown> };
    thread?: SwingThreadContext | null;
}): Promise<SwingDecisionCallResult> {
    const provider = resolveSwingAiProvider();
    // Every failure leaves here as a typed AiCallError (billing/config/
    // transient — lib/aiError.ts) and updates the global health flag
    // (lib/swing/aiHealth.ts). This is the single choke point for all swing AI
    // traffic, so a lapsed subscription latches the flag on the first tick
    // instead of dying as an anonymous 500 per symbol.
    try {
        let result: SwingDecisionCallResult;
        if (provider === 'claude') {
            const { json, responseId, model, usage, appendTurns } = await callClaudeSwingDecision(
                params.system,
                params.user,
                params.schema,
                { transcript: params.thread?.transcript ?? null },
            );
            result = { json, responseId, provider, model, usage, appendTurns };
        } else {
            const { json, responseId, model, usage, appendTurns } = await callAIThread(
                params.system,
                params.user,
                params.schema,
                { transcript: params.thread?.transcript ?? null },
            );
            result = { json, responseId, provider, model, usage, appendTurns };
        }
        await reportSwingAiSuccess();
        return result;
    } catch (err) {
        const aiErr = coerceAiCallError(err, provider);
        await reportSwingAiFailure(aiErr);
        throw aiErr;
    }
}

// Stateless convenience path (forex advisor, evaluations): same provider
// switch, no thread, parsed JSON only.
export async function callStatelessAI(
    system: string,
    user: string,
    schema?: { name: string; schema: Record<string, unknown> },
): Promise<unknown> {
    return (await callSwingDecision({ system, user, schema })).json;
}
