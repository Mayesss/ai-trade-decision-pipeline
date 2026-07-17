// lib/aiProvider.ts
//
// Provider switch for every swing AI call. Which model family gets prompted is
// decided by SWING_AI_PROVIDER (env), not by the call sites: 'openai' routes to
// the GPT Responses-API client in lib/ai.ts, 'claude' to the Anthropic Messages
// client in lib/claudeAi.ts (phase 2). Call sites are provider-agnostic — they
// pass system/user/schema plus a thread context and get parsed JSON back.

import { callAIThread } from './ai';
import { callClaudeSwingDecision } from './claudeAi';

export type SwingAiProvider = 'openai' | 'claude';

// Claude is the default since the phase-4 cutover (2026-07-17); set
// SWING_AI_PROVIDER=openai to roll back to GPT without a code change.
const DEFAULT_PROVIDER: SwingAiProvider = 'claude';

export function resolveSwingAiProvider(): SwingAiProvider {
    const raw = String(process.env.SWING_AI_PROVIDER || '')
        .trim()
        .toLowerCase();
    if (raw === 'claude') return 'claude';
    if (raw === 'openai') return 'openai';
    return DEFAULT_PROVIDER;
}

// Conversation context for a threaded (per-order) decision call. The two
// providers keep state in different places:
// - OpenAI Responses API is stateful server-side: the chain head id
//   (`resp_...`) is all we store; the server replays the conversation.
// - Claude Messages API is stateless: we store the full transcript ourselves
//   (swing.ai_threads.transcript, phase 3) and resend it every tick.
export type SwingThreadContext = {
    previousResponseId?: string | null;
    transcript?: unknown[] | null;
};

export type SwingDecisionCallResult = {
    json: any;
    // Provider id of THIS call (OpenAI `resp_...`, Claude `msg_...`) — persisted
    // on the decision row; chained decisions link through it on the dashboard.
    responseId: string | null;
    // Claude only (phase 3): the turns this call appends to the stored
    // transcript — the sent user turn plus the full assistant response content
    // (thinking blocks included, echoed back verbatim on the next tick).
    // null/undefined on the OpenAI path (server keeps the conversation).
    appendTurns?: unknown[] | null;
};

export async function callSwingDecision(params: {
    system: string;
    user: string;
    schema?: { name: string; schema: Record<string, unknown> };
    thread?: SwingThreadContext | null;
}): Promise<SwingDecisionCallResult> {
    const provider = resolveSwingAiProvider();
    if (provider === 'claude') {
        // Stateless Messages API: conversation context is the stored transcript
        // (phase 3 persists it; until then in-position ticks run stateless —
        // the prompt's "position adopted mid-life" branch covers that).
        const { json, responseId, appendTurns } = await callClaudeSwingDecision(
            params.system,
            params.user,
            params.schema,
            { transcript: params.thread?.transcript ?? null },
        );
        return { json, responseId, appendTurns };
    }
    const { json, responseId } = await callAIThread(params.system, params.user, params.schema, {
        previousResponseId: params.thread?.previousResponseId ?? null,
    });
    return { json, responseId };
}

// Stateless convenience path (forex advisor, evaluations): same provider
// switch, no thread, parsed JSON only.
export async function callStatelessAI(
    system: string,
    user: string,
    schema?: { name: string; schema: Record<string, unknown> },
): Promise<any> {
    return (await callSwingDecision({ system, user, schema })).json;
}
