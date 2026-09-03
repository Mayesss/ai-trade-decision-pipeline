// lib/aiProvider.ts
//
// Dialect switch for every swing AI call — WHICH WIRE FORMAT, not which
// vendor: 'responses' routes to the Responses client in lib/gatewayResponses.ts
// (which carries whatever vendor DEFAULT_AI_MODEL names — zai/glm-5.3 today),
// 'messages' to the Messages client in lib/gatewayMessages.ts. Decided by
// SWING_AI_PROVIDER (env, legacy name — it takes the legacy 'openai'/'claude'
// values as well as the dialect names) or else inferred from the model id.
// Call sites are dialect-agnostic: they pass system/user/schema plus a thread
// context and get parsed JSON back.

import { coerceAiCallError, type AiDialect } from './aiError';
import { callMessagesDecision } from './gatewayMessages';
import { callResponsesDecision } from './gatewayResponses';
import { dialectForAiModel } from './aiModel';
import { DEFAULT_AI_MODEL } from './constants';
import { reportSwingAiFailure, reportSwingAiSuccess } from './swing/aiHealth';

export function resolveSwingAiDialect(): AiDialect {
    const raw = String(process.env.SWING_AI_PROVIDER || '')
        .trim()
        .toLowerCase();
    // 'claude'/'openai' are the values already set in prod — kept as aliases
    // so the env var does not have to be edited in lockstep with a deploy.
    if (raw === 'messages' || raw === 'claude' || raw === 'anthropic') return 'messages';
    if (raw === 'responses' || raw === 'openai') return 'responses';
    // No env override: the dialect is the one DEFAULT_AI_MODEL speaks
    // (inferred from the model id — lib/constants.ts).
    return dialectForAiModel(DEFAULT_AI_MODEL);
}

// Conversation context for a threaded (per-order) decision call. Both
// dialects are stateless through the AI Gateway, so both chain through the
// stored transcript (swing.ai_threads.transcript) resent every tick — the
// Messages dialect stores full MessageParam turns (thinking blocks included),
// the Responses dialect plain {role, content} text turns. The formats differ,
// so a transcript written by the OTHER dialect must not be passed here.
export type SwingThreadContext = {
    transcript?: unknown[] | null;
};

export type SwingDecisionCallResult = {
    json: Record<string, unknown>;
    // Gateway id of THIS call (Responses `resp_...`, Messages `msg_...`) — persisted
    // on the decision row; chained decisions link through it on the dashboard.
    responseId: string | null;
    // Which dialect carried the call and which model actually answered, plus
    // token accounting in dialect-uniform field names. The decision row keeps
    // the MODEL (and the vendor read off it) — that, not the dialect, is what
    // a post-mortem needs to know about what ran and what it cost.
    dialect: AiDialect;
    model: string | null;
    usage: {
        input_tokens: number;
        output_tokens: number;
        cache_creation_input_tokens: number | null;
        cache_read_input_tokens: number | null;
    } | null;
    // The turns this call appends to the stored transcript — the sent user
    // turn plus the assistant response (messages: full content with thinking
    // blocks, echoed back verbatim next tick; responses: plain text turns).
    appendTurns?: unknown[] | null;
};

export async function callSwingDecision(params: {
    system: string;
    user: string;
    schema?: { name: string; schema: Record<string, unknown> };
    thread?: SwingThreadContext | null;
    // Abbreviated record of this turn to STORE in the transcript in place of
    // the full `user` (computeSwingState's userCompact). The model always
    // receives `user`; this only shrinks what a chained thread resends later.
    userForTranscript?: string | null;
}): Promise<SwingDecisionCallResult> {
    const dialect = resolveSwingAiDialect();
    // Every failure leaves here as a typed AiCallError (billing/config/
    // transient — lib/aiError.ts) and updates the global health flag
    // (lib/swing/aiHealth.ts). This is the single choke point for all swing AI
    // traffic, so a lapsed subscription latches the flag on the first tick
    // instead of dying as an anonymous 500 per symbol.
    try {
        let result: SwingDecisionCallResult;
        if (dialect === 'messages') {
            const { json, responseId, model, usage, appendTurns } = await callMessagesDecision(
                params.system,
                params.user,
                params.schema,
                {
                    transcript: params.thread?.transcript ?? null,
                    userForTranscript: params.userForTranscript ?? null,
                },
            );
            result = { json, responseId, dialect, model, usage, appendTurns };
        } else {
            const { json, responseId, model, usage, appendTurns } = await callResponsesDecision(
                params.system,
                params.user,
                params.schema,
                {
                    transcript: params.thread?.transcript ?? null,
                    userForTranscript: params.userForTranscript ?? null,
                },
            );
            result = { json, responseId, dialect, model, usage, appendTurns };
        }
        await reportSwingAiSuccess();
        return result;
    } catch (err) {
        const aiErr = coerceAiCallError(err, dialect);
        await reportSwingAiFailure(aiErr);
        throw aiErr;
    }
}

// Stateless convenience path (pages/api/evaluate.ts): same dialect switch, no
// thread, parsed JSON only. Callers may pass a schema — evaluate does not, so
// it is the one path with no shape enforcement (see lib/gatewayResponses.ts).
export async function callStatelessAI(
    system: string,
    user: string,
    schema?: { name: string; schema: Record<string, unknown> },
): Promise<unknown> {
    return (await callSwingDecision({ system, user, schema })).json;
}
