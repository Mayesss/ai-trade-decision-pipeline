// lib/openAi.ts
//
// OpenAI Responses API client for the swing decision (SWING_AI_PROVIDER=openai,
// and the default — DEFAULT_AI_MODEL is a gpt id). Sibling of lib/claudeAi.ts:
// both call the SAME Vercel AI Gateway and differ only in which dialect they
// speak, and lib/aiProvider.ts is the switch between them.
//
//   this file        → POST /v1/responses  (OpenAI Responses, raw fetch)
//   lib/claudeAi.ts  → POST /v1/messages   (Anthropic Messages, official SDK)
//
// Extracted verbatim from lib/ai.ts, which keeps the swing-decision DOMAIN
// (prompt assembly, post-processing, schemas) — this module is transport only.
//
// STATELESS, like the Claude path: the gateway accepts store/previous_response_id
// but never replays a prior conversation (verified 2026-08-28), so conversation
// memory is ours to keep in swing.ai_threads.transcript.

import { AiCallError } from './aiError';
import { aiModelForProvider, resolveAiGatewayKey } from './aiModel';
import { AI_BASE_URL } from './constants';

export type AiThreadCallResult = {
    json: Record<string, unknown>;
    // Gateway id of THIS call (`gen_...`) — persisted on the decision row so
    // the dashboard can link chained decisions. Conversation state does NOT
    // hang off this id: the AI Gateway's Responses endpoint is stateless, so
    // chaining runs through the stored transcript (appendTurns below).
    responseId: string | null;
    // Model that actually served the call (from the API response, not the
    // request) — persisted on the decision row for post-mortems.
    model: string | null;
    // Token accounting, normalized to the same field names the Claude client
    // returns so decision rows are provider-uniform. cached input tokens map to
    // cache_read; the Responses API has no cache-creation notion (null).
    usage: {
        input_tokens: number;
        output_tokens: number;
        cache_creation_input_tokens: number | null;
        cache_read_input_tokens: number | null;
    } | null;
    // The turns this call appends to the stored transcript (sent user turn +
    // assistant text) — same contract as the Claude client, so the analyze
    // persist path is provider-uniform.
    appendTurns: Array<{ role: 'user' | 'assistant'; content: string }>;
};

// Parses the error body once and keeps BOTH halves: the human message (for
// the thrown error text) and the machine `error.code`/`error.type` (e.g.
// `insufficient_quota`) that AiCallError classification runs on — previously
// the code was parsed here and thrown away.
async function readAiErrorDetails(res: Response): Promise<{ details: string; code: string | null }> {
    try {
        const errJson: unknown = await res.json();
        const body =
            errJson && typeof errJson === 'object'
                ? (errJson as { error?: { message?: unknown; code?: unknown; type?: unknown } | null; message?: unknown })
                : null;
        const msg =
            body?.error?.message ||
            body?.message ||
            (typeof errJson === 'string' ? errJson : JSON.stringify(errJson));
        const code = body?.error?.code || body?.error?.type || null;
        return { details: msg ? ` - ${msg}` : '', code: typeof code === 'string' && code ? code : null };
    } catch {
        try {
            const errText = await res.text();
            return { details: errText ? ` - ${errText.slice(0, 600)}` : '', code: null };
        } catch {
            return { details: '', code: null };
        }
    }
}

function openAiCallError(res: Response, details: string, code: string | null): AiCallError {
    return new AiCallError({
        message: `AI error: ${res.status} ${res.statusText}${details}`,
        provider: 'openai',
        status: res.status,
        code,
    });
}

// OpenAI Responses API through the Vercel AI Gateway. The gateway endpoint is
// STATELESS (it accepts store/previous_response_id but never replays a prior
// conversation — verified 2026-08-28), so conversation memory is OURS to keep,
// exactly like the Claude path: the caller passes the stored transcript
// (plain {role, content} turns from swing.ai_threads.transcript) and this call
// replays it in `input` ahead of the new user turn. The model manages a
// position with memory of its own thesis; a missing/foreign transcript
// degrades to a stateless call (the prompt's "position adopted mid-life"
// branch covers it), never fails the trading tick.
export async function callAIThread(
    system: string,
    user: string,
    schema?: { name: string; schema: Record<string, unknown> },
    // userForTranscript: what to ARCHIVE for this turn instead of what was
    // sent — the abbreviated record (computeSwingState's userCompact). The full
    // `user` still goes to the model; only the stored copy is slimmed, so a long
    // hold does not resend dozens of stale tapes. Defaults to `user`.
    opts?: { transcript?: unknown[] | null; userForTranscript?: string | null },
): Promise<AiThreadCallResult> {
    const apiKey = resolveAiGatewayKey('openai');

    // Whichever of the default/fallback model pair is the gpt-flavored one
    // (lib/constants.ts) — this client always speaks to OpenAI.
    const openAiModel = aiModelForProvider('openai');

    // Structured Outputs (json_schema, strict) guarantees the response shape at the
    // API layer when a caller supplies a schema; otherwise fall back to JSON mode.
    // Responses API uses a flattened text.format (no chat-completions wrapper).
    const format = schema
        ? { type: 'json_schema', name: schema.name, schema: schema.schema, strict: true }
        : { type: 'json_object' };

    // Stored transcripts round-trip through JSONB — replay only well-formed
    // plain-text turns; anything else is dropped rather than erroring the tick.
    const rawTranscript = opts?.transcript;
    const transcript = (Array.isArray(rawTranscript) ? rawTranscript : []).filter(
        (turn): turn is { role: 'user' | 'assistant'; content: string } =>
            !!turn &&
            typeof turn === 'object' &&
            'role' in turn &&
            'content' in turn &&
            (turn.role === 'user' || turn.role === 'assistant') &&
            typeof turn.content === 'string' &&
            turn.content.length > 0,
    );

    const res = await fetch(`${AI_BASE_URL}/responses`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
            model: openAiModel,
            // System prompt rides along on every turn (it is not part of the
            // stored transcript, so provider cutovers never replay a stale one).
            instructions: system,
            input: transcript.length ? [...transcript, { role: 'user', content: user }] : user,
            // gpt-5.x reasoning models only accept the default temperature (1);
            // determinism comes from reasoning effort + the post-processing gates.
            reasoning: { effort: 'medium' },
            text: { format },
            // Oversized replayed transcripts drop middle turns server-side
            // instead of erroring the tick (the stored transcript is also
            // capped at persist time, same as the Claude path).
            truncation: 'auto',
        }),
    });

    if (!res.ok) {
        const { details, code } = await readAiErrorDetails(res);
        throw openAiCallError(res, details, code);
    }

    // Raw REST shape (structural view of the fields read below): output is an
    // array of items (reasoning, message, ...); the assistant text lives on
    // the message item's output_text content part.
    const data: {
        output?: Array<{ type?: string; content?: Array<{ type?: string; text?: string }> | null } | null> | null;
        output_text?: unknown;
        id?: unknown;
        model?: unknown;
        usage?: {
            input_tokens?: unknown;
            output_tokens?: unknown;
            input_tokens_details?: { cached_tokens?: unknown } | null;
        } | null;
    } | null = await res.json();
    const message = Array.isArray(data?.output) ? data.output.find((item) => item?.type === 'message') : null;
    const text =
        message?.content?.find?.((c) => c?.type === 'output_text')?.text ||
        (typeof data?.output_text === 'string' ? data.output_text : '') ||
        '{}';
    const responseId = typeof data?.id === 'string' && data.id ? data.id : null;
    const model = typeof data?.model === 'string' && data.model ? data.model : openAiModel;
    const rawUsage = data?.usage;
    const usage =
        rawUsage && Number.isFinite(Number(rawUsage.input_tokens))
            ? {
                  input_tokens: Number(rawUsage.input_tokens),
                  output_tokens: Number(rawUsage.output_tokens) || 0,
                  cache_creation_input_tokens: null,
                  cache_read_input_tokens: Number.isFinite(Number(rawUsage.input_tokens_details?.cached_tokens))
                      ? Number(rawUsage.input_tokens_details?.cached_tokens)
                      : null,
              }
            : null;
    try {
        return {
            json: JSON.parse(text),
            responseId,
            model,
            usage,
            appendTurns: [
                // Archive the abbreviated turn when the caller supplied one —
                // the model already received the full `user` above.
                { role: 'user', content: opts?.userForTranscript || user },
                { role: 'assistant', content: text },
            ],
        };
    } catch {
        throw new Error(`AI returned non-JSON content: ${String(text).slice(0, 600)}`);
    }
}

// Stateless calls (forex advisor, evaluations) go through
// lib/aiProvider.callStatelessAI — same provider switch as the swing decision.
