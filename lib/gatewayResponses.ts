// lib/gatewayResponses.ts
//
// Responses-dialect client for the swing decision (SWING_AI_PROVIDER=responses,
// legacy alias 'openai', and the default). The dialect is the gateway's, not a
// vendor's: the gateway serves this wire format for every provider it hosts, so
// DEFAULT_AI_MODEL rides this client whatever vendor it names (zai/glm-5.3
// today) — which is why the module is named for the endpoint and not for whoever
// answers on it. Sibling of lib/gatewayMessages.ts: both call the SAME Vercel AI
// Gateway and differ only in which dialect they speak, and lib/aiProvider.ts is
// the switch between them.
//
//   this file               → POST /v1/responses  (Responses dialect, raw fetch)
//   lib/gatewayMessages.ts  → POST /v1/messages   (Messages dialect, official SDK)
//
// Extracted verbatim from lib/ai.ts, which keeps the swing-decision DOMAIN
// (prompt assembly, post-processing, schemas) — this module is transport only.
//
// STATELESS, like the Messages client: the gateway accepts store/previous_response_id
// but never replays a prior conversation (verified 2026-08-28), so conversation
// memory is ours to keep in swing.ai_threads.transcript.
//
// HOW THE ANSWER SHAPE IS ENFORCED (rewritten 2026-09-03)
// `text.format: json_schema` is NOT a guarantee on this endpoint. The gateway
// forwards it and a provider without a strict mode ignores it — "if a model
// doesn't support structured outputs, the request still succeeds but the
// response may not match your schema" (Vercel's own Responses docs). zai's
// upstream exposes json_object but no json_schema mode, so `strict: true` was
// a no-op the moment DEFAULT_AI_MODEL stopped being an OpenAI id: GLM answered
// in prose, its thinking spilling out of the reasoning item into the message
// item this client parses, and every such tick died with no decision written.
//
// So a schema call now goes out as a FORCED TOOL CALL — one function, whose
// parameters ARE the schema, with tool_choice 'required' (gateway-documented:
// auto | required | none; with a single tool declared, required forces it).
// Function calling is supported by every vendor this client can reach, and the
// answer arrives as function_call.arguments, a slot the model cannot fill with
// an essay. `strict: true` still rides on the function for the vendors that
// honor it. Belt and braces on top, because a forced call is not a grammar
// guarantee everywhere: an explicit output ceiling, an incomplete-response
// check, a salvage pass that must produce every required key, and ONE reshape
// retry whose nudge sits at the prompt TAIL so the cached prefix still hits.

import { AiCallError } from './aiError';
import { aiModelForDialect, reasoningEffortForModel, resolveAiGatewayKey } from './aiModel';
import { AI_BASE_URL } from './constants';

// Output ceiling. Reasoning tokens bill as output here, so the cap has to
// clear a full high-effort think PLUS the decision; the Messages client has always
// sent one (max_tokens: 16000) and this path sent none, taking whatever the
// provider defaulted to. Successful GLM ticks measured 883-3150 output tokens
// (avg 2166) against Sol's 299-1763, so 16000 leaves ~5x headroom.
const MAX_OUTPUT_TOKENS = 16000;

// Appended as a trailing user turn on the reshape retry. It sits at the TAIL
// on purpose: the cache prefix (system + transcript) stays byte-identical, so
// the retry still reads the warm prefix instead of paying for a fresh write.
const RESHAPE_NUDGE =
    'Your previous reply did not call the required function. Do not explain, do not think out loud in the answer: reply ONLY by calling the function with the decision as its arguments.';

export type ResponsesCallResult = {
    json: Record<string, unknown>;
    // Gateway id of THIS call (`gen_...`) — persisted on the decision row so
    // the dashboard can link chained decisions. Conversation state does NOT
    // hang off this id: the AI Gateway's Responses endpoint is stateless, so
    // chaining runs through the stored transcript (appendTurns below).
    responseId: string | null;
    // Model that actually served the call (from the API response, not the
    // request) — persisted on the decision row for post-mortems.
    model: string | null;
    // Token accounting, normalized to the same field names the Messages client
    // returns so decision rows are dialect-uniform. cached input tokens map to
    // cache_read; the Responses API has no cache-creation notion (null).
    usage: {
        input_tokens: number;
        output_tokens: number;
        cache_creation_input_tokens: number | null;
        cache_read_input_tokens: number | null;
    } | null;
    // The turns this call appends to the stored transcript (sent user turn +
    // assistant text) — same contract as the Messages client, so the analyze
    // persist path is dialect-uniform. With a forced tool call the assistant
    // turn archives the ARGUMENTS json as plain text: the replay filter below
    // only accepts text turns, and storing it as text is also what keeps a
    // replayed thread free of a dangling unanswered function_call.
    appendTurns: Array<{ role: 'user' | 'assistant'; content: string }>;
};

type AiSchema = { name: string; schema: Record<string, unknown> };

// Raw REST shape (structural view of the fields read below): output is an
// array of items (reasoning, message, function_call, ...). A forced tool call
// answers on function_call.arguments; json_object mode answers on the message
// item's output_text content part.
type ResponsesPayload = {
    output?: Array<{
        type?: string;
        name?: unknown;
        arguments?: unknown;
        content?: Array<{ type?: string; text?: string }> | null;
    } | null> | null;
    output_text?: unknown;
    id?: unknown;
    model?: unknown;
    // 'completed' | 'incomplete' | ... — 'incomplete' means the answer was cut
    // off (usually max_output_tokens burned by reasoning), and the shape
    // promise does not survive that. Previously read: nothing. That threw away
    // the one field that says WHY a reply was unusable.
    status?: unknown;
    incomplete_details?: { reason?: unknown } | null;
    usage?: {
        input_tokens?: unknown;
        output_tokens?: unknown;
        input_tokens_details?: { cached_tokens?: unknown } | null;
        output_tokens_details?: { reasoning_tokens?: unknown } | null;
    } | null;
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

function responsesCallError(res: Response, details: string, code: string | null): AiCallError {
    return new AiCallError({
        message: `AI error: ${res.status} ${res.statusText}${details}`,
        dialect: 'responses',
        status: res.status,
        code,
    });
}

// The decision schema as a single function the model is FORCED to call. The
// description matters: with tool_choice 'required' a model that has not
// internalized "this is how I answer" tends to call the tool and then keep
// talking, which wastes the output ceiling.
function decisionTool(schema: AiSchema) {
    return {
        type: 'function',
        name: schema.name,
        description:
            'Record the trading decision. Calling this function is the ONLY way to answer; every field is decided here, not in prose.',
        parameters: schema.schema,
        // Honored as constrained decoding by vendors that have a strict mode
        // (OpenAI), ignored by the rest — harmless either way, and the reason
        // FALLBACK_AI_MODEL keeps its guarantee if the pair is swapped back.
        strict: true,
    };
}

// The properties a salvaged object must carry before it may stand in for a
// clean parse — see parseAnswerJson. Without this a brace-extraction could
// lift some intermediate object out of the model's own reasoning and hand it
// on as a trading decision.
function requiredKeysOf(schema: AiSchema | undefined): string[] {
    const required = schema?.schema?.required;
    return Array.isArray(required) ? required.filter((k): k is string => typeof k === 'string') : [];
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
    return !!value && typeof value === 'object' && !Array.isArray(value);
}

// Tolerates what a model puts around JSON, in widening order of desperation:
// clean parse, markdown fence stripped, outermost {...} extracted. The last
// step only counts when the result carries every required key, so prose that
// merely CONTAINS braces cannot pass as an answer. Mirrors the Messages client's
// parseDecisionJson, which has had this since the schema-less callers landed.
// Returns null instead of throwing: the caller decides retry vs. give up.
function parseAnswerJson(text: string, requiredKeys: string[]): Record<string, unknown> | null {
    const raw = text.trim();
    if (!raw) return null;
    try {
        const direct: unknown = JSON.parse(raw);
        if (isPlainObject(direct)) return direct;
    } catch {
        /* widen */
    }
    const unfenced = raw
        .replace(/^```(?:json)?\s*/i, '')
        .replace(/\s*```$/, '')
        .trim();
    try {
        const fenced: unknown = JSON.parse(unfenced);
        if (isPlainObject(fenced)) return fenced;
    } catch {
        /* widen */
    }
    const start = unfenced.indexOf('{');
    const end = unfenced.lastIndexOf('}');
    if (start < 0 || end <= start) return null;
    try {
        const salvaged: unknown = JSON.parse(unfenced.slice(start, end + 1));
        if (!isPlainObject(salvaged)) return null;
        if (requiredKeys.length && !requiredKeys.every((key) => key in salvaged)) return null;
        return salvaged;
    } catch {
        return null;
    }
}

// Where the answer lives, preferring the channel the request asked for. The
// bare `output_text` is a last resort only: on a model that leaks thinking it
// can carry reasoning rather than the answer, which is exactly how a prose
// reply used to reach JSON.parse.
function extractAnswer(
    data: ResponsesPayload | null,
    schema: AiSchema | undefined,
): { text: string; source: 'function_call' | 'message' | 'output_text' | 'none' } {
    const output = Array.isArray(data?.output) ? data.output : [];
    if (schema) {
        const call = output.find(
            (item) => item?.type === 'function_call' && (item.name === schema.name || typeof item.name !== 'string'),
        );
        const args = call?.arguments;
        if (typeof args === 'string' && args.trim()) return { text: args, source: 'function_call' };
    }
    const message = output.find((item) => item?.type === 'message');
    const messageText = message?.content?.find?.((part) => part?.type === 'output_text')?.text;
    if (typeof messageText === 'string' && messageText.trim()) return { text: messageText, source: 'message' };
    if (typeof data?.output_text === 'string' && data.output_text.trim()) {
        return { text: data.output_text, source: 'output_text' };
    }
    return { text: '', source: 'none' };
}

function normalizeUsage(data: ResponsesPayload | null): ResponsesCallResult['usage'] {
    const rawUsage = data?.usage;
    if (!rawUsage || !Number.isFinite(Number(rawUsage.input_tokens))) return null;
    return {
        input_tokens: Number(rawUsage.input_tokens),
        output_tokens: Number(rawUsage.output_tokens) || 0,
        cache_creation_input_tokens: null,
        cache_read_input_tokens: Number.isFinite(Number(rawUsage.input_tokens_details?.cached_tokens))
            ? Number(rawUsage.input_tokens_details?.cached_tokens)
            : null,
    };
}

// Everything a post-mortem needs about a reply that could not be used, put in
// front of the offending text — the health flag stores only the first 300
// chars of the message (lib/swing/aiHealth.ts), and before this the model's
// prose ate the whole field and left no diagnosis behind.
function shapeFailureDetail(data: ResponsesPayload | null, source: string): string {
    const status = typeof data?.status === 'string' ? data.status : 'unknown';
    const incomplete = data?.incomplete_details?.reason;
    const usage = data?.usage;
    const parts = [
        `status=${status}`,
        incomplete ? `incomplete=${String(incomplete)}` : null,
        `answer_channel=${source}`,
        `out_tokens=${Number(usage?.output_tokens) || 0}`,
        Number.isFinite(Number(usage?.output_tokens_details?.reasoning_tokens))
            ? `reasoning_tokens=${Number(usage?.output_tokens_details?.reasoning_tokens)}`
            : null,
    ].filter(Boolean);
    return parts.join(' ');
}

// The Responses dialect through the Vercel AI Gateway. The gateway endpoint is
// STATELESS (it accepts store/previous_response_id but never replays a prior
// conversation — verified 2026-08-28), so conversation memory is OURS to keep,
// exactly like the Messages client: the caller passes the stored transcript
// (plain {role, content} turns from swing.ai_threads.transcript) and this call
// replays it in `input` ahead of the new user turn. The model manages a
// position with memory of its own thesis; a missing/foreign transcript
// degrades to a stateless call (the prompt's "position adopted mid-life"
// branch covers it), never fails the trading tick.
export async function callResponsesDecision(
    system: string,
    user: string,
    schema?: AiSchema,
    // userForTranscript: what to ARCHIVE for this turn instead of what was
    // sent — the abbreviated record (computeSwingState's userCompact). The full
    // `user` still goes to the model; only the stored copy is slimmed, so a long
    // hold does not resend dozens of stale tapes. Defaults to `user`.
    opts?: { transcript?: unknown[] | null; userForTranscript?: string | null },
): Promise<ResponsesCallResult> {
    const apiKey = resolveAiGatewayKey('responses');

    // Whichever of the default/fallback model pair is the non-Anthropic one
    // (lib/constants.ts) — this client always speaks the Responses dialect.
    const responsesModel = aiModelForDialect('responses');
    const reasoningEffort = reasoningEffortForModel(responsesModel);
    const requiredKeys = requiredKeysOf(schema);

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

    // First attempt keeps the historical `input` shape (a bare string when
    // there is no transcript); the retry always sends turns, because the nudge
    // is one more of them.
    const buildInput = (nudge: boolean) => {
        if (!transcript.length && !nudge) return user;
        const turns: Array<{ role: 'user' | 'assistant'; content: string }> = [
            ...transcript,
            { role: 'user', content: user },
        ];
        if (nudge) turns.push({ role: 'user', content: RESHAPE_NUDGE });
        return turns;
    };

    const buildBody = (nudge: boolean) =>
        JSON.stringify({
            model: responsesModel,
            // System prompt rides along on every turn (it is not part of the
            // stored transcript, so provider cutovers never replay a stale one).
            instructions: system,
            input: buildInput(nudge),
            // Reasoning models here only accept the default temperature (1);
            // determinism comes from reasoning effort + the post-processing
            // gates. Effort is per-model (the scales differ) — see
            // reasoningEffortForModel; omitted for an unlisted model.
            ...(reasoningEffort ? { reasoning: { effort: reasoningEffort } } : {}),
            // A schema is enforced by FORCING the one function (see the header
            // note); without one, json_object mode is all there is to ask for
            // and the ONE schema-less caller (pages/api/evaluate.ts) spells the
            // shape out in its prompt — so evaluations get the retry and the
            // salvage below, but no enforcement.
            ...(schema
                ? { tools: [decisionTool(schema)], tool_choice: 'required' }
                : { text: { format: { type: 'json_object' } } }),
            max_output_tokens: MAX_OUTPUT_TOKENS,
            // Oversized replayed transcripts drop middle turns server-side
            // instead of erroring the tick (the stored transcript is also
            // capped at persist time, same as the Messages client).
            truncation: 'auto',
        });

    const attempt = async (nudge: boolean) => {
        const res = await fetch(`${AI_BASE_URL}/responses`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${apiKey}`,
            },
            body: buildBody(nudge),
        });

        if (!res.ok) {
            const { details, code } = await readAiErrorDetails(res);
            throw responsesCallError(res, details, code);
        }

        const data: ResponsesPayload | null = await res.json();
        const { text, source } = extractAnswer(data, schema);
        // A truncated reply is unusable even when it happens to parse: half a
        // decision is worse than none.
        const incomplete = data?.status === 'incomplete';
        const json = incomplete ? null : parseAnswerJson(text, requiredKeys);
        return { data, text, source, json };
    };

    // One reshape retry. The failure is stochastic — the model that answers in
    // prose usually complies on a resample — and at zai pricing a retry on the
    // minority of ticks is far cheaper than a dropped decision.
    let result = await attempt(false);
    if (!result.json) {
        result = await attempt(true);
        // Prod-visible, because the recovered case is otherwise indistinguishable
        // from a clean tick: a rising count of these means the forced tool call
        // is being ignored and the retry is carrying the pipeline.
        console.warn(`[ai_reshape_retry] model=${responsesModel} first_attempt=${shapeFailureDetail(result.data, result.source)}`);
    }
    // A schema call is supposed to answer as a function_call. Landing on the
    // message channel means tool_choice was not honored — the 2026-09-03
    // failure mode, and worth a log line even when the JSON turned out fine.
    if (schema && result.json && result.source !== 'function_call') {
        console.warn(`[ai_answer_channel] model=${responsesModel} expected=function_call got=${result.source}`);
    }

    if (!result.json) {
        throw new AiCallError({
            message: `AI returned no usable JSON after 2 attempts (${shapeFailureDetail(
                result.data,
                result.source,
            )}): ${result.text.slice(0, 400)}`,
            dialect: 'responses',
            kind: 'transient',
        });
    }

    const { data, text, json } = result;
    return {
        json,
        responseId: typeof data?.id === 'string' && data.id ? data.id : null,
        model: typeof data?.model === 'string' && data.model ? data.model : responsesModel,
        usage: normalizeUsage(data),
        appendTurns: [
            // Archive the abbreviated turn when the caller supplied one —
            // the model already received the full `user` above.
            { role: 'user', content: opts?.userForTranscript || user },
            { role: 'assistant', content: text },
        ],
    };
}

// Stateless calls (pages/api/evaluate.ts) go through
// lib/aiProvider.callStatelessAI — same dialect switch as the swing decision.
