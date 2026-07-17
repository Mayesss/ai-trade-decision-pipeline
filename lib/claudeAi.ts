// lib/claudeAi.ts
//
// Anthropic Messages API client for the swing decision (SWING_AI_PROVIDER=claude).
//
// Differences from the OpenAI Responses client in lib/ai.ts:
// - STATELESS: there is no previous_response_id — the conversation lives in
//   swing.ai_threads.transcript (phase 3) and is resent on every tick. This
//   module takes the transcript in and hands the turns-to-append back out; it
//   never touches the store itself.
// - Structured outputs ride on output_config.format (json_schema) — the schema
//   is enforced at the API layer, so the text block is guaranteed parseable.
//   Claude's structured outputs reject numeric bound keywords (minimum/maximum),
//   so those are stripped from the OpenAI-shaped schemas; every numeric field
//   is already clamped in code after parse (sanitizeEntryLimit,
//   sanitizeExchangeTpSl, leverage clamps), so nothing is lost.
// - Prompt caching: the system prompt is byte-stable per venue/asset-class
//   (phase 0), so it carries a 1h-TTL cache breakpoint. At the 15-min cron
//   cadence a 5-min cache would expire between ticks; 1h survives them.

import Anthropic from '@anthropic-ai/sdk';

const DEFAULT_CLAUDE_MODEL = 'claude-opus-4-8';
const CLAUDE_EFFORTS = ['low', 'medium', 'high', 'xhigh', 'max'] as const;
type ClaudeEffort = (typeof CLAUDE_EFFORTS)[number];

function resolveClaudeModel(): string {
    return String(process.env.SWING_AI_CLAUDE_MODEL || '').trim() || DEFAULT_CLAUDE_MODEL;
}

// GPT ran reasoning effort 'medium'; start there and sweep later (phase 5).
function resolveClaudeEffort(): ClaudeEffort {
    const raw = String(process.env.SWING_AI_CLAUDE_EFFORT || '')
        .trim()
        .toLowerCase();
    return (CLAUDE_EFFORTS as readonly string[]).includes(raw) ? (raw as ClaudeEffort) : 'medium';
}

let cachedClient: Anthropic | null = null;
function claudeClient(): Anthropic {
    if (cachedClient) return cachedClient;
    if (!process.env.ANTHROPIC_API_KEY) throw new Error('Missing ANTHROPIC_API_KEY');
    cachedClient = new Anthropic();
    return cachedClient;
}

// Claude structured outputs support enums, type unions and additionalProperties:false,
// but not numeric bound constraints — strip minimum/maximum (and the string
// bounds, unused today, for safety) recursively. Post-parse code clamps instead.
export function toClaudeSchema(schema: Record<string, unknown>): Record<string, unknown> {
    const UNSUPPORTED = new Set(['minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum', 'minLength', 'maxLength', 'multipleOf']);
    const walk = (node: unknown): unknown => {
        if (Array.isArray(node)) return node.map(walk);
        if (node && typeof node === 'object') {
            const out: Record<string, unknown> = {};
            for (const [key, value] of Object.entries(node as Record<string, unknown>)) {
                if (UNSUPPORTED.has(key)) continue;
                out[key] = walk(value);
            }
            return out;
        }
        return node;
    };
    return walk(schema) as Record<string, unknown>;
}

// Transcript growth cap (replaces OpenAI's server-side `truncation: "auto"`).
// In-position ticks call every 15 min, so a 1–10 day hold could reach hundreds
// of turn pairs — keep the ENTRY pair (the thesis the prompt tells the model to
// manage against) plus the most recent management pairs, drop the middle.
const TRANSCRIPT_MAX_MESSAGES = 62; // entry pair + 30 management pairs

export function truncateClaudeTranscript(
    transcript: Anthropic.MessageParam[],
    maxMessages: number = TRANSCRIPT_MAX_MESSAGES,
): Anthropic.MessageParam[] {
    if (transcript.length <= maxMessages) return transcript;
    const head = transcript.slice(0, 2);
    let tail = transcript.slice(transcript.length - (maxMessages - 2));
    // The head pair ends with an assistant turn, so the tail must open with a
    // user turn to keep alternation valid. Turns are appended in pairs, so this
    // trims at most one dangling assistant message.
    while (tail.length && tail[0].role !== 'user') tail = tail.slice(1);
    return [...head, ...tail];
}

// Stored transcripts round-trip through JSONB — keep only well-formed turns and
// drop any cache_control that leaked into persistence (breakpoints are attached
// fresh at send time; stale ones would blow the 4-breakpoint request limit).
function sanitizeTranscript(input: unknown[] | null | undefined): Anthropic.MessageParam[] {
    if (!Array.isArray(input)) return [];
    const turns: Anthropic.MessageParam[] = [];
    for (const item of input) {
        const role = (item as any)?.role;
        const content = (item as any)?.content;
        if (role !== 'user' && role !== 'assistant') continue;
        if (typeof content === 'string') {
            turns.push({ role, content });
            continue;
        }
        if (!Array.isArray(content) || content.length === 0) continue;
        turns.push({
            role,
            content: content.map((block: any) => {
                if (block && typeof block === 'object' && 'cache_control' in block) {
                    const { cache_control: _dropped, ...rest } = block;
                    return rest;
                }
                return block;
            }),
        });
    }
    return turns;
}

// Advancing conversation breakpoint: cache the whole prior thread (everything
// up to and including the last assistant turn) so an in-position tick only
// pays fresh input for its new STATE+MARKET turn. Attached at send time to a
// copy — never persisted. 2 breakpoints total with the system block (limit: 4).
function withConversationBreakpoint(turns: Anthropic.MessageParam[]): Anthropic.MessageParam[] {
    if (!turns.length) return turns;
    const last = turns[turns.length - 1];
    const blocks = typeof last.content === 'string' ? [{ type: 'text' as const, text: last.content }] : [...last.content];
    for (let i = blocks.length - 1; i >= 0; i--) {
        const block = blocks[i] as any;
        // thinking blocks can't carry cache_control — anchor on the last text block
        if (block?.type === 'text') {
            blocks[i] = { ...block, cache_control: { type: 'ephemeral', ttl: '1h' } };
            return [...turns.slice(0, -1), { role: last.role, content: blocks }];
        }
    }
    return turns;
}

export type ClaudeSwingCallResult = {
    json: any;
    // Message id of THIS call (`msg_...`) — persisted on the decision row, same
    // slot the OpenAI path uses for `resp_...`.
    responseId: string | null;
    // The two turns this call adds to the conversation: the user turn we sent
    // and the assistant response VERBATIM (thinking blocks included — they must
    // be echoed back unchanged when the thread continues on the same model).
    appendTurns: Anthropic.MessageParam[];
    // Token accounting incl. cache activity — cache_read_input_tokens should be
    // non-zero on repeat ticks once the prefix is warm; zero means a silent
    // cache invalidator (or a prefix under the model's cacheable minimum).
    usage: {
        input_tokens: number;
        output_tokens: number;
        cache_creation_input_tokens: number | null;
        cache_read_input_tokens: number | null;
    };
};

export async function callClaudeSwingDecision(
    system: string,
    user: string,
    schema?: { name: string; schema: Record<string, unknown> },
    opts?: { transcript?: unknown[] | null },
): Promise<ClaudeSwingCallResult> {
    const client = claudeClient();
    const priorTurns = withConversationBreakpoint(sanitizeTranscript(opts?.transcript));

    const userTurn: Anthropic.MessageParam = {
        role: 'user',
        content: [{ type: 'text', text: user }],
    };

    const outputConfig: Anthropic.OutputConfig = { effort: resolveClaudeEffort() };
    if (schema) {
        outputConfig.format = { type: 'json_schema', schema: toClaudeSchema(schema.schema) };
    }

    const response = await client.messages.create({
        model: resolveClaudeModel(),
        // Output ceiling shared by adaptive thinking + the (small) JSON decision;
        // generous so thinking never truncates the answer mid-thought.
        max_tokens: 16000,
        thinking: { type: 'adaptive' },
        output_config: outputConfig,
        system: [
            {
                type: 'text',
                text: system,
                // 1h TTL: survives the 15-min cron gap (5-min default would not).
                cache_control: { type: 'ephemeral', ttl: '1h' },
            },
        ],
        messages: [...priorTurns, userTurn],
    });

    if (response.stop_reason === 'refusal') {
        throw new Error('Claude refused the swing decision request (stop_reason=refusal)');
    }
    if (response.stop_reason === 'max_tokens') {
        throw new Error('Claude swing decision truncated (stop_reason=max_tokens) — raise max_tokens or lower effort');
    }

    const textBlock = response.content.find((block): block is Anthropic.TextBlock => block.type === 'text');
    const text = textBlock?.text ?? '';
    const json = parseDecisionJson(text, Boolean(schema));

    return {
        json,
        responseId: typeof response.id === 'string' && response.id ? response.id : null,
        appendTurns: [userTurn, { role: 'assistant', content: response.content }],
        usage: {
            input_tokens: response.usage.input_tokens,
            output_tokens: response.usage.output_tokens,
            cache_creation_input_tokens: response.usage.cache_creation_input_tokens ?? null,
            cache_read_input_tokens: response.usage.cache_read_input_tokens ?? null,
        },
    };
}

// With a schema the API guarantees valid JSON; without one (forex advisor,
// evaluations — prompts demand strict JSON but don't ship a schema) the model
// may wrap the object in a markdown fence — tolerate that.
function parseDecisionJson(text: string, schemaEnforced: boolean): any {
    const raw = text.trim();
    try {
        return JSON.parse(raw);
    } catch {
        if (schemaEnforced) {
            throw new Error(`Claude returned non-JSON content despite schema enforcement: ${raw.slice(0, 600)}`);
        }
    }
    const unfenced = raw
        .replace(/^```(?:json)?\s*/i, '')
        .replace(/\s*```$/, '')
        .trim();
    try {
        return JSON.parse(unfenced);
    } catch {
        const start = unfenced.indexOf('{');
        const end = unfenced.lastIndexOf('}');
        if (start >= 0 && end > start) {
            try {
                return JSON.parse(unfenced.slice(start, end + 1));
            } catch {
                /* fall through */
            }
        }
        throw new Error(`Claude returned non-JSON content: ${raw.slice(0, 600)}`);
    }
}
