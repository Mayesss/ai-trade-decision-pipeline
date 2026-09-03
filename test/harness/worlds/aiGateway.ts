// WORLD BUILDER: Vercel AI Gateway — canned model answers, both dialect shapes.
//
// One host, two dialects:
//   POST /v1/responses  Responses dialect (lib/gatewayResponses.ts, raw fetch)
//   POST /v1/messages   Messages dialect  (lib/gatewayMessages.ts via SDK)
//
// The DECISION a test cares about goes in as `json`; the canned response
// wraps it in the dialect's envelope. What the snapshot then captures is the
// OUTGOING request — including the full prompt. That is the prompt
// regression net: a prompt change shows up as a snapshot diff.
//
// Note: the Anthropic SDK retries 408/429/5xx twice by default — error-path
// tests must account for repeated requests in the conversation.

import { http, HttpResponse } from 'msw';

import type { RequestHandler } from 'msw';

export const AI_GATEWAY_HOST = 'https://ai-gateway.vercel.sh';

type ResponsesOpts = { responseId?: string; model?: string; inputTokens?: number; outputTokens?: number };

// The envelope the gateway returns for /v1/responses. WHICH channel the answer
// arrives on follows the request, exactly as it does in production: a request
// that declares tools is a forced tool call and answers on
// function_call.arguments, a schema-less one answers as message output_text.
// Getting this wrong in the world would hide the whole 2026-09-03 failure
// class, where the model answered on the wrong channel.
function responsesEnvelope(
    answer: { kind: 'function_call'; name: string; args: string } | { kind: 'message'; text: string },
    opts: ResponsesOpts,
    extra: Record<string, unknown> = {},
) {
    return {
        id: opts.responseId ?? 'resp_test-1',
        model: opts.model ?? 'zai/glm-5.3',
        status: 'completed',
        output: [
            { type: 'reasoning', summary: [] },
            answer.kind === 'function_call'
                ? { type: 'function_call', name: answer.name, arguments: answer.args, call_id: 'call_test-1' }
                : { type: 'message', role: 'assistant', content: [{ type: 'output_text', text: answer.text }] },
        ],
        usage: {
            input_tokens: opts.inputTokens ?? 1000,
            output_tokens: opts.outputTokens ?? 200,
            input_tokens_details: { cached_tokens: 0 },
        },
        ...extra,
    };
}

export function responsesDecides(json: unknown, opts: ResponsesOpts = {}): RequestHandler {
    return http.post(`${AI_GATEWAY_HOST}/v1/responses`, async ({ request }) => {
        const body = (await request.clone().json()) as { tools?: Array<{ name?: string }> | null };
        const tool = body?.tools?.[0];
        return HttpResponse.json(
            tool
                ? responsesEnvelope(
                      { kind: 'function_call', name: String(tool.name ?? 'decision'), args: JSON.stringify(json) },
                      opts,
                  )
                : responsesEnvelope({ kind: 'message', text: JSON.stringify(json) }, opts),
        );
    });
}

// The 2026-09-03 failure shape: the model ignores the answer channel and
// writes its thinking into the message item instead. `then` is what the SECOND
// (nudged) attempt gets — pass a decision to exercise the reshape retry's
// recovery, or nothing to make both attempts fail.
export function responsesAnswersInProse(
    prose: string,
    then: { json?: unknown } = {},
    opts: ResponsesOpts = {},
): RequestHandler {
    let calls = 0;
    return http.post(`${AI_GATEWAY_HOST}/v1/responses`, async ({ request }) => {
        calls += 1;
        const body = (await request.clone().json()) as { tools?: Array<{ name?: string }> | null };
        const tool = body?.tools?.[0];
        if (calls > 1 && then.json !== undefined && tool) {
            return HttpResponse.json(
                responsesEnvelope(
                    { kind: 'function_call', name: String(tool.name ?? 'decision'), args: JSON.stringify(then.json) },
                    opts,
                ),
            );
        }
        return HttpResponse.json(responsesEnvelope({ kind: 'message', text: prose }, opts));
    });
}

// A reply cut off by the output ceiling. status 'incomplete' voids the shape
// promise even when the partial text happens to parse, so the client must
// refuse it rather than act on half a decision.
export function responsesTruncates(partial: string, opts: ResponsesOpts = {}): RequestHandler {
    return http.post(`${AI_GATEWAY_HOST}/v1/responses`, () =>
        HttpResponse.json(
            responsesEnvelope({ kind: 'message', text: partial }, opts, {
                status: 'incomplete',
                incomplete_details: { reason: 'max_output_tokens' },
            }),
        ),
    );
}

// Third dialect on the same host: POST /v1/chat/completions (OpenAI-compatible)
// — used by lib/swing/perplexity.ts (perplexity/* models). Handlers
// discriminate on the request body's `model` prefix and fall through
// (passthrough to the next handler) otherwise.

function chatCompletionEnvelope(content: string, model: string, opts: { inputTokens?: number; outputTokens?: number }) {
    return {
        id: 'chatcmpl-test-1',
        object: 'chat.completion',
        model,
        choices: [{ index: 0, message: { role: 'assistant', content }, finish_reason: 'stop' }],
        usage: {
            prompt_tokens: opts.inputTokens ?? 500,
            completion_tokens: opts.outputTokens ?? 100,
        },
    };
}

export function perplexityReports(
    text: string,
    opts: { model?: string; inputTokens?: number; outputTokens?: number } = {},
): RequestHandler {
    return http.post(`${AI_GATEWAY_HOST}/v1/chat/completions`, async ({ request }) => {
        const body = (await request.clone().json()) as { model?: string };
        if (!String(body?.model || '').startsWith('perplexity/')) return undefined;
        return HttpResponse.json(chatCompletionEnvelope(text, opts.model ?? 'perplexity/sonar', opts));
    });
}

export function messagesDecides(
    json: unknown,
    opts: { responseId?: string; model?: string; inputTokens?: number; outputTokens?: number } = {},
): RequestHandler {
    return http.post(`${AI_GATEWAY_HOST}/v1/messages`, () =>
        HttpResponse.json({
            id: opts.responseId ?? 'msg_test-1',
            type: 'message',
            role: 'assistant',
            model: opts.model ?? 'anthropic/claude-opus-4.8',
            content: [
                { type: 'thinking', thinking: 'canned reasoning', signature: 'test-signature' },
                { type: 'text', text: JSON.stringify(json) },
            ],
            stop_reason: 'end_turn',
            stop_sequence: null,
            usage: {
                input_tokens: opts.inputTokens ?? 1000,
                output_tokens: opts.outputTokens ?? 200,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
        }),
    );
}
