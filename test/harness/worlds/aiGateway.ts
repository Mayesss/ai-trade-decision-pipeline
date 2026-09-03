// WORLD BUILDER: Vercel AI Gateway — canned model answers, both provider shapes.
//
// One host, two dialects:
//   POST /v1/responses  OpenAI Responses API   (lib/ai.ts, raw fetch)
//   POST /v1/messages   Anthropic Messages API (lib/claudeAi.ts via SDK)
//
// The DECISION a test cares about goes in as `json`; the canned response
// wraps it in the provider's envelope. What the snapshot then captures is the
// OUTGOING request — including the full prompt. That is the prompt
// regression net: a prompt change shows up as a snapshot diff.
//
// Note: the Anthropic SDK retries 408/429/5xx twice by default — error-path
// tests must account for repeated requests in the conversation.

import { http, HttpResponse } from 'msw';

import type { RequestHandler } from 'msw';

export const AI_GATEWAY_HOST = 'https://ai-gateway.vercel.sh';

export function openAiDecides(
    json: unknown,
    opts: { responseId?: string; model?: string; inputTokens?: number; outputTokens?: number } = {},
): RequestHandler {
    return http.post(`${AI_GATEWAY_HOST}/v1/responses`, () =>
        HttpResponse.json({
            id: opts.responseId ?? 'resp_test-1',
            model: opts.model ?? 'zai/glm-5.3',
            output: [
                { type: 'reasoning', summary: [] },
                {
                    type: 'message',
                    role: 'assistant',
                    content: [{ type: 'output_text', text: JSON.stringify(json) }],
                },
            ],
            usage: {
                input_tokens: opts.inputTokens ?? 1000,
                output_tokens: opts.outputTokens ?? 200,
                input_tokens_details: { cached_tokens: 0 },
            },
        }),
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

export function claudeDecides(
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
