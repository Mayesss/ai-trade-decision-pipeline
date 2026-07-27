// lib/aiError.ts
//
// Typed failure surface for every AI provider call. Before this existed the
// clients threw plain Errors whose message was the only signal, so a lapsed
// OpenAI subscription (429 insufficient_quota) was indistinguishable from a
// transient rate limit or a 500 — nothing downstream could react. The kinds:
//
// - 'billing'   — quota exhausted / subscription lapsed. Will NOT self-heal;
//                 a human has to pay the bill.
// - 'config'    — bad or missing API key, unknown model. Will NOT self-heal;
//                 a human has to fix the env.
// - 'transient' — rate limits, 5xx, network, malformed model output. Expected
//                 to clear on its own; only a streak of these is noteworthy.

export type AiCallProvider = 'openai' | 'claude';
export type AiErrorKind = 'billing' | 'config' | 'transient';

export class AiCallError extends Error {
    readonly provider: AiCallProvider;
    readonly status: number | null;
    readonly code: string | null;
    readonly kind: AiErrorKind;

    constructor(params: {
        message: string;
        provider: AiCallProvider;
        status?: number | null;
        code?: string | null;
        kind?: AiErrorKind;
    }) {
        super(params.message);
        this.name = 'AiCallError';
        this.provider = params.provider;
        this.status = params.status ?? null;
        this.code = params.code ?? null;
        this.kind =
            params.kind ??
            classifyAiFailure({ status: this.status, code: this.code, message: params.message });
    }
}

// Maps a provider error's (status, code, message) onto a kind. Codes/messages
// covered: OpenAI `insufficient_quota` / `billing_hard_limit_reached` ("You
// exceeded your current quota, please check your plan and billing details"),
// Anthropic's out-of-credit 400 ("Your credit balance is too low..."), and
// both providers' auth failures. Anything unrecognized is 'transient' — the
// safe default, since a persistent kind latches the health flag.
export function classifyAiFailure(params: {
    status: number | null;
    code: string | null;
    message: string;
}): AiErrorKind {
    const code = String(params.code || '').toLowerCase();
    const msg = String(params.message || '').toLowerCase();
    if (
        code.includes('insufficient_quota') ||
        code.includes('billing') ||
        msg.includes('exceeded your current quota') ||
        msg.includes('billing') ||
        msg.includes('credit balance')
    ) {
        return 'billing';
    }
    if (
        params.status === 401 ||
        params.status === 403 ||
        code.includes('invalid_api_key') ||
        code.includes('authentication') ||
        code.includes('permission') ||
        msg.includes('api key')
    ) {
        return 'config';
    }
    return 'transient';
}

// Wraps anything a provider client can throw (network TypeError, JSON-parse
// Error, ...) so callers can rely on every AI failure being an AiCallError.
// Already-typed errors pass through untouched.
export function coerceAiCallError(err: unknown, provider: AiCallProvider): AiCallError {
    if (err instanceof AiCallError) return err;
    const message = err instanceof Error ? err.message : String(err);
    return new AiCallError({ message, provider, kind: 'transient' });
}
