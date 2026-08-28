// Minimal Next.js API req/res doubles — just enough for pages/api/analyze.ts,
// which reads req.method/url/query/headers and calls res.status(n).json(body).

export interface ApiResponseState {
    statusCode: number | null;
    body: unknown;
}

export function createApiRequest(params: {
    path: string;
    query?: Record<string, string>;
    headers?: Record<string, string>;
    method?: string;
}): unknown {
    const query = params.query ?? {};
    const search = new URLSearchParams(query).toString();
    return {
        method: params.method ?? 'GET',
        url: search ? `${params.path}?${search}` : params.path,
        query,
        headers: params.headers ?? {},
    };
}

export function createApiResponse(): { res: unknown; state: ApiResponseState } {
    const state: ApiResponseState = { statusCode: null, body: null };
    const res = {
        status(code: number) {
            state.statusCode = code;
            return res;
        },
        json(body: unknown) {
            state.body = body;
            return res;
        },
    };
    return { res, state };
}
