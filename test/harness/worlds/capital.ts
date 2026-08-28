// WORLD BUILDER: Capital.com REST protocol — mechanics only.
//
// Every authed call is preceded by POST /api/v1/session once per 10-minute
// module-level session cache; the session response MUST carry the CST and
// X-SECURITY-TOKEN response headers (lib/capital.ts throws otherwise), which
// then ride along as request headers on every subsequent call.

import { http, HttpResponse } from 'msw';

import type { HttpResponseResolver, RequestHandler } from 'msw';

export const CAPITAL_HOST = 'https://api-capital.backend-capital.com';

export const CAPITAL_SESSION_TOKENS = {
    cst: 'test-cst',
    securityToken: 'test-security-token',
} as const;

/** The login route — answers the tokens the client sends on authed calls. */
export function capitalSession(): RequestHandler {
    return http.post(`${CAPITAL_HOST}/api/v1/session`, () =>
        HttpResponse.json(
            {},
            {
                headers: {
                    CST: CAPITAL_SESSION_TOKENS.cst,
                    'X-SECURITY-TOKEN': CAPITAL_SESSION_TOKENS.securityToken,
                },
            },
        ),
    );
}

function toResolver(json: unknown | HttpResponseResolver): HttpResponseResolver {
    return typeof json === 'function' ? (json as HttpResponseResolver) : () => HttpResponse.json(json as object);
}

export function capitalGet(path: string, json: unknown | HttpResponseResolver): RequestHandler {
    return http.get(`${CAPITAL_HOST}${path}`, toResolver(json));
}

export function capitalPost(path: string, json: unknown | HttpResponseResolver): RequestHandler {
    return http.post(`${CAPITAL_HOST}${path}`, toResolver(json));
}

export function capitalPut(path: string, json: unknown | HttpResponseResolver): RequestHandler {
    return http.put(`${CAPITAL_HOST}${path}`, toResolver(json));
}

export function capitalDelete(path: string, json: unknown | HttpResponseResolver): RequestHandler {
    return http.delete(`${CAPITAL_HOST}${path}`, toResolver(json));
}
