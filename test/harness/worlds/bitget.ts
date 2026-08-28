// WORLD BUILDER: Bitget REST protocol — mechanics only.
//
// lib/bitget.ts treats a call as successful only when BOTH res.ok AND the
// body envelope carry code "00000"; the module returns `data`. Anything else
// throws BitgetApiError. The host is hardcoded in lib/bitget.ts.

import { http, HttpResponse } from 'msw';

import type { HttpResponseResolver, RequestHandler } from 'msw';

export const BITGET_HOST = 'https://api.bitget.com';

/** The success envelope every Bitget handler must return. */
export function bitgetData(data: unknown): Response {
    return HttpResponse.json({ code: '00000', msg: 'success', requestTime: 0, data });
}

/** A named Bitget API failure (res.ok but code != 00000 — how Bitget rejects). */
export function bitgetError(code: string, msg: string): Response {
    return HttpResponse.json({ code, msg, requestTime: 0, data: null });
}

function toResolver(data: unknown | HttpResponseResolver): HttpResponseResolver {
    return typeof data === 'function' ? (data as HttpResponseResolver) : () => bitgetData(data);
}

/** GET route under /api/v2/... answering `data` inside the success envelope. */
export function bitgetGet(path: string, data: unknown | HttpResponseResolver): RequestHandler {
    return http.get(`${BITGET_HOST}${path}`, toResolver(data));
}

/** POST route (signed calls: order placement, account settings, ...). */
export function bitgetPost(path: string, data: unknown | HttpResponseResolver): RequestHandler {
    return http.post(`${BITGET_HOST}${path}`, toResolver(data));
}
