import type { NextApiRequest } from 'next';

export interface CronInvokeResult {
    invoked: boolean;
    status: number | null;
    error: string | null;
    url: string | null;
    detached?: boolean;
}

export function resolveCronBaseUrl(req: NextApiRequest): string | null {
    const explicit = String(process.env.SCALP_ORCHESTRATOR_BASE_URL || process.env.APP_BASE_URL || process.env.URL || '').trim();
    if (explicit) return explicit.replace(/\/+$/, '');
    const host = String(req.headers.host || '').trim();
    if (!host) return null;
    const proto = host.includes('localhost') || host.startsWith('127.0.0.1') ? 'http' : 'https';
    return `${proto}://${host}`;
}

function buildCronUrl(
    baseUrl: string,
    path: string,
    query: Record<string, string | number | boolean | null | undefined>,
): string {
    const qs = new URLSearchParams();
    for (const [key, value] of Object.entries(query)) {
        if (value === undefined || value === null) continue;
        if (typeof value === 'boolean') qs.set(key, value ? '1' : '0');
        else qs.set(key, String(value));
    }
    const normalizedPath = path.startsWith('/') ? path : `/${path}`;
    return `${baseUrl}${normalizedPath}${qs.toString() ? `?${qs.toString()}` : ''}`;
}

export async function invokeCronEndpoint(
    req: NextApiRequest,
    path: string,
    query: Record<string, string | number | boolean | null | undefined>,
    timeoutMs = 4_000,
): Promise<CronInvokeResult> {
    const baseUrl = resolveCronBaseUrl(req);
    if (!baseUrl) return { invoked: false, status: null, error: 'missing_base_url', url: null };
    const url = buildCronUrl(baseUrl, path, query);
    const headers: Record<string, string> = {};
    const adminSecret = String(process.env.ADMIN_ACCESS_SECRET || '').trim();
    if (adminSecret) headers['x-admin-access-secret'] = adminSecret;
    const ctrl = new AbortController();
    const timeout = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers,
            cache: 'no-store',
            signal: ctrl.signal,
        });
        return {
            invoked: true,
            status: response.status,
            error: response.ok ? null : `http_${response.status}`,
            url,
        };
    } catch (err: any) {
        return {
            invoked: false,
            status: null,
            error: String(err?.message || err || 'invoke_failed'),
            url,
        };
    } finally {
        clearTimeout(timeout);
    }
}

export async function invokeCronEndpointDetached(
    req: NextApiRequest,
    path: string,
    query: Record<string, string | number | boolean | null | undefined>,
    timeoutMs = 750,
): Promise<CronInvokeResult> {
    const baseUrl = resolveCronBaseUrl(req);
    if (!baseUrl) return { invoked: false, status: null, error: 'missing_base_url', url: null, detached: false };
    return invokeCronUrlDetached(baseUrl, path, query, timeoutMs);
}

export async function invokeCronUrlDetached(
    baseUrl: string,
    path: string,
    query: Record<string, string | number | boolean | null | undefined>,
    timeoutMs = 750,
): Promise<CronInvokeResult> {
    const url = buildCronUrl(baseUrl, path, query);
    const headers: Record<string, string> = {};
    const adminSecret = String(process.env.ADMIN_ACCESS_SECRET || '').trim();
    if (adminSecret) headers['x-admin-access-secret'] = adminSecret;
    const ctrl = new AbortController();
    let timedOut = false;
    const timeout = setTimeout(() => {
        timedOut = true;
        ctrl.abort();
    }, timeoutMs);
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers,
            cache: 'no-store',
            signal: ctrl.signal,
        });
        return {
            invoked: true,
            status: response.status,
            error: response.ok ? null : `http_${response.status}`,
            url,
            detached: false,
        };
    } catch (err: any) {
        if (timedOut) {
            return {
                invoked: true,
                status: 202,
                error: null,
                url,
                detached: true,
            };
        }
        return {
            invoked: false,
            status: null,
            error: String(err?.message || err || 'invoke_failed'),
            url,
            detached: false,
        };
    } finally {
        clearTimeout(timeout);
    }
}
