// lib/bitget.ts
import crypto from 'crypto';
import { BITGET_ACCOUNT_TYPE } from './constants';

// ---- Utility: Bitget signing (HMAC-SHA256 + base64) ----
// Docs: https://www.bitget.com/api-doc/common/signature
// For v2: sign = base64( HMAC_SHA256(secret, timestamp + method + path + (query?"?"+query:"") + body) )

export function buildQuery(params: Record<string, string | number | undefined>) {
    return Object.entries(params)
        .filter(([, v]) => v !== undefined && v !== '')
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
        .join('&');
}

export async function signBitget(method: string, path: string, query: string, body: string) {
    const ts = Date.now().toString();
    const prehash = ts + method.toUpperCase() + path + (query ? `?${query}` : '') + body;

    const key = crypto.createHmac('sha256', process.env.BITGET_API_SECRET || '');
    key.update(prehash);
    const signB64 = key.digest('base64');

    return { ts, signB64 };
}

export async function bitgetFetch(
    method: 'GET' | 'POST',
    path: string,
    params: Record<string, string | number | undefined> = {},
    bodyObj?: unknown,
) {
    const query = buildQuery(params);
    const body = bodyObj ? JSON.stringify(bodyObj) : '';
    const { ts, signB64 } = await signBitget(method, path, query, body);

    const url = `https://api.bitget.com${path}${query ? `?${query}` : ''}`;
    const headers: Record<string, string> = {
        'ACCESS-KEY': process.env.BITGET_API_KEY ?? '',
        'ACCESS-SIGN': signB64,
        'ACCESS-PASSPHRASE': process.env.BITGET_API_PASSPHRASE ?? '',
        'ACCESS-TIMESTAMP': ts,
        'Content-Type': 'application/json',
        locale: 'en-US',
    };

    const res = await fetch(url, { method, headers, body: body ?? null });
    const json = await res.json();
    if (!res.ok || json.code !== '00000') {
        throw new Error(`Bitget error ${json.code || res.status}: ${json.msg || res.statusText}`);
    }
    return json.data;
}

export type ProductType = 'usdt-futures' | 'usdc-futures' | 'coin-futures';

export function resolveProductType(): ProductType {
    const t = (BITGET_ACCOUNT_TYPE || 'usdt-futures') as ProductType;
    return t;
}
