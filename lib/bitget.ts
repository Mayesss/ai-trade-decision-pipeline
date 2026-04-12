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

export async function signBitget(
  method: string,
  path: string,
  query: string,
  body: string,
) {
  const ts = Date.now().toString();
  const prehash = ts + method.toUpperCase() + path + (query ? `?${query}` : '') + body;

  const key = crypto.createHmac('sha256', process.env.BITGET_API_SECRET || '');
  key.update(prehash);
  const signB64 = key.digest('base64');

  return { ts, signB64 };
}

type BitgetFetchMethod = 'GET' | 'POST';

export interface BitgetFetchRetryOptions {
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  jitterRatio?: number;
  retryOnNetworkError?: boolean;
  retryOnRateLimit?: boolean;
  retryOn5xx?: boolean;
}

export interface BitgetFetchOptions {
  retry?: BitgetFetchRetryOptions;
  minIntervalMs?: number;
  timeoutMs?: number;
}

export class BitgetApiError extends Error {
  status: number;
  code: string | null;
  msg: string | null;
  method: BitgetFetchMethod;
  path: string;
  url: string;
  retryAfterMs: number | null;

  constructor(params: {
    status: number;
    code?: string | null;
    msg?: string | null;
    method: BitgetFetchMethod;
    path: string;
    url: string;
    retryAfterMs?: number | null;
  }) {
    const codePart = params.code ? ` ${params.code}` : '';
    const msgPart = params.msg ? `: ${params.msg}` : '';
    super(`Bitget error${codePart || ` ${params.status}`}${msgPart}`);
    this.name = 'BitgetApiError';
    this.status = Number(params.status) || 0;
    this.code = params.code || null;
    this.msg = params.msg || null;
    this.method = params.method;
    this.path = params.path;
    this.url = params.url;
    this.retryAfterMs = params.retryAfterMs ?? null;
  }
}

function sleepMs(ms: number): Promise<void> {
  const wait = Math.max(0, Math.floor(ms));
  if (!wait) return Promise.resolve();
  return new Promise((resolve) => setTimeout(resolve, wait));
}

function envInt(name: string, fallback: number): number {
  const n = Math.floor(Number(process.env[name]));
  if (!Number.isFinite(n)) return fallback;
  return n;
}

function parseRetryAfterMs(headers: Headers): number | null {
  const raw = String(headers.get('retry-after') || '').trim();
  if (!raw) return null;
  const seconds = Number(raw);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return Math.max(0, Math.floor(seconds * 1000));
  }
  const asDateMs = Date.parse(raw);
  if (!Number.isFinite(asDateMs)) return null;
  return Math.max(0, Math.floor(asDateMs - Date.now()));
}

function looksLikeRateLimitSignal(params: {
  status: number;
  code: string | null;
  msg: string | null;
}): boolean {
  if (params.status === 429) return true;
  const code = String(params.code || '').trim().toLowerCase();
  if (code === '429' || code === 'too_many_requests') return true;
  const msg = String(params.msg || '').trim().toLowerCase();
  if (!msg) return false;
  return (
    msg.includes('too many requests') ||
    msg.includes('request too frequent') ||
    msg.includes('too frequent') ||
    msg.includes('frequency limit')
  );
}

function shouldRetryBitgetFetch(
  err: unknown,
  method: BitgetFetchMethod,
  retry: Required<BitgetFetchRetryOptions>,
): boolean {
  if (method !== 'GET') return false;
  if (err instanceof BitgetApiError) {
    if (retry.retryOnRateLimit && looksLikeRateLimitSignal(err)) return true;
    if (retry.retryOn5xx && (err.status === 408 || (err.status >= 500 && err.status <= 599))) {
      return true;
    }
    return false;
  }
  if (!retry.retryOnNetworkError) return false;
  const message = String((err as any)?.message || err || '').toLowerCase();
  return (
    message.includes('fetch failed') ||
    message.includes('network') ||
    message.includes('socket') ||
    message.includes('timeout') ||
    message.includes('aborted') ||
    message.includes('abort') ||
    message.includes('econnreset') ||
    message.includes('econnrefused')
  );
}

function resolveRetryOptions(
  method: BitgetFetchMethod,
  options: BitgetFetchOptions | undefined,
): Required<BitgetFetchRetryOptions> {
  const raw = options?.retry || {};
  const maxAttemptsDefault = method === 'GET' ? envInt('BITGET_GET_MAX_ATTEMPTS', 5) : 1;
  return {
    maxAttempts: Math.max(1, Math.min(10, Math.floor(Number(raw.maxAttempts ?? maxAttemptsDefault) || maxAttemptsDefault))),
    baseDelayMs: Math.max(50, Math.min(10_000, Math.floor(Number(raw.baseDelayMs ?? envInt('BITGET_RETRY_BASE_DELAY_MS', 250)) || 250))),
    maxDelayMs: Math.max(200, Math.min(120_000, Math.floor(Number(raw.maxDelayMs ?? envInt('BITGET_RETRY_MAX_DELAY_MS', 6_000)) || 6_000))),
    jitterRatio: Math.max(0, Math.min(1, Number(raw.jitterRatio ?? 0.25) || 0)),
    retryOnNetworkError: raw.retryOnNetworkError !== false,
    retryOnRateLimit: raw.retryOnRateLimit !== false,
    retryOn5xx: raw.retryOn5xx !== false,
  };
}

function resolveFetchTimeoutMs(options: BitgetFetchOptions | undefined): number {
  const fallback = envInt('BITGET_HTTP_TIMEOUT_MS', 12_000);
  const timeoutMs = Math.floor(Number(options?.timeoutMs ?? fallback) || fallback);
  return Math.max(1_000, Math.min(120_000, timeoutMs));
}

function computeBackoffMs(
  attempt: number,
  retry: Required<BitgetFetchRetryOptions>,
  retryAfterMs: number | null,
): number {
  if (Number.isFinite(Number(retryAfterMs)) && Number(retryAfterMs) > 0) {
    return Math.max(50, Math.min(retry.maxDelayMs, Math.floor(Number(retryAfterMs))));
  }
  const exp = Math.min(retry.maxDelayMs, retry.baseDelayMs * 2 ** Math.max(0, attempt - 1));
  const jitter = Math.floor(exp * retry.jitterRatio * Math.random());
  return Math.max(50, exp + jitter);
}

let bitgetPacingLock: Promise<void> = Promise.resolve();
let bitgetLastPacedAtMs = 0;

function shouldApplyBitgetMarketPacing(method: BitgetFetchMethod, path: string): boolean {
  return method === 'GET' && path.startsWith('/api/v2/mix/market/');
}

async function applyBitgetPacing(method: BitgetFetchMethod, path: string, minIntervalMs: number): Promise<void> {
  if (!shouldApplyBitgetMarketPacing(method, path)) return;
  if (minIntervalMs <= 0) return;

  const waitLock = bitgetPacingLock;
  let release: () => void = () => undefined;
  bitgetPacingLock = new Promise<void>((resolve) => {
    release = resolve;
  });
  await waitLock;
  try {
    const now = Date.now();
    const waitMs = bitgetLastPacedAtMs + minIntervalMs - now;
    if (waitMs > 0) await sleepMs(waitMs);
    bitgetLastPacedAtMs = Date.now();
  } finally {
    release();
  }
}

export async function bitgetFetch(
  method: BitgetFetchMethod,
  path: string,
  params: Record<string, string | number | undefined> = {},
  bodyObj?: unknown,
  options?: BitgetFetchOptions,
) {
  const query = buildQuery(params);
  const body = bodyObj ? JSON.stringify(bodyObj) : '';
  const url = `https://api.bitget.com${path}${query ? `?${query}` : ''}`;
  const retry = resolveRetryOptions(method, options);
  const minIntervalMs = Math.max(0, Math.min(2_000, Math.floor(Number(options?.minIntervalMs ?? envInt('BITGET_MARKET_MIN_INTERVAL_MS', 60)) || 0)));
  const timeoutMs = resolveFetchTimeoutMs(options);

  for (let attempt = 1; attempt <= retry.maxAttempts; attempt += 1) {
    try {
      await applyBitgetPacing(method, path, minIntervalMs);
      const { ts, signB64 } = await signBitget(method, path, query, body);

      const headers: Record<string, string> = {
        'ACCESS-KEY': process.env.BITGET_API_KEY ?? '',
        'ACCESS-SIGN': signB64,
        'ACCESS-PASSPHRASE': process.env.BITGET_API_PASSPHRASE ?? '',
        'ACCESS-TIMESTAMP': ts,
        'Content-Type': 'application/json',
        locale: 'en-US',
      };

      const controller = new AbortController();
      let timedOut = false;
      const timeoutHandle = setTimeout(() => {
        timedOut = true;
        controller.abort();
      }, timeoutMs);
      const res = await fetch(url, {
        method,
        headers,
        body: body || undefined,
        signal: controller.signal,
      })
        .catch((err: unknown) => {
          if (timedOut) {
            throw new Error(`bitget_fetch_timeout_${timeoutMs}ms:${method}:${path}`);
          }
          throw err;
        })
        .finally(() => {
          clearTimeout(timeoutHandle);
        });
      const text = await res.text();
      let parsed: any = null;
      if (text) {
        try {
          parsed = JSON.parse(text);
        } catch {
          parsed = null;
        }
      }
      const code =
        parsed && Object.prototype.hasOwnProperty.call(parsed, 'code')
          ? String(parsed.code)
          : null;
      const msg =
        parsed && Object.prototype.hasOwnProperty.call(parsed, 'msg')
          ? String(parsed.msg || '')
          : null;
      if (!res.ok || code !== '00000') {
        throw new BitgetApiError({
          status: res.status,
          code,
          msg: msg || res.statusText || 'request_failed',
          method,
          path,
          url,
          retryAfterMs: parseRetryAfterMs(res.headers),
        });
      }
      return parsed?.data;
    } catch (err: unknown) {
      const canRetry = attempt < retry.maxAttempts && shouldRetryBitgetFetch(err, method, retry);
      if (!canRetry) throw err;
      const retryAfterMs = err instanceof BitgetApiError ? err.retryAfterMs : null;
      const backoffMs = computeBackoffMs(attempt, retry, retryAfterMs);
      await sleepMs(backoffMs);
    }
  }

  throw new Error('bitget_fetch_unreachable');
}

export type ProductType = 'usdt-futures' | 'usdc-futures' | 'coin-futures';

export function resolveProductType(): ProductType {
  const t = (BITGET_ACCOUNT_TYPE || 'usdt-futures') as ProductType;
  return t;
}
