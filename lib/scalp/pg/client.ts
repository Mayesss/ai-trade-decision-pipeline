import { PrismaClient } from '@prisma/client';

declare global {
    // eslint-disable-next-line no-var
    var __scalpPrismaClient: PrismaClient | undefined;
}

let warnedLegacyPgUrlFallback = false;

const PRIMARY_SCALP_PG_URL_ENV_KEYS = [
    'SCALP_PG_CONNECTION_STRING',
    'NEON__DATABASE_URL',
    'NEON__POSTGRES_PRISMA_URL',
    'NEON__POSTGRES_URL',
    'NEON_DATABASE_URL',
    'NEON_POSTGRES_PRISMA_URL',
    'NEON_POSTGRES_URL',
    'DATABASE_URL',
    'POSTGRES_PRISMA_URL',
    'POSTGRES_URL',
] as const;

const LEGACY_SCALP_PG_URL_ENV_KEYS = ['PRISMA_CONNECTION_STRING', 'PRISMA_PG_POSTGRES_URL'] as const;

type ScalpPgUrlResolution = {
    envKey: string;
    url: string;
    legacy: boolean;
};

type ScalpPgOpContext = {
    model?: string | null;
    operation?: string | null;
};

export class ScalpPgPayloadLimitError extends Error {
    readonly code = 'scalp_pg_payload_limit';
    readonly prismaCode = 'P6009';
    readonly requestId: string | null;
    readonly model: string | null;
    readonly operation: string | null;

    constructor(params: {
        message?: string;
        requestId?: string | null;
        model?: string | null;
        operation?: string | null;
        cause?: unknown;
    } = {}) {
        super(
            params.message ||
                'Scalp PG query payload exceeded Prisma Data Proxy limit (P6009). Reduce selected data size (aggregate/chunk/filter window).',
        );
        this.name = 'ScalpPgPayloadLimitError';
        this.requestId = params.requestId ?? null;
        this.model = params.model ?? null;
        this.operation = params.operation ?? null;
        (this as Error & { cause?: unknown }).cause = params.cause;
    }
}

export function isScalpPgPayloadLimitError(err: unknown): err is ScalpPgPayloadLimitError {
    return err instanceof ScalpPgPayloadLimitError;
}

function extractRequestId(text: string): string | null {
    const match = text.match(/request id was:\s*([^)]+)\)/i) || text.match(/request id[:=]\s*([A-Za-z0-9_-]+)/i);
    const id = String(match?.[1] || '').trim();
    return id || null;
}

function shouldAdaptPrismaPayloadLimitError(err: unknown): boolean {
    const anyErr = err as { code?: unknown; message?: unknown } | null | undefined;
    const code = String(anyErr?.code || '').trim().toUpperCase();
    if (code === 'P6009') return true;
    const message = String(anyErr?.message || '').toLowerCase();
    return message.includes('"code":"p6009"') || message.includes('code":"p6009') || message.includes('exceeded the the maximum of 5mb');
}

function adaptScalpPrismaError(err: unknown, context: ScalpPgOpContext = {}): never {
    if (isScalpPgPayloadLimitError(err)) {
        throw err;
    }
    if (shouldAdaptPrismaPayloadLimitError(err)) {
        const message = String((err as { message?: unknown })?.message || '').trim();
        const requestId = extractRequestId(message);
        const operation = String(context.operation || '').trim() || null;
        const model = String(context.model || '').trim() || null;
        const opHint = operation || model ? ` (${[model, operation].filter(Boolean).join('.')})` : '';
        const requestHint = requestId ? ` requestId=${requestId}.` : '';
        throw new ScalpPgPayloadLimitError({
            message: `Scalp PG query payload exceeded Prisma Data Proxy limit (P6009)${opHint}.${requestHint} Reduce payload via aggregation, narrower windows, or chunking.`,
            requestId,
            model,
            operation,
            cause: err,
        });
    }
    throw err;
}

function readEnv(name: string): string {
    return String(process.env[name] || '').trim();
}

function looksLikeLegacyPrismaDataProxyUrl(url: string): boolean {
    try {
        const parsed = new URL(url);
        const host = String(parsed.hostname || '').trim().toLowerCase();
        return host === 'db.prisma.io' || host.endsWith('.db.prisma.io');
    } catch {
        const normalized = String(url || '').trim().toLowerCase();
        return normalized.includes('db.prisma.io');
    }
}

function buildUrlFromPgParts(prefix = ''): string {
    const host = readEnv(`${prefix}PGHOST`);
    const user = readEnv(`${prefix}PGUSER`);
    const database = readEnv(`${prefix}PGDATABASE`);
    if (!host || !user || !database) return '';
    const password = readEnv(`${prefix}PGPASSWORD`);
    const auth = password
        ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}`
        : encodeURIComponent(user);
    return `postgresql://${auth}@${host}/${encodeURIComponent(database)}?sslmode=require`;
}

function resolveScalpPgUrl(): ScalpPgUrlResolution | null {
    for (const envKey of PRIMARY_SCALP_PG_URL_ENV_KEYS) {
        const url = readEnv(envKey);
        if (!url) continue;
        if (looksLikeLegacyPrismaDataProxyUrl(url)) continue;
        return { envKey, url, legacy: false };
    }
    const neonDoubleUnderscoreParts = buildUrlFromPgParts('NEON__');
    if (neonDoubleUnderscoreParts) {
        return { envKey: 'NEON__PG*', url: neonDoubleUnderscoreParts, legacy: false };
    }
    const neonSingleUnderscoreParts = buildUrlFromPgParts('NEON_');
    if (neonSingleUnderscoreParts) {
        return { envKey: 'NEON_PG*', url: neonSingleUnderscoreParts, legacy: false };
    }
    const plainPgParts = buildUrlFromPgParts('');
    if (plainPgParts) {
        return { envKey: 'PG*', url: plainPgParts, legacy: false };
    }
    for (const envKey of LEGACY_SCALP_PG_URL_ENV_KEYS) {
        const url = readEnv(envKey);
        if (!url) continue;
        if (looksLikeLegacyPrismaDataProxyUrl(url)) continue;
        return { envKey, url, legacy: true };
    }
    return null;
}

function bridgeScalpPgEnv(url: string): void {
    process.env.DATABASE_URL = url;
    process.env.PRISMA_CONNECTION_STRING = url;
}

export function isScalpPgConfigured(): boolean {
    return resolveScalpPgUrl() !== null;
}

export function createScalpPrismaClient(): PrismaClient {
    const resolved = resolveScalpPgUrl();
    if (!resolved) {
        throw new Error(
            'Missing scalp PG connection string. Set DATABASE_URL (Neon) or SCALP_PG_CONNECTION_STRING.',
        );
    }
    bridgeScalpPgEnv(resolved.url);
    if (resolved.legacy && !warnedLegacyPgUrlFallback) {
        warnedLegacyPgUrlFallback = true;
        console.warn(
            `[scalp-pg] ${resolved.envKey} is deprecated; prefer DATABASE_URL (Neon) or SCALP_PG_CONNECTION_STRING.`,
        );
    }
    const base = new PrismaClient({
        log: process.env.NODE_ENV === 'development' ? ['warn', 'error'] : ['error'],
    });
    const wrapped = base.$extends({
        query: {
            $allOperations({ model, operation, args, query }) {
                return query(args).catch((err: unknown) =>
                    adaptScalpPrismaError(err, {
                        model: model || null,
                        operation: operation || null,
                    }),
                );
            },
        },
    });
    return wrapped as unknown as PrismaClient;
}

export function scalpPrisma(): PrismaClient {
    if (!global.__scalpPrismaClient) {
        global.__scalpPrismaClient = createScalpPrismaClient();
    }
    return global.__scalpPrismaClient;
}
