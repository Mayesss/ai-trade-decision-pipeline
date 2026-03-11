import { PrismaClient } from '@prisma/client';

declare global {
    // eslint-disable-next-line no-var
    var __scalpPrismaClient: PrismaClient | undefined;
}

let warnedLegacyPgUrlFallback = false;

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

function resolveScalpPgUrl(): string {
    const preferred = String(process.env.PRISMA_CONNECTION_STRING || '').trim();
    if (preferred) return preferred;
    const legacy = String(process.env.PRISMA_PG_POSTGRES_URL || '').trim();
    if (legacy) return legacy;
    return '';
}

export function isScalpPgConfigured(): boolean {
    return Boolean(resolveScalpPgUrl());
}

export function createScalpPrismaClient(): PrismaClient {
    const url = resolveScalpPgUrl();
    if (!url) {
        throw new Error('Missing PRISMA_CONNECTION_STRING for scalp Postgres backend');
    }
    // Backward compatibility: if only legacy env is set, bridge it into the canonical var and warn once.
    if (!process.env.PRISMA_CONNECTION_STRING && process.env.PRISMA_PG_POSTGRES_URL) {
        process.env.PRISMA_CONNECTION_STRING = process.env.PRISMA_PG_POSTGRES_URL;
        if (!warnedLegacyPgUrlFallback) {
            warnedLegacyPgUrlFallback = true;
            console.warn(
                '[scalp-pg] PRISMA_PG_POSTGRES_URL is deprecated; please migrate to PRISMA_CONNECTION_STRING.',
            );
        }
    } else {
        process.env.PRISMA_CONNECTION_STRING = url;
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
