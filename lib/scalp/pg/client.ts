import { PrismaClient } from '@prisma/client';

declare global {
    // eslint-disable-next-line no-var
    var __scalpPrismaClient: PrismaClient | undefined;
}

let warnedLegacyPgUrlFallback = false;

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
    return new PrismaClient({
        log: process.env.NODE_ENV === 'development' ? ['warn', 'error'] : ['error'],
    });
}

export function scalpPrisma(): PrismaClient {
    if (!global.__scalpPrismaClient) {
        global.__scalpPrismaClient = createScalpPrismaClient();
    }
    return global.__scalpPrismaClient;
}
