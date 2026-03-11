import { PrismaClient } from '@prisma/client';

declare global {
    // eslint-disable-next-line no-var
    var __scalpPrismaClient: PrismaClient | undefined;
}

export function isScalpPgConfigured(): boolean {
    return Boolean(String(process.env.PRISMA_PG_POSTGRES_URL || '').trim());
}

export function createScalpPrismaClient(): PrismaClient {
    if (!isScalpPgConfigured()) {
        throw new Error('Missing PRISMA_PG_POSTGRES_URL for scalp Postgres backend');
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
