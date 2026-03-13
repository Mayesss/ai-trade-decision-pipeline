import { spawnSync } from 'node:child_process';

import nextEnv from '@next/env';

const { loadEnvConfig } = nextEnv;

const DATABASE_URL_CANDIDATES = [
    'DATABASE_URL',
    'SCALP_PG_CONNECTION_STRING',
    'NEON__POSTGRES_PRISMA_URL',
    'NEON_POSTGRES_PRISMA_URL',
    'NEON__DATABASE_URL',
    'NEON_DATABASE_URL',
    'NEON__POSTGRES_URL',
    'NEON_POSTGRES_URL',
    'POSTGRES_PRISMA_URL',
    'POSTGRES_URL',
    'PRISMA_CONNECTION_STRING',
    'PRISMA_PG_POSTGRES_URL',
];

const DIRECT_URL_CANDIDATES = [
    'DIRECT_URL',
    'NEON__DATABASE_URL_UNPOOLED',
    'NEON__POSTGRES_URL_NON_POOLING',
    'POSTGRES_URL_NON_POOLING',
];

function readEnv(name) {
    return String(process.env[name] || '').trim();
}

function looksLikeLegacyPrismaDataProxyUrl(url) {
    try {
        const parsed = new URL(url);
        const host = String(parsed.hostname || '').trim().toLowerCase();
        return host === 'db.prisma.io' || host.endsWith('.db.prisma.io');
    } catch {
        return String(url || '').trim().toLowerCase().includes('db.prisma.io');
    }
}

function buildUrlFromPgParts(prefix = '') {
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

function resolveDatabaseUrl() {
    for (const envKey of DATABASE_URL_CANDIDATES) {
        const url = readEnv(envKey);
        if (!url || looksLikeLegacyPrismaDataProxyUrl(url)) continue;
        return url;
    }

    return buildUrlFromPgParts('NEON__') || buildUrlFromPgParts('NEON_') || buildUrlFromPgParts('');
}

function resolveDirectUrl() {
    for (const envKey of DIRECT_URL_CANDIDATES) {
        const url = readEnv(envKey);
        if (url) return url;
    }
    return '';
}

loadEnvConfig(process.cwd());

const databaseUrl = resolveDatabaseUrl();
if (databaseUrl) {
    process.env.DATABASE_URL = databaseUrl;
    process.env.PRISMA_CONNECTION_STRING = databaseUrl;
    process.env.PRISMA_PG_POSTGRES_URL = databaseUrl;
    if (!readEnv('SCALP_PG_CONNECTION_STRING')) {
        process.env.SCALP_PG_CONNECTION_STRING = databaseUrl;
    }
}

const directUrl = resolveDirectUrl();
if (directUrl && !readEnv('DIRECT_URL')) {
    process.env.DIRECT_URL = directUrl;
}

const args = process.argv.slice(2);
if (args.length === 0) {
    console.error('Usage: node scripts/with-db-env.mjs <command> [args...]');
    process.exit(1);
}

const result = spawnSync(args[0], args.slice(1), {
    stdio: 'inherit',
    env: process.env,
});

if (result.error) {
    console.error(result.error.message);
    process.exit(1);
}

process.exit(result.status ?? 0);
