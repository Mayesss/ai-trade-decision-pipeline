import { Pool, type PoolClient } from 'pg';

export type ScalpPgSqlObject = {
    text?: string;
    sql?: string;
    strings?: readonly string[];
    values?: readonly unknown[];
};

export interface ScalpPgTxClient {
    $queryRaw<T = unknown>(query: ScalpPgSqlObject | TemplateStringsArray, ...values: unknown[]): Promise<T>;
    $queryRawUnsafe<T = unknown>(query: string, ...values: unknown[]): Promise<T>;
    $executeRaw(query: ScalpPgSqlObject | TemplateStringsArray, ...values: unknown[]): Promise<number>;
    $executeRawUnsafe(query: string, ...values: unknown[]): Promise<number>;
}

export interface ScalpPgTransactionOptions {
    maxWait?: number;
    timeout?: number;
    isolationLevel?: 'ReadUncommitted' | 'ReadCommitted' | 'RepeatableRead' | 'Serializable';
}

export interface ScalpPgClient extends ScalpPgTxClient {
    $transaction<T>(
        fn: (tx: ScalpPgTxClient) => Promise<T>,
        options?: ScalpPgTransactionOptions,
    ): Promise<T>;
    $disconnect(): Promise<void>;
}

type ScalpPgPoolConfig = {
    envKey: string;
    url: string;
    legacy: boolean;
};

type ScalpPgQueryConfig = {
    text: string;
    values: unknown[];
};

type ScalpPgQueryable = Pick<Pool, 'query'> | Pick<PoolClient, 'query'>;

declare global {
    // eslint-disable-next-line no-var
    var __scalpPgClient: ScalpPgClient | undefined;
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

function resolveScalpPgUrl(): ScalpPgPoolConfig | null {
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

function shouldUseSsl(connectionString: string): boolean {
    try {
        const parsed = new URL(connectionString);
        const sslMode = String(parsed.searchParams.get('sslmode') || '')
            .trim()
            .toLowerCase();

        if (sslMode === 'disable') return false;
        if (sslMode === 'require' || sslMode === 'verify-ca' || sslMode === 'verify-full') {
            return true;
        }

        const host = String(parsed.hostname || '').trim().toLowerCase();
        if (!host || host === 'localhost' || host === '127.0.0.1' || host === '::1') {
            return false;
        }
        return true;
    } catch {
        return true;
    }
}

function toPositiveInt(value: unknown, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return Math.floor(n);
}

function isTemplateStringsArray(value: unknown): value is TemplateStringsArray {
    return Array.isArray(value) && Array.isArray((value as { raw?: unknown }).raw);
}

function isScalpPgSqlObject(value: unknown): value is ScalpPgSqlObject {
    if (!value || typeof value !== 'object') return false;
    const row = value as Record<string, unknown>;
    return (
        typeof row.text === 'string' ||
        typeof row.sql === 'string' ||
        Array.isArray(row.strings) ||
        Array.isArray(row.values)
    );
}

function compileTemplateStrings(strings: readonly string[], values: readonly unknown[]): ScalpPgQueryConfig {
    let text = '';
    for (let idx = 0; idx < strings.length; idx += 1) {
        text += strings[idx] || '';
        if (idx < values.length) {
            text += `$${idx + 1}`;
        }
    }
    return {
        text,
        values: Array.from(values),
    };
}

function compileQuestionMarkSql(sql: string, values: readonly unknown[]): ScalpPgQueryConfig {
    let paramIdx = 0;
    const text = String(sql || '').replace(/\?/g, () => {
        paramIdx += 1;
        return `$${paramIdx}`;
    });
    return {
        text,
        values: Array.from(values),
    };
}

function compileSafeQuery(input: ScalpPgSqlObject | TemplateStringsArray, values: readonly unknown[]): ScalpPgQueryConfig {
    if (isTemplateStringsArray(input)) {
        return compileTemplateStrings(input, values);
    }

    if (isScalpPgSqlObject(input)) {
        const sqlValues = Array.isArray(input.values) ? input.values : values;

        if (typeof input.text === 'string' && input.text.trim().length > 0) {
            return {
                text: input.text,
                values: Array.from(sqlValues),
            };
        }

        if (Array.isArray(input.strings) && input.strings.length > 0) {
            return compileTemplateStrings(input.strings, sqlValues);
        }

        if (typeof input.sql === 'string' && input.sql.trim().length > 0) {
            return compileQuestionMarkSql(input.sql, sqlValues);
        }
    }

    throw new Error('Unsupported SQL input. Use sql`` helper output or a tagged template query.');
}

function compileUnsafeQuery(input: string | ScalpPgSqlObject, values: readonly unknown[]): ScalpPgQueryConfig {
    if (typeof input === 'string') {
        return {
            text: input,
            values: Array.from(values),
        };
    }

    if (isScalpPgSqlObject(input)) {
        return compileSafeQuery(input, values);
    }

    throw new Error('Unsupported SQL input. Use a SQL string or sql`` helper output.');
}

function mapIsolationLevel(level: ScalpPgTransactionOptions['isolationLevel']): string | null {
    if (!level) return null;
    switch (level) {
        case 'ReadUncommitted':
            return 'READ UNCOMMITTED';
        case 'ReadCommitted':
            return 'READ COMMITTED';
        case 'RepeatableRead':
            return 'REPEATABLE READ';
        case 'Serializable':
            return 'SERIALIZABLE';
        default:
            return null;
    }
}

async function connectWithOptionalTimeout(pool: Pool, maxWaitMs?: number): Promise<PoolClient> {
    const maxWait = toPositiveInt(maxWaitMs, 0);
    if (maxWait <= 0) return pool.connect();

    const pending = pool.connect();
    let timeoutHandle: NodeJS.Timeout | null = null;

    try {
        const timeout = new Promise<PoolClient>((_, reject) => {
            timeoutHandle = setTimeout(() => {
                reject(new Error(`Timed out waiting for PG connection after ${maxWait}ms`));
            }, maxWait);
        });

        return await Promise.race([pending, timeout]);
    } catch (err) {
        pending
            .then((client) => {
                try {
                    client.release();
                } catch {
                    // best effort
                }
            })
            .catch(() => {
                // best effort
            });
        throw err;
    } finally {
        if (timeoutHandle) clearTimeout(timeoutHandle);
    }
}

class ScalpPgExecutorImpl implements ScalpPgTxClient {
    constructor(private readonly db: ScalpPgQueryable) {}

    async $queryRaw<T = unknown>(
        query: ScalpPgSqlObject | TemplateStringsArray,
        ...values: unknown[]
    ): Promise<T> {
        const compiled = compileSafeQuery(query, values);
        const result = await this.db.query(compiled.text, compiled.values);
        return result.rows as unknown as T;
    }

    async $queryRawUnsafe<T = unknown>(query: string, ...values: unknown[]): Promise<T> {
        const compiled = compileUnsafeQuery(query, values);
        const result = await this.db.query(compiled.text, compiled.values);
        return result.rows as unknown as T;
    }

    async $executeRaw(
        query: ScalpPgSqlObject | TemplateStringsArray,
        ...values: unknown[]
    ): Promise<number> {
        const compiled = compileSafeQuery(query, values);
        const result = await this.db.query(compiled.text, compiled.values);
        return Number(result.rowCount || 0);
    }

    async $executeRawUnsafe(query: string, ...values: unknown[]): Promise<number> {
        const compiled = compileUnsafeQuery(query, values);
        const result = await this.db.query(compiled.text, compiled.values);
        return Number(result.rowCount || 0);
    }
}

class ScalpPgClientImpl extends ScalpPgExecutorImpl implements ScalpPgClient {
    constructor(private readonly pool: Pool) {
        super(pool);
    }

    async $transaction<T>(
        fn: (tx: ScalpPgTxClient) => Promise<T>,
        options: ScalpPgTransactionOptions = {},
    ): Promise<T> {
        const client = await connectWithOptionalTimeout(this.pool, options.maxWait);
        const tx = new ScalpPgExecutorImpl(client);

        try {
            await client.query('BEGIN');

            const isolationLevel = mapIsolationLevel(options.isolationLevel);
            if (isolationLevel) {
                await client.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`);
            }

            const timeoutMs = toPositiveInt(options.timeout, 0);
            if (timeoutMs > 0) {
                await client.query('SET LOCAL statement_timeout = $1', [timeoutMs]);
            }

            const out = await fn(tx);
            await client.query('COMMIT');
            return out;
        } catch (err) {
            try {
                await client.query('ROLLBACK');
            } catch {
                // best effort
            }
            throw err;
        } finally {
            client.release();
        }
    }

    async $disconnect(): Promise<void> {
        if (global.__scalpPgClient === this) {
            global.__scalpPgClient = undefined;
        }
        await this.pool.end();
    }
}

export function isScalpPgConfigured(): boolean {
    return resolveScalpPgUrl() !== null;
}

export function createScalpPrismaClient(): ScalpPgClient {
    const resolved = resolveScalpPgUrl();
    if (!resolved) {
        throw new Error(
            'Missing scalp PG connection string. Set DATABASE_URL (Neon) or SCALP_PG_CONNECTION_STRING.',
        );
    }

    if (resolved.legacy && !warnedLegacyPgUrlFallback) {
        warnedLegacyPgUrlFallback = true;
        console.warn(
            `[scalp-pg] ${resolved.envKey} is deprecated; prefer DATABASE_URL (Neon) or SCALP_PG_CONNECTION_STRING.`,
        );
    }

    const sslEnabled = shouldUseSsl(resolved.url);
    const pool = new Pool({
        connectionString: resolved.url,
        ssl: sslEnabled ? { rejectUnauthorized: false } : undefined,
        max: toPositiveInt(readEnv('SCALP_PG_POOL_MAX'), 10),
        idleTimeoutMillis: toPositiveInt(readEnv('SCALP_PG_POOL_IDLE_MS'), 30_000),
        connectionTimeoutMillis: toPositiveInt(readEnv('SCALP_PG_POOL_CONNECT_TIMEOUT_MS'), 10_000),
        allowExitOnIdle: true,
    });

    return new ScalpPgClientImpl(pool);
}

export function scalpPrisma(): ScalpPgClient {
    if (!global.__scalpPgClient) {
        global.__scalpPgClient = createScalpPrismaClient();
    }
    return global.__scalpPgClient;
}
