// Postgres health check for the swing schema. Run via: npm run db:pg:health
import { pgClient } from '../lib/db/client';

type CountRow = { count: bigint | number | string };

function toNumber(value: unknown): number {
    if (typeof value === 'bigint') return Number(value);
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
}

async function countTable(table: string): Promise<number | null> {
    try {
        const db = pgClient();
        const rows = await db.$queryRawUnsafe<CountRow[]>(`SELECT COUNT(*)::bigint AS count FROM ${table}`);
        return toNumber(rows?.[0]?.count);
    } catch (err) {
        const message = String((err as { message?: unknown } | null | undefined)?.message || '').toLowerCase();
        if (message.includes('does not exist') || message.includes('undefined_table')) {
            return null;
        }
        throw err;
    }
}

const SWING_TABLES = [
    'swing.decisions',
    'swing.positions',
    'swing.ai_threads',
    'swing.ai_cooldowns',
    'swing.break_triggers',
    'swing.tick_log',
    'swing.postmortems',
    'swing.lessons',
    'swing.account_snapshots',
    'swing.weekly_digests',
] as const;

async function main() {
    const db = pgClient();
    const ping = await db.$queryRaw<Array<{ ok: number }>>`SELECT 1::int AS ok`;

    const counts: Record<string, number | null> = {};
    for (const table of SWING_TABLES) {
        counts[table] = await countTable(table);
    }

    console.log(
        JSON.stringify(
            {
                ok: Array.isArray(ping) && ping.length > 0,
                generatedAtIso: new Date().toISOString(),
                counts,
            },
            null,
            2,
        ),
    );
}

main()
    .catch((err) => {
        console.error('pg-healthcheck failed:', err?.message || String(err));
        process.exitCode = 1;
    })
    .finally(async () => {
        try {
            await pgClient().$disconnect();
        } catch {
            // best effort
        }
    });
