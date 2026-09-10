// Clears a `default_transaction_read_only = on` flag that leaked onto Neon's
// POOLED server connections.
//
// Incident 2026-09-10 ~11:45 UTC: read-only analysis scripts ran
// `SET default_transaction_read_only = on` (session-level) over the pooled
// SCALP_PG_CONNECTION_STRING. Neon's pooler runs pgbouncer in transaction mode,
// so a session-level SET stays on the SERVER connection after the client
// disconnects and is handed to the next client — the app. ensureSwingSchema's
// `CREATE SCHEMA IF NOT EXISTS` then failed with "cannot execute CREATE SCHEMA
// in a read-only transaction" on every analyze/wake-watch tick that drew a
// poisoned connection; the 15-minute cadence fell from 25 ticks per slot to 0–5.
//
// Fix: open many connections at once (each open transaction pins a distinct
// server connection), read the flag, and SET it back to off on the ones that
// carry it. Repeat until a full round finds none. Nothing else is touched.
//
// Alternative with the same effect: restart the compute endpoint in the Neon
// console (drops every pooled server connection).
//
// Usage: node scripts/with-db-env.mjs node scripts/pg-reset-read-only.mjs
//
// Never again: for read-only analysis use `BEGIN READ ONLY` per transaction
// (transaction-scoped, pooler-safe) or the UNPOOLED connection string — never a
// session-level SET on a pooled URL.
import pg from 'pg';

const N = Number(process.env.PG_RESET_FANOUT || 60);
const url = process.env.SCALP_PG_CONNECTION_STRING;
if (!url) {
    console.error('SCALP_PG_CONNECTION_STRING is not set (run through scripts/with-db-env.mjs)');
    process.exit(1);
}

async function round(fix) {
    const clients = Array.from({ length: N }, () => new pg.Client({ connectionString: url, ssl: { rejectUnauthorized: false } }));
    await Promise.all(clients.map((c) => c.connect()));
    const states = await Promise.all(
        clients.map(async (c) => {
            await c.query('BEGIN');
            const r = await c.query('SHOW default_transaction_read_only');
            return r.rows[0].default_transaction_read_only;
        }),
    );
    let fixed = 0;
    if (fix) {
        await Promise.all(
            clients.map(async (c, i) => {
                if (states[i] === 'on') {
                    await c.query('SET SESSION default_transaction_read_only = off');
                    fixed += 1;
                }
            }),
        );
    }
    await Promise.all(clients.map((c) => c.query('COMMIT').catch(() => {})));
    await Promise.all(clients.map((c) => c.end()));
    return { on: states.filter((s) => s === 'on').length, off: states.filter((s) => s === 'off').length, fixed };
}

console.log('probe:', JSON.stringify(await round(false)));
for (let i = 0; i < 8; i += 1) {
    const r = await round(true);
    console.log(`fix round ${i + 1}:`, JSON.stringify(r));
    if (r.on === 0) break;
}
console.log('final probe:', JSON.stringify(await round(false)));
