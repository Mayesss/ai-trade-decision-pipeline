// Verify the corrected denominator against prod, read-only.
// Replays loadClosedPositionRiskRows' new SELECT and summarizeRStats' new
// resolver, and shows old-unit vs new-unit side by side.
import pg from 'pg';
import nextEnv from '@next/env';
const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

const MIN_STOP_PCT = 0.0005, MAX_STOP_PCT = 1;
const client = new pg.Client({ connectionString: process.env.NEON__DATABASE_URL_UNPOOLED, ssl: { rejectUnauthorized: false } });
await client.connect();
await client.query('BEGIN READ ONLY');
const { rows } = await client.query(`
  SELECT p.platform, p.exit_ts_ms, p.pnl_net, p.notional, p.entry_price,
         CASE WHEN jsonb_typeof(d.ai_decision_json->'risk_sizing'->'risk_usd')='number'
              THEN (d.ai_decision_json->'risk_sizing'->>'risk_usd')::float8 END AS budget,
         CASE WHEN jsonb_typeof(d.ai_decision_json->'stop_loss_price')='number'
              THEN (d.ai_decision_json->>'stop_loss_price')::float8 END AS stop_price
  FROM swing.positions p LEFT JOIN swing.decisions d ON d.id = p.decision_id
  WHERE p.status='closed' AND p.pnl_net IS NOT NULL AND p.exit_ts_ms IS NOT NULL
  ORDER BY p.exit_ts_ms ASC`);
await client.query('COMMIT');
await client.end();

const resolve = (r) => {
    if (r.entry_price > 0 && r.stop_price > 0 && r.notional > 0) {
        const pct = Math.abs(r.entry_price - r.stop_price) / r.entry_price;
        if (pct > MIN_STOP_PCT && pct < MAX_STOP_PCT) return { riskUsd: r.notional * pct, source: 'position_stop' };
    }
    return null;
};
const stats = (rs) => {
    if (!rs.length) return null;
    const sum = rs.reduce((a, b) => a + b, 0), wins = rs.filter((r) => r > 0);
    const losses = rs.filter((r) => r <= 0);
    const mean = (a) => a.length ? a.reduce((x, y) => x + y, 0) / a.length : null;
    return { n: rs.length, avgR: sum / rs.length, win: wins.length / rs.length,
        avgLossR: mean(losses), minR: Math.min(...rs) };
};
const fmt = (s) => s ? `n=${String(s.n).padStart(3)} avgR=${s.avgR.toFixed(3).padStart(7)} win=${(100*s.win).toFixed(1)}% avgLoss=${s.avgLossR?.toFixed(3)} worst=${s.minR.toFixed(2)}R` : 'n=0';

for (const [label, from] of [['all time', 0], ['since 09-11 freeze', Date.parse('2026-09-11T00:00:00Z')]]) {
    const set = rows.filter((r) => Number(r.exit_ts_ms) >= from);
    const oldR = set.filter((r) => r.budget > 0).map((r) => r.pnl_net / r.budget);
    const newR = set.map((r) => ({ r, k: resolve(r) })).filter((x) => x.k).map((x) => x.r.pnl_net / x.k.riskUsd);
    console.log(`\n${label}  (closed=${set.length})`);
    console.log(`  OLD (÷ budget)          ${fmt(stats(oldR))}`);
    console.log(`  NEW (÷ risk taken)      ${fmt(stats(newR))}`);
}
console.log('\nA clean stop-out should read ≈ -1R. That is the check.');
