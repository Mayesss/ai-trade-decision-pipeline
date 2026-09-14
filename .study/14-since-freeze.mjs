// Has anything actually improved since the 2026-09-11 freeze?
// Corrected R denominator (fill notional x stop distance).
import pg from 'pg';
import nextEnv from '@next/env';
const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());
const c = new pg.Client({ connectionString: process.env.NEON__DATABASE_URL_UNPOOLED, ssl: { rejectUnauthorized: false } });
await c.connect(); await c.query('BEGIN READ ONLY');
const { rows } = await c.query(`
  SELECT p.exit_ts_ms, p.pnl_net, p.notional, p.entry_price,
         CASE WHEN jsonb_typeof(d.ai_decision_json->'stop_loss_price')='number'
              THEN (d.ai_decision_json->>'stop_loss_price')::float8 END AS stop
  FROM swing.positions p LEFT JOIN swing.decisions d ON d.id=p.decision_id
  WHERE p.status='closed' AND p.pnl_net IS NOT NULL AND p.exit_ts_ms IS NOT NULL
  ORDER BY p.exit_ts_ms`);
await c.query('COMMIT'); await c.end();

const R = (r) => {
  if (!(r.entry_price > 0 && r.stop > 0 && r.notional > 0)) return null;
  const pct = Math.abs(r.entry_price - r.stop) / r.entry_price;
  if (!(pct > 0.0005 && pct < 1)) return null;
  return r.pnl_net / (r.notional * pct);
};
const mean = (a) => a.reduce((x,y)=>x+y,0)/a.length;
const sd = (a) => { const m = mean(a); return Math.sqrt(a.reduce((s,x)=>s+(x-m)**2,0)/(a.length-1)); };
const FREEZE = Date.parse('2026-09-11T00:00:00Z');
const L3 = Date.parse('2026-09-12T00:00:00Z');

const tag = (label, set) => {
  const rs = set.map(R).filter((x)=>x!=null);
  if (rs.length < 2) { console.log(`${label.padEnd(26)} n=${rs.length} -- untestable`); return null; }
  const m = mean(rs), s = sd(rs), se = s/Math.sqrt(rs.length), t = m/se;
  console.log(`${label.padEnd(26)} n=${String(rs.length).padStart(3)}  meanR=${m.toFixed(3).padStart(7)}  sd=${s.toFixed(2)}  t=${t.toFixed(2).padStart(5)}  95%CI [${(m-1.96*se).toFixed(2)}, ${(m+1.96*se).toFixed(2)}]  wins=${(100*rs.filter(x=>x>0).length/rs.length).toFixed(0)}%`);
  return { m, s, n: rs.length, se };
};
console.log('REALISED R, corrected denominator\n');
const pre = tag('before freeze', rows.filter(r=>Number(r.exit_ts_ms) < FREEZE));
const post = tag('since freeze (09-11)', rows.filter(r=>Number(r.exit_ts_ms) >= FREEZE));
const last3 = tag('last 3 days (09-12..14)', rows.filter(r=>Number(r.exit_ts_ms) >= L3));

if (pre && post) {
  const diff = post.m - pre.m, se = Math.sqrt(pre.se**2 + post.se**2);
  console.log(`\ndifference (since freeze - before): ${diff.toFixed(3)}R  t=${(diff/se).toFixed(2)}  -> ${Math.abs(diff/se) > 1.96 ? 'significant' : 'NOT distinguishable from zero'}`);
}

// How surprising is a run of good days on this few trades?
console.log('\nHow much evidence is "better lately"?');
if (last3) {
  const need = (target, sdv) => Math.ceil(2 * ((2.8 * sdv) / target) ** 2);
  console.log(`  last-3-day sample is n=${last3.n}. To show a +0.20R improvement over the prior mean`);
  console.log(`  at 80% power you would need ~${need(0.20, pre ? pre.s : 0.65)} trades PER ARM (sd=${(pre?pre.s:0.65).toFixed(2)}).`);
  console.log(`  At the current ~2 closes/day that is ~${Math.ceil(need(0.20, pre?pre.s:0.65)/2)} days per arm.`);
}
