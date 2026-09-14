// Is R actually measuring risk? A clean stop-out should be about -1R.
import { readFileSync } from 'node:fs';
const trades = JSON.parse(readFileSync('.study/trades.json', 'utf8'));
const m = trades.filter((t) => t.risk_usd > 0 && t.pnl_net != null).map((t) => ({ ...t, R: t.pnl_net / t.risk_usd }));
const q = (a, p) => { const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor(p * s.length))]; };
const losers = m.filter((t) => t.R <= 0).map((t) => t.R);
const winners = m.filter((t) => t.R > 0).map((t) => t.R);
console.log(`losers  n=${losers.length}  R: p10=${q(losers,0.1).toFixed(2)} median=${q(losers,0.5).toFixed(2)} p90=${q(losers,0.9).toFixed(2)}  min=${Math.min(...losers).toFixed(2)}`);
console.log(`winners n=${winners.length}  R: p10=${q(winners,0.1).toFixed(2)} median=${q(winners,0.5).toFixed(2)} p90=${q(winners,0.9).toFixed(2)}  max=${Math.max(...winners).toFixed(2)}`);
console.log(`\nshare of losers worse than -0.8R: ${(100 * losers.filter((r) => r < -0.8).length / losers.length).toFixed(1)}%`);
console.log(`share of losers between -0.2R and 0: ${(100 * losers.filter((r) => r > -0.2).length / losers.length).toFixed(1)}%`);
console.log(`median risk_usd budgeted: ${q(m.map((t) => t.risk_usd), 0.5).toFixed(2)}  median |pnl_net|: ${q(m.map((t) => Math.abs(t.pnl_net)), 0.5).toFixed(2)}`);
console.log(`median |pnl_pct| on the position: ${q(m.filter((t)=>t.pnl_pct!=null).map((t) => Math.abs(t.pnl_pct)), 0.5).toFixed(3)}%`);
console.log('\n=> if stop-outs land near -0.2R instead of -1R, risk_usd overstates the risk actually taken,');
console.log('   and "mean R" is compressed toward 0 -- which shrinks BOTH the estimate and its spread.');
