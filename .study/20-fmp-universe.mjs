import nextEnv from '@next/env';
const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());
const KEY = (process.env.FMP_API_KEY || '').trim();
const go = async (label, base) => {
  const url = base + (base.includes('?') ? '&' : '?') + 'apikey=' + KEY;
  try {
    const r = await fetch(url); const t = await r.text();
    let note = '';
    try { const j = JSON.parse(t);
      if (Array.isArray(j)) { note = `len=${j.length}`; if (j.length) note += `  keys: ${Object.keys(j[0]).slice(0,8).join(',')}`; }
      else if (j && j['Error Message']) note = 'RESTRICTED';
      else note = JSON.stringify(j).slice(0,90);
    } catch { note = t.slice(0,90); }
    console.log(`${String(r.status).padEnd(4)} ${label.padEnd(28)} ${note}`);
    return r.status === 200 ? JSON.parse(t) : null;
  } catch (e) { console.log(`ERR  ${label.padEnd(28)} ${e.message}`); return null; }
};
const scr = await go('company-screener', 'https://financialmodelingprep.com/stable/company-screener?marketCapMoreThan=10000000000&exchange=NASDAQ,NYSE&isActivelyTrading=true&limit=50');
await new Promise((r)=>setTimeout(r,350));
await go('actively-trading-list', 'https://financialmodelingprep.com/stable/actively-trading-list');
await new Promise((r)=>setTimeout(r,350));
await go('delisted-companies',   'https://financialmodelingprep.com/stable/delisted-companies?page=0');
if (scr && scr.length) console.log('\nscreener sample:', scr.slice(0,5).map((x)=>x.symbol).join(', '));
