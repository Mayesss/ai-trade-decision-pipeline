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
      if (Array.isArray(j)) {
        note = `len=${j.length}`;
        if (j.length) note += `  keys: ${Object.keys(j[0]).slice(0,7).join(',')}`;
        if (j.length && j[0].date) note += `  range ${j[j.length-1].date} .. ${j[0].date}`;
      } else if (j && j['Error Message']) note = 'ERROR: ' + String(j['Error Message']).slice(0,80);
      else note = JSON.stringify(j).slice(0,100);
    } catch { note = t.slice(0,100); }
    console.log(`${String(r.status).padEnd(4)} ${label.padEnd(30)} ${note}`);
  } catch (e) { console.log(`ERR  ${label.padEnd(30)} ${e.message}`); }
  await new Promise((r) => setTimeout(r, 350));
};

console.log('--- universe endpoints ---');
await go('sp500-constituent',   'https://financialmodelingprep.com/stable/sp500-constituent');
await go('nasdaq-constituent',  'https://financialmodelingprep.com/stable/nasdaq-constituent');
await go('stock-list',          'https://financialmodelingprep.com/stable/stock-list');
console.log('\n--- history depth (EOD) ---');
await go('AAPL from 2015',      'https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&from=2015-01-01&to=2026-09-14');
await go('AAPL from 2020',      'https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&from=2020-01-01&to=2026-09-14');
console.log('\n--- intraday ---');
await go('4hour chart',         'https://financialmodelingprep.com/stable/historical-chart/4hour?symbol=AAPL&from=2026-06-01&to=2026-09-14');
await go('1hour chart',         'https://financialmodelingprep.com/stable/historical-chart/1hour?symbol=AAPL&from=2026-08-01&to=2026-09-14');
console.log('\n--- batch / bulk ---');
await go('batch EOD (2 symbols)','https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL,MSFT&from=2026-09-01&to=2026-09-14');
