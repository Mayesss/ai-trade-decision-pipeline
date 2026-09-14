// What does the FMP key actually give us? Few calls, no key in output.
import nextEnv from '@next/env';
const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());
const KEY = (process.env.FMP_API_KEY || '').trim();
if (!KEY) { console.error('no FMP_API_KEY'); process.exit(1); }
console.log(`key present, length ${KEY.length}\n`);

const probes = [
  ['profile (any tier)',            'https://financialmodelingprep.com/api/v3/profile/AAPL'],
  ['daily history (v3)',            'https://financialmodelingprep.com/api/v3/historical-price-full/AAPL?from=2024-01-01&to=2024-03-01'],
  ['S&P500 constituents',           'https://financialmodelingprep.com/api/v3/sp500_constituent'],
  ['stock list (universe)',         'https://financialmodelingprep.com/api/v3/stock/list'],
  ['stable: historical EOD',        'https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&from=2024-01-01&to=2024-03-01'],
  ['4H intraday',                   'https://financialmodelingprep.com/api/v3/historical-chart/4hour/AAPL?from=2024-01-01&to=2024-03-01'],
];

for (const [label, base] of probes) {
  const url = base + (base.includes('?') ? '&' : '?') + 'apikey=' + KEY;
  try {
    const r = await fetch(url);
    const txt = await r.text();
    let note = '';
    try {
      const j = JSON.parse(txt);
      if (Array.isArray(j)) note = `array len=${j.length}` + (j.length ? ` first keys: ${Object.keys(j[0]).slice(0,6).join(',')}` : '');
      else if (j && typeof j === 'object') {
        const k = Object.keys(j);
        note = `object keys: ${k.slice(0,5).join(',')}`;
        if (Array.isArray(j.historical)) note += ` historical len=${j.historical.length}`;
        if (j['Error Message']) note = `ERROR: ${String(j['Error Message']).slice(0,110)}`;
      }
    } catch { note = txt.slice(0, 110); }
    console.log(`${r.status}  ${label.padEnd(26)} ${note}`);
  } catch (e) {
    console.log(`ERR  ${label.padEnd(26)} ${e.message}`);
  }
  await new Promise((r) => setTimeout(r, 400));
}
