import nextEnv from '@next/env';
import { bitgetFetch, resolveProductType } from '../lib/bitget';
const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());
async function main() {
  const rows: Array<Record<string, unknown>> = await bitgetFetch('GET', '/api/v2/mix/market/contracts', { productType: resolveProductType() });
  const usdt = rows.filter((r) => String(r.symbol).endsWith('USDT'));
  console.log(`Bitget USDT perpetuals available: ${usdt.length}`);
  const tickers: Array<Record<string, unknown>> = await bitgetFetch('GET', '/api/v2/mix/market/tickers', { productType: resolveProductType() });
  const byVol = tickers
    .filter((t) => String(t.symbol).endsWith('USDT'))
    .map((t) => ({ s: String(t.symbol), v: Number(t.usdtVolume ?? t.quoteVolume ?? 0) }))
    .sort((a, b) => b.v - a.v);
  console.log(`with 24h volume data: ${byVol.length}`);
  for (const n of [25, 50, 100, 200]) {
    const cut = byVol[n - 1];
    if (cut) console.log(`  top ${String(n).padStart(3)} by 24h volume -> smallest is ${cut.s} at $${(cut.v/1e6).toFixed(1)}M`);
  }
  console.log('\ncurrently traded (10):', ['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','LINKUSDT','BGBUSDT','ADAUSDT','AVAXUSDT','BNBUSDT','DOGEUSDT'].join(' '));
  console.log('top 30 by volume:', byVol.slice(0, 30).map((x) => x.s.replace('USDT','')).join(' '));
}
main().catch((e) => console.error(e.message));
