// BTC regime context for non-BTC crypto ticks: the roster's alts trade at
// 0.65–0.94 daily-return correlation to BTC (measured Jul 2026), so an alt
// tick that can't see BTC is blind to the dominant driver of its own next
// move. This block feeds the MEASURED coupling (rolling correlation/beta),
// BTC's own recent state, and the alt's idiosyncratic residual — the model
// does the reasoning (no bias/strength verdicts, per the prompt philosophy).
// Pure compute functions, tolerant of Bitget candle shapes ([ts,o,h,l,c,...]
// arrays with string values); the loader fails open to null so an exchange
// hiccup just omits the prompt block.

import { bitgetFetch, resolveProductType } from '../bitget';

export type BtcContext = {
  reference: 'BTCUSDT';
  // Pearson correlation of daily returns over the trailing window.
  corr_30d: number | null;
  corr_90d: number | null;
  // OLS beta of alt daily returns on BTC daily returns (90d window).
  beta_90d: number | null;
  // BTC's own recent moves, close-to-close from hourly bars, basis points.
  btc: {
    ret_1h_bp: number | null;
    ret_4h_bp: number | null;
    ret_24h_bp: number | null;
    ret_7d_bp: number | null;
  };
  // Alt 7d return minus beta_90d x BTC 7d return: the idiosyncratic component.
  // Positive = alt stronger than its BTC coupling implies.
  alt_vs_btc_residual_7d_bp: number | null;
};

export function swingBtcContextEnabled(): boolean {
  const raw = String(process.env.SWING_BTC_CONTEXT_ENABLED ?? '')
    .trim()
    .toLowerCase();
  if (['false', '0', 'no', 'off'].includes(raw)) return false;
  return true;
}

type CloseRow = { ts: number; close: number };

function normalizeCloses(raw: unknown): CloseRow[] {
  return (Array.isArray(raw) ? raw : [])
    .map((c: any): CloseRow | null => {
      const tsRaw = Number(Array.isArray(c) ? c[0] : (c?.ts ?? c?.timestamp ?? c?.time));
      const close = Number(Array.isArray(c) ? c[4] : c?.close);
      if (![tsRaw, close].every(Number.isFinite) || close <= 0) return null;
      const ts = tsRaw > 1e12 ? tsRaw : tsRaw * 1000;
      return { ts, close };
    })
    .filter((r): r is CloseRow => r !== null)
    .sort((a, b) => a.ts - b.ts);
}

function round1(x: number): number {
  return Number(x.toFixed(1));
}

function round2(x: number): number {
  return Number(x.toFixed(2));
}

// Daily returns over the last `days` bars of the timestamp-joined series.
// Joining by ts guards against one leg missing a bar (delisting gap, venue
// hiccup) which would otherwise shift the pairing and corrupt the correlation.
function joinedReturns(alt: CloseRow[], btc: CloseRow[], days: number): Array<[number, number]> {
  const btcByTs = new Map(btc.map((r) => [r.ts, r.close]));
  const common = alt.filter((r) => btcByTs.has(r.ts)).slice(-(days + 1));
  const out: Array<[number, number]> = [];
  for (let i = 1; i < common.length; i++) {
    const a = common[i].close / common[i - 1].close - 1;
    const b = (btcByTs.get(common[i].ts) as number) / (btcByTs.get(common[i - 1].ts) as number) - 1;
    out.push([a, b]);
  }
  return out;
}

// Pearson correlation and OLS beta (alt on btc) from paired returns.
// Requires most of the window to be present so a freshly listed alt reports
// null instead of a correlation estimated from a handful of days.
function corrBeta(pairs: Array<[number, number]>, minN: number): { corr: number | null; beta: number | null } {
  const n = pairs.length;
  if (n < minN) return { corr: null, beta: null };
  const meanA = pairs.reduce((s, p) => s + p[0], 0) / n;
  const meanB = pairs.reduce((s, p) => s + p[1], 0) / n;
  let cov = 0;
  let varA = 0;
  let varB = 0;
  for (const [a, b] of pairs) {
    cov += (a - meanA) * (b - meanB);
    varA += (a - meanA) ** 2;
    varB += (b - meanB) ** 2;
  }
  if (varA <= 0 || varB <= 0) return { corr: null, beta: null };
  return { corr: round2(cov / Math.sqrt(varA * varB)), beta: round2(cov / varB) };
}

// Close-to-close return `bars` back from the latest bar, in bp. The latest
// bar may be in progress — that is intentional (current price vs k ago).
function retBp(rows: CloseRow[], bars: number): number | null {
  if (rows.length < bars + 1) return null;
  const last = rows[rows.length - 1].close;
  const past = rows[rows.length - 1 - bars].close;
  return round1((last / past - 1) * 1e4);
}

export function computeBtcContext(params: {
  altDaily: unknown;
  btcDaily: unknown;
  btcHourly: unknown;
}): BtcContext | null {
  const altD = normalizeCloses(params.altDaily);
  const btcD = normalizeCloses(params.btcDaily);
  const btcH = normalizeCloses(params.btcHourly);

  const c30 = corrBeta(joinedReturns(altD, btcD, 30), 25);
  const c90 = corrBeta(joinedReturns(altD, btcD, 90), 75);

  const btcRets = {
    ret_1h_bp: retBp(btcH, 1),
    ret_4h_bp: retBp(btcH, 4),
    ret_24h_bp: retBp(btcH, 24),
    ret_7d_bp: retBp(btcH, 168),
  };

  const altRet7d = retBp(altD, 7);
  const btcRet7d = retBp(btcD, 7);
  const residual =
    altRet7d !== null && btcRet7d !== null && c90.beta !== null
      ? round1(altRet7d - c90.beta * btcRet7d)
      : null;

  const hasAny =
    c30.corr !== null || c90.corr !== null || Object.values(btcRets).some((v) => v !== null);
  if (!hasAny) return null;

  return {
    reference: 'BTCUSDT',
    corr_30d: c30.corr,
    corr_90d: c90.corr,
    beta_90d: c90.beta,
    btc: btcRets,
    alt_vs_btc_residual_7d_bp: residual,
  };
}

// Entry point for /api/analyze. Three public candle calls in parallel
// (~alt 1D, BTC 1D, BTC 1H); any failure or a BTC tick returns null and the
// prompt block is simply absent.
export async function loadBtcContext(symbol: string): Promise<BtcContext | null> {
  if (!swingBtcContextEnabled()) return null;
  if (String(symbol).toUpperCase() === 'BTCUSDT') return null;
  try {
    const productType = resolveProductType();
    const candles = (sym: string, granularity: string, limit: number) =>
      bitgetFetch('GET', '/api/v2/mix/market/candles', { symbol: sym, productType, granularity, limit });
    const [altDaily, btcDaily, btcHourly] = await Promise.all([
      candles(symbol, '1D', 95),
      candles('BTCUSDT', '1D', 95),
      candles('BTCUSDT', '1H', 172),
    ]);
    return computeBtcContext({ altDaily, btcDaily, btcHourly });
  } catch (err) {
    console.warn(`Could not build BTC context for ${symbol}:`, err);
    return null;
  }
}
