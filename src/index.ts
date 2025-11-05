// =============================
// wrangler.toml
// =============================
// name = "bitget-ai-decision-worker"
// main = "src/worker.ts"
// compatibility_date = "2025-11-02"
// compatibility_flags = ["nodejs_compat"]
//
// # Bindings
// [vars]
// BITGET_API_KEY = ""           # set in dashboard or with `wrangler secret put`
// BITGET_API_SECRET = ""        # secret
// BITGET_API_PASSPHRASE = ""    # secret
// BITGET_ACCOUNT_TYPE = "usdt-futures" # or "spot" (we default to futures)
// AI_MODEL = "gpt-4o-mini"      # any chat model your provider supports
// AI_BASE_URL = "https://api.openai.com/v1"  # or your proxy/base
//
// [kv_namespaces]
// binding = "DECISIONS"
// id = "<your_kv_namespace_id>"
//
// [[rules]]
// type = "ESModule"
// globs = ["**/*.ts"]
// =============================
// src/worker.ts (patched)
// =============================
export interface Env {
    BITGET_API_KEY: string;
    BITGET_API_SECRET: string;
    BITGET_API_PASSPHRASE: string;
    BITGET_ACCOUNT_TYPE?: 'spot' | 'usdt-futures' | 'usdc-futures' | 'coin-futures';
    AI_MODEL: string;
    AI_BASE_URL: string;
    OPENAI_API_KEY?: string; // put via wrangler secret put OPENAI_API_KEY
    DECISIONS: KVNamespace;
    TRADE_WINDOW_MINUTES: Number;
    // --- CoinDesk News ---
    COINDESK_API_KEY?: string;
    COINDESK_API_BASE?: string;
    COINDESK_NEWS_LIST_PATH?: string;
    COINDESK_NEWS_SINGLE_PATH?: string;
    // News defaults
    NEWS_LOOKBACK_HOURS?: string; // e.g. "12"
    NEWS_MAX_ARTICLES?: string; // e.g. "25"
}

// ---- Utility: Bitget signing (HMAC-SHA256 + base64) ----
// Docs: https://www.bitget.com/api-doc/common/signature
// For v2: sign = base64( HMAC_SHA256(secret, timestamp + method + path + (query?"?"+query:"") + body) )
// Headers required: ACCESS-KEY, ACCESS-SIGN, ACCESS-PASSPHRASE, ACCESS-TIMESTAMP

function buildQuery(params: Record<string, string | number | undefined>) {
    return Object.entries(params)
        .filter(([, v]) => v !== undefined && v !== '')
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
        .join('&');
}

async function signBitget(env: Env, method: string, path: string, query: string, body: string) {
    const ts = Date.now().toString();
    const prehash = ts + method.toUpperCase() + path + (query ? `?${query}` : '') + body;
    const key = await crypto.subtle.importKey(
        'raw',
        new TextEncoder().encode(env.BITGET_API_SECRET),
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign'],
    );
    const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(prehash));
    const signB64 = btoa(String.fromCharCode(...new Uint8Array(sig)));
    return { ts, signB64 };
}

async function bitgetFetch(
    env: Env,
    method: 'GET' | 'POST',
    path: string,
    params: Record<string, string | number | undefined> = {},
    bodyObj?: unknown,
) {
    const query = buildQuery(params);
    const body = bodyObj ? JSON.stringify(bodyObj) : '';
    const { ts, signB64 } = await signBitget(env, method, path, query, body);
    const url = `https://api.bitget.com${path}${query ? `?${query}` : ''}`;
    const headers: Record<string, string> = {
        'ACCESS-KEY': env.BITGET_API_KEY,
        'ACCESS-SIGN': signB64,
        'ACCESS-PASSPHRASE': env.BITGET_API_PASSPHRASE,
        'ACCESS-TIMESTAMP': ts,
        'Content-Type': 'application/json',
        locale: 'en-US',
    };
    const res = await fetch(url, { method, headers, body: body || undefined });
    const json = await res.json<any>();
    if (!res.ok || json.code !== '00000') {
        throw new Error(`Bitget error ${json.code || res.status}: ${json.msg || res.statusText}`);
    }
    return json.data;
}

// ---- Data builders ----

type ProductType = 'usdt-futures' | 'usdc-futures' | 'coin-futures';

function resolveProductType(env: Env): ProductType {
    const t = (env.BITGET_ACCOUNT_TYPE || 'usdt-futures') as ProductType;
    return t;
}

// Fetch minimal set needed for the prompt
async function fetchMarketBundle(env: Env, symbol: string, bundleTimeFrame: string) {
    const productType = resolveProductType(env);
    const isFutures = productType.endsWith('futures');

    let ticker;
    if (isFutures) {
        ticker = await bitgetFetch(env, 'GET', '/api/v2/mix/market/ticker', { symbol, productType });
    } else {
        const t = await bitgetFetch(env, 'GET', '/api/v2/spot/market/tickers', { symbol });
        ticker = Array.isArray(t) ? t[0] : t;
    }

    // Candles for 15m, last 30
    const candles = isFutures
        ? await bitgetFetch(env, 'GET', '/api/v2/mix/market/candles', {
              symbol,
              productType,
              granularity: bundleTimeFrame,
              limit: 30,
          })
        : await bitgetFetch(env, 'GET', '/api/v2/spot/market/candles', {
              symbol,
              granularity: bundleTimeFrame,
              limit: 30,
          });

    // Recent trades for CVD & order flow (last 200)
    const minutes = Number(env.TRADE_WINDOW_MINUTES || 30);
    const trades = await fetchTradesForMinutes(env, symbol, productType, minutes);

    // Orderbook (top 100)
    const orderbook = await bitgetFetch(env, 'GET', '/api/v2/spot/market/orderbook', {
        symbol,
        type: 'step0',
        limit: 100,
    });

    // Funding & OI (futures only)
    let funding: any = null,
        oi: any = null;
    if (isFutures) {
        try {
            funding = await bitgetFetch(env, 'GET', '/api/v2/mix/market/current-fund-rate', { symbol, productType });
        } catch {}
        try {
            oi = await bitgetFetch(env, 'GET', '/api/v2/mix/market/open-interest', { symbol, productType });
        } catch {}
    }

    return { ticker, candles, trades, orderbook, funding, oi, productType };
}

// Compute volume profile (price buckets) & CVD
function computeAnalytics(bundle: any) {
    // trades format differs; normalize to [{price, size, side, ts}]
    const normTrades = (bundle.trades || []).map((t: any) => ({
        price: parseFloat(t.price || t.fillPrice || t.p || t[1]),
        size: parseFloat(t.size || t.fillQuantity || t.q || t[2]),
        side: (t.side || t.S || t[3] || '').toString().toLowerCase(),
        ts: Number(t.ts || t.tradeTime || t[0] || Date.now()),
    }));

    // Derive buy/sell from side where possible; fallback: if not present, use price vs prev trade
    let lastPrice = normTrades[0]?.price || 0;
    const enriched = normTrades.map((tr: any) => {
        let dir = tr.side;
        if (!dir || dir === '') dir = tr.price >= lastPrice ? 'buy' : 'sell';
        lastPrice = tr.price;
        return { ...tr, dir };
    });

    // CVD
    const cvd = enriched.reduce((acc: number, t: any) => acc + (t.dir === 'buy' ? t.size : -t.size), 0);
    const buys = enriched.filter((t: any) => t.dir === 'buy').reduce((a: number, t: any) => a + t.size, 0);
    const sells = enriched.filter((t: any) => t.dir === 'sell').reduce((a: number, t: any) => a + t.size, 0);

    // Volume profile across bins (~ 50bps bin width around latest price)
    const last = Number(bundle.ticker?.lastPr || bundle.ticker?.last || bundle.ticker?.close || enriched.at(-1)?.price);
    const binPct = 0.005;
    const bins = new Map<number, number>();
    for (const t of enriched) {
        const bin = Math.round((t.price - last) / (last * binPct));
        bins.set(bin, (bins.get(bin) || 0) + t.size);
    }
    const volume_profile = Array.from(bins.entries())
        .sort((a, b) => a[0] - b[0])
        .map(([bin, vol]) => ({
            bin,
            price: last + bin * last * binPct,
            volume: Number(vol.toFixed(6)),
        }));

    // Liquidity map: largest resting sizes at top N levels
    const bids = (bundle.orderbook?.bids || []).map((l: any) => ({
        price: parseFloat(l[0] || l.price),
        size: parseFloat(l[1] || l.size),
    }));
    const asks = (bundle.orderbook?.asks || []).map((l: any) => ({
        price: parseFloat(l[0] || l.price),
        size: parseFloat(l[1] || l.size),
    }));
    const topWalls = {
        bid: bids.sort((a, b) => b.size - a.size).slice(0, 5),
        ask: asks.sort((a, b) => b.size - a.size).slice(0, 5),
    };

    return { cvd, buys, sells, volume_profile, topWalls };
}
// ---- fetch symbol meta ----

interface SymbolMeta {
    symbol: string;
    pricePlace: number;
    volumePlace: number;
    minTradeNum: string;
    sizeMultiplier?: string;
}

async function fetchSymbolMeta(env: Env, symbol: string, productType: ProductType): Promise<SymbolMeta> {
    const pt = (productType as string).toUpperCase();
    const all = await bitgetFetch(env, 'GET', '/api/v2/mix/market/contracts', { productType: pt });
    const meta = (all || []).find((x: any) => x.symbol === symbol);
    if (!meta) throw new Error(`No contract metadata for ${symbol}`);
    return meta;
}

// ---- convert notional ----
function roundToDecimals(value: number, decimals: number): number {
    const factor = Math.pow(10, decimals);
    return Math.floor(value * factor) / factor;
}

async function computeOrderSize(
    env: Env,
    symbol: string,
    notionalUSDT: number,
    productType: ProductType,
): Promise<number> {
    const pt = (productType as string).toUpperCase();

    // 1. Get contract metadata
    const all = await bitgetFetch(env, 'GET', '/api/v2/mix/market/contracts', { productType: pt });
    const meta = (all || []).find((x: any) => x.symbol === symbol);
    if (!meta) throw new Error(`No contract metadata for ${symbol}`);

    // 2. Get last price safely
    const ticker = await bitgetFetch(env, 'GET', '/api/v2/mix/market/ticker', {
        symbol,
        productType: pt,
    });

    // Some APIs return { data: {...} } or [ {...} ]; your bitgetFetch() already extracts data,
    // so we normalize to a single object.
    const t = Array.isArray(ticker) ? ticker[0] : ticker;
    const priceStr = t?.lastPr ?? t?.last ?? t?.close ?? t?.price;
    const price = parseFloat(priceStr);
    if (!isFinite(price) || price <= 0) throw new Error(`Invalid price for ${symbol}: ${priceStr}`);

    // 3. Compute raw size and round
    const rawSize = notionalUSDT / price;
    const decimals = Number(meta.volumePlace ?? 3);
    const minTradeNum = parseFloat(meta.minTradeNum ?? '0');

    const factor = Math.pow(10, decimals);
    const rounded = Math.floor(rawSize * factor) / factor;

    // 4. Enforce min size
    const finalSize = Math.max(rounded, minTradeNum);
    if (!isFinite(finalSize) || finalSize <= 0)
        throw new Error(`Failed to compute valid size (raw=${rawSize}, rounded=${rounded})`);

    return finalSize;
}
// ---- fetch open positions helper (patched) -----

type PositionInfo =
    | { status: 'none' }
    | {
          status: 'open';
          symbol: string;
          holdSide: 'long' | 'short';
          entryPrice: string;
          posMode?: 'one_way_mode' | 'hedge_mode';
          marginCoin?: string;
          available?: string; // size available to close (base units/contracts)
          total?: string; // total position size
      };

async function fetchPositionInfo(env: Env, symbol: string): Promise<PositionInfo> {
    const productType = resolveProductType(env);
    const positions = await bitgetFetch(env, 'GET', '/api/v2/mix/position/all-position', {
        productType,
    });

    const matches = (positions || []).filter((p: any) => p.symbol === symbol);
    if (!matches.length) return { status: 'none' };

    // choose the largest absolute size if multiple entries (hedge mode long/short)
    const chosen = matches
        .slice()
        .sort(
            (a: any, b: any) =>
                Math.abs(parseFloat(b.total || b.available || '0')) -
                Math.abs(parseFloat(a.total || a.available || '0')),
        )[0];

    return {
        status: 'open',
        symbol,
        holdSide: (chosen.holdSide || '').toLowerCase() as 'long' | 'short',
        entryPrice: chosen.openPriceAvg,
        posMode: chosen.posMode,
        marginCoin: chosen.marginCoin,
        available: chosen.available,
        total: chosen.total,
    };
}

// ---- fetch trades helper -----

async function fetchTradesForMinutes(env: Env, symbol: string, productType: ProductType, minutes: number) {
    const trades: any[] = [];
    const cutoff = Date.now() - minutes * 60_000;
    let lastId: string | undefined = undefined;
    const isFutures = productType.endsWith('futures');

    while (true) {
        const params: any = { symbol };
        if (isFutures) params.productType = productType;
        if (lastId) params.after = lastId; // paginate forward

        // Request up to 200 trades
        const batch = isFutures
            ? await bitgetFetch(env, 'GET', '/api/v2/mix/market/fills', params)
            : await bitgetFetch(env, 'GET', '/api/v2/spot/market/fills', params);

        if (!batch.length) break;

        trades.push(...batch);

        // Stop if we reached the time limit
        const lastTradeTs = Number(batch[batch.length - 1].ts || batch[0]?.ts);
        if (lastTradeTs < cutoff) break;

        // Paginate
        lastId = batch[batch.length - 1].tradeId || batch[batch.length - 1].id;
        if (!lastId) break; // safety exit
        if (trades.length > 5000) break; // sanity guard
    }

    // Filter to window exactly
    return trades.filter((t) => Number(t.ts) >= cutoff);
}
// ----- calculate Indicators ------

function computeVWAP(candles: any[]): number {
    let cumPV = 0,
        cumVol = 0;
    for (const c of candles) {
        const high = parseFloat(c[2]),
            low = parseFloat(c[3]),
            close = parseFloat(c[4]);
        const volume = parseFloat(c[5]);
        const typical = (high + low + close) / 3;
        cumPV += typical * volume;
        cumVol += volume;
    }
    return cumVol > 0 ? cumPV / cumVol : 0;
}

function computeRSI(candles: any[], period = 14): number {
    const closes = candles.map((c) => parseFloat(c[4]));
    if (closes.length <= period) return 50;

    let gains = 0,
        losses = 0;
    for (let i = closes.length - period; i < closes.length - 1; i++) {
        const diff = closes[i + 1] - closes[i];
        if (diff > 0) gains += diff;
        else losses -= diff;
    }
    const avgGain = gains / period;
    const avgLoss = losses / period;
    if (avgLoss === 0) return 100;
    const rs = avgGain / avgLoss;
    return 100 - 100 / (1 + rs);
}

function computeEMA(closes: number[], period: number): number[] {
    const k = 2 / (period + 1);
    let ema: number[] = [];
    ema[0] = closes[0];
    for (let i = 1; i < closes.length; i++) {
        ema[i] = closes[i] * k + ema[i - 1] * (1 - k);
    }
    return ema;
}

async function calculateMultiTFIndicators(env: Env, symbol: string): Promise<{ micro: string; macro: string }> {
    const productType = resolveProductType(env);
    const isFutures = productType.endsWith('futures');

    async function fetchCandles(tf: string) {
        return isFutures
            ? await bitgetFetch(env, 'GET', '/api/v2/mix/market/candles', {
                  symbol,
                  productType,
                  granularity: tf,
                  limit: 30,
              })
            : await bitgetFetch(env, 'GET', '/api/v2/spot/market/candles', {
                  symbol,
                  granularity: tf,
                  limit: 30,
              });
    }

    const [microCandles, macroCandles] = await Promise.all([
        fetchCandles('1m'), // micro: short-term (scalp)
        fetchCandles('1H'), // macro: higher trend
    ]);

    const build = (candles: any[]) => {
        const vwap = computeVWAP(candles);
        const rsi = computeRSI(candles);
        const closes = candles.map((c) => parseFloat(c[4]));
        const ema20 = computeEMA(closes, 20);
        const ema50 = computeEMA(closes, 50);
        const trend = ema20.at(-1)! > ema50.at(-1)! ? 'up' : 'down';
        return `VWAP=${vwap.toFixed(2)}, RSI=${rsi.toFixed(1)}, trend=${trend}`;
    };

    return {
        micro: build(microCandles),
        macro: build(macroCandles),
    };
}

// ------------- CoinDesk News API + Sentiment Aggregation -----------------

async function coindeskFetch(env: Env, path: string, query: Record<string, string | number | undefined> = {}) {
    if (!env.COINDESK_API_KEY) throw new Error('Missing COINDESK_API_KEY');
    const qs = Object.entries(query)
        .filter(([, v]) => v !== undefined && v !== '')
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
        .join('&');
    const coindeskBase = (env.COINDESK_API_BASE || 'https://data-api.coindesk.com').replace(/\/+$/, '');
    const url = `${coindeskBase}${path}${qs ? `?${qs}` : ''}`;
    const res = await fetch(url, {
        headers: { Authorization: `Bearer ${env.COINDESK_API_KEY}` },
    });

    if (!res.ok) {
        const text = await res.text().catch(() => '');
        throw new Error(`CoinDesk API error ${res.status}: ${res.statusText} ${text}`);
    }
    return res.json<any>();
}

/** Try to normalize “base asset ticker” from e.g. BTCUSDT → BTC, ETH-PERP → ETH */
function baseFromSymbol(symbol: string): string {
    const s = symbol.toUpperCase();
    // common quote suffixes
    const QUOTES = ['USDT', 'USDC', 'USD', 'BTC', 'ETH', 'EUR', 'PERP'];
    for (const q of QUOTES) {
        if (s.endsWith(q)) return s.slice(0, s.length - q.length).replace(/[-_]/g, '');
    }
    // fallback: strip non-letters at end
    return s.replace(/[^A-Z].*$/, '');
}

type Sentiment = 'NEGATIVE' | 'NEUTRAL' | 'POSITIVE';

interface Article {
    SENTIMENT: Sentiment;
    PUBLISHED_ON: number;
}

interface Payload {
    Data: Article[];
}

function getDominantSentiment(payload: Payload): Sentiment {
    // 1. Sort by newest first
    const sorted = [...payload.Data].sort((a, b) => b.PUBLISHED_ON - a.PUBLISHED_ON);

    // 2. Define scores
    const scores: Record<Sentiment, number> = {
        NEGATIVE: 0,
        NEUTRAL: 0,
        POSITIVE: 0,
    };

    // 3. Assign weights: newest = highest weight
    // Example: 1, 0.9, 0.8, 0.7, ... never below 0.1
    const weightStep = 0.1;

    sorted.forEach((item, index) => {
        const weight = Math.max(1 - index * weightStep, 0.1);
        scores[item.SENTIMENT] += weight;
    });

    // 4. Return sentiment with highest weighted score
    return Object.entries(scores).reduce((a, b) => (b[1] > a[1] ? b : a))[0] as Sentiment;
}

/** Fetch latest news for a base ticker (e.g., BTC) and summarize sentiment. */
async function fetchNewsSentiment(env: Env, symbolOrBase: string) {
    const base = baseFromSymbol(symbolOrBase);
    const listPath = env.COINDESK_NEWS_LIST_PATH || '/news/v1/article/list';

    const query = {
        categories: base,
        limit: 25,
        lang: 'EN',
    };

    let payload: any;
    try {
        payload = await coindeskFetch(env, listPath, query);
    } catch (e) {
        console.log('error fetching Coin Desk news ', e);
    }
    return getDominantSentiment(payload) || 'NEUTRAL';
}

// ---- AI prompt builder ----
function buildPrompt(
    env: Env,
    symbol: string,
    timeframe: string,
    bundle: any,
    analytics: ReturnType<typeof computeAnalytics>,
    position_status: string = 'none',
    news_sentiment: string = 'neutral',
    indicators: { micro: string; macro: string },
    lastDecision: TradeDecision | null,
) {
    const price = bundle.ticker[0]?.lastPr || bundle.ticker[0]?.last || bundle.ticker[0]?.close;
    const change = bundle.ticker[0]?.change24h || bundle.ticker[0]?.changeUtc24h || bundle.ticker[0]?.chgPct;
    const market_data = `price=${price}, change24h=${change}`;
    const order_flow = `buys=${analytics.buys.toFixed(3)}, sells=${analytics.sells.toFixed(
        3,
    )}, CVD=${analytics.cvd.toFixed(3)}`;
    const liquidity_data = `top bid walls: ${JSON.stringify(analytics.topWalls.bid)}, top ask walls: ${JSON.stringify(
        analytics.topWalls.ask,
    )}`;
    const derivatives = bundle.productType
        ? `funding=${bundle.funding?.[0]?.fundingRate ?? 'n/a'}, openInterest=${
              bundle.oi?.openInterestList?.[0]?.size ?? 'n/a'
          }`
        : 'n/a';

    const vol_profile_str = analytics.volume_profile
        .slice(0, 10)
        .map((v) => `(${v.price.toFixed(2)} → ${v.volume})`)
        .join(', ');

    const sys = `You are an expert quantitative crypto market analyst. Output JSON only.`;

    const user = `Assess short-term direction for ${symbol} based on inputs and constraints.
DATA INPUTS:
- Current price and % change: ${market_data}
- Volume profile (fixed lookback window: ${env.TRADE_WINDOW_MINUTES || 30}m): ${vol_profile_str}
- Order flow summary (buy/sell imbalance, CVD): ${order_flow}
- Order book & liquidity map (visible walls): ${liquidity_data}
- Funding rate, open interest, and liquidation data: ${derivatives}
- Market sentiment: ${news_sentiment.toLowerCase()}
- Current position: ${position_status}
- Technical indicators (short-term): ${indicators.micro}
- Macro indicators (1h context): ${indicators.macro}
- Last AI action: ${
        lastDecision ? `${lastDecision.action} (${new Date(lastDecision?.timestamp!).toLocaleString()})` : 'None'
    }


TASK:
1. Analyze whether current conditions favor short-term long, short, or no trade.
2. If in a position, decide whether to stay in, scale out, or close.
3. Explain reasoning briefly (price action, volume delta, liquidity shifts, or sentiment change).
4. Output one action from: BUY | SELL | HOLD | CLOSE.
5. Include 1–2 line market context.

Constraints:
- Time horizon = ${timeframe}
- Do not make predictions beyond 1 hour.
- Assume educational simulation, not live trading.
- Return JSON strictly as: {"action":"BUY|SELL|HOLD|CLOSE","summary":"...","reason":"..."}
- If a position is open, only options are HOLD|CLOSE.
`;
    return { system: sys, user };
}

// ---- AI call ----
async function callAI(env: Env, system: string, user: string) {
    const apiKey = env.OPENAI_API_KEY;
    const base = env.AI_BASE_URL;
    const model = env.AI_MODEL;

    const res = await fetch(`${base}/chat/completions`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
            model,
            messages: [
                { role: 'system', content: system },
                { role: 'user', content: user },
            ],
            temperature: 0.2,
            response_format: { type: 'json_object' },
        }),
    });
    if (!res.ok) throw new Error(`AI error: ${res.status} ${res.statusText}`);
    const data = await res.json<any>();
    const text = data.choices?.[0]?.message?.content || '{}';
    return JSON.parse(text);
}

// ---- Trade executor (patched) ----
interface TradeDecision {
    action: 'BUY' | 'SELL' | 'HOLD' | 'CLOSE';
    summary: string;
    reason: string;
}

async function flashClosePosition(env: Env, symbol: string, productType: ProductType, holdSide?: 'long' | 'short') {
    const body: any = {
        productType,
        symbol,
    };
    if (holdSide) body.holdSide = holdSide; // required in hedge mode, ignored in one-way
    const res = await bitgetFetch(env, 'POST', '/api/v2/mix/order/close-positions', {}, body);
    return res; // typically includes successList / failureList
}

async function executeDecision(
    env: Env,
    symbol: string,
    sideSizeUSDT: number,
    decision: TradeDecision,
    productType: ProductType,
    dryRun = true,
) {
    const clientOid = `cfw-${crypto.randomUUID()}`;

    // Map decision to Bitget order API (market order, small notional)
    if (decision.action === 'BUY' || decision.action === 'SELL') {
        if (dryRun) return { placed: false, orderId: null, clientOid };
        const size = await computeOrderSize(env, symbol, sideSizeUSDT, productType);

        // NOTE: Bitget expects size in contracts/base units, not USDT notional.
        // You may need to convert notional -> size based on instrument spec. Placeholder uses "sideSizeUSDT" directly.
        const body = {
            symbol,
            productType,
            marginCoin: 'USDT',
            marginMode: 'isolated',
            side: decision.action.toLowerCase(), // 'buy' | 'sell'
            orderType: 'market',
            size: size.toString(), // rounded base amount,
            clientOid,
            force: 'gtc',
        };
        const res = await bitgetFetch(env, 'POST', '/api/v2/mix/order/place-order', {}, body);
        return { placed: true, orderId: res?.orderId || res?.order_id || null, clientOid };
    }

    if (decision.action === 'CLOSE') {
        if (dryRun) return { placed: false, orderId: null, clientOid, closed: true };

        // fetch fresh position info to know posMode / side
        const pos = await fetchPositionInfo(env, symbol);
        if (pos.status === 'none') {
            return { placed: false, orderId: null, clientOid, closed: false, note: 'no open position' };
        }

        const isHedge = pos.posMode === 'hedge_mode';
        const res = await flashClosePosition(env, symbol, productType, isHedge ? pos.holdSide : undefined);
        const ok = Array.isArray(res?.successList) && res.successList.length > 0;
        const orderId = ok ? res.successList[0]?.orderId ?? null : null;
        return { placed: ok, orderId, clientOid, closed: ok, raw: res };
    }

    // HOLD
    return { placed: false, orderId: null, clientOid };
}

// ---- KV persistence ----
async function saveDecision(kv: KVNamespace, keyBase: string, payload: any) {
    const key = `${keyBase}:${new Date().toISOString()}`;
    await kv.put(key, JSON.stringify(payload, null, 2));
    return key;
}

async function loadLastDecision(kv: KVNamespace, symbol: string) {
    const prefix = `${symbol}:`;

    // Grab up to last 50 (safe + cheap)
    const list = await kv.list({ prefix });
    if (!list.keys.length) return null;

    // Sort by timestamp descending
    const sorted = list.keys.sort((a, b) => b.name.localeCompare(a.name));

    const key = sorted[0].name;
    const timestampStr = key.slice(key.indexOf(':') + 1);
    const timestamp = Date.parse(timestampStr);

    const stored = await kv.get(key, 'json');
    return stored?.decision
        ? {
              ...stored.decision,
              entryPrice: stored.execRes?.entryPrice,
              timestamp: isNaN(timestamp) ? undefined : timestamp,
          }
        : null;
}

// ---- Router ----
export default {
    async fetch(req: Request, env: Env): Promise<Response> {
        try {
            const url = new URL(req.url);
            if (url.pathname === '/health') return new Response('ok');

            if (url.pathname === '/reset' && req.method === 'POST') {
                const body = await req.json<any>().catch(() => ({}));
                const symbolFilter = body.symbol || null; // optional filter by symbol

                const list = await env.DECISIONS.list();
                let deleted = 0;

                for (const key of list.keys) {
                    if (!symbolFilter || key.name.startsWith(`${symbolFilter}:`)) {
                        await env.DECISIONS.delete(key.name);
                        deleted++;
                    }
                }

                return Response.json({ deleted, filter: symbolFilter || 'all' });
            }

            if (url.pathname === '/analyze' && req.method === 'POST') {
                const body = await req.json<any>().catch(() => ({}));
                const symbol = (body.symbol as string) || 'ETHUSDT';
                const timeFrame = body.timeFrame || '15m';
                const dryRun = body.dryRun !== false; // default true
                const sideSizeUSDT = Number(body.notional || 10);

                // 1) Fetch & compute
                const productType = resolveProductType(env);
                const positionInfo = await fetchPositionInfo(env, symbol);
                const positionForPrompt =
                    positionInfo.status === 'open'
                        ? `${positionInfo.holdSide}, entryPrice: ${positionInfo.entryPrice}`
                        : 'none';

                const news = await fetchNewsSentiment(env, symbol);
                const bundle = await fetchMarketBundle(env, symbol, timeFrame);
                const analytics = computeAnalytics(bundle);
                const indicators = await calculateMultiTFIndicators(env, symbol);
                const lastDecision = await loadLastDecision(env.DECISIONS, symbol);

                // 2) AI decision
                const { system, user } = buildPrompt(
                    env,
                    symbol,
                    timeFrame,
                    bundle,
                    analytics,
                    positionForPrompt,
                    news,
                    indicators,
                    lastDecision,
                );
                const decision: TradeDecision = await callAI(env, system, user);
                console.log(user);

                // 3) Execute (optional)
                const execRes = await executeDecision(env, symbol, sideSizeUSDT, decision, productType, dryRun);

                // 4) Persist
                const saveKey = await saveDecision(env.DECISIONS, symbol, {
                    decision,
                    bundleMeta: { productType: bundle.productType },
                    analytics,
                    execRes,
                    lastDecision,
                });

                return Response.json({
                    symbol,
                    timeFrame,
                    decision,
                    execRes,
                    kvKey: saveKey,
                });
            }

            if (url.pathname === '/debug-env') {
                return Response.json({
                    BITGET_API_KEY: env.BITGET_API_KEY ? '✅ set' : '❌ missing',
                    BITGET_API_SECRET: env.BITGET_API_SECRET ? '✅ set' : '❌ missing',
                    BITGET_API_PASSPHRASE: env.BITGET_API_PASSPHRASE ? '✅ set' : '❌ missing',
                    OPENAI_API_KEY: env.OPENAI_API_KEY ? '✅ set' : '❌ missing',
                    COINDESK_API_KEY: env.COINDESK_API_KEY ? '✅ set' : '❌ missing',
                });
            }

            if (url.pathname === '/history' && req.method === 'GET') {
                return new Response('KV listing requires account-side listing APIs; add Durable Object for paging.');
            }

            if (url.pathname === '/lastDecision' && req.method === 'GET') {
                const symbol = (url.searchParams.get('symbol') as string) || 'ETHUSDT';
                return Response.json(await loadLastDecision(env.DECISIONS, symbol));
            }

            if (url.pathname === '/' && req.method === 'GET') {
                return new Response('AI Trade Decision Worker running ✅');
            }
            return new Response('Not found', { status: 404 });
        } catch (err: any) {
            return new Response(JSON.stringify({ error: err.message || String(err) }, null, 2), {
                status: 500,
                headers: { 'Content-Type': 'application/json' },
            });
        }
    },
};

// =============================
// Notes
// - Endpoints used (as of Nov 2, 2025):
//   * Futures ticker: /api/v2/mix/market/ticker
//   * Funding rate: /api/v2/mix/market/current-fund-rate
//   * Open interest: /api/v2/mix/market/open-interest
//   * Futures trades: /api/v2/mix/market/fills
//   * Futures candles: /api/v2/mix/market/candles
//   * Spot candles: /api/v2/spot/market/candles
//   * Spot orderbook (used for liquidity map): /api/v2/spot/market/orderbook
//   * Spot trades: /api/v2/spot/market/fills
//   * Close positions (flash): /api/v2/mix/order/close-positions
// - Action mapping intentionally conservative; CLOSE/HOLD avoid live calls unless explicitly enabled with dryRun=false.
// - Everything defaults to dryRun to prevent unintended live trades. Flip `dryRun=false` per request to enable.
// - Reminder: for live BUY/SELL, convert notional -> contract/base size per instrument spec.
