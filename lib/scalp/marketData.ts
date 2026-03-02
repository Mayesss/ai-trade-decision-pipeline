import { fetchCapitalCandlesByEpic, fetchCapitalLivePrice, resolveCapitalEpicRuntime } from '../capital';
import type { ScalpBaseTimeframe, ScalpCandle, ScalpConfirmTimeframe, ScalpMarketSnapshot, ScalpSessionWindows } from './types';

function toFinite(value: unknown, fallback = NaN): number {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
}

export function pipSizeForScalpSymbol(symbol: string): number {
    const upper = String(symbol || '').toUpperCase();
    if (upper.includes('JPY')) return 0.01;
    return 0.0001;
}

export function timeframeMinutes(tf: ScalpBaseTimeframe | ScalpConfirmTimeframe): number {
    if (tf === 'M1') return 1;
    if (tf === 'M3') return 3;
    if (tf === 'M5') return 5;
    return 15;
}

function toCapitalTfSpec(tf: ScalpBaseTimeframe | ScalpConfirmTimeframe): { apiTf: string; sourceMinutes: number } {
    if (tf === 'M1') return { apiTf: '1m', sourceMinutes: 1 };
    if (tf === 'M3') {
        // Capital often rejects native 3m resolution; pull 1m and aggregate locally.
        return { apiTf: '1m', sourceMinutes: 1 };
    }
    if (tf === 'M5') return { apiTf: '5m', sourceMinutes: 5 };
    return { apiTf: '15m', sourceMinutes: 15 };
}

function aggregateCandles(candles: ScalpCandle[], tfMinutes: number): ScalpCandle[] {
    const tfMs = Math.max(1, Math.floor(tfMinutes)) * 60_000;
    const buckets = new Map<number, ScalpCandle[]>();
    for (const candle of candles) {
        const start = Math.floor(candle[0] / tfMs) * tfMs;
        if (!buckets.has(start)) buckets.set(start, []);
        buckets.get(start)!.push(candle);
    }
    const out: ScalpCandle[] = [];
    const keys = Array.from(buckets.keys()).sort((a, b) => a - b);
    for (const key of keys) {
        const rows = (buckets.get(key) || []).slice().sort((a, b) => a[0] - b[0]);
        if (!rows.length) continue;
        const first = rows[0]!;
        const last = rows[rows.length - 1]!;
        const high = Math.max(...rows.map((r) => r[2]));
        const low = Math.min(...rows.map((r) => r[3]));
        const volume = rows.reduce((acc, row) => acc + Number(row[5] || 0), 0);
        out.push([key, first[1], high, low, last[4], volume]);
    }
    return out;
}

function normalizeCandle(row: any): ScalpCandle | null {
    const ts = toFinite(row?.[0]);
    const open = toFinite(row?.[1]);
    const high = toFinite(row?.[2]);
    const low = toFinite(row?.[3]);
    const close = toFinite(row?.[4]);
    const volume = toFinite(row?.[5], 0);
    if (!(Number.isFinite(ts) && ts > 0)) return null;
    if (![open, high, low, close].every((v) => Number.isFinite(v) && v > 0)) return null;
    return [ts, open, high, low, close, volume];
}

function sortCandles(candles: ScalpCandle[]): ScalpCandle[] {
    return candles.slice().sort((a, b) => a[0] - b[0]);
}

function onlyClosedCandles(candles: ScalpCandle[], tfMinutes: number, nowMs: number): ScalpCandle[] {
    const closeMs = Math.max(1, Math.floor(tfMinutes)) * 60_000;
    return candles.filter((c) => c[0] + closeMs <= nowMs);
}

function estimateLimit(params: {
    nowMs: number;
    startMs: number;
    tfMinutes: number;
    minCandles: number;
    maxCandles: number;
}): number {
    const spanMs = Math.max(0, params.nowMs - params.startMs);
    const bars = Math.ceil(spanMs / (params.tfMinutes * 60_000));
    return Math.max(params.minCandles, Math.min(params.maxCandles, bars + 40));
}

export async function loadScalpMarketSnapshot(params: {
    symbol: string;
    nowMs: number;
    windows: ScalpSessionWindows;
    baseTf: ScalpBaseTimeframe;
    confirmTf: ScalpConfirmTimeframe;
    minBaseCandles: number;
    minConfirmCandles: number;
    maxCandlesPerRequest: number;
}): Promise<ScalpMarketSnapshot> {
    const symbol = String(params.symbol || '').trim().toUpperCase();
    const resolved = await resolveCapitalEpicRuntime(symbol);
    const baseMinutes = timeframeMinutes(params.baseTf);
    const confirmMinutes = timeframeMinutes(params.confirmTf);
    const lookbackStartMs = Math.min(params.windows.asiaStartMs, params.windows.raidStartMs) - 60 * 60 * 1000;

    const baseLimit = estimateLimit({
        nowMs: params.nowMs,
        startMs: lookbackStartMs,
        tfMinutes: baseMinutes,
        minCandles: params.minBaseCandles,
        maxCandles: params.maxCandlesPerRequest,
    });
    const confirmLimit = estimateLimit({
        nowMs: params.nowMs,
        startMs: lookbackStartMs,
        tfMinutes: confirmMinutes,
        minCandles: params.minConfirmCandles,
        maxCandles: params.maxCandlesPerRequest,
    });

    const baseTfSpec = toCapitalTfSpec(params.baseTf);
    const confirmTfSpec = toCapitalTfSpec(params.confirmTf);
    const baseTfApi = baseTfSpec.apiTf;
    const confirmTfApi = confirmTfSpec.apiTf;

    const [quote, baseRaw, confirmRaw] =
        baseTfApi === confirmTfApi
            ? await Promise.all([
                  fetchCapitalLivePrice(symbol),
                  fetchCapitalCandlesByEpic(resolved.epic, baseTfApi, Math.max(baseLimit, confirmLimit)),
                  Promise.resolve<any[]>([]),
              ])
            : await Promise.all([
                  fetchCapitalLivePrice(symbol),
                  fetchCapitalCandlesByEpic(resolved.epic, baseTfApi, baseLimit),
                  fetchCapitalCandlesByEpic(resolved.epic, confirmTfApi, confirmLimit),
              ]);

    const baseCandlesFull = sortCandles(baseRaw.map(normalizeCandle).filter((c): c is ScalpCandle => Boolean(c)));
    const confirmCandlesFull =
        baseTfApi === confirmTfApi
            ? baseCandlesFull.slice()
            : sortCandles(confirmRaw.map(normalizeCandle).filter((c): c is ScalpCandle => Boolean(c)));

    const baseCandlesPreClose =
        baseTfSpec.sourceMinutes === baseMinutes ? baseCandlesFull : aggregateCandles(baseCandlesFull, baseMinutes);
    const confirmCandlesPreClose =
        confirmTfSpec.sourceMinutes === confirmMinutes
            ? confirmCandlesFull
            : aggregateCandles(confirmCandlesFull, confirmMinutes);

    const baseCandles = onlyClosedCandles(baseCandlesPreClose, baseMinutes, params.nowMs);
    const confirmCandles = onlyClosedCandles(confirmCandlesPreClose, confirmMinutes, params.nowMs);

    const bid = Number.isFinite(toFinite(quote.bid)) ? toFinite(quote.bid) : null;
    const offer = Number.isFinite(toFinite(quote.offer)) ? toFinite(quote.offer) : null;
    const spreadAbs =
        bid !== null && offer !== null && offer > bid
            ? offer - bid
            : bid !== null && offer !== null
              ? Math.max(0, offer - bid)
              : 0;
    const spreadPips = spreadAbs > 0 ? spreadAbs / pipSizeForScalpSymbol(symbol) : 0;

    return {
        symbol,
        epic: resolved.epic,
        nowMs: params.nowMs,
        quote: {
            price: toFinite(quote.price),
            bid,
            offer,
            spreadAbs,
            spreadPips,
            tsMs: Number.isFinite(toFinite(quote.ts)) ? toFinite(quote.ts) : params.nowMs,
        },
        baseTf: params.baseTf,
        confirmTf: params.confirmTf,
        baseCandles,
        confirmCandles,
    };
}
