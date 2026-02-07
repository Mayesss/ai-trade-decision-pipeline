import type { PositionInfo } from './analytics';
import { kvGetJson, kvSetJson } from './kv';

const POSITION_EXTREMA_KEY_PREFIX = 'position:extrema';

type PositionExtremaState = {
    positionId: string;
    maxDrawdownPct: number;
    maxProfitPct: number;
    updatedAt: number;
};

type PositionExtremaValues = {
    maxDrawdownPct?: number;
    maxProfitPct?: number;
};

function toFinite(value: number): number | null {
    return Number.isFinite(value) ? Number(value) : null;
}

function makePositionId(position: Extract<PositionInfo, { status: 'open' }>): string {
    const side = position.holdSide;
    const entryTs = Number(position.entryTimestamp ?? 0);
    const entryTsPart = Number.isFinite(entryTs) && entryTs > 0 ? String(Math.trunc(entryTs)) : 'na';
    const entryPriceNum = Number(position.entryPrice);
    const entryPricePart = Number.isFinite(entryPriceNum) ? entryPriceNum.toFixed(8) : String(position.entryPrice ?? 'na');
    return `${side}:${entryTsPart}:${entryPricePart}`;
}

function keyFor(symbol: string, timeFrame: string) {
    return `${POSITION_EXTREMA_KEY_PREFIX}:${symbol.toUpperCase()}:${timeFrame}`;
}

export async function updatePositionExtrema(params: {
    symbol: string;
    timeFrame: string;
    position: PositionInfo;
    pnlPct: number;
}): Promise<PositionExtremaValues> {
    if (params.position.status !== 'open') return {};
    const pnlPct = toFinite(params.pnlPct);
    if (pnlPct === null) return {};

    const key = keyFor(params.symbol, params.timeFrame);
    const currentPositionId = makePositionId(params.position);
    const now = Date.now();

    const prev = await kvGetJson<PositionExtremaState>(key);
    const isSamePosition = prev?.positionId === currentPositionId;
    const baseDrawdown = isSamePosition && Number.isFinite(prev?.maxDrawdownPct) ? Number(prev?.maxDrawdownPct) : pnlPct;
    const baseProfit = isSamePosition && Number.isFinite(prev?.maxProfitPct) ? Number(prev?.maxProfitPct) : pnlPct;
    const nextState: PositionExtremaState = {
        positionId: currentPositionId,
        maxDrawdownPct: Math.min(baseDrawdown, pnlPct),
        maxProfitPct: Math.max(baseProfit, pnlPct),
        updatedAt: now,
    };

    const changed =
        !prev ||
        prev.positionId !== nextState.positionId ||
        prev.maxDrawdownPct !== nextState.maxDrawdownPct ||
        prev.maxProfitPct !== nextState.maxProfitPct;

    if (changed) {
        await kvSetJson(key, nextState);
    }

    return {
        maxDrawdownPct: nextState.maxDrawdownPct,
        maxProfitPct: nextState.maxProfitPct,
    };
}
