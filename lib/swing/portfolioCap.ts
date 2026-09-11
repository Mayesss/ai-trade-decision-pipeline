// Portfolio cap — the two pre-AI breadth gates for FLAT ticks
// (MAX_OPEN_POSITIONS, ONE_POSITION_PER_ASSET_CLASS in decisionConfig.ts).
//
// Pure verdict + one thin loader. "Occupied" is read off swing.ai_threads: a
// thread exists exactly while the system has capital committed to a symbol
// (resting entry or open position), and the wake-watcher reconciles it against
// the venues every minute, so it is the cheapest honest occupancy list there
// is — one indexed query, no venue calls. Categories come from the cron
// registry (vercel.json) and fall back to inference from the symbol.

import { resolveSwingCategory } from './category';
import { MAX_OPEN_POSITIONS, ONE_POSITION_PER_ASSET_CLASS } from './decisionConfig';
import { listSwingActiveThreads } from './pg';
import { getCronSymbolConfigs } from '../symbolRegistry';

import type { AnalysisPlatform } from '../platform';
import type { SwingAiThreadStatus } from './pg';

export type PortfolioOccupant = {
    platform: string;
    symbol: string;
    category: string | null;
    status: SwingAiThreadStatus;
};

export type PortfolioCapVerdict =
    | { blocked: false; openCount: number; selfOccupied: boolean }
    | {
          blocked: true;
          stage: 'position_cap' | 'asset_class_occupied';
          reason: string;
          openCount: number;
          maxOpen: number;
          occupants: string[];
      };

const key = (platform: string, symbol: string) => `${String(platform).toLowerCase()}:${String(symbol).toUpperCase()}`;

/**
 * Decide whether a flat tick for `self` may spend a market read and an AI
 * call. The symbol's own thread never blocks it (that tick manages the order
 * or position); a null category on either side disables the class rule for
 * that pair — the cap still counts it.
 */
export function evaluatePortfolioCapGate(params: {
    self: { platform: string; symbol: string; category: string | null };
    occupants: PortfolioOccupant[];
    maxOpen?: number;
    onePerClass?: boolean;
}): PortfolioCapVerdict {
    const maxOpen = params.maxOpen ?? MAX_OPEN_POSITIONS;
    const onePerClass = params.onePerClass ?? ONE_POSITION_PER_ASSET_CLASS;
    const selfKey = key(params.self.platform, params.self.symbol);
    const others = params.occupants.filter((o) => key(o.platform, o.symbol) !== selfKey);
    const selfOccupied = others.length !== params.occupants.length;
    const openCount = params.occupants.length;
    if (selfOccupied) return { blocked: false, openCount, selfOccupied: true };

    const label = (o: PortfolioOccupant) => `${o.symbol}${o.status === 'pending_entry' ? '(resting)' : ''}`;

    if (maxOpen > 0 && others.length >= maxOpen) {
        return {
            blocked: true,
            stage: 'position_cap',
            reason: `position_cap:${others.length}_of_${maxOpen}_committed`,
            openCount,
            maxOpen,
            occupants: others.map(label),
        };
    }
    const selfClass = params.self.category;
    if (onePerClass && selfClass) {
        const sameClass = others.filter((o) => o.category === selfClass);
        if (sameClass.length) {
            return {
                blocked: true,
                stage: 'asset_class_occupied',
                reason: `asset_class_occupied:${selfClass}:${sameClass.map(label).join(',')}`,
                openCount,
                maxOpen,
                occupants: sameClass.map(label),
            };
        }
    }
    return { blocked: false, openCount, selfOccupied: false };
}

/** Both gates off → nothing to load, nothing to evaluate. */
export function portfolioCapEnabled(): boolean {
    return MAX_OPEN_POSITIONS > 0 || ONE_POSITION_PER_ASSET_CLASS;
}

/**
 * Every committed thread with its asset class. Fails OPEN: a Postgres miss
 * returns an empty list and the tick proceeds as before — a cap that could
 * freeze the whole book on a DB hiccup would be a new outage mode.
 */
export async function loadPortfolioOccupants(): Promise<PortfolioOccupant[]> {
    const threads = await listSwingActiveThreads().catch((err: unknown) => {
        console.warn('portfolio cap: could not list active threads, failing open:', err);
        return [];
    });
    if (!threads.length) return [];
    const registry = new Map<string, string | null>();
    for (const config of getCronSymbolConfigs()) {
        registry.set(key(config.platform, config.symbol), config.category ?? null);
    }
    return threads.map((thread) => {
        const platform = thread.platform as AnalysisPlatform;
        const fromRegistry = registry.get(key(platform, thread.symbol));
        return {
            platform: thread.platform,
            symbol: thread.symbol,
            category: resolveSwingCategory({ category: fromRegistry ?? null, symbol: thread.symbol, platform }),
            status: thread.status,
        };
    });
}
