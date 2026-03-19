import type { ScalpSymbolMarketMetadata } from "../symbolMarketMetadata";
import type { ScalpVenueFeeSchedule } from "../fees";
import type { ScalpVenue } from "../venue";

export interface ScalpVenueResolvedInstrument {
  ticker: string;
  epic: string;
  source: string;
}

export interface ScalpVenueLivePrice {
  symbol: string;
  epic: string;
  price: number;
  bid: number | null;
  offer: number | null;
  ts: number;
  mappingSource?: string | null;
}

export type ScalpVenueCandles = any[];

export interface ScalpBrokerPositionSnapshot {
  epic: string;
  dealId: string | null;
  dealReference: string | null;
  side: "long" | "short" | null;
  posMode?: "one_way_mode" | "hedge_mode" | null;
  entryPrice: number | null;
  leverage: number | null;
  size: number | null;
  pnlPct: number | null;
  bid: number | null;
  offer: number | null;
  createdAtMs: number | null;
  updatedAtMs: number;
}

export interface ScalpVenueEntryResult {
  placed: boolean;
  dryRun: boolean;
  orderId: string | null;
  dealId: string | null;
  dealReference: string | null;
  clientOid: string;
  symbol: string;
  direction: "BUY" | "SELL";
  notionalUsd: number;
  leverage: number | null;
  orderType: "MARKET" | "LIMIT";
  size: number | null;
  epic: string | null;
  dealStatus: string | null;
  confirmStatus: string | null;
  rejectReason: string | null;
  raw?: unknown;
}

export interface ScalpVenueCloseOwnershipResult {
  closed: boolean;
  orderId: string | null;
  clientOid: string;
  partial: boolean;
  note?: string;
}

export interface ScalpVenueMarketAdapter {
  resolveEpicRuntime(symbol: string): Promise<ScalpVenueResolvedInstrument>;
  fetchLivePrice(symbol: string): Promise<ScalpVenueLivePrice>;
  fetchCandlesByEpic(
    epic: string,
    timeframe: string,
    limit: number,
  ): Promise<ScalpVenueCandles>;
  ensureSymbolMarketMetadata(
    symbol: string,
  ): Promise<ScalpSymbolMarketMetadata | null>;
}

export interface ScalpVenueBrokerAdapter {
  fetchAccountEquityUsd(): Promise<number | null>;
  fetchOpenPositionSnapshots(): Promise<ScalpBrokerPositionSnapshot[]>;
  executeScalpEntry(params: {
    symbol: string;
    direction: "BUY" | "SELL";
    notionalUsd: number;
    riskUsd?: number | null;
    leverage?: number | null;
    dryRun?: boolean;
    clientOid?: string;
    orderType?: "MARKET" | "LIMIT";
    limitLevel?: number | null;
    stopLevel?: number | null;
    profitLevel?: number | null;
  }): Promise<ScalpVenueEntryResult>;
  closePositionByOwnership(params: {
    dealId?: string | null;
    dealReference?: string | null;
    partialClosePct?: number | null;
    clientOid?: string;
  }): Promise<ScalpVenueCloseOwnershipResult>;
}

export interface ScalpVenueAdapter {
  venue: ScalpVenue;
  fees: ScalpVenueFeeSchedule;
  market: ScalpVenueMarketAdapter;
  broker: ScalpVenueBrokerAdapter;
}
