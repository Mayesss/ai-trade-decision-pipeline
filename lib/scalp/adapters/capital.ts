import {
  closeCapitalPositionByOwnership,
  executeCapitalScalpEntry,
  fetchCapitalAccountEquityUsd,
  fetchCapitalCandlesByEpic,
  fetchCapitalLivePrice,
  fetchCapitalOpenPositionSnapshots,
  fetchCapitalSymbolMarketMetadata,
  resolveCapitalEpicRuntime,
} from "../../capital";
import { getScalpVenueFeeSchedule } from "../fees";
import type { ScalpVenueAdapter } from "./types";

export const capitalScalpVenueAdapter: ScalpVenueAdapter = {
  venue: "capital",
  fees: getScalpVenueFeeSchedule("capital"),
  market: {
    resolveEpicRuntime(symbol: string) {
      return resolveCapitalEpicRuntime(symbol);
    },
    fetchLivePrice(symbol: string) {
      return fetchCapitalLivePrice(symbol);
    },
    fetchCandlesByEpic(epic: string, timeframe: string, limit: number) {
      return fetchCapitalCandlesByEpic(epic, timeframe, limit);
    },
    ensureSymbolMarketMetadata(symbol: string) {
      return fetchCapitalSymbolMarketMetadata(symbol);
    },
  },
  broker: {
    fetchAccountEquityUsd() {
      return fetchCapitalAccountEquityUsd();
    },
    fetchOpenPositionSnapshots() {
      return fetchCapitalOpenPositionSnapshots();
    },
    executeScalpEntry(params) {
      return executeCapitalScalpEntry(params);
    },
    closePositionByOwnership(params) {
      return closeCapitalPositionByOwnership(params);
    },
  },
};
