import { bitgetFetch, resolveProductType } from "../../bitget";

function toFinite(value: unknown, fallback = NaN): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toFiniteOrNull(value: unknown): number | null {
  const n = toFinite(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeText(value: unknown): string {
  return String(value || "").trim();
}

function normalizeBrokerText(value: unknown): string {
  return normalizeText(value).toUpperCase();
}

function normalizeTsMs(value: unknown): number | null {
  const n = toFinite(value);
  if (!(Number.isFinite(n) && n > 0)) return null;
  return n > 1e12 ? Math.floor(n) : Math.floor(n * 1000);
}

function extractRows(payload: unknown): Record<string, unknown>[] {
  const root = payload && typeof payload === "object" ? (payload as any) : {};
  const data = root.data && typeof root.data === "object" ? root.data : root;
  const candidates = [
    data.list,
    data.orderList,
    data.fillList,
    data.entrustedList,
    data.result,
    data.data,
    payload,
  ];
  for (const candidate of candidates) {
    if (Array.isArray(candidate)) {
      return candidate.filter(
        (row): row is Record<string, unknown> =>
          Boolean(row) && typeof row === "object" && !Array.isArray(row),
      );
    }
  }
  return [];
}

async function fetchBitgetOrders(params: {
  symbol: string;
  fromTsMs: number;
  toTsMs: number;
}): Promise<Record<string, unknown>[]> {
  const payload = await bitgetFetch("GET", "/api/v2/mix/order/orders-history", {
    productType: resolveProductType(),
    symbol: params.symbol,
    startTime: String(Math.max(0, Math.floor(params.fromTsMs))),
    endTime: String(Math.max(0, Math.floor(params.toTsMs))),
    limit: 100,
  });
  return extractRows(payload);
}

async function fetchBitgetPositionHistory(params: {
  symbol: string;
  fromTsMs: number;
  toTsMs: number;
}): Promise<Record<string, unknown>[]> {
  const payload = await bitgetFetch("GET", "/api/v2/mix/position/history-position", {
    productType: resolveProductType(),
    symbol: params.symbol,
    startTime: String(Math.max(0, Math.floor(params.fromTsMs))),
    endTime: String(Math.max(0, Math.floor(params.toTsMs))),
    pageSize: 100,
  });
  return extractRows(payload);
}

function sideToHoldSide(side: "BUY" | "SELL" | "long" | "short" | null | undefined) {
  const normalized = normalizeBrokerText(side);
  if (normalized === "BUY" || normalized === "LONG") return "long";
  if (normalized === "SELL" || normalized === "SHORT") return "short";
  return null;
}

function findEntryOrder(params: {
  orders: Record<string, unknown>[];
  dealReference?: string | null;
  brokerOrderId?: string | null;
  side: "long" | "short" | null;
}): Record<string, unknown> | null {
  const ref = normalizeText(params.dealReference);
  const orderId = normalizeText(params.brokerOrderId);
  const entrySide = params.side === "short" ? "SELL" : params.side === "long" ? "BUY" : null;
  return (
    params.orders.find((row) => ref && normalizeText(row.clientOid) === ref) ||
    params.orders.find((row) => orderId && normalizeText(row.orderId) === orderId) ||
    params.orders.find((row) => {
      const reduceOnly = normalizeBrokerText(row.reduceOnly);
      const source = normalizeBrokerText(row.enterPointSource);
      const side = normalizeBrokerText(row.side);
      return reduceOnly !== "YES" && source === "API" && (!entrySide || side === entrySide);
    }) ||
    null
  );
}

function findCloseOrders(params: {
  orders: Record<string, unknown>[];
  entryTsMs: number | null;
}): Record<string, unknown>[] {
  const entryTs = params.entryTsMs ?? 0;
  return params.orders
    .filter((row) => {
      const reduceOnly = normalizeBrokerText(row.reduceOnly);
      const ts = normalizeTsMs(row.uTime ?? row.cTime) ?? 0;
      return reduceOnly === "YES" && ts >= Math.max(0, entryTs - 1000);
    })
    .sort(
      (a, b) =>
        (normalizeTsMs(a.uTime ?? a.cTime) ?? 0) -
        (normalizeTsMs(b.uTime ?? b.cTime) ?? 0),
    );
}

function findPosition(params: {
  positions: Record<string, unknown>[];
  side: "long" | "short" | null;
  entryTsMs: number | null;
  exitTsMs: number | null;
}): Record<string, unknown> | null {
  const entryTs = params.entryTsMs ?? 0;
  const exitTs = params.exitTsMs ?? Number.MAX_SAFE_INTEGER;
  const bySide = params.positions.filter((row) => {
    const holdSide = sideToHoldSide((row.holdSide ?? row.side) as any);
    return !params.side || holdSide === params.side;
  });
  return (
    bySide.find((row) => {
      const ctime = normalizeTsMs(row.ctime ?? row.cTime ?? row.openTime);
      const utime = normalizeTsMs(row.utime ?? row.uTime ?? row.closeTime);
      return (
        ctime !== null &&
        Math.abs(ctime - entryTs) <= 10_000 &&
        (utime === null || utime <= exitTs + 5 * 60_000)
      );
    }) ||
    bySide
      .slice()
      .sort(
        (a, b) =>
          Math.abs((normalizeTsMs(a.ctime ?? a.cTime) ?? 0) - entryTs) -
          Math.abs((normalizeTsMs(b.ctime ?? b.cTime) ?? 0) - entryTs),
      )[0] ||
    null
  );
}

function classifyClose(params: {
  entry: Record<string, unknown> | null;
  close: Record<string, unknown> | null;
}): string {
  const closePrice = toFiniteOrNull(params.close?.priceAvg ?? params.close?.price);
  const stop = toFiniteOrNull(params.entry?.presetStopLossPrice);
  const takeProfit = toFiniteOrNull(params.entry?.presetStopSurplusPrice);
  if (closePrice !== null && stop !== null) {
    if (Math.abs(closePrice - stop) <= Math.max(0.02, Math.abs(stop) * 0.0015)) {
      return "SCALP_V2_RECONCILE_SL";
    }
  }
  if (closePrice !== null && takeProfit !== null) {
    if (
      Math.abs(closePrice - takeProfit) <=
      Math.max(0.02, Math.abs(takeProfit) * 0.0015)
    ) {
      return "SCALP_V2_RECONCILE_TP";
    }
  }
  const source = normalizeBrokerText(params.close?.enterPointSource);
  if (source === "SYS") return "SCALP_V2_RECONCILE_BROKER_CLOSE";
  return "SCALP_V2_RECONCILE_MANUAL_CLOSE";
}

function fallbackRiskUsdFromOrders(params: {
  side: "long" | "short" | null;
  entry: Record<string, unknown> | null;
  position: Record<string, unknown> | null;
}): number | null {
  const entryPrice =
    toFiniteOrNull(params.entry?.priceAvg ?? params.entry?.price) ??
    toFiniteOrNull(params.position?.openAvgPrice);
  const stop = toFiniteOrNull(params.entry?.presetStopLossPrice);
  const size =
    toFiniteOrNull(params.entry?.baseVolume ?? params.entry?.size) ??
    toFiniteOrNull(params.position?.openTotalPos);
  if (
    !(entryPrice !== null && entryPrice > 0) ||
    !(stop !== null && stop > 0) ||
    !(size !== null && size > 0)
  ) {
    return null;
  }
  const riskPerUnit =
    params.side === "short" ? stop - entryPrice : entryPrice - stop;
  if (!(Number.isFinite(riskPerUnit) && riskPerUnit > 0)) return null;
  return riskPerUnit * size;
}

export async function resolveBitgetBrokerCloseLedger(params: {
  symbol: string;
  side: "BUY" | "SELL" | "long" | "short" | null;
  dealReference?: string | null;
  brokerOrderId?: string | null;
  openedAtMs?: number | null;
  exitAtMs: number;
  riskUsd?: number | null;
  windowBeforeMs?: number;
  windowAfterMs?: number;
}): Promise<{
  found: boolean;
  tsExitMs: number | null;
  brokerRef: string | null;
  rMultiple: number;
  pnlUsd: number | null;
  reasonCodes: string[];
  rawPayload: Record<string, unknown>;
}> {
  const symbol = normalizeBrokerText(params.symbol);
  if (!symbol) {
    return {
      found: false,
      tsExitMs: null,
      brokerRef: null,
      rMultiple: 0,
      pnlUsd: null,
      reasonCodes: ["LEDGER_BROKER_CLOSE_LOOKUP_SKIPPED_NO_SYMBOL"],
      rawPayload: {},
    };
  }
  const exitAtMs = Math.floor(toFinite(params.exitAtMs, Date.now()));
  const openedAtMs = normalizeTsMs(params.openedAtMs) ?? exitAtMs;
  const fromTsMs = Math.max(
    0,
    Math.min(openedAtMs, exitAtMs) - (params.windowBeforeMs ?? 24 * 60 * 60_000),
  );
  const toTsMs = Math.max(openedAtMs, exitAtMs) + (params.windowAfterMs ?? 5 * 60_000);
  const side = sideToHoldSide(params.side);
  const [orders, positions] = await Promise.all([
    fetchBitgetOrders({ symbol, fromTsMs, toTsMs }),
    fetchBitgetPositionHistory({ symbol, fromTsMs, toTsMs }),
  ]);
  const sortedOrders = orders
    .slice()
    .sort(
      (a, b) =>
        (normalizeTsMs(a.cTime ?? a.uTime) ?? 0) -
        (normalizeTsMs(b.cTime ?? b.uTime) ?? 0),
    );
  const entry = findEntryOrder({
    orders: sortedOrders,
    dealReference: params.dealReference,
    brokerOrderId: params.brokerOrderId,
    side,
  });
  const entryTsMs = normalizeTsMs(entry?.cTime ?? entry?.uTime) ?? openedAtMs;
  const closeOrders = findCloseOrders({ orders: sortedOrders, entryTsMs });
  const finalClose = closeOrders.at(-1) ?? null;
  const closeTsMs = normalizeTsMs(finalClose?.uTime ?? finalClose?.cTime) ?? exitAtMs;
  const position = findPosition({
    positions,
    side,
    entryTsMs,
    exitTsMs: closeTsMs,
  });
  if (!position && !finalClose) {
    return {
      found: false,
      tsExitMs: null,
      brokerRef: null,
      rMultiple: 0,
      pnlUsd: null,
      reasonCodes: ["LEDGER_BROKER_CLOSE_NOT_FOUND"],
      rawPayload: {
        bitgetLookup: {
          symbol,
          fromTsMs,
          toTsMs,
          dealReference: params.dealReference ?? null,
          brokerOrderId: params.brokerOrderId ?? null,
          orderCount: orders.length,
          positionCount: positions.length,
        },
      },
    };
  }

  const pnlUsd =
    toFiniteOrNull(position?.netProfit) ??
    toFiniteOrNull(position?.pnl) ??
    toFiniteOrNull(finalClose?.totalProfits) ??
    null;
  const riskUsd =
    toFiniteOrNull(params.riskUsd) ||
    fallbackRiskUsdFromOrders({ side, entry, position });
  const rMultiple =
    pnlUsd !== null && riskUsd !== null && riskUsd > 0
      ? pnlUsd / riskUsd
      : 0;
  return {
    found: pnlUsd !== null,
    tsExitMs: normalizeTsMs(position?.utime ?? position?.uTime) ?? closeTsMs,
    brokerRef:
      normalizeText(position?.positionId) ||
      normalizeText(finalClose?.orderId) ||
      normalizeText(params.brokerOrderId) ||
      null,
    rMultiple: Number.isFinite(rMultiple) ? rMultiple : 0,
    pnlUsd,
    reasonCodes: [
      "LEDGER_BROKER_CLOSE_CONFIRMED",
      "SCALP_V2_RECONCILE_BROKER_HISTORY",
      classifyClose({ entry, close: finalClose }),
    ],
    rawPayload: {
      bitgetBrokerClose: {
        position: position
          ? {
              positionId: position.positionId,
              holdSide: position.holdSide,
              openAvgPrice: position.openAvgPrice,
              closeAvgPrice: position.closeAvgPrice,
              openTotalPos: position.openTotalPos,
              closeTotalPos: position.closeTotalPos,
              pnl: position.pnl,
              netProfit: position.netProfit,
              openFee: position.openFee,
              closeFee: position.closeFee,
              ctime: position.ctime ?? position.cTime,
              utime: position.utime ?? position.uTime,
            }
          : null,
        entryOrder: entry
          ? {
              orderId: entry.orderId,
              clientOid: entry.clientOid,
              side: entry.side,
              priceAvg: entry.priceAvg,
              baseVolume: entry.baseVolume ?? entry.size,
              presetStopLossPrice: entry.presetStopLossPrice,
              presetStopSurplusPrice: entry.presetStopSurplusPrice,
              cTime: entry.cTime,
              uTime: entry.uTime,
            }
          : null,
        closeOrders: closeOrders.map((row) => ({
          orderId: row.orderId,
          clientOid: row.clientOid,
          side: row.side,
          reduceOnly: row.reduceOnly,
          enterPointSource: row.enterPointSource,
          orderSource: row.orderSource,
          priceAvg: row.priceAvg,
          baseVolume: row.baseVolume ?? row.size,
          totalProfits: row.totalProfits,
          fee: row.fee,
          cTime: row.cTime,
          uTime: row.uTime,
        })),
      },
    },
  };
}
