import {
  aggregateScalpV2ParityWindow,
  importV1LedgerIntoScalpV2,
} from "./db";

export async function runScalpV2LedgerMigration(params: {
  limit?: number;
  parityWindowDays?: number;
} = {}): Promise<{
  imported: number;
  skipped: number;
  parity: {
    v1Trades: number;
    v1NetR: number;
    v2Trades: number;
    v2NetR: number;
  };
}> {
  const imported = await importV1LedgerIntoScalpV2({
    limit: params.limit,
  });
  const parity = await aggregateScalpV2ParityWindow({
    sinceDays: Math.max(1, Math.min(3650, Math.floor(params.parityWindowDays || 30))),
  });
  return {
    imported: imported.imported,
    skipped: imported.skipped,
    parity,
  };
}
