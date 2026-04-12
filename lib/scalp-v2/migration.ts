import {
  aggregateScalpV2CutoverParityWindow,
  importV1LedgerIntoScalpV2,
  importV1JournalIntoScalpV2,
  importV1SessionsIntoScalpV2,
} from "./db";

export async function runScalpV2CutoverMigration(params: {
  limit?: number;
  parityWindowDays?: number;
  journalParityLimit?: number;
} = {}): Promise<{
  tables: {
    ledger: {
      processed: number;
      inserted: number;
      updated: number;
      skipped: number;
    };
    sessions: {
      processed: number;
      inserted: number;
      updated: number;
      skipped: number;
    };
    journal: {
      processed: number;
      inserted: number;
      updated: number;
      skipped: number;
    };
  };
  parity: Awaited<ReturnType<typeof aggregateScalpV2CutoverParityWindow>>;
}> {
  const limit = Math.max(100, Math.min(500_000, Math.floor(params.limit || 50_000)));
  const [ledger, sessions, journal] = await Promise.all([
    importV1LedgerIntoScalpV2({
      limit,
    }),
    importV1SessionsIntoScalpV2({
      limit,
    }),
    importV1JournalIntoScalpV2({
      limit,
    }),
  ]);
  const parity = await aggregateScalpV2CutoverParityWindow({
    sinceDays: Math.max(1, Math.min(3650, Math.floor(params.parityWindowDays || 30))),
    journalLimit: Math.max(
      100,
      Math.min(50_000, Math.floor(params.journalParityLimit || 2_000)),
    ),
  });
  return {
    tables: {
      ledger,
      sessions,
      journal,
    },
    parity,
  };
}

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
  const cutover = await runScalpV2CutoverMigration({
    limit: params.limit,
    parityWindowDays: params.parityWindowDays,
    journalParityLimit: 500,
  });
  return {
    imported: cutover.tables.ledger.inserted,
    skipped: cutover.tables.ledger.skipped,
    parity: {
      v1Trades: cutover.parity.ledger.v1Trades,
      v1NetR: cutover.parity.ledger.v1NetR,
      v2Trades: cutover.parity.ledger.v2Trades,
      v2NetR: cutover.parity.ledger.v2NetR,
    },
  };
}
