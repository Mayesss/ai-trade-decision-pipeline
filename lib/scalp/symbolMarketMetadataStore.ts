import { Prisma } from "@prisma/client";

import { isScalpPgConfigured, scalpPrisma } from "./pg/client";
import {
  type ScalpSymbolMarketMetadata,
  normalizeScalpSymbolMarketMetadata,
} from "./symbolMarketMetadata";

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function toNumberOrNull(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function fromRow(
  row: Record<string, unknown> | null | undefined,
): ScalpSymbolMarketMetadata | null {
  if (!row) return null;
  const symbol = normalizeSymbol(row.symbol);
  if (!symbol) return null;
  return normalizeScalpSymbolMarketMetadata({
    symbol,
    epic: row.epic ? String(row.epic) : null,
    source: row.source === "capital" ? "capital" : "heuristic",
    assetCategory: String(row.assetCategory || "")
      .trim()
      .toLowerCase() as
      | "forex"
      | "crypto"
      | "commodity"
      | "index"
      | "equity"
      | "other",
    instrumentType: row.instrumentType ? String(row.instrumentType) : null,
    marketStatus: row.marketStatus ? String(row.marketStatus) : null,
    pipSize: toNumberOrNull(row.pipSize) ?? undefined,
    pipPosition: toNumberOrNull(row.pipPosition),
    tickSize: toNumberOrNull(row.tickSize),
    decimalPlacesFactor: toNumberOrNull(row.decimalPlacesFactor),
    scalingFactor: toNumberOrNull(row.scalingFactor),
    minDealSize: toNumberOrNull(row.minDealSize),
    sizeDecimals: toNumberOrNull(row.sizeDecimals),
    openingHours:
      row.openingHoursJson && typeof row.openingHoursJson === "object"
        ? (row.openingHoursJson as ScalpSymbolMarketMetadata["openingHours"])
        : null,
    fetchedAtMs: toNumberOrNull(row.fetchedAtMs) ?? Date.now(),
  });
}

export async function loadScalpSymbolMarketMetadata(
  symbolRaw: string,
): Promise<ScalpSymbolMarketMetadata | null> {
  const symbol = normalizeSymbol(symbolRaw);
  if (!symbol || !isScalpPgConfigured()) return null;
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{
      symbol: string;
      epic: string | null;
      source: string;
      assetCategory: string;
      instrumentType: string | null;
      marketStatus: string | null;
      pipSize: Prisma.Decimal | number | string | null;
      pipPosition: number | null;
      tickSize: Prisma.Decimal | number | string | null;
      decimalPlacesFactor: number | null;
      scalingFactor: number | null;
      minDealSize: Prisma.Decimal | number | string | null;
      sizeDecimals: number | null;
      openingHoursJson: Record<string, unknown> | null;
      fetchedAtMs: bigint | number | null;
    }>
  >(Prisma.sql`
      SELECT
        symbol,
        epic,
        source,
        asset_category AS "assetCategory",
        instrument_type AS "instrumentType",
        market_status AS "marketStatus",
        pip_size AS "pipSize",
        pip_position AS "pipPosition",
        tick_size AS "tickSize",
        decimal_places_factor AS "decimalPlacesFactor",
        scaling_factor AS "scalingFactor",
        min_deal_size AS "minDealSize",
        size_decimals AS "sizeDecimals",
        opening_hours_json AS "openingHoursJson",
        (EXTRACT(EPOCH FROM fetched_at) * 1000)::bigint AS "fetchedAtMs"
      FROM scalp_symbol_market_metadata
      WHERE symbol = ${symbol}
      LIMIT 1;
    `);
  return fromRow(rows[0] as Record<string, unknown> | undefined);
}

export async function loadScalpSymbolMarketMetadataBulk(
  symbolsRaw: string[],
): Promise<Map<string, ScalpSymbolMarketMetadata | null>> {
  const symbols = Array.from(
    new Set(symbolsRaw.map((row) => normalizeSymbol(row)).filter(Boolean)),
  );
  const out = new Map<string, ScalpSymbolMarketMetadata | null>();
  for (const symbol of symbols) out.set(symbol, null);
  if (!symbols.length || !isScalpPgConfigured()) return out;

  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{
      symbol: string;
      epic: string | null;
      source: string;
      assetCategory: string;
      instrumentType: string | null;
      marketStatus: string | null;
      pipSize: Prisma.Decimal | number | string | null;
      pipPosition: number | null;
      tickSize: Prisma.Decimal | number | string | null;
      decimalPlacesFactor: number | null;
      scalingFactor: number | null;
      minDealSize: Prisma.Decimal | number | string | null;
      sizeDecimals: number | null;
      openingHoursJson: Record<string, unknown> | null;
      fetchedAtMs: bigint | number | null;
    }>
  >(Prisma.sql`
      SELECT
        symbol,
        epic,
        source,
        asset_category AS "assetCategory",
        instrument_type AS "instrumentType",
        market_status AS "marketStatus",
        pip_size AS "pipSize",
        pip_position AS "pipPosition",
        tick_size AS "tickSize",
        decimal_places_factor AS "decimalPlacesFactor",
        scaling_factor AS "scalingFactor",
        min_deal_size AS "minDealSize",
        size_decimals AS "sizeDecimals",
        opening_hours_json AS "openingHoursJson",
        (EXTRACT(EPOCH FROM fetched_at) * 1000)::bigint AS "fetchedAtMs"
      FROM scalp_symbol_market_metadata
      WHERE symbol IN (${Prisma.join(symbols)});
    `);
  for (const row of rows) {
    const parsed = fromRow(row as Record<string, unknown>);
    if (!parsed) continue;
    out.set(parsed.symbol, parsed);
  }
  return out;
}

export async function saveScalpSymbolMarketMetadata(
  metadataRaw: ScalpSymbolMarketMetadata,
): Promise<boolean> {
  return (await saveScalpSymbolMarketMetadataBulk([metadataRaw])) > 0;
}

export async function saveScalpSymbolMarketMetadataBulk(
  rowsRaw: ScalpSymbolMarketMetadata[],
): Promise<number> {
  if (!rowsRaw.length || !isScalpPgConfigured()) return 0;
  const rows = rowsRaw
    .map((row) => normalizeScalpSymbolMarketMetadata(row))
    .filter((row) => Boolean(row.symbol));
  if (!rows.length) return 0;

  const payload = JSON.stringify(
    rows.map((row) => ({
      symbol: row.symbol,
      epic: row.epic,
      source: row.source,
      asset_category: row.assetCategory,
      instrument_type: row.instrumentType,
      market_status: row.marketStatus,
      pip_size: row.pipSize,
      pip_position: row.pipPosition,
      tick_size: row.tickSize,
      decimal_places_factor: row.decimalPlacesFactor,
      scaling_factor: row.scalingFactor,
      min_deal_size: row.minDealSize,
      size_decimals: row.sizeDecimals,
      opening_hours_json: row.openingHours
        ? JSON.stringify(row.openingHours)
        : null,
      fetched_at: new Date(row.fetchedAtMs).toISOString(),
    })),
  );

  const db = scalpPrisma();
  const updated = await db.$executeRaw(
    Prisma.sql`
      WITH input AS (
        SELECT
          UPPER(TRIM(x.symbol)) AS symbol,
          NULLIF(UPPER(TRIM(COALESCE(x.epic, ''))), '') AS epic,
          COALESCE(NULLIF(TRIM(x.source), ''), 'capital') AS source,
          LOWER(TRIM(COALESCE(x.asset_category, 'other'))) AS asset_category,
          NULLIF(UPPER(TRIM(COALESCE(x.instrument_type, ''))), '') AS instrument_type,
          NULLIF(UPPER(TRIM(COALESCE(x.market_status, ''))), '') AS market_status,
          x.pip_size::numeric AS pip_size,
          CASE WHEN x.pip_position IS NULL THEN NULL ELSE x.pip_position::int END AS pip_position,
          CASE WHEN x.tick_size IS NULL THEN NULL ELSE x.tick_size::numeric END AS tick_size,
          CASE WHEN x.decimal_places_factor IS NULL THEN NULL ELSE x.decimal_places_factor::int END AS decimal_places_factor,
          CASE WHEN x.scaling_factor IS NULL THEN NULL ELSE x.scaling_factor::int END AS scaling_factor,
          CASE WHEN x.min_deal_size IS NULL THEN NULL ELSE x.min_deal_size::numeric END AS min_deal_size,
          CASE WHEN x.size_decimals IS NULL THEN NULL ELSE x.size_decimals::int END AS size_decimals,
          CASE
            WHEN x.opening_hours_json IS NULL OR TRIM(x.opening_hours_json) = '' THEN NULL::jsonb
            ELSE x.opening_hours_json::jsonb
          END AS opening_hours_json,
          x.fetched_at::timestamptz AS fetched_at
        FROM jsonb_to_recordset(${payload}::jsonb) AS x(
          symbol text,
          epic text,
          source text,
          asset_category text,
          instrument_type text,
          market_status text,
          pip_size numeric,
          pip_position int,
          tick_size numeric,
          decimal_places_factor int,
          scaling_factor int,
          min_deal_size numeric,
          size_decimals int,
          opening_hours_json text,
          fetched_at timestamptz
        )
      )
      INSERT INTO scalp_symbol_market_metadata(
        symbol,
        epic,
        source,
        asset_category,
        instrument_type,
        market_status,
        pip_size,
        pip_position,
        tick_size,
        decimal_places_factor,
        scaling_factor,
        min_deal_size,
        size_decimals,
        opening_hours_json,
        fetched_at,
        updated_at
      )
      SELECT
        symbol,
        epic,
        source,
        asset_category,
        instrument_type,
        market_status,
        pip_size,
        pip_position,
        tick_size,
        decimal_places_factor,
        scaling_factor,
        min_deal_size,
        size_decimals,
        opening_hours_json,
        fetched_at,
        NOW()
      FROM input
      ON CONFLICT(symbol)
      DO UPDATE SET
        epic = EXCLUDED.epic,
        source = EXCLUDED.source,
        asset_category = EXCLUDED.asset_category,
        instrument_type = EXCLUDED.instrument_type,
        market_status = EXCLUDED.market_status,
        pip_size = EXCLUDED.pip_size,
        pip_position = EXCLUDED.pip_position,
        tick_size = EXCLUDED.tick_size,
        decimal_places_factor = EXCLUDED.decimal_places_factor,
        scaling_factor = EXCLUDED.scaling_factor,
        min_deal_size = EXCLUDED.min_deal_size,
        size_decimals = EXCLUDED.size_decimals,
        opening_hours_json = EXCLUDED.opening_hours_json,
        fetched_at = EXCLUDED.fetched_at,
        updated_at = NOW();
    `,
  );
  return Number(updated || 0);
}
