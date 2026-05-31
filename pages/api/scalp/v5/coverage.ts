// /api/scalp/v5/coverage — section 2 of the v5 dashboard: classifier, config,
// evaluator timestamps, and coverage counts. Pure SQL aggregate, no regime
// snapshot loop. Always returns fast even when the heavier endpoints time out.

export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../lib/scalp-v2/http";
import { startOfUtcDayMs } from "../../../../lib/scalp-v4/week";
import {
  isScalpV5Enabled,
  isScalpV5HardGateEnabled,
  resolveScalpV5Config,
} from "../../../../lib/scalp-v5";
import { scalpPrisma } from "../../../../lib/scalp/pg/client";
import { sql } from "../../../../lib/scalp/pg/sql";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") return res.status(405).json({ error: "Method Not Allowed" });
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const cfg = resolveScalpV5Config();
    const nowMs = Date.now();
    const staleThresholdMs = Math.max(
      60 * 60_000,
      Math.floor(
        Number(process.env.BULK_V5_STALE_OLDER_THAN_HOURS) > 0
          ? Number(process.env.BULK_V5_STALE_OLDER_THAN_HOURS) * 60 * 60_000
          : 6 * 24 * 60 * 60_000,
      ),
    );
    const staleBefore = new Date(nowMs - staleThresholdMs);
    // The evaluator week starts on SUNDAY UTC, not Monday — because the
    // weekly rollover (scripts/scalp-v5-sunday.ts) runs on Sunday and
    // writes v5_evaluated_at = NOW() then. If we used the standard
    // Monday boundary, every Monday 00:00 UTC the dashboard's "this
    // week N/total" widget would reset to 0 even though Sunday's
    // rollover already advanced every row's evidence ~hours earlier.
    // Anchor to the most recent Sunday 00:00 UTC, INCLUSIVE of today:
    // Sun→today, Mon→yesterday, … Sat→6 days back. This is what makes
    // the widget reset on the rollover Sunday itself (so it tracks that
    // day's progress) instead of one Sunday late.
    const ONE_DAY_MS = 24 * 60 * 60_000;
    const utcDay = new Date(nowMs).getUTCDay();
    const weekStartMs = startOfUtcDayMs(nowMs) - utcDay * ONE_DAY_MS;
    const weekStart = new Date(weekStartMs);
    const lastHourBefore = new Date(nowMs - 60 * 60_000);
    const last12hBefore = new Date(nowMs - 12 * 60 * 60_000);
    // Candle staleness thresholds. The load-candles cron runs every 2h; a
    // healthy symbol has been written within the last ~4h. >4h is "lagging",
    // >12h is "broken" (multiple cron ticks missed in a row).
    const candleLagBefore = new Date(nowMs - 4 * 60 * 60_000);
    const candleBrokenBefore = new Date(nowMs - 12 * 60 * 60_000);
    const db = scalpPrisma();

    // Aggregate counts + 12h throughput in three parallel queries. The
    // bucket query is grouped by hour so the UI can render a sparkline;
    // missing hours are filled with 0 client-side / here below. The candle
    // freshness query joins against enabled deployments so we only surface
    // staleness for symbols we actually trade.
    const [aggRows, bucketRows, candleRows] = await Promise.all([
      db.$queryRaw<Array<{
        totalDeployments: bigint;
        enabledDeployments: bigint;
        evaluated: bigint;
        missingEvidence: bigint;
        stale: bigint;
        evaluatedThisWeek: bigint;
        evaluatedLastHour: bigint;
        latestEvaluatedAt: Date | null;
        oldestEvaluatedAt: Date | null;
      }>>(sql`
        SELECT
          COUNT(*) AS "totalDeployments",
          COUNT(*) FILTER (WHERE enabled = TRUE) AS "enabledDeployments",
          COUNT(*) FILTER (WHERE v5_evaluated_at IS NOT NULL) AS "evaluated",
          COUNT(*) FILTER (WHERE v5_evaluated_at IS NULL) AS "missingEvidence",
          COUNT(*) FILTER (WHERE v5_evaluated_at IS NOT NULL AND v5_evaluated_at < ${staleBefore}) AS "stale",
          COUNT(*) FILTER (WHERE v5_evaluated_at >= ${weekStart}) AS "evaluatedThisWeek",
          COUNT(*) FILTER (WHERE v5_evaluated_at >= ${lastHourBefore}) AS "evaluatedLastHour",
          MAX(v5_evaluated_at) AS "latestEvaluatedAt",
          MIN(v5_evaluated_at) AS "oldestEvaluatedAt"
        FROM scalp_v2_deployments d
        WHERE d.candidate_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1
            FROM scalp_v2_candidates c
            WHERE c.id = d.candidate_id
              AND c.metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'
          );
      `),
      db.$queryRaw<Array<{ hourStart: Date; n: bigint }>>(sql`
        SELECT
          date_trunc('hour', v5_evaluated_at) AS "hourStart",
          COUNT(*) AS "n"
        FROM scalp_v2_deployments d
        WHERE d.candidate_id IS NOT NULL
          AND d.v5_evaluated_at >= ${last12hBefore}
          AND NOT EXISTS (
            SELECT 1
            FROM scalp_v2_candidates c
            WHERE c.id = d.candidate_id
              AND c.metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'
          )
        GROUP BY 1
        ORDER BY 1 ASC;
      `),
      // Candle freshness per (venue, symbol) we actually trade. Reads
      // MAX(updated_at) from the 1m candle table joined against enabled
      // deployments. LEFT JOIN so symbols with no candles at all show up
      // as lastWriteAt=NULL.
      db.$queryRaw<Array<{
        venue: string;
        symbol: string;
        lastWriteAt: Date | null;
      }>>(sql`
        SELECT
          d.venue,
          d.symbol,
          MAX(c.updated_at) AS "lastWriteAt"
        FROM scalp_v2_deployments d
        LEFT JOIN scalp_candle_history_weeks c
          ON c.symbol = d.symbol
         AND c.timeframe = '1m'
        WHERE d.enabled = TRUE
          AND d.candidate_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1
            FROM scalp_v2_candidates rc
            WHERE rc.id = d.candidate_id
              AND rc.metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'
          )
        GROUP BY d.venue, d.symbol
        ORDER BY "lastWriteAt" ASC NULLS FIRST;
      `),
    ]);
    const r = aggRows[0] || {
      totalDeployments: BigInt(0),
      enabledDeployments: BigInt(0),
      evaluated: BigInt(0),
      missingEvidence: BigInt(0),
      stale: BigInt(0),
      evaluatedThisWeek: BigInt(0),
      evaluatedLastHour: BigInt(0),
      latestEvaluatedAt: null,
      oldestEvaluatedAt: null,
    };

    // Fill 12 hourly buckets oldest→newest (last bucket is the partial
    // current hour). UI renders this as a sparkline.
    const buckets12h: number[] = new Array(12).fill(0);
    const nowHourStart = Math.floor(nowMs / (60 * 60_000)) * (60 * 60_000);
    for (const row of bucketRows) {
      const bucketMs = row.hourStart.getTime();
      const hoursAgo = Math.floor((nowHourStart - bucketMs) / (60 * 60_000));
      if (hoursAgo >= 0 && hoursAgo < 12) {
        buckets12h[11 - hoursAgo] = Number(row.n);
      }
    }

    // Candle-freshness buckets: missing (never loaded), broken (>12h stale),
    // lagging (4-12h stale), fresh (<4h). Plus the worst offender summary so
    // the dashboard can show "X missing · Y broken · Z lagging".
    let candlesFresh = 0;
    let candlesLagging = 0;
    let candlesBroken = 0;
    let candlesMissing = 0;
    const worstSymbols: Array<{ venue: string; symbol: string; ageMinutes: number | null }> = [];
    for (const row of candleRows) {
      if (!row.lastWriteAt) {
        candlesMissing += 1;
        worstSymbols.push({ venue: row.venue, symbol: row.symbol, ageMinutes: null });
        continue;
      }
      const ageMs = nowMs - row.lastWriteAt.getTime();
      const ageMinutes = Math.round(ageMs / 60_000);
      if (row.lastWriteAt < candleBrokenBefore) {
        candlesBroken += 1;
        worstSymbols.push({ venue: row.venue, symbol: row.symbol, ageMinutes });
      } else if (row.lastWriteAt < candleLagBefore) {
        candlesLagging += 1;
      } else {
        candlesFresh += 1;
      }
    }
    // Cap the worst-offender list for the dashboard tooltip.
    worstSymbols.splice(20);

    // ETA = (total - evaluatedThisWeek) / hourly rate.
    // Prefer the 12h average to smooth bursty workers; fall back to the
    // last-hour count if 12h is zero (cold start).
    const total = Number(r.totalDeployments);
    const evaluatedThisWeek = Number(r.evaluatedThisWeek);
    const lastHour = Number(r.evaluatedLastHour);
    const last12hSum = buckets12h.reduce((a, b) => a + b, 0);
    const ratePerHour = last12hSum > 0 ? last12hSum / 12 : lastHour;
    const remainingThisWeek = Math.max(0, total - evaluatedThisWeek);
    const etaHours = ratePerHour > 0 ? remainingThisWeek / ratePerHour : null;

    return res.status(200).json({
      ok: true,
      classifierVersion: cfg.classifierVersion,
      v5Enabled: isScalpV5Enabled(),
      v5HardGateEnabled: isScalpV5HardGateEnabled(),
      config: {
        holdoutWeeks: cfg.holdoutWeeks,
        minTradesPerCell: cfg.minTradesPerCell,
      },
      nowMs,
      evaluator: {
        latestEvaluationMs: r.latestEvaluatedAt ? r.latestEvaluatedAt.getTime() : null,
        oldestEvaluationMs: r.oldestEvaluatedAt ? r.oldestEvaluatedAt.getTime() : null,
      },
      coverage: {
        totalDeployments: Number(r.totalDeployments),
        enabledDeployments: Number(r.enabledDeployments),
        evaluated: Number(r.evaluated),
        missingEvidence: Number(r.missingEvidence),
        stale: Number(r.stale),
        staleThresholdMs,
      },
      progress: {
        weekStartMs,
        evaluatedThisWeek,
        remainingThisWeek,
        lastHour,
        buckets12h,
        ratePerHour,
        etaHours,
      },
      candleHealth: {
        // For enabled deployments only — these are the candles we depend on
        // for live evaluation. Missing/broken means the v5 evaluator is
        // producing stale or empty evidence on those symbols.
        fresh: candlesFresh,
        lagging: candlesLagging,
        broken: candlesBroken,
        missing: candlesMissing,
        // Worst offenders, capped at 20. ageMinutes=null means no candles at
        // all for that symbol.
        worstSymbols,
      },
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: (err as Error)?.message || String(err) });
  }
}
