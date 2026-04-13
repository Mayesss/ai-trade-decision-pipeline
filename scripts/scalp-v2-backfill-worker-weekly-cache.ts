import {
  resolveScalpV2WorkerWeeklyCacheBackfillOptions,
  runScalpV2WorkerWeeklyCacheBackfill,
} from "../lib/scalp-v2/backfillWorkerWeeklyCache";

function parseArgValue(argv: string[], key: string): string | undefined {
  const prefix = `${key}=`;
  for (const arg of argv) {
    if (arg.startsWith(prefix)) return arg.slice(prefix.length);
  }
  return undefined;
}

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = argv.includes("--apply") ? false : true;
  const verbose = argv.includes("--verbose");
  const startedAtMs = Date.now();

  const opts = resolveScalpV2WorkerWeeklyCacheBackfillOptions({
    dryRun,
    verbose,
    limit: Number(parseArgValue(argv, "--limit")),
    offset: Number(parseArgValue(argv, "--offset")),
    venue: parseArgValue(argv, "--venue") as any,
    session: parseArgValue(argv, "--session") as any,
    statuses: parseArgValue(argv, "--statuses") || undefined,
    symbols: parseArgValue(argv, "--symbols") || undefined,
    windowToTs: Number(parseArgValue(argv, "--windowToTs")),
    stageAWeeks: Number(parseArgValue(argv, "--stageAWeeks")),
    stageBWeeks: Number(parseArgValue(argv, "--stageBWeeks")),
    stageCWeeks: Number(parseArgValue(argv, "--stageCWeeks")),
    minCandles: Number(parseArgValue(argv, "--minCandles")),
    upsertBatchSize: Number(parseArgValue(argv, "--upsertBatchSize")),
    cacheVersion: parseArgValue(argv, "--cacheVersion") || undefined,
  });

  const stats = await runScalpV2WorkerWeeklyCacheBackfill(opts);

  console.log(
    JSON.stringify(
      {
        ok: true,
        dryRun: opts.dryRun,
        opts: {
          limit: opts.limit,
          offset: opts.offset,
          venue: opts.venue,
          session: opts.session,
          statuses: Array.from(opts.statuses),
          symbols: Array.from(opts.symbolFilter),
          windowToTs: opts.windowToTs,
          windowToIso: new Date(opts.windowToTs).toISOString(),
          stageWeeks: {
            a: opts.stageAWeeks,
            b: opts.stageBWeeks,
            c: opts.stageCWeeks,
          },
          minCandles: opts.minCandles,
          cacheVersion: opts.cacheVersion,
          upsertBatchSize: opts.upsertBatchSize,
        },
        stats,
        elapsedMs: Date.now() - startedAtMs,
      },
      null,
      2,
    ),
  );
}

main().catch((err: any) => {
  const details = {
    name: err?.name || null,
    message: String(err?.message || err || "scalp_v2_backfill_weekly_cache_failed"),
    stack: err?.stack || null,
    cause:
      err?.cause && typeof err.cause === "object"
        ? {
            name: (err.cause as any).name || null,
            message: String((err.cause as any).message || ""),
            stack: (err.cause as any).stack || null,
          }
        : err?.cause || null,
    errors: Array.isArray(err?.errors)
      ? err.errors.map((entry: any) => ({
          name: entry?.name || null,
          message: String(entry?.message || entry || ""),
          stack: entry?.stack || null,
        }))
      : null,
  };
  console.error(
    JSON.stringify(
      {
        ok: false,
        error: details.message,
        details,
      },
      null,
      2,
    ),
  );
  process.exit(1);
});
