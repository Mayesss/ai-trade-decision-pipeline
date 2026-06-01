# Neon Egress Guardrails

## Immediate Account Guardrail

Set a Neon organization spending limit and data-transfer alert from the Neon dashboard. This is intentionally not controlled by application code.

Recommended alert thresholds:

- 50 GB monthly data transfer: early warning.
- 90 GB monthly data transfer: free-tier limit warning.
- 120 GB monthly data transfer: incident threshold.

## Application Guardrails

- Production candle-history reads should use range, tail, freshness-stat, or weekly-bar helpers.
- Full history reads are cold-path only and emit `full_history_read_warning` in production.
- Bounded live candle reads prefer broker APIs, then KV weekly shards, then Neon PG.
- Stored-history backtests and promotion/audit scripts pin reads to PG so live broker data cannot change reproducibility.
- KV candle shards are merged by week and written with a TTL (`SCALP_CANDLE_HISTORY_KV_TTL_SECONDS`, default 400 days).
- Recurring cron paths must not load unbounded `candles_json` payloads.
- Local KV sync refuses unbounded reads unless `--allowFullHistory` is passed; prefer `--maxDays`.
- v4 regime builds classify from compact `scalp_v4_weekly_bars`; hourly refresh reads only the previous/current week of 1m candles and uses a server-side PG aggregate bootstrap only when compact bars are missing.

## Useful Env Flags

- `SCALP_CANDLE_HISTORY_AUDIT_LOG=1`: logs egress-safe candle-loader diagnostics.
- `SCALP_CANDLE_HISTORY_WARN_FULL_READS=1`: warns on full history reads outside production.
- `SCALP_CANDLE_HISTORY_READ_ORDER=broker,kv,pg`: default bounded read order.
- `SCALP_V4_INCREMENTAL_BOOTSTRAP_WEEKS=72`: compact v4 classifier history window and cold-cache bootstrap size.
