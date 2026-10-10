# 000b — H2 scout: exposure to the holdout period (OKX liquidations)

Ledger entry, not a hypothesis. Written 2026-10-10, during the H2 data scout
(`research/copytrade/scouts/h2-binance-data.md`).

## What happened

To check whether OKX publishes historical liquidations, the scout called
`GET /api/v5/public/liquidation-orders` once without a date bound. The
endpoint returned the latest 100 BTC-USDT-SWAP liquidation orders, dated
2026-10-09/10. That is inside the holdout period (2026-07-01 →) of every
registration on this programme.

## What was seen

- The 100 rows (time, side, size, bankruptcy price) were printed to the
  terminal. They were not saved, and no price outcome, markout or aggregate
  was computed from them.
- The venue is OKX, not Hyperliquid, so none of the sealed Hyperliquid
  archive was read. Binance data from 2026-07-01 on was sized from S3
  listings only. Nothing in that range was downloaded.

## What must not follow from it

- No threshold, cascade definition, side or horizon in any later
  registration may be chosen because of these rows.
- If a later registration uses OKX liquidations, it must not use the
  2026-10-09/10 window as discovery evidence.

## Mitigation

From now on, every scout and runner query to a venue API carries an
explicit end date before 2026-07-01, unless a REGISTERED rule's holdout read
is the purpose of the call.
