# H2 data scout: Binance USDⓈ-M (plus Bybit and OKX)

2026-10-10. Feasibility only: listings, HEAD sizes, schemas, row counts and
timing. No price outcome, markout or reversion was computed.
Script: `research/copytrade/scouts/h2_binance_scout.py`, which lists S3 and
inspects samples. Samples are in `research/copytrade/data/scout_binance/raw/`,
which is gitignored by `.gitignore:189`.

## Verdict first

**H2 cannot be tested as designed.** The design rests on Binance USDⓈ-M
`liquidationSnapshot`, and that dataset does not exist.
`data/futures/um/{daily,monthly}/liquidationSnapshot/` lists empty, and
`.../um/daily/liquidationSnapshot/BTCUSDT/BTCUSDT-liquidationSnapshot-2023-06-26.zip`
returns 404. The dataset exists only for **COIN-M** (`data/futures/cm/daily/liquidationSnapshot/`),
the inverse contracts, from **2023-06-25 to 2024-10-14**. Nothing was published after
that date. The COIN-M files are also sampled: they record the
1-per-second forceOrder stream (see below). None of the three venues publishes a
historical liquidation record that is free and complete for the discovery window.

## 1. data.binance.vision: USDⓈ-M datasets

Sources: listings from `https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?prefix=data/futures/um/...`.
"Window" means the daily files dated 2025-07-28 to 2026-06-30 (338 days). Sizes are the
zipped sizes in the listing. The "post" column is the 101 days from 2026-07-01 to 2026-10-09. Those sizes
come from the listing only. Nothing was downloaded from that range.

| dataset (daily) | first date (BTC / ETH / SOL / XRP / DOGE / HYPE / SUI) | last | window total, 6 symbols* | BTC per day (zip) |
|---|---|---|---|---|
| aggTrades | 2019-12-31 / 2019-12-31 / 2020-09-14 / 2020-01-06 / 2020-07-10 / 2025-05-30 / 2023-05-03 | 2026-10-09 | **23.9 GB** | 20 MB typical, 112 MB on 2026-02-05 (634 MB as CSV) |
| trades | 2019-09-08 / 2019-11-27 / 2020-09-14 / 2020-01-06 / 2020-07-10 / 2025-05-30 / 2023-05-03 | 2026-10-09 | 55.2 GB | 155 MB on 2026-02-05 (1.04 GB as CSV) |
| bookDepth | 2023-01-01 (SUI 2023-05-03, HYPE 2025-05-30) | 2026-10-09 | 1.01 GB | 0.5 MB |
| metrics | 2020-09-01 (BTC); 2021-12-01 (others); SUI/HYPE at listing | 2026-10-09 | 23 MB | 11 KB |
| klines 1m | same as aggTrades | 2026-10-09 | 122 MB | 67 KB |
| klines 1s | **does not exist** for um. The intervals are 1m to 1mo only | — | — | — |
| bookTicker | **2023-05-16 to 2024-03-30 only** (all symbols) | 2024-03-30 | **0 (none in window)** | ~165 MB |
| liquidationSnapshot | **does not exist** for um | — | **0** | — |
| COIN-M liquidationSnapshot | BTCUSD_PERP, ETHUSD_PERP, SOLUSD_PERP, XRPUSD_PERP, DOGEUSD_PERP 2023-06-25; SUIUSD_PERP 2024-09-19 | **2024-10-14** | 0 | 2.5 KB (1.2 MB for 472 days of BTC) |

\*The six symbols are BTC, ETH, SOL, XRP, DOGE and HYPE. Monthly zips exist for aggTrades, trades,
bookTicker, klines and fundingRate up to 2026-09. Their sizes match the daily files, and the monthly aggTrades for the six symbols total about 25.8 GB over 2025-07 to 2026-06, which includes all of July.
Holdout sizes, from the listing only: aggTrades for the six symbols total 4.6 GB over the 101 post-cutoff days.

**Sample schemas.** Each sample is one day of BTCUSDT on 2026-02-05, and each SHA-256 matched its `.CHECKSUM`.
- aggTrades: `agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time(ms), is_buyer_maker`. The day has 9.54 M rows, at most 2,181 per second, and rows in 86,321 of 86,400 seconds. There is no liquidation flag. According to Binance, insurance-fund and ADL trades are excluded ("only market trades").
- trades: `id, price, qty, quote_qty, time, is_buyer_maker`. The day has 19.58 M rows, at most 6,093 per second, and no flag.
- bookDepth: `timestamp, percentage, depth, notional`. These are cumulative depths at ±0.2/1/2/3/4/5% from mid, snapshotted 2,776 times a day (about every 31 s). They are not level-by-level and not top of book.
- metrics: open interest (`sum_open_interest`, `_value`), long/short ratios and the taker long/short volume ratio, all at **5-minute** cadence (288 rows a day). The zip entry for 2026-02-05 is dated 2026-08-15, so files can be regenerated. Checksums should be pinned at download time.

## 2. Is Binance liquidation data complete? No, it is sampled to 1 per second per symbol

- The Binance docs for `<symbol>@forceOrder` and `!forceOrder@arr` say: "Only the latest one liquidation order within 1000ms will be pushed as the snapshot". The sampled order is the **latest**, not the largest. The rule is the same for USDⓈ-M and COIN-M ([usds-m](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams), [coin-m](https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Liquidation-Order-Streams)). Coin Metrics documents the same behaviour, and dates it to a 2021-04-27 API change ([coinmetrics](https://docs.coinmetrics.io/market-data/market-data-overview/market-liquidations)).
- **The historical file has the same limitation.** The sample is COIN-M `BTCUSD_PERP-liquidationSnapshot-2024-08-05`, an 18 KB file from a crash day.
  - Its schema is `time, side, order_type, time_in_force, original_quantity, price, average_price, order_status, last_fill_quantity, accumulated_fill_quantity`. These are exactly the fields of a forceOrder stream event.
  - It has 1,904 rows, which are exactly 952 distinct events each written twice.
  - Between distinct events, the minimum gap is **970 ms** and the 1st percentile is 997 ms. All 14 gaps under 1 s are 970–999 ms, which is timer jitter. **No second contains two distinct events.**
  - 210 of the 951 gaps fall within 950–1,100 ms, and the longest back-to-back run is 11 seconds. In those stretches the stream was saturated, so the true count was larger.
  - Conclusion: the file is a recording of the throttled stream. Per-second **presence** is close to complete, but counts, notional and the median forced price are not.
- **Alternatives on Binance, assessed honestly:**
  - (a) aggTrades bursts combined with the 5-minute OI drop from `metrics`. aggTrades shows taker orders, which can be grouped by `transact_time` and side, but it carries no liquidation flag. A 5-minute OI drop cannot tell liquidations from stop-losses or voluntary deleveraging inside a 1-second cascade, so the labels would be noisy. This route can identify "forced-like" bursts, but it cannot produce a median forced print.
  - (b) Use Hyperliquid's complete liquidation tape, which is already local, as the cascade clock, and Binance aggTrades as the venue where the outcome is measured. That design is H3 rather than H2.
  - (c) Record `!forceOrder@arr` live from now on. It stays throttled and only covers the future.

## 3. Bybit and OKX

- **Bybit** ([public.bybit.com/trading](https://public.bybit.com/trading/)): free daily tick trades.
  - Coverage: BTCUSDT from 2020-03-25, ETH from 2020-10-21, SOL from 2021-06-29, XRP from 2021-05-13, DOGE from 2021-06-02 and SUI from 2023-05-03, all through 2026-10-09.
  - Schema: `timestamp(100 µs), symbol, side, size, price, tickDirection, trdMatchID, grossValue, homeNotional, foreignNotional, RPI`. There is **no liquidation flag**. This was checked on a 4 KB byte-range of the BTCUSDT file for 2026-02-05.
  - Window size, from 2,028 HEADs: BTC 24.9, ETH 27.8, SOL 11.4, XRP 9.9, DOGE 9.0 and SUI 5.0 GB, about **88 GB** in total. HYPEUSDT on 2026-02-05 is 94 MB.
  - **No historical liquidations.** The `allLiquidation` WebSocket pushes all liquidations every 500 ms, but only in real time ([docs](https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation)).
- **OKX** (`GET /api/v5/public/market-data-history`, free): tick trades (module 1), 400-level L2 order books (module 4), and 5000-level books from 2025-11-01 (module 5).
  - Window trade zips, 2025-07 to 2026-06: BTC 6.6, ETH 10.7, SOL 3.5, DOGE 2.8, XRP 2.0 and SUI 1.5 GB, about **27 GB** in total. BTC goes back to at least 2022-01.
  - The L2 books are large: BTC 400-level is 0.69–1.43 GB per day, and SOL/XRP/DOGE are 0.15–0.5 GB per day.
  - **No liquidation module.** `GET /api/v5/public/liquidation-orders` returns only about the last day, and a query for records before 2026-07-01 returned 0 rows. The WS docs say the channel "doesn't represent the total number of liquidations on OKX".
  - The OKX L2 history is the only free source of full depth, which is useful for a queue model.

## 4. Bytes for the H2 upper-bound test, discovery window, six symbols

| set | Binance (zip) |
|---|---|
| **Minimum (aggTrades + metrics + bookDepth)** | **25.0 GB**: aggTrades 23.9, bookDepth 1.0, metrics 0.02 |
| Adding raw trades (not needed; aggTrades groups the same fills) | +55.2 GB |
| Adding klines 1m (convenience) | +0.12 GB |
| Liquidation records | **not available** |

Unzipped, aggTrades is about 5.7 times larger, roughly 135 GB as CSV, so it should be converted to Parquet day by day.

**Proposed alts: XRPUSDT, DOGEUSDT and HYPEUSDT.** The case rests on liquidity on both venues, not on returns.
- Binance aggTrades bytes over the window: HYPE 2.6 GB, XRP 1.8 GB, DOGE 1.5 GB, against SUI 1.4 GB.
- Hyperliquid forced notional in the discovery window (local `liquidations.parquet`): HYPE $1.77 B, XRP $0.77 B, DOGE $0.31 B, against SUI $0.13 B.
- HYPE has no Binance history before 2025-05-30. XRP and DOGE run back to 2020.

**Earlier years (more spiral days).**
- aggTrades runs back to 2019-12/2020 for BTC, ETH, XRP, DOGE and SOL, about **104 GB** for 2020 to 2025-07-27 across the six symbols.
- metrics starts in 2020-09 for BTC and in 2021-12 for the others.
- bookDepth starts in 2023.
- The only liquidation files are the COIN-M sampled ones for 2023-06 to 2024-10.

## Recommendation

H2 as written cannot be tested. On no venue is there a free, complete historical liquidation
record for 2025–26, so cascades cannot be rebuilt "from Binance's liquidation
records", and the median forced print on Binance cannot be observed. The best free alternative is to keep
**Hyperliquid's complete liquidation tape as the cascade definition**, which is already local, and to
measure the outcome on **Binance aggTrades**, which are millisecond ticks with the taker side: 25 GB for six
symbols in the window. OKX trades (27 GB) and OKX L2 books can serve as a second venue and as the source for the
queue model. This merges H2 into H3. It answers whether HL-defined cascades overshoot and
revert on the deepest book, at second granularity. It does not answer what a fill at
Binance's own median forced print earns. A Binance-native cascade
definition, built from aggTrades bursts plus 5-minute OI drops, can be registered as a
secondary trial, with its label noise stated up front. If that is wanted, start recording
Binance `!forceOrder@arr` and Bybit `allLiquidation` live now. The recording is forward-only, and Binance's
remains 1 per second.

## Deviations to disclose

- One COIN-M liquidationSnapshot day was downloaded (BTCUSD_PERP, 2024-08-05, 18 KB) to settle the
  question of the snapshot schema and throttling. It falls outside the "BTCUSDT, 2025-08 to 2026-06" sample rule. It is not
  holdout data.
- **The holdout boundary was crossed once, by accident:** an unbounded probe of OKX
  `public/liquidation-orders` returned the latest 100 BTC-USDT-SWAP liquidations
  (2026-10-09 18:36 to 2026-10-10 13:22 UTC): sizes, bankruptcy prices and sides. They were printed to the terminal
  and not saved. No Binance or Hyperliquid data from 2026-07-01 or later was fetched, and nothing was computed on it.
