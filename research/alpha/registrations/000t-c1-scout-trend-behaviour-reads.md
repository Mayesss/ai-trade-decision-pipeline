# 000t — C1 scout: trend-following behaviour statistics on Binance perps, 2020 → 2026-06

Ledger entry, not a hypothesis. Written 2026-10-13 by the orchestrator, from
the C1 scout's report (`research/alpha/scouts/c1-crypto-trend.md`; outputs in
`research/alpha/data/scout_c1/derived/`).

## What happened

1. **Pulls** (checksummed; every Binance file matched its `.CHECKSUM`; one
   404, LUNA 2022-06): Binance USDⓈ-M 1d klines 2019-12 → 2026-06 for 787
   symbols; funding for 1,293 top-20 member-months; Bitget candle open times
   for 116 symbols (listing existence only); the bounded HL first-listing
   file from the L2 scout. 2,491 archive keys dated after 2026-06 were
   counted, not fetched. The orchestrator checked: last kline and funding
   month 2026-06, all checksums pass.
2. **Computed, unconditional or signal-state-only:** point-in-time universe
   counts and liquidity; Donchian sub-signal flips per coin-month (≈ 11.6)
   and long fractions per lookback; turnover (≈ 4.4× capital a year) and
   gross-exposure distributions (mean 0.08 at the paper's sizing); 90-day
   vol levels; the universe's **unconditional funding level by year**
   (+5.4%/yr overall, −2.2%/yr over 2025-01 → 2026-06); equal-weight basket
   return correlations by year (0.89–0.97 between ranks 1–20 and 21–100);
   BTC/ETH/basket moves in five pre-named stress episodes (2020-03-12 day:
   −43% for the equal-weight top 20).
3. **Not computed:** any return after a signal, any strategy P&L or equity
   curve, funding by signal state, any comparison of lookbacks or universes
   by outcome.
4. **Designer knowledge (disclosed):** the drafting and orchestrating model's
   training data cover the crypto market path through about mid-2026, i.e.
   span (a) 2025-03-20 → 2026-06-30 is not unseen by the people choosing to
   test it.

## What must not follow from it

- No trend rule parameter may be set from these statistics: the rule is the
  paper's, verbatim (lookbacks {5, 10, 20, 30, 60, 90, 150, 250, 360}, 25%
  per-coin vol target, 200% cap, top 20 by volume, 10 bp costs, 20%
  rebalance threshold).
- Span (a) may be a stratum of a trend registration, never its sole decider;
  the registration states designer knowledge, `000l` (the 2026-09-14
  alpha-lab sweep computed momentum ICs on 83 Bitget perps through 2026-09,
  inside any forward span starting 2026-07-01) and `000q` (post-move path
  quantities on seasoned coins, Binance 2021-12 → 2024-01).

## Mitigation

016 (crypto trend, verbatim) is registered before its forward span is
read; its guard refuses any read before the registered end date + 7 days.
