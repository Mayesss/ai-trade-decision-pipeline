# 000k — N2 scout: unconditional vol levels and a published short-vol backtest

Ledger entry, not a hypothesis. Written 2026-10-12 by the orchestrator, from
the N2 options scout's report (`research/copytrade/scouts/n2-options.md`;
numbers in `data/scout_n2/analysis_all.txt`).

## What happened

1. **Unconditional levels printed.** The DVOL median (59.4) and the
   underlying's weekly SD (BTC 9.0%, ETH 11.3%), the latter over 2019–2023
   plus 2025-07-28 → 2026-06-30 (009's holdout A excluded). With the
   stylised premium (7% / 9% of spot), these allow a rough
   implied-versus-realised comparison. **It was never computed;** the
   windows differ; no map- or stress-conditional version exists.
2. **Underlying-only payoff shapes.** The payoff of hypothetical 1-SD OTM
   strangles and puts (no premium, no IV) over the same weeks: share of
   payoff in the top 1/3/5 weeks, SD/mean. These are properties of the
   underlying's distribution, used for power.
3. **A stylised short-straddle tail** (constant premium, underlying moves):
   worst week −35% (BTC) / −55% (ETH), worst 4 weeks −61% / −73%.
4. **Literature.** A Deribit Insights weekend-strangle backtest (27.4% APR,
   23.8% max drawdown, 2020 → 2024-08) overlaps 009's holdout A; the
   Almeida et al. VRP estimate and Amberdata's "IV above RV ~70% of the
   time" are public priors on the sign.
5. **A deleted pull.** Deribit daily perp candles stamped 08:00 UTC (the
   last ran ~8 h into 2026-07-01; the 2023-12-31 one into holdout A) were
   deleted before any value was read; only the stamp hour was printed.

## What must not follow from it

- No short-vol parameter (tenor, moneyness, sizing, filter threshold) may be
  set from items 1–3 beyond power and tail inputs, labelled as such.
- The sign of the VRP is a public prior; its size on our calendars is
  unknown to our code. A registration states that 009's holdout A is fresh
  to our computations but not to published short-vol backtests.

## Mitigation

A short-vol registration fixes tenor, moneyness and sizing from cost
arithmetic, liquidity and the published literature, and decides on a
calendar our code has not priced.
