# H11 scout: who wins lending-protocol liquidations on a mid-size chain

2026-10-10. Market-structure measurement, not a return test, not a trial.
Context: `docs/forced-flow-research-horizons-2026-10-10.md` §4 H11 and
`docs/hlp-benchmark-2026-10-10.md` §1/§8. Pre-stated stop rule (H11): if the
share of liquidations won by addresses **outside the top 10** over one month on
a mid-size chain is **under 10%**, H11 stops.

Code: `research/copytrade/scouts/h11_liquidations.py` (stdlib, keyless,
read-only RPC; run with `.venv/bin/python -I`). Raw pulls:
`research/copytrade/data/scout_h11/<chain>/` (gitignored).

## 1. Short answer

- **Venue: Aave v3 on Arbitrum, September 2026.** Fully measurable on free,
  keyless endpoints, with each liquidation priced by Aave's own oracle at
  its block. Base and Polygon were not feasible without a key.
- **It is a small, quiet market.** September had 145 liquidations, $0.49 M of
  debt repaid and **$30 k of gross bonus** for the whole month. The median
  liquidation was **$1.60**: 90 of the 145 were dust under $10. Only 22
  were above $1 k. Two events, $278 k and $136 k, were 85% of the USD.
  Gas was cents ($7 for the month).
- **Winners: 26 distinct liquidator contracts.** Every winning liquidator is a
  contract, and none is the tx sender. By count, top-1 / top-3 / top-10 won
  25.5% / 56.6% / 87.6%. **12.4% went to addresses outside the top 10.** By
  USD, with the top 10 ranked by USD: 56.6% / 94.2% / 99.3%, so **0.7%
  went outside**.
- **Stop-rule reading: the rule does not fire on the stated metric, only
  narrowly, and the result is fragile.** By count, 12.4% (≥ 10%). By USD, the
  answer depends on how "top 10" is ranked: 0.7% if the top 10 is ranked by
  USD, 86% if it is ranked by count (those two one-off winners). On
  economically meaningful liquidations (≥ $100) outsiders took 10.5%
  (4 of 38) in September, **0%** in August 2026, and **8.5%** in the crash month
  October 2025, the only window with a real sample (n = 1,132, $18.7 M).
- **The contest has changed shape.** In Aug–Sep 2026, **96–99% of liquidated
  USD went through one shared contract that reports itself as "Atlas ETH"
  (atlETH)**, apparently an auction (OEV-style) dispatcher with competing
  solver contracts. In October 2025 there was none of it, and the crash-night
  winners paid a median **$37 of gas per liquidation tx** (max $816) in a priority-gas war.
  So in quiet months the open part of the contest is an auction, and in the
  crash it was a gas race. HLP's money was made in the crash.

## 2. Venue and why

**Aave v3 on Arbitrum** (Pool `0x794a61358d6845594f94dc1db02a252b5b4814ad`).

- It is the one mid-size lending market where the whole measurement is
  possible on free, keyless endpoints: the official Arbitrum RPC
  (`arb1.arbitrum.io/rpc`) serves `eth_getLogs` over 1M-block ranges (a month
  is 11 calls), and Blast's public endpoint (`arbitrum-one.public.blastapi.io`)
  serves historical `eth_call`, so every liquidation is priced with **Aave's
  own oracle at the liquidation block** rather than a month-midpoint guess.
- Mid-size by construction: Aave's second-tier deployment, not Ethereum
  mainnet; liquidation gas costs cents.
- Other candidates, checked 2026-10-10:

| Candidate | Keyless route | Outcome |
|---|---|---|
| Aave v3 Base | `mainnet.base.org` returns 429 "request limit reached" on every `eth_getLogs`, even 1k blocks; publicnode 403; Blast public 400 on logs (archive `eth_call` works) | **not feasible keyless**; needs a keyed RPC or indexer |
| Aave v3 Optimism | official RPC: logs OK (≤125k-block spans); no keyless archive found | measured with DefiLlama midpoint prices (§4) — **dust-only month** |
| Aave v3 Avalanche | official C-chain RPC: logs OK (500k spans); no keyless archive | measured with midpoint prices (§4) |
| Aave v3 Polygon | `polygon-rpc.com` 401, publicnode 403, Blast 403 | not feasible keyless |
| HyperEVM (HyperLend, Hypurrfi) | `rpc.hyperliquid.xyz/evm`: max 1,000 blocks per `eth_getLogs` and "rate limited" at once; ~2.6M blocks/month → ~2,600 throttled calls, and history depth unknown | not attempted this pass; feasible but slow, pool addresses to be confirmed from the protocols' docs |
| Compound v3, Morpho Blue | logs are public, but different events (`AbsorbCollateral`/`BuyCollateral`; Morpho `Liquidate`) and Morpho's volume is mostly on Base/mainnet (Base logs blocked above) | not done; same script shape would work on Arbitrum |

## 3. Data route

1. Block bounds by binary search on block timestamps; window
   2026-09-01 00:00 → 2026-10-01 00:00 UTC (blocks 500,455,225 → 510,496,864).
2. `eth_getLogs(address=Pool, topic0=keccak("LiquidationCall(address,address,address,uint256,uint256,address,bool)"))`
   = `0xe413a321…5286` (keccak implemented in the script and checked against
   two known hashes).
3. Per event: `eth_getTransactionByHash` (tx.from, tx.to), `eth_getTransactionReceipt`
   (status, gas used × effective gas price), Aave oracle `getAssetPrice` for the
   debt asset, the collateral asset and WETH at the event's block (oracle found
   via `Pool.ADDRESSES_PROVIDER().getPriceOracle()`), `eth_getCode` for each
   liquidator and tx.to.
4. Traffic: September ≈ 720 calls, ~5 MB; all five windows ≈ 7,000 calls, 51 MB on disk (cache included).

Options that need a key/account/payment (not used; for the owner): The Graph
gateway (Aave v3 subgraphs, needs an API key and GRT billing), Dune (account;
`aave_v3_arbitrum.Pool_evt_LiquidationCall` and `ethereum`-style `transactions`
tables would give gas and failed txs in one query), Alchemy/Infura/QuickNode
(free tiers with key: archive state + `debug_trace*` for Base/Polygon),
Arbiscan/Basescan (key; per-address tx lists incl. reverted ones).

## 4. Numbers

All figures: successful `LiquidationCall` events, USD at the Aave oracle price
at the event block (Arbitrum) or one DefiLlama price per asset at the month
midpoint (Optimism, Avalanche; marked ★). "Liquidator" = the event's
`liquidator` field (msg.sender into the Pool, i.e. the bot contract), used as
the economic-owner proxy. Reports: `data/scout_h11/report_<chain>_<month>.txt`.

### 4.1 Primary: Aave v3 Arbitrum, 2026-09

| | value |
|---|---|
| liquidation events / txs / distinct borrowers | 145 / 144 / 138 |
| debt repaid, total | $490,686 |
| median / mean / max per event | $1.60 / $3,384 / $277,631 |
| events ≥ $10 / ≥ $100 / ≥ $1 k (median of those) | 55 ($538) / 38 ($1,129) / 22 ($1,955) |
| gross bonus (collateral seized − debt repaid, oracle USD) | $30,345; median 4.5% of debt on ≥ $1 k events |
| gas paid by winning txs (whole month) | $7.39; median $0.04 per tx |
| winners that are contracts / == tx.from / == tx.to | 145 / 0 / 89 |
| Timeboost (express lane) used by winners | 1 of 145 |
| routing: direct / via "Atlas ETH" contract / other | 89 events (1.0% of USD) / 53 (98.8%) / 3 (0.2%) |

Concentration (top-10 ranked by the same metric):

| identity | metric | distinct | top-1 | top-3 | top-10 | **outside top-10** |
|---|---|---|---|---|---|---|
| liquidator contract | count | 26 | 25.5% | 56.6% | 87.6% | **12.4%** |
| liquidator contract | USD | 26 | 56.6% | 94.2% | 99.3% | **0.7%** |
| tx.from (EOA) | count | 31 | 26.2% | 49.0% | 81.4% | 18.6% |
| tx.from (EOA) | USD | 31 | 58.3% | 93.6% | 99.0% | 1.0% |
| tx.to | count | 17 | 36.6% | 77.2% | 95.2% | 4.8% |
| tx.to | USD | 17 | 98.8% | 99.4% | 100% | 0.0% |

The same, liquidator contract only, by size tier:

| tier | n | distinct | outside top-10, count | outside top-10, USD (USD-ranked) | outsiders' USD if top-10 ranked by count |
|---|---|---|---|---|---|
| all | 145 | 26 | 12.4% | 0.7% | 86.3% |
| ≥ $100 | 38 | 14 | 10.5% | 0.6% | 84.7% |
| ≥ $1 k | 22 | 12 | 9.1% | 0.5% | 85.6% |

The 86% is two events. The $277,631 WBTC and $136,091 USDC/weETH
liquidations were each won by a contract that won nothing else in the month
(`0x754bedb7…`, `0x34023f11…`), both via the Atlas contract. The outsiders
(by count) are 16 addresses with 18 events, median $48. 14 addresses won
exactly once. The top contract by count (`0x24c1aa9a…`, 37 wins) repaid $0.17
in total: dust cleanup.

tx.from vs liquidator. Winners never call from the liquidator address, and
big bots fan out over many EOAs: `0xd12810b1…` used 10 EOAs in September.
In October 2025 one contract used 140. So EOA concentration understates
operator concentration: by tx.from, October 2025's outside-top-10 is 67% of
count and 29% of USD. The contract is the better owner proxy. tx.to is the
worst, because the Atlas contract is shared infrastructure.

Bytecode check: each of the 26 liquidator contracts has distinct bytecode.
There are no clone families, so no obvious "one operator, many contracts".
Fresh per-liquidation deployments by an incumbent cannot be ruled out
without deployer data.

### 4.2 Robustness windows (same code)

| window | events | USD repaid | median | distinct liq. | outside top-10, count (all / ≥ $100) | outside top-10, USD (USD-ranked / count-ranked) | gross bonus | gas (winners) |
|---|---|---|---|---|---|---|---|---|
| **Arbitrum 2026-09** | 145 | $0.49 M | $1.60 | 26 | **12.4% / 10.5%** | **0.7% / 86.3%** | $30 k | $7 |
| Arbitrum 2026-08 | 122 | $0.28 M | $11 | 25 | 16.4% / 0.0% | 0.1% / 3.3% | $13 k | $9 |
| Arbitrum 2025-10 (crash month) | 1,132 | $18.7 M | $1,592 | 25 | 11.9% / 8.5% | 1.2% / 16.5% | $1.11 M | $59 k |
| Avalanche 2026-09 ★ | 57 | $0.17 M | $0.94 | 16 | 10.5% / 0.0% | 0.0% / 0.7% | (★ unreliable) | $2 |
| Optimism 2026-09 ★ | 569 | $1.2 k | $0.00 | 10 | 0% / 0% | 0% / 0% | $68 | $64 |

- **October 2025** is the only window with a sample big enough to mean
  much. About 884 of its 1,132 events and $17.5 M of its $18.7 M fell in the
  night of 10–11 October (20:00–04:00 UTC, block-interpolated). In that
  night 21 contracts won. Outside the count-ranked top 10: 8.6% of events
  and 0.9% of USD (USD-ranked). Top-1 by USD held only 18%, and top-10
  held 98.8%. Winners paid a median $37 per tx in gas. No Atlas routing yet.
- **Optimism**: the month was all dust ($1.2 k in total), and 10 contracts
  won everything. It is not a venue.
- **Avalanche** ★: tiny. With midpoint prices the "bonus" comes out at
  30% (median on ≥ $1 k events), which is a price artifact. Only counts
  are trustworthy.

## 5. Stop-rule reading

Pre-stated: "if under 10% [won outside the top 10], stop". Primary window:
Arbitrum Aave v3, September 2026.

- **By count: 12.4%, so the rule does not fire.** That is 18 of 145 events,
  and it falls to 10.5% on events ≥ $100 and to 9.1% on events ≥ $1 k.
- **By USD: under 10% if the top 10 is ranked by USD (0.7%), so on that
  reading the rule fires.** If the top 10 is ranked by count, outsiders took
  86%, all of it from two one-off winners of the two biggest liquidations.
  With about 25 distinct winners a month, one large win puts any newcomer
  into the USD top 10. The USD-ranked figure is therefore close to
  tautological, and it is a weak test of "can a newcomer win".

**Reading: the stop rule does not cleanly fire, and the count figure clears
the bar narrowly. The result is not robust.** Of the three Arbitrum
windows, the economically meaningful (≥ $100) outsider share is 10.5%, 0%
and 8.5%. The one large-sample month, the crash month that matters for an
HLP-like backstop return, is **8.5% by count and 1.2% of USD**. A
conservative owner would treat that as a stop for "be a liquidation bot on
Aave". It is not a stop for the narrower question the September data
raises: can a non-incumbent win **auctioned** liquidations (Atlas route)?
Two one-off contracts won $414 k of the $491 k there, but that needs the
operator check in §7 before it means anything.

Economic scale, for the HLP bar (≥ 25 bp/fill or ~20%/yr in a spiral
year): the whole Arbitrum Aave v3 gross bonus was **$13–30 k a month** in
quiet months. It was **$1.1 M** in the crash month, gross of $59 k gas,
swap costs and, on the auction route, the winning bid. Contracts outside the
count-ranked top 10 took **$165 k gross bonus** in the crash month, spread
over 15 addresses.

## 6. What was not measured, and how one would

- **Failed competing attempts.** Only winners emit `LiquidationCall`. Losers
  revert (or never land). On Arbitrum's FCFS sequencer a loser usually lands
  and reverts. Measure: scan the blocks around each liquidation for reverted
  txs to the Pool / to known liquidator contracts (`eth_getBlockByNumber` full
  + receipts — keyless but ~10–100× the traffic), or Dune's `transactions`
  with `success = false`.
- **Bids paid inside the auction route.** For Atlas-routed liquidations the
  winner pays a bid out of the bonus; the event's seized-minus-repaid
  overstates the liquidator's take. Measure: decode the Atlas metacall's
  solver operations / bid token transfers in the same receipt.
- **Timeboost.** Arbitrum receipts carry a `timeboosted` flag (express-lane
  auction): 1 of 145 winners in September, 3 of 122 in August, all dust, so
  the express lane is not how liquidations are won here. Not tabulated for
  October 2025.
- **Swap / flash-loan costs, inventory risk, and who the economic owner is.**
  Liquidator contract addresses are a proxy for operators; one operator can
  run several contracts and many EOAs. Bytecode clustering was done once
  (each top liquidator has distinct bytecode, so no obvious one-operator
  cluster); deployer lookup needs an explorer API or trace data.
- **Builder inclusion.** Not applicable on Arbitrum/Optimism (single
  sequencer, FCFS + Timeboost); relevant on Base (priority ordering) and on
  mainnet (PBS) only.

## 7. Next step, if H11 continues

1. **Operator check (cheap, needs an explorer key or a trace-capable RPC):**
   the deployer of each liquidator contract, to tell whether the one-off
   September winners are newcomers or incumbents' fresh contracts. A free
   Arbiscan key, or Dune, would do it. Both need a sign-up, so the owner
   decides.
2. **Confirm what the "Atlas ETH" contract is** (`0x8ad1ae9d97c79aa68a0a151e83ff3942f68f86c1`
   on Arbitrum). Confirm whether Aave on Arbitrum routes liquidations
   through an OEV auction, who may bid, and how much of the bonus the
   winning bid gives back. If the auction is open to anyone, the "contest"
   is a sealed-bid auction. The relevant measurement is then winning bid ÷
   bonus, not who is fastest.
3. **Measure the losers:** reverted liquidation attempts per event (block
   receipts around each liquidation; keyless but ~10–100× traffic), for the
   crash night of 2025-10-10 first.
4. Only then a paper bot: watch health factors on Arbitrum Aave v3 and log
   which liquidations we *would* have submitted, with what latency, against
   the realised winner. No transactions.

Expectation is unchanged from the horizons doc (1 in 6 or worse). The
measured market is small outside crashes, and in the one crash it was a gas
race among ~20 contracts.
