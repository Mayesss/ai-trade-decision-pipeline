# A1 scout: "be the house" (counterparty-side products that monetise persistent retail losers)

2026-10-13. Round 4, scout A1. Behaviour only. **No return, mean, Sharpe,
drawdown, volatility or hit-rate was computed from any NAV, share-price or
pool-value series.** Every return figure below is a *public prior*: quoted
from a source I opened, with its date and period. The only numbers computed
here are coverage counts, plus Kelly arithmetic on stated assumptions (§2.3).

## 1. Summary

1. **There are two kinds of house product. Only one of them is HLP's kind.**
   - *Backstop vaults* (HLP, Lighter **LLP**) take over positions the order book cannot absorb. They are long the spiral tail, as HLP is.
   - *GLP-style pools* (Jupiter **JLP**, GMX **GM/GLV**, Aster **ALP**, Avantis, Ostium, gTrade) are the oracle-priced counterparty to every trade, and they carry basket beta. Delta-hedged, they earn fees plus traders' net losses, but they are short traders' winning trends. Their skew is the opposite of HLP's.
2. **Best candidate: Lighter LLP.** It is a near-copy of HLP's design: market making, takeover of fully liquidated positions, and ADL backstopped by LLP. It runs on a retail-heavy, zero-fee venue. Since 2026-01, depositing requires staking 1 LIT per 10 USDC, which keeps outside capital out. Free, keyless, **end-datable** daily history: 529 days, 2025-01-18 → 2026-06-30.
3. **Second: a delta-hedged GLP-style pool.**
   - **JLP** has the largest flow of losing traders. Its free history is thin: 259 daily market prices, 2025-10-15 → 2026-06-30.
   - **GMX GM/GLV** has the best data: daily fee-APR and trader-PnL-APR snapshots since 2023-09, end-datable. Its loser flow is smaller.
4. **The tail that dominates every house product is the venue, not the market.** Over about 16 months the surveyed venues had five capital-impairing or capital-freezing events:
   - GMX v1 GLP drained (2025-07);
   - Lighter outage losses (2025-10);
   - Paradex chain rollback (2026-01);
   - Drift admin-key exploit, about $285 M (2026-04);
   - Ostium OLP −72% (2026-07, after the cutoff).

   Kelly sizing is therefore set by the exploit rate, which is estimated and not measured. Cap each venue at about **20–25% of capital**.
5. **Odds that something here beats HLP risk-adjusted: about 1 in 6 for LLP** (alone or as half of an HLP+LLP "house basket"), and about 1 in 10 for hedged JLP or GM. These are not powered alpha tests; see the §3 arithmetic. Hedged GLP-style pools are **lending-plus**. A backstop vault is a positive-skew wager on spirals.

## 2. Ranked products

### 2.1 Table

The "beat HLP" column means beating HLP over the same interval on Sharpe *and* on drawdown, after the exploit allowance. The HLP column is the benchmark: `docs/hlp-benchmark-2026-10-10.md`, +19.4% over 2025-08 → 2026-07, Sharpe 1.4, max DD −2.5%, about 2–4%/yr in calm periods.

| # | product | mechanism; who pays and why it persists | return-source split | hedgeability, cost | public prior (period) | tail: worst documented; worst plausible week / month at stated size | size (Kelly) | capacity $10k / $100k / $1M | access | free data (end-datable?) | odds: effect exists / beats HLP |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | **Lighter LLP** (Ethereum zk-rollup perp DEX) | Protocol pool in a single account. It quotes the book, takes 1% fees on partial liquidations, takes over full liquidations, and **backstops ADL** ([docs][lt1], [strategies][lt2]). Payers: over-levered retail on a zero-fee venue, plus spiral flow the book cannot absorb. It persists because the protocol hands the backstop only to LLP, as HL does with HLP. | Market making + liquidation fees + backstop takeovers. No basket beta in USDC. Since 2026, segregated strategy buckets (crypto / FX / RWA) cap each bucket's loss. XLP is a separate pool for pre-launch and RWA markets. | No beta to hedge in USDC. **LIT gate:** 1 LIT staked per 10 USDC ([The Block, 2026-01-14][tb-lit]). At the pool's share price this adds LIT exposure; a short LIT perp, if one exists, would offset it at the funding cost (not verified). | **>250% "YTD" 2025** on USDC, a depositor's claim from the private-beta era on small TVL ([0xjermo, 2026-01-02][jermo]). Not a forecast. | **2025-10-10/11:** the cascade took the database offline and the unified pool "suffered losses that HLP reportedly avoided" ([Datawallet][dw]; size not verified). **2026-02:** the ARC squeeze ($8.2 M one-sided loss) was capped at about $75k for LLP by segregation ([Datawallet][dw]). Plausible at 25% of capital: worst non-exploit week about −1 to −2.5% of capital (a −5…−10% LLP day). Venue exploit: −17% of capital (−70% of the deposit). | Quarter to half Kelly → **20–25%** (§2.3) | $10k and $100k: yes. $1M: needs 100k LIT staked; withdrawals have a 5-minute cooldown ([eth_defi constants][edf]). | Non-US individual: ToS not verified. LIT must be bought and staked. | **Yes.** `/api/v1/pnl`, daily, `start_timestamp`/`end_timestamp`: **529 days, 2025-01-18 → 2026-06-30**. XLP: 280 days, 2025-09-24 → 2026-06-30. 0.27 MB. | 0.6 / **0.17** |
| 2 | **Jupiter JLP**, delta-hedged by us (Solana) | Basket pool, about 45% SOL, 16% WBTC, 8% WETH, 30% USDC ([weekly report, 2025-12-08][jw-dec]). Counterparty to every Perps trade. Payers: retail traders, persistently long-skewed: "frequently over 95% long" ([DeFi Jangle, 2024-11-30][dj]). | 75% of fees (open/close, price impact, borrow, swap, mint/burn, liquidation) plus traders' net PnL plus basket beta ([Earn docs][jup-earn]). Traders' realized PnL in sample weeks: −$10.0 M net (week to 2025-03-31) ([risk report][jw-mar]); −$54.2 M net (week to 2025-10-13) ([weekly][jw-oct]). | Short SOL/BTC/ETH perps on HL or Binance. Long-skewed markets usually pay the short. Costs: rebalancing taker fees, plus the gap between the JLP virtual price and its market price. The protocol's own delta-neutral vault is **reduce-only** ([docs][jup-dn]). Gauntlet's hJLP hedged on **Drift** ([vaultbook][gt]), which was exploited in 2026-04. | Unhedged: average +9.22% a month, Sharpe 1.44, Jan–Jul 2024, a bull market ([Vaultka, 2024-08-19][vk]). Fees "30–80% APR" in neutral conditions, period not stated ([DeFi Jangle][dj]). **Trader-win episode:** from 2024-09-01, SOL +82%, traders +$150 M, JLP +33.5% ([DeFi Jangle][dj]). | Unhedged 2024-04-11→13: −16.1% ([Vaultka][vk]). Hedged leg in the week to 2025-10-13: JLP −5.07% against its basket ([weekly][jw-oct]; the report's basket "0.00%" reads oddly). Hedge-side tail: HL **ADL'd profitable positions** on 2025-10-10 ($2.0 B of HLP's own) ([HLP doc §2][hlp]). A short hedge can be closed at the worst moment. Plausible at 20%: worst week −1.6% of capital; trader-win month −1 to −2%; Solana/Jupiter exploit −16%. | Quarter Kelly → **15–20%** | Mint can hit the AUM limit (then buy on secondary at a premium); 0.20% base mint/burn fee ([Earn docs][jup-earn]). Fine at $1M. | Owner-side check: Jupiter app terms, Solana wallet. | **Weak.** CoinGecko keyless serves only the last 365 days: **259 daily market prices, 2025-10-15 → 2026-06-30**. These are market prices, not the virtual price. The Pyth Benchmarks history endpoint returned 404 (reported deprecated or keyed); GeckoTerminal is limited to 180 days (401). Weekly trader-PnL and JLP-vs-basket figures sit in Jupiter's dated forum reports. | 0.6 / 0.10 |
| 3 | **GMX v2 GM / GLV**, delta-hedged by us (Arbitrum) | Isolated pools, e.g. ETH/USD [WETH-USDC], back one market each. GLV is a pool of GM pools. **63% of fees** go to LPs ([docs][gmx-lp]). Payers: GMX traders. GMX's announcements claim the trader-PnL effect is "small (and generally positive)" ([GLV post, 2024-09-05][gmx-glv]). | Fees + capped pending trader PnL (`MAX_PNL_FACTOR`) + a 50/50 long/short token basket ([docs][gmx-lp]). Funding passes between the two sides of open interest, not to the pool. | Short the long-token perp, about 0.5× notional. Same costs as JLP. | No verified multi-year figure. GMX publishes a fee APR and a "performance vs geometric-mean benchmark" metric ([docs][gmx-lp]). | **v1** (not v2): **2025-07-09, ~$40–42 M drained from GLP** by re-entrancy that distorted the average short price ([Halborn][hal], [GMX][gmx-rec]). Funds were returned less a $5 M bounty; GLP redemptions were frozen; v1 has been wound down. v2 GM pools were not hit. Plausible at 20%: worst week about −1% of capital (pending-PnL swing in a trending week); exploit −16%. | Quarter Kelly → **10–15%** | No lock-up; deposits and withdrawals pay price impact. Fine at $1M. | Permissionless contracts. Front-end geofence not verified. | **Best of the GLP-style pools.** The GMX Subsquid GraphQL is keyless and filterable by `snapshotTimestamp_lt`. Daily GM prices from **2023-07-05** (1,092 points per pool). Separate `aprSnapshots` (fee legs) and `pnlAprSnapshots` (trader-PnL leg) from **2023-09-26** (~1,009). GLV from **2024-09-04** (~660). All end 2026-06-30. 1.3 MB. | 0.5 / 0.07 |
| 4 | **Aster ALP** (BNB Chain, Arbitrum) | GLP-fork counterparty to Simple (1001×) mode. NAV moves with the pool's PnL ([docs][ast]). Payers: 1001× traders, the most extreme losers available. | Trading PnL vs traders + fees + funding + liquidations + platform fees ([docs][ast]). The basket composition is set by target weights. | As for JLP, by basket. | None verified. | **2025-09-25:** Aster's XPL perp printed above $4 against about $1.30 elsewhere and forced liquidations. Aster compensated; which pool paid is not stated ([The Block][tb-xpl]). | ≤ 10% | **48 h** burn cooldown. A burn is capped at 50% of (pool − own position) ([burn docs][ast-burn]). Mint/burn fee 0.25% base ([fees][ast-fee]). | Owner-side check. | None found that can be end-dated and is keyless. | 0.5 / 0.05 |
| 5 | **edgeX eLP** | Vault takes the opposite side of edgeX traders ([docs][edx]). | Passive market making + liquidation fees + a share of fees ([docs][edx]). | Little beta (it nets traders). | None verified. | None found. Redemption lock-up of at most 2 days with **no accrual** while locked ([docs][edx]). | ≤ 10% | Not verified. | Not verified. | None found. | 0.4 / 0.05 |
| 6 | **Avantis** junior / senior vault (Base) | USDC vault is the counterparty. Junior tranche "absorbs a greater share of losses". Time locks of 30/90 days earn more fees. Up to 20% loss rebates to contrarian traders ([Alea, 2025-09-02][alea]). | Fees (only winners pay on zero-fee perps) + trader PnL, tranched. The **junior tranche is the levered house variant.** | Mostly nets out. | None verified. | None found. | ≤ 10% | Locks of 30/90 days. | Not verified. | None found. | 0.4 / 0.05 |
| 7 | **Paradex Gigavault** (DIME VTF) | Protocol-operated vault on Paradex (Starknet appchain); a market-making style strategy (details not documented). | Not documented. | n/a | A third-party "~30% APR" figure is undated, so not used. | **2026-01-19:** a database migration set the BTC mark to $0, triggered mass liquidations, and the chain was rolled back to block 1,604,710 ([Yahoo][px]). **2025-10:** GigaLiquidator overload, trade busts and account resyncs; socialized losses avoided ([Coin360][px-c360]). Socialized-loss charges fall on withdrawals during a shortfall ([docs][px-sl]). | ≤ 5% | VTF lock-up of 1–4 days, owner-set ([docs][px-vtf]). | Not verified. | **No.** `/v1/vaults/history` has no from/to bounds ([docs][px-api]). | 0.3 / 0.04 |

Not examined in depth: **Extended** (Starknet; has an LP vault whose mechanism I did not verify; about an hour of downtime after the 2025-10-10/11 cascade from a partial-liquidation bug, then a postmortem and compensation; [Alea digest, 2025-11-10][ext]) and Solana GLP clones (Flash, Adrena). Each is a smaller copy of rows 1–3.

### 2.2 Why loser persistence is (or is not) passed to the depositor

- **Passed through:**
  - LLP and HLP: the account is the counterparty, and NAV is the account value.
  - JLP: pool assets are counted net of the value reserved for traders' profits ([docs][jup-earn]).
  - GM: pool value includes capped net pending PnL ([docs][gmx-lp]).
  - ALP: NAV moves with pool PnL.
- **Not passed through: gTrade.** The docs give `gToken price = 1 + accRewardsPerToken − max(0, accPnlPerTokenUsed)` ([docs][gns]).
  - Traders' net losses build an over-collateralisation buffer that "does not raise the gToken price". Above 100% collateral, a share of losses buys back and burns GNS.
  - So the depositor gets fees and is short traders' wins, but not their losses. The loser leg belongs to GNS holders. The payoff is concave. Rejected (§4).

### 2.3 Sizing (fraction of Kelly)

Model: one year, a non-jump return *r* with probability 1−*p*, and a venue impairment costing *L* of the deposit with probability *p*. Arithmetic on assumptions only:

| case | r | p | L | full Kelly | ½ | ¼ |
|---|---|---|---|---|---|---|
| LLP base (spiral year like HLP's) | 20% | 6% | 70% | 1.04 | 0.52 | 0.26 |
| LLP pessimistic | 10% | 10% | 70% | 0.29 | 0.14 | 0.07 |
| JLP hedged base | 15% | 6% | 80% | 0.78 | 0.39 | 0.19 |
| JLP hedged pessimistic | 8% | 10% | 80% | 0.00 | 0 | 0 |
| GM hedged base | 8% | 6% | 80% | 0.42 | 0.21 | 0.11 |
| HLP spiral year | 19% | 4% | 50% | 1.71 | 0.85 | 0.43 |
| HLP calm year | 3% | 4% | 50% | 0.59 | 0.29 | 0.15 |

- **Base rate *p*.** Among roughly 10 house products surveyed here, one permanent impairment of about −70% (Ostium, 2026-07) and four freezes or losses occurred in about 16 months. That gives *p* ≈ 5–10%/yr for impairment, plus about 20%/yr for a freeze.
- **Recommendation.** Quarter to half Kelly under the base case, which means **≤ 25% of capital per venue**, and less for the GLP-style pools.
- The hedged pessimistic case has a Kelly of zero. That is the honest statement: a hedged GLP pool is lending-plus, and an exploit rate of ~10%/yr erases it.

## 3. Top two: rule sketches, next steps, power

### 3.1 Lighter LLP, alone or as an HLP+LLP "house basket"

**Rule sketch (pre-registrable):**
- Deposit 50% of the house sleeve in HLP and 50% in LLP. Each is capped at 25% of total capital.
- Stake LIT at 1:10 for the LLP deposit. Hedge the LIT with a perp short at the LIT stake value if a liquid perp exists; otherwise count LIT in the sleeve's risk.
- No market timing, and no other conditioning.
- Rebalance to 50/50 quarterly, or when either leg moves more than 20% from its target.
- **Exit** a venue on any of:
  - a published exploit or oracle incident with depositor loss;
  - a change to the backstop routing or the ADL rules;
  - a LIT-gate change that turns the pool into an open-capacity pool.

**Criterion vs HLP over the same interval:**
- an annualised return difference > 0, together with a worst-day difference ≥ −2 pts and a drawdown ≤ 2× HLP's;
- decided on the components below rather than on the NAV level, because the NAV level has no power.

**Behaviour-only next step:** a feasibility note from the pulled LLP file, counts only.
- Days with any `trade_pnl` / `pool_pnl` change.
- How share supply and flows (`pool_inflow` / `outflow`) evolve: do the shares reconcile with the account value?
- Whether the 2025-10-10/11 outage days are present or missing.
- How many spiral days (004 D8 switch, from our HL archive) fall inside the LLP span.
- Also check whether LLP NAV can be rebuilt from the `/pnl` fields at all. If not, the end-datable series is PnL-and-flows only.

**Decision calendar:**
- **Discovery:** LLP 2025-01-18 → 2026-06-30 (529 daily points). Pre-public-mainnet months (beta, small TVL) are a separate stratum.
- **HLP on the same span:**
  - the `vaultDetails.allTime` 14-day grid from 2023-05 (cached);
  - the daily archive reconstruction 2025-07-28 → 2026-06-30;
  - so the overlap with daily data on both legs is **about 11 months**.
- **2026-07-01 → is held back and unread.** The `/pnl` endpoint can serve it later with an end date.

**Power:**
- Daily NAV vol of a backstop vault is about 15%/yr (HLP: 14.6%, almost all of it upside). Assume a correlation of about 0.5 between the two backstops (both are long spirals). The difference then has σ ≈ 15%/yr.
- SE of the mean difference over 0.92 yr ≈ **16%/yr**; over the 1.45-yr LLP span ≈ 12%. A 10%/yr edge at t = 2 needs **about 9 years**.
- The P&L sits in about 2 spiral events a year, so the effective N is about 3.
- **A NAV-level trial is not powered.** Only the calm-carry component is testable: ex-spiral days, with σ near HLP's downside deviation of 2.7%/yr, say 4%/yr. SE over 0.92 yr ≈ 4%/yr, so a 10%/yr carry gap would show at about t = 2.5.
- Otherwise the decision is an allocation with a priced tail, logged as such, and not an alpha claim.

### 3.2 Delta-hedged GMX GM (data) or JLP (flow)

**Rule sketch:**
- Hold GM ETH/USD [WETH-USDC] (or JLP).
- Short the long-token perp (for JLP, the SOL/BTC/ETH basket at the published weights) on HL to net delta. Rebalance when |delta| exceeds 5% of notional, or weekly.
- **Exit** on either of:
  - a pool exploit;
  - a hedge-venue ADL (re-hedge on a second venue).

**Criterion vs HLP over the same interval:**
- hedged return > HLP;
- worst week ≥ −3% of the deposit;
- no trader-win month below −5%.

**Behaviour-only next step:**
- From the pulled GMX files, coverage only: gaps, carried-over rows, and whether `pnlAprSnapshots` is a trailing window or a point value (read the squid source, not the values).
- Then a written decomposition plan: fee leg + PnL leg + hedge funding (our local HL/Binance funding to 2026-06) − rebalance cost.
- For JLP, the only cheap pre-cutoff decomposition is Jupiter's weekly forum reports (trader PnL, JLP vs basket). Tabulating them belongs inside a registration, because they are return figures.

**Calendar:** GM daily 2023-07-05 → 2026-06-30 (about 3 yr; fee/PnL legs from 2023-09-26). GLV from 2024-09-04. JLP 2025-10-15 → 2026-06-30 only. Holdout 2026-07-01 → unread.

**Power:**
- The fee leg is nearly deterministic, so its SE is negligible.
- The trader-PnL leg is the risk. With about 3 years and σ(PnL leg) of about 8–10%/yr (assumed; not measured), SE ≈ 5%/yr. A trader-loss carry of 5%/yr would show at about t = 1.
- **Unpowered** for the loser leg. Powered only to confirm that fees exceed hedge and rebalancing costs, which is a lending-plus claim.

## 4. Rejected, with reasons

| product | reason | established fact |
|---|---|---|
| **Drift** insurance fund / Drift vaults, Gauntlet hJLP (hedged on Drift) | **About $285 M drained** (reported 2026-04-02; TVL fell to about $24 M from a peak above $550 M; the stolen assets included JLP). A compromised admin key listed a wash-traded fake collateral token and raised withdrawal limits ([Unchained][drift1]). The IF was reportedly "intact", but its return is stuck in a governance dispute (forum post of 2026-05-10, [Drift gov][drift2]). An unpriced key and governance tail. | 8 (costs and tails decide) |
| **Ostium OLP** | **2026-07-15: $18–23.7 M of about $32.7 M drained (−72%)** by a fabricated oracle report signed with a compromised key ([ValueTheMarkets][ost]). Post-cutoff; listed in Exposure. In 2026 Ostium also retired the "single-pool model that absorbed all trader risk" and routed net directional flow off-chain to Jump and prime brokers ([Defiant][ost2]), so the loser leg leaves the pool. | 8 |
| **gTrade gToken vaults** (gUSDC/gDAI/gETH) | Depositors get fees and are short traders' wins. Traders' net losses go to an over-collateralisation buffer and GNS burns, not to the gToken price ([docs][gns]). Withdrawals wait 1–3 epochs. Concave, lending-minus-tail. | 1 (the loser leg is not ours) |
| **Hyperliquid HIP-3, deployer side** (trade.xyz etc.) | Deploying needs **500k HYPE** staked for ≥ 183 days, slashable ([HL docs][hip3]). Retail access through Kinetiq Launch exLST pays about **4.5 of each 100 of fees** plus HYPE staking, slashable ([Kinetiq][kq]). Fee-only: the backstop for each HIP-3 dex is a protocol strategy, not a depositor pool. HYPE beta. Lending-like. | 3 / 8 |
| **Other HL protocol vaults** | None exist. HLP is the only `parent`, with 7 children ([l1-vaults A1][l1]). | n/a |
| **HL user vaults / copy designs** | Already tested: registration 007 failed. | 1 |
| **Options AMMs / Derive vaults** | The LP is short vol. Lyra's own update: ETH pool rounds down to about −7.7% (2021-11 → 2022-05) with delta unhedged ([Derive][lyra]). | 6 (short-vol tail-capped to about 2–3%/yr) |
| **GMX v1 GLP** | Wound down after the 2025-07-09 exploit; minting disabled ([GMX][gmx-rec]). | n/a |
| **Jupiter's own delta-neutral JLP vault** | Reduce-only, deposits disabled; also hedged on Binance through a third-party operator ([docs][jup-dn]). Rebuild it ourselves (§3.2). | n/a |

## 5. Exposure (current-state or post-2026-07-01 items seen; for the ledger)

**Current-state metadata reads (no APR, NAV or return recorded):**
- Lighter `/api/v1/systemConfig`. Printed: key names and `liquidity_pool_index` (281474976710654).
- GMX `/glvs`: three GLVs, including GLV [USDG-USDG] listed 2026-10-04.
- GMX Subsquid GraphQL schema introspection.
- GeckoTerminal: the list of JLP pools (names, creation dates).
- Pyth Hermes `price_feeds?query=JLP`: feed metadata, no price.
- Current docs pages: Jupiter Earn and delta-neutral (75% split, 0.20% fee, reduce-only status); Lighter liquidations and LLP strategies; Aster ALP (the fee "current values" 0.25%/0.05%); Paradex VTF and socialized-loss pages; GMX providing-liquidity, the liquidity API and the GraphQL changelog; gTrade gToken; edgeX eStrategy; HL HIP-3; the Kinetiq deployer guide.

**Server returned post-cutoff rows despite an end date:**
- Lighter `/pnl` returned **2 rows per pool (LLP, XLP) at or after 2026-07-01** although `end_timestamp` was 2026-06-30T23:59:59Z. They were dropped in memory, never written or printed.

**Errors:**
- CoinGecko 401: the full-range request exceeded the 365-day keyless window. No data.
- GeckoTerminal 401: the 180-day limit. No data.

**Post-cutoff facts read (news and docs):**
- Ostium OLP drain on 2026-07-15 and its magnitudes. Ostium's redesign toward Jump hedging (2026, month unknown).
- The GMX GraphQL changelog dated 2026-07-14 → 2026-09-01.
- Pyth Benchmarks going keyed or deprecated (2026-07-31 / 08-26, per a search summary; the endpoint returned 404).
- The GMX docs index mentions a "USDG launch boost" targeting 8% APR for 8 weeks (a current incentive target).

**Undated or likely-current return snippets in search results (not used):**
- Trading Strategy pages showing LLP one-month and lifetime figures and GLV [WETH-USDC] CAGRs;
- a Coin360 snippet giving LLP TVL and APR;
- a third-party "~30% APR" for Paradex Gigavault, and Coin360's "~11.8% APR" for Gigavault (current, opened while checking the 2025-10 incident);
- a "September 2026" JLP AI-generated note (title only, not opened);
- a HIP-3 article snippet valuing the 500k-HYPE stake at about $32 M (this implies a HYPE price);
- Kinetiq's "$20M+" barrier.

## 6. Files created; constraints honoured

**Files** (none committed):
- `research/alpha/scouts/a1-house-side.md`: this report.
- `research/alpha/scouts/a1_fetch.py`: end-dated pulls (Lighter / GMX / JLP). It refuses an existing output directory, drops rows at or after the cutoff in memory, and writes `SHA256SUMS`.
- `research/alpha/scouts/a1_coverage.py`: counts, first/last date and spacing only. It refuses if any point is at or after 2026-07-01.
- `research/alpha/scouts/a1_pdftext.py`: a crude PDF text extractor. It failed on Jupiter's JLP risk report (custom font encoding), so nothing from that report is cited.

**Data** (`research/alpha/data/scout_a1/`, gitignored, 4.8 MB, each pull in its own directory with `SHA256SUMS`):

| directory | contents |
|---|---|
| `lighter_pnl_20261013/` | LLP 529 d (2025-01-18 → 2026-06-30) and XLP 280 d (2025-09-24 → 2026-06-30), daily `/pnl` |
| `gmx_squid_20261013/` | GLV [ETH-USDC] 665 / GLV [WBTC-USDC] 658 daily price, fee-APR and PnL-APR snapshots (from 2024-09-04 / 09-11); GM ETH and GM BTC: 1,092 prices (from 2023-07-05), ~1,008 APR snapshots (from 2023-09-26); all to 2026-06-30 |
| `jlp_coingecko365_20261013/` | JLP market price, market cap and volume: 259 daily, 2025-10-15 → 2026-06-30 |
| `jlp_coingecko_20261013/` | the 401 error body only |
| `jlp_risk_report_pdf/` | Jupiter's JLP risk report PDF (3.4 MB), unread |

**Constraints honoured:**
- No statistic links any signal or state to a later price, return, NAV or payoff.
- No count is split by a future quantity or by position state.
- **No return, mean, Sharpe, drawdown, volatility or hit-rate was computed from any NAV, share-price, pool-value or APR series on any dates.** Only point counts and first/last dates were read, all before 2026-07-01.
- Every series query carried an end bound of 2026-06-30T23:59:59Z.
- Downloads were free and keyless, read with `python -I` with paths as arguments, and the scripts sit outside the data directories. Rate limits were respected (Lighter about 1 request/s, GMX at 0.5 s pacing).
- Nothing was circumvented. The CoinGecko and GeckoTerminal 401s were recorded and left.
- No credentials or keys; nothing paid.
- Nothing was published or uploaded; nothing was committed.
- Nothing under `research/copytrade/`, `lib/`, `pages/`, `components/`, `vercel.json` or `package.json` was touched. No `results.jsonl` was written. No Neon queries.
- Citations are limited to pages I opened. Figures I saw only in search snippets (for example an LLP −5.35% day on 2025-10-11 and a $21.5 M LLP loss) were **dropped as unverified**.

[lt1]: https://docs.lighter.xyz/trading/liquidations-and-llp-insurance-fund.md
[lt2]: https://docs.lighter.xyz/trading/liquidations-and-llp-insurance-fund/llp-strategies.md
[tb-lit]: https://www.theblock.co/post/385696/lighter-lit-staking
[jermo]: https://0xjermo.substack.com/p/the-thesis-lighter
[dw]: https://www.datawallet.com/fr/crypto/lighter-explained
[edf]: https://raw.githubusercontent.com/tradingstrategy-ai/web3-ethereum-defi/master/eth_defi/lighter/constants.py
[jup-earn]: https://docs.jup.ag/user-docs/earn/jlp/earn
[jup-dn]: https://docs.jup.ag/user-docs/earn/jlp/delta-neutral
[jw-dec]: https://discuss.jup.ag/t/jupiter-weekly-report-dec-8th-2025/39808
[jw-mar]: https://discuss.jup.ag/t/weekly-risk-report-march-31st-2025/36940
[jw-oct]: https://discuss.jup.ag/t/jupiter-weekly-report-oct-13-2025/39566
[dj]: https://defijangle.substack.com/p/jlp-on-jupiter
[vk]: https://vaultka.substack.com/p/vaultka-jlp-vaults-initial-backtesting
[gt]: https://vaultbook.gauntlet.xyz/drift-vaults/hjlp
[hlp]: ../../../docs/hlp-benchmark-2026-10-10.md
[gmx-lp]: https://docs.gmx.io/docs/providing-liquidity.md
[gmx-glv]: https://gmxio.substack.com/p/gmx-introduces-gmx-liquidity-vaults
[hal]: https://www.halborn.com/blog/post/explained-the-gmx-hack-july-2025
[gmx-rec]: https://gmxio.substack.com/p/glp-funds-on-arbitrum-fully-recovered
[ast]: https://docs.asterdex.com/earn/overview/aster-alp.md
[ast-fee]: https://docs.asterdex.com/earn/overview/aster-alp/alp-fees.md
[ast-burn]: https://docs.asterdex.com/earn/overview/aster-alp/how-to-burn-alp.md
[tb-xpl]: https://www.theblock.co/post/372500/aster-compensates-traders-xpl-liquidations
[edx]: https://edgex-1.gitbook.io/edgex-documentation/estrategy
[alea]: https://alearesearch.substack.com/p/avantis-the-universal-leverage-layer
[px]: https://finance.yahoo.com/news/bitcoin-price-crashes-zero-paradex-173520198.html
[px-sl]: https://docs.paradex.trade/risk/socialized-losses.md
[px-c360]: https://coin360.com/review/paradex
[px-vtf]: https://docs.paradex.trade/vtfs/key-features.md
[px-api]: https://docs.paradex.trade/api/prod/vaults/get-historical-data.md
[ext]: https://alearesearch.io/podcast-digest/proof-of-vision/proof-of-vision-ep40-ft-extended
[gns]: https://docs.gains.trade/liquidity-farming-pools/gtoken-vaults
[drift1]: https://unchainedcrypto.com/drift-protocol-suffers-285-million-exploit-after-admin-key-compromise-and-oracle-manipulation-unchained/
[drift2]: https://driftgov.discourse.group/t/bifurcating-the-insurance-fund-return-staker-capital-consider-protocol-owned-accumulation-for-recovery/332
[ost]: https://www.valuethemarkets.com/cryptocurrency/news/ostium-trading-protocol-halts-following-serious-anomaly
[ost2]: https://thedefiant.io/news/defi/ostium-launches-institutional-hedging-layer
[hip3]: https://hyperliquid.gitbook.io/Hyperliquid-docs/hyperliquid-improvement-proposals-hips/hip-3-builder-deployed-perpetuals
[kq]: https://kinetiq.xyz/docs/launch-deployer-and-operator-guide
[l1]: ../../copytrade/scouts/l1-vaults.md
[lyra]: https://insights.derive.xyz/amm-performance-update/
