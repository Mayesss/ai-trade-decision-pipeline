# H1 scout: house-sleeve terms and incident record (HLP, Lighter LLP)

2026-10-14. Desk research for the owner's allocation decision (ledger entry
`research/alpha/registrations/000v-house-sleeve-allocation-reads.md`).
**No return, APR, Sharpe or drawdown figure is computed or reported here.**
Both venues' APIs also serve APR/Sharpe fields; I left them out on purpose.
Every URL was opened on 2026-10-14. API snapshots were taken about 16:30 UTC
that day. Where a source shows its own date, it is given as "(src date)".
Anything marked **NV** is not verified.

## 0. What changed since A1 (2026-10-13) and H7 (2026-10-10)

1. **LIT is hedgeable.** A1 had this as "not verified". LIT USDT-margined perps trade on Hyperliquid, Lighter, Binance, Bybit and OKX, with roughly $20–150 M of open interest per venue (§B2).
2. **The LLP gate is unchanged: 1 LIT staked per 10 USDC.** Lighter's docs still say so. Unstaking now has a documented **3-day lockup**, and stakers get a **fixed 6% staking APR** paid in LIT. The LLP share cooldown is **5 min**, confirmed from `systemConfig`. The operator fee is **0**.
3. **LLP now has 8 strategy buckets**, not the 3 in the docs' example. One bucket holds about 64% of the collateral. The depositor holds one share class across all of them (§B5).
4. **The 2025-10-10/11 LLP loss now has a size:** about 5–5.35% of the pool, "roughly $21.5 M". A1 had "size not verified".
5. **HLP TVL is $180.0 M**, against about $425 M on 2025-10-01 (H7). HLP's docs now say it also "supplies USDC in Earn", i.e. lends idle USDC to the portfolio-margin pool.
6. **Hyperliquid USDC is now natively minted through Circle CCTP.** The legacy Arbitrum bridge is "deprecated" and holds less than 10% of HyperCore USDC. The new escrow, CoreDepositWallet on HyperEVM, can be upgraded by an EOA with **no delay** (L2BEAT).
7. **Lighter governance changes in the period:**
   - Pause and Emergency Guardian logic was added on 2026-10-02. Both guardians are currently set to the 0x address.
   - The verifier and contract were upgraded about 15 times from 2026-07-01 to today.
   - L2BEAT logged three liveness gaps of 2–3 h: 2026-09-20, 2026-10-01 and 2026-10-11.
8. **No depositor-loss incident found for HLP or LLP from 2026-07-01 to today** (search-limited; §A4, §B5).
9. **New dated supply event for the gate asset:** the LIT insider cliff ends on **2026-12-27**. About 3.2 M LIT a week then vests until about 2029-12 (§B2).

---

## A. Hyperliquid HLP

### A1. Deposit and withdrawal terms

| item | current value | source |
|---|---|---|
| Lock-up | **4 days after your most recent deposit.** A new deposit resets the clock for the whole balance. | [HL docs, Protocol vaults][hl-pv]; [HL docs, depositors (legacy)][hl-dep]; reset rule: [Datawallet (src 2026-08-26)][dw-hlp] |
| Lock-up, observed | `lockupUntil − vaultEntryTime` = **4.000 days** for the 5 most recent entrants (entries 2026-09-21 → 2026-10-09). | info `vaultDetails`, 2026-10-14 |
| Fees / profit share to depositors | **None.** "Protocol vaults do not have any fees or profit share." The API shows `leaderCommission` = 0. `leaderFraction` = 0.20% is the leader's own equity share, not a fee. | [HL docs, legacy vaults][hl-leg]; API |
| Deposit cap | None documented. API shows `allowDeposits: true`, `isClosed: false`. | API |
| Minimum deposit | None documented for the vault ("no minimum beyond account activation", per a third party). Bridge minimum is 5 USDC; deposits below that are lost. New accounts pay a one-time **1 USDC activation fee** on their first outbound action. | [Datawallet][dw-hlp]; [HL docs USDC/bridge][hl-usdc-api]; [Circle CCTP on HyperCore][cctp-hc] |
| Withdrawal mechanics | Vault page → Withdraw. "There may be some slippage as you withdraw and open positions are closed." The API shows `alwaysCloseOnWithdraw: false`. | [HL docs, depositors (legacy)][hl-dep] |
| Mandate | Market making, liquidations, a share of fees, **and supplying USDC in Earn** (new wording since H7). Per Datawallet, Earn lends idle USDC to the portfolio-margin pool and the protocol keeps 10% of the interest as a liquidation buffer. Per DefiLlama via Datawallet, HLP now receives only about 1% of fee revenue; 99% goes to the Assistance Fund (**NV** at source). | [HL docs, Protocol vaults][hl-pv]; [Datawallet][dw-hlp] |

### A2. Getting USDC on and off, and terms of use

- **On-ramp from Arbitrum.** In the app, deposit USDC on Arbitrum with a little ETH for gas. Wallet-connected accounts can deposit "via Arbitrum only". Email accounts can also deposit from Ethereum, Base or Polygon through Across. ([how to start][hl-start], [deposited USDC FAQ][hl-faq-usdc])
- **Rail.** "CCTP is the preferred method for minting USDC natively on the Hyperliquid L1… The legacy bridge is deprecated." It holds "less than 10% of the USDC supply on HyperCore." ([HL docs USDC][hl-usdc], [USDC API][hl-usdc-api])
- **Fees.**
  - Circle charges a flat **0.20 USDC** forwarding fee on Arbitrum → HyperCore, and no fast-transfer fee. The figure comes from Circle's sandbox example, so treat the mainnet value as **NV**. ([Circle how-to][cctp-in])
  - HyperCore → EVM costs a HyperCore `maxFee` (amount not stated) plus a 0.2 USDC forwarding fee to Arbitrum. ([Circle withdraw how-to][cctp-out])
  - The legacy bridge's **1 USDC withdrawal fee** is reported only by third parties; I did not find it on the current HL docs page (**NV**).
  - Legacy-bridge withdrawals arrive in "3–4 minutes" and need no Arbitrum transaction. ([HL USDC API][hl-usdc-api])
- **Terms of use** (Hyperliquid Corp., **last updated 2026-06-15**, read from the app's `TermsOfUse` bundle at [app.hyperliquid.xyz/terms][hl-tos]). "Restricted Persons" are:
  - residents or entities in the **US or Ontario**;
  - residents or entities in sanctioned or export-controlled jurisdictions;
  - citizens of those Restricted Territories, wherever they live.

  Outcome markets also exclude persons in jurisdictions "we designate from time to time". **Germany and the EU are not named.** Warning only: I found no MiCA authorisation for the interface operator (**NV**).

### A3. HLP size now (info endpoint, 2026-10-14 16:32 UTC)

| field | value |
|---|---|
| TVL (`portfolio.accountValueHistory` last point; stats-data `tvl` agrees: $180,027,815) | **$180.03 M** |
| Followers | Not countable: the endpoint returns only the top 100, whose equity totals $113.0 M of $180.0 M. Total count is **NV**. |
| Lock-up fields | `lockupUntil` per follower. Only 1 of the top 100 is locked now, until 2026-10-16. Lock length is 4.0 days (above). |
| `allowDeposits` / `isClosed` | true / false |
| Child vaults | 7, unchanged from H7 |

TVL context: $603.9 M peak (2025-09), $268.6 M (2026-06), about $184 M (2026-08), $183.0 M (2026-10-02). Sources: [CoinGecko (src 2026-07-02)][cg-hlp], [Datawallet][dw-hlp], VaultVision (search snippet only, page not opened, **NV**). TVL has roughly halved in 4 months. No source ties the outflow to a single incident.

### A4. Incident record (HLP / HL depositors)

| date | event | mechanism | $ size | HLP depositor impact | source |
|---|---|---|---|---|---|
| 2025-03-12 | ETH 50× whale | Withdrew margin to force liquidation of a ~$340 M long; HLP took it over | ~$4 M HLP loss | HL: "no protocol exploit"; leverage caps cut | [DL News][dl-eth] |
| 2025-03-26 | **JELLY** | Self-liquidating short plus spot pump. **Validators delisted and force-settled at $0.0095** (the attacker's entry) | Unrealised peak −$12–13.5 M | Closed at about **+$703 k**: an admin intervention that rescued HLP | [CoinGecko][cg-hlp], [Datawallet][dw-hlp] |
| 2025-07-29 | API outage | Traffic spike, 37 min | ~$2 M refunded to traders | None to HLP stated | [hyperacademy (src 2026-10-09)][ha] |
| 2025-10-10 | Market-wide cascade | HLP backstop; first cross-margin ADL | +$40–41.5 M gain | Positive | [CoinGecko][cg-hlp]; H7 §1 |
| 2025-11-12 | POPCAT | Fake bid wall, $3 M burned, cascade | ~$4.9 M bad debt | Loss, under 1% of TVL | [CoinGecko][cg-hlp], [Datawallet][dw-hlp] |
| 2026-01-31 | ETH long liquidation | Backstop | ~+$15 M | Positive | [CoinGecko][cg-hlp] |
| 2026-04-09 | FARTCOIN | 4 linked wallets built a $15 M long, pulled margin, sold spot | $1.2–1.5 M loss (~0.35% drawdown) | Loss | [Phemex (src 2026-04-09)][px-fart], [Datawallet][dw-hlp] |
| 2026-08 | Google-ads phishing | User wallets drained | ~$550 k (researcher report, unconfirmed) | None; not protocol | [hyperacademy][ha] |
| 2026-10-10 | Papertrade launch | Wallet moved HL's ETH mid to game Papertrade's HL-referenced pricing | $1.28 M profit taken from **Papertrade**'s pool | **None reported for HLP.** It shows HL's book is used as an oracle elsewhere | [The Block (src 2026-10-11)][tb-paper] |

**Window 2026-07-01 → today:** no HLP loss event, ADL, validator or bridge
incident, or delisting intervention touching HLP was found. Searches covered
July, August, September and October 2026, plus HL status/news. Coverage was
search-limited, so absence is **not proof**.

## B. Lighter LLP

### B1. Deposit terms

| item | current value | source |
|---|---|---|
| LIT gate | "**For every 1 LIT staked, participants may deposit up to 10 USDC into the LLP.**" Unchanged since 2026-01-14. Pre-gate deposits had a grace period to 2026-01-28. After that, uncovered amounts were returned at max(3%, 100 USDC) a day from 2026-01-29. | [Lighter docs, LIT utility][lt-lit]; [The Block (src 2026-01-14)][tb-lit]; [KuCoin (src 2026-01-29)][kc-unc] |
| Staked LIT | Lives in the staking pool (account 281474976624800, which holds **113.7 M LIT**, about 45% of circulating supply). **Unstaking: 3-day lockup**: docs say so, and `systemConfig.staking_pool_lockup_period` = 259,200,000 ms. **Rewards: "fixed 6% APR"**, paid in LIT bought on market. A ≥100 LIT stake waives withdrawal and transfer fees (2026-01 sources; current status **NV**). | [docs][lt-lit]; API `systemConfig`; [The Block][tb-lit] |
| LLP share cooldown | **5 minutes** (`liquidity_pool_cooldown_period` = 300,000 ms). Public pools "do not impose lockup periods". | API `systemConfig`; [docs, Public Pools][lt-pp] |
| Deposit cap | Only the LIT gate. No pool-level cap documented (**NV**). | — |
| Operator fee / profit share | **0.0000.** Operator holds 1 M of 24.47 B shares. | API `account?by=index&value=281474976710654` |
| Effect on a $10k deposit | Needs 1,000 LIT staked, about **$3.2 k at $3.23**. That is 32% extra capital, about 24% of the combined position. The ratio moves with the LIT price: at the 2026-03-31 low ($0.78) it was about 8%. | arithmetic on [CoinGecko API][cg-lit] |

### B2. LIT token and hedgeability

**Spot.** $3.23, market cap $807 M, circulating 250 M of 1 B (CoinGecko
[`coins/markets`][cg-lit], 2026-10-14 16:33 UTC).
- ATH $7.86 (2025-12-30); ATL $0.78 (2026-03-31). The gate asset is highly volatile.
- Supply overhang: insider allocation is 500 M (50%). The cliff ends **2026-12-27**, then about 3.2 M LIT a week vests to about 2029-12. A 15.6 M LIT burn happened in 2026-07. ([Tokenomist (src 2026-07-18)][tkn])

**Perps (2026-10-14, about 16:34 UTC, public endpoints).** All are linear USDT/USDC.

| venue | OI | 24h volume | funding (snapshot) | max lev |
|---|---|---|---|---|
| **Hyperliquid** `LIT` | 46.4 M LIT ≈ **$150 M** | $19.3 M | 0.00125%/h | 5× |
| **Binance** `LITUSDT` (listed 2025-12-23; price matches Lighter's LIT, not old Litentry) | 15.2 M LIT ≈ $49 M | $39.3 M | 0.005% / 4 h | — |
| **Bybit** `LITUSDT` | $29.2 M | $17.5 M | 0.005% / 4 h | — |
| **OKX** `LIT-USDT-SWAP` | $20.5 M | 8.66 M LIT ≈ $28 M | 0.005% / 4 h | — |
| **Lighter** `LIT` (market 120) | 10.25 M LIT ≈ $33 M | $11.6 M | ≈0.0096% per 8 h-equivalent | 5× (IMF 20%) |

- All venues sit at the baseline positive rate, so **a short currently receives funding**. This is a single snapshot.
- Hedge on a non-Lighter venue. A short on Lighter fails together with the LLP leg (wrong-way venue risk).
- Sources: HL `metaAndAssetCtxs`; Binance `fapi/v1/premiumIndex`, `ticker/24hr`, `openInterest`, `fundingInfo`, `exchangeInfo`; Bybit `v5/market/tickers`; OKX `public/open-interest`, `market/ticker`, `public/funding-rate`; Lighter `orderBookDetails?market_id=120` and `funding-rates`.

### B3. Getting funds in and out; account type; terms of use

- **Deposit routes.** ([Lighter API docs][lt-dep])
  - Universal Deposit Address from Ethereum, Arbitrum, Base, Optimism, Polygon, BNB, HyperEVM, Solana and others. Minimum $5 for a new account. Bridge/swap fees are quoted per transfer ("estimates only").
  - Or a direct Ethereum L1 `deposit` to `0x3B4D…5ca7`, minimum 1 USDC.
  - Or the legacy CCTP intent address on Arbitrum, Base or Avalanche, minimum 5 USDC.
- **Withdrawals.** "Secure" withdrawals settle via Ethereum, to the owner's address only. "Fast" withdrawals go through the fast-withdraw pool, any address, USDC only, minimum 4 USDC. **Withdrawal fees are not stated in the docs (NV).** ([Lighter API docs][lt-dep])
- **Standard vs Premium.**
  - Standard: zero fees, 300 ms taker latency. Premium: fees, with LIT-stake discounts. ([docs, Trading fees][lt-fees])
  - Nothing in the docs ties LLP access to account type, so it **appears irrelevant to an LLP-only depositor (NV)**. A staked LIT position counts toward Premium fee tiers too.
- **Terms of service** (Elliot Technologies, Inc., **last updated 2025-12-29**, Florida law, mandatory arbitration; [lighter.xyz/terms][lt-tos]).
  - Prohibited Persons: located in the **US, Canada, UK, China, North Korea, Russia, Ukraine, Cuba, Iran, Venezuela, Syria**, or any comprehensively sanctioned territory, plus sanctions-listed persons.
  - **Germany and the EU are not named.** The ToS lists ADL losses as an assumed risk. Warning only.

### B4. LLP size now

LLP `total_asset_value` = **$86.33 M** (USDC margin $86.26 M), account
281474976710654, 2026-10-14 about 16:33 UTC. XLP (281474976680784) is
$40.4 k. Lighter's total value secured is $1.12 B; L2BEAT warns that LIT is
43.6% of it ([L2BEAT][l2b-lt]).

### B5. Incident record and strategy segregation

| date | event | mechanism | $ size | LLP depositor impact | source |
|---|---|---|---|---|---|
| 2025-10-10/11 | Cascade outage | DB growth; about 4.5 h down; 3 tx batches lost; single sequencer and proving bottleneck (Beosin) | **LLP about −5% to −5.35%, "roughly $21.5 M"** | Loss. Compensation was 25k points to LLP holders out of 250k total. | [Phemex (src 2025-10-20)][px-lt]; [cryptonews (src 2025-10-17)][cn-lt]; [L2BEAT][l2b-lt]; [Datawallet][dw-lt] |
| 2026-02-25/26 | ARC squeeze | Whale TWAP-built about $50 M of OI. Liquidations went into Strategy #7; the bucket was exhausted, then ADL hit the shorts | Whale −$8.2 M (one outlet says $5.3 M) | **LLP loss capped at the bucket's ~$75 k.** ARC OI was then capped at $40 M and moved to a ~$100 k bucket. | [CoinAlertNews (src 2026-02-26)][can-arc]; [Datawallet][dw-lt] |
| 2026-09-20, 2026-10-01, 2026-10-11 | Liveness gaps | No proof or state updates on L1 for 2 h 01 m, 3 h 02 m and 2 h 32 m | — | None reported. Signals operator fragility. | [L2BEAT liveness][l2b-lt] |
| 2026-07-01 → today | — | **No LLP loss, exploit or ADL event found** (search-limited) | — | — | searches in §Not verified |

**Segregation now.**
- The API `pool_info.strategies` lists **8 buckets**: $54.87 M (63.6%), $14.09 M (16.3%), $10.06 M (11.7%), $2.25 M, $2.04 M, $1.98 M, $0.74 M and $0.24 M.
- Per the docs, each strategy is "a segregated collateral shard". Its max loss is its allocated collateral; when that is depleted, "that strategy becomes subject to ADL". Collateral in other strategies is unaffected. ([docs, LLP strategies][lt-strat])
- **The depositor bears all buckets.** There is one share class, priced as total_asset_value / total_shares across the whole pool ([API docs, public pools][lt-ppapi]). The worst case is therefore the sum of losses across buckets, each capped at its allocation, and the largest bucket is 64% of the pool.
- The market-to-bucket mapping is "communicated separately" and is not public (**NV**).

### B6. Security model

**Escape hatch** ([Lighter docs, architecture][lt-arch]; [L2BEAT][l2b-lt]):
- Users can submit exits through an Ethereum **priority queue**: withdrawals, **public-pool share burns**, and reduce-only IOC orders.
- If they are not processed within **14 days**, desert mode can be triggered. The window was cut from 18 days on 2025-10-27.
- In desert mode, users prove balances, positions and pool shares from Ethereum-posted blobs and withdraw on L1.
- The desert-exit circuit and a state snapshot (batch #166859) are public, and L2BEAT reproduced the state root.

**Upgrades and admin** (L2BEAT):
- Stage 0. The governor is a 3/5 multisig, with a 21-day upgrade delay.
- The Security Council (4/7) can cut that delay to zero, and "in practice every upgrade so far has been fast-tracked". L2BEAT flags "**There is no delay on code upgrades (CRITICAL)**".
- Several verifiers were deployed without published sources, for example on 2026-07-20, 2026-08-24/26, 2026-09-03/10/15 and 2026-10-02.
- Pause Guardian (2–24 h) and Emergency Guardian (24–72 h) were added on 2026-10-02. They are currently 0x. A pause halts withdrawals but "does not block desert mode".

**Oracle:** Stork is the primary oracle; "external signatures are currently not verified and the sequencer must be trusted" (L2BEAT).

**Audits:** 9 audit PDFs are listed, but auditor names and dates do not render (**NV**) ([docs, audits][lt-aud]).

**Bottom line:** if the operator halts, an LLP depositor has a trust-minimised exit after about 14 days, unless an instant upgrade removes it first.

## C. Hyperliquid security model

**Bridge** ([L2BEAT][l2b-hl]; [Bridge2.sol][br2]):
- The legacy Arbitrum bridge has hot and cold validator sets, **4 validators each, equal power**. A request needs 2/3+1, i.e. **3 of 4**. The challenge period is 200 s.
- Any locker can lock the bridge. Only a cold-set quorum can unlock it, and that quorum can also rotate the validator set (`emergencyUnlock`).
- L2BEAT risks: "Funds can be stolen if the permissioned validator majority signs an invalid withdrawal request (CRITICAL)". "Funds can be frozen if the permissioned validator set stops processing withdrawals (CRITICAL)". Sequencer failure: "No mechanism". Exit window: "None".

**Native USDC (CCTP):**
- Circle's CoreDepositWallet on HyperEVM holds the USDC. HyperCore balances are "protocol-level credits, not Circle-issued USDC" ([Circle][cctp-hc]).
- The CoreDepositWallet "**can upgrade with no delay**", and the upgrader is a single EOA ([L2BEAT][l2b-hl]).

**L1 consensus** (info `validatorSummaries`, 2026-10-14):
- 35 validators registered, **27 active**, 6 jailed. The top 10 hold 76% of active stake.
- The **5 Hyper Foundation validators hold 47.3%**. That is more than 1/3, so the Foundation alone can halt consensus; it is less than 2/3, so it cannot finalise alone.

**Admin and governance powers:**
- Validators vote to delist validator-operated perps. The documented rule settles at the 1-h TWAP oracle ([HL docs, Delisting][hl-delist]).
- The JELLY precedent (2025-03-26) settled at $0.0095 instead, and turned an HLP loss of up to $13.5 M into a profit ([CoinGecko][cg-hlp]). The same power can be used again, in HLP's favour or not.
- Oracle: validators publish a stake-weighted median of CEX prices ([HL docs, Oracle][hl-oracle]).
- Audits: Zellic audited the legacy bridge only ([HL docs, Audits][hl-aud]).

**Exit if validators halt:** **none.** There is no forced-withdrawal or escape
hatch. A halt freezes HLP shares and HyperCore USDC until validators resume.

## Not verified

- HLP total follower count (the API truncates to the top 100).
- Hyperliquid's current withdrawal fee: the 1 USDC legacy fee is third-party only, and the HyperCore `maxFee` amount is unknown. Circle's 0.20 USDC forwarding fee comes from a sandbox example.
- HLP's ~1% share of fees (DefiLlama via Datawallet).
- Date of the Earn/lending launch: "native lending launched in September 2026" appears only on a Datawallet heatmap page that I did not open.
- Lighter withdrawal fees, and whether the ≥100-LIT fee waiver still applies.
- Any LLP pool-level deposit cap.
- LLP strategy-to-market mapping.
- Whether account type (Standard/Premium) affects LLP deposits.
- Whether LLP shares are usable as collateral (promised 2026-01-29).
- Lighter audit authors and dates.
- How LLP shares are valued in desert mode (open positions at the last state).
- MiCA status of both front-ends for an EU user.
- Completeness of "no incident since 2026-07-01". My searches covered general and month-specific HL/HLP/Lighter/LLP incident queries, July–October 2026. I did not read X/Discord, so a small event could be missed.
- Blockworks' "validator put" piece returned 403 and was not read.

[hl-pv]: https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/vaults/protocol-vaults
[hl-dep]: https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/vaults/for-vault-depositors-legacy
[hl-leg]: https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/vaults/hypercore-vaults-legacy
[hl-start]: https://hyperliquid.gitbook.io/hyperliquid-docs/onboarding/how-to-start-trading
[hl-faq-usdc]: https://hyperliquid.gitbook.io/hyperliquid-docs/support/faq/deposit-or-transfer-issues-missing-lost/deposited-usdc
[hl-usdc]: https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/usdc
[hl-usdc-api]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/usdc
[hl-delist]: https://hyperliquid.gitbook.io/hyperliquid-docs/trading/delisting
[hl-oracle]: https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/oracle
[hl-aud]: https://hyperliquid.gitbook.io/hyperliquid-docs/audits
[hl-tos]: https://app.hyperliquid.xyz/terms
[cctp-in]: https://developers.circle.com/cctp/howtos/transfer-usdc-from-arbitrum-to-hypercore
[cctp-out]: https://developers.circle.com/cctp/howtos/withdraw-usdc-from-hypercore-to-evm
[cctp-hc]: https://developers.circle.com/cctp/concepts/cctp-on-hypercore.md
[dw-hlp]: https://www.datawallet.com/crypto/hyperliquid-hlp-explained
[cg-hlp]: https://www.coingecko.com/learn/hyperliquid-hlp-vault-analysis
[dl-eth]: https://www.dlnews.com/articles/defi/hyperliquid-denies-hack-after-investors-suffer-hlp-loss/
[ha]: https://hyperacademy.io/en/articles/hyperliquid-security-and-hack-risk
[px-fart]: https://phemex.com/news/article/hyperliquid-hlp-incurs-12m-loss-from-fartcoin-position-manipulation-71932
[tb-paper]: https://www.theblock.co/news/defi/2026-10-11-trader-nets-1-28-million-on-new-1000x-exchange-papertrade-by-moving-hyperliquids-eth-prices-418239
[l2b-hl]: https://l2beat.com/scaling/projects/hyperliquid.md
[br2]: https://raw.githubusercontent.com/hyperliquid-dex/contracts/master/Bridge2.sol
[lt-lit]: https://docs.lighter.xyz/about-lighter/lit-utility.md
[lt-pp]: https://docs.lighter.xyz/trading/public-pools.md
[lt-strat]: https://docs.lighter.xyz/trading/liquidations-and-llp-insurance-fund/llp-strategies.md
[lt-fees]: https://docs.lighter.xyz/trading/trading-fees.md
[lt-arch]: https://docs.lighter.xyz/about-lighter/technical-architecture-lighter-core.md
[lt-aud]: https://docs.lighter.xyz/security/security-audits
[lt-dep]: https://apidocs.lighter.xyz/docs/deposits-transfers-and-withdrawals
[lt-ppapi]: https://apidocs.lighter.xyz/docs/manage-public-pools-shares
[lt-tos]: https://lighter.xyz/terms
[tb-lit]: https://www.theblock.co/post/385696/lighter-lit-staking
[kc-unc]: https://www.kucoin.com/news/flash/lighter-launches-lit-staking-access-mechanism-for-llp-uncovered-portions-to-be-returned
[cg-lit]: https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=lighter
[tkn]: https://tokenomist.ai/research/lighter-lit-tokenomics-robinhood-hype-a-real-burn-and-the-december-2026-cliff-2
[l2b-lt]: https://l2beat.com/scaling/projects/lighter.md
[px-lt]: https://phemex.com/news/article/lighter-exchange-outage-amid-19b-crypto-liquidation-28446
[cn-lt]: https://cryptonews.net/news/market/31810785/
[dw-lt]: https://www.datawallet.com/crypto/lighter-explained
[can-arc]: https://coinalertnews.com/news/2026/02/26/whale-loses-8m-arc-lighter
