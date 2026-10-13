# E1 desk scout: structural payers beyond trading

2026-10-13, round 4, scout E1. Behaviour-only. Scope: rewards, yields, event
contracts, futures/FX premia, pre-IPO perps, and anything else where somebody
keeps paying for a structural reason.

**No statistic links any signal, state or event to a later price, yield or
payoff. No return, yield, mean, Sharpe or drawdown was computed from any
series.** Every return figure below is either quoted from a cited source
(marked *public prior*) or is arithmetic on stated assumptions (marked
*assumption*). What I measured are counts, dates, terms, pool sizes, fee
schedules and current-state venue metadata.

---

## 1. Summary

1. **Every payer here is real, and every one is lending-like at our robust size today.** None reaches the owner's 30–100%/yr zone. The best is the Binance reward stack, and its cadence collapsed in 2026-H1.
2. **BNB reward stack (N7-2), behaviour step done.** From Binance's own CMS, 2024-01 → 2026-06: **66 HODLer projects** (5 in 2024-H2, 54 in 2025, 7 in 2026-H1), **30 Launchpools** (21 / 8 / 1 in 2024 / 2025 / 2026-H1) and **4 Megadrops** (the last in 2025-04). HODLer reward size fell from a median 2% of max supply (2025-H1) to 1% (2026-H1). The pool denominator **is public for Launchpool**: 16.6–19.1M BNB per event since 2024-08 (Binance's `listV3` endpoint; DOGS's 11.2M is the exception, because it ran alongside TON). It is not published for HODLer airdrops, but the same Simple Earn BNB feeds both, so it is a usable proxy. A 2025-cadence stack is about **3–8%/yr on capital** hedged (central 5; assumption). A 2026-H1 cadence gives about **1–4%** (central 2.5), and more than half of that is fact-3 funding carry, not the payer. BNB perps exist on HL (10×, OI ≈ $48M) and Bitget (75×, OI ≈ $64M).
3. **Crypto dollar yields** (sUSDe, Pendle PT, Aave/Morpho/Sky) are lending-like. sUSDe realised 15.7 / 6.2 / 4.0% (2024 / 2025 / 2026-H1). The tail is documented: USDe $0.65 on Binance (2025-10-10), xUSD −77% (2025-11-04).
4. **Perp-DEX points** paid big once each (HL 31% of supply, 2024-11; Lighter 25%, about $675M, 2025-12). Each one is a single bet with ex-post rules. **Not testable.**
5. **Event contracts:** I found no access change since `l5b-kalshi.md`. There is one new venue the owner can reach: **HL HIP-4 outcome markets** (live 2026-05-02 as a "limited-feature initial release" with two daily markets, per FalconX 2026-05-21; tier-0 fees 7 bp taker / 4 bp maker; no maker rebate). It has no bias study and two months of pre-cutoff history.
6. **Futures/FX premia:** Capital.com charges **4%/yr on each side** of CFD financing, which kills carry by construction. Futures (IBKR) avoid that fee, but full commodity curves are not keyless (EIA covers energy contracts 1–4 only).
7. **Pre-IPO perps / hyperps have no structural payer.** Funding is multiplied by 0.005, so nobody pays carry, and only **3 pre-IPO conversions** fall before the cutoff. Published evidence shows convergence, not systematic overpayment. Reject as a payer.

---

## 2. Ranked table

**Bar.** HLP gives +19% in a spiral year, 2–4%/yr calm, a Sharpe of 1.4,
and a maximum drawdown of −2.5%; its lending leg is about 3%/yr.

**Ranking:** by the certainty of the payer × deployability at our size. Dollar yields (#3) are pure lending, not an edge, so they rank below the reward stack and the maker book despite similar odds.

**Odds** are given as "exists" / "deployable at our size, net, beating
HLP-calm (≥ 4%/yr on capital)". Returns are on total capital deployed, and
every one is an *assumption* unless marked *public prior*.

| # | light | mechanism | why consistent (who keeps paying, and why) | evidence + decay (sample periods) | access / warning | costs | free data (source, coverage, end-datable?) | expected return at robust size | tail: worst plausible week / month | Kelly fraction | capacity | odds |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | **Binance reward stack, BNB delta-hedged** (HODLer + Launchpool + Megadrop; short BNB perp on HL) | Hold BNB in Simple Earn, which makes it eligible for all three programmes. Sell each reward at listing by a fixed rule. Hedge BNB 1:1 with a short perp. | **Binance's listing and marketing budget, plus projects paying for distribution.** Listing fee is "0" in the announcements; the projects pay in tokens. Lasts as long as Binance lists through these programmes. | Counts (§3.1): Launchpool **21 / 8 / 1** events (2024 / 2025 / 2026-H1); HODLer projects **5 / 54 / 7** (2024-H2 / 2025 / 2026-H1); Megadrop **4** in total. *Public prior* (Binance via sponsored press): about $226 of rewards per BNB, 2024-01 → 2025-Q1. **Decay is measured: the 2026-H1 cadence is about a quarter of 2025's.** | Binance Simple Earn plus HL perp. **Germany is not on the HODLer excluded list** (Dec 2024 text: AU, CA, CY, HK, JP, NL, NZ, UK, US …). Binance is not licensed for DE (warning line). | Earn: 0. Hedge: HL 4.5 bp taker / 1.5 bp maker, once per rebalance. Reward sale: about 10 bp of reward value. | Binance CMS list + detail API (keyless; **not end-datable**, so drop at load); Launchpool `listV3` (keyless, per-pool staked amount and users; drop at load). Reward-token spot klines from data.binance.vision (keyless, date-bounded). HL BNB funding (local to 2026-06). | **2025 cadence: about 3–8%/yr (central 5); 2026-H1 cadence: about 1–4%/yr (central 2.5)** on capital, including HL short funding at an assumed 1–5%/yr, which is fact-3 carry rather than the payer (*assumption*, §3.1) | Ordinary: **−1 to −3% / −2 to −4%** (reward token gaps down before the sale; funding flips against the short; hedge slippage on a BNB jump). **Venue: Binance freezes without BNB collapsing means −(Binance share of capital), unpriced, a few %/yr.** If BNB collapses with Binance (the FTT template), the HL short pays. | Kelly is undefined for a binary venue tail. About 75% of the book sits on Binance by construction, so size the **whole programme** as money one accepts losing (for example ≤ 10–20% of investable capital), not by Kelly. | Per-user cap of 4% of the pool (about 700k BNB), not binding. HL BNB book: about $105–143k within 10 bp. **About $0.25–1M.** | 9 in 10 / **1 in 4** (1 in 2 if 2025 cadence returns) |
| 1b | Same payer, **stablecoin Launchpool pools** (USDC / FDUSD / USD1) | Stake stablecoins in Launchpool and sell rewards at listing. No BNB hedge. | Same budget: 15–20% of each Launchpool's rewards go to stablecoin pools. | Stablecoin pools held **$0.29–3.6B** per pool, with 5.5k–238k stakers. Only **1 Launchpool in 2026-H1** (OPN), and 0 since (exposure §5). | Binance (warning as above) | ≈ 0 plus sale cost | as #1 | **< 1%/yr** at 2026 cadence (one event, about 2 days a half-year) *assumption* | stablecoin depeg on venue (USDe 2025-10-10) | n/a | large | 9 in 10 / 1 in 20 |
| 2 | **Event contracts, maker side** (Kalshi favourite side; HL HIP-4 outcomes) | Rest maker bids on favourites and collect the longshot premium. | Retail takers overpay for small probabilities (*public prior*, Bürgi–Deng–Whelan 2026). Thin tails keep arbitrage capital out. | As in `l5b-kalshi.md`: maker +1.8% per contract, 2021 → 2025-04, slope halved in 2025. New: arXiv 2602.19520 (353M trades, Kalshi and Polymarket) finds **underconfidence in politics** (favourites underpriced) replicating across venues. | Kalshi: Germany unlisted, KYC unverified; GGL warning. **HIP-4 on HL: reachable now**, no bias study. Polymarket: close-only for DE. | Kalshi maker fees on some series; HIP-4 tier 0: 4 bp maker / 7 bp taker; collateral idle at 0% (Kalshi non-US) | Kalshi archive (keyless, date-bounded); HIP-4 on HL fills (2026-05-02 →, only 2 months pre-cutoff) | **3–8%/yr** (l5b central) | correlated event cluster: −5 to −10% / −10 to −15% | ≤ 1/4 Kelly per cluster | $10–100k | 3 in 4 / 1 in 3 |
| 3 | **Crypto dollar yields** (sUSDe; Pendle PT fixed; Aave/Morpho/Sky lending) | Lend dollars to leveraged longs, either directly (lending markets) or via funding (sUSDe). PT locks a fixed rate. | **Leveraged longs pay funding and borrow rates** to stay long. The demand is structural in a retail bull market and cyclical in level. | *Public prior:* sUSDe realised **15.7% (2024-02 → 2025-01), 6.2% (2025), 4.0% (2026-H1)** (l2 on-chain read). Coin Metrics: BTC/ETH funding ≈ 11% annualised in 2024, ≈ 5% in 2025. **Decays with funding.** | DeFi (self-custody) or CEX Earn. MiCA warning for unlicensed stablecoins. | gas; PT exit spread | DefiLlama yields (not end-datable); on-chain `convertToAssets` (end-datable, the l2 method) | **3–6%/yr: lending-like** | week: venue-local depeg (USDe < $0.66 on Binance, 21:36–22:16 UTC 2025-10-10, recovered). Month: **protocol insolvency, xUSD −77% (2025-11-04, $93M loss, withdrawals frozen)**. | diversified lending: ≤ 1/3 per protocol | very large | 9 in 10 / 1 in 3 (just above 4% in a good year) |
| 4 | **Commodity term-structure carry + momentum; FX carry** (futures, not CFDs) | Long backwardated, short contango; vol-targeted. | **Producers' hedging pressure** (Keynes–Hicks) pays speculators a basis premium. Carry trades are paid for crash risk. | *Public prior:* Szymanowska et al. (JF 2014): basis and other sorts give **spot premia of 5–14%/yr and term premia of 1–3%/yr**. Koijen et al. ("Carry", NBER w19325): carry predicts returns in every asset class, and does badly in global recessions. FX carry decay after 2008: sources 403'd, **not cited**. | IBKR futures (lawful). **Capital.com CFDs: a 4%/yr fee on each side of financing kills it.** | futures commissions plus roll (not verified this session); CFD 4%/yr | EIA NYMEX energy contracts 1–4, daily, keyless XLS (page lists ranges to 2024); Cboe VX; FRED FX and short rates (keyless). **Full multi-commodity curves: none keyless** (Nasdaq Data Link needs a key: owner ask). | at 10% vol, if Sharpe 0.3–0.5 survives: **3–5%/yr**; at 20% vol **6–10%** | 10% vol: −4 / −8%. 20% vol: −8 to −10% / −15 to −20% (2008, 2020-04 negative WTI) | ¼ Kelly (Sharpe 0.4 → Kelly vol 40%, use ≤ 10–20%) | large | 3 in 4 / 1 in 5 |
| 5 | **Perp-DEX points / airdrops** (delta-neutral volume or liquidity farming) | Generate points cheaply with a hedged book. Receive tokens at TGE. | The protocol's token budget buys users and volume. **Each budget is paid once per protocol.** | *Public prior:* HL genesis **31% of supply (310M HYPE), 2024-11-29**. Lighter **25%, about $675M, 2025-12-30/31**. Aster S2 delayed over "data inconsistencies" (one user, $9M of volume, quoted 336 ASTER; 2025-10). | HL / Lighter / others, grey zone | 2 × taker per round trip (HL 9 bp; zero-fee venues pay through spread); hedge funding | none usable: allocation formulas are secret and set ex post | **unknowable ex ante.** A lottery ticket bought with fees. | lose the fees (−0.1 to −1% of volume farmed); sybil disqualification | not computable | small | 1 in 2 that some farm pays / **1 in 10** as a strategy |
| 6 | **HL pre-IPO perps (xyz IPOP) and hyperps** | Trade the pre-listing perp into conversion at the first regular session (IPO) or the CEX spot listing (TGE). | **No structural payer.** Funding samples are 0.005× (IPOP) or 1% weight (hyperps), so there is no carry. Convergence is to the open print, not a fixed price. | 3 pre-cutoff conversions: CBRS 2026-05-14, QNT 2026-06-04, SPCX 2026-06-12. Coin Metrics (2026-06-09): CBRS perp at **$280–320 pre-IPO, open $350**, final-hour VWAP $354.5 (the perp traded *below* the open, so longs gained: no overpaying long). Keyrock (2024): HL/AEVO pre-launch prices converge to post-TGE; "no consistent winners". | HL xyz, 5× strict isolated | 9 bp taker standard (growth mode 0.9 bp) | xyz fills on disk to 2026-06-30 (unopened; see §4) | ≈ 0 structural; a discretionary bet | gap at conversion: ±30–90% (CBRS IPO $185 vs open $350) | n/a | ~$25M OI caps | 1 in 10 / 1 in 20 |
| 7 | **Other**: Bitget Launchpool / PoolX (same payer as #1); exchange maker rebates; insurance underwriting | See §4 | Bitget: listing budget. Rebates: exchange buys liquidity. Insurance: cover buyers hedge hacks. | Bitget: per-user caps reported VIP-tiered (search summary only, unverified; not cited). HL rebates need **> 0.5% of 14-day maker volume** (−0.1 bp). | Bitget usable | — | Bitget announcements (not checked) | Bitget: a small add-on to #1 | — | — | small | see §4 |

---

## 3. Top candidates: rule sketch, calendar, power, next step

#1 is the brief's named behaviour step. #2 (the Kalshi maker book)
already has a full feasibility scout and verdict in `l5b-kalshi.md`, and
nothing material changed (§2, row 2). So §3.2 covers the strongest *new*
candidate, the futures premium (#4).

### 3.1 #1: Binance reward stack, BNB delta-hedged (N7-2)

**Behaviour step done.** Counts and terms only; no price or yield.

**Event calendar.** Binance CMS catalogs 48, 93 and 49, titles
2024-01 → 2026-06, then the bodies of the 110 reward-programme articles.
Release-date half-years:

| programme | 2024-H1 | 2024-H2 | 2025-H1 | 2025-H2 | 2026-H1 | total | monthly cadence 2025 → 2026-H1 |
|---|---|---|---|---|---|---|---|
| **HODLer Airdrops** (projects, by Binance's own ordinal) | — (launched 2024-06-19) | 5 (#1 BANANA … #4–5 1000CAT+PENGU) | 20 (#6–#25) | 34 (#26–#59; #40's announcement has a non-standard title, ordinal gap only) | **7** (#60 BREV … #66 OPG, 2026-06-30) | **66** | 4.5 → **1.2** |
| **Launchpool** (ordinals #43–#72, no gaps) | 13 | 8 | 7 | 1 (KITE) | **1** (OPN, 2026-03-03) | **30** | 0.67 → **0.17** |
| **Megadrop** | 2 (BB 2024-04, LISTA 2024-05) | 1 (SOLV, announced 2024-12-30) | 1 (KERNEL, 2025-04) | 0 | 0 | **4** | → 0 |

**Terms** (`data/scout_e1/reward_terms/events.jsonl`):

- **HODLer reward size, as % of the token's max supply** (median, min–max).
  This is a term, not a value.

  | half-year | events | median | min–max |
  |---|---|---|---|
  | 2024-H2 | 5 | 7.0% | 2–12.7% |
  | 2025-H1 | 20 | 2.0% | 0.75–5% |
  | 2025-H2 | 32 | 1.5% | 0.35–3.0% |
  | 2026-H1 | 7 | 1.0% | 0.25–1.5% |

  **Both the event count and the share per event shrank.**
- **Eligibility window** (the retroactive snapshot, announced after it
  closes): 13 days for #1, then 3–6 days in 2025-H1, 4 days through mid-2025-09, then
  **3 days** from 2025-09-25 (MIRA) onward. Because it is retroactive and random, the stake must be
  held continuously.
- **Cap:** "BNB Holding Hard Cap: user's average / total average ≤ 4%" on
  every HODLer event from #1. At our size it never binds.
- **Second tranche:** 53 of the 97 events (HODLer, Launchpool and Megadrop
  together) say an additional amount goes to "other marketing campaigns"
  later. That tranche is **not** a HODLer payment.
- **Listing:**
  - Announcement to spot listing is 0–3 days in 2024–25.
  - **The five 2026 HODLer bodies from #62 on name no spot-listing
    time** (distribution "within 5 hours"). Listing came in separate
    announcements (Alpha or futures; not read). A sale rule must name its
    venue.
- **Launchpool mechanics** (from the bodies): hourly snapshots; rewards
  split equally across concurrent projects; an hourly per-user reward
  cap.
- **Eligibility for Germany:** HODLer bodies from 2024-12 list the
  excluded residences as AU, CA, CU, Crimea, CY, HK, IR, JP, NZ, NL, KP,
  RU, SY, UK, US and its territories, and the non-government-controlled
  areas of Ukraine. **Germany is not excluded.** The list is "not
  exhaustive and may be subject to changes".

**The pool size (the denominator) is public for Launchpool.**
- Binance's keyless `bapi/earn/v1/friendly/launchpool/project/listV3`
  returns, per completed project and per pool:
  - total staked (`totalInvestAmount`);
  - number of stakers;
  - reward tokens;
  - the hourly cap.
- **BNB pool size:**
  - 2024-01 → 2024-07: 14.9–24.8M BNB (117k–268k stakers).
  - From TON (2024-08), when Simple Earn BNB began auto-participating,
    stakers jump to 0.99–1.67M and the pool sits at **16.6–19.1M BNB**.
    The exception is DOGS (11.2M), which ran concurrently with TON;
    concurrent projects split the BNB equally.
- **Stablecoin pools:** $0.29–3.6B per pool (FDUSD, USDC, TUSD, USD1,
  U).
- **HODLer:** no per-event total was found in any public source (Binance
  pages, news searches). The announcements say the same Simple Earn
  (plus On-Chain Yields) BNB is "automatically eligible for HODLer
  Airdrops (as well as Launchpool and Megadrop)". The Launchpool BNB
  pool (16.6–19.1M) is therefore a close proxy, slightly low because
  On-Chain Yields count for HODLer and Launchpool only.
- A registration should state the proxy and a sensitivity range (±15%).

**BNB hedge venues** (current-state only, 2026-10-13 09:04 UTC):

| venue | listed | max lev | OI | 24 h notional | spread | depth within 10 bp (bid / ask) | fees |
|---|---|---|---|---|---|---|---|
| Hyperliquid (main dex `BNB`) | yes | 10×, cross allowed | $47.7M | $10.1M | 0.13 bp | $105k / $143k (the API's 20 levels only) | 4.5 / 1.5 bp |
| Bitget `BNBUSDT` | yes, normal | 75× | $64.1M | $8.2M | 0.13 bp | $151k / $608k (≤ 50 bp: $0.43M / $2.5M) | 6 / 2 bp (API) |

A $250k–$1M hedge, entered by maker over hours, fits either venue. HL is
preferred for the venue-tail offset: if Binance fails and BNB gaps down,
the HL short pays.

**Expected return (assumption arithmetic; no series computed).**
- Per year, on the BNB leg:

  yield = Σ_events (reward tokens × sale price under the pre-registered
  rule) / (pool BNB × BNB price) + Simple Earn base APR + HL short
  funding.

- Inputs I assume:
  - **Per-event value $V$:** N7's press figures put 2025 HODLer events at
    about $10–12.5M (cited there; not re-opened by me).
  - **Pool:** 17–19M BNB × $600–1,000, i.e. $10–19B.
  - **Launchpool events:** worth $10–30M each to the BNB pool.
  - **HL funding to the short:** 1–5%/yr. This is the established carry
    (fact 3), not the reward payer.
- **Capital** = BNB + HL margin at about 3× (×1.33).

| regime | HODLer | Launchpool | funding | on capital |
|---|---|---|---|---|
| 2025 cadence (54 + 8 events) | 2.8–5.4% | 0.4–2.4% | 1–5% | **≈ 3–8%/yr, central 5** |
| 2026-H1 cadence (14 + 2 a year; per-event share halved, so $V$ ≈ $5–10M) | 0.4–1.4% | 0.1–0.6% | 1–5% | **≈ 1–4%/yr, central 2.5**, of which more than half is the funding carry, not the payer |

**Verdict:** lending-like at today's cadence. For an owner who already
holds BNB unhedged, it is a free overlay. As a hedged book it does not
clear HLP-calm unless the 2025 cadence returns.

**Pre-registrable rule (sketch).**
- Hold a fixed BNB amount continuously in Simple Earn Flexible, with a
  1× short BNB perp on HL rebalanced when |delta| > 5%.
- Sell every reward at one pre-stated rule: the first-hour VWAP after its
  first Binance spot listing, or after futures listing when there is no
  spot listing, within 7 days. Otherwise mark it at 0.
- Primary outcome: monthly net yield on capital. Sources:
  - reward tokens, from announcements × the per-user share (stake / pool
    proxy);
  - sale price from data.binance.vision 1m klines;
  - funding from HL;
  - fees.
- Criterion: annualised net ≥ 4%/yr, month-clustered, one-sided
  t_{G−1}. Report the venue tail as a stated assumption, not an estimate.
- Placebo: the same book with the rewards zeroed (pure carry), to show
  the payer's own contribution.

**Decision calendar.**
- **Discovery:** 2025-01 → 2026-06 (18 months: the 2025 boom plus the
  2026-H1 slump). 2024 (Launchpool era, HODLer from 2024-07) stays as
  context.
- **Holdout:** 2026-07-01 → is held back. Its announcement titles were
  dropped unread (counts only; see §5.1). The only post-cutoff fact seen
  is that no Launchpool ran after OPN.
- **Forward paper:** from registration.

**Power arithmetic.**
- The quantity is accounting, not inference: given the terms and the
  sale rule, each month's yield is nearly determined. The uncertainty is
  the dispersion of $V$ and the regime.
- With 18 months and a monthly coefficient of variation of about 1
  (lumpy events):
  - SE(annualised mean) ≈ mean / √18 ≈ 0.24 × mean;
  - to pass ≥ 4% at one-sided t_{17,0.95} = 1.74 needs a true mean of
    about **≥ 6.8%/yr**.
- So a 2025-like central of 5% is **under-powered** against the 4% bar,
  and a 2026-like 2.5% fails outright.
- Expected outcome: "exists, lending-like". It is worth one cheap
  measurement trial only if the owner wants the overlay quantified.

**Behaviour-only next steps.**
1. The owner checks Simple Earn BNB and HODLer eligibility on the actual
   account (DE residence; Binance's EU entity). The country list above is
   from Binance's text, not the account.
2. For each HODLer event, count from announcements (terms only) whether
   a spot listing followed and when. 2026's bodies omit it, so the
   separate listing notices of 2025-12 → 2026-06 must be read, titles
   ≤ 2026-06-30.
3. From the local HL funding archive (≤ 2026-06), count BNB-hours at the
   baseline rate versus away from it. This is a level distribution, not
   conditioned on any event.
4. Write the DRAFT with the sale rule frozen before any reward-token
   kline is pulled.

### 3.2 #4: Commodity term-structure carry + momentum on futures (the one scalable levered premium)

- **Rule sketch.**
  - Universe: about 20 liquid CME/ICE commodity futures (energy, metals,
    grains, softs, livestock), trading the second contract.
  - Monthly signals: carry = annualised (F1 − F2) / F2, plus 12-1
    time-series momentum.
  - Long the top tercile and short the bottom on carry; add
    sign(momentum).
  - Portfolio vol target 10–15%. Execute on IBKR futures, never CFDs
    (the 4%/yr fee).
- **Calendar.**
  - Szymanowska et al.'s sample ends before 2014 (JF 2014).
    Post-publication 2014-02 → 2026-06 is about 148 months of unread-by-us
    history.
  - 2026-07 → is held back.
- **Power.**
  - SR 0.4 net gives t ≈ 0.4 × √12.3 ≈ 1.4, about **30%** power at
    one-sided 5%.
  - Carry plus momentum at SR 0.6 gives t ≈ 2.1, about **55%** power.
  - Under-powered unless pre-2014 history is used, and that is in-sample
    for the papers.
- **Economics at a robust size.**
  - At 15% vol and SR 0.4: about **6%/yr gross**, about 5.5% net.
  - Worst month −10 to −15% (2008 H2; 2020-04 negative WTI).
  - Below HLP risk-adjusted (Sharpe 1.4).
- **Blocker:** full per-contract settlement history is not keyless.
  - EIA's keyless daily XLS covers NYMEX crude, gasoline and heating-oil
    contracts 1–4 only, and the page lists ranges to 2024.
  - Cboe VX is keyless.
  - Nasdaq Data Link needs a (free) key: **owner ask**.
- **Next behaviour step:** the owner decides on a key. Then count, for
  each contract, available months, roll dates and gaps. No returns.

---

## 4. Rejected ideas

| idea | why rejected (and the established fact that kills it, if any) |
|---|---|
| **Perp-DEX points farming as a registered strategy** | Each airdrop is one bet per protocol. Allocation formulas are secret and set ex post (Aster S2: "data inconsistencies", a $9M-volume user quoted 336 ASTER). Sybil filters are set ex post. Payoffs cannot be pre-registered or tested: HL 2024 and Lighter 2025 are two draws, not a sample. Delta-neutral volume on HL pays 2 × taker on both legs, the cost that killed every carry design (**fact 3**). Keep only as a discretionary lottery if the owner wants one; never a trial. |
| **Pre-IPO / pre-launch perps as a payer** | No carry: the funding multiplier is 0.005 on IPOP and hyperps sample the premium at 1% weight. No hedger or insurer pays: the counterparty is another speculator. Published cases show convergence (Keyrock; CBRS final-hour VWAP within 1.3% of the open). There are only **3 IPO conversions before 2026-07-01** (CBRS, QNT, SPCX), so a test has no power. The hyperp short side carries the XPL-type squeeze tail (2.5× in minutes, 2025-08; **fact 5** tail). The xyz fills on disk (2025-10-13 → 2026-06-30, unopened) could support **descriptive** work only: fills and accounts per pre-IPO market, spread and depth before and after conversion, OI-cap binding, who holds at conversion. Nothing there tests a premium with 3 events. |
| **Exchange maker-rebate programmes** | HL pays −0.001% only above 0.5% of 14-day maker volume, which is tens of millions of USD a day. Binance and Bitget MM programmes are similar. Resting fills lose 14 bp at 15 min to non-forced flow (**fact 4**). Unreachable at our size, and the adverse selection exceeds the rebate by 100×. |
| **CFD-based commodity or FX carry (Capital.com)** | Financing is "Our daily fee +/- Interest-rate benchmark", and "Our daily fee is 4% per year" (0.01111%/day for USD, EUR, CHF and JPY) ([capital.com fees](https://capital.com/en-int/ways-to-trade/fees-and-charges), read 2026-10-13; the help-centre page returned 403). Today's table shows US Crude and Brent spot charged −0.01096%/day on **both** sides. A long pays benchmark + 4% and a short receives benchmark − 4%. Published term premia are 1–3%/yr and FX carry is a few %/yr, so the fee is larger than the premium at any leverage (the fee scales with notional). Only futures remain (#4). |
| **Stablecoin Launchpool pools as a standalone** | Same payer as #1, but 1 event in 2026-H1 and 0 since (exposure item). A 2-day farm twice a year is under 1%/yr. Fold it into #1 as an add-on. |
| **Insurance underwriting (Nexus Mutual-style cover staking)** | The payer is real: protocols and funds buy hack cover. But the claims tail is a correlated smart-contract or exchange failure (unpriced). Claims history is too thin to price it, and the payout is concave. It fails "an unpriced tail is not acceptable". |
| **Stablecoin and LST depeg backstop buying** | Already rejected by N7 (§A7): a handful of events a year, and a real insolvency (UST; xUSD −77% 2025-11-04) is unpriced. Nothing new changes it. |
| **Funding-rate fixed-income (Pendle Boros) as an edge** | It prices the same funding (**fact 3**). Its history before 2026-07 is too short (HL markets from 2025-09). Lending-like at best. |
| **Polymarket favourite-side making** | Close-only for Germany by the venue's own geoblock: research data only. |

---

## 5. Exposure, files, constraints

### 5.1 Exposure (anything dated on or after 2026-07-01 that I saw)

No price, yield or return **series** dated on or after 2026-07-01 was
fetched. Point items seen:

1. **Binance CMS lists** (newest-first, not end-datable). Articles dated
   ≥ 2026-07-01 were dropped at load and **counted only**: catalog 48 (new
   listings) 88, catalog 93 (activities) 263, catalog 49 (news) 123. No
   title was printed or written. The catalog-48 total (2,281) was printed
   by the first probe; N3 had already seen it (2,280).
2. **Launchpool `listV3`.**
   - Fetcher: 0 completed projects start on or after 2026-07-01. That
     implies **no Launchpool between 2026-03-03 (OPN) and 2026-10-13**: a
     post-cutoff calendar count, with no outcome.
   - A paging probe printed the first project id of the newest page,
     `OPN_USD1` (pre-cutoff, 2026-03), which already showed that no
     later project was completed. It also printed `OMNI_FDUSD` for
     page 2 (2024).
   - The first structure probe printed part of the endpoint's current
     summary: `grossValue` ≈ 7.05e11, a cumulative aggregate whose unit
     I did not establish. Not used.
3. **trade.xyz pre-IPO specification index** (current page): post-cutoff
   anticipated listing dates and statuses.
   - SKHY 2026-07-10 converted; CXMT 2026-07-27 converted; UNITREE
     2026-08-14 converted; SHEIN 2026-09-01 converted; OURA 2026-10-02
     live.
   - That is **5 post-cutoff pre-IPO events, with dates and statuses only.
     No prices.**
4. **Search-result snippets** (not opened, not used, listed for
   completeness):
   - Aave V3 Ethereum USDC supply APR as of about 2026-10: 1-year average
     3.70%, 6-month 3.30%, 30-day 5.19% (Aavescan snippet).
   - LlamaRisk 2026-08-25: PT-sUSDe-26NOV2026 implied 4.72%, sUSDe
     4.25%.
   - Kalshi volume about $37.2B and Polymarket $8.16B in Aug 2026 (German
     snippet).
   - "As of October 2026 the HL docs list no second airdrop" (eco.com
     snippet).
   - Sky Savings Rate 3.52% → 3.60% (undated).
   - Lighter vesting cliff 2026-12 (a future date).
5. **Current-state metadata**, by design: HL and Bitget BNB perp OI,
   24-hour volume and a book snapshot (2026-10-13 09:04 UTC). Also
   Capital.com's current overnight-fee table (oil: −0.01096%/day on both
   sides).
6. **Binance announcement bodies** (all released < 2026-07-01).
   - 0 of the 110 were flagged as edited on or after 2026-07-01. Their text
     may carry post-cutoff updates, and none were printed in full.
   - `relatedArticles` (which can name post-cutoff articles) was
     discarded unread.

### 5.2 Files created

- `research/alpha/scouts/e1-structural-payers.md` (this file).
- Scripts:
  - `research/alpha/scouts/e1_binance_cms.py`: CMS titles, drop-at-load
    end-dating.
  - `research/alpha/scouts/e1_binance_detail.py`: announcement bodies;
    resumable, with 429 backoff.
  - `research/alpha/scouts/e1_launchpool_pools.py`: per-pool staked
    amounts.
  - `research/alpha/scouts/e1_reward_terms.py`: term extraction and
    counts.
  - `research/alpha/scouts/e1_bnb_perp_meta.py`: HL and Bitget BNB perp
    current metadata.
- Data (gitignored; one directory per pull, each with `SHA256SUMS`):
  - `research/alpha/data/scout_e1/binance_cms_titles/`: 3 catalogs,
    2024-01 → 2026-06 titles, plus `counts.json`.
  - `research/alpha/data/scout_e1/binance_cms_bodies/` and
    `binance_cms_bodies_2/`: 110 reward-programme articles, body text
    only. The first pull stopped at 32 on HTTP 429; the second resumed
    at a 6 s spacing with a 90 s backoff.
  - `research/alpha/data/scout_e1/launchpool_pools/`: 70 Launchpool
    projects, 2020 → 2026-03, per-pool fields. `annualRate` was dropped
    at load.
  - `research/alpha/data/scout_e1/bnb_perp_meta/`: one JSON.
  - `research/alpha/data/scout_e1/reward_terms/`: the extracted event
    table (`events.jsonl`).
- Total size: about 1.6 MB.

### 5.3 Constraints honoured

- **Forbidden conditionings honoured:**
  - no statistic links any signal, state or event to a later price,
    yield or payoff;
  - no return, yield, mean, Sharpe or drawdown was computed from any
    series;
  - no count was split by a future quantity or by position state.
- Binance's posted per-pool APR (`annualRate`) was **dropped at load,
  unread**. No listing-day price of any reward token was fetched.
- **Calendar cut:**
  - announcement lists were end-dated at load (§5.1);
  - nothing dated ≥ 2026-07-01 was fetched as a price, yield or return
    series;
  - the HL and Bitget BNB metadata is current-state by design.
- **Data handling:**
  - all pulls were keyless and free;
  - each pull went into its own new directory with `SHA256SUMS`;
  - every read used `python3 -I` with paths as arguments, and scripts sit
    outside the data directories;
  - HTTP 429 was respected with a slower, backed-off resume. 403s
    (ScienceDirect, Bocconi IRIS, etf.com, Capital.com help) were
    recorded and not retried around.
- **Untouched:**
  - nothing under `research/copytrade/` was created, modified or opened.
    The only contact was a directory listing of
    `research/copytrade/data/pruned/by_dex/`, which showed the dex names;
    no xyz file was opened;
  - no Neon query, no commit, no `results.jsonl`;
  - nothing published or uploaded;
  - no `lib/`, `pages/`, `components/`, `vercel.json` or `package.json`.
- **Citations:**
  - only pages I opened are cited;
  - search-summary-only items are labelled or dropped;
  - sponsored content is flagged (the $226-per-BNB figure is Binance's
    own analysis carried by sponsored press, via N7).
- **One incidental tool artefact:** WebFetch saved the NBER PDF of
  "Carry" (w19325) to the session's tool-results folder, outside the
  repo. It was not readable here (no PDF text tool), so no Sharpe figure
  from it is quoted.


### 5.4 Sources opened (2026-10-13)

**First-party endpoints and documents:**
- Binance CMS list: `https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId={48,93,49}`.
- Binance CMS detail: `…/cms/article/detail/query?articleCode=…`.
- Binance Launchpool: `https://www.binance.com/bapi/earn/v1/friendly/launchpool/project/listV3`.
- Hyperliquid info API (`metaAndAssetCtxs`, `l2Book`).
- Bitget `api/v2/mix/market/{contracts,ticker,merge-depth}`.

**Venue docs:**
- trade.xyz:
  - [index](https://docs.trade.xyz/llms.txt);
  - [Pre-IPO Perpetuals](https://docs.trade.xyz/perpetuals/markets/pre-ipo-perpetuals.md);
  - [Pre-IPO Specification Index](https://docs.trade.xyz/perpetuals/specifications-and-schedules/pre-ipo-specification-index.md);
  - [Alternative Settlement Events](https://docs.trade.xyz/perpetuals/markets/pre-ipo-perpetuals/alternative-settlement-events.md);
  - [Pre-IPO risks](https://docs.trade.xyz/legal-and-disclaimers/perpetuals-risks-and-disclaimers/pre-ipo-perpetual-markets-risks-and-disclaimers.md);
  - [Events](https://docs.trade.xyz/events.md);
  - [Events fees](https://docs.trade.xyz/events/fees.md).
- Hyperliquid:
  - [hyperps](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/hyperps);
  - [fees](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees).
- [Capital.com fees and charges](https://capital.com/en-int/ways-to-trade/fees-and-charges).
- [EIA NYMEX futures prices page](https://www.eia.gov/dnav/pet/pet_pri_fut_s1_d.htm) (series list only).

**Research and press:**
- Coin Metrics:
  - [SOTN 367, 2026-06-09](https://coinmetrics.substack.com/p/state-of-the-network-issue-367) (pre-IPO perps);
  - [SOTN 335, 2025-10-28](https://coinmetrics.substack.com/p/state-of-the-network-issue-335) (USDe).
- [The Block, Binance $283M compensation, 2025-10-12](https://theblock.co/post/374295/binance-pays-283-million-in-compensation-following-fridays-depegs-covering-user-losses).
- [Decrypt, Stream xUSD, 2025-11-04](https://decrypt.co/347285/stream-finance-stablecoin-plunges-77-protocol-fund-manager-loses-93-million).
- [The Defiant, Ventuals wind-down](https://thedefiant.io/news/defi/hyperliquid-based-ventuals-winds-down-on-chain-pre-ipo-markets) (undated; "froze … at 24-hour average prices").
- [FalconX, 2026-05-21](https://www.falconx.io/newsroom/the-drivers-behind-hyperliquids-next-phase-hype-etfs-hip-4-outcome-markets-priority-fees-and-its-usdc-agreement) (HIP-4).
- Forklog:
  - [HYPE genesis, 2024-11-29](https://forklog.com/en/hyperliquid-to-launch-hype-token-airdrop/);
  - [Lighter airdrop, 2025-12-31](https://forklog.com/en/lighter-airdrop-joins-cryptos-ten-largest/).
- [The Block, Aster S2 delay, 2025-10-10](https://www.theblock.co/post/374256/yzi-labs-backed-aster-delays-stage-2-airdrop-citing-potential-data-inconsistencies).
- [Keyrock, pre-token markets](https://keyrock.com/are-pre-token-point-markets-price-indicator/) (2024 charts; market-maker research, not peer-reviewed).
- [arXiv 2602.19520](https://arxiv.org/abs/2602.19520).
- [Szymanowska et al., JF 2014 (IDEAS)](https://ideas.repec.org/a/bla/jfinan/v69y2014i1p453-482.html).
- [Koijen et al., "Carry", NBER w19325](https://www.nber.org/papers/w19325) (abstract only).
- [BTC-Echo, 2025-11-26](https://www.btc-echo.de/news/hype-polymarket-kalshi-deutschen-nutzern-prediction-markets-strafen-219878/) (paywalled; only "GGL classifies event bets as illegal" visible).

**Carried from earlier scouts, not re-opened:**
- N7's sponsored-press figure ($226 per BNB) and its pool sizes per HODLer event (Newton $12.5M, Yield Basis $10M).
- l2's sUSDe on-chain read.
- l5b's Kalshi paper.

**Opened but not used:**
- A farming "playbook" (pccrypto Substack, 2025-10-23): no figures, and affiliate links throughout.
- [The Defiant, tradexyz pre-IPO](https://thedefiant.io/tradexyz-launches-pre-ipo-perpetuals): undated.

**Failed (403):** ScienceDirect (Koijen et al., JFE), Bocconi IRIS (FX
carry, JFE 2024), etf.com (Swedroe), Capital.com help centre.
