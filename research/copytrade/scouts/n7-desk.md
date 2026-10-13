# N7 desk scout: new lights, and the N4 feasibility check (maker-only carry with a gap stop)

2026-10-12. Desk research only. Work done: literature, venue fee pages,
archive listings, and a schema check of two free order-book samples. **No
strategy outcome, P&L, forward return or conditional mean was computed on
any local or downloaded data.** Every return figure below is quoted from a
cited source, or is arithmetic on published fees and published facts and
labelled as such. No local file dated on or after 2026-07-01 was opened. The
local Binance store was checked by **filenames** and by one schema
`DESCRIBE`.

Files created:
- `research/copytrade/scouts/n7_schema.py`: schema inspection of the two
  samples. It prints field names, message types, counts and update spacing.
  It reads no price in any computation.
- `research/copytrade/data/scout_n7/binance_bookticker/`: one Binance UM
  bookTicker day, ENSUSDT 2023-05-17 (3.9 MB). Binance's `.CHECKSUM`
  verified OK; `SHA256SUMS`.
- `research/copytrade/data/scout_n7/bybit_ob500/`: one Bybit linear ob500
  day, ENAUSDT 2025-01-01 (44 MB); `SHA256SUMS`.
- `research/copytrade/data/scout_n7/schema.jsonl`: the schema output.

N1–N3, N5 and N6 belong to other scouts and are not repeated here.

---

## Summary (6 lines)

1. **N4 does not deserve a data scout as framed.** Even with free
   execution and no price-gap loss, 012's HL-pair carry collects +27.8 bp
   of funding over a hold of about 16 days. That is about 6.3%/yr on N, or
   **about 3.2%/yr on gross capital**: HLP's lending leg, before any cost.
   - Realistic maker execution costs 15–30 bp per position, against 35 bp
     taker. That alone does not flip the sign (§B4).
   - The gap tail (mean −38 bp, median +2 bp) sits in a spread that is
     arbitrage-bounded and mostly mean-reverting, or it comes from venue
     jumps such as halts, delistings and venue-set settlement prices.
     **A stop crystallises the first kind and cannot fill inside the
     second.**
   - Odds that N4 yields an edge that beats HLP: **about 1 in 25.**
2. **Best new light: crypto trend following on liquid perps** (N7-1).
   - Convex. Low turnover, so the fees clear.
   - Strong published in-sample evidence: Sharpe > 1.5 net, 2015–2025.
   - Live evidence of decay since 2020-21, and almost no out-of-paper
     calendar left to test on.
3. **The most certain cash flow: the BNB reward stack, delta-hedged**
   (N7-2). Binance pays BNB holders in airdrops. The payer is structural:
   the exchange's listing and marketing budget.
   - The 2024 boom level (claimed 50%+/yr) is not today's level.
   - The scout's job is to measure the current yield from public
     announcements.
4. **A levered, scheduled-flow premium: the month-end liquidity cycle in
   equity indices** (N7-3).
   - Etula et al. (RFS 2020): +48 bp abnormal per month over T−3…T+3, and
     −37 bp over T−8…T−4.
   - 2014 → 2026-06 is out of the paper's sample. The US settlement-cycle
     changes (T+2 in 2017, T+1 in 2024) give a sharp, pre-registrable
     prediction: the window should move.
5. **Two lending-plus lights, kept for honesty:**
   - **SPAC trust arbitrage** (N7-4): a convex floor at T-bills, small
     capacity.
   - **A short VIX-futures roll sized to its measured tail** (N7-5): a
     priced premium. After sizing for the measured 5 Feb 2018 day (−96%
     at 1×), it probably does not beat HLP risk-adjusted.
6. **Fourteen ideas were rejected** (§A7). Decay is documented for the
   pre-FOMC drift and the index-inclusion effect. Others died on latency
   races, account limits, size, or one of the established facts.

---

## Part A — N7: new lights, ranked

### A0. The ranked table

Odds are "effect exists" / "deployable at our size, net, beating HLP calm
(≥ 4%/yr on capital)". Returns are honest ranges after haircuts for
decay, not the literature's headline.

| # | light | mechanism (one line) | why it could be consistent | evidence + decay | access | free data | expected return | tail (worst week / month) | odds |
|---|---|---|---|---|---|---|---|---|---|
| 1 | **Crypto trend following** (Donchian ensemble, volatility-targeted, top-20 liquid perps) | Ride multi-week trends. Flat or short in downtrends. | Under-reaction and herding in a retail market. Leverage spirals (fact 2) extend daily moves. Payoff is convex: long the tail. | Zarattini, Pagani & Barbon 2025: net Sharpe > 1.5, alpha 10.8%/yr vs BTC, 2015–2025-03. Liu & Tsyvinski RFS 2021: strong TS momentum. **Decay:** Grobys et al. 2025 find crypto momentum crash-prone and fragile; a 2026 SSRN note finds TSMOM insignificant after 2020-07. | HL, Bitget, Binance perps; 4.5–6 bp taker, 1.5–2 bp maker; turnover is weekly-scale | Binance klines (keyless archive; local `binance_years/` 1h to 2024-01); HL candles | **10–30%/yr** at a 20–25% vol target if alive; −5 to +5% if not | **−15 to −20% / −25 to −30%** at a 25% vol target (trend-reversal gaps, e.g. a Luna-style week while long) | 1 in 3 / 2 in 3 if it exists (overall ≈ 1 in 5) |
| 2 | **BNB reward stack, delta-hedged** (Simple Earn BNB for HODLer airdrops plus Launchpool; short BNB perp, preferably on HL) | Collect the airdrops, sell at listing, hedge the BNB price. | **The payer is structural:** Binance and the listed projects pay for distribution. It continues while the programme does. | Binance's own analysis (via sponsored press): about $226 per BNB in rewards, 2024-01 → 2025-Q1. A CoinMarketCap study: 53–78% combined yield in 2024. About 48 HODLer projects Feb → Oct 2025 (numbering), pools ≈ $10–12.5M each. **Decay: likely**, because the 2024 listing boom is not 2025–26. | Binance Simple Earn (no fee) + BNB perp (HL 4.5 bp / BN 5 bp); Binance not licensed for DE (warning) | Binance announcements (web), Binance spot klines for each airdropped token (keyless archive) | **3–10%/yr** in a 2025-like regime (my estimate, §A2); 20–50% in a 2024-like boom | week −2 to −5% (listing dumps, funding spike); **month: venue failure, −100% of the Binance balance** (unpriced, a few % a year) | 9 in 10 (the cash flow is paid) / 1 in 2 |
| 3 | **Month-end liquidity cycle, equity index** (long T−3…T+3, flat or short T−8…T−4, window shifted by the settlement cycle) | Institutions sell to raise month-end cash, then prices rebound. | Pension and fund payment cycles are contractual, and their selling has a fixed settlement-driven date. Limits to arbitrage are documented. | Etula et al. RFS 2020, US to 2013, 1995+ subsample: raw T−3…T+3 **+77 bp**; abnormal +48 bp; T−8…T−4 abnormal −37 bp; also in other developed markets. McConnell & Xu 2008 (1926–2005). **Decay:** mixed. A Fed WP says the turn-of-month effect fades after 1990; Carchano & Pardo found it in both halves of 1991–2008. | Index CFDs (Capital.com) or micro futures (IBKR); costs ≪ 5 bp per round trip (verify) | Ken French daily factors (1926 →, keyless, 178 KB; filter < 2026-07-01); FRED SP500 (2016-10 →) | if half the effect survives: about 3–5%/yr unlevered, **8–15%/yr at 2–3×** | 1×: worst 7-day window about −15% (2008); **3×: about −45%** | 1 in 3 / 4 in 5 if it exists (overall ≈ 1 in 4) |
| 4 | **SPAC trust arbitrage** (buy ≤ trust value, vote or redeem, keep rights and warrants) | T-bill floor plus a free option on a deal pop. | Sponsors pay IPO investors in warrants and an overfunded trust to rent their capital. Retail holders sell below trust. | Gahng, Ritter & Zhang RFS 2023: SPAC IPO investors earned **23.9%/yr** historically. The abstract itself says future returns are likely lower. 2025 revival: 138 SPACs, $25.8B (FTI, via secondary). Median redemption 92–99.6% in 2025, so the arb is crowded. | US equities broker (IBKR); cents a share; lawful | SEC EDGAR (keyless with a User-Agent) for trust per share; **no clean keyless price source verified** (Stooq now has a JS challenge) | **5–8%/yr** (T-bill about 4% + 1–3% + options) | week ≈ 0; month −1 to −3% if bought ≤ trust | 4 in 5 / 3 in 5 to beat 4%; 1 in 20 to reach 15% |
| 5 | **Short VIX-futures roll, sized to the measured tail** | Sell the front VIX future in contango and collect the roll. | Hedgers buy insurance persistently, and the variance risk premium is priced. | Simon & Campasano (EFMA 2013, 2006–2011): the basis predicts futures returns; a hedged version is profitable. **Decay:** Quantpedia reports the out-of-sample conditional version "slightly negative". The measured tail: VIX +115.6% and XIV −96.3% on 2018-02-05. | VIX CFD (Capital.com) or VX/VXM futures (IBKR) | Cboe per-contract VX CSVs, keyless (checked for 2018) | at 0.2× notional, so that the worst day is about −20%: **about 3–8%/yr** | worst day/week **−18 to −20%** at 0.2×; month about −25% | 3 in 4 (a priced premium) / 1 in 4 to beat HLP risk-adjusted |

**Reading.** None of the five is a hidden 100%-a-year edge.
- **#1** is the only one with a plausible path into the owner's
  30–100% zone with a 30–50% drawdown. Its main enemy is decay plus the
  lack of unread calendar.
- **#2** is the most certain money: a cash flow somebody is contractually
  paying. Its size today is unknown, and the first scout step answers that
  cheaply.
- **#3** is cheap to test with real power (150+ post-paper months, plus
  international indices).
- **#4 and #5** are lending-plus, listed so that the orchestrator does not
  re-derive them.

### A1. N7-1 — Crypto trend following (time-series momentum, Donchian ensemble)

- **Mechanism.** Long (or long/short) liquid coins in established
  multi-week uptrends, sized to a volatility target, and flat otherwise.
  Low turnover: positions change on breakouts, not hourly.
- **Why consistent.** Trends in a retail-dominated, leverage-heavy market
  come from under-reaction to slow information plus forced flow that
  feeds on itself across days. The loser is the late, levered crowd and
  the shorts squeezed in uptrends. Fact 2 shows the forced-flow overshoot
  itself is seconds-scale and uncapturable. Trend asks a different
  question: whether the **multi-week** drift is.
- **Evidence.**
  - Zarattini, Pagani & Barbon, *Catching Crypto Trends; A Tactical
    Approach for Bitcoin and Altcoins* (SFI RP 25-80, 2025;
    [RePEc](https://ideas.repec.org/p/chf/rpseri/rp2580.html),
    [SSRN 5209907](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5209907)).
    - Ensemble of Donchian channels, volatility sizing (25% target,
      leverage cap 200% per
      [CXO's summary](https://www.cxoadvisory.com/technical-trading/crypto-asset-trend-following-strategies/)).
    - Rotating top-20 liquid coins: **net Sharpe > 1.5, alpha 10.8%/yr
      vs BTC.**
    - Survivorship-free data, 2015 (BTC from 2010) → 2025-03. Costs of
      10/25/50 bp tested.
  - Liu & Tsyvinski, *Risks and Returns of Cryptocurrency*, RFS 34(6) 2021
    ([RePEc](https://ideas.repec.org/a/oup/rfinst/v34y2021i6p2689-2727..html)):
    "strong time-series momentum".
  - **Against.** Grobys, Kolari, Sandretto, Shahzad & Äijö, *Cryptocurrency
    momentum has (not) its moments*, FMPM 2025
    ([open copy](https://osuva.uwasa.fi/items/2b92e591-acb3-445f-b33d-1a943a8972f5)):
    severe momentum crashes, and one coin can make momentum insignificant
    (mostly cross-sectional).
  - A 2026 SSRN note (Arefev,
    [SSRN 7405060](https://papers.ssrn.com/sol3/Delivery.cfm/7405060.pdf?abstractid=7405060&mirid=1),
    non-peer-reviewed synthesis) reports TSMOM indistinguishable from zero
    over 2016–2023 and negative after 2020-07.
  - **Decay verdict:** credible and unresolved. The headline in-sample
    Sharpe should be halved or worse.
- **Access.** HL perps (taker 4.5 bp, maker 1.5 bp; verified, §B1), Bitget
  (2/6 bp), Binance (2/5 bp). Majors have ample depth at < $1M.
  - The drag is **funding paid while long in uptrends**. It must be
    modelled. It must not be measured conditionally in the scout (see the
    forbidden list below).
  - Warning line: HL is EU-grey; Bitget and Binance are research data plus
    owner use.
- **Data.**
  - Binance spot and UM klines (keyless archive). Locally:
    `binance_years/` 1h, 2021-11 → 2024-01, 257 symbols.
  - HL 8h candles (`carry/`).
- **Calendars.**
  - The paper's sample ends 2025-03, so the only **out-of-paper** span is
    2025-04 → 2026-06 (15 months) plus sealed 2026-07 →.
  - The Binance 2021–23 calendar carries 009 (one trial, funding factor);
    no trend statistic has been computed on it.
  - The HL year (16 trials) has never been read for trend. Its price paths
    were read inside 010 and 012 gap terms.
  - **Power problem:** 15 months of a true Sharpe-1 strategy give t ≈ 1.1.
    A registration should be a pre-registered sign gate on 2025-04 →
    2026-06, then a forward paper period. That is honest but slow.
- **Behaviour-only first step.**
  - The point-in-time universe size each month (top-20 by 30-day volume,
    listed on HL or Bitget).
  - Signal turnover (breakout flips per coin per month) and implied fee
    drag at 5 bp per fill.
  - Unconditional funding level of the universe.
  - Margin at a 25% vol target.
  - **Forbidden conditionings** (name them in the brief):
    - no return after a signal;
    - no funding conditional on signal state;
    - no "already in position" split;
    - no comparison of lookback sets.
- **Pre-registrable rule (sketch).**
  - The paper's ensemble and lookbacks copied verbatim, with no tuning.
  - Long-only primary (the paper's); long/short reported.
  - 25% vol target, leverage cap 2×.
  - Daily close decision; maker entry with a taker fallback.
  - Costs at 6 bp per fill plus real funding.
  - Criterion: alpha vs BTC buy-and-hold > 0 with t ≥ 1.645 on the
    out-of-paper span, then a forward paper span to 2027-06.
- **Expected return / tail / capacity / Kelly.**
  - If alive at half the paper's Sharpe (≈ 0.75) and 25% vol: about 15–20%
    a year above cash, with a 30–45% maximum drawdown.
  - Worst week −15 to −20%, worst month −25 to −30% (a trend break while
    long).
  - Capacity: millions in the top 20.
  - Full Kelly at SR 0.75 and σ 25% is a leverage of SR/σ = 3×. Run at
    **¼–⅓ Kelly**, about the 25% vol target itself.
- **Odds.** Effect (alpha vs BTC, net, post-2021) exists: **1 in 3**.
  Deployable if it exists: **2 in 3**.

### A2. N7-2 — BNB reward stack, delta-hedged

- **Mechanism.**
  - Hold BNB in Simple Earn, which makes it eligible for HODLer airdrops,
    and/or Launchpool. Sell each reward token at listing. Short an equal
    BNB notional in a perp so the book is price-neutral.
  - Net = airdrop value + Simple Earn APR + funding received by the short
    − fees − hedge slippage.
- **Why consistent.** The payer is not a counterparty in a trade but a
  marketing budget. Binance and the listed projects pay BNB holders for
  distribution and for BNB demand. It persists as long as Binance runs the
  programme and keeps listing tokens. The loser, if any, is the buyer of
  the airdropped token at listing.
- **Evidence** (weaker than academic; flagged):
  - Binance's own analysis, carried by sponsored press: 1 BNB held from
    2024-01-01 through 2025-Q1 received about **$226 of token rewards**
    valued at first-day prices
    ([The Daily Hodl, sponsored](https://dailyhodl.com/2025/04/15/bnb-delivers-177-in-returns-over-the-last-15-months-as-binance-dominates-airdrop-rewards-and-token-listings/);
    [blockchain.news](https://blockchain.news/news/bnb-holders-177-percent-returns-launchpool-airdrops-2025)).
    The same piece cites a CoinMarketCap study: **53–78% combined yields
    in 2024**.
  - Per-event top pools: SAGA, ENA and PIXEL paid $9–13 per BNB staked.
  - **2025 cadence:** HODLer project numbers run from #10 (MyShell,
    2025-02) to #58 (Allora, 2025-10), so about 6 a month. Reported pools
    are about $10–12.5M each (Newton $12.5M, Yield Basis $10M)
    ([cryptoninjas YB](https://www.cryptoninjas.net/news/binance-lists-yield-basis-yb-after-10m-airdrop-to-bnb-holders-53rd-hodler-project/amp/),
    [ALLO](https://www.cryptoninjas.net/news/binance-lists-allora-allo-as-58th-hodler-airdrop-with-15m-tokens-backing-bnb-earn-subscriptions/)).
  - **Snapshots are retroactive, random and hourly-averaged**, with a cap
    of 4% of the pool average per user
    ([NFT Plazas, YB terms](https://nftplazas.com/yield-basis-yb-to-binance/)).
    So the stake must be held continuously, and the hedge too.
- **My arithmetic** (illustrative, unverified inputs):
  - About 6 airdrops a month × about $10M is about $0.7B a year.
  - The pool is the BNB in Simple Earn, which is unpublished in what I
    found. If that is 20–40M BNB at about $600–1,000 ($12–40B), the
    HODLer yield is **about 2–6%/yr**. Add Launchpool events and Simple
    Earn's base APR.
  - That puts a 2025-like regime at **3–10%/yr hedged**. A 2024-like
    listing boom gives 20–50%.
  - **The decay is in the listing cycle, not in the mechanism.**
- **Access.**
  - Binance Simple Earn: no fee; flexible redemption.
  - The hedge: a BNB perp short.
    - On Binance it costs 5 bp taker / 2 bp maker.
    - **Better on Hyperliquid:** if Binance froze, BNB would gap down (as
      FTT did), and an HL short would pay out. That partly hedges the venue
      tail. Verify HL BNB depth and funding history in the scout.
  - Warning line: Binance is not licensed for German residents; some Earn
    products are geo-restricted in parts of the EU. Check the owner's
    account.
- **Data.**
  - Binance announcements (public web; airdrop size, snapshot window,
    listing time).
  - Binance spot klines of each reward token from its listing day
    (keyless archive; small).
  - BNB perp funding: local `carry/` to 2026-06, plus HL.
  - Unknown: **the eligible BNB pool size per event.** If Binance does not
    publish it, the yield must be inferred from user-level reports, which
    is weak.
  - Calendars: an entirely new family. 2026-07 → stays sealed.
- **Behaviour-only first step (counts and terms only).**
  - Count every HODLer, Launchpool and Megadrop event from 2024-01 →
    2026-06 from announcements.
  - Record reward token amounts, snapshot windows, eligibility rules and
    caps.
  - Find out whether a pool-size figure exists anywhere public.
  - Check the BNB perp's existence and depth on HL and Bitget.
  - Check the owner's Simple Earn eligibility.
  - **Forbidden conditionings:**
    - no listing-day price paths by event;
    - no "hold vs sell" comparisons;
    - no funding conditional on airdrop windows.
- **Pre-registrable rule (sketch).**
  - Continuous Simple Earn BNB with a 1× BNB short on HL.
  - Every reward sold at a fixed time after listing (for example the first
    1h VWAP), not chosen ex post.
  - Outcome: monthly net yield on capital.
  - Criterion: annualised net ≥ 4%/yr (HLP calm) with a month-clustered
    t ≥ t_{G−1}.
  - Data window 2025-01 → 2026-06; forward from 2026-07.
- **Tail and sizing.**
  - Market risk is small: a hedged book.
  - The tail is the venue. Binance freezing or failing is **unpriced**, at
    a few % a year. Size the Binance balance as money one accepts losing
    (Kelly is meaningless for a binary venue tail).
  - The HL short partly offsets it.
  - Worst ordinary week about −2 to −5%: a listing dumps before the sale,
    or BNB funding spikes against the short.
- **Odds.** The cash flow exists: **9 in 10.** Net ≥ 4%/yr hedged, today:
  **1 in 2.** ≥ 15%/yr: **1 in 5.**

### A3. N7-3 — Month-end liquidity cycle in equity indices, re-anchored to the settlement cycle

- **Mechanism.**
  - Institutions that need cash on the last day of the month (T) sell by
    T−(settlement+1). Prices are pressed down over T−8…T−4 and rebound
    over T−3…T+3.
  - Long the index over the rebound window; flat (primary) or short
    (variant) over the pressure window.
- **Why consistent.** The payment calendar (pensions, salaries, fund
    redemptions) is contractual. The settlement lag fixes the selling day
    mechanically, and the sellers are agents who trade for liquidity, not
    price. Etula et al. attribute their persistence to agency and
    reputational constraints.
- **Evidence.**
  - Etula, Rinne, Suominen & Vaittinen, *Dash for Cash*, RFS 33(1) 2020
    ([RePEc](https://ideas.repec.org/a/oup/rfinst/v33y2020i1p75-111..html);
    the working-paper text was read).
    - Since 1995 (T+3 settlement): CRSP VW raw **−17 bp over T−8…T−4 and
      +77 bp over T−3…T+3**. Abnormal −37 bp and +48 bp (T−3…T−1:
      +25 bp).
    - Cumulative excess return in the window: 103%, which is 73% of the
      US equity excess return. Correlation of pressure and rebound −0.54.
    - The pattern holds in other developed markets. A
      settlement-convention change in Europe gives a difference-in-
      differences test.
    - Equity sample to 2013-12.
  - McConnell & Xu, FAJ 64(2) 2008
    ([RePEc](https://ideas.repec.org/a/taf/ufajxx/v64y2008i2p49-64.html)):
    1926–2005; the US equity premium was earned almost entirely at the
    turn of the month; 31 of 35 countries.
  - **Decay, mixed.** Carchano & Pardo (index futures 1991–2008): the
    turn-of-month effect is significant in both halves. A Fed working
    paper claims it disappears after 1990 (both via a
    [secondary search summary](https://cxoadvisory.com/calendar-effects/stock-index-futures-calendar-effects);
    not read in full).
- **The pre-registrable sharpening.** US settlement moved from T+3 to T+2
  in 2017 and to T+1 on **2024-05-28**
  ([Davis Polk](https://www.davispolk.com/insights/client-update/sec-adopts-t-1-settlement-effective-may-2024)).
  The 2017 date is to be confirmed against the SEC release. If the
  mechanism is real, **the pressure window moves later by one day at each
  change.** A calendar-only rule fixed now, with windows defined by the
  settlement lag, tests the mechanism rather than the calendar anomaly.
- **Access.**
  - US500 CFD on Capital.com (spread and overnight financing to verify)
    or MES micro futures via IBKR.
  - Two round trips a month, so costs are negligible relative to 50–80 bp
    per event.
  - Leverage 2–3× is available on both.
  - Lawful (warning line: CFDs are leveraged retail products).
- **Data.**
  - Ken French daily factors (`F-F_Research_Data_Factors_daily_CSV.zip`,
    178 KB, keyless, last modified 2026-09-25). It **contains post-2026-07
    rows: filter at load.**
  - French's developed-market regional daily factors (for power).
  - FRED SP500 (2016-10 →, keyless; checked).
- **Unread calendars.**
  - 2014-01 → 2026-06 is out of the paper's sample and never read by this
    programme: about 150 US months, plus the regions.
  - 2026-07 → sealed.
- **Power** (arithmetic, not data). A 7-day window has a monthly SD of
  about 2.5–3%. Over 150 months the SE is about 0.23%. The full +48 bp
  effect gives t ≈ 2; half of it gives t ≈ 1. Regions add partial
  independence. The test is decent for a large effect and weak for a
  halved one.
- **Behaviour-only first step.**
  - Build the trading-day calendar (T, T−k, T+k per market).
  - Record settlement-regime dates per market (US, EU T+2 in 2014, others).
  - Check CFD and futures costs and financing on the owner's venues.
  - Check data coverage.
  - **Forbidden:** any return by window before registration, including
    "just looking at 2014–2016".
- **Pre-registrable rule (sketch).**
  - Long from the close of T−(s+1) to the close of T+3, where s is the
    settlement lag in force. Flat otherwise. 2× notional.
  - Primary: US. Regions reported. Variant: short over T−8…T−(s+2).
  - Criterion: mean window excess return > 0, month-clustered
    t ≥ t_{G−1}(0.95) over 2014-01 → 2026-06.
  - Second criterion: the settlement-shift prediction. The rebound's start
    moves with s, comparing the T+3, T+2 and T+1 eras.
- **Expected return / tail / Kelly.**
  - At 1× and half the effect: about 3%/yr abnormal plus the equity
    premium earned in-window, about 4–6%/yr total. At 2–3×: **8–15%/yr.**
  - Tail at 3×: a crash inside a window (2008-type), worst week about
    −45%, month about −50%.
  - Sharpe of a 7-days-a-month exposure is perhaps 0.5–0.7, so full Kelly
    is about 2–3×. **¼–½ Kelly ≈ 1–1.5×** is the sane size, which brings
    the return back to about 5–8%/yr.
- **Odds.** The effect persists after 2013: **1 in 3.** Deployable if it
  exists: **4 in 5.** It reaches the owner's 30% zone: **under 1 in 10.**

### A4. N7-4 — SPAC trust arbitrage

- **Mechanism.** Buy SPAC shares at or below their cash-in-trust value
  (US T-bills). Either redeem at trust at the vote, or keep the shares if
  the deal pops. Keep the free rights and warrants.
- **Why consistent.** Sponsors rent capital and pay for it with warrants,
  rights and an overfunded trust. Holders who need liquidity sell below
  trust. The floor is legal (the redemption right).
- **Evidence.**
  - Gahng, Ritter & Zhang, *SPACs*, RFS 36 (2023), 3463–3501: SPAC IPO
    investors earned **23.9%/yr** over the SPAC period. Sponsors earned
    over 100%. Post-merger common shares underperformed by about 30%/yr.
    The authors expect future investor returns to be lower.
  - 2025 revival: 138 SPACs raised $25.8B (FTI Consulting via
    [Angel Investors Network](https://angelinvestorsnetwork.com/market-analysis/spac-revival-2026-is-the-market-actually-back)).
  - Median redemption rates 91.7% (Q1), 99.6% (Q2), 96.2% (Q3) and 94.5%
    (Q4) in 2025
    ([Houlihan Capital Q4 2025](https://www.houlihancapital.com/wp-content/uploads/2026/02/Q4-2025-SPAC-Report-1.pdf)).
    **Almost every holder redeems: the arb is crowded and priced near
    trust.**
- **Access.** IBKR or a similar broker with US stocks, rights and
  warrants (EU retail can hold US common shares). Cents per share. Thin
  books: many small positions. Lawful.
- **Data.**
  - SEC EDGAR (keyless with a User-Agent) for trust per share, deadlines
    and votes.
  - **Prices:** no keyless, terms-clean daily source verified (Stooq
    returned a JavaScript challenge). This is the scout's first blocker.
  - Calendars: new; 2026-07 → sealed.
- **Behaviour-only first step.**
  - Count SPACs trading 2025–26 and their trust per share.
  - Daily volume and quoted spread (levels only).
  - Rights and warrants attached.
  - Broker access for the owner.
- **Rule (sketch).**
  - Buy when price ≤ trust − x (x fixed now from costs), no deal
    announced, and ≤ 12 months to the deadline. Redeem at every vote.
    Sell rights and warrants at the merger.
  - Criterion: annualised ≥ T-bill + 2%.
- **Return / tail.**
  - **5–8%/yr**, near-zero beta.
  - Worst month −1 to −3% (bought slightly above trust, or a deal pop
    reverses before the vote). Trust fraud is rare but non-zero.
  - Capacity: about $100k–$1M across many names.
  - Kelly: not binding. Leverage (Reg-T 2×) costs about the T-bill rate,
    so it adds little.
- **Odds.** Exists: **4 in 5.** ≥ 4%/yr: **3 in 5.** ≥ 15%: **1 in 20.**
  Small-capacity and high-Sharpe, but lending-plus.

### A5. N7-5 — Short VIX-futures roll, sized to the measured tail

- **Mechanism.** Short the front VIX future when the curve is in contango
  and collect the roll-down.
- **Why consistent.** Portfolio hedgers buy volatility insurance
  regardless of price, so the variance risk premium is a **priced**
  premium, with the crash in the price.
- **Evidence.**
  - Simon & Campasano, *The VIX Futures Basis: Evidence and Trading
    Strategies* (EFMA 2013,
    [paper](https://www.efmaefm.org/0EFMAMEETINGS/EFMA%20ANNUAL%20MEETINGS/2013-Reading/papers/EFMA2013_0164_fullpaper.pdf)),
    2006–2011. The basis does not predict spot VIX but does predict VIX
    futures returns. A hedged short-in-contango strategy was profitable.
  - **Decay:** Quantpedia's replication reports an out-of-sample
    "slightly negative" result for the conditional rule
    ([Quantpedia](https://quantpedia.com/strategies/exploiting-term-structure-of-vix-futures)).
  - **Measured tail:** on 2018-02-05 the VIX rose 115.6%. XIV (short VIX
    futures, 1×) lost 96.3% and was terminated. SVXY lost about 96.1%
    ([etf.com](https://www.etf.com/sections/news/inverse-vix-etn-shuts-down)).
- **Access.** VIX CFD (Capital.com) or VX/VXM futures (IBKR). Lawful.
- **Data.** Cboe per-contract VX CSVs, keyless (format checked on a 2018
  file: trade date, OHLC, settle, volume, OI). They run from 2013 in this
  format; older years are in the CFE archive. Calendars unread by this
  programme: all; 2026-07 → sealed.
- **Behaviour-only first step.**
  - Contract coverage and roll calendar.
  - Fraction of days in contango (a state, not a return).
  - CFD financing and spread.
- **Rule (sketch).** Short the front VX in contango; notional fixed at
  0.2× capital; no stop (a stop gaps). Criterion: annualised net ≥ HLP
  calm with a pre-stated worst-day bound.
- **Return / tail / Kelly.**
  - A one-day loss of up to 96% at 1× means the notional must be about
    0.2× capital for a −20% worst day.
  - At that size the premium yields an estimated **3–8%/yr** on capital.
  - Kelly under a fat left tail is far below the Gaussian figure: ≤ 0.2–0.3×
    notional.
  - Worst week −18 to −20%, month about −25%.
- **Odds.** The premium exists: **3 in 4.** It beats HLP risk-adjusted:
  **1 in 4.** It is negatively skewed, the opposite of the owner's
  convexity preference. Kept only because the brief names "levered risk
  premia with a measured tail".

### A6. Note handed to N3 (not my light)

Two related events belong to N3, and I add nothing to its scout:
- **Pre-launch perps** (HL hyperps, Bybit pre-market) shorted into the
  token generation event.
- **Exchange delisting notices.**

My only finding: event studies of listings exist; for example,
[Blockchain Research Lab WP3](https://www.blockchainresearchlab.org/wp-content/uploads/2019/10/Exploring-Market-Reactions-to-Exchange-Listings-of-Cryptocurrencies-BRL-working-paper3.pdf)
reports +5.7% abnormal on listing day, over 327 listings. Delisting
studies are thin, and the announcement move is a seconds-latency race.

### A7. Rejected ideas, and why

| idea | why rejected |
|---|---|
| **Pre-FOMC drift** (Lucca & Moench 2015, 1994–2011) | **Decayed.** Kurov, Wolfe & Gilbert (FRL 40, 2021): the drift "essentially disappeared after 2015" ([RePEc](https://ideas.repec.org/a/eee/finlet/v40y2021ics1544612320315956.html)). |
| **Index-inclusion front-running** (S&P 500 adds and deletes) | **Decayed.** Greenwood & Sammon (JF 2025): addition effect 7.4% in the 1990s, 0.3% / < 1% in the last decade ([Harvard DASH](https://dash.harvard.edu/entities/publication/2cc676bd-c8da-40e7-b112-3ecc8644e687)). |
| **FX month-end fix hedging flow** (Melvin & Prins, JFM 2015, 2004–2012) | Real but tiny. A 10% monthly equity move predicts **14 bp** of currency depreciation in the last hour before the fix (working paper text read), so about 5 bp per event for typical months. Under 1%/yr unlevered, and the fix window was widened in 2015. |
| **Market intraday momentum / gamma-hedging close flow** (Gao et al. JFE 2018; Baltussen et al. JFE 2021, 1974–2020) | **Decay plus data.** A 2022–2026 test on 1,085 SPX sessions finds the unconditional slope flat every year ([dev.to, non-peer-reviewed](https://dev.to/firmtape/intraday-momentum-is-dead-in-the-0dte-era-we-measured-it-on-1085-spx-sessions-43g0)). A gamma-conditioned version needs dealer-gamma data that is not free, and there is no keyless multi-year intraday index source. |
| **Conditional VIX-basis trade** (Simon & Campasano, hedged) | Out-of-sample "slightly negative" (Quantpedia). Only the unconditional premium survives, as N7-5. |
| **Sports value betting vs soft bookmakers** (Kaunitz, Zhong & Kreiner, [arXiv 1710.02824](https://arxiv.org/pdf/1710.02824)) | Works on paper, but **bookmakers limit winning accounts within months** (the paper's own experience). Capacity collapses, and a German stake tax applies. Not consistent at any size. |
| **Exchange listing/delisting announcement trades** | A seconds-latency race on announcements, like H3/fact 2. Thin delisting evidence. Handed to N3. |
| **CEX–DEX arbitrage / AMM loss-versus-rebalancing capture** | A block-builder and searcher latency race. Same failure as cross-venue seconds capture (fact 2, H3). |
| **DeFi lending liquidations** | Measured by H11: top-10 liquidators took 87.6% by count and over 99% by USD, and quiet months route through an auction. Not open at our size. |
| **Stablecoin or LST depeg backstop buying** | A handful of events a year: untestable with power. The tail (a real insolvency, as with UST) is unpriced. Fails "consistent". |
| **Leveraged-ETF volatility-decay pair** (short both the bull and the bear 3× ETF) | Borrow fees on LETFs typically absorb the decay; it is short gamma on trending days; EU retail access to US ETFs is restricted (PRIIPs). No verified net evidence. |
| **Funding carry with taker fills; follower/copy designs; cross-venue seconds capture; deep resting ladders** | Killed by established facts 1, 2, 3, 4 and 5. |
| **Maker-executed cross-venue carry (N4)** | See Part B: capped at about the lending rate even if execution were free. |
| **Merger arbitrage** | Concave (short a deal-break put). Historically lending-plus. Strictly dominated by N7-4's convex floor for our purposes. No new evidence sought. |

---

## Part B — N4 feasibility: maker-only carry with a gap stop

### B1. Fees at retail tiers (per fill, % of notional)

| venue | maker | taker | rebate reachable at our size? | source |
|---|---|---|---|---|
| Hyperliquid, tier 0 (< $5M 14-day) | **0.015** | 0.045 | **No.** Rebates (−0.001 to −0.003%) need > 0.5–3% of *exchange-wide* maker volume. HYPE staking cuts fees 5–40% (> 10 to > 500k HYPE). | verified: [HL fees](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees) |
| Binance USDⓈ-M, VIP 0 | **0.020** (0.018 with BNB) | 0.050 (0.045) | No; maker rebates exist only at top VIP or MM tiers. Albers et al. used a best tier of −0.5 bp maker / 1.5 bp taker. | secondary: [Finder](https://www.finder.com/binance-futures-fees); BNB 10%: [Binance](https://www.binance.com/en/support/articles/360033544231); the fee page did not render |
| Bybit, non-VIP | **0.020** | 0.055 | No | secondary ([Bitsgap](https://bitsgap.com/blog/bybit-trading-fees-explained-what-it-costs)); the official page timed out |
| Bitget, standard | **0.020** | 0.060 | No; BGB payment is reportedly −20% (secondary) | verified: [Bitget support, 2026-08-20](https://www.bitget.com/support/articles/12560603892733) |

### B2. Can a credible maker-fill model be built from free data?

| source | what it is | coverage | size | verdict |
|---|---|---|---|---|
| **Local Binance `bookDepth`** (`data/binance/um/bookDepth_parquet/`) | Schema `timestamp, percentage, depth, notional`. Cumulative depth at **±1…5% bands**, about 2,757 snapshots a day (≈ 31 s). | By filename, **not cascade days only**: BTC 334, ETH 325, SOL 317, HYPE 302, XRP 274, DOGE 121 days in 2025-07-28 → 2026-06-30 (the HL year) | on disk | **Useless at the touch.** No best bid or ask, no queue. Only good for depth-at-distance (fact 4's 1–3% orders). |
| **Local Binance `aggTrades`** | `price, quantity, transact_time, is_buyer_maker` | same days | on disk | Supports a conservative **trade-through** fill rule (a resting bid fills only when a sell prints strictly below it). No queue. |
| **Binance UM `bookTicker` archive** (keyless) | `update_id, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, transaction_time, event_time` | **2023-05-16 → 2024-03-30 only**, 315 symbols (listing checked) | BTC 2–8 GB a month; small alts 2–17 MB a day | Top of book plus aggTrades gives a touch-fill model with a queue-size bound. **Gaps:** the one ENS day inspected spans only 15.2 h of 24 (262,483 updates, mean 209 ms). The window shares 010's discovery calendar. |
| **Bybit `ob500` archive** (keyless, `quote-saver.bycsi.com`) | JSON lines `{topic, type, ts, cts, data{s,b,a,u,seq}}`. A 500-level snapshot plus deltas (ENA 2025-01-01: 2 snapshots, 786,330 deltas, about 110 ms). Plus trades at `public.bybit.com/trading/`. | linear perps from **early 2023** (2023-01-01 absent, 2023-02-01 present) → now | BTC 100–220 MB a day; SOL 112, WIF 100, ENA 44 MB | **The one free source for a full queue-position model** (displayed size ahead, decremented by trades). Bybit leg only. |
| **Tardis free first-of-month** | Documented: "Historical datasets for the first day of each month are available to download without API key" ([Tardis API docs](https://docs.tardis.dev/downloadable-csv-files/api.md)) | 12 days a year per exchange, including HL | — | **Blocked from this machine** (Cloudflare 403 on curl, with and without a browser user agent). Not routed around. 12 days a year is too few for a carry model anyway. |
| **HL order-book history** | `hyperliquid-archive` L2 snapshots | — | requester-pays | **Forbidden here.** The HL leg could use the local every-account fills (exact maker fills by price), but only in the HL year (16 trials). |

**Verdict on (ii).** A credible model is possible on the **Bybit** leg
(full L2 + trades, 2023 →). It is crude on the **Binance** leg (top of
book, 2023-05 → 2024-03, with gaps). On the **HL** leg, which was 012's
whole lead, it is only a trade-through rule from the local fills of the
HL year. A queue model for HL needs requester-pays data.

### B3. Published evidence on adverse selection at the touch

- **Albers, Cucuringu, Howison & Shestopaloff**, *The Market Maker's
  Dilemma* ([arXiv 2502.18625v2](https://arxiv.org/html/2502.18625v2)).
  A live experiment on **Binance's BTCUSDT perp**:
  - Data: 2024-02-12/19 (232,897 minimum-size orders, 127,051 filled) and
    2024-08-21/31.
  - 1-second markouts of touch fills: **−0.06 bp (front of queue) to
    −1.16 bp (back of queue)**.
  - Unconditional expected maker return over 5 s: **about −0.8 bp** (−0.3
    net of their −0.5 bp rebate).
  - Fill probability falls to about 30% when the near queue is large, and
    exceeds 90% when it is tiny.
  - Fills cluster when the next move is adverse: P(fill) is about 0 if
    the mid moves away from the order, and about 1 if it moves through.
  - A naive two-sided maker **lost almost 60% in about 3 days** (Sharpe
    −109).
- **Fact 4 (003).** HL resting orders at 1–3% from price, filled by
  non-forced flow: −14 bp at 15 minutes. These are deep orders that fill
  only on moves, which is the extreme of the same selection.
- **Kaminski & Lo**, *When Do Stop-Loss Rules Stop Losses?*, JFM 18
  (2014) ([MIT copy](https://dspace.mit.edu/bitstream/handle/1721.1/114876/Lo_When%20Do%20Stop-Loss.pdf)).
  Under a random walk a stop always lowers expected return. A stop adds
  value only when the stopped series trends (momentum). Under mean
  reversion it locks in the loss before the reversion.

**What this means for carry.** At the touch on a major, adverse selection
is about 1 bp per fill at seconds. That is small next to a 2 bp maker fee.
On alts, at minute horizons, and for a **hedged pair** it is worse,
because of legging. The fill of leg A is selected for a price move that
makes the leg-B order (same coin, other venue) less likely to fill at the
intended spread. You then either chase B as taker, or carry naked
exposure until B fills.

### B4. The arithmetic (012's per-position numbers, bp of N; hold about 13–17 days)

012's diagnostics: funding **+27.8**, price gap **−38.0** (median +2.2),
fees and slippage **−34.8**, plus margin top-ups. All are in bp of N. The
registered mean of −25.8 is on gross capital, R = (funding + gap − costs) / 2,
as in 010 §4. The "net" column below is on N.

| execution | fees (4 fills) | adverse selection | legging/chase | total cost | net = 27.8 − 38.0 − cost |
|---|---|---|---|---|---|
| taker × 4 (012) | 19–21 + slippage tiers | — | — | **≈ 35** | **≈ −45** on N; 012's registered −25.8 is this sum plus top-ups, **÷ 2 (gross capital)** |
| maker one leg + taker hedge | 2 × (1.5 + 5.0) = 13 | 2 × 1–5 = 2–10 | slippage on the taker leg 2 × 1–6 = 2–12 | **≈ 17–35** | **≈ −27 to −45** |
| maker × 4 | 2 × (1.5 + 2.0) = 7 | 4 × 1–5 = 4–20 | chase 30–50% of pairs at +3.5 bp + spread: 2–8 | **≈ 13–35** | **≈ −23 to −45** |
| **maker × 4, perfect gap stop (gap = 0)** | 7 | 4–20 | 2–8 | 13–35 | **≈ −7 to +15** |
| **free execution, no gap loss** | 0 | 0 | 0 | 0 | **+27.8 ≈ 6.3%/yr on N ≈ 3.2%/yr on gross capital** |

**The decisive line is the last one.** 012's funding leg, collected in
full with zero cost and zero gap loss, is about 3%/yr on gross capital:
HLP's lending leg. Leverage of 3× per leg lifts that to about 9%/yr on
equity, but it brings in exactly the tail the gap stop is meant to
remove: a +33% squeeze liquidates the short leg.

**Can a gap stop truncate the −38 bp tail?**
- The loss is a fat left tail of a spread that is arbitrage-bounded and
  mostly reverting. The median is +2.2, so most positions converge
  slightly in our favour.
- Under Kaminski & Lo, a stop on a reverting series **crystallises**
  losses that would otherwise have reverted. When HL's premium widens,
  HL funding to the short also rises, so stopping out gives up the
  compensating carry.
- The part that is not reverting is venue-specific jumps: halts,
  delistings, venue-set settlement prices, name and oracle breaks. A stop
  cannot fill inside a jump; the exit comes after it, at taker cost and a
  wide spread.
- **A stop helps only on continuous, trending divergence.** Whether that
  is a large share of the −38 bp could be measured only by computing the
  tail's path shape. That would be a conditional outcome statistic on
  012's own exposed data: forbidden here, and a new trial on the HL year.

**Verdict.**
- **N4 does not deserve a data scout as framed.**
  - Even the best case (maker execution about 13 bp plus a stop that
    removes about 70% of the gap loss) nets roughly 0 to +10 bp per
    15-day position. That is ≤ 2%/yr on gross capital, below HLP's
    lending leg.
  - It would be trial 17 on the HL year, or it would need the HL holdout,
    which D18 reserves for L3.
- **Odds:**
  - maker execution cuts realised cost to ≤ 15 bp on HL pairs: 1 in 2;
  - a stop removes ≥ 60% of the gap loss without cutting funding: 1 in 4;
  - the joint result beats HLP: **about 1 in 25.**
- **Redirect, if a maker-fill model is wanted at all:** build it once on
  Bybit `ob500` plus trades (free, 2023 →) as shared infrastructure.
  - It matters more for **weekly cross-sectional carry** (009 F1). There,
    taker costs (13.4 bp a week) were a third of the funding collected
    (+37.4 bp a week). Maker execution might add about 4%/yr, lifting a
    Sharpe of about 0.4 to about 0.6.
  - It would also serve any maker-executed N1 (`xyz`) design.
  - Still lending-plus. Not a priority.

---

## Constraints honoured

- No outcome, P&L, forward return or conditional statistic was computed
  on any local or downloaded data.
- Local data: filenames plus one schema `DESCRIBE` of a 2025-11-07
  bookDepth file and one aggTrades file. Nothing dated ≥ 2026-07-01 was
  opened.
- Downloads: 2 files, 48 MB in total, each in a new directory with
  `SHA256SUMS`, read with `python -I`.
- Listings and HEAD requests only for the Binance, Bybit and Tardis
  archives. One 2018 Cboe VX CSV header was printed (format check).
- Nothing was published or uploaded. No `lib/`, `pages/`, registration,
  `results.jsonl` or other-scout files were touched. No commits.
- **Correction to the brief:** the local Binance aggTrades and bookDepth
  are not "HL-cascade days only". By filename they cover 121–334 days per
  coin across the whole HL year.
