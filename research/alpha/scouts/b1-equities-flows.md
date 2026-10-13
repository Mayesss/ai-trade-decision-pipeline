# B1 scout: US equities, ETFs, rates — scheduled and structural flows

2026-10-13. Round 4, behaviour only. **No return, P&L, conditional mean,
hit rate or funding level was computed on any data.** Every return figure
below is quoted from a source I opened (sample period stated), or is
arithmetic on published fees, published magnitudes and **unconditional**
volatilities (labelled). Rows dated on or after 2026-07-01 were dropped at
load and never printed.

Forbidden conditionings honoured: no statistic links any signal, state or
calendar window to a later price or return; no return by window, event or
calendar on any data (not even 2014–2016); no count split by a future
quantity or position state; no comparison of variants' outcomes; no funding
statistic of any kind. The xyz fills were **not opened**; no 1-minute bars
were built.

---

## 1. Summary

1. **Best: the month-end institutional-flow family.** T1 is Etula's liquidity cycle with windows moved by the settlement lag (149 months unread, 44/80/25 under T+3/T+2/T+1). T2 is Harvey–Mazzoleni–Melone's rebalancing front-run (1997–2023: SR 1.11, skew +5.23). Free data, ≈ 1–2 bp per round trip on MES or the xyz SP500. T1 is out of sample; T2 is a replication (authors' sample runs to 2025).
2. **Second: the Treasury auction cycle.** Best power here (2y/5y: t ≈ 3.5–4 full, 1.7–2.0 half, over 150 auctions each), futures only (IBKR). It is lending-plus at a robust size: ≈ 5%/yr at 10% vol and half the effect.
3. **Dead or lending-like:**
   - overnight drift: ≈ 0 in 2021–2025 per its own authors, and costs exceed it;
   - xyz funding carry: ≈ 4.7%/yr total against a 3.6–4.4% T-bill;
   - PEAD: gone for large caps since 2006;
   - earnings-week premium on xyz names: blocked by data and survivorship.
4. **Honest landing:** only #1 at near-full effect reaches 20–30%/yr (2–3×, 25–35% vol). At half effect each light gives ≈ 5–11%/yr.

---

## 2. Ranked table

Costs per round trip (RT) are from published schedules (§2.2). The
"half-effect" columns use half the literature magnitude. Odds are "effect
exists out of sample" / "deployable and beating HLP risk-adjusted".

| # | light | mechanism | who pays, why it persists | evidence, sample, decay | instrument, cost/RT | free data (coverage; end-datable?) | unread calendar; power (lit / half) | E[r] at robust size (assumptions) | tail at that size | Kelly | capacity | odds |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1a | **Month-end liquidity cycle, settlement-shifted** (T1) | Institutions sell to have cash on the last trading day T, at T−s for settlement lag s. Prices are pressed over T−8…T−(s+1) and rebound over T−s…T+3. | Pension, salary and redemption payments are contractual and month-end. Sellers are agents trading for liquidity; arbitrage is limited by risk and agency (Etula). | Etula et al. RFS 2020, US 1995-07→2013-12: T−3…T+3 raw +77 bp, abnormal +48; T−8…T−4 abnormal −37. 20 of 23 markets show significant reversals. EU DiD around the 2014-10-06 T+2 switch. Reversals 2–3× larger when T is a Friday. **Decay:** unknown after 2013 (US). | MES ≈ 0.8 bp; xyz SP500 2.2 bp + ≈0.8 bp per calendar day held long in excess terms (a short gains it); CFD long ≈ spread + 1.4 bp/day | French daily (US 1926→, regions 1990→; static file, **filtered** at load); calendar built (149 months); settlement dates verified | **2014-01→2026-05, 149 months** (44 T+3, 80 T+2, 25 T+1; 64 Friday-T). L/S abnormal 85 bp: **t 2.67 (power 0.85) / t 1.34 (0.38)**. Long-only 48 bp: t 2.11 / 1.06 | Half effect, L/S, xyz: ≈ 36 bp/month net → 4.3%/yr at 1×, vol ≈ 13% (SR 0.33). At 1.2× (½ Kelly of the half case) ≈ **5%/yr**; at full effect ≈ 11%/yr at 1.2×, 25%/yr at 2.6× | Long leg in a crash window. Documented 2-day S&P falls: −10.5% (2025-04-03/04), −13.9% (2020-03) → **week −13…−17%, month −15…−20% at 1.2×** | Full Kelly 2.5× (half effect) to 5.4× (full); use 1–1.2× | Unlimited at our size | 1 in 3 / 1 in 5 |
| 1b | **Pension-rebalancing front-run** (T2) | Simulated 60/40 drift → expected rebalancing trades: Threshold (bands 0–2.5%) and Calendar (month-end; week-4 sign). Long the asset rebalancers will buy. | DB pensions, TDFs and balanced funds rebalance mechanically to policy weights, on calendars or bands. Their trades are predictable and they do not hide them. | Harvey, Mazzoleni & Melone (NBER w33554, Jan-2026 draft), 1997-09-10→2023-03-17: equity −16/−17 bp next day; strategy 10.20%/yr excess, vol 9.17%, **SR 1.11, skew 5.23**; SR 0.90 excluding GFC and Mar-2020; robust to transaction costs. **Decay:** the authors say the effect strengthened through 2025 (footnote); post-publication window ≈ 2025-03→ | Equity leg xyz SP500 (2.2 bp) or MES; bond leg ZN/ZF (IBKR, ≈1–2 bp); daily resizing | French Mkt (S&P proxy) + Treasury 10y par yield (note-return proxy by duration), both free and end-dated at load | All of 2014-01→2026-06 is unread by us **but in the authors' sample**. Truly post-publication ≈ 16 months: t ≈ 1.15 at SR 1 (powerless). Over 12.5 yr: **t 3.5 / 1.8** | At SR 0.55 (half) and 20% vol: ≈ **11%/yr**; at SR 1.1 and 20% vol: 22%/yr; costs ≈ 0.5–1.5%/yr (assumption: turnover) | Positive skew, but sizes up in turmoil. Worst week at 20% vol ≈ −8…−12% (assumption: 3–4σ weekly) | Full Kelly vol = SR (55–110%); use ¼–⅓ → 15–25% vol | Large (index futures) | 3 in 5 (in 2014–25) · 1 in 3 post-pub / 1 in 4 |
| 2 | **Treasury auction cycle** | Dealers absorb pre-announced supply; on-the-run notes cheapen 5–10 days before an auction and recover after. | The Treasury, through issuance (hidden cost 9–18 bp of auction size), and end-investors with slow-moving capital. Supply is scheduled every month. | Lou, Yan & Zhang RFS 2013, 1980-01→2008-06: 5-day pre-minus-post differential 2y 8.89 bp (t 2.93), 5y 22.54 (t 3.67), 10y 23.84 (t 1.78); SR 1.08 (1.44 in 1998–2008, ≈1 after spread). **Decay:** Fleming, Liu & Nguyen (NY Fed SR 1188, 2026): intraday pressure persists, "not increased in recent years". The multi-day effect after 2008 is not verified. | ZT/ZF/ZN futures via IBKR ≈ 1–2 bp RT; not on xyz; Capital.com bond CFDs unverified | fiscaldata auctions (calendar fields only, end-dated); Treasury par yields (end-dated at load) | 2014-01→2026-06: 150 2y, 150 5y, 151 10y auction dates. **2y t 4.0 / 2.0; 5y 3.5 / 1.7; 10y 2.1 / 1.05** (yield SDs for 2y/5y assumed) | 5y, half effect: 1.35%/yr at 1× (SR ≈ 0.5); at 10% vol (≈3.7× notional) **≈ 5%/yr**; full ≈ 10%/yr | Rate shock in-window: 5y yield ±40 bp/week, ±80 bp/month (assumption) → −7% week, −13% month at 3.7× | Full Kelly ≈ 18× notional (Gaussian, too much); use ≈ 3–4× | Large | 1 in 3 / 1 in 5 |
| 3 | **FOMC-cycle even weeks** | Equity premium earned in weeks 0, 2, 4, 6 after an FOMC meeting (Fed's biweekly internal cycle and informal communication). | Holders of Fed-news risk in even weeks; mechanism attributed to the Fed's communication pattern, not to a payer. Persistence is uncertain once known. | Cieslak, Morse & Vissing-Jorgensen JF 2019, 1994→2013/16: +13.6 bp/day (week 0), +10 bp/day (weeks 2/4/6) vs odd weeks; robust in 3 subperiods and abroad. **Decay:** no post-2016 replication found; the week-0 core includes the pre-FOMC drift, which is dead after 2015 (Kurov et al., N7). | xyz SP500: ≈26 RT/yr × 2.2 bp ≈ 0.6%/yr + long funding ≈ 1.5%/yr (excess terms); MES ≈ 0.2%/yr | French daily; FOMC calendar pulled (99 scheduled meetings 2014→2026-06) | 2017-01→2026-06: 75 cycles, 2,363 days (1,259 even, 1,104 odd). **t 2.2–2.95 / 1.1–1.5** | Long even / flat odd at half effect (5 bp/day): ≈ 7%/yr gross at 12.9% vol (SR 0.55), ≈ 5% net on xyz; L/S at half: 6.7%/yr at 17.7% vol (SR 0.38) | Equity crash in an even week: as #1a at 1× | ½ Kelly ≈ 1–1.5× | Unlimited | 1 in 4 / 1 in 8 |
| 4 | **Earnings-announcement premium on xyz single names** | Announcing firms earn a premium in announcement weeks. | Risk-based (Savor–Wilson: announcers carry market-wide cash-flow news) or attention or limits to arbitrage. | Savor & Wilson (JF 2016; 2011 draft read), 1973→2009: weekly announcers earn 0.40–0.79%/week excess. Barber et al. JFE 2013: > 11%/yr, 46 countries (abstract via search). **Decay:** "Disappearing EAP" (Heitz et al. 2020) and Heater et al. (−0.30% in the 2010s): titles and summaries only, 403. | xyz single names: taker 0.9 bp + spread 1.4–1.7 bp p50 → ≈ 4–5 bp RT | Prices: Nasdaq API, **10 years only** (AAPL 2016-10-12→2026-06-30, 2,441 rows) and **current tickers only** (survivorship). Timing: EDGAR 8-K 2.02 needs a contact UA (403); Nasdaq's historical calendar has no BMO/AMC time | xyz: ≈ 17 names with 2 seasons, ≈ 40–60 events to 2026-06: powerless. Nasdaq 2016-10→: ≈ 1,900 events for 48 names (≈ t 2.3–3.8 / 1.2–1.9) but **selected on 2026 survival** | ≈ 3–6%/yr at 1× if a 30 bp/event premium survives (assumption) | Single-name earnings gaps of −20…−30% in a 5–10-name book: week −8…−10% | ≈ 1× | Small (xyz OI caps $25 M for most names) | 1 in 4 / 1 in 10 |
| 5 | **xyz equity-perp funding carry** (short perp, long stock) | Collect the baseline funding that longs pay. | xyz longs pay 0.5 × HL baseline ≈ 5.5%/yr at a zero premium: structural, by formula. | Mechanics only (N1, docs.trade.xyz). | Perp 0.9 bp + spread; stock at IBKR (≈1 bp); per position-year ≈ 5 bp | Funding history is available per coin hourly via HL `fundingHistory` (≈ 59 US coins; 17 with ≈ 5,000+ h by 2026-06). **Not fetched.** | n/a (the payoff is the funding series itself; reading it is a trial) | Zero premium: (5.475% + ≈0.4% dividend) / 1.25 capital ≈ **4.7%/yr total**, vs T-bill 3.60–4.37% (2025-07→2026-06) → **excess +0.3…+1.1%/yr**. Lending | Weekend internal pricing (±5% bounds, resets → ≈ ±15%) with the stock leg frozen; perp liquidation on a gap; ADL; deployer halt settles at mark; venue failure: −20% (the margin share) | n/a | $25 M–1 bn OI caps | 9 in 10 (cash flow paid) / 1 in 10 |
| 6 | **Overnight drift** (02:00–03:00 ET, S&P futures) | Dealers unwind end-of-day sell imbalances at the European open. | Dealers' inventory cost, paid by end-of-day sellers. | Boyarchenko, Larsen & Whelan RFS 2023, 1998→2020: 3.7%/yr (1.48 bp/day). **Dead:** the same authors (Liberty Street, 2026-07-01) find it ≈ 0 in 2021-01→2025-12, as closing-imbalance dispersion fell (SD 6.5% → 2.9%). NightShares overnight ETFs closed 2023 (search summary). | xyz SP500/XYZ100 taker 2.2 bp RT > 1.48 bp effect; maker 0.6 bp + adverse selection (established fact 4) | No free intraday index data. Local xyz fills could give 1-minute bars (not built) | XYZ100 ≈ 220 nights, SP500 88: t ≈ 1.5–2.2 full / 0.7–1.1 half (σ_hour 10–15 bp assumed) | **Negative after cost even at full effect** | — | — | — | 1 in 15 / ≈ 0 |

### 2.1 Reading

- **None of these is a hidden 100%-a-year edge.** The only path into the
  owner's 30–100% zone runs through #1 at near-full effect, levered 2–3×
  with a 30–40% drawdown budget. #1b's positive skew is the one convex
  payoff here.
- **#5 is lending, plainly:** at a zero premium it is ≈ 1%/yr over T-bills,
  with an unpriced venue tail. It beats HLP-calm's 2–4% in total-return
  terms only because T-bills now pay ~4%, and HLP is +19% in a spiral year.
- **#2 is the best-powered existence test but lending-plus at robust
  size.** It would beat HLP only at full effect and ≥ 10% vol.

### 2.2 Costs (published schedules; current-state reads)

- **xyz SP500/XYZ100** (N1, docs.trade.xyz, growth mode):
  - taker 0.9 bp and maker 0.3 bp; spread p50 0.4 bp → **RT ≈ 2.2 bp
    taker**.
  - Funding baseline 0.5 × 0.01%/8h ≈ **5.475%/yr paid by longs** at a
    zero premium.
  - Collateral (USDC) earns nothing.
  - The perp tracks the **price** index (v2: spot in the cash session,
    futures discounted to spot otherwise).
  - Excess-return shortfall of a long perp against futures ≈ f − (r − q)
    ≈ 5.5 − (3.8 − 1.2) ≈ **2.9%/yr ≈ 0.8 bp per calendar day**. Here r is
    the 13-week bill (3.83% on 2026-06-30, read) and q ≈ 1.2% (assumption).
    A short perp gains the same.
- **MES via IBKR:**
  - fixed $0.25/contract/side (IBKR micro-futures page, search summary;
    the page is 403 to fetch);
  - exchange + clearing + NFA ≈ $0.35/side (third-party summaries,
    unverified);
  - spread one tick (0.25 pt) on ≈ $30 k notional (assumption);
  - → **RT ≈ 0.8 bp**.
- **Capital.com index CFD** (fees page, read):
  - long overnight = SOFR + 4%/yr platform fee (÷360 per day); short =
    SOFR − 4%;
  - → long shortfall in excess terms ≈ 4% + q ≈ **1.4 bp per calendar
    day**, plus spread (US500 not legible on the page; assume ≈ 1 bp).
- **Treasury futures (IBKR):** ZF tick = $7.8125/$100k (0.8 bp) and ZN
  1/64 (1.6 bp), plus commission ≈ 0.15 bp → **RT ≈ 1–2 bp**. Tick sizes
  are standard CME specs from memory, not opened.

---

## 3. Top two: pre-registrable rule sketches (fixed from mechanism and literature, not from data)

### 3.1 #1 — Month-end institutional-flow family (two trials; family count 2 within B1)

**Shared calendar** (built, `derived_20261013T092459Z/month_end_calendar_US.csv`):
- T = the last US trading day of the month (dates of the French daily
  file).
- Settlement lag s is the lag in force on day T−s:
  - **3** before 2017-09-05 (SEC Rel. 34-80295, adopted 2017-03-22,
    compliance 2017-09-05);
  - **2** to 2024-05-27;
  - **1** from 2024-05-28 (Rel. 34-96930, adopted 2023-02-15, compliance
    2024-05-28).
- Months 2014-01 → 2026-05: **149** (44 / 80 / 25). 2026-06 is dropped
  because its T+3 falls in July.

**T1 — settlement-shifted liquidity cycle (discovery).**
- **Windows:**
  - pressure P_m = trading days T−8 … T−(s+1) (5/6/7 days for s =
    3/2/1);
  - rebound R_m = T−s … T+3 (7/6/5 days);
  - returns close-to-close.
  - In the T+3 era these are exactly Etula's windows.
- **Asset:** US total market (French Mkt + RF, which includes dividends).
  Executed on MES (primary) or the xyz SP500 perp.
- **Statistic:** y_m = Σ_{d∈R_m}(r_d − μ) − Σ_{d∈P_m}(r_d − μ). Here μ is
  the full-sample mean daily excess return over days outside every window,
  estimated jointly. The index is the market, so no β is needed. Report the
  long-only rebound (Σ_R) alongside.
- **Existence criterion:** mean y_m > 0 with t ≥ t_{148}(0.95, one-sided)
  = 1.655, month-clustered (G = 149, one observation per month).
- **Mechanism criterion** (reported; decisive only if T1 passes):
  - In the T+2 and T+1 eras (105 months), the shifted windows beat the
    unshifted T+3 windows (paired by month, t_{104}).
  - The settlement change moves the rebound later.
- **Pre-stated secondary:** Friday-T months (64) larger than non-Friday
  months (85), per Etula's payroll coincidence. Reported, not decisive.
- **Placebo:** the same window shapes anchored at T−12 (mid-month; no overlap
  with P_m or R_m) must fail.
- **Regions** (reported, not decisive): French Europe, Japan, Asia-Pacific
  ex Japan and North America daily (1990→, USD). The regional settlement
  dates need a per-region T:
  - EU T+2 from 2014-10-06 (Etula, read);
  - Japan T+2 from 2019-07-16 (JPX via search; JPX page 403);
  - Australia T+2 from 2016-03-07 (ASX media release, read);
  - Canada T+1 from 2024-05-27 (ASC release, read; the 2017 T+2 date is
    unverified).
  - The regional factor files mix country holidays: a caveat.
- **Costs, then the economic bar:**
  - net of 0.8 bp/RT (MES) or of 2.2 bp/RT plus funding (xyz);
  - bar: annualised net at the ½-Kelly size beats HLP calm (≥ 4%/yr) with
    the stated tail.

**T2 — rebalancing front-run (replication; label it so).**
- **Signals:** Threshold and Calendar exactly per HMM (B.1)–(B.2):
  - a simulated 60/40 rebalanced at bands δ ∈ {0, 0.1, …, 2.5%} (averaged)
    and at the last business day of each month;
  - strategy weight = mean of (−Threshold/1.5%) and the Calendar term. The
    Calendar term is sign(−Calendar) in the last week of the month, and
    sign(Calendar_{t−4}) on the first business day.
  - Position (S&P − 10y note) × weight.
- **Proxies, fixed now:**
  - S&P 500 → French Mkt total return;
  - 10y note futures → the 10y par-yield return approximation
    y_{t−1}/252 − D_t·Δy_t, with D_t the modified duration of a 10y par
    bond at y_{t−1}.
  - Proxy error is a stated limitation. Futures-exact data is not free.
- **Calendar:**
  - primary 2014-01-02 → 2026-06-30 (inside the authors' sample to
    2023-03 and their 2025 extension);
  - a pre-stated "post-publication" subwindow 2025-04-01 → 2026-06-30,
    reported, not decisive (t ≈ 1.15 even at SR 1).
- **Existence criterion:** mean strategy excess > 0, month-clustered
  t ≥ t_{149}(0.95).
- **Leg split:** the equity leg is reported alone, because it is what xyz
  can carry.
- **Family:** T1 and T2 share month-end days, so they are one family. A
  Holm adjustment across the 2 trials, plus the carry-over family count
  from both roots.

**Behaviour-only next step** (no returns):
1. Write the regional T calendars per settlement regime.
2. Confirm IBKR access to MES and ZN for the owner; read the CME fee
   finder for MES and ZN.
3. Verify the Capital.com US500 spread and any bond CFDs.
4. Have two reviewers check the window and duration definitions **before
   anything is computed**.
5. Ledger the calendars this registration reads as `000p`:
   - French US and regional 2014-01 → 2026-06;
   - Treasury 10y par yields;
   - prior reads: Etula's international DiD read 2013-10 → 2015-10, and
     HMM's sample read 1997 → 2025.

### 3.2 #2 — Treasury auction cycle (one trial)

- **Events:** every fixed-rate 2y, 5y and 10y note auction date, new or
  reopening, from 2014-01 to 2026-06 (150 / 150 / 151; calendar built;
  FRN and TIPS excluded by flag).
- **Asset per tenor:** the front ZT/ZF/ZN future; the on-the-run note is
  not free. The free proxy for the test is the par-yield duration return
  at that tenor.
- **Rule** (Lou–Yan–Zhang's design):
  - short the tenor from the close of auction day −5 to the close of day
    −1;
  - long from the close of the auction day to day +5;
  - the statistic is (post-5d return − pre-5d return) per auction.
- **Primary:** 5y (the paper's strongest t), pooled with 2y and 10y as
  secondary.
- **Clustering:** by calendar month (2y, 5y and 7y auctions share a week),
  G ≈ 150; criterion t ≥ t_{G−1}(0.95).
- **Placebo:** the same ±5-day windows anchored 10 trading days after each
  auction.
- **Costs:** 2 RT per auction at 1–2 bp.
- **Behaviour-only next step:**
  - pull the 2y and 5y par-yield columns (already on disk) and record
    their unconditional daily SDs, to replace the assumed 4.5 / 5.5 bp;
  - confirm the owner's futures access (IBKR) or Capital.com bond CFDs.
  - Without futures access this light is not deployable.

---

## 4. Rejected ideas (and the fact that kills each)

| idea | why |
|---|---|
| **Overnight drift (index, European open)** | Dead by its authors' own 2021–2025 update (Liberty Street, 2026-07-01). The cost (2.2 bp RT) exceeds the full effect (1.48 bp/night). |
| **Index overnight-only exposure** (close-to-open; LPS 2019: "for the largest stocks essentially all of their risk premium is earned overnight", 1993–2013) | The premium is ≈ 2.8 bp/night (≈ 7%/yr ÷ 252, assumption). xyz costs 2.2 bp RT + 1.5 bp/day of funding. NightShares' overnight ETFs closed in 2023 after underperforming (search summary). |
| **Cross-sectional overnight tug of war on xyz names** (overnight-winner decile +3.47%/month overnight, −3.02% intraday; 1993–2013, all CRSP) | Parked, not killed. Only ≈ 17 names have > 6 months, and deciles need hundreds. Post-2013 decay unverified. Costs ≈ 7 bp per night for a pair against ≈ 16 bp/night for whole-CRSP deciles. |
| **PEAD** | Martineau (CFR 2022, 1984→2020): none for large caps since 2006, and gone for microcaps since 2016. xyz names are large caps. |
| **Pre-holiday effect** | Robins & Smith (JWM; ASU summary read): disappeared after 1978 for the market and large caps, 1926–2017. |
| **Option-expiration week** (Stivers & Sun JBF 2013; 1996–2008 working-paper sample) | No post-sample evidence found. The delta-hedging mechanism has been reshaped by daily (0DTE) expiries. Unverifiable cheaply; low prior. |
| **Aggregate short interest → market** (Rapach, Ringgenberg & Zhou JFE 2016) | Monthly, so ≈ 150 obs. An updated replication (2024 Erasmus thesis, search summary) loses significance excluding the GFC. FINRA's free daily short-volume CDN has 2025 files, but a 2014 file returned 403, so coverage is partial. |
| **Pre-FOMC drift, index inclusion, FX fix, intraday momentum, LETF pair, merger arb** | N7 §A7; no new evidence found. |
| **xyz weekend fade** | N1: index power is too low (MDE ≥ 50 bp per event). |
| **Short-vol on VIX** | N7-5. |

---

## 5. Exposure list

**Current-state reads** (no prices):
- SEC T+2 compliance guide;
- SEC press release 2023-29;
- ASC (Canada) T+1 release;
- ASX T+2 media release;
- Capital.com CFD fee page;
- federalreserve.gov `fomccalendars.htm`. It lists meeting dates after
  2026-07-01, which were dropped at load and never printed.
- FINRA CDN header probe (2025-01-02 file exists; 2014 returned 403);
- Nasdaq API (service current; the query was end-dated).

**Post-cutoff rows on disk:**
- Ken French zips: 43 (US) and 44 (regional) rows dated ≥ 2026-07-01,
  dropped at load and never printed.
- Treasury 2026 yield and bill CSVs: 142 rows ≥ cutoff, dropped at load.
- No other pull contains post-cutoff rows: fiscaldata auctions and Nasdaq
  AAPL were end-dated, with 0 rows ≥ cutoff.

**Outcome-adjacent literature** (read; it constrains the "unread"
claims):
- **Liberty Street, 2026-07-01** (post-cutoff publication): the overnight
  drift ≈ 0 over 2021–2025. **The 2021–2025 overnight calendar is
  therefore no longer clean for this programme.**
- **NY Fed SR 1188** (revised July 2026): intraday auction pressure "not
  increased in recent years". This is a qualitative outcome over part of
  the 2014–2026 auction calendar.
- **HMM Jan-2026 draft:** the sample is 1997→2023-03, with an extension to
  2025 "even stronger" (footnote 32). **T2's calendar is in-sample for the
  authors.**
- **Etula:** international DiD on 2013-10 → 2015-10 (overlaps the T1
  regional calendar).
- **Search summaries:**
  - NightShares 2022–2023 performance (−6.9% vs +22% S&P);
  - Heater et al. (announcement returns −0.30% in the 2010s);
  - a Rapach replication thesis;
  - the April-2025 two-day −10.5% S&P fall;
  - the February-2020 −11.5% week (AP).

**Rate-level read:** the 13-week bill range for 2025-07 → 2026-06
(3.60–4.37%, last 3.83%), used for the carry opportunity cost.

**Unconditional volatilities computed** (labelled, for power only), daily,
2014-01 → 2026-06:
- US market total return: 1.120%;
- Europe 1.030%, Japan 1.170%, Asia-Pacific ex Japan 0.872%, North
  America 1.086%, Developed 0.893%, Developed ex US 0.851%;
- 10y par-yield daily change: 5.21 bp.

**Nasdaq earnings-calendar probe** (2024-01-25, 117 rows): the file holds
`eps`, `epsForecast` and `surprise`. Only the field names and the `time`
label counts were read.

**Not opened:** every `research/copytrade/data/` file except N1's
`activity_by_coin.csv` (coin names, class and first fill day, all
pre-cutoff). xyz fills and snapshots were not opened.

**Failed or blocked** (recorded; nothing circumvented):
- FRED `fredgraph.csv` timed out repeatedly; replaced by Treasury CSVs.
- SEC EDGAR returned 403 to a UA without contact email. I did not send the
  owner's email; **the owner can supply a contact UA.**
- 403 from IBKR (two pages), JPX, SSRN, IIMB, etf.com and alphaarchitect.

---

## 6. Files created; constraints honoured

**Scripts:**
- `research/alpha/scouts/b1_pull.py`: the pulls.
- `research/alpha/scouts/b1_read.py`: behaviour-only reader. It writes
  calendars, coverage counts and unconditional SDs.

**Data** (each pull dir has `SHA256SUMS` and `PULL_LOG.json`; total
4.8 MB), all under `research/alpha/data/scout_b1/`:
- `french_20261013T090723Z/`: 7 Ken French daily zips.
- `auctions_20261013T091053Z/`: superseded.
- `auctions_20261013T091941Z/`: with FRN/TIPS flags; 4,552 records,
  2014-01 → 2026-06.
- `fomc_20261013T091103Z/`: the calendar page plus historical 2014–2020.
- `treasury_20261013T091152Z/`: par yields and bill rates 2014–2026.
- `nasdaq_20261013T091235Z/`: the AAPL history probe and the earnings
  calendar probe.
- `edgar_20261013T091228Z/` and `fred_20261013T091239Z/`: failure logs
  only.
- `derived_20261013T092459Z/`:
  - `month_end_calendar_US.csv`;
  - `fomc_announcement_days.csv`;
  - `fomc_cycle_week_by_day.csv`;
  - `auctions_2y5y10y.csv`;
  - plus `SHA256SUMS`.

**Constraints honoured:**
- Downloaded files were read with `python -I`, paths as arguments. Scripts
  sit outside the data dirs.
- Papers were text-extracted in the session scratchpad (a pypdf venv
  there), not in the repo.
- Nothing paid, keyed or requester-pays; no credentials; no access control
  circumvented; well under 1 GB.
- Nothing published or uploaded; no commits.
- Nothing under `research/copytrade/` was created or modified;
  `lib/`, `pages/`, `components/` and config were not touched; no
  `results.jsonl`; no Neon.

**Sources opened:**
- SEC: https://www.sec.gov/tm/t2-sbrefa ; https://www.sec.gov/newsroom/press-releases/2023-29
- Etula et al.: https://acris.aalto.fi/ws/portalfiles/portal/40965155/hhz054.pdf
- Harvey, Mazzoleni & Melone: https://www.nber.org/papers/w33554 ; https://www.edhec.edu/sites/default/files/2026-03/Scientific%20paper.%20ssrn-5122748.pdf
- Lou, Yan & Zhang: https://www.fmg.ac.uk/sites/default/files/publications/DP684.pdf
- Cieslak, Morse & Vissing-Jorgensen (2015 draft): https://stern.nyu.edu/sites/default/files/assets/documents/cycle_paper_cieslak_morse_vissingjorgensen.pdf
- Boyarchenko, Larsen & Whelan: https://www.newyorkfed.org/medialibrary/media/research/staff_reports/sr917.pdf ; https://libertystreeteconomics.newyorkfed.org/2026/07/the-disappearing-overnight-drift/
- Lou, Polk & Skouras: https://personal.lse.ac.uk/polk/research/TugOfWar.pdf
- Martineau: https://cfr.ivo-welch.info/published/papers/martineau2021rest.pdf
- Savor & Wilson (2011 draft): https://faculty.wharton.upenn.edu/wp-content/uploads/2012/04/Draft20111215p_edited.pdf
- NY Fed SR 1188: https://www.newyorkfed.org/research/staff_reports/sr1188
- ASX: https://asx.com.au/content/dam/asx/about/media-releases/2016/ASXT2GoLive7March2016.pdf
- ASC: https://www.asc.ca/en/news-and-publications/news-releases/2024/05/may-27-canadian-securities-regulators-announce-move-to-t1-settlement-cycle
- Capital.com: https://capital.com/en-eu/ways-to-trade/fees-and-charges/cfd-and-ko
- Robins & Smith (ASU summary): https://knowledge.wpcarey.asu.edu/20190311-dont-bet-holiday-effect-boost-market-returns
- N1 and N7 scouts (local).

**Search summaries only (not opened):** Barber et al. 2013, Heitz et al.,
Heater et al., Stivers & Sun, Rapach et al., NightShares, JPX T+2, the IBKR
MES commission, and the stress-episode figures.
