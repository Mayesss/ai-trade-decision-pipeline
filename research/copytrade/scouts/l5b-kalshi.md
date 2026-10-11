# L5b scout: favourite–longshot bias on Kalshi (feasibility only)

2026-10-11. This scout covers access, costs, data and capacity. **No trading
outcome was computed.** No resolution P&L, hit rate or bucket return was
derived from data. Every return figure below is quoted from a published
source and cited. Earlier work is in `scouts/l5-desk.md` (the L5b sections);
this file builds on it and does not repeat it.

Scripts:
- `research/copytrade/scouts/l5b_costs.py`: fee, hurdle, diversification and
  turnover arithmetic. It reads no data.
- `research/copytrade/scouts/l5b_api_probe.py`: one keyless access probe. It
  prints the HTTP status and field names only.

Downloads, gitignored, are in `research/copytrade/data/scout_l5b/`. The only
one is `paper/kalshi_whelan.pdf` (0.97 MB).

---

## Summary (5 lines)
1. **The paper** (Bürgi, Deng & Whelan, Jan 2026; Kalshi 2021 → Apr 2025;
   46,282 contracts, 12,403 events). Buying the 91–99¢ favourite earns about
   **+0.5% per trade for takers, after the taker fee, and +1.8% for makers,
   who paid no fee then.** Makers buying at 50¢ or more earn +2.6%. Contracts
   at 10¢ or less lose 73% for takers and 35% for makers.
2. **Decay.** The bias shows up in every year, but its slope in 2025 (Jan–Apr
   only) is about half its 2024 value (0.021 vs 0.048), and the paper was
   published in Dec 2025 / Jan 2026. It is weakest, and not significant, in
   politics and entertainment, and strongest in crypto.
3. **Access.** Kalshi onboards non-US users subject to its Member Agreement.
   Two third-party readings of that agreement (Jun and Oct 2026) do **not**
   list Germany. KYC needs a government ID plus country of residence. Funding
   is by Visa/Mastercard debit card, wire (minimum $1,000) or crypto. Kalshi's
   interest (APY) on collateral is **US-only**. Polymarket is close-only for
   Germany, by the venue's own geoblock.
4. **Costs.** The taker fee is 0.07·P(1−P) per contract: 0.35% of stake at
   95¢, but 6.65% of the maximum profit. Tail spreads were 3–4¢ in a
   $7.9M-volume market. The opportunity cost of 4% over a 3-month lock-up is
   **1.0% of stake per trade**, which is above the published taker edge (0.5%)
   and roughly equal to a maker edge halved for decay (0.9%).
5. **Data and capacity.** The archive (trades, candlesticks, settled markets)
   is **public and keyless**: verified with one date-bounded request. It covers
   2021 → the cutoff. The median contract traded only $9k in total volume, and
   the median trade is $35, so a $10k book fits easily. A $100k book needs
   hundreds of concurrent positions, and correlated clusters cut the effective
   number of independent bets.

---

## Q1. The paper's numbers

**Source.** Bürgi, Deng & Whelan, *Makers and Takers: The Economics of the
Kalshi Prediction Market*, January 2026
([karlwhelan.com PDF](https://www.karlwhelan.com/Papers/Kalshi.pdf),
[UCD WP2025_19](https://www.ucd.ie/economics/t4media/WP2025_19.pdf),
[CEPR DP20631](https://cepr.org/publications/dp20631),
[VoxEU column](https://cepr.org/voxeu/columns/economics-kalshi-prediction-market)).
I read the full 44-page PDF.

**Sample (§3.1).**
- Coverage runs from Kalshi's launch in 2021 to **April 2025**. The authors
  stopped there because maker fees started after April 2025.
- Filters:
  - At least $1,000 of total volume per contract.
  - Final bid–ask spread of 20¢ or less.
  - Hourly markets (crypto and index) excluded; only markets open 24 h or
    more are kept.
- Size: 46,282 Yes contracts in 12,403 events (3.7 contracts per event).
- Prices are the last trade on the closing day and on each of the 10 days
  before it. That gives 156,986 Yes prices, or 313,972 Yes+No prices.
  - Most observations sit close to close: 19,338 contracts have a closing-day
    price only (Table 1).
- Two-thirds of prices are below 10¢ or above 90¢ (Table 2).
- Sports trading started in Jan 2025. The results are "not sensitive to"
  ending the sample in Dec 2024 or dropping the sports category.

**Fee treatment.**
- In the sample period only takers paid a fee, $0.07·P(1−P) per contract,
  with the total rounded up to the cent. Makers paid nothing.
- Taker returns are **net of that fee**: the fee is imputed on a
  100-contract order (§3.3, eq. 2).
- Maker returns have no fee to deduct.
- The regressions (Tables 4–9) are **pre-fee**.
- Returns are computed at transaction prices, so the spread is already inside
  them.

**Returns by 10¢ bucket, after fees.** Values are read from the data markers
of Fig. 9, which replots Fig. 6, so allow about ±1 pp.

| bucket | 1–10 | 11–20 | 21–30 | 31–40 | 41–50 | 51–60 | 61–70 | 71–80 | 81–90 | **91–99** |
|---|---|---|---|---|---|---|---|---|---|---|
| Takers (net of taker fee) | −73% | −33% | −23% | −10.5% | −7.5% | −3.5% | −2% | −0.5% | +1% | **+0.5%** |
| Makers (no fee) | −35% | −12% | −4% | −1% | −1% | +3.5% | +3.5% | +6% | +5% | **+1.8%** |

Numbers stated in the text:
- Takers average **−31.46%** and makers **−9.64%** (§4).
- Contracts at 10¢ or less lose "over 60%" for all buyers (§3.3).
- Makers buying at 50¢ or more earn **+2.6%**, with a per-trade SD of
  **33%** (§4, §6).
- The average pre-fee return per contract is −20%.
- Above 70¢, post-fee returns are positive, statistically significant, but
  small.

**By horizon (Figs. 7–8).**
- The taker pattern holds on every day from 10 days out to the close.
- For makers, most day-by-day returns away from the close are **not
  significant**. The 91–99¢ maker returns sit at about 0 to +3%.
- The largest maker gains are on the closing day. The authors link this to a
  "Yogi Berra" late effect.

**By category (Table 8).** These are pre-fee Mincer–Zarnowitz slopes ψ; a
larger ψ means a stronger bias.

| category | ψ | significant? | n |
|---|---|---|---|
| Crypto | 0.058 | yes | 8,150 |
| Other | 0.053 | yes | 15,192 |
| Economics | 0.034 | yes | 24,405 |
| Financials | 0.032 | yes | 27,123 |
| Climate & weather | 0.031 | yes | 29,924 |
| Politics | 0.022 | **no** | 26,819 |
| Entertainment | 0.020 | **no** | 25,541 |

- Sports is not reported separately. On Polymarket the bias is absent in
  sports (Cardozo & Rivero-Wildemauwe 2026, in `l5-desk.md`).
- By volume quintile (Table 6), the bias is present in every quintile and
  largest in the lowest (0.045 vs 0.032 in the top quintile).
- By trade size (Table 7), it is largest in the top quintile.

**Over time (Table 9).**

| year | 2021 | 2022 | 2023 | 2024 | 2025 (Jan–Apr) |
|---|---|---|---|---|---|
| ψ | 0.041 | 0.023 | 0.036 | 0.048 | **0.021** (p < 0.1) |

- The decline is not monotonic. The one weak year is four months long.
- The authors say only "some evidence" of weakening. They expect a possible
  post-publication fade (§6).
- **No post-April-2025 measurement exists in this paper.**

**Cross-venue check.** On Polymarket, purchases at 90¢ or more earn +0.83¢
per $ (flow-weighted, Cardozo et al.). This is the same order of magnitude as
the Kalshi taker figure.

---

## Q2. Practical access for a non-US person resident in Germany

**Kalshi's own pages.**
- Help article *Can I trade on Kalshi from outside the United States?*
  (dated 20 Mar 2026) ([help.kalshi.com](https://help.kalshi.com/en/articles/14026044-can-i-trade-on-kalshi-from-outside-the-united-states)):
  - "Yes, you can trade on Kalshi from many countries", subject to the Member
    Agreement, which holds the country list.
  - KYC: "verify your identity and confirm your country of residence".
  - No US phone number is needed.
  - Deposits: **debit card (Visa/Mastercard), wire (minimum $1,000), crypto.**
    Withdrawals: debit card or crypto. ACH, PayPal and Venmo are US-only.
- Interest on cash and collateral (APY) is "only available for eligible users
  in the United States" ([help.kalshi.com APY](https://help.kalshi.com/navigating-the-exchange/your-portfolio/apy-on-kalshi)).
  For us, collateral earns 0%.
- The Member Agreement PDF itself
  ([kalshi.com/docs/kalshi-member-agreement.pdf](https://kalshi.com/docs/kalshi-member-agreement.pdf))
  returned HTTP 429 again. The country list therefore rests on two
  independent third-party readings:
  - **CoinPerps, 12 Jun 2026** ([link](https://www.coinperps.com/learn/kalshi-restricted-countries)).
    Member Agreement v1.6 lists 54 restricted jurisdictions, including
    Belgium, Bulgaria, France, Hungary, Ireland, Italy, Monaco, Poland,
    Portugal, Switzerland and the UK. **Germany is not listed.**
  - **Kagels-Trading, checked 6 Oct 2026** ([link](https://www.kagels-trading.de/kalshi/)).
    The list now has 55 entries: "Deutschland und Österreich stehen nicht
    darauf". The site found no geoblock, but the official app is absent from
    the German App Store and Google Play. It did not test sign-up.
  - Third-party reports say Spain ordered ISP blocking in May 2026. Lists
    change unilaterally.
- **Practical status: probably open, unverified.** Whether Kalshi's KYC
  accepts a German residence document can only be learned by attempting it.
  The scout rules forbid creating an account.

**Other venues with the same structure.**
- **Polymarket (international):** Germany is close-only by the venue's own
  geoblock, on both the frontend and the API
  ([docs](https://docs.polymarket.com/developers/CLOB/geoblock)). Research
  data only.
- **Polymarket US:** a US-only app.
- **Betfair Exchange:** withdrew from Germany
  ([InterGame](https://www.intergameonline.com/igaming/news/betfair-pulls-out-of-germany-in-tax-row)).
- **Smarkets:** no German-specific statement found; not verified.
- No other CFTC-style event-contract venue that onboards German residents was
  found.

**Legal warning (one line).** The GGL treats prediction-market bets as
unlicensable and participation from Germany as prohibited (5 Sep 2025; a
reported notice of 13 Aug 2026), and contracts on financial underlyings fall
under the BaFin binary-option retail ban. Per the owner's ruling this is a
warning, not a blocker (detail in `l5-desk.md`).

---

## Q3. Costs

**Fee formula.**
- Taker: ceil(0.07 × C × P × (1−P)) dollars per order, where C is the number
  of contracts. The paper confirms this for the sample period.
- Maker: since mid-2025 makers pay a fee of 0.0175 × C × P × (1−P) (25% of
  the taker rate), on roughly a hundred named series only. These include
  NFL/NBA/NHL games, golf and tennis majors, and some economics series. Other
  series have no maker fee.
- S&P 500 and Nasdaq-100 markets reportedly use a halved multiplier (0.035).
- Sources: [Oddpool](https://www.oddpool.com/research/prediction-market-fees-explained),
  [InGame](https://www.ingame.com/kalshis-change-may-increase-fee-sports-traders/),
  [DeFi Rate](https://defirate.com/prediction-markets/fees/). Kalshi's own fee
  schedule PDF ([kalshi.com/docs/kalshi-fee-schedule.pdf](https://kalshi.com/docs/kalshi-fee-schedule.pdf))
  returned 429.
- Kalshi reportedly keeps up to 2% on card deposits (Kagels).

**Effective fee** (from `l5b_costs.py`; large orders, so rounding is
negligible):

| price | taker ¢/contract | taker % of stake | maker ¢ | maker % of stake | taker % of max profit |
|---|---|---|---|---|---|
| 2¢ | 0.137 | 6.86% | 0.034 | 1.72% | 0.14% |
| 5¢ | 0.333 | 6.65% | 0.083 | 1.66% | 0.35% |
| 10¢ | 0.630 | 6.30% | 0.158 | 1.57% | 0.70% |
| 90¢ | 0.630 | 0.70% | 0.158 | 0.18% | 6.30% |
| 95¢ | 0.333 | 0.35% | 0.083 | 0.09% | 6.65% |

- **Rounding trap:** a one-contract taker order at 95¢ pays a full cent, which
  is 20% of the 5¢ maximum profit. Orders must be 100 contracts or more.

**Spreads at the tails.**
- In the paper's live snapshot of the April-2025 CPI ladder ($7.9M volume;
  Fig. 1), Yes+No asks summed to 103–104¢. That is a **3–4¢ round-trip
  spread** on strikes priced at 95–98¢ and 6–11¢.
- Depth at the touch was $17–$34, with about $3k one tick away (Fig. 2).
- A 3¢ spread on a contract whose maximum profit is 5¢ means a taker pays
  away most of the edge. That is why the published taker edge at 91–99¢ is
  only +0.5%.

**Capital lock-up and the annualised hurdle.**
- The full stake (90–98¢) is collateral until settlement.
- Kalshi pays non-US users no interest, so the foregone risk-free rate (4%)
  is a direct cost.

| holding period | hurdle, % of stake | on a 95¢ favourite |
|---|---|---|
| 1 day | 0.011% | 0.01¢ |
| 10 days (the paper's horizon) | 0.11% | 0.10¢ |
| 30 days | 0.33% | 0.31¢ |
| **3 months** | **1.0%** | **0.95¢** (19% of the 5¢ maximum premium) |
| 6 months | 2.0% | 1.9¢ |

Published per-trade returns, annualised under a 3-month lock-up:

| leg | per trade | annualised | excess over 4% |
|---|---|---|---|
| Taker, 91–99¢ | 0.5% | 2.0%/yr | **−2.0%/yr** |
| Maker, 91–99¢ | 1.8% | 7.4%/yr | **+3.4%/yr** |
| Maker, halved for decay | 0.9% | 3.6%/yr | **−0.4%/yr** |
| Maker, ≥50¢ (+2.6%) | 2.6% | 10.8%/yr | +6.8%/yr |

The +2.6% figure for makers at 50¢ or more is dominated by closing-day
trades, so it is not representative.

**Conclusion.** With long-dated contracts (months), the trade fails for
takers and is marginal for makers. It can only work on short-dated contracts
(10 days or less, as in the paper), where the hurdle is negligible and
capacity binds instead.

---

## Q4. The trade's shape

- **Payoff.** Buying the favourite at P earns (1−P)/P per win and loses 100%
  of the stake on a loss.
  - At 95¢: +5.26% per win and −100% per loss, so one loss erases about 19
    wins.
  - The paper's per-trade SD is 33% for makers at 50¢ or more. At 95¢ it is
    about 19–22% (`l5b_costs.py`).
- **Independent bets needed** for the mean edge to sit 2 SE from zero (about
  the number to diversify over within one year):

  | edge per trade | trades needed |
  |---|---|
  | +1.8% (maker, published) | ~435 |
  | +0.9% (maker, halved for decay) | ~2,200 |
  | +0.5% (taker) | ~7,700 |

  The paper's filtered sample holds about 2,900 events a year (12,403 over
  ~4.3 years). Only the maker case can be diversified within a year, and only
  if events are truly independent.
- **Correlation inside events and categories.**
  - Mutually exclusive ladders (who wins X) help the "No on every longshot"
    book: at most one longshot wins, so at most one leg loses.
  - Threshold ladders (CPI above 0.0/0.1/…/0.4%) hurt: one surprise print
    kills every adjacent favourite at once.
  - The paper clusters standard errors by event for exactly this reason.
  - Category clusters: same-day weather across cities (shared weather
    systems), macro release days (CPI, FOMC), one sports slate, and crypto
    price-range markets that all load on BTC.
  - **The crypto category has the strongest bias (ψ = 0.058), and it also
    loads on the same crash tail as our HLP and perp books.** A wide-move
    week would hit the "sell crypto longshots" leg alongside them.
- **Settlement risk.** Kalshi resolves markets under its own rulebook as a
  CFTC DCM, not through an optimistic oracle. That is less fragile than
  Polymarket's UMA, but disputed settlements are a residual tail on the short
  side.

---

## Q5. Free historical data

**Kalshi public API** (base `https://api.elections.kalshi.com/trade-api/v2`).

| endpoint | what it serves | date bound | auth |
|---|---|---|---|
| `GET /historical/markets` ([docs](https://docs.kalshi.com/api-reference/historical/get-historical-markets)) | settled markets older than the cutoff, incl. `result`, `settlement_value_dollars`, `last_price_dollars`, `volume_fp`, `open_interest_fp`, `close_time`; filters `tickers`, `event_ticker`, `series_ticker`; 1,000 per page | **none** → not called | `security: []` |
| `GET /historical/trades` ([docs](https://docs.kalshi.com/api-reference/historical/get-historical-trades)) | every trade older than the cutoff: `yes_price_dollars`, `count_fp`, `taker_outcome_side`/`taker_book_side`, `created_time`; 1,000 per page | `min_ts` / `max_ts` | `security: []` |
| `GET /historical/markets/{ticker}/candlesticks` ([docs](https://docs.kalshi.com/api-reference/historical/get-historical-market-candlesticks)) | OHLC of yes_bid / yes_ask / price, plus volume and open interest | `start_ts` / `end_ts`; period 1, 60 or 1440 min | `security: []` |
| `GET /markets` ([docs](https://docs.kalshi.com/api-reference/market/get-markets)) | live and recent markets; markets settled before the cutoff are only in `/historical/markets` | `min_/max_close_ts`, `min_/max_settled_ts` | `security: []` |

**Archive mechanics.**
- The archive window is a moving cutoff exposed by `GET /historical/cutoff`
  ([docs](https://docs.kalshi.com/getting_started/historical_data)). That
  endpoint has no date bound, so it was not called.
- The archive starts at Kalshi's 2021 launch: the paper pulled 2021 →
  Apr 2025 through this API.

**Key needed? No.**
- One probe (`l5b_api_probe.py`): `/historical/trades` bounded to
  2025-04-01 → 2025-04-02 UTC, `limit=1`, no key. It returned **HTTP 200**.
  Only field names were printed.
- Settled prices and resolutions sit in `/historical/markets`, also
  `security: []`. It was not called because it has no date filter.

**How to keep a registration's holdout sealed with these endpoints.**
1. Pull trades bounded by `max_ts`.
2. Fetch resolutions with `tickers=` taken only from those trades, so the
   resolution pull is date-bounded by construction.
3. Never page `/historical/markets` unfiltered.

**Rate limits** ([docs](https://docs.kalshi.com/getting_started/rate_limits)).
- Basic tier: 200 read tokens/s at 10 tokens per request, about **20
  requests/s**.
- The docs do not say what limit applies to unauthenticated calls.

**Full-history size (estimates).**

| item | estimate |
|---|---|
| Trade count, to end-2025 | ~65 M, per a third-party summary of a 2026 paper (unverified) |
| Trade pulls | ~65k requests at 1,000 per page, about 1 h at 20 req/s if keyless calls get the same limit |
| Trade bytes | ~250 B of JSON per trade → **~15–20 GB raw JSON**, a few GB as parquet |
| Markets | ~211k binary markets → ~211 paged requests |
| Daily candles | one request per market → ~211k requests |
| Ready-made alternative | Jon Becker's MIT-licensed Kalshi + Polymarket dump, **36 GiB compressed** ([repo](https://github.com/Jon-Becker/prediction-market-analysis)) |

- Kalshi volume has grown sharply since 2025: about $40B of notional in Aug
  2026, mostly sports (secondary,
  [KuCoin/Dune](https://www.kucoin.com/news/flash/kalshi-s-us-web-traffic-surges-1-520-as-trading-volume-hits-40b)).
  2026 trade counts will dwarf the paper's sample.
- **All of this is above the 100 MB scout limit. A full pull needs owner
  approval.** A useful discovery subset would be smaller: the paper's
  filtered universe of daily-close prices plus resolutions, roughly 50k
  markets × 11 candles.

**Polymarket as research data** (keyless; from `l5-desk.md` plus Gamma):
- **Gamma** (`gamma-api.polymarket.com/markets`): metadata, `closed`,
  outcome prices and resolution status.
- **CLOB** `GET /prices-history` per token, with `startTs` / `endTs` /
  `fidelity` ([docs](https://docs.polymarket.com/developers/CLOB/timeseries)).
- **On-chain CTF-exchange fills** via subgraph, from about 2022.

---

## Q6. Capacity

**Published sizes (2021 → Apr 2025, paper Table 3 and §3.1).**
- Total two-sided volume per contract:
  - **Median $8,982.** Deciles run from $1.2k to $36k.
  - The top decile averages **$526k**.
- Trade size: **mean $100, median $35.**
- Order-book depth at the touch is tens to a few thousand dollars, even in a
  $7.9M market (Fig. 2).
- The authors cite small volumes as the first reason the bias survives (§6).

**What a book could deploy.** Assumptions: our share is 10% or less of a
contract's lifetime volume on the favourite side, and we fill as a maker.

| book | positions needed | verdict |
|---|---|---|
| **$10k** | 20–50 concurrent positions of $200–$500 | feasible across typical markets |
| **$100k** | ~200+ concurrent $500 positions in median markets, or concentration in top-decile markets | top-decile markets are mostly 2025+ sports and macro. They have the deepest books and the most professional makers (Susquehanna is a Kalshi market maker), the most maker fees, and, on Polymarket, no bias in sports. |

- Short holds (10 days or less) give many turns a year, so dollar turnover,
  not capital, is the binding constraint.
- **Turnover arithmetic (`l5b_costs.py`).** Annual return on the book equals
  edge × (365 / holding days) × utilisation.
  - With a +0.9% edge, 10-day holds and 50% utilisation, that is 16%/yr.
  - It is only real if the fills happen at the published prices. Utilisation
    above about 20% has no published support.

---

## Q7. Consistency

- **Why it could persist:** retail takers overweight small probabilities
  (the paper calibrates β ≈ 0.06–0.12, tightly identified). Arbitrage capital
  is capped at the tails by thin books ($35 median trades), full-stake
  collateral that earns nothing for non-US users, and a heavy left tail that
  "proper risk aversion" declines (§6).
- **Strongest reason it is not capturable for us:** the edge on the
  favourite side is small. It is +1.8% for incumbent makers in 2021–Apr 2025,
  measured partly on last-trade and closing-day prices that a forward
  strategy cannot assume it gets. It may have halved in 2025, before the
  paper's own publication. Against it stand a 3–4¢ tail spread, maker fees on
  the deepest series, zero interest on collateral and correlated
  event-cluster tails. Whatever survives is capped at tens of thousands of
  dollars of turnover per week.

---

## Verdict

**Feasible to register as research. Needs owner decision Y before any live
leg.**

- **Research.** The data is free and keyless, back to 2021, with
  date-bounded trade and candle endpoints. A registration can test the
  post-publication period (2026-01 → 2026-06) as a holdout against a
  2021–2025 discovery. Every bucket return above is published, not ours. A
  full pull exceeds 100 MB and needs owner approval of the bytes (Q5).
- **Decision Y.** The owner must accept the German legal warning and attempt
  Kalshi KYC as a German resident. Germany is absent from the country list
  per third parties, but it is unverified whether onboarding succeeds.
  Deposits must be by card, wire (minimum $1k) or crypto.

**Expected net return per year** (from published numbers and the cost
arithmetic only; no data computed):

| scenario | assumptions | total return | vs 4% rf |
|---|---|---|---|
| Taker-only, any horizon | published +0.5%; fails the 3-month hurdle | 0–2% | −4% to −2% |
| Maker, decayed | +0.9% edge, short holds, 20–50% utilisation, halved again for execution (queue position, adverse selection, no closing-day prints) | **~3–8%** | **−1% to +4%** |
| Upside | maker at the published +1.8%, unhaircut | >15% | — |

- The upside case is not supported by capacity evidence.
- **Against HLP** (19% in a spiral year, 2–4% calm; the lending leg about
  3%):
  - The central case is comparable to HLP-calm and to lending, with a
    different risk shape: a short-tail book of many small, mostly
    independent bets.
  - Its crypto-category leg shares HLP's crash tail. The non-crypto
    categories (weather, economics, financials) do not.
  - The main argument for registering is diversification, not level.

---

## Hygiene

**Endpoints touched:**
- `api.elections.kalshi.com/trade-api/v2/historical/trades`: **one** keyless
  GET.
  - Bounds: `min_ts=2025-04-01`, `max_ts=2025-04-02` UTC, `limit=1`.
  - Only the HTTP status and field names were printed. No price, count or
    outcome value was read, printed or stored.
- Not called: `/historical/markets` and `/historical/cutoff` (no date bound),
  and any endpoint dated on or after 2026-07-01.
- `kalshi.com/docs/kalshi-member-agreement.pdf` and
  `kalshi.com/docs/kalshi-fee-schedule.pdf`: one GET each, both returned 429.
  Nothing was saved.
- `karlwhelan.com/Papers/Kalshi.pdf`: the paper, 0.97 MB, saved to
  `research/copytrade/data/scout_l5b/paper/` (gitignored). It was read as a
  document.
- Web pages: Kalshi help centre and API docs, Polymarket docs, GitHub, arXiv
  abstract, CEPR/UCD, and press and review sites listed above.

**Rules kept:**
- No outcomes: no resolution, hit rate, P&L or bucket return was computed.
  `l5b_costs.py` is arithmetic on the published fee formula and on published
  returns only.
- No account, no API key, nothing paid. Total download about 1 MB.
- Nothing published or uploaded. No commit.
- Files created: this file, `scouts/l5b_costs.py`, `scouts/l5b_api_probe.py`
  and `data/scout_l5b/paper/kalshi_whelan.pdf`. Nothing else was modified.
- Python was run with `-I`.
