# L5 desk scout: funding-settlement effects (L5a) and the prediction-market favourite–longshot bias (L5b)

2026-10-11. This is desk work: literature, docs, legal texts, file listings,
and one behaviour-only count from local files. No return around a funding
timestamp and no prediction-market P&L was computed. Every number about
outcomes below is quoted from a published source, with its venue and period.

Scripts:
- `research/copytrade/scouts/l5_listing.py`: S3 listing and HEAD byte counts only.
- `research/copytrade/scouts/l5_behaviour.py`: trade counts and quantity by
  second around the hour. The price column is never selected.

Outputs, gitignored, are in `research/copytrade/data/scout_l5/`
(`listings/`, `behaviour/`).

---

## Verdicts

- **L5a: feasible to register, with a low prior. It needs owner decision Y:
  which venue a German resident would trade on.** Binance closed derivatives
  to German residents in 2021. On Hyperliquid, funding is hourly, so the 8 h
  settlement event the light is built on does not exist there. A Binance
  effect would have to be captured on another venue (Hyperliquid, or OKX
  Europe X-Perps) and tested as such. Data costs nothing: 1m klines for 3
  coins over 3 years come to 195 MB.
- **L5b: blocked by German law. There is no lawful venue for a resident of
  Germany.** The GGL classifies prediction-market bets as unlicensable,
  illegal gambling, and says that taking part from Germany is prohibited and
  can be punished. No prediction market holds an EU or German authorisation.
  L5b ends here. The literature and data sections are kept short and are for
  the record only.

---

## L5a: funding-settlement effects in crypto perpetuals

### Summary (5 lines)
1. **Literature.** No published study documents a tradeable price pattern
   around 8 h funding settlement. What is documented is volume and
   quoted-spread U-shapes over the funding cycle (Ruan & Streltsov,
   2017–2023). The closest return-predictability paper finds a clock-time
   effect of about 0.5 bp. It is unchanged when the funding quarter-hours
   are excluded, and the authors call it "small relative to trading costs"
   (Kim & Hansen 2026, Binance, 2021–2024, out of sample).
2. **Mechanism.** Funding is paid peer to peer, to positions held at the
   timestamp (Binance ±15 s, Bybit ±5 s). The rate is a 5-second-sampled
   average premium that every venue shows live, so the payment is fully
   predictable minutes ahead. The baseline is 1 bp per 8 h.
3. **Data.** Binance 1m klines for BTC, ETH and SOL over 2023-07 → 2026-06
   are **195 MB**. aggTrades are **51.4 GB** and trades **101.5 GB**. Bybit
   tick data is roughly 60–94 GB per coin over 3 years. All free.
4. **Costs.** A taker round trip is about **10–11 bp** on Binance VIP0 and
   about **9–10 bp** on Hyperliquid tier 0. A maker entry with a taker exit is
   about **7 bp** on Binance and **6 bp** on Hyperliquid, before adverse
   selection. That is 20 times the only published predictable clock-time
   component.
5. **Behaviour (local, HL-cascade-selected days).** The top of every hour
   carries a burst, about ×2 activity in the first 10 s. Funding hours
   08/16 UTC are not distinguishable from the non-funding 4h-candle hours
   04/12/20. 00 UTC (funding plus the daily candle) is the busiest boundary
   for SOL, but for BTC and ETH it sits below 22–23 UTC, which is not a
   funding time. In the last 10 s before all 9 funding boundaries the
   taker-buy share by count is 0.51–0.58, against a baseline of about 0.50.
   That is inside the scatter of other hours.

### Q1. Literature

| source | venue, period | finding | effect size | costs | out of sample |
|---|---|---|---|---|---|
| Ruan & Streltsov, *Perpetual Futures Contracts and Cryptocurrency Market Quality* ([SSRN 5323703](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5323703); [Cornell summary](https://business.cornell.edu/article/2025/02/perpetual-futures-contracts-and-cryptocurrency)) | spot and perp order books, many venues, 2017–2023; Huobi shutdown plus 95 staggered listings | Spot volume and percent quoted spreads are **U-shaped over the 8 h funding cycle**, higher around settlement. The same shape appears in the perp market. The authors read it as informed trading that is larger when funding is larger. | not in the abstract; spreads and volume only, **no returns** | n/a | identification by natural experiment, not a trading test |
| Kim & Hansen, *The Quarter-Hour Effect* ([arXiv 2607.09426](https://arxiv.org/abs/2607.09426)) | Binance USDT-M BTC, ETH, XRP, SOL, DOGE, ADA, 2021-01 → 2024-10 | Volatility and volume burst at 1-min, 5-min and quarter-hour marks. The 10-second opening return is predictable. | OOS R² is 3.4% on average. The sign-weighted predictable part is **about 0.5 bp per boundary**. **Excluding the three funding quarter-hours leaves the result "essentially unchanged."** | Stated: "small relative to trading costs" (5.0 bp taker, 2.0 bp maker) | yes: monthly walk-forward from 2021-07 |
| Hansen, Kim & Kimbrough, *Periodicity in Cryptocurrency Volatility and Liquidity* ([arXiv 2109.12142](https://arxiv.org/abs/2109.12142)) | BTC and ETH; Coinbase, Binance, Uniswap v2 | Day-of-week, hour and within-hour patterns in volatility and volume, "grown stronger over the years". Related to algorithmic trading and to funding times. | volatility and volume only | n/a | n/a |
| MDPI 2026, *Temporal Dynamics of Market Microstructure in Cryptocurrency Perpetual Futures* ([IJFS 14(5):103](https://www.mdpi.com/2227-7072/14/5/103)) | 26 exchanges, 812 symbols, 2025-11 → 2026-01 | Funding-rate spreads across venues peak about 2 h after the 00/08/16 settlements | "statistically significant but economically modest" | no | no |
| Turn-of-the-candle ([PMC10015199](https://pmc.ncbi.nlm.nih.gov/articles/PMC10015199/)) | BTC, from mid-2020 | Positive returns concentrate at minutes 0/15/30/45 | small, per minute | no | no |
| Hour-of-day BTC (Padyšák & Vojtko, via [Quantpedia](https://quantpedia.com/are-there-seasonal-intraday-or-overnight-anomalies-in-bitcoin/)) | Gemini hourly, 2015–2022 | Returns at 22–23 UTC stand out, 03–04 UTC are worst | per-hour mean, not a funding effect | no | no |
| He, Manela, Ross & von Wachter, *Fundamentals of Perpetual Futures* ([arXiv 2212.06888](https://arxiv.org/abs/2212.06888)) | Binance mechanics | Documents the 5-second premium sampling (T = 5,760 per 8 h) | mechanism only | — | — |

**8 h versus 1 h funding.** No paper compares price behaviour under 8 h and
1 h funding. Venues now switch a symbol's interval dynamically: Binance moves
to 1 h when the cap is hit and back to 4 h after 16 quiet cycles; OKX uses
N ∈ {1, 2, 4, 8}. This gives a natural experiment, but nobody has published
it.

**Bottom line.** The published evidence is about activity and spreads, and
it agrees with our local count. The only out-of-sample clock-time return
predictability is about 0.5 bp, and it does not come from funding.

### Q2. Mechanism: who is paid, when the snapshot is taken, what is visible

| venue | schedule | snapshot rule | rate formation, and is it visible beforehand | source |
|---|---|---|---|---|
| Binance USDⓈ-M | 00/08/16 UTC by default; switches to 1 h when the cap or floor is hit; back to 4 h after 16 cycles at ≤0.025% | Only positions open at the timestamp pay or receive. "Actual funding time may deviate by ±15 s." | Premium index every 5 s, time-weighted over 5,760 points. Interest 0.01% per 8 h; clamp ±0.05%. **The estimated rate and a countdown are shown live.** | [Binance FAQ](https://www.binance.com/en/support/faq/introduction-to-binance-futures-funding-rates-360033525031) |
| Bybit | 8 h default, 4 h or 1 h on some symbols | Pay or receive only if the position is held at the funding time. A trade within about 5 s of it is not guaranteed in or out. | position value × rate; predicted rate shown | [Bybit fee calc](https://www.bybit.com/en/help-center/article/Funding-fee-calculation), [intro](https://www.bybit.com/en/help-center/article/Introduction-to-Funding-Rate) |
| OKX | 00/08/16 UTC; some contracts on 1/2/4 h | Positions held "at the point of fee assessment". Closing before it exempts you. | Weighted average premium over the window; formula changed in 2025 (divisor 8/N). Current rate available via `GET /api/v5/public/funding-rate`. | [OKX mechanism](https://www.okx.com/en-us/help/perps-funding-fee-mechanism), [FAQ](https://www.okx.com/en-us/help/funding-fees-for-perpetual-contracts-faq) |
| Hyperliquid | **every hour**; each payment is 1/8 of the computed 8 h rate | Docs give `size × oracle × rate`; no explicit snapshot wording | Premium sampled every 5 s and averaged over the hour; interest 0.00125%/h (11.6% APR to shorts); cap 4%/h; peer to peer, no fee taken | [HL docs](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding) |

Who is paid: if the rate is positive, longs pay shorts; if negative, shorts
pay longs. The exchange takes nothing.

Consequences for the hypothesis:
1. **The payment is known before it happens.** It is a time-weighted average
   that is mostly fixed in the last minutes. A trader who wants to avoid
   paying closes a few seconds early and reopens just after. That is a round
   trip in fees (≥ 9 bp taker) to avoid about 1 bp at the baseline rate, so
   it pays only when funding is far above baseline.
2. **Funding-time flow therefore only matters when funding is extreme.**
   Those episodes are rare and clustered, which is exactly where 001–006
   kept finding thin samples.
3. **On Hyperliquid the 8 h event does not exist.** Any capture there is a
   cross-venue bet that Binance and Bybit settlement flow moves HL prices.

### Q3. Free data, and bytes for 3 coins × 3 years (2023-07 → 2026-06)

Sizes come from S3 listings (Binance) and HEAD requests on 10 sampled days
per coin (Bybit). See `data/scout_l5/listings/`. Nothing was downloaded.

| dataset | BTC | ETH | SOL | **total** |
|---|---|---|---|---|
| Binance `um/monthly/klines/1m` (OHLCV, taker-buy volume, trade count) | 66.6 MB | 68.4 MB | 60.1 MB | **195 MB** |
| Binance `um/monthly/aggTrades` (tick, taker side) | 19.8 GB | 22.4 GB | 9.2 GB | **51.4 GB** |
| Binance `um/monthly/trades` | 32.4 GB | 48.7 GB | 20.4 GB | **101.5 GB** |
| Binance `um/monthly/fundingRate` | 32 KB | 32 KB | 32 KB | 0.1 MB |
| Bybit `public.bybit.com/trading/<SYM>/` (daily csv.gz ticks, since 2020-03) | ≈ 94 GB (mean); ≈ 60 GB (median day) | ≈ 87 GB | ≈ 75 GB | **≈ 190–256 GB** (rough; the sample includes 2024-08-05) |

All 36 months are present for every Binance set. Notes:
- 1m klines are enough for a first registration (minute ±N around 00/08/16,
  with flow from the taker-buy-volume column). They fit under the
  owner's 1 GB ask threshold.
- Tick data is needed only for second-level windows, which the
  ±15 s settlement jitter makes questionable anyway.
- Binance `um` has no 1s klines (see `h2-binance-data.md`).
- Hyperliquid multi-year tick history is not free. The local archive covers
  2025-07-28 → 2026-06-30 only, and that year is the discovery set read
  14 times.

### Q4. Costs, and the effect needed to clear them

| venue | taker | maker | typical spread, BTC/ETH | typical spread, SOL | source |
|---|---|---|---|---|---|
| Binance USDⓈ-M, regular/VIP0 | 5.0 bp (4.5 bp with the BNB 10% discount) | 2.0 bp (1.8) | 1 tick (0.1 / 0.01 USDT), ≪ 1 bp | 1 tick (0.01) ≈ 0.5–1 bp | [Finder summary](https://www.finder.com/binance-futures-fees), [Binance BNB discount](https://www.binance.com/en/support/articles/360033544231); Kim & Hansen state the same 5.0/2.0 bp |
| Hyperliquid, tier 0 (< $5 M 14-day volume) | 4.5 bp | 1.5 bp | about 1 tick; a one-off CEO snapshot had BTC at a $1 spread ([The Block](https://www.theblock.co/post/387129)) | ≈ 1 bp | [HL fees](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees) |

Spreads widen around settlement (Ruan & Streltsov), so near-boundary
crossing costs are worse than the calm figures above.

**Gross effect per trade needed to break even, in majors:**
- Taker round trip: Binance ≈ 10 bp fees + ≈ 0.1–1 bp spread → **≥ 10–11 bp**.
  Hyperliquid → **≥ 9–10 bp**.
- Maker entry with taker exit: Binance **≥ 7 bp**, Hyperliquid **≥ 6 bp**.
  On top of that comes adverse selection on the maker fill. It is not
  measured, and it is largest in exactly the windows where informed flow
  clusters.
- Holding across the timestamp on the paying side adds the funding itself:
  1 bp per 8 h at baseline, more when funding is extreme.

**Context.**
- These bars are 12–20 times the 0.5 bp predictable component in Kim & Hansen.
- **Power, for registration (no outcome used):** three settlements a day over
  three years gives about 3,300 events per coin. Assume an hourly return
  sigma of about 40–60 bp. Then a 10 bp mean event return has a standard
  error near 1 bp, which is about 10 SE.
- An effect large enough to clear costs would therefore be easy to see in
  free 1m data. Its absence from the literature is weak evidence against it.

**Against HLP.**
- HLP's calm-year 2–4% needs only about 0.3 bp per trade net at 1×, over
  about 1,100 trades a year. The bar is the cost, not the return.
- An intraday strategy flat at every settlement has no spiral tail. It also
  has no spiral upside, which is where HLP made 17 of its 19 points.

### Q5. Behaviour check (local, counts and quantity only)

**Data and method.**
- Local Binance aggTrades for BTC (334 days), ETH (325) and SOL (318),
  2025-07-28 → 2026-06-30, read with a guard at < 2026-07-01.
- Columns read: `transact_time`, `quantity`, `is_buyer_maker`. Price is
  never selected.
- Activity per second at offsets −600…+599 s around every UTC hour boundary,
  relative to the same hour's −600…−301 s baseline.
- Output: `data/scout_l5/behaviour/funding_second_profile{,_per_hour}.json`.

Quantity in the first 10 s after the boundary, as a multiple of baseline:

| boundary (UTC) | BTC | ETH | SOL | type |
|---|---|---|---|---|
| 00 | ×3.83 | ×2.54 | ×3.75 | funding + daily candle |
| 04 | ×2.32 | ×1.93 | ×2.12 | 4h candle |
| 08 | ×2.11 | ×1.86 | ×2.37 | funding |
| 12 | ×3.03 | ×2.10 | ×2.51 | 4h candle |
| 16 | ×2.40 | ×1.98 | ×2.34 | funding |
| 20 | ×2.98 | ×2.33 | ×2.49 | 4h candle |
| 22 / 23 | ×4.35 / ×3.94 | ×3.36 / ×3.35 | ×2.78 / ×2.76 | none (US close) |
| other hours | ×1.8–2.6 | ×1.5–2.5 | ×1.4–2.4 | — |

**Readings.**
1. Every hour has a top-of-hour burst (the Kim & Hansen effect).
2. 08 and 16 UTC look like the non-funding 4h boundaries. 00 UTC is busy,
   but it is also the daily candle. It is the top boundary for SOL, and for
   BTC and ETH it is exceeded by 22–23 UTC.
3. Pooled over the three funding hours, the 10 minutes after the boundary
   carry about 10–20% more activity than other hours. That is confounded
   with session and candle clocks.
4. In the last 10 s before funding, activity is not elevated (×0.7–1.4).
   The taker-buy share by count is 0.520/0.581/0.523 for BTC at 00/08/16,
   0.530/0.528/0.507 for ETH, and 0.514/0.517/0.513 for SOL, against
   baselines of about 0.49–0.50.
5. That is a consistent small tilt across all 9 cells. Non-funding hours
   range from 0.448 to 0.538 in the same window, so the tilt is not
   distinguishable from that scatter. It is also count-weighted, not
   notional.

**Caveats.**
- The days were selected for HL cascades (high volatility), so they are not
  a random sample.
- This is one year, all inside the HL discovery span: the same calendar
  days, on a different venue.
- Second-level alignment is blurred by Binance's ±15 s settlement jitter.

### Q6. One line
- **Why it could be consistent:** the payment is mechanical and recurs three
  times a day, and anyone with a position near a boundary has a reason to
  time trades around it.
- **Why it is probably arbitraged away:** the rate is published live and the
  payment is about 1 bp at baseline. Avoiding it costs more than paying it
  except when funding is extreme, so the only flow it forces is small and
  predictable. The documented clock-time predictability (about 0.5 bp) is
  roughly 20 times below a round trip.

### Access for the owner (affects the L5a verdict)
- **Binance:** wound down derivatives for users in Germany on 2021-07-30
  ([The Block](https://www.theblock.co/post/113068/binance-crypto-derivatives-trading-europe-shut-down),
  [Cointelegraph](https://cointelegraph.com/news/binance-to-shut-down-crypto-derivatives-trading-in-europe)).
  Binance data is research data only.
- **OKX Europe:** "X-Perps" since 2026-04-15, under a Maltese MiFID licence.
  These are 5-year-expiry futures with a funding mechanism, 10× cap and an
  appropriateness test
  ([Cointelegraph](https://cointelegraph.com/news/okx-europex-perps-regulated-crypto-derivatives)).
  The funding schedule was not checked.
- **Hyperliquid:** EU persons are not listed as restricted in its terms, as
  read by third parties. Commentary is that the authorisation duty falls on
  the provider, not the user
  ([FinTelegram](https://fintelegram.com/perps-at-the-eu-perimeter-when-permissionless-meets-mifid-ii/),
  [CryptoTicker](https://cryptoticker.io/en/hyperliquid-germany-mica-licence/)).
  This is not legal advice. The programme already treats HL as usable.

---

## L5b: favourite–longshot bias in prediction markets

### Summary (5 lines)
1. **Legal (decisive): no lawful venue for a German resident.**
   - The GGL (5 Sep 2025) names Polymarket and calls prediction-market
     bets "nicht erlaubnisfähig und damit illegal". It adds that
     participation from Germany is prohibited and "strafbar".
   - §285 StGB carries up to 6 months or 180 daily rates for the player.
2. **Polymarket and Kalshi.** Polymarket geoblocks Germany (close-only).
   Kalshi's own list reportedly does not, but that does not make
   participation lawful. Contracts on financial underlyings also fall
   under the BaFin binary-option retail ban (ESMA statement, 3 Jul 2026).
3. **Other venues.** Betfair Exchange is not licensable in Germany: licensed
   sports betting is fixed-odds only (§3(1) S.4 GlüStV 2021). No prediction
   market holds an EU or German authorisation.
4. **Literature (for the record).** The bias is present on Kalshi (Bürgi,
   Deng & Whelan; 300k+ contracts) and on Polymarket in aggregate:
   sub-10¢ purchases lose 19.3¢ per $, flow-weighted. It shrinks to −6.3¢
   equal-weighted and flips to +4.1¢ when grouped by parent event, and is
   absent in sports (Cardozo & Rivero-Wildemauwe 2026).
5. **Capturability (for the record).** Fees at the tails (Kalshi ≈ 6% of a
   10¢ price), capital lock-up and oracle risk eat much of the measured
   premium. Moot, because access is unlawful.

### Q1. Lawful access from Germany (first and decisive)

**The legal frame (primary sources).**
- **GGL press release, 5 Sep 2025.**
  ([gluecksspiel-behoerde.de](https://www.gluecksspiel-behoerde.de/de/news/ggl-warnt-vor-teilnahme-an-illegalen-gesellschaftswetten))
  - Bets "wie sie etwa auf der Plattform Polymarket angeboten werden, sind
    nach geltendem deutschem Recht nicht erlaubnisfähig".
  - Legal basis cited: §3 Abs. 1 Satz 4 i.V.m. §4 Abs. 5 GlüStV 2021.
  - "Sowohl das Veranstalten oder Vermitteln solcher illegalen Wetten, als
    auch die Teilnahme daran, sowie die Bewerbung des Angebotes, sind
    strafbar."
- **GGL FAQ "Gesellschaftswetten".**
  ([link](https://www.gluecksspiel-behoerde.de/de/fuer-spielende/informationen-fuer-spielende-faqs/faq-illegales-gluecksspiel/faq-gesellschaftswetten))
  - Names "sogenannte Prognosemärkte" explicitly.
  - Says "eine Teilnahme, bzw. eine Platzierung einer Wette ist aus
    Deutschland heraus jedoch verboten".
  - Cites §3 Abs. 1 S. 3 GlüStV (bets are gambling) and §§284 f. StGB.
    The GGL files criminal complaints.
- **GlüStV 2021, §3 Abs. 1 S. 4.**
  ([gesetze-bayern.de](https://www.gesetze-bayern.de/Content/Document/StVGlueStV2021-3))
  - Licensable sports bets are defined as "Wetten **zu festen Quoten**" on
    sporting events.
  - Exchange (peer-to-peer) betting and non-sport event bets therefore fall
    outside every licensable category.
- **§285 StGB.** ([gesetze-im-internet.de](https://www.gesetze-im-internet.de/stgb/__285.html))
  - Taking part in unauthorised public gambling (§284) is punishable by up
    to 6 months' imprisonment or a fine of up to 180 daily rates.
  - Enforcement against players is real:
    - AG München, 26.9.2014, 1115 Cs 254 Js 176411/13, convicted a player
      of unlicensed online blackjack ([press release](https://www.justiz.bayern.de/gerichte-und-behoerden/amtsgerichte/muenchen/presse/2015/01.php)).
    - In 2025 the police summoned players under §285, with leads from bank
      payment monitoring ([wallstreet-online, 22.05.2025](https://www.wallstreet-online.de/nachricht/19380312-polizei-beschuldigter-beteiligung-unerlaubten-gluecksspiel); a promotional source).
    - Civil side: AG München, 20.12.2023, 251 C 17550/23, held that a
      player who knowingly takes part violates §285 and cannot reclaim
      losses ([gesetze-bayern.de](https://www.gesetze-bayern.de/Content/Document/Y-300-Z-BECKRS-B-2023-N-41833)).
- **Financial-law overlay.**
  - ESMA public statement, 3 Jul 2026: event contracts on MiFID C(4)–(10)
    underlyings are binary options. National retail bans apply to them,
    and EU distribution needs investment-firm authorisation
    ([PwC Legal](https://legal.pwc.de/en/news/articles/esmas-public-statement-confirms-that-existing-binary-options-prohibitions-apply-to-event-contracts),
    [AMF copy](https://www.amf-france.org/en/news-publications/news/esma-public-statement-esma-reminds-firms-existing-rules-and-obligations-under-binary-option-measures)).
  - Germany's ban: BaFin Allgemeinverfügung under Art. 42 MiFIR, since
    2 Jul 2019 ([BaFin](https://www.bafin.de/SharedDocs/Veroeffentlichungen/DE/Aufsichtsrecht/Verfuegung/vf_190701_allgvfg_Binaere_Optionen.html)).
    It binds providers. The player is not penalised by it, but no
    authorised channel exists.
  - ESMA TRV 2/2026, as reported: the largest platforms hold no EU
    authorisation ([CryptoTicker](https://cryptoticker.io/de/polymarket-esma-eu-zulassung/)).
- **GGL joins European regulators' joint statement on prediction markets, 19 Jun 2026.**
  Signatories: BE, FR, DE, IT, NL, PL, PT, ES, CH
  ([GGL](https://www.gluecksspiel-behoerde.de/de/news/ggl-schliesst-sich-gemeinsamem-statement-europaeischer-gluecksspielaufsichtsbehoerden-zu-herausforderungen-durch-prognosemaerkte-prediction-markets-an)).

**Verdict per venue.**

| venue | venue's own status for DE | German law | **verdict** |
|---|---|---|---|
| Polymarket (international) | **DE is "close-only" on frontend and API**: no new positions ([Polymarket geoblock docs](https://docs.polymarket.com/developers/CLOB/geoblock)) | Named by the GGL as illegal; participation punishable | **Unlawful** (and geoblocked) |
| Polymarket US (QCEX, CFTC DCM) | US-facing app; access by waitlist ([Crowdfund Insider](https://www.crowdfundinsider.com/2025/07/246515-polymarket-acquires-cftc-regulated-qcex-returns-to-the-us-as-environment-embraces-innovation/), [Bitcoin Magazine](https://bitcoinmagazine.com/markets/polymarket-rolls-out-us-app)) | Same GGL position; no German licence | **Unlawful / not accessible** |
| Kalshi | DE is **not** on the restricted list of Member Agreement v1.6, as read by a third party ([CoinPerps, 2026-06-12](https://www.coinperps.com/learn/kalshi-restricted-countries-prediction-market-perps); the [primary PDF](https://kalshi.com/docs/kalshi-member-agreement.pdf) returned HTTP 429) | Non-financial contracts (sports, politics) are unlicensed gambling, so §285 applies. Financial-underlying contracts are binary options sold without EU authorisation. The venue not blocking Germany does not make taking part lawful. | **Unlawful** (unclear only in that no court has ruled on a Kalshi player) |
| Betfair Exchange | Not offered to German customers; Betfair withdrew over the betting tax ([InterGame](https://www.intergameonline.com/igaming/news/betfair-pulls-out-of-germany-in-tax-row)) | Exchange betting is not a licensable form (fixed odds only); not on the GGL whitelist | **Unlawful** |
| An EU-licensed prediction or exchange venue | None found. ESMA reports that no large platform holds an EU authorisation. | — | **None exists** |

**None is lawful. L5b ends here.** Reopening it would need a German
licence or an EU MiFID-authorised event-contract venue that serves German
retail. The binary-options ban makes the second unlikely for financial
underlyings.

### Q2. Literature (for the record)
- **Kalshi.** Bürgi, Deng & Whelan, *Makers and Takers*
  ([UCD WP2025_19](https://www.ucd.ie/economics/t4media/WP2025_19.pdf),
  [CESifo 12122](https://www.ifo.de/DocDL/cesifo1_wp12122.pdf),
  [CEPR DP20631](https://cepr.org/publications/dp20631)).
  - Over 300k contracts. A strong favourite–longshot pattern: sub-10¢
    contracts win far less often than break-even and carry very large
    average losses, worse for takers.
  - Makers out-earn takers, but both show the pattern.
  - Exact bucket returns were not extracted (the PDF could not be parsed
    here).
- **Polymarket.** Cardozo & Rivero-Wildemauwe 2026
  ([arXiv 2609.12878](https://arxiv.org/abs/2609.12878)).
  - 588 M trades, 2.48 M accounts.
  - Sub-10¢ purchases lose **19.3¢ per $**, flow-weighted; ≥90¢ purchases
    earn 0.83¢.
  - Equal-weighted, longshots lose 6.3¢. Grouped by parent event, they
    **gain 4.1¢**.
  - Present in crypto and politics, **absent in sports**.
  - The sign depends on aggregation, which is a red flag for a trading
    rule.
- **Polymarket calibration.** A Princeton thesis on 188,509 resolved
  markets, 2022-11 → 2025-12, finds a recalibration slope of 1.112, i.e.
  underconfident, consistent with the bias
  ([link](https://theses-dissertations.princeton.edu/entities/publication/d5708372-5ea3-4252-b669-7f9cee07387c)).
- **Kalshi and Polymarket by domain.** Le 2026 (353 M trades) finds
  persistent underconfidence in political markets (prices compressed toward
  50%) ([arXiv 2602.19520](https://arxiv.org/abs/2602.19520)).
- **Older markets.**
  - InTrade: Page & Clemen (2013, *Economic Journal* 123(568)) find the
    bias grows with time to expiry, consistent with a time-value-of-money
    explanation.
  - Horse racing: Snowberg & Wolfers (2010, *JPE* 118(4)) attribute it to
    probability misperception.
  - Both are cited from memory and were not re-fetched.
- **Net.** Recent crypto prediction markets show the bias in aggregate. It
  is not robust across weighting and is absent in sports.

### Q3. Free data (for the record)
- **Polymarket.**
  - CLOB `GET /prices-history` (per asset, `startTs`/`endTs`, `fidelity` in
    minutes) ([docs](https://docs.polymarket.com/developers/CLOB/timeseries)).
  - Gamma (market metadata and resolution).
  - On-chain CTF-exchange fills on Polygon, via subgraph, from about 2022.
    Cardozo et al. used 588 M trades.
  - Not sized, because the light is legally blocked.
- **Kalshi.**
  - `GET /historical/markets`, `/historical/markets/{ticker}/candlesticks`
    and `/historical/trades`, behind a moving live/historical cutoff
    ([docs](https://docs.kalshi.com/getting_started/historical_data)).
  - Not sized and not called.

### Q4. Costs and frictions (for the record)
- **Fees.**
  - Kalshi's taker fee is about 0.07·P·(1−P) per contract: 0.63¢ at 10¢,
    i.e. 6.3% of the price. The maker fee is about a quarter of that
    ([rivermarkets summary](https://www.rivermarkets.com/insights/kalshi-fees.html)).
  - Polymarket international charges no fee on most markets.
- **Capital lock-up.**
  - Selling a 5¢ longshot ties up 95¢ to earn 5¢ until resolution.
  - At a 4% risk-free rate over 6 months, about 1.9¢ of a 5¢ "premium" is
    time value alone. This is Page & Clemen's horizon effect.
- **Tail spreads.**
  - At the tails the tick is 1¢ on a 3–5¢ price, i.e. 20–33% of the price.
- **Resolution and oracle risk.**
  - Polymarket resolves via a UMA optimistic oracle, with disputed
    resolutions on record.
  - This is a fat-tailed loss for the seller of a longshot. It is exactly
    the spiral-style tail HLP is long and this trade would be short.

### Q5. One line
- **Why it could be consistent:** it is a preference for lottery-like
  payoffs, documented over decades and across venues, and it is renewed by
  retail inflows.
- **Why it is probably not capturable:** for us, because it is unlawful from
  Germany. Even elsewhere, the measured premium is largely time value,
  fees and tail spread, and its sign flips with how contracts are grouped.

---

## Hygiene

**Endpoints touched:**
- `s3-ap-northeast-1.amazonaws.com/data.binance.vision`: S3 listing XML
  only, metadata (keys and sizes). Counts are filtered by file-name date to
  2023-07 … 2026-06. The listing returns key names for later months, but no
  object was fetched.
- `public.bybit.com/trading/BTCUSDT/`: directory index (file names) plus
  30 HEAD requests (BTC, ETH, SOL × 10 days, all dated ≤ 2026-06-15). No
  bodies were fetched.
- `kalshi.com/docs/kalshi-member-agreement.pdf`: one GET for the legal
  document, which returned 429. The HTML error page is saved in the session
  scratchpad, not the repo.
- Web pages: venue docs and help pages, GGL, BaFin, gesetze-im-internet,
  gesetze-bayern, arXiv, SSRN, news. No trading or market-data API was
  called. No account, no key, nothing paid.

**Local data read:**
- `research/copytrade/data/binance/um/aggTrades_parquet/{BTC,ETH,SOL}USDT/`,
  with the SQL guard `transact_time < 1782864000000` (2026-07-01 UTC).
- Columns read: `transact_time`, `quantity`, `is_buyer_maker`.
- **No price column, no return, no markout.**

**Downloads:** none, beyond the listing JSON written to
`research/copytrade/data/scout_l5/listings/`. Python ran with `-I`.

**Holdout:** nothing dated on or after 2026-07-01 was read from any
market-data source. The local data sits inside the HL discovery span
(2025-07-28 → 2026-06-30) on a different venue. No trial count is consumed,
since no outcome was computed.

**Files created:**
- this file
- `scouts/l5_listing.py`
- `scouts/l5_behaviour.py`
- `data/scout_l5/**` (gitignored)

Nothing else was modified, and nothing was committed or published.
