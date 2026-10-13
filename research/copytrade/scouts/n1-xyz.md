# N1 scout: xyz (HIP-3) equity / index / commodity / FX perps off-hours

2026-10-12. Feasibility only, behaviour only. This scout computes no outcome. It does not
link any off-hours perp price, flow, funding or positioning to any later price, and it splits no count by a
future quantity. Costs are measured within 1 s only.

Scripts (all `research/copytrade/scouts/`):
- `n1_common.py`: session calendar and guarded reads.
- `n1_activity.py`: item 2.
- `n1_costs.py`: item 3.
- `n1_who.py`: item 4.
- `n1_deviation.py`: items 5 and 7.
- `n1_fred_fetch.py`: item 6.
- `n1_docs.py`, `n1_html_text.py`: docs parsing.

Outputs: `data/scout_n1/derived/` (activity_by_coin.csv, activity_by_class.csv, costs_by_*.csv,
who_by_session.csv, deviation_indices.txt).

## Summary

1. **The premise needs restating.** For US single names the xyz oracle is *externally* priced 24/5 (Blue Ocean
   ATS overnight, via Pyth). Indices and commodities are priced 23/5 from CME futures, and FX 24/5. **"Off-hours"
   in the xyz sense means weekends (Fri 20:00 ET → Sun 20:05 ET for stocks; Fri 17:00 → Sun 18:00 ET for
   indices and commodities) and exchange holidays.** Weekday overnight is not off-hours for US underlyings.
   Re-anchoring happens when the external feed resumes on Sunday evening ET, **not at the Monday official
   open.** The exception is **Korean, Japanese and Hong Kong names**, which have a real internal-priced session
   every weekday night: SKHX had 92 such nights with $1.39 bn traded.
2. **Universe:** 84 coins traded on xyz from 2025-10-13 to 2026-06-30, for **$326 bn notional and 185 M
   trades.**
   - Weekend share of notional: indices 6.6%, US stocks 6.0%, commodities 7.0%, FX 23%, Korea 12.6%.
   - Weekend $/hour is about 7–10% of regular-session $/hour, and about 25% for commodities.
   - Weekends per coin: XYZ100 37, SP500 15, the first US names (Nov 2025) 32–33, commodities 17–27.
3. **Costs are tiny on paper.**
   - Growth mode is on for almost all coins since listing (XYZ100 and the first names since 2025-11-23).
     Tier-0 fees are **taker 0.9 bp and maker 0.3 bp.** GOLD, MSTR, PURRDAT and STRC pay standard fees of 9 bp
     taker and 3 bp maker.
   - The 1-second spread proxy is about one tick everywhere: indices 0.4 bp; US stocks a median of 1.4–1.7 bp
     and p90 8 bp.
   - **Weekend depth is about 2–3× thinner.** The notional-weighted sweep cost of a $50–250k taker order is 13 bp
     on stocks at weekends against 3 bp in the regular session. On indices it is 1.7 bp against 0.4 bp.
4. **Who trades off-hours.** The past-only behaviour classes show:
   - previously liquidated accounts carry a **smaller** share at weekends (23% against 33–41% in the regular
     session);
   - new accounts carry a larger share (24–35% against 20–25%);
   - maker-dominant MM-like accounts are a steady about 17% of notional, with a taker ratio of 0.10.
   - **No skill score exists for xyz,** and none was computed (it needs P&L; see Risks).
5. **Past-only deviation of the index perps at Sunday 20:00 UTC from the previous official close:**
   - XYZ100: median −4 bp, |dev| p50 31 bp, p90 107 bp (n = 37).
   - SP500: median −21 bp, |dev| p50 33 bp, p90 109 bp (n = 15).
   - Peg at the official close: median error −2 to −3 bp, |err| p50 4 bp.
6. **Data:**
   - FRED (keyless, end-dated) covers official index closes, WTI/Brent spot and FX noon rates.
   - **No free, keyless, terms-compatible source of per-stock official opens and closes was found.** Stooq sits
     behind a JS challenge. Pyth Benchmarks and Hermes history now need a key. Dukascopy has moved to a
     requester-pays S3 bucket and rate-limits its public feed. The Nasdaq API's terms are unverified.
   - The registered test can use the perp's own last external print as its reference, so local data suffices.
7. **Power is poor for a weekend-only index test.**
   - The minimum detectable mean effect is about **53 bp per event** over 37 XYZ100 weekends, and about 68 bp over
     the proposed 23-weekend discovery block.
   - That is larger than the typical deviation itself. Pooling about 25 single names a weekend helps only through
     their idiosyncratic part.
   - Korean nights (about 90 a coin) are the best-powered sub-universe.

## 1. Mechanics (sources: docs.trade.xyz, read 2026-10-12, plus Wayback snapshots for history)

- **Oracle.** External pricing is used when the underlying feed is live (Pyth, via a relayer about every 3 s).
  Otherwise an internal EMA applies: `S_t = β S + (1-β)(S + IPD)`. IPD is the impact-price difference against the
  book, so the oracle drifts toward the perp's own impact bid and ask. β = exp(−Δt*/τ), and each step moves at most
  9.5%.
  - **τ was 8 h at launch, 1 h from 2025-11-21, and 30 min from 2026-04-30.**
  - The internal mode starts from the last external price. On resumption "the oracle reverts to the externally
    derived price on the next tick".
  - Relayer updates are clamped to ±50 bp per update, about every 3 s.
  - URLs: docs.trade.xyz/perpetuals/mechanics/oracle-price; changelog/oracle-time-constant-updates.
- **External price** is a separate field, frozen at the last external price while the market is closed
  (…/mechanics/external-price).
- **Mark** is the median of (oracle, oracle + 150 s EMA of (mid − oracle), median(bid, ask, last)). It is used
  for margin, liquidations and triggers (…/mechanics/mark-price).
- **Sessions** (…/specifications-and-schedules/specification-index):

  | Market | External pricing | Internal sessions |
  |---|---|---|
  | US single names and ETFs | 24/5, Sun 20:05 ET (20:00 before 2026-08-13) → Fri 20:00 ET. Pre, regular, post and overnight (Blue Ocean ATS) | Weekends and NYSE holidays |
  | Indices (XYZ100, SP500) | 23/5 from CME index futures, discounted to spot. Spot index only in the cash session (methodology v2 since 2026-02-19; v1 used futures with a fixed 4% r−q) | Daily 17–18 ET break, weekends, futures holidays |
  | Commodities | Sun 18:00 → Fri 17:00 ET. Spot for metals; designated futures with a 5th–10th business-day roll for CL, Brent, NATGAS and COPPER | Daily 17–18 ET break, weekends |
  | FX | Sun 17:00 → Fri 17:00 ET | Weekends |
  | Korea stocks | KRX main session 09:01–15:20 and after-hours 16:05–20:00 KST | 15:20–16:05 and **20:00–09:01 KST on weekdays**, plus weekends |
  | Japan stocks | TSE day session 08:20–16:30 and night session 17:00–23:59 JST (as documented now) | Weekdays and weekends outside those hours |
  | HK stocks | 09:30–12:00 and 13:00–16:00 HKT | Every weekday night, plus weekends |
  | Pre-IPO (SPCX, QNT, CBRS) | None | Always internal |

  - **History caveat:** 24/5 coverage of US single names is documented on 2026-01-15 (Wayback). The Nov–Dec 2025
    snapshots do not show single-name hours, so it is unknown whether overnight was external in Nov–Dec 2025.
- **Discovery bounds.**
  - Mark is held within ±1/maxLev of a reference price, which is the last external price at the start of an
    internal session.
  - v2 (2026-03-13, CL first) adds ratchet "resets": a trigger at 90% of the bound moves the reference to the
    bound. Resets were raised for most names on 2026-06-12.
  - Current bounds and resets: SP500 ±2%/1, XYZ100 ±3.5%/1, megacaps ±5%/2 (±10% before 2026-06-12), most names
    ±10%/0–1, metals ±4%/2, CL and Brent ±5%/2.
  - Positions whose liquidation price lies outside the active bound cannot be liquidated while it is in effect.
- **Funding.**
  - HyperCore pays hourly. xyz scales the HL formula by 0.5 since 2025-12-19:
    `F = 0.5·[P + clamp(r − P, ±0.05%)]`, a baseline of about 5.5%/yr.
  - The interest-rate term is 0 for EUR, GBP, JPY and KRW (`perpDexs`).
  - Pre-IPO markets use a multiplier of 0.005. The HL cap is 4%/h.
- **Fees.**
  - HIP-3 standard is 2× the validator-perp fees (tier 0: taker 0.090%, maker 0.030%), with 50% to HL and 50% to
    the deployer. `deployerFeeScale` is 1.0.
  - **Growth mode** cuts fees by ≥90%: tier 0 is taker 0.009% and maker 0.003%. HL's `meta` shows growth mode
    enabled on 123 assets, with `lastFeeScaleChangeTime` at or near listing (XYZ100, TSLA and NVDA on 2025-11-23).
  - **Not in growth mode:** GOLD, MSTR, PURRDAT, STRC and BMNR, by rule (gold; crypto wrappers).
  - Rebates are also cut by 90%. Tier is on 14-day volume, shared with HL. Sources: docs.trade.xyz/perpetuals/mechanics/fees;
    hyperliquid.gitbook.io/…/hip-3-builder-deployed-perpetuals; `api.hyperliquid.xyz/info` meta(dex=xyz).
- **Max leverage** (current): SP500 50×, XYZ100 30×, FX 50×, metals 25×, CL/Brent/COPPER 20×,
  megacaps 20× (10× before 2026-06-12), most names 10×, pre-IPO 5×.
  - Cross margin is available on the majors. The others use isolated margin, and pre-IPO markets strict isolated.
- **OI caps** (per market):
  - 2026-01-15 (Wayback): XYZ100 $200 M, SILVER/TSLA $100 M, NVDA/GOLD/GOOGL $50 M, others $25 M.
  - Now (`perpDexs`): SP500 $1.25 bn, XYZ100/CL/SKHX $1 bn, most new names $25 M.
- **Halts.** The deployer `haltTrading` action cancels orders and settles at mark (HIP-3 docs). Corporate actions
  are handled by settle-and-reopen, as in the Kioxia split. Holidays and weekends are not halts: trading continues
  on the internal oracle.
- **Re-anchoring.**
  - The oracle snaps back to the external price at the first tick after the feed resumes. The mark follows,
    clamped to ±50 bp per update, through the median rule.
  - For US names this happens at Sun 20:05 ET (Blue Ocean open), for indices and commodities at Sun 18:00 ET
    (CME Globex open), and for FX at Sun 17:00 ET.

## 2. Universe and activity (2025-10-13 → 2026-06-30, all fills; sessions in n1_common.py, DST-aware)

**Sessions by class:**
- **US stocks and ETFs** (and IPOP):
  - REG = 09:30–16:00 ET, with 13:00 on half days;
  - XH = the rest of the 24/5 week;
  - WKD = Fri 20:00 → Sun 20:00 ET;
  - HOL = NYSE weekday holidays.
- **Indices:** REG = cash hours; XH = futures hours; BRK = 17–18 ET; WKD = Fri 17:00 → Sun 18:00 ET.
- **Commodities:** CME Globex as REG.
- **FX:** Sun 17:00 → Fri 17:00 ET as REG.
- **Korea, Japan and HK:** local cash hours as REG.

**By class:**

| class | notional $bn | REG | XH (weekday off-cash) | BRK | WKD | HOL | liq share REG / WKD |
|---|---|---|---|---|---|---|---|
| commodity (8) | 163.9 | 91.6% | – | 1.2% | 7.0% | 0.2% | 1.15% / 0.99% |
| index (2) | 83.2 | 47.4% | 43.7% | 0.8% | 6.6% | 1.6% | 0.39% / 0.90% |
| us_stock (~60) | 61.8 | 54.6% | 38.6% | – | 6.0% | 0.9% | 1.07% / 1.41% |
| ipop (3) | 11.4 | 57.4% | 37.7% | – | 3.6% | 1.3% | 1.73% / 0.63% |
| korea (4) | 5.5 | 44.1% | 42.4% | – | 12.6% | 0.9% | 1.13% / 1.55% |
| fx (3) | 1.9 | 77.2% | – | – | 22.8% | 0.0% | 0.48% / 0.71% |
| japan, hk (5) | 0.2 | – | – | – | 3–7% | – | – |

**Top coins.** WKD $/h is weekend notional per calendar hour. "wknds" counts weekends with any trade against
weekends since listing. XH nights are weekday nights with any trade. Full table: `activity_by_coin.csv`.

| coin | class | first fill | $bn | REG % | XH % | WKD % | WKD $/h | REG $/h | wknds | XH nights | hol days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| CL | cmdty | 2026-01-06 | 62.7 | 89.3 | – | 9.0 | 4.6 M | 19.6 M | 25/25 | – | 2 |
| XYZ100 | index | 2025-10-13 | 55.3 | 47.6 | 43.2 | 6.9 | 2.1 M | 22.7 M | 37/37 | 220 | 16 |
| SILVER | cmdty | 2025-12-26 | 53.9 | 95.9 | – | 3.3 | 1.3 M | 17.1 M | 27/27 | – | 4 |
| BRENTOIL | cmdty | 2026-03-04 | 28.8 | 88.6 | – | 9.5 | 3.3 M | 13.3 M | 17/17 | – | 2 |
| SP500 | index | 2026-03-18 | 28.0 | 47.0 | 44.7 | 5.9 | 2.3 M | 28.1 M | 15/15 | 88 | 6 |
| GOLD | cmdty | 2025-12-22 | 13.1 | 91.0 | – | 7.7 | 0.76 M | 3.9 M | 27/27 | – | 6 |
| MU | stock | 2025-12-19 | 10.6 | 52.6 | 41.8 | 4.7 | 372 k | 6.6 M | 28/28 | 161 | 14 |
| SPCX | ipop | 2026-05-17 | 10.0 | 56.6 | 38.8 | 3.3 | 1.1 M | 29 M | 7/7 | 37 | 4 |
| SNDK | stock | 2026-01-12 | 6.8 | 58.1 | 38.4 | 3.0 | 179 k | 5.2 M | 24/24 | 141 | 10 |
| NVDA | stock | 2025-11-12 | 6.7 | 57.2 | 35.2 | 6.8 | 287 k | 3.8 M | 33/33 | 193 | 16 |
| SKHX | korea | 2026-02-19 | 4.6 | 44.6 | 42.2 | 12.5 | 495 k | 3.6 M | 19/19 | 110 | 10 |
| INTC | stock | 2025-12-03 | 4.0 | 57.9 | 35.9 | 5.2 | 147 k | 2.5 M | 30/30 | 175 | 14 |
| TSLA | stock | 2025-11-13 | 3.7 | 60.9 | 31.5 | 6.7 | 158 k | 2.3 M | 33/33 | 192 | 16 |
| CRCL | stock | 2025-12-15 | 3.2 | 58.5 | 35.8 | 5.2 | 124 k | 2.1 M | 28/28 | 165 | 14 |
| COPPER | cmdty | 2026-01-14 | 2.6 | 92.6 | – | 6.4 | 143 k | 0.9 M | 24/24 | – | 2 |
| GOOGL | stock | 2025-11-18 | 2.3 | 56.9 | 36.4 | 5.6 | 85 k | 1.3 M | 32/32 | 188 | 16 |
| MSTR | stock | 2025-12-02 | 2.0 | 53.2 | 36.3 | 9.4 | 131 k | 1.1 M | 30/30 | 176 | 14 |
| NATGAS | cmdty | 2026-01-21 | 1.9 | 91.7 | – | 6.5 | 108 k | 0.67 M | 23/23 | – | 2 |
| HOOD, PLTR, AMD, COIN | stock | Nov–Dec 2025 | 1.4–1.8 each | 57–61 | 31–36 | 6.6–7.5 | 69–78 k | 0.9–1.1 M | 30–33 | 174–191 | 14–16 |
| MSFT, ORCL, AMZN, META, AAPL | stock | Nov–Dec 2025 | 0.85–1.1 each | 52–57 | 35–41 | 6.2–7.3 | 37–53 k | 0.5–0.66 M | 30–32 | 173–188 | 14–16 |
| EUR, JPY | fx | 2025-12-23 | 0.94 each | 75–79 | – | 21–25 | 150–180 k | 220–230 k | 27/27 | – | 3 |
| SMSN | korea | 2026-02-20 | 0.86 | 41.9 | 43.8 | 12.6 | 94 k | 0.63 M | 19/19 | 108 | 10 |

- **The other ~55 coins total about $13 bn.** Most were listed in Mar–Jun 2026, with 1–17 weekends each.
- **Pre-period.** Before 2025-11-12 only XYZ100 traded, about $2.5 bn in total.
- **Korean internal nights** (20:00–09:01 KST, weekdays):
  - SKHX: 92 nights, $1.39 bn;
  - SMSN: 90 nights, $280 M;
  - HYUNDAI: 86 nights, $24 M;
  - KR200: 56 nights, $8.6 M.

## 3. Costs and depth proxies (contemporaneous, at most 1 s)

**Sample and definitions.**
- Sample: every 3rd calendar day, 2025-10-13 → 2026-06-30 (87 days, about 63 M taker events).
- A taker event is one taker's fills in one (coin, ms, side).
- **Spread proxy:** the gap between a taker event's best touched price and the touched price of the most recent
  opposite-side taker event, if that event is no more than 1 s earlier.
- **Sweep:** the event's VWAP against its first touched price.

| class / session | spread p50 / p75 / p90 (bp) | sweep, notional-weighted (bp) | event size p50 / p90 / p99 ($) | TWAP share |
|---|---|---|---|---|
| index REG | 0.40 / 0.78 / 1.22 | 0.69 | 680 / 15.0 k / 100 k | 3.7% |
| index XH | 0.40 / 0.41 / 0.81 | 0.67 | 365 / 7.5 k / 65 k | 4.8% |
| index WKD | 0.41 / 0.42 / 0.83 | 2.05 | 237 / 2.7 k / 36 k | 8.6% |
| us_stock REG | 1.68 / 4.05 / 7.81 | 2.26 | 329 / 4.6 k / 35 k | 4.0% |
| us_stock XH | 1.57 / 4.28 / 9.69 | 3.48 | 201 / 2.3 k / 24 k | 4.7% |
| us_stock WKD | 1.39 / 3.80 / 7.99 | 4.93 | 149 / 1.2 k / 10 k | 7.8% |
| commodity REG | 0.90 / 2.14 / 4.42 | 1.74 | 334 / 8.9 k / 73 k | 1.7% |
| commodity WKD | 0.84 / 1.69 / 3.08 | 4.67 | 200 / 2.8 k / 29 k | 4.3% |
| korea REG / WKD | 3.2 / 6.4 / 10.5 · 2.5 / 6.4 / 12.7 | 5.1 · 6.9 | 240 / 2.5 k / 19 k · 106 / 1.4 k / 13 k | 3.6 · 8.0% |
| fx REG / WKD | 1.9 / 5.0 / 6.2 · 0.9 / 0.9 / 1.9 | 1.1 · 0.7 | ~300–450 / 2.1–2.3 k / 5–7 k | 3.1 · 0.6% |

**Notional-weighted sweep by taker-event size, REG against WKD (bp):**

| size | index | us_stock | commodity | korea |
|---|---|---|---|---|
| < $10 k | 0.07 / 0.11 | 0.40 / 1.04 | 0.24 / 0.46 | 1.3 / 2.5 |
| $10–50 k | 0.13 / 0.54 | 1.26 / 4.63 | 0.57 / 1.76 | 4.5 / 8.6 |
| $50–250 k | 0.36 / 1.72 | 3.32 / 12.8 | 1.46 / 5.5 | 10.5 / 14.3 |
| ≥ $250 k | 2.8 / 10.4 | 16.5 / 31.3 | 7.4 / 21.7 | 28 / 16 (n = 37) |

**Reading.**
- Quoted spreads stay at about one tick at weekends, so makers keep quoting tight. Depth behind the touch is about
  3× thinner.
- Round trip at weekends for $10–50 k, taker on both legs with growth-mode fees:
  - indices about 3 bp (2 × 0.9 fee + 0.4 spread + about 0.5 sweep);
  - single stocks about 8–12 bp;
  - Korea about 15–20 bp;
  - GOLD and MSTR add 16 bp more in fees.
- Weekend funding at the 5.5%/yr baseline costs about 3 bp per 48 h; premium-driven funding is not measured.
- **Not measured:** the resting book (no historical L2) and maker fill probability.

## 4. Who trades off-hours (counts and volume shares only)

**Classes** come from the 001-style features at S = 2026-01-12, 03-09 and 05-04 (lookback [S − 91 d, S)) and are
applied to the hold window [S, S + 56 d). In priority order:
- **mm_like:** maker share ≥ 0.8 and ≥ 10 k fills;
- **liquidated:** ≥ 1 liquidation in the lookback;
- **large:** account value ≥ $100 k;
- **retail:** $1–100 k;
- **micro:** < $1 k;
- **no_value;**
- **new:** no xyz fill in the lookback.

Account counts at the three dates:
- mm_like 236 / 303 / 430;
- liquidated 5.4 k / 26.9 k / 45.9 k;
- new: not counted (not in the features).

Shares of two-sided notional (shares of taker notional in parentheses):

| group, session | mm_like | liquidated | new | large | retail | micro + no_value |
|---|---|---|---|---|---|---|
| equity REG | 17.0% (5.5%) | 33.0% (36.6%) | 19.5% (19.1%) | 14.9% (17.5%) | 11.0% (14.4%) | 4.6% (7.0%) |
| equity XH | 19.2% (5.6%) | 29.0% (32.4%) | 20.3% (20.8%) | 16.2% (20.5%) | 10.6% (14.4%) | 4.8% (6.3%) |
| equity WKD+HOL | 17.0% (3.5%) | 23.5% (26.0%) | 24.4% (27.0%) | 15.6% (18.5%) | 12.3% (16.4%) | 7.3% (8.6%) |
| cmdty+FX REG | 13.5% (4.7%) | 40.9% (37.9%) | 24.9% (32.8%) | 8.2% (9.9%) | 9.1% (10.3%) | 3.4% (4.5%) |
| cmdty+FX WKD+HOL | 13.6% (3.8%) | 23.4% (23.7%) | 34.7% (40.6%) | 11.6% (10.6%) | 9.5% (12.7%) | 7.2% (8.7%) |

**Taker ratios and liquidations.**
- Taker notional over own notional: mm_like 0.10–0.18 against 0.46–0.84 for every other class.
- Liquidated notional share by session (§2): indices 0.39% in REG against 0.90% at weekends; stocks 1.07%
  against 1.41%; commodities 1.15% against 0.99%.

**Reading.** Off-hours flow is more "new account" and less "previously liquidated". It is not more dominated by the
known loser cohort. MM presence is unchanged in volume share and takes less at weekends.

## 5. Off-hours deviation size, past-only (indices only)

- **Reference:** FRED official close.
- **Perp price:** the last trade in the 5 min before a checkpoint inside the internal session. Every checkpoint
  had a trade.
- **Window:** nothing after the checkpoint was read.
- The deviation includes the external Fri 16:00–17:00 ET futures hour.

| coin | checkpoint | n | p5 | p25 | p50 | p75 | p95 | abs p50 / p90 |
|---|---|---|---|---|---|---|---|---|
| XYZ100 vs NASDAQ100 | Sat 12:00 UTC | 37 | −83 | −53 | −10 | +9 | +31 | 25 / 72 |
| | Sun 12:00 UTC | 37 | −93 | −36 | −7 | +17 | +71 | 26 / 89 |
| | Sun 20:00 UTC | 37 | −120 | −36 | −4 | +24 | +76 | 31 / 107 |
| | weekday-holiday noon | 8 | −113 | −37 | −9 | +8 | +98 | 26 / 145 |
| SP500 vs SP500 | Sat 12:00 UTC | 15 | −65 | −42 | −23 | +1 | +34 | 23 / 71 |
| | Sun 12:00 UTC | 15 | −90 | −49 | −13 | +20 | +84 | 31 / 92 |
| | Sun 20:00 UTC | 15 | −140 | −43 | −21 | +19 | +65 | 33 / 109 |

**Peg quality during open hours.** This compares the perp's last trade in the 60 s before 16:00 ET with the
official close.
- XYZ100 against NDX: n = 179 days; p5/p50/p95 −11/−2/+9 bp; |err| median 4 bp.
- SP500: n = 72 days; −13/−3/+6 bp; |err| median 4 bp.
- XYZ100 therefore tracks the NDX level.

Single stocks, commodities and FX are not done here. There is no free official-close source for stocks. FRED's WTI
and Brent are EIA spot assessments and its FX series are noon rates, both mis-timed against the Friday 17:00 ET
close, so either would bias the deviation.

## 6. Data sources

| source | URL | keyless | terms | coverage pulled | used |
|---|---|---|---|---|---|
| FRED CSV | fred.stlouisfed.org/graph/fredgraph.csv?id=…&cosd=2023-01-01&coed=2026-06-30 | yes | FRED ToU; S&P and Nasdaq series copyrighted (personal research, no redistribution) | SP500, NASDAQ100, DJIA, DCOILWTICO, DCOILBRENTEU, DEXUSEU, DEXJPUS, DEXUSUK; 911 rows each, last row 2026-06-30, **0 rows ≥ 2026-07-01 received** | closes only (no opens) |
| HL info API | api.hyperliquid.xyz/info `meta`(dex=xyz), `perpDexs` | yes | public | current metadata (no prices) | fees, growth mode, OI caps, leverage |
| trade.xyz docs | docs.trade.xyz/llms-full.txt, /llms.txt | yes | public docs | current (2026-10-12) | mechanics |
| Wayback snapshots of trade.xyz docs | web.archive.org (10 pages, 2025-10-28 → 2026-06-17, plus 2 from 2025-11-05) | yes | public archive | — | mechanics history |
| Stooq CSV | stooq.com/q/d/l | — | JS proof-of-work challenge | none | **not usable** |
| Pyth Benchmarks / Hermes history | benchmarks.pyth.network, hermes …/updates/price/{t} | **no** (404; 401; API key since 2026-07/08 per Pyth notice) | — | none | skipped |
| Dukascopy | datafeed.dukascopy.com | 429/503; the export wiki points to a **requester-pays** S3 bucket | personal-use prompt | none | skipped (requester-pays) |
| Cboe index history | cdn(-api).cboe.com/…/SPX_History.csv | yes | — | none: a static full-history file with no end-date parameter, so it would deliver rows ≥ 2026-07-01 | not pulled |
| Nasdaq API | api.nasdaq.com/api/quote/…/historical | yes | terms unverified (fetch timed out); automated-access clause likely | none | not pulled; owner to judge |
| Keyed free tiers (Alpaca, Tiingo, Polygon) | — | **no** | — | none | need owner OK |
| Off-hours fair-value proxies (CME Sunday open, Blue Ocean ATS) | CME DataMine, Databento | paid | — | none | no free historical source found |

**Downloads:** about 4.4 MB in total. Directories:
- `data/scout_n1/fred_20261012T205723Z/`;
- `data/scout_n1/hl_meta_20261012/`;
- `data/scout_n1/docs_tradexyz_20261012/`;
- `data/scout_n1/wayback_tradexyz_20261012/`.

Each has a `SHA256SUMS`. FRED sums:
SP500 7136a240…, NASDAQ100 80222231…, DJIA 13d09ebc…, DCOILWTICO 09db8482…, DCOILBRENTEU 458f64c6…, DEXUSEU 3ad973b6…,
DEXJPUS 86c6b03d…, DEXUSUK da8b79ad…; meta_xyz c37719a6…, perpDexs 44a8c7b7…; llms-full 875e579a…, llms 57a4b2b8….

**Feasibility note.** A registered test does not need external stock data. Its reference can be the perp's own last
print before the internal session (Fri 20:00 ET for stocks, 17:00 ET for indices and commodities), and its exit the
perp after the resumption. Both are in the local fills, and both are forbidden in this scout. FRED adds an
official-close check for the indices.

## 7. Power sketch (underlying dispersion from FRED only)

**Event noise.** No source gave Friday-close → Monday-open, so the across-weekend close-to-close move stands in for
it as an upper bound on per-event noise.
- NDX: σ = 111 bp (2023-01 → 2026-06, n = 182; 112 bp in the discovery year).
- SPX: σ = 81 bp (80 bp).
- Daily close-to-close: NDX 127 bp, SPX 94 bp.

**Minimum detectable mean per event** (80% power, two-sided 5%, t_{G−1}; clusters = weekends):

| sample | G | σ (bp) | MDE (bp per event) |
|---|---|---|---|
| XYZ100, all weekends | 37 | 111 | **53** |
| XYZ100, proposed discovery | 23 | 111 | 68 |
| XYZ100, proposed validation | 9 | 111 | 118 |
| SP500, all | 15 | 81 | 63 |
| US single names pooled by weekend (assumption: σ = 250 bp, ρ = 0.4, k ≈ 22–29 names) | 23 / 32 | ≈ 162 | 100 / 83 |
| Korean nights (assumption: SKHX + SMSN, σ = 200 bp, ρ = 0.6) | ≈ 90 | ≈ 179 | ≈ 53 |

**As a slope** (the fraction β of the deviation that a fade captures):
- The Sun-20:00 deviation has σ ≈ 45–50 bp (from the IQRs in §5). SE(β) ≈ 111 / (45 × √37) ≈ 0.4, so the MDE is
  β ≈ 1.2 for XYZ100 alone, and **the index-only weekend test cannot detect even full reversion.**
- Single names pooled at about 900 coin-weekends, with an effective N of about 300 after clustering, would give
  β ≈ 0.3. This needs the stock deviation σ, which is unmeasured here.

**Event counts** through 2026-06-30:
- weekends: 32 (equities from 2025-11-22) and 37 (XYZ100);
- coin-weekends: about 920 single names, 47 index, 182 commodity, 60 FX, 69 Korea;
- Korean internal nights: about 90 a coin;
- US weekday overnight (220 nights for XYZ100): **not internal-priced events.**

## 8. Venue warning line

- Hyperliquid is unlicensed in the EU. This is a warning, not a blocker.
- trade.xyz Terms §2.2 bar US, Canada and UK persons and sanctioned territories. §2.3 bars circumventing geo-blocks by
  VPN. The docs restate "including to United States persons".
- The Terms govern the Interface. Direct HyperCore API access is not addressed.
- Germany and the EU are not named as restricted.

## 9. Proposed split (from activity, coverage and mechanics only; chosen before any outcome)

- **Excluded pre-period: 2025-10-13 → 2025-11-21.** Only XYZ100 traded ($2.5 bn), τ = 8 h, standard fees,
  5 weekends.
- **Discovery: weekends from Fri 2025-11-21 to Sun 2026-04-26.** τ = 1 h, growth-mode fees.
  - 23 weekends; 504 single-name, 29 index, 110 commodity, 36 FX and 33 Korea coin-weekends.
  - $12.0 bn weekend notional.
- **Internal validation: Fri 2026-05-01 → 2026-06-30.** τ = 30 min, the same mechanics as the sealed holdout.
  - 9 weekends; 418 single-name, 18 index, 72 commodity, 24 FX and 36 Korea coin-weekends.
  - $9.9 bn weekend notional.
  - It is cluster-thin, so it serves as an existence check only, with a t_8 critical value.
- **Alternative (balanced, rejected):** a split at 2026-04-13 gives 20 / 12 weekends and 412 / 510 single-name
  coin-weekends, but the τ change falls inside validation.
- **Holdout:** 2026-07-01 → stays sealed.

## 10. Risks and unknowns

- **No xyz skill score exists.** The derived features carry no score, and 001 scored only the main dex. Item 4
  therefore uses behaviour classes. The real 001 score needs round-trip P&L, which this brief forbids; if the
  orchestrator wants it, it must be computed in a registered step.
- **Mechanics changed mid-sample.** These are confounds for any pooled test:
  - τ (8 h → 1 h → 30 min);
  - funding ×0.5 (2025-12-19);
  - discovery bounds v1 → v2 (2026-03-13) and more resets (2026-06-12);
  - index methodology v2 (2026-02-19);
  - leverage raises (2026-06-12);
  - US internal-session end 20:00 → 20:05 ET (2026-08-13, holdout era).
- **Unknown overnight coverage before mid-January 2026.** Overnight external pricing for US names in Nov–Dec 2025
  is unverified. If it was internal then, those nights were off-hours events.
- **No free stock reference.** No free source exists for stock official opens and closes, or for any off-hours fair
  value (CME Sunday open, ATS). Only the perp itself and FRED index closes are available.
- **The re-anchor is not the Monday open.** It happens at the Sunday-evening feed resumption, which is a CME or ATS
  print. That print is the real "truth" a weekend fade resolves against. Its timing and the ±50 bp-per-update clamp
  matter for execution.
- **Capacity is thin at weekends.** A $50–250 k taker order costs 13 bp on stocks against 3 bp in the regular
  session. Maker fill probability is unmeasured, and there is no historical L2.
- **Holiday calendars** for KRX, JPX and HKEX were typed from memory; this is minor (Korea, Japan and HK are about 2%
  of volume).
- **Prior from public literature.** A search returned the title and summary of a Blockworks/HL-Policy-Center piece,
  "Weekend trading: evidence of price discovery in Hyperliquid's weekend markets". It claims weekend xyz prices carry
  price discovery. The piece itself was not read. If true, a pure fade is the wrong sign.

## 11. Reads (for the ledger)

**Local fills.**
- `data/pruned/by_dex/xyz/fills/perp/all/date=2025-10-13 … 2026-06-30` (261 partitions), always filtered on
  timestamp < 2026-07-01 00:00 UTC.
- Used for:
  - coin list and volumes;
  - minute aggregates over all days;
  - taker events on every 3rd day;
  - class volumes 2026-01-12 → 2026-06-29;
  - XYZ100 and SP500 taker prints on all days (deviation and tracking).
- **Printed artefact:** the first coin listing printed `max(timestamp)::date` as 2026-07-01 for every coin. This came
  from a session-local (Europe/Berlin) date cast of timestamps up to 2026-06-30 23:59 UTC. The query carried
  `timestamp < '2026-07-01 00:00+00'`, and every later query set TimeZone = UTC. **No row at or after the cutoff was
  read.**

**Local derived files.**
- `data/derived/xyz/features_{2026-01-12,2026-03-09,2026-05-04}.parquet`: lookbacks end before S.
- `data/derived/xyz/snapshot_accounts.parquet`: one query, filtered to date < 2026-07-01; min/max/count only.
- `data/archive/by_dex/xyz/snapshots/`: **not opened or listed.**

**Downloads.**
- FRED 2023-01-01 → 2026-06-30 (end-dated in the request; 0 rows ≥ cutoff).
- One FRED probe (SP500, 2025-10-01 → 10).

**Current-state reads** (dated after the cutoff, no price, funding or OI values):
- **HL `meta`(xyz) and `perpDexs`:** names, leverage, growth mode, fee-scale timestamps and OI caps. These reveal
  names listed after 2026-07-01 (for example GIGADEV, SKHY, CXMT, UNITREE, LRCX) and delistings.
- **trade.xyz docs as of 2026-10-12:** changelog entries dated 2026-08-13, 2026-09-28 and 2026-10-09; pre-IPO
  anticipated listing dates.
  - The docs also contain **example prices of unknown date**: SKHX 886,000 KRW, SMSN 181,000 KRW, HYUNDAI 499,000
    KRW, USD/KRW 1,444.10; plus an illustrative Kioxia ¥45,000 and a 2025-10-14 NQ example.
  - These are noted, not used.
- **Pyth Hermes feed lookup:** AAPL feed metadata, including the current `market_hours`; no price.

**Failed or no-data probes:**
- Stooq: challenge page.
- Pyth Benchmarks: 404. Hermes history: 401.
- Dukascopy: 429/503 error bodies.
- Cboe: 307 redirect only; no file body fetched.

**Web pages:** HIP-3 docs, trade.xyz Terms, the Dukascopy export wiki, and search-result summaries, including the
Blockworks title noted in §10.

**Forbidden statistics respected:**
- No off-hours price, basis, flow, funding or positioning was linked to any later price (open, close, later perp, or
  funding).
- No gap-fill counts, conditional means, correlations or hit rates.
- No count was split by a future quantity.
- No P&L, markout or realised spread beyond 1 s. The only later-than-reference comparison is perp at a checkpoint
  against the PREVIOUS official close, as allowed.
- Nothing dated ≥ 2026-07-01 was read as data.
- Nothing paid, keyed or requester-pays was used, and nothing over 1 GB was downloaded.
- No touch of `lib/`, `pages/`, `vercel.json`, registrations, `results.jsonl` or other scouts. No commits. Nothing was
  published.

## 12. Honest odds

- **Effect exists (any tradeable off-hours mispricing of either sign, beyond about 5 bp of cost): about 1 in 3.**
  - For: weekends are about 7% of volume with one-tick quotes and thin depth. Flow is new-account-heavy, and the
    discovery bounds cap the price.
  - Against: MMs quote tightly through weekends, and the oracle follows the book with τ = 30 min. The public prior is
    "weekend prices discover", not "weekend prices overshoot".
- **Deployable at our size: about 1 in 6.** Power for indices alone is too low (MDE ≥ 50 bp per event, larger than
  the typical deviation). Single names need about 900 coin-weekends and an unmeasured σ. Korean nights are the
  best-powered sub-universe but cost 15–20 bp per round trip.
- **Capacity per event** (taker, at ≤ about 5 bp extra impact):
  - indices about $0.5–1 M notional per weekend (XYZ100 and SP500 together);
  - CL about $1–2 M;
  - single names about $20–50 k per coin, or about $0.5–1 M per weekend across about 25 names.
  - At a hypothetical 20 bp net edge that is **about $2–6 k per weekend**, or about $0.1–0.3 M a year.
  - Korean nights: about $50–100 k per night (SKHX + SMSN) at about 15 bp cost.
