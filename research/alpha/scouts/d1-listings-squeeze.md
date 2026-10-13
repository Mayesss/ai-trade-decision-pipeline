# D1 scout: new-perp listings, the squeeze tail by mechanism, and the one trial for the unread confirmation calendar

2026-10-13. Round 4, behaviour-only scout. No outcome was computed on any listing window, in any calendar.
013's Result § is the only listing outcome cited. The forbidden conditionings and the exposure are in §6.

## 1. Summary

1. **Recommendation: spend the confirmation calendar on 013 T2 frozen** (Binance listing family, trial 3).
   - Test it as a single-stage existence test on the tail- and market-adjusted α.
   - Critical value: max(t_{G−2}(0.975), CAL ≈ 2.2), with CAL to be re-derived.
   - Compute the stop variant (§3.2) in the same run as a **reported, non-deciding** overlay.
2. **No tail mechanism fixes what limits this test.** Fitting the noise to the T1 and T2 standard errors in 013's Result §:
   - the per-event noise is about 21–25% in the body plus the squeeze atoms;
   - T2 has about 58 events on the confirmation calendar.

   So any T2-sized trial has a standard error of 3.7–4.7%. At a true α of 2 / 3.4 / 5% it passes 4–8% / 7–14% / 13–22% of the time. A stop changes this by less than 0.03.
3. **Margin depth (a) is rejected.**
   - On seasoned coins, once the high reaches +90% the median 7-day exit is still ×1.6–1.8. About 10% of these go on to reach ×2.86.
   - So 0.5× margin saves only 0.14–0.22 of notional per squeeze, and it doubles the worst loss per event.
   - The documented squeezes either persisted for days (MYX, ALPACA) or spiked past ×2.86 (XPL on HL).
4. **A price stop (c) is the only construction that changes the tail.**
   - Its effect on the mean has no sign from mechanism (Kaminski & Lo). On seasoned coins it was close to zero, and slightly positive on coins 30–120 days old.
   - It cuts the plausible worst month at 2.5% from −29.7% to about −20%.
   - It keeps Kelly positive when squeezes cluster: about 0.14 against about 0 for frozen, at an assumed μ of 3.4% with 3% squeeze weeks.
   - It is a sizing overlay, not a better test, and it can hurt the attention-fade trades (a second pop, then the dump).
5. **Odds:** about 1 in 10 to confirm and about 1 in 20 to reach paper. At a robust size the book is 3–7%/yr before 013's peso charge and about 0–4%/yr after it. That is below HLP on a risk-adjusted basis and far below the mandate.
   - **Spend the calendar only because it is cheap** (013's code and panels are reused).
   - Rank it below any light with real upside.

## 2. Mechanism options

Arithmetic base from 013's Result §, T1, discovery:
- mean +2.10%, median +9.05%;
- 17 of 257 events (6.6%) liquidated at −100%;
- funding −0.87%.

So the mean of the non-liquidated events is (2.10 + 6.6) / 0.934 ≈ **+9.3%**. The atoms cost about 6.6 × (1 + 0.093) ≈ **7.2 points of mean**. Any tail construction is worth at most a share of those 7.2 points, less what it costs on the body.

**Proxy evidence.** I ran a new behaviour look on **seasoned** coins, never on listing windows (`d1_seasoned_paths.py`).
- Data: the 009 panel, 1h bars, 2021-12 → 2024-01.
- Coin-weeks start ≥ 30 days after a coin's first kline and end ≥ 7 days before its segment ends. BTC and ETH are out.
- Each coin-week is a 7-day short under each tail rule.
- Output: `research/alpha/data/scout_d1/derived/seasoned_paths.txt`.

Squeeze shape (daily starts, small caps, 85,540 coin-weeks; Monday starts and the young stratum agree):

| fact | small caps | young coins (30–120 d) |
|---|---|---|
| P(1h high ≥ +30% / +50% / +90.5% / +186% within 7 d) | 6.5% / 2.1% / 0.52% / 0.06% | 9.0% / 3.5% / 1.06% / 0.09% |
| given +90.5% touched: exit/entry quartiles | 1.37 / **1.61** / 1.86 | 1.32 / **1.67** / 2.02 |
| given +90.5% touched: exit back at or below entry | 10 of 443 | 5 of 94 |
| given +90.5% touched: later touched ×2.86 | 48 of 443 (11%) | 8 of 94 |
| mean P&L on those weeks: 1× / 0.5× / stop at +50% | −1.00 / −0.79 / −0.51 | −1.00 / −0.78 / −0.50 |
| weeks touching [1.5, 1.905) without liquidation: 1× vs stop at +50% | −0.34 vs −0.50 (3× as many weeks) | −0.40 vs −0.50 |
| mean difference against 1×, per coin-week: 0.5× / stop +30% / stop +50% / 0.5× + funding exit | +0.11% / −0.01% / +0.00% / +0.15% | +0.24% / **+0.35%** / **+0.28%** / +0.20% |
| SD per coin-week: 1× / stop +30% / stop +50% | 14.6% / 13.1% / 13.8% | 17.5% / 14.4% / 15.5% |

Daily starts overlap, so their standard errors (e.g. 0.065–0.095% for the young stops) are understated by about 2–3×. Seasoned coins are a proxy: a new listing's float is thinner and its liquidation rate is 12× higher (6.6% against 0.5%).

### (a) Margin depth (0.5× isolated: margin = 2 × notional; liquidation at ×(1 + 2)/1.05 = ×2.86)

- **Rationale.** A squeeze must exceed +186% to liquidate. The event then books its actual move at exit, so a transient spike can revert.
- **Sign from mechanism: none.** It is a bet that spikes revert. The programme's fact 2 (forced flow reverts within seconds) is about the book's microstructure. It is not about cornered-float squeezes, where the holder keeps the price up.
- **Cost.** Capital per event doubles at the same notional, so the structural worst event, week and month double: at 2.5% notional, −5%, −35% and −95%. At equal structural bounds the notional halves, and so does the book.
- **Tail, by arithmetic.**
  - Gain per squeeze = 1 − [q·2 + (1 − q)·E(exit move | not liquidated at 0.5×)].
  - With the proxy's q ≈ 0.11 and a median exit of ×1.6–1.7, the gain is about +0.2 per atom. On T1 that is about 0.066 × 0.2 ≈ **+1.3% per event, at twice the per-event loss bound**.
- **Documented squeezes** (opened sources, §7). None is computed by us, and none sits in a known listing window.
  - **MYX:** about +1,400% in a week, to $18.42 (Cointelegraph, 2025-09-10). Peak $19.03 on 2025-09-11, then −52% to about $9 two weeks later (Rekt, 2025-09-23). Binance settled MYX funding hourly. It stayed far above ×2.86 for days, so 0.5× loses twice what 1× loses.
  - **ALPACA** (a delisting, not a listing): $0.029 → $1.47 within days. The funding cap went from ±2% to ±4% and settlement from 4 h to 1 h. $43 M of shorts were liquidated, and the price was still about 18× the start at publication (crypto.news, 2025-05-01). Persistent, so 0.5× is worse.
  - **XPL**, HL hyperp, 2025-08-26: about ×2.5–3 to $1.80 within minutes, against $0.55 on Binance pre-market, with more than $17 M liquidated and then a correction (Forklog, 2025-08-28). The spike crossed ×2.86 from $0.60, so 0.5× would also have been liquidated. Only a mark price built on an external index would have survived it, and HL added such inputs afterwards.
  - **JELLY**, HL, 2025-03-26: HLP was about $12 M underwater. Validators delisted the contract, and Binance and OKX listed JELLY perps within about an hour (The Block, 2025-03-26). The risk here is venue action, and margin depth does not address it.
- **Point in time:** yes; it is a parameter.
- **Verdict: rejected** (§5).

### (b) Pre-entry crowding and float facts, known at first trade + 24 h

| fact | sign from mechanism | free and point-in-time? | verdict |
|---|---|---|---|
| **Day-1 funding sum ≥ 0 (T2)** | Funding is the *price* of crowding (premium + clamp). A sustained discount means hedgers crowd the short, so the decline is already charged *and* the squeeze has fuel. Excluding those events raises EV and lowers squeeze risk: **+** | yes (funding archive; 013's build) | **keep: the one filter** |
| OI / 24 h volume at entry | ambiguous: sticky positions, but the side is unknown | yes (metrics 5-min archive from 2021-12; day 0 for 227/239 in 2025) | reject: no sign |
| Long/short account ratios (metrics) | more short accounts means fuel (−); more retail long means fade (+). Account-weighted and noisy; funding already prices the money-weighted crowding | yes | reject: a second crowding measure is a free degree of freedom |
| Circulating supply / FDV at listing | low float: squeezable over 7 days (−), unlocks later (+, mostly beyond 7 days) | **no.** CoinGecko and CMC history are keyed, DefiLlama emissions is Pro-only, and Binance announcement APIs cannot be end-dated (`000i`, n3 §A4/A6) | reject: not computable |
| Spot-first vs perp-first | perp-first means hedgers short the perp (−) | yes | reject: **0 of 148** confirmation events are spot-first (013 §1), so there is no variation |
| Listing on several venues at once | deeper float (+) against more short capacity (−) | Bitget has survivorship (96/239 unknown in 2025, removed symbols); HL is small (45/239 in 2025, 4/41 in 2026-H1) | reject |
| Pre-market history (HL hyperp before Binance) | price discovery already done (smaller pop) against speculative longs | proxy only (30 cases, 2023-06 → 2026-06); hyperps carry no API flag | reject: too few, no sign. Binance pre-market perps are already out |
| Day-1 return (first trade → entry) | overreaction (+) against a squeeze in progress (−); funding sign already separates these | yes | reject |
| Liquidity tier at entry | deep books are hard to corner (+) | yes | **reject: contaminated.** 013's Result § printed the most liquid tier at +13.2% (t 2.68, n 33). Any liquidity cut would be mining |

### (c) Exit rules

- **Price stop.**
  - Kaminski & Lo: under a random walk a 0/1 stop always lowers expected return; with momentum it can add value (§7).
  - A squeeze of a cornered float is momentum: forced buybacks feed buying, and MYX and ALPACA ran for days. The edge itself is a *reversal* (attention fades), so a stop also cuts trades that pop again before they fade. **The sign is not determined.** The proxy is about 0 on seasoned coins and slightly + on young coins.
  - **The tail improves for certain:** the atom goes from −1 to about −0.5 − gap, and the SD falls 6–18%.
  - **Gap risk.** A trigger on the 1m *close* followed by a next-open fill filters single-minute wicks but fills late in a fast squeeze. Charge the exit slippage ×2 and keep 013's liquidation on the last-price high before the fill (conservative).
  - Point in time: yes, and computable from 013's existing panel without a new pull.
- **Time stop** (a shorter hold). It cuts funding and squeeze exposure in proportion and also the captured decline. 7 days is the literature's reversal window, and 013 fixed it. **Rejected:** no hold length has a sign from mechanism, so changing it is tuning.
- **Funding-cap trigger** (exit at the first settlement at the floor, or when the interval shortens with a negative rate).
  - The mechanism is real: a spot-led corner puts the perp at a discount, and shorts pay up to 2%/h. ALPACA's −2%/h is about −48% a day at 1×.
  - On seasoned coins a −0.3% trigger added about +0.15%/week.
  - **Rejected for this trial:**
    - it is redundant with a price stop (the perp rises with the index in a corner);
    - the cap level per contract is not in the archive;
    - `000n` already exposed the confirmation calendar's aggregate capped-funding row counts (6.45 rows per event-day against 6), which is exactly this trigger's frequency.

### (d) Sizing

- 013's bounds are exact at 1×, from rolling caps of ≤ 7 entries per 16 days and ≤ 19 per 40 days: per event c, per week 7c, per month 19c.
- T2 runs at about 55 entries a year (013: 96 taken over 1.55 discovery years), so the caps rarely bind.
- **Synthetic Kelly** (`d1_kelly.py`; body and atoms fitted to 013's T2 standard error; weekly market factor; Poisson exits; assumed, not measured):

| rule, μ per event | full Kelly c\*, no squeeze weeks | full Kelly c\*, plus an **unpriced** 3%/week squeeze-week risk |
|---|---|---|
| T2 frozen, 2% / 3.4% / 5% | 0.18 / 0.26 / 0.33 | ≈ 0 / ≈ 0 / 0.10 |
| T2 + stop at +50%, 2% / 3.4% / 5% | 0.18 / 0.29 / 0.35 | ≈ 0 / **0.14** / 0.17 |

- ¼ Kelly is 4–9% without clustering, so the −30% month line binds at 2.5% (as in 013).
- With clustered squeezes, frozen T2's Kelly collapses at μ ≤ 3.4%; the stop's does not.
- **Venues are not independent.** One token on Binance, Bybit, OKX, Bitget and HL is one price: MYX squeezed across Binance and Bitget, and HL hyperps can diverge (XPL ×3 on HL against Binance). A second venue adds capacity, not diversification, and it is a new trial (013 D1).

### (e) Hedging

- **A long β leg** (BTC or a trimmed alt index, β ≈ 1.5).
  - Cost: in a bear calendar it gives back the market fall the short earned. Long funding is about +0.01%/8h baseline, about 0.2% of hedge notional per 7 days, plus 2 × 5–6 bp in fees.
  - It works only for **correlated** alt rallies. A +30% alt week against 7 open positions at 2.5% is about −7.9% of the book unhedged and about 0 hedged.
  - It is useless for idiosyncratic atoms: MYX's +1,400% against a flat market leaves the sector hedge offsetting only a few percent of the loss.
- **A sector basket:** the same, with worse liquidity and its own squeezes.
- **Verdict:** report the hedged book (013 already requires the raw and adjusted books both to pay). No hedge leg in the trade.

## 3. The recommended rule vs T2 frozen

### 3.1 The two candidates

- **T2 frozen**: 013 §4 exactly, restricted to T2 events. 1× isolated, last-price-high liquidation, 7 days, archived funding, 013's costs.
- **T2-stop** (the best construction from mechanism; §4.2): T2 frozen plus a 1m-close buy-stop at +50%, filled at the next traded 1m open with the exit slippage ×2. The 1× liquidation check on last-price highs stays. Everything else as T2.

### 3.2 Numbers

Both are synthetic and assumed; no listing outcome was used.
- Confirmation calendar: 121 primary events / 38 weeks (013 §2.2).
- T2 share: 48%, from 013's discovery count (115 of 239). It was **not counted** on the confirmation calendar.
- So n ≈ 58 events, G ≈ 29 weeks.
- Noise is calibrated to 013's cited discovery standard errors (T1 1.20 / 0.49 = 2.45% on 239; T2 3.37 / 1.24 = 2.72% on 115). That gives three fits: body k 2.5 with p 4%; k 1.5 with p 6.6%; and, for T1, k ≈ 3.3 with p 6.6%.

| | **T2 frozen** | **T2-stop** |
|---|---|---|
| test | tail-adjusted, quarter-β α (013 §7.2), single stage, family trial 3 | the same; atom = the stopped loss |
| critical value (one-sided 2.5%) | max(t₂₇(0.975) = 2.05, CAL). The null 97.5th percentile over null, drift and misaligned-regime worlds is up to **2.17** (600 draws), so **≈ 2.2** | null 97.5th ≤ 1.77, so **2.05** |
| size at 2.05 (null worlds) | 0.015–0.028 | 0.005–0.012 |
| α standard error on the confirmation | 3.7–4.7% | 3.5–4.5% |
| **P(pass)** at μ 1 / 2 / 3.4 / 5 / 8% | 0.03–0.05 / 0.04–0.08 / 0.07–0.14 / 0.13–0.22 / 0.32–0.45 | 0.01–0.02 / 0.03–0.05 / 0.06–0.10 / 0.12–0.19 / 0.30–0.47 |
| P(pass), averaged over the posterior of α | **≈ 0.09** (posterior N(2.3%, 2.0%) from the prior N(1%, 3%) and the discovery α 3.37 ± 2.72; less after winner's curse) | ≈ 0.07–0.10 (Δ_stop ~ N(0, 1%), proxy) |
| worst event / week / month (structural, c 2.5%) | −2.5% / −17.5% / −47.5% | the same (a wick can still liquidate at 1×) |
| **plausible worst month** (013 construction: half of 19 exits squeezed, half −25%) | −29.7% | **−20.2%** (squeezed exit −0.60) |
| model month, 1 in 10 years, c 2.5% (no / 3% squeeze weeks) | −4.9% / −7.5% | −4.5% / −5.5% |
| model week, 1 in 10 years, c 2.5% | −3.7% / −7.5% | −3.2% / −4.5% |
| full Kelly (§2d), μ 3.4% | 0.26, or ≈ 0 with clustered squeezes | 0.29, or 0.14 |
| robust size | 2.5% (the −30% month line) | 2.5% at 013's bounds; 3.5% if sized on the plausible month (−28.3%; the structural month is then −66.5%) |
| **book at the robust size**, μ 2 / 3.4 / 5%, 55 entries a year | +2.8 / +4.7 / +6.9 %/yr | the same at 2.5%; +3.9 / +6.5 / +9.6 at 3.5% |
| after 013's peso floor (one squeeze month per pooled sample, 0.156/yr × the plausible month) | −4.6%/yr, so ≈ 0 / 0 / +2.3 | −3.2%/yr at 2.5%, so ≈ 0 / +1.5 / +3.7 |
| capacity | Binance only: $50–250k per event (n3 §C; day-0 volume median $74–177 M, OI after 1 h $1.6–3.1 M), i.e. about $2–10 M of capital at 2.5%. Bitget is a subset and a new trial; HL is negligible (4 of 41 in 2026-H1, 3–5× caps) | the same; a stop costs little extra impact at these sizes |
| P(deploy \| confirm) | 0.40 flat / 0.61 bear calendar (013 §6, reviewer at μ 4%) | similar |
| **odds overall** | **≈ 1 in 10 to confirm, ≈ 1 in 20 to paper** | the same, or slightly less |

### 3.3 Which one should spend the calendar, and why

**T2 frozen.**
1. **It is the only rule whose design predates the discovery read.** Every new rule, this one included, was designed after 013's 6.6% and its strata were printed. The stop is motivated by the liquidation count, which is allowed, but its level is a free parameter.
2. **The stop does not raise the power of the existence test.** Power is set by n and by body noise (σ ≈ 21–25% per event), not by the atoms.
3. **The stop's effect on the mean has no sign from mechanism.** A T2-stop failure would not say whether T2 or the stop failed. A T2 frozen result is clean.
4. **It is cheap.** 013's panels, `simulate`, the quarter-β α, `confirm` and `deploy` exist, reviewed twice and hashed. Only the trial-3 wrapper, the critical value and π_pool are new.
5. **The stop's real value is sizing and Kelly robustness**, which matter only after a confirm. So compute T2-stop in the same run as a pre-registered **reported, non-deciding** overlay, and make it the form of the forward paper stage if (and only if) T2 frozen confirms and deploys.
6. **The honest caveat.**
   - At about 1 in 10, the read is a lottery ticket with a 3–7%/yr payoff.
   - **The orchestrator may prefer to leave the calendar unread** until a light with real upside needs Binance listing windows. Its option value is small, though: power is limited by n, not by design.
   - To raise n with the rule unchanged, a registration may openly claim the sealed 2026-07-01 → 2026-12-31 listings as part of the same existence test, read once after the last hold ends. That adds about 15–20 T2 events at 2026-H1's pace (41 listings), so the standard error falls only about 12–15%.

## 4. Rule sketches, pre-registrable

### 4.1 Trial 3 — "013 T2 frozen on the confirmation calendar" (recommended)

- **Family.** Binance USDⓈ-M listing windows. **This is trial 3.** T1 and T2 failed in discovery (013), and T2 was not carried by 013's rule. This is a new registration (015+), not a revival of 013. The family-wise rate reaches ≤ 7.5% with this third test at 2.5%.
- **What was seen.**
  - 013's Result §: every number, including the strata (perp-first +2.70%, most-liquid tier +13.2%, continuations, pre-market, by year) and T2's discovery α of +3.37% (t 1.24).
  - `000n`: aggregate hold-window funding and zero-trade row counts on the confirmation calendar, i.e. capped-funding episodes.
  - `000i`: the post-cutoff announcement titles and literature.
  - 013 §0: public priors, including the 2025-10-10 cascade and the 2025–26 decline (the reason for the α test).
  - The later-segment survival facts for AIAUSDT (confirmation) and PUMPUSDT.
  - This scout's seasoned-coin proxy (§6).
- **Events.** 013's confirmation primary events (121; 013 §2.2 and §2.5 rules unchanged) with T2's feature (funding settled in (first trade, entry] sums to ≥ 0; JUP-style missing-month rule). The T2 count on the confirmation calendar is printed only inside the gated run.
- **Trade, costs, funding, liquidation:** 013 §4–§5 verbatim (`ct/listings013.py`, hash `db5f315a…`).
- **Statistic.**
  - 013 `trial_stats` with β per calendar quarter of first trade (groups of ≥ 10 events), CR1 by listing week, df G − 2.
  - **α_tail = α̂ − max(0, π_pool − π_conf)·(1 + the mean of the non-liquidated net).**
  - π_pool = max(T2's rate pooled over reported + discovery + confirmation, discovery + confirmation). The drafter takes T2's discovery and reported liquidation counts from 013's result row, a read of 013 output that must be ledgered.
- **CONFIRM iff** all of:
  - α_tail > 0;
  - t_tail ≥ max(t_{G−2}(0.975), CAL), with CAL re-derived by 013's `simulate_worlds` restricted to T2-sized samples (≥ 1,500 draws; null, drift, aligned and misaligned regime, squeeze and dilution worlds). This scout's estimate is about 2.2;
  - α_tail > 0 without the best listing week;
  - n ≥ 10 and G ≥ 5.
- **Deploy:** 013 §6 (`deploy`, min(raw, adjusted), peso floor) at c\* ≤ 2.5%.
- **Reported, decides nothing:**
  - the T2-stop overlay (§4.2) with its book, Kelly and plausible month;
  - the untailed α; the split-β α;
  - stressed costs; month-flip placebos.
- **Paper form:** T2-stop if T2 frozen deploys **and** the overlay's confirmation mean ≥ the frozen mean − 1 point; otherwise T2 frozen.
- **Runner:** an r013-style guarded runner in a new module (013's hashed modules read-only), one row, run once.

### 4.2 T2-stop: the best rule from mechanism (the alternative; reported overlay under 4.1)

- 013 T2 exactly, plus:
  - **Stop:** at the first traded 1m bar after entry whose **close** ≥ 1.50 × the entry price, exit at the **open of the next traded 1m bar**.
  - **Costs on a stopped exit:** that fill's slippage is the exit tier × 2, plus the fee.
  - **Liquidation still applies:** if any 1m last-price high before that fill reaches 013's 1× liquidation price (about ×1.904), the event is liquidated (−1).
  - **Funding** accrues to the exit. No re-entry. Status `stopped` is counted before any decision.
- **Why +50%:** half of the margin, at 1×. It sits beyond the body's 7-day noise (about 2σ), so the stop is a squeeze exit, not a noise exit. It is fixed now and is not chosen on listing data.
- **Why a close trigger:** it filters single-minute wicks without a mark-price pull. The live analogue is a mark-price-triggered stop-market order. A paper stage should measure the mark-vs-close mismatch.
- **Tail.** The same structural bounds as 013 at c = 2.5%. The plausible month is −20.2% and the plausible event about −1.5%.

## 5. Rejected options and why

- **(a) 0.5× margin** (alone, or with a funding exit). The proxy and the documented cases say squeezes that reach +90% mostly persist (median exit ×1.6–1.8; 11% go on to ×2.86). The saving is about 0.2 per atom while the loss bound per event doubles. XPL crossed ×2.86 anyway.
- **Liquidity filter.** Contaminated: 013 printed the most liquid tier's result.
- **OI/volume, long/short ratios, day-1 return, multi-venue, pre-market history.** No sign from mechanism, a second crowding degree of freedom, or survivorship / too few events.
- **Float / FDV.** No free point-in-time source.
- **Perp-first filter.** No variation on the confirmation calendar.
- **Shorter or longer hold.** No sign from mechanism; tuning.
- **Funding-cap exit.** Redundant with the price stop. The cap is not in the archive. Its frequency on the confirmation calendar is already exposed (`000n`).
- **Hedge leg.** Useless against idiosyncratic atoms and costly in a bear calendar. It is reported, not traded.
- **Other venues** (Bitget, HL, Bybit, OKX). Same token, same squeeze. There is no free point-in-time Bitget listing or funding archive (survivorship), and HL is too small. Each is a new trial.
- **T1 events with a stop** (n 121). More events, but the discovery α of T1 is 1.20 (t 0.49). At a standard error of about 3.2% it would need α̂ ≥ 6.6%: about 1 in 20.

## 6. Exposure, files, constraints

**Read (documents):**
- `docs/alpha-discovery-orchestrator-prompt-2026-10-13.md`;
- `research/copytrade/registrations/013-new-perp-listings.md` (all of it, including the Result §), `000n`, `000i`;
- `reviews/013-review-a.md` (all) and `013-review-b.md` (all);
- `013-review-r1.md` (grep lines on squeeze, margin, Kelly and T2);
- `scouts/n3-listings.md`, `scouts/n3_power.py`;
- code: `ct/listings013.py`, `r013_run.py` (the power and schedule sections).

**Read (data):**
- `data/r013/schedule.json`: first-trade times and the premarket / continuation flags, all three calendars. These are pre-entry facts and 013's power input. Used as event times in synthetic draws. **No count of any pre-entry fact was made on the confirmation calendar.** The T2 share was taken from 013's discovery count.
- `data/r013/MANIFEST.json`: hashes only.
- `data/binance_years/panel` (009 panel: `bars_1h`, `funding`, `life.json`, 2021-11 → 2024-01-07). **Post-entry quantities were computed on seasoned coin-weeks only:**
  - touch frequencies;
  - exit distributions conditional on a touch;
  - each rule's mean difference and SD.

  The rules are age ≥ 30 days, ≥ 7 days before the segment ends, BTC/ETH out. **Ledger this** as a new behaviour look on 009's discovery calendar (009 closed). It bears on any future seasoned-coin squeeze or stop rule over 2021-12 → 2024-01. It contains no listing window (days 0–29 are excluded).

**Not opened:**
- every `events.json`, `bars_1m.parquet`, `funding.parquet`, `market_1h.parquet` and `coverage.json`;
- `raw_*`, `plan.csv`, `logs/`;
- `results.jsonl`.

**Web.** Opened, all dated before 2026-07-01:
- Cointelegraph (2025-09-10, MYX);
- Rekt (2025-09-23, MYX);
- Forklog (2025-08-28, XPL);
- The Block (2025-03-26, JELLY);
- crypto.news (2025-05-01, ALPACA);
- SWoPEc (Kaminski & Lo, 2008).

Two fetches failed (MIT DSpace HTTP 405; Wayback blocked).

**Disclosure:**
- **one fetched page is dated after the cutoff:** Binance's mark-price FAQ, "last updated 2026-09-25". It is contract mechanics (mark = median of two index-based prices and the contract price), with no market data. Nothing here depends on it.
- **Search-engine summaries** (third-party snippets, dates not all verified) were also seen, including aggregator notes on Binance's dynamic funding intervals. They were not used.

**Files created:**
- `research/alpha/scouts/d1-listings-squeeze.md` (this file);
- `research/alpha/scouts/d1_seasoned_paths.py`, `d1_power.py`, `d1_kelly.py`;
- `research/alpha/data/scout_d1/derived/`: `seasoned_paths.txt`, `power_calib.json`, `power_k2.5_p0.04.json`, `power_k3.0_p0.066.json`, `power_k1.5_p0.066.json`, `kelly_tail.txt`, `SHA256SUMS`.
- There was no network pull.
- `d1_power.py` imports `ct.listings013` read-only and was run with `python -I -B`, so no bytecode was written. `research/copytrade` was unchanged afterwards (`git status`, `find -newer`).

**Forbidden conditionings, honoured:**
- No post-entry quantity on any listing window (confirmation, discovery or 2020–23).
- No count split by a future quantity or by position state.
- No pre-entry count on the confirmation calendar.
- Nothing from after 2026-07-01 except the disclosed docs page.
- No file under `research/copytrade/` created or modified.
- No `results.jsonl` written; no Neon query.
- No commit; nothing published.
- No paid, keyed or access-controlled source.

## 7. Sources (opened)

- Kaminski, K. & Lo, A. W., "When Do Stop-Loss Rules Stop Losses?", SIFR Research Report 63 (2008); J. Financial Markets 18 (2014). https://swopec.hhs.se/sifrwp/abs/sifrwp0063.htm
- Cointelegraph, "What is MYX Finance, why is it up 1,400%" (2025-09-10). https://cointelegraph.com/markets/what-is-myx-finance-why-is-it-up-1-400-percent
- Rekt, "Parabolic Mirage" (2025-09-23). https://rekt.news/parabolic-mirage
- Forklog, "Sudden surge in XPL premarket price leads to $17 million in liquidations" (2025-08-28). https://forklog.com/en/news/sudden-surge-in-xpl-premarket-price-leads-to-17-million-in-liquidations
- The Block, "Hyperliquid delists JELLYJELLY memecoin amid whale manipulation fiasco" (2025-03-26). https://www.theblock.co/post/348314/hyperliquid-delists-jellyjelly-memecoin-amid-whale-manipulation-fiasco
- crypto.news, "ALPACA token surges amid Binance delisting news" (2025-05-01). https://crypto.news/alpaca-token-surges-amid-binance-delisting-news/
- Binance FAQ, mark price (**last updated 2026-09-25, after the cutoff; disclosed; not relied on**). https://www.binance.com/en/support/faq/how-to-calculate-mark-price-in-binance-futures-360033525071
