# Independent review of the house-sleeve memo (HLP + Lighter LLP), 2026-10-14

Reviewer brief: an adversarial review of `docs/house-sleeve-decision-2026-10-14.md`. Its inputs are ledger
`000v`, `scouts/h1_fetch.py`, `h1_analyze.py`, `h1_monitor.py`, the pull `data/scout_h1/pull_20261014/`, the
terms file `scouts/h1-house-sleeve-terms.md`, and the background `a1-house-side.md` and
`docs/hlp-benchmark-2026-10-10.md`. No existing file was modified. Nothing was published or uploaded, and no
key, login or paid source was used.

**What was run**

- `shasum -a 256` on the pull: all four raw files match `SHA256SUMS`.
- `python3 -I -S research/alpha/scouts/h1_analyze.py research/alpha/data/scout_h1/pull_20261014` to stdout:
  **byte-identical** to `analysis.json` after key-sorted comparison.
- `python3 -I -S research/alpha/scouts/h1_monitor.py`, once (2026-10-14 16:52 UTC): exit 0, every check `ok`.
- Fresh free public pulls into the session scratchpad, not the repo:
  - Lighter `/pnl` for LLP, 300 days and 35 days, plus `/account`;
  - Binance `fapi/v1/fundingRate` (500 rows), `openInterest` and `premiumIndex` for LITUSDT;
  - Bitget `v2/mix/market` `open-interest`, `ticker`, `history-fund-rate` (200 rows) and `contracts` for
    LITUSDT.
- My own recomputations from the pull: LIT rolling windows, a modified-Dietz HLP index, LLP rolling windows
  tolerant of timestamp shifts, the LLP perp/spot PnL split, and the Kelly closed form.

---

## Verdict: UNSOUND as written (fixable)

The numbers are faithfully transcribed. The script reproduces exactly, and every figure in the §3 table matches
`analysis.json`. The terms and incident facts match the terms file.

**The recommendation does not survive its own inputs once they are decomposed:**

- **LLP's carry is mostly a two-month-old spot book.** About 84% of LLP's PnL since 2026-07-14 came from
  `trade_spot_pnl`, the spot book that started 2026-08-13. Since 2026-08-13 the perp/backstop book made −1.9%/yr.
  The spot book made +$2.4 M in three weeks and has been flat since 2026-09-03.
- **The hedge's funding is measured on the wrong venue.** The memo quotes Hyperliquid's LIT funding. The
  memo's chosen hedge venues, Binance and Bitget, realised 6.7–9.1%/yr.
- **"Expected" leaves out both the priced tail and the cash alternative.**

Re-run with the measured perp-only carry, the venue's funding and a cash yield, the LLP Kelly fraction is **0**
under the memo's own pessimistic base rate. The tail-adjusted excess return of the whole sleeve is roughly
**−1% to +1% of capital a year**, against a −17% exploit tail. That may still be a choice the owner wants. But it
is not the "+2.5–3% a year" the memo presents.

---

## 1. Numbers checked against `analysis.json`, the terms file and recomputation

Every number in §2–§4 and §6 was checked. Matches are listed compactly; mismatches are findings.

**Match:**

- **§3 table, all rows:**
  - LLP since gate: 11.8 / 1.6 / −0.5 / −0.40 / 9.8;
  - LLP 3 months: 19.2 / 2.0 / −0.4 / −0.16 / 13.3;
  - LLP 2025: 252 / 26 / −8.9 / −6.7;
  - HLP 2 years: 16.6 / 8.5 / −0.8 / 6.2;
  - HLP since gate: 10.8 / 8.0 / −0.5 / +0.3;
  - HLP 6 months: 2.1 / 0.6 / 0.0;
  - 50/50: 11.3 / 4.4 / −0.1 / 3.9.
- **LLP:** −5.3% day (−5.35%), worst 7 days −5.0%, Sharpe 7–9 (7.3 and 9.4).
- **HLP:** 2026-01-21 → 02-04 +7.0%, about 90% of its 2026 (0.90 of log return since the gate; 0.87 of
  calendar 2026). Top-3 fortnights 61% of the 2-year return. Correlations −0.18 / +0.37. 2025-10: +9.7% /
  −3.8%. TVL $180 M; peak $604 M.
- **LIT:** $3.23, vol 128%, worst 30 days −55.4% (→ −18% of the deposit at the 0.32 ratio). Funding on HL
  11.0 / 12.4 / 12.6%/yr, 92% of hours positive. Unlocks 3.2 M a week = 1.3% of 250 M.
- **Kelly table:** all five rows match, and the solver matches the closed form f* = (1−p)/L − p/r.
- **§4 return arithmetic:** 1.64 and 10.5% (= (0.118 + 0.32·0.06 + 0.32·0.11)/1.64), 6.05%.
- **§1:** −11% and −6% tails; 2.0% + 0.5–1.0% = 2.5–3.0%.
- **Terms facts:** HLP −$1.8 M MM / +$58.8 M backstop (benchmark §4, archive year 2025-08-06 → 2026-07-08).
- **LLP $515 M before the gate:** my `/pnl` NAV on 2026-01-13 is $515.1 M.
- **OI, verified fresh:** Bitget 1.375 M LIT ≈ **$4.5 M**; Binance 15.2 M LIT ≈ **$49.6 M**.

**Mismatches and mislabels:**

| # | sev | finding | fix |
|---|---|---|---|
| N1 | MINOR | **LIT "−79% (2025-12-30 → 2026-03-31)" mixes two sources.** The −79.2% is on HL perp closes from **2025-12-22** (the first candle) to 2026-03-30. The dates are CoinGecko's ATH/ATL, and CoinGecko's ATH $7.86 → ATL $0.78 is **−90%**. | Quote one source: "−79% on HL closes 2025-12-22 → 2026-03-30 (−90% ATH→ATL, CoinGecko)". |
| N2 | MINOR | **"+139% best 30-day rally" is not in `analysis.json`.** It is the intraday high against the close 30 days earlier; close-to-close it is +120.5%. LIT's **2026-03-30 → 2026-09-23 run was +557%** ($0.81 → $5.31 close), and the memo never mentions it. | Label it intraday, add the script output, and state the 6-month run (it drives §3 below). |
| N3 | MINOR | **"LLP $86M" is `total_asset_value`, which excludes the spot inventory.** Share price × shares, and the monitor's `/pnl` NAV, give **$88.8 M**. The $2.5 M gap equals cumulative `trade_spot_pnl`. | Quote $88.8 M (share-price NAV) and footnote the TAV. |
| N4 | MINOR | **§8: "Arbitrum route about 0.20 USDC" drops the terms file's NV flag.** The figure comes from Circle's sandbox example. | Restore "(NV)". |

## 2. Method review of `h1_analyze.py`

| # | sev | finding | fix |
|---|---|---|---|
| **M0** | **BLOCKER** | **LLP's return is not one strategy any more, and the script does not decompose it.** `/pnl` splits `trade_pnl` (perps) from `trade_spot_pnl` (spot, non-zero from 2026-08-13). Dollar PnL over average NAV: <br>• **since gate:** perp **5.8%/yr**, spot 2.6% (total 8.4%; the time-weighted share price gives 11.8%); <br>• **since 2026-04-14:** perp 4.6%, spot 5.9%; <br>• **since 2026-07-14:** perp **2.5%**, spot 13.6% (84% of PnL). <br>• **Since 2026-08-13, chained:** perp **−1.9%/yr**, spot +22.5%. <br>The spot leg made **+$2.4 M from 08-13 to 09-03, then went flat**: one burst, not a carry. The assets show what the spot book holds: ETH, XAUT, rhSPY and rhQQQ, which are tokenized gold and US equities. The "LLP last 3 months +19.2%" row is therefore mostly this burst. The "since gate +11.8%" headline that drives r ≈ 10% in §4 and §6 is about half spot. | Add a perp/spot split to `h1_analyze.py` from `/pnl` (this pull has no `/pnl`; add it to `h1_fetch.py`). Use the **perp-only carry (≈5%/yr since the gate, ≈2.5% in the last quarter)** as r in §4 and §6. Treat the spot book as an unmeasured new strategy with its own (unknown) risk, and state what it holds. |
| M1 | MINOR | **HLP index flow bias.** dPnL / AV(start) ignores intra-period flows; the 2024-12-11 period had a +72%-of-AV inflow. A modified-Dietz denominator (AV0 + ½·net flow) gives 2-year CAGR **15.2%** (memo 16.6%), since-gate **10.3%** (memo 10.8%) and 6 months 2.2% (2.1%). | Use modified Dietz, or footnote "+0.5 to +1.4 pp/yr flow bias". |
| M2 | MINOR | **The 13–14-day grid hides intra-period drawdowns.** JELLY (unrealised −$12–13.5 M) does not appear; the 2025-03-26 period reads +0.35%. HLP's "max DD −0.8%" is a grid statistic, not a depositor drawdown. | Say so in §3. For the post-2025-07 span, use the daily H7 reconstruction already cached. |
| M3 | MINOR | **ex-top-3 is not comparable across grids.** Removing 3 of 272 LLP days is not the same as removing 3 of 20 HLP fortnights. On the common fortnight grid, LLP since-gate ex-top-3 is **7.5%** (`basket_since_gate_hlp0`), not 9.8%. The HLP-vs-LLP contrast still holds. | Report ex-top-3 for both on the common grid. |
| M4 | MINOR | **`worst_rolling` needs an exact timestamp match.** LLP timestamps move between 20:00 and 21:00 UTC (3 shifts), so windows crossing a shift are dropped: 21 of 629 7-day windows and **90 of 606 30-day windows**. On the index-based grid, worst 30 days is **−3.0% (2025-11-06)**, not −2.5%. Not quoted in the memo. | Match on the nearest point within ±2 h, or index by row. |
| M5 | MINOR | **Correlations are noise.** n = 47 and n = 20 give SE ≈ 0.15 and 0.22; neither −0.18 nor +0.37 differs from 0. The case for holding both rests on **one** event (2025-10). | Say "one event; correlation not measurable". |
| — | ok | **Checked and found correct:** <br>• **LLP share price validity:** share price × `pool_total_shares` reproduces `/pnl` NAV (inflow − outflow + trade_pnl + **trade_spot_pnl**) to <0.1% on matched days. Daily-return reconciliation: median abs error 1.5e-4 with spot, 1.9e-4 without; p90 6.6e-4 vs 1.4e-3. **The orchestrator's reconciliation claim holds.** The 2026-03-09 −0.40% day is a real trade loss (−$0.67 M on $168 M), not an artifact. <br>• **Vol annualisation:** the HLP grid is near-regular at 13–14 days; LLP is daily, with 3 one-hour shifts. <br>• **`align()`:** correct. The last joint period is about 20 h mis-timed, which is immaterial. <br>• **Kelly solver:** matches the closed form. <br>• **`stats()` ex-top-3:** equivalent to zeroing the top periods while keeping elapsed time, which is acceptable. | — |

## 3. LIT hedge economics (memo §4)

| # | sev | finding | fix |
|---|---|---|---|
| **H1** | **MAJOR** | **Funding is quoted from the wrong venue.** "+11–12.5%/yr, 92% of hours positive" is **Hyperliquid's** LIT funding; the memo puts the hedge on Bitget or Binance. Realised, from fresh pulls: <br>• **Binance:** **7.7%/yr** over 83 days, **6.7%** over 30 days, 89% positive; 54 negative prints, first 2026-08-13. <br>• **Bitget:** 9.1% over 33 days. <br>Return per $1 of LLP: memo carry + Binance funding = **9.9%**, not 10.5%. **Perp-only carry (M0) + Binance funding = 5.7%**; last-quarter perp carry gives 4.2%. | Fetch the hedge venue's funding history in `h1_fetch.py`. Restate §4 with r_LLP = perp-only carry and venue funding. |
| **H2** | **MAJOR** | **1× isolated margin with weekly checks does not survive LIT's own history.** <br>• A 1× isolated short liquidates near **+95–100%**. LIT's intraday-high rallies over the prior close: 7 days **+72%**, 14 days **+83%**, 30 days **+139%**; March → September +557%. <br>• The "+40% → add margin" review only runs weekly (§7 Routine), and **the monitor does not fetch the LIT price at all**. A +39% drift (no alert), then a +72% week, is +139%: liquidated. <br>• Cost: liquidation takes the margin (**−4% of capital**) while the stake's gain is unrealised and now unhedged. LIT then reverting, which it has done (−79%), makes it a realised loss of about **−4% of capital**, more than the whole sleeve's annual carry. | Either 2× notional margin (capital per $1 → 1.96; return 8.3% on memo inputs, 4.8% on perp-only inputs), or a **daily** automated LIT price check (+25% → same-day top-up). Name the top-up source: LLP USDC has a 5-minute cooldown, while the stake does not (3 days). Add the LIT price check to `h1_monitor.py`, with the last-rebalance price as an argument. |
| **H3** | **MAJOR** | **The gate is a LIT *count*, so Lighter exposure drifts with the LIT price.** "No venue above 16%" holds only at $3.23. At the 2026-09-23 close of $5.31, Lighter is 12% + 6.6% = **18.6%**; at the $7.86 ATH, **21.7%**, inside A1's 20–25% ceiling. Margin need grows by the same factor. A quarterly rebalance is too slow for a 128%-vol token. | Add a band rule. When LIT moves ±30% from the last rebalance: <br>• withdraw LLP USDC to restore CEX margin; <br>• unstake the LIT the smaller deposit no longer needs (3 days); <br>• close the matching short. <br>This restores both the 16% cap and the margin in one action. |
| H4 | MINOR | **Exit sequencing and stake liquidity are unstated.** <br>• Exiting Lighter means LLP burn (5 min), then unstake (3 days), then sell LIT; the short must stay on until the LIT is sold. <br>• The terms file lists desert-mode exits for withdrawals, **pool share burns** and reduce-only orders. Staked LIT (staking pool account 281474976624800) is not listed, so in a Lighter halt **the stake may have no escape hatch** (NV). | Write the 4-step exit order into §7. Add "desert-mode coverage of staked LIT" to Not verified, and size the stake as possibly −100% in a halt. |
| H5 | MINOR | **"Preferably not on Hyperliquid" is weakly argued, and no fallback is chosen.** <br>• An HL failure does not move LIT much, so wrong-way risk there is small. HL+hedge = 12% + 4% = 16%, within the cap. <br>• HL has the deepest LIT book ($150 M OI) and the highest funding. <br>• Bitget is thin: $4.5 M OI, $1.1 M 24-hour volume, which is squeeze-prone. <br>• Perp access for a German-resident retail account on Binance or Bitget is **not verified**; several CEXs restrict perps for EU retail. | Make step 1 verify CEX perp access. Pre-decide the fallback: HL LIT perp, or Bybit/OKX. |
| H6 | MINOR | **Costs and supply effects are missing.** <br>• Perp fees: Bitget taker 0.06% per side, plus a monthly re-hedge. <br>• LIT spread on entry and exit, and the USDC→USDT conversion for margin: about 0.1–0.3%/yr all in. <br>• Unlocks (~167 M LIT/yr, about 67% of today's float) also **expand gate capacity**, so LLP crowding protection weakens. LLP NAV already rose **+44% in 30 days** ($61.5 M on 2026-09-10 → $88.8 M), which dilutes per-$ carry. | Add the costs line. Add a review trigger for **TVL growth** (for example >2× from entry), not only for falls. |

## 4. Sizing (memo §6)

| # | sev | finding | fix |
|---|---|---|---|
| **S1** | **BLOCKER** | **Under corrected inputs, the sizing rule gives zero for LLP.** <br>• With r = perp-only leg return 5.7% (M0 + H1) and L = 70%: **p = 10% → full Kelly 0**; p = 5% → 0.48. <br>• Kelly must also be on **excess** return over the owner's cash alternative. With r − rf at rf = 3%: memo's r = 10% gives 0.59 at p = 5% and **0 at p = 10%**; corrected r gives **0 at both**. <br>• "Half the pessimistic case ≈ 15%" therefore becomes **0**. | Redo the §6 table with r = perp-only carry, venue funding and r − rf (state the rf the owner actually gets). Size LLP on that, or say plainly that the 16% is a judgement against a Kelly of zero. |
| **S2** | **MAJOR** | **"Expected +2.5–3%/yr on total capital" is carry with no default.** <br>• Tail cost at the memo's own p and L: Lighter 16% × (5–10%) × 70% = **0.56–1.12%**; HL 12% × (3–5%) × 50% = **0.18–0.30%**. <br>• Corrected carry: LLP leg 20% × 5.7% = 1.14%; HLP 12% × 4–8% = 0.48–0.96%. <br>• Tail-adjusted: **≈0.2–1.4% of capital**. Cash on the same 32% at rf 2–4% would earn **0.6–1.3%**. <br>• **Excess ≈ −1.1% to +0.8% of capital/yr**, before tax and FX. | Put this line in §1, beside the −11% / −6% tails. |
| **S3** | **MAJOR** | **The "worst plausible year without an exploit: −1%" is too rosy.** <br>• The memo's own exit rule 4 lets each venue fall 10% before exiting: 16% × 10% + 12% × 10% = **−2.8% of capital**, all within the rules. <br>• H2 adds a hedge-liquidation-and-reversal path of about −4%. <br>• LLP's own history has −8.9% (2025-03, beta era) and a −3.0% 30-day window. | Restate the line as "−3 to −5% of capital (rules-permitted drawdown + hedge path)". |
| S4 | MAJOR | **The base rate leaves out freezes.** A1 §2.3 gives p ≈ 5–10%/yr impairment **plus ~20%/yr for a freeze**. A Lighter freeze strands the LLP and the stake (H4) while the CEX short stays live and may need margin. The HL freeze case ("Exit if halted: None") likewise needs cash outside the sleeve. | Add the freeze rate to §5 and a "keep X% liquid outside the sleeve for margin during a freeze" line. |
| S5 | MINOR | **HLP "12% is about half Kelly on calm carry" uses 3%; the measured calm rate is 2.1%** (last 6 months; benchmark: +1.9%/yr in non-event fortnights). At r = 2.1%, p = 5%, L = 50% → Kelly **0**; p = 3% → 0.51. HLP's calm carry is **below cash**, so the leg is justified only by spirals (2-year ex-top-3 6.2%, Dietz-adjusted lower). | Say so explicitly. The HLP leg is a paid option on spirals, priced at (rf − 2%) a year. |
| S6 | MINOR | **"16% sits at half the pessimistic Kelly" is generous: half of 0.29 is 14.3%.** At entry, 16% is consistent with the table (12% LLP + 4% stake). It is not consistent after LIT drift (H3). | Use 14%, or cite H3's band rule. |

## 5. Exit rules (§7) and `h1_monitor.py`

| # | sev | finding | fix |
|---|---|---|---|
| **E1** | **MAJOR** | **Rule 1 fires on routine events.** Any "exploit or oracle incident that costs depositors money" includes: <br>• HLP: ETH whale (−$4 M, 2025-03), POPCAT (−$4.9 M, 2025-11), FARTCOIN (−$1.2–1.5 M, 2026-04), all under 1% of TVL and called "absorbed" in §5; <br>• LLP: ARC (−$75 k). <br>That is about 2 a year on HLP. The rule would have forced exits from HLP repeatedly, after the loss and before the spiral payoff the leg exists for. | Add materiality: a loss above 2% of vault TVL, any loss passed to depositors outside the vault (socialised), or any bridge, validator or sequencer event. |
| **E2** | **MAJOR** | **Rule 2 has, arguably, already fired on HLP and is ambiguous for LLP.** <br>• HLP's mandate now includes **lending USDC in Earn** (new since H7; a new credit channel). Its fee share is reportedly cut to ~1% (NV). The 2-year history was earned under the old mandate. <br>• LLP changes buckets routinely (3 → 8; ARC moved to a ~$100 k bucket). | Decide now: accept HLP's Earn lending explicitly, or don't enter. Narrow rule 2 to changes that **weaken** segregation or move losses onto depositors. Add HLP's Earn exposure to §2 and §5. |
| E3 | MAJOR | **The monitor does not match the memo:** <br>• **LIT funding is read from Hyperliquid, not the hedge venue** (prints +12.6%; Binance 30-day is +6.7%); <br>• **no LIT-price/margin check** (the memo's "+40% → add margin" review is unimplemented; see H2); <br>• it has an **extra hard exit** ("HLP accepts deposits / not closed") that §7 does not list. | Add a Binance/Bitget funding fetch and a LIT-price-vs-rebalance check. Either add "HLP closed/deposits off" to §7 or make it an alert. |
| E4 | MINOR | **A failed fetch can pass silently.** With an empty funding page, `rates` is empty → 0.0 → `ok`. `llp pool status` is printed but never checked. | Treat empty or failed fetches as `ALERT`; check `status != 0`. |
| E5 | MINOR | **HLP drawdown on the `allTime` ~14-day grid understates the peak**, because intra-fortnight highs are missed. The last point is live, so the current value is fine. | Take max(peak) across the `allTime`, `month` and `week` grids. |
| E6 | MINOR | **Two comments are wrong.** The docstring cites "§6" for the triggers, which are in §7. `LLP_DAY_ALERT` says "2x the post-gate worst week", but the post-gate worst week is −0.39%, so 2× is −0.78%, not −2%. | Fix the comments, or pick −0.8% deliberately. |
| E7 | MINOR | **Rules 3 and 4 cannot act in time for jump risk.** Lighter upgrades are instant, and several verifiers were deployed without published sources, so "an upgrade that weakens the hatch" cannot be detected before it applies. A −10% drawdown exit triggers only after the loss. | State that these are post-event rules, not protection. Optionally check the Pause/Emergency Guardian addresses through a free public Ethereum RPC. |
| — | ok | **LLP TVL ~30-day check is correct.** The monitor's +35.3% is real (NAV $65.6 M on 09-15 → $88.8 M), and both `/pnl` pulls agree on every overlapping day. HLP TVL change from the `month` grid: −3.9%. Ran once: exit 0, no trigger. | — |

## 6. Omissions that could change the owner's decision

| # | sev | finding | fix |
|---|---|---|---|
| **O1** | **MAJOR** | **Currency.** The owner is Germany-based and the repo reports costs in EUR. 32% of capital in USDC carries EUR/USD risk of roughly 7–8%/yr vol, about **±2.4% of total capital at 1σ**. That is as large as the whole sleeve's claimed carry. | State the base currency. Either hedge FX or count it in the sleeve's risk. |
| **O2** | **MAJOR** | **Tax asymmetry in the hedge** (likely, to be confirmed by an adviser; not verified here). The stake (private-sale rules for held tokens) and the perp short (derivative or other income) may fall into different tax baskets. Hedge gains could then be taxed while stake losses are stranded, or the reverse. Staking rewards and LLP returns are likely taxed as income on receipt. A hedged position can therefore owe tax in a flat year. | Before step 2, add a paragraph naming this question for the adviser: same-basket offset of stake vs short, and the treatment of LLP share-price gains. |
| O3 | MAJOR | **Opportunity cost is absent** (see S1/S2). The script itself uses rf = 4% (`sharpe_ex4`). The memo never compares to cash or USDC lending. | Add an rf column to §1 and §6. |
| O4 | MINOR | **HLP's TVL collapse cuts both ways.** At $180 M (from $604 M), the same liquidation flow is a larger position per $ of HLP. Spiral gains and spiral losses per $ both grow. The memo reads only the upside. | One line in §2. |
| O5 | MINOR | **Common-mode risk across "two venues".** Both legs are USDC: a depeg or a Circle-side freeze, including the CCTP escrow on HL, hits 28% of capital at once. Both are crypto-perp venues exposed to one EU/MiCA access action. In a broad crash, LLP has historically had the outage (and LIT drops, so the short pays), while the CEX may ADL the profitable short and leave the stake unhedged. | List these as shared tails in §5. |
| O6 | MINOR | **Operational.** <br>• Wallet and key hygiene: the HL Google-ads phishing incident is in the record. <br>• Separate the hot wallet from the cold one. <br>• The 4-day HLP lock resets on every deposit, so a tranche schedule extends it. | Add to §8. Note the tranche/lock interaction. |
| O7 | MINOR | **The LLP spot book holds tokenized equities and gold** (rhSPY, rhQQQ, XAUT): new issuer and RWA risk since 2026-08. The memo says only "spot". | One line in §2. |

---

## Required fixes (to reach SOUND WITH FIXES)

1. **M0/S1:** decompose LLP returns into perp and spot from `/pnl`. Use the perp-only carry (~5%/yr since the gate,
   ~2.5% in the last quarter) as r. Redo §4 and the §6 Kelly table on excess return over the owner's rf, then re-decide the LLP size.
2. **H1/E3:** take funding from the hedge venue (Binance 6.7–7.7%, Bitget ~9%) in the memo, `h1_fetch.py` and
   `h1_monitor.py`.
3. **H2:** raise margin to about 2× notional, or add a daily LIT-price check with a named same-day top-up source.
   Implement the LIT check in the monitor.
4. **H3:** add a LIT-drift band rule that keeps Lighter at or below 16% and restores margin in one action.
5. **S2/S3/O3:** restate §1. "Expected" must net the priced tail and compare against cash. Use about −3 to −5% for
   the worst non-exploit year.
6. **E1/E2:** add a materiality threshold to exit rule 1. Decide now on HLP's Earn lending and narrow rule 2.
7. **S4/H4:** add A1's ~20%/yr freeze rate, the 4-step Lighter exit order, and the NV on staked-LIT desert-mode coverage.
8. **O1/O2:** state the base currency and the FX choice. Add the hedge tax-asymmetry question for the adviser before step 2.
9. **Minor:** N1–N4, M1–M5, H5–H6, S5–S6, E4–E7, O4–O7 as listed.

Step 1 of the memo ($500 per venue, full mechanical cycle) is low-risk and informative under any outcome of these
fixes, including the CEX perp-access check (H5). It can proceed before the rewrite.
