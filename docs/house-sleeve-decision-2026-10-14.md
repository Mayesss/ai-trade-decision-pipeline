# House sleeve: HLP + Lighter LLP — allocation decision

2026-10-14. Owner decision memo, **revised after an independent review**
(`research/alpha/registrations/reviews/h1-house-sleeve-review.md`, verdict on
the first draft: *unsound as written, fixable*). Ledger entry `000v`
(written before any read). This is an **allocation with a priced tail, not an
alpha claim**; the effective N is about 3 spiral events (A1 §3.1).

Sources:
- statistics: `research/alpha/scouts/h1_analyze.py` on the pull
  `research/alpha/data/scout_h1/pull_20261014b/` (output `analysis.json`);
- terms, incidents and security model:
  `research/alpha/scouts/h1-house-sleeve-terms.md` (every fact linked);
- background: `research/alpha/scouts/a1-house-side.md`,
  `docs/hlp-benchmark-2026-10-10.md`.

Base currency is assumed to be **EUR**. **rf** means the owner's cash
alternative, assumed **2–3%/yr**; plug in the real rate.

## 1. Decision

**LLP: no.** Its carry since the LIT gate comes mostly from the perp and
backstop book, at about **6%/yr**. The gate and its hedge dilute that to
**5–6% on all the capital the leg needs**. That is 2–4 points over cash,
against an estimated venue tail of **3.5–7 points a year** (5–10% odds of
losing about 70%). Kelly on the excess over cash is **0**. The first
draft's "+11.8%" was flattered by a three-week spot-trading burst.

**HLP: optional, at most 6% of capital, and only as an explicit bet that
spirals continue.**
- Its calm-month carry (**+2.1%/yr** over the last 6 months) is below cash.
- Over two years it made +16.6%/yr (about 15% flow-adjusted), but only
  **+6.2%** without its three best fortnights.
- If you expect spiral years to keep coming (r ≈ 6.5–8%, p ≈ 3–5%), Kelly
  over cash is 0.36–1.4. Half of the low end is ~18%; the venue cap and the
  frozen-if-halted exit argue for much less.
- If you don't expect spirals, Kelly is 0, so hold cash.

**Default if unsure: cash.** Neither vault clearly pays for its tail. The
whole sleeve as first drafted (32% of capital) had an expected excess over
cash of about **−1% to +1% of capital a year** after the tail (review S2).

## 2. What changed from the first draft

| first draft | corrected | why |
|---|---|---|
| LLP carry ≈ 10–12% (share price since gate +11.8%, last 3 months +19.2%) | **perp/backstop book ≈ 6.0%/yr** since the gate; 3.5% in the last quarter; 12.3% in the last 5 weeks | A spot book started 2026-08-13. It made +$2.3M in three weeks (+44%/yr on NAV) while the perp book lost −$1.2M, then went flat. A burst is not carry |
| hedge funding +11–12.5% | **Binance +7.7%, Bitget +9.2%** (since 2026-07; last 30 days 6.7% / 9.3%) | The 11–12.5% was Hyperliquid's funding; the hedge was never meant to sit there |
| Kelly on gross return | **Kelly on return over cash** | The alternative isn't zero |
| 1× margin, weekly check | **2× margin, or a daily price check** | LIT rallied +72% in 7 days and +139% in 30 (intraday highs); 1× liquidates near +95% |
| expected +2.5–3% of capital/yr | **−1% to +1% over cash** after the tail | the tail and cash were left out |

## 3. The numbers

Annualised. LLP is a daily share price that reconciles with `/pnl` NAV once
spot P&L is included. HLP is chained ΔPnL / account value on the API's 13–14-day
grid. That grid **hides intra-fortnight drawdowns**: JELLY (2025-03, about
−$12–13M unrealised) does not appear, so HLP's "max DD" is a grid statistic.

| series | span | return/yr | vol | max DD | ex-top-3 |
|---|---|---|---|---|---|
| LLP share price since the gate | 2026-01-14 → 10-13 | +11.8% | 1.6% | −0.5% | +9.8% (days) |
| **LLP perp/backstop book only** | 2026-01-14 → now | **+6.0%** | | | |
| LLP perp book, first half 2026 | 01-14 → 07-14 | +6.5% | | | |
| LLP perp book, last quarter | 07-14 → now | +3.5% (spot +12.6%) | | | |
| LLP 2025 (beta, small TVL) | 2025 | +252% | 26% | −8.9% | not repeatable |
| **HLP, 2 years** | 2024-10 → 2026-10 | **+16.6%** (≈15% flow-adj.) | 8.5% | −0.8% (grid) | **+6.2%** |
| HLP since the gate | 2026-01 → 10 | +10.8% | 8.0% | −0.5% (grid) | **+0.3%** |
| **HLP calm, last 6 months** | 2026-04 → 10 | **+2.1%** | 0.6% | 0.0% | |

The LLP perp-book rows are dollar-weighted (P&L ÷ average NAV), which is
lower than a time-weighted rate: NAV was $515M in January and fell to about
$77M. Time-weighted, the perp book is roughly 7%/yr. Either way, single
digits.

- **LLP's tail:** −5.3% in a day in the 2025-10-11 outage; worst 7 days −5.0%;
  worst 30 days −3.0%.
- **Correlation:** the fortnightly correlation is −0.18 since 2025-01 and
  +0.37 since the gate. In 2025-10 the two vaults moved opposite (HLP +9.7%,
  LLP −3.8%).

## 4. The LLP leg, priced in full

Per $1 of USDC in LLP:
- stake 0.1 LIT, which is **$0.33** at $3.26;
- staking pays 6%/yr in LIT;
- an off-venue short of equal size, with margin;
- about 0.3%/yr in trading costs.

Return on all the capital the leg needs:

| perp carry | funding | 1× margin (capital $1.65) | 2× margin (capital $1.98) |
|---|---|---|---|
| 3.5% | 7% | 4.5% | 3.8% |
| **6%** | **7%** | **6.0%** | **5.0%** |
| 6% | 10% | 6.6% | 5.5% |
| 10% | 7% | 8.4% | 7.0% |

Less rf 2–3%, the central case leaves **+2–4%**. The estimated tail costs
p × L = 5–10% × 70% = **3.5–7%/yr**. So the leg does not pay.

It would pay if the perp carry ran at **≥ 12%/yr** with funding ≥ 7%. That
gives about 8% on the leg, roughly 5 points over cash, enough to cover the
tail at the low end of p.

Other LLP-specific weights:
- the 1:10 gate is counted in LIT, so Lighter exposure grows when LIT rises;
- insider unlocks (about 3.2M LIT a week from 2026-12-27) expand gate
  capacity, and NAV already rose 44% in 30 days, which dilutes the carry;
- Lighter contract upgrades take effect with no delay;
- whether staked LIT is covered by the escape hatch is unverified.

## 5. HLP, if you take the option

| | |
|---|---|
| **What you hold** | A share of the backstop. It earns when liquidation cascades overwhelm the book, about 1–3 fortnights a year; between them about 2%/yr. HLP now also lends idle USDC in Hyperliquid's own Earn market. That is a mandate change, accepted here as the same venue's risk |
| **Size** | **≤ 6% of capital** (half the first draft). Kelly is 0 without spirals and 0.36–1.4 with them; the frozen-if-halted exit and the freeze rate argue for the low end |
| **Lock-up** | 4 days from your last deposit; each deposit resets it |
| **Tail** | **No exit if validators halt**: the vault and USDC stay frozen until they resume. The new USDC escrow can be upgraded by one key with no delay. Impairment p ≈ 3–5%/yr at about −50%, so **−3% of capital** at 6%. Add A1's freeze rate of about **20%/yr** (temporary, but illiquid while it lasts) |
| **Shared tails** | USDC depeg or a Circle freeze; one EU/MiCA access action covers both venues; FX: 6% of capital in USD is about ±0.45% of capital at 1σ |
| **Tax** | Likely taxable income in Germany. Check with an adviser before sizing |

## 6. Exit rules for an HLP position, decided now

Exit in full on any of these:
1. a loss **socialised to depositors outside the vault**, or one event
   costing HLP **more than 5% of TVL**;
2. a bridge, validator or USDC-escrow incident;
3. a mandate change that **moves losses onto depositors** or adds credit
   exposure **outside Hyperliquid** (Earn on HyperCore is accepted);
4. HLP's index falls **−10%** from its high (`h1_monitor.py`).

**Review the same day, don't exit automatically:** one event costing
2–5% of TVL; TVL down more than 50% in 30 days.

Routine absorbed losses (whale manipulation of a thin market, under 1% of
TVL: ETH whale 2025-03, POPCAT 2025-11, FARTCOIN 2026-04) are **not**
exits. They're the job HLP is paid for.

**Weekly:** `python3 -I -S research/alpha/scouts/h1_monitor.py` plus a news
check.

## 7. What would reopen LLP

`h1_monitor.py` prints the trailing perp-book carry. Reopen if **both**:
- LLP's perp/backstop carry stays **≥ 12%/yr over two consecutive
  quarters**, without a spot burst carrying it;
- the gate's capital cost falls (gate removed or loosened, or LIT much
  cheaper), or off-venue LIT funding stays ≥ 7%.

Then redo §4 with the new numbers before depositing.

## 8. Response to the review

| finding | disposition |
|---|---|
| M0 LLP carry is a spot burst (BLOCKER) | **Accepted and verified**: perp book 6.0%/yr since the gate. §1, §3 and §4 rewritten |
| S1 Kelly gross, not over cash (BLOCKER) | **Accepted.** `kelly_excess` added; LLP Kelly 0 |
| H1 funding from the wrong venue | Accepted. Binance and Bitget funding pulled and used |
| H2 1× margin too thin; H3 LIT drift | Accepted. Moot for the decision (no LLP); kept in §4 for a reopen |
| S2/S3/O3 expected return ignores tail and cash | Accepted. §1 states the excess over cash |
| S4 freeze rate missing | Accepted (§5) |
| E1 exit rule 1 fires on routine losses; E2 HLP Earn mandate | Accepted. Materiality thresholds added; Earn accepted explicitly (§5, §6) |
| E3/E6 monitor reads HL funding, wrong comments, silent on empty fetch | Fixed. The monitor now watches HLP exits and LLP's reopen condition, and fails loudly on an empty fetch |
| O1 FX, O2 tax asymmetry | Stated (§5). The hedge tax question is moot without LLP |
| Minor: worst 30 days −3.0%; grid hides DD; ex-top-3 on mismatched grids; TVL $88.8M | Fixed or stated (§3) |

## 9. What this memo does not claim

- No forecast. Whether HLP's spirals continue is the whole bet, and the
  sample holds about 3 of them.
- No measured exploit or freeze rate. *p* is judgement from a small,
  survivor-biased sample.
- Both vaults are now read through 2026-10-14, so a future trial on either
  would be forward-only (ledger 000v).
