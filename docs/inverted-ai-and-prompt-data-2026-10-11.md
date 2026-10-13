# Would trading against the AI have won? And is there alpha in the prompt data?

**2026-10-11, exploratory. Not a registration and not a trial.** These are the
owner's two questions after the live trader was switched off (2026-10-10):

1. The AI always took the obvious, logical trade, the one other traders (and
   their AIs) probably took too, maybe earlier. Was that side crowded and
   already used up, so that the opposite side would have won?
2. Leaving the decisions aside, can a quant edge be found in the data that was
   collected and put into the prompts?

**Short answers.**
1. **On paper, partly yes. As an edge, no.** Inverted, the AI earned
   +0.95 ATR per trade net at 48 h. But that rests on about 15 independent
   weeks. It is just as large when the AI itself was fading the move. And the
   mechanical version of "fade the obvious move" lost money net over 18
   months before the AI ever traded. What inverts is three particular months,
   not crowding.
2. **Not from Neon.** Everything the prompts saw except the Perplexity digest
   has years of public history elsewhere. Neon holds 3.5 months of it. The
   bar-derived features were already swept on 115 symbols (2021–2026, with a
   holdout) on 2026-09-14, and none survived costs. Your crowding thesis does
   have a well-powered test, and it is already drafted: **009 F2 (leverage
   crowding), on four years of Binance data.**

Reproduce with `node .study/36-inverted-ai.cjs "$PWD"`. It reads only
`.study/panel.json` and `.study/candles-deep.json`, with zero Neon reads.

---

## 1. Neon data-transfer check (done before any pull)

| | |
|---|---|
| Transfer in October so far | ~25–40 MB/day; ~250 MB for 10-01 → 10-10 (consumption API) |
| Allowance (spec §5) | 500 GB/month |
| The whole database | ~160 MB on disk; `swing.decisions` 101 MB, 26.5k rows |
| Directional decisions, ever | **512** (2026-06-27 → 2026-10-10): 17 / 252 / 11 / 220 / 12 by month |
| Already cached locally | 453 (dump of 2026-09-11) |

Even a full dump is about 0.03% of the allowance, so transfer is not the
constraint. The ~€400 incident was bulk bars, and nothing here needs bars from
Neon. The only query run was catalogue sizing (`BEGIN READ ONLY`, unpooled
URL, a few KB). **I did not pull the ~60 directional decisions made after
2026-09-11.** They are about 4 weeks of data, so even a perfect result would
read t ≈ 1. They cannot change the verdict below. The pull costs under 1 MB
if you want it anyway.

## 2. Question 1: the inverted AI

Outcome: the forward return over `h` in units of the decision's own 4H ATR,
with the sign flipped (the opposite trade). Costs are a rough round trip of
12 bp on Bitget and 10 bp on Capital, converted to ATR per decision. There is
no stop and no target, so this is a pure forward return.

| horizon | n | inverted, gross | inverted, net | net % of price per trade |
|---|---|---|---|---|
| 4 h | 453 | +0.051 ATR | **−0.116** | −0.08% |
| 8 h | 450 | +0.165 | −0.002 | −0.02% |
| 24 h | 435 | +0.312 | +0.146 | +0.17% |
| 48 h | 411 | +1.113 | **+0.949** | +0.82% |

At the AI's actual holding period (median 3.8 h), inverting loses after costs.
The paper gain appears only at 24–48 h. Three facts stop it from being an edge:

**(a) Its significance depends on how it is counted.** The 48 h result has
t = 2.7 clustered by day and t = 1.8 clustered by calendar week. 25
instruments share one macro tape, and the 48 h windows overlap, so the
week-clustered t is the honest one. It is not significant, and the 2026-09-11
audit had already run 16 tests on this sample.

**(b) It is not crowding.** If the gain came from the AI chasing a crowded
move, inversion would pay when the AI chased and not when it faded. It pays
about the same either way:

| 48 h, net | n | mean (ATR) | t (week) |
|---|---|---|---|
| inverted AI, all | 411 | +0.949 | 1.81 |
| … where the AI traded *with* the last 24 h move | 349 | +0.963 | 1.49 |
| … where the AI traded *against* it | 62 | +0.872 | 1.84 |
| mechanical "fade the last 24 h move", same timestamps | 411 | +0.627 | 0.85 |

The AI's direction was anti-correlated with what happened next in those
particular weeks, whatever the reason it gave. That is the signature of a
regime (mostly July, one engine version), not of a repeatable mechanism.

**(c) The mechanical version of your thesis loses.** "Take the opposite of the
obvious move" can be tested without the AI, on every bar the AI never saw
(2024-11 → 2026-06-26, the same 25 instruments, one entry a day):

| fade the last 24 h move | n | net (ATR) | t (week) |
|---|---|---|---|
| 24 h hold, all | 10,893 | −0.168 | −5.7 |
| 48 h hold, all | 10,893 | −0.174 | −4.4 |
| 48 h hold, crypto only | 5,331 | −0.151 | −2.3 |
| 48 h hold, Capital only | 5,562 | −0.197 | −4.8 |

Gross, that is roughly zero. Costs make it reliably negative. This matches
2026-09-14 (spec §8). In the cross-section, 24 h reversal shows in rank IC
(t ≈ 4, replicated on a holdout), yet a costed book loses gross. Most
pumped coins revert, but the few that keep running dominate the P&L. The
obvious move is neither reliably right nor reliably wrong. It is priced.

**Could the inverted AI be tested properly?** Only forward. Replaying the
LLM on past data is contaminated, because the model's training data contains
those prices and headlines. The audit's power calculation needs about 4,900
decisions to see a realistic 0.1 ATR edge at t > 3. At the last cadence (a
daily look on 9 symbols), that takes years and costs tokens every day. **Not
worth it.**

## 3. Question 2: the data that went into the prompts

| input | in Neon | public history elsewhere | already tested? |
|---|---|---|---|
| OHLCV bars, indicators (EMA, RSI, ATR, returns, structure) | snapshots, 3.5 months | years (Bitget, Binance from 2020, Capital) | **yes**: 44-signal sweep, 115 symbols, 2021-06 → 2026-09, holdout. 7 replicate on rank IC; all lose as a costed book (spec §8, 2026-09-14) |
| Perp funding | snapshot, Bitget | Binance/Bybit/OKX/HL from 2020–23 | **in flight**: 009 F1 (carry), 010 (cross-venue) |
| Fear & Greed index | daily | from 2018 (public) | no. A single market-wide daily number gives about 1 independent observation a week, so testing needs years. Low odds |
| News headlines + label | 5 per tick | CoinDesk / Marketaux archives | no. Headline effects decay in minutes, and our samples were taken at a 4H/daily cadence after the fact. Low odds |
| Perplexity digest | text, 3.5 months | none | no. Unstructured, too short to test, and an LLM re-scoring it would be contaminated |
| Event reactions (macro releases) | measured | rebuildable from calendar + bars | no. Post-release drift in majors is heavily arbitraged. Low odds |
| The AI's 26k HOLDs | yes | none | the audit flagged them as untested. But a HOLD is not a position, so whether it was "good" depends on a counterfactual the audit already found underpowered |

**Conclusion.** Neon's unique asset is the AI's own outputs, and §2 covers
those. Every other input has a deeper source outside Neon, so mining 3.5
months of prompt snapshots would be the least powerful version of a test that
can be run on years. That is why this note recommends no Neon pull.

## 4. Research steering

What the evidence so far keeps saying: **directional calls from public
information at hour-to-day horizons are priced.** That covers an LLM, the
mechanical fade, the 44-signal sweep and copying wallets. Edges that last at
our size come from **being paid to carry a risk or provide a service**, or
from **a constraint that keeps others out** (size, access, attention), not
from reading the same chart better.

**Keep the current order** (`docs/edge-discovery-2026-10-11.md` §1): L4/009
first, then L2/010, then L1/007, L5b, L3. In particular:

1. **Your crowding thesis is 009 F2.** "Crowded coins underperform": long the
   coins whose open interest grew least, short those whose grew most, weekly,
   Binance 2021-12 → 2023 discovery with a sealed holdout (~17k coin-weeks,
   detectable IC ~0.02–0.04). That is the version of your idea with real
   power. 009 also considered the retail long/short-ratio contrarian and
   folded it into F2 as the same mechanism. If you want the AI-specific angle
   ("everyone's AI chases the same thing"), the measurable footprint is in
   the positioning data (taker imbalance, retail L/S ratio, OI), not in our
   512 decisions.
2. **009 F1 (funding carry)** is the textbook structural premium: shorts get
   paid to hold crowded longs. That is the best-documented mechanism on the
   list.

**Two lights not yet on the list.** Both are structural premia, and both are
measurable for free before any spend. Proposed for the orchestrator to scout,
not started here, because another agent owns `research/`:

| light | mechanism | free first look | effect exists | deployable | warnings |
|---|---|---|---|---|---|
| **Crypto options volatility premium** (Deribit BTC/ETH: sell delta-hedged or defined-risk volatility) | option buyers pay for crash insurance; implied vol is widely reported to exceed realised most of the time | Deribit DVOL (public API, from 2021) vs realised vol from bars. Behaviour only: how often the gap is positive and how big, net of a costed hedge | 1 in 2 | 1 in 4 | short-tail by construction: 2025-10-10 is the stress test. Contract minimums need far more than the current ~$140. Venue access is a warning line, not a blocker |
| **Token-unlock supply events** (short perps into large scheduled cliff unlocks) | known, dated supply hits a thin float; practitioner reports describe pressure in the weeks before | unlock schedules (point-in-time, since they are announced in advance) against bars. Count events per year first; power is the risk | 1 in 3 | 1 in 3 | well-known trade, so shorts may already pay it away in funding. Calendar data is mostly paid |

**Where the LLM machinery can still earn its keep:** as a tool, not a
trader. It can turn unstructured sources (unlock schedules, governance
proposals, exchange notices) into dated event tables that a quant test then
consumes. It must not pick direction, and any test must be forward-only or
use data after the model's training cutoff.

**Do not pursue:** inverting the AI, forward-running the LLM for a sample,
news or Fear & Greed timing at hour-to-day horizons, or any pull of prompt
snapshots from Neon.

## 5. What was not done

- The post-2026-09-11 decisions were not pulled (see §1 for why).
- The costs in §2 are flat round-trip assumptions, not per-instrument spreads
  or funding. More precise costs would change the numbers slightly, not the
  sign of the conclusions.
- The two new lights are proposals with my odds. No scout has run, and the
  claims behind them (the size of the volatility premium, the effect of
  unlocks) are to be checked by that scout, not assumed.
