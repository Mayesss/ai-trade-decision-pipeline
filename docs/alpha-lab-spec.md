# Alpha lab — implementation spec

**Living document.** Unlike the dated docs in this folder, this one is edited in
place. It is the source of truth for the alpha-lab migration and is written to
be picked up cold by a session that has none of the conversation behind it.

- **Why** any of this: `docs/decision-engine-edge-audit-2026-09-11.md` (the
  audit that killed the old premise) and `docs/alpha-lab-plan-2026-09-12.md`
  (architecture and the paper review). Read the audit's §0 at minimum.
- **Sources**: `docs/references.md` — papers, platform limits with dates checked,
  and an explicit split between what was verified and what was not.
- **What to do**: this file. Work the checklists, tick them, keep them honest.

### Session protocol

1. Read §1 (what we are building) and §2 (invariants). Never violate §2.
2. Check §6 for unlocked decisions — if the next task depends on one, ask
   rather than assume.
3. Pick the next unchecked item in §7. Do it. Tick it. Note surprises in §8.
4. `npx tsc --noEmit && npm run lint && npm test` must pass before any commit
   that touches shared code (§5).

---

## 1. What we are building, in plain words

**A lab that tests whether a trading idea contains real information — and that
measures how often it fools itself.**

You state an idea in words. The lab turns it into a precise score that can be
computed for every instrument at every moment in history. It writes the idea
down *before* testing it. It tests it on data that the idea's author has never
seen. Then it reports whether the result can be told apart from luck, counting
every other idea that was tested alongside it.

**The core is not the backtester.** Backtesters are commodity — there are a
dozen good ones. The core is the bookkeeping:

- a **ledger** of every idea ever tried, written before results exist,
- a **budget** of how much untouched data remains, spent down and never refilled,
- a stream of **deliberately worthless ideas** run through the same pipeline, so
  the lab can state its own error rate rather than assert it.

**The promise.** Every result carries the number of attempts behind it. A "no"
is a finished product, not a failure.

**What it is not.** Not a signal service. Not a prediction engine. Not a
strategy generator that hands you winners — that machine is easy to build, and
§1.2 of the plan shows it manufactures false positives at a measurable rate.

### 1.1 North star

The long-term goal is a lab that **runs its own research loop**: proposing
hypotheses, testing them, retiring what fails, and compounding what survives —
with a human setting budgets and direction rather than approving each idea, and
eventually trading the survivors.

The thing that makes that safe is a single asymmetry, and it must hold from the
first line of code:

> The system may be autonomous over **search**. It is never autonomous over
> **the ledger or the sample budget.**

It can propose, test, mutate and discard on its own. It cannot grant itself more
data, reuse a spent holdout slice, hide a trial, or read its own scoreboard
(invariant 3). Autonomy over search compounds knowledge; autonomy over the
bookkeeping just lets it overfit faster and with more confidence — which is
precisely the failure mode both reviewed papers leave open.

Stage 7 is where the loop closes. Everything before it exists so that closing
it is safe.

**Why we believe the honest version is the valuable one.** We ran the
dishonest-by-accident version for three months. It produced no detectable edge,
and the metric we were reading it by was mis-scaled 5–16× in the direction that
hides problems. That is the case study, and it is the reason the bookkeeping
comes before the engine.

## 2. Invariants

Violating any of these silently invalidates everything downstream. They are not
style preferences.

1. **No evaluation without a ledger row written first.** A result whose trial
   count is unknown is uninterpretable.
2. **No bulk bar data out of Neon.** There is deliberately no panel table; bars
   live in R2 as Parquet. See plan §4.1 — this rule exists because we already
   paid ~€400 for breaking it.
3. **No numeric score crosses into hypothesis generation.** Feedback to a
   generator is qualitative prose only — never a metric, rank, or leaderboard.
4. **Holdout slices are consumed once.** One slice per generation, never reused.
5. **Synthetic nulls are indistinguishable from real hypotheses** at evaluation
   time. The evaluator must not be able to tell.
6. **The live system stays live.** It is running and being monitored. Research
   work is additive and must not alter live trading behaviour as a side effect;
   any change to trading logic is a deliberate, separately decided act.
7. **Every published number carries its trial count.**

## 3. Target architecture, one screen

```
  hypothesis (words + rationale)
        │
        ▼
  research.hypotheses  ──►  research.trials  ──►  research.budgets
   (registered BEFORE)         (one per eval)      (slice, spend-down)
        │
        ▼
  research.jobs  ◄── claim/reclaim pattern copied from
        │            claimSwingPostmortemById (lib/swing/pg.ts:1463)
        ▼
  Python worker (Vercel project #2, or local)
        │  reads  ──►  R2: panel/<snapshot>/<platform>/<symbol>.parquet   [free egress]
        │  writes ──►  research.results  (scalars only)                   [~KB]
        ▼
  FDR report: real survivors vs synthetic-null survivors
```

Neon is the **control plane** (small rows). R2 is the **data plane** (free
egress). Local disk is the **dev plane** (zero cost). Plan §4.1 has the numbers.

### 3.1 R2 cost rules

R2 has no egress charge, which is why the panel lives there — but it bills
**operations**, and the expensive class contains a trap.

| | price | includes |
|---|---|---|
| Class A | $4.50 / M | mutating ops **and `ListObjects` / `ListBuckets`** |
| Class B | $0.36 / M | `GetObject`, `HeadObject` |
| Standard storage | $0.015 / GB-month | free tier 10 GB + 1M Class A + 10M Class B |

Rules:

1. **Never list the bucket.** Listing is Class A — 12× the price of a read, and
   a fan-out that lists once per job turns 1,000 cheap reads into 1,000
   expensive ones. Object keys are resolved from `research.panel_versions` in
   Neon (which is why that table holds the key) and fetched directly. The key
   layout is deterministic: `panel/<snapshot>/<platform>/<symbol>.parquet`.
2. **Standard storage class, never Infrequent Access.** IA bills a 30-day
   minimum duration even if the object is deleted sooner, plus $0.01/GB
   retrieval — wrong shape for a panel that is re-read constantly and
   re-snapshotted.
3. **Keep per-symbol Parquet small enough to avoid multipart upload** — a
   multipart PUT costs several Class A ops. Per-symbol files are ~1–3 MB, so
   single-PUT; revisit if partitioning changes.
4. **Snapshots are immutable.** Never rewrite a published snapshot; write a new
   one and register a new `panel_versions` row. Rewriting silently invalidates
   every result already recorded against it.

At our scale this keeps R2 inside the free tier indefinitely: tens of MB of
storage, ~100 objects per snapshot, reads in the thousands.

### 3.2 The experiment: what is actually being measured

The project's question is no longer "does this system make money". It is:

> **Can an AI discovery process distinguish alpha from noise — and does the AI
> part contribute anything a dumb search would not?**

That is two questions, and they need three arms. Running two would leave the
second unanswered while feeling like an answer.

| arm | generator | budget |
|---|---|---|
| **A** | AI: hypothesis generation + mutation/crossover | N trials |
| **B** | synthetic nulls: shuffled labels, phase-randomised series, scrambled timestamps | N trials |
| **C** | random search over the **same** DSL primitives | N trials |

- **A vs B** — is the pipeline calibrated, and does it surface anything real?
  This is the false-discovery rate.
- **A vs C** — does the AI beat dumb search? **This is the actual A/B test**, and
  without arm C a win over B proves only that *something* worked, not that the
  AI did. Random expression search is a strong baseline and cheap to build.

Textbook factors (momentum, carry, vol) make a useful reference point but are
not a controlled arm — they come from outside the search space.

### Controls that make the comparison valid

1. **Matched trial budgets.** Same N per arm. Unequal budgets void the
   comparison outright: expected best-of-N under the null rises with N
   (plan §1.2), so the bigger budget "wins" on noise alone.
2. **Identical search space.** Arm C draws from the same DSL primitives as arm
   A. Vary the *strategy*, never the space (§6.7).
3. **Identical validator.** One deterministic evaluator for all arms.
4. **Blind evaluation.** `is_synthetic_null` is never visible to the evaluator.

### Treatment vs infrastructure — the rule for "should this be AI?"

> **Anything that varies between arms is the treatment. Anything shared must be
> deterministic.**

| component | AI? | why |
|---|---|---|
| hypothesis generation | **treatment** | it is what arm A *is* |
| mutation / crossover | **treatment** | part of the search strategy, not plumbing — arm C mutates randomly |
| validation, backtest, IC, inference | **never** | arithmetic. An AI validator makes arms incomparable and the FDR uncomputable |
| ledger, custody, budget accounting | **never** | see §1.1 — autonomy over search, never over bookkeeping |
| feedback summarisation | AI allowed, **prose only** | invariant 3: never a score, rank, or leaderboard |

This settles "should mutation be AI" as a category error: mutation is not a
design choice, it is the independent variable.

### The loop, per arm

```
information (market state — computeSwingState, recycled)
      |
      v
hypothesis      [A: AI]   [C: random]   [B: synthetic null]
      |
      v
REGISTER  <-- invariant 1: ledger row written BEFORE anything runs
      |
      v
factor          compiles to a deterministic spec; AST gate rejects lookahead
      |
      v
validation      rank IC on the discovery slice - cheap, deterministic
      |
      v
backtest        costs attached, holdout slice - survivors only, expensive
      |
      v
feedback        QUALITATIVE PROSE ONLY, never a score  <-- invariant 3
      |
      v
mutation / crossover --> new hypothesis, same arm, budget decremented
```

### Expected outcome, pre-registered

**The most likely result is that all three arms find nothing.** The panel is
worth ~2,000-4,300 effective observations against the ~4,900 needed to resolve a
0.10 ATR/trade edge at |t| > 3 (plan §2). That is a real result and it gets
published as one.

The pre-registered response to a null result is **widen breadth - more
instruments - not more trials.** Running more trials against the same spent
sample is the failure mode this entire design exists to prevent.

## 4. Repo disposition

Grounded in the actual dependency graph, not intent. Sizes are lines.

### 4.1 LIVE — the running trader

This is the system in production. Leave it alone unless a change is explicitly
decided. `pages/api/analyze.ts` is 4,433 lines and also holds the market-state
computation the research side reuses (§4.5).

| what | size |
|---|---|
| `pages/api/analyze.ts` | 4,433 |
| `pages/api/swing/*` | 9-line auth wrappers around the base handlers |
| `vercel.json` crons | 28 entries: 24 symbol ticks + wake-watch (1/min) + postmortem-drain + weekly-digest + summary-warm-fallback |
| `lib/swing/prompt.ts` | 1,685 |
| `lib/swing/decisionRules.ts` | 812 |
| `lib/swing/decisionConfig.ts` | 652 |
| `lib/swing/wakeWatch.ts` | 576 |
| `lib/trading.ts` | 1,310 |
| `restingEntryWindows` / `lastScan` / `recentActions` / `aiHealth` / `sync` / `flatGates` / `portfolioCap` / `riskSizing` / `warmLatch` / `cronControl` | 76–180 each |

### 4.2 RECYCLE — shared code. Port into the lab; do not move or rewrite in place.

These are used by **both** the live trader and the lab. Any change here can
break live execution silently, which is exactly what the contract tests exist
to catch (§5).

| module | size | note |
|---|---|---|
| `lib/swing/waveGeometry.ts` | 242 | **zero imports** — direct port, no work |
| `lib/swing/sessionLevels.ts` | 326 | **zero imports** — direct port |
| `lib/swing/signals.ts` | 308 | near-pure (types + indicators) |
| `lib/indicators.ts` | 791 | pure math but imports `./bitget` — **extract the pure core before porting** |
| `lib/swing/category.ts`, `lib/platform.ts`, `lib/symbolRegistry.ts` | 79–83 | universe definition — see §4.4 |
| `lib/swing/btcContext.ts` / `sessionEvents.ts` / `forexEvents.ts` | 168 / 500 / 643 | feature sources |
| `lib/bitget.ts` / `lib/capital.ts` | 317 / 3,816 | panel build only — one-time fetch, never per-job |
| `lib/swing/positionDecisionMatch.ts` / `capitalWindows.ts` | 465 / 470 | attribution; twice-repaired, hard to rebuild |
| `lib/swing/rStats.ts` + `.study/04`–`09` | — | inference → port to Python |

### 4.3 Not currently used by the live path

`lib/swing/lessons.ts` (314), `lib/swing/postmortem.ts` (1,330) and
`lib/swing/perplexity.ts` (165). The lessons loop did not generalise
(`week-one-review` §3) and post-mortems are the largest token cost. Nothing is
removed: the live system is running, and `postmortem.ts` holds outcome-labelling
logic worth reading before anything is decided about it. If cost is the concern,
minting can be disabled by config without touching code.

### 4.4 Coupling to fix early

`lib/symbolRegistry.ts` imports `vercel.json` — **the cron list defines the
traded universe**. The lab must not inherit that, or adding a research
instrument means adding a production cron. Give the lab its own universe
manifest before stage 3.

### 4.5 REASSIGNED — the trader becomes the generator

Decision 6.1 does not throw the LLM machinery away; it repoints it. These move
from "decide a trade" to "propose and mutate a hypothesis" (arm A of §3.2):

| module | size | becomes |
|---|---|---|
| `lib/swing/prompt.ts` → `computeSwingState` | 1,685 | the **information** node: STATE/MARKET payload construction is exactly the context a hypothesis generator needs. Keep the state builder, drop the trade-decision framing. |
| `lib/aiProvider.ts` + `gatewayResponses.ts` / `gatewayMessages.ts` | 124 / 431 / 289 | model transport for the generator. Already a single choke point — keep it. |
| `lib/swing/decisionSchema.ts` | 174 | pattern for the hypothesis response schema (note: `json_schema` is not enforced off OpenAI — see the gateway dialect notes). |
| `lib/aiModel.ts` | 71 | generator model selection — and the place the **training cutoff** (§6.3) gets pinned, since that cutoff is the custody boundary. |
| `lib/swing/lessons.ts` | 314 | **read before deleting.** It failed to generalise, but its scope-ladder and reinforce/promote logic is a first draft of correlation-gated library admission (plan §1.3). Learn from it, then delete it. |

## 5. Test policy

**Keep all of it. 54 unit + 39 contract files, 486 tests, currently green.**

They matter *more* during this migration, not less: §4.2 is shared code, and the
contract tests are the only thing that catches "I refactored `indicators.ts`
and the live trader now sizes differently." Green tests are the merge gate for
any commit touching shared code.

Rules:
- Delete a test only in the same commit as the code it covers.
- One scenario per test **file** — `pages/api/analyze.ts` and `lib/capital.ts`
  hold module-level state (see the `swing-fixtures` skill).
- New outbound host → add a world in `test/harness/worlds/`; never loosen
  `onUnhandledRequest`.
- New default-ON feature → pin it OFF in `test/harness/setup-env.ts`.
- Read every snapshot diff before `-u`. The 2026-09-11 re-baseline of
  `entry-sell` / `live-entry-sell` pins `risk_sizing` including the new
  `effective_risk_usd` and `exposure_capped`.

**Caveat, stated so nobody mistakes it later:** these are a *regression* net,
not a *correctness* net. Green means "the live trader behaves as it did
yesterday" — not "the live trader is good". The audit established it is not.

## 6. Decisions to lock

Each blocks work downstream. Recommendation given; owner decides.

| # | decision | recommendation | status |
|---|---|---|---|
| 6.1 | Does the live trader keep running? | — | **2026-09-14: the system is LIVE and stays live.** The owner is running it and monitoring by hand. Recent near-flat results reflect the 2026-09-11 changes (risk 10% -> 1%, portfolio cap, per-venue hard gates), which cut position size and trade count — small dollar swings are the expected consequence of smaller size, not evidence about edge. The measurement window is deliberately being allowed to run; see §10. |
| 6.2 | Do post-mortems keep running? | They are the single largest token cost (post-mortem spend exceeded decision spend). Disabling minting is the cheapest cost reduction that does not touch trading logic. | **OPEN** |
| 6.3 | Which generator model, and its training cutoff? | Must be pinned explicitly — the cutoff *is* the custody boundary (plan §3.2). Re-pin whenever the model changes. | **OPEN — blocks stage 1** |
| 6.4 | First trial budget size | Small. The whole panel is worth ~2,000–4,300 effective observations; that is roughly one honest answer. | **OPEN** |
| 6.5 | Is `.study/` committed? | Contains prod decision data; the audit §9 cites it as the reproduction path. | **OPEN** |
| 6.6 | Measurement freeze | Closed out by 6.1, not abandoned: the window ends because the product changed, not because the target was hit. Record the final R sample as the closing measurement of the old system. | **RESOLVED by 6.1** |
| 6.7 | Does arm C (random search) share the AI arm's DSL exactly? | Yes — same primitives, same budget, same validator. Varying the space instead of the strategy would void the comparison. | **RECOMMENDED** |

## 7. Checklists

### 7.0 Preflight — manual, outside the repo

Cloudflare (account exists, R2 activated 2026-09-12):
- [ ] Create R2 bucket `alpha-lab-panel`, **private**, no public access, no custom domain
- [ ] Confirm **Standard** storage class, not Infrequent Access (§3.1 rule 2)
- [ ] Create an S3-compatible API token scoped to that bucket only, read+write
- [ ] Record account id, access key id, secret
- [ ] Cloudflare skills are optional and **not installable from the VSCode extension**
      (`/plugin` is a Claude Code CLI surface). Either run
      `/plugin marketplace add cloudflare/skills` + `/plugin install cloudflare@cloudflare`
      from the CLI, or skip it and drive R2 with `npx wrangler r2 ...` directly.
- [ ] Set a Cloudflare billing alert (free tier is generous here, but the guard costs nothing)

Vercel:
- [ ] Create a **second project** from this same repo, root directory `research/`
- [ ] Confirm the research project has **zero crons** (the live project keeps all 28)
- [ ] Set `maxDuration` and memory on the research project (Pro: 800s / 4 GB available)
- [ ] Confirm a research deploy cannot redeploy the live project

Neon:
- [ ] Confirm the organisation spending limit is set
- [ ] Review data-transfer alerts against the 500 GB allowance (`docs/neon-egress.md` currently suggests 50/90/120 GB)
- [ ] `research` schema is created **by code**, never by hand

Env vars (research project + `.env.local`):
- [ ] `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET`
- [ ] Neon pooled URL for workers; unpooled reserved for analysis (`BEGIN READ ONLY`, never a session `SET` — see the 2026-09-10 incident)

Local:
- [ ] Python toolchain (`uv`), `polars` / `pyarrow` / `numpy` / `scipy`
- [ ] Gitignored local panel mirror directory

### 7.1 Stages

Mirrors plan §6. Tick as completed; record surprises in §8.

- [ ] **0** — Pin §6.3 (generator model + training cutoff). Add the lab's own universe manifest (§4.4).
- [ ] **1** — `research.hypotheses` / `trials` / `budgets` + custody gate in code. No engine.
- [ ] **2** — `research.jobs` queue cloned from `claimSwingPostmortemById`; Python worker skeleton on Vercel project #2; end-to-end "claimed a job, wrote a row".
- [ ] **3** — Panel builder → Parquet → R2 + local mirror; registers `research.panel_versions`.
- [ ] **4** — Vectorised IC engine + inference ported from `.study/04`–`09`; costs from the 282 real fills.
- [ ] **5** — Hypothesis DSL + **AST lookahead gate**. 10–20 hand-written hypotheses. First honest budget.
- [ ] **6** — Synthetic null injection + FDR reporting. *(This is the product.)*
- [ ] **7** — LLM generator, archetype memory, narrowed feedback channel.
- [ ] **8** — Execution automation. **Gated** on plan §7 kill criteria.

## 8. Running log

Append surprises, reversals and things a future session would otherwise
rediscover the hard way. Newest last.

- **2026-09-11** — R denominator fixed: R now divides by realised risk
  (`resolveRealizedRiskUsd`), not the budget. Average loss moved −0.09R →
  −0.64R; worst trade −0.75R → −3.45R. No DB backfill needed — historical rows
  are repaired on read.
- **2026-09-12** — Two papers reviewed (XAlpha 2607.08332, AlphaSeek
  2608.13913). Both are CSI300 cross-sectional factor miners; neither does trial
  accounting. Take the AST gate, tri-alignment judge, archetype memory and
  correlation-gated admission; reject OOS-in-selection-score and
  backtest-metrics-as-gradient.
- **2026-09-12** — Egress: first draft put the bar panel in Neon. Corrected to
  R2 before any code was written. ~60 GB → ~3 MB per sweep.
- **2026-09-12** — Decision 6.1 locked: trading stops; the LLM machinery is
  repurposed as the hypothesis generator/mutator (§4.5). Earlier advice to keep
  it running as a "data generator" was wrong under the factor framing — forward
  bars accrue without trading, so only new fill data is lost.
- **2026-09-12** — Experiment design fixed at **three arms** (§3.2): AI vs
  synthetic null answers "is the pipeline calibrated"; AI vs random search over
  the same DSL answers "does the AI contribute". Matched trial budgets are the
  control that makes it valid.
- **2026-09-14** — Retrospective veto test on the 453 live decisions
  (`.study/16-veto-test.mjs`, `17-trend-oos.mjs`). Base-rate veto (k-NN over 5
  state features, 73,504 reference bars, strict no-lookahead): **fails** — the
  keep-vs-veto separation flips sign with k (k=50 −0.117, k=100 −0.225, k=200
  +0.573, k=500 +0.158). Only the pre-specified k=200 looked good; tuning k
  post hoc would have manufactured a working veto.
  A diagnostic then surfaced a stronger candidate — AI direction agreeing with
  EMA20/EMA50 trend: +0.141 vs −0.874, separation 1.016 ATR, **t=2.45**. Tested
  on the 73,504 independent reference bars it **vanished**: mean +0.0116,
  t=0.48, 49.0% win rate, stable across venues and both halves. Noise.
  Two lessons: (a) pre-specifying k is what saved the first result from being a
  false positive; (b) the sample's effective n is ~44/30, so the smallest
  detectable separation is 1.206 ATR — "no effect" here means *undetectable*,
  not *absent*. Untouched by this: the v2 veto where the LLM emits a structured
  claim that is itself backtested.
- **2026-09-14 (later)** — Panel widened: 83 Bitget USDT perps, 334,766 4H bars
  back to 2024-03 (`.study/22-wide-panel.ts`, manifest in
  `.study/panel-wide/_manifest.json`). 37 of the top-120 by volume were dropped
  as too thin (<1,500 bars); tokenised equities (NVDA, MSTR, XAU, XAG) survive
  but with short history, so they are not yet an equity cross-section.
  **First cross-sectional factor test with real power**
  (`.study/23-xsec-ic.mjs`, 5,474 dates, avg breadth 60, 4 pre-specified
  signals, Bonferroni |t|>2.50 on non-overlapping dates):
  1-day reversal IC **+0.0275, t=4.03** (clears); 5-day momentum IC
  **−0.0207, t=−3.02** (clears, negative); trend −0.0020, t=−0.22 (nothing,
  consistent with the 09-14 trend result dying); low-vol +0.0166, t=2.47 (just
  under). These markets mean-revert at 24h.
  **Positive control passed** — the instrument detects a well-documented effect,
  which is the counterpart to synthetic nulls (§3.3).
  Structural fact about the old engine (`.study/24-ai-vs-reversal.mjs`):
  corr(AI direction, last-24h move) = **0.706**, and 85% of its decisions traded
  WITH the recent move. It was a momentum chaser.
  **Do NOT over-read that as the cause of the losses.** Chasing vs fading
  decisions performed the same (−0.299 vs −0.379, t=−0.26, n=67 fading — badly
  underpowered). More importantly the reversal effect is **cross-sectional and
  market-neutral** (rank IC), while the engine took **absolute directional**
  bets. Those are different businesses; the link is unproven.
  **Caveats before anyone believes the reversal result:** (a) the whole panel
  was used — no holdout was reserved, so this needs re-testing on held-out data;
  (b) costs are NOT netted, and a 24h-horizon long/short across 60 perps is
  high-turnover — the week-one review found fees were 58% of Bitget net loss at
  3.8h holds.
- **2026-09-14 (sweep + holdout + costs)** — Full pipeline run end to end.
  **Sweep** (`.study/27-sweep.mjs`, 40 signals + 4 tested earlier = family 44,
  Bonferroni |t|>3.27): 9 survive on the discovery panel (83 symbols,
  2024-03..2026-09). Collinearity (`.study/28`): mean |corr| 0.37, ~3-4 distinct
  effects — `px_vs_ema20` vs `rsi14` correlate 0.97; `vol_per_atr` is
  independent of everything (|corr| < 0.13) and has the largest IC.
  **Holdout** (`.study/26`, `.study/29`): 32 symbols, 146,653 bars,
  2021-06..2024-03 — never examined before. **7 of 9 replicate with the same
  sign** (px_vs_ema20, rsi14, ret_12, ret_1, ret_2, vol_per_atr, resid_ret6);
  the other 2 kept the sign but fell under threshold. 9 further signals look
  strong ONLY in the holdout (`corr60_btc` t=5.35) — these are discoveries ON
  the holdout, now contaminated, and were NOT adopted.
  **COSTS KILLED IT** (`.study/30`, `.study/31`). A quintile long/short book on
  the 7 replicated signals loses **gross**, before fees, in both periods and
  under both equal-dollar and risk-parity weighting (discovery −0.104%/24h,
  holdout +0.007%/24h; turnover ~130% per rebalance).
  **Why, and this is the lesson:** the quintile diagnostic shows the signal
  predicts the MEDIAN and anti-predicts the MEAN. Discovery Q5 median −0.129%
  vs Q1 median −0.038% (spread +0.091%, signal works), but Q5 mean +0.102% vs
  Q1 mean +0.008% (spread −0.094%, signal loses). Holdout identical in shape.
  Most pumped coins revert; a few keep exploding and their tails dominate any
  magnitude-weighted P&L. Rank IC is blind to magnitude, so it reported
  t=−4.8 replicated across two regimes for a strategy that cannot be traded.
  **Standing rule from this: rank IC is never sufficient evidence. Every
  candidate goes to a costed, magnitude-weighted portfolio before it is
  believed.** Without that step this would have shipped.
- **2026-09-14 (vol_per_atr post-mortem)** — Tested the one independent
  survivor on its own. **Dead, and it exposed a harness bug.**
  `vol_per_atr` was `log(1+baseVolume)/ATR`, mixing a token COUNT with a
  PRICE-unit ATR — so cross-sectionally it ranks by price scale, not by volume
  vs volatility. Measured: rank-corr with log(price) **-0.991** (discovery) /
  **-0.993** (holdout); rank persistence **0.998**. It was "buy expensive coins,
  short cheap ones".
  As a portfolio (`.study/33`): discovery gross +0.0012%/24h, holdout
  **-0.0242%/24h** — sign flips between periods, 4/6 then **2/6** sub-periods
  positive. Turnover was genuinely low (7.1% / 2.3%) so costs were not the
  killer; there was simply no effect.
  **The harness bug:** for a near-static signal, date-level ICs are the same bet
  re-tested, so the non-overlapping t-stat is meaningless. vol_per_atr's t=-4.51
  came from ~888 re-tests of ~6 independent bets.
  **Fixed in `.study/27-sweep.mjs`**: rank persistence is now computed per
  signal, printed, and any signal above 0.9 is excluded from the survivor list
  with `[t INVALID: static]`. Discovery survivors accordingly drop 9 -> 8.
  Audit of the whole family (`.study/34`): 11 of 40 signals are above 0.9 and
  had invalid inference; of the 9 original survivors only vol_per_atr was
  affected — the other 8 have persistence <= 0.60 (most near zero), so their
  statistics stand. `corr60_btc` (the tempting holdout-only t=5.35) is
  disqualified twice over: discovered on the holdout AND persistence 0.956.
  **Net state:** no tradeable candidate survives. The reversion cluster has
  valid, replicated statistics but dies on the mean/median skew; vol_per_atr
  dies on construction and inference. Two permanent guards were added to the
  instrument as a result, both found empirically rather than by argument.
- **2026-09-15** — §10 patience posture broken one day in, under its own cost
  exception (§11). The §9.5 env fallback is applied instead of the ladder
  shift; the ladder shift stays parked as the second step. One code addition
  found necessary while doing it: the entry stop floor never reached AMENDS, so
  the model could re-create a sub-ATR stop on the next bar close — an amend
  floor (`AMEND_SL_MIN_ATR`, 1 primary-ATR from current price, drop-not-widen)
  now closes that. Symbol universe 24 -> 8.
- **2026-09-16** — The four §11 thresholds became code defaults instead of
  Vercel env vars (the env write was blocked in-session; a default the repo
  asserts beats a value only visible in a dashboard). Harness pins the entry
  floor and wake distance at the historical values; `decisionConfig.prodDefaults`
  asserts the live ones.
- **2026-09-16** — Scheduled AI look moved from every 4H close to ONCE A DAY
  per venue (§12) without shifting the timeframe ladder: the two §9.2
  blockers turned out to be API facts (Capital serves no MONTH resolution;
  Bitget serves ~90 daily bars), so cadence and ladder were decoupled instead.
  Perplexity digest restored on flat scans. Capital account measured at $81
  equity: at 1% risk with a 3-ATR stop no commodity clears its minimum deal
  size — GOLD and OIL_CRUDE out, US100/TLT/GBPUSD in, 9 symbols.
- **2026-09-16** — Decision model back to `zai/glm-5.3` (owner decision, cost).
  The R sample start moves to this deploy: everything since 09-11 was a
  different geometry, cadence, universe and model.
- **2026-09-16** — Post-deploy: dashed resting-entry lines drawn to "now" or to
  the 48h backstop on BTCUSDT and EURUSD were chart artifacts, not orders.
  Cause: the window builder read fills only inside the chart window and gate
  withdrawals only from the ticks the client had loaded, so an order issued
  before the window could never be ended. Fixed server-side: fills and
  `…_resting_entry_withdrawn` ticks are read over the same 48h lookback as the
  issuing decisions. Also found: the 08:00 UTC look died on a gateway 402 (no
  credit balance) and the 12:00 close was skipped as off-boundary — the daily
  look is now owed until served (§12).

---

## 9. Horizon expansion — PARKED, notes kept

**Status 2026-09-16: parked indefinitely — no longer needed for cadence.**
§12 moved the scheduled look to once a day while keeping the 4H ladder, which
was the whole economic point of the shift. What remains here is only the
question of whether the model should *see* daily bars as primary, and nothing
measured so far says it matters.

**Status 2026-09-15: still parked; the §9.5 fallback was applied instead (§11).**

**Status 2026-09-14: parked, not scheduled.** Shifting the primary timeframe
4H -> 1D was considered and deliberately deferred — the current configuration
has only been live since 2026-09-11 and changing it again would reset the
measurement window before it has produced anything readable (§10). These notes
are kept so that if it is picked up later, the work already done is not
repeated.

### 9.1 Why — and why NOT

**Not for edge.** The horizon scan (`.study/35-horizon-scan.mjs`) tested the
replicated composite at 24h / 48h / 7d / 14d on both panels:

| horizon | discovery net/yr @6bp | holdout net/yr @6bp | turnover |
|---|---|---|---|
| 24h | −68.2% | −14.5% | 126% |
| 48h | −29.1% | −24.9% | 146% |
| 7d | **−38.6%** | **+33.1%** | 156% |
| 14d | **−72.0%** | **+42.2%** | 155% |

The sign **disagrees between periods** exactly where it looks good, on 55–126
rebalances. That is the same pattern that killed the price tilt. Longer holds
also did **not** cut turnover (~150% throughout) because turnover is driven by
signal *persistence*, not holding period — the composite is built from `ret_1`,
`ret_2`, `rsi14`, which have near-zero persistence. The slow, persistent signals
(`ret_120`, `ret_180`, `ema50_200`, `px_vs_ema200`) were all in the sweep and
all showed nothing.

**The real reasons** are operational: ~6x fewer decisions per day, so token
spend (currently exceeding the account balance) falls sharply; less fee drag per
unit of time; fewer, more considered decisions to monitor by hand. Expect lower
bleed, **not** an edge.

### 9.2 Two blockers found 2026-09-14 — solve these first

1. **Capital has no MONTH resolution.** `toCapitalResolution`
   (`lib/capital.ts:455`) tops out at `WEEK`. The current ladder is
   15m / 1H / 4H / 1D / 1W; shifting every rung up leaves MACRO and CONTEXT both
   at 1W. Decide deliberately: collapse to a 4-rung ladder, or accept the
   duplication, or stop the shift at MACRO.
2. **Bitget history is thin at high timeframes.** Per the comment at
   `lib/indicators.ts:683`, a request returns ~90 bars at 1D and ~13 at 1W. A 1D
   primary needs EMA200. The `SPOT_BACKFILL_GRANULARITY` path exists for exactly
   this — verify it actually delivers enough warmup at the new rungs **before**
   trusting any indicator.

### 9.3 Blast radius (measured, smaller than feared)

- 6 files reference `PRIMARY_TIMEFRAME`.
- `lib/swing/prompt.ts` has **zero** hardcoded "4H" — every mention is
  interpolated from the constants, so prompt text updates itself.
- The boundary gate (`primary_close_off_boundary`, `pages/api/analyze.ts:912`)
  keys off primary close, so decision cadence changes automatically.
- **Contract fixtures and snapshots are 4H** — all 39 contract files need
  `npm run test:fixtures:capture` then a snapshot re-baseline. Per the
  `swing-fixtures` skill: after re-capture, verify `promptSkipped` and the entry
  direction, because a BUY can silently be demoted to HOLD and turn an entry
  test into a hold test.
- Gates tuned for the 4H cadence need re-reading, not just re-passing: wake
  thresholds, quiet-tick, boundary dedupe, session windows, and
  `ENTRY_SL_MIN_ATR` — whose *unit* changes meaning when the primary ATR becomes
  a daily ATR (a 1-ATR floor becomes ~2.4x wider in price terms).

### 9.4 Deploy care

Change -> re-capture fixtures -> `npx tsc --noEmit`, `npm run lint`,
`npm test` -> verify a dry-run tick per venue -> deploy. A timeframe change
alters bracket geometry and decision cadence for anything already open, so plan
how in-flight positions are handled before deploying, not after.

### 9.5 Cheap fallback if the ladder shift stalls

**Applied 2026-09-15 — see §11 for what was actually set and why it grew by one
code change.**

Env-only, no code, instantly reversible, achieves most of the economic effect:
`SWING_ENTRY_SL_MIN_ATR` 1 -> 3 (stops ~a daily ATR, so positions stop dying to
intrabar noise and holds lengthen), `SWING_REENTRY_COOLDOWN_MIN` 240 -> 1440,
and cut the symbol list from 24 crons to ~8 (linear token-cost reduction, and
easier to watch by hand).

### 9.6 Reading the results

Widening the stop floor does not move brackets on positions already open, so the
first days mix two geometries. Mark the changeover timestamp and read before and
after separately. And per the 09-11 audit: a few days is 5–20 trades, which
cannot distinguish a real change from noise. Judge the burn rate, not the P&L.

---

## 10. Current posture: let the 2026-09-11 configuration run

**Decided 2026-09-14.** No further changes to trading logic, sizing, gates,
prompt, model or timeframe. The system runs as configured and is monitored.

### Why

The configuration deployed 2026-09-11 — risk 10% -> 1%, portfolio cap
(`SWING_MAX_OPEN_POSITIONS`), per-venue hard gates, entry stop floor, model
pinned to `openai/gpt-5.6-sol` — has been live for three days. Changing anything
now restarts the clock, and "tweak until it works" is the failure mode the
window exists to prevent (`week-one-review` §12).

### How to read it, and how not to

- **Small dollar swings are expected.** Cutting risk 10% -> 1% cuts position
  size roughly tenfold, and the portfolio cap plus hard gates cut trade count.
  Near-flat P&L is the designed consequence of smaller size. It is not evidence
  about edge in either direction.
- **Do not read a few days.** 5-20 trades cannot distinguish a real change from
  noise. Measured 2026-09-14: n=17 since the freeze, mean R -0.202 against
  -0.151 before, difference t = -0.34 — indistinguishable from zero, and stated
  here so nobody later mistakes it for a negative finding. It is a
  *no-information* result.
- **Read R, not cash** (`rStats`, now on the corrected denominator), and read
  the burn rate: token spend per day, Neon transfer, trade count against
  expectation.
- **Judge at a pre-set count, not on a feeling.** Per the audit, 200 closes is a
  survival checkpoint rather than enough power to settle whether an edge exists;
  whatever is claimed from this window must state the power it actually had.

### What would justify acting before then

A safety or cost problem — runaway spend, a gate misfiring, positions opening
outside intent. Not a run of red days, and not a run of green ones.

**Invoked 2026-09-15** on the cost clause — see §11. Doing this a second time
inside the window would be the "tweak until it works" failure, not another
exception.

---

## 11. Horizon loosening — applied 2026-09-15

### Why now, and why this is the §10 exception rather than a breach

- Token spend was already stated (§9.1) to exceed the account balance. That is
  the cost problem §10 names.
- 24 symbols were scanning for **4** slots (`SWING_MAX_OPEN_POSITIONS=4`, one
  per asset class). Six times the analysis cost for the same exposure.
- The 09-11 freeze was about **sizing** (risk 10% -> 1%, cap, hard gates).
  Bracket geometry and hold length were never part of what it measured, and
  the week-one review already attributed the losses to stops inside one bar's
  noise.
- Cheapest route that gets most of the daily-cadence effect (§9.5), reversible
  by env.

### What changed

All thresholds are **code defaults** (decided 2026-09-16: the Vercel env write
was blocked in-session, and a default the repo asserts is easier to find than
an env var). The env names still override; none is set in production.

| Lever | Before | After | Where |
|---|---|---|---|
| Entry stop floor | 1 primary-ATR | **3** | `ENTRY_SL_MIN_ATR` (env `SWING_ENTRY_SL_MIN_ATR`) |
| Re-entry cooldown (same side) | 240 min | **1440** | `REENTRY_COOLDOWN_MIN` (env `SWING_REENTRY_COOLDOWN_MIN`) |
| In-position emergency look | 1.5 ATR move | **3** | `IN_POSITION_EMERGENCY_MOVE_ATR` in analyze.ts (env `SWING_INPOS_EMERGENCY_MOVE_ATR`) |
| Position wake band min distance | 0.3 ATR | **1** | `POSITION_WAKE_MIN_ATR` (env `SWING_POSITION_WAKE_MIN_ATR`) |
| **Amend** stop floor (new) | none | **1** ATR from current price | `AMEND_SL_MIN_ATR` (env `SWING_AMEND_SL_MIN_ATR`) |
| Symbol universe | 24 crons | **8** (→ 9, reshaped in §12 the next day) | `vercel.json` |

`test/unit/swing/decisionConfig.prodDefaults.test.ts` asserts the four
decisionConfig values, so a default cannot drift silently. The harness pins the
entry floor at 1 and the wake distance at 0.3 (fixtures were written against
them; the new floors would demote entry scenarios to HOLD).

Kept: BTCUSDT, ETHUSDT, GOLD, OIL_CRUDE, US500, DE40, EURUSD, USDJPY — two per
class. Dropped the crypto alts first (they close six bars a day, no session
gap, so they were the bulk of the calls).

The two emergency/wake thresholds move **with** the stop: at a 3-ATR stop a
1.5-ATR emergency look would fire on every position halfway to its stop — the
intrabar consultation the change exists to remove.

**Why the amend floor had to be code.** `sanitizeExchangeTpSl` applied the
entry floor to entries only (by design: a trail to breakeven is legitimate).
So the model could open at 3 ATR and pull the stop back to 0.6 ATR on the very
next 4H tick, and the whole change would have been cosmetic. The amend floor is
measured from the **current** price (there is no entry to anchor to), is
deliberately lower than the entry floor (one bar's noise is the width a stop
must survive; how much profit to give back beyond that is the model's call),
and a sub-floor amend is **dropped** — standing stop stays, like a loosening
amend — never widened. Stated in the in-position HARD CONSTRAINTS. Pinned OFF
in the test harness per the swing-fixtures convention;
`decisionRules.amendFloorEnabled.test.ts` covers the ON path.

### Not changed

Risk 1%, cap 4, one per class, primary 4H (the ladder shift stays parked, §9),
everything else in the prompt. (The model was still sol on 09-15; it went to
GLM on 09-16, see §12.)

### Deploy order (owner)

1. Nothing to set in Vercel — the thresholds are code defaults. Check that
   none of the four env names is set there (as of 2026-09-16 none was), or an
   old override would win.
2. Commit + deploy. Record the deploy timestamp here as the **changeover**.
3. Dry-run tick per venue (`swing-tick`). Two things to look for: on Capital,
   whether the 3x smaller notional at 1% risk falls under an instrument's
   minimum size (which either bumps risk above 1% or refuses the trade), and
   the share of entries refused with `entry_stop_below_floor`.
4. Positions open at changeover keep their old brackets and the old
   thresholds do not apply to them retroactively — mixed geometry for a few
   days, as §9.6 warned.

### How to read it

- Split at the changeover; never pool the two geometries.
- Watch the **refusal counts** first: `entry_stop_below_floor` on flat ticks,
  `sl_below_amend_floor_dropped` in the bracket notes on in-position ticks. A
  high entry-refusal rate means paying for HOLDs while the model learns the
  new floor; fewer trades is the intended effect, near-zero trades is not.
- Then the burn rate: AI calls/day should fall roughly 3x from the symbol cut
  alone, and wake-driven calls further.
- Then R, and only at a pre-set count (§10). Expect longer holds and fewer,
  larger-per-trade stop-outs in R terms; do not read the first week's P&L.
- Second step, only after this has run: the ladder shift (§9), whose two
  blockers (§9.2) remain unsolved.

---

## 12. Daily decision cadence — applied 2026-09-16

### Why

Research summary, written up in the session that decided this:

- LLM trading agents in the literature decide once a day per ticker; the field
  audit found 1 of 19 studies modeling transaction costs, and its worked
  example loses 11% of cumulative return at 10 bp of friction and 27% at 25 bp.
- A reproducibility study of the TradingAgents framework measured output
  variance at roughly half the expected return; stochastic configurations
  underperform a passive benchmark. Six looks a day is six draws from that
  distribution.
- Gârleanu & Pedersen: trade slowly toward the target, act on **moves**, not
  on the clock. The wake bands and the emergency look are the move-based half
  of this system and are untouched; only the time-based half slows down.
- Own data (§9.1, edge audit): no information at any horizon; the one measured
  effect of 4H looks was churn.
- Nothing class-specific favours more frequency anywhere: trend persistence
  runs 1–12 months in all four classes; forex volatility concentrates in the
  London/NY overlap, so more forex calls would be calls at the noisiest hours
  on the instrument with the worst spread per ATR. Index returns accrue
  overnight, so a decision at a fixed hour, sized for the gap, is the fitting
  cadence. **One cadence for all symbols.**

### What changed

| Lever | Before | After | Where |
|---|---|---|---|
| Scheduled AI look | every primary (4H) close | **once a day per venue** | `DECISION_CADENCE='1D'` (env `SWING_DECISION_CADENCE=primary` restores) |
| Decision hour | — | Bitget **00:00 UTC**, Capital **08:00 UTC** | `DECISION_HOUR_UTC` (env `SWING_DECISION_HOUR_UTC_BITGET/_CAPITAL`) |
| Retry window for an unserved look | — (2-min tolerance) | **6 h** | `DECISION_RETRY_WINDOW_MIN` (env `SWING_DECISION_RETRY_WINDOW_MIN`) |
| Flat cooldown clamp | 360–1440 min | **1470–10080** | `HOLD_COOLDOWN_*` derive from the cadence |
| Perplexity digest | in-position ticks only | **flat and in-position** | `analyze.ts` bundle; `PERPLEXITY_FRESH_HOURS` 6 → 24 under the daily cadence |
| Symbol universe | 8 | **9** (see below) | `vercel.json` |
| Decision model | `openai/gpt-5.6-sol` | **`zai/glm-5.3`** (effort `high`, the middle of GLM's low/high/max) | `DEFAULT_AI_MODEL` in `lib/constants.ts` |

**Model.** Owner decision 2026-09-16: GLM's reasoning is close to sol's at
roughly $1.40/$4.40 per 1M in/out against $2/$10. The 09-11 argument for
pinning sol was the model confound inside one measurement window; §11–12 reset
that window anyway, so the pin bought nothing further. GLM has no json_schema
mode — schema calls already go out as forced tool calls
(`lib/gatewayResponses.ts`), which is what made GLM usable in September. Watch
the `reshape` retry rate and output-token counts in the first days (GLM
measured 883–3150 output tokens per tick against sol's 299–1763).

**Ladder unchanged.** The §9 shift (4H → 1D primary) was the planned route to a
daily cadence, but its two blockers are API facts, not code debt: Capital's
price API has no MONTH resolution, so a 1W macro would leave nothing for
context, and Bitget serves ~90 daily bars where EMA200 needs 200. Decoupling
cadence from ladder gets the economic effect with the 4H fixtures, ATR units
and bracket geometry intact. The daily-scale geometry comes from the 3-ATR
entry floor (§11). The cadence gate is `isDailyDecisionTime` in
`decisionConfig.ts`; both venue hours are 4H closes, so every indicator is
still read on closed bars (asserted in `decisionConfig.decisionTime.test.ts`).

**The look is owed, not timed.** The 4H gate asked "is it :00 now?" with a
2-minute tolerance, and a miss cost four hours. Under a daily cadence a miss
costs a day, and on 2026-09-16 itself two were measured on BTCUSDT: the 08:00
UTC look died on a gateway **402** (no credit balance on the AI Gateway — check
the balance, a BYOK key does not exempt you) and the 12:00 UTC close was
skipped as off-boundary with the tick logged at 12:00:15. So `analyze.ts` now
asks "has today's look been served?": from the decision hour for
`DECISION_RETRY_WINDOW_MIN` (6h) every cron tick is due until one claims the
day (KV marker `swing:decision:served:v1:<venue>:<symbol>`), and a tick that
dies before deciding releases the claim so the next 15-min tick retries.
Deterministic gate skips (session window, base gates, cap) keep the claim —
those are decisions, not failures. Manual and dryRun calls never claim.

**Still fires off-schedule:** wake bands (flat and in-position), the in-position
emergency look (now 3 ATR), a swept resting entry's re-issue decision,
failed-break fires, session-window owed looks, manual/API calls.

**Capital hour caveat.** 08:00 UTC is outside every session decision window in
summer time. In winter Xetra opens at 08:00 UTC, so DE40's scheduled look
lands inside its 30-min post-open window and the gate defers it to the
window's end via the owed look. Accepted.

### Symbol set, measured

Probe run 2026-09-16 against the live Capital account (read-only): equity
**$81.17**, available margin $71.54. At 1% risk ($0.81) and a 3×4H-ATR stop:

| symbol | 4H ATR % | min notional | notional @3 ATR | opens? | max floor (ATR) |
|---|---|---|---|---|---|
| GOLD | 0.72 | $43.35 | $37.57 | no | 2.6 |
| OIL_CRUDE | 1.74 | $99.52 | $15.53 | no | 0.5 |
| US500 | 0.31 | $76.03 | $86.17 | **thin** | 3.4 |
| DE40 | 0.49 | $25.44 | $55.72 | yes | 6.6 |
| US100 | 0.49 | $29.08 | $55.34 | yes | 5.7 |
| TLT | 0.45 | $8.08 | $60.42 | yes | 22 |
| EURUSD | 0.13 | $115.41 | $209.33 | yes | 5.4 |
| USDJPY | 0.26 | $100.00 | $105.50 | yes | 3.2 |
| GBPUSD | 0.15 | $134.71 | $185.72 | yes | 4.1 |
| NATURALGAS / COPPER / SILVER / HK50 / UK100 | — | — | — | no | ≤ 2.5 |

"Max floor" is the widest stop, in 4H ATR, at which the 1%-risk notional still
clears the minimum deal size. **No Capital commodity is openable**, so that
class is absent from the book and the one-per-class cap leaves at most three
Capital classes plus crypto. US500 and USDJPY clear by a thin margin: a stop
the model sets much wider than the floor drops the entry to HOLD with
`MIN_SIZE_EXCEEDS_REQUESTED_NOTIONAL`, and a volatility rise flips them. Kept
deliberately, read the refusal reasons.

Kept: BTCUSDT, ETHUSDT, US500, US100, DE40, TLT, EURUSD, USDJPY, GBPUSD.

At $81 of equity, 1% risk is under a dollar a trade and fees are a large
fraction of it. This does not change any decision here — the freeze reads R,
not cash — but it is the number to keep in view when reading the burn rate.

### How to read it

- Expected scheduled AI calls: **9 per day** (2 crypto at 00:00 UTC, 7 Capital
  at 08:00 UTC on trading days), plus wakes. Materially more than that is a
  gate defect.
- Perplexity: about one uncached digest per symbol per day.
- Changeover = the deploy timestamp; split every read at it (§9.6).
- Then as §11: refusal counts first, burn rate second, R at a pre-set count.


---

## 13. Lesson loop off — applied 2026-09-17

### Why

The lesson library overfits, measured against the live rows on 09-17:

- **41 active lessons, 21 of them resting on a single post-mortem.** Symbol
  scope (27 rows) averages 1.26 supporting post-mortems and is single-symbol by
  construction.
- **35 of 41 carry a hand-invented numeric threshold** — 15 distinct ATR values
  between 0.05 and 2.0 (0.5 nine times, 0.3 eight, 0.1 six, 0.25 twice), and 30
  carry a minute threshold as well. One-to-four trades cannot distinguish 0.25
  from 0.3 ATR; that precision is noise written down as doctrine.
- **`confidence` is a self-report that only rises** — `mergeSwingLesson` takes
  `max(new, existing)` and only an explicit `revise` lowers it. The prompt's
  per-scope caps then select what the model sees *by that number*, so the
  selection criterion is the one field with no feedback.
- **The evidence channel is one-way.** 253 `right_to_skip` post-mortems against
  27 `wrong_to_skip`, and `right_to_skip` is hard-gated to reinforce-only.
  Vetoes accumulate support; permissions structurally cannot. Half the active
  rows open with "Never / Do not / Veto".
- **Nothing has ever been tested out of sample.** There is still no
  `lesson_impressions` table, so no lesson has been joined to the decisions it
  was shown at. Gap 1 of `docs/opinionation-and-learning-loop.md` is open, and
  every row is in-sample by construction.
- **No decay, no expiry.** 58 of the 77 rows ever written were minted in the
  two weeks around the stop-floor removal, ~20 of them restatements of "put the
  floor back". The floor returned in §11; nothing removes lessons fitted to the
  regime that no longer exists.

What survives the criticism is the code-owned scope ladder: every `global` row
is backed by 4–9 distinct symbols and 4–20 distinct positions. That mechanism —
not the rows it produced — is what §1.3's correlation-gated library admission
should inherit.

### What changed

| Lever | Before | After | Where |
|---|---|---|---|
| Lesson injection | on | **off** (env `SWING_LESSONS_MODE=on` restores) | `resolveSwingLessonsMode`, `lib/swing/lessons.ts` |
| Post-mortem analyst | `all` (losses + wins + refusals) | **off** (env `SWING_POSTMORTEM_MODE=all\|loss` restores) | `resolveSwingPostmortemMode`, `lib/swing/postmortem.ts` |
| Lesson prose in the prompt | failed-break doctrine cited "your own post-mortem lesson standard" | **no mention of lessons on a lesson-free tick** | `prompt.ts`; asserted in `prompt.situationalDoctrine.test.ts` |
| Decision model | `zai/glm-5.3` | **unchanged** — opus-5 priced and rejected, below | `DEFAULT_AI_MODEL`, `lib/constants.ts` |

Rows are untouched: 41 active lessons and 469 post-mortems stay readable for
the lab. Manual and backfill post-mortem triggers still bypass the mode filter,
so a single analysis can be asked for on demand.

**Why both halves go, not just injection.** The analyst pass exists to feed the
library. With injection off its only product is unread rows — bought as the
pipeline's largest token consumer by a wide margin: 255 calls over the 14 days
to 09-16 spent 9.9M input and 1.35M output tokens, at ~39k input per call with
essentially no cache reads (51k cached out of 9.9M). That is ~$21/month at GLM
promo pricing, ~$43 at list, for rows nothing reads.

**And the prompt must not mention what it no longer has.** The `LESSONS` block
and its doctrine paragraph were already gated on the payload; the failed-break
doctrine was not, and told the model a break "has FAILED by your own
post-mortem lesson standard". An instruction that cites a memory the model does
not have is worse than silence — it invites acting on one. Now cut, with a unit
test asserting `/lesson/i` matches nothing in a lesson-free tick's prompt.

### opus-5: measured, then rejected

The freed budget was nearly spent on `anthropic/claude-opus-5` for the decision
call (registry `agent_loop_hard`: 96% of the top bench score at half the output
price of `fable-5.1` / `gpt-6-astra`). It was measured rather than modelled — a
real 09-16 prompt replayed twice through the gateway at effort `high`, the
second pass confirming the 1h cache breakpoint:

| | opus-5, measured | glm-5.3 |
|---|---|---|
| Cached system block | 8,996 tok @ $0.50/1M read, $6.25 write | @ $0.12/1M read |
| Uncached input | 4,916 tok @ $5/1M | @ $0.70/1M |
| Output | 1,379–1,516 tok @ $25/1M | @ $2.20/1M |
| Per decision | **$0.117 cold / $0.065 warm** | **~$0.010** |
| Latency | 20–23 s | sub-second TTFT |

Monthly, with the analyst already off: ~$24 at the scheduled 9 calls/day, ~$45
at 15/day, ~$81 at 25/day — against ~$3–8 for the same traffic on GLM. **Owner
decision: not at this account size.** §12's equity note is the context — at $81
of equity, $80/month of inference is not a rounding error, and nothing measured
says the decision quality gap is worth it. The 09-16 reasoning for GLM stands
unchanged.

What the exercise did establish, and is worth keeping: the decision prompt is
**13,912 tokens**, 8,996 of them in the cacheable system block, and the 1h
breakpoint works exactly as designed.

### Trap found on the way, for whoever changes the model next

`SWING_AI_PROVIDER` is **set in Production** (Secret, 58 days old, value not
readable from the CLI). `resolveSwingAiDialect` honours it *over* the model id,
so changing `DEFAULT_AI_MODEL` to an Anthropic id while that var still reads
`openai`/`responses` routes the tick to the Responses client and runs
`FALLBACK_AI_MODEL` instead — the old model, with nothing in the logs saying
so. Any future swap across dialects must set or clear that var in the same
deploy, and confirm the vendor on the decision row afterwards. It is also the
no-deploy rollback in the other direction.

Nothing to do for this deploy: the model did not change, so the dialect did
not either.

### How to read it

- Changeover = the deploy timestamp; split every read at it (§9.6). One change
  only, and it IS a prompt change: the lesson block and its doctrine paragraph
  leave the user turn on the ~10% of ticks that carried them.
- Post-mortems stop accruing, so the weekly digest's verdict counts freeze.
  `right_to_skip` / refusal investigations no longer answer "was that skip
  right" — that question moves to the lab's skip counterfactual.
- Expect the AI-call count to fall to roughly the scheduled 9/day plus wakes,
  with no analyst traffic behind it — and the monthly bill with it, by the
  ~$21–43 the analyst was costing.
- Decision rows do not persist `usage`, so the table above had to be measured
  out of band. Storing the `usage` the provider already returns
  (`SwingDecisionCallResult`) on the decision row would make the next model
  question a query instead of a probe.

---

## 14. Two defects found auditing call volume — fixed 2026-09-17

Both surfaced while counting AI calls per day in `swing.tick_log`; neither was
visible from the dashboard.

### The drain ignored the analyst switch

`SWING_POSTMORTEM_MODE` gates **enqueue** (§13). The cron drain
(`/api/swing/postmortem-drain`, every 15 min) claimed and ran any mature queued
row regardless — and four refusal investigations were already sitting queued
when the switch flipped, created 09-16 18:48–19:46 and maturing 12 h later. They
would have made four analyst calls a few hours after the analyst was turned off,
on a cron, with nothing in the dashboard saying so.

The drain now returns `postmortem_mode_off` and claims nothing. Rows are left
**queued, not skipped**: turning the analyst back on resumes them where they
stand. `/api/swing/postmortem` is untouched — admin-only, and the way an
operator asks for one analysis on purpose. Contract test:
`test/contract/postmortemDrain.contract.test.ts` (any DB query or outbound host
fails it).

### An unsizeable trim killed the tick

`closeCapitalPosition` threw `Cannot resolve partial close size for Capital
position` when a trim quantized against `minDealSize` to zero or to the whole
position. It threw from inside the exec path, so it escaped to the top-level
handler: the tick died, no decision row was written, and the model's stop
management in that same decision was lost with it.

**COPPER, 09-16: 36 consecutive ticks over 2 h 41 m (10:01–12:42 UTC) on an open
position, every one of them lost.** The position was only released at 12:47,
when the model happened to ask for a 100% close — which quantizes fine. 39
occurrences all-time since 08-13 (GBPUSD twice, COPPER otherwise); this is the
same failure class as the Bitget 40762 tick-killer fixed on 07-24.

Root cause is structural at this account size: §12's equity note is $81, so
every Capital position sits at or near the venue minimum and **most trims are
inexpressible**. Owner's rule, 2026-09-17:

| Requested trim | Behaviour |
|---|---|
| ≥ 50%, unsizeable | **Escalates to a full close** — the intent is decisively risk-off and all of it is the only version the venue can express. `note: trim_escalated_to_full_close` so the log does not read as a model-requested 100% exit. |
| < 50%, unsizeable | **Dropped.** Position runs on, `placed: false`, `note: trim_below_venue_min_size`. |

Either way the tick survives, and the bracket amend the caller applies *before*
the trim still lands — which is the part that matters for a position that would
otherwise go unmanaged. Unit tests in `test/unit/capital.test.ts`.

**Not done:** the model is never told that partial exits are unavailable on an
instrument, so it will keep asking. A measurement in the prompt ("minimum deal
size on this epic equals your position size") is the honest fix, and it belongs
with the §13 style — state the measurement, let the model draw the conclusion.

### Also seen, not fixed

A **402 Payment Required** from the gateway killed 44 ticks between 03:00 and
09:00 UTC on 09-16 (`stage='ai_unavailable'`, "A positive credit balance is
required for all requests, including BYOK"). The health flag did its job — the
drain and the ticks backed off rather than hammering — and it cleared on its
own, but nothing alerts on it: six hours with no AI at all read as a quiet day.

---

## 15. Swing geometry made to stick — applied 2026-09-23

### Why

A week after §11–12 the positions still looked like scalps. Four trades had
closed since the 09-16 deploy (8 `swing.positions` rows; the extras are
Capital position splits). That is far too few to measure anything, but the
model's own reasons showed the mechanism, and the code confirmed each step:

| Trade | Entry fired by | Stop | TP | TP/SL | Then | Hold |
|---|---|---|---|---|---|---|
| BTCUSDT long 09-20 | wake band 15:11 | 3.19% (3 ATR) | 1.5% | 0.47 | next look: stop 78200 → 80400; TP filled | 10h |
| DE40 short 09-22 | wake band 07:31 | 1.18% | 0.95% | 0.8 | stop tightened 30 min later; own band fired → CLOSE | 1.3h |
| GBPUSD short 09-21 | wake band 18:48 | 0.55% | 0.31% | 0.56 | CLOSE at the next 4H close (failed break) | 9h |
| US100 long 09-17 | wake-fired check 14:20 | 1.76% | 2.9% | 1.6 | 35% trim + stop to breakeven at the first daily look | 18–26h |

1. **The floored stop was treated as borrowed.** BTC at entry: "the 3-ATR
   floor forces the exchange stop to 78200 as the catastrophe leg". Nine hours
   later: "The 78200 stop is a 3-ATR catastrophe default and far too loose…
   tighten it to 80400". The 1-ATR amend floor (§11) allowed that on the very
   next look, so the entry floor held for one look only.
2. **Targets never widened.** The prompt said "Targets have no floor… no
   required ratio to the stop", so the model kept its natural 1–1.7 ATR
   targets against 3-ATR stops. At TP/SL 0.5 a trade needs about a 67% hit
   rate to break even.
3. **Wake bands carry the intraday looks.** None of the four entries happened
   at the scheduled hour. The in-position band prose still described a 4H-close
   schedule under the daily cadence.
4. **Bug:** the wake-watcher read `SWING_INPOS_EMERGENCY_MOVE_ATR` with its own
   default of 1.5 while analyze defaulted it to 3 (§11 changed only one of the
   two). Each look it fired re-armed it.

### What changed

| Lever | Before | After | Where |
|---|---|---|---|
| Amend stop floor | 1 ATR from price | **= entry floor (3)** | `AMEND_SL_MIN_ATR` (env `SWING_AMEND_SL_MIN_ATR`) |
| Entry target floor (new) | none | **TP ≥ 1× the entry's own stop distance**, refused (not widened) | `ENTRY_TP_MIN_R` (env `SWING_ENTRY_TP_MIN_R`), `entry_dropped='entry_target_below_stop_ratio'` |
| Watcher emergency look | 1.5 ATR (bug) | **3**, one shared constant | `IN_POSITION_EMERGENCY_MOVE_ATR` in decisionConfig.ts |
| Prompt | "catastrophe" stop, "targets have no floor", 4H-close band prose | floor = the minimum width of an invalidation; target floor stated; bands relative to the daily look | `lib/swing/prompt.ts` HARD CONSTRAINTS + bracket section |
| R window | since 2026-09-16 | **since 2026-09-24** | `R_SAMPLE_SINCE_MS` |

The target floor is **not** the `ENTRY_TP_MIN_ATR=2` removed on 09-02. That
one was a fixed ATR distance, and a violating target was widened to the
fallback, so asking for a near target produced a far one. This one is a ratio
to the model's own stop and refuses the trade. A range trade with a stop
proportionate to its target stays expressible; what is refused is a small
target carrying a wide stop.

Harness: `SWING_ENTRY_TP_MIN_R` is pinned to 0 (fixtures carry sub-1R
targets); `decisionRules.targetFloorEnabled.test.ts` tests it switched on.
`decisionConfig.prodDefaults.test.ts` asserts all three defaults.

### Not changed

Wake bands, flat and in-position, stay on. Turning them off (entries and
exits only at the daily look plus the bracket) was the cleaner "real swing"
option, but it removes the entry timing the model currently relies on. It is
the next step if holds stay short.

### How to read it

- The window restarts at the deploy (only 4 closes were lost). Split every
  read there.
- Expect fewer entries: `entry_target_below_stop_ratio` refusals where the
  nearest structure sits under 1R. Count them in `swing.decisions` before
  calling the drop a problem.
- Expect `sl_below_amend_floor_dropped` notes on in-position looks: the model
  still trying to tighten. If they dominate, the prompt wording is not landing.
- Hold time and TP/SL at entry are the geometry to watch; avg R needs the
  sample size §10 describes.
