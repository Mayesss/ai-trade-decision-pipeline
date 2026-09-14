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

---

## 9. Horizon expansion — PARKED, notes kept

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
