# Alpha-discovery lab: plan

**2026-09-12.** Successor plan to `docs/decision-engine-edge-audit-2026-09-11.md`,
which found no detectable information in the live decision engine and a sample
too small to have detected a realistic amount of it. This document is the
architecture that follows from that result.

Sources read for this plan:
- XAlpha, *A Memory-Driven AI Quant Researcher for Hypothesis-to-Code Alpha
  Discovery* — arXiv 2607.08332 (Liu, Fu, Wang, Liu; HKU / Grace Investment Machine)
- AlphaSeek, *Trajectory-Level Self-Iterative Factor Mining Framework for
  Multi-Source Financial Data* — arXiv 2608.13913 (ICIC 2026)
- Vercel Functions limits, docs revision 2026-08-24

## 1. What the papers change

### 1.1 What they actually are

Both are **cross-sectional equity factor miners on CSI300 daily OHLCV**. That is
a different statistical setting from ours, and the difference is the whole story.

| | XAlpha / AlphaSeek | this project |
|---|---|---|
| universe | 300 A-share names | 25 instruments, 4 asset classes |
| bar | daily | 4H |
| target | 10-day forward cross-sectional return | outcome of a directional entry |
| metric | IC / RankIC | R multiple, ATR-normalised return |
| split | train 2011/2016–2020, val 2021, test 2022–2025 | none until 2026-09-11 |

Reported: XAlpha IC 0.0619 / RankIC 0.0748 / IR 1.5368 / ARR 17.95%.
AlphaSeek IC 0.0454 / IR 1.29 / ARR 8.28% / MDD 6.28%.

An IC of 0.05 is a per-name correlation explaining ~0.25% of variance. It
becomes an IR above 1 only because it is applied across 300 names, 25 times a
year — Grinold's law, `IR ≈ IC × √breadth`. **Their results are a breadth
story, not a prediction story.** Nothing in either paper suggests an LLM can
forecast one instrument's direction, which is what our engine was asked to do
and what the 09-11 audit found it could not.

### 1.2 The shared blind spot

Neither paper implements trial accounting. Explicitly absent from both:
Deflated Sharpe Ratio, Probability of Backtest Overfitting, family-wise error
control, or any count of how many candidates were evaluated before the winner.
AlphaSeek's own text concedes the "mechanism for controlling multiple testing
bias across trajectory iterations is not specified."

What they regularise instead is *complexity, redundancy and semantic
consistency* — all worth having, none of which bounds the false-discovery rate.

XAlpha's stated cadence: ~15s per factor, ~16 min per generation, ~3h per mining
cycle — that is 64 factors per generation and ~704 per cycle. Simulated in
`.study/13-xalpha-cadence.mjs`, **under a pure null with no edge whatsoever**:

| trials | E[best t] | P(best t > 3) | P(best t > 2) |
|---|---|---|---|
| 64 (one generation) | 2.35 | 8.4% | 77.2% |
| 704 (one cycle) | **3.14** | **61.2%** | 100% |

So a single mining cycle on pure noise clears t > 3 more often than not. Both
systems operate squarely inside the regime where the top candidate reaches
conventional significance by chance alone. (Real candidates are correlated, so
effective N is below nominal N and the true figures sit somewhat lower — the
direction is unaffected.) Their held-out test windows are the
only thing standing between that and a false claim, and the test window is
reported once per paper with no correction for how many architectures were
tried against it.

A secondary signal: two 2026 papers, same benchmark, same test window,
both claiming SOTA with different winners. When a field converges on one
public test period, the field's collective trial count is the number that
matters and nobody is tracking it.

### 1.3 Take / reject

**Take:**

| idea | why |
|---|---|
| **AST gate on generated code** (XAlpha) | Static inspection before execution. We can make it mechanically forbid lookahead — no negative shifts, no forward indexing, no access to rows beyond `t`. This turns the single most common backtest bug into a compile error. Highest-value steal on the list. |
| **Tri-alignment / consistency judge** (both) | Check hypothesis text ↔ code ↔ stated economic rationale agree before the factor is allowed to run. Cheap guard against a generator that writes momentum code under a mean-reversion story. |
| **A/B/C research-archetype memory** (XAlpha) | Structured mechanism families distilled from literature, reused as hypothesis priors. This is the concrete form of "generate economically plausible hypotheses first". |
| **Correlation-gated library admission** (both; XAlpha max abs corr < 0.60) | Exactly the mechanism our lessons loop lacked — it minted ~20 near-duplicate lessons because nothing deduped against the whole library (`week-one-review` §3). Same bug, published solution. |
| **Compact GOOD/BAD feedback** (XAlpha Cross Brain) | A deliberately narrow feedback channel. See §3.4 — we narrow it further. |
| **Complexity + redundancy penalties** | Cheap priors against overfit expressions. |

**Reject:**

| idea | why |
|---|---|
| OOS terms inside the selection score (XAlpha: `0.70·α_train + 0.30·r_train,OOSevo`) | Selecting on held-out performance is how a holdout stops being one. §3.2. |
| Backtest metrics (ARR/IR/MDD/Calmar) as the evolutionary gradient (AlphaSeek `R_n`) | Optimises the most gameable, highest-variance quantity available. §2. |
| Unbounded generations | Every generation spends sample. §3.1. |
| Their universe assumptions | 300 liquid names with daily rebalancing. We have 25 instruments. Breadth must be earned differently. |

**Net: the papers change the architecture, not the conclusion.** They give a
good skeleton for hypothesis generation and a validated set of code-quality
guards. They do not solve — and barely acknowledge — the problem this lab
exists to solve. Adopting either wholesale imports the flaw.

## 2. The central reframe: measure factors, not strategies

This is the biggest practical change and it comes directly from the papers.

The 09-11 audit tested 453 directional decisions and got an effective n of 94.
The factor framing instead scores **every instrument at every bar** and measures
rank correlation with forward returns:

| framing | observations available |
|---|---|
| strategy / entry decisions (current) | 453 (effective 94) |
| factor / IC across the panel | ~128,000 bar-observations, ~21,000 symbol-days |

Measured available history (probe, 2026-09-11): Bitget 4H back to 2023-01-17
(~8,000 bars/symbol, 3.7y); Capital 4H at least 2 years (3,197 bars for GOLD at
730d, no wall hit). 10 crypto + 15 CFD symbols.

To resolve an IC of 0.05 at |t| > 3 needs n ≈ (3/0.05)² ≈ 3,600 independent
observations. After a correlation haircut (25 instruments share macro drivers)
the panel is worth perhaps 2,000–4,300 effective observations. **That is
roughly one honest answer** — enough to test a handful of pre-registered
hypotheses properly, and nowhere near enough for a 700-factor mining cycle.

Consequences:
- A hypothesis is **a score function over (instrument, time) → real number**,
  not an entry rule. Entry rules come later, only for survivors.
- Primary metric is **rank IC against forward volatility-normalised return**,
  with the same day-clustered / block-bootstrap inference the audit used.
- Sharpe, ARR and MDD are computed **only** for candidates that already cleared
  IC on held-out data, and only with costs attached.

## 3. Non-negotiable foundations

These come before any generator. Each one exists because of a specific,
measured failure.

### 3.1 Trial ledger

Every hypothesis gets a row **before** it runs: spec hash, natural-language
statement, economic rationale, data window, fold assignment, family, generator
provenance (human / LLM / mutation-of-X), and the trial budget it draws from.

Nothing may be evaluated without a ledger row. A result whose trial count is
unknown is uninterpretable — that is the entire lesson of §1.2.

### 3.2 Sample custody, with the model cutoff as a boundary

Three slices, enforced by the system rather than by discipline:

| slice | period | use |
|---|---|---|
| **discovery** | history up to the LLM's training cutoff | generation + filtering only. Never confirmation. |
| **clean holdout** | post-cutoff → today | confirmation. Guaranteed un-memorised. |
| **forward** | today onward | final confirmation; grows daily |

The middle slice is a structural advantage specific to an LLM lab: anything
after the generator model's training cutoff cannot have been memorised by it.
Pre-cutoff backtests are contaminated twice over — the model has seen the price
history *and* read the literature on whatever factor it proposes.

The holdout is split into G disjoint sub-slices, one per generation, never
reused. Access is gated in code, not by convention.

### 3.3 Synthetic null injection (negative controls)

**The calibration instrument, and the part no published system has.**

Continuously inject hypotheses that are known to be worthless — shuffled
labels, phase-randomised series, factors built on scrambled timestamps — into
the same pipeline, indistinguishable from real candidates at evaluation time.

The lab's headline output is then not a pile of alphas but a **measured
false-discovery rate**: if 5 of 100 real hypotheses survive and 5 of 100
synthetic nulls also survive, the lab has demonstrated zero skill, and it says
so out loud. If real survivors materially exceed null survivors, that gap is
the only defensible claim the lab can make.

This is also the sellable artifact from the product discussion: "our discovery
process has a measured FDR" is a claim no strategy generator on the market
makes.

### 3.4 A deliberately narrow feedback channel

The dangerous edge in both papers is `validation → feedback → next hypothesis`.
AlphaSeek is explicit: it "dynamically selects the search direction ... according
to the historical conditional expected value" of backtest performance. That is
gradient ascent on held-out data.

Rules here:
- Feedback crossing into generation is **qualitative only** — a failure
  diagnosis in words ("effect present only in high-vol regimes", "vanished
  post-2024"), never a score, rank, or metric.
- Generation budget declared up front; each generation consumes its own
  holdout slice.
- The generator never sees the leaderboard.

## 4. Where the backtest engine goes

**Correction to my previous advice:** I said Vercel was the wrong place for
this. Having checked the actual limits, that was too categorical — Vercel is a
reasonable home for the engine through at least the first two stages.

Measured limits (docs revision 2026-08-24):

| | Hobby | Pro |
|---|---|---|
| max duration | 300s (default and max) | 300s default, **800s max**, 1800s extended (beta) |
| memory | 2 GB / 1 vCPU | 4 GB / 2 vCPU |
| Python bundle | 500 MB uncompressed (5 GB via large-functions beta) | same |
| request/response body | 4.5 MB | 4.5 MB |
| concurrency | up to 30,000 | up to 30,000 |

Against our job shapes:

| job | cost | fits? |
|---|---|---|
| one hypothesis, full panel (128k bars, vectorised) | seconds; ~20 MB working set | comfortably |
| walk-forward, k folds | seconds–minutes | yes |
| sweep over many hypotheses | embarrassingly parallel | **fan out** — one invocation per (hypothesis, fold); 30k concurrency is a genuine strength here |
| LLM generation loop | long-running orchestration | cron-driven queue drain, or Vercel Workflows |

The constraints that actually bite, and their mitigations:

- **4.5 MB payload** — never return results inline. The worker writes to Neon
  and returns a job id. This is the right design regardless.
- **Ephemeral filesystem** — no local data cache. Keep the bar panel as a Neon
  table (or object storage) and read per invocation; 128k bars is ~20 MB.
- **500 MB bundle** — `polars` + `numpy` + `scipy` fits. Adding `statsmodels`
  and `arch` makes it tight; we already hand-rolled block bootstrap, clustered
  SEs and permutation tests in `.study/`, so port those rather than pulling the
  heavy stack. Large-functions beta is the escape hatch.
- **Fan-out vs Neon connections** — 1,000 concurrent invocations must use the
  **pooled** URL, and per `week-one-review` §9 must never issue a session-level
  `SET`. Cap concurrency deliberately.
- **Cost model** — billed on active CPU, which is exactly what batch compute
  consumes. Fine at our cadence; a wrong fit at scale.

**Migration trigger** (write it down now): move to a persistent worker
(Modal / Fly / Railway) when any single job needs >800s, when a sweep's monthly
active-CPU bill exceeds a small always-on container, or when we need a warm
in-memory panel across jobs.

**Deploy isolation — the decisive argument.** The research engine must be a
**separate Vercel project in the same repo**, not functions added to the
existing Next.js app. A research deploy must never be able to redeploy the live
trader, and a Next.js project routes `/api/*` to Next.js, so mixing Python
functions in-project means fighting the router. Two projects, one repo, one
Neon.

**Communication: Postgres, not HTTP.** Ledger, job state and scalar results live
in Neon, already joinable with the live decision log. We already have a proven
durable queue to copy — `claimSwingPostmortemById` (`lib/swing/pg.ts:1463`):
atomic `queued → running` claim, attempt counting, 15-minute stale reclaim for
dead workers, hardened after the 09-08 incident. HTTP is used only to nudge a
drain; the contract is the table.

**But bar data does NOT go in Neon.** See §4.1 — this is the constraint that
shapes the whole data layer.

### 4.1 Egress: Neon is a control plane, never a data plane

An earlier draft of this plan put the bar panel in a `research.panel` table.
That is a costly mistake and we have already paid for it once.

Neon bills **network transfer** on all outbound data through its proxy:
500 GB/month included on Launch/Scale, **$0.10/GB** thereafter (Free: 5 GB, then
suspend). Measured projection for the panel (128,000 bar-rows, `.study` sizing):

| | per full panel read | per 1,000-job sweep |
|---|---|---|
| 6 columns over the PG text protocol | ~18 MB | ~18 GB |
| 20 columns | ~61 MB | ~60 GB |

One sweep consumes 12% of the monthly allowance. Fifty sweeps in a development
month — which is a normal iteration count for stages 3–5 — is ~3 TB, roughly
$250 in overage. That is the shape of the ~€400 month already incurred on the
scalp subsystem, which stored candles as `scalp_candle_history_weeks`
(`candles_json` JSONB) and bulk-read them; commits `2155acb` ("kv cache and less
neon data transfer") and `21bf5c3` ("merge new weeks instead of reload") were
the remediation. **The research lab must not recreate that table under a new
name.**

Three planes, split by access pattern:

| plane | home | holds | egress |
|---|---|---|---|
| **control** | Neon Postgres | hypotheses, trials, budgets, jobs, scalar results, live decision log | ~3 MB per 1,000-job sweep |
| **data** | Cloudflare R2 (S3-compatible) | bar panel + features, partitioned Parquet | **free — R2 charges no egress on any storage class** |
| **dev** | local disk | the same Parquet | zero |

R2 pricing: $0.015/GB-month storage, Class B reads $0.36/M, with a free tier of
10 GB storage and 10M Class B reads — the panel is tens of MB, so storage is
effectively free and reads are rounding error. (S3 in `us-east-1` alongside
Vercel's `iad1` is an alternative via free same-region transfer, but R2's
unconditional zero egress avoids coupling the design to a region.)

Billed-egress reduction: **~60 GB → ~3 MB per sweep**, about four orders of
magnitude.

Supporting rules:

1. **Remove the affordance, don't rely on discipline.** There is no
   `research.panel` table in the schema (§5). A bulk bar read from Neon is
   unrepresentable rather than merely discouraged.
2. **Columnar projection.** A factor needs 3–5 of ~20 columns; Parquet reads
   only those, cutting bytes and parse time ~4×.
3. **Development never touches the cloud.** Stages 3–5 iterate against the local
   Parquet copy. Only registered, budgeted runs go to R2/Vercel. This alone is
   the largest practical saving and costs nothing to adopt.
4. **Aggregate in SQL.** Anything that must come out of Neon for analysis —
   the live decision log, post-mortems — is aggregated server-side and returns
   scalars, never `SELECT *`. `lib/swing/pg.ts` already projects away
   `prompt_json` for this reason; keep that discipline.
5. **Account guardrails.** `docs/neon-egress.md` already defines alert
   thresholds (50 / 90 / 120 GB). Raise them to suit the 500 GB allowance and
   keep the organisation spending limit set.
6. **Run Neon's egress optimizer skill** against the repo before stage 6:
   `npx skills add neondatabase/agent-skills -s neon-postgres-egress-optimizer`.
   It scans for `SELECT *`, missing pagination and high-frequency static
   queries.

This also settles fan-out vs warm worker: with the panel in R2, fan-out costs
nothing in egress, and the only residual waste is re-parsing Parquet per
invocation (Vercel bills active CPU, so this is cents, not euros). A warm
persistent worker that parses once stays the escape hatch if sweeps grow large
enough for that parse cost to matter.

## 5. Architecture

```
repo/
  lib/, pages/, components/      # existing Next.js app (live trader + dashboard)
  research/                      # NEW — separate Vercel project
    api/                         #   Python functions (claim → run → write)
    engine/                      #   vectorised backtester + inference
    specs/                       #   hypothesis DSL + AST gate
    panel/                       #   panel builder -> Parquet -> R2 (+ local mirror)
```

**Control plane** (`research` schema, same Neon database — small rows only):

- `research.hypotheses` — id, spec_hash (unique), statement, rationale, family,
  provenance, generator_model, created_at, **is_synthetic_null** (never exposed
  to the evaluator)
- `research.trials` — id, hypothesis_id, budget_id, fold_id, registered_at,
  status. One row per evaluation, written *before* the run.
- `research.jobs` — the queue: status, attempts, claimed_at, updated_at,
  payload. Mirrors the postmortem claim pattern exactly.
- `research.results` — trial_id, ic, rank_ic, t_clustered, p_block, n, n_eff,
  cost-adjusted metrics, curve (small array)
- `research.budgets` — slice, trials_allowed, trials_spent, opened_at, closed_at
- `research.panel_versions` — **metadata only**: snapshot_id, R2 object key,
  row count, symbol list, date range, content hash. Never the bars themselves.

**Data plane** (R2, Parquet, partitioned by symbol and year):
`panel/<snapshot_id>/<platform>/<symbol>.parquet`, immutable once written, with
a local mirror for development. Deliberately **no `research.panel` table** — a
bulk bar read from Neon must be unrepresentable, not merely discouraged (§4.1).

## 6. Build order

| stage | what | rough effort |
|---|---|---|
| **0** | Freeze scope. Retire lessons loop, wake machinery, prompt decision logic from the research path — but leave the live trader deployed and running (§8). | 1 day |
| **1** | `research.hypotheses` + `trials` + `budgets`; custody gate in code. No engine yet. | 2–3 days |
| **2** | `research.jobs` queue cloned from the postmortem claim pattern; Python worker skeleton on a second Vercel project; end-to-end "hello, claimed a job, wrote a row". | 2–3 days |
| **3** | Panel builder: 128k bars + the feature vocabulary from `signals.ts` / `waveGeometry.ts` → Parquet → R2, with a local mirror. Registers a row in `research.panel_versions`. | 3–4 days |
| **4** | Vectorised IC engine + inference ported from `.study/04`–`09`. Costs modelled from our 282 real fills. | 4–5 days |
| **5** | Hypothesis DSL + **AST lookahead gate**. Hand-write 10–20 hypotheses. Run the first honest budget. | 1 week |
| **6** | Synthetic null injection + FDR reporting. | 3–4 days |
| **7** | LLM generator with archetype memory and the narrowed feedback channel. | 1–2 weeks |
| **8** | Execution automation. **Gated** on §7 kill criteria. | — |

Stages 1–6 are the product. Stage 7 is the accelerator. Stage 8 is only
justified if stage 6 produces a favourable number.

## 7. How we know the lab works

Pre-registered, before any of it is built:

1. **FDR gap.** Real-hypothesis survival rate must exceed synthetic-null
   survival rate by a margin significant at the declared level. If it does not,
   the lab has no skill and we say so.
2. **Holdout decay.** Median IC decay from discovery slice to clean holdout is
   reported for every survivor. A survivor whose IC halves out of sample is
   reported as such, not quietly dropped.
3. **Cost survival.** Any candidate is reported gross and net of measured costs.
   Neither paper does this; AlphaSeek lists it as a limitation.
4. **Trial count attached to every claim.** No number leaves the lab without
   the denominator.

Kill criterion: if after the first full budget the FDR gap is indistinguishable
from zero, the conclusion is that this universe is too small to support
discovery at this sample size — and the correct response is to widen breadth
(more instruments) rather than run more trials.

## 8. Open decisions for the owner

1. **Does the live trader keep running?** Recommendation: yes, at minimum size,
   reframed as a *data generator* rather than a profit centre. It is the only
   source of post-cutoff, un-memorised, timestamped decisions — the scarcest
   input the lab has. But it costs real money and the 09-11 audit says its edge
   is undetectable. Owner's call.
2. **Does the measurement freeze survive?** The 200-close target was already
   downgraded to a survival checkpoint. If the live system becomes a data
   generator, the freeze's purpose changes and §12 of the week-one review
   should be restated rather than silently abandoned.
3. **Vercel plan.** Pro confirmed (800s, 4 GB, 1800s extended in beta), so
   fan-out and long single jobs are both available.
4. **Object storage account.** R2 needs a Cloudflare account (free tier covers
   this panel comfortably). If that is unwelcome, the fallback is S3 in
   `us-east-1` next to Vercel `iad1`, or a local-only panel with no cloud sweeps
   until stage 6.
5. **Does `.study/` get committed?** §9 of the audit references it as the
   reproduction path, but the dumps contain prod decision data.
