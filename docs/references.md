# References

Sources the alpha-lab design rests on. Kept in-repo so a later session can
check a claim without re-deriving it, and so nothing load-bearing is cited from
memory.

**Two tiers, deliberately separated.** A project whose product is calibrated
honesty cannot cite sloppily — see §4.

---

## 1. Verified in-session (fetched and read, date noted)

### 1.1 Primary papers — LLM-driven alpha discovery

**XAlpha: A Memory-Driven AI Quant Researcher for Hypothesis-to-Code Alpha
Discovery** — arXiv `2607.08332`. Liu, Fu, Wang, Liu (HKU School of Computing
and Data Science; Grace Investment Machine). *Read 2026-09-12.*

- Architecture: Report-to-Memory Absorption layer (A/B/C taxonomy) → Macro
  Brain (research direction, archetype selection) → Micro Brain (factor code,
  AST gate, tri-alignment judge, mutation/crossover) → Cross Brain (GOOD/BAD
  feedback, memory update).
- Universe CSI300, daily OHLCV. Target: 10-day forward open-to-open return.
  Split: train 2011–2020, validation 2021, test 2022–2025.
- Reported: IC 0.0619, RankIC 0.0748, ICIR 0.3703, ARR 17.95%, IR 1.5368.
- Selection score: `0.70·α_train + 0.30·r_train,OOSevo · d_complexity`;
  library admission threshold 0.65; final library max abs correlation < 0.60.
- Cost: ~15s/factor, ~16 min/generation, ~3h/mining cycle, 2× H100.
- **No** Deflated Sharpe, PBO, or family-wise error control. No transaction
  costs in the portfolio implementation (their stated limitation).

**AlphaSeek: Trajectory-Level Self-Iterative Factor Mining Framework for
Multi-Source Financial Data** — arXiv `2608.13913`, ICIC 2026 (Toronto, July
2026); also Springer LNCS `10.1007/978-981-92-3400-4_22`. *Read 2026-09-12.*

- Optimisation unit is the whole research trajectory
  `τ = (s₀, a₀, …, sₙ)` — hypothesis text, factor set, code, backtest results,
  metrics, feedback. Terminal reward `R(τ) = ℒ(f_τ(X),y) − λℛ(f_τ)`.
- Evolution operators: parallel direction expansion, mutation (freezes nodes
  before `s_k`), crossover across high-return trajectories.
- Regularisation: complexity (symbolic length, parameter/feature count),
  redundancy (AST largest-common-isomorphic-subtree similarity), consistency
  (`𝒞(h,d,f) = α·C₁(h,d) + (1−α)·C₂(d,f)`, failure triggers regeneration).
- Split: train 2016–2020, validation 2021, test 2022–2025.
- Reported on CSI300: ARR 8.28%, IR 1.29, MDD 6.28%, IC 0.0454. CSI500 transfer
  is zero-shot, not forward validation.
- **Own words:** the "mechanism for controlling multiple testing bias across
  trajectory iterations is not specified."

> **How we use these.** Take: the AST gate (extended to mechanically forbid
> lookahead), the tri-alignment judge, the A/B/C archetype memory, and
> correlation-gated library admission. Reject: out-of-sample terms inside the
> selection score, and backtest metrics as the evolutionary gradient. Both are
> cross-sectional equity miners whose results rest on breadth across 300 names
> — the architecture transfers, the statistics do not. Full reasoning in
> `alpha-lab-plan-2026-09-12.md` §1.

### 1.2 Platform limits the architecture depends on

| source | what we rely on | checked |
|---|---|---|
| [Vercel Functions limits](https://vercel.com/docs/functions/limitations) (docs rev. 2026-08-24) | Pro: 300s default / **800s max** / 1800s extended beta; 4 GB / 2 vCPU; Python bundle 500 MB (5 GB large-functions beta); request/response body **4.5 MB**; concurrency to 30,000; ephemeral filesystem | 2026-09-12 |
| [Neon network transfer](https://neon.com/docs/introduction/network-transfer) | Launch/Scale: **500 GB/month included, $0.10/GB overage**; Free: 5 GB then suspend. Top egress causes: large result sets, high-frequency repeated queries in serverless, `pg_dump`, logical replication | 2026-09-12 |
| [Cloudflare R2 pricing](https://developers.cloudflare.com/r2/pricing/) | **Egress free on all storage classes.** Standard $0.015/GB-month; Class A (mutating, **incl. `ListObjects` / `ListBuckets`**) $4.50/M; Class B (`GetObject`, `HeadObject`) $0.36/M. Free tier 10 GB + 1M Class A + 10M Class B. Infrequent Access: 30-day minimum billed duration + $0.01/GB retrieval. Multipart uploads cost multiple Class A ops. Deletes and authenticated failures are free; usage rounds up | 2026-09-12 |
| [Cloudflare agent skills for Claude Code](https://developers.cloudflare.com/agent-setup/claude-code/) | Install via `/plugin marketplace add cloudflare/skills` then `/plugin install cloudflare@cloudflare`; includes a `wrangler` skill covering R2. MCP server optional | 2026-09-12 |
| [Neon egress optimizer skill](https://neon.com/docs/introduction/network-transfer#use-the-egress-optimizer-agent-skill) | `npx skills add neondatabase/agent-skills -s neon-postgres-egress-optimizer` — scans for `SELECT *`, missing pagination, high-frequency static queries | 2026-09-12 |

## 2. Our own measurements

These are first-party and reproducible; prefer them over any external claim
about this system.

- `docs/decision-engine-edge-audit-2026-09-11.md` — 453 live directional
  decisions, no detectable information at any horizon; effective n 94 against a
  nominal 435; ~4,900 observations needed to resolve a 0.10 ATR/trade edge at
  |t| > 3.
- Same doc §6 — R was divided by the risk *budget* while the exposure cap
  overrode it on essentially every trade; denominator 5–16× too large.
- `.study/12-trial-budget.mjs` — expected best t-statistic under a pure null by
  trial count: 100 → 2.51, 1,000 → 3.24, 10,000 → 3.86.
- `.study/13-xalpha-cadence.mjs` — at XAlpha's stated cadence (~704 factors per
  mining cycle) the expected best t under the null is **3.14**, with
  P(best t > 3) = **61.2%**.
- `.study/11-history-probe.ts` — available 4H history: Bitget to 2023-01-17
  (~8,000 bars/symbol); Capital ≥ 2 years (3,197 bars for GOLD at 730d).
- Egress projection — 128,000 bar-rows over the PG text protocol: ~18 MB
  (6 columns) to ~61 MB (20 columns) per full read; ~60 GB per 1,000-job sweep.

## 3. Methodology — from background knowledge, **verify before external use**

Load-bearing for the design's reasoning, but **not** fetched in-session. Check
the primary source before any of this appears in a public document.

| idea | attributed to | where we use it |
|---|---|---|
| Fixed-fractional risk, 1–2% ceiling | Carver, *Systematic Trading* | `riskSizing.ts` default; week-one review §12 |
| Backtest overfitting; Deflated Sharpe Ratio; Probability of Backtest Overfitting (CSCV) | Bailey & López de Prado; Bailey, Borwein, López de Prado, Zhu | plan §3.1, §7 — the trial ledger exists to make DSR computable |
| Cross-validation and backtest dangers in finance | López de Prado, *Advances in Financial Machine Learning* (~ch. 10–14) | holdout custody, plan §3.2 |
| The |t| > 3.0 bar for a claimed new factor | Harvey, Liu & Zhu, *…and the Cross-Section of Expected Returns*; Harvey & Liu, *Backtesting* | audit §4; plan §1.2 |
| Fundamental law of active management, `IR ≈ IC × √breadth` | Grinold & Kahn, *Active Portfolio Management* | plan §1.1 — why CSI300 results do not transfer to 25 instruments |
| Adaptive data analysis; the reusable holdout | Dwork, Feldman, Hardt, Pitassi, Reingold, Roth (Science / STOC 2015) | invariant 3 and plan §3.4 — why the feedback arrow is the dangerous edge |
| Reality Check / Superior Predictive Ability / StepM | White (2000); Hansen (2005); implementations in Kevin Sheppard's `arch` | plan §6 stage 4 |
| "Efficiently inefficient" markets | Pedersen / AQR | framing for why a filter can be defensible where prediction is not |

## 4. Citation discipline

1. Anything in §3 is **unverified** here. Fetch the primary source before it
   leaves the repo.
2. Numbers quoted from a vendor's docs carry the date checked — pricing and
   platform limits move.
3. Statistics about third parties (fund underperformance rates, factor decay,
   and similar) are **not** recorded in this file precisely because they were
   never verified. Do not reintroduce them from a chat transcript.
4. When our own measurement and an external claim disagree, ours wins for this
   system and the disagreement gets written down.
