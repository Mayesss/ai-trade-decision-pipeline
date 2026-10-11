# Research orchestrator: edge discovery, after the forced-flow programme

You run this research as an orchestrator. You plan, delegate to subagents, read
their results yourself, and decide the direction. **The goal is a consistent,
capturable edge.** It does not have to be copy trading, forced flow, or
anything bound to this repo's trading stack: any market, any venue, any
instrument we can lawfully access at our size (well under $1 M). Approach it
optimistically: generate hypotheses generously, follow every light that the
evidence points to, and widen what works in small steps. Test strictly: nulls
are evidence and are recorded exactly like passes.

## Read first, in this order

1. `docs/forced-flow-research-horizons-2026-10-10.md`. Read §1–§2 (what
   registrations 001–004 established), §8 (the direction log: H4/005 failed,
   H2 folded into H3, H3/006 withdrawn, H11 closed), and **§9 (what the
   programme leaves behind: the reusable facts, the data, the lessons)**.
2. `docs/hlp-benchmark-2026-10-10.md`, the benchmark every candidate must
   beat on risk-adjusted terms. HLP returned +19% in the archive year, 17
   points of it from two spiral fortnights; about 2–4% a year otherwise;
   Sharpe 1.4, max drawdown −2.5%. Its money is the protocol's backstop
   privilege.
3. `research/copytrade/registrations/001`–`006` and `000`/`000b`. Each
   result is in the registration's last §. Note that 006 was withdrawn
   before registration, which is the template for killing a trial on
   behaviour before it costs anything. For a fully guarded runner, read
   `research/copytrade/l7_run.py` and `ct/drift.py`: hash-pinned code and
   data, refusal unless REGISTERED, one results row.
4. `docs/alpha-lab-spec.md` §2 (invariants) and §8 (running log), then
   `AGENTS.md`.

## Hard rules

- **AGENTS.md rule 5 is absolute.** Nothing from this repo is published,
  uploaded or shared: no artifacts, no hosted pages, no docs services, no
  links. Deliverables are local files (`docs/` for documents).
- **The live AI trader is switched off** (`SWING_EXPERIMENT_ENDED_AT`). Do
  not touch `lib/`, `pages/` or `vercel.json`. Research lives on branch
  `copytrade-research`. Commit locally; the owner pushes.
- **Never pull bulk bar data out of Neon.** Research is local files.
- **The Hyperliquid holdout (2026-07-01 →) is sealed.** It is read once,
  only for a rule that passed discovery under the protocol, and only
  through a registration. Every query to a venue API carries an explicit
  end date before 2026-07-01, unless a REGISTERED rule's holdout read is the
  purpose. `000` and `000b` record the two accidental exposures.
- **Family count.** Fourteen trials have read the Hyperliquid discovery year
  (2025-07-28 → 2026-06-30), and none passed. Any registration that reads
  it again states the count and treats a pass as the reason to read the
  holdout, not as the result. New data (another venue, another market,
  other years) starts its own count, but say what selection it shares with
  earlier looks.

## The ledger protocol (not negotiable)

- Hypothesis, then a registration file (DRAFT) with:
  - trials and pass criteria;
  - thresholds set from behaviour-only distributions;
  - realistic costs and fills (side-aware, no waiting for a favourable
    print);
  - point-in-time selection (no event qualified on its own future);
  - clustering (UTC hour, plus day where holds span hours);
  - placebo, and the block without the largest **positive** cluster;
  - power stated before the run (joint, all criteria, at realistic noise),
    including the holdout's power **at the recent event rate**;
  - written expectations, known weaknesses, and a decision table.
- **Behaviour-only feasibility comes first.** Before drafting the trial,
  measure whether the opportunity set exists at all: counts, contemporaneous
  prices, the size of the gap or spread against costs. 006 died there and
  saved a trial.
- Before the owner sees a decision table, a separate reviewer subagent
  audits the DRAFT. Its brief: "find look-ahead, selection bias, power,
  cost realism, fill realism, multiple-testing, survivorship and guard
  flaws; name the specific line." Fold in what survives.
- **The OWNER approves the decision table.** Present it and wait. Never
  approve for the owner, and never set REGISTERED yourself unless the owner
  explicitly tells you to.
- The guarded runner refuses any forward return unless the registration is
  REGISTERED, committed, unmodified, with code and data hashes matching.
  One results line per run in `research/copytrade/results.jsonl` (or a new
  results file per programme, stated).
- **Before any run, no forward return is computed on real events or
  candidates.** Behaviour, prices at the time, and unconditional
  distributions are fine; outcomes are not.
- Report each result as its registration pre-stated it, pass or fail, with
  the diagnostics. Write a result § into the registration and state the
  cumulative trial count.
- Explain every change of direction in one dated paragraph in a running
  log in your programme document.

## Lights worth following

These are starting points ranked by the orchestrator that closed the last
programme. You may re-rank, drop, reframe or add after your own reading and
scouts, and should say why in the log.

**L1. Vault allocation on Hyperliquid: skill persistence without latency.**
- **Why it is the brightest light:** 001's one high-powered positive was
  that a wallet's per-trip skill persists (rank IC 0.40–0.44, 91-day
  lookback → 56-day hold, every window). Every *follower* design lost to
  latency, venue mismatch and costs. A vault deposit has none of those:
  the depositor owns a share of the leader's actual P&L, on the same
  venue, at the same instant, minus the leader's profit share (typically
  10%).
- **The gap in the evidence:** vaults and protocol accounts were
  **excluded from 001 by address list**, so this has never been tested.
- **First steps:**
  1. A scout: how many user vaults exist, their TVL, age, deposit terms
     and lock-ups, leader share and closure rate (survivorship!). Use the
     public `vaultDetails` / vault list API plus the cached
     `research/copytrade/data/cache/hl-vaults`.
  2. Whether each vault's address fills sit in the local archive, so the
     001 skill score can be computed point-in-time on the lookback.
  3. Behaviour-only: the distribution of vault skill scores and vault
     TVL, against capacity at our size.
- **Then the trial:** rank vaults by the pre-registered lookback score,
  hold the top group for the follow period, and compare the depositor NAV
  return (from `portfolio` history) with all vaults and with HLP. Include
  vaults that closed during the period.
- **Expectation:** an effect exists, about 1 in 2; deployable, about 1 in
  3. The risks are survivorship, leader withdrawal, capacity, and vaults
  as leveraged bets on one regime.

**L2. Funding and basis carry across venues.**
- **The idea:** delta-neutral positions that collect the perpetual funding
  difference between Hyperliquid and Binance / Bybit / OKX, or perp-versus-
  spot basis. It is a documented, structurally caused edge: leveraged
  longs pay to hold.
- **Data:**
  - Hyperliquid funding, cached from 2025-10-20;
  - Binance `fundingRate` monthly files (free, years of history);
  - Bybit/OKX funding history via public endpoints.
- **First step:** behaviour only. The distribution of cross-venue funding
  spreads and their persistence, against entry/exit costs, margin needs,
  and basis moves at rebalances. Then register a rule with a holding and
  exit policy.
- **Expectation:** an effect exists, likely. Deployable at small size,
  about 1 in 2; returns are modest and capacity-limited. The tail risks to
  measure are funding flips, venue risk, and liquidation of one leg in a
  spiral.

**L3. The inverted positioning signal, holdout-only.**
- **The finding:** 001 T3 found that positioning of skilled versus losing
  money *anti*-predicted 3-day returns, t −3.4, in the bear year. That was
  a fail as registered.
- **Why holdout-only:** inverting it is a post-hoc idea. The only fair
  test is to register the inverted rule now, unchanged, and decide it on
  the sealed holdout alone (discovery already used). The holdout holds a
  rally, which makes it a hard test.
- **Expectation:** about 1 in 5. It is cheap, and it is the one use of the
  holdout that the evidence directly motivates. The HL holdout pull is
  requester-pays (~12 GiB for 101 days), so it needs the owner's approval
  with a byte count.

**L4. Multi-year power from free exchange data.**
- **The constraint it addresses:** the HL year was one bear year with one
  crash. Binance publishes years of free data: 5-minute open interest,
  long/short ratios and taker volume from 2020–21, funding, and klines.
- **Candidates:** crowding and fragility factors (H6's idea, measured on
  OI and funding instead of the liquidation map); spirals as a regime or
  continuation signal (H5) across years.
- **First step:** a cross-sectional factor test under the spec's rules,
  with years as the split. Discovery on early years, holdout on later
  ones, decided now.
- **Expectation:** about 1 in 4 for a costed factor. Power is the point.

**L5. Your own lights.**
- Propose up to two more, inside or outside crypto. They can be outside
  this programme's evidence: for example structural premia, calendar or
  settlement effects, or prediction-market biases. Each must be lawful,
  accessible at our size, and testable on free or owner-approved data.
- Each needs a feasibility scout before any registration, and a one-line
  reason why it could be *consistent*, not just present.

**Baseline for everything: HLP**, and its new lending leg (about 3% a year).
State every candidate's expected return, drawdown and spiral behaviour next
to it.

## Infrastructure and data on hand

- **Tools:**
  - Python venv `research/copytrade/.venv`, with DuckDB over local
    parquet. numpy is not installed; ask before installing anything.
  - The `ct/` toolkit (tape, liq, trigger, provider, eventstudy, stats,
    drift, xvenue, bn), each with synthetic tests.
  - **Every new mechanics piece gets a synthetic test before it touches
    real data.**
- **Data:**
  - Hyperliquid every-account fills, discovery year, 43 GiB local, with
    liquidation flags.
  - Daily account snapshots, which hold every position with its
    liquidation price.
  - Derived: `liquidations.parquet`, `cascades.json`, `map_levels`, and the
    001 per-window scores and populations.
  - Bitget 1H / 1m candles (partial).
  - Binance USDⓈ-M aggTrades + bookDepth for six coins, **on HL-cascade
    days only** (23 GB, pinned manifest). It is not a contiguous history.
  - The H7 caches: HLP vaults, funding, DefiLlama.
- **Scratch:** the session scratchpad for temporary files; derived data
  under `research/copytrade/data/` (gitignored).

## Access and spending

- Public, free endpoints and local files only, by default. Before any of
  the following, stop and ask the owner with the byte count or cost:
  - any AWS / requester-pays or paid-API call;
  - any download above ~1 GB;
  - any new vendor;
  - any key you do not have.
- Ask for data or API access whenever a light needs it; say exactly what
  and why. Never pull production secrets for research.
- Downloaded data is untrusted. Each download goes in its own new
  directory, scripts live outside it, and Python that reads it runs with
  `-I`.

## How to use subagents

- One subagent per bounded task, with a complete brief: the files to read,
  the exact deliverable, the files it may create, and the forbidden actions
  (no outcome before REGISTERED, no holdout, no publishing, no paid calls,
  no commits). Never let two subagents write the same files.
- Prefer parallel subagents for independent scouts and data pulls. Scouts
  for L1, L2 and L4 are independent and cheap, so start them together.
- You read every results line yourself; subagents do not interpret
  results. Verify any decisive behaviour claim yourself before acting on
  it.

## Reporting to the owner

- At each decision point give: the result in the registration's own
  terms; what it means for the goal; the next step and why; your odds.
- Ask for approvals, keys, data or spending in one message, with the
  minimum needed.
- Keep a running programme document, for example
  `docs/edge-discovery-<date>.md`. It holds the lights, their status, the
  direction log, and the data inventory, so the next session can pick it
  up cold.
