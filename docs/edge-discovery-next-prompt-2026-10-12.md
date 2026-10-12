# Research orchestrator, round 3: higher returns, fresh perspective

You run this research as an orchestrator. You plan, delegate to subagents,
read their results yourself, and decide the direction. **The goal is a
consistent, capturable edge,** in any market, venue or instrument we can
access at our size (well under $1 M).

Two programmes have already run. Together they spent 18 registered trials
without a pass. Do not repeat them. Start from what they established, look
at the market from a different angle, and aim higher.

## The owner's mandate (2026-10-12, verbatim intent)

- **Autonomy.** You may register rules yourself and take decisions as you
  would recommend, as long as they move us toward the goal. Report each
  decision and its reason in the direction log. The owner still sees
  every result.
- **Risk.** Risky strategies are in scope **when the risk is estimated
  and smaller than the reward.**
  - Every candidate states its expected return, its tail (the worst
    plausible week and month), and its sizing (a fraction of Kelly).
  - A risk premium is acceptable if it is priced. An unpriced tail is not.
- **Data.** Download freely when the pull is free and safe: public,
  keyless, not malicious, each pull in its own directory, read with
  `-I`, checksummed. Ask first for anything paid, requester-pays, keyed,
  or above about 5 GB.
  - "Data not everybody sees" means **lawful combinations**: our own
    archives, joined with public data in ways others do not join them.
    It never means non-public data obtained without authorisation.
- **Ambition.** The owner's words: "aim for 100% a week, without
  overfitting … aim for the stars so when you land you land on the
  clouds."
  - Be honest about the arithmetic: 100% a week compounds to 2^52 a year,
    and no repeatable strategy does that at any size.
  - Read the instruction as a search direction:
    - prefer convex, asymmetric payoffs;
    - prefer high-turnover edges with real per-trade expectancy;
    - prefer levered risk premia with a measured tail;
    - over 3%-a-year carry.
  - Plausible landing zones: about **30–100%+ a year** with 30–50%
    drawdowns, or a small-capacity, high-Sharpe edge.
- **Legality.** It is a warning line only, never a blocker (the owner is
  not a German citizen; Binance and Bitget work for the owner). A venue's
  own geoblock is still a practical fact.

## Read first, in this order

1. `docs/edge-discovery-2026-10-11.md`: the last programme, end to end.
   Especially its direction log, the "distance to goal" entry
   (2026-10-12) and the session close.
2. `docs/forced-flow-research-horizons-2026-10-10.md` §1–§2 and §9: what
   the programme before established.
3. `docs/hlp-benchmark-2026-10-10.md`: the benchmark.
   - HLP: +19% in the spiral year, 2–4% a year calm, Sharpe 1.4, maximum
     drawdown −2.5%.
   - Its lending leg pays about 3% a year.
   - HLP is **long the spiral tail**.
4. The result section at the end of each registration in
   `research/copytrade/registrations/`: 001–007, 009, 010, 012. Read 006
   for the withdrawn template.
5. The ledger exposures `000`, `000b`, `000c` … `000h`.
6. `research/copytrade/registrations/reviews/`. Every draft last round
   had a blocker that a reviewer found. Read these to see what reviewers
   catch.
7. `docs/alpha-lab-spec.md` §2 (invariants), then `AGENTS.md`.

## Hard rules (unchanged unless the mandate above says otherwise)

- **AGENTS.md rule 5 is absolute.** Nothing is published, uploaded or
  shared: no artifacts, no hosted pages, no docs services. Deliverables
  are local files.
- **The live AI trader is off.** Do not touch `lib/`, `pages/` or
  `vercel.json`. Research lives on branch `copytrade-research`. Commit
  locally; the owner pushes.
- **Never pull bulk bar data out of Neon.**
- **Holdouts:**
  - The HL holdout (2026-07-01 →) is still unread. Under 012's D18, only
    **L3** may now claim it; 012 closed without reading it. Any new claim
    needs a registration that amends D18 openly.
  - 009's holdout A (Binance 2024-01 → 2025-07) is closed and released.
  - 010's fresh window (2026-07 → 2026-12) was never read.
  - **Every new registration states which calendars it reads and which
    earlier looks share them.**
- **Family counts:**
  - HL discovery year (2025-07-28 → 2026-06-30): **16 trials, no pass.**
  - Binance 2021–23: 1.
  - L2 carry: 3.
  - New data starts its own count.
- **The ledger protocol is unchanged:**
  - hypothesis, then a DRAFT with trials, criteria, power, costs, clusters
    and placebo;
  - behaviour-only feasibility first;
  - **two independent reviews**;
  - REGISTERED;
  - a guarded runner that runs once, with one results row;
  - the result written into the registration.

  You may now take the REGISTERED step yourself (mandate), after both
  reviews.

## What is established (keep; do not re-test)

1. **Trader skill persists on Hyperliquid, mostly as loser persistence.**
   - 001 T1: rank IC 0.40–0.44.
   - 007: every decile of vault depositors lost money in the bear year;
     the top's apparent edge was low BTC beta.
   - No follower or depositor design earned.
2. **Forced flow overshoots on its own venue, for seconds.**
   - The overshoot belongs to whoever is in the book or is the backstop
     (HLP). It is not capturable by placement, by entering after the
     flow, or across venues at one second (002–006).
   - Spirals decide every mean: a market-wide liquidation rate above
     **$77 M per 5 minutes** (p99.9) separates spirals from ordinary
     cascades.
3. **Funding is a real, large cash flow, and the trades built on it fail
   on price and fees.**
   - 009, cross-sectional: funding about +37 bp a week (≈ 19%/yr gross);
     net +8.8%/yr, not significant.
   - 010 and 012, cross-venue: funding +28–35 bp per position. **Taker
     fees (about 35 bp per position, four fills) and price-gap jump tails
     (mean −38 bp, median +2 bp) take it all.**
   - Only BTC/ETH/SOL cross-venue carry was statistically positive: about
     3%/yr, the lending rate.
4. **Adverse selection is large on HL.** A resting order filled by
   non-forced flow loses 14 bp at 15 minutes (003, t −8).
5. **Costs decide.** Every design that crossed the spread several times
   per trade died on fees. Maker execution, lower turnover, or edges much
   larger than about 40 bp per round trip are required.
6. **Positioning of skilled versus losing money anti-predicted 3-day
   returns** in the bear year (001 T3, t −3.4). Its only fair test is L3,
   on the holdout.

## Lessons from round 2: process faults to design out

- **A brief that allows "counts" must forbid conditioning on position
  state or on any future of the signal.** 000f: a split by "already held"
  leaked persistence.
- **Gates must not sit inside functions shared by primary and reported
  variants.** 007 run note 1.
- **Registrations that share raw data need ordering rules and read
  whitelists.** 000g.
- **Fetchers never cache an error or null.** Paginate forward. Check
  coverage at the tail. (012 review; the 010 erratum: the Bybit
  backward-paging bug.)
- **Two point bars in series destroy power.** Holdouts should be existence
  tests with small-sample t critical values (t_{G−1}), and the economic
  bar applied after.
- **State tests that assert "refused today (DRAFT)" go stale at
  REGISTERED.** Two remain in `tests/test_factors.py` and
  `tests/test_vaults.py`. Their update was blocked by the session's
  safety classifier, so leave them to the owner and do not route around
  it.

## Assets on hand (local, gitignored under `research/copytrade/data/`)

- **HL every-account fills,** 2025-07-28 → 2026-06-30, with liquidation
  flags: about 43 GiB. Includes the **`xyz` dex (HL equity perps; 1.1 GB
  of fills)** and daily account snapshots with every position and its
  liquidation price.
  - Derived: `liquidations.parquet`, `cascades.json`, `map_levels`, and
    the 001 features.
- **Binance USDⓈ-M metrics, funding and 1h klines,** all USDT perps,
  2021-11 → 2024-01 (`binance_years/`).
- **HL, Binance, Bybit and OKX funding;** HL 8h candles; Binance and Bybit
  klines; 2023 → 2026-06 (`carry/`, `carry012/`).
- **Binance aggTrades and bookDepth,** six coins, HL-cascade days only.
- **Vault ledgers and truncated vault NAV grids** (`vaults/`), HLP caches,
  DefiLlama, Ethena yields.
- **Toolkit:** `ct/` (tape, liq, trigger, provider, eventstudy, stats,
  drift, xvenue, factors, carry, carry012, vaults). Guarded-runner
  patterns: `r009_run.py`, `r010_run.py`, `r012_run.py`, `r007_run.py`.
  Python venv with DuckDB; no numpy.

## Lights for round 3 (re-rank freely; say why in the log)

**N1. HL equity perps (`xyz`) against the underlying stock: weekends and
overnight.**
- **Why:** HL's equity perps trade 24/7, while their underlying stocks
  trade about 6.5 hours on weekdays. Off-hours prices are set by thin,
  retail-heavy flow, and the Monday open re-anchors them. We hold
  **every account's fills** on these markets, so we can see who moves
  the off-hours price. Almost nobody combines that with exchange opens.
- **Data:** the local `xyz` fills and snapshots; daily or minute equity
  opens from a free source (check terms; for example the exchanges' own
  daily files, or Stooq).
- **First step (behaviour):**
  - the off-hours basis distribution against the next open;
  - the share of off-hours volume from accounts in the bottom skill
    deciles;
  - taker and maker costs on `xyz`;
  - depth.
- **Expectation:** an effect exists about 1 in 2. Deployable about 1 in 3,
  because capacity is thin. Return per event could be large, from
  weekend gaps of several percent.

**N2. Buying the spiral tail cheaply, and selling the premium when
spirals are unlikely: options plus the liquidation map.**
- **Why:** HLP earns its year in spiral hours (it is long that tail).
  Crypto options carry a volatility risk premium on average, but their
  pricing of spirals may not use what we can see: the every-account
  liquidation map (dense forced flow within a few %) and the market-wide
  liquidation rate.
- **Two registrable ideas:**
  - (a) Buy OTM options / long gamma when the map shows dense nearby
    liquidation levels.
  - (b) Sell short-dated volatility only when the map is sparse and the
    stress rate is low.
- **Data:** Deribit's public historical trades and implied volatility
  (check what is keyless and free, and the date bounds); the local map.
- **Risk:** (a) has a bounded loss (the premium); (b) carries a tail that
  must be sized.
- **Expectation:** 1 in 4 that an effect exists. Returns are convex.

**N3. Listings and scheduled supply: the first days of a new perp, and
token unlocks.**
- New-listing funding extremes and price paths on Binance and HL. Use the
  free Binance archive from each listing day and HL candles.
- Public unlock calendars.
- High per-event moves, many events a year, and no forced-flow race.
- **Expectation:** 1 in 4.

**N4. Maker-only carry with a gap stop.**
- 010 and 012 died on four taker fills (about 35 bp) and gap jump tails.
  A version with maker entry and exit plus a price-gap stop could flip
  the sign.
- It needs a credible maker-fill model; the free bookDepth periods give
  partial books. Adverse selection (fact 4) is the risk.
- **Expectation:** 1 in 5.

**N5. Crowd positioning extremes, multi-day, majors.**
- Aggregate leverage and direction of the losing cohort (fact 1) at
  extremes, as a contrarian timer for BTC/ETH over days.
- The HL year is at 16 trials, so any test there is decided only on L3's
  holdout claim: amend D18 openly if it is used.
- **Expectation:** 1 in 5.

**N6. L3 itself** (inverted 001 T3, holdout-only). Its data pull is about
12 GiB of requester-pays data (about $1). It **needs the owner's yes**,
because that pull is not free.

**N7. Your own lights,** including outside crypto (event contracts,
structural premia in other asset classes). Each must be accessible, carry
a one-line reason it could be *consistent*, and pass a behaviour-only
scout first.

## Working method

- Scouts first, in parallel. One subagent per bounded task, with a
  complete brief: the files to read, the deliverable, the files it may
  create, and the forbidden actions. **Name forbidden conditionings
  explicitly.**
- You read every results line yourself, and verify any decisive claim
  before acting on it.
- Keep a running programme document, `docs/edge-discovery-<date>.md`, with
  the lights, the status, the direction log, the data inventory, and the
  distance to goal after each result. Keep it so the next session can
  pick it up cold.
- **Report to the owner at each result:**
  - the result in the registration's own terms;
  - the distance to goal (% a year, Sharpe, drawdown against HLP);
  - the next step;
  - your odds.
- If the session grows long, finish the open jobs, update the programme
  document, and write the next prompt, as this one was written.
