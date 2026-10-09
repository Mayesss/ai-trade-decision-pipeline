# Copy-trade research plan — 2026-10-07

Written to be picked up cold. Read with `docs/alpha-lab-spec.md` (the invariants
in its §2 still apply) and the audit that ended the AI decision engine,
`docs/decision-engine-edge-audit-2026-09-11.md`.

## 0. Status

**What was decided in the session that wrote this:**

- The AI trade picker never showed positive R. The owner intends to retire it.
  *Switching off the live trader is a separate, deliberate act* (spec §2
  invariant 6) and is not part of this plan.
- Replacement idea: copy other traders. Research may use any exchange or free
  public source. **Execution stays on Bitget / Capital only.**
- Monitoring focuses on **perp DEX wallets (Hyperliquid first)**, not Bitget's
  elite-trader program. A Bitget recorder was considered and dropped (§2).
- No LLM anywhere in the loop. Perplexity / Google Trends are out (§9).
- Test on **past data, point-in-time**, rather than waiting 6–8 weeks forward.
  Forward data still serves as the final holdout.
- Cost constraint: must not add load to Neon compute, Neon transfer or Upstash
  KV (§7).
- AWS account created 2026-10-07 (C1 resolved; P1 in §8).
- **Holdout stays 2026-07-01 → latest, with the P0 exposure recorded** (owner,
  2026-10-07). P0's 60-day window (2026-08-08 → 10-07) lies inside it; what
  was seen is listed in `research/copytrade/registrations/000-p0-holdout-exposure.md`.
  The archive's later months are the second, untouched holdout.
- Pre-mortem done before any download (§13); the simulator and metrics were
  rebuilt around it.
- **Goal restated (owner, 2026-10-08):** find leaders whose edge is *not
  crowded* and replicate them, from home-made analysis of the archive.
  Capital is no longer a constraint — a **paper account** comes first (P7),
  so the "$100 real size" question is retired. Registration 001 draft
  revision 4 carries the consequences; owner decisions D1–D15 open.

**Open — owner decisions:**

| # | decision | recommendation |
|---|---|---|
| C1 | ~~Create an AWS account~~ | **Resolved 2026-10-07.** |
| C2 | Research ledger in git instead of Neon | Yes, for this workstream (§6). Deviates from spec §3 — lock it in the spec if accepted. |
| C3 | Object store: Vercel Blob instead of R2 | Acceptable; not needed until a cloud job reads research data (§7). |
| C4 | Pass threshold and delay grid (§5.6) | Lock at P4, before any discovery-period result is seen. |
| C5 | **Where the copier trades.** The frozen AI trader holds BTCUSDT/ETHUSDT on the same Bitget account; a copier there would net against its positions and reconcile against its threads | A **Bitget sub-account** with its own API key (and a separate Capital account for the TradFi track), or retire the AI trader first. Needed before the paper copier (P7) places anything. |

## 1. The question

> Does a wallet's **past** performance, measured only with data available at
> the time, predict the **follower-realizable, market-hedged** return of
> copying it on Bitget, after delay and costs?

Three things in that sentence carry the design:

- **past … available at the time** — selection is point-in-time; no wallet is
  chosen using information from after its selection date.
- **follower-realizable** — the follower acts after a delay, on Bitget, paying
  Bitget fees and funding, at Bitget prices.
- **market-hedged** — copied profit that is just crypto beta is not skill;
  you could get it by holding BTC.

### Hypothesis set (decided 2026-10-07)

Nothing requires following one leader. The research tests a **set**, in this
order, each registered before it runs and counted as its own trial:

| # | hypothesis | unit / power | role |
|---|---|---|---|
| **H0** | **Skill persists**: a wallet's rank on the lookback predicts its copy return in the next window | wallet × window, thousands per window — the high-power test | **gate**: if H0 fails, H1 and H2 are not run |
| **H1** | **Consensus** (product B): trade where the selected leaders agree — vote ±1 per symbol, leader sizing ignored, flat below an agreement threshold (`targets.Consensus`) | portfolio × window — ~4 windows, low power | **primary product** |
| **H2** | **Top-K copy** (product A): K leaders, equal capital share, netted per symbol (`targets.Copy`) | portfolio × window | comparison product |

Why consensus first: one wallet's hedge leg or bait trade is outvoted;
trades are fewer and larger (better at the owner's account size); and it is a
signal *derived* from leaders rather than a copy of their timing. Why the gate:
both products assume skill persists; testing that directly costs nothing extra
and has far more power than either product test.

### Tracks — not crypto only

| track | leaders | executed on | data | status |
|---|---|---|---|---|
| **Crypto** | Hyperliquid main-dex perps | Bitget | archive from 2025-07-28 | P2 next |
| **TradFi via HIP-3** | Hyperliquid builder markets: `xyz` (SP500, XYZ100, JP225, GOLD, SILVER, CL, BRENTOIL, NATGAS, COPPER, EUR, GBP, JPY, TLT, ~90 stocks), `mkts` (US500, USTECH, SMALL2000, USBOND) | **Capital** — overlaps its universe (US500, US100, TLT, EURUSD, GBPUSD, USDJPY, GOLD, OIL) | `xyz` from 2025-10-13; `mkts` only from 2026-07-01 (inside the holdout — unusable for discovery) | needs `CapitalVenue` (§14) |
| **CFTC positioning** | not wallets: weekly Commitments of Traders by trader category, same instruments | Capital | free, back to the 1980s — the power the wallet tracks lack | later (§9) |

TradFi-via-HIP-3 caveats: these markets trade 24/7 while Capital closes
nights and weekends (a Saturday leader trade is copyable only at Monday's
open, gap included — the follower's venue calendar handles it); costs are
spread + overnight financing, not a taker fee; FX may be quoted inversely
(`xyz:JPY` vs USDJPY — `targets.hl_px` handles it, the mapping must measure
it); `xyz` volume was thin early (6 MiB/day of fills in 2025-11 vs 212 MiB in
2026-06), so its discovery period gives ~2–3 non-overlapping windows. It is a
forward-heavy track. At the live account size several Capital instruments are
not openable at all (spec §12) — that limits execution, not research.

### Expected outcome, pre-registered

**Most likely: nothing survives.** Hyperliquid whales are already copied by
commercial tools (HyperX, Nansen and others), which crowds any edge. Leader
returns degrade for followers (one exchange-published figure: 97% of lead
traders profitable, 43.6% profitable for their followers — vendor source,
directional only). A null result is a finished product, as in the spec.

## 2. Why DEX wallets, not Bitget copy-traders

| | Bitget elite traders | Hyperliquid wallets |
|---|---|---|
| History | none — forward recording only | every fill since 2025-07-28 (third-party archive) |
| Survivorship | deleted traders vanish | dead wallets stay in the archive |
| PnL | platform-reported, gameable | recomputed from fills |
| Timestamps | scrape time | block time |
| Access | unofficial app endpoint, ToS risk | public by design |
| Per-trader sample | 30-day ROI summary | every trade → skill vs luck is testable |

Costs of the switch, handled in §5.2: hidden hedge legs on other venues,
multi-wallet traders and vault/bot wallets, crowding and bait trades, and the
research-venue ≠ execution-venue gap.

Second DEX (Aster or Lighter) only **after** Hyperliquid gives a first answer,
as a generalization check — the spec's "widen breadth, not trials" response to
a null. Analysts flag much of their volume as incentive farming. Spot DEXs
(Uniswap, Solana) are a separate idea family: their tokens are mostly not on
Bitget when the signal fires.

## 3. Data sources

| Source | Content | Cost | Role |
|---|---|---|---|
| **Hyperliquid public API** | leaderboard, per-wallet fills, positions, candles, funding. TWAP slices only via a separate endpoint and only ~13 days back (§11) | free, no account | **P0 pilot** — plumbing only, never evidence (survivor-biased, incomplete for TWAP users) |
| **Hydromancer Reservoir** (`s3://hydromancer-reservoir`) | all fills incl. liquidations/ADL; **daily snapshots of every account's value and positions**; 1s candles. Parquet, weekly updates, complete from 2025-07-28 | data free; AWS requester-pays transfer | **evidence source** (needs C1) |
| Hyperliquid official (`s3://hl-mainnet-node-data/node_fills_by_block`) | raw fills, LZ4 | requester-pays | fallback if Hydromancer stops updating |
| Bitget public API | contracts, candles, funding history | free | execution-side prices, fees, coin availability |
| Dune `hyperliquid.perp_*` | same coverage, SQL in their cloud | entitlement + credits | **not used**: free-tier queries are public (violates AGENTS.md rule 5) |
| Allium, Dwellir | full history | enterprise / custom quote | not needed |

**The daily account snapshots matter as much as the fills.** They give the
point-in-time universe — which wallets existed and how they stood on date X,
including ones that later blew up. That is what makes selection
survivorship-free.

**History limit, stated up front.** Complete data starts 2025-07-28: about 14
months. With a 3-month selection lookback and a reserved holdout (§5.5), the
discovery period holds only **~4 non-overlapping 8-week windows**. Power comes
from the number of wallets per window (thousands), not from time. The test can
answer "does past wallet performance predict future copy returns
cross-sectionally"; it says little about regime dependence.

## 4. Repository layout

New, additive, nothing in the live path touched:

```
research/copytrade/
  ct/net.py               # HTTP, on-disk cache, rate pacing
  ct/hl.py                # Hyperliquid public API (fills + TWAP fills merged)
  ct/bitget.py            # Bitget candles, funding, listing dates
  ct/symbols.py           # HL coin -> Bitget symbol, verified by price
  ct/replay.py            # leader events, exact leader baseline, polling follower, self-checks
  ct/targets.py           # target builders: LeaderBook, Copy (single / top-K), Consensus
  ct/venue.py             # execution venues: BitgetVenue; CapitalVenue to come (§14)
  ct/features.py          # copyability features (behaviour only)
  ct/archive.py           # Hydromancer S3 via the `copytrade` AWS profile; footer-only reads
  p0_pilot.py             # P0 / P0b runner
  registrations/          # ledger: pre-registrations committed BEFORE the run (§6); 000 = P0 exposure
  results.jsonl           # one line per run, cites its registration commit — from P5
  data/                   # gitignored — caches, raw + derived files, local only
```

P0 is **standard-library Python only** (3.14 is installed; `uv` is not), so it
needs no install. DuckDB / polars arrive with the archive work in P2–P3,
installed into a local venv. Nothing here is deployed and nothing runs on a
Vercel cron.

## 5. Method

### 5.1 Universe (point-in-time)

On each selection date *t*, candidates = accounts present in the daily snapshot
at *t* with at least N days of history **before** *t*. Metrics are computed from
fills and snapshots strictly before *t*.

### 5.2 Cleaning rules — written and frozen at P4, before results

Candidates for the rule set (thresholds fixed at P4):

- **Vaults and protocol accounts** (HLP and similar): excluded by address list.
- **Market makers / bots**: fill count per day, maker share, median holding
  time below a floor, near-zero net exposure.
- **Hedge legs**: a wallet whose net direction is not explained by its own
  PnL path (e.g. persistent short with funding income, PnL uncorrelated with
  the coin) — likely one leg of a cross-venue book. Exclude.
- **Multi-wallet traders**: not detectable reliably; documented as a known
  limitation rather than guessed at.
- **Copyability** (`ct/features.py`), measured on the lookback only:
  median holding time (floor: a multiple of the poll interval, e.g. ≥ 12×),
  share of round trips under 1 h, fills per day, **maker share by notional**
  (`crossed` false — a maker's edge is the spread, and the follower always
  pays Bitget's taker fee), typical gross leverage.
- **Rule for every filter above: behaviour only, never returns.** Cleaning
  and copyability thresholds may be set by looking at feature distributions
  in the discovery period; no copy return is computed before the P4
  registration is committed.
- **Tradeability**: only fills in coins **listed on Bitget at that moment**.
  Listing date per symbol from the Bitget contracts endpoint, or the first
  available Bitget candle as a proxy (verify which works, P0). HL→Bitget
  symbol mapping (e.g. `kPEPE` vs `1000PEPE`) as an explicit table.

### 5.3 Selection rule (the treatment)

One primary rule, frozen at P4. Proposed shape: rank by risk-adjusted
realized PnL over the lookback (not raw ROI), with minimum trade count and a
max-drawdown limit; take the top K. The same selected set feeds H1
(consensus) and H2 (top-K copy); H0 uses the full ranking, not just the top.

### 5.4 Comparison arms — same K, same windows, same simulator, per product

| arm | selection | answers |
|---|---|---|
| **R** | the frozen rule (§5.3) | — |
| **N1** | K random wallets from the cleaned universe | does selection beat no selection? |
| **N2** | top K by raw 30-day ROI (the leaderboard) | does the rule beat what everyone already copies? |
| **N3** | rule R on time-shuffled fill labels | synthetic null — the simulator's own false-positive rate |

### 5.5 Simulation — the polling follower (`ct/replay.py: follow`)

Rebuilt 2026-10-07 after the pre-mortem (§13). The P0 version copied every
1-second event at a fixed delay, which no deployable copier can do and which
overstated fees on leaders who scale in with many slices.

- **Poller.** The follower reads the leader's positions every *poll*
  minutes (fills completed by then) and acts at the open of the next minute.
  **Poll grid: 1, 10, 60 minutes** — a 1-minute cron, the existing 10-minute
  watcher, hourly. No 0-delay run: it is not deployable. P4 picks **one**
  as primary; the others are robustness, not extra trials.
- **Sizing — leverage-normalized, not leader-scaled.** Target = leader
  position / leader equity × follower capital × *target leverage* / *leader's
  typical leverage*. Copying a 20× leader at 20× would compare leverage, not
  skill. Leader equity comes from the **previous day's** snapshot (a
  same-day snapshot may be taken at day end — look-ahead); typical leverage
  from the lookback.
- **Capital is not compounded** within a window, so runs are comparable.
- **Leverage cap** on the follower's gross notional / equity at every poll
  (pilot: 3×); the whole book scales down when exceeded.
- **Rebalance band.** A coin trades only when off target by more than a
  fraction (pilot: 25%); opens, closes and side flips always trade.
- **Bitget order rules.** Quantity rounded down to the lot step; skipped
  below the minimum quantity or the $5 minimum notional. Counted, never
  hidden.
- **Costs.** Taker fee 0.06% (all contracts). Slippage proxy = a fraction of
  the execution minute's high-low range (pilot 25%; P4 freezes a level and
  reports 0 / 10 / 25% as sensitivity). Funding at Bitget settlements where
  Bitget history exists; the discovery period needs HL funding as a proxy
  (P4 decision, §11).
- **Liquidation.** Hourly: if equity at the hour's adverse extremes falls
  under maintenance margin (pilot 1% of gross — verify Bitget's tiers at
  P4), the whole book is closed at those extremes and following stops.
- **Window end: close everything, paying costs.** Never mark to market. In
  P0b a mark-to-market end put 56% of one wallet's result in five positions
  that were never realized.
- **Two account sizes, always.** Nominal ($10,000: does the signal exist) and
  real (~$100, the Bitget share of the live account: does it work for the
  owner). Skipped orders at the real size are a finding, not noise.
- **Mechanics check, every run.** A costless 1-minute follower with no band
  or cap must land within ~1% of the leader's exact result at the same scale
  (`leader_pnl`). P0b: $2,395.27 vs $2,394.22.
- **Windows:** selection on 3-month lookback; hold the selected set for 8
  weeks; step forward 8 weeks. Selection dates falling in the 2025-10-25 →
  12-14 snapshot gap use the last snapshot before them (§12).
- **Periods:**
  - discovery: 2025-07-28 → 2026-06-30
  - **holdout: 2026-07-01 → latest**, never read until the rule is frozen;
    read once (spec invariant 4)
  - forward: archive weeks that arrive after the holdout read; a second
    holdout at no infrastructure cost

### 5.6 Reporting and pass criteria

**Units.** Copies have no stop, so the repo's R (realized risk to the stop)
does not exist. Two measures replace it:

- **Primary — daily net return on capital** of the follower book per
  wallet-window (closed at window end, all costs in). This is what a
  follower actually earns, and leverage normalization (§5.5) makes it
  comparable across leaders.
- **Diagnostic — per-trip net ATR-R:** net / (max notional the follower
  *actually held* × daily ATR% at entry). The denominator is the follower's
  realized exposure after skips and caps, never the leader's or a planned
  one (the repo's realized-risk lesson). Reported **equal-weighted** (mean,
  median, 10%-trimmed mean: "is the typical copied trade good") **and
  risk-weighted** (Σ net / Σ risk: "are the trades the leader sized up
  good"). In P0b they disagreed: median −0.10 R, risk-weighted +0.12 R,
  dollars +13%. Dollar PnL alone is never reported as a result.

**Independence.** Fills and trips are not independent observations; neither
are wallets on the same day (they share the market). The unit is the
**wallet × window**; standard errors cluster by window date (or a block
bootstrap over dates). A t-stat over trips or fills would repeat the spec's
near-static-signal error (t = −4.5 from ~6 real bets).

**Regime.** The data spans one large drawdown (BTC ~$115k in 2025-08 →
~$62k in 2026-07). Long-biased leaders look bad and short-biased ones good
for reasons unrelated to skill; the **hedged** result is the one that counts.

Every run reports, per arm and poll interval:

1. **raw** follower result (daily net return; ATR-R both weightings)
2. **market-hedged** follower result — BTC-perp hedge, beta from trailing 30
   days at each rebalance, point-in-time. **This method is fixed now**;
   trying hedge variants later counts as extra trials.
3. **buy-and-hold BTC** over the same windows
4. counts: orders, skipped below minimum / lot step, leverage-capped polls,
   liquidations, positions closed by window end

| raw | hedged | reading |
|---|---|---|
| + | ≈ 0 | market exposure in a rising period — reject |
| + | + | selection skill — candidate |
| ≈ 0 | + | skill hidden by market timing — a hedged book may work |

**Pass** (threshold values locked at P4, decision C4): arm R's hedged daily
net return > 0 at the **primary** poll interval and cost level, at the
registered threshold with date-clustered errors, **and** R beats N1 and N2,
**and** N3 stays null, **and** the equal-weighted and risk-weighted ATR-R
agree in sign, **and** holdout keeps the sign. Hedged results must
clear **double** costs (two legs) before a hedged book is considered.

Standing rule from the spec, applied here: a ranking statistic is never
sufficient — every candidate goes to the costed, magnitude-weighted
simulation before it is believed.

### 5.7 Hedging live positions — not decided here

BTC/ETH copies cannot be hedged with BTC (the trade *is* the market). Alt
copies could be hedged at the **book** level (net beta once per rebalance, not
per trade). At current equity a separate hedge order often falls under
Bitget's minimum size. Default: unhedged; revisit only if §5.6 shows hedged
alt returns surviving double costs.

## 6. Ledger: git, not Neon (decision C2)

The research runs locally, so the ledger can live in the repo:

- A registration is a file in `registrations/` stating hypothesis, rule,
  cleaning rules, arms, delays, costs, hedge method, pass criteria and the
  data period it will read. It is **committed before the run**; the commit
  timestamp proves order.
- A run appends one line to `results.jsonl` citing its registration's commit
  hash. Failed and null runs are appended too. History is never rewritten.
- The holdout read is its own registration, made once.

This keeps spec invariants 1, 4 and 7 without a database. If the lab later
moves to the spec's Neon `research.*` schema, these files import as rows.

## 7. Cost guardrails

Measured baselines: Neon compute is the bill (0.25 CU, every wake bills ~6–7
minutes; target ~0.5 CU-h/day — `docs/neon-compute-cost.md`). Upstash bills per
command, $0.20/100k (`docs/kv-cost-reduction.md`).

| workload | runs | Neon | KV | other |
|---|---|---|---|---|
| P0 pilot (HL API) | local | 0 | 0 | free |
| archive ingest + simulation | local | 0 | 0 | AWS transfer (below) |
| forward data | next archive weeks | 0 | 0 | same |
| ledger | git | 0 | 0 | 0 |
| *later* live copy poller | Vercel, per minute | **0 if KV-only** | ~130k cmds/month ≈ $0.26 | small |

Rules:

1. **No research code on a Vercel cron that touches Neon.** If the AI trader
   is switched off, its daily windows disappear too, so any research write
   would buy a wake of its own.
2. **Raw data never transits Neon or KV.** Local disk; Blob only for derived
   files a cloud job reads, and never `list()` ($5 / M).
3. **One write per run**, never row-by-row, to any billed store.
4. **A future live poller follows the wake-watch pattern**: per-minute path
   reads KV and the exchange only; Postgres only when it acts.
5. **AWS:** $1 budget alert before the first byte. Read Parquet with column
   and date pruning (DuckDB over S3, if its requester-pays support works with
   this bucket — unverified) or whole daily files one month at a time. A new
   AWS account includes 100 GB/month of free transfer out, which should cover
   requester-pays reads. Full-column history is estimated at 100–200 GB
   (**estimate, not measured**); pruned reads should be several times less.
   Measure the size of one day's files before planning the rest.

## 8. Stages

- [x] **P0 — pilot, no account.** HL public API: pull the leaderboard and
      fills for ~50 wallets; Bitget contracts + candles + funding for the
      mapped coins; build the HL→Bitget symbol table; replay one wallet end to
      end. Verify: API rate limits, leaderboard endpoint, Bitget listing-date
      source. Output is plumbing, **not evidence**. *Done 2026-10-07 — §11.*
- [x] **P1 — owner: AWS account (C1).** Budget alert at $1. Requester-pays
      access configured locally. Nothing committed that holds credentials.
      *Done 2026-10-07: zero-spend budget, IAM user `copytrade-reader` with
      ListBucket + GetObject on this bucket only, CLI profile `copytrade`.
      Archive sized in §12.*
- [x] **P0c — multi-leader engine** (2026-10-07). Target builders (`Copy`
      single/top-K, `Consensus`) and a venue layer with a trading calendar.
      Checks: the single-leader run reproduces P0b exactly; costless
      `Copy(K=3)` equals the mean of the three single copies to the cent;
      `Consensus` holds gross near its target (peak 1.17× for 1.0×, band
      drift). No multi-leader returns printed (holdout, registration 000).
- [x] **P2 — snapshots.** Pull daily account snapshots (small) for the full
      period, **main dex and `xyz`**. Measure bytes per day. Build the
      point-in-time universe table. Gate: confirm the snapshot timestamp (the
      number in the file name) and use day D−1 for selection on day D.
      *Done 2026-10-07 — 693 files, 6.48 GiB transferred (exactly as
      listed), 8 parallel downloads; `p2_snapshots.py`, `p2_checks.py`,
      `p2_accounts.py`. Findings:*
      - *Timing — the D−1 rule is replaced by an as-of join on each file's
        own timestamp* (`<block>_<epoch ms>.parquet`). Most snapshots are
        taken 00:00–00:12 UTC at the start of their date, but **63 of 384
        (main) and 17 of 309 (`xyz`) were taken 3–21 h into it**, spread over
        the whole period. A fixed "file D = start of D" rule would have been
        look-ahead on those days. 2026-03-24 has two files (both kept; as-of
        handles it).
      - *Coverage — snapshots list only accounts holding a position* (no
        zero-size rows). A trader flat at snapshot time is absent that day, so
        **the universe comes from fills (P3)**; snapshots supply equity where
        they exist, and equity between snapshots is the last known value.
      - *Consistency* — one account value per user per snapshot (checked on
        three days per dex).
      - *Equity table* `data/derived/<dex>/snapshot_accounts.parquet`: one row
        per (snapshot, user) — timestamp, account value, positions, gross and
        net notional. Main: 27.2 M rows, 703 k users; `xyz`: 8.1 M rows,
        277 k users. Gross leverage p50 / p90 / p99: main 2.9 / 11.5 / 38.6×,
        `xyz` 4.7 / 18.3 / 44.9× — normalization (§5.5) is not optional.
        Account value p10 is $9 (main) / $5 (`xyz`): a minimum-equity
        cleaning rule is needed.
      - *Unverified:* whether `xyz` account value is the xyz-dex margin
        account only (HIP-3 dexes margin separately) — check against the API
        for a few users before P4 uses it.
- [x] **P3 — fills.** Pull pruned fill columns for the discovery period
      only. Holdout dates are **not downloaded** yet. *Pulled 2026-10-07:
      all 599 discovery days (338 main, 261 `xyz`), ~53 GiB transferred
      (one connection reset cost one partial day; per-day retries added),
      43 GiB on disk. Running total this month incl. P2: ~60 GiB. Gates
      (`p3_checks.py`, 6 sample days per dex for G3–G5):*
      - *G1 UTC partitions: 338/338 and 261/261 days clean.*
      - *G2 dex: main files hold only `hyperliquid`, `xyz` files only `xyz`.*
      - *G3 order: **0 chain breaks in file order** in 5.5 M sampled fills;
        a (timestamp, trade_id) sort breaks 2.2 M. File order is execution
        order — never re-sort within a millisecond.*
      - *G4 realized PnL: exact cash flow vs the archive's `realized_pnl`
        agrees on 54,867/54,868 (main) and 91,500/91,500 (`xyz`) round
        trips at a tolerance of 0.1% of position notional. At a 1%-of-PnL
        tolerance the misses were 17% below $0.01 per coin, 1.2% at
        $0.01–1, 0.3% above $1, with errors ≤ 0.05% of notional:
        Hyperliquid's reported PnL uses a rounded entry price. The exact cash
        flow (what the replay uses) is the accurate one; the self-check in
        `replay.closed_pnl_check` now uses the same notional-relative rule.*
      - *G5 archive vs API: 15/15 compared addresses identical (trade ids,
        price, size, start position); 1 not retained by the API — old fills
        of active wallets are gone there, which is why the archive is the
        source.*
      - *Adapter `ct/archive_fills.py` maps archive rows to the API fill
        format, so replay, self-checks and features run unchanged.*

      *Original notes:* *Downloader ready
      2026-10-07 (`p3_fills.py`, `archive.download_columns`): footer, then
      only the wanted column chunks by byte range into a sparse file,
      rewritten compact. Holdout dates refused in code. Verified on
      `xyz` 2025-11-15: pruned file identical to the same 14 columns of the
      full file (118,828 rows) at 43% of the transfer. **Exact plan from
      footers: 52.67 GiB** (42% of 126.5 GiB; 338 main + 260 `xyz` days).
      Held until the owner confirms the P2 transfer billed at $0 (free tier
      applies to requester-pays).* Gates, all before any copy return is
      computed:
      - file order is execution order within a millisecond (startPosition
        chain test, as in P0);
      - closedPnl self-check on archive fills;
      - main Hyperliquid dex only (`dex` column), perps only;
      - date partitions are UTC;
      - archive vs API cross-check for a few wallets on discovery dates.
- [x] **P4 — pre-register.** *Done 2026-10-09: registration 001 revision 4
      committed as REGISTERED (all D1–D15 approved 2026-10-08; T2b on all
      scored day traders; block fetch complete, fees confirmed).* Freeze cleaning rules, selection rule, arms,
      poll interval, costs, sizing, hedge method, pass thresholds (C4), and
      the H0 → H1/H2 order. Commit. Nothing below runs before this commit
      exists. *In progress 2026-10-07: draft
      `registrations/001-skill-persistence-and-crypto-books.md` awaiting the
      owner's decisions D1–D8. Inputs were behaviour-only
      (`p4_features.py`, `p4_describe.py`; `realized_pnl` never read):*
      - *Features per wallet at each selection date via DuckDB (4.5 min for
        all 7 windows); cross-checked against a plain-Python reference on 20
        wallets per dex — identical round trips, median hold, maker share.
        The cross-check caught a NULL-instead-of-0 bug for wallets with no
        maker fills (they would have fallen out of the maker filter).*
      - *Eligible universe under the proposed filters: main 2,248–3,968
        wallets per window; `xyz` 52 / 212 / 511 (thin early).*
      - *"Share of trips under 1 h" is redundant with "median hold ≥ 2 h" —
        dropped.*
      - *Account value is often stale (snapshots list position holders
        only): among filtered wallets 59–87% have a value ≤ 7 days old —
        proposed as a filter.*
      - *DuckDB note: registering pyarrow tables back into the connection
        deadlocked (0% CPU) on duckdb 1.5.6 / Python 3.14; intermediate
        results stay in DuckDB temp tables.*
      - *Draft revised: the deciding test is now T3 "H0-copy" — every
        eligible wallet copied in simulation, score vs the follower's
        hedged net return (~10k copies, high power); the consensus / top-K
        books are descriptive here and get pass/fail at the holdout.*
      - *Draft re-centred (owner, 2026-10-08) on the two ideas with a
        mechanism, renamed `registrations/001-slow-traders-copy-and-crowd-divergence.md`:
        **A** copy only slow traders (median hold ≥ 24 h) with a 1 h vs 24 h
        lag mechanism check and a cross-regime score (good in both rising
        and falling BTC); **B** skilled-vs-crowd positioning from the
        full-population snapshots. The data spans more than a bear market:
        Jul 2025 up, Aug–Oct flat at the top, Nov −23%, Dec–Mar grind down,
        Apr +14%, May–Jun down; the holdout (Jul–Sep 2026) is mostly a rally.
        BTC 1H history 2019-07 → 2026-10 (63,517 bars, no gaps) labels
        regimes (`ct/regimes.py`).*
      - *Prepare phase (owner: prepare, don't launch), 2026-10-08: archive
        leader loader and as-of equity (`ct/leaders.py`), `LeaderBook` lag,
        daily + on-trade BTC hedge with holdings beta, scores and statistics
        (`ct/scores.py`, `ct/stats.py`), B's tilts and costed long/short
        (`ct/crowd.py`), populations (`p5_population.py`), all checked on
        synthetic data (`tests/test_prepare.py`). Bugs caught there: the
        flip split in round trips, a daily-only hedge leaving 30% of a BTC
        book unhedged. The runner `p5_run.py` refuses to start unless 001 is
        committed as REGISTERED. Price/funding prefetch running (free).*
      - *Draft revision 4 (2026-10-08), after an independent review of
        revision 3 — see the registration's revision history for the full
        list. The two findings that forced it: **(1)** B's crowd was empty —
        signed notional sums to zero per coin across all accounts (verified
        on one snapshot, 190/190 markets), so "cohort minus everyone" was
        the cohort's own tilt; the crowd is now the bottom score quintile.
        **(2)** the holdings-beta hedge neutralised timing skill (a BTC
        timer scored zero minus costs); ranking now uses regression alpha
        and earning uses a static lookback hedge. Also added: loser-
        persistence reading rules, a paired lag test, crowding features
        (shadower excess, post-trade path, watchability) with a T4 trial, a
        day-trader trial T2b at a 10-minute poll, a 1-minute lag/poll point,
        and the bear-only regime caveat. Five trials, α = 0.01. Nothing
        computed; prepare-phase items listed in the registration's §10.*
      - *Old candle stores checked (2026-10-07): the Neon scalp tables were
        dropped in 2026-08 (a recovery branch may hold them; bulk bars out
        of Neon are forbidden by spec invariant 2 regardless). Upstash KV
        still holds orphaned `scalp:candles*` keys: the 30 history keys are
        **Capital** 1-minute candles (FX crosses, EURUSD, USDJPY, GBPUSD,
        XAUUSD, BTCUSD CFD), 2025-12-01 → 2026-03-09 — no use for Bitget
        execution, but the only Capital minute history we have, covering
        the first `xyz` hold window. Copied read-only (31 KV commands,
        59 MiB) to `data/kv_capital_1m/` for plan stage T1, since the cost
        doc slates those keys for deletion. The 749 weekly chunk keys cover
        62 symbols for only ~4 weeks (2026-05-25 → 06-21) with unreliable
        source labels — not used.*
- [ ] **P5 — discovery run.** *First start 2026-10-09 07:21 stopped at
      07:52: network-bound on funding marks and ATR (amendment 1 in the
      registration, mechanics only, no outcome written). Restarted after the
      fix (`p5_run.py`, background, log `data/scratch/p5_run.log`).* T1, T2,
      T2b, T3, T4 as registered. Append every run to `results.jsonl`.
- [ ] **P6 — holdout.** Registration, then download and read once.
- [ ] **P7 — forward + paper.** Re-run the frozen rule on each new archive
      month; in parallel, the paper copier of §14 step 3 (needs C5).
- [ ] **P8 — gated: live following.** Only if P6 passes and paper tracking
      holds (§14). Tiny size, kill switches on.
- [ ] **T1 — TradFi track** (parallel, after P3): `CapitalVenue` — Capital
      minute history from a **demo** account, spread + financing costs, the
      trading calendar (port from `lib/market`), xyz → Capital symbol map with
      inverse-quote detection. Then the same P3–P7 on `xyz`.

## 9. Other sources — later hypothesis families

Same rule for all: store each record's **as-known-at** time (official
publication time and our first-seen time), never the event time; amendments are
new rows. The simulation may act only after both.

| source (free) | public after | executable here? |
|---|---|---|
| exchange funding / OI / liquidations (Bitget, Binance, Bybit, HL) | real time | yes — Bitget perps |
| CFTC Commitments of Traders | Tue positions, Fri publish | yes — Capital FX / indices, BTC |
| SEC Form 4 (insiders) | 2 business days | single stocks — probably not at current equity |
| Congress (House Clerk, Senate eFD) | up to 45 days | same; post-STOCK-Act evidence mixed |
| SEC 13F / 13D | 45 / 10 days | same |
| FINRA short volume, USAspending, LDA lobbying | daily–quarterly | same |
| Wikipedia pageviews | daily | attention proxy; reproducible |
| Google Trends | — | **avoid**: sampled, rescaled, not reproducible |
| Reddit / X | — | paid APIs; skip |

Register a hypothesis only if it maps to an instrument tradable on Bitget or
Capital at the account's size. EDGAR requires a contact `User-Agent`; the owner
picks what goes in it.

## 10. Not verified yet

- Hydromancer: current update cadence, exact schema, bytes per day — and
  whether its fills include TWAP slices (its docs say so; §11 shows why it
  matters).
- DuckDB requester-pays reads against `hydromancer-reservoir`.
- AWS free-tier transfer applying to requester-pays (believed yes).
- Dune free-tier query visibility (believed public).
- Aster / Lighter data access.

Verified in P0 (§11): HL rate limits, the leaderboard endpoint, Bitget
listing dates (via first daily candle).

## 11. P0 findings — 2026-10-07

Run: `python3 research/copytrade/p0_pilot.py` (day 2026-10-07, 60-day window,
50 wallets, seed 7). Report: `research/copytrade/data/p0/2026-10-07/report.json`
(local). **Plumbing results only — nothing here is evidence about copying.**

### APIs, as measured

| item | finding | consequence |
|---|---|---|
| HL rate limit | 1200 weight/min/IP; fills pages cost 20 + 1 per 20 items (a full page ~120) | budgeted at 800/min; 50 wallets × 60 days took a few minutes |
| HL leaderboard | `stats-data.hyperliquid.xyz/Mainnet/leaderboard`, undocumented, ~39 MB, 47,345 rows; 12,604 with ≥ $10k account value and volume this month | fine as a sampling frame for pilots; survivors only by construction |
| HL "10k fills retained" | **did not hold**: wallets returned up to 30,722 fills in 60 days, reaching the window start | the third-party figure is stale or wrong; the truncation flag is now a weak hint only |
| **HL TWAP fills** | **`userFillsByTime` omits TWAP slice fills.** They come from `userTwapSliceFillsByTime`, and only the **last ~13 days** survive there | wallets that TWAP have unrecoverable gaps on the free API. Now merged where available; gaps detected (below) |
| Fill order | fills sharing a millisecond are **not** in `tid` order; the API's order is execution order | sort stable on time only. A (time, tid) sort broke the position chain 171,872 times in 224,949 fills; API order 289 |
| Bitget `launchTime` | empty on all 817 contracts | listing date = first daily candle, found by binary search on `endTime` |
| Bitget candle window | a start+end range wider than ~90 days at 1D fails (40017); `endTime` alone works | used for the listing search |
| Bitget 1m history | available back to at least 2025-08 | covers the whole discovery period |
| **Bitget funding history** | **~90 days only** (back to 2026-07-09) | the discovery period (2025-07 → 2026-06) needs a funding proxy: HL's own funding history. Decide at P4 |
| Bitget taker fee | 0.06% on every contract | single constant |

### Data quality

- **Symbol mapping:** 168 of 178 live HL perps map to Bitget with prices
  agreeing within 3%. The price check caught a ticker collision: HL `PURR` vs
  Bitget `PURRUSDT`, 89× apart. 9 HL perps have no Bitget contract.
- **Fill coverage:** 81.2% of the sample's fills are in mapped perps, 16.1%
  in spot or builder-deployed (HIP-3) markets, 2.2% in coins Bitget lacks.
- **Incomplete histories:** **27 of 50 wallets** have at least one coin whose
  position chain breaks (a position change with no fill behind it) —
  **8.7% of all fills**. Those coins are excluded from replay. This exclusion
  is itself a selection (it drops TWAP users), which the archive should remove.
- **Self-check:** with breaks excluded, the frictionless replay matches the
  leader's own reported `closedPnl` on **314 of 325** closed coin histories
  within 1%; the other 11 differ by at most $2.40 at a $10,000 follower size
  (price rounding). The check is built into the pilot and must keep passing.

### Two bugs found by the self-check, fixed before any number was read

1. Sorting fills by (time, tid) scrambled same-millisecond fills.
2. Events containing both buys and sells were priced at the average of all
   fills; the frictionless baseline now uses the exact signed cash flow.

Without the self-check, the first run's replay of one wallet reported +$3,444
where the leader realized far less — and it would have looked plausible.

### The one-wallet replay (mechanics only)

Wallet `0x3de9…`, account $431,679, follower capital $10,000, 1,316 copyable
events across 15 coins:

| run | net $ | gross $ | fees $ | slippage $ | funding $ |
|---|---|---|---|---|---|
| leader, frictionless | 1,939 | 1,939 | 0 | 0 | 0 |
| Bitget, 0-min delay | 1,338 | 1,994 | 339 | 305 | 11 |
| Bitget, 1-min | 1,312 | 1,946 | 340 | 284 | 11 |
| Bitget, 5-min | 1,224 | 1,853 | 340 | 278 | 11 |
| Bitget, 30-min | 1,062 | 1,670 | 339 | 256 | 12 |

What it shows about the machinery, not about copying:

- Venue gap is small: Bitget gross at zero delay is within 3% of the leader's
  own result, so the price mapping and unit conversion hold.
- Costs are the first-order term for an active wallet: fees plus slippage
  took about a third of gross here. The slippage proxy (25% of the minute's
  range) is a guess and is the least trustworthy number on the page.
- 91 of 1,316 changes fell under Bitget's $5 minimum at $10,000 of capital.
  At the account's real size (~$81, spec §12) most of them would.
- One wallet over 60 days is an anecdote. Leader selection, survivorship and
  every statistic are untouched until P2–P5.

### What P0 changes in the plan

- **C1 (AWS account) is now more pressing.** The free API is incomplete for
  over half the sampled wallets, and the gap cannot be repaired from it. The
  archive is the only complete source; confirm it includes TWAP slices.
- **New P4 decision:** funding source for the discovery period (HL funding as
  a proxy for Bitget, or exclude funding and bound its size).
- **Keep the closedPnl self-check** as a gate in every later stage.

## 12. Archive sizing — 2026-10-07

Measured with LIST calls and Parquet footers only (`ct/archive.py`;
196 KB transferred in total).

**Coverage.** Fills: every day 2025-07-28 → 2026-10-06 (436 days), updated
daily, not weekly as the docs said. Snapshots: 383 days; **missing 2025-07-28,
2025-08-01 and 2025-10-25 → 2025-12-14** (51 days, inside the discovery
period). A selection date in that gap must use the last snapshot before it or
derive what it needs from fills — decide at P4.

**TWAP slices are in `fills/perp/all/`.** On 2026-09-15, 320,301 rows of
`all/` carry a `twap_id` — exactly the row count of `twap_fills/` that day. So
the archive closes the gap that broke 27 of 50 wallets on the free API (§11).

**Size per day** (one Parquet object each):

| dataset | per day | days | total |
|---|---|---|---|
| fills `all/` | 364 MiB (2025-09) – 428 MiB (2026-09); 6.8 M rows, 7 row groups | 436 | ~170 GiB |
| snapshots | 16–20 MiB; 273 k rows (one per account × market) | 383 | ~7 GiB |

**Column pruning.** Share of the 2026-09-15 fills file by column:
`tx_hash` 25%, `client_order_id` 17%, `start_position` 11%, `trade_id` 8%,
`fee` 8%, `realized_pnl` 6%, `order_id` 6%, `size` 5%, `address` 4%, `price`
4%, `timestamp` 2%, everything else < 1% each. The replay needs `address`,
`coin`, `timestamp`, `side`, `size`, `price`, `start_position`,
`realized_pnl` (self-check), `trade_id` (dedupe), plus the small flags
(`direction`, `twap_id`, `is_liquidation`, `crossed`): **~42% of the file**.

**Transfer plan:**

| pull | when | estimate |
|---|---|---|
| snapshots, all columns | P2 | ~7 GiB |
| fills, pruned columns, discovery period (338 days) | P3 | ~57 GiB |
| fills, pruned columns, holdout (2026-07-01 →) | P6, preferably a later month | ~17 GiB |

P2 + P3 ≈ 64 GiB (~69 GB) in one month, inside the 100 GB/month free
transfer if it applies to requester-pays; ~$6 if it does not. Column ranges
mean ~12 columns × 7 row groups ≈ 84 GETs per day, ~$0.02 in total. Local
disk: ~64 GiB of 288 GiB free.

Rows cannot be pruned by wallet before download (files are not sorted by
address), so every pull carries all wallets for the chosen columns.

**`xyz` (TradFi HIP-3), listed 2026-10-07:** fills 2025-10-13 → 2026-10-06
(359 days), growing from 6.2 MiB/day (2025-11-15) to 211.9 MiB/day
(2026-06-15); snapshots 308 days, same 51-day gap (2025-10-25 → 12-14),
~5 MiB/day. Rough estimate for its discovery period with pruned columns:
under 10 GiB of fills plus ~1.5 GiB of snapshots — measure per day before
pulling, as for the main dex.

## 13. Pre-mortem — 2026-10-07, before any download

Asked by the owner: are we measuring PnL instead of net R, are scalpers
going to beat our execution timing, and what else can go wrong. Each trap,
and where it is now handled:

| trap | handled in |
|---|---|
| Dollar PnL across leaders compares leverage, not skill | leverage-normalized sizing (§5.5) |
| No stop on copies → R undefined | daily net return primary; per-trip ATR-R on the follower's actual notional (§5.6) |
| A few large trades carry the dollars | ATR-R equal-weighted **and** risk-weighted; both must agree in sign (§5.6) |
| Mark-to-market at window end | close everything at window end, with costs (§5.5) |
| Follower liquidated where the leader survives | leverage cap + hourly liquidation check (§5.5) |
| Copying every 1-second event / 0-delay | poller at deployable intervals 1/10/60 min (§5.5) |
| Scalpers and makers | copyability features on the lookback: holding time, maker share, fills/day (§5.2) |
| Maker leader vs taker follower | maker share is a feature; follower always pays taker (§5.2, §5.5) |
| Treating fills/trips/wallets as independent | wallet × window unit, date-clustered errors (§5.6) |
| Delays and cost levels as extra trials | one primary configuration at P4; the rest is robustness (§5.5) |
| Bear-market regime | hedged result is the decisive one (§5.6) |
| Same-day snapshot look-ahead | D−1 snapshot (§5.5, P2 gate) |
| Ranking on leaderboard ROI (deposit-distorted) | selection on position-level returns from fills (§5.3) |
| Rule-building leaking outcomes | features only, never returns, before P4 (§5.2) |
| Archive quirks (order, dex, UTC) | P3 gates (§8) |
| Results irrelevant at the owner's size | every run at nominal and real capital (§5.5) |
| P0 already looked inside the holdout | kept, exposure recorded (§0, registration 000) |

### P0b — the rebuilt simulator on the P0 wallet (mechanics only)

Same wallet as §11 (`0x3de9…`, inside the holdout, typical leverage taken
from the replayed window — look-ahead, pilot only). Leader profile: 204
fills/day, maker share 14%, 91 round trips, median hold 10.8 h, 18% of trips
under 1 h, typical leverage 0.81×.

| run | net % | fees $ | slip $ | orders | < min | < lot | max lev | max DD | daily mean / sd % | R median | R wtd |
|---|---|---|---|---|---|---|---|---|---|---|---|
| $10k, 1 min | 13.68 | 315 | 255 | 541 | 1 | 2 | 2.77 | 14.6% | 0.228 / 2.39 | −0.099 | 0.112 |
| $10k, 10 min | 13.18 | 296 | 236 | 449 | 0 | 0 | 2.30 | 15.3% | 0.220 / 2.42 | −0.101 | 0.118 |
| $10k, 60 min | 13.10 | 272 | 237 | 372 | 0 | 0 | 2.22 | 13.1% | 0.218 / 2.43 | −0.135 | 0.128 |
| $100, 1 min | 12.10 | 2.60 | 2.08 | 339 | 442 | 887 | 2.75 | 14.5% | 0.202 / 2.31 | −0.139 | 0.111 |
| $100, 10 min | 12.61 | 2.44 | 2.01 | 303 | 282 | 532 | 2.18 | 14.7% | 0.210 / 2.36 | −0.120 | 0.127 |
| $100, 60 min | 12.75 | 2.23 | 2.07 | 259 | 165 | 310 | 2.15 | 12.4% | 0.212 / 2.36 | −0.162 | 0.138 |

What it shows about the machinery:

- The views disagree, as the pre-mortem predicted: +13% in dollars, a
  typical trip at −0.10 to −0.16 R, risk-weighted +0.11 to +0.14 R. A
  dollar-only report would have read as a win.
- 0.22% / 2.4% daily over 60 days is t ≈ 0.7 — indistinguishable from luck,
  for one wallet, before any multiple-testing correction.
- At ~$100, over half the intended orders are unplaceable (min size, lot
  step), yet the result tracks the nominal run — the big moves are still
  captured. Whether that holds across wallets is a P5 question.
- Poll interval barely matters for a leader with a 10.8 h median hold; it
  will matter for the short-hold leaders the copyability filter targets.

Two bugs found and fixed while building it: the leverage cap and the
liquidation check both measured equity without the starting capital (every
target scaled to zero — caught by zero orders), and the mark-to-market end
(§5.5).

## 14. From research to following — how execution is planned

Three stages, each gated by the one before. Nothing in stage 3 exists yet,
and none of it runs before P6 passes.

### Stage 1 — research (local, offline; P2–P7)

Simulation decides **whether** anything is worth following: H0 (does skill
persist), then H1 / H2 (do the consensus and top-K books make money after
costs, hedged, at both account sizes). The output is a frozen rule, not a
list of wallets.

### Stage 2 — admission (periodic, human-approved)

Every window (8 weeks, the same cadence as the research), the **frozen**
selection rule runs on the trailing 3-month lookback:

1. Pull recent fills / snapshots (archive, or the HL API for the most recent
   days).
2. Apply cleaning + copyability filters and the ranking → K addresses, each
   with its normalization (equity, typical leverage) and the product's
   parameters.
3. Write that as a small, versioned leader-set file; the owner reviews and
   commits it. That commit is the admission.

Mid-window removals (a leader blows up, stops trading, breaches a drawdown
limit) happen only by rules registered at P4 — an ad-hoc removal is a new
trial. "Autonomy over search, never over the book" (spec §1.1) applies: the
machine proposes the set, a human admits it.

### Stage 3 — following (live, additive, separate from the frozen trader)

A new cron route — not the AI trader's analyze path or wake-watcher:

1. **Read the leaders** every poll (1 minute if the primary interval is 1;
   Vercel's cron floor). Positions come straight from HL `clearinghouseState`
   (weight 2 per leader, so K = 20 costs 40 of the 1,200/min budget); the
   `xyz` track needs the same call with a `dex` field (verify). If the read
   fails or is stale, **hold** — never trade on stale leader data.
2. **Compute target weights** with the same builder logic as the simulator
   (`Copy` / `Consensus`), then the same follower rules: leverage cap, band,
   lot step, minimum size, venue calendar.
3. **Diff against the follower's own positions** and send market orders
   through the existing venue clients (`lib/trading.ts` for Bitget,
   `lib/capital.ts` for Capital).
4. **State in KV only** on the per-minute path (last targets, last read,
   leader-set version); Postgres only when an order actually executes. This
   is the wake-watch cost pattern (§7 rule 4): ~130k KV commands/month,
   ~$0.26, and Neon stays asleep.

**Pull, not push — and why.** Copying is **state-based**: each poll reads
every leader's full position state and makes the follower's book match. A
failed poll is repaired by the next one. Hyperliquid also offers push
(websocket `userFills`, `clearinghouseState` with a `dex` field, etc., for
any address), but: Vercel functions cannot hold a socket, so push needs an
always-on worker; the documented limit is **10 unique users across
user-specific subscriptions per IP** (so K = 20 needs 2+ IPs); and an
event-driven copier drifts on any missed message unless it also re-reads
state. If push is ever added, it is a fast *trigger* for the same state read,
never the source of truth. Whether it is worth adding is decided by the
simulation's poll grid: if the 1-minute poll loses little against the
leader, polling stays.

Push options, if the poll grid says speed matters (researched 2026-10-07,
not verified hands-on):

| option | how | for | against |
|---|---|---|---|
| **QuickNode Streams → webhook** | their HyperCore stream, filtered by a watched-address list in their KV store, POSTs order/fill events to a Vercel function (their sample app "hypercore order monitor" does this for whale traders) | true push, seconds, no per-IP subscription limit, nothing always-on here | paid (pricing unverified); new vendor; the leader list is configured there — public addresses, but AGENTS.md rule 5 makes it an owner decision |
| **Vercel function holding a client websocket** | Fluid Compute keeps a socket open up to maxDuration (800 s on Pro); a cron starts overlapping ~10-minute listeners that subscribe to the leaders | no new vendor; per Vercel's guide, billed for active CPU while handling messages, not idle open time (memory-time billing to verify) | HL limit of 10 unique users per IP; Vercel egress IPs are **shared** with other tenants and HL limits are per IP — a risk for plain REST polling from Vercel too |
| Vercel Workflow / Queues | durable sleep loops, queues | — | not push: no event source, only a cron replacement |

**Not a derived strategy.** The copier does not learn rules from the
winners' past trades and trade those without them — if their edge is
information or timing, a rulebook loses exactly that, and it is the strategy
discovery the audit already closed. That could be a later lab hypothesis,
separately registered. H1 (consensus) still reads live positions; it only
aggregates them.

**Simulator–live parity.** The research engine is Python; the live path is
TypeScript. The builder and follower rules are small, so they are ported and
pinned by golden files: the simulator writes (leader positions, prices) →
(targets, orders) cases, and a TS test must reproduce them exactly. Without
that, the live copier could quietly differ from what was tested.

**Rollout.**
1. **Paper** (P7): the full loop with `dryRun` — reads leaders, computes and
   logs orders, places nothing. Run for weeks and compare paper fills with
   the simulator over the same days (tracking error).
2. **Tiny live** (P8): the smallest size the venue accepts, kill switches on —
   daily loss limit, max gross leverage, leader-data staleness, venue
   closed. Scale only after live tracks paper.

**Account isolation (decision C5).** The frozen AI trader holds BTCUSDT and
ETHUSDT on the same Bitget account. A copier there would net against its
positions and confuse its thread reconciliation. Use a Bitget sub-account with
its own API key (and a separate Capital account for TradFi), or retire the
AI trader first — before the paper copier places anything.

## Sources

- [Hydromancer Reservoir: Hyperliquid](https://docs.hydromancer.xyz/reservoir/hyperliquid)
- [Hyperliquid docs: Historical data](https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data)
- [Dwellir: userFillsByTime (10k-fill retention)](https://www.dwellir.com/docs/hyperliquid/hyperliquid-index/fills/user-fills-by-time)
- [Dwellir: Hyperliquid historical data](https://www.dwellir.com/docs/hyperliquid/historical-data)
- [Dune: Hyperliquid perpetuals](https://docs.dune.com/data-catalog/curated/perpetuals/hyperliquid/overview)
- [Allium: Hyperliquid data FAQ](https://docs.allium.so/historical-data/supported-blockchains/hyperliquid/data-faq.md)
- [DefiLlama: Hyperliquid competitive intelligence](https://defillama.com/pro/hyperliquid-competitive-intelligence-60s8le)
- [BlockEden: Perp DEX wars 2026](https://blockeden.xyz/blog/2026/01/29/perp-dex-wars-2026-hyperliquid-lighter-aster-edgex-paradex-decentralized-derivatives/)
- [Vercel Blob usage and pricing](https://vercel.com/docs/storage/vercel-blob/usage-and-pricing)
- [KuCoin: Is crypto copytrading profitable in 2026](https://www.kucoin.com/blog/is-crypto-copytrading-profitable-in-2026)
- [Congressional trading after disclosure delays](https://www.lambdafin.com/articles/congressional-stock-trading-performance-after-disclosure-delays)
- [Bitget Elite Trading API Guide](https://www.bitget.com/api-doc/uta/copy/Elite-Trading-API-Guide)
