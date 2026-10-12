# Edge discovery programme, round 3 (higher returns, fresh perspective)

**Started 2026-10-12, to be picked up cold.** Brief:
`docs/edge-discovery-next-prompt-2026-10-12.md` (holds the owner's mandate
verbatim). Predecessors: `docs/edge-discovery-2026-10-11.md` (round 2),
`docs/forced-flow-research-horizons-2026-10-10.md` (round 1). Benchmark:
`docs/hlp-benchmark-2026-10-10.md`. Ledger:
`research/copytrade/registrations/`, results in
`research/copytrade/results.jsonl`. Branch `copytrade-research`; commits are
local, the owner pushes. Nothing is published (AGENTS.md rule 5).

**Goal.** A consistent, capturable edge at our size (well under $1 M), in any
market we can access. The owner's "100% a week" is a search direction:
convex payoffs, high per-trade expectancy, levered premia with a measured
tail. Plausible landing zones: 30–100%+ a year with 30–50% drawdowns, or a
small-capacity high-Sharpe edge.

**Benchmark.** HLP: +19% in the spiral year, 2–4%/yr calm, Sharpe 1.4, max
drawdown −2.5%, long the spiral tail. Its lending leg: about 3%/yr.

---

## 1. Lights and status

| id | light | status | effect exists | deployable | vs HLP |
|---|---|---|---|---|---|
| N6 / L3 | Inverted 001 T3 positioning, decided on the unread HL holdout only (008) | **drafted; to review** | 1 in 5 (gross) | 1 in 20 (this book cannot trade: inverted discovery is +7.4 bp/day gross but **+0.1 bp/day net** at 001's 7.3 bp/day taker cost) | a confirm would only earn a cost-reduced follow-up; ≈ +9%/yr, Sharpe ≈ 0.8 at maker-like costs is an untested upper bound |
| N1 | HL `xyz` perps off-hours vs the underlying | **scout done; parked** — the premise mostly does not hold (US names are priced externally 24/5; re-anchoring is Sunday evening, not Monday's open); an index test cannot detect even full reversion | 1 in 3 | 1 in 6 | cheap to trade (index weekend round trip ~3 bp) but ~$2–6k per weekend at a hypothetical 20 bp edge |
| N2(a) | Long gamma when the liquidation map is dense | **closed at scout** (wing fills cost 10–20% of premium; a dense map coincides with *lower* IV) | 1 in 10 | 1 in 25 | −20 to −40% of the premium budget a year |
| N2(b) | Sized short-vol book on Deribit BTC/ETH (014: weekly ATM straddle, daily delta hedge; T2 idles on a DVOL spike) | **Revision 1; in review** | VRP sign public; net of costs 1 in 2 | 1 in 6 | n 0.40: +5–6%/yr after tail charge at VRP 0.10, +15–17% at 0.15; realistic worst month −5.6%, adversarial stress −27% |
| N3(i) | New Binance perps: short at first trade + 24 h, 1× isolated, 7 days (013) | **drafted; in review** | 1 in 2 | 1 in 4 | 3% per event, ~127 events/yr: +15%/yr at 4% net per event, +30% at 8% (assumed); worst month −28% plausible |
| N3(ii–iii) | Listing announcements; token unlocks | **parked** (no end-datable announcement API; no free point-in-time unlock calendar; `000i`) | 1 in 2 / 1 in 3 | 1 in 10 / 1 in 8 | — |
| N7-1 | Crypto trend following (Donchian ensemble, vol-targeted) | desk only; candidate | 1 in 3 | 1 in 5 overall | 10–30%/yr if alive; convex; only 2025-04 → 2026-06 is out of the paper's sample (t ≈ 1.1 at Sharpe 1) |
| N7-3 | Month-end liquidity cycle, equity index (settlement-lag natural experiment) | desk only; candidate | 1 in 3 | — | 8–15%/yr at 2–3×, tail −45% at 3× |
| N7-2 | BNB reward stack, delta-hedged | desk only; an operations question (pool size unpublished), not a trading test | 9 in 10 | 1 in 2 | 3–10%/yr in a 2025-like regime; Binance venue-failure tail |
| N7-4/5 | SPAC trust arbitrage; short VIX roll | desk only; deprioritised (lending-plus) | — | — | 3–8%/yr |
| N4 | Maker-only carry with a gap stop | **closed at desk** (perfect execution caps it at ~3.2%/yr on gross capital = the lending leg) | — | 1 in 25 | — |
| N5 | Crowd positioning extremes, majors, multi-day | **merged into N6**: the HL year is at 16 trials, so the only fair test is the holdout claim that 008 makes | — | — | — |

## 2. Family counts and holdouts

- **HL discovery year (2025-07-28 → 2026-06-30):** 16 trials, no pass.
- **HL holdout calendar (2026-07-01 → 2027-06-30):** unread. Under 012's
  D18 only L3 may claim it. Any other claim (e.g. an `xyz` confirmation)
  needs a registration that amends D18 openly.
- **The `xyz` dex** is in the same archive year but a different market. Its
  fills fed 001's features (`data/derived/xyz/features_*`), so N1 shares
  account-scoring selection with 001; it starts its own trial count and
  states that overlap.
- **Binance 2021–23:** 1 trial (009). 009's holdout A (2024-01 → 2025-07)
  is closed, released and unread.
- **L2 carry:** 3 trials, closed.
- **New data starts its own count,** stating the selection it shares.
- **2026-07-01 → is held back for every new light;** every scout query
  carries an end date before it.

## 3. Direction log

**2026-10-12 — round 3 opened; reading list done; five workers launched in
parallel.** Read in the brief's order: round 2 end to end, round 1 §1–2 and
§9, the HLP benchmark, every registration's result section, the 000–000h
ledger, all reviews, spec §2, AGENTS.md.

*What reviewers catch* (design these out from the first draft): forward
conditioning hidden in "counts" (000f); survivorship from cached nulls and
backward paging (010 erratum, 012 M1); gates shared by primary and variants
(007); guards that check a label instead of recomputing a decision (009 r3,
010 r1); unhashed modules on the decision path; understated tails (010 B3);
two point bars in series killing power (010 N5).

*Ranking and why.*
1. **N6 / L3 first to register.** The rule is fixed by 001 T3 (inverted,
   unchanged), it is decided on data nobody has read, most of what it needs
   is on disk (holdout snapshots, public Bitget closes), and the owner
   approved the requester-pays pull. Cheap and clean, low odds. It also
   absorbs N5: with 16 trials on the HL year, a positioning timer can only
   be decided on the holdout, and one claim is better than two.
2. **N1 first for discovery.** The best odds in the brief, a data
   combination few others have (every account's `xyz` fills joined with
   exchange opens), large per-event moves, and real volume.
3. **N2 and N3** next: convex or high per-event, and each can run on
   multi-year data with its own count.
4. **N4** is a feasibility question first (is there a credible maker-fill
   model?), so it goes to the desk scout rather than a data scout.
5. **N7** desk scout for lights outside the brief.

*Workers (all behaviour-only, forbidden conditionings named in each
brief):*
- `scouts/n1-xyz.md` — mechanics (oracle off-hours, funding, fees), session
  volume split, costs, who trades off-hours by skill decile, the
  deviation's distribution against the **previous** close only, free
  equity open/close sources, power, a proposed split. Forbidden: any link
  from an off-hours price/flow to any later price.
- `scouts/n2-options.md` — free Deribit data, costs, map/stress state
  frequencies, the **coincident** association of map state with implied
  vol, a multi-year signal proxy (Binance 2021–23 metrics), literature,
  tail sizing. Forbidden: any IV/map pairing with a later realised vol or
  payoff.
- `scouts/n3-listings.md` — point-in-time event sets (Binance, HL, Bitget
  listings; announcements; unlock calendars), first-hour data coverage,
  costs and shortability, literature sample periods, power. Forbidden: any
  post-event observation beyond row-count coverage.
- `scouts/n7-desk.md` — up to five new lights, ranked, plus the N4 maker-fill
  feasibility verdict.
- `registrations/008-inverted-positioning-holdout.md` (DRAFT), `r008_run.py`,
  `r008_fetch.py`, tests — the guarded holdout-only design, power by
  synthetic simulation, existence-test confirm with t_{G−1}, family and D18
  statement. Forbidden: any holdout quantity beyond counts and coverage.

*Orchestrator reads (for the ledger):* one DuckDB query over the `xyz` fills
(2025-10-13 → 2026-06-30): per-coin trade counts, notional volume, first and
last dates. No price relation, no outcome.

**2026-10-12 — N7/N4 desk scout: N4 closes on arithmetic; no hidden
100%-a-year light.** (`scouts/n7-desk.md`; checked by the orchestrator.)
- **N4.** Even with free execution and no gap loss, 012's funding
  (+27.8 bp of N over ~16 days) is 6.3%/yr on N, **3.2%/yr on gross
  capital: HLP's lending leg.** Retail maker fees are 1.5–2 bp; no rebate is
  reachable at our size. Free data cannot model HL maker fills (book history
  is requester-pays); Bybit ob500 is the only free queue-level source. A gap
  stop on a mostly-reverting spread (median +2 bp) crystallises the tail
  rather than truncating it. **Closed; no data scout.**
- **N7 ranked:** trend following (the only plausible path to 30%+, decaying
  evidence, low power out of sample); the BNB reward stack (certain cash
  flow, unknown size, an operations question); month-end equity liquidity
  cycle (a clean natural experiment on free data, but a 3× equity tail);
  SPAC trusts and short VIX (lending-plus). Fourteen ideas rejected with
  reasons (pre-FOMC, index inclusion, FX fix, gamma momentum, sports value
  betting, CEX–DEX and DeFi liquidation races, depeg backstops, …).
- **Correction to the brief (verified):** the local Binance aggTrades and
  bookDepth are not "cascade days only": BTCUSDT has 334 daily files,
  2025-07-28 → 2026-06-30 (HYPE 302). They sit in the HL-year calendar.
- **Decision:** none of the N7 lights is drafted yet. Trend following is
  held as the convex candidate if N1/N2 fail; it needs a forward holdout to
  have power.

**2026-10-12 — N3 scout: new perps are testable cleanly; announcements and
unlocks are not.** (`scouts/n3-listings.md`; the orchestrator recounted the
event table: 671 listings 2020 → 2026-06, per-year 77/58/26/99/131/239/41,
exactly as reported; the power arithmetic re-read in
`data/scout_n3/derived/power_out.txt`.)
- **Events.** Binance's archive gives every listing from its first trade
  with full first-hour 1m bars, funding and premium. **The regime changed:**
  spot traded before the perp in 77/99 listings in 2023, 15/239 in 2025,
  0/41 in 2026-H1.
- **Shortable on the owner's venues by Binance day 1:** HL or Bitget in
  51/99, 89/131, 156/239, 32/41 (2023 → 2026-H1). Capacity about
  $50–250k per event (first-hour volume medians $10–33 M, 1-hour OI
  $1.6–3.1 M).
- **Costs and tail.** New Binance perps settle funding every 4 h (±2% cap;
  a capped contract can go hourly). Squeezes are real (MYX +1,400% in 5
  days; XPL hyperp 2.5× in minutes; JELLY settled at a chosen price).
- **Power.** Non-event small-cap 7-day excess SD 8.3% (kurtosis 16). On
  holdout A (260 events, 67 weeks) the week-clustered MDE is 2.8%, or 5.6%
  if listing windows are twice as volatile. Power is not the constraint.
- **Exposure.** The scout's web helpers saw Binance announcement titles
  after 2026-07-01 and post-cutoff unlock literature: **ledgered as
  `000i`.** Published studies already report the post-listing decline for
  2023–26 listings, so our calendars are fresh to our code but not to public
  priors.
- **Decisions.**
  1. **Draft 013 now** (new Binance perps). The calendar follows the
     regime we would trade: **discovery = 2024-01-01 → 2025-07-27** (009's
     released, unread holdout A); **confirmation = 2025-07-28 →
     2026-06-30**, Binance only; 2020–23 a reported block (spot-first,
     overlaps 009's discovery); 2026-07 → sealed. At most two trials. The
     squeeze tail is bounded by construction (isolated margin) rather than
     by stops. Confirmation is an existence test with t_{G−1}.
  2. **The 1m-kline pull (~1.03 GB) goes ahead without asking:** free,
     keyless, under the mandate's 5 GB line.
  3. **Announcements and unlocks are parked:** no end-datable source, and
     the 2026-07 → announcement calendar is partly seen (`000i`).

**2026-10-12 — N1 scout: `xyz` is cheap and busy, but the light's premise
mostly does not hold, and a weekend test lacks power.** (`scouts/n1-xyz.md`;
the orchestrator re-read `data/scout_n1/derived/`: index weekend notional
$5.46 bn of $83 bn = 6.6%; XYZ100 Sunday 20:00 UTC deviation from the
previous close, |median| 31 bp, |p90| 107 bp, n 37; NDX weekend
close-to-close SD 111 bp.)
- **Mechanics.** US single names take an outside price 24/5 (Blue Ocean
  overnight via Pyth); indices and commodities follow CME 23/5. Prices are
  set internally only at weekends, holidays, the daily 17–18 ET break, and
  on every weekday night for Korean, Japanese and HK names. The internal
  price is a moving average toward the perp's own book (time constant 8 h →
  1 h on 2025-11-21 → 30 min on 2026-04-30), bounded at ±1/max leverage
  around the last outside price. **Re-anchoring is on the first outside
  tick on Sunday evening ET, not at Monday's open.**
- **Costs are low:** growth-mode fees 0.9 bp taker / 0.3 bp maker on 123
  assets; spreads about one tick; a weekend round trip of $10–50k costs
  about 3 bp in indices and 8–12 bp in single names. Weekend depth is about
  3× thinner.
- **Who trades off-hours is not "the losers".** Accounts liquidated before
  the window carry a *smaller* share of weekend notional (23%) than of the
  regular session (33–41%). New accounts carry more. There is no `xyz`
  skill score; computing one needs account P&L, so it would be a registered
  step.
- **Power.** Clustered by weekend, the MDE is 53 bp per event for XYZ100
  (37 weekends) and 68 bp in a 23-weekend discovery, against a typical
  deviation of 31 bp: **an index fade test cannot detect even full
  reversion.** Single names pooled: 83–100 bp. Korean nights (~90): ~53 bp.
- **Free data.** FRED index closes (keyless, server-side end date) only. No
  free, terms-compatible per-stock opens, and no free off-hours fair value
  (Stooq challenge, Pyth keyed, Dukascopy requester-pays). A test could use
  the perp's own last outside price and its post-resumption price, both
  local.
- **Exposure:** current-state docs and metadata, four example prices, one
  headline claiming weekend `xyz` price discovery: **ledgered as `000j`.**
- **Decision: N1 is parked, not drafted.** Odds lowered to 1 in 3 / 1 in 6.
  The capacity arithmetic (about $0.1–0.3 M a year at a hypothetical 20 bp
  net edge) is not worth a trial whose index form cannot see its own
  effect. A mechanism-signed design note for later: when the internal price
  is pinned at its bound, or the moving-average oracle lags the book, the
  move at resumption should *continue*, not revert. It is pooled across
  single names and Asian nights for power. It is drafted only if N2 and
  013 fail and a power check on counts supports it.

**2026-10-12 — N2 scout: the map adds nothing measurable to options; what
remains is the plain volatility premium, which fits the mandate as a
levered premium with a measured tail.** (`scouts/n2-options.md`; the
orchestrator re-read `data/scout_n2/analysis_all.txt`: BTC map density vs
DVOL Spearman −0.46; ≥ $77 M buckets on 29 of 338 days, in 18 of 49 weeks;
≥ $15 M in 44 of 49 weeks; the straddle tails below.)
- **Data.** Deribit's history API is keyless and date-bounded (option trades
  with IV, mark and index, back to at least 2019-07; DVOL hourly from
  2021-03-24). The full BTC+ETH tape is 7–12 GB (not pulled; would need the
  owner). A design-sized pull is about 0.2 GB. No free multi-year
  liquidation record and no multi-year map proxy exist.
- **Costs.** Fee 0.0003 per contract capped at 12.5% of premium: 8–12.5% of
  premium on 1–7-day 5–15-delta wings, plus 2–7% half-spread. A near-ATM
  3–7-day straddle sold and held to expiry costs about 1.5–4% of premium.
- **Coincident pricing.** A dense BTC map coincides with *lower* implied vol
  (−0.46; DVOL 47.6 sparse vs 39.0 dense): the map marks calm, leveraged
  regimes. Skew does not follow the map's side. DVOL barely moves in
  stress hours (+0.4 / +0.7 points).
- **Power.** Strangle payoff SD/mean 2.7–2.8, puts 4.9; the top 3 of ~300
  weeks carry 16–40% of the payoff. Map-filtered (a): MDE ≈ 1.5 of premium
  on weekly HL-year trades. Short-vol (b): MDE ≈ 0.22 on three years weekly,
  against a published VRP ≈ 0.10; the map's increment ≈ 0.48.
- **Tail (stylised, underlying moves only).** A weekly ATM straddle: worst
  week −35% (BTC, 2020-03) / −55% (ETH, 2021-01), worst 4 weeks −61% /
  −73%. At ¼ Kelly: worst week −11% / −16%, month −18% / −22%.
- **Exposure:** unconditional vol levels and a published short-vol backtest
  overlapping holdout A: **ledgered as `000k`.**
- **Decisions.**
  1. **(a) closed** at scout: the costs and the coincident sign both point
     against it.
  2. **The map is dropped from N2.** It lives only in the 16-trial HL year
     and its increment is undetectable.
  3. **Draft 014: a sized short-vol book on Deribit,** T1 unconditional, T2
     standing aside in a pre-declared multi-year stress state. The drafter
     is asked to raise power by construction (delta-hedging on the perp,
     shorter tenors, BTC+ETH pooled), not by data. Discovery 2021-03-24 →
     2023-12-31; decision holdout 2024-01-01 → 2026-06-30 (unread by our
     code; a published backtest covers part of it, `000k`); confirmation as
     an existence test. The design-sized pull (≈ 0.2 GB) goes ahead under
     the mandate; the full tape does not.
  4. **Why spend a trial on a known premium:** the question is not whether
     the VRP exists but whether it survives Deribit's real costs at a size
     whose tail is pre-priced, which is the mandate's "levered premium with
     a measured tail". Expected landing: +8–15%/yr, Sharpe under 1, worst
     month −18 to −44%. That is above HLP's calm return and far worse in
     drawdown; the owner sees that trade-off in the registration.

**2026-10-12 — 008 drafted; correction: the inverted book does not net.**
(`registrations/008-inverted-positioning-holdout.md`, DRAFT; 86/86 synthetic
tests.)
- **Correction to this document's N6 row (orchestrator's error).** I used
  001's synthetic "~5 bp/day" cost remark. The book's own code
  (`ct/crowd.long_short`, checked by the orchestrator) pays 11 bp one way on
  a gross-1 tranche at entry and exit, a third turning daily: **7.33 bp/day.**
  001's net −14.75 bp/day is therefore about −7.4 gross; inverted, **+7.4
  gross and +0.1 bp/day net (≈ +0.6%/yr).** Half of 001's t −3.4 was its own
  cost.
- **Design.** The confirm becomes an existence test on the book *before
  trading costs* (funding kept), BTC-beta-adjusted, week-clustered t ≥
  t_{G−1}(0.95), median > 0, both halves > 0. The beta adjustment matters:
  a raw-mean rule would confirm pure beta 15–41% of the time in a known
  rally. A confirm can lead only to a cost-reduced or paper registration,
  never to trading this book.
- **Calendar.** 001's 56-day grid continued: S = 2026-06-08 (lookback all
  discovery; 30 signal days in July) and S = 2026-08-03 (53 days). End
  2026-09-28: 83 book days, 13 week clusters. Power at the full inverted
  magnitude **0.30** (half: 0.14); false confirm 0.044–0.051 under the plain
  and beta nulls. The later end (2026-11-23) gives 0.44 for ~13.5 GiB and
  cannot run before 2026-11-30.
- **Pull.** 33 HL fill days (2026-07-01 → 08-02), 7.91 GiB listed, about
  3.3 GiB transferred (~$0.30), requester-pays, under the owner's approval
  and cap. Everything else is on disk.
- **Family.** First and only reader of the HL holdout calendar under D18;
  2026-09-28 → 2027-06-30 stays unread. HL discovery year stays at 16.
- **Orchestrator reading.** 1 in 5 that the gross effect exists; power 0.3
  even if it does; and deployable only after a further registration.
  The value is closing fact 6 honestly for about $0.30. **Next: two
  independent reviews in parallel,** with the before-costs confirm, the
  beta adjustment and the new restart path as named probes.
- **Test suite note.** The two state tests the brief named
  (`test_factors`, `test_vaults`) pass since commit `72124e2`. One stale
  state test remains: `tests/test_carry012.py` ("status line says DRAFT";
  012 is REGISTERED). Left for the owner.

**2026-10-12 — 008 review A: not ready (one blocker, four majors); the
holdout months were already seen by other studies.**
(`registrations/reviews/008-review-a.md`.) Sound: the book is 001 T3's code
byte-identical with the sign flipped; the calendar has no look-ahead; power
reproduces independently (0.311 / 0.154); the 13-week clustered t holds its
size under fat tails and GARCH-type BTC.
- **B1.** One pooled BTC beta across two cohorts and three months of very
  different drift leaks beta error into α: false confirms 0.11–0.25 with no
  alpha. Fix: a separate BTC slope per calendar month (size back to
  0.046–0.049, about −0.005 power).
- **M1.** BTC is the only factor for a dollar-neutral alt book; an
  alt-minus-BTC trend gives false confirms 0.11–0.18. Fix: add an
  equal-weight alt-index regressor.
- **M2.** The economic bar binds almost never (P(bar | confirm) 0.96–0.99)
  and routes confirms to a paper test of a book §0 calls undeployable. Fix:
  apply it to the shrunk estimate or the lower bound; every confirm leads to
  a cost-reduced registration.
- **M3 (verified by the orchestrator).** Earlier looks at the same months:
  the 2026-09-14 alpha-lab sweep (83 Bitget perps through 2026-09), and the
  2026-10-11 inverted-AI study, which found a contrarian payoff in "three
  particular months", 2026-06-27 → 10-10, one day before this round's brief
  ranked N6 first. **Ledgered as `000l`.** Orchestrator decision: 008's
  confirm regression also controls for a point-in-time short-term reversal
  factor, so a confirm cannot be the reversal already known to have paid.
- **M4.** The tail figures contradict each other; one scenario carried
  through (about −8 to −10% a week, −12 to −16% a month).
- Ten minors, including the symbol table dated 2026-10-07 (coins delisted
  during the holdout drop out with hindsight) and a byte undercount in the
  fetch on a failed attempt.
- **Next.** Revision 1 waits for review B, so that one revision addresses
  both.

**2026-10-12 — 008 review B: not ready (two blockers, four majors); Revision
1 ordered.** (`registrations/reviews/008-review-b.md`.) Sound: 86 tests,
reused modules identical to `efefc7f`, hashes match, every §3 figure
reproduces.
- **B1.** The gates check whichever repo is passed in: a throwaway repo
  with an open gate let the run read the real data tree; a mock-hooks dict
  missing a key fell back to the real requester-pays hook.
- **B2.** The Bitget close fetch stops on symbols Bitget has removed
  (no history served, per the N3 scout), so it could never finish, and the
  frozen code would need an amendment after a started row.
- **M1** snapshot set not pinned (adding files moved 83 → 85 signal days);
  **M2** the 15 GiB cap is not cumulative and failed transfers are not
  counted; **M3** 008 reads holdout rows of `snapshot_accounts.parquet`,
  which D18 (a) says no registration reads; **M4** survivorship from the
  2026-10-07 symbol table now biases toward a confirm.
- **Orchestrator decisions.**
  1. **D18 tension resolved openly.** D18 (b) lets L3 read "the series its
     registration names"; D18 (a)'s "neither is read by any registration"
     was written for 012. 008 carries an explicit D18 amendment: L3 alone
     may read the on-disk holdout snapshot rows, through its guard, for the
     dates it names. Approved by the orchestrator under the owner's
     2026-10-12 delegation; reported to the owner.
  2. **Revision 1** addresses every blocker and major of both reviews plus
     `000l`: α must survive a per-month BTC slope, an equal-weight alt
     index and a short-term reversal factor; the economic bar on the shrunk
     estimate; every confirm leads to a cost-reduced registration on the
     remaining calendar; one coherent tail scenario; guard bound to the real
     repo; delisted symbols skipped and recorded; pinned snapshot paths;
     cumulative byte accounting; read-once rows committed immediately and
     discarded rows detected; package versions frozen.
  3. After Revision 1, one re-review, then REGISTERED under the mandate if
     no blocker remains.

**2026-10-12 — 014 drafted: power is low and the tail caps the size, so a
confirm would land at lending-like returns; one bounded computation
authorised to settle it.** (`registrations/014-short-vol-deribit.md`,
DRAFT; 121 synthetic checks; pull 248 MB, 0 rows ≥ 2026-07-01.)
- **Family (Deribit options, new count 2).** T1: a weekly 7-day ATM straddle
  on BTC and ETH, sold Friday 08:00 UTC, delta-hedged daily on the perp,
  held to expiry. T2: the same, idle when DVOL ≥ 1.25 × its trailing 7-day
  minimum (DVOL, not Binance OI: clean, point-in-time, no missing hours).
  The construction was chosen on synthetic power (H8 and H24 tied; the tie
  went to the smaller synthetic worst week; disclosed as chosen after seeing
  both tables).
- **Costs** about 4.9% of premium in total: break-even at gross VRP ≈ 0.05.
- **Statistics.** A negatively skewed weekly series makes the plain t
  over-reject 2–6×; Hall's skew-corrected t with floors restores size
  (net-zero false pass: discovery 0.009–0.018, holdout 0.038–0.051).
- **Power at the published VRP prior (0.10 of premium): discovery 0.21–0.24,
  joint 0.06.** At 0.15: 0.54–0.63 and 0.30–0.39.
- **Size and tail.** Under an adversarial bound (each event's whole weekly
  excursion as one gap between hedges), tail-aware Kelly is 0.40–0.55 and
  the proposed n = 0.10 notional per unit of equity gives a worst week −5.4%
  and month −6.7%, but **only +2.2–2.5%/yr at VRP 0.10**: HLP-calm territory.
- **Orchestrator decision.** The bound, not the data, sets the return. The
  realistic hedged loss in the worst weeks *before* 2021-03-24 lies outside
  both test calendars and decides sizing, so it is authorised as a bounded
  computation, ledgered in advance as **`000m`**. If the realistic tail
  allows a materially larger n, 014 goes to review; if not, 014 is parked as
  "a confirm cannot beat the lending leg at a tail-respecting size", and no
  trial is spent.

**2026-10-13 — 013 drafted; two independent reviews launched.**
(`registrations/013-new-perp-listings.md`, DRAFT; 124 synthetic checks; pull
20,721 files, 0.73 GB, every file matched Binance's .CHECKSUM.)
- **Family (2, one-sided 2.5% each; exactly one carried to confirmation).**
  T1: short every new Binance perp (first archive segment) at first trade +
  24 h, 1× isolated, no stop, 7-day hold. T2: T1 restricted to events whose
  day-1 funding summed ≥ 0 (shorts not yet crowded; known at entry).
- **Sizing and tail.** 3% of capital per event, ≤ 4 entries a week and 15 a
  month; worst event −3% (the margin), worst week −12% (bound), worst month
  −28% plausible / −45% bound. About 127 events a year. Assumed, not
  estimated: +15%/yr at a true 4% net per event, +30%/yr at 8%.
- **Power** (listing windows twice as volatile, the likely case): joint
  0.12 / 0.34 / 0.60 at a true 4 / 6 / 8% net per event. The confirmation's
  own false pass 0.049–0.052; a beta-only world passes the full path
  0.005–0.021.
- **Counts** (reported / discovery / confirmation): 255 / 257 / 148
  events; pre-market 0 / 4 / 22 (excluded, reported); spot-first
  224 / 61 / 0 — the confirmation calendar is all perp-first.
- **Calendar lock (D17 b), added here as the drafter asked:** the
  confirmation calendar's listing windows (2025-07-28 → 2026-06-21, Binance
  new perps, days 0–9) are reserved for 013's confirmation; no other
  registration computes outcomes on them until 013 reaches a terminal state.
- **Read during drafting:** one first-minute OHLC row (ALPHAUSDT
  2020-11-23, reported block) printed from a header-less file; nothing
  computed from it. Listed in 013 §13.
- **Reviews:** A (design, statistics, tail: execution-venue validity, mark
  vs last-price liquidation, overlapping holds in week clusters, β fixed at
  1, slippage tiers); B (guard bound to the real repo, point-in-time event
  rules, delistings mid-hold, checksum code, read whitelist).

**2026-10-13 — 014 Revision 1: the realistic tail is a quarter of the
bound; size 0.10 → 0.40; 014 goes to review.** (`000m` addendum; the
orchestrator re-read `tail_results.json`.)
- **The real stress weeks** (21, chosen by underlying excursion ≥ 25%,
  2019-03 → 2021-03, outside both calendars), H24 book unchanged: worst
  −14.1% of notional (2019-10-25: BTC +40% inside the first day), then
  −12.3% (2021-01-01) and −10.9% (2019-05-10); 2020-03-06 only −5.2%, because
  the crash came on day 6 when the put was deep and hedged. No liquidation
  at any size tried. Worst 4-week path in the 2020-03 cluster −11.1%.
- **Sizing.** Tail-aware Kelly at VRP 0.10 rises to 1.65 (fat world);
  n = 0.40 = ¼ of it, capped by the adversarial stress (worst week ≤ 25% of
  equity → n ≤ 0.46).
- **At n = 0.40:** realistic worst week/month −5.6% of equity; adversarial
  stress −21.5% / −26.8%. **Expected +5.3–6.3%/yr after the tail charge at
  VRP 0.10; +15–17% at 0.15; about 0 at 0.05.** Joint power 0.06 / 0.30–0.38
  at 0.10 / 0.15. False pass at net zero ≤ 0.0015 joint.
- **Reading.** Above HLP's calm years, with a different (short-vol) tail;
  the 30–100% zone is out of reach (full Kelly ≈ 37–41%/yr gross with the
  stress at −89% of equity). The test can only see a premium nearer 0.15
  than 0.10. It is the programme's one priced, measured-tail premium, so it
  goes to two independent reviews, as decided in advance.

## 4. Distance to goal

Unchanged from round 2 at the start of this round: no candidate has a
demonstrated edge that beats HLP on a risk-adjusted basis. Best supported:
010 T2 (BTC/ETH/SOL cross-venue carry), +2.96%/yr, economically the lending
rate.

## 5. Data inventory

Everything in round 2 §5, plus what this round's scouts add (each under
`research/copytrade/data/scout_<id>/`, gitignored, with `SHA256SUMS`).

| asset | where | coverage | notes |
|---|---|---|---|
| HL `xyz` fills, every account | `data/pruned/by_dex/xyz/fills/perp/all/` | 2025-10-13 → 2026-06-30 | 6.7 GB pruned; 84 coins incl. indices (XYZ100, SP500), single stocks, commodities (CL, BRENTOIL, GOLD, SILVER), FX |
| HL `xyz` daily snapshots | `data/archive/by_dex/xyz/snapshots/perp/` | 2025-10-13 → **2026-10-06** | **partitions ≥ 2026-07-01 are holdout: filter by partition date** |
| HL `xyz` derived features | `data/derived/xyz/` | selections 2026-01-12, 03-09, 05-04 | 001-style account features |

## 6. Workers in flight

| worker | deliverable | started | state |
|---|---|---|---|
| N1 scout | `scouts/n1-xyz.md` | 2026-10-12 | done; `000j` |
| N2 scout | `scouts/n2-options.md` | 2026-10-12 | done; `000k` |
| 014 drafter | `registrations/014-short-vol-deribit.md` | 2026-10-12 | Revision 1 (realistic tail, n 0.40) |
| 014 review A (design) | `registrations/reviews/014-review-a.md` | 2026-10-13 | running |
| 014 review B (guard/data) | `registrations/reviews/014-review-b.md` | 2026-10-13 | running |
| N3 scout | `scouts/n3-listings.md` | 2026-10-12 | done; `000i` |
| N7 + N4 desk | `scouts/n7-desk.md` | 2026-10-12 | done |
| 013 drafter | `registrations/013-new-perp-listings.md` | 2026-10-12 | drafted |
| 013 review A (design) | `registrations/reviews/013-review-a.md` | 2026-10-13 | running |
| 013 review B (guard/data) | `registrations/reviews/013-review-b.md` | 2026-10-13 | running |
| 008 drafter | `registrations/008-inverted-positioning-holdout.md` | 2026-10-12 | drafted; reviews next |
| 008 review A (design) | `registrations/reviews/008-review-a.md` | 2026-10-12 | done: 1 blocker, 4 majors; `000l` |
| 008 review B (guard/data) | `registrations/reviews/008-review-b.md` | 2026-10-12 | done: 2 blockers, 4 majors |
| 008 Revision 1 | drafter, resumed | 2026-10-12 | running |
