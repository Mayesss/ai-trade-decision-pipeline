# Research orchestrator, round 3 continued: finish 008, then choose

You run this research as an orchestrator: plan, delegate to subagents, read
every result yourself, verify decisive claims, and decide. The goal is
unchanged: **a consistent, capturable edge** at our size (well under $1 M).
The owner's mandate (autonomy to register and decide, risk allowed when
estimated and smaller than the reward, free safe data without asking, aim
very high) is in `docs/edge-discovery-next-prompt-2026-10-12.md` and still
applies in full.

## Read first

1. `docs/edge-discovery-2026-10-12.md`: this round's programme document —
   lights table, family counts, the direction log (§3), the distance to goal
   (§4). Read the direction log end to end.
2. The registrations of this round:
   `research/copytrade/registrations/008-inverted-positioning-holdout.md`
   (REGISTERED, Amendment 1, §16), `013-new-perp-listings.md` (Result §),
   `014-short-vol-deribit.md` (PARKED note at the top).
3. Ledger entries `000i` … `000o` and the reviews `reviews/008-*`,
   `013-*`, `014-*`.
4. `docs/alpha-lab-spec.md` §2 and `AGENTS.md`.

## Job 1 — finish 008 (time-gated; do this first)

008 is REGISTERED (`2bd855a`, Amendment 1 `963dcc8`). Fetch attempts 1–2
ran on 2026-10-13. Attempt 2 got everything except one genuine Bitget gap:
`closes_partial:PONSUSDT:1787760000000` (19 hours missing). By the
registered persistence rule, the same refusal on a chain of 3 attempts, each
≥ 24 h after the previous by commit time, records the name as unavailable
(1 of 168; ceiling 8), and the fetch then completes.

1. **Attempt 3 no earlier than 2026-10-14 06:40 +0200; attempt 4 no earlier
   than 24 h after attempt 3's end commit.** Before each attempt:
   `git status` must show no untracked or modified `.py` at
   `research/copytrade/` top level or in `ct/` (check (d) refuses
   otherwise), and nothing else may be running that commits
   `results.jsonl`. Command, from `research/copytrade/`:
   `.venv/bin/python -I r008_fetch.py` (it commits its own rows; it never
   pushes).
2. If a **new** refusal appears, read it; do not route around it. More than
   8 persistently unavailable names, persisting on 3 attempts ≥ 24 h apart,
   closes 008 unread (registered).
3. **When the `holdout_fetched` row exists — the A1 procedural check
   (binding, ledger `000o`):** verify that the fetched row's
   `registration_commit` equals `963dcc856366a4d05bfc68499d0b255b1ab5a358`,
   and that `git diff 2bd855a..963dcc8` touches only `r008_fetch.py` and
   `r008_run.py` among guarded code (plus the registration's status line,
   two hash rows and §16). Record both in a `000o` addendum *before* the
   approval line. Also read the fetched row's byte count (cap 15 GiB;
   ≈ 3.35 GiB so far), the unavailable names and the coverage.
4. **Approve and read:** add the line
   `008 holdout read: APPROVED <date> row <sha256 of the holdout_fetched line>`
   under §12 of 008, commit, then
   `.venv/bin/python -I r008_run.py --check` and
   `.venv/bin/python -I r008_run.py --run`. One read. A mechanical stop
   after `holdout_started` has a registered restart path (§11); a commit
   failure must be committed by hand, as is.
5. Write the result into 008 in its own terms (the confirm is an existence
   test on the factor-adjusted α *before trading costs*; labels
   `confirm_gross_only` / `confirm_gross_and_shrunk_net` /
   `not_demonstrated`; the persistence-sensitive and survivorship flags are
   reported). **Any confirm leads only to a cost-reduced registration on
   the remaining calendar (decisive span from 2026-10-08); this taker book
   is never traded.** Expected odds: about 1 in 5 that the gross effect
   exists; power 0.28 at the full inverted magnitude.

## Job 2 — choose the next registration

Nothing tested so far beats HLP. Candidates, with this orchestrator's
reading (re-rank freely; say why in the log):

- **013 T2 frozen, on the unread confirmation calendar** (Binance new perps
  listed 2025-07-28 → 2026-06-21; never read by any outcome computation;
  `000n` printed aggregate funding-row totals for it). Cheap: the panels,
  code and calibration exist. A new trial (Binance listing family: 3),
  selected after seeing T2 do best, which must be stated. In discovery: α
  +3.4% per event (t 1.24), book ≈ +6%/yr at 2.5% per event, drawdown −8%.
  Odds of a confirm about 1 in 5; a likely return of 3–6%/yr unless sized up.
- **A listings rule that addresses the squeeze tail by mechanism.** 013
  showed a median +9% net event and a mean of +2%: 6.6% of events liquidated
  at 1×. Any new rule must be designed from mechanism (not from 013's
  discovery outcomes, which are now read) and decided on the unread
  confirmation calendar. One trial per calendar: choose between this and the
  T2 follow-up.
- **N7-1, crypto trend following** (`scouts/n7-desk.md`): the only light
  with a plausible path to 30%+, decaying evidence; only 2025-04 → 2026-06 is
  out of the paper's sample (low power). Would need a forward holdout.
- **N1, `xyz` off-hours** (parked; `scouts/n1-xyz.md`): a mechanism-signed
  continuation design at the price bounds, pooled across single names and
  Asian nights, only if a power check on counts supports it.
- **014 short vol** (parked): only if redesigned per its PARKED note
  (forward pricing; hedging more often or an explicit single-gap atom; size
  detached from the test; the regime null). Its two Deribit calendars
  (2021-03-24 → 2023-12-31, 2024-01-01 → 2026-06-30) are unread.

## Holdouts and family counts

- **HL discovery year (2025-07-28 → 2026-06-30):** 16 trials, no pass.
- **HL holdout calendar:** 008 is its first and only reader (2026-07-01 →
  2026-09-28, by D18 amendment D13). **2026-09-28 → 2027-06-30 stays
  unread**; a confirm of 008 would use it (decisive span from 2026-10-08).
- **Binance listing windows:** 2 trials (013), no pass. The confirmation
  calendar is unread (lock lifted).
- **Deribit options:** 0 trials (014 parked); both calendars unread.
- **Binance 2021–23:** 1 (009). **L2 carry:** 3, closed.
- **The 2026-07 → 2026-09 market was looked at by other studies** (`000l`):
  any registration on those months states it.

## Lessons from this round (design them out from the first draft)

- **Market adjustment is not optional.** Every review this round found a
  test that a beta or a falling alt market could pass (008 B1, 013 B1 twice).
  Decide on α with β estimated per regime cell (month or quarter), plus alt
  and reversal factors where the book is an alt book; calibrate critical
  values on regime worlds.
- **Integrity faults recur until ported:** guards bound to the real repo;
  gate tokens on reads; rows committed by the runner and scanned with
  `--all --reflog --text --no-textconv --no-ext-diff`; shadow-module scans
  (packages, sourceless `.pyc`, `.so`, git-ignored); a fresh
  `sys.pycache_prefix`; no test that goes stale at commit or REGISTERED.
  `r008_run.py` and `r013_run.py` are the reviewed templates.
- **Endpoint boundaries:** Bitget `history-candles` `endTime` is exclusive
  (`000o`). `ct/bitget.py` has the off-by-one; closed runs stand, and any new
  Bitget fetcher is tested against an exclusive-end mock. HL
  `fundingHistory` is inclusive at both ends.
- **Size before odds:** a premium visible in the data can be capped by its
  tail at a survivable size (014: ≈ 2–3%/yr). Compute the robust size and
  the expected return at that size before spending a trial.
- **Commit drafts before any authorised computation** that depends on them
  (014's tail run had no committed Revision 0).
- **Sequence commits:** a guard that refuses on any modified `.py` blocks
  every registration while another draft is being edited. Plan for it.

## Hard rules (unchanged)

- **AGENTS.md rule 5:** nothing is published, uploaded or shared.
- The live trader is off; do not touch `lib/`, `pages/`, `vercel.json`.
  Branch `copytrade-research`; commit locally; the owner pushes.
- Never pull bulk bar data out of Neon.
- The ledger protocol: hypothesis → DRAFT → behaviour-only feasibility →
  two independent reviews → REGISTERED (the orchestrator may take this step
  under the mandate) → a guarded runner that runs once → the result written
  into the registration.
- One stale state test remains for the owner: `tests/test_carry012.py`
  ("the status line says DRAFT"; 012 is REGISTERED).

## Report to the owner at each result

The result in the registration's own terms; the distance to goal (%/yr,
Sharpe, drawdown against HLP); the next step; your odds.
