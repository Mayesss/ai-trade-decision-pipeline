# Next session: the edge research is paused; what is open, and when to reopen

The owner and the orchestrator agreed on 2026-10-13 to **stop the
open-ended search** after round 4. This prompt says what is still open, how
to finish each item if the owner asks, and what would justify reopening the
search. Read it before doing anything in `research/`.

## State in one paragraph

Four rounds and about 20 trials (registrations 001–016, ledger `000`–`000u`)
found nothing that beats Hyperliquid's HLP vault (+19% in the spiral year,
2–4%/yr calm, Sharpe 1.4, max drawdown −2.5%). Every premium found was
either gone after publication (015, the month-end liquidity cycle:
`not_demonstrated`, about a third of the published size at most) or real but
eaten by costs and tails at a size we could survive (funding carry 009–012,
new-listing shorts 013, short vol 014). The programme documents, newest
first: `docs/alpha-discovery-2026-10-13.md` (round 4), `docs/edge-discovery-2026-10-12.md`,
`docs/edge-discovery-2026-10-11.md`, `docs/forced-flow-research-horizons-2026-10-10.md`.

## Open items

1. **016 — crypto trend following, the paper's rule verbatim**
   (`research/alpha/registrations/016-crypto-trend-verbatim.md`). Status at
   the end of round 4: see the registration's status line and the programme
   document §3 (Revision 2 with the forward span to **2028-12-31**; the read
   is gated to **2029-01-08**). Nothing runs before then. When the date
   comes: `python3 -I -S research/alpha/r016_fetch.py`, the approval line
   the registration specifies, then `python3 -I -S research/alpha/r016_run.py
   --run`, once. Outcomes: *alive* / *not_demonstrated* / *decayed*; any owner
   allocation to the rule ends at the read unless the outcome is *alive*.
2. **008 — inverted 001 T3 positioning on the HL holdout: OPTIONAL (owner,
   2026-10-13).** Odds 1 in 5 that the gross effect exists, 1 in 20
   deployable; even a confirm only earns a cost-reduced follow-up. Attempts
   1–2 ran on 2026-10-13; one genuine Bitget gap (PONSUSDT) remains. The
   persistence rule sets only a *minimum* gap (≥ 24 h by commit time), no
   deadline. If the owner asks to finish it, follow
   `docs/edge-discovery-next-prompt-2026-10-13.md` Job 1 exactly:
   - from `research/copytrade/`, with no untracked or modified `.py` at its
     top level or in `ct/`: `.venv/bin/python -I r008_fetch.py` (attempt 3),
     and again ≥ 24 h after attempt 3's end commit (attempt 4);
   - when the `holdout_fetched` row exists: the A1 check (`000o`: the
     fetched row's `registration_commit` is
     `963dcc856366a4d05bfc68499d0b255b1ab5a358`, and `2bd855a..963dcc8`
     touches only `r008_fetch.py` and `r008_run.py` among guarded code),
     recorded in a `000o` addendum; the approval line under §12; then
     `r008_run.py --check` and `--run`, once; the result in 008's own terms.
3. **Never run two guarded runners at once** (each commits to git), and
   never commit a REGISTERED simulation in the real repository.

## Owner options that are allocations, not trials

From the round-4 scouts (`research/alpha/scouts/`): HLP itself; an HLP +
Lighter LLP "house sleeve", each capped at about 20–25% of capital
(venue-exploit tail estimated at 5–10%/yr per venue; `a1-house-side.md`);
the BNB reward stack, hedged, about 2.5%/yr at the 2026-H1 pace
(`e1-structural-payers.md`); plain dollar lending at 3–6%/yr with
documented depeg tails. None is an edge; each has a stated tail.

## What would justify reopening the search

Not another public anomaly on free data at retail cost: that space has been
searched four times. Reopen only for a different kind of edge, and scout it
first:
- real market-making infrastructure (the HLP benchmark shows that is where
  the money on Hyperliquid went: the backstop, not the book);
- lawful data that others do not have;
- an execution advantage the owner can actually build.

## Held-back calendars (unread; do not spend casually)

- Binance new-perp confirmation calendar (listed 2025-07-28 → 2026-06-21).
- Europe month-end calendar 2014 → 2026-05 (015's confirmation, not
  triggered).
- Deribit options calendars 2021-03-24 → 2026-06-30 (014 parked).
- HL holdout 2026-09-28 → 2027-06-30; 2026-07-01 → 09-28 belongs to 008.
- Crypto 2026-07-01 → 2028-12-31 is claimed by 016.
- The HMM rebalancing post-publication months (2025-03 →).

## Hard rules (unchanged)

- AGENTS.md rule 5: nothing is published, uploaded or shared.
- The live AI trader is off; do not touch `lib/`, `pages/`, `components/`,
  `vercel.json`, `package.json`. Never pull bulk bar data out of Neon.
- Ledger protocol: hypothesis → DRAFT → behaviour-only feasibility → two
  independent reviews → REGISTERED → a guarded runner that runs once → the
  result in the registration's own terms.
- Owner standing instructions: fix review blockers rather than parking
  around them; if any action is blocked (guard, permission classifier),
  stop and ask the owner, saying why.
