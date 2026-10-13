# 000e — 007 drafting and review: event counts inside hold windows

Ledger entry, not a hypothesis. Written 2026-10-11 by the orchestrator.

## What happened

While drafting and reviewing registration 007 (vault deposits), three reads
touched event *counts and times* inside the discovery hold windows. None
read an amount.
1. The drafter counted `vaultDistribution` events inside the four holds of
   the eligible sets: 12 / 8 / 4 / 2.
2. The reviewer read ledger event types and times, grid timestamps and the
   list's `leader` field. Over the 196 pinned ledgers it found 61 vaults
   with a distribution, 15 with two or more, and 16 with deposits more than
   a day after their first distribution. It also found that one
   vault-window distributes 2 days after S. The orchestrator re-counted the
   61 / 15 / 16 from types and times only.
3. The reviewer found non-leader withdrawals in the lookback for
   68 / 58 / 48 / 54 eligible vaults per window (presence of `user`, not
   amounts).

## What was seen

Counts and timestamps of ledger events. No deposit, withdrawal,
distribution, commission or NAV amount was read. No return was computed.

## What must not follow from it

- Distribution timing may shape the *definition* of closure, because it
  revealed that the draft's definition was wrong. But no threshold may be
  tuned to these counts.
- The in-hold distribution counts carry weak information on vault outcomes
  (leaders distribute after gains or at wind-down). 007 discloses them in
  §7, and the holdout, whose events are unseen, decides.

## Addendum (2026-10-11): reads during 007 Revisions 1 and 2

Behaviour and structure only; no ledger amount, NAV, pnl or return of any
vault was read.
- Revision 1:
  - per-vault-day fill counts and first and last fill times for the 196
    candidates, up to 2026-06-07 (stored; only "22,606 rows, 196 vaults"
    printed);
  - the median lookback net leverage from snapshots at 7 dates (quantiles
    printed);
  - non-leader `vaultWithdraw` presence in the lookback, by `user` against
    the leader;
  - the 7-day activity rule (inactive counts 2/1/2/2);
  - whether a grid timestamp or snapshot row exists in [S, S+10 d];
  - the minimum in-hold grid timestamp count (4);
  - leader-sharing counts;
  - the SHA-256 of 734 files;
  - BTC hold returns (public market data).
- Revision 2: no new vault data. BTC's 56-day return SD and hold returns
  were recomputed from closes before 2026-06-08.

None of these may set a 007 threshold beyond what the registration states;
the holdout decides.
