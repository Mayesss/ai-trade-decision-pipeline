# The flat-tick decision structure

Status: IMPLEMENTED 2026-08-30. Supersedes the wake/entry parts of
[resting-entry-separation.md](./resting-entry-separation.md), which stands for
the resting-order tooling itself.

Decisions taken during implementation:

- **`wakeAutoEntry` deleted on both venues at once**, not staged behind the
  Bitget stop-order validation. That validation has since passed, so the gap it
  opened on crypto is closed — see "Venue status".
- **`confirm_minutes` bounds unchanged** (5–60, ~10 suggested). The model keeps
  a short window for fast tape; 10 stays the prompt's stated figure.
- **DB column keeps its old name.** `swing.ai_cooldowns.wake_sustain_minutes`
  still stores the model-facing `cooldown_wake_confirm_minutes`, to avoid a
  rename migration on a live table. Mapped and commented at the column.

## The principle

Every field answers exactly one question, and no field presumes the answer to
another. Two things kept getting welded together in this codebase:

- a **tool** to a **strategy** (the pullback limit) — fixed 2026-08-30
- a **measurement** to a **policy** (`cooldown_wake_sustain_minutes`) — this doc

Both welds were added for good empirical reasons, and both produced the same
failure: the model could not reach the thing it needed without also accepting
something it did not want.

## The surface

A flat tick asks one question: **am I committing?**

### Yes — `BUY` / `SELL`

Plus one fill mechanism:

| | rests | fills |
| --- | --- | --- |
| `market` | — | now |
| `limit` | against the trade (BUY below, SELL above) | when price comes back |
| `stop` | with the trade (BUY above, SELL below) | when price goes your way |

Plus `strategy` (below) and optionally `entry_trigger_price` (failed-break
watch — bookkeeping, never an order).

### No — `HOLD`

Optionally, a plan:

| field | question it answers |
| --- | --- |
| `cooldown_wake_above` / `_below` | where do I want to be looking? |
| `cooldown_wake_confirm_minutes` | what counts as an event there? |
| `cooldown_wake_note` | why — the plan, handed back at fire time |
| `cooldown_minutes` | how long do I stay quiet otherwise? |

That is the entire flat surface.

## What changes

### `sustain_minutes` → `confirm_minutes`, and it stops implying a trade

The current field does three jobs at once:

1. defines what "broken" means (hold N minutes beyond the level)
2. suppresses the touch-wake, so an early reclaim never reaches the model
3. **pre-authorizes a mechanical entry** on confirmation

Job 1 is knowable when the band is set — ~10 minutes beyond a level is a
property of the market, testable and stable. Job 3 is *not* knowable then: it
asks the model to commit to a trade whose setup does not exist yet.

Welding them forces a bad choice. Take the confirmation and surrender the
decision, or keep the decision and be woken on the first touch — the single most
ambiguous moment on the chart, where a sweep and a real break are identical. The
model picks the second and decides badly. That is a tooling artifact, not a
model failure.

After the split, `confirm_minutes` means one thing: **do not wake me on every
poke; wake me when the level has actually gone.** No trade implied, ever.

### `wakeAutoEntry` is deleted

A stop order *is* "enter when price crosses X." `wakeAutoEntry` is a stop order
implemented in application code because, at the time, the model had no stop-order
primitive. It now does, on both venues (see "Venue status"), so the mechanism is
redundant — and the venue version is better, because it triggers in the venue's
matching engine rather than on our 1-minute watcher.

The `on_confirm: 'wake' | 'enter'` field considered in the previous round is not
built. A wake always wakes. Commitment lives in orders.

### Why this beats mechanical entry at its own job

`wakeAutoEntry` existed to solve a measured problem: the model converted its own
sustained-confirmed breakout plans at **3.3% (4/121)**, re-arming retest bands in
endless HOLD chains (replay, 2026-08-29). Mechanical entry solved it by removing
the model from the path.

But the refusal happened **at wake time** — plan armed, wake fires, model
declines. A stop order removes the *wake* from the path instead: the commitment
is made once, at plan time, while the model is already reasoning about the level.
There is no second decision to refuse.

That is a fix rather than a bypass, and it leaves the model in charge.

## The trade-off the model now faces

Both routes to a break entry stay open, and neither is prescribed:

- **rest a stop beyond the level** — fills on the touch, fast, no confirmation
  filter, and it is exactly what a liquidity sweep hunts
- **wake band with `confirm_minutes`** — filtered by the hold window, but you
  enter a beat later, by decision, with the confirmation in hand

That is a real trade the model can reason about from the structure and liquidity
in front of it. Which is the point.

## Strategy naming

`strategy` stays an **enum** (`SWING_STRATEGIES`), not free text, and not
dropped:

- Free text loses the only thing the label is for. 200 trades producing 200
  distinct strings cannot be grouped, and grouping — win rate by play, per
  instrument — is the entire purpose.
- Nuance is not lost to the enum, because `summary` and `reason` are already free
  text on every decision. **The enum is the index; `reason` is the content.**
- `other` is in the list so the model never has to mislabel.

Self-correcting rather than frozen: **watch the `other` rate.** Above ~10–15%
the taxonomy is wrong for this system, and the revision comes from reading what
the model actually wrote in `reason` on those rows.

Code never branches on `strategy`. It is recorded and handed back through
`recent_actions` so the model can see which of its own plays worked here.

## Venue status

Resting stop orders are live on BOTH venues. Capital via /workingorders
(type LIMIT|STOP); Bitget via place-plan-order (planType normal_plan),
validated end-to-end on demo 2026-08-30 — see
[resting-entry-separation.md](./resting-entry-separation.md).

The window this doc warned about — crypto break entries routed through the
confirmed wake with no stop-order alternative, the path measured at 3.3%
conversion — is closed. Both venues can now commit to a break with an order
instead of waiting for a wake to decide at.

## Resting entries persist (2026-08-30)

A resting entry used to die at every evaluation: the tick cancelled it BEFORE
asking the model, so the model could only ever see the corpse
(`cancelled_pending_entry`) and **silence destroyed the commitment**. That is the
opposite of the in-position bracket, where `null` on a TP/SL leg means "leave the
standing leg alone" — and it forced a re-commit every 4H, which is exactly where
the 3.3% refusal pattern lives.

Now the order is standing state. The tick READS both books, shows the model
`state.position.resting_entry` (kind / side / price / age_min), and cancels only
when the decision says so:

| decision | effect on the standing order |
| --- | --- |
| `HOLD` | left resting — the default |
| `BUY`/`SELL` with a resting price | superseded: cancel, then place |
| `BUY`/`SELL` at market | cancelled, position opened |
| `withdraw_resting_entry: true` on a flat HOLD | cancelled, no position |

`withdraw_resting_entry` is inert on an entry action (which already supersedes)
and in a position (where nothing rests).

**Safety kept intact.** The read fails closed — not knowing what rests is exactly
when placing would stack exposure (the DE40 double fill), so a failed read skips
the tick. The supersede path still cancels BEFORE placing, and a cancel that
races a fill still surfaces the position. What changed is only the default: an
order survives unless something says otherwise.

**Age backstop.** `RESTING_ENTRY_MAX_AGE_MINUTES` (48h) bounds an order's life if
this pipeline stops running — a stop firing two days late on a thesis nobody
re-checked is the failure it prevents. It is a net under our own outages, not a
view on how long an idea stays good. Capital's venue-side `goodTillDate` now
matches it (it was 70 minutes, which would have killed orders before the model
ever revisited them).

## Open items

1. ~~Resting-order TTL is one tick (~4H).~~ **DONE** — see "Resting entries
   persist" above. Orders now survive evaluations; the model owns their fate.
2. **Force confirmation (≥0.5 ATR)** currently confirms instantly and triggers a
   trade. With no mechanical entry it becomes another reason a wake fires early
   — a label on the wake, not an action.
3. ~~`confirm_minutes` bounds.~~ SETTLED: kept at 5–60 with ~10 named in the
   prompt as the tested figure. A short window stays available for fast tape.
