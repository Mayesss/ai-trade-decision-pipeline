# Measured: what actually causes the HOLDs, and why the lessons are all "don't"

Read-only measurement over prod Neon, 2026-09-02.
Window: 2026-07-18 → 2026-09-01 (45 days), 26 symbols, 90,976 ticks,
18,426 decision rows, 196 closed positions.
Companion to `docs/opinionation-and-learning-loop.md` (the survey this tests).

---

## 1. The funnel

| Stage | n | share |
|---|---|---|
| ticks logged | 90,976 | 100% |
| — `primary_close_gate` (cadence, not opinion) | 73,813 | 81.1% |
| — `capital_market_closed` (venue) | 4,649 | 5.1% |
| — **`actionability_gate`** | 5,309 | 5.8% |
| — `insufficient_margin` (operational) | 3,077 | 3.4% |
| — `quiet_position` / `flat_cooldown` | 2,170 | 2.4% |
| — `signal_strength_gate` | **54** | 0.06% |
| — `extension_gate` | **62** | 0.07% |
| — `ai_bouncer` / `flat_dedupe` | **12** | 0.01% |

**Decisions: 18,426 rows, 97.95% HOLD. 15,991 of them (88.6% of all HOLDs)
never reached the model** — they are `pre_ai_skip` rows.

**Of the 2,435 calls the model actually saw:**

| action | n | share |
|---|---|---|
| HOLD | 2,058 | **84.5%** |
| BUY | 148 | 6.1% |
| SELL | 133 | 5.5% |
| CLOSE | 96 | 3.9% |
| **REVERSE** | **0** | **0%** |

REVERSE has never been emitted once in 2,435 calls. It has a schema field, a
prompt paragraph, an anti-flip guard and execution support in `lib/trading.ts`.
It is dead surface.

### Which gates are worth arguing about

The three Layer-0 gates I flagged in the survey as strategy filters —
`signal_strength`, `extension`, `bouncer` — together skipped **128 ticks in 45
days (0.14%)**. They are rounding error. The prose describing them costs more
than they save.

`actionability_gate` is the only pre-AI opinion with real volume (5,309), and
of those, **1,133 are legacy rows** from branches since retired
(`into_context_wall` 805, `micro_entry_ok_false` 328). The live branch is
`boxed_or_unconfirmed`: **4,174 skips** — 100% of the currently-firing gate.
That single branch is the entire pre-AI strategy filter.

`insufficient_margin` (3,077, and 2,050 of them in the last 30d) is bigger than
every opinion gate and is an operational problem, not a doctrine one — the
account cannot cover min size on a chunk of the 26 symbols, so those symbols
are silently dead.

---

## 2. Why the model itself HOLDs (n = 2,058, its own `reason` text)

Keyword classification, multi-count:

| theme | n | % |
|---|---|---|
| awaiting acceptance / confirmation / "no BOS/CHoCH" | 1,188 | **57.7%** |
| "wait" / "monitor" / "reassess" / "stand aside" is the verdict | 1,022 | 49.7% |
| cites an ATR distance to a level ("only 0.35 ATR away") | 852 | 41.4% |
| names room / reward / target as the blocker | 490 | 23.8% |
| cites a lesson | 460 | 22.4% |
| channel crest / wave position | 360 | 17.5% |
| chop / tightly-packed levels | 357 | 17.3% |
| "would pay into" / "chase into" a level | 275 | 13.4% |
| "blocked" / "not permitted" / "guard" | 170 | 8.3% |
| extension | 68 | 3.3% |

Hand-reading the 22 most recent HOLD reasons gives the shape the keywords only
approximate. **14 of 22 are the same sentence:** *the direction is right, but
the next opposing level is too close, so no trade.* Verbatim samples:

> "…4H structure is range-bound and price is only 0.43 ATR above strong 97.342
> support at the channel floor."

> "Primary resistance is only 1.38 ATR away, **short of clean bounce room**."

> "Primary resistance is only 1.315 ATR away and context resistance just 0.253
> ATR away, **limiting a compliant swing target**."

> "A short near 1.3513 has only about 1 primary ATR to 1.3492 support; a long
> conflicts with bearish micro structure. **Wait for acceptance outside the
> range.**"

That last one is the whole problem in one line: presented with a range, the
model's only conceived move is to wait for the breakout out of it.

### The arithmetic behind it

`ENTRY_TP_MIN_ATR = 2` (`decisionConfig.ts:437`), surfaced in the prompt as
*"ALWAYS set take_profit_price… at least ~2 primary-ATR away"*. Combined with
`ENTRY_SL_MIN_ATR = 1`, this means **no trade is expressible unless there is
≥2 ATR of clear room to the next opposing level.** Most ranges are narrower
than 2 ATR. So the constraint does not express a risk preference — it
mathematically forbids trading inside a range, which is the same breakout bias
as the actionability gate, restated as arithmetic instead of prose. The model
is not being timid; it is correctly reporting that no compliant trade exists.
"limiting a **compliant** swing target" is the model saying so out loud.

Stack that with the wave-position doctrine (17.5% of HOLDs cite channel
position — a rule that exists *only* in prose, enforced by nothing, measured by
nothing) and the confirmation demand (57.7%), and the picture is a model
holding because the rules leave it nothing else to do.

### Corroborating: what it does when it *can* act

- Entry tool: **resting limit 200, market 79, resting stop 2.** It reaches for
  the resting limit readily — the de-opinionated paragraph works. The stop is
  near-unused, and that paragraph is the one that warns it "is exactly what a
  liquidity sweep is hunting".
- CLOSE: **53 trims vs 43 full closes.** In-position judgment is healthy.
- Flat HOLDs: only 676 of 2,058 set a wake band; **1,381 set neither band nor
  cooldown** — two thirds of refusals leave no plan behind at all.
- `strategy` (added 2026-08-30): **3 of 3 post-feature entries named one**
  (2 `breakout_retest`, 1 `breakout`). The field works; there has just been
  almost nothing to label.

### Flag, not a finding: the entry rate collapsed

All-time entry rate 281/2,435 = **11.5%**. Since commit `219d82e`
("fondamental changes", 2026-08-30) it is **3/125 = 2.4%**, a 96% HOLD rate.
Only ~2 days and 125 calls, so this is not conclusive — but it points the wrong
way, and it is the window in which the current prompt shipped. Worth re-reading
after another week of data before drawing any conclusion.

---

## 3. The lesson library is a prohibition ratchet

**22 rows, 18 active. Origin counts across the entire library: `loss` and
`refusal` only. Zero win-origin lessons — the win analyst has never produced
one.**

Content of the 18 active lessons:

- **10** are entry prohibitions ("do not enter / do not market-buy / wait for…").
- **4** are exit or stop mechanics.
- **4** are permissive ("act, do not re-demand a retest") — **and all 4 came
  from refusal investigations**, the only path that produces a "do".
- **18 of 18 are about breakouts, breakdowns, retests, wakes or reclaims.**
  Not one concerns range trading, mean reversion, or pullback continuation.
  The library has no vocabulary for a strategy the actionability gate never
  let it try. The gate was opened 2026-09-03 (§4), so the *input* side of this
  is fixed; whether the vocabulary actually broadens now depends entirely on the
  loop growing a positive term.

### Why this is structural, not a tuning accident

Four separate mechanisms, three of them **code-enforced**:

1. **Loss analyst framing** (`postmortem.ts:773`). Fields are
   `what_went_wrong` / `suggestions`. The only lesson shape the prompt
   illustrates is a prohibition — its worked example of a bound is literally
   *"an unbounded 'do not X near Y'"*. Losses are the highest-volume trigger.
2. **Win analyst is throttled three ways** (`:755`). It *does* permit
   "Positive-playbook lessons"— then: `lucky_win` → forced `'none'`
   (code-enforced, `lessons.ts` `resolveLessonDecision`); `'new'` allowed
   *"ONLY when the win hinged on a repeatable measurable condition the library
   does not cover"*; and *"most earned wins are the process working and teach
   nothing new ('none')"*. On `earned_win` the **first-listed** action is
   `reinforce` an existing lesson — and existing lessons are don'ts, so **a win
   strengthens a prohibition.** Measured outcome: zero win-origin lessons.
3. **The refusal path is a one-way caution ratchet.** 113 `right_to_skip` vs 11
   `wrong_to_skip`. `right_to_skip` is code-gated to `'reinforce'` or `'none'`
   only. So the single highest-volume verdict in the whole loop — 113, more
   than every close post-mortem combined (34) — says *"you were right not to
   trade"*, and its only permitted library effect is to make a prohibition
   stronger. Meanwhile 101 of 124 refusal post-mortems wrote no lesson at all.
4. **`what_worked` and `exit_quality` are written and then thrown away.** Every
   win evaluation fills them into `report_json`; `grep` finds **no consumer
   anywhere in `lib/`, `pages/` or `components/`.** The system already records
   what worked and discards it at injection time.

Plus the framing at the point of injection: the block is titled *"LESSONS (from
forensic evaluations of your past trading)"* and every entry is tagged with
provenance that can only be negative — *"learned from 2 losses, 1 missed
entry"*. (The code comment above it at `prompt.ts:692` still says "distilled
from post-mortems of past LOSING trades" and "max 5"; the real cap is 18.)

### The compounding loop

    actionability gate admits only breakout/bounce ticks
      → only breakout trades get taken
        → only breakout post-mortems get written
          → only breakout lessons enter the library
            → those lessons make breakout entries stricter
              → fewer entries  → 84.5% HOLD

Nothing in this system can produce a lesson about a strategy the gate never let
it try, and every lesson it *can* produce makes the one permitted strategy
harder to execute. That is the mechanism behind "the AI is caged": it is not a
tone problem in the prompt, it is a closed loop with no positive term in it.

**Status after 2026-09-03:** both halves are now addressed in code — the
coercions and floors are gone, and the anchor/geometry doors widen the first
arrow so range fades, reversals and deep pullbacks can enter the sample (§4).
Neither touches the second half. The loop still has **no positive term**: a new
door only produces new prohibitions until the win analyst and `right_to_skip`
can say "right to trade". Until then the library can re-impose in `LESSONS` text
exactly what was just deleted from code, and close the new doors from the
inside. Fixing the one-way ratchet is now the load-bearing item.

---

## 4. What this changes about the plan

The survey's Step 0 is now done, and it re-orders the rest.

**Demoted — not worth the effort:** `signal_strength_gate`, `extension_gate`,
the bouncer, `flat_dedupe`. 128 skips in 45 days. Delete the prompt prose that
describes them (it is pure token cost and it teaches caution the gates don't
actually apply); leave the code alone.

**Promoted to first — the binding constraint:**

1. **`ENTRY_TP_MIN_ATR = 2` and the "room" doctrine.** This is the binding
   constraint on 24-64% of refusals depending on how you count. Making the TP
   floor a function of the available range (or dropping the floor and letting
   the model justify its own R) is the single highest-leverage change in the
   repo. It is also the change that makes range trading *expressible* — which
   no amount of prompt rewording can do while the arithmetic forbids it.
   **Done 2026-09-02:** floor dropped outright, along with `ENTRY_SL_MIN_ATR`,
   `TP_MAX_ATR` and `EXCHANGE_SL_MAX_ATR_MULT`.

**`boxed_or_unconfirmed` — kept, then opened (2026-09-02 → 2026-09-03):**

First decision: keep it. It is a budget filter, not a veto — it declines to pay
for a call on a tick the model has historically HOLDed anyway, so it forfeits no
order directly. That reasoning still holds for what it *now* rejects.

What it did NOT hold for was the gate's shape. Both original doors key on the
same thing — **a move that has already happened**:

| door | test | admits |
|---|---|---|
| (a) `confirmed` | breakout/breakdown/BOS/retest, `breakState ≠ inside` | breakout family |
| (b) `bounce` | at a level (≤0.6 ATR) **+ ≥1.5 ATR room** + micro turning | asymmetric bounce |

Every strategy that starts from **location** rather than momentum fails both, so
relaxing their thresholds only ever admits weaker breakouts and weaker bounces.
The schema offers the model **nine** strategies (`decisionSchema.ts`) — including
`range_fade`, `pullback`, `reversal`. Nine strategies out, two doors in.

Two concrete blind spots this produced:

1. **The symmetric range never arrived.** (b) demands room on the far side, so a
   tight box was `boxed_or_unconfirmed` — the exact trade dropping
   `ENTRY_TP_MIN_ATR` was meant to make expressible. The two changes cancelled.
2. **The gate rewarded chasing.** `breakState` is "last close beyond the last
   swing extreme", so it stays true while price holds out there. A SHALLOW
   pullback passes (a). A DEEP one — back at the channel floor, the better entry
   — flips to `inside` and then needs a swing level within 0.6 ATR to survive.
   Price that has LEFT a level was admitted; price that came BACK to one was
   dropped.

**Added 2026-09-03 — two doors keyed on a different measurement:** not "has a
move been confirmed" but **"is there a defined risk anchor near price"**, which
is what a range fade, a reversal at a level and a pullback all actually need,
and which is strategy-neutral by construction.

- **(c) anchor** — nearest primary level ≤ `ACTIONABILITY_NEAR_ATR`, either side,
  no room and no micro requirement. Reasons: `at_primary_support`,
  `at_primary_resistance`, `at_primary_level_boxed`.
- **(d) geometry** — `channel_pos` within `ACTIONABILITY_CHANNEL_EDGE` (0.15) of
  either channel end, or a trendline within `NEAR_ATR`. Reasons: `channel_low`,
  `channel_high`, `at_support_trendline`, `at_resistance_trendline`. This is the
  door for setups with no swing S/R nearby at all.

`boxed_or_unconfirmed` survives as the name of the **only** remaining rejection:
no break, no level in reach either side, not at a channel edge, no trendline —
nothing to anchor invalidation to, so there is no trade to price. Still water,
and the only thing code decides alone. Everything admitted still faces the
ai-bouncer, which prices the call and may decline it — a cost filter may skip
work, never override a gate.

**Cost:** the gate's 4,174 skips are ~1.7× the 2,435 calls actually made, so an
unbounded opening could have neared 3× the AI spend. The doors are deliberately
narrow (a level within 0.6 ATR, the outer 15% of the channel) and the bouncer is
the backstop. **Measure the admission mix before tuning anything** — the reason
string is on every decision row, so per-door volume and per-door outcome are one
query.

**What this does NOT fix:** the compounding loop in §3 has two halves. This
opens the sample; nothing here gives the lesson library a positive term. A new
door plus a one-way ratchet means the first losses on a range fade mint
prohibitions and the door closes itself in prose. That work is still ahead.

**Then the prose pass** (survey Step 2), now targeted by measurement rather
than taste: the confirmation demand (57.7%), the wave-position rule (17.5%,
enforced by nothing, measured by nothing), and the in-position "DEFAULT is
HOLD" block.

**Then the lesson loop's missing positive term** (new, ahead of the old Step 3):
un-throttle the win analyst, give `what_worked` an injection path as a
*playbook* block alongside `LESSONS`, and stop letting `right_to_skip`'s 113
verdicts be a one-way ratchet. This is cheap — `what_worked` is already being
written to the database.

Attribution plumbing (`lesson_impressions`) and the skip counterfactual keep
their places after those, and the sample-size caution in the survey should be
relaxed: **196 closed trades, 158 completed post-mortems** is enough for coarse
segmentation, more than I assumed.
