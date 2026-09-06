# Win evaluations (spec)

## Why

The lesson library learns from losses (restrictive pressure) and refused wakes
(corrective pressure). Wins teach two things nobody extracts today: **exit
quality** (a +0.6R close that ran another 2R is a repeatable leak — the
post-exit market evidence already exists but only losers get analyzed) and
**pattern confirmation** (a win earned by following the process is positive
reinforcement for the lesson/setup that shaped it — the same `reinforce`
action, fed from the winning side, which also drives the scope ladder's
promotions with positive evidence instead of only pain).

Named **win evaluation**, not post-mortem — nothing died.

## Shape: the existing pipeline, one branch deeper

No new tables, crons, endpoints, or triggers. A winning close enqueues into
`swing.postmortems` exactly like a losing one (`trigger_source='close'`); the
runner picks the analyst by PnL sign:

- `pnl > 0` → win-evaluation analyst (`WIN_EVALUATION_SYSTEM_PROMPT/SCHEMA`)
- else → the existing loss analyst

The only enqueue-side change: `SWING_POSTMORTEM_MODE` default flips
`'loss'` → `'all'` (the mode already exists; `'loss'` remains available to
turn win evaluations off, `'off'` kills both). Same 12h delay, same drain,
same idempotency, same dossier builder — the post-exit market section is the
star witness for exit quality.

The analyst selection stays PnL-based, but the dossier no longer lets the sign
speak for the exit: `position.closed_by` names what actually ended the trade
(`take_profit` / `stop_loss` / `ai_close` / `unknown`), with
`take_profit_at_exit` / `stop_loss_at_exit` — the levels that were resting at
the close, replayed from the tick trail
(`positionDecisionMatch.resolveBracketAtExit`). This matters most exactly here:
most winners end on a stop TRAILED into profit, and `closed_by: 'stop_loss'`
tells the analyst to judge the trail, not a target it never reached.
`closed_by_basis: 'pnl_sign'` marks the fallback where no levels could be
recovered.

## Verdicts

| verdict | meaning |
|---|---|
| `earned_win` | process-sound: entry, management and exit all defensible on the information at each timestamp |
| `lucky_win` | got paid DESPITE a process flaw (violated a lesson, chased, mis-sized) — the dangerous one |
| `exit_flaw` | the win was real but the exit demonstrably leaked: closed well before the thesis target with continuation after (premature), or gave back a large recorded MFE (late) |

Report fields: `verdict`, `confidence`, `timeline_analysis`, `what_worked`
(array — the measurable conditions that made it work), `exit_quality`
(judged against the post-exit path), `lesson_adherence`, and the standard
lesson block (`lesson_action` with the full new/reinforce/revise/retire/none
set, `reinforce_lesson_id`, `lesson`, `lesson_scope`).

## Lesson gates (code-enforced, mirror of `bad_luck`)

`resolveLessonDecision` gains `kind: 'win'`:

- **`lucky_win` teaches nothing** — `lesson_action` forced to `none`. A win
  that violated a lesson does not weaken that lesson (one lucky outcome is
  variance), and it must never mint "the violation worked" doctrine. The
  violation itself is recorded in `lesson_adherence` text only.
- **`earned_win`** may `reinforce` (positive rent for the lesson that shaped
  the trade — cross-symbol reinforcement promotes on the ladder as usual) or
  `revise` (a shown lesson that ALMOST blocked this good trade has a bound set
  slightly too wide — the win is evidence to loosen it). `new` is allowed but
  prompt-guarded: only when the win hinged on a repeatable, measurable
  condition the library doesn't cover, written in the same imperative,
  numerically-bounded format ("Prefer X when Y is within Z primary-ATR"), never
  a platitude.
- **`exit_flaw`** may `new`/`reinforce`/`revise` — exit-mechanics lessons,
  the same family as the existing management lessons (#6, #10).

Scope ladder applies unchanged: new win-born lessons enter at symbol scope.

## Analyst prompt (essence)

Same forensic discipline as the loss analyst, with the polarity flipped and
two extra rules:

- **No survivor bias**: only credit a decision if the information AT ITS
  TIMESTAMP supported it — a good outcome does not retroactively validate a
  bad process (that verdict is `lucky_win`, the mirror of `bad_luck`).
- **Exit quality is judged against the post-exit section**: continuation well
  past the exit → premature; reversal shortly after → well-timed; a giveback
  of most of the recorded MFE before the close → late. Numbers, not vibes.

## UI

Everything rides the existing surfaces automatically (timeline dot at exit,
click-through modal, digest verdict counts, and — because wins keep
`trigger_source='close'` with a real position key — the chart position-overlay
chips now show verdicts on winning trades too). Two touches:

- Modal: render `what_worked` and `exit_quality` blocks (conditional, like the
  refusal fields).
- Copy: where the tooltip/modal says "Post-mortem", say "Win evaluation" when
  the verdict is one of the win set (pure display logic, no data change).

## Cost & volume

At the historical rate (~2–3 trades/day, roughly half winners) this adds ~1–2
analyst calls per day, 12h after each winning close. No backfill by default —
old wins predate the current doctrine and would grade a dead regime (a manual
`trigger='backfill'` enqueue still works per position if ever wanted).

## Out of scope

- Renaming the `swing.postmortems` table or the drain (plumbing keeps its
  name; "win evaluation" is the analyst/report identity).
- Missed-move detector (separate spec if wanted).
- Any change to loss post-mortem behavior beyond sharing the enqueue path.
