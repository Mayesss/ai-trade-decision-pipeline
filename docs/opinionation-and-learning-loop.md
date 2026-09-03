# Construction site: prompt opinionation + the self-learning loop

Status: survey / scoping. No code changed. 2026-09-02.

Framing question: *does this project earn the title "self-improving AI trading
decision maker"?* Today it is honestly **an AI-executed, human-improved
pipeline with a working lesson memory bolted to one corner of it.** The trade
DECISION is the model's. The trading DOCTRINE is ours, hard-coded, and outside
the loop. Two construction sites follow from that: strip the doctrine the model
does not need, and widen the loop so it can revise what remains.

---

## Part 1 — Where opinion actually lives

Opinion is not only in the prompt prose. It sits in four layers, ordered by how
invisible (and therefore how dangerous) each one is.

### Layer 0 — pre-AI filters: opinion the model never sees

This is the strongest opinionation in the system, because a skipped tick leaves
no trace of a decision the model would have made. The flat-scan stack in
`pages/api/analyze.ts`:

| # | Gate | Where | The embedded opinion |
|---|------|-------|----------------------|
| 1 | actionability | `lib/swing/signals.ts:125` `evaluateActionability` | Only *confirmed primary structure* or a *tight bounce with room* is worth looking at. Everything else is `boxed_or_unconfirmed` → skipped. This hard-codes a breakout/bounce trader. Range fades, compression/coil anticipation, and anything mean-reverting are structurally unreachable — not rejected by the model, never offered to it. |
| 2 | signal_strength LOW | `analyze.ts:2535` + `computeSignalStrength` `signals.ts:228` | A hand-tuned integer score (aligned≥5→+3, regime≥0.5→+1, location≥0.6→+1, ext≥2.5→−1, cutoffs at 2 and 4). Presented as a budget gate; it is a strategy filter with unvalidated weights. |
| 3 | overextension | `analyze.ts:2592-2609` | Beyond `microAvoid`/`primaryAvoid` no fresh entry is even considered. Justified in the comment by "the AI always HOLDs there" — which was measured when a market fill was the only way in. Resting entries changed that premise (the same argument already retired the wall branch and `micro_entry_ok`); this gate has not been re-examined since. |
| 4 | same-setup dedupe | `analyze.ts:2703` | A repeated setup that already got a HOLD is not re-asked. |
| 5 | ~~ai-bouncer~~ | *removed 2026-09-03* | A second, cheaper LLM with *its own prompt and its own opinion*, allowed to cancel the expensive call. Correctly scoped (it could only skip), but a second uninspected worldview in the path — and the concern in item 7 below is why it is gone: it declined 13 of 71 calls while spending a 1.8k-token, 6.4s reasoning call on all 71, with no scorecard on what it killed. The hard gates above are now the whole filter.

Honest counterweight: several of these numbers *were* validated against
recorded history (`ACTIONABILITY_WALL_ATR`, `decisionConfig.ts:220`, re-checked
over 8 weeks / 800 calls). That is the good habit here — the gap is that it is
manual, one-off, and never re-run.

### Layer 1 — post-AI coercion: opinion that overrides the model

`lib/swing/decisionRules.ts` `postprocessDecision`:

- **Trend guard** (`:96`) — no counter-trend entry against an aligned
  primary+micro trend. The escape hatch (`HIGH` strength + confirmed break)
  is disabled entirely under `strict`, **which is the default**
  (`resolveDecisionPolicy`, `decisionConfig.ts:165`). So in production the model
  cannot take a counter-trend entry at all, ever, regardless of what it sees.
- **Anti-flip** (`:133`) — a repeated CLOSE/REVERSE is coerced to HOLD.
- **Re-entry cooldown** (`:120`) — 240 min directional block. Evidence-backed
  (fee bleed on repeat opens), but still a fixed rule, not a learned one.
- **Base-gate coercion** — executability failure forces HOLD/CLOSE. Legitimate.

And a set of clamps that read as venue hygiene but are trading opinions:
`ENTRY_TP_MIN_ATR=2`, `ENTRY_SL_MIN_ATR=1`, `EXCHANGE_SL_MAX_ATR_MULT=3`,
`ENTRY_LIMIT_MAX_ATR=1.5`, `TP_MAX_ATR=10`. Together they forbid every R-shape
outside one band. A trade the model wants at 0.8-ATR risk / 1.5-ATR target is
not expressible.

### Layer 2 — prompt prose

`lib/swing/prompt.ts:1366-1441` (`const sys`). The file already contains
genuinely de-opinionated writing — the `strategy` paragraph (`:1258`, "no play
here is preferred… Nothing in this prompt tells you which play that should be")
and the resting-entry tools paragraph (`:1249`) are the house style and the
template for the rest. What is left, by line:

| Line | Text | Kind |
|---|---|---|
| `:1373` | "Prefer fewer, higher-quality trades; avoid churn." | Bare preference, unmeasured |
| `:1393` | "Structure (BOS/CHoCH/breakout-retest) **outweighs** raw momentum." | Unvalidated prior asserted as fact |
| `:1394` | "prefer entries aligned with macro+context. Counter-regime only at extreme location…" | Strategy doctrine (and it duplicates the Layer-1 trend guard) |
| `:1395` | Level-bounce carve-out: "Do not reject these solely for regime misalignment" | A *counter-patch to the line above it* — the prompt now argues with itself |
| `:1396` | "avoid fresh entries… strongly prefer none"; "RSI extremes are NOT a counter-trend trigger by themselves" | Restates a hard gate (Layer 0 #3) as soft advice, plus a second flat assertion |
| `:1397` | "AVOID fresh longs at channel_pos ≳ 0.75"; "If a good setup sits at a bad wave position, HOLD and wait" | Hard numeric thresholds invented in prose, with no measurement behind them and no code enforcing them |
| `:1265` | "If the expected swing is not clearly larger than cost… **prefer HOLD**" | Preference |
| `:1399` | In-position: "the **DEFAULT action is HOLD**"; a closed list of the only three justified CLOSE reasons; "Proximity … impatience are NOT close reasons"; "Every early full exit forfeits the multi-ATR target" | The single heaviest doctrine block in the prompt — it defines the exit strategy outright |
| `:1403` | TP "at least ~2 primary-ATR away" | Mandated minimum R |
| `:616` | "Prefer 5–6 when volatility is elevated" | Leverage preference |

The pattern worth naming: the prompt has become a **pile of opposing nudges**.
Each carve-out was added because the previous nudge over-fired. Removing the
nudge is cheaper than adding the next patch.

### Layer 3 — the analysts inherit the same worldview

`POSTMORTEM_SYSTEM_PROMPT` / `WIN_EVALUATION_SYSTEM_PROMPT` /
`REFUSAL_INVESTIGATION_SYSTEM_PROMPT` (`lib/swing/postmortem.ts:755-810`) are
written in the trading prompt's vocabulary and are *shown the trading system
prompt as evidence* — but their only output channel is a **lesson about the
trade**. There is no verdict for "the doctrine was wrong". That is the ceiling
of the loop: the system can learn anything except that its own instructions are
the problem.

---

## Part 2 — The self-learning loop: what works, what's missing

### What is genuinely built (and good)

- Post-mortem on every closed position (`SWING_POSTMORTEM_MODE=all`): loss
  analyst, win analyst, plus refusal investigations on missed wake-band entries.
- A lesson library with add / reinforce / **revise** / retire, a **code-owned**
  scope ladder (`promotedScopeOnReinforce`) so a two-loss BTC pattern cannot
  become a universal veto, and hard gates that stop variance becoming doctrine
  (`bad_luck`, `lucky_win`, `right_to_skip` → no new rule).
- Per-scope prompt caps and provenance (`origin_counts` → "learned from 2
  losses, 1 missed entry") so the trader can weigh *how* a lesson was learned.
- A deterministic weekly digest over the durable tables.

This is a real memory. It is more than most projects with this title have.

### The seven gaps

**1. Nothing measures whether a lesson helps.** No record of *which lesson ids
were shown at which decision* (recoverable only by regexing `prompt_json`), so
there is no join from lesson → subsequent decisions → outcomes. `confidence` is
an LLM's self-report, never a posterior. **Fix:** a `swing.lesson_impressions`
(decision_id, lesson_id) row written at prompt assembly, then a lesson
scorecard in the weekly digest.

**2. The library only grows.** No age decay, no "never referenced" retirement,
no size cap. Retirement requires an analyst to be shown a lesson *and* judge it
wrong. Meanwhile the per-scope caps (3/5/10) mean a low-confidence lesson
silently stops being shown — neither tested nor retired, just orphaned.

**3. Nothing learns from the skips — the biggest gap.** The largest population
in the system by far is ticks the AI never saw (Layer 0). Refusal
investigations cover only the AI's *own* HOLDs on wake bands. Nobody ever asks
"would this skipped tick have been a good trade?" — and that is answerable from
data already recorded (`swing.tick_log` carries the stage, reason and gate
measurements; the price path afterwards is fetchable). This is the single
highest-value addition, because it is the only thing that can put **Layer 0**
inside the loop. Today Layer 0 is unfalsifiable by construction.

**4. Nothing learns the thresholds.** Every number in `decisionConfig.ts` is a
human guess with an env override. `ACTIONABILITY_WALL_ATR` proves the analysis
is doable — it was done once, by hand. There is no recurring report of "what
did entries at extension 1.5–2.5 actually do".

**5. The doctrine is explicitly outside the loop.** `weeklyDigest.ts:1-8` says
it in its own header: *"No AI involvement: the judgment pass (prompt changes,
capability add/drop decisions) happens interactively."* So the system's
**strongest** opinions (Layers 0–2) are the only ones it cannot revise, while
the lessons it *can* revise are capped at ~18 short lines in the user turn and
are outranked by the system prompt above them. A perfectly learned lesson still
has to argue with a hard-coded "prefer HOLD".

**6. Strategy attribution is captured and then dropped.** `strategy` exists
verbatim "so that which plays work on which instruments becomes measurable over
time" (`prompt.ts:1258`). Nothing measures it. The digest segments by
side/platform/symbol only, and no strategy performance ever returns to the
prompt.

**7. The bouncer had no scorecard.** It cancelled expensive calls and nothing
checked whether the setups it killed would have paid. Resolved on 2026-09-03 by
removing it rather than by building the scorecard — the measured skip rate (13
of 71) did not justify a second opinion in the path.

### The sample-size constraint (read before building anything)

Closed trades arrive slowly. Per-lesson or per-strategy PnL expectancy will be
noise for months. So the design rule for everything below: **prefer the
high-N populations** — skips, refusals, never-filled resting entries, and
forward price paths — over low-N realized PnL. A counterfactual over thousands
of skipped ticks is statistically real this quarter; "does lesson #14 make
money" is not.

---

## Part 3 — Proposed order of work

Sequenced so each step's evidence justifies the next. Nothing here is started.

**Step 0 — Measure before cutting.** One local report over `swing.tick_log` +
`swing.decisions`: tick volume by skip stage, AI-call share, action mix,
and — per skip stage — the forward price path (e.g. best MFE/MAE within the
next 1/2/6 primary bars). Answers "what is each Layer-0 gate actually costing
us" with a number instead of a belief. Read-only; no schema change.

**Step 1 — Skip counterfactual worker.** Promote Step 0 from a one-off script
to a cron that samples skipped ticks, measures the forward path, and lands a
verdict row. This is the mechanism that makes Layer 0 falsifiable, and it feeds
Steps 2 and 5.

**Step 2 — Prose de-opinionation pass on `prompt.ts`.** Convert the Layer-2
table's rows from *prescription* to *measurement + framing*, in the house style
already set by the `strategy` and resting-entry paragraphs. Delete the
self-contradicting patch pairs (`:1394`/`:1395`) rather than adding a third
patch. Rule of thumb: **if code enforces it, state it once in HARD CONSTRAINTS
and nowhere else; if code does not enforce it, it is either a measurement the
model can read for itself or it should not be in the prompt.** Every edit shows
up as a contract-snapshot diff — that is the safety net (see the `swing-fixtures`
skill; re-baseline only after reading the diff).

**Step 3 — Lesson impressions + scorecard.** `swing.lesson_impressions` written
at assembly; digest gains per-lesson shown/taken/outcome counts; add age decay
and auto-retire for never-reinforced, never-referenced lessons. Closes gaps 1
and 2.

**Step 4 — Strategy scorecard.** Segment the digest by `strategy` and feed the
top-line result back into the prompt *as a measurement block*, not as advice
("on this instrument your breakout-retest entries: 11 taken, expectancy X") —
the model draws its own conclusion. Closes gap 6. Cheap; the field is already
being captured.

**Step 5 — Give the analysts a doctrine channel.** Add a verdict/side-channel
for "the system prompt or a gate caused this" — a `doctrine_candidate` row, not
an auto-applied edit. The weekly judgment pass then reviews *candidates with
evidence* instead of vibes. This is the step that changes the title from
"human-improved" to "self-improving with a human commit gate", and it should be
last, because it is only as good as the evidence Steps 0–4 produce.

**Deliberately not proposed:** auto-editing the system prompt, auto-tuning
thresholds from live PnL, or letting any analyst write directly to
`decisionConfig`. With this trade volume that fits noise, and it touches live
money.

---

## Addendum (2026-09-02): Step 0 is done — see `docs/measured-hold-causes.md`

The measurement re-orders this plan. Corrections to the survey above:

- **The gates I called out are mostly harmless by volume.** `signal_strength`,
  `extension`, `bouncer` and `flat_dedupe` together skipped 128 ticks in 45
  days. Not worth touching; their prompt prose is worth deleting.
- **The real pre-AI filter is one branch**: `boxed_or_unconfirmed` in
  `evaluateActionability` — 4,174 skips, 100% of the live gate. **Opened
  2026-09-03**: both original doors keyed on "a move already happened", so every
  location-first strategy failed them. Two doors added on a different
  measurement — a risk anchor near price (a level within NEAR_ATR, a channel
  edge, a trendline). `boxed_or_unconfirmed` now names only still water. Full
  reasoning in `docs/measured-hold-causes.md` §4.
- **The real binding constraint is arithmetic, not prose**: `ENTRY_TP_MIN_ATR=2`
  plus `ENTRY_SL_MIN_ATR=1` make any trade inside a sub-2-ATR range
  inexpressible. Listed above under "clamps that read as venue hygiene" — it
  should have been the headline.
- **Gap 6 was understated.** The win analyst has produced *zero* lessons ever,
  `what_worked` has no consumer, and `right_to_skip` (113 verdicts, the loop's
  most common outcome) can only strengthen prohibitions. The library is a
  one-way caution ratchet by construction.
- **Relax the sample-size caution**: 196 closed positions and 158 completed
  post-mortems is more than assumed. Coarse segmentation is viable now.
