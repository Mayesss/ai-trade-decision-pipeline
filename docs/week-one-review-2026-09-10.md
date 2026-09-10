# Week one after the 2026-09-02/03 overhaul: what is actually going wrong

Read-only measurement over prod Neon, 2026-09-10 morning. Companion to
`docs/measured-hold-causes.md` (the measurement that motivated the overhaul).
Windows: "before" = up to 2026-09-02 12:00Z; "after" = since then; "this week"
= closes since Monday 2026-09-07.

## 1. Headline

| | before (Aug) | Sep 2–6 | this week (Sep 7–10) |
|---|---|---|---|
| closed positions | ~2 / week | 17 | 102 (81 Capital, 21 Bitget) |
| win rate | 0/9 | 7/17 = 41% | 19/102 = **19%** |
| net PnL | −12.15 (4 wks) | +0.36 | **−14.37** |
| median hold | 3.5 h | 4.6 h | **3.8 h** |

The hypothesis under test was "the model holds too long and sets stops/targets
too far because the prompt says *swing*". The data says the opposite: the
model trades ~40 times a day, most trades die inside their first 4H bar, and
the brackets are the tightest they have ever been.

## 2. The mechanism: the stop floor was deleted and the model went to 0.6 ATR

Bracket geometry on entry orders, parsed from the stored prompt + decision
(ATR = primary/4H ATR at decision time):

| | n | median SL | median TP | SL < 0.5 ATR | SL < 1 ATR |
|---|---|---|---|---|---|
| before 09-02 (floors in code) | 17 | **1.50 ATR** | 2.59 ATR | 0% | 6% |
| after 09-02 (no floors) | 133 | **0.62 ATR** | 1.39 ATR | 29% | **84%** |
| after — Bitget | 45 | 0.61 | 1.34 | 31% | 87% |
| after — Capital | 88 | 0.63 | 1.40 | 28% | 83% |

`ENTRY_SL_MIN_ATR=1` / `ENTRY_TP_MIN_ATR=2` were dropped 2026-09-02 (see
`measured-hold-causes.md` §4). The prompt still says *"every bracket you leave
behind must stand on its own for at least one full 4H bar"*; the model reads
that and places a 0.6-ATR stop anyway. The prose did not fail — the arithmetic
backstop behind it was removed and nothing replaced it.

What the stops do (closes since 09-07 with a recovered close cause):

| closed_by | n | wins | net |
|---|---|---|---|
| stop_loss | 33 | 2 | **−14.10** |
| ai_close | 21 | 1 | −2.97 |
| take_profit | 8 | 8 | +5.10 |
| unknown (no post-mortem yet) | 21 | 6 | −0.88 |

Stop-outs ARE the week's loss. Targets, when reached, pay. With SL 0.62 / TP
1.39 the break-even win rate is ~31%; the realized rate is 19%.

Post-exit paths (12 h after exit, from the post-mortem dossiers, small n):

- stop-outs (n=15): price came back to the entry price in **15/15**, reached
  the original TP in 3/15, median adverse continuation after the stop only
  −0.85%. The stops are sitting inside intrabar noise.
- ai_close (n=14): 6/14 later reached the TP the model abandoned.

## 3. The loop diagnoses this correctly — and cannot act on it

The lesson library went from 8 active rows on 09-02 to **51 active** on 09-10
(12–16 new per day). Roughly 20 of the 51 say the same thing in different
words: *stop ≥ 0.5–1 primary-ATR, beyond the swept extreme, must survive one
4H bar*. Another ~10 say *do not market-enter a reclaim on the reclaim tick*.

Why the same lesson is minted 20 times instead of once:

1. **Every one of the 51 is `symbol` scope.** The ladder promotes a lesson to
   `asset_class` only when a post-mortem on a *different* symbol chooses
   `reinforce` on it (`promotedScopeOnReinforce`, `lessons.ts`).
2. The analyst is shown `loadActiveSwingLessons({symbol, assetClass})` — the
   *current* symbol's lessons plus class/global rows. A GOLD post-mortem never
   sees SILVER's identical lesson, so it can never reinforce it, so it can
   never promote it, so it mints a new GOLD copy.
3. The trading prompt caps `symbol` lessons at **3** (`PROMPT_LESSON_SCOPE_CAPS`).
   XRPUSDT has 5, four symbols have 4 — the newest are already invisible.
4. 15 of 26 symbols have 0–2 lessons: the stop rule reaches them only after
   they have lost on it themselves.

Net: the self-improvement loop is a correct diagnostic machine with no
generalization step. More days do not converge; they add more symbol-local
duplicates and the losses that pay for them.

## 4. Other contributors, in order of size

- **Re-entry churn.** 19 re-entries within 2 h of a losing close on the same
  symbol (4 wins, −1.52). Two causes: `REENTRY_COOLDOWN_MIN` was set 240→0 on
  09-02, and wake-watch fired a fresh *flat* AI call on every TP/SL fill
  (fixed 2026-09-08 23:23, `f26ff1a`). Sep 9 volume already dropped.
- **Capital breadth.** Capital went from 0 entries/day (Sep 3–4, 36 calls, all
  HOLD) to 24–37 entries/day from Monday. 92 of 123 entry reasons this week
  cite a sweep/reclaim; the Capital lessons are dominated by "reclaim on the
  reclaim tick" and "stop inside the sweep zone". Up to **9 positions open at
  once**.
- **Model change, confounded.** `DEFAULT_AI_MODEL` moved `openai/gpt-5.6-sol`
  → `zai/glm-5.3` in the same commit as the overhaul (`ddbcc1b`, 09-03). Output
  tokens per decision call went 0.5–0.7k → 2.4–3.6k. Whether the tight stops
  are the prompt's doing or the model's cannot be separated from this data.
- **Fees (Bitget).** gross −1.79 vs net −4.25 this week: fees are 58% of the
  Bitget net loss at 3.8 h median hold.
- **Spend.** Sep 7: 174 decision calls (1.78M in / 0.56M out tokens) + 50
  post-mortems (2.26M in / 0.34M out). Post-mortem tokens now exceed decision
  tokens.

## 5. Operational flags found on the way (revised after checking)

- **Bitget margin skips are legitimate.** `swing.positions` only stores CLOSED
  positions; open Bitget positions live in `swing.ai_threads` (4 `in_position`
  threads: BNB, LINK, SOL, BTC on 09-10) and on the venue. Four positions on a
  ~$55 account consume all margin, hence `insufficient_margin ... have≈0`.
- **Capital two-row shape is by design, the post-mortem enqueue is not.**
  `capitalWindows.ts` documents it: an AI-initiated Capital close writes a
  `capital:` snapshot row (pnl_pct, no cash) AND a `capital-tx:` row from the
  venue's transaction history (cash pnl_net, no price). Every read path merges
  the pair (`mergeCapitalPositionWindows`), so dashboard PnL is not double
  counted and the −10.12 above is correct. But `maybeEnqueueSwingPostmortem`
  dedupes on `position_key` alone, and the twins have different keys, so both
  rows enqueue an analysis. Since 09-01: 21 twin pairs, 10 analysed twice
  (16 analyst calls), **6 lessons minted or reinforced from their own twin**
  (e.g. EURUSD #45 "loss: 2" = post-mortems 934+1094, GOLD #55, SILVER #65,
  UK100 #41 support 7). Still happening: UK100 and NATURALGAS closes on 09-10
  each queued twice. The 09-08 `fixes` commit did not touch the enqueue.
  This matters for §7 item 2: `support_count`/`origin_counts` are exactly what
  the scope ladder promotes on, so fixing the ladder before this would promote
  lessons on fabricated cross-evidence.
- **12 post-mortems stuck in `running`** since 09-08/09-09 (attempts=1, no
  error): the drain has no stale-lock reclaim, so a worker that died mid-call
  leaves the row dead forever. Those trades never get analysed. Reset is an
  UPDATE on prod → Neon console, per the house rule.
- 21 Capital closes this week have no post-mortem on either twin (mostly
  COPPER/GOLD/OIL trims or pyramided legs → `disposition != flat`, expected).
- One decision row had `state.volatility.atr_pct.primary = 0` (DOGEUSDT).

## 6. Answers to the questions asked

- *Remove the word "swing"?* No. Holds are one bar long and stops are 0.6 ATR;
  the model is already scalping on a 4H cadence. Removing "swing" pushes the
  wrong way.
- *Better prompt?* The sentence that would fix this is already in the prompt
  and is ignored. The thing that worked before was a code floor. The model's
  own lessons ask for that floor back, 20 times.
- *Be patient?* Not with the loop in its current shape — §3 shows it cannot
  generalize, so waiting buys duplicate lessons, not convergence.

## 7. What would change the outcome (not done — decisions for the owner)

1. Put a stop floor back as a hard constraint that sizing refuses below —
   the model's own median recommendation is 0.5–1 primary-ATR, or "≥ the
   recent 4H bar range". State it once in HARD CONSTRAINTS.
2. Give the loop its generalization step: show the analyst same-class lessons
   from *other* symbols so `reinforce` can promote; or dedupe new lessons
   against the whole library before insert; or start reclaim/stop lessons at
   `asset_class` scope. Retire the ~20 duplicates once one canonical row exists.
3. Restore a post-stop cooldown (`SWING_REENTRY_COOLDOWN_MIN=240` restores the
   old behaviour with one env var).
4. De-confound the model: one week on `gpt-5.6-sol` with everything else
   unchanged, or the reverse. Until then no prompt conclusion is clean.
5. Narrow while learning: fewer Capital symbols or a concurrent-position cap.
6. Fix the §5 enqueue twin bug and reset the 12 stuck rows — the twin
   double-counts are the evidence the loop learns from. Small and independent;
   do it before item 2.

## 8. Applied 2026-09-10 (same day, after review)

Code (uncommitted at the time of writing; takes effect on deploy):

1. **Entry stop floor** — `ENTRY_SL_MIN_ATR` (default 1 primary-ATR,
   `SWING_ENTRY_SL_MIN_ATR` overrides, 0 disables) in `decisionConfig.ts`.
   `sanitizeExchangeTpSl` flags a sub-floor entry stop (`sl_below_entry_floor`,
   `entryStopBelowFloor`); `analyze.ts` refuses the entry
   (`entry_dropped='entry_stop_below_floor'`, HOLD, bracket torn down) rather
   than widening it. Applies to BUY/SELL and the new side of a REVERSE; never
   to amends. Stated once in the prompt's HARD CONSTRAINTS; the bracket
   paragraph no longer says "no minimum on either".
2. **Post-mortem twin guard** — `maybeEnqueueSwingPostmortem` asks
   `findSwingPostmortemTwin` (same platform/symbol, compatible side, exit within
   10 min) before inserting; the transaction-history twin of an AI close no
   longer gets its own evaluation.
3. **Re-entry cooldown** default back to 240 min (`REENTRY_COOLDOWN_MIN`);
   pinned to 0 in the test harness so scenarios stay stable.
4. **Analyst sees siblings** — `loadActiveSwingLessons({includeSiblingSymbols})`
   returns symbol-scoped rows from other instruments to the post-mortem analyst
   only; each `lessons_shown` entry carries `shown_to_trader` so adherence is
   graded only on rows the trader saw, and the analyst is told to `reinforce`
   a sibling (which promotes it) instead of minting a copy.
5. **Stale-running reclaim** — the drain re-claims `running` rows older than
   45 min with fewer than 3 attempts, so a killed worker's row is retried
   instead of dying silently.

Data (`docs/lessons-repair-2026-09-10.sql`, applied by the owner in the Neon
console the same day; UNDO inside): twin double-counts removed from lessons
#41/#44/#45/#55/#63 (support and origin counts; the source-id removal needed
the FOLLOW-UP block in the same file — jsonb `- text` does not remove numeric
elements); #41 promoted to global; 14 lessons retired (11 restating the stop
floor now in code, 3 copies of #41); the 12 stuck post-mortems and the 2
queued transaction-row twins marked failed. Library after: 37 active, 36
retired.

Not done: deploy, the model de-confound (§7 item 4), narrowing Capital
breadth (§7 item 5).

## 9. Applied 2026-09-10, afternoon (after the first deploy)

Incident first: between ~11:45 and ~13:00 UTC the 15-min cadence fell from 25
ticks per slot to 0–5. Cause: this review's read-only analysis scripts ran a
session-level `SET default_transaction_read_only = on` on the POOLED Neon URL;
pgbouncer (transaction mode) kept the flag on the server connections and
handed them to the app, whose `CREATE SCHEMA IF NOT EXISTS` then failed on
every tick that drew one. Repaired by `scripts/pg-reset-read-only.mjs` (owner
ran it; 0/40 pooled connections read-only afterwards, 13:15 slot back to 25).
The 12:00 UTC bar close was missed for nearly every symbol; open positions
were covered by their exchange brackets. Rule going forward: read-only work
uses `BEGIN READ ONLY` per transaction or the unpooled URL, and any setting
altered during analysis is reset before the session ends.

Follow-ups from the deeper cut (code, this repo):

1. **Position → placing order.** `upsertSwingPosition` now links a position to
   the latest executed BUY/SELL within 48h before entry, then falls back to
   the old 6h any-decision rule. 33 of 125 positions since 09-02 were linked
   to the HOLD tick that observed a resting fill; backfill in
   `docs/positions-repair-2026-09-10.sql` (owner runs it).
2. **Refusal pre-filter.** A refusal investigation whose post-refusal path
   stayed within half a primary ATR in both directions (flat 0.5% when the
   ATR is unknown) gets a mechanical `right_to_skip` with `lesson_action:
   none` and no analyst call. On last week's 94 investigations this removes
   ~26 calls (25 right, 1 wrong). `SWING_REFUSAL_MIN_EXCURSION_ATR=0` disables.
3. **Perplexity in-position only.** Flat scans no longer fetch the Sonar
   digest (was 110–145 searches/day). Contract test moved to an in-position
   scenario; flat-hold pins the absence.
4. **J225 cron removed** — minimum size needs ~320 of margin on a ~93 account;
   454 wasted ticks last week. HK50 (25) and TLT are marginal and kept.
5. **USDJPY notional in USD.** `quoteNotionalToUsd`: for USD-based forex pairs
   the size is the dollar notional; five historical rows repaired in the same
   SQL file.

Re-measured after the first week with the floor live (owner's call):
resting-limit outcomes by tool (§ "deeper cut": limits −9.34 vs market +2.85
on the placing-order attribution), and the Capital hour-of-day buckets.

## 10. Session decision windows (2026-09-10, evening)

Decision: gate the flat look rather than shift the bar clock (the clock shift
is written up in `docs/session-bar-clock-shift.md` for a later session — it
fixes what we measure, the gate fixes when we decide, and two thirds of the
awkward-hour decisions were wakes the clock shift would not touch).

Measured basis (Capital, placing-order attribution, week of 09-07):

| decision hour (UTC) | trades | wins | net |
|---|---|---|---|
| 06, 12, 16 | 22 | 2 | −7.71 |
| all others | 43 | 13 | −2.30 |

Implementation (`session_window_gate` in `analyze.ts`, config in
`decisionConfig.ts resolveSessionWindowConfig`, windows in
`sessionEvents.ts evaluateSessionDecisionWindow`):

- Windows: 120 min before a cash open through 30 min after it; 60 min after a
  cash close. Home and cross-venue influence events both count. The European
  cash close (15:30 UTC) was added as an influence event on the US index,
  metals, energy and FX calendars — that is the 16:00 bucket.
- Inside a window, flat: no evaluation (bar close or wake), a standing resting
  entry is withdrawn (owner's call: nothing of ours fills into the open), the
  wake-watch fired marker is held for the rest of the window so a band does not
  re-fire every 4 minutes, and an owed-look marker is set.
- After the window: the first quarter tick evaluates flat even off-boundary
  (the owed look), once. In-position ticks are never touched.
- Prompt: one sentence in the venue-clock note tells the model the schedule
  and that resting orders will not survive into the open.
- Env: `SWING_SESSION_WINDOW_ENABLED` (default on), `_PRE_OPEN_MIN` (120),
  `_POST_OPEN_MIN` (30), `_POST_CLOSE_MIN` (60). Read at call time.
- Measure: `tick_log` stage `session_window_gate` carries window kind/event and
  whether an order was withdrawn; the skip counterfactual on those ticks is the
  test of the 120/30/60 numbers.
