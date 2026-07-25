# In-position wake bands — spec

Status: IMPLEMENTED 2026-07-25 (ships behind `ENABLE_POSITION_WAKE_BANDS`, default OFF).
Deviation from the original design: `sanitizePositionWake` is pure (no flag check —
flag gating lives in the `postprocessDecision` routing, read at CALL time like
`ENABLE_CRYPTO_MARGIN_RECYCLE`, and in the analyze persistence / watcher check).

## Problem

In a position, the AI's early-wake surface is blunt: the 1-min watcher fires only on a
≥1.5-ATR move since the last AI look (`SWING_INPOS_EMERGENCY_MOVE_ATR`), plus the
failed-break recross. The model usually *knows* the decision-relevant level — "thesis
dead if we lose 3.42", "at 117.8k decide trail-vs-take" — and those levels are routinely
inside 1.5 ATR. A slow grind to the structural level never trips the emergency wake, so
the next look is the 4H close, often after the venue-side SL already decided. This is the
flat `cooldown_wake` insight applied to position management.

## Semantics

**Additive wake, not a cooldown.** An in-position band suppresses nothing: the 4H
close tick, the emergency ATR wake, failed-break watch, and venue close detection all
run unchanged. The band only adds "also wake me early if price gets here". Every real
AI look (scheduled or woken) **rewrites** the bands — that is the whole
"4H tick overrides it" story.

**Replace-on-every-look (null = clear) — deliberately NOT uniform with TP/SL
keep-if-null.** The fail-safe directions differ, so the semantics should differ:

- TP/SL amends are *protective orders*. A dropped/forgotten amend must not strip
  protection → keep-if-null is correct there.
- A wake band is an *attention request from a specific market view*. Its failure modes
  are asymmetric: a **stale** band (keep-if-null) fires a spurious AI call at a level
  the model no longer cares about, possibly for days; a **forgotten** band (replace)
  merely falls back to today's status quo (4H cadence + emergency wake). Stale is
  worse than forgotten → replace is correct here.
- Keep-if-null also needs an explicit "clear" signal (e.g. `0`), and models are bad at
  emitting explicit clears; stale bands would accumulate silently.
- Restating is cheap: the in-position prompt echoes the currently armed bands
  (`market.position_wake_armed`, below), so the model re-arms or drops with one field.

**No timeout.** Lifecycle is the position's. Staleness is bounded by three things
without any TTL: (1) every in-position AI call replaces the bands (worst-case
staleness = one 4H bar), (2) firing consumes them (via replacement by the woken
look's decision — see "Consumption"), (3) position close ends the thread, which
deletes the row that stores them.

**One-shot.** A crossing fires one early analyze (KV fired-marker dedupes in-flight,
same as flat bands). The woken look re-arms whatever still matters. Note a restated
just-fired band is auto-dropped by the side check (price is now beyond it), so there
is no re-fire churn; a genuine retrace back through the level followed by a deliberate
re-arm is a legitimate second wake.

## Model surface

**Reuse the existing JSON fields** `cooldown_wake_above` / `cooldown_wake_below` /
`cooldown_wake_note` — eligibility already branches on `positionOpen` in
`normalizeDecision` (lib/ai.ts ~1687); instead of zeroing them in-position, route them
to the new path. `cooldown_minutes` stays flat-only and is ignored in-position. No
schema shape change; history/dashboard display (`history.ts` reads
`cooldown_wake_above/below`) works for in-position rows for free.

**Eligibility:** in-position `HOLD` or partial `CLOSE` (`exit_size_pct < 100`) — the
same gate as TP/SL amends (`tpslAmendEligible`). Phase 2 (separable, do not block v1):
also allow on entry actions (`BUY`/`SELL`/`REVERSE`), validated against the freshly
attached bracket — the entry decision knows the thesis level best; until then the
failed-break trigger + bracket cover the first bars.

**Prompt (in-position management section), draft wording:**

> - Wake bands (HOLD or partial CLOSE only): set cooldown_wake_above /
>   cooldown_wake_below to price levels INSIDE your bracket (above your stop, below
>   your take-profit) where you want to be re-evaluated the MOMENT price touches
>   them instead of waiting for the next 4H close — the structural level that would
>   change your management ("wake me if we lose 3.42 support", "wake me at 117.8k to
>   decide trail vs take"). Enforced in code: above must sit above current price,
>   below beneath it; a band at/beyond your SL/TP is dropped (the venue bracket fires
>   there anyway); a band closer than ~0.3 ATR to current price is dropped (noise).
>   Whenever you set a band, ALSO set cooldown_wake_note — one short line stating the
>   decision you plan to make there; it is echoed back as market.position_wake.note
>   when the band fires. Your bands are REPLACED by every decision: re-state them each
>   look if you still want them (your currently armed bands are shown in
>   market.position_wake_armed); null clears them. A band is an early look, never
>   protection — the exchange-side bracket remains the guard.

**Prompt echoes:**
- Every in-position prompt, when bands are armed and not fired this tick:
  `market.position_wake_armed = { above, below, note, set_at }`.
- On the fired tick: `market.position_wake = { crossed: 'above'|'below', level, note,
  set_at }` (mirror of `market.cooldown_wake`).

## Sanitation — `sanitizePositionWake` (lib/ai.ts, next to `sanitizeHoldCooldown`)

Inputs: `action`, `positionOpen`, `side` (`long`/`short`), `price` (= `marketAnchor`),
`primaryAtr`, effective bracket AFTER this tick's amend sanitation
(`exchangeTpsl.takeProfitPrice ?? currentTakeProfit`, same for SL), raw
`wakeAbove/wakeBelow/wakeNote`.

Rules (each violation drops that band with a note; never fails the decision):
1. Eligible only when `positionOpen && (HOLD || partial CLOSE)` → else all null.
2. Side of price: `above > price`, `below < price` (`wake_above_dropped_not_above_price`, …).
3. Inside the bracket, by position side — generic rule: on each side of price the band
   must lie STRICTLY between current price and the bracket level on that side, when
   that level exists (`wake_below_dropped_beyond_bracket`, …).
   - long: `SL < wake_below < price < wake_above < TP`
   - short: `TP < wake_below < price < wake_above < SL`
   Missing bracket level on a side → only checks 2 and 4 apply on that side.
4. Min distance from price: `|band − price| ≥ SWING_POSITION_WAKE_MIN_ATR × primaryAtr`
   (default **0.3**). This is the churn guard — a band glued to price would fire every
   ~5 min (fired-marker TTL), each fire a full AI call. ATR unknown → keep the band
   (checks 2–3 still bound it), note `wake_min_dist_unverified`.
5. Note kept only alongside ≥1 surviving band; trimmed, 200 chars (same as flat).

Sanitized values are written back onto the decision (`cooldown_wake_*`) and notes
appended to `cooldown_notes`, so history shows what was actually armed.

## Storage — columns on `swing.ai_threads`

The band's lifecycle IS the thread's lifecycle, and the watcher already loads
in-position threads every minute — so this costs zero extra queries. Do NOT reuse
`swing.ai_cooldowns` (flat-only semantics: `until_ms`, lease-against-stateless-scans).

```sql
ALTER TABLE swing.ai_threads ADD COLUMN IF NOT EXISTS wake_above     NUMERIC;
ALTER TABLE swing.ai_threads ADD COLUMN IF NOT EXISTS wake_below     NUMERIC;
ALTER TABLE swing.ai_threads ADD COLUMN IF NOT EXISTS wake_note      TEXT;
ALTER TABLE swing.ai_threads ADD COLUMN IF NOT EXISTS wake_set_at_ms BIGINT;
```

(Existing `ADD COLUMN IF NOT EXISTS` migration pattern, pg.ts ~178.)

Helpers: `setSwingThreadWake(platform, symbol, {above, below, note, setAtMs})` —
called on every non-dry in-position AI call with the sanitized values (nulls included:
replace semantics land here naturally); extend `listSwingInPositionThreads()` to
return the wake columns. Thread deletion on close clears bands automatically.

**No claim lease in v1.** The flat-band lease exists because those rows are consumed
by stateless scans and were once deleted-at-detection (AVAX 2026-07-23 lesson). Here we
never delete at detection — bands are only overwritten after the decision is durably
recorded — so a run that dies mid-AI leaves them armed and the watcher re-fires after
the 4-min marker TTL. Residual risk: a watcher-fired run colliding with the 4H
boundary tick double-calls the AI (both are valid management looks). Accept in v1;
add `wake_claimed_until_ms` later only if observed.

## Watcher — pages/api/swing/wake-watch.ts, step 2 extension

Step 2 already iterates open positions with a live price in hand. Add, per marker:

```ts
const bands = wakeByKey.get(`${marker.platform}:${marker.symbol}`); // from listSwingInPositionThreads
const crossed = wakeBandCrossed(marker.price, bands?.wakeAbove, bands?.wakeBelow); // reuse as-is
if (crossed) await maybeFire(marker.platform, marker.symbol, `position_wake_${crossed}`);
else /* existing emergency check */
```

Band check first (more specific fire reason); the shared per-symbol fired marker
dedupes band-vs-emergency anyway. Zero extra fetches, zero extra queries.

## Fired run — pages/api/analyze.ts

1. **Detection on EVERY in-position tick** (mirror of failed-break, ~1075): if the
   thread carries bands and `effectivePrice` is at/beyond one, set
   `positionWake = { crossed, level, note, setAtMs }` + `emitGateDebug('position_wake_detected', …)`.
   Do NOT clear the row at detection.
2. **Quiet-skip bypass** (~1120): `if (positionOpen && inPositionOffCadenceTick &&
   !failedBreak && !positionWake) { …skip… }` — without this the watcher's fired
   analyze dies at the sub-emergency quiet skip and the feature does nothing.
3. **Prompt injection**: `market.position_wake` on the fired tick;
   `market.position_wake_armed` on ordinary in-position ticks with bands armed.
4. **Persistence** (post-decision block, next to `upsertSwingAiCooldown` ~2757): on a
   non-dry in-position AI call, `setSwingThreadWake` with the sanitized values.
   Consumption of a fired band falls out of replacement: the woken look's decision
   overwrites (a restated just-fired band is dropped by the side check). Crash before
   the decision is recorded → bands untouched → watcher re-fires. Quiet-skip ticks
   never touch bands (only real AI calls rewrite).
5. **Snapshot**: add `positionWake` next to `cooldownWake` so "what does the AI do when
   its own in-position wake fires, and does it pay" stays a SQL query; post-mortem
   dossiers pick it up via decisions/tick_log automatically.

## Interactions

- **Emergency ATR wake stays** — band = level the AI anticipated; emergency = backstop
  for what it didn't.
- **Auto margin-recycle BE-raise** can later move the stop past a band without an AI
  look. Harmless: price reaching such a band would hit the (nearer) stop first, so the
  band is dead weight until the next look rewrites it. No cross-validation needed.
- **Same-tick SL/TP amend + bands**: bands validate against the post-amend effective
  bracket (order matters: run `sanitizeExchangeTpSl` first — already the case).
- **Capital**: watcher step 2 covers Capital markers with mid price; a closed venue
  yields no price → `wakeBandCrossed` returns null → quiet. Nothing special.
- **REVERSE / full CLOSE**: fields ineligible (not HOLD/partial) → nulls → replace
  clears; thread end deletes anyway.

## UI (chart overlay) — added 2026-07-25

In-position bands ride the chart's existing cooldown-band overlay (gray dashed
level-window segments) with zero frontend change — the `ChartPanel` renderer is
level-window generic and the segments join the same `cooldowns` array:

- `lib/history.ts`: `isPositionWakeBandDecision` (band present + NO
  `cooldown_minutes` — a sufficient discriminator, since flat bands cannot survive
  sanitation without minutes) and `isPositionWakeEntry` (`snapshot.positionWake`,
  the truncation row) both enter the per-symbol marker index.
- `pages/api/chart.ts`: historical rows draw a one-primary-bar window (a band lives
  until the next look replaces it), truncated at the next indexed decision — the
  lived window, like flat cooldowns. The currently-armed bands are merged from the
  `ai_threads` row (extend-or-add to now), mirroring the active-cooldown merge.
  Data-driven, no flag check: with the flag off no such rows/bands exist.

Decision-JSON surfaces (dashboard detail, legacy band read) show the sanitized
`cooldown_wake_*` fields on in-position rows for free via the write-back.

## Config

- `ENABLE_POSITION_WAKE_BANDS` — gates prompt block, watcher check, detection,
  persistence. Ship default-off, flip after a dry-run smoke test (repo flag pattern,
  cf. `ENABLE_CRYPTO_MARGIN_RECYCLE`).
- `SWING_POSITION_WAKE_MIN_ATR` — min band distance from price in primary-ATR units,
  default 0.3.

## Tests

- `sanitizePositionWake` matrix: long/short × side checks × bracket bounds ×
  min-distance × missing bracket/ATR × note handling × eligibility (HOLD, partial
  CLOSE, full CLOSE, entries, flat).
- Watcher: band crossing fires with `position_wake_*` reason; band+emergency dedupe;
  thread without bands untouched.
- Analyze: quiet-skip bypass on `positionWake`; detection does not clear at detect;
  replace-on-decision (incl. null-clears and just-fired restatement drop).

## Touched files

`lib/ai.ts` (prompt, eligibility routing, `sanitizePositionWake`), `lib/swing/pg.ts`
(columns + helpers), `pages/api/swing/wake-watch.ts` (step 2), `pages/api/analyze.ts`
(detect / bypass / inject / persist / snapshot), tests alongside
`lib/swing/wakeWatch.test.ts` and `lib/ai.schema.test.ts`.
