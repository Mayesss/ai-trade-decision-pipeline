# Neon Compute Cost

Companion to [neon-egress.md](neon-egress.md). Egress and storage are the
cheap axes here; **compute-hours are the bill.**

## What the 2026-09 budget warning actually was

Measured from the Neon consumption API for 2026-08-24 → 09-21:

| axis | value | verdict |
|---|---|---|
| compute | **6.00 CU-hours every single day** (~180/month) | the entire bill |
| storage (root branch) | 6 MB (171 MB before the 08-28 scalp drop) | negligible |
| egress | ~40 MB/day, 1.15 GB/month | negligible |

`6.00` is not a coincidence: it is exactly `0.25 CU × 86400 s`. The endpoint
(`ep-silent-flower-agj9jr38`, fixed 0.25 CU) reported `current_state: active`
with `started_at: 2026-09-03` — **19 days without suspending once.** Every
second of every day was billed, essentially all of it idle.

Not a repeat of the bulk-bars egress incident. The bars-live-in-R2 rule held.

## Why it never scaled to zero

`suspend_timeout_seconds: 0` means Neon's **5-minute default**. The compute
sleeps only after 5 minutes with no query, and one cron never let that happen:

- `/api/swing/wake-watch` runs `* * * * *` and fired three unconditional
  Postgres SELECTs **every minute**. A per-minute query against a 5-minute
  timer means the compute can never sleep. This was the whole cause.

The other crons were not the problem in September (the schedule they ran on
is superseded — see [Thinning the schedule](#thinning-the-schedule-2026-10-06)):

- `/api/dashboard/summary-warm-fallback` early-returns on the KV warm latch.
  **KV-only on the happy path — no Postgres.** That stays true only while it
  runs right after an analyze firing; it now follows the daily looks.
- `/api/swing/postmortem-drain` returned `postmortem_mode_off` ahead of any
  query once the analyst went off by default on 2026-09-17. It was a tested
  no-op; on 2026-10-06 the owner retired it as a cron (see
  [Postmortem drain](#postmortem-drain-retired-as-a-cron-2026-10-06)).
- `/api/swing/weekly-digest` runs once a week. Irrelevant.
- `/api/evaluate` is **not on a cron at all** — dashboard/manual only.

So after the watcher was fixed, the only thing that woke Neon on a schedule was
the 9 analyze crons at `*/15`.

## The fix: a KV-cached watcher work list

`lib/swing/wakeWorkCache.ts` + `lib/swing/wakeWorkVersion.ts`.

The three lists the watcher reads (wake bands, break triggers, in-position
threads) are static config between analyze runs — the watcher compares them
against a **live venue price**, never a stored one — and nothing else in the
repo reads them. So they cache, and the watcher drops from 60 Postgres touches
an hour to roughly 4, landing inside the analyze window that already woke the
compute.

Correctness model — **Postgres stays the source of truth; KV only skips reads**:

- Every writer that can change what the watcher would see bumps a version
  counter. A snapshot serves only while its stamped version still matches.
- The **claim-lease test moved out of SQL into JS** (`swingCooldownClaimIsLive`),
  applied on cached rows against the current clock. A lease lapsing is a
  time-based transition with no accompanying write, so a SQL-side filter would
  have frozen a crashed run's row out of the list for a whole cache TTL —
  exactly the added wake latency the lease exists to prevent (BGBUSDT
  2026-07-29). This way a cached row re-arms the instant its lease expires.
- A **partial read is never cached.** Caching `[]` for a list that merely
  failed would hide real wake work for a whole TTL.
- TTL is 900s and is only a backstop for a bump that never landed (KV hiccup).
  It is deliberately *longer* than the 15-minute analyze cycle that does the
  real invalidating: a shorter TTL would force extra Postgres reads on an
  unaligned phase and wake the compute for nothing. 15 minutes bounds a
  worst-case failure to one analyze cycle — the watcher degrades to the tick it
  was built to beat, never to something worse.
- **Every KV failure path falls through to Postgres.** KV down = today's
  behaviour, never a missed wake.

### Adding a writer to those tables

`swing.ai_cooldowns`, `swing.break_triggers`, `swing.ai_threads` — any new
function that inserts, updates or deletes a row **must** call
`bumpWakeWorkVersion()` after the write, or the watcher can run up to 15
minutes on a stale list. The 13 existing writers in `lib/swing/pg.ts` all do.

## Account guardrail: the scale-to-zero timer is capped by plan

Scale-to-zero is a per-endpoint setting, not application code:

```
neon api /projects/<project>/endpoints/<endpoint> -X PATCH \
  -F endpoint.suspend_timeout_seconds=<seconds>
```

**On Launch the floor is 300s, which is also the default** — probed 2026-09-22,
120/180/240 all rejected with `suspend interval is too short for your plan`.
So there is no gain available on this lever at the current plan; the endpoint
is pinned at an explicit 300. A shorter timer (60s, worth roughly another 20
points of duty cycle) needs Scale, which costs more than it saves at this size.

The consequence: every wake is followed by 5 billed idle minutes before the
compute suspends. With analyze on `*/15` that is ~7 busy minutes per 15, so the
floor for that cron layout was **roughly 45-50% duty cycle, not near-zero.**
The only remaining lever was fewer wake windows — pulled on 2026-10-06, below.

## Verifying it worked

`/api/swing/wake-watch` returns `workSource`: `kv` means that tick cost zero
Postgres round trips. Under the 2026-10-06 schedule expect `pg` on the :05 tick
after each venue's daily look, on the tick after any watcher write, and about
once a day from the TTL backstop; `kv` for the rest. `workRefreshed: true`
means the run rebuilt the snapshot itself after its own fires/writes.

Then re-read consumption. The daily figure should fall off `6.00` toward
~2.5-3.0 CU-hours (the ~45-50% the 300s timer allows), not to near-zero:

```
neon api /consumption_history/v2/projects -Q granularity=daily \
  -Q from=<iso> -Q to=<iso> -Q org_id=<org> -Q project_ids=<project> \
  -Q metrics=compute_unit_seconds,public_network_transfer_bytes
```

## Thinning the schedule (2026-10-06)

### Why

Measured from the Neon operations log for 2026-10-04 → 10-06: **53–74 compute
starts a day**, clustered at :00/:15/:30/:45 (plus a few at :37), the compute
awake ~70% of the clock. The endpoint is already a fixed 0.25 CU and the
5-minute suspend timer is the Launch floor, so the only lever left is fewer
wake windows. Yet the scheduled decision has been daily since 2026-09-16
(`DECISION_CADENCE = '1D'`), while `vercel.json` still ran the 9 analyze
crons every 15 minutes: 96 firings a day, of which at most 2 (one per venue)
can serve a look.

### What a non-owed 15-minute tick did, and what touched Postgres

A tick that is not the owed daily look still runs the whole "upkeep surface"
of `pages/api/analyze.ts` before the scheduled-look gate ends it. In order
(**PG** = Postgres; quarter = :15/:30/:45, hourly = :00):

| # | Step | Store | Note |
|---|---|---|---|
| 1 | last-scan start stamp | KV | dashboard timeline dot |
| 2 | cron kill-switch read | KV | |
| 3 | Capital tradeability | venue | closed → skip; hourly ticks write a skip row (**PG**) |
| 4 | position read | venue | |
| 5 | open-warmup gate (Capital, flat) | — | hourly skip row (**PG**) |
| 6 | portfolio cap (flat) | **PG** read every flat tick | blocked → hourly skip row (**PG**) |
| 7 | account equity snapshot (hourly) | venue + **PG** insert | weekly-digest equity trace |
| 8 | scheduled-look served marker | KV | only inside the retry window |
| 9 | margin pre-skip (flat) | venue | blocked → skip row (**PG**) |
| 10 | AI thread reconcile | **PG** read every tick | fill → `in_position`, venue close → thread end + Capital close persistence (**PG** writes on transition) |
| 11 | resting-entry read, age backstop (hourly), stale-thread cleanup | venue, **PG** on transition | |
| 12 | market bundle + chart/overlay cache warm | venue, KV, **PG** read (positions mirror) | |
| 13 | failed-break check (in position) | **PG** read | |
| 14 | in-position quiet skip | KV + **PG** `tick_log` insert | |
| 15 | session-window gate (Capital, flat, in a window) | venue cancel, KV owed marker, **PG** `tick_log` | withdraws resting entries; parks the flat look |
| 16 | owed session-window look release | KV | first off-boundary tick after the window |
| 17 | flat scheduled-look gate | **PG** cooldown peek + **PG** `tick_log` insert | `not_primary_close` |
| 18 | warm latch (finally) | KV | last finisher rebuilds the dashboard summary (**PG** reads) |

So every tick touched Postgres several times (`tick_log`, thread, cooldown,
portfolio occupants), and the warm-latch summary rebuild read it again. The
production-shaped transcript is pinned in
`test/contract/analyze/__snapshots__/cron-retry-served.txt`.

What those ticks actually decided over 10-03 → 10-06 (`swing.tick_log`):
overwhelmingly skips — `quiet_position` 432, `primary_close_gate` 331,
`capital_market_closed` 217, `session_window_gate` 187,
`asset_class_occupied` 179. The only AI calls they caused were **5
`session_window_owed` looks** (GBPUSD, US100, TLT, EURUSD ×2) and 2 quarter
ticks that saw a wake band crossed (which the watcher sees too).

### The new schedule

| Cron | Before | After |
|---|---|---|
| analyze, 2 Bitget symbols | `*/15 * * * *` | `0 0-5 * * *` |
| analyze, 7 Capital symbols | `*/15 * * * *` | `0 8-13 * * 1-5` |
| wake-watch | `* * * * *` | `5,15,25,35,45,55 * * * *` |
| summary-warm-fallback | `3,18,33,48 * * * *` | `5 0-5 * * *` (`?venue=bitget`), `5 8-13 * * 1-5` (`?venue=capital`) |
| postmortem-drain | `7,22,37,52 * * * *` | removed |
| weekly-digest | `30 5 * * 0` | unchanged |

**Owed-look semantics kept.** `decisionDayKey` makes the daily look owed from
`DECISION_HOUR_UTC` (Bitget 00:00, Capital 08:00) for
`DECISION_RETRY_WINDOW_MIN` (6 h): every cron tick in the window is due until
one claims the served marker. The analyze crons now fire exactly there — on
the decision hour, then hourly to the end of the window — so a dead AI call or
a crashed tick costs an hour, not a day (it used to cost 15 minutes; hourly is
the trade). A tick outside its window can never owe a look, so none fire there.
Capital is weekdays only: every instrument is closed on Saturday and Sunday,
and those ticks could only ever record `capital_market_closed`. If
`SWING_DECISION_HOUR_UTC_*` or `SWING_DECISION_RETRY_WINDOW_MIN` change,
`vercel.json` must change with them. `test/unit/cronSchedule.test.ts` fails
otherwise: it expands every schedule and asserts the window, the
decision-hour start, hourly retries, the weekday split, the fallback offset,
the watcher period and the absence of the drain.

Move-driven looks never came from the analyze cron by design: wake bands,
emergency moves, failed breaks, closes and sweeps are fired by wake-watch.

**Every remaining timer is KV-only on its happy path.** Nothing runs every 15
minutes any more. wake-watch runs every 10 and touches Postgres only when it
has work: a stale or invalidated snapshot, a fire, or a sustained-band touch
write. The fallback is one KV `GET` when the latch completed, and it fires
only right after an analyze firing, so even its rebuild lands in an awake
compute.

### Where the upkeep went

| Duty (step above) | Now |
|---|---|
| venue-side close reconcile (10) | wake-watch step 4, unchanged (`position_closed`) |
| resting entry filled → thread `in_position` (10) | **wake-watch step 5, new**: `entry_filled` fires a `reconcileOnly` run. Without it a filled entry's later close would go unseen until the next look. |
| resting entry vanished → stale thread (11) | **wake-watch step 5, new**: `pending_entry_gone` (venue order books empty) fires a `reconcileOnly` run. Left alone, the stale thread holds an asset-class slot in the portfolio cap for up to a day. |
| session-window withdraw (15) | **wake-watch step 5, new**: a Capital `pending_entry` thread inside a window fires one plain wake per window; the analyze session gate withdraws as before |
| owed session-window look (16) | **wake-watch step 6, new**: fires once per marker after its window, unless another window is active or the symbol holds a position; analyze now also consumes the marker on a wake fire (`owedLookConsumer`), so a parked band wake that re-fires after the window is that look, not a second one |
| failed break (13), position wake, emergency (14) | wake-watch steps 2–3, unchanged; failed-break window widened (below) |
| flat band crossing seen by the cron's peek (17) | wake-watch step 1 (the cron peek was only a backstop) |
| resting-entry age backstop (11) | window ticks only. Justified: `RESTING_ENTRY_MAX_AGE_MINUTES` (1470 under `1D`) is a net under our own outages, and the model sees every standing entry at its daily look. Worst case an aged entry is swept up to ~a day late. |
| equity snapshot (7) | window ticks only: 6 a day per venue instead of 24. Justified: the weekly digest reads first/last/min/max equity per week, and R is read from fills, not equity. Intraday min/max get coarser. |
| chart/overlay cache warm (12), summary warm (18) | after window ticks only; outside the windows the dashboard builds on demand (a page load then touches Postgres — user-driven, not scheduled) |
| last-scan markers (1) | window ticks + wake fires. Dashboard scan-staleness threshold 35 min → 26 h (`pages/index.tsx`); `TICK_CYCLE_MS` 15 → 60 min (`lib/swing/lastScan.ts`) |

Reconcile fires (`reconcileOnly` and the existing `postCloseReconcile`) now
skip the gates that only decide whether a look is worth spending: closed
market, open warmup, portfolio cap, margin. Without that, a bracket fill seen
as Capital closes for the weekend returned at `capital_market_closed` before
the reconcile ran. The thread never changed, so the watcher re-fired it every
tick until Monday, with a cold start each time. Those runs never reach the AI.

### Behaviour that changes (owner-visible)

- **Fewer clock-driven AI looks.** Quarter ticks inside session windows
  (pre-open 120 min, opening drive 30, post-close 60) parked a "look" every
  day and released it as an AI call after each window: 5 such calls in
  10-03 → 10-06, ~1.7/day, none of them deferred from a look that was actually
  due. Now an owed marker exists only when a real look (the daily look, a
  wake) hit a window, so these stop. This is consistent with the daily-cadence
  intent ("the time-based half as slow as the signal allows"), but it is a
  trading change inside the measurement window.
- **Wake latency 1 → ≤10 minutes** for bands, emergency moves, closes.
- **Sustained confirms are judged at ticks.** `WAKE_CONFIRM_MIN_MINUTES` 5 →
  10 (a 5-minute ask fired after 10 anyway), and every window rounds up to the
  next tick. The owner's premise "bands always ask for ≥10 minutes" did not
  hold in code: the clamp floor was 5, and a band with no confirm window
  (`confirmMinutes` null) fires on the first observed cross. With a 10-minute
  tick, "first observed" can be up to 10 minutes late, and a
  touch-and-reclaim inside one tick is never seen — no sweep evidence, no
  reclaim wake. The extension confirm (≥0.5 ATR beyond the band) still fires
  on the first tick that sees it.
- **Failed-break window 10 → 20 minutes** (`FAILED_BREAK_POST_CLOSE_WINDOW_MIN`
  = phase + 1.5 ticks). It must hold the first two ticks after a 4H close
  (:05 and :15). The old window admitted only one, so a single failed candle
  fetch lost the check for the whole bar.
- **Fired-marker TTL 240 → 300 s** (the watcher's `maxDuration`, shorter than a
  tick): it still dedupes within and across an overlapping run, and an event
  that still stands re-fires on the next tick.
- **Session-sweep touch state TTL** = window + 2 ticks (was window + 10 min),
  so the state survives until the tick that abandons it.
- **Work-list snapshot TTL 900 s → 12 h.** Invalidation is the version bump;
  the TTL only backstops a lost bump and sets a floor on cold starts.
  Upkeep-only cron ticks don't bump, so the snapshot is normally rebuilt only
  after each venue's look (~00:05, ~08:05). 12 h then expires once a day
  outside a window (~20:05); 6 h would expire three times. The price: a bump
  lost to a KV hiccup can leave the watcher on a stale list for up to 12 h
  (logged as `[wake-work] version bump failed`; INCR `swing:wake-work:v` to
  force a rebuild).
- **Watcher phase :05.** The snapshot rebuild after an analyze firing (:00)
  lands inside that firing's wake instead of 10 minutes later on a suspended
  compute. A watcher run that fired or wrote also rebuilds the snapshot before
  it returns, while its own fires have the compute awake.
- **Prompt text** (`lib/swing/prompt.ts`): "watched roughly once per MINUTE" /
  "woken within ~a minute" now read "checked every ~10 minutes" / "within ~10
  minutes", the confirm clamp shows 10–60, and the model is told sub-tick
  pokes can go unseen. Every figure comes from `WAKE_WATCH_TICK_MINUTES`.

### Expected wakes per day

A wake window is ~1–2 minutes of work plus the 5 billed idle minutes. Firings
less than ~5 minutes apart share one.

| Source | Weekday | Sat/Sun |
|---|---|---|
| Bitget window ticks 00:00–05:00 (each also covers its :05 fallback + watcher rebuild) | 6 | 6 |
| Capital window ticks 08:00–13:00 | 6 | 0 |
| Work-list TTL backstop (~20:05 weekdays, ~12:05 weekends) | ~1 | ~1 |
| Watcher-fired analyze runs (bands, emergencies, closes, failed breaks, reclaims, resting-entry upkeep, owed looks) — 10-03→10-06 averaged ~5/day | ~3–6 | fewer |
| Sustained-band touch writes (arm / extend / sweep are Postgres writes; each tick that writes is a wake unless a fire shares it) | 0–a few | 0–a few |
| Weekly digest (Sun 05:30) | 0 | 1 on Sunday |
| **Total** | **~16–20** | **~8–12** |

That is down from 53–74, roughly 2 h of awake compute a day instead of ~17 h:
about **0.5 CU-hours/day**, against ~4.2 at the measured 70%. Dashboard page
loads outside the windows are extra, and they are user-driven.

### Confirming it from the Neon operations log

Read-only; this changes no Neon setting. The log is newest-first and paged;
follow `pagination.cursor` with `-Q cursor=…` until the range you want is
covered.

```
# compute starts per UTC day
neon api /projects/holy-resonance-21485949/operations -Q limit=1000 \
  | jq -r '.operations[] | select(.action == "start_compute") | .created_at[0:10]' \
  | sort | uniq -c

# when in the hour they happen — the schedule's signature
neon api /projects/holy-resonance-21485949/operations -Q limit=1000 \
  | jq -r '.operations[] | select(.action == "start_compute") | .created_at[11:16]' \
  | sort | uniq -c
```

What healthy looks like, from the first full day after the deploy:

- **~16–20 starts on a weekday, ~8–12 on a weekend day.**
- Starts at `HH:00` (seconds past the minute) for HH 00–05 every day and 08–13
  on weekdays: the analyze windows.
- A handful at watcher minutes (`:05/:15/…/:55`) elsewhere: event fires, touch
  writes, and one TTL rebuild (~20:05 weekdays / ~12:05 weekends).

Red flags:

- Starts at `:00/:15/:30/:45` outside the windows: the old analyze schedule is
  still deployed.
- `:03/:18/:33/:48`: the old fallback schedule.
- A start on most watcher minutes: the snapshot is not serving. Check
  `workSource` in the wake-watch responses and `[wake-work]` log lines.
- The pre-change log also showed starts at `:37`. That was the old drain slot,
  but the drain returned before any query, so the cause was not the drain;
  `:37` is on no cron in the new schedule. A start at any minute this
  schedule doesn't explain is a request someone made (dashboard, manual
  call). The operations log can't tell which, so look at the Vercel logs for
  that minute.

Then confirm the bill side with the consumption query in
[Verifying it worked](#verifying-it-worked): the daily `compute_unit_seconds`
should fall from ~15,000 (≈4.2 CU-h) toward ~2,000 (≈0.5 CU-h).

## Postmortem drain: retired as a cron (2026-10-06)

The analyst has been off by default since 2026-09-17
(`resolveSwingPostmortemMode`, `lib/swing/postmortem.ts`), so the
every-15-minutes drain was a tested no-op. The owner retired it: removed from
`vercel.json` and from `UNAUTHENTICATED_CRON_ROUTES` (`lib/admin.ts`). The
route (`pages/api/swing/postmortem-drain.ts`) and the queue
(`swing.postmortems`) stay. Rows queued before the switch are still there and
still claimable.

To switch the analyst back on:

1. Set `SWING_POSTMORTEM_MODE=loss` (losses only) or `all` (losses, wins and
   refusal investigations) in the Vercel production env and redeploy. Close
   triggers enqueue again from then on, each maturing
   `SWING_POSTMORTEM_DELAY_MINUTES` after the exit.
2. Drain by hand. The route is admin-only now, and each call runs up to 3
   mature rows:

   ```
   curl -H "x-admin-access-secret: $ADMIN_ACCESS_SECRET" \
     "https://<production-host>/api/swing/postmortem-drain"
   ```
3. Or put the cron back. That takes both a `vercel.json` entry and the path
   back in `UNAUTHENTICATED_CRON_ROUTES`, because Vercel crons cannot send the
   admin header. Schedule it on the analyze minutes (e.g. `2 0-5,8-13 * * *`)
   so its Postgres claims land in an awake compute instead of buying wakes of
   their own. `test/unit/cronSchedule.test.ts` asserts the cron is absent;
   update that test in the same change.

`test/contract/postmortemDrain.contract.test.ts` pins both halves: a manual
call while the mode is off still claims and calls nothing, and an
unauthenticated call is refused when `ADMIN_ACCESS_SECRET` is set.
