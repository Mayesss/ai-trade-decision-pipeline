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

The other crons are not the problem, and it is worth knowing why:

- `/api/dashboard/summary-warm-fallback` (`3,18,33,48`) early-returns on the
  KV warm latch. **KV-only on the happy path — no Postgres.**
- `/api/swing/postmortem-drain` (`7,22,37,52`) returns `postmortem_mode_off`
  ahead of any query, since the analyst went off by default on 2026-09-17.
  **No Postgres, no AI, no outbound** — asserted by
  `test/contract/postmortemDrain.contract.test.ts`. It is a deliberate,
  tested no-op kept so that flipping `SWING_POSTMORTEM_MODE` back on resumes
  the queue where it stands. Don't "clean it up" — it costs nothing.
- `/api/swing/weekly-digest` runs once a week. Irrelevant.
- `/api/evaluate` is **not on a cron at all** — dashboard/manual only.

So after the watcher is fixed, the only thing that wakes Neon on a schedule is
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
floor for this cron layout is **roughly 45-50% duty cycle, not near-zero.** The
only remaining lever that would move it is a slower analyze cadence, which is a
trading decision, not a cost one.

## Verifying it worked

`/api/swing/wake-watch` returns `workSource`: `kv` means that minute cost zero
Postgres round trips. Expect `pg` on the watcher minute right after each
analyze cycle and `kv` for the rest.

Then re-read consumption. The daily figure should fall off `6.00` toward
~2.5-3.0 CU-hours (the ~45-50% the 300s timer allows), not to near-zero:

```
neon api /consumption_history/v2/projects -Q granularity=daily \
  -Q from=<iso> -Q to=<iso> -Q org_id=<org> -Q project_ids=<project> \
  -Q metrics=compute_unit_seconds,public_network_transfer_bytes
```
