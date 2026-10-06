// The vercel.json cron schedule as a contract (2026-10-06, docs/neon-compute-cost.md).
//
// Neon bills compute x awake time and suspends only after 5 idle minutes, so
// every distinct firing minute that touches Postgres costs a wake window. The
// schedule therefore fires analyze only where a look can be OWED — inside each
// venue's decision retry window — and everything else on a timer is KV-only on
// its happy path. These tests pin the properties that keep both the owed-look
// semantics (decisionConfig.ts decisionDayKey) and the wake budget honest.
import assert from 'node:assert/strict';
import { test } from 'vitest';

import vercelConfig from '../../vercel.json';
import { DECISION_HOUR_UTC, DECISION_RETRY_WINDOW_MIN } from '../../lib/swing/decisionConfig';
import { getCronSymbolConfigs } from '../../lib/symbolRegistry';
import { WAKE_WATCH_TICK_MINUTES, WAKE_WATCH_TICK_OFFSET_MINUTES } from '../../lib/swing/wakeWatch';

type Cron = { path: string; schedule: string };
const crons = (vercelConfig as { crons: Cron[] }).crons;

// Minimal 5-field cron expansion: numbers, a-b ranges, lists, * and */n.
function expandField(field: string, min: number, max: number): Set<number> {
    const out = new Set<number>();
    for (const part of field.split(',')) {
        const [range, stepRaw] = part.split('/');
        const step = stepRaw ? Number(stepRaw) : 1;
        let lo = min;
        let hi = max;
        if (range !== '*') {
            const [a, b] = range.split('-').map(Number);
            lo = a;
            hi = b ?? a;
        }
        for (let v = lo; v <= hi; v += step) out.add(v);
    }
    return out;
}

// Firing minutes-of-week (0 = Sunday 00:00 UTC) for one schedule.
function firings(schedule: string): number[] {
    const [m, h, dom, mon, dow] = schedule.trim().split(/\s+/);
    assert.equal(dom, '*', `day-of-month must stay * (${schedule})`);
    assert.equal(mon, '*', `month must stay * (${schedule})`);
    const minutes = expandField(m, 0, 59);
    const hours = expandField(h, 0, 23);
    const days = expandField(dow, 0, 6);
    const out: number[] = [];
    for (const d of days) for (const hh of hours) for (const mm of minutes) out.push(d * 1440 + hh * 60 + mm);
    return out.sort((a, b) => a - b);
}

const analyzeCrons = crons.filter((c) => c.path.startsWith('/api/swing/analyze'));
const venues = ['bitget', 'capital'] as const;
const scheduleOf = (venue: string) => {
    const schedules = new Set(analyzeCrons.filter((c) => c.path.includes(`platform=${venue}`)).map((c) => c.schedule));
    // The per-venue warm latch counts a venue's crons in one cycle — they must
    // share a schedule or the latch never completes.
    assert.equal(schedules.size, 1, `${venue} analyze crons must share one schedule: ${[...schedules].join(' | ')}`);
    return [...schedules][0];
};

test('every analyze cron belongs to a venue and the registry sees them all', () => {
    assert.equal(getCronSymbolConfigs().length, analyzeCrons.length);
    for (const c of analyzeCrons) {
        assert.ok(venues.some((v) => c.path.includes(`platform=${v}`)), c.path);
    }
});

test('analyze fires only inside its venue decision retry window, from the decision hour, at least hourly', () => {
    for (const venue of venues) {
        const hour = DECISION_HOUR_UTC[venue];
        const fires = firings(scheduleOf(venue));
        const byDay = new Map<number, number[]>();
        for (const f of fires) {
            const day = Math.floor(f / 1440);
            byDay.set(day, [...(byDay.get(day) ?? []), f % 1440]);
        }
        for (const [day, minutes] of byDay) {
            const start = hour * 60;
            const end = start + DECISION_RETRY_WINDOW_MIN;
            for (const m of minutes) {
                // A firing outside the window can never owe a look: pure upkeep
                // that wakes Neon for nothing.
                assert.ok(m >= start && m < end, `${venue} day ${day}: firing at minute ${m} outside [${start}, ${end})`);
            }
            // The look is attempted on the decision hour itself...
            assert.equal(minutes[0], start, `${venue} day ${day}: first firing must be the decision hour`);
            // ...and retried at least hourly until the window closes, so a
            // dead AI call or a crashed tick costs at most an hour, not a day.
            for (let i = 1; i < minutes.length; i++) {
                assert.ok(minutes[i] - minutes[i - 1] <= 60, `${venue} day ${day}: retry gap > 60 min`);
            }
            assert.ok(end - minutes[minutes.length - 1] <= 60, `${venue} day ${day}: window tail left unretried`);
        }
        // Every analyze firing lands on the hour: no quarter ticks.
        assert.ok(fires.every((f) => f % 60 === 0), `${venue}: off-hour firing`);
    }
});

test('bitget is scheduled every day; capital on weekdays only (venue closed on weekends)', () => {
    const days = (venue: string) => new Set(firings(scheduleOf(venue)).map((f) => Math.floor(f / 1440)));
    assert.deepEqual([...days('bitget')].sort(), [0, 1, 2, 3, 4, 5, 6]);
    assert.deepEqual([...days('capital')].sort(), [1, 2, 3, 4, 5]);
});

test('the summary-warm fallback follows each venue firing by a few minutes and never runs on its own', () => {
    const fallbacks = crons.filter((c) => c.path.startsWith('/api/dashboard/summary-warm-fallback'));
    assert.equal(fallbacks.length, venues.length);
    const analyzeFires = new Set(venues.flatMap((v) => firings(scheduleOf(v))));
    for (const fb of fallbacks) {
        for (const f of firings(fb.schedule)) {
            const delay = [1, 2, 3, 4, 5, 6, 7, 8, 9].find((d) => analyzeFires.has(f - d));
            // Off an analyze cycle the latch's done flag is missing by
            // construction and the fallback would rebuild from Postgres.
            assert.ok(delay !== undefined, `fallback firing at minute-of-week ${f} follows no analyze firing`);
        }
    }
});

test('wake-watch runs once per watcher tick, phased off the analyze :00', () => {
    const watch = crons.filter((c) => c.path === '/api/swing/wake-watch');
    assert.equal(watch.length, 1);
    const minutes = firings(watch[0].schedule)
        .filter((f) => f < 60)
        .map((f) => f % 60);
    const expected: number[] = [];
    for (let m = WAKE_WATCH_TICK_OFFSET_MINUTES; m < 60; m += WAKE_WATCH_TICK_MINUTES) expected.push(m);
    assert.deepEqual(minutes, expected);
    assert.equal(firings(watch[0].schedule).length, 7 * 24 * expected.length, 'every hour of every day');
    // The snapshot rebuild after an analyze firing must land while that
    // firing's wake is still open (Neon suspends 5 idle minutes after it ends).
    assert.ok(WAKE_WATCH_TICK_OFFSET_MINUTES > 0 && WAKE_WATCH_TICK_OFFSET_MINUTES <= 5);
});

test('the postmortem drain is retired as a cron', () => {
    assert.ok(!crons.some((c) => c.path.startsWith('/api/swing/postmortem-drain')));
});
