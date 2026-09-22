// Cache-invalidation counter for the 1-minute wake-watcher's work list.
//
// Leaf module on purpose: lib/swing/pg.ts (the writers) and
// lib/swing/wakeWorkCache.ts (the reader) both depend on it, and keeping the
// counter here is what stops those two from importing each other.
//
// Why it exists: wake-watch runs every minute and its three work-list SELECTs
// were unconditional, so the Neon compute never went 5 minutes without a query
// and never scaled to zero — 6.00 CU-hours every single day, essentially all
// of it idle. The bands themselves are static config between analyze runs (the
// watcher compares them against a LIVE VENUE price, never a stored one), so
// they cache well; what they need is a reliable "something changed" signal.
//
// Every writer that can change what the watcher would see bumps this. A bump
// that fails (KV down) degrades to staleness bounded by the snapshot TTL, not
// to a lost wake — see wakeWorkCache for the fail-open read path.
import { kvIncr } from '../kv';

export const WAKE_WORK_VERSION_KEY = 'swing:wake-work:v';

export async function bumpWakeWorkVersion(): Promise<void> {
    try {
        await kvIncr(WAKE_WORK_VERSION_KEY);
    } catch (err) {
        // Never let cache bookkeeping fail a durable write: the caller has
        // already committed to Postgres, and a missed bump costs at most one
        // snapshot TTL of staleness.
        console.warn('[wake-work] version bump failed:', err);
    }
}
