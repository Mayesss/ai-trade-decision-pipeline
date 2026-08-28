// Ordered record of EVERY outgoing effect a code path produces.
//
// Two kinds of entries land here:
//   HTTP  every request intercepted by msw (test/harness/index.ts)
//   SQL   every query against the swing Postgres (test/harness/pg.ts)
//
// Together they form the conversation that goes into the snapshot — the
// complete view of what the code under test does to the outside world.

export interface RecordedEntry {
    /** HTTP method or effect kind (SQL). */
    method: string;
    /** URL or effect target (e.g. neon-postgres). */
    url: string;
    headers: Record<string, string>;
    body: unknown;
}

const entries: Promise<RecordedEntry>[] = [];

/** Asynchronously described entry (HTTP requests need their body read). */
export function recordPending(entry: Promise<RecordedEntry>): void {
    entries.push(entry);
}

/** Synchronous entry — SQL queries. */
export function recordEffect(kind: string, target: string, body: unknown): void {
    entries.push(
        Promise.resolve({
            method: kind,
            url: target,
            headers: {},
            body,
        }),
    );
}

export async function allEntries(): Promise<RecordedEntry[]> {
    return Promise.all(entries);
}

export function resetEntries(): void {
    entries.length = 0;
}
