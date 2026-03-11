export type ScalpBackend = 'pg';

export function resolveScalpBackend(): ScalpBackend {
    return 'pg';
}

export function scalpPgReadsEnabled(): boolean {
    return true;
}

export function scalpPgWritesEnabled(): boolean {
    return true;
}
