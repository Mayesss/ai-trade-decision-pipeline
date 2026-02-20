const DEFAULT_FMP_BASE_URL = 'https://financialmodelingprep.com/api/v3';
const DEFAULT_REFRESH_MINUTES = 15;
const DEFAULT_STALE_MINUTES = 45;
const DEFAULT_BLOCK_IMPACTS = ['HIGH'];
const DEFAULT_CALL_WARN_THRESHOLD = 180;

function toPositiveInt(value: string | undefined, fallback: number) {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return Math.floor(n);
}

function parseImpacts(raw: string | undefined): string[] {
    if (!raw) return DEFAULT_BLOCK_IMPACTS;
    const impacts = String(raw)
        .split(',')
        .map((item) => item.trim().toUpperCase())
        .filter((item) => item.length > 0);
    return impacts.length ? impacts : DEFAULT_BLOCK_IMPACTS;
}

export function getForexEventConfig() {
    return {
        fmpApiKey: String(process.env.FMP_API_KEY || '').trim(),
        fmpBaseUrl: String(process.env.FMP_BASE_URL || DEFAULT_FMP_BASE_URL).replace(/\/+$/, ''),
        refreshMinutes: toPositiveInt(process.env.FOREX_EVENT_REFRESH_MINUTES, DEFAULT_REFRESH_MINUTES),
        staleMinutes: toPositiveInt(process.env.FOREX_EVENT_STALE_MINUTES, DEFAULT_STALE_MINUTES),
        blockImpacts: parseImpacts(process.env.FOREX_EVENT_BLOCK_IMPACTS),
        callWarnThreshold: toPositiveInt(process.env.FOREX_EVENT_CALL_WARN_THRESHOLD, DEFAULT_CALL_WARN_THRESHOLD),
    };
}

export function normalizeImpact(value: unknown): 'LOW' | 'MEDIUM' | 'HIGH' | 'UNKNOWN' {
    const raw = String(value ?? '')
        .trim()
        .toUpperCase();
    if (!raw) return 'UNKNOWN';
    if (raw.includes('HIGH')) return 'HIGH';
    if (raw.includes('MEDIUM')) return 'MEDIUM';
    if (raw.includes('LOW')) return 'LOW';
    return 'UNKNOWN';
}

export function formatUtcDate(date: Date): string {
    const year = date.getUTCFullYear();
    const month = `${date.getUTCMonth() + 1}`.padStart(2, '0');
    const day = `${date.getUTCDate()}`.padStart(2, '0');
    return `${year}-${month}-${day}`;
}

export function currentUtcDayKey(nowMs = Date.now()): string {
    return formatUtcDate(new Date(nowMs));
}
