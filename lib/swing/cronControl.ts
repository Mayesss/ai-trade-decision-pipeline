import { kvGetJson, kvSetJson } from '../kv';

const SWING_CRON_CONTROL_KEY = 'swing:cron:control:v1';

export interface SwingCronControlState {
    hardDeactivated: boolean;
    reason: string | null;
    updatedAtMs: number | null;
    updatedBy: string | null;
}

function asRecord(value: unknown): Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
    return value as Record<string, unknown>;
}

function parseBool(value: unknown): boolean {
    if (typeof value === 'boolean') return value;
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    return ['1', 'true', 'yes', 'on'].includes(normalized);
}

function normalizeOptionalText(value: unknown, maxLength: number): string | null {
    const text = String(value || '').trim();
    if (!text) return null;
    return text.slice(0, Math.max(1, Math.floor(maxLength)));
}

function normalizeUpdatedAtMs(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return null;
    return Math.floor(n);
}

function normalizeState(value: unknown): SwingCronControlState | null {
    const row = asRecord(value);
    if (Object.keys(row).length === 0) return null;
    return {
        hardDeactivated: parseBool(row.hardDeactivated),
        reason: normalizeOptionalText(row.reason, 240),
        updatedAtMs: normalizeUpdatedAtMs(row.updatedAtMs),
        updatedBy: normalizeOptionalText(row.updatedBy, 120),
    };
}

function defaultState(): SwingCronControlState {
    return {
        hardDeactivated: false,
        reason: null,
        updatedAtMs: null,
        updatedBy: null,
    };
}

/**
 * The experiment-ended switch. When SWING_EXPERIMENT_ENDED_AT is set (an ISO
 * date), the swing crons are hard-deactivated regardless of the KV control
 * state: a flushed KV cannot bring the trader back. Set 2026-10-10 when the
 * owner ended the AI lab experiment (docs/forced-flow-research-horizons-2026-10-10.md).
 */
export function experimentEndedAt(): string | null {
    const raw = String(process.env.SWING_EXPERIMENT_ENDED_AT || '').trim();
    return raw || null;
}

export async function loadSwingCronControlState(): Promise<SwingCronControlState> {
    const ended = experimentEndedAt();
    if (ended) {
        return {
            hardDeactivated: true,
            reason: `AI lab experiment ended ${ended}; the AI trader is switched off (SWING_EXPERIMENT_ENDED_AT)`,
            updatedAtMs: null,
            updatedBy: 'env:SWING_EXPERIMENT_ENDED_AT',
        };
    }
    const raw = await kvGetJson<unknown>(SWING_CRON_CONTROL_KEY);
    return normalizeState(raw) || defaultState();
}

export async function setSwingCronControlState(params: {
    hardDeactivated: boolean;
    reason?: string | null;
    updatedBy?: string | null;
}): Promise<SwingCronControlState> {
    const next: SwingCronControlState = {
        hardDeactivated: Boolean(params.hardDeactivated),
        reason: normalizeOptionalText(params.reason, 240),
        updatedAtMs: Date.now(),
        updatedBy: normalizeOptionalText(params.updatedBy, 120),
    };
    await kvSetJson(SWING_CRON_CONTROL_KEY, next);
    return next;
}

