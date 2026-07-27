// Global AI-call health flag (KV). Written from the provider switch
// (lib/aiProvider.callSwingDecision), the single choke point every swing AI
// call goes through — decisions, postmortems, forex advisor, evaluations —
// so ONE place knows whether the model is reachable at all.
//
// Why it exists: when the OpenAI subscription lapsed (2026-07) every tick
// died with an untyped 500 and the dashboard showed nothing — no banner, no
// stale marker, nothing. This blob is the single source of truth the UI (and
// a future /api/health check) can read to say "the AI has been down since T".
//
// Latching rules:
// - 'billing' / 'config' errors (see lib/aiError.ts) latch degraded=true on
//   the FIRST failure — they never self-heal, so there is nothing to wait for.
// - 'transient' errors latch only after TRANSIENT_DEGRADE_THRESHOLD
//   consecutive failures (~100 calls/hour on the swing path, so a genuine
//   outage latches within minutes while a lone rate-limit blip stays quiet).
// - Any successful call fully clears the flag.
//
// KV cost (budget burned once already — keep it lean): failures pay GET+SET;
// successes pay one GET and only SET when there is actually a streak/flag to
// clear, so the steady healthy state adds a single read per AI call.
import { kvGetJson, kvSetJson } from '../kv';
import type { AiCallError, AiCallProvider, AiErrorKind } from '../aiError';

const SWING_AI_HEALTH_KEY = 'swing:ai:health:v1';
const TRANSIENT_DEGRADE_THRESHOLD = 5;

export interface SwingAiHealthState {
    degraded: boolean;
    // Provider + classification of the latest failure (null when healthy).
    provider: AiCallProvider | null;
    kind: AiErrorKind | null;
    reason: string | null;
    // Start of the CURRENT consecutive-failure streak (survives while the
    // streak grows) — "down since" for the UI.
    sinceMs: number | null;
    lastErrorAtMs: number | null;
    // Stamped when a success clears a streak/flag — "recovered at".
    lastOkAtMs: number | null;
    consecutiveFailures: number;
}

function defaultState(): SwingAiHealthState {
    return {
        degraded: false,
        provider: null,
        kind: null,
        reason: null,
        sinceMs: null,
        lastErrorAtMs: null,
        lastOkAtMs: null,
        consecutiveFailures: 0,
    };
}

function normalizeState(value: unknown): SwingAiHealthState {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return defaultState();
    const row = value as Record<string, unknown>;
    const num = (v: unknown): number | null => {
        const n = Number(v);
        return Number.isFinite(n) && n > 0 ? Math.floor(n) : null;
    };
    const text = (v: unknown, max: number): string | null => {
        const t = String(v || '').trim();
        return t ? t.slice(0, max) : null;
    };
    const kindRaw = String(row.kind || '');
    const providerRaw = String(row.provider || '');
    return {
        degraded: row.degraded === true,
        provider: providerRaw === 'openai' || providerRaw === 'claude' ? providerRaw : null,
        kind: kindRaw === 'billing' || kindRaw === 'config' || kindRaw === 'transient' ? kindRaw : null,
        reason: text(row.reason, 300),
        sinceMs: num(row.sinceMs),
        lastErrorAtMs: num(row.lastErrorAtMs),
        lastOkAtMs: num(row.lastOkAtMs),
        consecutiveFailures: Math.max(0, Math.floor(Number(row.consecutiveFailures) || 0)),
    };
}

export async function loadSwingAiHealth(): Promise<SwingAiHealthState> {
    try {
        return normalizeState(await kvGetJson<unknown>(SWING_AI_HEALTH_KEY));
    } catch {
        return defaultState();
    }
}

// Best-effort, never throws — health accounting must never fail (or slow
// down) the trading tick it is reporting on.
export async function reportSwingAiFailure(err: AiCallError): Promise<void> {
    try {
        const prev = await loadSwingAiHealth();
        const now = Date.now();
        const consecutiveFailures = prev.consecutiveFailures + 1;
        const persistent = err.kind === 'billing' || err.kind === 'config';
        const next: SwingAiHealthState = {
            degraded: prev.degraded || persistent || consecutiveFailures >= TRANSIENT_DEGRADE_THRESHOLD,
            provider: err.provider,
            kind: err.kind,
            reason: String(err.message || '').slice(0, 300),
            sinceMs: prev.consecutiveFailures > 0 && prev.sinceMs ? prev.sinceMs : now,
            lastErrorAtMs: now,
            lastOkAtMs: prev.lastOkAtMs,
            consecutiveFailures,
        };
        await kvSetJson(SWING_AI_HEALTH_KEY, next);
    } catch (kvErr) {
        console.warn('swing AI health failure-report write failed:', kvErr);
    }
}

export async function reportSwingAiSuccess(): Promise<void> {
    try {
        const prev = await loadSwingAiHealth();
        // Steady healthy state: nothing to clear, skip the write.
        if (!prev.degraded && prev.consecutiveFailures === 0) return;
        const next: SwingAiHealthState = {
            ...defaultState(),
            lastErrorAtMs: prev.lastErrorAtMs,
            lastOkAtMs: Date.now(),
        };
        await kvSetJson(SWING_AI_HEALTH_KEY, next);
    } catch (kvErr) {
        console.warn('swing AI health success-report write failed:', kvErr);
    }
}
