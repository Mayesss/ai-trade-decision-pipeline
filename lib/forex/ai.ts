import { callAI } from '../ai';
import type {
    ForexModuleName,
    ForexPairEligibility,
    ForexPacketSnapshot,
    ForexPermission,
    ForexRegime,
    ForexRegimePacket,
    ForexRiskState,
} from './types';

const MODULE_SET: ForexModuleName[] = ['pullback', 'breakout_retest', 'range_fade', 'none'];
const REGIME_SET: ForexRegime[] = ['trend_up', 'trend_down', 'range', 'high_vol', 'event_risk'];
const PERMISSION_SET: ForexPermission[] = ['long_only', 'short_only', 'both', 'flat'];
const RISK_SET: ForexRiskState[] = ['normal', 'elevated', 'extreme'];

function clampConfidence(value: unknown): number {
    const n = Number(value);
    if (!Number.isFinite(n)) return 0;
    return Math.max(0, Math.min(1, n));
}

function normalizeEnum<T extends string>(value: unknown, allowed: T[], fallback: T): T {
    const raw = String(value ?? '')
        .trim()
        .toLowerCase() as T;
    return allowed.includes(raw) ? raw : fallback;
}

function normalizeModules(value: unknown): ForexModuleName[] {
    if (!Array.isArray(value)) return ['none'];
    const modules = value
        .map((v) => normalizeEnum<ForexModuleName>(v, MODULE_SET, 'none'))
        .filter((v) => MODULE_SET.includes(v));
    if (!modules.length) return ['none'];
    return Array.from(new Set(modules));
}

export function regimeExclusiveModules(regime: ForexRegime): ForexModuleName[] {
    if (regime === 'trend_up' || regime === 'trend_down') return ['pullback'];
    if (regime === 'high_vol') return ['breakout_retest'];
    if (regime === 'range') return ['range_fade'];
    return ['none'];
}

function fallbackPacket(entry: ForexPairEligibility, nowMs: number): ForexRegimePacket {
    const trend = entry.metrics.trendStrength;
    const chop = entry.metrics.chopScore;
    const spreadStress = entry.metrics.spreadToAtr1h;

    let regime: ForexRegime = 'range';
    let permission: ForexPermission = 'flat';
    let allowedModules: ForexModuleName[] = ['none'];
    let riskState: ForexRiskState = 'normal';

    if (entry.metrics.shockFlag || spreadStress > 0.2) {
        regime = 'high_vol';
        riskState = 'elevated';
    }

    if (chop > 0.45 && trend < 0.9 && spreadStress < 0.15) {
        regime = 'range';
        permission = 'both';
        allowedModules = ['range_fade'];
    }

    if (trend > 0.7 && chop < 0.5) {
        regime = 'trend_up';
        permission = 'both';
        allowedModules = ['pullback', 'breakout_retest'];
    }

    if (entry.metrics.spreadToAtr1h > 0.18) {
        permission = 'flat';
        allowedModules = ['none'];
        riskState = 'elevated';
    }

    return {
        pair: entry.pair,
        generatedAtMs: nowMs,
        regime,
        permission,
        allowed_modules: allowedModules,
        risk_state: riskState,
        confidence: 0.5,
        htf_context: {
            nearest_support: null,
            nearest_resistance: null,
            distance_to_support_atr1h: null,
            distance_to_resistance_atr1h: null,
        },
        notes_codes: ['AI_FALLBACK_PACKET'],
    };
}

function enforceHardRules(packet: ForexRegimePacket, opts: { eventBlocked: boolean }): ForexRegimePacket {
    const next = { ...packet, notes_codes: [...(packet.notes_codes || [])] };

    if (next.confidence < 0.55) {
        next.permission = 'flat';
        next.allowed_modules = ['none'];
        if (!next.notes_codes.includes('CONFIDENCE_UNDER_055_FORCE_FLAT')) {
            next.notes_codes.push('CONFIDENCE_UNDER_055_FORCE_FLAT');
        }
    }

    if (next.risk_state === 'extreme') {
        next.allowed_modules = ['none'];
        if (!next.notes_codes.includes('RISK_EXTREME_DISABLE_MODULES')) {
            next.notes_codes.push('RISK_EXTREME_DISABLE_MODULES');
        }
    }

    if (opts.eventBlocked) {
        next.regime = 'event_risk';
        next.permission = 'flat';
        next.allowed_modules = ['none'];
        if (!next.notes_codes.includes('EVENT_GATE_ACTIVE_DISABLE_MODULES')) {
            next.notes_codes.push('EVENT_GATE_ACTIVE_DISABLE_MODULES');
        }
    }

    if (next.permission === 'flat') {
        next.allowed_modules = ['none'];
    } else {
        // Keep module execution deterministic and non-conflicting across regimes.
        const exclusive = regimeExclusiveModules(next.regime);
        next.allowed_modules = exclusive;
        if (!next.notes_codes.includes('MODULE_EXCLUSIVITY_ENFORCED')) {
            next.notes_codes.push('MODULE_EXCLUSIVITY_ENFORCED');
        }
    }

    return next;
}

function buildAiPrompt(entry: ForexPairEligibility) {
    const m = entry.metrics;
    const system = `You are an FX regime classifier. Return strict JSON only with keys: pair, regime, permission, allowed_modules, risk_state, confidence, htf_context, notes_codes.
regime ∈ trend_up|trend_down|range|high_vol|event_risk.
permission ∈ long_only|short_only|both|flat.
allowed_modules can include pullback, breakout_retest, range_fade, none.
risk_state ∈ normal|elevated|extreme.
confidence must be 0..1.`;

    const user = JSON.stringify({
        pair: entry.pair,
        features: {
            session: m.sessionTag,
            spread_pips: m.spreadPips,
            spread_to_atr1h: m.spreadToAtr1h,
            atr1h_percent: m.atr1hPercent,
            trend_strength: m.trendStrength,
            chop_score: m.chopScore,
            shock_flag: m.shockFlag,
            eligible_now: entry.eligible,
            selector_score: entry.score,
        },
        constraints: {
            focus: 'capital.com forex only',
            objective: 'produce conservative regime packet for deterministic execution modules',
        },
    });

    return { system, user };
}

export async function buildForexRegimePacket(params: {
    entry: ForexPairEligibility;
    nowMs?: number;
    eventBlocked: boolean;
}): Promise<ForexRegimePacket> {
    const nowMs = Number.isFinite(params.nowMs as number) ? Number(params.nowMs) : Date.now();
    const entry = params.entry;

    let packet = fallbackPacket(entry, nowMs);

    try {
        const { system, user } = buildAiPrompt(entry);
        const raw = await callAI(system, user);

        packet = {
            pair: entry.pair,
            generatedAtMs: nowMs,
            regime: normalizeEnum(raw?.regime, REGIME_SET, packet.regime),
            permission: normalizeEnum(raw?.permission, PERMISSION_SET, packet.permission),
            allowed_modules: normalizeModules(raw?.allowed_modules),
            risk_state: normalizeEnum(raw?.risk_state, RISK_SET, packet.risk_state),
            confidence: clampConfidence(raw?.confidence),
            htf_context: {
                nearest_support: Number.isFinite(Number(raw?.htf_context?.nearest_support))
                    ? Number(raw.htf_context.nearest_support)
                    : packet.htf_context.nearest_support,
                nearest_resistance: Number.isFinite(Number(raw?.htf_context?.nearest_resistance))
                    ? Number(raw.htf_context.nearest_resistance)
                    : packet.htf_context.nearest_resistance,
                distance_to_support_atr1h: Number.isFinite(Number(raw?.htf_context?.distance_to_support_atr1h))
                    ? Number(raw.htf_context.distance_to_support_atr1h)
                    : packet.htf_context.distance_to_support_atr1h,
                distance_to_resistance_atr1h: Number.isFinite(Number(raw?.htf_context?.distance_to_resistance_atr1h))
                    ? Number(raw.htf_context.distance_to_resistance_atr1h)
                    : packet.htf_context.distance_to_resistance_atr1h,
            },
            notes_codes: Array.isArray(raw?.notes_codes)
                ? raw.notes_codes.map((x: unknown) => String(x)).filter((x: string) => x.length > 0)
                : ['AI_NOTES_EMPTY'],
        };

        packet.notes_codes = Array.from(new Set(packet.notes_codes));
        if (!packet.notes_codes.includes('AI_PACKET_OK')) packet.notes_codes.push('AI_PACKET_OK');
    } catch (err) {
        packet.notes_codes.push('AI_PACKET_FALLBACK');
    }

    return enforceHardRules(packet, { eventBlocked: params.eventBlocked });
}

export async function buildForexPacketSnapshot(params: {
    entries: ForexPairEligibility[];
    eventBlockedPairs?: Set<string>;
    nowMs?: number;
}): Promise<ForexPacketSnapshot> {
    const nowMs = Number.isFinite(params.nowMs as number) ? Number(params.nowMs) : Date.now();
    const blocked = params.eventBlockedPairs ?? new Set<string>();

    const packets = await Promise.all(
        params.entries.map((entry) =>
            buildForexRegimePacket({
                entry,
                nowMs,
                eventBlocked: blocked.has(entry.pair),
            }),
        ),
    );

    return {
        generatedAtMs: nowMs,
        packets,
    };
}
