import { deriveAdaptiveFeatureContext } from '../adaptive/features';
import { selectAdaptivePatternArm } from '../adaptive/snapshot';
import {
  ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID,
  type ScalpAdaptiveSnapshotCatalog,
  type ScalpAdaptiveSelectionResult,
} from '../adaptive/types';
import { anchoredVwapReversionM15M3Strategy } from './anchoredVwapReversionM15M3';
import { basisDislocationReversionProxyM15M3Strategy } from './basisDislocationReversionProxyM15M3';
import { compressionBreakoutPullbackM15M3Strategy } from './compressionBreakoutPullbackM15M3';
import { fundingOiExhaustionProxyM15M3Strategy } from './fundingOiExhaustionProxyM15M3';
import { hssIctM15M3GuardedStrategy } from './hssIctM15M3Guarded';
import { pdhPdlReclaimM15M3Strategy } from './pdhPdlReclaimM15M3';
import {
  REGIME_PULLBACK_M15_M3_STRATEGY_ID,
  regimePullbackM15M3Strategy,
} from './regimePullbackM15M3';
import { relativeValueSpreadProxyM15M3Strategy } from './relativeValueSpreadProxyM15M3';
import { sessionSeasonalityBiasM15M3Strategy } from './sessionSeasonalityBiasM15M3';
import { trendDayReaccelerationM15M3Strategy } from './trendDayReaccelerationM15M3';
import type { ScalpStrategyDefinition, ScalpStrategyPhaseInput, ScalpStrategyPhaseOutput } from './types';

export { ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID };

function dedupeReasonCodes(codes: string[]): string[] {
  return Array.from(
    new Set(
      codes
        .map((code) => String(code || '').trim().toUpperCase())
        .filter((code) => code.length > 0),
    ),
  );
}

function deepCloneState<T>(state: T): T {
  return JSON.parse(JSON.stringify(state)) as T;
}

function parseCatalog(raw: unknown): ScalpAdaptiveSnapshotCatalog | null {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const obj = raw as Record<string, unknown>;
  if (!Array.isArray(obj.patternArms)) return null;
  const minConfidence = Number(obj.minConfidence);
  const edgeScoreThreshold = Number(obj.edgeScoreThreshold);
  const minSupport = Math.floor(Number(obj.minSupport));
  const generatedAtMs = Math.floor(Number(obj.generatedAtMs));
  const incumbentArmRaw =
    obj.incumbentArm && typeof obj.incumbentArm === 'object' && !Array.isArray(obj.incumbentArm)
      ? (obj.incumbentArm as Record<string, unknown>)
      : null;
  if (!incumbentArmRaw) return null;
  const incumbentStrategyId = String(incumbentArmRaw.strategyId || '').trim().toLowerCase();
  if (!incumbentStrategyId) return null;
  return {
    version: 1,
    minConfidence: Number.isFinite(minConfidence) ? minConfidence : 0.6,
    edgeScoreThreshold: Number.isFinite(edgeScoreThreshold) ? edgeScoreThreshold : 0.08,
    minSupport: Number.isFinite(minSupport) && minSupport > 0 ? minSupport : 30,
    generatedAtMs: Number.isFinite(generatedAtMs) && generatedAtMs > 0 ? generatedAtMs : Date.now(),
    patternArms: obj.patternArms as ScalpAdaptiveSnapshotCatalog['patternArms'],
    incumbentArm: {
      armId: String(incumbentArmRaw.armId || 'incumbent_arm').trim() || 'incumbent_arm',
      strategyId: incumbentStrategyId,
      strategyLabel: String(incumbentArmRaw.strategyLabel || incumbentStrategyId),
    },
  };
}

function resolveIncumbentStrategyId(input: ScalpStrategyPhaseInput, catalog: ScalpAdaptiveSnapshotCatalog | null): string {
  const cfgAdaptive = input.cfg.adaptive;
  const incumbentFromCfg = String(cfgAdaptive?.incumbentArm?.strategyId || '').trim().toLowerCase();
  const incumbentFromCatalog = String(catalog?.incumbentArm?.strategyId || '').trim().toLowerCase();
  const candidate = incumbentFromCfg || incumbentFromCatalog || REGIME_PULLBACK_M15_M3_STRATEGY_ID;
  if (!candidate || candidate === ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID) {
    return REGIME_PULLBACK_M15_M3_STRATEGY_ID;
  }
  return candidate;
}

const INCUMBENT_STRATEGY_MAP: Record<string, ScalpStrategyDefinition> = Object.freeze({
  [regimePullbackM15M3Strategy.id]: regimePullbackM15M3Strategy,
  [compressionBreakoutPullbackM15M3Strategy.id]: compressionBreakoutPullbackM15M3Strategy,
  [trendDayReaccelerationM15M3Strategy.id]: trendDayReaccelerationM15M3Strategy,
  [anchoredVwapReversionM15M3Strategy.id]: anchoredVwapReversionM15M3Strategy,
  [fundingOiExhaustionProxyM15M3Strategy.id]: fundingOiExhaustionProxyM15M3Strategy,
  [basisDislocationReversionProxyM15M3Strategy.id]: basisDislocationReversionProxyM15M3Strategy,
  [relativeValueSpreadProxyM15M3Strategy.id]: relativeValueSpreadProxyM15M3Strategy,
  [sessionSeasonalityBiasM15M3Strategy.id]: sessionSeasonalityBiasM15M3Strategy,
  [pdhPdlReclaimM15M3Strategy.id]: pdhPdlReclaimM15M3Strategy,
  [hssIctM15M3GuardedStrategy.id]: hssIctM15M3GuardedStrategy,
});

function resolveIncumbentStrategyDefinition(strategyId: string): {
  strategy: ScalpStrategyDefinition;
  fallbackUsed: boolean;
} {
  const normalized = String(strategyId || '').trim().toLowerCase();
  const found = INCUMBENT_STRATEGY_MAP[normalized] || null;
  if (found && found.id !== ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID) {
    return { strategy: found, fallbackUsed: false };
  }
  return { strategy: regimePullbackM15M3Strategy, fallbackUsed: true };
}

function evaluateIncumbentPhase(
  strategy: ScalpStrategyDefinition,
  input: ScalpStrategyPhaseInput,
): ScalpStrategyPhaseOutput {
  return strategy.applyPhaseDetectors({
    ...input,
    state: deepCloneState(input.state),
  });
}

function incumbentConfidence(phase: ScalpStrategyPhaseOutput): number {
  const hasEntry = Boolean(phase.entryIntent);
  if (hasEntry) return 0.72;
  const hasSetupSignal = phase.reasonCodes.some((code) => code.includes('SETUP_') || code.includes('CONFIRM_'));
  if (hasSetupSignal) return 0.58;
  return 0.42;
}

function selectArm(params: {
  patternSelection: ScalpAdaptiveSelectionResult;
  incumbentConfidence: number;
  incumbentArmId: string;
  minConfidence: number;
}): {
  armType: 'pattern' | 'incumbent' | 'none';
  armId: string | null;
  confidence: number;
  skipReason: string | null;
} {
  const patternConfidence = params.patternSelection.confidence;
  const incumbentConfidence = params.incumbentConfidence;
  const bestConfidence = Math.max(patternConfidence, incumbentConfidence);
  if (bestConfidence < params.minConfidence) {
    return {
      armType: 'none',
      armId: null,
      confidence: bestConfidence,
      skipReason: 'below_min_confidence',
    };
  }
  if (incumbentConfidence >= patternConfidence) {
    return {
      armType: 'incumbent',
      armId: params.incumbentArmId,
      confidence: incumbentConfidence,
      skipReason: null,
    };
  }
  return {
    armType: 'pattern',
    armId: params.patternSelection.selectedArmId,
    confidence: patternConfidence,
    skipReason: null,
  };
}

export const adaptiveMetaSelectorM15M3Strategy: ScalpStrategyDefinition = {
  id: ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID,
  shortName: 'Adaptive Meta',
  longName: 'Adaptive Meta Selector M15/M3',
  preferredBaseTf: 'M15',
  preferredConfirmTf: 'M3',
  applyPhaseDetectors(input: ScalpStrategyPhaseInput): ScalpStrategyPhaseOutput {
    const catalog = parseCatalog(input.cfg.adaptive?.catalog);
    const incumbentStrategyId = resolveIncumbentStrategyId(input, catalog);
    const incumbentStrategy = resolveIncumbentStrategyDefinition(incumbentStrategyId);
    const incumbentPhase = evaluateIncumbentPhase(incumbentStrategy.strategy, input);
    const context = deriveAdaptiveFeatureContext({
      baseCandles: input.market.baseCandles,
      confirmCandles: input.market.confirmCandles,
      nowMs: input.nowMs,
      entrySessionProfile: input.cfg.sessions.entrySessionProfile,
    });
    const patternSelection = selectAdaptivePatternArm(context, catalog);
    const minConfidenceRaw =
      Number(input.cfg.adaptive?.thresholds?.minConfidence) ||
      Number(input.cfg.adaptive?.minConfidence) ||
      Number(catalog?.minConfidence);
    const minConfidence = Number.isFinite(minConfidenceRaw)
      ? Math.max(0, Math.min(1, minConfidenceRaw))
      : 0.6;
    const incumbentArmId = catalog?.incumbentArm?.armId || 'incumbent_arm';
    const incumbentConf = incumbentConfidence(incumbentPhase);
    const selected = selectArm({
      patternSelection,
      incumbentConfidence: incumbentConf,
      incumbentArmId,
      minConfidence,
    });

    let entryIntent = null;
    const reasonCodes = [...incumbentPhase.reasonCodes];
    if (incumbentStrategy.fallbackUsed && incumbentStrategyId !== incumbentStrategy.strategy.id) {
      reasonCodes.push('ADAPTIVE_INCUMBENT_FALLBACK_USED');
    }
    if (selected.armType === 'incumbent') {
      entryIntent = incumbentPhase.entryIntent || null;
      reasonCodes.push('ADAPTIVE_INCUMBENT_SELECTED', `ADAPTIVE_ARM_${String(selected.armId || incumbentArmId).toUpperCase()}`);
      if (!entryIntent) {
        reasonCodes.push('ADAPTIVE_INCUMBENT_NO_ENTRY_INTENT');
      }
    } else if (selected.armType === 'pattern') {
      entryIntent = incumbentPhase.entryIntent || null;
      reasonCodes.push('ADAPTIVE_PATTERN_SELECTED', `ADAPTIVE_ARM_${String(selected.armId || 'pattern').toUpperCase()}`);
      if (!entryIntent) {
        reasonCodes.push('ADAPTIVE_PATTERN_NO_ENTRY_INTENT');
      }
    } else {
      reasonCodes.push(
        'ADAPTIVE_NO_EDGE_SKIP',
        selected.skipReason ? `ADAPTIVE_SKIP_${selected.skipReason.toUpperCase()}` : 'ADAPTIVE_SKIP_UNSPECIFIED',
      );
      entryIntent = null;
    }

    const snapshotId = String(input.cfg.adaptive?.snapshotId || '').trim() || null;
    return {
      state: incumbentPhase.state,
      reasonCodes: dedupeReasonCodes(reasonCodes),
      entryIntent,
      meta: {
        adaptiveDecision: {
          selectedArmType: selected.armType,
          selectedArmId: selected.armId,
          confidence: selected.confidence,
          skipReason: selected.skipReason,
          snapshotId,
          incumbentStrategyId,
          incumbentExecutedStrategyId: incumbentStrategy.strategy.id,
          incumbentConfidence: incumbentConf,
          patternConfidence: patternSelection.confidence,
          featureHash: context.featureHash,
          tokenMap: context.tokenMap,
        },
      },
    };
  },
};
