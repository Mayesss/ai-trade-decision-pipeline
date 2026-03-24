import assert from "node:assert/strict";
import test from "node:test";

import { getScalpStrategyConfig } from "../config";
import { deriveAdaptiveFeatureContext } from "../adaptive/features";
import { buildScalpSessionWindows } from "../sessions";
import { createInitialScalpSessionState, deriveScalpDayKey } from "../stateMachine";
import type { ScalpCandle, ScalpStrategyConfig } from "../types";
import {
  ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID,
  adaptiveMetaSelectorM15M3Strategy,
} from "./adaptiveMetaSelectorM15M3";

function buildCandles(params: {
  endTsMs: number;
  bars: number;
  tfMinutes: number;
  startPrice: number;
}): ScalpCandle[] {
  const out: ScalpCandle[] = [];
  let price = params.startPrice;
  for (let i = params.bars - 1; i >= 0; i -= 1) {
    const tsMs = params.endTsMs - i * params.tfMinutes * 60_000;
    const drift = Math.sin((params.bars - i) / 8) * 0.0008;
    const open = price;
    const close = Math.max(0.01, open + drift);
    const high = Math.max(open, close) + 0.0005;
    const low = Math.min(open, close) - 0.0005;
    out.push([tsMs, open, high, low, close, 120]);
    price = close;
  }
  return out;
}

function buildInput(params: {
  nowMs: number;
  cfgPatch?: Partial<ScalpStrategyConfig>;
}): {
  cfg: ScalpStrategyConfig;
  state: ReturnType<typeof createInitialScalpSessionState>;
  windows: ReturnType<typeof buildScalpSessionWindows>;
  market: {
    symbol: string;
    epic: string;
    nowMs: number;
    quote: {
      price: number;
      bid: number;
      offer: number;
      spreadAbs: number;
      spreadPips: number;
      tsMs: number;
    };
    baseTf: "M15";
    confirmTf: "M3";
    baseCandles: ScalpCandle[];
    confirmCandles: ScalpCandle[];
  };
} {
  const baseCfg = getScalpStrategyConfig();
  const cfg: ScalpStrategyConfig = {
    ...baseCfg,
    ...params.cfgPatch,
    sessions: {
      ...baseCfg.sessions,
      ...(params.cfgPatch?.sessions || {}),
      entrySessionProfile: "berlin",
      blockedBerlinEntryHours: [],
    },
    timeframes: {
      ...baseCfg.timeframes,
      asiaBase: "M15",
      confirm: "M3",
    },
    adaptive: {
      ...baseCfg.adaptive!,
      enabled: true,
      ...(params.cfgPatch?.adaptive || {}),
    },
  };

  const dayKey = deriveScalpDayKey(params.nowMs, cfg.sessions.clockMode);
  const windows = buildScalpSessionWindows({
    dayKey,
    clockMode: cfg.sessions.clockMode,
    asiaWindowLocal: cfg.sessions.asiaWindowLocal,
    raidWindowLocal: cfg.sessions.raidWindowLocal,
  });
  const state = createInitialScalpSessionState({
    symbol: "EURUSD",
    strategyId: ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID,
    dayKey,
    nowMs: params.nowMs,
    killSwitchActive: false,
  });
  const baseCandles = buildCandles({
    endTsMs: params.nowMs,
    bars: 280,
    tfMinutes: 15,
    startPrice: 1.1,
  });
  const confirmCandles = buildCandles({
    endTsMs: params.nowMs,
    bars: 360,
    tfMinutes: 3,
    startPrice: 1.1,
  });

  return {
    cfg,
    state,
    windows,
    market: {
      symbol: "EURUSD",
      epic: "REPLAY:EURUSD",
      nowMs: params.nowMs,
      quote: {
        price: 1.1,
        bid: 1.0999,
        offer: 1.1001,
        spreadAbs: 0.0002,
        spreadPips: 2,
        tsMs: params.nowMs,
      },
      baseTf: "M15",
      confirmTf: "M3",
      baseCandles,
      confirmCandles,
    },
  };
}

test("adaptive selector picks pattern arm when pattern confidence dominates", () => {
  const nowMs = Date.UTC(2026, 2, 10, 9, 0, 0);
  const prepared = buildInput({ nowMs });
  const context = deriveAdaptiveFeatureContext({
    baseCandles: prepared.market.baseCandles,
    confirmCandles: prepared.market.confirmCandles,
    nowMs,
    entrySessionProfile: prepared.cfg.sessions.entrySessionProfile,
  });
  prepared.cfg.adaptive = {
    ...prepared.cfg.adaptive!,
    snapshotId: "snap_pattern",
    thresholds: { minConfidence: 0.6 },
    catalog: {
      version: 1,
      minConfidence: 0.6,
      minSupport: 30,
      edgeScoreThreshold: 0.08,
      generatedAtMs: nowMs,
      incumbentArm: {
        armId: "incumbent_arm",
        strategyId: "regime_pullback_m15_m3",
        strategyLabel: "Regime Pullback",
      },
      patternArms: [
        {
          armId: "pattern_001",
          ngram: context.tokens.slice(0, 2),
          support: 40,
          winRate: 0.64,
          meanProxyR: 0.7,
          edgeLocal: 0.12,
          edgeSession: 0.1,
          edgeGlobal: 0.07,
          score: 0.11,
          confidence: 0.92,
        },
      ],
    },
  };

  const phase = adaptiveMetaSelectorM15M3Strategy.applyPhaseDetectors({
    state: prepared.state,
    market: prepared.market,
    windows: prepared.windows,
    nowMs,
    cfg: prepared.cfg,
  });

  assert.ok(phase.reasonCodes.includes("ADAPTIVE_PATTERN_SELECTED"));
  const adaptiveDecision = (phase.meta?.adaptiveDecision || {}) as Record<string, unknown>;
  assert.equal(adaptiveDecision.selectedArmType, "pattern");
  assert.equal(adaptiveDecision.snapshotId, "snap_pattern");
});

test("adaptive selector breaks confidence tie in favor of incumbent arm", () => {
  const nowMs = Date.UTC(2026, 2, 10, 9, 0, 0);
  const prepared = buildInput({ nowMs });
  const context = deriveAdaptiveFeatureContext({
    baseCandles: prepared.market.baseCandles,
    confirmCandles: prepared.market.confirmCandles,
    nowMs,
    entrySessionProfile: prepared.cfg.sessions.entrySessionProfile,
  });
  prepared.cfg.adaptive = {
    ...prepared.cfg.adaptive!,
    snapshotId: "snap_tie",
    thresholds: { minConfidence: 0.4 },
    catalog: {
      version: 1,
      minConfidence: 0.4,
      minSupport: 30,
      edgeScoreThreshold: 0.08,
      generatedAtMs: nowMs,
      incumbentArm: {
        armId: "incumbent_arm",
        strategyId: "regime_pullback_m15_m3",
        strategyLabel: "Regime Pullback",
      },
      patternArms: [
        {
          armId: "pattern_001",
          ngram: context.tokens.slice(0, 2),
          support: 35,
          winRate: 0.55,
          meanProxyR: 0.2,
          edgeLocal: 0.03,
          edgeSession: 0.02,
          edgeGlobal: 0.01,
          score: 0.02,
          confidence: 0.42,
        },
      ],
    },
  };

  const phase = adaptiveMetaSelectorM15M3Strategy.applyPhaseDetectors({
    state: prepared.state,
    market: prepared.market,
    windows: prepared.windows,
    nowMs,
    cfg: prepared.cfg,
  });

  const adaptiveDecision = (phase.meta?.adaptiveDecision || {}) as Record<string, unknown>;
  assert.equal(adaptiveDecision.selectedArmType, "incumbent");
  assert.ok(phase.reasonCodes.includes("ADAPTIVE_INCUMBENT_SELECTED"));
});

test("adaptive selector returns explicit no-edge skip when confidence is below threshold", () => {
  const nowMs = Date.UTC(2026, 2, 10, 9, 0, 0);
  const prepared = buildInput({ nowMs });
  prepared.cfg.adaptive = {
    ...prepared.cfg.adaptive!,
    snapshotId: "snap_skip",
    thresholds: { minConfidence: 0.95 },
    catalog: {
      version: 1,
      minConfidence: 0.95,
      minSupport: 30,
      edgeScoreThreshold: 0.08,
      generatedAtMs: nowMs,
      incumbentArm: {
        armId: "incumbent_arm",
        strategyId: "regime_pullback_m15_m3",
        strategyLabel: "Regime Pullback",
      },
      patternArms: [],
    },
  };

  const phase = adaptiveMetaSelectorM15M3Strategy.applyPhaseDetectors({
    state: prepared.state,
    market: prepared.market,
    windows: prepared.windows,
    nowMs,
    cfg: prepared.cfg,
  });

  const adaptiveDecision = (phase.meta?.adaptiveDecision || {}) as Record<string, unknown>;
  assert.equal(adaptiveDecision.selectedArmType, "none");
  assert.ok(phase.reasonCodes.includes("ADAPTIVE_NO_EDGE_SKIP"));
  assert.equal(phase.entryIntent, null);
});
