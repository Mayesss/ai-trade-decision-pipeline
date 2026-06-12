import assert from "node:assert/strict";
import test from "node:test";

import { buildSessionStructureComposerTuneId } from "../composer/sessionStructureComposer";
import { getScalpStrategyConfig } from "../config";
import { buildScalpSessionWindows } from "../sessions";
import { createInitialScalpSessionState, deriveScalpDayKey } from "../stateMachine";
import type { ScalpCandle, ScalpStrategyConfig } from "../types";
import { sessionStructureComposerV1Strategy } from "./sessionStructureComposerV1";
import type { ScalpStrategyPhaseInput } from "./types";

function m15(tsMs: number, open: number, high: number, low: number, close: number, volume = 100): ScalpCandle {
  return [tsMs, open, high, low, close, volume];
}

function buildInput(params: {
  nowMs: number;
  tuneId: string;
  baseCandles?: ScalpCandle[];
  quotePrice?: number;
  cfgPatch?: Partial<ScalpStrategyConfig>;
}): ScalpStrategyPhaseInput {
  const cfg: ScalpStrategyConfig = {
    ...getScalpStrategyConfig(),
    ...(params.cfgPatch || {}),
    sessions: {
      ...getScalpStrategyConfig().sessions,
      ...(params.cfgPatch?.sessions || {}),
      entrySessionProfile: "berlin",
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
    venue: "capital",
    symbol: "EURUSD",
    strategyId: sessionStructureComposerV1Strategy.id,
    tuneId: params.tuneId,
    dayKey,
    nowMs: params.nowMs,
    killSwitchActive: false,
  });
  const base = params.baseCandles || [];
  const quotePrice = params.quotePrice ?? base.at(-1)?.[4] ?? 1.1;
  return {
    state,
    market: {
      symbol: "EURUSD",
      epic: "EURUSD",
      nowMs: params.nowMs,
      quote: {
        price: quotePrice,
        bid: quotePrice - 0.000025,
        offer: quotePrice + 0.000025,
        spreadAbs: 0.00005,
        spreadPips: 0.5,
        tsMs: params.nowMs,
      },
      baseTf: "M15",
      confirmTf: "M3",
      baseCandles: base,
      confirmCandles: [],
    },
    windows,
    nowMs: params.nowMs,
    cfg,
  };
}

function buildVwapPullbackFixture(nowMs: number): ScalpCandle[] {
  const step = 15 * 60_000;
  const start = nowMs - 110 * step;
  const out: ScalpCandle[] = [];
  let price = 1.098;
  for (let idx = 0; idx < 106; idx += 1) {
    const open = price;
    const close = open + 0.000015;
    out.push(m15(start + idx * step, open, close + 0.00008, open - 0.00008, close, 100));
    price = close;
  }
  out.push(m15(nowMs - 3 * step, 1.0997, 1.1002, 1.0995, 1.1001, 130));
  out.push(m15(nowMs - 2 * step, 1.1001, 1.1006, 1.0999, 1.1005, 140));
  out.push(m15(nowMs - step, 1.1003, 1.1009, 1.0997, 1.1008, 160));
  out.push(m15(nowMs, 1.1002, 1.1014, 1.0994, 1.1012, 180));
  return out;
}

test("session structure composer emits structure_level intent for VWAP pullback continuation", () => {
  const nowMs = Date.UTC(2026, 0, 5, 8, 30, 0, 0);
  const tuneId = buildSessionStructureComposerTuneId({
    contextId: "m30_session_momentum",
    levelId: "session_vwap",
    triggerId: "vwap_pullback_continuation",
    confirmationId: "m15_close_acceptance",
    managementId: "fixed_2r_time_4h",
    digest: "abcdef1234",
  });
  const input = buildInput({
    nowMs,
    tuneId,
    baseCandles: buildVwapPullbackFixture(nowMs),
    quotePrice: 1.1012,
  });
  const phase = sessionStructureComposerV1Strategy.applyPhaseDetectors(input);
  assert.equal(phase.entryIntent?.model, "structure_level");
  assert.equal(phase.entryIntent?.side, "BUY");
  assert.ok(phase.reasonCodes.includes("SESSION_COMPOSER_STRUCTURE_LEVEL_READY"));
});

test("session structure composer blocks after two trades or same-session stopped trade", () => {
  const nowMs = Date.UTC(2026, 0, 5, 8, 30, 0, 0);
  const tuneId = buildSessionStructureComposerTuneId({
    contextId: "m30_session_momentum",
    levelId: "session_vwap",
    triggerId: "vwap_pullback_continuation",
    confirmationId: "m15_close_acceptance",
    managementId: "fixed_2r_time_4h",
    digest: "abcdef1234",
  });
  const twoTrades = buildInput({ nowMs, tuneId });
  twoTrades.state.stats.tradesPlaced = 2;
  assert.ok(
    sessionStructureComposerV1Strategy
      .applyPhaseDetectors(twoTrades)
      .reasonCodes.includes("SESSION_COMPOSER_MAX_TWO_TRADES_PER_SYMBOL_DAY"),
  );

  const stopped = buildInput({ nowMs, tuneId });
  stopped.state.stats.consecutiveLosses = 1;
  stopped.state.stats.lastExitAtMs = stopped.windows.raidStartMs + 60_000;
  assert.ok(
    sessionStructureComposerV1Strategy
      .applyPhaseDetectors(stopped)
      .reasonCodes.includes("SESSION_COMPOSER_SAME_SESSION_REENTRY_AFTER_STOP_BLOCKED"),
  );
});
