import assert from "node:assert/strict";
import test from "node:test";

import { getScalpStrategyConfig } from "../config";
import { buildScalpSessionWindows } from "../sessions";
import { createInitialScalpSessionState, deriveScalpDayKey } from "../stateMachine";
import type { ScalpCandle, ScalpEntrySessionProfile, ScalpStrategyConfig } from "../types";
import { modelGuidedComposerV2Strategy } from "./modelGuidedComposerV2";
import type { ScalpStrategyPhaseInput } from "./types";

/* ------------------------------------------------------------------ */
/*  Timestamp helpers — one "inside" and one "outside" per session     */
/* ------------------------------------------------------------------ */

// Berlin: 08:00–12:00 Europe/Berlin (CET = UTC+1 in winter)
const BERLIN_INSIDE = Date.UTC(2026, 0, 5, 8, 30, 0, 0); // 09:30 CET
const BERLIN_OUTSIDE = Date.UTC(2026, 0, 5, 12, 30, 0, 0); // 13:30 CET

// Tokyo: 09:00–13:00 Asia/Tokyo (JST = UTC+9)
const TOKYO_INSIDE = Date.UTC(2026, 0, 5, 1, 30, 0, 0); // 10:30 JST
const TOKYO_OUTSIDE = Date.UTC(2026, 0, 5, 5, 0, 0, 0); // 14:00 JST

// New York: 08:00–12:00 America/New_York (EST = UTC-5)
const NEWYORK_INSIDE = Date.UTC(2026, 0, 5, 14, 0, 0, 0); // 09:00 EST
const NEWYORK_OUTSIDE = Date.UTC(2026, 0, 5, 18, 0, 0, 0); // 13:00 EST

// Sydney: 08:00–12:00 Australia/Sydney (AEDT = UTC+11)
const SYDNEY_INSIDE = Date.UTC(2026, 0, 4, 22, 0, 0, 0); // 09:00 AEDT
const SYDNEY_OUTSIDE = Date.UTC(2026, 0, 5, 2, 0, 0, 0); // 13:00 AEDT

const SESSIONS: ScalpEntrySessionProfile[] = ["tokyo", "berlin", "newyork", "sydney"];

const INSIDE_TS: Record<ScalpEntrySessionProfile, number> = {
  tokyo: TOKYO_INSIDE,
  berlin: BERLIN_INSIDE,
  newyork: NEWYORK_INSIDE,
  sydney: SYDNEY_INSIDE,
};

const OUTSIDE_TS: Record<ScalpEntrySessionProfile, number> = {
  tokyo: TOKYO_OUTSIDE,
  berlin: BERLIN_OUTSIDE,
  newyork: NEWYORK_OUTSIDE,
  sydney: SYDNEY_OUTSIDE,
};

/* ------------------------------------------------------------------ */
/*  All composer arms (base arm × timeframe variant)                   */
/* ------------------------------------------------------------------ */

const BASE_ARMS = [
  "regime",
  "compress",
  "orb",
  "auction",
  "trend",
  "vwap",
  "basis",
  "relative",
  "season",
  "reclaim",
  "hss",
  "adaptive",
];

const TF_VARIANTS = ["m15_m3", "m5_m1", "m5_m3"];

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function buildLinearCandles(params: {
  startTsMs: number;
  count: number;
  stepMs: number;
  startPrice: number;
  driftPerBar: number;
  rangeAbs: number;
  volume: number;
}): ScalpCandle[] {
  const out: ScalpCandle[] = [];
  let price = params.startPrice;
  for (let i = 0; i < params.count; i += 1) {
    const ts = params.startTsMs + i * params.stepMs;
    const open = price;
    const close = open + params.driftPerBar;
    const high = Math.max(open, close) + params.rangeAbs;
    const low = Math.min(open, close) - params.rangeAbs;
    out.push([ts, open, high, low, close, params.volume]);
    price = close;
  }
  return out;
}

function buildInput(params: {
  session: ScalpEntrySessionProfile;
  nowMs: number;
  armId: string;
}): ScalpStrategyPhaseInput {
  const cfg: ScalpStrategyConfig = {
    ...getScalpStrategyConfig(),
    sessions: {
      ...getScalpStrategyConfig().sessions,
      entrySessionProfile: params.session,
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
    symbol: "BTCUSDT",
    strategyId: "model_guided_composer_v2",
    tuneId: `mdl_${params.armId}_abc1234567`,
    dayKey,
    nowMs: params.nowMs,
    killSwitchActive: false,
  });
  const base = buildLinearCandles({
    startTsMs: params.nowMs - 220 * 15 * 60_000,
    count: 220,
    stepMs: 15 * 60_000,
    startPrice: 100,
    driftPerBar: 0.02,
    rangeAbs: 0.05,
    volume: 120,
  });
  const confirm = buildLinearCandles({
    startTsMs: params.nowMs - 220 * 3 * 60_000,
    count: 220,
    stepMs: 3 * 60_000,
    startPrice: 100,
    driftPerBar: 0.001,
    rangeAbs: 0.015,
    volume: 80,
  });
  return {
    state,
    market: {
      symbol: "BTCUSDT",
      epic: "REPLAY:BTCUSDT",
      nowMs: params.nowMs,
      quote: {
        price: 100,
        bid: 99.99,
        offer: 100.01,
        spreadAbs: 0.02,
        spreadPips: 2,
        tsMs: params.nowMs,
      },
      baseTf: "M15",
      confirmTf: "M3",
      baseCandles: base,
      confirmCandles: confirm,
    },
    windows,
    nowMs: params.nowMs,
    cfg,
  };
}

/* ------------------------------------------------------------------ */
/*  Tests: composer gates entries outside session window                */
/* ------------------------------------------------------------------ */

for (const session of SESSIONS) {
  for (const baseArm of BASE_ARMS) {
    for (const tf of TF_VARIANTS) {
      const armId = `${baseArm}_${tf}`;

      test(`composer session gate: ${armId} × ${session} — blocks outside session window`, () => {
        const nowMs = OUTSIDE_TS[session];
        const input = buildInput({ session, nowMs, armId });
        const phase = modelGuidedComposerV2Strategy.applyPhaseDetectors(input);

        assert.equal(
          phase.state.state,
          "IDLE",
          `Expected IDLE outside ${session} window for ${armId}`,
        );
        assert.ok(
          phase.reasonCodes.includes("SESSION_FILTER_OUTSIDE_ENTRY_PROFILE"),
          `Missing SESSION_FILTER_OUTSIDE_ENTRY_PROFILE for ${armId} × ${session}`,
        );
        assert.ok(
          phase.reasonCodes.includes(`SESSION_PROFILE_${session.toUpperCase()}`),
          `Missing SESSION_PROFILE_${session.toUpperCase()} reason code`,
        );
        assert.equal(
          phase.entryIntent,
          null,
          `entryIntent must be null outside session for ${armId} × ${session}`,
        );
      });

      test(`composer session gate: ${armId} × ${session} — allows inside session window`, () => {
        const nowMs = INSIDE_TS[session];
        const input = buildInput({ session, nowMs, armId });
        const phase = modelGuidedComposerV2Strategy.applyPhaseDetectors(input);

        assert.ok(
          !phase.reasonCodes.includes("SESSION_FILTER_OUTSIDE_ENTRY_PROFILE"),
          `Should NOT be gated inside ${session} window for ${armId}`,
        );
        assert.ok(
          phase.reasonCodes.includes("MODEL_GUIDED_COMPOSER_ACTIVE"),
          `Missing MODEL_GUIDED_COMPOSER_ACTIVE inside session for ${armId}`,
        );
      });
    }
  }
}

/* ------------------------------------------------------------------ */
/*  Cross-session isolation: verify different sessions produce          */
/*  different gating at the same UTC timestamp                         */
/* ------------------------------------------------------------------ */

test("composer session gate: same UTC timestamp is inside one session but outside another", () => {
  // 09:30 Berlin CET (UTC 08:30) — inside Berlin, outside Tokyo (18:30 JST — past 13:00)
  const utcTs = Date.UTC(2026, 0, 5, 8, 30, 0, 0);
  const armId = "relative_m5_m1";

  const berlinInput = buildInput({ session: "berlin", nowMs: utcTs, armId });
  const berlinPhase = modelGuidedComposerV2Strategy.applyPhaseDetectors(berlinInput);
  assert.ok(
    !berlinPhase.reasonCodes.includes("SESSION_FILTER_OUTSIDE_ENTRY_PROFILE"),
    "Berlin should be inside session at this UTC time",
  );

  const tokyoInput = buildInput({ session: "tokyo", nowMs: utcTs, armId });
  const tokyoPhase = modelGuidedComposerV2Strategy.applyPhaseDetectors(tokyoInput);
  assert.ok(
    tokyoPhase.reasonCodes.includes("SESSION_FILTER_OUTSIDE_ENTRY_PROFILE"),
    "Tokyo should be outside session at this UTC time",
  );
});
