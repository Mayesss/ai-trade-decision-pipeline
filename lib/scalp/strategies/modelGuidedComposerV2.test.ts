import assert from "node:assert/strict";
import test from "node:test";

import { buildModelGuidedComposerTuneId } from "../../scalp-v2/composerExecution";
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

// Pacific: 10:00–14:00 America/Los_Angeles (PST = UTC-8)
const PACIFIC_INSIDE = Date.UTC(2026, 0, 5, 19, 0, 0, 0); // 11:00 PST
const PACIFIC_OUTSIDE = Date.UTC(2026, 0, 5, 23, 0, 0, 0); // 15:00 PST

// Sydney: 08:00–12:00 Australia/Sydney (AEDT = UTC+11)
const SYDNEY_INSIDE = Date.UTC(2026, 0, 4, 22, 0, 0, 0); // 09:00 AEDT
const SYDNEY_OUTSIDE = Date.UTC(2026, 0, 5, 2, 0, 0, 0); // 13:00 AEDT

const SESSIONS: ScalpEntrySessionProfile[] = ["tokyo", "berlin", "newyork", "pacific", "sydney"];

const INSIDE_TS: Record<ScalpEntrySessionProfile, number> = {
  tokyo: TOKYO_INSIDE,
  berlin: BERLIN_INSIDE,
  newyork: NEWYORK_INSIDE,
  pacific: PACIFIC_INSIDE,
  sydney: SYDNEY_INSIDE,
};

const OUTSIDE_TS: Record<ScalpEntrySessionProfile, number> = {
  tokyo: TOKYO_OUTSIDE,
  berlin: BERLIN_OUTSIDE,
  newyork: NEWYORK_OUTSIDE,
  pacific: PACIFIC_OUTSIDE,
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
  tuneId?: string;
  symbol?: string;
  baseTf?: "M15" | "M5";
  confirmTf?: "M3" | "M1";
  baseCandles?: ScalpCandle[];
  confirmCandles?: ScalpCandle[];
  quotePrice?: number;
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
    symbol: params.symbol || "BTCUSDT",
    strategyId: "model_guided_composer_v2",
    tuneId: params.tuneId || `mdl_${params.armId}_abc1234567`,
    dayKey,
    nowMs: params.nowMs,
    killSwitchActive: false,
  });
  const baseTf = params.baseTf || "M15";
  const confirmTf = params.confirmTf || "M3";
  const base =
    params.baseCandles ||
    buildLinearCandles({
      startTsMs: params.nowMs - 220 * 15 * 60_000,
      count: 220,
      stepMs: 15 * 60_000,
      startPrice: 100,
      driftPerBar: 0.02,
      rangeAbs: 0.05,
      volume: 120,
    });
  const confirm =
    params.confirmCandles ||
    buildLinearCandles({
      startTsMs: params.nowMs - 220 * 3 * 60_000,
      count: 220,
      stepMs: 3 * 60_000,
      startPrice: 100,
      driftPerBar: 0.001,
      rangeAbs: 0.015,
      volume: 80,
    });
  const quotePrice =
    params.quotePrice ??
    (confirm.at(-1)?.[4] || base.at(-1)?.[4] || 100);
  return {
    state,
    market: {
      symbol: params.symbol || "BTCUSDT",
      epic: `REPLAY:${params.symbol || "BTCUSDT"}`,
      nowMs: params.nowMs,
      quote: {
        price: quotePrice,
        bid: quotePrice - 0.0001,
        offer: quotePrice + 0.0001,
        spreadAbs: 0.0002,
        spreadPips: 2,
        tsMs: params.nowMs,
      },
      baseTf,
      confirmTf,
      baseCandles: base,
      confirmCandles: confirm,
    },
    windows,
    nowMs: params.nowMs,
    cfg,
  };
}

function prependLowVolHistory(
  candles: ScalpCandle[],
  stepMs: number,
  bars: number,
): ScalpCandle[] {
  if (!candles.length || bars <= 0) return candles.slice();
  const first = candles[0] as ScalpCandle;
  const out: ScalpCandle[] = [];
  let price = first[1] - bars * 0.00002;
  for (let i = bars; i >= 1; i -= 1) {
    const ts = first[0] - i * stepMs;
    const open = price;
    const close = open + 0.00002;
    const high = Math.max(open, close) + 0.00006;
    const low = Math.min(open, close) - 0.00006;
    out.push([ts, open, high, low, close, first[5]]);
    price = close;
  }
  return [...out, ...candles];
}

function buildOrbEntryFixture(): {
  nowMs: number;
  baseCandles: ScalpCandle[];
  confirmCandles: ScalpCandle[];
  quotePrice: number;
} {
  const nowMs = Date.UTC(2026, 0, 6, 7, 32, 0, 0);
  const baseCore: ScalpCandle[] = [
    [Date.UTC(2026, 0, 6, 5, 55, 0, 0), 1.0989, 1.0993, 1.0987, 1.0991, 100],
    [Date.UTC(2026, 0, 6, 6, 0, 0, 0), 1.0991, 1.0995, 1.0989, 1.0992, 100],
    [Date.UTC(2026, 0, 6, 6, 5, 0, 0), 1.0992, 1.0996, 1.099, 1.0993, 100],
    [Date.UTC(2026, 0, 6, 6, 10, 0, 0), 1.0993, 1.0997, 1.0991, 1.09935, 100],
    [Date.UTC(2026, 0, 6, 6, 15, 0, 0), 1.09935, 1.09975, 1.09915, 1.0994, 100],
    [Date.UTC(2026, 0, 6, 6, 20, 0, 0), 1.0994, 1.0998, 1.0992, 1.09945, 100],
    [Date.UTC(2026, 0, 6, 6, 25, 0, 0), 1.09945, 1.09985, 1.09925, 1.0995, 100],
    [Date.UTC(2026, 0, 6, 6, 30, 0, 0), 1.0995, 1.0999, 1.0993, 1.09955, 100],
    [Date.UTC(2026, 0, 6, 6, 35, 0, 0), 1.09955, 1.09995, 1.09935, 1.0996, 100],
    [Date.UTC(2026, 0, 6, 6, 40, 0, 0), 1.0996, 1.1, 1.0994, 1.09965, 100],
    [Date.UTC(2026, 0, 6, 6, 45, 0, 0), 1.09965, 1.10005, 1.09945, 1.0997, 100],
    [Date.UTC(2026, 0, 6, 6, 50, 0, 0), 1.0997, 1.1001, 1.0995, 1.09975, 100],
    [Date.UTC(2026, 0, 6, 6, 55, 0, 0), 1.09975, 1.10015, 1.09955, 1.0998, 100],
    [Date.UTC(2026, 0, 6, 7, 0, 0, 0), 1.09995, 1.10025, 1.09985, 1.10005, 100],
    [Date.UTC(2026, 0, 6, 7, 5, 0, 0), 1.10005, 1.1003, 1.09995, 1.10015, 100],
    [Date.UTC(2026, 0, 6, 7, 10, 0, 0), 1.10015, 1.10035, 1.1, 1.1002, 100],
    [Date.UTC(2026, 0, 6, 7, 15, 0, 0), 1.1002, 1.1003, 1.10008, 1.10022, 100],
    [Date.UTC(2026, 0, 6, 7, 20, 0, 0), 1.10022, 1.10135, 1.10018, 1.10122, 100],
    [Date.UTC(2026, 0, 6, 7, 25, 0, 0), 1.10122, 1.10128, 1.101, 1.10112, 100],
    [Date.UTC(2026, 0, 6, 7, 30, 0, 0), 1.10112, 1.1013, 1.10102, 1.1012, 100],
  ];
  const confirmCore: ScalpCandle[] = [
    [Date.UTC(2026, 0, 6, 7, 21, 0, 0), 1.10122, 1.10125, 1.10105, 1.1011, 50],
    [Date.UTC(2026, 0, 6, 7, 22, 0, 0), 1.1011, 1.10112, 1.10065, 1.10078, 50],
    [Date.UTC(2026, 0, 6, 7, 23, 0, 0), 1.10078, 1.10092, 1.10028, 1.10042, 50],
    [Date.UTC(2026, 0, 6, 7, 24, 0, 0), 1.10042, 1.10058, 1.10018, 1.10048, 50],
    [Date.UTC(2026, 0, 6, 7, 25, 0, 0), 1.10048, 1.10088, 1.10036, 1.10072, 50],
    [Date.UTC(2026, 0, 6, 7, 26, 0, 0), 1.10072, 1.10102, 1.1006, 1.10092, 50],
    [Date.UTC(2026, 0, 6, 7, 27, 0, 0), 1.10092, 1.10112, 1.10082, 1.101, 50],
    [Date.UTC(2026, 0, 6, 7, 28, 0, 0), 1.101, 1.10116, 1.10088, 1.10105, 50],
    [Date.UTC(2026, 0, 6, 7, 29, 0, 0), 1.10105, 1.10118, 1.10092, 1.10108, 50],
    [Date.UTC(2026, 0, 6, 7, 30, 0, 0), 1.10108, 1.10122, 1.10098, 1.10112, 50],
    [Date.UTC(2026, 0, 6, 7, 31, 0, 0), 1.10112, 1.10124, 1.101, 1.10116, 50],
    [Date.UTC(2026, 0, 6, 7, 32, 0, 0), 1.10116, 1.10128, 1.10104, 1.1012, 50],
  ];

  return {
    nowMs,
    baseCandles: prependLowVolHistory(baseCore, 5 * 60_000, 220),
    confirmCandles: prependLowVolHistory(confirmCore, 60_000, 220),
    quotePrice: 1.1012,
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

test("composer regime gate suppresses only entries when out of regime", () => {
  const fixture = buildOrbEntryFixture();
  const armId = "orb_m5_m1";

  const baseTune = buildModelGuidedComposerTuneId({
    armId,
    digest: "1111111111",
  });
  const compressionTune = buildModelGuidedComposerTuneId({
    armId,
    digest: "2222222222",
    regimeGateId: "regime_vol_compression",
  });
  const expansionTune = buildModelGuidedComposerTuneId({
    armId,
    digest: "3333333333",
    regimeGateId: "regime_vol_expansion",
  });

  const basePhase = modelGuidedComposerV2Strategy.applyPhaseDetectors(
    buildInput({
      session: "berlin",
      nowMs: fixture.nowMs,
      armId,
      tuneId: baseTune,
      symbol: "EURUSD",
      baseTf: "M5",
      confirmTf: "M1",
      baseCandles: fixture.baseCandles,
      confirmCandles: fixture.confirmCandles,
      quotePrice: fixture.quotePrice,
    }),
  );
  assert.deepEqual(basePhase.entryIntent, { model: "ifvg_touch" });

  const compressionPhase = modelGuidedComposerV2Strategy.applyPhaseDetectors(
    buildInput({
      session: "berlin",
      nowMs: fixture.nowMs,
      armId,
      tuneId: compressionTune,
      symbol: "EURUSD",
      baseTf: "M5",
      confirmTf: "M1",
      baseCandles: fixture.baseCandles,
      confirmCandles: fixture.confirmCandles,
      quotePrice: fixture.quotePrice,
    }),
  );
  assert.equal(compressionPhase.entryIntent, null);
  assert.ok(compressionPhase.reasonCodes.includes("REGIME_GATE_ENTRY_BLOCKED"));
  assert.ok(compressionPhase.reasonCodes.includes("REGIME_GATE_OUT_OF_RANGE"));

  const expansionPhase = modelGuidedComposerV2Strategy.applyPhaseDetectors(
    buildInput({
      session: "berlin",
      nowMs: fixture.nowMs,
      armId,
      tuneId: expansionTune,
      symbol: "EURUSD",
      baseTf: "M5",
      confirmTf: "M1",
      baseCandles: fixture.baseCandles,
      confirmCandles: fixture.confirmCandles,
      quotePrice: fixture.quotePrice,
    }),
  );
  assert.deepEqual(expansionPhase.entryIntent, { model: "ifvg_touch" });
  assert.equal(expansionPhase.reasonCodes.includes("REGIME_GATE_ENTRY_BLOCKED"), false);
});
