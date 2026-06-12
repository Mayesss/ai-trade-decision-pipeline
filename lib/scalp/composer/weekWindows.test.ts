import assert from "node:assert/strict";
import test from "node:test";

import {
  resolveScalpV2CompletedWeekWindowToUtc,
  resolveScalpV2WeekCompleteAtUtc,
  resolveScalpV2WeekCompleteConfig,
} from "./weekWindows";

function withWeekCompleteEnv<T>(
  env: {
    finalSession?: string;
    graceMinutes?: string;
  },
  fn: () => T,
): T {
  const prevFinalSession = process.env.SCALP_V2_WEEK_COMPLETE_FINAL_SESSION;
  const prevGrace = process.env.SCALP_V2_WEEK_COMPLETE_GRACE_MINUTES;
  try {
    if (env.finalSession === undefined) delete process.env.SCALP_V2_WEEK_COMPLETE_FINAL_SESSION;
    else process.env.SCALP_V2_WEEK_COMPLETE_FINAL_SESSION = env.finalSession;
    if (env.graceMinutes === undefined) delete process.env.SCALP_V2_WEEK_COMPLETE_GRACE_MINUTES;
    else process.env.SCALP_V2_WEEK_COMPLETE_GRACE_MINUTES = env.graceMinutes;
    return fn();
  } finally {
    if (prevFinalSession === undefined) delete process.env.SCALP_V2_WEEK_COMPLETE_FINAL_SESSION;
    else process.env.SCALP_V2_WEEK_COMPLETE_FINAL_SESSION = prevFinalSession;
    if (prevGrace === undefined) delete process.env.SCALP_V2_WEEK_COMPLETE_GRACE_MINUTES;
    else process.env.SCALP_V2_WEEK_COMPLETE_GRACE_MINUTES = prevGrace;
  }
}

test("v2 week completion defaults to Saturday Pacific close plus grace", { concurrency: false }, () => {
  withWeekCompleteEnv({}, () => {
    const saturdayOfDstWeek = Date.UTC(2026, 4, 2, 12, 0, 0); // Saturday, May 2, 2026
    assert.deepEqual(resolveScalpV2WeekCompleteConfig(), {
      finalSession: "pacific",
      graceMinutes: 60,
    });
    assert.equal(
      resolveScalpV2WeekCompleteAtUtc(saturdayOfDstWeek),
      Date.UTC(2026, 4, 2, 22, 0, 0),
    );
  });
});

test("v2 completed-week window rolls after final session close grace", { concurrency: false }, () => {
  withWeekCompleteEnv({ finalSession: "pacific", graceMinutes: "60" }, () => {
    const currentWeekStart = Date.UTC(2026, 3, 27, 0, 0, 0);
    const nextWeekStart = Date.UTC(2026, 4, 4, 0, 0, 0);

    assert.equal(
      resolveScalpV2CompletedWeekWindowToUtc(Date.UTC(2026, 4, 2, 21, 59, 59)),
      currentWeekStart,
    );
    assert.equal(
      resolveScalpV2CompletedWeekWindowToUtc(Date.UTC(2026, 4, 2, 22, 0, 0)),
      nextWeekStart,
    );
    assert.equal(
      resolveScalpV2CompletedWeekWindowToUtc(Date.UTC(2026, 4, 2, 23, 3, 0)),
      nextWeekStart,
    );
  });
});

test("v2 week completion session and grace are configurable", { concurrency: false }, () => {
  withWeekCompleteEnv({ finalSession: "newyork", graceMinutes: "30" }, () => {
    const saturdayOfDstWeek = Date.UTC(2026, 4, 2, 12, 0, 0);
    assert.deepEqual(resolveScalpV2WeekCompleteConfig(), {
      finalSession: "newyork",
      graceMinutes: 30,
    });
    assert.equal(
      resolveScalpV2WeekCompleteAtUtc(saturdayOfDstWeek),
      Date.UTC(2026, 4, 2, 16, 30, 0),
    );
  });
});
