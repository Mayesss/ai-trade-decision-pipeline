import { describe, expect, it } from 'vitest';
import {
  clipSegmentsAtWithdrawals,
  isRestingEntryWithdrawnTick,
  restingEntryWithdrawnCause,
} from '../../../components/ChartPanel';

// The 2026-09-11 COPPER case: a 06:01 buy limit withdrawn by the session
// window gate at 07:00 (tick log only), then a fresh flat HOLD at 09:31. The
// server window ran to "now" because no decision row cancelled it.
const T0 = 1789099265; // 06:01:05 Berlin, seconds
const WITHDRAW_MS = 1789102800_000; // 07:00:00 Berlin
const NOW = 1789117027;

const withdrawTick = {
  ts: WITHDRAW_MS,
  kind: 'scan_skip' as const,
  reason: 'flat_skip_session_window_pre_open_london_open_resting_entry_withdrawn',
};
const plainSkip = { ts: WITHDRAW_MS + 900_000, kind: 'scan_skip' as const, reason: 'flat_skip_session_window_pre_open_london_open' };

describe('isRestingEntryWithdrawnTick', () => {
  it('recognises only the withdrawn suffix on skip ticks', () => {
    expect(isRestingEntryWithdrawnTick(withdrawTick)).toBe(true);
    expect(isRestingEntryWithdrawnTick(plainSkip)).toBe(false);
    expect(isRestingEntryWithdrawnTick({ kind: 'ai_call', reason: 'x_resting_entry_withdrawn' })).toBe(false);
  });
  it('words the cause without the wrapper tokens', () => {
    expect(restingEntryWithdrawnCause(withdrawTick.reason)).toBe('session window pre open london open');
    expect(restingEntryWithdrawnCause(undefined)).toBe('gate');
  });
});

describe('clipSegmentsAtWithdrawals', () => {
  const segment = { side: 'buy' as const, price: 6.524, fromTime: T0, toTime: NOW, filled: false };

  it('ends the segment at the first withdrawal inside it and marks it', () => {
    const [out] = clipSegmentsAtWithdrawals([segment], [plainSkip, withdrawTick]);
    expect(out.toTime).toBe(WITHDRAW_MS / 1000);
    expect(out.withdrawn).toBe(true);
  });
  it('leaves segments alone when the withdrawal falls outside them', () => {
    const earlier = { ...segment, fromTime: T0 - 7200, toTime: T0 - 3600 };
    expect(clipSegmentsAtWithdrawals([earlier], [withdrawTick])[0]).toEqual(earlier);
    expect(clipSegmentsAtWithdrawals([segment], [])[0]).toEqual(segment);
  });
  it('never clips a filled segment — the fill, not the gate, ended it', () => {
    const filled = { ...segment, filled: true };
    expect(clipSegmentsAtWithdrawals([filled], [withdrawTick])[0]).toEqual(filled);
  });
  it('keeps a minimum visible length when the withdrawal races the placement', () => {
    const [out] = clipSegmentsAtWithdrawals([segment], [{ ...withdrawTick, ts: (T0 + 5) * 1000 }]);
    expect(out.toTime).toBe(T0 + 60);
  });
});
