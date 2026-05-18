// Horizontal weekly-NetR bar track — direct port of the v3 "Windows Results"
// AgGrid column (legacy.tsx:6063-6148) into the v5 zinc/emerald/rose terminal
// palette. Each week becomes a thin colored bar inside a fixed-height track;
// height ∝ |NetR| / globalMaxAbs, emerald positive / rose negative, gray slot
// when the week has no trades.

export interface WeeklyNetRTrackProps {
  // Per-week NetR values in oldest→newest order. Length defines bar count.
  values: number[];
  // Pass to share a normalization scale across multiple tracks (eg all cells
  // of the same deployment). Defaults to max |value| across this row alone.
  globalMaxAbs?: number;
  // Tooltip helper — given a week index, returns the displayed label.
  weekLabel?: (idx: number, value: number) => string;
  // Track height in px. v3 used 48px; we default to that.
  heightPx?: number;
  // Track width — defaults to 100% of the container. Pass an exact px to keep
  // alignment when used outside a grid.
  widthPx?: number;
}

export function WeeklyNetRTrack({
  values,
  globalMaxAbs,
  weekLabel,
  heightPx = 40,
  widthPx,
}: WeeklyNetRTrackProps) {
  if (values.length === 0) {
    return (
      <div
        className="flex items-center justify-center rounded-sm bg-zinc-900/40 text-[11px] text-zinc-600"
        style={{ height: heightPx, width: widthPx ?? "100%" }}
      >
        (no weekly evidence)
      </div>
    );
  }
  const maxAbs =
    globalMaxAbs && globalMaxAbs > 0
      ? globalMaxAbs
      : Math.max(0.0001, ...values.map((v) => Math.abs(v)));
  return (
    <div
      className="flex items-end gap-[2px] rounded-sm bg-zinc-900/60 px-1 py-1"
      style={{ height: heightPx, width: widthPx ?? "100%" }}
    >
      {values.map((value, idx) => {
        const empty = !Number.isFinite(value) || value === 0;
        const heightPct = empty ? 6 : Math.max(6, Math.round((Math.abs(value) / maxAbs) * 100));
        const toneClass = empty
          ? "bg-zinc-700/60"
          : value > 0
            ? "bg-emerald-400/90"
            : "bg-rose-400/90";
        const tip = weekLabel
          ? weekLabel(idx, value)
          : `w${idx + 1}: ${value >= 0 ? "+" : ""}${value.toFixed(2)}R`;
        return (
          <span
            key={`netr-bar-${idx}`}
            className={`flex-1 min-w-[3px] rounded-sm ${toneClass}`}
            style={{ height: `${heightPct}%` }}
            title={tip}
          />
        );
      })}
    </div>
  );
}
