// 12-week NetR track: divergent bars from a zero baseline (up = +R, down = -R)
// with a thin cumulative-NetR trajectory line overlaid. Empty weeks render as
// a faint dot on the zero line so they read differently from very-small bars.

export interface WeeklyNetRTrackProps {
  // Per-week NetR values in oldest→newest order. Length defines bar count.
  values: number[];
  // Pass to share a normalization scale across multiple tracks (eg all cells
  // of the same deployment). Defaults to max |value| across this row alone.
  globalMaxAbs?: number;
  // Override the trajectory line. When provided, the line shows this series
  // instead of the per-week cumulative of `values` — used to overlay the
  // deployment-wide running NetR on top of each cell's bars. The Σ readout is
  // suppressed in that mode (the cell's own sum is shown elsewhere in the row).
  cumulativeOverride?: number[];
  // Tooltip helper — given a week index, returns the displayed label.
  weekLabel?: (idx: number, value: number) => string;
  // Track height in px. Default matches the v5 cell-evidence row.
  heightPx?: number;
  // Track width — defaults to 100% of the container. Pass an exact px to keep
  // alignment when used outside a grid.
  widthPx?: number;
}

export function WeeklyNetRTrack({
  values,
  globalMaxAbs,
  cumulativeOverride,
  weekLabel,
  heightPx = 56,
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
      : Math.max(0.0001, ...values.map((v) => Math.abs(Number.isFinite(v) ? v : 0)));

  // Trajectory line: either the per-week cumulative of this cell's `values`,
  // or an externally-supplied override (used to overlay the deployment-wide
  // running NetR across every cell track).
  const usesOverride = Array.isArray(cumulativeOverride) && cumulativeOverride.length > 0;
  const cum: number[] = usesOverride
    ? cumulativeOverride!.slice(0, values.length)
    : (() => {
        const out: number[] = [];
        let running = 0;
        for (const v of values) {
          running += Number.isFinite(v) ? v : 0;
          out.push(running);
        }
        return out;
      })();
  const maxCumAbs = Math.max(0.0001, ...cum.map((c) => Math.abs(c)));
  const finalCum = cum.length > 0 ? cum[cum.length - 1] : 0;
  const finalColor =
    finalCum > 0 ? "text-emerald-400" : finalCum < 0 ? "text-rose-400" : "text-zinc-500";
  const finalDotClass =
    finalCum > 0 ? "fill-emerald-300" : finalCum < 0 ? "fill-rose-300" : "fill-zinc-400";

  const N = values.length;
  const barWidth = 0.7;
  const barInset = (1 - barWidth) / 2;

  return (
    <div className="flex items-stretch gap-2" style={{ width: widthPx ?? "100%" }}>
      <div className="relative flex-1 rounded-sm bg-zinc-900/60" style={{ height: heightPx }}>
        <svg
          viewBox={`0 -1 ${N} 2`}
          preserveAspectRatio="none"
          className="absolute inset-0 h-full w-full"
          aria-hidden
        >
          <line
            x1={0}
            x2={N}
            y1={0}
            y2={0}
            className="stroke-zinc-700/70"
            strokeWidth={1}
            vectorEffect="non-scaling-stroke"
          />

          {values.map((v, i) => {
            const empty = !Number.isFinite(v) || v === 0;
            const tip = weekLabel
              ? weekLabel(i, Number.isFinite(v) ? v : 0)
              : `w${i + 1}: ${v >= 0 ? "+" : ""}${(Number.isFinite(v) ? v : 0).toFixed(2)}R`;
            if (empty) {
              return (
                <circle key={`d${i}`} cx={i + 0.5} cy={0} r={0.07} className="fill-zinc-600/70">
                  <title>{tip}</title>
                </circle>
              );
            }
            const norm = Math.max(-1, Math.min(1, v / maxAbs));
            const isPos = norm > 0;
            const h = Math.abs(norm);
            return (
              <rect
                key={`b${i}`}
                x={i + barInset}
                y={isPos ? -h : 0}
                width={barWidth}
                height={h}
                className={isPos ? "fill-emerald-400/85" : "fill-rose-400/85"}
              >
                <title>{tip}</title>
              </rect>
            );
          })}

          <polyline
            points={cum.map((c, i) => `${i + 0.5},${-c / maxCumAbs}`).join(" ")}
            fill="none"
            className="stroke-zinc-200/60"
            strokeWidth={1.25}
            strokeLinejoin="round"
            strokeLinecap="round"
            vectorEffect="non-scaling-stroke"
          />
          <circle cx={N - 0.5} cy={-finalCum / maxCumAbs} r={0.09} className={finalDotClass} />
        </svg>
      </div>
      {usesOverride ? null : (
        <div
          className={`flex w-[4.5rem] shrink-0 flex-col justify-center text-right text-[11px] leading-tight ${finalColor}`}
          title={`cumulative 12w netR: ${finalCum >= 0 ? "+" : ""}${finalCum.toFixed(2)}R`}
        >
          <span className="text-zinc-500 text-[10px]">Σ 12w</span>
          <span>
            {finalCum >= 0 ? "+" : ""}
            {finalCum.toFixed(2)}R
          </span>
        </div>
      )}
    </div>
  );
}
