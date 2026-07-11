import React from 'react';

// Shared loading skeletons for the swing chart panel. The chart placeholder is
// deliberately quiet: an empty panel with a soft highlight sweeping left →
// right (see .skeleton-shimmer in globals.css). Colors are slate utilities the
// .theme-dark remap already covers.

export function ChartSkeleton({ minHeight = 260 }: { minHeight?: number }) {
  return (
    <div
      className="skeleton-shimmer h-full w-full rounded-xl border border-slate-200 bg-slate-50/80"
      style={{ minHeight }}
      aria-hidden="true"
    />
  );
}

// Placeholder for the decision-timeline strip under the chart: baseline plus a
// spread of grey dots in the real hourly/quarter size rhythm. The timeline-dot
// classes keep the circles round under the global sharp-corner rule.
export function TimelineSkeleton() {
  return (
    <div className="relative mt-1 h-6 overflow-hidden" aria-hidden="true">
      <div
        className="timeline-connector absolute top-1/2 h-[2px] -translate-y-1/2"
        style={{ left: 0, right: 56 }}
      />
      <div
        className="flex h-full animate-pulse items-center justify-between"
        style={{ marginRight: 56 }}
      >
        {Array.from({ length: 16 }).map((_, idx) => (
          <span
            key={idx}
            className={`timeline-dot timeline-dot-skip rounded-full ${
              idx % 4 === 0 ? 'h-3.5 w-3.5' : 'h-2 w-2'
            }`}
          />
        ))}
      </div>
    </div>
  );
}
