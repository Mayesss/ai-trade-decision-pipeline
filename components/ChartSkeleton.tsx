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

// Placeholder for the decision-timeline strip under the chart: just the
// baseline with the shimmer band running along it (no fake dots). rightInset
// matches the chart's price-scale width so the line ends where the pane does.
export function TimelineSkeleton({ rightInset = 56 }: { rightInset?: number }) {
  return (
    <div className="relative mt-1 h-6" aria-hidden="true">
      <div
        className="skeleton-shimmer timeline-skeleton-line timeline-connector absolute top-1/2 h-[2px] -translate-y-1/2"
        style={{ left: 0, right: rightInset }}
      />
    </div>
  );
}
