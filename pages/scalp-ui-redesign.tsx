import Head from 'next/head';
import Script from 'next/script';
import type { LucideIcon } from 'lucide-react';
import {
  Activity,
  AlarmClockCheck,
  ArrowUpRight,
  Bot,
  BrainCircuit,
  CandlestickChart,
  CheckCircle2,
  Gauge,
  Globe2,
  PauseCircle,
  Radar,
  ShieldAlert,
  ShieldCheck,
  TimerReset,
  TriangleAlert,
} from 'lucide-react';
import { useId } from 'react';

type Tone = 'positive' | 'warning' | 'critical' | 'neutral';
type StepState = 'ok' | 'watch' | 'blocked';

type PulseCard = {
  id: string;
  label: string;
  value: string;
  delta: string;
  tone: Tone;
  icon: LucideIcon;
  spark: number[];
};

type PipelineStep = {
  id: string;
  label: string;
  state: StepState;
  icon: LucideIcon;
};

type SymbolTile = {
  symbol: string;
  strategy: string;
  status: StepState;
  icon: LucideIcon;
  pnlSpark: number[];
  mix: number[];
};

const pulseCards: PulseCard[] = [
  {
    id: 'ops-health',
    label: 'Ops Health',
    value: 'Stable',
    delta: '+3 lanes',
    tone: 'positive',
    icon: ShieldCheck,
    spark: [9, 12, 13, 15, 14, 16, 18, 17, 19, 21, 20, 22],
  },
  {
    id: 'forward',
    label: 'Forward Edge',
    value: 'Thin',
    delta: '-2 lanes',
    tone: 'warning',
    icon: Gauge,
    spark: [14, 13, 13, 12, 12, 11, 10, 9, 10, 9, 8, 8],
  },
  {
    id: 'guardrail',
    label: 'Guardrail',
    value: 'Armed',
    delta: '2 pauses',
    tone: 'neutral',
    icon: ShieldAlert,
    spark: [4, 5, 4, 4, 6, 5, 5, 4, 3, 3, 4, 3],
  },
  {
    id: 'latency',
    label: 'Worker Latency',
    value: 'Watch',
    delta: '+7s',
    tone: 'critical',
    icon: TimerReset,
    spark: [9, 8, 8, 9, 10, 10, 11, 12, 11, 13, 14, 15],
  },
];

const pipelineSteps: PipelineStep[] = [
  { id: 'discover', label: 'Discover', state: 'ok', icon: Radar },
  { id: 'load', label: 'Load', state: 'ok', icon: Globe2 },
  { id: 'prepare', label: 'Prepare', state: 'watch', icon: BrainCircuit },
  { id: 'worker', label: 'Worker', state: 'ok', icon: Bot },
  { id: 'promote', label: 'Promote', state: 'watch', icon: ArrowUpRight },
  { id: 'execute', label: 'Execute', state: 'ok', icon: CandlestickChart },
  { id: 'monitor', label: 'Monitor', state: 'ok', icon: Activity },
  { id: 'pause', label: 'Panic Stop', state: 'blocked', icon: PauseCircle },
];

const symbolTiles: SymbolTile[] = [
  {
    symbol: 'BTCUSDT',
    strategy: 'compression_breakout_pullback',
    status: 'watch',
    icon: CandlestickChart,
    pnlSpark: [8, 9, 7, 10, 11, 10, 9, 8, 6, 5, 4, 5, 4, 3],
    mix: [32, 28, 21, 19],
  },
  {
    symbol: 'ETHUSDT',
    strategy: 'regime_pullback',
    status: 'ok',
    icon: CheckCircle2,
    pnlSpark: [4, 5, 6, 5, 6, 7, 9, 8, 10, 11, 12, 11, 13, 14],
    mix: [41, 24, 18, 17],
  },
  {
    symbol: 'XAUUSD',
    strategy: 'guarded_low_dd',
    status: 'ok',
    icon: ShieldCheck,
    pnlSpark: [6, 6, 7, 8, 8, 7, 8, 9, 9, 10, 11, 10, 11, 12],
    mix: [35, 29, 20, 16],
  },
  {
    symbol: 'EURUSD',
    strategy: 'trend_reacceleration',
    status: 'blocked',
    icon: TriangleAlert,
    pnlSpark: [9, 8, 8, 7, 7, 6, 5, 5, 4, 4, 3, 3, 2, 2],
    mix: [23, 21, 30, 26],
  },
];

const sessionRows = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const sessionBlocks = ['00', '03', '06', '09', '12', '15', '18', '21'];
const sessionHeat: number[][] = [
  [0.2, 0.24, 0.28, 0.51, 0.68, 0.54, 0.35, 0.21],
  [0.18, 0.22, 0.35, 0.57, 0.74, 0.49, 0.32, 0.19],
  [0.17, 0.23, 0.31, 0.53, 0.72, 0.56, 0.31, 0.2],
  [0.14, 0.2, 0.27, 0.47, 0.7, 0.58, 0.29, 0.17],
  [0.12, 0.16, 0.24, 0.39, 0.62, 0.52, 0.26, 0.15],
  [0.08, 0.11, 0.15, 0.22, 0.3, 0.24, 0.14, 0.1],
  [0.07, 0.1, 0.14, 0.21, 0.28, 0.23, 0.13, 0.08],
];

const guardrailSlices = [
  { id: 'risk', label: 'Risk', pct: 36, tone: 'positive' as Tone },
  { id: 'latency', label: 'Latency', pct: 19, tone: 'warning' as Tone },
  { id: 'fill', label: 'Fill Drift', pct: 26, tone: 'critical' as Tone },
  { id: 'sessions', label: 'Session Drift', pct: 19, tone: 'neutral' as Tone },
];

function toneSwatch(tone: Tone): string {
  if (tone === 'positive') return 'from-emerald-400 to-teal-300';
  if (tone === 'warning') return 'from-amber-300 to-orange-300';
  if (tone === 'critical') return 'from-rose-400 to-pink-400';
  return 'from-sky-400 to-cyan-300';
}

function toneText(tone: Tone): string {
  if (tone === 'positive') return 'text-emerald-100';
  if (tone === 'warning') return 'text-amber-100';
  if (tone === 'critical') return 'text-rose-100';
  return 'text-sky-100';
}

function stepBadge(state: StepState): string {
  if (state === 'ok') return 'border-emerald-300/40 bg-emerald-300/20 text-emerald-100';
  if (state === 'watch') return 'border-amber-300/40 bg-amber-300/20 text-amber-100';
  return 'border-rose-300/45 bg-rose-300/20 text-rose-100';
}

function Sparkline({
  points,
  tone,
}: {
  points: number[];
  tone: Tone;
}) {
  const gradientId = useId().replace(/:/g, '');
  const width = 176;
  const height = 58;
  const min = Math.min(...points);
  const max = Math.max(...points);
  const range = Math.max(1, max - min);

  const coords = points.map((value, index) => {
    const x = (index / Math.max(1, points.length - 1)) * width;
    const y = height - ((value - min) / range) * height;
    return { x, y };
  });

  const line = coords.map((c) => `${c.x.toFixed(2)},${c.y.toFixed(2)}`).join(' ');
  const area = `${line} ${width},${height} 0,${height}`;
  const stroke =
    tone === 'positive'
      ? '#34d399'
      : tone === 'warning'
        ? '#fbbf24'
        : tone === 'critical'
          ? '#fb7185'
          : '#38bdf8';

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="h-14 w-full">
      <defs>
        <linearGradient id={`fill-${tone}-${gradientId}`} x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor={stroke} stopOpacity="0.42" />
          <stop offset="100%" stopColor={stroke} stopOpacity="0" />
        </linearGradient>
      </defs>
      <polyline points={area} fill={`url(#fill-${tone}-${gradientId})`} />
      <polyline points={line} fill="none" stroke={stroke} strokeWidth="2.8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function RingChart({ slices }: { slices: typeof guardrailSlices }) {
  const size = 190;
  const stroke = 26;
  const radius = (size - stroke) / 2;
  const circumference = 2 * Math.PI * radius;
  let progress = 0;

  return (
    <svg viewBox={`0 0 ${size} ${size}`} className="h-48 w-48">
      <circle cx={size / 2} cy={size / 2} r={radius} fill="none" stroke="#112236" strokeWidth={stroke} />
      {slices.map((slice) => {
        const ratio = slice.pct / 100;
        const dash = circumference * ratio;
        const gap = circumference - dash;
        const rotation = (progress / 100) * 360 - 90;
        progress += slice.pct;
        const color =
          slice.tone === 'positive'
            ? '#34d399'
            : slice.tone === 'warning'
              ? '#fbbf24'
              : slice.tone === 'critical'
                ? '#fb7185'
                : '#38bdf8';

        return (
          <circle
            key={slice.id}
            cx={size / 2}
            cy={size / 2}
            r={radius}
            fill="none"
            stroke={color}
            strokeWidth={stroke}
            strokeDasharray={`${dash} ${gap}`}
            transform={`rotate(${rotation} ${size / 2} ${size / 2})`}
            strokeLinecap="round"
          />
        );
      })}
    </svg>
  );
}

function MixBars({ values }: { values: number[] }) {
  return (
    <div className="flex h-2.5 overflow-hidden rounded-full bg-slate-800">
      {values.map((v, idx) => (
        <span
          key={`${idx}-${v}`}
          style={{ width: `${v}%` }}
          className={
            idx === 0
              ? 'bg-emerald-400'
              : idx === 1
                ? 'bg-sky-400'
                : idx === 2
                  ? 'bg-amber-300'
                  : 'bg-rose-400'
          }
        />
      ))}
    </div>
  );
}

export default function ScalpUiRedesignPage() {
  return (
    <>
      <Head>
        <title>Scalp UI Compact Redesign</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="" />
        <link
          href="https://fonts.googleapis.com/css2?family=Sora:wght@500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap"
          rel="stylesheet"
        />
      </Head>
      <Script src="https://mcp.figma.com/mcp/html-to-design/capture.js" strategy="afterInteractive" />

      <main className="min-h-screen bg-[#060d14] text-slate-100">
        <div className="mx-auto max-w-[1450px] px-4 py-6 sm:px-6 lg:px-8">
          <section className="relative overflow-hidden rounded-[28px] border border-cyan-300/25 bg-[radial-gradient(circle_at_0%_10%,rgba(56,189,248,0.32),transparent_45%),radial-gradient(circle_at_100%_0%,rgba(16,185,129,0.2),transparent_40%),linear-gradient(145deg,#091421,#0d1d2e_45%,#071019)] p-5 sm:p-6">
            <div className="absolute right-4 top-4 rounded-full border border-cyan-200/35 bg-cyan-200/10 px-3 py-1 text-[11px] text-cyan-100">
              compact scalp cockpit
            </div>
            <p
              className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/80"
              style={{ fontFamily: 'IBM Plex Mono, monospace' }}
            >
              Scalp Deployment
            </p>
            <h1 className="mt-2 max-w-3xl text-2xl sm:text-4xl" style={{ fontFamily: 'Sora, sans-serif' }}>
              Visual-first control plane, less logs and raw tables.
            </h1>
            <div className="mt-5 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
              {pulseCards.map((card) => {
                const Icon = card.icon;
                return (
                  <article key={card.id} className="rounded-2xl border border-white/10 bg-slate-900/55 p-3">
                    <div className="flex items-center justify-between">
                      <div className={`inline-flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br ${toneSwatch(card.tone)}`}>
                        <Icon className="h-4 w-4 text-[#031018]" />
                      </div>
                      <span className={`text-[11px] ${toneText(card.tone)}`}>{card.delta}</span>
                    </div>
                    <p className="mt-3 text-xs text-slate-300">{card.label}</p>
                    <p className="text-xl font-semibold" style={{ fontFamily: 'Sora, sans-serif' }}>
                      {card.value}
                    </p>
                    <Sparkline points={card.spark} tone={card.tone} />
                  </article>
                );
              })}
            </div>
          </section>

          <section className="mt-5 grid grid-cols-1 gap-5 xl:grid-cols-[1.3fr_1fr]">
            <article className="rounded-3xl border border-slate-700/60 bg-[#0b1623] p-4 sm:p-5">
              <div className="mb-4 flex items-center justify-between">
                <h2 className="text-lg sm:text-xl" style={{ fontFamily: 'Sora, sans-serif' }}>
                  Pipeline Flow
                </h2>
                <span className="rounded-full border border-sky-300/30 bg-sky-300/15 px-2.5 py-1 text-xs text-sky-100">8 active lanes</span>
              </div>
              <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
                {pipelineSteps.map((step) => {
                  const Icon = step.icon;
                  return (
                    <div key={step.id} className="rounded-2xl border border-white/10 bg-slate-900/60 p-3">
                      <div className="flex items-start justify-between">
                        <div className="inline-flex h-9 w-9 items-center justify-center rounded-xl border border-slate-700/80 bg-[#0b1320]">
                          <Icon className="h-4 w-4 text-slate-100" />
                        </div>
                        <span className={`rounded-full border px-2 py-0.5 text-[10px] ${stepBadge(step.state)}`}>{step.state}</span>
                      </div>
                      <p className="mt-3 text-sm text-slate-200">{step.label}</p>
                      <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-slate-800">
                        <div
                          className={`h-full w-full ${
                            step.state === 'ok'
                              ? 'bg-emerald-400'
                              : step.state === 'watch'
                                ? 'bg-amber-300'
                                : 'bg-rose-400'
                          }`}
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
            </article>

            <article className="rounded-3xl border border-slate-700/60 bg-[#0b1623] p-4 sm:p-5">
              <h2 className="text-lg sm:text-xl" style={{ fontFamily: 'Sora, sans-serif' }}>
                Guardrail Mix
              </h2>
              <div className="mt-3 flex flex-col items-center gap-2">
                <RingChart slices={guardrailSlices} />
                <div className="grid w-full grid-cols-2 gap-2">
                  {guardrailSlices.map((slice) => (
                    <div key={slice.id} className="rounded-xl border border-white/10 bg-slate-900/60 px-3 py-2">
                      <p className="text-xs text-slate-300">{slice.label}</p>
                      <p className={`text-sm font-semibold ${toneText(slice.tone)}`}>{slice.pct}%</p>
                    </div>
                  ))}
                </div>
              </div>
            </article>
          </section>

          <section className="mt-5 rounded-3xl border border-slate-700/60 bg-[#0b1623] p-4 sm:p-5">
            <div className="mb-3 flex items-center justify-between">
              <h2 className="text-lg sm:text-xl" style={{ fontFamily: 'Sora, sans-serif' }}>
                Symbol Pulse
              </h2>
              <span className="rounded-full border border-emerald-300/35 bg-emerald-300/15 px-2.5 py-1 text-xs text-emerald-100">
                graph-only view
              </span>
            </div>
            <div className="grid grid-cols-1 gap-3 lg:grid-cols-2 2xl:grid-cols-4">
              {symbolTiles.map((tile) => {
                const Icon = tile.icon;
                return (
                  <article key={tile.symbol} className="rounded-2xl border border-white/10 bg-slate-900/60 p-3">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <div className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-slate-700/70 bg-[#09111c]">
                          <Icon className="h-4 w-4 text-slate-100" />
                        </div>
                        <div>
                          <p className="text-sm font-semibold" style={{ fontFamily: 'Sora, sans-serif' }}>
                            {tile.symbol}
                          </p>
                          <p className="text-[11px] text-slate-400">{tile.strategy}</p>
                        </div>
                      </div>
                      <span className={`rounded-full border px-2 py-0.5 text-[10px] ${stepBadge(tile.status)}`}>{tile.status}</span>
                    </div>
                    <div className="mt-3">
                      <Sparkline
                        points={tile.pnlSpark}
                        tone={tile.status === 'ok' ? 'positive' : tile.status === 'watch' ? 'warning' : 'critical'}
                      />
                    </div>
                    <div className="mt-2">
                      <MixBars values={tile.mix} />
                    </div>
                  </article>
                );
              })}
            </div>
          </section>

          <section className="mt-5 rounded-3xl border border-slate-700/60 bg-[#0b1623] p-4 sm:p-5">
            <div className="mb-3 flex items-center justify-between">
              <h2 className="text-lg sm:text-xl" style={{ fontFamily: 'Sora, sans-serif' }}>
                Session Heat
              </h2>
              <span className="rounded-full border border-slate-600 bg-slate-800/80 px-2.5 py-1 text-xs text-slate-300">UTC blocks</span>
            </div>

            <div className="overflow-x-auto">
              <div className="min-w-[580px]">
                <div className="mb-2 grid grid-cols-[74px_repeat(8,minmax(0,1fr))] gap-2">
                  <div />
                  {sessionBlocks.map((block) => (
                    <p key={block} className="text-center text-[10px] uppercase tracking-[0.16em] text-slate-400">
                      {block}
                    </p>
                  ))}
                </div>
                <div className="space-y-2">
                  {sessionRows.map((day, rowIdx) => (
                    <div key={day} className="grid grid-cols-[74px_repeat(8,minmax(0,1fr))] gap-2">
                      <p className="pt-1 text-xs text-slate-400">{day}</p>
                      {sessionHeat[rowIdx].map((intensity, colIdx) => (
                        <span
                          key={`${day}-${colIdx}`}
                          className="h-7 rounded-lg border border-slate-800"
                          style={{
                            backgroundColor: `rgba(56, 189, 248, ${0.08 + intensity * 0.82})`,
                            boxShadow: intensity > 0.56 ? '0 0 0 1px rgba(16,185,129,0.35) inset' : 'none',
                          }}
                        />
                      ))}
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </section>

          <section className="mt-5 flex items-center justify-end">
            <p className="rounded-full border border-slate-700 bg-slate-900/60 px-3 py-1 text-[11px] text-slate-400">
              icon + graph dominant layout for fast scan decisions
            </p>
          </section>
        </div>
      </main>
    </>
  );
}
