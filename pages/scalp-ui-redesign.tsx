import Head from 'next/head';

type CronRow = {
  id: string;
  cadence: string;
  role: string;
  status: 'healthy' | 'lagging' | 'blocked';
  sla: string;
  lastRun: string;
  p95: string;
};

type DeploymentRow = {
  deploymentId: string;
  symbol: string;
  strategy: string;
  tune: string;
  promotionEligible: boolean;
  forwardExp: number;
  profitableWindowsPct: number;
  maxDdR: number;
  guardrail: string;
};

const cycleCards = [
  {
    label: 'Cycle Progress',
    value: '76%',
    detail: '184 / 240 tasks complete',
    tone: 'sky',
  },
  {
    label: '2W Forward Expectancy',
    value: '-0.27 R',
    detail: 'Gate floor >= 0.00 R',
    tone: 'rose',
  },
  {
    label: '4W Forward Expectancy',
    value: '-0.19 R',
    detail: 'Selection was +0.74 R',
    tone: 'amber',
  },
  {
    label: 'Promotion Eligible',
    value: '2 / 11',
    detail: 'Strict forward gate applied',
    tone: 'emerald',
  },
];

const cronRows: CronRow[] = [
  {
    id: 'scalp_cycle_start',
    cadence: 'Daily',
    role: 'Freeze universe + emit chunk manifest',
    status: 'healthy',
    sla: '< 20s',
    lastRun: '06:00',
    p95: '7.3s',
  },
  {
    id: 'scalp_cycle_worker',
    cadence: 'Every 2m',
    role: 'Claim and run one replay chunk',
    status: 'healthy',
    sla: '< 500s',
    lastRun: '08:16',
    p95: '332s',
  },
  {
    id: 'scalp_cycle_aggregate',
    cadence: 'Every 10m',
    role: 'Compute candidate summary + robustness labels',
    status: 'lagging',
    sla: '< 45s',
    lastRun: '08:10',
    p95: '53s',
  },
  {
    id: 'scalp_promotion_gate_apply',
    cadence: 'Daily',
    role: 'Set promotionEligible + forwardValidation',
    status: 'healthy',
    sla: '< 30s',
    lastRun: '06:11',
    p95: '11.5s',
  },
  {
    id: 'scalp_execute_deployments',
    cadence: 'Every 1m',
    role: 'Run enabled + gate-passed deployments only',
    status: 'healthy',
    sla: '< 60s',
    lastRun: '08:16',
    p95: '13.2s',
  },
  {
    id: 'scalp_live_guardrail_monitor',
    cadence: 'Every 10m',
    role: 'Pause hard-breach deployments',
    status: 'healthy',
    sla: '< 30s',
    lastRun: '08:10',
    p95: '9.2s',
  },
  {
    id: 'scalp_housekeeping',
    cadence: 'Hourly',
    role: 'Prune stale locks/cycles + compact lists',
    status: 'healthy',
    sla: '< 30s',
    lastRun: '08:00',
    p95: '5.8s',
  },
];

const deploymentRows: DeploymentRow[] = [
  {
    deploymentId: 'btcusdt_cbp_dd8',
    symbol: 'BTCUSDT',
    strategy: 'compression_breakout_pullback_m15_m3',
    tune: 'dd8_e2_p8_tr1.5_ts18_sw0.20',
    promotionEligible: false,
    forwardExp: -0.24,
    profitableWindowsPct: 33,
    maxDdR: 5.7,
    guardrail: 'expectancy drift',
  },
  {
    deploymentId: 'btcusdt_guarded_pf',
    symbol: 'BTCUSDT',
    strategy: 'regime_pullback_m15_m3',
    tune: 'guarded_high_pf_default',
    promotionEligible: false,
    forwardExp: -0.18,
    profitableWindowsPct: 39,
    maxDdR: 4.9,
    guardrail: 'forward pct low',
  },
  {
    deploymentId: 'ethusdt_guarded_base',
    symbol: 'ETHUSDT',
    strategy: 'regime_pullback_m15_m3',
    tune: 'default',
    promotionEligible: true,
    forwardExp: 0.05,
    profitableWindowsPct: 58,
    maxDdR: 3.1,
    guardrail: 'none',
  },
  {
    deploymentId: 'xauusd_guarded_lowdd',
    symbol: 'XAUUSD',
    strategy: 'regime_pullback_m15_m3',
    tune: 'xauusd_low_dd',
    promotionEligible: true,
    forwardExp: 0.08,
    profitableWindowsPct: 62,
    maxDdR: 2.6,
    guardrail: 'none',
  },
];

const blockedHoursRows = [
  { variant: 'none', windowsBest: 8, expectancy: -0.12, pnlR: -3.8 },
  { variant: '[10,11]', windowsBest: 4, expectancy: -0.09, pnlR: -2.4 },
  { variant: '[9,10]', windowsBest: 2, expectancy: -0.15, pnlR: -4.1 },
  { variant: '[11,12]', windowsBest: 3, expectancy: -0.13, pnlR: -3.2 },
  { variant: '[10]', windowsBest: 1, expectancy: -0.14, pnlR: -3.7 },
  { variant: '[11]', windowsBest: 0, expectancy: -0.19, pnlR: -5.5 },
];

const stressRows = [
  { scenario: 'baseline', netR: -1.8, expectancy: -0.09, pf: 0.94, dd: 4.9 },
  { scenario: 'slippage x2', netR: -2.0, expectancy: -0.10, pf: 0.92, dd: 5.1 },
  { scenario: 'spread +25%', netR: -2.4, expectancy: -0.12, pf: 0.89, dd: 5.4 },
  { scenario: 'spread +50%', netR: -2.9, expectancy: -0.14, pf: 0.86, dd: 5.8 },
  { scenario: 'x2 slip +50% spread', netR: -3.5, expectancy: -0.17, pf: 0.81, dd: 6.3 },
];

function toneClasses(tone: string): string {
  if (tone === 'rose') return 'border-rose-300/60 bg-rose-200/20 text-rose-100';
  if (tone === 'amber') return 'border-amber-300/60 bg-amber-200/20 text-amber-100';
  if (tone === 'emerald') return 'border-emerald-300/60 bg-emerald-200/20 text-emerald-100';
  return 'border-sky-300/60 bg-sky-200/20 text-sky-100';
}

function statusClasses(status: CronRow['status']): string {
  if (status === 'healthy') return 'bg-emerald-300/20 text-emerald-100 border-emerald-300/50';
  if (status === 'lagging') return 'bg-amber-300/20 text-amber-100 border-amber-300/50';
  return 'bg-rose-300/20 text-rose-100 border-rose-300/50';
}

export default function ScalpUiRedesignPage() {
  return (
    <>
      <Head>
        <title>Scalp Ops Console Redesign</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="" />
        <link
          href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap"
          rel="stylesheet"
        />
      </Head>

      <main className="min-h-screen bg-[#08121a] text-slate-100">
        <div className="mx-auto max-w-[1500px] px-4 py-6 sm:px-6 lg:px-8">
          <div className="relative overflow-hidden rounded-3xl border border-cyan-300/25 bg-[radial-gradient(circle_at_20%_0%,rgba(59,130,246,0.35),transparent_50%),radial-gradient(circle_at_80%_20%,rgba(16,185,129,0.2),transparent_45%),linear-gradient(140deg,#0b1722,#0f2533_40%,#08131d)] p-6">
            <div className="absolute right-5 top-5 rounded-full border border-cyan-200/30 bg-cyan-100/10 px-3 py-1 text-xs font-medium tracking-wider text-cyan-100">
              cycle rc_20260308_0600
            </div>
            <p className="text-xs uppercase tracking-[0.26em] text-cyan-200/80" style={{ fontFamily: 'IBM Plex Mono, monospace' }}>
              Scalp Research Ops
            </p>
            <h1 className="mt-2 max-w-3xl text-3xl sm:text-4xl" style={{ fontFamily: 'Space Grotesk, sans-serif' }}>
              Deploy only what passes forward evidence, not yesterday’s rank.
            </h1>
            <p className="mt-3 max-w-4xl text-sm text-slate-200/85">
              New control plane aligned to research cycle cron pipeline, promotion gate outcomes, blocked-hour falsification, execution stress,
              and live guardrail actions.
            </p>
            <div className="mt-6 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
              {cycleCards.map((card) => (
                <article key={card.label} className={`rounded-2xl border p-4 shadow-sm ${toneClasses(card.tone)}`}>
                  <p className="text-[11px] uppercase tracking-[0.18em] opacity-85" style={{ fontFamily: 'IBM Plex Mono, monospace' }}>
                    {card.label}
                  </p>
                  <p className="mt-2 text-2xl font-semibold" style={{ fontFamily: 'Space Grotesk, sans-serif' }}>
                    {card.value}
                  </p>
                  <p className="mt-1 text-xs opacity-90">{card.detail}</p>
                </article>
              ))}
            </div>
          </div>

          <section className="mt-5">
            <article className="rounded-3xl border border-slate-700/70 bg-[#0d1a26] p-4">
              <div className="flex items-center justify-between">
                <h2 className="text-lg font-semibold text-slate-50" style={{ fontFamily: 'Space Grotesk, sans-serif' }}>
                  Cron Execution Pipeline
                </h2>
                <span className="rounded-full border border-sky-300/40 bg-sky-200/15 px-2 py-1 text-xs text-sky-100">
                  timeout-safe chunks
                </span>
              </div>
              <div className="mt-4 overflow-x-auto">
                <table className="w-full min-w-[760px] border-separate border-spacing-y-2 text-left text-sm">
                  <thead className="text-[11px] uppercase tracking-[0.18em] text-slate-400" style={{ fontFamily: 'IBM Plex Mono, monospace' }}>
                    <tr>
                      <th className="px-3 py-1">Cron</th>
                      <th className="px-3 py-1">Cadence</th>
                      <th className="px-3 py-1">Role</th>
                      <th className="px-3 py-1">SLA</th>
                      <th className="px-3 py-1">P95</th>
                      <th className="px-3 py-1">Last</th>
                      <th className="px-3 py-1">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {cronRows.map((row) => (
                      <tr key={row.id} className="rounded-2xl bg-slate-900/65 text-slate-100">
                        <td className="rounded-l-xl px-3 py-3 font-medium">{row.id}</td>
                        <td className="px-3 py-3 text-slate-300">{row.cadence}</td>
                        <td className="px-3 py-3 text-slate-200/90">{row.role}</td>
                        <td className="px-3 py-3 text-slate-300">{row.sla}</td>
                        <td className="px-3 py-3 text-slate-300">{row.p95}</td>
                        <td className="px-3 py-3 text-slate-300">{row.lastRun}</td>
                        <td className="rounded-r-xl px-3 py-3">
                          <span className={`rounded-full border px-2 py-1 text-xs ${statusClasses(row.status)}`}>{row.status}</span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </article>

          </section>

          <section className="mt-5 rounded-3xl border border-slate-700/70 bg-[#0d1a26] p-4">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold text-slate-50" style={{ fontFamily: 'Space Grotesk, sans-serif' }}>
                Deployment Registry and Forward Validation
              </h2>
              <span className="rounded-full border border-cyan-300/40 bg-cyan-200/10 px-2 py-1 text-xs text-cyan-100">
                source: data/scalp-deployments.json
              </span>
            </div>
            <div className="mt-4 overflow-x-auto">
              <table className="w-full min-w-[960px] border-separate border-spacing-y-2 text-left text-sm">
                <thead className="text-[11px] uppercase tracking-[0.18em] text-slate-400" style={{ fontFamily: 'IBM Plex Mono, monospace' }}>
                  <tr>
                    <th className="px-3 py-1">Deployment</th>
                    <th className="px-3 py-1">Symbol</th>
                    <th className="px-3 py-1">Strategy</th>
                    <th className="px-3 py-1">Tune</th>
                    <th className="px-3 py-1">Forward Exp</th>
                    <th className="px-3 py-1">Profitable %</th>
                    <th className="px-3 py-1">Max DD (R)</th>
                    <th className="px-3 py-1">Guardrail</th>
                    <th className="px-3 py-1">Promotion</th>
                  </tr>
                </thead>
                <tbody>
                  {deploymentRows.map((row) => (
                    <tr key={row.deploymentId} className="bg-slate-900/65 text-slate-100">
                      <td className="rounded-l-xl px-3 py-3 font-medium">{row.deploymentId}</td>
                      <td className="px-3 py-3">{row.symbol}</td>
                      <td className="px-3 py-3 text-slate-300">{row.strategy}</td>
                      <td className="px-3 py-3 text-slate-300">{row.tune}</td>
                      <td className={`px-3 py-3 ${row.forwardExp >= 0 ? 'text-emerald-200' : 'text-rose-200'}`}>
                        {row.forwardExp.toFixed(2)}
                      </td>
                      <td className="px-3 py-3">{row.profitableWindowsPct}%</td>
                      <td className="px-3 py-3">{row.maxDdR.toFixed(1)}</td>
                      <td className="px-3 py-3 text-slate-300">{row.guardrail}</td>
                      <td className="rounded-r-xl px-3 py-3">
                        <span
                          className={`rounded-full border px-2 py-1 text-xs ${
                            row.promotionEligible
                              ? 'border-emerald-300/40 bg-emerald-200/15 text-emerald-100'
                              : 'border-rose-300/40 bg-rose-200/15 text-rose-100'
                          }`}
                        >
                          {row.promotionEligible ? 'eligible' : 'blocked'}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="mt-5 grid grid-cols-1 gap-5 2xl:grid-cols-2">
            <article className="rounded-3xl border border-slate-700/70 bg-[#0d1a26] p-4">
              <h2 className="text-lg font-semibold text-slate-50" style={{ fontFamily: 'Space Grotesk, sans-serif' }}>
                Blocked Berlin Hours Falsification
              </h2>
              <p className="mt-1 text-xs text-slate-300">Result should hold across walk-forward windows, not just aggregate.</p>
              <div className="mt-4 overflow-x-auto">
                <table className="w-full min-w-[560px] border-separate border-spacing-y-2 text-left text-sm">
                  <thead className="text-[11px] uppercase tracking-[0.18em] text-slate-400" style={{ fontFamily: 'IBM Plex Mono, monospace' }}>
                    <tr>
                      <th className="px-3 py-1">Variant</th>
                      <th className="px-3 py-1">Best windows</th>
                      <th className="px-3 py-1">Expectancy</th>
                      <th className="px-3 py-1">PnL (R)</th>
                      <th className="px-3 py-1">Interpretation</th>
                    </tr>
                  </thead>
                  <tbody>
                    {blockedHoursRows.map((row) => (
                      <tr key={row.variant} className="bg-slate-900/65">
                        <td className="rounded-l-xl px-3 py-3">{row.variant}</td>
                        <td className="px-3 py-3">{row.windowsBest}/18</td>
                        <td className="px-3 py-3">{row.expectancy.toFixed(2)}</td>
                        <td className="px-3 py-3">{row.pnlR.toFixed(1)}</td>
                        <td className="rounded-r-xl px-3 py-3 text-slate-300">{row.variant === 'none' ? 'strong contender' : 'inconclusive'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </article>

            <article className="rounded-3xl border border-slate-700/70 bg-[#0d1a26] p-4">
              <h2 className="text-lg font-semibold text-slate-50" style={{ fontFamily: 'Space Grotesk, sans-serif' }}>
                Execution Stress Ladder
              </h2>
              <p className="mt-1 text-xs text-slate-300">Monotonic deterioration should hold under harsher assumptions.</p>
              <div className="mt-4 overflow-x-auto">
                <table className="w-full min-w-[560px] border-separate border-spacing-y-2 text-left text-sm">
                  <thead className="text-[11px] uppercase tracking-[0.18em] text-slate-400" style={{ fontFamily: 'IBM Plex Mono, monospace' }}>
                    <tr>
                      <th className="px-3 py-1">Scenario</th>
                      <th className="px-3 py-1">Net R</th>
                      <th className="px-3 py-1">Expectancy</th>
                      <th className="px-3 py-1">PF</th>
                      <th className="px-3 py-1">Max DD</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stressRows.map((row) => (
                      <tr key={row.scenario} className="bg-slate-900/65">
                        <td className="rounded-l-xl px-3 py-3">{row.scenario}</td>
                        <td className="px-3 py-3">{row.netR.toFixed(1)}</td>
                        <td className="px-3 py-3">{row.expectancy.toFixed(2)}</td>
                        <td className="px-3 py-3">{row.pf.toFixed(2)}</td>
                        <td className="rounded-r-xl px-3 py-3">{row.dd.toFixed(1)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </article>
          </section>
        </div>
      </main>
    </>
  );
}
