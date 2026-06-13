/**
 * Requeue stale composer candidates so the next research bulk re-evaluates them
 * to the CURRENT completed-week window (refreshing worker stage-C evidence so
 * they can clear the promotion freshness gate).
 *
 * The weekly rollover only requeues candidates ONE week stale
 * (previousWindowOnly). Promote-eligible passers whose evidence is months old
 * (e.g. the 2026-03 stage-C passers) are never refreshed by it — this targets
 * exactly those.
 *
 * Default: dry-run, stage-C passers only. Flags:
 *   --apply           actually requeue (mutates candidate rows -> status=discovered)
 *   --all             all stale candidates, not just stage-C passers
 *   --deployed-only   only candidates with a linked deployment (default: all passers)
 *   --enabled-only    restrict to enabled deployments (implies --deployed-only)
 *
 * Usage:
 *   node scripts/with-db-env.mjs node --import tsx scripts/scalp-composer-requeue-stale.ts
 *   node scripts/with-db-env.mjs node --import tsx scripts/scalp-composer-requeue-stale.ts --apply
 */
import { requeueScalpComposerDeploymentCandidatesForWindow } from '../lib/scalp/composer/db';
import { isScalpPgConfigured } from '../lib/scalp/pg/client';
import { resolveScalpComposerCompletedWeekWindowToUtc } from '../lib/scalp/composer/weekWindows';

async function main() {
  if (!isScalpPgConfigured()) {
    console.error('PG not configured');
    process.exit(1);
  }
  const apply = process.argv.includes('--apply');
  const stageCPassedOnly = !process.argv.includes('--all');
  const enabledOnly = process.argv.includes('--enabled-only');
  // Default: requeue ALL matching candidates, not only deployment-linked ones —
  // stage-C passers that never promoted have no deployment to join on.
  const requireDeployment = process.argv.includes('--deployed-only') || enabledOnly;
  const windowToTs = resolveScalpComposerCompletedWeekWindowToUtc(Date.now());

  const opts = {
    windowToTs,
    previousWindowOnly: false, // catch months-stale rows, not just last week
    stageCPassedOnly,
    requireDeployment,
    includeDisabledDeployments: !enabledOnly,
    reasonCode: 'SCALP_COMPOSER_REQUEUE_STALE_PASSERS',
  };

  console.log(
    JSON.stringify({
      windowTo: new Date(windowToTs).toISOString().slice(0, 10),
      scope: stageCPassedOnly ? 'stage_c_passers_only' : 'all_stale',
      requireDeployment,
      enabledOnly,
      mode: apply ? 'APPLY' : 'dry-run',
    }),
  );

  const wouldRequeue = await requeueScalpComposerDeploymentCandidatesForWindow({
    ...opts,
    dryRun: true,
  });
  console.log(`stale candidates matching = ${wouldRequeue}`);

  if (!apply) {
    console.log('dry-run only — re-run with --apply to requeue them to "discovered".');
    process.exit(0);
  }

  const requeued = await requeueScalpComposerDeploymentCandidatesForWindow(opts);
  console.log(`requeued ${requeued} candidates to "discovered" for window ${new Date(windowToTs).toISOString().slice(0, 10)}.`);
  console.log('Next composer bulk will re-evaluate them to the current week.');
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
