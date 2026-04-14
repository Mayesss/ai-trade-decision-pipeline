import { runScalpV2ResearchJob } from '../lib/scalp-v2/pipeline';

const start = Date.now();
const interval = setInterval(() => {
  process.stderr.write(`... ${((Date.now() - start) / 1000).toFixed(1)}s\n`);
}, 5000);

async function main() {
  const result = await runScalpV2ResearchJob({ batchSize: 50 });
  clearInterval(interval);
  console.log(JSON.stringify({
    elapsed: ((Date.now() - start) / 1000).toFixed(1) + 's',
    ok: result.ok,
    busy: result.busy,
    processed: result.processed,
    succeeded: result.succeeded,
    pendingAfter: result.pendingAfter,
    details: result.details,
  }, null, 2));
  process.exit(0);
}

main().catch((err) => { clearInterval(interval); console.error(err); process.exit(1); });
