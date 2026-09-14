// Read-only dump of directional decisions + closed trades.
// UNPOOLED url, BEGIN READ ONLY per transaction (docs/week-one-review §9).
import { writeFileSync } from 'node:fs';
import pg from 'pg';
import nextEnv from '@next/env';
const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

const url = (process.env.NEON__DATABASE_URL_UNPOOLED || '').trim();
const client = new pg.Client({ connectionString: url, ssl: { rejectUnauthorized: false } });
await client.connect();
await client.query('BEGIN READ ONLY');

const decisions = (await client.query(`
  SELECT d.id, d.decided_at_ms, d.symbol, d.platform, d.category, d.action,
         (d.snapshot_json->>'price')::float8                              AS price,
         (d.snapshot_json->'momentumSignals'->>'primaryAtr')::float8      AS atr,
         d.ai_decision_json->>'signal_strength'                           AS signal_strength,
         d.ai_decision_json->>'strategy'                                  AS strategy,
         d.ai_decision_json->>'ai_model'                                  AS ai_model,
         CASE WHEN jsonb_typeof(d.ai_decision_json->'risk_sizing'->'risk_usd')='number'
              THEN (d.ai_decision_json->'risk_sizing'->>'risk_usd')::float8 END AS risk_usd,
         (d.ai_decision_json->>'stop_loss_price')::float8                 AS sl,
         (d.ai_decision_json->>'take_profit_price')::float8               AS tp,
         (d.exec_result_json->>'placed')::bool                            AS placed,
         d.exec_result_json->>'epic'                                      AS epic
  FROM swing.decisions d
  WHERE d.dry_run = false AND d.action IN ('BUY','SELL')
  ORDER BY d.decided_at_ms ASC`)).rows;

const trades = (await client.query(`
  SELECT p.id, p.platform, p.symbol, p.side, p.entry_ts_ms, p.exit_ts_ms,
         p.entry_price, p.exit_price, p.pnl_net, p.pnl_pct, p.decision_id,
         CASE WHEN jsonb_typeof(d.ai_decision_json->'risk_sizing'->'risk_usd')='number'
              THEN (d.ai_decision_json->'risk_sizing'->>'risk_usd')::float8 END AS risk_usd,
         d.ai_decision_json->>'signal_strength' AS signal_strength
  FROM swing.positions p
  LEFT JOIN swing.decisions d ON d.id = p.decision_id
  WHERE p.status='closed' AND p.pnl_net IS NOT NULL AND p.exit_ts_ms IS NOT NULL
  ORDER BY p.exit_ts_ms ASC`)).rows;

await client.query('COMMIT');
await client.end();

writeFileSync('.study/decisions.json', JSON.stringify(decisions));
writeFileSync('.study/trades.json', JSON.stringify(trades));
console.log('decisions', decisions.length, 'trades', trades.length);
console.log('with price+atr', decisions.filter((d) => d.price > 0 && d.atr > 0).length);
