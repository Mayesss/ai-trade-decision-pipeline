// One-off migration for the lesson scope ladder (2026-08): every lesson was
// being written straight to GLOBAL scope, which let two-loss patterns veto all
// entries everywhere. New rule: lessons enter at SYMBOL scope and earn
// promotion through cross-symbol/cross-class reinforcement. This script
// applies the ladder retroactively — each active lesson is rescoped by the
// DISTINCT origin symbols of its source post-mortems:
//   1 symbol                    → scope 'symbol'  (that symbol)
//   >1 symbols, one asset class → scope 'asset_class'
//   >1 asset classes            → scope 'global'  (earned it for real)
// Provenance columns (symbol = first origin, asset_class) are populated in
// all cases so future promotions can compare origins.
// Also drops the orphaned swing.refusal_outcomes table from the discarded
// scoreboard design. Run: node --import tsx scripts/rescope-lessons.ts
import { scalpPrisma, isScalpPgConfigured } from '../lib/db/client';
import { sql } from '../lib/db/sql';
import { resolveSwingCategory } from '../lib/swing/category';
import type { AnalysisPlatform } from '../lib/platform';

async function main() {
    if (!isScalpPgConfigured()) throw new Error('PG not configured (source .env.local)');
    const db = scalpPrisma();

    const lessons = await db.$queryRaw<Array<any>>(sql`
        SELECT id, scope, lesson, source_postmortem_ids
        FROM swing.lessons
        WHERE status = 'active'
        ORDER BY id;
    `);
    console.log(`${lessons.length} active lessons`);

    for (const lesson of lessons) {
        const sourceIds: number[] = (Array.isArray(lesson.source_postmortem_ids)
            ? lesson.source_postmortem_ids
            : JSON.parse(String(lesson.source_postmortem_ids || '[]'))
        )
            .map((n: unknown) => Number(n))
            .filter((n: number) => Number.isFinite(n) && n > 0);
        const origins = sourceIds.length
            ? await db.$queryRaw<Array<{ symbol: string; platform: string }>>(sql`
                  SELECT DISTINCT symbol, platform FROM swing.postmortems
                  WHERE id IN (SELECT jsonb_array_elements_text(${JSON.stringify(sourceIds)}::jsonb)::bigint);
              `)
            : [];
        if (!origins.length) {
            console.log(`  #${lesson.id}: no resolvable origin post-mortems — left as-is (${lesson.scope})`);
            continue;
        }
        const symbols = [...new Set(origins.map((o) => String(o.symbol).toUpperCase()))];
        const classes = [
            ...new Set(
                origins.map((o) =>
                    resolveSwingCategory({ symbol: o.symbol, platform: o.platform as AnalysisPlatform }),
                ),
            ),
        ];
        const scope = symbols.length === 1 ? 'symbol' : classes.length === 1 ? 'asset_class' : 'global';
        const symbol = symbols[0];
        const assetClass = classes[0] ?? null;
        await db.$executeRaw(sql`
            UPDATE swing.lessons
            SET scope = ${scope}, symbol = ${symbol}, asset_class = ${assetClass}
            WHERE id = ${Number(lesson.id)};
        `);
        console.log(
            `  #${lesson.id}: ${lesson.scope} → ${scope} (origins: ${symbols.join(',')} | classes: ${classes.join(',')})`,
        );
    }

    await db.$executeRaw(sql`DROP TABLE IF EXISTS swing.refusal_outcomes;`);
    console.log('dropped swing.refusal_outcomes (discarded scoreboard design)');
}

main()
    .then(() => process.exit(0))
    .catch((err) => {
        console.error('rescope failed:', err);
        process.exit(1);
    });
