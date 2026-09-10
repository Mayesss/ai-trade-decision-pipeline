-- Lesson library + post-mortem queue repair — 2026-09-10
-- Companion to docs/week-one-review-2026-09-10.md §3/§5 and the code changes of
-- the same day (ENTRY_SL_MIN_ATR floor, post-mortem twin guard, sibling lessons
-- for the analyst, stale-running reclaim).
--
-- (1) Twin repair: five lessons counted a Capital close twice (snapshot row +
--     transaction row → two post-mortems). The duplicate post-mortem id is
--     removed from the sources and support/origin counts are decremented.
--     Confidence is left as written (the reinforce path's confidence update is
--     not reconstructible).
-- (2) Promote #41 (UK100, "never market-enter a reclaim on the reclaim tick",
--     the highest-support row of a rule minted on six instruments) to global.
-- (3) Retire 11 rows whose content is the entry stop floor now enforced in code
--     (same logic as lessons-reset-2026-09-02.sql: a lesson that re-imposes a
--     rule code enforces is redundant), and the 3 symbol copies of #41.
-- (4) Post-mortems: 12 rows stuck in 'running' since 09-08/09 (dead workers,
--     no reclaim existed) → failed; 2 queued transaction-row twins of a queued
--     snapshot row → failed (one evaluation per close).
--
-- Rows before the change (for the UNDO below):
--   {"id":"26","scope":"symbol","symbol":"XRPUSDT","asset_class":"crypto","status":"active","support_count":2,"origin_counts":{"win":1,"loss":1},"source_postmortem_ids":[882,919,956],"lesson":"If an invalidation swing (low for longs, high for shorts) was swept and reclaimed within the last 2 primary bars, rest y"}
--   {"id":"31","scope":"symbol","symbol":"GOLD","asset_class":"commodity","status":"active","support_count":1,"origin_counts":{"refusal":1},"source_postmortem_ids":[898,933,1041],"lesson":"Reclaim/bounce stop: clear the DEEPER of the sweep extreme and the prior-two-day wick/structural low (prior-day high/low"}
--   {"id":"41","scope":"symbol","symbol":"UK100","asset_class":"index","status":"active","support_count":6,"origin_counts":{"loss":2,"refusal":4},"source_postmortem_ids":[943,927,939,944,948,996,1065],"lesson":"Never market-enter a reclaim on the reclaim tick: require reclaimed_minutes_ago >= 10 or a micro close back above the le"}
--   {"id":"44","scope":"symbol","symbol":"US500","asset_class":"index","status":"active","support_count":3,"origin_counts":{"loss":2,"refusal":1},"source_postmortem_ids":[931,952,968,969],"lesson":"Never rest a stop <0.5 primary-ATR beyond a swept extreme in a both-extremes-swept tape (invalidation = primary CLOSE th"}
--   {"id":"45","scope":"symbol","symbol":"EURUSD","asset_class":"forex","status":"active","support_count":2,"origin_counts":{"loss":2},"source_postmortem_ids":[934,1094],"lesson":"Fade a swept session extreme only after a confirmed reclaim (lower-TF close back through the level); skip it if the sess"}
--   {"id":"46","scope":"symbol","symbol":"DE40","asset_class":"index","status":"active","support_count":1,"origin_counts":{"refusal":1},"source_postmortem_ids":[936,974],"lesson":"A confirmed session_reclaim (held ≥10 min) with a ≤0.8-ATR swept-extreme stop clears a MARKET bounce only if no cash/ses"}
--   {"id":"47","scope":"symbol","symbol":"US100","asset_class":"index","status":"active","support_count":3,"origin_counts":{"win":1,"loss":1,"refusal":1},"source_postmortem_ids":[947,1059,1092],"lesson":"No market entry on a sweep/reclaim trigger held <30m while primary is range/inside with chop_risk and the last three 4H "}
--   {"id":"48","scope":"symbol","symbol":"NATURALGAS","asset_class":"commodity","status":"active","support_count":2,"origin_counts":{"loss":2},"source_postmortem_ids":[949,1116],"lesson":"Breakdown-short stops go ≥0.25 primary-ATR beyond the voiding resistance (primary swing-pivot), never just above the nea"}
--   {"id":"51","scope":"symbol","symbol":"BGBUSDT","asset_class":"crypto","status":"active","support_count":2,"origin_counts":{"loss":1,"refusal":1},"source_postmortem_ids":[951,1075],"lesson":"If your invalidation stop would sit under ~0.6 primary-ATR from the fill, do not pay market: move the stop beyond the ne"}
--   {"id":"52","scope":"symbol","symbol":"GOLD","asset_class":"commodity","status":"active","support_count":1,"origin_counts":{"loss":1},"source_postmortem_ids":[955],"lesson":"Fading into a supply/demand zone: stop must clear the zone's far boundary and nearest liquidity extreme (prior-day high/"}
--   {"id":"55","scope":"symbol","symbol":"GOLD","asset_class":"commodity","status":"active","support_count":1,"origin_counts":{"loss":1},"source_postmortem_ids":[960,1040],"lesson":"Don't void a swept-level fade on ONE primary close beyond the level. A 2nd close confirms acceptance only if it advances"}
--   {"id":"59","scope":"symbol","symbol":"AVAXUSDT","asset_class":"crypto","status":"active","support_count":1,"origin_counts":{"loss":1},"source_postmortem_ids":[1007],"lesson":"Place stops ≥1 primary-ATR beyond the level they defend and ≥0.8 primary-ATR from entry — the bracket must survive one f"}
--   {"id":"60","scope":"symbol","symbol":"LINKUSDT","asset_class":"crypto","status":"active","support_count":2,"origin_counts":{"loss":1,"refusal":1},"source_postmortem_ids":[1008,1083],"lesson":"Set the stop at least 0.5 primary-ATR from a resting entry AND below the most recent swing-low wick; a stop under 0.3 pr"}
--   {"id":"63","scope":"symbol","symbol":"GBPUSD","asset_class":"forex","status":"active","support_count":3,"origin_counts":{"loss":2,"refusal":1},"source_postmortem_ids":[1030,1037,1093],"lesson":"Buy a swept-level reclaim only after it has held ≥30 min or a primary bar has CLOSED beyond the level; if the opposite c"}
--   {"id":"64","scope":"symbol","symbol":"SOLUSDT","asset_class":"crypto","status":"active","support_count":1,"origin_counts":{"loss":1},"source_postmortem_ids":[1042],"lesson":"On breakout entries, never rest the stop inside a liquidity-grab zone: if the base was swept ≥3 times in the last 24h, p"}
--   {"id":"65","scope":"symbol","symbol":"SILVER","asset_class":"commodity","status":"active","support_count":2,"origin_counts":{"loss":2},"source_postmortem_ids":[1048,1082],"lesson":"Rest the SL ≥0.25 primary-ATR past any shelf (or below the prior-day low) swept ≥2 times in the last two sessions, and n"}
--   {"id":"70","scope":"symbol","symbol":"ETHUSDT","asset_class":"crypto","status":"active","support_count":2,"origin_counts":{"loss":1,"refusal":1},"source_postmortem_ids":[1077,1134],"lesson":"In a primary range (break_state=inside, no BOS), buffer a breakout stop at least 0.3 primary-ATR beyond the pivot or req"}
--   postmortem {"id":"940","status":"running","error":null}
--   postmortem {"id":"965","status":"running","error":null}
--   postmortem {"id":"967","status":"running","error":null}
--   postmortem {"id":"976","status":"running","error":null}
--   postmortem {"id":"998","status":"running","error":null}
--   postmortem {"id":"1003","status":"running","error":null}
--   postmortem {"id":"1010","status":"running","error":null}
--   postmortem {"id":"1012","status":"running","error":null}
--   postmortem {"id":"1013","status":"running","error":null}
--   postmortem {"id":"1026","status":"running","error":null}
--   postmortem {"id":"1027","status":"running","error":null}
--   postmortem {"id":"1129","status":"running","error":null}
--   postmortem {"id":"1138","status":"queued","error":null}
--   postmortem {"id":"1149","status":"queued","error":null}

-- APPLY (run in the Neon console inside one transaction; nothing here deletes rows):
BEGIN;
UPDATE swing.lessons SET source_postmortem_ids = source_postmortem_ids - '943', support_count = support_count - 1, origin_counts = jsonb_set(origin_counts, '{loss}', to_jsonb(GREATEST(0, coalesce((origin_counts->>'loss')::int,0) - 1))) WHERE id = 41 AND source_postmortem_ids @> '[943]'::jsonb;
UPDATE swing.lessons SET source_postmortem_ids = source_postmortem_ids - '968', support_count = support_count - 1, origin_counts = jsonb_set(origin_counts, '{loss}', to_jsonb(GREATEST(0, coalesce((origin_counts->>'loss')::int,0) - 1))) WHERE id = 44 AND source_postmortem_ids @> '[968]'::jsonb;
UPDATE swing.lessons SET source_postmortem_ids = source_postmortem_ids - '1094', support_count = support_count - 1, origin_counts = jsonb_set(origin_counts, '{loss}', to_jsonb(GREATEST(0, coalesce((origin_counts->>'loss')::int,0) - 1))) WHERE id = 45 AND source_postmortem_ids @> '[1094]'::jsonb;
UPDATE swing.lessons SET source_postmortem_ids = source_postmortem_ids - '1040', support_count = support_count - 0 WHERE id = 55 AND source_postmortem_ids @> '[1040]'::jsonb;
UPDATE swing.lessons SET source_postmortem_ids = source_postmortem_ids - '1037', support_count = support_count - 1, origin_counts = jsonb_set(origin_counts, '{loss}', to_jsonb(GREATEST(0, coalesce((origin_counts->>'loss')::int,0) - 1))) WHERE id = 63 AND source_postmortem_ids @> '[1037]'::jsonb;
UPDATE swing.lessons SET scope = 'global', asset_class = NULL WHERE id = 41;
UPDATE swing.lessons SET status = 'retired' WHERE id IN (26, 31, 44, 48, 51, 52, 59, 60, 64, 65, 70, 46, 47, 63);
UPDATE swing.postmortems SET status = 'failed', error = 'dropped_stale_running_2026-09-10' WHERE id IN (940, 965, 967, 976, 998, 1003, 1010, 1012, 1013, 1026, 1027, 1129) AND status = 'running';
UPDATE swing.postmortems SET status = 'failed', error = 'twin_of_1136_dropped_2026-09-10' WHERE id = 1138 AND status = 'queued';
UPDATE swing.postmortems SET status = 'failed', error = 'twin_of_1143_dropped_2026-09-10' WHERE id = 1149 AND status = 'queued';
COMMIT;

-- UNDO:
-- UPDATE swing.lessons SET scope='symbol', asset_class='crypto', status='active', support_count=2, origin_counts='{"win":1,"loss":1}'::jsonb, source_postmortem_ids='[882,919,956]'::jsonb WHERE id=26;
-- UPDATE swing.lessons SET scope='symbol', asset_class='commodity', status='active', support_count=1, origin_counts='{"refusal":1}'::jsonb, source_postmortem_ids='[898,933,1041]'::jsonb WHERE id=31;
-- UPDATE swing.lessons SET scope='symbol', asset_class='index', status='active', support_count=6, origin_counts='{"loss":2,"refusal":4}'::jsonb, source_postmortem_ids='[943,927,939,944,948,996,1065]'::jsonb WHERE id=41;
-- UPDATE swing.lessons SET scope='symbol', asset_class='index', status='active', support_count=3, origin_counts='{"loss":2,"refusal":1}'::jsonb, source_postmortem_ids='[931,952,968,969]'::jsonb WHERE id=44;
-- UPDATE swing.lessons SET scope='symbol', asset_class='forex', status='active', support_count=2, origin_counts='{"loss":2}'::jsonb, source_postmortem_ids='[934,1094]'::jsonb WHERE id=45;
-- UPDATE swing.lessons SET scope='symbol', asset_class='index', status='active', support_count=1, origin_counts='{"refusal":1}'::jsonb, source_postmortem_ids='[936,974]'::jsonb WHERE id=46;
-- UPDATE swing.lessons SET scope='symbol', asset_class='index', status='active', support_count=3, origin_counts='{"win":1,"loss":1,"refusal":1}'::jsonb, source_postmortem_ids='[947,1059,1092]'::jsonb WHERE id=47;
-- UPDATE swing.lessons SET scope='symbol', asset_class='commodity', status='active', support_count=2, origin_counts='{"loss":2}'::jsonb, source_postmortem_ids='[949,1116]'::jsonb WHERE id=48;
-- UPDATE swing.lessons SET scope='symbol', asset_class='crypto', status='active', support_count=2, origin_counts='{"loss":1,"refusal":1}'::jsonb, source_postmortem_ids='[951,1075]'::jsonb WHERE id=51;
-- UPDATE swing.lessons SET scope='symbol', asset_class='commodity', status='active', support_count=1, origin_counts='{"loss":1}'::jsonb, source_postmortem_ids='[955]'::jsonb WHERE id=52;
-- UPDATE swing.lessons SET scope='symbol', asset_class='commodity', status='active', support_count=1, origin_counts='{"loss":1}'::jsonb, source_postmortem_ids='[960,1040]'::jsonb WHERE id=55;
-- UPDATE swing.lessons SET scope='symbol', asset_class='crypto', status='active', support_count=1, origin_counts='{"loss":1}'::jsonb, source_postmortem_ids='[1007]'::jsonb WHERE id=59;
-- UPDATE swing.lessons SET scope='symbol', asset_class='crypto', status='active', support_count=2, origin_counts='{"loss":1,"refusal":1}'::jsonb, source_postmortem_ids='[1008,1083]'::jsonb WHERE id=60;
-- UPDATE swing.lessons SET scope='symbol', asset_class='forex', status='active', support_count=3, origin_counts='{"loss":2,"refusal":1}'::jsonb, source_postmortem_ids='[1030,1037,1093]'::jsonb WHERE id=63;
-- UPDATE swing.lessons SET scope='symbol', asset_class='crypto', status='active', support_count=1, origin_counts='{"loss":1}'::jsonb, source_postmortem_ids='[1042]'::jsonb WHERE id=64;
-- UPDATE swing.lessons SET scope='symbol', asset_class='commodity', status='active', support_count=2, origin_counts='{"loss":2}'::jsonb, source_postmortem_ids='[1048,1082]'::jsonb WHERE id=65;
-- UPDATE swing.lessons SET scope='symbol', asset_class='crypto', status='active', support_count=2, origin_counts='{"loss":1,"refusal":1}'::jsonb, source_postmortem_ids='[1077,1134]'::jsonb WHERE id=70;
-- UPDATE swing.postmortems SET status='running', error=NULL WHERE id=940;
-- UPDATE swing.postmortems SET status='running', error=NULL WHERE id=965;
-- UPDATE swing.postmortems SET status='running', error=NULL WHERE id=967;
-- UPDATE swing.postmortems SET status='running', error=NULL WHERE id=976;
-- UPDATE swing.postmortems SET status='running', error=NULL WHERE id=998;
-- UPDATE swing.postmortems SET status='running', error=NULL WHERE id=1003;
-- UPDATE swing.postmortems SET status='running', error=NULL WHERE id=1010;
-- UPDATE swing.postmortems SET status='running', error=NULL WHERE id=1012;
-- UPDATE swing.postmortems SET status='running', error=NULL WHERE id=1013;
-- UPDATE swing.postmortems SET status='running', error=NULL WHERE id=1026;
-- UPDATE swing.postmortems SET status='running', error=NULL WHERE id=1027;
-- UPDATE swing.postmortems SET status='running', error=NULL WHERE id=1129;
-- UPDATE swing.postmortems SET status='queued', error=NULL WHERE id=1138;
-- UPDATE swing.postmortems SET status='queued', error=NULL WHERE id=1149;

-- ---------------------------------------------------------------------------
-- FOLLOW-UP (2026-09-10, after the block above was applied): the five
-- `source_postmortem_ids - '<id>'` statements were no-ops — jsonb's `- text`
-- operator removes STRING elements, and the ids are stored as numbers — so
-- support/origin counts were corrected but the twin ids stayed in the source
-- lists. Verified state after APPLY: #41 [943,927,939,944,948,996,1065],
-- #44 [931,952,968,969], #45 [934,1094], #55 [960,1040], #63 [1030,1037,1093].
-- This rebuilds each array without the twin id. Counts are NOT touched again.
BEGIN;
UPDATE swing.lessons SET source_postmortem_ids = (SELECT coalesce(jsonb_agg(e), '[]'::jsonb) FROM jsonb_array_elements(source_postmortem_ids) e WHERE e <> '943'::jsonb)  WHERE id = 41;
UPDATE swing.lessons SET source_postmortem_ids = (SELECT coalesce(jsonb_agg(e), '[]'::jsonb) FROM jsonb_array_elements(source_postmortem_ids) e WHERE e <> '968'::jsonb)  WHERE id = 44;
UPDATE swing.lessons SET source_postmortem_ids = (SELECT coalesce(jsonb_agg(e), '[]'::jsonb) FROM jsonb_array_elements(source_postmortem_ids) e WHERE e <> '1094'::jsonb) WHERE id = 45;
UPDATE swing.lessons SET source_postmortem_ids = (SELECT coalesce(jsonb_agg(e), '[]'::jsonb) FROM jsonb_array_elements(source_postmortem_ids) e WHERE e <> '1040'::jsonb) WHERE id = 55;
UPDATE swing.lessons SET source_postmortem_ids = (SELECT coalesce(jsonb_agg(e), '[]'::jsonb) FROM jsonb_array_elements(source_postmortem_ids) e WHERE e <> '1037'::jsonb) WHERE id = 63;
COMMIT;

-- UNDO (follow-up only):
-- UPDATE swing.lessons SET source_postmortem_ids = '[943,927,939,944,948,996,1065]'::jsonb WHERE id = 41;
-- UPDATE swing.lessons SET source_postmortem_ids = '[931,952,968,969]'::jsonb WHERE id = 44;
-- UPDATE swing.lessons SET source_postmortem_ids = '[934,1094]'::jsonb WHERE id = 45;
-- UPDATE swing.lessons SET source_postmortem_ids = '[960,1040]'::jsonb WHERE id = 55;
-- UPDATE swing.lessons SET source_postmortem_ids = '[1030,1037,1093]'::jsonb WHERE id = 63;
