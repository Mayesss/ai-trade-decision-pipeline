-- swing.positions repair — 2026-09-10 (second batch, after the deploy)
-- Companion to docs/week-one-review-2026-09-10.md §8 and the code changes of
-- the same afternoon. Run in the Neon console inside one transaction.
--
-- (1) decision_id → the PLACING order. The old fallback linked a position to
--     the most recent decision within 6h of entry, which for a resting fill is
--     the HOLD tick that observed the fill. New rule (lib/swing/pg.ts
--     upsertSwingPosition): the latest executed BUY/SELL within 48h before
--     entry. 33 of 125 closed positions since 09-02 change; all 125 have such
--     an order. Pairs are id:old>new.
-- (2) USDJPY notional was stored in yen (size × price) — the base leg IS the
--     dollar, so USD notional = size. Five transaction rows; the derived
--     pnl_pct on these rows follows from notional at read time.

BEGIN;
-- (1) decision_id backfill
UPDATE swing.positions SET decision_id = 18269 WHERE id = 1031167;
UPDATE swing.positions SET decision_id = 18490 WHERE id = 1035782;
UPDATE swing.positions SET decision_id = 18484 WHERE id = 1037292;
UPDATE swing.positions SET decision_id = 18475 WHERE id = 1040631;
UPDATE swing.positions SET decision_id = 18626 WHERE id = 1052339;
UPDATE swing.positions SET decision_id = 18616 WHERE id = 1052340;
UPDATE swing.positions SET decision_id = 18556 WHERE id = 1052411;
UPDATE swing.positions SET decision_id = 18707 WHERE id = 1062666;
UPDATE swing.positions SET decision_id = 18698 WHERE id = 1073681;
UPDATE swing.positions SET decision_id = 19477 WHERE id = 1076161;
UPDATE swing.positions SET decision_id = 19473 WHERE id = 1078569;
UPDATE swing.positions SET decision_id = 19640 WHERE id = 1080508;
UPDATE swing.positions SET decision_id = 19646 WHERE id = 1081033;
UPDATE swing.positions SET decision_id = 19647 WHERE id = 1081034;
UPDATE swing.positions SET decision_id = 19677 WHERE id = 1082321;
UPDATE swing.positions SET decision_id = 19646 WHERE id = 1083932;
UPDATE swing.positions SET decision_id = 19733 WHERE id = 1088316;
UPDATE swing.positions SET decision_id = 19772 WHERE id = 1090260;
UPDATE swing.positions SET decision_id = 19775 WHERE id = 1092077;
UPDATE swing.positions SET decision_id = 19843 WHERE id = 1093641;
UPDATE swing.positions SET decision_id = 19941 WHERE id = 1101917;
UPDATE swing.positions SET decision_id = 19978 WHERE id = 1103982;
UPDATE swing.positions SET decision_id = 19975 WHERE id = 1104555;
UPDATE swing.positions SET decision_id = 19734 WHERE id = 1104815;
UPDATE swing.positions SET decision_id = 19930 WHERE id = 1109236;
UPDATE swing.positions SET decision_id = 20012 WHERE id = 1124619;
UPDATE swing.positions SET decision_id = 20138 WHERE id = 1132685;
UPDATE swing.positions SET decision_id = 20243 WHERE id = 1144004;
UPDATE swing.positions SET decision_id = 20261 WHERE id = 1144005;
UPDATE swing.positions SET decision_id = 20261 WHERE id = 1154594;
UPDATE swing.positions SET decision_id = 20451 WHERE id = 1154844;
UPDATE swing.positions SET decision_id = 20216 WHERE id = 1155888;
UPDATE swing.positions SET decision_id = 20484 WHERE id = 1161051;
-- (2) USDJPY notional: yen → USD (= size)
UPDATE swing.positions SET notional = round(notional / entry_price, 2)
 WHERE platform = 'capital' AND symbol = 'USDJPY' AND notional > 1000 AND entry_price > 0
   AND id IN (28223, 30416, 85442, 356544, 1159110);
COMMIT;

-- UNDO:
-- UPDATE swing.positions SET decision_id = 18284 WHERE id = 1031167;
-- UPDATE swing.positions SET decision_id = 18495 WHERE id = 1035782;
-- UPDATE swing.positions SET decision_id = 18476 WHERE id = 1037292;
-- UPDATE swing.positions SET decision_id = 18453 WHERE id = 1040631;
-- UPDATE swing.positions SET decision_id = 18685 WHERE id = 1052339;
-- UPDATE swing.positions SET decision_id = 18674 WHERE id = 1052340;
-- UPDATE swing.positions SET decision_id = 18673 WHERE id = 1052411;
-- UPDATE swing.positions SET decision_id = 18685 WHERE id = 1062666;
-- UPDATE swing.positions SET decision_id = NULL  WHERE id = 1073681;
-- UPDATE swing.positions SET decision_id = 19406 WHERE id = 1076161;
-- UPDATE swing.positions SET decision_id = 19543 WHERE id = 1078569;
-- UPDATE swing.positions SET decision_id = 19631 WHERE id = 1080508;
-- UPDATE swing.positions SET decision_id = 19611 WHERE id = 1081033;
-- UPDATE swing.positions SET decision_id = NULL  WHERE id = 1081034;
-- UPDATE swing.positions SET decision_id = 19648 WHERE id = 1082321;
-- UPDATE swing.positions SET decision_id = 19655 WHERE id = 1083932;
-- UPDATE swing.positions SET decision_id = 19722 WHERE id = 1088316;
-- UPDATE swing.positions SET decision_id = 19750 WHERE id = 1090260;
-- UPDATE swing.positions SET decision_id = 19764 WHERE id = 1092077;
-- UPDATE swing.positions SET decision_id = 19824 WHERE id = 1093641;
-- UPDATE swing.positions SET decision_id = 19902 WHERE id = 1101917;
-- UPDATE swing.positions SET decision_id = 19933 WHERE id = 1103982;
-- UPDATE swing.positions SET decision_id = 19936 WHERE id = 1104555;
-- UPDATE swing.positions SET decision_id = 19723 WHERE id = 1104815;
-- UPDATE swing.positions SET decision_id = NULL  WHERE id = 1109236;
-- UPDATE swing.positions SET decision_id = 20128 WHERE id = 1124619;
-- UPDATE swing.positions SET decision_id = NULL  WHERE id = 1132685;
-- UPDATE swing.positions SET decision_id = 20233 WHERE id = 1144004;
-- UPDATE swing.positions SET decision_id = 20270 WHERE id = 1144005;
-- UPDATE swing.positions SET decision_id = 20270 WHERE id = 1154594;
-- UPDATE swing.positions SET decision_id = 20431 WHERE id = 1154844;
-- UPDATE swing.positions SET decision_id = 20200 WHERE id = 1155888;
-- UPDATE swing.positions SET decision_id = 20459 WHERE id = 1161051;
-- UPDATE swing.positions SET notional = 485713.49999999994 WHERE id = 28223;
-- UPDATE swing.positions SET notional = 484959 WHERE id = 30416;
-- UPDATE swing.positions SET notional = 469576.7 WHERE id = 85442;
-- UPDATE swing.positions SET notional = 16353.800000000001 WHERE id = 356544;
-- UPDATE swing.positions SET notional = 15323.500000000002 WHERE id = 1159110;
