-- =============================================================================
-- UNOPTIMIZED QUERY 2: Total Costs by Chronic Condition Category
-- =============================================================================
-- Business Question: What are total costs by chronic condition category?
--
-- Anti-patterns demonstrated:
--   1. Skewed join on high-frequency ICD codes (E11=diabetes, I10=hypertension
--      dominate millions of rows causing massive data skew)
--   2. No skew handling — few tasks will run 10+ minutes while others finish
--      in seconds
--   3. icd10_lookup (75K rows) is shuffle-joined instead of broadcast
--   4. No date filter on claims — scans entire 50M row history
--
-- Query Profile indicators to look for:
--   - Stage timeline: ~198 tasks finish quickly, 2 tasks run 10+ minutes
--   - SortMergeJoin task metrics: max shuffle read >> median (e.g., 50MB vs 4GB)
--   - Exchange node: enormous shuffle bytes from skewed ICD codes
--   - icd10_lookup being shuffle-joined (should be broadcast at 75K rows)
-- =============================================================================

SELECT
  lkp.category,
  lkp.chronic_flag,
  COUNT(DISTINCT c.member_id) AS affected_members,
  SUM(c.paid_amount)          AS total_cost
FROM serverless_stable_swv01_catalog.dbsql_opt.claims c
JOIN serverless_stable_swv01_catalog.dbsql_opt.claim_diagnoses cd
  ON c.claim_id = cd.claim_id
JOIN serverless_stable_swv01_catalog.dbsql_opt.icd10_lookup lkp
  ON cd.icd10_code = lkp.icd10_code
GROUP BY lkp.category, lkp.chronic_flag;
