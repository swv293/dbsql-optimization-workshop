-- =============================================================================
-- UNOPTIMIZED QUERY 2: Total Costs by Chronic Condition Category
-- =============================================================================
-- Business Question: What are total costs by chronic condition category?
--
-- Anti-patterns demonstrated:
--   1. Skewed join on high-frequency ICD codes (E11=diabetes, I10=hypertension
--      dominate millions of rows causing massive data skew)
--   2. No skew handling — even with serverless AQE auto-skew detection,
--      extreme skew (30% of 150M rows on 2 codes) can exceed automatic
--      thresholds and still produce long-tail tasks
--   3. icd10_lookup (75K rows) is shuffle-joined — serverless may auto-broadcast
--      this but stale stats can prevent it
--   4. No date filter on claims — scans entire 50M row history, bypassing
--      Predictive I/O data skipping on the service_date cluster key
--   5. No ANALYZE TABLE — serverless CBO relies on fresh statistics to make
--      optimal join strategy decisions
--
-- DBSQL Serverless Query Profile indicators to look for:
--   - Stage timeline: most tasks finish quickly, 2-3 tasks run much longer
--     (AQE may partially mitigate but extreme skew still shows)
--   - SortMergeJoin task metrics: max shuffle read >> median
--   - Exchange node: enormous shuffle bytes from skewed ICD codes
--   - Scan node: no cluster pruning applied (missing date filter)
--   - Photon executes fast per-row but is bottlenecked by I/O volume
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
