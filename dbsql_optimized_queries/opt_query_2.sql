-- =============================================================================
-- OPTIMIZED QUERY 2: Total Costs by Chronic Condition Category
-- =============================================================================
-- Business Question: What are total costs by chronic condition category?
--
-- Optimizations applied:
--   1. BROADCAST HINT on icd10_lookup (75K rows): eliminates shuffle join
--      for the small lookup table, sending it to all executors
--   2. AQE SKEW JOIN TUNING: spark.sql.adaptive.skewJoin settings handle
--      the data skew caused by high-frequency ICD codes (E11, I10)
--   3. DATE FILTER: Restricts claims to 2023, leveraging partition pruning
--      on the service_date partitioned claims table
--
-- Expected improvements:
--   - Elimination of the "long tail" tasks caused by skewed ICD codes
--   - Broadcast of lookup eliminates one shuffle stage entirely
--   - Date filter reduces claims scan from 50M to ~17M rows
--
-- Run these SET commands before the query to tune AQE skew handling:
-- SET spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5;
-- SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB;
-- =============================================================================

SET spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5;
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB;

SELECT
  lkp.category,
  lkp.chronic_flag,
  COUNT(DISTINCT c.member_id) AS affected_members,
  SUM(c.paid_amount)          AS total_cost
FROM serverless_stable_swv01_catalog.dbsql_opt.claims c
JOIN serverless_stable_swv01_catalog.dbsql_opt.claim_diagnoses cd
  ON c.claim_id = cd.claim_id
JOIN /*+ BROADCAST(lkp) */ serverless_stable_swv01_catalog.dbsql_opt.icd10_lookup lkp
  ON cd.icd10_code = lkp.icd10_code
WHERE c.service_date BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY lkp.category, lkp.chronic_flag;
