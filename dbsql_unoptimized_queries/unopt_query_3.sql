-- =============================================================================
-- UNOPTIMIZED QUERY 3: High-Cost Admissions Above Plan Average (UM Review)
-- =============================================================================
-- Business Question: Identify members with high-cost IP admissions above their
--                    plan's average inpatient cost for utilization management.
--
-- Anti-patterns demonstrated:
--   1. Correlated subquery re-executes per outer row — even Photon's vectorized
--      execution cannot optimize away the repeated scans
--   2. Inner subquery re-materializes claims table multiple times — serverless
--      AQE cannot reuse intermediate results across correlated iterations
--   3. No date filter — full claims history scanned, bypassing Predictive I/O
--      and any liquid clustering pruning on service_date
--   4. Serverless auto-scales compute but the correlated pattern serializes
--      the work, negating the elasticity benefit
--
-- DBSQL Serverless Query Profile indicators to look for:
--   - Repeated Exchange/Sort nodes: subquery re-materializes identical scans
--   - Spill metrics: "Disk Bytes Spilled" on shuffle stages — even with
--     serverless auto-tuned shuffle partitions, correlated subqueries
--     create unpredictable intermediate data sizes
--   - Wall clock time: dominated by inner aggregation running per outer row
--   - Photon nodes show fast per-batch processing but total time is high
--     due to repeated invocations
-- =============================================================================

SELECT
  c.member_id,
  c.claim_id,
  c.paid_amount,
  c.service_date
FROM serverless_stable_swv01_catalog.dbsql_opt.claims c
WHERE c.claim_type = 'IP'
  AND c.paid_amount > (
    SELECT AVG(c2.paid_amount)
    FROM serverless_stable_swv01_catalog.dbsql_opt.claims c2
    JOIN serverless_stable_swv01_catalog.dbsql_opt.members m2
      ON c2.member_id = m2.member_id
    WHERE c2.claim_type = 'IP'
      AND m2.plan_id = (
        SELECT m3.plan_id
        FROM serverless_stable_swv01_catalog.dbsql_opt.members m3
        WHERE m3.member_id = c.member_id
      )
  );
