-- =============================================================================
-- UNOPTIMIZED QUERY 3: High-Cost Admissions Above Plan Average (UM Review)
-- =============================================================================
-- Business Question: Identify members with high-cost IP admissions above their
--                    plan's average inpatient cost for utilization management.
--
-- Anti-patterns demonstrated:
--   1. Correlated subquery re-executes per outer row (catastrophic at scale)
--   2. Inner subquery re-materializes claims table multiple times
--   3. No shuffle partition tuning — default 200 partitions for 50M rows
--      causes massive spill to disk
--   4. No date filter — scans full claims history
--
-- Query Profile indicators to look for:
--   - Repeated Exchange/Sort nodes: subquery re-materializes identical scans
--   - Spill metrics: high "Disk Bytes Spilled" on shuffle stages
--   - Wall clock time dominated by inner aggregation running per outer row
--   - spark.sql.shuffle.partitions=200 vs actual data volume mismatch
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
