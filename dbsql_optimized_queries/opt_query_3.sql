-- =============================================================================
-- OPTIMIZED QUERY 3: High-Cost Admissions Above Plan Average (UM Review)
-- =============================================================================
-- Business Question: Identify members with high-cost IP admissions above their
--                    plan's average inpatient cost for utilization management.
--
-- Optimizations applied:
--   1. CTE REWRITE: Replaces correlated subquery with a single-pass CTE
--      that computes plan-level averages once, then joins back
--   2. SHUFFLE PARTITION TUNING: Increased from default 200 to 800 based on
--      data volume (total_shuffle_bytes / 128MB ≈ 800 for 50M claims)
--   3. DATE FILTER: Restricts to 2023, enabling partition pruning
--   4. ORDERED RESULT: Adds ORDER BY for immediate UM review prioritization
--
-- Expected improvements:
--   - Eliminates repeated scan subtrees (correlated subquery executed ONCE)
--   - Eliminates disk spill by right-sizing shuffle partitions
--   - 70-90% reduction in wall-clock time
-- =============================================================================

SET spark.sql.shuffle.partitions = 800;

WITH plan_avg_ip_cost AS (
  SELECT
    m.plan_id,
    AVG(c.paid_amount) AS avg_ip_cost
  FROM serverless_stable_swv01_catalog.dbsql_opt.claims c
  JOIN serverless_stable_swv01_catalog.dbsql_opt.members m
    ON c.member_id = m.member_id
  WHERE c.claim_type = 'IP'
    AND c.service_date BETWEEN '2023-01-01' AND '2023-12-31'
  GROUP BY m.plan_id
)
SELECT
  c.member_id,
  c.claim_id,
  c.paid_amount,
  c.service_date,
  pa.avg_ip_cost,
  c.paid_amount - pa.avg_ip_cost AS cost_above_avg
FROM serverless_stable_swv01_catalog.dbsql_opt.claims c
JOIN serverless_stable_swv01_catalog.dbsql_opt.members m
  ON c.member_id = m.member_id
JOIN plan_avg_ip_cost pa
  ON m.plan_id = pa.plan_id
WHERE c.claim_type = 'IP'
  AND c.service_date BETWEEN '2023-01-01' AND '2023-12-31'
  AND c.paid_amount > pa.avg_ip_cost
ORDER BY cost_above_avg DESC;
