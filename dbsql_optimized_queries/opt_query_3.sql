-- =============================================================================
-- OPTIMIZED QUERY 3: High-Cost Admissions Above Plan Average (Serverless)
-- =============================================================================
-- Business Question: Identify members with high-cost IP admissions above their
--                    plan's average inpatient cost for utilization management.
--
-- Serverless-native optimizations applied:
--
--   1. CTE REWRITE: Replaces correlated subquery with a single-pass CTE.
--      This is the #1 optimization regardless of compute type — correlated
--      subqueries defeat ALL serverless auto-optimizations (AQE, Photon
--      vectorization, Predictive I/O) because they force row-by-row execution.
--
--   2. SERVERLESS AUTO-TUNED SHUFFLES: No need for
--      SET spark.sql.shuffle.partitions = 800. Serverless AQE automatically:
--      (a) starts with a reasonable partition count based on data volume,
--      (b) coalesces small partitions post-shuffle,
--      (c) splits large partitions that would cause spill.
--      Manual SET commands on serverless are either ignored or can actually
--      DEGRADE performance by overriding the auto-tuner.
--
--   3. LIQUID CLUSTERING + PREDICTIVE I/O: Date filter leverages liquid
--      clustering on service_date (set up in Q1). Predictive I/O uses the
--      cluster metadata plus learned skip patterns to minimize I/O.
--
--   4. PHOTON VECTORIZED JOIN: The CTE rewrite enables Photon to execute
--      the plan_avg → claims join as a vectorized hash join instead of the
--      row-by-row correlated lookup. This is where Photon shines — batch
--      processing with SIMD-style operations.
--
-- Expected serverless improvements:
--   - Eliminates repeated scan subtrees (correlated → single-pass CTE)
--   - No disk spill: serverless auto-sizes shuffle partitions to fit memory
--   - Photon vectorized hash join replaces row-by-row correlated lookup
--   - 70-90% wall-clock time reduction
-- =============================================================================

-- No SET commands needed — serverless auto-tunes shuffle partitions via AQE

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

-- NOTE on serverless behavior:
-- - The CTE materializes once and is reused — Photon caches it in memory
-- - AQE auto-coalesces the small plan_avg_ip_cost result (500 plans)
--   into a single partition for efficient broadcast-style join
-- - ORDER BY leverages Photon's vectorized top-K sort
-- - Serverless result cache: identical query on unchanged data returns instantly
-- - Compare Query Profile: single scan subtree vs. repeated scans in unoptimized
