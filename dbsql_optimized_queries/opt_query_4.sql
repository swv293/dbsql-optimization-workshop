-- =============================================================================
-- OPTIMIZED QUERY 4: Provider Readmission Rate Ranking (Stars/Quality)
-- =============================================================================
-- Business Question: Rank providers by readmission rate within specialty
--                    for Stars/quality reporting.
--
-- Optimizations applied:
--   1. TABLE MAINTENANCE: Z-ORDER providers on (specialty, state) for
--      file pruning + ANALYZE TABLE for optimizer statistics
--   2. PARTITION BY provider_id: Uses low-cardinality ID instead of
--      high-cardinality free-text provider_name (800K → efficient partitions)
--   3. PRE-AGGREGATE BEFORE WINDOW: CTE computes admit stats per provider,
--      then applies RANK() on the smaller aggregated result
--   4. BROADCAST HINT: providers table (800K) broadcast to avoid shuffle
--   5. DATE FILTER: Restricts to 2023 for partition pruning
--   6. RANK BY SPECIALTY: Window function ranks within specialty, not
--      globally by provider_name
--
-- Expected improvements:
--   - Eliminates 800K Exchange partitions from provider_name windowing
--   - Z-ORDER enables file pruning on specialty-based queries
--   - Pre-aggregation reduces data flowing into the window function
-- =============================================================================

-- Step 1: Run once — Z-ORDER providers on specialty (most common filter/join column)
OPTIMIZE serverless_stable_swv01_catalog.dbsql_opt.providers ZORDER BY (specialty, state);

-- Step 2: Compute statistics for cost-based optimizer decisions
ANALYZE TABLE serverless_stable_swv01_catalog.dbsql_opt.providers COMPUTE STATISTICS FOR ALL COLUMNS;

-- Step 3: Optimized readmission ranking query
WITH admit_base AS (
  SELECT
    c.provider_id,
    c.service_date,
    c.claim_type,
    ROW_NUMBER() OVER (
      PARTITION BY c.provider_id
      ORDER BY c.service_date
    ) AS visit_seq,
    LAG(c.service_date) OVER (
      PARTITION BY c.provider_id
      ORDER BY c.service_date
    ) AS prev_service_date
  FROM serverless_stable_swv01_catalog.dbsql_opt.claims c
  WHERE c.service_date BETWEEN '2023-01-01' AND '2023-12-31'
    AND c.claim_type = 'IP'
),
provider_stats AS (
  SELECT
    provider_id,
    COUNT(*)                                                    AS total_admits,
    SUM(CASE WHEN DATEDIFF(service_date, prev_service_date) <= 30
             THEN 1 ELSE 0 END)                                 AS readmissions
  FROM admit_base
  GROUP BY provider_id
)
SELECT
  p.provider_name,
  p.specialty,
  p.state,
  ps.total_admits,
  ps.readmissions,
  ROUND(ps.readmissions / ps.total_admits * 100, 2)             AS readmit_rate_pct,
  RANK() OVER (PARTITION BY p.specialty
               ORDER BY ps.readmissions / ps.total_admits DESC) AS specialty_rank
FROM provider_stats ps
JOIN /*+ BROADCAST(p) */ serverless_stable_swv01_catalog.dbsql_opt.providers p
  ON ps.provider_id = p.provider_id;
