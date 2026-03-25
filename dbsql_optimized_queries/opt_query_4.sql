-- =============================================================================
-- OPTIMIZED QUERY 4: Provider Readmission Rate Ranking (Serverless)
-- =============================================================================
-- Business Question: Rank providers by readmission rate within specialty
--                    for Stars/quality reporting.
--
-- Serverless-native optimizations applied:
--
--   1. LIQUID CLUSTERING (replaces Z-ORDER): Cluster providers on (specialty,
--      state) instead of legacy OPTIMIZE ... ZORDER BY. Liquid clustering is
--      incremental — new data is automatically clustered on write without
--      full table rewrites. Predictive I/O uses the cluster metadata to
--      skip files not matching specialty filters.
--
--   2. PARTITION BY provider_id (not provider_name): Uses low-cardinality ID
--      instead of high-cardinality free-text. Even with serverless AQE
--      auto-coalescing tiny partitions, 800K unique provider_name values
--      create massive scheduling overhead that cannot be fully optimized away.
--
--   3. PRE-AGGREGATE BEFORE WINDOW: CTE computes admit stats per provider_id
--      first (~80K distinct providers with IP claims), then applies RANK()
--      on the much smaller aggregated result. Photon's vectorized window
--      function runs on 80K rows instead of millions.
--
--   4. BROADCAST HINT: providers (800K rows) is broadcast. With fresh stats
--      from ANALYZE TABLE, serverless CBO may auto-broadcast, but the hint
--      guarantees it.
--
--   5. ANALYZE TABLE (replaces COMPUTE STATISTICS): Feeds the serverless CBO
--      with fresh column-level statistics including histograms. This enables
--      optimal join ordering and accurate cardinality estimates.
--
--   6. DATE FILTER + LIQUID CLUSTERING: service_date filter on claims leverages
--      the liquid clustering set up in Q1 for efficient pruning.
--
-- Expected serverless improvements:
--   - Liquid clustering enables Predictive I/O file skipping on specialty
--   - Pre-aggregation: window function processes 80K rows, not 50M
--   - Photon vectorized window/sort on the small aggregated dataset
--   - AQE auto-sizes shuffle partitions for the provider_id window
-- =============================================================================

-- Step 1 (run once): Liquid clustering on providers — replaces legacy Z-ORDER
-- Unlike Z-ORDER which requires periodic manual OPTIMIZE runs, liquid clustering
-- automatically reorganizes data on new writes. Incremental, not full-rewrite.
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.providers
  CLUSTER BY (specialty, state);

OPTIMIZE serverless_stable_swv01_catalog.dbsql_opt.providers;
ANALYZE TABLE serverless_stable_swv01_catalog.dbsql_opt.providers COMPUTE STATISTICS FOR ALL COLUMNS;

-- Step 2: Optimized readmission ranking query
WITH admit_base AS (
  SELECT
    c.provider_id,
    c.service_date,
    c.claim_type,
    ROW_NUMBER() OVER (
      PARTITION BY c.provider_id    -- low-cardinality ID, not free-text name
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
  RANK() OVER (PARTITION BY p.specialty                         -- rank within specialty
               ORDER BY ps.readmissions / ps.total_admits DESC) AS specialty_rank
FROM provider_stats ps
JOIN /*+ BROADCAST(p) */ serverless_stable_swv01_catalog.dbsql_opt.providers p
  ON ps.provider_id = p.provider_id;

-- NOTE on serverless behavior:
-- - Liquid clustering vs Z-ORDER: LC is incremental and write-time optimized;
--   Z-ORDER requires periodic full-table OPTIMIZE runs. LC is the serverless way.
-- - AQE auto-sizes shuffle partitions for the provider_id window function
-- - Photon vectorized sort for the LAG/ROW_NUMBER windows
-- - Predictive I/O learns that specialty + state are commonly filtered columns
-- - Compare Query Profile: look for "cluster pruning" in the FileScan node
--   details on the providers table (not present in unoptimized version)
