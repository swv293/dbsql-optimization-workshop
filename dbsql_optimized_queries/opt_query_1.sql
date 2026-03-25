-- =============================================================================
-- OPTIMIZED QUERY 1: PMPM Cost by Line of Business (Serverless)
-- =============================================================================
-- Business Question: What is per-member-per-month (PMPM) medical cost by LOB?
--
-- Serverless-native optimizations applied:
--
--   1. LIQUID CLUSTERING (run once): Replaces legacy PARTITIONED BY (service_date)
--      with liquid clustering on (service_date, claim_status). Serverless Photon
--      engine co-locates data by these keys incrementally — no full rewrite needed.
--      Predictive I/O then uses cluster metadata for automatic file skipping.
--
--   2. COLUMN PRUNING: Only select needed columns (lob, service_date, member_id,
--      paid_amount) instead of SELECT *. Photon's columnar reader skips entire
--      column chunks it doesn't need — but only if you don't ask for *.
--
--   3. PREDICATE PUSHDOWN + PREDICTIVE I/O: Date filter on service_date enables
--      two layers of skipping: (a) liquid cluster pruning at the file level,
--      (b) Predictive I/O's learned data skipping within files. Combined, this
--      can skip 60-80% of files before reading a single byte.
--
--   4. BROADCAST HINT: providers (800K rows) is broadcast. Serverless auto-broadcast
--      often handles this, but explicit hints guarantee it regardless of stats age.
--
--   5. ANALYZE TABLE (run once): Feeds fresh statistics to the serverless CBO so
--      it can make optimal join order and strategy decisions automatically.
--
-- Expected serverless improvements:
--   - 80-90% reduction in bytes scanned (column pruning + cluster pruning)
--   - Photon vectorized aggregation on the pruned dataset
--   - Serverless disk cache warms on second run for near-instant repeated queries
-- =============================================================================

-- Step 1 (run once): Migrate claims from legacy partitioning to liquid clustering
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.claims
  CLUSTER BY (service_date, claim_status);

-- Step 2 (run once): Trigger initial clustering and refresh statistics
OPTIMIZE serverless_stable_swv01_catalog.dbsql_opt.claims;
ANALYZE TABLE serverless_stable_swv01_catalog.dbsql_opt.claims COMPUTE STATISTICS FOR ALL COLUMNS;

-- Step 3: Optimized PMPM query — let serverless do the heavy lifting
SELECT
  m.lob,
  DATE_TRUNC('month', c.service_date)                        AS service_month,
  COUNT(DISTINCT c.member_id)                                 AS member_count,
  SUM(c.paid_amount)                                          AS total_paid,
  SUM(c.paid_amount) / COUNT(DISTINCT c.member_id)            AS pmpm
FROM serverless_stable_swv01_catalog.dbsql_opt.claims c
JOIN serverless_stable_swv01_catalog.dbsql_opt.members m
  ON c.member_id = m.member_id
JOIN /*+ BROADCAST(p) */ serverless_stable_swv01_catalog.dbsql_opt.providers p
  ON c.provider_id = p.provider_id
WHERE c.service_date BETWEEN '2023-01-01' AND '2023-12-31'
  AND c.claim_status = 'PAID'
GROUP BY m.lob, DATE_TRUNC('month', c.service_date);

-- NOTE on serverless behavior:
-- - No SET commands needed: serverless auto-tunes shuffle partitions via AQE
-- - Photon handles the COUNT(DISTINCT) with vectorized hash aggregation
-- - Second execution benefits from serverless result cache (if data unchanged)
-- - Predictive I/O learns from this query's access pattern for future runs
