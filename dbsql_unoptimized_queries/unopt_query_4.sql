-- =============================================================================
-- UNOPTIMIZED QUERY 4: Provider Readmission Rate Ranking (Stars/Quality)
-- =============================================================================
-- Business Question: Rank providers by readmission rate across all specialties
--                    for Stars/quality reporting.
--
-- Anti-patterns demonstrated:
--   1. PARTITION BY on provider_name (high-cardinality free-text, 800K uniques)
--      creates 800K tiny shuffle partitions — even with serverless AQE
--      coalescing, this creates massive scheduling overhead
--   2. No liquid clustering on providers table for specialty-based filtering —
--      Predictive I/O has no cluster metadata to skip files
--   3. Full scan of entire claims history (no date filter)
--   4. RANK() window also partitioned by provider_name instead of specialty
--   5. Mixing aggregation with window functions forces Photon to materialize
--      intermediate results unnecessarily
--
-- DBSQL Serverless Query Profile indicators to look for:
--   - Window node: PARTITION BY provider_name = 800K shuffle partitions
--   - Sort stage: entire claims history sorted globally (Photon accelerates
--     sort but volume is the problem)
--   - FileScan on providers: no file/cluster pruning on specialty
--   - Exchange node: high shuffle bytes but tiny per-partition computation
--   - Serverless auto-scale may spin up extra compute but the bottleneck
--     is the sort/shuffle, not compute capacity
-- =============================================================================

SELECT
  p.provider_name,
  p.specialty,
  p.state,
  COUNT(c.claim_id) AS total_admits,
  SUM(CASE WHEN c.claim_type = 'IP'
           AND DATEDIFF(c.service_date,
               LAG(c.service_date) OVER (PARTITION BY p.provider_name
                                         ORDER BY c.service_date)) <= 30
      THEN 1 ELSE 0 END) AS readmissions,
  RANK() OVER (PARTITION BY p.provider_name
               ORDER BY COUNT(c.claim_id) DESC) AS provider_rank
FROM serverless_stable_swv01_catalog.dbsql_opt.claims c
JOIN serverless_stable_swv01_catalog.dbsql_opt.providers p
  ON c.provider_id = p.provider_id
GROUP BY p.provider_name, p.specialty, p.state;
