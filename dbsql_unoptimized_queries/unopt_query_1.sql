-- =============================================================================
-- UNOPTIMIZED QUERY 1: PMPM Cost by Line of Business
-- =============================================================================
-- Business Question: What is per-member-per-month (PMPM) medical cost by LOB?
--
-- Anti-patterns demonstrated:
--   1. SELECT * pulls ALL columns from 3 tables (massive I/O even with Photon)
--   2. No date filter on claims = full table scan of 50M rows — bypasses
--      Predictive I/O's data skipping and any partition/cluster pruning
--   3. No predicate pushdown opportunity for the serverless optimizer
--   4. SortMergeJoin on providers (800K rows) — serverless auto-broadcast
--      threshold may not catch this depending on stats freshness
--   5. GROUP BY references columns not in SELECT * — Photon still must
--      deserialize all columns before discarding them
--
-- DBSQL Serverless Query Profile indicators to look for:
--   - FileScan node: "Bytes Read" shows full table size (~50M rows x all cols)
--     vs. "Bytes Read from Cache" — serverless disk cache won't help on first run
--   - Photon indicator: nodes show Photon execution but I/O dominates
--   - No partition/cluster pruning shown in the Scan node details
--   - SortMergeJoin on providers instead of BroadcastHashJoin
-- =============================================================================

SELECT
  m.*,
  c.*,
  p.*
FROM serverless_stable_swv01_catalog.dbsql_opt.claims c
JOIN serverless_stable_swv01_catalog.dbsql_opt.members m
  ON c.member_id = m.member_id
JOIN serverless_stable_swv01_catalog.dbsql_opt.providers p
  ON c.provider_id = p.provider_id
GROUP BY m.lob, DATE_TRUNC('month', c.service_date);
