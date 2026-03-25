-- =============================================================================
-- UNOPTIMIZED QUERY 1: PMPM Cost by Line of Business
-- =============================================================================
-- Business Question: What is per-member-per-month (PMPM) medical cost by LOB?
--
-- Anti-patterns demonstrated:
--   1. SELECT * pulls ALL columns from 3 tables (massive I/O)
--   2. No date filter on claims = full table scan of 50M rows
--   3. No predicate pushdown opportunity
--   4. SortMergeJoin on providers (800K rows) instead of broadcast
--   5. GROUP BY references columns not in SELECT *
--
-- Query Profile indicators to look for:
--   - FileScan node: "Bytes Read" shows full table size (~50M rows x all cols)
--   - High rows output at Scan dropping sharply at a late Filter node
--   - SortMergeJoin node on providers: small table shuffle-joined
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
