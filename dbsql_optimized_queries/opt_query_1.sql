-- =============================================================================
-- OPTIMIZED QUERY 1: PMPM Cost by Line of Business
-- =============================================================================
-- Business Question: What is per-member-per-month (PMPM) medical cost by LOB?
--
-- Optimizations applied:
--   1. COLUMN PRUNING: Only select needed columns (lob, service_date,
--      member_id, paid_amount) instead of SELECT *
--   2. PREDICATE PUSHDOWN: Date filter on service_date pushes down to
--      partition pruning on the claims table (partitioned by service_date)
--   3. BROADCAST HINT: providers table (800K rows) is broadcast to all
--      executors, eliminating expensive shuffle join
--   4. FILTER BEFORE JOIN: claim_status = 'PAID' filter reduces rows
--      before the join operation
--
-- Expected improvements:
--   - 80-90% reduction in bytes scanned (column pruning + partition pruning)
--   - Elimination of shuffle for providers join (broadcast)
--   - Significant wall-clock time reduction
-- =============================================================================

SELECT
  m.lob,
  DATE_TRUNC('month', c.service_date)                        AS service_month,
  COUNT(DISTINCT c.member_id)                                 AS member_count,
  SUM(c.paid_amount)                                          AS total_paid,
  SUM(c.paid_amount) / COUNT(DISTINCT c.member_id)            AS pmpm
FROM serverless_stable_swv01_catalog.dbsql_opt.claims c
JOIN serverless_stable_swv01_catalog.dbsql_opt.members m
  ON c.member_id = m.member_id
  AND c.service_date BETWEEN '2023-01-01' AND '2023-12-31'
JOIN /*+ BROADCAST(p) */ serverless_stable_swv01_catalog.dbsql_opt.providers p
  ON c.provider_id = p.provider_id
WHERE c.service_date BETWEEN '2023-01-01' AND '2023-12-31'
  AND c.claim_status = 'PAID'
GROUP BY m.lob, DATE_TRUNC('month', c.service_date);
