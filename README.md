# DBSQL Optimization Workshop (Serverless Compute)

Hands-on enablement workshop for Databricks SQL query optimization on **serverless compute** using HLS (Health/Life Sciences) payer data schemas.

Based on the [Databricks Optimize Data Workloads Guide](https://www.databricks.com/discover/pages/optimize-data-workloads-guide).

## Serverless-Specific Optimizations Demonstrated

This workshop highlights optimizations unique to DBSQL Serverless:

| Serverless Feature | How It Applies | Queries |
|-------------------|----------------|---------|
| **Liquid Clustering** | Replaces legacy `PARTITIONED BY` + `Z-ORDER` with incremental, write-time clustering | Q1, Q2, Q4 |
| **Predictive I/O** | Automatic learned data skipping using cluster metadata + access patterns | Q1, Q2, Q3, Q4 |
| **Photon (always on)** | Vectorized execution for joins, aggregations, sorts, and window functions | All |
| **AQE Auto-Tuning** | No manual `SET` commands — shuffle partitions, skew handling, and broadcast decisions are automatic | Q2, Q3 |
| **Serverless Result Cache** | Repeated identical queries on unchanged data return instantly | All |
| **Elastic Auto-Scale** | Compute scales dynamically, but bad query patterns still bottleneck on I/O and shuffles | All |

### Key Serverless Principle

> On serverless, **manual SET commands (shuffle partitions, AQE thresholds) are unnecessary and can actually degrade performance** by overriding the auto-tuner. Focus on: (1) good SQL patterns, (2) liquid clustering, (3) fresh statistics, (4) broadcast hints for small tables.

## Workshop Structure

Each query follows a "see it break → understand why → fix it" loop using the DBSQL Query Profile.

### Data Schema

| Table | Rows | Clustering | Description |
|-------|------|-----------|-------------|
| `members` | ~5M | state (legacy partition) | Health plan member enrollment and demographics |
| `claims` | ~50M | service_date, claim_status (liquid) | Medical claims transactions |
| `claim_diagnoses` | ~150M | claim_id (liquid) | Claim-level diagnosis detail (multi-dx per claim) |
| `providers` | ~800K | specialty, state (liquid) | Provider master reference |
| `drug_claims` | ~30M | fill_date (legacy partition) | Pharmacy claims |
| `icd10_lookup` | ~75K | — | ICD-10 diagnosis code reference (small, broadcast) |

All tables include column comments, table descriptions, and PHI/PII governed tags.

### Anti-Pattern Coverage (Serverless)

| Query | HLS Use Case | Anti-Pattern | Serverless Profile Signal | Serverless Fix |
|-------|-------------|--------------|--------------------------|----------------|
| Q1 | PMPM Cost Reporting | SELECT *, no date filter | Large FileScan bytes, no cluster pruning, Photon bottlenecked on I/O | Column pruning + liquid clustering + Predictive I/O + BROADCAST |
| Q2 | Chronic Condition Analysis | Skewed join on ICD codes | Long-tail tasks (AQE partially mitigates), stale stats prevent auto-broadcast | BROADCAST hint + date filter + liquid clustering on claim_diagnoses + ANALYZE TABLE |
| Q3 | Utilization Management | Correlated subquery | Repeated scan subtrees, spill despite auto-tuned shuffles | CTE rewrite (let serverless AQE auto-tune shuffles) |
| Q4 | Readmission Ranking | Window on high-cardinality text, no liquid clustering | 800K Exchange partitions, no cluster pruning on specialty | PARTITION BY ID + liquid clustering + ANALYZE TABLE |

### Facilitation Flow (per query)

| Step | Action | Tool |
|------|--------|------|
| 1. Run unoptimized | Execute the bad query, note runtime | DBSQL Editor |
| 2. Open profile | Query History → See Query Profile | DBSQL Query Profile |
| 3. Identify bottleneck | Point to largest node by time/bytes (red/orange) — look for Photon vs non-Photon nodes | Query Profile DAG |
| 4. Diagnose anti-pattern | Map node type to issue (FileScan=full scan, Exchange=shuffle, no "cluster pruning"=missing liquid clustering) | Discussion |
| 5. Apply table maintenance | Run `ALTER TABLE ... CLUSTER BY`, `OPTIMIZE`, `ANALYZE TABLE` | DBSQL Editor |
| 6. Run optimized query | Execute the fixed query | DBSQL Editor |
| 7. Compare profiles | Show side-by-side: bytes scanned, cluster pruning %, Photon utilization, elapsed time | Query History |

## Directory Structure

```
setup/                          — Table creation and tag scripts
dbsql_unoptimized_queries/      — Unoptimized queries (unopt_query_1..4)
dbsql_optimized_queries/        — Optimized queries (opt_query_1..4)
```

## Workspace

- **Workspace**: fevm-serverless-stable-swv01
- **Catalog**: `serverless_stable_swv01_catalog`
- **Schema**: `dbsql_opt`
- **Warehouse**: Serverless Starter Warehouse
- **Compute**: DBSQL Serverless (Photon always on, AQE auto-tuned)
