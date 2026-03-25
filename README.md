# DBSQL Optimization Workshop

Hands-on enablement workshop for Databricks SQL query optimization using HLS (Health/Life Sciences) payer data schemas.

Based on the [Databricks Optimize Data Workloads Guide](https://www.databricks.com/discover/pages/optimize-data-workloads-guide).

## Workshop Structure

Each query follows a "see it break → understand why → fix it" loop using the DBSQL Query Profile.

### Data Schema

| Table | Rows | Description |
|-------|------|-------------|
| `members` | ~5M | Health plan member enrollment and demographics (partitioned by state) |
| `claims` | ~50M | Medical claims transactions (partitioned by service_date) |
| `claim_diagnoses` | ~150M | Claim-level diagnosis detail (multi-dx per claim) |
| `providers` | ~800K | Provider master reference |
| `drug_claims` | ~30M | Pharmacy claims (partitioned by fill_date) |
| `icd10_lookup` | ~75K | ICD-10 diagnosis code reference |

All tables include column comments and PHI/PII governed tags.

### Anti-Pattern Coverage

| Query | HLS Use Case | Anti-Pattern | Profile Signal | Fix |
|-------|-------------|--------------|----------------|-----|
| Q1 | PMPM Cost Reporting | SELECT *, no date filter | Large FileScan bytes | Column pruning + predicate pushdown + BROADCAST |
| Q2 | Chronic Condition Analysis | Skewed join on ICD codes | Few tasks hanging, max >> median shuffle | BROADCAST small lookup + AQE skew join |
| Q3 | Utilization Management | Correlated subquery | Repeated scan subtrees, disk spill | CTE rewrite + shuffle partition tuning |
| Q4 | Readmission Ranking | Window on high-cardinality text | 800K Exchange partitions | PARTITION BY ID + ZORDER + ANALYZE TABLE |

### Facilitation Flow (per query)

1. **Run unoptimized** — Execute the bad query, note runtime
2. **Open profile** — Query History → See Query Profile
3. **Identify bottleneck** — Largest node by time/bytes (red/orange)
4. **Diagnose anti-pattern** — Map node type to issue
5. **Fix** — Apply optimization from the optimized query
6. **Rerun optimized** — Compare runtime side-by-side
7. **Quantify improvement** — % reduction in bytes scanned + elapsed time

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
