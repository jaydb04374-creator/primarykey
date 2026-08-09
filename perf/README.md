# TPC-DS Performance Benchmarks

TPC-DS v2.4 queries (from [databricks/spark-sql-perf](https://github.com/databricks/spark-sql-perf/tree/master/src/main/resources/tpcds_2_4))
benchmarked against Databricks SQL warehouses on the `samples.tpcds_sf1` (1GB) and
`samples.tpcds_sf1000` (1TB) sample datasets.

## Contents

- `tpcds_queries/` — the 11 query files used: `q24a`, `q24b`, `q34`, `q39a`, `q39b`,
  `q52`, `q64`, `q72`, `q82`, `q95`, `q99`.
- `results/sf_1/` — 5 runs per query against `samples.tpcds_sf1`.
- `results/sf_1000/` — 5 runs per query against `samples.tpcds_sf1000`.
- `results/sf1_vs_sf1000_comparison.{csv,json}` — side-by-side comparison of the two scales.

Each `raw_runs.csv`/`.json` holds per-run records; `summary.csv`/`.json` holds the
5-run averages per query.

## Methodology

- **Warehouse:** 2X-Small Serverless SQL warehouse (workspace tier limit — Medium+
  not available on this account).
- **Result caching disabled** per session (`SET use_cached_result = false`) so every
  run reflects genuine re-execution, not a cache hit.
- **Metrics captured per run:** execution time (server-side, from Databricks query
  history), data scanned (bytes read), rows read, rows returned, and an *estimated*
  DBU cost (`execution_time_hours × 4 DBU/hr`, the published 2X-Small rate — exact
  per-query billing isn't available in real time via `system.billing.usage`).

## Key finding

`q24a`/`q24b` scale worst going from sf1 → sf1000: ~178x–328x execution time increase
for 1000x more data, driven by a correlated `HAVING` subquery over a materialized CTE
that becomes a shuffle/broadcast bottleneck at scale. Most other queries scale
sub-linearly thanks to selective predicates. See `results/sf1_vs_sf1000_comparison.csv`
for the full breakdown.
