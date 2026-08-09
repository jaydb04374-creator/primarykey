# TPC-DS Performance Benchmarks

TPC-DS v2.4 queries (from [databricks/spark-sql-perf](https://github.com/databricks/spark-sql-perf/tree/master/src/main/resources/tpcds_2_4))
benchmarked against Databricks SQL warehouses on the `samples.tpcds_sf1` (1GB) and
`samples.tpcds_sf1000` (1TB) sample datasets.

## Contents

```
perf/
├── tpcds_queries/                              11 query files: q24a, q24b, q34, q39a,
│                                                q39b, q52, q64, q72, q82, q95, q99
└── results/
    ├── sf_1/                                   5 runs/query against samples.tpcds_sf1
    ├── sf_1000/                                5 runs/query against samples.tpcds_sf1000
    ├── sf1_vs_sf1000_comparison.{csv,json}      1GB vs 1TB side-by-side
    ├── serverless/                             sf1 runs, explicitly re-verified serverless
    │   └── sf_1000/                            sf1000 runs, explicitly re-verified serverless
    ├── serverless_vs_2xsmall_sf1_comparison.{csv,json}
    └── serverless_vs_2xsmall_sf1000_comparison.{csv,json}
```

Each `raw_runs.csv`/`.json` holds one row per individual run; `summary.csv`/`.json`
holds the 5-run averages per query (min/max also included).

## Methodology

- **Warehouse:** "Serverless Starter Warehouse" — 2X-Small, `enable_serverless_compute: true`.
  This is a **free-tier Databricks workspace**, which caps SQL warehouse size at 2X-Small
  for both editing an existing warehouse and creating a new one — Medium and larger
  could not be provisioned to compare against a bigger warehouse.
- **Result caching disabled** per session (`SET use_cached_result = false`) so every run
  reflects genuine re-execution, not a cache hit. Verified `result_from_cache: false`
  on all runs across all four benchmark passes (220 total executions).
- **Metrics captured per run**, pulled from the Statement Execution API result manifest
  and Databricks query history (`include_metrics=true`):
  - Execution time (server-side, from query history — not client wall-clock)
  - Data scanned (bytes read)
  - Rows read and rows returned
  - **Estimated** DBU cost: `execution_time_hours × 4 DBU/hr` (the published 2X-Small
    rate). Exact per-query billed DBUs aren't available in real time —
    `system.billing.usage` only updates with hours of latency and couldn't attribute
    cost to individual queries at benchmark time.
- **Known data quirk:** a query-history indexing race caused a minority of runs across
  all passes (worst case 23/55 on one sf1 pass) to initially return no metrics —
  timing hit the API before the run was indexed. All were recovered by re-querying
  after the full batch completed; no runs were dropped or estimated.
- **`wall_clock_s`** (client-observed submit→result time) is unreliable in the
  `serverless/sf_1000` run specifically — several entries show inflated values (up to
  ~1300s) inconsistent with their `execution_time_ms`, most likely polling-subprocess
  overhead on the client side rather than real query behavior. Use `execution_time_ms`
  for all timing analysis, not `wall_clock_s`.

## Results: sf1 (1GB) vs sf1000 (1TB)

5-run averages, 2X-Small warehouse, cache disabled.

| Query | sf1 Exec | sf1000 Exec | Exec × | sf1 Scanned | sf1000 Scanned | Scan × | sf1 Rows Ret | sf1000 Rows Ret |
|---|---|---|---|---|---|---|---|---|
| q24a | 833ms | 148.03s | 177.8x | 308.6KB | 45.3GB | 153915.1x | 0 | 2919 |
| q24b | 388ms | 127.52s | 328.3x | 249.0KB | 44.7GB | 188194.5x | 0 | 322 |
| q34 | 1.44s | 3.47s | 2.4x | 16.5MB | 881.6MB | 53.5x | 218 | 8489 |
| q39a | 1.78s | 2.91s | 1.6x | 2.1MB | 94.4MB | 44.8x | 206 | 14551 |
| q39b | 1.52s | 2.83s | 1.9x | 2.1MB | 94.5MB | 44.8x | 10 | 594 |
| q52 | 694ms | 1.34s | 1.9x | 4.1MB | 496.8MB | 120.7x | 100 | 100 |
| q64 | 6.92s | 43.61s | 6.3x | 215.2MB | 32.3GB | 153.5x | 9 | 12185 |
| q72 | 3.99s | 18.76s | 4.7x | 82.5MB | 4.5GB | 55.6x | 100 | 100 |
| q82 | 1.90s | 6.03s | 3.2x | 113.6MB | 8.1GB | 73.3x | 4 | 19 |
| q95 | 1.78s | 13.63s | 7.7x | 132.7MB | 2.7GB | 20.7x | 1 | 1 |
| q99 | 955ms | 8.35s | 8.7x | 83.9MB | 1.1GB | 13.2x | 90 | 100 |

### Key finding: q24a / q24b scale far worse than everything else

At sf1 they return **0 rows** — no items match `i_color = 'pale'`/`'chiffon'` at that
scale — and scan almost nothing. At sf1000 they scan **45–47GB each** and take
**127–148 seconds**, a **178x–328x** execution-time jump for "only" 1000x more data —
well beyond linear scaling. The likely cause is the correlated
`HAVING sum(netpaid) > (select 0.05*avg(netpaid) from ssales)` subquery: the `ssales`
CTE materializes a large intermediate result that becomes a shuffle/broadcast
bottleneck once the underlying `store_sales`/`store_returns` join is big enough,
and a 2X-Small warehouse doesn't have the parallelism to absorb it.

By contrast, `q39a`/`q39b`/`q52`/`q34` scale **sub-linearly** (1.6x–2.4x time for
1000x data) because their filters (`d_year=2001`, narrow `d_moy`, price bands) prune
most of the data before the expensive joins run. `q64`/`q72`/`q95`/`q99` land in
between (4.7x–8.7x).

## Results: Serverless vs 2X-Small

**Caveat — read this before drawing conclusions:** every run in this benchmark set,
across *both* labels, used the same warehouse (`27ca8b6378a7f82e`, 2X-Small,
`enable_serverless_compute: true`) — there was no separate classic/non-serverless
tier available to compare against (blocked by the free-tier size cap noted above).
"2X-Small" and "Serverless" describe the same compute here. The tables below are two
independent 5-run passes on identical infrastructure, so the deltas reflect
run-to-run variance (warehouse warm-up state, storage-layer caching, background
load) — not a genuine serverless-vs-classic performance difference.

### sf1

| Query | 2X-Small | Serverless | Diff |
|---|---|---|---|
| q24a | 833ms | 823ms | -1.1% |
| q24b | 388ms | 594ms | +52.9% |
| q34 | 1.44s | 1.48s | +2.7% |
| q39a | 1.78s | 2.18s | +22.8% |
| q39b | 1.52s | 1.59s | +4.2% |
| q52 | 694ms | 767ms | +10.5% |
| q64 | 6.92s | 8.11s | +17.2% |
| q72 | 3.99s | 3.25s | -18.6% |
| q82 | 1.90s | 1.63s | -14.3% |
| q95 | 1.78s | 2.12s | +19.6% |
| q99 | 955ms | 1.19s | +24.2% |

### sf1000

| Query | 2X-Small | Serverless | Diff |
|---|---|---|---|
| q24a | 148.03s | 130.03s | -12.2% |
| q24b | 127.52s | 112.65s | -11.7% |
| q34 | 3.47s | 4.51s | +29.8% |
| q39a | 2.91s | 2.92s | +0.4% |
| q39b | 2.83s | 3.10s | +9.6% |
| q52 | 1.34s | 1.43s | +6.4% |
| q64 | 43.61s | 41.86s | -4.0% |
| q72 | 18.76s | 19.66s | +4.8% |
| q82 | 6.03s | 7.05s | +16.8% |
| q95 | 13.63s | 6.44s | -52.7% |
| q99 | 8.35s | 8.51s | +1.9% |

Data scanned was nearly identical between the two passes for every query at both
scales — the query plans behaved consistently, only timing varied. There's no
systematic direction (roughly half the queries got faster, half slower), which is
the signature of noise rather than a real effect. `q24b` (sf1, +53%) and `q95`
(sf1000, −53%) are the largest swings, most likely explained by warehouse
warm-up/JIT-compile timing for whichever query happened to run first/cold in a
session, not the queries themselves changing behavior.

## Overall takeaways

1. **Selective predicates matter far more than warehouse size at this scale.**
   Queries that prune early (`q34`, `q39a/b`, `q52`) stayed under 5 seconds even at
   1000x data volume on the smallest available warehouse.
2. **Correlated subqueries over large CTEs are the real risk** — `q24a`/`q24b`
   demonstrate that a query which is nearly free at small scale can become the
   single most expensive query in the set once data volume crosses a threshold that
   changes its join strategy.
3. **No query failed or spilled to disk** across 220 total executions (4 passes ×
   11 queries × 5 runs) on a 2X-Small warehouse, even at 1TB scale — the workspace's
   free-tier compute ceiling didn't block correctness, only made the worst-case
   queries (q24a/q24b) slow.
4. **This benchmark cannot answer "does serverless outperform classic compute?"**
   — that would require a non-serverless warehouse, which this workspace's tier
   doesn't allow. If that comparison matters, it needs a paid-tier workspace.
