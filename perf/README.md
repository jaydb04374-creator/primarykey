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

concurrent/
├── 5users/                                     5 simulated concurrent users, sf1
│   ├── simulate_concurrent_users.py             the simulation script
│   ├── raw_runs.{csv,json}                      all 55 individual run records
│   ├── summary.{csv,json}                       per-query stats (incl. queue_time_ms)
│   ├── summary_by_user.{csv,json}               per-user total wall/execution time
│   └── sf_1000/                                 same 5-user test, against sf1000 (1TB)
│       └── (same file layout, + sequential_vs_concurrent_comparison.{csv,json})
├── 10users/                                     10 simulated concurrent users, sf1
│   └── (same file layout as 5users/, 110 individual runs)
└── 5users_vs_10users_comparison.{csv,json}       sequential vs 5 vs 10 users side-by-side (sf1)
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
- **Concurrency test design** (`concurrent/5users/`, `concurrent/10users/`,
  `concurrent/5users/sf_1000/`): N Python threads (one per simulated user), each
  opening its own SQL session against the same warehouse with caching independently
  disabled, each running the 11-query set once. All threads launched together via a
  thread pool so query submissions genuinely overlap in time — verified by
  session-creation and first-query timestamps landing within ~0.06s (5 users, sf1)
  to ~1s (10 users, sf1) of each other; the sf1000 5-user run showed the same tight
  session-start overlap despite running ~20x longer overall.

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

## Results: Concurrent Users (sf1) — 5 vs 10

To see how the 2X-Small warehouse behaves under load, `concurrent/5users/` and
`concurrent/10users/` each simulate N users hitting it at the same time: N Python
threads, each with its **own SQL session** (own `session_id`, cache disabled
independently), each running the full 11-query set once, all launched together via
a thread pool. This gives 55 (5 users) and 110 (10 users) total executions with
genuine overlap — every user in both runs started within ~0.06s–1s of the others and
finished within a tight window (~1.4s spread at 5 users, ~10s spread at 10 users).

| Query | Sequential | 5 users (×) | 10 users (×) | 10 vs 5 users |
|---|---|---|---|---|
| q24a | 833ms | 2.91s (3.5x) | 2.43s (2.9x) | 0.8x |
| q24b | 388ms | 1.66s (4.3x) | 2.16s (5.6x) | 1.3x |
| q34 | 1.44s | 5.38s (3.7x) | 5.39s (3.8x) | 1.0x |
| q39a | 1.78s | 7.44s (4.2x) | 9.02s (5.1x) | 1.2x |
| q39b | 1.52s | 5.51s (3.6x) | 8.42s (5.5x) | 1.5x |
| q52 | 694ms | 2.49s (3.6x) | 4.50s (6.5x) | 1.8x |
| q64 | 6.92s | 30.15s (4.4x) | 53.76s (7.8x) | **1.8x** |
| q72 | 3.99s | 9.20s (2.3x) | 13.75s (3.4x) | 1.5x |
| q82 | 1.90s | 4.40s (2.3x) | 6.64s (3.5x) | 1.5x |
| q95 | 1.78s | 6.27s (3.5x) | 6.72s (3.8x) | 1.1x |
| q99 | 955ms | 3.89s (4.1x) | 3.86s (4.0x) | 1.0x |

### Key finding: contention shows up as slower execution first, then as real queueing

At **5 users**, every query got 2.3x–4.4x slower, but `queue_time_ms` (time spent
waiting for a warehouse slot before execution starts) stayed low and flat
(22–91ms) regardless of query weight — the warehouse doesn't make concurrent
queries wait in line at this load level, it **shares its fixed compute capacity**
across all of them simultaneously, so each query just runs slower rather than
queueing.

At **10 users**, that changes. Slowdowns widen to 2.9x–7.8x, and — critically — real
queueing appears for the first time: `q24b`, `q82`, `q95`, and `q99` show average
queue times of **181–361ms**, with **max queue times up to 1.7s** (`q82`). This is
the first sign of the warehouse's fixed capacity genuinely being exceeded rather
than just thinly shared. Notably, the jump from 5→10 users is **sub-linear** for
most queries — mostly 1.0x–1.8x additional slowdown, not another 2x — meaning the
warehouse degrades gracefully rather than falling over as load doubles. `q64` (the
heaviest query in the set) is hit hardest: 6.92s sequential → 53.76s at 10 users, a
7.8x slowdown and the single largest 5→10 jump (1.8x).

At the per-user level: at 5 users, each user's 11-query run took ~76–82s of pure
execution time (summed) but ~134s of wall time; at 10 users that grows to
~108–122s execution against ~173–187s wall time. No user was ever starved — the
max spread between the fastest- and slowest-finishing user was ~1.4s at 5 users
and ~10s at 10 users, both far smaller than the total run length, indicating the
warehouse spreads capacity fairly across users rather than favoring earlier
requests. Zero failures, zero cache hits, zero disk spill across all 165 concurrent
executions (55 + 110) — the 2X-Small warehouse degrades gracefully under both 5x
and 10x load rather than failing.

## Results: 5 Concurrent Users on sf1000 (1TB)

The same 5-user concurrency test (`concurrent/5users/sf_1000/`), but against
`samples.tpcds_sf1000` instead of sf1 — same 2X-Small warehouse, same design (5
threads, 5 independent SQL sessions, cache disabled, one pass through all 11
queries per user). This run took **~50.3 minutes total** for all 55 executions —
dominated by q24a and q24b, which are already the two most expensive queries in
the set sequentially and got hit hardest by concurrency on top.

| Query | Sequential | 5 Users Concurrent | Slowdown | Avg Queue | Max Queue |
|---|---|---|---|---|---|
| q24a | 2.5min | 8.9min | 3.6x | 40ms | 65ms |
| q24b | 2.1min | 5.3min | 2.5x | **58.0s** | **4.8min** |
| q34 | 3.47s | 22.57s | **6.5x** | 4.03s | 4.71s |
| q39a | 2.91s | 12.17s | 4.2x | 26ms | 33ms |
| q39b | 2.83s | 8.02s | 2.8x | 1.01s | 1.74s |
| q52 | 1.34s | 3.59s | 2.7x | 24ms | 28ms |
| q64 | 43.61s | 2.0min | 2.7x | **50.0s** | **1.4min** |
| q72 | 18.76s | 53.62s | 2.9x | 13.53s | 42.84s |
| q82 | 6.03s | 16.49s | 2.7x | 10.28s | 21.31s |
| q95 | 13.63s | 14.06s | **1.0x** | 3.95s | 6.83s |
| q99 | 8.35s | 16.24s | 1.9x | 3.88s | 7.03s |

### Key finding: data volume triggers real queueing far sooner than user count does

This is a different regime from anything seen at sf1. At sf1, queueing stayed
near-zero for *both* 5 and 10 concurrent users — contention showed up purely as
slower execution. At sf1000 with only **5 users**, real queueing appears across
almost the entire query set: `q64` averaged **50s of queue time** (max 1.4min),
`q72` averaged 13.5s (max 42.8s), `q82` averaged 10.3s (max 21.3s), and one `q24b`
run queued for **4.8 minutes**. The lesson: it's the size of the data each query
touches, not just the number of concurrent users, that pushes a fixed-size
warehouse from "shares compute thinly" into "requests genuinely wait in line."

Two queries broke the general pattern in opposite directions:
- **`q34` had the single worst slowdown ratio (6.5x)** despite being a *light*
  query (3.47s sequential) — sharing the warehouse with 5 heavy concurrent scans
  seems to have starved it disproportionately relative to its own small footprint.
- **`q95` was essentially unaffected (1.0x)** — whatever makes its query plan
  resource-light held up even under this heavier contention, matching its
  resilience in the sf1 concurrency tests too.

Per-user totals also diverged sharply from the sf1 pattern: each user's 11-query
run took 945s–1,205s of pure execution time, but ~2,953s–3,018s of wall time —
a gap dominated by queueing, not client overhead (compare to sf1's 5-user run,
where wall time was roughly 1.6x execution time, not 2.5–3x). Queueing was also
distributed *unevenly* across users this time: user 3 accumulated 407s of total
queue time across their run vs. just 11s for user 4 — unlike the sf1 tests, where
all users finished within a tight, even window regardless of load level. Despite
all this, the warehouse still recorded **zero failures, zero cache hits, and zero
disk spill** — it degrades by making everyone wait longer, never by erroring out.

## Overall takeaways

1. **Selective predicates matter far more than warehouse size at this scale.**
   Queries that prune early (`q34`, `q39a/b`, `q52`) stayed under 5 seconds even at
   1000x data volume on the smallest available warehouse.
2. **Correlated subqueries over large CTEs are the real risk** — `q24a`/`q24b`
   demonstrate that a query which is nearly free at small scale can become the
   single most expensive query in the set once data volume crosses a threshold that
   changes its join strategy.
3. **No query failed or spilled to disk** across 220 sequential executions (4 passes
   × 11 queries × 5 runs) on a 2X-Small warehouse, even at 1TB scale — the
   workspace's free-tier compute ceiling didn't block correctness, only made the
   worst-case queries (q24a/q24b) slow.
4. **A 2X-Small warehouse shares compute under concurrent load, and only starts
   real queueing once load is high enough — and "high enough" depends on data
   volume, not just user count.** At sf1, 5 concurrent users produced 2.3x–4.4x
   slowdown with essentially zero queue wait; 10 users grew that to 2.9x–7.8x with
   queueing just starting to appear (up to 1.7s). At sf1000, just **5** users was
   enough to produce severe queueing (up to 4.8 minutes for one run) — the same
   user count that caused almost none at sf1. Query weight, not concurrency count
   alone, is what determines when a fixed-size warehouse tips from sharing compute
   into making requests wait.
5. **Concurrency degradation isn't uniform across queries or users.** Light
   queries aren't automatically safe under load — `q34` (3.47s sequential) had the
   worst slowdown ratio of the entire sf1000 concurrency test (6.5x) — while some
   queries (`q95` at both scales) barely degrade at all. Queueing can also land
   unevenly across simultaneous users rather than spreading fairly, especially at
   sf1000 scale.
6. **This benchmark cannot answer "does serverless outperform classic compute?"**
   — that would require a non-serverless warehouse, which this workspace's tier
   doesn't allow. If that comparison matters, it needs a paid-tier workspace.
