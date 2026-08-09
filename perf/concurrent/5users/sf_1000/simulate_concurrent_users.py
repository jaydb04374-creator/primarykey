#!/usr/bin/env python3
"""
Simulate N concurrent users running the same TPC-DS query set against a single
Databricks SQL warehouse, to observe queueing/contention effects under load.

Each "user" is a Python thread with its own SQL session (own session_id) against
the target warehouse/catalog/schema, with result caching disabled. Each user runs
through the full query list once, sequentially within their own thread, while all
threads run in parallel — so the warehouse sees genuine concurrent query traffic.

For each run we capture the same metrics as the sequential benchmarks (execution
time, data scanned, rows read/returned, estimated DBUs) PLUS a queueing metric:
  queue_time_ms = metrics.queue_end_time_ms - query_start_time_ms
which is the time a query spent waiting for a warehouse execution slot before it
started running — the metric that actually reveals concurrency contention.

Usage:
    python3 simulate_concurrent_users.py

Configuration is set via the constants below.
"""
import json
import subprocess
import time
import csv
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration -----------------------------------------------------------
WAREHOUSE_ID = "27ca8b6378a7f82e"
CATALOG = "samples"
SCHEMA = "tpcds_sf1000"
QUERIES = ["q24a", "q24b", "q34", "q39a", "q39b", "q52", "q64", "q72", "q82", "q95", "q99"]
QUERY_DIR = "/Users/rajdb/performance/tpcds_queries"
NUM_USERS = 5
RUNS_PER_USER = 1  # each user runs the full query set this many times
DBU_RATE_2XSMALL_PER_HOUR = 4.0
MAX_POLL_ITERS = 900  # sf1000 queries (q24a/b especially) can take minutes each
POLL_SLEEP = 2.0

OUT_DIR = "/Users/rajdb/performance/concurrent/5users/sf_1000"
OUT_CSV = os.path.join(OUT_DIR, "raw_runs.csv")
OUT_JSON = os.path.join(OUT_DIR, "raw_runs.json")
FIELDNAMES = [
    "user_id", "query", "run", "statement_id", "state",
    "submit_epoch_rel_s", "end_epoch_rel_s", "wall_clock_s",
    "execution_time_ms", "queue_time_ms", "total_time_ms", "compilation_time_ms",
    "data_scanned_bytes", "rows_read", "rows_returned",
    "spill_to_disk_bytes", "result_from_cache", "warehouse_size",
    "dbus_consumed_estimated",
]

results_lock = threading.Lock()
all_results = []
batch_start_epoch = None


def api_post(path, payload):
    r = subprocess.run(
        ["databricks", "api", "post", path, "--json", json.dumps(payload), "-o", "json"],
        capture_output=True, text=True,
    )
    try:
        return json.loads(r.stdout)
    except Exception:
        return {"error": r.stdout + r.stderr}


def api_get(path):
    r = subprocess.run(
        ["databricks", "api", "get", path, "-o", "json"],
        capture_output=True, text=True,
    )
    try:
        return json.loads(r.stdout)
    except Exception:
        return {"error": r.stdout + r.stderr}


def create_session():
    payload = {"warehouse_id": WAREHOUSE_ID, "catalog": CATALOG, "schema": SCHEMA}
    resp = api_post("/api/2.0/sql/sessions", payload)
    return resp["session_id"]


def disable_cache(session_id):
    payload = {
        "warehouse_id": WAREHOUSE_ID, "session_id": session_id,
        "statement": "SET use_cached_result = false", "wait_timeout": "30s",
    }
    api_post("/api/2.0/sql/statements", payload)


def run_one_query(user_id, session_id, query, run_num, sql):
    payload = {
        "warehouse_id": WAREHOUSE_ID, "session_id": session_id, "statement": sql,
        "wait_timeout": "0s", "format": "JSON_ARRAY", "disposition": "INLINE",
    }
    submit_epoch = time.time()
    resp = api_post("/api/2.0/sql/statements", payload)
    stmt_id = resp.get("statement_id")
    state = resp.get("status", {}).get("state")

    if stmt_id:
        for _ in range(MAX_POLL_ITERS):
            if state in ("SUCCEEDED", "FAILED", "CANCELED"):
                break
            time.sleep(POLL_SLEEP)
            d = api_get(f"/api/2.0/sql/statements/{stmt_id}")
            state = d.get("status", {}).get("state")
    end_epoch = time.time()

    rows_returned = None
    m = {}
    query_start_time_ms = None
    if stmt_id and state == "SUCCEEDED":
        final = api_get(f"/api/2.0/sql/statements/{stmt_id}")
        manifest = final.get("manifest", {})
        rows_returned = manifest.get("total_row_count")

        hist = api_get(
            f"/api/2.0/sql/history/queries?filter_by.statement_ids={stmt_id}"
            f"&include_metrics=true&max_results=5"
        )
        res = hist.get("res", [])
        if res:
            entry = res[0]
            m = entry.get("metrics", {})
            query_start_time_ms = entry.get("query_start_time_ms")

    exec_ms = m.get("execution_time_ms")
    queue_end_ms = m.get("queue_end_time_ms")
    queue_time_ms = None
    if queue_end_ms is not None and query_start_time_ms is not None:
        queue_time_ms = queue_end_ms - query_start_time_ms

    dbu_est = (
        round((exec_ms / 1000.0 / 3600.0) * DBU_RATE_2XSMALL_PER_HOUR, 6)
        if exec_ms is not None else None
    )

    row = {
        "user_id": user_id, "query": query, "run": run_num,
        "statement_id": stmt_id, "state": state,
        "submit_epoch_rel_s": round(submit_epoch - batch_start_epoch, 3),
        "end_epoch_rel_s": round(end_epoch - batch_start_epoch, 3),
        "wall_clock_s": round(end_epoch - submit_epoch, 2),
        "execution_time_ms": exec_ms,
        "queue_time_ms": queue_time_ms,
        "total_time_ms": m.get("total_time_ms"),
        "compilation_time_ms": m.get("compilation_time_ms"),
        "data_scanned_bytes": m.get("read_bytes"),
        "rows_read": m.get("rows_read_count"),
        "rows_returned": rows_returned,
        "spill_to_disk_bytes": m.get("spill_to_disk_bytes"),
        "result_from_cache": m.get("result_from_cache"),
        "warehouse_size": "2X-Small",
        "dbus_consumed_estimated": dbu_est,
    }

    with results_lock:
        all_results.append(row)
        with open(OUT_CSV, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=FIELDNAMES).writerow(row)
        with open(OUT_JSON, "w") as f:
            json.dump(all_results, f, indent=2)

    print(
        f"[user{user_id}] {query} run{run_num}: state={state} exec={exec_ms}ms "
        f"queue={queue_time_ms}ms bytes={row['data_scanned_bytes']} "
        f"rows_ret={rows_returned} t={row['submit_epoch_rel_s']}s->{row['end_epoch_rel_s']}s",
        flush=True,
    )
    return row


def user_workload(user_id):
    """One simulated user: own session, runs the full query list RUNS_PER_USER times."""
    session_id = create_session()
    disable_cache(session_id)
    print(f"[user{user_id}] session ready: {session_id}", flush=True)

    sqls = {}
    for q in QUERIES:
        with open(os.path.join(QUERY_DIR, f"{q}.sql")) as f:
            sqls[q] = f.read()

    for run_num in range(1, RUNS_PER_USER + 1):
        for q in QUERIES:
            run_one_query(user_id, session_id, q, run_num, sqls[q])


def main():
    global batch_start_epoch
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_CSV, "w", newline="") as f:
        csv.DictWriter(f, fieldnames=FIELDNAMES).writeheader()

    batch_start_epoch = time.time()
    print(f"Starting {NUM_USERS} concurrent users x {RUNS_PER_USER} run(s) x {len(QUERIES)} queries", flush=True)

    with ThreadPoolExecutor(max_workers=NUM_USERS) as pool:
        futures = [pool.submit(user_workload, uid) for uid in range(1, NUM_USERS + 1)]
        for fut in as_completed(futures):
            fut.result()  # re-raise any exception

    batch_end_epoch = time.time()
    print(f"ALL DONE total_wall_s={round(batch_end_epoch - batch_start_epoch, 2)}", flush=True)


if __name__ == "__main__":
    main()
