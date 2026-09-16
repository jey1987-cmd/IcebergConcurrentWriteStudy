#!/usr/bin/env python3
"""
Scripted baseline writer: fixed-schedule, deterministic-timing writes to
the shared Iceberg table. One process per role, run concurrently -- this
establishes the frequency-only comparison point against the later
agent-driven writers (same roles, but decision-timed instead of
fixed-interval).

Usage (run once per role, in separate terminals, after start_run.py).
--experiment-condition and --concurrent-writer-count tag every logged
attempt for the conflict-rate-vs-writer-count (n^2 scaling) comparison
across conditions -- e.g. for the "scripted, 3 writers" condition:
    python baseline_writer.py --role result_writer --experiment-condition scripted_3 --concurrent-writer-count 3
    python baseline_writer.py --role telemetry_writer --experiment-condition scripted_3 --concurrent-writer-count 3
    python baseline_writer.py --role quarantine_writer --experiment-condition scripted_3 --concurrent-writer-count 3

Fixed-schedule design: each write lands on a fixed tick grid
(next_tick += interval), not "sleep(interval) after the previous write
finishes" -- so a slow or retried commit doesn't drift the schedule.

Write pattern per role:
    result_writer     -- append (low conflict risk)
    telemetry_writer  -- append (low conflict risk)
    quarantine_writer -- overwrites an existing result_writer row,
                          matched by task_id (high conflict risk -- this
                          is the role actually being tested)

Conflict handling: the raw REST client only auto-retries on auth-expiry,
not on commit conflicts. table.append()/overwrite() do attempt one
internal retry-with-rebase on a 409, but under real concurrent load that
internal retry can itself fail with
ValidationException("Cannot find starting snapshot...") -- observed
directly while testing this script against three writers running
concurrently. So this loop catches that alongside
CommitFailedException/CommitStateUnknownException (confirmed empirically
to be the actual conflict exception BigLake raises) and always reloads
the table fresh via catalog.load_table() before rebuilding and retrying.

Logging: every logical write attempt (whether it succeeded on the first
try, needed retries, or was ultimately abandoned) is logged to the
separate BigQuery write_attempts table -- deliberately independent of
the Iceberg table under test, so logging a write's outcome never itself
contends for the same optimistic-concurrency lock being measured.
"""

import argparse
import datetime
import json
import random
import time
import uuid

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException, ValidationException
from pyiceberg.expressions import EqualTo

from bq_client import ensure_write_attempts_table, get_bq_client, log_attempt
from catalog_client import TABLE_IDENTIFIER, get_catalog, get_row_count

RUN_CONFIG_PATH = "current_run.json"

ROLE_INTERVAL_SECONDS = {
    "result_writer": 10.0,
    "telemetry_writer": 15.0,
    "quarantine_writer": 20.0,
}

MAX_RETRIES = 5
BASE_BACKOFF_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 8.0

ARROW_SCHEMA = pa.schema([
    pa.field("record_id", pa.string(), nullable=False),
    pa.field("experiment_run_id", pa.string(), nullable=False),
    pa.field("write_timestamp", pa.timestamp("us"), nullable=False),
    pa.field("writer_role", pa.string(), nullable=False),
    pa.field("writer_type", pa.string(), nullable=False),
    pa.field("task_id", pa.string(), nullable=True),
    pa.field("result_value", pa.string(), nullable=True),
    pa.field("result_status", pa.string(), nullable=True),
    pa.field("quarantine_reason", pa.string(), nullable=True),
    pa.field("metric_name", pa.string(), nullable=True),
    pa.field("metric_value", pa.float64(), nullable=True),
    pa.field("source_component", pa.string(), nullable=True),
])

EMPTY_PAYLOAD = {
    "task_id": None, "result_value": None, "result_status": None,
    "quarantine_reason": None, "metric_name": None, "metric_value": None,
    "source_component": None,
}


def build_row(record_id, run_id, writer_role, writer_type, payload) -> pa.Table:
    write_ts = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    full = dict(EMPTY_PAYLOAD)
    full.update(payload)
    row = {
        "record_id": [record_id],
        "experiment_run_id": [run_id],
        "write_timestamp": [write_ts],
        "writer_role": [writer_role],
        "writer_type": [writer_type],
        **{k: [v] for k, v in full.items()},
    }
    return pa.table(row, schema=ARROW_SCHEMA)


def pick_target_task_id(table):
    """An existing result_writer row for quarantine_writer to correct.
    Returns None if none exist yet (e.g. very start of a run)."""
    scan = table.scan(row_filter=EqualTo("writer_role", "result_writer"), selected_fields=("task_id",))
    rows = scan.to_arrow().to_pylist()
    if not rows:
        return None
    return random.choice(rows)["task_id"]


def do_append(table, run_id: str, writer_role: str, writer_type: str, n: int, timing: dict) -> str:
    """Returns record_id. timing is unused here -- append roles don't
    scan to pick a target, so there's no scan_duration_ms to report."""
    record_id = str(uuid.uuid4())
    if writer_role == "result_writer":
        payload = {
            "task_id": f"task-{n:06d}",
            "result_value": str(round(random.uniform(0, 1000), 2)),
            "result_status": "success" if random.random() > 0.05 else "failure",
        }
    elif writer_role == "telemetry_writer":
        payload = {
            "metric_name": random.choice(["cpu_pct", "queue_depth", "latency_ms"]),
            "metric_value": round(random.uniform(0, 100), 2),
            "source_component": "pipeline",
        }
    else:
        raise ValueError(f"do_append does not handle role: {writer_role}")
    table.append(build_row(record_id, run_id, writer_role, writer_type, payload))
    return record_id


def do_quarantine_overwrite(table, run_id: str, writer_type: str, timing: dict) -> str | None:
    """Returns record_id, or None for a legitimate no-op (nothing to
    correct yet, not a conflict). Writes timing['scan_duration_ms']
    BEFORE attempting the commit below -- table.overwrite() can raise a
    conflict exception, and if it does, the caller still needs the scan
    cost from THIS attempt (previously lost: scan_duration_ms was only
    ever returned on the happy path, so it came back null for every
    attempt that needed a retry -- which was effectively all of them).

    This is quarantine's structural extra cost: it scans the whole
    table for a target, so this cost is expected to climb with table
    size (confirmed directly: table_row_count_at_attempt vs.
    first_attempt_duration_ms had pearson_r=0.99 over one 20-minute
    run)."""
    scan_start = time.monotonic()
    target_task_id = pick_target_task_id(table)
    timing["scan_duration_ms"] = (time.monotonic() - scan_start) * 1000
    if target_task_id is None:
        return None
    record_id = str(uuid.uuid4())
    payload = {
        "task_id": target_task_id,
        "result_value": str(round(random.uniform(0, 1000), 2)),
        "result_status": "corrected",
        "quarantine_reason": random.choice(["data_quality_format", "late_arrival", "duplicate"]),
    }
    row = build_row(record_id, run_id, "quarantine_writer", writer_type, payload)
    table.overwrite(row, overwrite_filter=EqualTo("task_id", target_task_id))
    return record_id


def commit_with_retry(catalog, do_attempt_fn) -> tuple[str, int, float, float, str | None, int, float | None]:
    """Runs do_attempt_fn(table, timing) -> record_id_or_None, reloading
    the table fresh and retrying on conflict. timing is a fresh dict
    per attempt that do_attempt_fn may write scan_duration_ms into --
    passed in (rather than returned) so it survives even when the
    commit itself raises partway through the attempt.

    Tracks first_attempt_duration_ms separately from the total: the
    first attempt's own round-trip time (scan/reload/commit call, zero
    contention involved yet) is the baseline cost of one commit: normal
    network/BigLake overhead. total_duration_ms - first_attempt_duration_ms
    isolates the actual contention overhead (backoff waits + retried
    attempts) -- without this split, commit_duration_ms alone conflates
    the two and understates how much of the latency is real conflict cost.

    Also captures table_row_count (cheap, from snapshot summary) at the
    moment of each fresh load, since baseline latency for a scan-based
    role like quarantine_writer is not actually constant -- it's coupled
    to how large the table has grown. scan_duration_ms is only non-null
    for quarantine_writer (from do_attempt_fn); both are returned from
    the FIRST attempt specifically, matching first_attempt_duration_ms.

    Returns (outcome, retry_count, first_attempt_duration_ms,
    total_duration_ms, record_id, table_row_count, scan_duration_ms).
    """
    start = time.monotonic()
    first_attempt_duration_ms = None
    first_table_row_count = None
    first_scan_duration_ms = None
    attempt = 0
    while True:
        attempt_start = time.monotonic()
        row_count = None
        timing = {}
        try:
            table = catalog.load_table(TABLE_IDENTIFIER)
            row_count = get_row_count(table)
            record_id = do_attempt_fn(table, timing)
            if first_attempt_duration_ms is None:
                first_attempt_duration_ms = (time.monotonic() - attempt_start) * 1000
                first_table_row_count = row_count
                first_scan_duration_ms = timing.get("scan_duration_ms")
            duration_ms = (time.monotonic() - start) * 1000
            if record_id is None:
                return ("skipped", attempt, first_attempt_duration_ms, duration_ms, None,
                        first_table_row_count, first_scan_duration_ms)
            outcome = "committed" if attempt == 0 else "retried_committed"
            return (outcome, attempt, first_attempt_duration_ms, duration_ms, record_id,
                    first_table_row_count, first_scan_duration_ms)
        except (CommitFailedException, CommitStateUnknownException, ValidationException) as e:
            if first_attempt_duration_ms is None:
                first_attempt_duration_ms = (time.monotonic() - attempt_start) * 1000
                first_table_row_count = row_count
                first_scan_duration_ms = timing.get("scan_duration_ms")
            attempt += 1
            if attempt > MAX_RETRIES:
                duration_ms = (time.monotonic() - start) * 1000
                return ("failed", attempt, first_attempt_duration_ms, duration_ms, None,
                        first_table_row_count, first_scan_duration_ms)
            backoff = min(MAX_BACKOFF_SECONDS, BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
            backoff += random.uniform(0, BASE_BACKOFF_SECONDS)
            print(f"    conflict (attempt {attempt}), retrying after {backoff:.2f}s: {e}")
            time.sleep(backoff)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True, choices=list(ROLE_INTERVAL_SECONDS))
    parser.add_argument("--writer-type", default="scripted", choices=["scripted", "agent"])
    parser.add_argument("--experiment-condition", required=True,
                         help="e.g. 'scripted_1', 'scripted_3', 'agent_2', 'agent_3'")
    parser.add_argument("--concurrent-writer-count", type=int, required=True,
                         help="How many writer processes are running concurrently in this condition")
    args = parser.parse_args()
    role = args.role
    writer_type = args.writer_type
    experiment_condition = args.experiment_condition
    concurrent_writer_count = args.concurrent_writer_count
    interval = ROLE_INTERVAL_SECONDS[role]

    with open(RUN_CONFIG_PATH) as f:
        run_config = json.load(f)
    run_id = run_config["experiment_run_id"]
    end_time = datetime.datetime.fromisoformat(run_config["end_time"])
    starting_row_count = run_config.get("starting_row_count")

    print(f"[{role}] starting, run={run_id}, interval={interval}s, "
          f"starting_row_count={starting_row_count}, ends={end_time.isoformat()}")

    catalog = get_catalog()
    bq_client = get_bq_client()
    ensure_write_attempts_table(bq_client)

    n = 0
    next_tick = time.monotonic()
    while datetime.datetime.now(datetime.timezone.utc) < end_time:
        if role == "quarantine_writer":
            (outcome, retry_count, first_attempt_ms, duration_ms, record_id,
             table_row_count, scan_duration_ms) = commit_with_retry(
                catalog, lambda table, timing: do_quarantine_overwrite(table, run_id, writer_type, timing))
        else:
            (outcome, retry_count, first_attempt_ms, duration_ms, record_id,
             table_row_count, scan_duration_ms) = commit_with_retry(
                catalog, lambda table, timing: do_append(table, run_id, role, writer_type, n, timing))

        if outcome != "skipped":
            log_attempt(
                bq_client,
                attempt_id=str(uuid.uuid4()),
                experiment_run_id=run_id,
                attempt_timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                writer_role=role,
                writer_type=writer_type,
                outcome=outcome,
                retry_count=retry_count,
                first_attempt_duration_ms=round(first_attempt_ms, 2),
                commit_duration_ms=round(duration_ms, 2),
                record_id=record_id,
                agent_decision_reasoning=None,
                experiment_condition=experiment_condition,
                concurrent_writer_count=concurrent_writer_count,
                run_starting_row_count=starting_row_count,
                table_row_count_at_attempt=table_row_count,
                scan_duration_ms=round(scan_duration_ms, 2) if scan_duration_ms is not None else None,
            )
            contention_ms = duration_ms - first_attempt_ms
            print(f"  [{role}] {outcome} (retries={retry_count}, total={duration_ms:.0f}ms, "
                  f"first_attempt={first_attempt_ms:.0f}ms, contention={contention_ms:.0f}ms, "
                  f"table_rows={table_row_count}, "
                  f"record={record_id[:8] if record_id else None})")
        else:
            print(f"  [{role}] skipped -- no eligible row to correct yet")

        n += 1
        next_tick += interval
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)

    print(f"[{role}] done. {n} tick(s) processed.")


if __name__ == "__main__":
    main()
