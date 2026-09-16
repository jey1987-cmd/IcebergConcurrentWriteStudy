#!/usr/bin/env python3
"""
Starts a new experiment run: generates a shared experiment_run_id and a
common end time, so writer processes launched separately (one per role,
scripted baseline or agent) stay synchronized on the same run and stop
together instead of drifting apart.

Records the table's starting row count into current_run.json -- run
reset_table.py first so this is 0 for every condition. If you skip the
reset deliberately, the starting count still gets documented rather than
silently lost, but cross-condition latency comparisons will be
confounded by table-growth effects (see reset_table.py for why).

Usage: python start_run.py --duration-seconds 180
"""

import argparse
import datetime
import json
import uuid

from catalog_client import TABLE_IDENTIFIER, get_catalog, get_row_count

RUN_CONFIG_PATH = "current_run.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-seconds", type=int, default=180)
    args = parser.parse_args()

    catalog = get_catalog()
    table = catalog.load_table(TABLE_IDENTIFIER)
    starting_row_count = get_row_count(table)
    if starting_row_count != 0:
        print(f"WARNING: table has {starting_row_count} rows, not 0 -- "
              f"run reset_table.py first unless this is deliberate.")

    start = datetime.datetime.now(datetime.timezone.utc)
    end = start + datetime.timedelta(seconds=args.duration_seconds)
    run_id = f"{start.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"

    config = {
        "experiment_run_id": run_id,
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
        "duration_seconds": args.duration_seconds,
        "starting_row_count": starting_row_count,
    }
    with open(RUN_CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)

    print(f"Started run {run_id}")
    print(f"  starting_row_count: {starting_row_count}")
    print(f"  start: {start.isoformat()}")
    print(f"  end:   {end.isoformat()}  ({args.duration_seconds}s)")
    print(f"Config written to {RUN_CONFIG_PATH} -- launch each writer process now.")


if __name__ == "__main__":
    main()
