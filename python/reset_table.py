#!/usr/bin/env python3
"""
Resets the shared Iceberg table to empty. Run before EVERY experimental
condition (scripted_1, scripted_2, scripted_3, each agent variant).

Why this is mandatory, not optional: the scripted_3 run showed
quarantine_writer's target-picking scan cost climbing steadily with
table size -- its baseline (zero-retry) latency rose from 14.4s to
55.2s over one 20-minute run as result_writer/telemetry_writer kept
appending. Different conditions produce different accumulation rates
(more concurrent writers = faster growth), so without a reset, every
cross-condition "baseline vs. contention latency" comparison would be
confounded by how much table growth happened to occur before that
condition ran, not just by the conflict behavior under study.

IMPORTANT -- this drops and recreates the table rather than deleting
rows in place. table.delete() looked like a reset, but a DELETE is
itself a new commit that ADDS a snapshot to the table's metadata log --
it never removes old ones. This PyIceberg version's manage_snapshots()
API has no expire_snapshots/prune operation, so after ~a dozen resets
across a long session, metadata.json grew large enough that BigLake
started rejecting commits outright with 429 "Table metadata is too
large" (RESOURCE_EXHAUSTED) -- observed directly, crashing all three
agent_3 replicate-2 writers simultaneously. Dropping and recreating the
table is the only way available here to actually reset metadata size,
not just data content.

Usage: python reset_table.py
"""

from catalog_client import get_catalog
from create_shared_table import SHARED_WRITE_SCHEMA, TABLE_IDENTIFIER


def main():
    catalog = get_catalog()

    if catalog.table_exists(TABLE_IDENTIFIER):
        catalog.drop_table(TABLE_IDENTIFIER)

    catalog.create_table(
        TABLE_IDENTIFIER,
        schema=SHARED_WRITE_SCHEMA,
        properties={
            # Keep in sync with create_shared_table.py: disables
            # PyIceberg's internal retry-with-rebase so the outer retry
            # loop in baseline_writer.py/agent_writer.py is the sole
            # source of retry_count/commit_duration_ms.
            "commit.retry.num-retries": "1",
        },
    )
    print(f"Reset {TABLE_IDENTIFIER}: dropped and recreated (metadata history cleared, 0 rows).")


if __name__ == "__main__":
    main()
