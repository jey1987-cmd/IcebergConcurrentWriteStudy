#!/usr/bin/env python3
"""
Creates the shared Iceberg table that all three agent roles -- and their
scripted baseline counterparts -- write to concurrently.

Write model, per role:
  result_writer     -- append (low conflict risk)
  telemetry_writer  -- append (low conflict risk)
  quarantine_writer -- row-level overwrite of an existing result_writer
                        row, matched by task_id (high conflict risk --
                        this is the role actually being tested)

Outcome/retry/latency metrics are NOT stored on this table -- they go to
the separate BigQuery write_attempts table (see bq_client.py), so that
logging a write's outcome never itself contends for the same
optimistic-concurrency lock this study is measuring.

Column groups:
  - Identity/grouping: record_id, experiment_run_id, write_timestamp
  - The independent variable: writer_role, writer_type
    (writer_type = 'scripted' | 'agent' is the study's core comparison)
  - result_writer / quarantine_writer payload (quarantine overwrites
    these columns in place on the matched row): task_id, result_value,
    result_status, quarantine_reason (set only by quarantine_writer)
  - telemetry_writer payload: metric_name, metric_value, source_component
"""

from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, NestedField, StringType, TimestampType

from catalog_client import TABLE_IDENTIFIER, get_catalog

SHARED_WRITE_SCHEMA = Schema(
    # Identity / grouping
    NestedField(1, "record_id", StringType(), required=True),
    NestedField(2, "experiment_run_id", StringType(), required=True),
    NestedField(3, "write_timestamp", TimestampType(), required=True),
    # Independent variable
    NestedField(4, "writer_role", StringType(), required=True),
    NestedField(5, "writer_type", StringType(), required=True),
    # result_writer / quarantine_writer payload
    NestedField(6, "task_id", StringType(), required=False),
    NestedField(7, "result_value", StringType(), required=False),
    NestedField(8, "result_status", StringType(), required=False),
    NestedField(9, "quarantine_reason", StringType(), required=False),
    # telemetry_writer payload
    NestedField(10, "metric_name", StringType(), required=False),
    NestedField(11, "metric_value", DoubleType(), required=False),
    NestedField(12, "source_component", StringType(), required=False),
)


def main():
    catalog = get_catalog()

    if catalog.table_exists(TABLE_IDENTIFIER):
        print(f"Dropping existing table (schema is changing): {TABLE_IDENTIFIER}")
        catalog.drop_table(TABLE_IDENTIFIER)

    table = catalog.create_table(
        TABLE_IDENTIFIER,
        schema=SHARED_WRITE_SCHEMA,
        properties={
            # Disable PyIceberg's own internal retry-with-rebase (default
            # 4 sub-retries) so baseline_writer.py's outer retry loop is
            # the sole source of retry_count/commit_duration_ms -- without
            # this, the two retry layers compound and conflate library
            # overhead with the actual conflict behavior under study.
            "commit.retry.num-retries": "1",
        },
    )
    print(f"Created table: {TABLE_IDENTIFIER}")
    print(table.schema())


if __name__ == "__main__":
    main()
