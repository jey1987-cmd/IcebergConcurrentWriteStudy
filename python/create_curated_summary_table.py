#!/usr/bin/env python3
"""
Creates the downstream Iceberg table the streaming curate layer
(streaming_curate.py) writes to on every upstream commit notification.

Schema matches curate_new_record()'s output columns exactly:
  triggered_by_role, triggered_by_event_id, upstream_row_count_at_curation,
  curated_at.

Uses the project's one proven-working catalog connection (catalog_client.py
-- same helper create_shared_table.py uses), not the ad-hoc,
profile-key-driven connection currently hardcoded in streaming_curate.py's
own get_iceberg_catalog(). See the flag at the bottom of this file before
running the curate layer.
"""

from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, TimestampType

from catalog_client import NAMESPACE, get_catalog

DOWNSTREAM_TABLE_NAME = "curated_summary"
DOWNSTREAM_TABLE_IDENTIFIER = f"{NAMESPACE}.{DOWNSTREAM_TABLE_NAME}"

CURATED_SUMMARY_SCHEMA = Schema(
    NestedField(1, "triggered_by_role", StringType(), required=True),
    NestedField(2, "triggered_by_event_id", StringType(), required=True),
    NestedField(3, "upstream_row_count_at_curation", LongType(), required=True),
    NestedField(4, "curated_at", TimestampType(), required=True),
)


def main():
    catalog = get_catalog()

    if catalog.table_exists(DOWNSTREAM_TABLE_IDENTIFIER):
        print(f"Table already exists: {DOWNSTREAM_TABLE_IDENTIFIER}")
        return

    table = catalog.create_table(
        DOWNSTREAM_TABLE_IDENTIFIER,
        schema=CURATED_SUMMARY_SCHEMA,
        properties={
            # Keep in sync with create_shared_table.py/reset_table.py:
            # disables PyIceberg's own internal retry-with-rebase so
            # append_with_retry() in streaming_curate.py is the sole
            # source of retry/backoff behavior on this table.
            "commit.retry.num-retries": "1",
        },
    )
    print(f"Created table: {DOWNSTREAM_TABLE_IDENTIFIER}")
    print(table.schema())


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# FLAG -- read before starting the curate layer:
# ---------------------------------------------------------------------------
# streaming_curate.py's own get_iceberg_catalog() / get_shared_table() /
# get_downstream_table() will NOT find the tables this script creates, as
# currently written:
#   - get_iceberg_catalog() builds its connection from gcp_profile.json
#     keys that don't exist in this project's profile (biglake_rest_uri,
#     gcs_bucket) -- both resolve to None, so load_catalog() will fail.
#   - get_shared_table() defaults to namespace "concurrency_study" /
#     table "shared_results"; the real upstream table (created by
#     create_shared_table.py) is "concurrent_agents.concurrent_writes".
#   - get_downstream_table() defaults to the same "concurrency_study"
#     namespace, so even the table this script creates
#     ("concurrent_agents.curated_summary") won't be found under that name.
#   - streaming_agent_subscriber.py has the identical get_iceberg_catalog()
#     / get_shared_table() problem, plus its do_commit() writes columns
#     (record_id, role, value, written_at) that don't match the real
#     upstream table's schema (record_id, experiment_run_id,
#     write_timestamp, writer_role, writer_type, task_id, result_value,
#     result_status, quarantine_reason, metric_name, metric_value,
#     source_component) and leaves two required fields
#     (experiment_run_id, writer_type) unset -- those commits will fail.
#
# Before the first end-to-end run, either point both streaming scripts'
# catalog helpers at catalog_client.get_catalog() (this script's pattern)
# and decide whether they should reuse concurrent_agents.concurrent_writes
# with its real schema or write to a new, intentionally simpler shared
# table -- that's a design call, not made here.
# ---------------------------------------------------------------------------
