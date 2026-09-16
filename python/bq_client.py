#!/usr/bin/env python3
"""
BigQuery write_attempts logging table -- deliberately independent of the
Iceberg table under test, so that logging a write's outcome never itself
contends for the same optimistic-concurrency lock being measured.
"""

import json

from google.cloud import bigquery

with open("gcp_profile.json") as f:
    _profile = json.load(f)

PROJECT_ID = _profile["project_id"]
LOCATION = _profile.get("vertex_location", "us-central1")
DATASET = "concurrent_agents_study"
TABLE_NAME = "write_attempts"
TABLE_REF = f"{PROJECT_ID}.{DATASET}.{TABLE_NAME}"


def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)


def ensure_write_attempts_table(client):
    client.create_dataset(bigquery.Dataset(f"{PROJECT_ID}.{DATASET}"), exists_ok=True)

    schema = [
        bigquery.SchemaField("attempt_id", "STRING"),
        bigquery.SchemaField("experiment_run_id", "STRING"),
        bigquery.SchemaField("attempt_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("writer_role", "STRING"),
        bigquery.SchemaField("writer_type", "STRING"),  # 'scripted' | 'agent'
        bigquery.SchemaField("outcome", "STRING"),  # 'committed' | 'retried_committed' | 'failed'
        bigquery.SchemaField("retry_count", "INT64"),
        # Baseline round-trip cost of the first attempt alone (zero
        # contention yet) vs. commit_duration_ms (total, including all
        # retries/backoff) -- subtract to isolate contention overhead.
        bigquery.SchemaField("first_attempt_duration_ms", "FLOAT64"),
        bigquery.SchemaField("commit_duration_ms", "FLOAT64"),
        bigquery.SchemaField("record_id", "STRING"),  # links to the Iceberg row, null if failed
        bigquery.SchemaField("agent_decision_reasoning", "STRING"),  # null for scripted writers
        # e.g. 'scripted_1', 'scripted_3', 'agent_2', 'agent_3' -- the
        # experimental condition this attempt belongs to, for the
        # conflict-rate-vs-writer-count (n^2 scaling) comparison.
        bigquery.SchemaField("experiment_condition", "STRING"),
        bigquery.SchemaField("concurrent_writer_count", "INT64"),
        # Table-growth controls: the table's row count when this run
        # started (should be 0 if reset_table.py was run first -- see
        # that file for why table growth confounds latency comparisons
        # across conditions) and at the moment of this specific attempt
        # (from cheap snapshot-summary metadata, not a scan). scan_duration_ms
        # is quarantine_writer-only: the target-picking table scan timed
        # separately from the commit itself, null for append roles.
        bigquery.SchemaField("run_starting_row_count", "INT64"),
        bigquery.SchemaField("table_row_count_at_attempt", "INT64"),
        bigquery.SchemaField("scan_duration_ms", "FLOAT64"),
    ]
    client.create_table(bigquery.Table(TABLE_REF, schema=schema), exists_ok=True)
    print(f"Table {TABLE_REF} ready.")


def log_attempt(client, **fields) -> None:
    errors = client.insert_rows_json(TABLE_REF, [fields])
    if errors:
        print(f"  write_attempts insert errors: {errors}")
