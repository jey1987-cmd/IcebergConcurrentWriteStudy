#!/usr/bin/env python3
"""
Creates the two new BigQuery log tables needed by the streaming
extension: streaming_write_attempts (per-agent commit outcomes, written
by streaming_agent_subscriber.py) and curate_events (curate-layer
freshness-lag log, written by streaming_curate.py).

Deliberately its own dataset (BQ_LOG_DATASET below, matching the literal
value hardcoded in both streaming scripts), independent of the original
scripted study's concurrent_agents_study.write_attempts table (bq_client.py)
-- keeps the streaming study's logging from mixing with the baseline study's.

Run once before starting the agent subscribers or the curate layer.
"""

import json

from google.cloud import bigquery

with open("gcp_profile.json") as f:
    _profile = json.load(f)

PROJECT_ID = _profile["project_id"]
LOCATION = _profile.get("vertex_location", "us-central1")
BQ_LOG_DATASET = "iceberg_concurrency_study"


def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)


def ensure_dataset(client):
    dataset = bigquery.Dataset(f"{PROJECT_ID}.{BQ_LOG_DATASET}")
    dataset.location = LOCATION
    client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {PROJECT_ID}.{BQ_LOG_DATASET} ready.")


def ensure_streaming_write_attempts_table(client):
    # Columns match log_attempt() in streaming_agent_subscriber.py exactly --
    # deliberately mirrors bq_client.py's write_attempts columns (same
    # commit_with_retry return shape, reused from baseline_writer.py) so
    # streaming results stay comparable to the scripted/agent conditions.
    table_ref = f"{PROJECT_ID}.{BQ_LOG_DATASET}.streaming_write_attempts"
    schema = [
        bigquery.SchemaField("attempt_id", "INT64"),
        bigquery.SchemaField("writer_role", "STRING"),   # result_writer | telemetry_writer | quarantine_writer
        bigquery.SchemaField("writer_type", "STRING"),   # 'streaming_agent'
        bigquery.SchemaField("event_id", "STRING"),
        bigquery.SchemaField("attempt_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("outcome", "STRING"),       # committed | retried_committed | failed
        bigquery.SchemaField("retry_count", "INT64"),
        bigquery.SchemaField("commit_duration_ms", "FLOAT64"),
        # Null for the first event a given process handles (no prior
        # event to measure the gap from).
        bigquery.SchemaField("inter_arrival_ms", "FLOAT64"),
        bigquery.SchemaField("record_id", "STRING"),               # links to the Iceberg row, null if skipped/failed
        bigquery.SchemaField("first_attempt_duration_ms", "FLOAT64"),
        bigquery.SchemaField("table_row_count_at_attempt", "INT64"),
        bigquery.SchemaField("scan_duration_ms", "FLOAT64"),       # quarantine_writer-only, null otherwise
        bigquery.SchemaField("agent_decision_reasoning", "STRING"),
    ]
    client.create_table(bigquery.Table(table_ref, schema=schema), exists_ok=True)
    print(f"Table {table_ref} ready.")


def ensure_curate_events_table(client):
    # Columns match log_curate_event() in streaming_curate.py exactly.
    table_ref = f"{PROJECT_ID}.{BQ_LOG_DATASET}.curate_events"
    schema = [
        bigquery.SchemaField("source_role", "STRING"),   # role whose commit triggered this curation
        bigquery.SchemaField("event_id", "STRING"),
        bigquery.SchemaField("upstream_committed_at", "TIMESTAMP"),
        bigquery.SchemaField("curated_at", "TIMESTAMP"),
        bigquery.SchemaField("freshness_lag_ms", "FLOAT64"),
    ]
    client.create_table(bigquery.Table(table_ref, schema=schema), exists_ok=True)
    print(f"Table {table_ref} ready.")


def main():
    client = get_bq_client()
    ensure_dataset(client)
    ensure_streaming_write_attempts_table(client)
    ensure_curate_events_table(client)


if __name__ == "__main__":
    main()
