#!/usr/bin/env python3
"""
Streaming curate layer for the streaming extension of the Iceberg agent-
concurrency study.

Subscribes to the "curate trigger" topic, which each of the three agent
subscribers publishes to immediately after a successful commit to the
shared upstream table. On receiving a notification, this layer reads the
newly committed record from the upstream table and writes a curated,
aggregated summary to a separate downstream table -- reacting on arrival
rather than polling on a fixed schedule, completing the genuinely
event-driven pipeline: source events -> agents -> upstream table ->
curate layer (on arrival) -> downstream table.

This lets the study measure downstream freshness lag under contention
(does the curate layer fall behind when the upstream table is under
heavy quarantine-driven contention?) in addition to the original
single-table conflict-rate and scan-cost findings.

Requires: pip install google-cloud-pubsub google-cloud-bigquery pyiceberg
          --break-system-packages
"""

import json
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.cloud.pubsub_v1.subscriber.scheduler import ThreadScheduler
from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException, ValidationException
import pyarrow as pa

from catalog_client import TABLE_IDENTIFIER, get_catalog
from create_curated_summary_table import DOWNSTREAM_TABLE_IDENTIFIER

MAX_RETRIES = 5
BASE_BACKOFF_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 8.0

with open('gcp_profile.json', 'r') as f:
    profile = json.load(f)

PROJECT_ID = profile['project_id']
CURATE_TOPIC_ID = profile.get('curate_topic', 'iceberg-curate-trigger')
BQ_LOG_DATASET = "iceberg_concurrency_study"

# Explicit, all-non-nullable schema matching CURATED_SUMMARY_SCHEMA's
# `required=True` fields exactly -- pa.table() without a schema infers
# every column as nullable, which PyIceberg's append() rejects as
# incompatible against a table of required columns (same reasoning as
# baseline_writer.py's ARROW_SCHEMA).
CURATED_ARROW_SCHEMA = pa.schema([
    pa.field("triggered_by_role", pa.string(), nullable=False),
    pa.field("triggered_by_event_id", pa.string(), nullable=False),
    pa.field("upstream_row_count_at_curation", pa.int64(), nullable=False),
    pa.field("curated_at", pa.timestamp("us"), nullable=False),
])


def get_iceberg_catalog():
    return get_catalog()


def get_shared_table(catalog):
    return catalog.load_table(TABLE_IDENTIFIER)


def get_downstream_table(catalog):
    return catalog.load_table(DOWNSTREAM_TABLE_IDENTIFIER)


def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)


def log_curate_event(bq_client, notification, freshness_lag_ms):
    table_ref = f"{PROJECT_ID}.{BQ_LOG_DATASET}.curate_events"
    row = {
        "source_role": notification["source_role"],
        "event_id": notification["event_id"],
        "upstream_committed_at": notification["committed_at"],
        "curated_at": datetime.now(timezone.utc).isoformat(),
        "freshness_lag_ms": freshness_lag_ms,
    }
    errors = bq_client.insert_rows_json(table_ref, [row])
    if errors:
        print(f"  [curate] log insert error: {errors}")


def append_with_retry(catalog, curated_row):
    """Appends to the downstream table, reloading and retrying on
    conflict -- same reasoning and exception set as baseline_writer.py's
    commit_with_retry(). Without this, a single commit conflict (not an
    edge case: Pub/Sub's default subscriber delivers with several
    concurrent callback threads, so multiple curate_new_record() calls
    can genuinely be appending to the same downstream table at once)
    raises out of append() with nothing to catch it here."""
    attempt = 0
    while True:
        try:
            downstream = get_downstream_table(catalog)
            downstream.append(curated_row)
            return
        except (CommitFailedException, CommitStateUnknownException, ValidationException) as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                raise
            backoff = min(MAX_BACKOFF_SECONDS, BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
            backoff += random.uniform(0, BASE_BACKOFF_SECONDS)
            print(f"  [curate] downstream append conflict (attempt {attempt}), "
                  f"retrying after {backoff:.2f}s: {e}")
            time.sleep(backoff)


def curate_new_record(catalog, notification):
    """Read the upstream table's current state and write a curated,
    aggregated summary row to the downstream table, on arrival."""
    upstream = get_shared_table(catalog)

    # NOTE: this reads the upstream table's current row count as the
    # curated signal -- adjust to whatever aggregation your actual
    # analysis needs (e.g., rolling average latency per role).
    scan = upstream.scan()
    row_count = sum(1 for _ in scan.to_arrow().to_pylist())

    curated_row = pa.table({
        "triggered_by_role": [notification["source_role"]],
        "triggered_by_event_id": [notification["event_id"]],
        "upstream_row_count_at_curation": [row_count],
        # Iceberg column is TimestampType (naive) -- matches
        # create_curated_summary_table.py / baseline_writer.py's
        # build_row(), which strips tzinfo the same way.
        "curated_at": [datetime.now(timezone.utc).replace(tzinfo=None)],
    }, schema=CURATED_ARROW_SCHEMA)
    append_with_retry(catalog, curated_row)


def main():
    catalog = get_iceberg_catalog()
    bq_client = get_bq_client()

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        PROJECT_ID, f"{CURATE_TOPIC_ID}-curate-sub")
    topic_path = subscriber.topic_path(PROJECT_ID, CURATE_TOPIC_ID)

    try:
        subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path})
        print(f"[curate] created subscription {subscription_path}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("[curate] subscription already exists, continuing.")
        else:
            raise

    def callback(message):
        # Everything below is wrapped in try/except deliberately: any
        # exception that escapes a pubsub_v1 subscriber callback is
        # re-raised through future.result() in main() below, which kills
        # this ENTIRE process, not just the one message -- confirmed
        # directly (a single downstream commit conflict took the whole
        # curate layer down mid-run). A conflict here is expected
        # behavior under load, not a reason to stop curating everything
        # after it.
        try:
            notification = json.loads(message.data.decode("utf-8"))
            committed_at = datetime.fromisoformat(notification["committed_at"])
            now = datetime.now(timezone.utc)
            freshness_lag_ms = (now - committed_at).total_seconds() * 1000

            curate_new_record(catalog, notification)
            log_curate_event(bq_client, notification, freshness_lag_ms)

            print(f"[curate] curated event from {notification['source_role']} "
                  f"(freshness lag: {freshness_lag_ms:.0f}ms)")
            message.ack()
        except Exception as e:
            print(f"[curate] failed to curate event, nacking for redelivery: {e}")
            message.nack()

    # Serialize callbacks: pubsub_v1's default subscriber runs several
    # in parallel, which let concurrent curate_new_record() calls
    # commit-race each other on the SAME single-writer downstream table
    # -- confirmed directly (append_with_retry's conflicts kept citing
    # "concurrent update" even though this process is curated_summary's
    # only writer). Curation is inherently sequential work anyway.
    scheduler = ThreadScheduler(executor=ThreadPoolExecutor(max_workers=1))
    # Orthogonal to the scheduler above (bounds leased/outstanding
    # messages, not concurrent callback execution).
    flow_control = pubsub_v1.types.FlowControl(max_messages=1)
    future = subscriber.subscribe(subscription_path, callback=callback,
                                   scheduler=scheduler, flow_control=flow_control)
    print("[curate] listening for upstream commit notifications...")

    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()
        print("[curate] stopped.")


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# SETUP NOTES
# ---------------------------------------------------------------------------
# 1. Requires the downstream Iceberg table -- run
#    create_curated_summary_table.py before this script. Reads the upstream
#    table via the same catalog_client.get_catalog() connection as every
#    other script here, so upstream (concurrent_agents.concurrent_writes)
#    and downstream (concurrent_agents.curated_summary) are always resolved
#    consistently with what the agent subscribers actually write to.
# 2. Requires a "curate_events" table in BQ_LOG_DATASET -- run
#    create_streaming_log_tables.py before this script.
# 3. The row-count scan in curate_new_record() reads the ENTIRE upstream
#    table on every single event -- this will get expensive and slow as
#    the table grows, mirroring (and potentially compounding) the same
#    scan-cost mechanism already established for the quarantine writer.
#    This is worth treating as a genuine finding if it shows up, not
#    just an inefficiency to fix -- but if it makes the curate layer
#    fall too far behind to be useful, consider curating only a bounded
#    recent window instead of a full-table scan.
# 4. freshness_lag_ms depends on reasonably synchronized clocks between
#    the agent subscribers and this script -- both should be running on
#    infrastructure with real NTP sync (standard on GCP Compute/Cloud Run),
#    not an assumption to skip checking.