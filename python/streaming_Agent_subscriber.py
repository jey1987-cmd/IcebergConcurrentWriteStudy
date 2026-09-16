#!/usr/bin/env python3
"""
Agent subscribers for the streaming extension of the Iceberg agent-
concurrency study.

Each of the three roles (result, telemetry, quarantine) runs as its own
independent Pub/Sub subscription to the same event topic -- Pub/Sub
delivers a copy of every published event to each subscription, so all
three agents genuinely react to the same event stream, exactly as in
the original study's design.

On receiving an event, each agent asks Gemini for a brief decision
before acting, then commits to the SAME shared Iceberg table used in the
original (non-streaming) study (concurrent_agents.concurrent_writes),
via the same commit/retry/schema machinery as agent_writer.py --
do_append, do_quarantine_overwrite and commit_with_retry are imported
directly from baseline_writer.py, not reimplemented, so this genuinely
preserves the original contention and scan-cost mechanics rather than
approximating them on a lookalike table. After a successful commit, the
agent publishes a lightweight notification to a second topic, which the
streaming curate layer (separate script) subscribes to in order to
curate "on arrival" rather than on a polling schedule.

This removes the polling-interval variable entirely: write timing is now
driven by external event arrival, not by an internal decision cycle or
fixed clock, directly testing whether the original study's throughput
confound (Section V-B) persists once that variable is eliminated.

Run three copies of this script concurrently, one per ROLE value
('result', 'telemetry', 'quarantine'), each as its own process.

Requires: pip install google-cloud-pubsub google-cloud-bigquery pyiceberg
          google-genai --break-system-packages
"""

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.cloud.pubsub_v1.subscriber.scheduler import ThreadScheduler
from google import genai

from baseline_writer import RUN_CONFIG_PATH, commit_with_retry, do_append, do_quarantine_overwrite
from catalog_client import get_catalog

with open('gcp_profile.json', 'r') as f:
    profile = json.load(f)

PROJECT_ID = profile['project_id']
LOCATION = profile.get('vertex_location', 'us-central1')
EVENT_TOPIC_ID = profile.get('pubsub_topic', 'iceberg-streaming-events')
CURATE_TOPIC_ID = profile.get('curate_topic', 'iceberg-curate-trigger')
BQ_LOG_DATASET = "iceberg_concurrency_study"
MODEL_NAME = "gemini-2.5-flash"
WRITER_TYPE = "streaming_agent"

ROLE = sys.argv[1] if len(sys.argv) > 1 else None
if ROLE not in ("result", "telemetry", "quarantine"):
    print("Usage: python streaming_agent_subscriber.py <result|telemetry|quarantine>")
    sys.exit(1)

WRITER_ROLE = f"{ROLE}_writer"  # matches create_shared_table.py's writer_role values exactly

genai_client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)


# ---------------------------------------------------------------------------
# BigQuery logging (own dataset/table -- independent of the Iceberg table
# under test, same reasoning as bq_client.py)
# ---------------------------------------------------------------------------

def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)


def log_attempt(bq_client, attempt_id, event_id, inter_arrival_ms, outcome,
                 retry_count, first_attempt_ms, duration_ms, record_id,
                 table_row_count, scan_duration_ms, reasoning):
    table_ref = f"{PROJECT_ID}.{BQ_LOG_DATASET}.streaming_write_attempts"
    row = {
        "attempt_id": attempt_id,
        "writer_role": WRITER_ROLE,
        "writer_type": WRITER_TYPE,
        "event_id": event_id,
        "attempt_timestamp": datetime.now(timezone.utc).isoformat(),
        "outcome": outcome,
        "retry_count": retry_count,
        "commit_duration_ms": duration_ms,
        "inter_arrival_ms": inter_arrival_ms,
        "record_id": record_id,
        "first_attempt_duration_ms": first_attempt_ms,
        "table_row_count_at_attempt": table_row_count,
        "scan_duration_ms": scan_duration_ms,
        "agent_decision_reasoning": reasoning,
    }
    errors = bq_client.insert_rows_json(table_ref, [row])
    if errors:
        print(f"  [{ROLE}] log insert error: {errors}")


# ---------------------------------------------------------------------------
# Gemini decision step
# ---------------------------------------------------------------------------

def ask_gemini_for_decision(role, event):
    prompt = (
        f"You are an autonomous '{role}' agent in a data pipeline. An "
        f"event has just arrived: {json.dumps(event)}. In one brief "
        f"sentence, state your decision about how to process this event "
        f"as the {role} agent."
    )
    response = genai_client.models.generate_content(model=MODEL_NAME, contents=prompt)
    return response.text.strip()


# ---------------------------------------------------------------------------
# Quarantine retry budget
# ---------------------------------------------------------------------------

QUARANTINE_MAX_ROUNDS = 3
QUARANTINE_ROUND_PAUSE_SECONDS = 2.0


def attempt_quarantine_commit(catalog, run_id):
    """do_quarantine_overwrite has to win an optimistic-concurrency race
    against result_writer/telemetry_writer's commits. Isolated testing
    confirmed the overwrite itself is not broken -- with zero other
    writers active it lands instantly -- but under real contention from
    the other two roles' own event backlogs, baseline_writer's single
    commit_with_retry budget (MAX_RETRIES=5, ~15s of backoff) was
    calibrated against the scripted study's fixed 10s/15s/20s tick
    intervals, which leave real quiet gaps between commits; here,
    backlog-draining bursts can run longer than that. Rather than
    inflate baseline_writer.py's shared retry constants (used by every
    non-streaming condition too, and load-bearing for existing
    findings), give quarantine a few extra whole rounds of that same
    retry loop, pausing between rounds. Also retries on "skipped" (no
    result_writer row yet), since one might land during the pause."""
    total_retry_count = 0
    total_duration_ms = 0.0
    first_attempt_ms = None
    outcome, record_id, table_row_count, scan_duration_ms = "skipped", None, None, None
    for round_num in range(1, QUARANTINE_MAX_ROUNDS + 1):
        (outcome, retry_count, round_first_attempt_ms, round_duration_ms, record_id,
         table_row_count, scan_duration_ms) = commit_with_retry(
            catalog, lambda table, timing: do_quarantine_overwrite(table, run_id, WRITER_TYPE, timing))
        if first_attempt_ms is None:
            first_attempt_ms = round_first_attempt_ms
        total_retry_count += retry_count
        total_duration_ms += round_duration_ms
        if outcome not in ("failed", "skipped"):
            break
        if round_num < QUARANTINE_MAX_ROUNDS:
            print(f"  [quarantine] round {round_num} outcome={outcome}, "
                  f"pausing {QUARANTINE_ROUND_PAUSE_SECONDS}s before another round")
            time.sleep(QUARANTINE_ROUND_PAUSE_SECONDS)
    return (outcome, total_retry_count, first_attempt_ms, total_duration_ms,
            record_id, table_row_count, scan_duration_ms)


# ---------------------------------------------------------------------------
# Pub/Sub callback
# ---------------------------------------------------------------------------

_state = {"attempt_id": 0, "last_event_time": None}


def make_callback(catalog, bq_client, run_id, curate_publisher, curate_topic_path):
    def callback(message):
        event = json.loads(message.data.decode("utf-8"))
        now = time.time()
        inter_arrival_ms = (
            (now - _state["last_event_time"]) * 1000
            if _state["last_event_time"] is not None else None
        )
        _state["last_event_time"] = now
        _state["attempt_id"] += 1
        attempt_id = _state["attempt_id"]

        reasoning = ask_gemini_for_decision(ROLE, event)
        print(f"[{ROLE}] event {event['event_id'][:8]}: {reasoning}")

        if ROLE == "quarantine":
            (outcome, retry_count, first_attempt_ms, duration_ms, record_id,
             table_row_count, scan_duration_ms) = attempt_quarantine_commit(catalog, run_id)
        else:
            (outcome, retry_count, first_attempt_ms, duration_ms, record_id,
             table_row_count, scan_duration_ms) = commit_with_retry(
                catalog, lambda table, timing: do_append(table, run_id, WRITER_ROLE, WRITER_TYPE, attempt_id, timing))

        if outcome != "skipped":
            log_attempt(bq_client, attempt_id, event['event_id'], inter_arrival_ms,
                        outcome, retry_count, round(first_attempt_ms, 2), round(duration_ms, 2),
                        record_id, table_row_count,
                        round(scan_duration_ms, 2) if scan_duration_ms is not None else None,
                        reasoning)
        else:
            print(f"  [{ROLE}] skipped -- no eligible row to correct yet")

        success = outcome in ("committed", "retried_committed")
        if success:
            notification = json.dumps({
                "source_role": ROLE,
                "event_id": event['event_id'],
                "committed_at": datetime.now(timezone.utc).isoformat(),
            }).encode("utf-8")
            curate_publisher.publish(curate_topic_path, notification)

        message.ack()

    return callback


def main():
    with open(RUN_CONFIG_PATH) as f:
        run_config = json.load(f)
    run_id = run_config["experiment_run_id"]
    print(f"[{ROLE}] using run {run_id} (starting_row_count={run_config.get('starting_row_count')})")

    catalog = get_catalog()
    bq_client = get_bq_client()

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        PROJECT_ID, f"{EVENT_TOPIC_ID}-{ROLE}-sub")
    topic_path = subscriber.topic_path(PROJECT_ID, EVENT_TOPIC_ID)

    try:
        subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path})
        print(f"[{ROLE}] created subscription {subscription_path}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"[{ROLE}] subscription already exists, continuing.")
        else:
            raise

    curate_publisher = pubsub_v1.PublisherClient()
    curate_topic_path = curate_publisher.topic_path(PROJECT_ID, CURATE_TOPIC_ID)
    try:
        curate_publisher.create_topic(request={"name": curate_topic_path})
    except Exception as e:
        if "already exists" not in str(e).lower():
            raise

    callback = make_callback(catalog, bq_client, run_id, curate_publisher, curate_topic_path)
    # pubsub_v1's default subscriber runs callbacks concurrently across
    # several worker threads -- confirmed directly that this let MULTIPLE
    # quarantine events commit-race each other (and result/telemetry)
    # simultaneously WITHIN this one process, on top of the cross-role
    # contention the study is actually trying to measure. Pinning this
    # subscriber to one callback at a time restores the single-writer-
    # per-role model the scripted/agent studies were built around.
    scheduler = ThreadScheduler(executor=ThreadPoolExecutor(max_workers=1))
    # Orthogonal to the scheduler above (bounds leased/outstanding
    # messages, not concurrent callback execution) -- avoids holding a
    # backlog of leased-but-unprocessed messages against their ack
    # deadline while one slow event (quarantine especially) is mid-retry.
    flow_control = pubsub_v1.types.FlowControl(max_messages=1)
    future = subscriber.subscribe(subscription_path, callback=callback,
                                   scheduler=scheduler, flow_control=flow_control)
    print(f"[{ROLE}] listening for events...")

    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()
        print(f"[{ROLE}] stopped.")


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# SETUP NOTES
# ---------------------------------------------------------------------------
# 1. Run three separate processes, one per role:
#      python streaming_agent_subscriber.py result
#      python streaming_agent_subscriber.py telemetry
#      python streaming_agent_subscriber.py quarantine
# 2. Each creates its OWN subscription to the shared event topic, so all
#    three genuinely receive a copy of every published event -- confirm
#    this with the BigQuery log after a short test run (each event_id
#    should appear once per role, in streaming_write_attempts).
# 3. Requires current_run.json to already exist -- run start_run.py (and
#    reset_table.py first, unless a pre-existing row count is deliberate)
#    before starting these processes, same as baseline_writer.py /
#    agent_writer.py. experiment_run_id is a required column on the
#    shared table, so this is not optional here either.
# 4. Commits go to the REAL shared table (concurrent_agents.concurrent_writes,
#    via catalog_client.py), using the exact same do_append /
#    do_quarantine_overwrite logic as agent_writer.py -- quarantine's
#    target-picking scan (pick_target_task_id in baseline_writer.py) is
#    the real, tested scan-cost mechanism, not a placeholder filter.
# 5. Requires a "streaming_write_attempts" table in BQ_LOG_DATASET --
#    see create_streaming_log_tables.py.
