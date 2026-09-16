#!/usr/bin/env python3
"""
Source event generator for the streaming extension of the Iceberg agent-
concurrency study.

Publishes one JSON event per tick to the shared event topic (EVENT_TOPIC_ID)
-- Pub/Sub fans each message out to all three agent subscriptions
(streaming_agent_subscriber.py, one per role), which is what removes the
polling-interval variable from the original study: write timing is driven
by these events' arrival, not an internal schedule.

Event shape (the only fields the subscribers read):
    {"event_id": <uuid4>, "payload_value": <float>, "generated_at": <iso8601>}

Usage:
    python streaming_event_generator.py --duration-seconds 120 --interval-seconds 3

Requires: pip install google-cloud-pubsub --break-system-packages
"""

import argparse
import json
import time
import uuid
from datetime import datetime, timezone
import random

from google.cloud import pubsub_v1

with open('gcp_profile.json', 'r') as f:
    profile = json.load(f)

PROJECT_ID = profile['project_id']
EVENT_TOPIC_ID = profile.get('pubsub_topic', 'iceberg-streaming-events')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-seconds", type=int, default=120)
    parser.add_argument("--interval-seconds", type=float, default=3.0,
                         help="Average gap between events -- actual gap is "
                              "randomized (0.5x-1.5x) so inter_arrival_ms "
                              "isn't a trivially constant value.")
    args = parser.parse_args()

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, EVENT_TOPIC_ID)
    try:
        publisher.create_topic(request={"name": topic_path})
        print(f"[generator] created topic {topic_path}")
    except Exception as e:
        if "already exists" not in str(e).lower():
            raise
        print(f"[generator] topic already exists: {topic_path}")

    end_time = time.monotonic() + args.duration_seconds
    n = 0
    while time.monotonic() < end_time:
        event = {
            "event_id": str(uuid.uuid4()),
            "payload_value": round(random.uniform(0, 1000), 2),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        publisher.publish(topic_path, json.dumps(event).encode("utf-8"))
        n += 1
        print(f"[generator] published event {n}: {event['event_id'][:8]}")

        gap = args.interval_seconds * random.uniform(0.5, 1.5)
        time.sleep(min(gap, max(0.0, end_time - time.monotonic())))

    print(f"[generator] done. {n} event(s) published over {args.duration_seconds}s.")


if __name__ == "__main__":
    main()
