#!/usr/bin/env python3
"""
Agent-driven writer: decision-timed writes to the shared Iceberg table,
using Gemini (via Vertex AI) to decide when to write rather than a fixed
schedule. Same roles, same commit/retry/logging machinery as
baseline_writer.py (imported directly, not reimplemented) -- the only
thing that changes is HOW the next write's timing is chosen.

Study design constraint: the agent is NOT given visibility into other
writers' recent conflict/retry history. It only knows its own elapsed
time since its last write and its target average interval (matching the
scripted baseline for the same role). This keeps the comparison to
"decision-timed vs. fixed-schedule" clean, rather than confounding it
with "contention-aware scheduling" -- a different, more elaborate
question this study isn't set up to answer.

Polling design: rather than re-asking the model every N seconds on a
hardcoded cadence (expensive, and not really agentic), each response
includes the model's own suggested check-back delay when it decides to
wait. This also means the agent's own reasoning latency and self-paced
polling are part of what makes its timing distribution genuinely
different from the scripted tick-based schedule, not just added noise.

Usage: same CLI shape as baseline_writer.py (role, experiment-condition,
concurrent-writer-count); writer-type is always 'agent' here:
    python agent_writer.py --role result_writer --experiment-condition agent_1 --concurrent-writer-count 1

Replication: run every condition (agent_1, agent_2, agent_3) at least
twice, same as the scripted baseline. The scripted_2/scripted_3
comparison showed directly that a single replicate can look like a real
effect and reverse on a second one (telemetry_writer's apparent
n=2->n=3 drop). Don't draw conclusions here from n=1 either.
"""

import argparse
import datetime
import json
import time
import uuid

from google import genai

from baseline_writer import ROLE_INTERVAL_SECONDS, RUN_CONFIG_PATH, commit_with_retry, do_append, do_quarantine_overwrite
from bq_client import ensure_write_attempts_table, get_bq_client, log_attempt
from catalog_client import get_catalog

with open("gcp_profile.json") as f:
    _profile = json.load(f)

PROJECT_ID = _profile["project_id"]
LOCATION = _profile.get("vertex_location", "us-central1")
MODEL_NAME = "gemini-2.5-flash"

genai_client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)

DEFAULT_WAIT_SECONDS = 3.0
MAX_WAIT_SECONDS = 15.0

ROLE_CONTEXT = {
    "result_writer": "You just finished processing a unit of work and are "
                      "deciding whether to report its result now.",
    "telemetry_writer": "You are monitoring pipeline health and deciding "
                         "whether to emit a telemetry reading now.",
    "quarantine_writer": "You are reviewing a previously-written result for "
                          "possible data-quality issues and deciding whether "
                          "to commit a correction now.",
}


def ask_agent_should_write(role: str, target_interval: float, elapsed: float) -> tuple[bool, float, str]:
    """Asks Gemini whether this agent should write now. Returns
    (should_write, wait_seconds, reasoning). wait_seconds is the model's
    own suggested check-back delay when it decides to wait (ignored if
    should_write is True)."""
    prompt = f"""{ROLE_CONTEXT[role]}
Your target average interval between writes is {target_interval:.0f} seconds.
It has been {elapsed:.1f} seconds since your last write.

Decide whether to write now or wait longer. You don't need to wait for
exactly {target_interval:.0f} seconds every time, but your decisions
should average out to roughly that cadence over many cycles. If you
decide to wait, also suggest how many seconds to wait before checking
again (a real autonomous agent doesn't poll on a fixed clock tick).

Respond in strict JSON only, no other text:
{{"decision": "write"|"wait", "wait_seconds": <number, only if waiting>, "reasoning": "<one short sentence>"}}
"""
    try:
        response = genai_client.models.generate_content(model=MODEL_NAME, contents=prompt)
        text = response.text.strip()
        if text.startswith("```"):
            text = text.strip("`").replace("json", "", 1).strip()
        parsed = json.loads(text)
        decision = parsed["decision"]
        reasoning = parsed.get("reasoning", "")
        if decision == "write":
            return True, 0.0, reasoning
        wait_seconds = min(MAX_WAIT_SECONDS, max(0.5, float(parsed.get("wait_seconds", DEFAULT_WAIT_SECONDS))))
        return False, wait_seconds, reasoning
    except Exception as e:
        # Fail safe: an unparsable/errored response defaults to waiting,
        # not writing blind -- a parse error should never turn into an
        # uncontrolled write-storm.
        return False, DEFAULT_WAIT_SECONDS, f"decision call failed, defaulting to wait: {e}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True, choices=list(ROLE_INTERVAL_SECONDS))
    parser.add_argument("--experiment-condition", required=True,
                         help="e.g. 'agent_1', 'agent_2', 'agent_3'")
    parser.add_argument("--concurrent-writer-count", type=int, required=True)
    args = parser.parse_args()
    role = args.role
    experiment_condition = args.experiment_condition
    concurrent_writer_count = args.concurrent_writer_count
    target_interval = ROLE_INTERVAL_SECONDS[role]
    writer_type = "agent"

    with open(RUN_CONFIG_PATH) as f:
        run_config = json.load(f)
    run_id = run_config["experiment_run_id"]
    end_time = datetime.datetime.fromisoformat(run_config["end_time"])
    starting_row_count = run_config.get("starting_row_count")

    print(f"[{role}] (agent) starting, run={run_id}, target_interval={target_interval}s, "
          f"starting_row_count={starting_row_count}, ends={end_time.isoformat()}")

    catalog = get_catalog()
    bq_client = get_bq_client()
    ensure_write_attempts_table(bq_client)

    n = 0
    last_write_time = time.monotonic()
    while datetime.datetime.now(datetime.timezone.utc) < end_time:
        elapsed = time.monotonic() - last_write_time
        should_write, wait_seconds, reasoning = ask_agent_should_write(role, target_interval, elapsed)

        if not should_write:
            print(f"  [{role}] wait {wait_seconds:.1f}s ({elapsed:.1f}s elapsed): {reasoning}")
            time.sleep(wait_seconds)
            continue

        if role == "quarantine_writer":
            (outcome, retry_count, first_attempt_ms, duration_ms, record_id,
             table_row_count, scan_duration_ms) = commit_with_retry(
                catalog, lambda table, timing: do_quarantine_overwrite(table, run_id, writer_type, timing))
        else:
            (outcome, retry_count, first_attempt_ms, duration_ms, record_id,
             table_row_count, scan_duration_ms) = commit_with_retry(
                catalog, lambda table, timing: do_append(table, run_id, role, writer_type, n, timing))

        last_write_time = time.monotonic()
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
                agent_decision_reasoning=reasoning,
                experiment_condition=experiment_condition,
                concurrent_writer_count=concurrent_writer_count,
                run_starting_row_count=starting_row_count,
                table_row_count_at_attempt=table_row_count,
                scan_duration_ms=round(scan_duration_ms, 2) if scan_duration_ms is not None else None,
            )
            contention_ms = duration_ms - first_attempt_ms
            print(f"  [{role}] {outcome} (retries={retry_count}, total={duration_ms:.0f}ms, "
                  f"contention={contention_ms:.0f}ms, reasoning={reasoning!r}, "
                  f"record={record_id[:8] if record_id else None})")
        else:
            print(f"  [{role}] skipped -- no eligible row to correct yet")
        n += 1

    print(f"[{role}] (agent) done. {n} write(s) issued.")


if __name__ == "__main__":
    main()
