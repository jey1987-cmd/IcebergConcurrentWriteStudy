# Iceberg Concurrent Write Study

An empirical study of optimistic-concurrency behavior on a BigLake Iceberg
REST-catalog table under concurrent writers, comparing a **scripted,
fixed-schedule** writer against an **LLM-agent-driven, decision-timed**
writer, and extending the setup into a genuinely event-driven streaming
pipeline.

Three writer roles share one Iceberg table:

| Role | Write pattern | Conflict risk |
|---|---|---|
| `result_writer` | append | low |
| `telemetry_writer` | append | low |
| `quarantine_writer` | overwrite of an existing `result_writer` row, matched by `task_id` | high — this is the role actually under test |

Every write attempt (first try, retried, or abandoned) is logged to a
separate BigQuery table so that logging never itself contends for the
same optimistic-concurrency lock being measured.

## Two conditions

- **Scripted baseline** ([python/baseline_writer.py](python/baseline_writer.py)) — each role writes on a fixed tick grid (10s / 15s / 20s), independent of commit latency.
- **Agent-driven** ([python/agent_writer.py](python/agent_writer.py)) — the same commit/retry machinery, but a Gemini (Vertex AI) call decides *when* to write next each cycle, given only its own elapsed time and target interval (deliberately blind to other writers' conflict history, to keep the comparison to "decision-timed vs. fixed-schedule" clean rather than "contention-aware scheduling").

Conflict handling ([python/baseline_writer.py](python/baseline_writer.py)) reloads the table fresh and retries with exponential backoff on `CommitFailedException` / `CommitStateUnknownException` / `ValidationException` — the exceptions BigLake actually raises under contention, confirmed empirically.

## Streaming extension

The original design polls or ticks on an internal clock. The streaming
extension removes that variable entirely: a [source event generator](python/streaming_event_generator.py) publishes to a Pub/Sub topic, all three roles run as independent [agent subscribers](python/streaming_Agent_subscriber.py) reacting to event arrival (each gets its own subscription, so every event fans out to all three), and a [curate layer](python/streaming_curate.py) reacts to successful commits by writing an aggregated summary to a downstream table — completing an event-driven pipeline: `source events → agents → upstream table → curate layer (on arrival) → downstream table`.

## Key findings

From the hour-long streaming run (`experiment_run_id 20260916T185226_3e4e6e43`, full data/log in [python/streaming_run_results.txt](python/streaming_run_results.txt), queries in [python/streaming_run_analysis.sql](python/streaming_run_analysis.sql)):

- **Scan cost scales with table size.** `quarantine_writer`'s target-picking scan (a full-table scan) showed `first_attempt_duration_ms` climbing from ~8s at 12 rows to ~41s at 234 rows — Pearson r = 0.99 (n=4; directionally consistent with r>0.96 from the earlier scripted study, but too small a sample here to call it independently confirmed).

- **`quarantine_writer` starved under contention.** 3 of 4 attempts failed after exhausting retries (avg 18 retries per failure); `result_writer`/`telemetry_writer` — pure appends — committed at 57–99% success (with retry) and only 1–2 outright failures each across 305/258 attempts.

- **Curate freshness lag showed high variance, not a stable or steadily degrading trend.** Across 130 curated events, the standard deviation of freshness lag exceeded its mean for both `result` and `telemetry` sources (result: mean 255.1s, SD 369.1s; telemetry: mean 288.2s, SD 359.3s), with individual measurements ranging from under one second to over 26 minutes. This is not a monotonic climb: the single highest observed lag occurred roughly halfway through the run, and the final ~10–15 minutes of curated events consistently showed *low* lag (100–200s), not a continued climb. The pattern is consistent with the curate layer processing commit notifications in the order they *arrive* rather than the order their underlying events were *generated* — a fast-processing `telemetry` commit can be curated well ahead of an older, contention-delayed `result` or `quarantine` commit, so any single freshness-lag measurement reflects that specific upstream write's own processing delay rather than a system-wide, time-dependent degradation. Practically, this means a notification-triggered curate layer downstream of a contended table should be expected to produce highly variable, effectively unpredictable per-record latency under load, not a gracefully degrading service.

Full per-role outcome breakdown, fan-out verification, and inter-arrival distribution are in Sections 1–3 and 5 of the results file.

## Setup

Requires a GCP project with a BigLake Iceberg REST catalog and a service
account / user with `gcloud` CLI auth (BigLake's catalog endpoint rejects
standard ADC and service-account tokens under this project's
`iam.disableServiceAccountKeyCreation` org policy — [python/gcloud_auth_manager.py](python/gcloud_auth_manager.py) reuses the `gcloud` CLI's own OAuth client instead, which the endpoint does accept).

```bash
pip install "pyiceberg[gcsfs]" pyarrow google-cloud-bigquery google-cloud-pubsub google-genai

cp python/gcp_profile.json.example python/gcp_profile.json
# edit gcp_profile.json with your project_id / catalog / bucket
```

**Original (scripted vs. agent) study**, from `python/`:

```bash
python catalog_connectivity_test.py      # one-time: confirms catalog access
python create_shared_table.py            # one-time: creates the shared table
python reset_table.py                    # before EVERY condition (see script docstring for why)
python start_run.py --duration-seconds 180

# in separate terminals, one per role:
python baseline_writer.py --role result_writer     --experiment-condition scripted_1 --concurrent-writer-count 1
python agent_writer.py    --role telemetry_writer   --experiment-condition agent_1    --concurrent-writer-count 1
```

**Streaming extension**, additionally:

```bash
python create_curated_summary_table.py
python create_streaming_log_tables.py
python start_run.py --duration-seconds 3600

# in separate terminals:
python streaming_event_generator.py --duration-seconds 3600 --interval-seconds 10
python streaming_Agent_subscriber.py result
python streaming_Agent_subscriber.py telemetry
python streaming_Agent_subscriber.py quarantine
python streaming_curate.py
```

## Layout

```
python/
  catalog_client.py               BigLake Iceberg REST catalog connection
  gcloud_auth_manager.py          gcloud-CLI-token auth workaround
  bq_client.py                    write_attempts logging table (scripted/agent study)
  create_shared_table.py          upstream Iceberg table schema + creation
  reset_table.py                  drop/recreate the table between conditions
  baseline_writer.py              fixed-schedule scripted writer + shared commit/retry logic
  agent_writer.py                 Gemini-decision-timed writer (reuses baseline_writer's machinery)
  catalog_connectivity_test.py    one-time catalog round-trip check
  streaming_event_generator.py    Pub/Sub source event publisher
  streaming_Agent_subscriber.py   per-role event-driven agent (reuses baseline_writer's machinery)
  streaming_curate.py             on-arrival curate layer -> downstream table
  create_curated_summary_table.py downstream Iceberg table schema + creation
  create_streaming_log_tables.py  BigQuery log tables for the streaming extension
  streaming_run_analysis.sql      analysis queries used to produce the results below
  streaming_run_results.txt       full results/log from the hour-long streaming run
```

## Limitations

- Single hour-long streaming run (manually stopped at ~68 minutes); the scan-cost correlation in that run is n=4 — directionally consistent with the earlier scripted study's larger-sample finding, not an independent confirmation.
- `gcp_profile.json` (project-specific IDs) and `current_run.json` (per-run coordination state, regenerated by `start_run.py`) are gitignored; see `python/gcp_profile.json.example`.
