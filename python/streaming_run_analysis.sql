-- Analysis queries for the streaming extension's hour-long run.
-- Run started 2026-09-16T18:52:26Z (experiment_run_id 20260916T185226_3e4e6e43,
-- current_run.json) -- all queries below are scoped to that timestamp so
-- none of the earlier short validation-test data leaks in.

-- ---------------------------------------------------------------------------
-- 1. Fan-out verification (confirm the concurrency fix held for the full run)
-- ---------------------------------------------------------------------------
SELECT COUNT(*) AS total_events,
       COUNTIF(roles_reached = 3) AS full_fanout,
       COUNTIF(roles_reached < 3) AS incomplete_fanout
FROM (
  SELECT event_id, COUNT(DISTINCT writer_role) AS roles_reached
  FROM `project-c76e1c3d-e880-4f96-877.iceberg_concurrency_study.streaming_write_attempts`
  WHERE attempt_timestamp >= '2026-09-16T18:52:26Z'
  GROUP BY event_id
);

-- ---------------------------------------------------------------------------
-- 2. Per-role outcome summary (the headline result table)
-- ---------------------------------------------------------------------------
SELECT writer_role, outcome, COUNT(*) AS n,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY writer_role), 1) AS pct,
       AVG(retry_count) AS avg_retries
FROM `project-c76e1c3d-e880-4f96-877.iceberg_concurrency_study.streaming_write_attempts`
WHERE attempt_timestamp >= '2026-09-16T18:52:26Z'
GROUP BY writer_role, outcome
ORDER BY writer_role, outcome;

-- ---------------------------------------------------------------------------
-- 3. Quarantine's scan-cost mechanism under streaming (extends the r>0.96 finding)
-- Correlation coefficient to compute from this output the same way as the
-- original scripted_3/agent_3 comparison -- even a small n is worth
-- reporting alongside the original four data points if the relationship
-- holds directionally.
-- ---------------------------------------------------------------------------
SELECT attempt_id, table_row_count_at_attempt, first_attempt_duration_ms, outcome
FROM `project-c76e1c3d-e880-4f96-877.iceberg_concurrency_study.streaming_write_attempts`
WHERE writer_role = 'quarantine_writer' AND attempt_timestamp >= '2026-09-16T18:52:26Z'
ORDER BY attempt_id;

-- ---------------------------------------------------------------------------
-- 4. Curate layer freshness lag over time (new headline figure)
-- Plot as a time series (curated_at on x-axis, freshness_lag_ms on y-axis)
-- -- the shape of this curve (still climbing vs. plateauing) is the key
-- visual for this finding.
-- ---------------------------------------------------------------------------
SELECT curated_at, source_role, freshness_lag_ms
FROM `project-c76e1c3d-e880-4f96-877.iceberg_concurrency_study.curate_events`
WHERE curated_at >= '2026-09-16T18:52:26Z'
ORDER BY curated_at;

-- ---------------------------------------------------------------------------
-- 5. Realized event inter-arrival distribution (confirms genuine streaming,
-- not disguised polling)
-- ---------------------------------------------------------------------------
SELECT MIN(inter_arrival_ms) AS min_ms, MAX(inter_arrival_ms) AS max_ms,
       AVG(inter_arrival_ms) AS avg_ms,
       APPROX_QUANTILES(inter_arrival_ms, 4)[OFFSET(2)] AS median_ms
FROM `project-c76e1c3d-e880-4f96-877.iceberg_concurrency_study.streaming_write_attempts`
WHERE writer_role = 'result_writer' AND attempt_timestamp >= '2026-09-16T18:52:26Z';

-- ---------------------------------------------------------------------------
-- 6. Cross-role timing correlation (tests the "backlog-draining bursts"
-- explanation directly). Plot as three lines (one per role) over time --
-- if quarantine's failures cluster specifically during minutes where
-- result/telemetry show commit spikes, that's direct, visual evidence
-- for the contention explanation rather than just an inference.
-- ---------------------------------------------------------------------------
SELECT DATE_TRUNC(attempt_timestamp, MINUTE) AS minute,
       writer_role, COUNT(*) AS commits_this_minute
FROM `project-c76e1c3d-e880-4f96-877.iceberg_concurrency_study.streaming_write_attempts`
WHERE attempt_timestamp >= '2026-09-16T18:52:26Z' AND outcome IN ('committed', 'retried_committed')
GROUP BY minute, writer_role
ORDER BY minute, writer_role;

-- ---------------------------------------------------------------------------
-- 7. Final drain-completeness check (confirm the true final counts once
-- the pipeline is stopped)
-- ---------------------------------------------------------------------------
SELECT writer_role, COUNT(*) AS total_processed,
       MAX(attempt_timestamp) AS last_attempt
FROM `project-c76e1c3d-e880-4f96-877.iceberg_concurrency_study.streaming_write_attempts`
WHERE attempt_timestamp >= '2026-09-16T18:52:26Z'
GROUP BY writer_role;
