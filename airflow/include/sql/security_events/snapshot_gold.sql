CREATE TABLE IF NOT EXISTS gold.snapshot_alerts_5m (
  run_id uuid,
  run_ts timestamptz,
  pipeline_id text,
  window_start timestamptz,
  window_end timestamptz,
  severity text,
  alert_count bigint,
  unique_src_ips bigint,
  unique_dest_ips bigint,
  max_severity_score int
);

CREATE TABLE IF NOT EXISTS gold.snapshot_alerts_1h (
  run_id uuid,
  run_ts timestamptz,
  pipeline_id text,
  window_start timestamptz,
  window_end timestamptz,
  severity text,
  alert_count bigint,
  unique_src_ips bigint,
  unique_dest_ips bigint,
  max_severity_score int
);

CREATE TABLE IF NOT EXISTS gold.snapshot_alerts_daily (
  run_id uuid,
  run_ts timestamptz,
  pipeline_id text,
  event_date date,
  severity text,
  alert_count bigint,
  unique_src_ips bigint,
  unique_dest_ips bigint,
  max_severity_score int
);

CREATE TABLE IF NOT EXISTS gold.snapshot_top_talkers_daily (
  run_id uuid,
  run_ts timestamptz,
  pipeline_id text,
  event_date date,
  src_ip inet,
  dest_ip inet,
  total_bytes bigint,
  event_count bigint,
  rank int
);

CREATE TABLE IF NOT EXISTS gold.snapshot_top_signatures_daily (
  run_id uuid,
  run_ts timestamptz,
  pipeline_id text,
  event_date date,
  signature text,
  signature_id int,
  alert_count bigint,
  rank int
);

CREATE TABLE IF NOT EXISTS gold.snapshot_protocol_mix_daily (
  run_id uuid,
  run_ts timestamptz,
  pipeline_id text,
  event_date date,
  protocol text,
  event_count bigint,
  bytes_sum bigint,
  pct_of_total numeric(5,2)
);

INSERT INTO gold.snapshot_runs (run_id, run_ts, pipeline_id, status, notes)
VALUES ('{{ params.run_id }}'::uuid, now(), '{{ params.pipeline_id }}', 'success', 'snapshot')
ON CONFLICT (run_id) DO NOTHING;

INSERT INTO gold.snapshot_alerts_5m (
  run_id, run_ts, pipeline_id, window_start, window_end, severity,
  alert_count, unique_src_ips, unique_dest_ips, max_severity_score
)
SELECT
  '{{ params.run_id }}'::uuid, now(), '{{ params.pipeline_id }}',
  window_start, window_end, severity,
  alert_count, unique_src_ips, unique_dest_ips, max_severity_score
FROM gold.alerts_5m;

INSERT INTO gold.snapshot_alerts_1h (
  run_id, run_ts, pipeline_id, window_start, window_end, severity,
  alert_count, unique_src_ips, unique_dest_ips, max_severity_score
)
SELECT
  '{{ params.run_id }}'::uuid, now(), '{{ params.pipeline_id }}',
  window_start, window_end, severity,
  alert_count, unique_src_ips, unique_dest_ips, max_severity_score
FROM gold.alerts_1h;

INSERT INTO gold.snapshot_alerts_daily (
  run_id, run_ts, pipeline_id, event_date, severity,
  alert_count, unique_src_ips, unique_dest_ips, max_severity_score
)
SELECT
  '{{ params.run_id }}'::uuid, now(), '{{ params.pipeline_id }}',
  event_date, severity,
  alert_count, unique_src_ips, unique_dest_ips, max_severity_score
FROM gold.alerts_daily;

INSERT INTO gold.snapshot_top_talkers_daily (
  run_id, run_ts, pipeline_id, event_date, src_ip, dest_ip,
  total_bytes, event_count, rank
)
SELECT
  '{{ params.run_id }}'::uuid, now(), '{{ params.pipeline_id }}',
  event_date, src_ip, dest_ip,
  total_bytes, event_count, rank
FROM gold.top_talkers_daily;

INSERT INTO gold.snapshot_top_signatures_daily (
  run_id, run_ts, pipeline_id, event_date, signature, signature_id,
  alert_count, rank
)
SELECT
  '{{ params.run_id }}'::uuid, now(), '{{ params.pipeline_id }}',
  event_date, signature, signature_id,
  alert_count, rank
FROM gold.top_signatures_daily;

INSERT INTO gold.snapshot_protocol_mix_daily (
  run_id, run_ts, pipeline_id, event_date, protocol,
  event_count, bytes_sum, pct_of_total
)
SELECT
  '{{ params.run_id }}'::uuid, now(), '{{ params.pipeline_id }}',
  event_date, protocol,
  event_count, bytes_sum, pct_of_total
FROM gold.protocol_mix_daily;
