CREATE TABLE IF NOT EXISTS gold.alerts_1h (
  window_start timestamptz,
  window_end timestamptz,
  severity text,
  alert_count bigint,
  unique_src_ips bigint,
  unique_dest_ips bigint,
  max_severity_score int,
  PRIMARY KEY (window_start, severity)
);

WITH windowed AS (
  SELECT
    date_trunc('hour', event_ts) AS window_start,
    severity,
    src_ip,
    dest_ip,
    severity_score
  FROM silver.security_events
  WHERE event_ts >= now() - interval '2 hours'
    AND is_alert = true
)
INSERT INTO gold.alerts_1h (
  window_start,
  window_end,
  severity,
  alert_count,
  unique_src_ips,
  unique_dest_ips,
  max_severity_score
)
SELECT
  window_start,
  window_start + interval '1 hour' AS window_end,
  severity,
  count(*) AS alert_count,
  count(distinct src_ip) AS unique_src_ips,
  count(distinct dest_ip) AS unique_dest_ips,
  max(severity_score) AS max_severity_score
FROM windowed
GROUP BY window_start, severity
ON CONFLICT (window_start, severity) DO UPDATE SET
  window_end = EXCLUDED.window_end,
  alert_count = EXCLUDED.alert_count,
  unique_src_ips = EXCLUDED.unique_src_ips,
  unique_dest_ips = EXCLUDED.unique_dest_ips,
  max_severity_score = EXCLUDED.max_severity_score;
