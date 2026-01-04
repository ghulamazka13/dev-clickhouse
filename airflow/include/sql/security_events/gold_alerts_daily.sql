CREATE TABLE IF NOT EXISTS gold.alerts_daily (
  event_date date,
  severity text,
  alert_count bigint,
  unique_src_ips bigint,
  unique_dest_ips bigint,
  max_severity_score int,
  PRIMARY KEY (event_date, severity)
);

WITH daily AS (
  SELECT
    event_date,
    severity,
    src_ip,
    dest_ip,
    severity_score
  FROM silver.security_events
  WHERE event_date >= current_date - interval '7 days'
    AND is_alert = true
)
INSERT INTO gold.alerts_daily (
  event_date,
  severity,
  alert_count,
  unique_src_ips,
  unique_dest_ips,
  max_severity_score
)
SELECT
  event_date,
  severity,
  count(*) AS alert_count,
  count(distinct src_ip) AS unique_src_ips,
  count(distinct dest_ip) AS unique_dest_ips,
  max(severity_score) AS max_severity_score
FROM daily
GROUP BY event_date, severity
ON CONFLICT (event_date, severity) DO UPDATE SET
  alert_count = EXCLUDED.alert_count,
  unique_src_ips = EXCLUDED.unique_src_ips,
  unique_dest_ips = EXCLUDED.unique_dest_ips,
  max_severity_score = EXCLUDED.max_severity_score;
