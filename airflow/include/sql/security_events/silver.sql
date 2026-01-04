CREATE TABLE IF NOT EXISTS silver.security_events (
  event_id text PRIMARY KEY,
  event_ts timestamptz,
  event_date date,
  event_hour timestamptz,
  sensor_type text,
  sensor_name text,
  event_type text,
  severity text,
  severity_score int,
  is_alert boolean,
  src_ip inet,
  dest_ip inet,
  src_port int,
  dest_port int,
  protocol text,
  bytes bigint,
  packets bigint,
  uid text,
  conn_state text,
  duration double precision,
  signature text,
  signature_id int,
  category text,
  alert_action text,
  tags jsonb,
  message text,
  ingested_at timestamptz default now()
);

WITH src AS (
  SELECT
    event_id,
    event_ts,
    (event_ts AT TIME ZONE 'UTC')::date AS event_date,
    date_trunc('hour', event_ts) AS event_hour,
    lower(sensor_type) AS sensor_type,
    sensor_name,
    lower(event_type) AS event_type,
    lower(severity) AS severity,
    CASE lower(severity)
      WHEN 'low' THEN 1
      WHEN 'medium' THEN 5
      WHEN 'high' THEN 10
      ELSE 0
    END AS severity_score,
    (lower(event_type) = 'alert') AS is_alert,
    src_ip,
    dest_ip,
    src_port,
    dest_port,
    lower(protocol) AS protocol,
    bytes,
    packets,
    uid,
    conn_state,
    duration,
    signature,
    signature_id,
    category,
    alert_action,
    tags,
    message
  FROM bronze.security_events_raw
  WHERE event_ts >= COALESCE((SELECT max(event_ts) FROM silver.security_events), '1970-01-01'::timestamptz) - interval '5 minutes'
    AND event_ts IS NOT NULL
)
INSERT INTO silver.security_events (
  event_id, event_ts, event_date, event_hour, sensor_type, sensor_name, event_type, severity,
  severity_score, is_alert, src_ip, dest_ip, src_port, dest_port, protocol, bytes, packets,
  uid, conn_state, duration, signature, signature_id, category, alert_action, tags, message
)
SELECT
  event_id, event_ts, event_date, event_hour, sensor_type, sensor_name, event_type, severity,
  severity_score, is_alert, src_ip, dest_ip, src_port, dest_port, protocol, bytes, packets,
  uid, conn_state, duration, signature, signature_id, category, alert_action, tags, message
FROM src
ON CONFLICT (event_id) DO UPDATE SET
  event_ts = EXCLUDED.event_ts,
  event_date = EXCLUDED.event_date,
  event_hour = EXCLUDED.event_hour,
  sensor_type = EXCLUDED.sensor_type,
  sensor_name = EXCLUDED.sensor_name,
  event_type = EXCLUDED.event_type,
  severity = EXCLUDED.severity,
  severity_score = EXCLUDED.severity_score,
  is_alert = EXCLUDED.is_alert,
  src_ip = EXCLUDED.src_ip,
  dest_ip = EXCLUDED.dest_ip,
  src_port = EXCLUDED.src_port,
  dest_port = EXCLUDED.dest_port,
  protocol = EXCLUDED.protocol,
  bytes = EXCLUDED.bytes,
  packets = EXCLUDED.packets,
  uid = EXCLUDED.uid,
  conn_state = EXCLUDED.conn_state,
  duration = EXCLUDED.duration,
  signature = EXCLUDED.signature,
  signature_id = EXCLUDED.signature_id,
  category = EXCLUDED.category,
  alert_action = EXCLUDED.alert_action,
  tags = EXCLUDED.tags,
  message = EXCLUDED.message;
