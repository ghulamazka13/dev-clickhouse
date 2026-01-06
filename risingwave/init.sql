DROP SINK IF EXISTS security_events_sink;
DROP MATERIALIZED VIEW IF EXISTS security_events_mv;
DROP SOURCE IF EXISTS security_events_source;

CREATE SOURCE IF NOT EXISTS security_events_source (
  event_id text,
  event_time text,
  sensor_type text,
  sensor_name text,
  event_type text,
  severity text,
  src_ip text,
  dest_ip text,
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
  message text
)
WITH (
  connector = 'kafka',
  topic = 'raw.security_events',
  properties.bootstrap.server = 'kafka:9092',
  scan.startup.mode = 'latest'
)
FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW IF NOT EXISTS security_events_mv AS
SELECT
  event_id,
  event_time::timestamptz AS event_ts,
  sensor_type,
  sensor_name,
  event_type,
  severity,
  src_ip,
  dest_ip,
  src_port,
  dest_port,
  protocol,
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
FROM security_events_source;

CREATE SINK IF NOT EXISTS security_events_sink
FROM security_events_mv
WITH (
  connector = 'jdbc',
  jdbc.url = 'jdbc:postgresql://postgres:5432/analytics?user=rw_writer&password=rw_writer',
  schema.name = 'bronze',
  table.name = 'security_events_raw',
  type = 'append-only'
);
