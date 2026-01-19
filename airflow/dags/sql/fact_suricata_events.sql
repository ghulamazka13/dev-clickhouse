WITH
  parseDateTime64BestEffort('{{ start_ts }}') AS start_ts,
  parseDateTime64BestEffort('{{ end_ts }}') AS end_ts
INSERT INTO {{ params.target_table }} (
  event_id,
  event_ts,
  date_key,
  time_key,
  sensor_key,
  signature_key,
  protocol_key,
  event_type,
  severity,
  src_ip,
  dest_ip,
  src_port,
  dest_port,
  bytes,
  packets,
  flow_id,
  http_url,
  message,
  updated_at
)
SELECT
  b.event_id,
  b.event_ts,
  toYYYYMMDD(b.event_ts) AS date_key,
  toUInt32(toHour(b.event_ts) * 10000 + toMinute(b.event_ts) * 100 + toSecond(b.event_ts)) AS time_key,
  s.sensor_key,
  sig.signature_key,
  p.protocol_key,
  b.event_type,
  b.severity,
  b.src_ip,
  b.dest_ip,
  b.src_port,
  b.dest_port,
  b.bytes,
  b.packets,
  b.flow_id,
  b.http_url,
  b.message,
  now64(3, 'UTC') AS updated_at
FROM bronze.suricata_events_raw b
LEFT JOIN gold.dim_sensor s
  ON s.sensor_key = cityHash64(ifNull(b.sensor_type, ''), ifNull(b.sensor_name, ''))
LEFT JOIN gold.dim_signature sig
  ON sig.signature_key = cityHash64(
    ifNull(b.signature_id, -1),
    ifNull(b.signature, ''),
    ifNull(b.category, ''),
    ifNull(b.alert_action, '')
  )
LEFT JOIN gold.dim_protocol p
  ON p.protocol_key = cityHash64(ifNull(b.protocol, ''))
LEFT JOIN {{ params.target_table }} existing
  ON existing.event_id = b.event_id
  AND existing.event_ts = b.event_ts
WHERE b.event_ts >= start_ts AND b.event_ts < end_ts
  AND existing.event_id IS NULL;
