INSERT INTO {{ params.target_table }} (
  event_id,
  event_ts,
  event_ingested_ts,
  event_start_ts,
  event_end_ts,
  date_key,
  time_key,
  sensor_key,
  protocol_key,
  event_key,
  zeek_uid,
  src_ip,
  dest_ip,
  src_port,
  dest_port,
  application,
  network_type,
  direction,
  community_id,
  bytes,
  packets,
  orig_bytes,
  resp_bytes,
  orig_pkts,
  resp_pkts,
  conn_state,
  conn_state_description,
  duration_seconds,
  history,
  vlan_id,
  message,
  updated_at
)
WITH
  parseDateTime64BestEffort('{{ start_ts }}') AS start_ts,
  parseDateTime64BestEffort('{{ end_ts }}') AS end_ts
SELECT
  b.event_id,
  b.event_ts,
  b.event_ingested_ts,
  b.event_start_ts,
  b.event_end_ts,
  toYYYYMMDD(b.event_ts) AS date_key,
  toUInt32(toHour(b.event_ts) * 10000 + toMinute(b.event_ts) * 100 + toSecond(b.event_ts)) AS time_key,
  s.sensor_key,
  p.protocol_key,
  e.event_key,
  b.zeek_uid,
  b.src_ip,
  b.dest_ip,
  b.src_port,
  b.dest_port,
  b.application,
  b.network_type,
  b.direction,
  b.community_id,
  b.bytes,
  b.packets,
  b.orig_bytes,
  b.resp_bytes,
  b.orig_pkts,
  b.resp_pkts,
  b.conn_state,
  b.conn_state_description,
  b.duration AS duration_seconds,
  b.history,
  b.vlan_id,
  b.message,
  now64(3, 'UTC') AS updated_at
FROM bronze.zeek_events_raw b
LEFT JOIN gold.dim_sensor s
  ON s.sensor_key = cityHash64('zeek', ifNull(b.sensor_name, ''))
LEFT JOIN gold.dim_protocol p
  ON p.protocol_key = cityHash64(ifNull(b.protocol, ''))
LEFT JOIN gold.dim_event e
  ON e.event_key = cityHash64(
    ifNull(b.event_dataset, ''),
    ifNull(b.event_kind, ''),
    ifNull(b.event_module, ''),
    ifNull(b.event_provider, '')
  )
LEFT JOIN {{ params.target_table }} existing
  ON existing.event_id = b.event_id
  AND existing.event_ts = b.event_ts
WHERE b.event_ts >= start_ts AND b.event_ts < end_ts
  AND existing.event_id IS NULL;
