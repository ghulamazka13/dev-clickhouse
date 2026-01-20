INSERT INTO {{ params.target_table }} (
  event_id,
  event_ts,
  event_ingested_ts,
  event_start_ts,
  event_end_ts,
  date_key,
  time_key,
  agent_key,
  host_key,
  rule_key,
  event_key,
  lag_seconds,
  duration_seconds,
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
  a.agent_key,
  h.host_key,
  r.rule_key,
  e.event_key,
  if(b.event_ingested_ts IS NULL, NULL, dateDiff('second', b.event_ts, b.event_ingested_ts)) AS lag_seconds,
  if(b.event_start_ts IS NULL OR b.event_end_ts IS NULL, NULL, dateDiff('second', b.event_start_ts, b.event_end_ts)) AS duration_seconds,
  b.message,
  now64(3, 'UTC') AS updated_at
FROM bronze.wazuh_events_raw b
ASOF LEFT JOIN gold.dim_agent a
  ON a.agent_name = coalesce(nullIf(b.agent_name, ''), toString(b.agent_ip))
  AND b.event_ts >= a.effective_from
ASOF LEFT JOIN gold.dim_host h
  ON h.host_name = coalesce(nullIf(b.host_name, ''), toString(b.host_ip))
  AND b.event_ts >= h.effective_from
ASOF LEFT JOIN gold.dim_rule r
  ON r.rule_id = nullIf(b.rule_id, '')
  AND b.event_ts >= r.effective_from
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
