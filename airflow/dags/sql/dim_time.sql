WITH
  parseDateTime64BestEffort('{{ start_ts }}') AS start_ts,
  parseDateTime64BestEffort('{{ end_ts }}') AS end_ts
INSERT INTO {{ params.target_table }} (
  time_key,
  hour,
  minute,
  second,
  updated_at
)
SELECT
  s.time_key,
  s.hour,
  s.minute,
  s.second,
  now64(3, 'UTC') AS updated_at
FROM (
  SELECT DISTINCT
    toUInt32(toHour(event_ts) * 10000 + toMinute(event_ts) * 100 + toSecond(event_ts)) AS time_key,
    toHour(event_ts) AS hour,
    toMinute(event_ts) AS minute,
    toSecond(event_ts) AS second
  FROM (
    SELECT event_ts
    FROM bronze.wazuh_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
    UNION ALL
    SELECT event_ts
    FROM bronze.suricata_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
    UNION ALL
    SELECT event_ts
    FROM bronze.zeek_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
  )
) s
LEFT JOIN {{ params.target_table }} d
  ON d.time_key = s.time_key
WHERE d.time_key IS NULL;
