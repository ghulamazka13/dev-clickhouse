CREATE TABLE IF NOT EXISTS gold.protocol_mix_daily (
  event_date date,
  protocol text,
  event_count bigint,
  bytes_sum bigint,
  pct_of_total numeric(5,2),
  PRIMARY KEY (event_date, protocol)
);

WITH daily AS (
  SELECT
    event_date,
    COALESCE(protocol, 'unknown') AS protocol,
    count(*) AS event_count,
    sum(bytes) AS bytes_sum
  FROM silver.security_events
  WHERE event_date >= current_date - interval '7 days'
  GROUP BY event_date, COALESCE(protocol, 'unknown')
),
totals AS (
  SELECT event_date, sum(event_count) AS total_count
  FROM daily
  GROUP BY event_date
)
INSERT INTO gold.protocol_mix_daily (
  event_date, protocol, event_count, bytes_sum, pct_of_total
)
SELECT
  d.event_date,
  d.protocol,
  d.event_count,
  d.bytes_sum,
  CASE WHEN t.total_count > 0
    THEN round(d.event_count::numeric / t.total_count * 100, 2)
    ELSE 0
  END AS pct_of_total
FROM daily d
JOIN totals t ON d.event_date = t.event_date
ON CONFLICT (event_date, protocol) DO UPDATE SET
  event_count = EXCLUDED.event_count,
  bytes_sum = EXCLUDED.bytes_sum,
  pct_of_total = EXCLUDED.pct_of_total;
