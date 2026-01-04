CREATE TABLE IF NOT EXISTS gold.top_talkers_daily (
  event_date date,
  src_ip inet,
  dest_ip inet,
  total_bytes bigint,
  event_count bigint,
  rank int,
  PRIMARY KEY (event_date, rank, src_ip, dest_ip)
);

WITH daily AS (
  SELECT
    event_date,
    src_ip,
    dest_ip,
    sum(bytes) AS total_bytes,
    count(*) AS event_count
  FROM silver.security_events
  WHERE event_date >= current_date - interval '7 days'
  GROUP BY event_date, src_ip, dest_ip
),
ranked AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY event_date ORDER BY total_bytes DESC) AS rank
  FROM daily
)
INSERT INTO gold.top_talkers_daily (
  event_date, src_ip, dest_ip, total_bytes, event_count, rank
)
SELECT event_date, src_ip, dest_ip, total_bytes, event_count, rank
FROM ranked
WHERE rank <= 20
ON CONFLICT (event_date, rank, src_ip, dest_ip) DO UPDATE SET
  total_bytes = EXCLUDED.total_bytes,
  event_count = EXCLUDED.event_count;
