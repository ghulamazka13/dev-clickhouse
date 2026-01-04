CREATE TABLE IF NOT EXISTS gold.top_signatures_daily (
  event_date date,
  signature text,
  signature_id int,
  alert_count bigint,
  rank int,
  PRIMARY KEY (event_date, rank, signature_id)
);

WITH daily AS (
  SELECT
    event_date,
    signature,
    signature_id,
    count(*) AS alert_count
  FROM silver.security_events
  WHERE event_date >= current_date - interval '7 days'
    AND signature IS NOT NULL
  GROUP BY event_date, signature, signature_id
),
ranked AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY event_date ORDER BY alert_count DESC) AS rank
  FROM daily
)
INSERT INTO gold.top_signatures_daily (
  event_date, signature, signature_id, alert_count, rank
)
SELECT event_date, signature, signature_id, alert_count, rank
FROM ranked
WHERE rank <= 20
ON CONFLICT (event_date, rank, signature_id) DO UPDATE SET
  signature = EXCLUDED.signature,
  alert_count = EXCLUDED.alert_count;
