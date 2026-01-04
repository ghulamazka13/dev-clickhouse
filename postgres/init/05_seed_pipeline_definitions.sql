INSERT INTO control.pipeline_definitions (
  pipeline_id, enabled, schedule_cron, bronze_table, silver_table, gold_tables,
  silver_sql_path, gold_sql_paths, dq_profile, sla_minutes, freshness_threshold_minutes, owner
) VALUES (
  'security_events',
  true,
  '*/5 * * * *',
  'bronze.security_events_raw',
  'silver.security_events',
  '["gold.alerts_5m","gold.alerts_1h","gold.alerts_daily","gold.top_talkers_daily","gold.top_signatures_daily","gold.protocol_mix_daily"]'::jsonb,
  '/opt/airflow/include/sql/security_events/silver.sql',
  '[
    "/opt/airflow/include/sql/security_events/gold_alerts_5m.sql",
    "/opt/airflow/include/sql/security_events/gold_alerts_1h.sql",
    "/opt/airflow/include/sql/security_events/gold_alerts_daily.sql",
    "/opt/airflow/include/sql/security_events/gold_top_talkers_daily.sql",
    "/opt/airflow/include/sql/security_events/gold_top_signatures_daily.sql",
    "/opt/airflow/include/sql/security_events/gold_protocol_mix_daily.sql"
  ]'::jsonb,
  'security_events',
  10,
  10,
  'data-eng'
)
ON CONFLICT (pipeline_id) DO UPDATE SET
  enabled = EXCLUDED.enabled,
  schedule_cron = EXCLUDED.schedule_cron,
  bronze_table = EXCLUDED.bronze_table,
  silver_table = EXCLUDED.silver_table,
  gold_tables = EXCLUDED.gold_tables,
  silver_sql_path = EXCLUDED.silver_sql_path,
  gold_sql_paths = EXCLUDED.gold_sql_paths,
  dq_profile = EXCLUDED.dq_profile,
  sla_minutes = EXCLUDED.sla_minutes,
  freshness_threshold_minutes = EXCLUDED.freshness_threshold_minutes,
  owner = EXCLUDED.owner,
  updated_at = now();

INSERT INTO control.dq_rules (pipeline_id, rule_name, rule_type, params, severity)
VALUES
  ('security_events', 'no_missing_event_ts', 'null_rate', '{"column":"event_ts","threshold":0}', 'critical'),
  ('security_events', 'severity_values', 'allowed_values', '{"column":"severity","values":["low","medium","high"]}', 'critical'),
  ('security_events', 'no_duplicate_event_id', 'duplicate', '{"column":"event_id"}', 'critical')
ON CONFLICT DO NOTHING;
