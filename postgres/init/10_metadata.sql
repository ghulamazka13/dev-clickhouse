CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.gold_dags (
  dag_id TEXT PRIMARY KEY,
  schedule_cron TEXT NOT NULL,
  timezone TEXT NOT NULL,
  owner TEXT NOT NULL,
  tags TEXT[] NOT NULL DEFAULT '{}',
  max_active_tasks INTEGER NOT NULL DEFAULT 8,
  default_window_minutes INTEGER NOT NULL DEFAULT 10,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS metadata.gold_pipelines (
  dag_id TEXT NOT NULL REFERENCES metadata.gold_dags(dag_id) ON DELETE CASCADE,
  pipeline_id TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  sql_path TEXT NOT NULL,
  window_minutes INTEGER,
  depends_on TEXT[],
  target_table TEXT NOT NULL,
  params JSONB NOT NULL DEFAULT '{}'::jsonb,
  pipeline_order INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (dag_id, pipeline_id)
);

INSERT INTO metadata.gold_dags (
  dag_id,
  schedule_cron,
  timezone,
  owner,
  tags,
  max_active_tasks,
  default_window_minutes,
  enabled
)
VALUES (
  'gold_star_schema',
  '*/5 * * * *',
  'Asia/Jakarta',
  'data-eng',
  ARRAY['gold', 'clickhouse', 'star-schema'],
  8,
  10,
  TRUE
)
ON CONFLICT (dag_id) DO NOTHING;

INSERT INTO metadata.gold_pipelines (
  dag_id,
  pipeline_id,
  enabled,
  sql_path,
  window_minutes,
  depends_on,
  target_table,
  pipeline_order
)
VALUES
  ('gold_star_schema', 'dim_date', TRUE, 'sql/dim_date.sql', 10, NULL, 'gold.dim_date', 1),
  ('gold_star_schema', 'dim_time', TRUE, 'sql/dim_time.sql', 10, NULL, 'gold.dim_time', 2),
  ('gold_star_schema', 'dim_event', TRUE, 'sql/dim_event.sql', 10, NULL, 'gold.dim_event', 3),
  ('gold_star_schema', 'dim_sensor', TRUE, 'sql/dim_sensor.sql', 10, NULL, 'gold.dim_sensor', 4),
  ('gold_star_schema', 'dim_protocol', TRUE, 'sql/dim_protocol.sql', 10, NULL, 'gold.dim_protocol', 5),
  ('gold_star_schema', 'dim_signature', TRUE, 'sql/dim_signature.sql', 10, NULL, 'gold.dim_signature', 6),
  ('gold_star_schema', 'dim_tag', TRUE, 'sql/dim_tag.sql', 10, NULL, 'gold.dim_tag', 7),
  ('gold_star_schema', 'dim_agent_scd2', TRUE, 'sql/dim_agent_scd2.sql', 10, NULL, 'gold.dim_agent', 8),
  ('gold_star_schema', 'dim_host_scd2', TRUE, 'sql/dim_host_scd2.sql', 10, NULL, 'gold.dim_host', 9),
  ('gold_star_schema', 'dim_rule_scd2', TRUE, 'sql/dim_rule_scd2.sql', 10, NULL, 'gold.dim_rule', 10),
  ('gold_star_schema', 'fact_wazuh_events', TRUE, 'sql/fact_wazuh_events.sql', 10, ARRAY['dim_date', 'dim_time', 'dim_agent_scd2', 'dim_host_scd2', 'dim_rule_scd2', 'dim_event'], 'gold.fact_wazuh_events', 11),
  ('gold_star_schema', 'fact_suricata_events', TRUE, 'sql/fact_suricata_events.sql', 10, ARRAY['dim_date', 'dim_time', 'dim_sensor', 'dim_signature', 'dim_protocol'], 'gold.fact_suricata_events', 12),
  ('gold_star_schema', 'fact_zeek_events', TRUE, 'sql/fact_zeek_events.sql', 10, ARRAY['dim_date', 'dim_time', 'dim_sensor', 'dim_protocol', 'dim_event'], 'gold.fact_zeek_events', 13),
  ('gold_star_schema', 'bridge_wazuh_event_tag', TRUE, 'sql/bridge_wazuh_event_tag.sql', 10, ARRAY['dim_tag', 'fact_wazuh_events'], 'gold.bridge_wazuh_event_tag', 14),
  ('gold_star_schema', 'bridge_suricata_event_tag', TRUE, 'sql/bridge_suricata_event_tag.sql', 10, ARRAY['dim_tag', 'fact_suricata_events'], 'gold.bridge_suricata_event_tag', 15),
  ('gold_star_schema', 'bridge_zeek_event_tag', TRUE, 'sql/bridge_zeek_event_tag.sql', 10, ARRAY['dim_tag', 'fact_zeek_events'], 'gold.bridge_zeek_event_tag', 16)
ON CONFLICT (dag_id, pipeline_id) DO NOTHING;
