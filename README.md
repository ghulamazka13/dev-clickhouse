# Near Real-Time Security Analytics POC

This repo is an end-to-end near real-time analytics POC using a Medallion architecture with ClickHouse storage and Airflow batch transforms.

Dataflow
- Redpanda (remote) -> ClickHouse Kafka Engine -> bronze.* (raw)
- Airflow runs ClickHouse SQL to build the gold star schema (dims/facts/bridges)
- Superset reads gold only

## Quickstart

1) Start the stack

```bash
docker compose up -d
```

2) Wait for services

```bash
docker compose ps
```

3) Open UIs
- Airflow: http://localhost:8088 (admin/admin)
- Superset: http://localhost:8089 (admin/admin)
- ClickHouse HTTP: http://localhost:8123
- CHouse UI: http://localhost:8087

4) Trigger the gold pipeline (optional)

```bash
docker compose exec -T airflow-webserver airflow dags trigger gold_star_schema
```

5) Check data in ClickHouse

```bash
docker compose exec -T clickhouse clickhouse-client \
  --user etl_runner --password etl_runner \
  --query "SELECT count() FROM bronze.suricata_events_raw;"
```

## Access control
ClickHouse RBAC is enabled on init.
- etl_runner: read/write bronze + gold
- superset: read-only on gold

Default credentials:
- ClickHouse admin: admin/admin
- etl_runner: etl_runner/etl_runner
- superset: superset/superset

Roles/users are defined in `clickhouse/init/00_databases.sql`.

## ClickHouse ingestion (Kafka Engine)
- Kafka table: `bronze.security_events_kafka`
- Materialized Views: `bronze.suricata_events_mv`, `bronze.wazuh_events_mv`, `bronze.zeek_events_mv`
- Broker: `10.110.12.20:9092`
- Topic: `malcolm-logs`

## Airflow gold pipeline
- DAG: `gold_star_schema`
- Metadata source-of-truth: Postgres tables `metadata.gold_dags` + `metadata.gold_pipelines`
- Metadata updater DAG: `metadata_updater` (exports a YAML snapshot to `airflow/dags/gold_pipelines.yml`)
- SQL templates: `airflow/dags/sql/*.sql` (one pipeline per file)
- Default window: 10 minutes (override with `dag_run.conf` `start_ts`/`end_ts` or `window_minutes`)

To add a new gold pipeline:
1) Insert a row in `metadata.gold_pipelines` with `pipeline_id`, `sql_path`, and `target_table`
2) Create the SQL template under `airflow/dags/sql/` using `{{ start_ts }}`, `{{ end_ts }}`, and `{{ params.* }}`
3) Trigger `metadata_updater` (optional) to regenerate the YAML snapshot

Example backfill run:

```bash
docker compose exec -T airflow-webserver airflow dags trigger gold_star_schema \
  -c '{"start_ts":"2026-01-01T00:00:00Z","end_ts":"2026-01-02T00:00:00Z"}'
```

Run a single pipeline:

```bash
docker compose exec -T airflow-webserver airflow dags trigger gold_star_schema \
  -c '{"pipeline_id":"fact_wazuh_events","start_ts":"2026-01-01T00:00:00Z","end_ts":"2026-01-02T00:00:00Z"}'
```

## Metadata store (Postgres)
Metadata schema and seed data live in `postgres/init/10_metadata.sql`.
If the Postgres volume already exists, apply the SQL manually:

```bash
docker compose exec -T postgres psql -U airflow -d airflow -f /docker-entrypoint-initdb.d/10_metadata.sql
```

Example insert for a new pipeline:

```sql
INSERT INTO metadata.gold_pipelines (
  dag_id,
  pipeline_id,
  enabled,
  sql_path,
  window_minutes,
  depends_on,
  target_table,
  pipeline_order
) VALUES (
  'gold_star_schema',
  'fact_new_events',
  TRUE,
  'sql/fact_new_events.sql',
  10,
  ARRAY['dim_date', 'dim_time'],
  'gold.fact_new_events',
  20
);
```

## Superset
Use the ClickHouse connector:

```
clickhouse+connect://superset:superset@clickhouse:8123/default
```

Add datasets from the gold schema (facts + dims).

## Optional one-time backfill (from Postgres)
If you have historical data in Postgres, run a one-time import into ClickHouse using the `postgresql` table function or CSV streaming. See `scripts/clickhouse_examples.sql` for query examples and adapt the import to your source.

## Troubleshooting
- If Kafka ingestion is empty, verify the broker `10.110.12.20:9092` is reachable from the ClickHouse container.
- If gold DAG fails, check Airflow logs and ClickHouse permissions for `etl_runner`.
