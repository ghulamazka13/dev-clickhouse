# Near Real-Time Security Analytics POC

This repo is an end-to-end open source near real-time analytics POC using a Medallion architecture and metadata-driven Airflow.

Dataflow
- Dummy producer -> Kafka -> Kafka UI -> RisingWave -> Postgres bronze
- Airflow builds silver and gold, plus monitoring and data quality
- Superset reads gold only using bi_reader

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
- Kafka UI: http://localhost:18080
- Airflow: http://localhost:8088 (admin/admin)
- Superset: http://localhost:8089 (admin/admin)

4) Trigger the pipeline (optional)

```bash
docker compose exec -T airflow-webserver airflow dags trigger main_controller_dag
```

5) Check data in Postgres

```bash
docker compose exec -T postgres psql -U postgres -d analytics
```

## Access control (Medallion + grants)
- bronze: raw, restricted
- silver: cleaned, restricted
- gold: business, BI readable
- control: metadata
- monitoring: pipeline metrics

Roles
- rw_writer: insert only to bronze
- etl_runner: select bronze, DML silver/gold/monitoring
- bi_reader: select only on gold (Superset must use this)

## pg_duckdb
The Postgres image includes pg_duckdb. It is enabled on init:

```sql
CREATE EXTENSION IF NOT EXISTS pg_duckdb;
```

## Airflow metadata-driven pipelines
- Control tables live in control.*
- main_controller_dag reads control.pipeline_definitions and builds per-pipeline TaskGroups
- dag_factory implements lag, schema checks, volume checks, silver/gold builds, DQ, snapshots, and alerts

## Superset
Use the bi_reader account so only gold schema is visible. See superset/bootstrap/README_superset.md.

## Producer configuration
Environment variables for producer/producer.py:
- EVENTS_PER_SEC (default 10)
- MIX_ZEEK_PERCENT (default 60)
- MIX_SURICATA_PERCENT (default 40)
- SEED (default 42)
- KAFKA_BROKERS (default kafka:9092)
- KAFKA_TOPIC (default raw.security_events)

## Smoke test
Use scripts/smoke_test.sh (bash shell). On Windows, run via WSL or Git Bash.

## Backfill
Trigger the backfill DAG with dag_run.conf:

```bash
docker compose exec -T airflow-webserver airflow dags trigger backfill_maintenance_dag -c '{"pipeline_id":"security_events","start_ts":"2026-01-01T00:00:00Z","end_ts":"2026-01-02T00:00:00Z"}'
```

## Logstash future (Kafka output)
To replace the dummy producer with Logstash later:

```conf
output {
  kafka {
    bootstrap_servers => "kafka:9092"
    topic_id => "raw.security_events"
    codec => json
  }
}
```

Downstream remains unchanged.

## Troubleshooting
- If RisingWave init fails, rerun: docker compose run --rm risingwave-init
- If Airflow DAGs are missing, wait for the scheduler to parse or restart airflow-scheduler
- If Superset is empty, add the Postgres database using bi_reader and create datasets from gold schema
