#!/usr/bin/env bash
set -euo pipefail

docker compose up -d

echo "Waiting for services..."
sleep 30

docker compose exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | grep raw.security_events

docker compose up -d producer

echo "Bronze row count:"
docker compose exec -T postgres psql -U postgres -d analytics -c "SELECT count(*) FROM bronze.security_events_raw;"

echo "Triggering Airflow controller DAG..."
docker compose exec -T airflow-webserver airflow dags trigger main_controller_dag

echo "Open Kafka UI: http://localhost:8080"
echo "Open Airflow: http://localhost:8088"
echo "Open Superset: http://localhost:8089"
