import json
import logging
import os

import redis

from airflow import DAG  # imported so Airflow safe mode parses this file
from dag_factory import build_datasource_to_dwh_dag

REDIS_HOST = os.environ.get("METADATA_REDIS_HOST", "metadata-redis")
REDIS_PORT = int(os.environ.get("METADATA_REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("METADATA_REDIS_DB", "0"))
REDIS_KEY = os.environ.get("METADATA_REDIS_KEY", "pipelines")


def _load_metadata():
    try:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        payload = client.get(REDIS_KEY)
        if not payload:
            logging.warning("No metadata found in Redis key %s", REDIS_KEY)
            return {"dags": []}
        data = json.loads(payload)
        if not isinstance(data, dict) or "dags" not in data:
            logging.warning("Invalid metadata payload in Redis key %s", REDIS_KEY)
            return {"dags": []}
        return data
    except Exception as exc:
        logging.warning("Redis metadata unavailable: %s", exc)
        return {"dags": []}


metadata = _load_metadata()
for dag_cfg in metadata.get("dags", []):
    if not isinstance(dag_cfg, dict):
        continue
    if not dag_cfg.get("enabled", True):
        continue
    dag_name = dag_cfg.get("dag_name")
    if not dag_name:
        continue
    try:
        dag = build_datasource_to_dwh_dag(dag_cfg)
    except Exception as exc:
        logging.warning("Failed to build DAG %s: %s", dag_name, exc)
        continue
    globals()[dag_name] = dag
