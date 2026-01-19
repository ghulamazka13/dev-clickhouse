import logging
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

import clickhouse_connect
import pendulum
import yaml
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from jinja2 import Template


def _normalize_ts(value: Any) -> Optional[str]:
    if not value:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return str(value)


def _get_window(
    context: Dict[str, Any],
    default_window_minutes: int,
) -> Tuple[str, str]:
    params = context.get("params") or {}
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    start_ts = _normalize_ts(conf.get("start_ts") or params.get("start_ts"))
    end_ts = _normalize_ts(conf.get("end_ts") or params.get("end_ts"))
    if start_ts and end_ts:
        return start_ts, end_ts

    try:
        window_minutes = int(
            conf.get("window_minutes")
            or params.get("window_minutes")
            or default_window_minutes
        )
    except (TypeError, ValueError):
        window_minutes = default_window_minutes
    end = pendulum.now("UTC")
    start = end.subtract(minutes=window_minutes)
    return start.to_iso8601_string(), end.to_iso8601_string()


def _load_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def _render_sql(sql_text: str, context: Dict[str, Any]) -> str:
    return Template(sql_text).render(**context)


def _split_statements(sql_text: str) -> List[str]:
    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]
    return statements


def _normalize_list(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return [str(value)]


def _resolve_sql_path(base_dir: str, sql_path: str) -> str:
    if os.path.isabs(sql_path):
        return sql_path
    return os.path.join(base_dir, sql_path)


def run_pipeline(pipeline: Dict[str, Any], default_window_minutes: int, **context) -> None:
    pipeline_id = pipeline.get("pipeline_id")
    requested = _normalize_ts((context.get("params") or {}).get("pipeline_id"))
    if requested and requested != pipeline_id:
        logging.info("Skipping pipeline %s due to pipeline_id filter %s", pipeline_id, requested)
        return

    start_ts, end_ts = _get_window(context, pipeline.get("window_minutes", default_window_minutes))
    sql_path = pipeline["sql_path"]
    params = pipeline.get("params") or {}

    render_context = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "window_minutes": pipeline.get("window_minutes", default_window_minutes),
        "params": params,
        "pipeline_id": pipeline_id,
    }

    sql_text = _load_sql(sql_path)
    sql_text = _render_sql(sql_text, render_context)
    statements = _split_statements(sql_text)

    logging.info(
        "Running pipeline %s statements=%s window=%s -> %s",
        pipeline_id,
        len(statements),
        start_ts,
        end_ts,
    )

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "etl_runner"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "etl_runner"),
        database=os.environ.get("CLICKHOUSE_DATABASE", "default"),
    )
    try:
        for statement in statements:
            client.command(statement)
    finally:
        client.close()


class GoldPipelineGenerator:
    def __init__(self, config_path: Optional[str] = None):
        self._config_path = config_path or os.environ.get(
            "GOLD_PIPELINES_PATH",
            os.path.join(os.path.dirname(__file__), "..", "gold_pipelines.yml"),
        )

    def _get_metadata_uri(self) -> Optional[str]:
        return os.environ.get("METADATA_DATABASE_URI") or os.environ.get(
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
        )

    def _load_configs_from_file(self) -> List[Dict[str, Any]]:
        config_path = os.path.abspath(self._config_path)
        if not os.path.exists(config_path):
            logging.warning("Gold pipeline config not found at %s", config_path)
            return []
        with open(config_path, "r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        dag_cfg = payload.get("dag") or {}
        pipelines = payload.get("pipelines") or []
        if not isinstance(pipelines, list):
            logging.warning("Gold pipeline config missing pipelines list")
            pipelines = []
        dag_cfg = {
            "dag_id": dag_cfg.get("dag_id", "gold_star_schema"),
            "schedule_cron": dag_cfg.get("schedule_cron", "*/5 * * * *"),
            "timezone": dag_cfg.get("timezone", "Asia/Jakarta"),
            "owner": dag_cfg.get("owner", "data-eng"),
            "tags": dag_cfg.get("tags") or ["gold", "clickhouse"],
            "max_active_tasks": int(dag_cfg.get("max_active_tasks") or 8),
            "default_window_minutes": int(dag_cfg.get("default_window_minutes") or 10),
            "enabled": dag_cfg.get("enabled", True),
            "pipelines": pipelines,
            "base_dir": os.path.dirname(config_path),
        }
        return [dag_cfg]

    def load_configs_from_postgres(self) -> List[Dict[str, Any]]:
        metadata_uri = self._get_metadata_uri()
        if not metadata_uri:
            logging.warning("Metadata database URI not configured")
            return []

        dag_sql = text(
            """
            SELECT
              dag_id,
              schedule_cron,
              timezone,
              owner,
              tags,
              max_active_tasks,
              default_window_minutes,
              enabled
            FROM metadata.gold_dags
            """
        )
        pipeline_sql = text(
            """
            SELECT
              dag_id,
              pipeline_id,
              enabled,
              sql_path,
              window_minutes,
              depends_on,
              target_table,
              params,
              pipeline_order
            FROM metadata.gold_pipelines
            ORDER BY dag_id, pipeline_order, pipeline_id
            """
        )

        engine = create_engine(metadata_uri)
        try:
            with engine.begin() as conn:
                dag_rows = conn.execute(dag_sql).mappings().all()
                pipeline_rows = conn.execute(pipeline_sql).mappings().all()
        except Exception as exc:
            logging.warning("Failed to load gold metadata from Postgres: %s", exc)
            return []
        finally:
            engine.dispose()

        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        dag_map: Dict[str, Dict[str, Any]] = {}

        for row in dag_rows:
            dag_id = row.get("dag_id")
            if not dag_id:
                continue
            tags = _normalize_list(row.get("tags"))
            dag_map[dag_id] = {
                "dag_id": dag_id,
                "schedule_cron": row.get("schedule_cron") or "*/5 * * * *",
                "timezone": row.get("timezone") or "Asia/Jakarta",
                "owner": row.get("owner") or "data-eng",
                "tags": tags or ["gold", "clickhouse"],
                "max_active_tasks": int(row.get("max_active_tasks") or 8),
                "default_window_minutes": int(row.get("default_window_minutes") or 10),
                "enabled": row.get("enabled", True),
                "pipelines": [],
                "base_dir": base_dir,
            }

        for row in pipeline_rows:
            dag_id = row.get("dag_id")
            if not dag_id or dag_id not in dag_map:
                continue
            pipeline_id = row.get("pipeline_id")
            if not pipeline_id:
                continue
            params = row.get("params") or {}
            if not isinstance(params, dict):
                params = {}
            target_table = row.get("target_table")
            if target_table:
                params = dict(params)
                params.setdefault("target_table", target_table)

            pipeline: Dict[str, Any] = {
                "pipeline_id": pipeline_id,
                "enabled": row.get("enabled", True),
                "sql_path": row.get("sql_path"),
                "depends_on": _normalize_list(row.get("depends_on")),
                "params": params,
                "pipeline_order": row.get("pipeline_order", 0),
            }
            window_minutes = row.get("window_minutes")
            if window_minutes is not None:
                pipeline["window_minutes"] = int(window_minutes)
            dag_map[dag_id]["pipelines"].append(pipeline)

        for dag_cfg in dag_map.values():
            dag_cfg["pipelines"].sort(
                key=lambda item: (item.get("pipeline_order", 0), item.get("pipeline_id", ""))
            )

        return list(dag_map.values())

    def load_configs(self) -> List[Dict[str, Any]]:
        source = os.environ.get("GOLD_PIPELINES_SOURCE", "postgres").lower()
        if source == "file":
            return self._load_configs_from_file()

        configs = self.load_configs_from_postgres()
        if configs:
            return configs
        return self._load_configs_from_file()

    def generate_dag(self, dag_cfg: Dict[str, Any]) -> DAG:
        dag_id = dag_cfg["dag_id"]
        schedule_cron = dag_cfg.get("schedule_cron") or "*/5 * * * *"
        timezone = dag_cfg.get("timezone") or "Asia/Jakarta"
        owner = dag_cfg.get("owner") or "data-eng"
        tags = dag_cfg.get("tags") or []
        max_active_tasks = int(dag_cfg.get("max_active_tasks") or 8)
        default_window_minutes = int(dag_cfg.get("default_window_minutes") or 10)
        base_dir = dag_cfg.get("base_dir") or os.path.dirname(__file__)
        pipelines = [
            pipeline
            for pipeline in (dag_cfg.get("pipelines") or [])
            if pipeline.get("enabled", True)
        ]

        default_args = {
            "owner": owner,
            "retries": 1,
        }

        with DAG(
            dag_id=dag_id,
            default_args=default_args,
            start_date=days_ago(1),
            schedule_interval=schedule_cron,
            catchup=False,
            max_active_runs=1,
            max_active_tasks=max_active_tasks,
            tags=tags,
            params={
                "pipeline_id": Param("", type="string"),
                "start_ts": Param("", type="string"),
                "end_ts": Param("", type="string"),
                "window_minutes": Param(default_window_minutes, type="integer"),
            },
            timezone=timezone,
        ) as dag:
            start = EmptyOperator(task_id="start")
            end = EmptyOperator(task_id="end")

            task_map: Dict[str, PythonOperator] = {}
            for pipeline in pipelines:
                pipeline = dict(pipeline)
                pipeline_id = pipeline.get("pipeline_id")
                if not pipeline_id:
                    continue
                sql_path = pipeline.get("sql_path")
                if not sql_path:
                    logging.warning("Missing sql_path for pipeline %s", pipeline_id)
                    continue
                pipeline["sql_path"] = _resolve_sql_path(base_dir, sql_path)
                task_map[pipeline_id] = PythonOperator(
                    task_id=pipeline_id,
                    python_callable=run_pipeline,
                    op_kwargs={
                        "pipeline": pipeline,
                        "default_window_minutes": default_window_minutes,
                    },
                )

            for pipeline in pipelines:
                pipeline_id = pipeline.get("pipeline_id")
                if not pipeline_id:
                    continue
                task = task_map.get(pipeline_id)
                if not task:
                    continue
                depends_on = pipeline.get("depends_on") or []
                if depends_on:
                    for dep in depends_on:
                        upstream = task_map.get(dep)
                        if upstream:
                            upstream >> task
                        else:
                            logging.warning("Missing dependency %s for pipeline %s", dep, pipeline_id)
                else:
                    start >> task
                task >> end

        return dag
