import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

from dag_factory import build_pipeline_taskgroup, normalize_pipeline

DAG_ID = "main_controller_dag"

DEFAULT_PIPELINES = [
    {
        "pipeline_id": "security_events",
        "enabled": True,
        "schedule_cron": "*/5 * * * *",
        "bronze_table": "bronze.security_events_raw",
        "silver_table": "silver.security_events",
        "gold_tables": [
            "gold.alerts_5m",
            "gold.alerts_1h",
            "gold.alerts_daily",
            "gold.top_talkers_daily",
            "gold.top_signatures_daily",
            "gold.protocol_mix_daily",
        ],
        "silver_sql_path": "/opt/airflow/include/sql/security_events/silver.sql",
        "gold_sql_paths": [
            "/opt/airflow/include/sql/security_events/gold_alerts_5m.sql",
            "/opt/airflow/include/sql/security_events/gold_alerts_1h.sql",
            "/opt/airflow/include/sql/security_events/gold_alerts_daily.sql",
            "/opt/airflow/include/sql/security_events/gold_top_talkers_daily.sql",
            "/opt/airflow/include/sql/security_events/gold_top_signatures_daily.sql",
            "/opt/airflow/include/sql/security_events/gold_protocol_mix_daily.sql",
        ],
        "dq_profile": "security_events",
        "sla_minutes": 10,
        "freshness_threshold_minutes": 10,
        "owner": "data-eng",
    }
]


def load_pipelines():
    try:
        hook = PostgresHook(postgres_conn_id="analytics_db")
        rows = hook.get_records(
            """
            SELECT pipeline_id, enabled, schedule_cron, bronze_table, silver_table,
                   gold_tables, silver_sql_path, gold_sql_paths, dq_profile,
                   sla_minutes, freshness_threshold_minutes, owner
            FROM control.pipeline_definitions
            WHERE enabled = true
            """
        )
    except Exception as exc:
        logging.warning("Using default pipeline definitions: %s", exc)
        return []

    columns = [
        "pipeline_id",
        "enabled",
        "schedule_cron",
        "bronze_table",
        "silver_table",
        "gold_tables",
        "silver_sql_path",
        "gold_sql_paths",
        "dq_profile",
        "sla_minutes",
        "freshness_threshold_minutes",
        "owner",
    ]

    pipelines = []
    for row in rows:
        pipeline = dict(zip(columns, row))
        pipelines.append(normalize_pipeline(pipeline))
    return pipelines


default_args = {
    "owner": "data-eng",
    "retries": 1,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["controller", "metadata"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    pipelines = load_pipelines() or DEFAULT_PIPELINES
    for pipeline in pipelines:
        taskgroup = build_pipeline_taskgroup(pipeline, dag)
        start >> taskgroup >> end
