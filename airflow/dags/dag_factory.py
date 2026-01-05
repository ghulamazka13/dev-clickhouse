import json
import logging
import os
import re
import subprocess
import sys
import uuid

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

EXPECTED_BRONZE_SCHEMA = {
    "event_id": "text",
    "event_ts": "timestamp with time zone",
    "sensor_type": "text",
    "sensor_name": "text",
    "event_type": "text",
    "severity": "text",
    "src_ip": "inet",
    "dest_ip": "inet",
    "src_port": "integer",
    "dest_port": "integer",
    "protocol": "text",
    "bytes": "bigint",
    "packets": "bigint",
    "uid": "text",
    "conn_state": "text",
    "duration": "double precision",
    "signature": "text",
    "signature_id": "integer",
    "category": "text",
    "alert_action": "text",
    "tags": "jsonb",
    "message": "text",
}

SODA_CONFIG_PATH = os.environ.get(
    "SODA_CONFIG_PATH", "/opt/airflow/include/dq/soda_config.yml"
)
SODA_CHECKS_PATH = os.environ.get(
    "SODA_CHECKS_PATH", "/opt/airflow/include/dq/soda_checks.yml"
)
SODA_DATASOURCE = os.environ.get("SODA_DATASOURCE", "analytics")


def load_sql(path):
    if isinstance(path, str) and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as handle:
            return handle.read()
    return path


def normalize_pipeline(pipeline):
    normalized = dict(pipeline)
    for key in ("gold_tables", "gold_sql_paths"):
        value = normalized.get(key)
        if value is None:
            normalized[key] = []
            continue
        if isinstance(value, str):
            try:
                normalized[key] = json.loads(value)
            except json.JSONDecodeError:
                normalized[key] = []
    return normalized


def _get_hook():
    return PostgresHook(postgres_conn_id="analytics_db")


def start_pipeline_run(pipeline_id, **context):
    run_id = context["run_id"]
    hook = _get_hook()
    hook.run(
        """
        INSERT INTO monitoring.pipeline_runs
            (run_id, pipeline_id, run_ts, status, notes, started_at)
        VALUES (%s, %s, now(), %s, %s, now())
        ON CONFLICT (run_id) DO NOTHING
        """,
        parameters=(run_id, pipeline_id, "running", "started"),
    )


def end_pipeline_run(pipeline_id, **context):
    run_id = context["run_id"]
    dag_run = context["dag_run"]
    tis = dag_run.get_task_instances()
    failed = any(ti.state == State.FAILED for ti in tis)
    status = "failed" if failed else "success"
    hook = _get_hook()
    hook.run(
        """
        UPDATE monitoring.pipeline_runs
        SET status = %s, ended_at = now()
        WHERE run_id = %s
        """,
        parameters=(status, run_id),
    )


def compute_lag(pipeline_id, bronze_table, **_):
    hook = _get_hook()
    row = hook.get_first(f"SELECT max(event_ts) FROM {bronze_table}")
    max_ts = row[0] if row else None
    lag_seconds = None
    if max_ts is not None:
        lag_row = hook.get_first(
            "SELECT EXTRACT(EPOCH FROM (now() - %s::timestamptz))",
            parameters=(max_ts,),
        )
        lag_seconds = lag_row[0] if lag_row else None
    hook.run(
        """
        INSERT INTO monitoring.lag_metrics
            (pipeline_id, observed_at, max_event_ts, lag_seconds)
        VALUES (%s, now(), %s, %s)
        """,
        parameters=(pipeline_id, max_ts, lag_seconds),
    )


def schema_drift_check(pipeline_id, bronze_table, **_):
    schema_name, table_name = bronze_table.split(".")
    hook = _get_hook()
    rows = hook.get_records(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        parameters=(schema_name, table_name),
    )
    actual = {row[0]: row[1] for row in rows}
    for column_name, expected_type in EXPECTED_BRONZE_SCHEMA.items():
        actual_type = actual.get(column_name)
        if actual_type is None:
            status = "missing"
        elif actual_type != expected_type:
            status = "mismatch"
        else:
            status = "ok"
        hook.run(
            """
            INSERT INTO monitoring.schema_drift
                (pipeline_id, observed_at, column_name, expected_type, actual_type, status)
            VALUES (%s, now(), %s, %s, %s, %s)
            """,
            parameters=(pipeline_id, column_name, expected_type, actual_type, status),
        )


def volume_check(pipeline_id, bronze_table, **_):
    hook = _get_hook()
    recent = hook.get_first(
        f"""
        SELECT count(*)
        FROM {bronze_table}
        WHERE event_ts >= now() - interval '5 minutes'
        """
    )[0]
    last_hour = hook.get_first(
        f"""
        SELECT count(*)
        FROM {bronze_table}
        WHERE event_ts >= now() - interval '1 hour'
        """
    )[0]
    baseline = int(last_hour / 12) if last_hour else 0
    status = "ok"
    if baseline > 0 and recent < baseline * 0.5:
        status = "low"
    hook.run(
        """
        INSERT INTO monitoring.volume_metrics
            (pipeline_id, observed_at, window_minutes, event_count, baseline_count, status)
        VALUES (%s, now(), %s, %s, %s, %s)
        """,
        parameters=(pipeline_id, 5, recent, baseline, status),
    )


def run_soda_scan(pipeline_id, **_):
    output_path = f"/tmp/soda_scan_{pipeline_id}.json"
    cmd = [
        sys.executable,
        "-m",
        "soda",
        "scan",
        "-d",
        SODA_DATASOURCE,
        "-c",
        SODA_CONFIG_PATH,
        SODA_CHECKS_PATH,
        "--scan-results-file",
        output_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode not in (0, 1):
        raise AirflowException(f"Soda failed: {result.stderr}")
    if not os.path.exists(output_path):
        raise AirflowException("Soda output file missing")

    with open(output_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    failed = bool(data.get("hasFailures")) or bool(data.get("hasErrors"))
    for check in data.get("checks", []):
        if check.get("outcome") in ("fail", "error"):
            failed = True
            break

    status = "fail" if failed else "pass"
    hook = _get_hook()
    hook.run(
        """
        INSERT INTO gold.dq_results
            (run_id, pipeline_id, run_ts, status, results_json)
        VALUES (%s::uuid, %s, now(), %s, %s::jsonb)
        """,
        parameters=(str(uuid.uuid4()), pipeline_id, status, json.dumps(data)),
    )

    if failed:
        raise AirflowException("Data quality checks failed")


def alerting(pipeline_id, freshness_threshold_minutes, **context):
    threshold_seconds = int(freshness_threshold_minutes or 10) * 60
    hook = _get_hook()
    issues = []

    lag_row = hook.get_first(
        """
        SELECT lag_seconds
        FROM monitoring.lag_metrics
        WHERE pipeline_id = %s
        ORDER BY observed_at DESC
        LIMIT 1
        """,
        parameters=(pipeline_id,),
    )
    if lag_row and lag_row[0] is not None and lag_row[0] > threshold_seconds:
        issues.append(("lag", "warning", f"lag_seconds={lag_row[0]}"))

    vol_row = hook.get_first(
        """
        SELECT status, event_count, baseline_count
        FROM monitoring.volume_metrics
        WHERE pipeline_id = %s
        ORDER BY observed_at DESC
        LIMIT 1
        """,
        parameters=(pipeline_id,),
    )
    if vol_row and vol_row[0] == "low":
        issues.append(
            (
                "volume",
                "warning",
                f"event_count={vol_row[1]} baseline_count={vol_row[2]}",
            )
        )

    drift_row = hook.get_first(
        """
        SELECT count(*)
        FROM monitoring.schema_drift
        WHERE pipeline_id = %s
          AND observed_at >= now() - interval '15 minutes'
          AND status != 'ok'
        """,
        parameters=(pipeline_id,),
    )
    if drift_row and drift_row[0] > 0:
        issues.append(("schema_drift", "critical", "schema drift detected"))

    dq_row = hook.get_first(
        """
        SELECT status
        FROM gold.dq_results
        WHERE pipeline_id = %s
        ORDER BY run_ts DESC
        LIMIT 1
        """,
        parameters=(pipeline_id,),
    )
    if dq_row and dq_row[0] == "fail":
        issues.append(("dq", "critical", "dq checks failed"))

    if not issues:
        logging.info("No alerts for pipeline %s", pipeline_id)
        return

    for alert_type, severity, message in issues:
        hook.run(
            """
            INSERT INTO monitoring.alerts
                (pipeline_id, alert_type, severity, message)
            VALUES (%s, %s, %s, %s)
            """,
            parameters=(pipeline_id, alert_type, severity, message),
        )

    webhook = os.environ.get("ALERT_WEBHOOK_URL")
    if webhook:
        payload = {
            "pipeline_id": pipeline_id,
            "issues": [
                {"type": t, "severity": s, "message": m} for t, s, m in issues
            ],
        }
        try:
            import urllib.request

            req = urllib.request.Request(
                webhook,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception as exc:
            logging.warning("Alert webhook failed: %s", exc)


def create_snapshot_id():
    return str(uuid.uuid4())


def build_pipeline_taskgroup(pipeline, dag):
    pipeline = normalize_pipeline(pipeline)
    pipeline_id = pipeline["pipeline_id"]
    bronze_table = pipeline["bronze_table"]
    silver_sql_path = pipeline["silver_sql_path"]
    silver_sql = load_sql(silver_sql_path)
    gold_sql_paths = pipeline.get("gold_sql_paths", [])
    gold_tables = pipeline.get("gold_tables", [])
    snapshot_sql_path = pipeline.get(
        "snapshot_sql_path", "/opt/airflow/include/sql/security_events/snapshot_gold.sql"
    )
    snapshot_sql = load_sql(snapshot_sql_path)

    optimize_sql = f"ANALYZE {pipeline['silver_table']};"
    for table in gold_tables:
        optimize_sql += f"\nANALYZE {table};"

    with TaskGroup(group_id=f"{pipeline_id}_pipeline", dag=dag) as taskgroup:
        start_run = PythonOperator(
            task_id="start_run",
            python_callable=start_pipeline_run,
            op_kwargs={"pipeline_id": pipeline_id},
        )

        compute_lag_task = PythonOperator(
            task_id="compute_lag",
            python_callable=compute_lag,
            op_kwargs={"pipeline_id": pipeline_id, "bronze_table": bronze_table},
        )

        schema_drift_task = PythonOperator(
            task_id="schema_drift_check",
            python_callable=schema_drift_check,
            op_kwargs={"pipeline_id": pipeline_id, "bronze_table": bronze_table},
        )

        volume_task = PythonOperator(
            task_id="volume_check",
            python_callable=volume_check,
            op_kwargs={"pipeline_id": pipeline_id, "bronze_table": bronze_table},
        )

        build_silver = PostgresOperator(
            task_id="build_silver",
            postgres_conn_id="analytics_db",
            sql=silver_sql,
        )

        gold_tasks = []
        for path in gold_sql_paths:
            sql = load_sql(path)
            base = os.path.basename(path).replace(".sql", "")
            gold_tasks.append(
                PostgresOperator(
                    task_id=f"build_{base}",
                    postgres_conn_id="analytics_db",
                    sql=sql,
                )
            )

        dq_scan = PythonOperator(
            task_id="run_dq",
            python_callable=run_soda_scan,
            op_kwargs={"pipeline_id": pipeline_id},
        )

        optimize = PostgresOperator(
            task_id="optimize",
            postgres_conn_id="analytics_db",
            sql=optimize_sql,
        )

        snapshot_id = PythonOperator(
            task_id="snapshot_id",
            python_callable=create_snapshot_id,
        )

        snapshot = PostgresOperator(
            task_id="snapshot_gold",
            postgres_conn_id="analytics_db",
            sql=snapshot_sql,
            params={
                "pipeline_id": pipeline_id,
                "snapshot_task_id": f"{taskgroup.group_id}.snapshot_id",
            },
        )

        alert_task = PythonOperator(
            task_id="alerting",
            python_callable=alerting,
            op_kwargs={
                "pipeline_id": pipeline_id,
                "freshness_threshold_minutes": pipeline.get(
                    "freshness_threshold_minutes", 10
                ),
            },
            trigger_rule=TriggerRule.ALL_DONE,
        )

        end_run = PythonOperator(
            task_id="end_run",
            python_callable=end_pipeline_run,
            op_kwargs={"pipeline_id": pipeline_id},
            trigger_rule=TriggerRule.ALL_DONE,
        )

        start_run >> [compute_lag_task, schema_drift_task, volume_task] >> build_silver

        if gold_tasks:
            build_silver >> gold_tasks[0]
            for upstream, downstream in zip(gold_tasks, gold_tasks[1:]):
                upstream >> downstream
            gold_tasks[-1] >> dq_scan
        else:
            build_silver >> dq_scan

        dq_scan >> optimize >> snapshot_id >> snapshot
        snapshot >> alert_task >> end_run

    return taskgroup


QUALIFIED_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ensure_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    if isinstance(value, set):
        return list(value)
    return []


def _require_qualified_name(value, field_name):
    if not isinstance(value, str) or not QUALIFIED_NAME_RE.match(value):
        raise AirflowException(f"Invalid {field_name}: {value}")
    return value


def _require_identifier(value, field_name):
    if not isinstance(value, str) or not IDENTIFIER_RE.match(value):
        raise AirflowException(f"Invalid {field_name}: {value}")
    return value


def _resolve_sql_path(path):
    if not path:
        raise AirflowException("sql_merge_path is required")
    if path.startswith("/"):
        return path
    if path.startswith("airflow/"):
        return os.path.join("/opt/airflow", path[len("airflow/") :])
    return os.path.join("/opt/airflow", path)


def _render_merge_sql(template_path, replacements):
    path = _resolve_sql_path(template_path)
    if not os.path.exists(path):
        raise AirflowException(f"Merge SQL template not found: {path}")
    with open(path, "r", encoding="utf-8") as handle:
        sql_template = handle.read()
    for key, value in replacements.items():
        sql_template = sql_template.replace(f"{{{{{key}}}}}", value)
    return sql_template


def freshness_check(
    pipeline_id,
    datasource_table,
    datasource_timestamp_column,
    freshness_threshold_minutes,
    **_,
):
    datasource_table = _require_qualified_name(datasource_table, "datasource_table")
    ts_col = _require_identifier(
        datasource_timestamp_column, "datasource_timestamp_column"
    )
    hook = _get_hook()
    row = hook.get_first(
        f"SELECT EXTRACT(EPOCH FROM (now() - max({ts_col}))) AS lag_seconds FROM {datasource_table}"
    )
    lag_seconds = row[0] if row else None
    if lag_seconds is None:
        raise AirflowException(
            f"Freshness check failed for {pipeline_id}: no data in {datasource_table}"
        )
    threshold_seconds = int(freshness_threshold_minutes or 0) * 60
    if lag_seconds > threshold_seconds:
        raise AirflowException(
            f"Freshness check failed for {pipeline_id}: lag {lag_seconds}s exceeds {threshold_seconds}s"
        )
    logging.info(
        "Freshness check passed for %s: lag_seconds=%s", pipeline_id, lag_seconds
    )


def schema_check(pipeline_id, datasource_table, expected_columns, **_):
    datasource_table = _require_qualified_name(datasource_table, "datasource_table")
    expected = _ensure_list(expected_columns)
    if not expected:
        logging.info("Schema check skipped for %s: no expected columns", pipeline_id)
        return
    for column_name in expected:
        _require_identifier(column_name, "expected_columns")
    schema_name, table_name = datasource_table.split(".")
    hook = _get_hook()
    rows = hook.get_records(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        parameters=(schema_name, table_name),
    )
    actual = {row[0] for row in rows}
    missing = sorted(set(expected) - actual)
    if missing:
        raise AirflowException(
            f"Schema check failed for {pipeline_id}: missing columns {missing}"
        )
    logging.info("Schema check passed for %s", pipeline_id)


def merge_to_datawarehouse(
    pipeline_id,
    datasource_table,
    datasource_timestamp_column,
    datawarehouse_table,
    unique_key,
    merge_window_minutes,
    expected_columns,
    sql_merge_path,
    **_,
):
    datasource_table = _require_qualified_name(datasource_table, "datasource_table")
    datawarehouse_table = _require_qualified_name(
        datawarehouse_table, "datawarehouse_table"
    )
    ts_col = _require_identifier(
        datasource_timestamp_column, "datasource_timestamp_column"
    )
    unique_key = _require_identifier(unique_key, "unique_key")
    columns = [_require_identifier(col, "expected_columns") for col in _ensure_list(expected_columns)]
    if not columns:
        raise AirflowException(
            f"Merge failed for {pipeline_id}: expected_columns is required"
        )
    window_minutes = int(merge_window_minutes or 0)
    column_list = ", ".join(columns)
    update_set = ",\n  ".join(f"{col} = EXCLUDED.{col}" for col in columns)
    index_name = f"ux_{datawarehouse_table.replace('.', '_')}_{unique_key}"
    merge_sql = _render_merge_sql(
        sql_merge_path,
        {
            "DATASOURCE_TABLE": datasource_table,
            "DATAWAREHOUSE_TABLE": datawarehouse_table,
            "TS_COL": ts_col,
            "UNIQUE_KEY": unique_key,
            "WINDOW_MINUTES": str(window_minutes),
            "COLUMN_LIST": column_list,
            "UPDATE_SET": update_set,
            "UNIQUE_INDEX_NAME": index_name,
        },
    )
    hook = _get_hook()
    hook.run(merge_sql)
    logging.info("Merge completed for %s into %s", pipeline_id, datawarehouse_table)


def build_datasource_to_dwh_taskgroup(pipeline, dag):
    pipeline_id = pipeline["pipeline_id"]
    datasource_table = pipeline["datasource_table"]
    datasource_timestamp_column = pipeline["datasource_timestamp_column"]
    datawarehouse_table = pipeline["datawarehouse_table"]
    unique_key = pipeline["unique_key"]
    merge_window_minutes = pipeline.get("merge_window_minutes", 10)
    expected_columns = _ensure_list(pipeline.get("expected_columns"))
    sql_merge_path = pipeline["sql_merge_path"]
    freshness_threshold_minutes = pipeline.get("freshness_threshold_minutes", 2)

    with TaskGroup(group_id=f"{pipeline_id}_pipeline", dag=dag) as taskgroup:
        freshness_task = PythonOperator(
            task_id="freshness_check",
            python_callable=freshness_check,
            op_kwargs={
                "pipeline_id": pipeline_id,
                "datasource_table": datasource_table,
                "datasource_timestamp_column": datasource_timestamp_column,
                "freshness_threshold_minutes": freshness_threshold_minutes,
            },
        )

        schema_task = PythonOperator(
            task_id="schema_check",
            python_callable=schema_check,
            op_kwargs={
                "pipeline_id": pipeline_id,
                "datasource_table": datasource_table,
                "expected_columns": expected_columns,
            },
        )

        merge_task = PythonOperator(
            task_id="merge_to_datawarehouse",
            python_callable=merge_to_datawarehouse,
            op_kwargs={
                "pipeline_id": pipeline_id,
                "datasource_table": datasource_table,
                "datasource_timestamp_column": datasource_timestamp_column,
                "datawarehouse_table": datawarehouse_table,
                "unique_key": unique_key,
                "merge_window_minutes": merge_window_minutes,
                "expected_columns": expected_columns,
                "sql_merge_path": sql_merge_path,
            },
        )

        analyze_task = PostgresOperator(
            task_id="analyze_target",
            postgres_conn_id="analytics_db",
            sql=f"ANALYZE {datawarehouse_table};",
        )

        freshness_task >> schema_task >> merge_task >> analyze_task

    return taskgroup


def build_datasource_to_dwh_dag(dag_cfg):
    dag_name = dag_cfg["dag_name"]
    schedule_cron = dag_cfg.get("schedule_cron") or "*/5 * * * *"
    timezone = dag_cfg.get("timezone") or "Asia/Jakarta"
    owner = dag_cfg.get("owner") or "data-eng"
    tags = _ensure_list(dag_cfg.get("tags")) or []
    max_active_tasks = int(dag_cfg.get("max_active_tasks") or 8)
    pipelines = dag_cfg.get("pipelines") or []

    default_args = {
        "owner": owner,
        "retries": 1,
    }

    with DAG(
        dag_id=dag_name,
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval=schedule_cron,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=max_active_tasks,
        tags=tags,
    ) as dag:
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end")

        for pipeline in pipelines:
            if not pipeline.get("enabled", True):
                continue
            taskgroup = build_datasource_to_dwh_taskgroup(pipeline, dag)
            start >> taskgroup >> end

    return dag
