"""Orchestrates the rental_analytics dbt project (staging -> intermediate -> marts) on BigQuery.

Demonstrates: scheduling, cross-layer dependencies, retries + retry delay,
failure handling (trigger rules), task groups, callbacks, DAG run parameters,
and environment-specific configuration.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from common.callbacks import on_task_failure, on_task_retry, on_task_success, send_notification
from common.config import get_env_config

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt/profiles"

config = get_env_config()

default_args = {
    "owner": "rishi",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": on_task_failure,
    "on_success_callback": on_task_success,
    "on_retry_callback": on_task_retry,
}


def dbt_command(subcommand: str, select: str | None = None, include_full_refresh: bool = False) -> str:
    """Build a dbt CLI invocation.

    target_override/full_refresh are DAG run params, rendered by Airflow's
    Jinja engine at task-execution time -- the {{ }} blocks below are left
    untouched by Python and resolved per-run, not per-DAG-parse.
    """
    target_expr = "{{ params.target_override or '" + config.dbt_target + "' }}"
    cmd = (
        "dbt " + subcommand
        + " --project-dir " + DBT_PROJECT_DIR
        + " --profiles-dir " + DBT_PROFILES_DIR
        + " --target " + target_expr
    )
    if select:
        cmd += " --select " + select
    if include_full_refresh:
        cmd += " {{ '--full-refresh' if params.full_refresh else '' }}"
    return cmd


def dbt_layer_group(group_id: str, select: str) -> TaskGroup:
    """One TaskGroup per dbt layer: run the layer's models, then test them."""
    with TaskGroup(group_id=group_id) as group:
        run = BashOperator(
            task_id="run",
            bash_command=dbt_command("run", select=select, include_full_refresh=True),
        )
        test = BashOperator(
            task_id="test",
            bash_command=dbt_command("test", select=select),
        )
        run >> test
    return group


def _notify_pipeline_failure(**context) -> None:
    send_notification(
        subject=f"PIPELINE FAILED: {context['dag'].dag_id}",
        message=f"run_id={context['run_id']} -- check task logs for the failing step.",
    )


with DAG(
    dag_id="rental_analytics_dbt_pipeline",
    description="Orchestrates the rental_analytics dbt project on BigQuery (staging -> intermediate -> marts).",
    schedule_interval="0 6 * * *",  # daily at 06:00 UTC
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "full_refresh": Param(
            False,
            type="boolean",
            description="Append --full-refresh to dbt run (rebuilds incremental marts, e.g. fct_listing_day, from scratch).",
        ),
        "target_override": Param(
            "",
            type="string",
            description="Override the dbt --target for this run only (e.g. 'dev'). Leave blank to use the environment default.",
        ),
    },
    tags=["dbt", "rental_analytics", config.env],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_command("deps"),
        retries=1,
        retry_delay=pendulum.duration(minutes=1),
    )

    staging = dbt_layer_group("staging", select="staging")
    intermediate = dbt_layer_group("intermediate", select="intermediate")
    marts = dbt_layer_group("marts", select="marts")

    generate_docs = BashOperator(
        task_id="generate_docs",
        bash_command=dbt_command("docs generate"),
    )

    notify_failure = PythonOperator(
        task_id="notify_failure",
        python_callable=_notify_pipeline_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    dbt_deps >> staging >> intermediate >> marts >> generate_docs
    [dbt_deps, staging, intermediate, marts, generate_docs] >> notify_failure
