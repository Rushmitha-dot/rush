"""Daily product finder run. Airflow lives in its own venv (.venv-airflow) and
calls the pipeline's venv through the runner CLI, so Airflow's pinned
dependencies never have to agree with dbt's.

One run per review date. max_active_runs=1 because DuckDB allows a single
writer, and catchup replays the whole 2023 window in order.
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT = Path(__file__).resolve().parents[1]
PY = f"cd {PROJECT} && PYTHONPATH=src .venv/bin/python -m finder.runner"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="product_finder_daily",
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 9, 9),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["115", "reviews"],
) as dag:
    # exit 99 = no drop that day (some 2023 days have no reviews), which Airflow marks skipped
    load = BashOperator(
        task_id="load_check_score",
        bash_command=f"[ -d {PROJECT}/data/drops/dt={{{{ ds }}}} ] || exit 99; "
                     f"{PY} load {{{{ ds }}}} --trigger airflow",
        skip_on_exit_code=99,
    )
    build = BashOperator(
        task_id="dbt_build_and_publish",
        bash_command=f"{PY} transform --batch-date {{{{ ds }}}} --trigger airflow",
    )
    fresh = BashOperator(
        task_id="freshness",
        bash_command=f"{PY} freshness --expect {{{{ ds }}}}",
    )
    load >> build >> fresh
