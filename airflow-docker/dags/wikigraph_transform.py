"""Transform: raw.page -> stg -> mart, via dbt, one Airflow task per model."""

from __future__ import annotations

import os

import pendulum
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig

DBT_PROJECT = "/opt/airflow/dbt/wikigraph"

profile_config = ProfileConfig(
    profile_name="wikigraph",
    target_name="dev",
    # Reuse the same profiles.yml dbt uses on the command line — one source of
    # truth, so "works in the shell but not in Airflow" can't happen.
    profiles_yml_filepath=f"{DBT_PROJECT}/profiles.yml"
)

wikigraph_transform = DbtDag(
    dag_id="wikigraph_transform",
    project_config=ProjectConfig(DBT_PROJECT),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=os.environ.get("DBT_EXECUTABLE_PATH", "/home/airflow/.local/bin/dbt")
    ),
    operator_args={
        "install_deps": False,
    },
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["wikigraph", "dbt", "transform"]
)