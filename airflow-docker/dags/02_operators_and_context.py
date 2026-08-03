"""
LEVEL 2 — Operators, Jinja templating, and run context.

TaskFlow (@task) is sugar over the classic Operator classes. You'll need the
classic style for anything that isn't plain Python — shell commands, SQL,
API calls via a provider, etc. The two styles mix freely in one DAG.

Note the import paths: in Airflow 3, BashOperator/PythonOperator/EmptyOperator
live in the `standard` provider package, not `airflow.operators.*`.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag, get_current_context, task


def _classic_python_callable(page_title: str, **context) -> str:
    """A plain function used by the classic PythonOperator.

    `context` holds everything about this run: the logical date, the DAG run,
    the task instance, params, and more.
    """
    logical_date = context["logical_date"]
    run_id = context["dag_run"].run_id
    print(f"Processing {page_title} for {logical_date:%Y-%m-%d} (run {run_id})")
    return f"{page_title}:{logical_date:%Y%m%d}"


@dag(
    dag_id="02_operators_and_context",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",       # 06:00 every day, cron syntax
    catchup=False,
    tags=["tutorial", "wikigraph"],
    # `params` gives the DAG runtime inputs you can override when you
    # trigger it manually from the UI ("Trigger DAG w/ config").
    params={"depth": 2, "namespace": "0"},
)
def operators_and_context():

    start = EmptyOperator(task_id="start")

    # --- Bash, with Jinja templating ---------------------------------------
    # Anything in {{ }} is rendered at RUN time, not parse time.
    # {{ ds }} = logical date as YYYY-MM-DD. Check the rendered value in the
    # UI under the task instance -> "Rendered Templates" tab.
    make_partition_dir = BashOperator(
        task_id="make_partition_dir",
        bash_command=(
            "mkdir -p /opt/airflow/logs/wikigraph/{{ ds }} && "
            "echo 'partition ready for {{ ds }} at depth {{ params.depth }}'"
        ),
    )

    # --- Classic PythonOperator --------------------------------------------
    # `op_kwargs` passes static arguments to the callable.
    classic = PythonOperator(
        task_id="classic_python",
        python_callable=_classic_python_callable,
        op_kwargs={"page_title": "Graph_theory"},
    )

    # --- TaskFlow task reading the same context ----------------------------
    @task
    def taskflow_with_context() -> str:
        # get_current_context() is how you reach the context without
        # declaring **context in the signature.
        ctx = get_current_context()
        depth = ctx["params"]["depth"]
        print(f"Crawling to depth {depth} on {ctx['ds']}")
        return ctx["ds"]

    # --- Templated arguments in a TaskFlow task ----------------------------
    # You can pass templated strings into @task functions too.
    @task
    def report(partition: str, run_label: str) -> None:
        print(f"partition={partition} label={run_label}")

    end = EmptyOperator(task_id="end")

    # --- Dependencies with >> -----------------------------------------------
    # Use this when a task produces no value you need (BashOperator, sensors,
    # EmptyOperator). A list on either side means parallel.
    partition = taskflow_with_context()

    start >> [make_partition_dir, classic] >> partition
    partition >> report(partition, "{{ dag_run.run_id }}") >> end


operators_and_context()
