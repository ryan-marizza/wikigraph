"""
LEVEL 4 — Control flow: branching, task groups, trigger rules, sensors.

Real pipelines aren't straight lines. They skip work that isn't needed,
wait on external systems, and still need to run a cleanup step whether the
main body succeeded or not.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import dag, get_current_context, task, task_group


@dag(
    dag_id="04_branching_and_groups",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "wikigraph"],
    # A DAG-level timeout. If the whole run exceeds this, it's marked failed.
    dagrun_timeout=timedelta(hours=1),
)
def branching_and_groups():

    @task
    def check_dump_size() -> int:
        """Decide how much work today needs."""
        ctx = get_current_context()
        # Fake: weekends get a big dump, weekdays a small one.
        return 5_000_000 if ctx["logical_date"].weekday() >= 5 else 50_000

    # --- Branching ----------------------------------------------------------
    # @task.branch returns the task_id (or list of ids) to RUN. Every other
    # directly-downstream task is marked "skipped" — not failed, skipped.
    @task.branch
    def choose_path(size: int) -> str:
        if size > 1_000_000:
            return "full_rebuild.load_full"
        return "incremental_update"

    # --- Task groups --------------------------------------------------------
    # @task_group is purely organizational: it collapses tasks into one box
    # in the UI and namespaces their task_ids as "<group>.<task>".
    # Groups can nest. They are NOT a scheduling boundary.
    @task_group(group_id="full_rebuild")
    def full_rebuild():
        @task
        def load_full() -> None:
            print("Loading the entire dump")

        @task
        def rebuild_indexes() -> None:
            print("Rebuilding all indexes")

        load_full() >> rebuild_indexes()

    @task
    def incremental_update() -> None:
        print("Applying today's deltas only")

    # --- Trigger rules ------------------------------------------------------
    # Default is "all_success": run only if every upstream task SUCCEEDED.
    # After a branch, one side is always skipped, so the join task would
    # never run with the default. "none_failed_min_one_success" is the
    # standard fix: run if nothing failed and at least one parent succeeded.
    @task(trigger_rule="none_failed_min_one_success")
    def publish_graph() -> None:
        print("Publishing the graph snapshot")

    # "all_done" runs regardless of upstream success/failure/skip — the
    # right rule for cleanup, teardown, and notification tasks.
    @task(trigger_rule="all_done")
    def cleanup_temp_files() -> None:
        print("Removing scratch files (runs even if the pipeline failed)")

    # Other useful rules: all_failed, one_failed, one_success, none_skipped.

    # --- Sensors ------------------------------------------------------------
    # A sensor is a task that waits for a condition. ALWAYS set a timeout, or
    # a stuck sensor will hold a worker slot forever.
    #
    # mode="reschedule" frees the worker slot between checks instead of
    # sleeping on it — use it for anything with poke_interval over ~60s.
    #
    # This one needs a Connection named "fs_default" (Admin -> Connections,
    # type "File (path)"). Delete this task if you'd rather skip that setup.
    wait_for_dump = FileSensor(
        task_id="wait_for_dump",
        filepath="/opt/airflow/dags",   # exists, so it succeeds immediately
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 10,
        mode="reschedule",
        soft_fail=True,   # mark SKIPPED instead of FAILED on timeout
    )

    done = EmptyOperator(task_id="done", trigger_rule="all_done")

    # --- Wiring -------------------------------------------------------------
    size = check_dump_size()
    branch = choose_path(size)

    rebuild = full_rebuild()
    incremental = incremental_update()
    publish = publish_graph()

    wait_for_dump >> size >> branch >> [rebuild, incremental] >> publish
    publish >> cleanup_temp_files() >> done


branching_and_groups()
