"""
LEVEL 1 — The anatomy of a DAG.

Every DAG needs three things:
  1. A schedule (when to run)
  2. Some tasks (what to do)
  3. Dependencies between them (what order)

This one fakes a tiny Wikipedia-link pipeline so the shape is concrete.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

# `default_args` are applied to EVERY task in the DAG unless the task
# overrides them. Useful for retry policy, owner, alerting.
default_args = {
    "owner": "ryan",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="01_basics",
    description="TaskFlow fundamentals: linear chains and fan-out/fan-in",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",          # cron ("0 6 * * *"), preset, timedelta, or None
    catchup=False,              # don't backfill every day since start_date
    max_active_runs=1,          # only one run of this DAG at a time
    default_args=default_args,
    tags=["tutorial", "wikigraph"],
)
def basics():
    # --- A task is just a function with @task on it. -----------------------
    # Its return value is pushed to XCom (Airflow's small inter-task
    # message store) automatically. Keep XComs small — IDs, counts, paths.
    # Never pass a DataFrame through XCom; write it to disk/S3 and pass the path.

    @task
    def fetch_seed_pages() -> list[str]:
        """Pretend we hit the Wikipedia API for a starting set of articles."""
        return ["Graph_theory", "Directed_graph", "Adjacency_matrix"]

    @task
    def count_pages(pages: list[str]) -> int:
        """Downstream tasks receive upstream return values as arguments."""
        return len(pages)

    @task
    def summarize(total: int) -> None:
        # print() and the logging module both land in the task log,
        # which you can read in the UI under the task instance.
        print(f"Seeded {total} pages")

    # --- Wiring it up ------------------------------------------------------
    # Calling a @task function does NOT run it. It creates a task instance
    # and records the dependency. This is the whole trick of TaskFlow.
    pages = fetch_seed_pages()
    total = count_pages(pages)
    summarize(total)

    # --- Fan-out / fan-in --------------------------------------------------
    # Two tasks that both depend on `pages` will run in PARALLEL,
    # then a third waits for both.

    @task
    def score_by_length(pages: list[str]) -> dict[str, int]:
        return {p: len(p) for p in pages}

    @task
    def score_by_underscores(pages: list[str]) -> dict[str, int]:
        return {p: p.count("_") for p in pages}

    @task
    def merge_scores(a: dict[str, int], b: dict[str, int]) -> None:
        for page in a:
            print(f"{page}: length={a[page]} underscores={b[page]}")

    merge_scores(score_by_length(pages), score_by_underscores(pages))


# The DAG only registers if you CALL the decorated function at module level.
# Forgetting this line is the #1 reason a DAG doesn't show up in the UI.
basics()
