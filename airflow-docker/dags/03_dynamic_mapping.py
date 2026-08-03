"""
LEVEL 3 — Dynamic task mapping.

The problem: you don't know at parse time how many things you need to
process. Maybe today's crawl returns 40 pages, tomorrow 4,000.

The answer is `.expand()`. Airflow creates one task instance per input
element at RUN time, runs them in parallel (subject to your pool/concurrency
limits), and shows them as a collapsible group in the UI.

This is the feature that replaces the old "generate DAGs in a for loop" hack.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="03_dynamic_mapping",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "wikigraph"],
)
def dynamic_mapping():

    @task
    def list_categories() -> list[str]:
        """Returns a variable-length list — the thing we'll map over."""
        return ["Mathematics", "Computer_science", "Physics", "Linguistics"]

    # --- Simple 1:1 mapping -------------------------------------------------
    # `.expand()` takes the SAME keyword args as the function, but each value
    # must be a list. One task instance is created per element.
    @task
    def fetch_category(category: str) -> dict:
        # Simulated work. In reality: an API call, a SQL query, a file read.
        page_count = len(category) * 7
        return {"category": category, "pages": page_count}

    fetched = fetch_category.expand(category=list_categories())

    # --- Mapping with fixed arguments too -----------------------------------
    # `.partial()` pins the args that DON'T vary; `.expand()` supplies the
    # ones that do. Note the per-task retry override — mapped tasks that hit
    # flaky APIs are exactly where you want this.
    @task(retries=3, retry_delay=timedelta(seconds=30), max_active_tis_per_dag=2)
    def extract_links(record: dict, max_depth: int) -> int:
        print(f"Extracting from {record['category']} to depth {max_depth}")
        return record["pages"]

    link_counts = extract_links.partial(max_depth=2).expand(record=fetched)

    # --- Reducing the results ------------------------------------------------
    # A normal (unmapped) task that takes the mapped output receives a LIST
    # of every mapped return value. This is the fan-in step.
    @task
    def total_links(counts: list[int]) -> int:
        total = sum(counts)
        print(f"{len(counts)} categories, {total} links total")
        return total

    total = total_links(link_counts)

    # --- Mapping over multiple arguments (cross product) ---------------------
    # Expanding two kwargs produces every COMBINATION: 2 x 2 = 4 instances.
    # Use expand_kwargs() instead if you want zipped pairs, not a cross product.
    @task
    def build_index(namespace: str, algorithm: str) -> None:
        print(f"Indexing namespace={namespace} with {algorithm}")

    indexed = build_index.expand(
        namespace=["0", "14"],
        algorithm=["pagerank", "betweenness"],
    )

    # --- expand_kwargs: one instance per dict --------------------------------
    # When each mapped run needs a different SET of arguments, build the
    # dicts yourself and map over them.
    @task
    def make_jobs(total: int) -> list[dict]:
        return [
            {"shard": i, "size": total // 4}
            for i in range(4)
        ]

    @task
    def write_shard(shard: int, size: int) -> None:
        print(f"Writing shard {shard} ({size} edges)")

    written = write_shard.expand_kwargs(make_jobs(total))

    total >> indexed
    written


dynamic_mapping()
