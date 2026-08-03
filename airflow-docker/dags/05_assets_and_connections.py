"""
LEVEL 5 — Assets (data-aware scheduling), Variables, Connections, and Hooks.

Two DAGs live in this file. That's allowed: Airflow parses the module and
registers every DAG object it finds.

The big idea: instead of guessing "the upstream DAG probably finishes by
07:00, so I'll schedule at 07:30", you declare that DAG A PRODUCES an Asset
and DAG B CONSUMES it. Airflow triggers B the moment A updates the asset.
No cron guessing, no cross-DAG sensors.

(Assets were called "Datasets" in Airflow 2.x — same concept, renamed in 3.0.)
"""

from datetime import datetime

from airflow.sdk import Asset, Variable, dag, task

# --- Declare the assets -------------------------------------------------
# The string is just a unique identifier — a URI by convention. Airflow does
# not read or validate the underlying data; it only tracks the "updated"
# signal. Define these once and import them wherever they're needed.
PAGE_EDGES = Asset("postgres://wikigraph/page_edges")
PAGERANK_SCORES = Asset("postgres://wikigraph/pagerank_scores")


# =========================================================================
# PRODUCER — runs on a clock, publishes an asset
# =========================================================================
@dag(
    dag_id="05a_produce_edges",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "wikigraph", "assets"],
)
def produce_edges():

    @task
    def read_config() -> dict:
        """Variables are key/value config stored in the metadata DB.

        Set them in the UI: Admin -> Variables. Any variable whose name
        contains 'secret', 'password', 'token', or 'key' is masked in logs.

        Fetch them INSIDE a task, never at module level — module-level
        Variable.get() hits the database on every DAG parse (every ~30s,
        for every DAG file). That's a classic way to melt your scheduler.
        """
        return {
            "batch_size": int(Variable.get("wikigraph_batch_size", default=1000)),
            "namespace": Variable.get("wikigraph_namespace", default="0"),
        }

    @task
    def query_source(config: dict) -> int:
        """Using a Hook to talk to an external system via a Connection.

        Connections live in Admin -> Connections and hold host/port/login/
        password. Referencing one by conn_id keeps credentials out of code
        and out of git.

        This uses the Airflow metadata DB purely as a demo target. In your
        project you'd point it at a separate wikigraph database.
        Requires apache-airflow-providers-postgres (bundled in the image).
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="postgres_default")
        rows = hook.get_first("SELECT count(*) FROM dag;")[0]
        print(f"Found {rows} rows (batch_size={config['batch_size']})")
        return rows

    # --- The `outlets` argument is the whole point ---------------------------
    # When this task succeeds, Airflow records that PAGE_EDGES was updated
    # and immediately schedules anything waiting on it.
    @task(outlets=[PAGE_EDGES])
    def write_edges(row_count: int) -> None:
        print(f"Wrote {row_count} edges — PAGE_EDGES is now updated")

    write_edges(query_source(read_config()))


# =========================================================================
# CONSUMER — has no clock at all; runs when its input asset updates
# =========================================================================
@dag(
    dag_id="05b_compute_pagerank",
    start_date=datetime(2026, 1, 1),
    schedule=[PAGE_EDGES],      # <-- a list of Assets, not a cron string
    catchup=False,
    tags=["tutorial", "wikigraph", "assets"],
)
def compute_pagerank():

    @task(outlets=[PAGERANK_SCORES])
    def run_pagerank() -> None:
        # This DAG is itself a producer, so you can chain assets into
        # arbitrarily deep graphs. The "Asset" view in the UI draws the
        # whole dependency graph across DAGs for you.
        print("Computing PageRank over the fresh edge list")

    run_pagerank()


# To wait on SEVERAL assets, pass them all — the DAG fires only once every
# one has been updated since the last run:
#     schedule=[PAGE_EDGES, PAGERANK_SCORES]
#
# For OR logic and other combinations, Airflow 3 supports expressions:
#     from airflow.sdk import AssetAny, AssetAll
#     schedule=AssetAny(PAGE_EDGES, PAGERANK_SCORES)
#
# And you can combine a clock with assets using AssetOrTimeSchedule if you
# want "when data lands, or at 6am, whichever comes first".

produce_edges()
compute_pagerank()
