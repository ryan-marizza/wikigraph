"""Platform smoke test. Proves connections, mounts, and imports work.

Deliberately contains NO pipeline logic — if this is green and the real DAG
is red, the problem is your code, not your infrastructure.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="wikigraph_smoke",
    description="Verify warehouse connectivity and shard visibility",
    schedule=None,
    start_date=pendulum.datetime(2026,1,1,tz="UTC"),
    catchup=False,
    tags=['wikigraph', 'smoke'],
)
def wikigraph_smoke():

    @task
    def check_warehouse() -> str:
        """Verify warehouse connectivity and raw.page row visibility."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="wikigraph_warehouse")
        version = hook.get_first("SELECT version()")[0]
        rows = hook.get_first("SELECT count(*) FROM raw.page")[0]
        print(f"warehouse: {version}")
        print(f"raw.page rows: {rows}")
        return version

    @task
    def check_shards() -> str:
        """Verify shard visibility."""
        from wikigraph.shards import discover

        shards = discover()
        for s in shards:
            print(f"{s.name:>16}  {s.size_bytes / 1e9:6.1f} GB  {s.path}")
        return len(shards)

    @task
    def check_staging_writable() -> str:
        """Verify the staging mount is writable."""
        from pathlib import Path

        from wikigraph.config import STAGING_DIR

        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        probe = Path(STAGING_DIR) / ".write_probe"
        probe.write_text("ok")
        probe.unlink()
        return f"{STAGING_DIR} is writable"

    check_warehouse()
    check_shards()
    check_staging_writable()

wikigraph_smoke()