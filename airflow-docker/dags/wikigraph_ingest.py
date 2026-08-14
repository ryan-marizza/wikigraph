"""Ingest: MediaWiki XML shards -> Parquet -> raw.page.

Orchestration only. All logic lives in the wikigraph package under src/.
"""

from __future__ import annotations

import datetime as dt
import os

import pendulum
from airflow.sdk import Param, dag, task

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
}

def _warehouse_dsn() -> str:
    """Return the warehouse DSN from the environment."""
    return os.environ["AIRFLOW_CONN_WIKIGRAPH_WAREHOUSE"].replace("postgres://", "postgresql://")

@dag(
    dag_id="wikigraph_ingest",
    description="Parse MediWiki XML shards to Parquet and COPY into raw.page",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=['wikigraph', 'ingest'],
    params={
        "shards": Param(
            [], type="array",
            description="Shard names to process, e.g. ['p10p1400054']. Empty = all.",
        ),
        "dump_date": Param("2026-07-01", type="string", format="date")
    },
)
def wikigraph_ingest():

    @task
    def discover_shards(**context) -> list[dict]:
        from wikigraph.shards import discover

        wanted = set(context['params']['shards'] or [])
        shards = [s.as_dict() for s in discover() if not wanted or s.name in wanted]
        if not shards:
            raise ValueError(
                f"No shards matched {wanted or '(all)'} - check WIKIGRAPH_RAW_DIR"
            )
        print(f"selected {len(shards)} shard(s): {[s['name'] for s in shards]}")
        return shards

    @task(
        pool="shard_parse",
        execution_timeout=pendulum.duration(hours=2),
        map_index_template="{{ task.op_kwargs['shard']['name'] }}",
    )
    def parse_shard(shard: dict) -> dict:
        from pathlib import Path

        from wikigraph.config import STAGING_DIR
        from wikigraph.load import upsert_manifest
        from wikigraph.parse import parse_shard_to_parquet

        dsn = _warehouse_dsn()
        dump_date = dt.date.fromisoformat(shard['dump_date'])

        upsert_manifest(
            dsn, shard["name"], dump_date,
            status="parsing",
            file_bytes=shard["size_bytes"],
            parse_started=pendulum.now("UTC"),
        )

        out = Path(STAGING_DIR) / shard["dump_date"] / f"{shard['name']}.parquet"
        stats = parse_shard_to_parquet(shard["path"], shard["name"], dump_date, out)


        # --- GATE 1: the grain assertion from the design doc. ---
        # If any page has >1 <revision>, one-row-per-page is wrong and every
        # downstream count is silently off. Fail here, loudly.
        if stats["multi_revision_pages"] > 0:
            upsert_manifest(dsn, shard["name"], dump_date, status="failed",
                            error_detail="multiple <revision> blocks per page")
            raise ValueError(
                f"{shard['name']}: {stats['multi_revision_pages']} pages have "
                "multiple <revision> blocks. The one-row-per-page assumption is invalid"
            )

        # --- GATE 2: a silent zero is worse than a crash. ---
        if stats["pages_written"] == 0:
            upsert_manifest(dsn, shard["name"], dump_date, status="failed",
                            error_detail="zero ns=0 pages written")
            raise ValueError(
                f"{shard['name']}: no ns=0 pages were written to Parquet. "
                "Check the XML and the parser."
            )

        upsert_manifest(dsn, shard["name"], dump_date, 
                        status="parsed",
                        pages_seen=stats["pages_seen"],
                        pages_loaded=stats["pages_written"],
                        parse_ended=pendulum.now("UTC"),
        )

        # Only a small dict passes the task boundary - this goes through XCom.

        return stats

    @task(
        pool="warehouse_load",
        execution_timeout=pendulum.duration(hours=2),
        map_index_template="{{ task.op_kwargs['stats']['shard_name'] }}",
    )
    def load_shard(stats: dict) -> dict:
        from wikigraph.load import load_parquet, upsert_manifest

        dsn = _warehouse_dsn()
        dump_date = dt.date.fromisoformat(stats["dump_date"])
        result = load_parquet(dsn, stats["parquet_path"], stats["shard_name"])

        # --- GATE 3: reconciliation across stages. ---
        # Not "did it throw" but "does the count match what we parsed".
        if result["rows_loaded"] != stats["pages_written"]:
            upsert_manifest(dsn, stats["shard_name"], dump_date, status="failed",
                            error_detail="row count mismatch parse vs. load")
            raise ValueError(
                f"{stats['shard_name']}: {result['rows_loaded']} rows loaded "
                f"but {stats['pages_written']} pages were written to Parquet. "
                "Check the warehouse and the COPY."
            )

        upsert_manifest(dsn, stats["shard_name"], dump_date,
                        status="loaded", load_ended=pendulum.now("UTC"))

        return result

    @task
    def summarize(results: list[dict]) -> None:
        total = sum(r["rows_loaded"] for r in results)
        print(f"loaded {total:,} rows across {len(results)} shard(s)")
        for r in sorted(results, key=lambda x: x["shard_name"]):
            print(f"  {r['shard_name']:>16}  {r['rows_loaded']:>10,} rows")

    shards = discover_shards()
    parsed = parse_shard.expand(shard=shards)
    loaded = load_shard.expand(stats=parsed)
    summarize(loaded)

wikigraph_ingest()