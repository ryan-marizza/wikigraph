"""Load parsed Parquet into raw.page via binary COPY. Idempotent per shard."""
from __future__ import annotations

import re
from pathlib import Path
import psycopg
import pyarrow.parquet as pq
from psycopg import sql

COLUMNS = [
    "page_id", "dump_date", "shard_name", "title", "namespace",
    "is_redirect", "redirect_target", "revision_id", "revision_ts",
    "contributor_name", "contributor_id", "text_bytes", "wikitext", 
]

PG_TYPES = [
    "integer", "date", "text", "text", "smallint",
    "boolean", "text", "bigint", "timestamptz",
    "text", "bigint", "integer", "text",
]

def partition_name(shard_name: str) -> str:
    """'p10p1400054' -> 'page_p10p1400054'. Sanitized for use as an identifier."""
    return "page_" + re.sub(r"[^0-9a-zA-Z_]", "_", shard_name)

def load_parquet(
        dsn: str,
        parquet_path: str | Path,
        shard_name: str,
        batch_size: int = 2_000
) -> dict:
    """Load one shard's Parquet into its own partition of raw.page.

    Safe to run repeatedly: the partition is truncated first, so the end state
    depends only on the input file, never on how many times this ran.
    """
    part = partition_name(shard_name)
    pf = pq.ParquetFile(parquet_path)
    rows = 0

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # 1. Ensure this shard's partition exists.
            #    DDL can't take bound parameters, so sql.Literal/Identifier do the
            #    quoting. Never build DDL with f-strings.
            cur.execute(
                sql.SQL(
                    "CREATE TABLE IF NOT EXISTS raw.{part} "
                    "PARTITION OF raw.page FOR VALUES IN ({val})"
                ).format(part = sql.Identifier(part), val = sql.Literal(shard_name))
            )

            # 2. Idempotency: replace, never append.
            cur.execute(
                sql.SQL("TRUNCATE raw.{part}").format(part = sql.Identifier(part))
            )

            # 3. Stream Parquet straight into COPY. Copying into the PARTITION
            #    rather than the parent table skips tuple routing overhead.
            copy_stmt = sql.SQL(
                "COPY raw.{part} ({cols}) FROM STDIN (FORMAT BINARY)"
            ).format(
                part = sql.Identifier(part),
                cols = sql.SQL(", ").join(map(sql.Identifier, COLUMNS))
            )

            with cur.copy(copy_stmt) as cp:
                cp.set_types(PG_TYPES)
                for batch in pf.iter_batches(batch_size=batch_size, columns=COLUMNS):
                    cols = [c.to_pylist() for c in batch.columns]
                    for row in zip(*cols):
                        cp.write_row(row)
                        rows += 1

        # TRUNCATE + COPY commit together: no window where the partition is empty.
        conn.commit()

        # 4. Refresh planner statistics. Do NOT skip this — see the note above.
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("ANALYZE raw.{part}").format(part = sql.Identifier(part)))

        return {"shard_name": shard_name, "rows_loaded": rows, "partition": part}

def upsert_manifest(dsn: str, shard_name: str, dump_date, **fields) -> None:
    """Record ingest state. Called at parse start, parse end, and load end.

    This table is how you answer 'what is actually in the warehouse and when did
    it get there' three weeks from now, when the Airflow logs have rotated away.
    """
    if not fields:
        return
    cols = list(fields)
    assignments = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in cols
    )
    stmt = sql.SQL(
        "INSERT INTO raw.ingest_manifest (shard_name, dump_date, {cols}) "
        "VALUES (%s, %s, {ph}) "
        "ON CONFLICT (shard_name, dump_date) DO UPDATE SET {assign}"
    ).format(
        cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
        ph=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
        assign=assignments,
    )

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(stmt, [shard_name, dump_date, *[fields[c] for c in cols]])
