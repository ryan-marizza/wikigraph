"""Sample real articles out of the warehouse into a test fixture.

Committing a few hundred real articles turns "it worked when I ran it" into a
regression suite. Deterministic by page_id so the corpus is stable across runs
and a diff means something.

Usage:  python scripts/sample_articles.py --n 300
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
from dotenv import load_dotenv

import psycopg

load_dotenv()

OUT = pathlib.Path("tests/fixtures/articles.jsonl")

PG_DSN = f"postgres://{os.environ['PG_ETL_USER']}:{os.environ['PG_ETL_PASSWORD']}@localhost:{os.environ['PG_HOST_PORT']}/wikigraph"

# Stratified: a uniform sample is 90% short stubs and would never exercise the
# template-heavy, table-heavy tail where the bugs live.
QUERY = """
WITH banded AS (
  SELECT page_id, title, wikitext,
         ntile(4) OVER (ORDER BY text_bytes) AS band
  FROM raw.page
  WHERE shard_name = %(shard)s AND NOT is_redirect AND wikitext IS NOT NULL
)
SELECT page_id, title, wikitext FROM (
  SELECT *, row_number() OVER (PARTITION BY band ORDER BY page_id) AS rn
  FROM banded
) x
WHERE rn <= %(per_band)s
ORDER BY page_id
"""

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=300)
    ap.add_argument("--shard", default="p10p1400054")
    args = ap.parse_args()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(PG_DSN) as conn:
        rows = conn.execute(
            QUERY, {"shard": args.shard, "per_band": args.n // 4}
        ).fetchall()

    with OUT.open("w", encoding="utf-8", newline="\n") as fh:
        for page_id, title, wikitext in rows:
            fh.write(json.dumps(
                {"page_id": page_id, "title": title, "wikitext": wikitext},
                ensure_ascii=False) + "\n")
    mb = OUT.stat().st_size / 1e6
    print(f"wrote {len(rows)} articles to {OUT} ({mb:.1f} MB)")


if __name__ == "__main__":
    main()