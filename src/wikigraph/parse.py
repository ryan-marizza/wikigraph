"""Stream MediaWiki XML export shards into Parquet.

Design constraints this file satisfies:
  * flat memory regardless of shard size (see the iter_pages cleanup idiom)
  * ns=0 filtering at parse time — never write namespaces you don't want
  * counts the grain assertion (pages with >1 <revision>) for free
  * zero Airflow imports, so it's unit-testable in milliseconds
"""
from __future__ import annotations

from datetime import datetime, timezone
from importlib.resources import path
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree

from .config import MW_NS, PARQUET_ROW_GROUP


def q(tag:str) -> str:
    """Qualify a tag name with the MediaWiki namespace.
    lxml reports tags as '{http://...}page', not 'page'."""
    return f"{{{MW_NS}}}{tag}"

SCHEMA = pa.schema([
    ("page_id",         pa.int32()),
    ("dump_date",       pa.date32()),
    ("shard_name",      pa.string()),
    ("title",           pa.string()),
    ("namespace",       pa.int16()),
    ("is_redirect",     pa.bool_()),
    ("redirect_target", pa.string()),
    ("revision_id",     pa.int64()),
    ("revision_ts",     pa.timestamp("us", tz="UTC")),
    ("contributor_name",pa.string()),
    ("contributor_id",  pa.int64()),
    ("text_bytes",      pa.int32()),
    ("wikitext",        pa.string())
])

def _ts(raw: str | None):
    if not raw:
        return None
    return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

def _page_to_row(elem, shard_name:str, dump_date) -> dict:
    redirect = elem.find(q("redirect"))
    rev = elem.find(q("revision"))

    contributor_name = contributor_id = None
    revision_id = revision_ts = text_bytes = wikitext = None

    if rev is not None:
        rid = rev.findtext(q("id"))
        revision_id = int(rid) if rid else None
        revision_ts = _ts(rev.findtext(q("timestamp")))

        contrib = rev.find(q("contributor"))
        if contrib is not None:
            # Anonymous edits have <ip> instead of <username>.
            contributor_name = contrib.findtext(q("username")) or contrib.findtext(q("ip"))

            cid = contrib.findtext(q("id"))
            contributor_id = int(cid) if cid else None

        text_el = rev.find(q("text"))
        if text_el is not None:
            b = text_el.get("bytes")
            text_bytes = int(b) if b else None
            # Postgres text columns cannot contain NUL bytes. Wikitext occasionally
            # does. Strip defensively here rather than debugging a COPY failure
            # 40 minutes into a load.

            wikitext = (text_el.text or "").replace("\x00", "")

    return {
        "page_id":            int(elem.findtext(q("id"))),
        "dump_date":          dump_date,
        "shard_name":         shard_name,
        "title":              elem.findtext(q("title")),
        "namespace":          int(elem.findtext(q("ns"))),
        "is_redirect":        redirect is not None,
        "redirect_target":    redirect.get("title") if redirect is not None else None,
        "revision_id":        revision_id,
        "revision_ts":        revision_ts,
        "contributor_name":   contributor_name,
        "contributor_id":     contributor_id,
        "text_bytes":         text_bytes,
        "wikitext":           wikitext,
    }

def iter_pages(path:str | Path, shard_name: str, dump_date) -> Iterator[tuple[dict, int]]:
    """Yield (row, n_revisions) for every <page> element. Memory stays flat.

    The cleanup block at the bottom of the loop is load-bearing — see the
    measurements in the runbook. clear() frees the page's children; deleting
    preceding siblings frees the empty stubs that would otherwise accumulate
    on the root element for the entire run.
    """
    context = etree.iterparse(str(path), events=("end",), tag=q("page"), huge_tree=True)
    for _event, elem in context:
       n_rev = len(elem.findall(q("revision")))
       yield _page_to_row(elem, shard_name, dump_date), n_rev

       elem.clear()
       parent = elem.getparent()
       while elem.getprevious() is not None:
           del parent[0]
    del context


def parse_shard_to_parquet(
        path: str | Path,
        shard_name: str,
        dump_date,
        out_path: str | Path,
        limit: int | None = None,
) -> dict:
    """Stream one shard to a Parquet file, ns=0 only.

    Returns a stats dict — this becomes the manifest row and the reconciliation
    baseline that the loader checks itself against.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    buf: list[dict] = []
    pages_seen = pages_written = multi_rev = 0
    writer = pq.ParquetWriter(out_path, SCHEMA, compression="zstd")

    def flush():
        nonlocal buf
        if not buf:
            return
        cols = {f.name: [r[f.name] for r in buf] for f in SCHEMA}
        writer.write_table(pa.Table.from_pydict(cols, schema=SCHEMA))
        buf = []

    try:
        for row, n_rev in iter_pages(path, shard_name, dump_date):
            pages_seen += 1
            if n_rev > 1:
                multi_rev += 1
            if row["namespace"] != 0:
                continue
            buf.append(row)
            pages_written += 1
            if len(buf) >= PARQUET_ROW_GROUP:
                flush()
            if limit and pages_written >= limit:
                break
        flush()
    finally:
        writer.close()

    return {
        "shard_name":           shard_name,
        "dump_date":            str(dump_date),
        "parquet_path":         str(out_path),
        "pages_seen":           pages_seen,
        "pages_written":        pages_written,
        "multi_revision_pages": multi_rev,
        "bytes_out":            out_path.stat().st_size
    }