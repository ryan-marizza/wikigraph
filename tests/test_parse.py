import datetime as dt

import pyarrow.parquet as pq

from wikigraph.parse import parse_shard_to_parquet

FIXTURE = "tests/fixtures/mini_dump.xml"
DUMP = dt.date(2026, 7, 1)

def test_counts_and_filtering(tmp_path):
    stats = parse_shard_to_parquet(FIXTURE, "p1p9", DUMP, tmp_path / "x.parquet")
    assert stats["pages_seen"] == 6
    assert stats["pages_written"] == 5
    assert stats["multi_revision_pages"] == 1


def test_row_contents(tmp_path):
    out = tmp_path / "x.parquet"
    parse_shard_to_parquet(FIXTURE, "p1p9", DUMP, out)
    rows = {r["page_id"]: r for r in pq.read_table(out).to_pylist()}

    assert all(r["namespace"] == 0 for r in rows.values())
    assert(set(rows) == {101, 102, 104, 105, 106})

    #redirect
    assert rows[102]["is_redirect"] is True
    assert rows[102]["redirect_target"] == "Dog"
    #anonymous editor: IP lands in contributor_name, id is null
    assert rows[102]["contributor_name"] == "192.0.2.7"
    assert rows[102]["contributor_id"] is None

    #non-redirect has no target
    assert rows[101]["is_redirect"] is False
    assert rows[101]["redirect_target"] is None

    #XML entity is unescaped by the parser
    assert rows[104]["title"] == "Salt & Pepper"
    #bytes="0" must be 0, not None - the falsy-string trap
    assert rows[104]["text_bytes"] == 0
    assert rows[104]["wikitext"] == ""

    #page with no <text> element survives with nulls
    assert rows[105]["wikitext"] is None
    assert rows[105]["text_bytes"] is None

    #only the first revision is kept
    assert rows[106]["revision_id"] == 9006
    assert rows[106]["wikitext"] == "aaa"


def test_limit_stops_early(tmp_path):
    stats = parse_shard_to_parquet(FIXTURE, "p1p9", DUMP, tmp_path / "x.parquet", limit=2)
    assert stats["pages_seen"] == 2
