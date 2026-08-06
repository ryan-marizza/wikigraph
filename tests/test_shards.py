import pytest

from wikigraph.shards import discover


def test_raises_on_empty_dir(tmp_path):
    with pytest.raises(FileNotFoundError):
        discover(tmp_path)

def test_parses_shard_name(tmp_path):
    (tmp_path / "enwiki-2026-07-01-p10p1400054.xml").write_text("x")
    (tmp_path / "not-a-shard.xml").write_text("x")
    shards = discover(tmp_path)
    assert len(shards) == 1
    assert shards[0].name == "p10p1400054"
    assert shards[0].dump_date == "2026-07-01"

def test_finds_nested_shards(tmp_path):
    d = tmp_path / "enwiki-2026-07-01-p10p1400054.xml"
    d.mkdir()
    (d / "enwiki-2026-07-01-p10p1400054.xml").write_text("x")
    assert discover(tmp_path)[0].name == "p10p1400054"
