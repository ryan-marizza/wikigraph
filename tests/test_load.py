# pytest passes fixtures in by name, which pylint reads as shadowing the
# module-level fixture functions. It isn't.
# pylint: disable=redefined-outer-name
import datetime as dt
import os
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from psycopg import sql

from wikigraph.load import COLUMNS, PG_TYPES, load_parquet, partition_name, upsert_manifest
from wikigraph.parse import SCHEMA, parse_shard_to_parquet

FIXTURE_XML = Path(__file__).parent / "fixtures" / "mini_dump.xml"
DUMP = dt.date(2026, 7, 1)

# Shard names the integration tests are allowed to create and drop partitions
# for. The second one exercises the sanitizing in partition_name() against a
# real identifier, not just a string comparison.
SHARD = "pytest_p1p9"
ODD_SHARD = "pytest-p1.p9"
TEST_SHARDS = (SHARD, ODD_SHARD)


# --------------------------------------------------------------------------
# Parquet fixtures, built from mini_dump.xml by the real parser.
#
# Generated rather than committed: a checked-in .parquet is a binary blob that
# silently rots the moment parse.SCHEMA changes, and the whole point of these
# tests is to catch exactly that kind of drift between parse and load.
# --------------------------------------------------------------------------
def _build(tmp_dir: Path, shard: str, **kwargs) -> Path:
    # Named after the partition, not the shard: shard names in these tests
    # include characters that are legal in Postgres and illegal in a Windows
    # filename. partition_name() already sanitizes to [0-9a-zA-Z_].
    out = tmp_dir / f"{partition_name(shard)}.parquet"
    parse_shard_to_parquet(FIXTURE_XML, shard, DUMP, out, **kwargs)
    return out


@pytest.fixture(scope="session")
def parquet(tmp_path_factory) -> Path:
    """The 5 ns=0 pages of mini_dump.xml, shard_name = SHARD."""
    return _build(tmp_path_factory.mktemp("staging"), SHARD)


@pytest.fixture(scope="session")
def rows_in_parquet(parquet) -> list[tuple]:
    """Expected COPY payload: one tuple per row, in COLUMNS order."""
    return [tuple(r[c] for c in COLUMNS) for r in pq.read_table(parquet).to_pylist()]


# --------------------------------------------------------------------------
# Test double for psycopg. Records what the loader did, in order, so the
# SQL-shape tests run everywhere — including CI, which has no Postgres.
# --------------------------------------------------------------------------
def render(obj) -> str:
    """Composed SQL -> the string Postgres would actually receive."""
    return obj.as_string() if isinstance(obj, sql.Composable) else str(obj)


class FakeCopy:
    def __init__(self, conn, stmt):
        self.conn = conn
        self.stmt = stmt
        self.types = None
        self.rows = []

    def set_types(self, types):
        self.types = list(types)
        self.conn.log("set_types", list(types))

    def write_row(self, row):
        self.rows.append(tuple(row))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, query, params=None):
        self.conn.log("execute", render(query), params)

    def copy(self, stmt):
        cp = FakeCopy(self.conn, render(stmt))
        self.conn.copies.append(cp)
        self.conn.log("copy", cp.stmt)
        return cp

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConnection:
    def __init__(self, dsn, **kwargs):
        self.dsn = dsn
        self.kwargs = kwargs
        self.events = []
        self.copies = []
        self._autocommit = kwargs.get("autocommit", False)

    def log(self, kind, *payload):
        self.events.append((kind, *payload))

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        self._autocommit = value
        self.log("autocommit", value)

    def cursor(self):
        return FakeCursor(self)

    def execute(self, query, params=None):
        # upsert_manifest goes straight through the connection.
        self.log("execute", render(query), params)

    def commit(self):
        self.log("commit")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.log("close")
        return False

    # -- assertion helpers ------------------------------------------------
    def kinds(self) -> list[str]:
        return [e[0] for e in self.events]

    def statements(self) -> list[str]:
        return [e[1] for e in self.events if e[0] == "execute"]

    def index_of(self, kind, needle="") -> int:
        for i, event in enumerate(self.events):
            if event[0] == kind and (not needle or needle in str(event[1])):
                return i
        raise AssertionError(f"no {kind} event matching {needle!r} in {self.events}")


@pytest.fixture
def fake_db(monkeypatch) -> list[FakeConnection]:
    """Replace psycopg.connect; return the list of connections that were opened."""
    opened: list[FakeConnection] = []

    def connect(dsn, **kwargs):
        conn = FakeConnection(dsn, **kwargs)
        opened.append(conn)
        return conn

    monkeypatch.setattr("wikigraph.load.psycopg.connect", connect)
    return opened


# --------------------------------------------------------------------------
# partition_name
# --------------------------------------------------------------------------
def test_partition_name_prefixes():
    assert partition_name("p10p1400054") == "page_p10p1400054"


@pytest.mark.parametrize(
    "shard, expected",
    [
        ("p1-p9", "page_p1_p9"),
        ("p1.p9", "page_p1_p9"),
        ("p1 p9", "page_p1_p9"),
        ("keeps_underscores", "page_keeps_underscores"),
        ("MiXeD123", "page_MiXeD123"),
        ("", "page_"),
    ],
)
def test_partition_name_sanitizes(shard, expected):
    assert partition_name(shard) == expected


def test_partition_name_defuses_injection():
    part = partition_name('x"; DROP TABLE raw.page; --')
    assert part == "page_x___DROP_TABLE_raw_page____"
    assert '"' not in part and ";" not in part


# --------------------------------------------------------------------------
# load_parquet — SQL shape and COPY payload, no database required
# --------------------------------------------------------------------------
def test_load_issues_statements_in_order(parquet, fake_db):
    load_parquet("postgresql://fake/db", parquet, SHARD)

    conn = fake_db[0]
    part = f'raw."{partition_name(SHARD)}"'
    create, truncate = conn.statements()[0], conn.statements()[1]

    assert create == (
        f"CREATE TABLE IF NOT EXISTS {part} "
        f"PARTITION OF raw.page FOR VALUES IN ('{SHARD}')"
    )
    assert truncate == f"TRUNCATE {part}"

    # Truncate must land before the COPY, and ANALYZE after the commit —
    # otherwise the partition is either double-loaded or analyzed empty.
    assert conn.index_of("execute", "TRUNCATE") < conn.index_of("copy")
    assert conn.index_of("commit") < conn.index_of("execute", "ANALYZE")
    assert conn.statements()[-1] == f"ANALYZE {part}"

    # ANALYZE has to run outside the transaction to be durable on its own.
    assert conn.index_of("commit") < conn.index_of("autocommit")
    assert ("autocommit", True) in conn.events


def test_copy_targets_partition_with_all_columns(parquet, fake_db):
    load_parquet("postgresql://fake/db", parquet, SHARD)

    cols = ", ".join(f'"{c}"' for c in COLUMNS)
    assert fake_db[0].copies[0].stmt == (
        f'COPY raw."{partition_name(SHARD)}" ({cols}) FROM STDIN (FORMAT BINARY)'
    )


def test_copy_declares_one_type_per_column(parquet, fake_db):
    load_parquet("postgresql://fake/db", parquet, SHARD)

    assert len(COLUMNS) == len(PG_TYPES)
    assert fake_db[0].copies[0].types == PG_TYPES


def test_copy_writes_every_row_in_column_order(parquet, rows_in_parquet, fake_db):
    stats = load_parquet("postgresql://fake/db", parquet, SHARD)

    written = fake_db[0].copies[0].rows
    assert written == rows_in_parquet
    assert stats == {
        "shard_name": SHARD,
        "rows_loaded": len(rows_in_parquet),
        "partition": partition_name(SHARD),
    }


def test_copy_row_values_survive_the_round_trip(parquet, fake_db):
    load_parquet("postgresql://fake/db", parquet, SHARD)
    by_id = {row[0]: dict(zip(COLUMNS, row)) for row in fake_db[0].copies[0].rows}

    assert set(by_id) == {101, 102, 104, 105, 106}
    # Every row carries the partition key and the dump date COPY needs.
    assert all(r["shard_name"] == SHARD for r in by_id.values())
    assert all(r["dump_date"] == DUMP for r in by_id.values())

    assert by_id[102]["is_redirect"] is True
    assert by_id[102]["redirect_target"] == "Dog"
    assert by_id[102]["contributor_id"] is None            # anonymous edit
    assert by_id[101]["revision_ts"] == dt.datetime(
        2026, 6, 1, 12, 0, tzinfo=dt.timezone.utc
    )
    # NULL and empty-string must stay distinguishable all the way into COPY.
    assert by_id[104]["wikitext"] == ""
    assert by_id[104]["text_bytes"] == 0
    assert by_id[105]["wikitext"] is None
    assert by_id[105]["text_bytes"] is None


def test_batch_size_does_not_change_the_payload(parquet, rows_in_parquet, fake_db):
    # 5 rows over batches of 2 — proves batching neither drops nor reorders.
    load_parquet("postgresql://fake/db", parquet, SHARD, batch_size=2)
    assert fake_db[0].copies[0].rows == rows_in_parquet


def test_parquet_path_accepts_str_and_path(parquet, rows_in_parquet, fake_db):
    load_parquet("postgresql://fake/db", str(parquet), SHARD)
    load_parquet("postgresql://fake/db", Path(parquet), SHARD)
    assert fake_db[0].copies[0].rows == fake_db[1].copies[0].rows == rows_in_parquet


def test_columns_are_selected_by_name_not_file_order(tmp_path, rows_in_parquet, fake_db):
    # COPY lists columns in COLUMNS order, so the reader must too. Shuffling the
    # file's own column order must not shift values into the wrong columns.
    table = pq.read_table(_build(tmp_path, SHARD))
    shuffled = tmp_path / "shuffled.parquet"
    pq.write_table(table.select(list(reversed(COLUMNS))), shuffled)

    load_parquet("postgresql://fake/db", shuffled, SHARD)
    assert fake_db[0].copies[0].rows == rows_in_parquet


def test_empty_parquet_still_truncates(tmp_path, fake_db):
    empty = tmp_path / "empty.parquet"
    pq.write_table(pa.Table.from_pylist([], schema=SCHEMA), empty)

    stats = load_parquet("postgresql://fake/db", empty, SHARD)

    assert stats["rows_loaded"] == 0
    assert fake_db[0].copies[0].rows == []
    # An empty input is a legitimate result, not a no-op: the partition still
    # gets emptied, or a rerun would leave yesterday's rows behind.
    assert any("TRUNCATE" in s for s in fake_db[0].statements())


def test_missing_parquet_opens_no_connection(tmp_path, fake_db):
    with pytest.raises((FileNotFoundError, OSError)):
        load_parquet("postgresql://fake/db", tmp_path / "nope.parquet", SHARD)
    assert fake_db == []


def test_ddl_quotes_a_hostile_shard_name(tmp_path, fake_db):
    hostile = 'x"; DROP TABLE raw.page; --'
    load_parquet("postgresql://fake/db", _build(tmp_path, hostile), hostile)

    create = fake_db[0].statements()[0]
    assert f'raw."{partition_name(hostile)}"' in create
    # The value side is a quoted literal, so the payload cannot break out.
    assert create.endswith("""FOR VALUES IN ('x"; DROP TABLE raw.page; --')""")
    assert "DROP TABLE raw.page;" not in create.replace("'x\"; DROP TABLE raw.page; --'", "")


# --------------------------------------------------------------------------
# upsert_manifest
# --------------------------------------------------------------------------
def test_upsert_manifest_no_fields_is_a_no_op(fake_db):
    assert upsert_manifest("postgresql://fake/db", SHARD, DUMP) is None
    assert fake_db == []


def test_upsert_manifest_builds_insert_on_conflict(fake_db):
    upsert_manifest("postgresql://fake/db", SHARD, DUMP, status="parsed", pages_seen=6)

    conn = fake_db[0]
    assert conn.kwargs == {"autocommit": True}      # one statement, no open txn
    assert conn.statements() == [
        'INSERT INTO raw.ingest_manifest (shard_name, dump_date, "status", "pages_seen") '
        "VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (shard_name, dump_date) DO UPDATE SET "
        '"status" = EXCLUDED."status", "pages_seen" = EXCLUDED."pages_seen"'
    ]


def test_upsert_manifest_params_line_up_with_columns(fake_db):
    ended = dt.datetime(2026, 7, 1, 3, 0, tzinfo=dt.timezone.utc)
    upsert_manifest(
        "postgresql://fake/db", SHARD, DUMP,
        status="loaded", pages_loaded=5, load_ended=ended,
    )

    _, statement, params = fake_db[0].events[0]
    assert params == [SHARD, DUMP, "loaded", 5, ended]
    # Placeholder count must match the parameter count exactly.
    assert statement.count("%s") == len(params)


def test_upsert_manifest_quotes_field_names(fake_db):
    upsert_manifest("postgresql://fake/db", SHARD, DUMP, **{'weird"name': 1})
    assert '"weird""name"' in fake_db[0].statements()[0]


# --------------------------------------------------------------------------
# Integration: a real COPY into a real partition.
#
# Skipped unless the warehouse env vars are set (`.\tasks.ps1 test` exports
# them from .env; CI does not), so these are opt-in by construction.
# --------------------------------------------------------------------------
def _integration_dsn() -> str | None:
    dsn = os.environ.get("WIKIGRAPH_TEST_DSN")
    if dsn:
        return dsn
    needed = ("PG_ETL_USER", "PG_ETL_PASSWORD", "PG_HOST_PORT", "PG_DB")
    if not all(os.environ.get(k) for k in needed):
        return None
    host = os.environ.get("PG_TEST_HOST", "localhost")
    return (
        f"postgresql://{os.environ['PG_ETL_USER']}:{os.environ['PG_ETL_PASSWORD']}"
        f"@{host}:{os.environ['PG_HOST_PORT']}/{os.environ['PG_DB']}"
    )


def _drop_test_objects(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        for shard in TEST_SHARDS:
            conn.execute(
                sql.SQL("DROP TABLE IF EXISTS raw.{}").format(
                    sql.Identifier(partition_name(shard))
                )
            )
        conn.execute(
            "DELETE FROM raw.ingest_manifest WHERE shard_name = ANY(%s)", [list(TEST_SHARDS)]
        )


@pytest.fixture
def warehouse():
    """A reachable, migrated warehouse. Cleans up its own partitions afterwards."""
    dsn = _integration_dsn()
    if not dsn:
        pytest.skip("no warehouse env vars (WIKIGRAPH_TEST_DSN or PG_*); skipping")
    try:
        with psycopg.connect(dsn, connect_timeout=5) as conn:
            if conn.execute("SELECT to_regclass('raw.page')").fetchone()[0] is None:
                pytest.skip("raw.page missing — run `.\\tasks.ps1 migrate`")
    except psycopg.OperationalError as exc:
        pytest.skip(f"warehouse not reachable: {exc}")

    _drop_test_objects(dsn)
    try:
        yield dsn
    finally:
        _drop_test_objects(dsn)


def _fetch(dsn: str, query: str, params=None) -> list[tuple]:
    with psycopg.connect(dsn) as conn:
        return conn.execute(query, params).fetchall()


@pytest.mark.integration
def test_pg_types_match_the_live_table(warehouse):
    rows = _fetch(warehouse, """
        SELECT attname, format_type(atttypid, atttypmod)
          FROM pg_attribute
         WHERE attrelid = 'raw.page'::regclass AND attnum > 0 AND NOT attisdropped
    """)
    # Binary COPY has no type negotiation: a mismatch here is silent corruption
    # or an unhelpful "insufficient data left in message" at load time.
    alias = {"timestamp with time zone": "timestamptz"}
    live = {name: alias.get(typ, typ) for name, typ in rows}
    assert [live[c] for c in COLUMNS] == PG_TYPES


@pytest.mark.integration
def test_load_writes_rows_into_its_own_partition(warehouse, parquet):
    stats = load_parquet(warehouse, parquet, SHARD)
    assert stats["rows_loaded"] == 5

    part = partition_name(SHARD)
    assert _fetch(warehouse, f"SELECT count(*) FROM raw.{part}")[0][0] == 5
    # And it is reachable through the parent, i.e. the partition is attached.
    assert _fetch(
        warehouse, "SELECT count(*) FROM raw.page WHERE shard_name = %s", [SHARD]
    )[0][0] == 5


@pytest.mark.integration
def test_loaded_values_match_the_parquet(warehouse, parquet):
    load_parquet(warehouse, parquet, SHARD)
    rows = {
        r[0]: dict(zip(COLUMNS, r))
        for r in _fetch(
            warehouse,
            sql.SQL("SELECT {} FROM raw.page WHERE shard_name = %s")
            .format(sql.SQL(", ").join(map(sql.Identifier, COLUMNS)))
            .as_string(),
            [SHARD],
        )
    }

    assert set(rows) == {101, 102, 104, 105, 106}
    assert rows[101]["dump_date"] == DUMP
    assert rows[102]["is_redirect"] is True
    assert rows[102]["redirect_target"] == "Dog"
    assert rows[102]["contributor_name"] == "192.0.2.7"
    assert rows[102]["contributor_id"] is None
    assert rows[104]["title"] == "Salt & Pepper"      # entity survives the round trip
    assert rows[104]["wikitext"] == ""                # empty text, not NULL
    assert rows[105]["wikitext"] is None
    assert rows[106]["revision_id"] == 9006           # first revision only


@pytest.mark.integration
def test_reload_replaces_rather_than_appends(warehouse, parquet, tmp_path):
    load_parquet(warehouse, parquet, SHARD)
    load_parquet(warehouse, parquet, SHARD)

    def count():
        return _fetch(
            warehouse, "SELECT count(*) FROM raw.page WHERE shard_name = %s", [SHARD]
        )[0][0]

    assert count() == 5

    # Idempotent means "end state depends only on the input" — shrink the input
    # and the table must shrink with it, not keep the orphaned rows.
    assert load_parquet(warehouse, _build(tmp_path, SHARD, limit=2), SHARD)["rows_loaded"] == 2
    assert count() == 2


@pytest.mark.integration
def test_sanitized_shard_name_loads(warehouse, tmp_path):
    stats = load_parquet(warehouse, _build(tmp_path, ODD_SHARD), ODD_SHARD)

    assert stats["partition"] == "page_pytest_p1_p9"
    assert _fetch(
        warehouse, "SELECT count(*) FROM raw.page WHERE shard_name = %s", [ODD_SHARD]
    )[0][0] == 5


@pytest.mark.integration
def test_shard_name_mismatch_is_rejected(warehouse, parquet):
    # The parquet's shard_name column is the partition key. Loading it into a
    # partition declared for a different value must fail loudly, not scatter
    # rows into the wrong partition.
    with pytest.raises(psycopg.errors.CheckViolation):
        load_parquet(warehouse, parquet, ODD_SHARD)


@pytest.mark.integration
def test_upsert_manifest_inserts_then_updates(warehouse):
    upsert_manifest(warehouse, SHARD, DUMP, status="parsing", pages_seen=6)
    upsert_manifest(warehouse, SHARD, DUMP, status="loaded", pages_loaded=5)

    rows = _fetch(
        warehouse,
        "SELECT status, pages_seen, pages_loaded FROM raw.ingest_manifest "
        "WHERE shard_name = %s AND dump_date = %s",
        [SHARD, DUMP],
    )
    # One row, updated in place — and the field the second call omitted is
    # untouched rather than nulled out.
    assert rows == [("loaded", 6, 5)]
