-- Objects in this file are owned by 'etl' so the loader can create partitions
-- and truncate them at runtime without superuser rights.
SET ROLE etl;

-- Landing table. Mirrors <page> 1:1 for ns=0.
--
-- PARTITIONED BY shard_name because the partition is the unit of reprocessing:
-- rerunning a shard TRUNCATEs one partition instead of trying to delete 900k
-- rows out of a 18M-row table. This is what makes the loader idempotent (Rule 2).
CREATE TABLE IF NOT EXISTS raw.page (
    page_id           integer     NOT NULL,
    dump_date         date        NOT NULL,
    shard_name        text        NOT NULL,
    title             text        NOT NULL,   -- display form, as it appears in the XML
    namespace         smallint    NOT NULL,
    is_redirect       boolean     NOT NULL,
    redirect_target   text,                   -- raw title from <redirect title="...">
    revision_id       bigint,
    revision_ts       timestamptz,
    contributor_name  text,
    contributor_id    bigint,
    text_bytes        integer,
    wikitext          text,                   -- large; Postgres TOASTs and compresses it
    ingested_at       timestamptz NOT NULL DEFAULT now()
) PARTITION BY LIST (shard_name);

-- NOTE: no primary key. A PK on a partitioned table must INCLUDE the partition
-- key, which would mean (page_id, shard_name) — and that's a weaker guarantee
-- than you want anyway. Uniqueness of page_id is asserted in dbt at the stg
-- layer (Step 14), where it can be checked across the whole table at once.

-- Manifest: what has been ingested, when, and whether it worked.
-- This is the difference between "the DAG is green" and "I can prove what is
-- in the warehouse." Green DAGs lie; row counts don't.
CREATE TABLE IF NOT EXISTS raw.ingest_manifest (
    shard_name     text        NOT NULL,
    dump_date      date        NOT NULL,
    file_bytes     bigint,
    pages_seen     bigint,     -- every <page> in the shard, all namespaces
    pages_loaded   bigint,     -- ns=0 only
    parse_started  timestamptz,
    parse_ended    timestamptz,
    load_ended     timestamptz,
    status         text        NOT NULL,   -- pending | parsing | parsed | loaded | failed
    error_detail   text,
    PRIMARY KEY (shard_name, dump_date)
);

RESET ROLE;