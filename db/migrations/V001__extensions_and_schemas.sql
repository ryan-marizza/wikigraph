-- Extensions required by the design doc.
-- pg_trgm  : trigram similarity, for fuzzy title search (goal #2)
-- unaccent : strips diacritics so "Beyonce" matches "Beyoncé"
-- pg_stat_statements : query performance history. Requires the
--                      shared_preload_libraries flag set in docker-compose.yml.

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Three schemas with real ownership separation.
CREATE SCHEMA IF NOT EXISTS raw  AUTHORIZATION etl;
CREATE SCHEMA IF NOT EXISTS stg  AUTHORIZATION dbt;
CREATE SCHEMA IF NOT EXISTS mart AUTHORIZATION dbt;

-- dbt reads raw but cannot write it.
GRANT USAGE ON SCHEMA raw TO dbt;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO dbt;

-- ...and automatically gets SELECT on tables etl creates LATER (the per-shard
-- partitions the loader creates at runtime). Without this line, dbt would fail
-- on every new shard until you manually re-granted.
ALTER DEFAULT PRIVILEGES FOR ROLE etl IN SCHEMA raw GRANT SELECT ON TABLES TO dbt;

-- analyst reads marts only
GRANT USAGE ON SCHEMA mart TO analyst;
ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA mart GRANT SELECT ON TABLES TO analyst;