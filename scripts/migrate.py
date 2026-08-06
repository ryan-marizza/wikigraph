"""Apply database migrations. Idempotent — safe to run any number of times.

Two phases:
  1. ensure_roles()  — create login roles from environment variables.
                       Passwords never appear in a .sql file or in git.
  2. apply()         — run db/migrations/V*.sql in filename order, once each,
                       recording what was applied in public.schema_migrations.

Run via:  .\\tasks.ps1 migrate
"""
from __future__ import annotations

import os
import pathlib
import sys

import psycopg
from psycopg import sql

DSN = os.environ["WIKIGRAPH_ADMIN_DSN"]           # connects as superuser 'postgres'
MIGRATIONS = pathlib.Path(__file__).resolve().parent.parent / "db" / "migrations"

ROLES = [
    (os.environ.get("PG_ETL_USER", "etl"), os.environ.get("PG_ETL_PASSWORD", "wikigraph")),
    (os.environ.get("PG_DBT_USER", "dbt"), os.environ.get("PG_DBT_PASSWORD", "wikigraph")),
]

def ensure_roles(conn: psycopg.Connection) -> None:
    """Create or update login roles. Uses sql.Identifier/Literal so the role
    name and password are correctly quoted — never use f-strings for DDL."""
    for name, password in ROLES:
        exists = conn.execute(
            sql.SQL("SELECT 1 FROM pg_roles WHERE rolname = {}").format(sql.Literal(name))
        ).fetchone()
        if exists:
            print(f"Role {name} already exists, updating password")
            conn.execute(
                sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(
                    sql.Identifier(name), sql.Literal(password)
                )
            )
            print(f"Role {name} password updated")
        else:
            conn.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(name), sql.Literal(password)
                )
            )
            print(f"Role {name} created")

    if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname = 'analyst'").fetchone():
        conn.execute("CREATE ROLE analyst NOLOGIN")
        print("Role analyst created")

def apply(conn: psycopg.Connection) -> None:
    """Run every unapplied V*.sql migration in version order, one transaction each."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS public.schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now()
        )
    """)
    conn.commit()

    applied = {
        r[0] for r in conn.execute("SELECT version FROM public.schema_migrations")
    }

    files = sorted(MIGRATIONS.glob("V*.sql"))

    if not files:
        print(f"WARNING: No migration files found in {MIGRATIONS}", file=sys.stderr)

    for path in files:
        version = path.name.split("__")[0]
        if version in applied:
            print(f"skipping {path.name} (already applied)")
            continue

        try:
            conn.execute(path.read_text(encoding="utf-8"))
            conn.execute(
                "INSERT INTO public.schema_migrations (version) VALUES (%s)", (version,)
            )
            conn.commit()
        except Exception:
            conn.rollback()
            print(
                f"FAILED on {path.name}, rolling back. "
                "Fix the error and re-run this script.",
                file=sys.stderr,
            )
            raise

def main() -> None:
    """Entry point: ensure roles exist, then apply pending migrations."""
    with psycopg.connect(DSN, autocommit=False) as conn:
        ensure_roles(conn)
        conn.commit()
        apply(conn)
        print("All migrations applied successfully.")

if __name__ == "__main__":
    main()
