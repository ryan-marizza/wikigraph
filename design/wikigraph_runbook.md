# WikiGraph — Hands-On Runbook (Windows / Docker Desktop)

**Goal of this document:** get you from "Airflow runs, I have 19 XML files" to "I can run a SQL query against `mart.dim_article` and see real Wikipedia articles, and the whole thing rebuilds from scratch with two commands."

**Scope:** one shard (`p10p1400054`), end to end. Backfilling the other 18 is a later, easier problem — it's the same DAG with different parameters.

**Your setup, as stated:** Windows, files on `C:\`, no WSL2 dev shell, Docker Desktop running Airflow, a few years of Python/SQL/Docker, new to Airflow and dbt.

**How to read this:** every step has four parts.

- **Why** — what this buys you and what breaks later if you skip it
- **Do** — exact commands and file contents
- **Verify** — how you know it worked, with the output you should see
- **If it breaks** — the failure modes I expect you to actually hit

Do not skip **Verify**. The whole point of this ordering is that each step is provable before the next one depends on it. A pipeline where you're not sure which layer is wrong is much harder to debug than one where you stopped at the first red light.

**Time estimate:** Steps 1–8 are an evening or two. Steps 9–16 are a weekend. The single longest wall-clock item is one shard parse, which you'll measure in Step 8.

---

## Table of contents

| Step | What | Est. |
|---|---|---|
| 0 | Orientation: the two rules | 10 min |
| 1 | Repo scaffolding + Windows git hygiene | 30 min |
| 2 | Python package and local venv | 30 min |
| 3 | Docker Desktop prep: disk, RAM, network | 20 min |
| 4 | The warehouse Postgres container | 45 min |
| 5 | Roles, schemas, and a migration runner | 1 hr |
| 6 | Measure your disk before optimizing it | 20 min |
| 7 | The parser, as a testable library | 2 hr |
| 8 | Run the parser on a real shard | 1 hr + wait |
| 9 | The loader: Parquet → Postgres | 1.5 hr |
| 10 | Custom Airflow image + warehouse wiring | 1.5 hr |
| 11 | Pools and a smoke DAG | 45 min |
| 12 | The ingest DAG | 2 hr |
| 13 | dbt: what it is and project setup | 1 hr |
| 14 | Your first dbt models and tests | 2 hr |
| 15 | Cosmos: dbt models as Airflow tasks | 1 hr |
| 16 | End-to-end verification | 45 min |

---

## Step 0 — Orientation: the two rules

### Why

Almost every bad Airflow codebase is bad for one of two reasons. Internalize these now and you'll skip a lot of pain.

**Rule 1: DAG files contain orchestration only. All real logic lives in an importable Python package.**

If your XML parser lives inside an `@task` function, then to test one line of it you have to save the file, wait ~30 seconds for the scheduler to re-parse the DAG, trigger a run, wait for a worker to pick it up, and read the result out of the Airflow UI's log viewer. That's a two-minute feedback loop for a one-second question.

If it lives in `src/wikigraph/parse.py`, you run `pytest tests/test_parse.py` and get an answer in 200 milliseconds. You touch Airflow only when you're wiring things together, which is maybe 5% of your total time on this project.

This is also the difference between code you can move to Snowflake later and code you can't. A parser that imports nothing from Airflow runs anywhere.

**Rule 2: Every task must be safe to run twice.**

Reruns are not an edge case in data engineering — they are the normal operating mode. Tasks fail on transient errors, you find a bug and reprocess, a shard arrives late. If rerunning a load appends rows instead of replacing them, you get silent duplicates, and you find out three transformations downstream when a `unique` test fails and you have no idea when the corruption started.

The technique used throughout this runbook: **partition-per-shard, and reruns truncate the partition before loading it.** Same input always produces the same end state. This is why the design doc partitions `raw.page` by `shard_name` — the partition is the unit of reprocessing.

### The shape of what you're building

```
raw_data/*.xml  ──parse──▶  Parquet  ──COPY──▶  raw.page  ──dbt──▶  stg.*  ──dbt──▶  mart.*
   (12 GB/shard)            (staging)           (typed)             (clean)          (analytics)
      Step 7-8               volume              Step 9              Step 14          Step 14
```

Four containers when you're done:

- **Airflow** (already running) — scheduler, worker, API server, and its own metadata Postgres
- **`wikigraph-warehouse`** (Step 4) — a *separate* Postgres holding your actual data

They talk over a shared Docker network you'll create in Step 3.

### Why the warehouse is not Airflow's metadata database

You already have a Postgres container — the one Airflow uses for its own bookkeeping. It is tempting to just make a `wikigraph` database inside it. Don't. Four reasons, and you'd feel all of them:

1. **Autovacuum and checkpoints.** A 100 GB table generates enormous vacuum and WAL activity. On a shared instance that stalls Airflow's scheduler heartbeats, and your tasks start getting killed as "zombies" — with no error message that points at the real cause.
2. **Opposite tuning.** A warehouse wants `work_mem = 64MB` and `synchronous_commit = off`. A metadata store wants small `work_mem` and durable commits. You cannot tune one instance for both.
3. **Lifecycle.** You will want to `docker compose down -v` the warehouse and rebuild from scratch, repeatedly, during development. You do not want to lose your DAG run history every time you do that.
4. **It's what real shops do.** Orchestrator metadata and warehouse data are never colocated in production. Building the habit costs you nothing here.

**Verify:** nothing to run. Move on.

---

## Step 1 — Repo scaffolding + Windows git hygiene

### Why

Two things: a layout that separates library code from orchestration (Rule 1), and a `.gitattributes` file that will save you an hour of confusion later.

The `.gitattributes` point is Windows-specific and worth explaining. Git on Windows defaults to `core.autocrlf=true`, which converts line endings to CRLF (`\r\n`) on checkout. Linux containers run shell scripts and expect LF (`\n`). A `.sh` file with CRLF endings fails inside a container with `bad interpreter: /bin/bash^M` or, worse, an error message that mentions a totally unrelated line. This bites essentially every Windows developer working with Docker exactly once, and it costs about an hour because the error doesn't point at line endings.

You'll notice this runbook avoids shell scripts in the Docker init path entirely (Step 5 uses Python instead) partly for this reason. But you'll write `.sh` files eventually, so set the guard now.

### Do

Open PowerShell. Navigate to wherever your existing project lives — the one with `airflow-docker/`, `design/`, `eda/`, `raw_data/`. I'll call this `$REPO` from here on.

```powershell
cd C:\path\to\your\wikigraph      # adjust to your actual location
```

If you haven't already:

```powershell
git init
```

Create the new directories. This *extends* your existing layout rather than replacing it:

```powershell
New-Item -ItemType Directory -Force -Path `
  docker\warehouse, `
  db\migrations, `
  src\wikigraph, `
  scripts, `
  tests\fixtures, `
  dbt, `
  staging
```

Your tree should now be:

```
wikigraph/
├── airflow-docker/          # EXISTING — your Airflow stack
│   ├── config/
│   ├── dags/
│   ├── plugins/
│   └── docker-compose.yaml
├── design/                  # EXISTING
├── eda/                     # EXISTING
├── raw_data/                # EXISTING — 19 shards, gitignored
├── docker/warehouse/        # NEW — the warehouse container
├── db/migrations/           # NEW — versioned SQL DDL
├── src/wikigraph/           # NEW — THE package. Zero Airflow imports.
├── scripts/                 # NEW — migrate.py and friends
├── tests/                   # NEW
├── dbt/                     # NEW — Step 13
└── staging/                 # NEW — local Parquet scratch, gitignored
```

Create `.gitattributes` at the repo root:

```
# Normalize text files in the repo to LF. Critical on Windows:
# anything that runs inside a Linux container MUST have LF endings.
* text=auto eol=lf

# Explicitly force LF on things Linux executes or parses.
*.sh      text eol=lf
*.sql     text eol=lf
*.yml     text eol=lf
*.yaml    text eol=lf
Dockerfile text eol=lf

# Binary — never touch these.
*.parquet binary
*.xml.bz2 binary
*.png     binary
```

Create `.gitignore`:

```
.env
.venv/
raw_data/
staging/
__pycache__/
*.pyc
.pytest_cache/
dbt/**/target/
dbt/**/dbt_packages/
dbt/**/logs/
NOTES.md.bak
```

> **Note on `raw_data/`:** 228 GB must never go near git. Also add it explicitly even though it's already there — if you ever `git add -A` before the ignore is in place, git will try to hash 228 GB and appear to hang forever.

Create `.env.example` at the repo root — this one *is* committed, with no real secrets:

```bash
# ---- Host paths (Windows). Use FORWARD SLASHES. Docker Compose handles them fine
# ---- and they avoid escaping headaches inside YAML.
WIKIGRAPH_RAW_DATA=C:/path/to/your/wikigraph/raw_data

# ---- Warehouse ----
PG_DB=wikigraph
PG_HOST_PORT=5433
POSTGRES_PASSWORD=change_me_superuser
PG_ETL_USER=etl
PG_ETL_PASSWORD=change_me_etl
PG_DBT_USER=dbt
PG_DBT_PASSWORD=change_me_dbt

# ---- Airflow reads this and registers a connection named 'wikigraph_warehouse'
# ---- automatically. 'warehouse' is the container hostname on the shared network.
AIRFLOW_CONN_WIKIGRAPH_WAREHOUSE=postgres://etl:change_me_etl@warehouse:5432/wikigraph

# ---- Custom Airflow image tag (Step 10) ----
AIRFLOW_IMAGE_NAME=wikigraph/airflow:local

# ---- Dump being processed ----
WIKIGRAPH_DUMP_DATE=2026-07-01
```

Now copy it and fill in real values:

```powershell
Copy-Item .env.example .env
notepad .env
```

Set `WIKIGRAPH_RAW_DATA` to your actual absolute path with forward slashes, and change the three passwords to anything you like. They're local-only, but pick distinct values so you can tell from an error message which role failed to authenticate.

> **Password gotcha:** avoid `@`, `:`, `/`, `#`, and `%` in these passwords. `AIRFLOW_CONN_*` is parsed as a URI, and unencoded special characters will mangle it in confusing ways. Letters, digits, underscores, and hyphens only — this is a local dev database, not the place to demonstrate password entropy.

Commit:

```powershell
git add .gitattributes .gitignore .env.example
git commit -m "Scaffold: repo layout, line-ending guard, env template"
```

### Verify

```powershell
Get-Content .env | Select-String "WIKIGRAPH_RAW_DATA"
Test-Path (Get-Content .env | Select-String "WIKIGRAPH_RAW_DATA").ToString().Split("=")[1]
```

Should print `True`. If it prints `False`, your path is wrong — fix it now, because five more things will read it.

Also confirm git is not tracking your data:

```powershell
git status --short
```

Should show nothing about `raw_data`. If it lists XML files, your `.gitignore` isn't being picked up — check it's at the repo root, not in a subdirectory.

### If it breaks

- **`git init` says it's already a repo** — fine, skip it.
- **`.gitignore` isn't working for files already tracked** — `.gitignore` only affects *untracked* files. If you already committed something, `git rm --cached -r raw_data` first.

---

## Step 2 — Python package and local venv

### Why

This is Rule 1 made concrete. You're creating an installable package so that `import wikigraph` works from three different places — your PowerShell terminal, pytest, and the Airflow container — without any `sys.path` hacking.

The `pip install -e .` ("editable install") part matters: it means changes to `src/wikigraph/parse.py` take effect immediately without reinstalling. That's your fast feedback loop.

Doing this *before* touching Docker is deliberate. You want to be able to develop and test the parser entirely on Windows, with a normal debugger, and only involve containers when you're orchestrating.

### Do

Create `pyproject.toml` at the repo root:

```toml
[project]
name = "wikigraph"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "lxml>=5.2",
    "pyarrow>=16",
    "psycopg[binary]>=3.2",
]

[project.optional-dependencies]
dev = ["pytest>=8", "ruff>=0.5"]

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]

[tool.ruff]
line-length = 100

[tool.pytest.ini_options]
testpaths = ["tests"]
```

Create the package marker:

```powershell
New-Item -ItemType File -Force -Path src\wikigraph\__init__.py
```

Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

> **If `Activate.ps1` is blocked** with "running scripts is disabled on this system," run this once (it allows locally-created scripts, still blocks unsigned downloads):
> ```powershell
> Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
> ```

Now create `src/wikigraph/config.py` — one place where every path and constant lives:

```python
"""Single source of truth for paths and constants.

Everything reads from environment variables with sensible local defaults,
so the same code runs on your Windows host and inside the Airflow container
without conditionals.
"""
from __future__ import annotations

import os
from pathlib import Path

# On your host these default to repo-relative paths.
# Inside Airflow, the container sets them to /opt/airflow/... (Step 10).
RAW_DIR = Path(os.environ.get("WIKIGRAPH_RAW_DIR", "raw_data"))
STAGING_DIR = Path(os.environ.get("WIKIGRAPH_STAGING_DIR", "staging"))
DUMP_DATE = os.environ.get("WIKIGRAPH_DUMP_DATE", "2026-07-01")

# MediaWiki export schema namespace. Every XML tag is prefixed with this.
MW_NS = "http://www.mediawiki.org/xml/export-0.11/"

# Rows per Parquet row group. 50k balances write buffering against memory.
PARQUET_ROW_GROUP = 50_000
```

### Verify

```powershell
python -c "import wikigraph.config as c; print(c.MW_NS); print(c.RAW_DIR.resolve())"
```

Expected:

```
http://www.mediawiki.org/xml/export-0.11/
C:\path\to\your\wikigraph\raw_data
```

Also confirm the heavy dependencies actually imported — on Windows these are wheels, but it's worth knowing now rather than in Step 7:

```powershell
python -c "import lxml.etree, pyarrow, psycopg; print('lxml', lxml.etree.__version__); print('pyarrow', pyarrow.__version__); print('psycopg', psycopg.__version__)"
```

### If it breaks

- **`ModuleNotFoundError: No module named 'wikigraph'`** — the editable install didn't take. Confirm your venv is active (prompt shows `(.venv)`), then re-run `pip install -e ".[dev]"`. Check that `src/wikigraph/__init__.py` exists; setuptools won't find the package without it.
- **`pyarrow` fails to install** — you're probably on Python 3.13+ before wheels landed, or a 32-bit Python. `python -c "import sys; print(sys.version, sys.maxsize > 2**32)"` should show 3.11/3.12 and `True`.

Commit:

```powershell
git add pyproject.toml src/
git commit -m "Python package skeleton with config module"
```

---

## Step 3 — Docker Desktop prep: disk, RAM, network

### Why

Three settings that are annoying to change later and painful to discover the hard way.

**Disk.** Docker Desktop stores all named volumes inside a single virtual disk image with a fixed maximum size. The default is often 64 GB. Your warehouse will want ~35 GB for one shard and ~120 GB after backfill. When that virtual disk fills, Postgres does not fail gracefully — it can leave the data directory in a state you have to `down -v` to recover from. Raise it now.

**RAM.** You'll tune Postgres's `shared_buffers` to a fraction of what Docker has. You need to know the number.

**Network.** Your Airflow stack and your warehouse are two separate Docker Compose projects. By default, each Compose project creates its own isolated network, and containers in one cannot resolve hostnames in the other. An **external network** that both attach to is the clean fix — cleaner than merging everything into one giant compose file, because it keeps their lifecycles independent (Rule from Step 0).

### Do

**3a. Raise the disk limit.**

Docker Desktop → **Settings** (gear icon) → **Resources** → **Advanced**.

- **Disk image size**: set to at least **150 GB**. This is a maximum, not an allocation — the file grows on demand, so setting it high costs you nothing until you use it.
- If your `C:` drive is short on space, use **Disk image location** on the same screen to move it to another drive. Do this *now*; moving it later requires recreating all volumes.
- **Memory**: note the current value. If it's below 8 GB and your machine has 16 GB+, raise it to 8–12 GB.
- **CPUs**: note this too. It sets your parallelism ceiling in Step 11.

Click **Apply & restart**. This takes a minute or two.

**3b. Write down your numbers.**

Create `NOTES.md` at the repo root. This file is where measured facts live, and it's the single highest-value habit in this runbook — capacity planning is mostly just "I wrote down what happened last time."

```markdown
# WikiGraph — Measured Facts

## Environment
- Date:
- Host: Windows, Docker Desktop <version>
- Docker RAM: ___ GB
- Docker CPUs: ___
- Docker disk image max: ___ GB, located on drive ___
- Host free space on raw_data drive: ___ GB

## Measurements
(filled in as we go)
```

Fill in the blanks. Get Docker's version with:

```powershell
docker version --format '{{.Server.Version}}'
```

And host free space:

```powershell
Get-PSDrive C | Select-Object Used,Free
```

**3c. Create the shared network.**

```powershell
docker network create wikigraph-net
```

### Verify

```powershell
docker network ls | Select-String wikigraph-net
docker info --format '{{.MemTotal}}' 
```

The first should print a line with `wikigraph-net` and driver `bridge`. The second prints Docker's memory in bytes — divide by 1073741824 for GB and confirm it matches what you set.

### If it breaks

- **`network with name wikigraph-net already exists`** — good, it's there. Move on.
- **Settings → Resources shows no Advanced tab** — you're on the Hyper-V backend or an older version. The WSL2 backend is the default and better; check **Settings → General → Use the WSL 2 based engine**. With the WSL2 backend, disk limits are managed by WSL rather than Docker Desktop, and you may need to set them in `%UserProfile%\.wslconfig` instead. If you go that route, restart WSL with `wsl --shutdown` after editing.

---

## Step 4 — The warehouse Postgres container

### Why

This is the database the whole project writes to. Two things make this compose file different from a stock `postgres` container, and both matter:

**The tuning flags.** Default Postgres is configured to start on a Raspberry Pi. `shared_buffers` defaults to 128 MB. For bulk-loading tens of millions of rows and running analytical scans over them, the defaults will be 10–50× slower than a tuned instance. The flags below are a reasonable warehouse profile for a laptop; each one is commented with what it does.

**`synchronous_commit=off`.** This is a deliberate durability tradeoff and you should understand it rather than copy it. Normally Postgres waits for the write-ahead log to hit physical disk before telling you a commit succeeded. Turning it off means a commit returns as soon as the WAL is in memory, so a **power loss or container kill can lose the last ~200ms of committed transactions**. Data corruption is still impossible — this is not `fsync=off` — you just lose the tail. For this project that's obviously correct: every row is rebuildable from the XML, and the speedup on bulk `COPY` is large. For anything with data you can't regenerate, it would be obviously wrong. **Do not carry this flag into a real system without thinking about it.**

**Port 5433, not 5432.** Your Airflow metadata Postgres is probably already on 5432. Using 5433 on the host means no collision and no ambiguity about which database you're connected to.

### Do

Create `docker/warehouse/docker-compose.yml`:

```yaml
name: wikigraph-warehouse

services:
  warehouse:
    image: postgres:17
    container_name: wikigraph-warehouse
    restart: unless-stopped
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${PG_DB}
      PGDATA: /var/lib/postgresql/data/pgdata
    ports:
      # host:container — 5433 on the host avoids colliding with Airflow's metadata DB
      - "${PG_HOST_PORT:-5433}:5432"
    volumes:
      # NAMED VOLUME, never a bind mount to C:\ — see the warning below
      - wikigraph_pgdata:/var/lib/postgresql/data
    shm_size: 1gb          # parallel query workers use shared memory; 64MB default is too small
    command:
      - postgres
      # ---- memory ----
      - -c
      - shared_buffers=2GB              # ~25% of Docker's RAM
      - -c
      - effective_cache_size=6GB        # a HINT to the planner about OS cache; ~50-75% of RAM
      - -c
      - work_mem=64MB                   # per sort/hash operation. Multiplied by concurrent ops!
      - -c
      - maintenance_work_mem=1GB        # used by CREATE INDEX and VACUUM. Bigger = much faster.
      # ---- write-ahead log / bulk load behaviour ----
      - -c
      - max_wal_size=8GB                # big COPYs generate lots of WAL; small values force
      - -c                              # constant checkpoints, which murder load throughput
      - min_wal_size=1GB
      - -c
      - checkpoint_timeout=30min
      - -c
      - checkpoint_completion_target=0.9   # spread checkpoint I/O out instead of spiking
      - -c
      - wal_compression=zstd
      - -c
      - synchronous_commit=off          # DEV ONLY. See the explanation above. Not fsync=off.
      # ---- planner assumptions (SSD) ----
      - -c
      - random_page_cost=1.1            # default 4.0 assumes spinning rust; wrong on SSD
      - -c
      - effective_io_concurrency=200
      # ---- parallelism ----
      - -c
      - max_worker_processes=8
      - -c
      - max_parallel_workers=8
      - -c
      - max_parallel_workers_per_gather=4
      - -c
      - max_parallel_maintenance_workers=4
      # ---- partitioning: the design doc depends on these being on (they default on,
      # ---- but being explicit documents the intent) ----
      - -c
      - enable_partition_pruning=on
      - -c
      - enable_partitionwise_join=on
      - -c
      - enable_partitionwise_aggregate=on
      # ---- observability: you will want these the first time something is slow ----
      - -c
      - shared_preload_libraries=pg_stat_statements
      - -c
      - track_io_timing=on
      - -c
      - log_min_duration_statement=5000    # log any query over 5 seconds
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d ${PG_DB}"]
      interval: 10s
      timeout: 5s
      retries: 10

volumes:
  wikigraph_pgdata:

networks:
  default:
    name: wikigraph-net
    external: true
```

> **Tuning note.** `shared_buffers=2GB` and `effective_cache_size=6GB` assume you gave Docker ~8 GB. If you gave it 16 GB, use `4GB` and `12GB`. If you gave it 4 GB, use `1GB` and `3GB`. The ratio matters more than the absolute number.

> ### ⚠️ Never bind-mount PGDATA to a Windows path
>
> You might be tempted to write `- ./pgdata:/var/lib/postgresql/data` so you can "see the files." **Do not.** Postgres relies on POSIX file permissions and `fsync()` semantics that the Windows filesystem bridge does not provide correctly. The container will either refuse to start with a permissions error, or — worse — appear to work and corrupt data silently under load. The named volume `wikigraph_pgdata` lives inside Docker's Linux VM where Postgres gets real POSIX behavior. This applies to *any* database in Docker on Windows or macOS, not just this one.

> **Why `postgres:17` and not `18`?** 17 is mature, and every tuning guide and Stack Overflow answer you'll find applies to it directly. Postgres 18 also changed the default `PGDATA` layout to a version-specific path (`/var/lib/postgresql/18/docker`), which would silently break the `PGDATA` line above. Not worth the friction for this project. Nothing in the design depends on 18-only features.

Now create a task runner. Windows has no `make`, so this is a small PowerShell dispatcher that gives you the same convenience. Create `tasks.ps1` at the repo root:

```powershell
#Requires -Version 5.1
<#
  WikiGraph task runner.  Usage:  .\tasks.ps1 <command>
  Reads .env from the repo root and exposes it to child processes.
#>
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('db-up','db-down','db-nuke','db-shell','db-logs','migrate',
                 'test','lint','airflow-up','airflow-down','airflow-build','af','dbt')]
    [string]$Command,

    # Everything after the command is passed through (used by 'af' and 'dbt').
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest
)

$ErrorActionPreference = 'Stop'
$Repo = $PSScriptRoot

# ---- load .env into this process's environment ----
$envFile = Join-Path $Repo '.env'
if (-not (Test-Path $envFile)) { throw ".env not found at $envFile. Copy .env.example first." }
Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith('#') -and $line.Contains('=')) {
        $k, $v = $line.Split('=', 2)
        [Environment]::SetEnvironmentVariable($k.Trim(), $v.Trim(), 'Process')
    }
}

$WarehouseDir = Join-Path $Repo 'docker\warehouse'
$AirflowDir   = Join-Path $Repo 'airflow-docker'
$AdminDsn     = "postgresql://postgres:$env:POSTGRES_PASSWORD@localhost:$env:PG_HOST_PORT/$env:PG_DB"

# Docker Compose discovers docker-compose.yaml AND docker-compose.override.yml
# by looking in the CURRENT DIRECTORY. The --project-directory flag does NOT
# change that -- it only sets the base path for relative volume mounts. So we
# genuinely have to cd into the directory, or the override silently won't load
# (which looks like "my mounts vanished" and is very confusing to debug).
# $envFile is absolute, so it still resolves from anywhere.
function Invoke-Compose {
    param([string]$Dir, [string[]]$ComposeArgs)
    Push-Location $Dir
    try { docker compose --env-file $envFile @ComposeArgs }
    finally { Pop-Location }
}

switch ($Command) {
    'db-up'    { Invoke-Compose $WarehouseDir @('up','-d') }
    'db-down'  { Invoke-Compose $WarehouseDir @('down') }
    'db-logs'  { Invoke-Compose $WarehouseDir @('logs','-f','warehouse') }
    'db-nuke'  {
        Write-Host "This DELETES all warehouse data." -ForegroundColor Yellow
        if ((Read-Host "Type 'yes' to confirm") -eq 'yes') {
            Invoke-Compose $WarehouseDir @('down','-v')
        } else { Write-Host "Aborted." }
    }
    'db-shell' {
        # psql from INSIDE the container, so you don't need psql.exe on Windows
        docker exec -it wikigraph-warehouse psql -U $env:PG_ETL_USER -d $env:PG_DB
    }
    'migrate' {
        $env:WIKIGRAPH_ADMIN_DSN = $AdminDsn
        python (Join-Path $Repo 'scripts\migrate.py')
    }
    'test' { python -m pytest -q }
    'lint' { python -m ruff check src tests scripts }

    'airflow-build' { docker build -t $env:AIRFLOW_IMAGE_NAME (Join-Path $AirflowDir 'docker') }
    'airflow-up'    { Invoke-Compose $AirflowDir @('up','-d') }
    'airflow-down'  { Invoke-Compose $AirflowDir @('down') }

    # Run any airflow CLI command:   .\tasks.ps1 af dags list
    'af'  { Invoke-Compose $AirflowDir (@('exec','-T','airflow-scheduler','airflow') + $Rest) }

    # Run any dbt command:           .\tasks.ps1 dbt build
    'dbt' {
        $inner = "cd /opt/airflow/dbt/wikigraph && dbt $($Rest -join ' ') --profiles-dir ."
        Invoke-Compose $AirflowDir @('exec','-T','airflow-scheduler','bash','-lc', $inner)
    }
}
```

Start the warehouse:

```powershell
.\tasks.ps1 db-up
.\tasks.ps1 db-logs
```

Watch the log until you see `database system is ready to accept connections`, then press `Ctrl+C` to stop following.

### Verify

```powershell
docker exec -it wikigraph-warehouse psql -U postgres -d wikigraph -c "SELECT version();"
```

You should see a `PostgreSQL 17.x on x86_64-pc-linux-gnu...` line.

Confirm the tuning actually applied — this is worth doing, because a typo in the `command:` list can cause Postgres to ignore a setting silently:

```powershell
docker exec -it wikigraph-warehouse psql -U postgres -d wikigraph -c "SELECT name, setting, unit FROM pg_settings WHERE name IN ('shared_buffers','work_mem','max_wal_size','synchronous_commit','random_page_cost');"
```

Expected (units vary — `shared_buffers` reports in 8kB blocks, so 2GB shows as `262144`):

```
        name         | setting | unit
---------------------+---------+------
 max_wal_size        | 8192    | MB
 random_page_cost    | 1.1     |
 shared_buffers      | 262144  | 8kB
 synchronous_commit  | off     |
 work_mem            | 65536   | kB
```

Confirm it's on the shared network:

```powershell
docker inspect wikigraph-warehouse --format '{{json .NetworkSettings.Networks}}'
```

Should mention `wikigraph-net`.

### If it breaks

- **`network wikigraph-net declared as external, but could not be found`** — you skipped Step 3c. Run `docker network create wikigraph-net`.
- **Container restarts in a loop** — `.\tasks.ps1 db-logs` and read the actual error. Most common: a typo in the `command:` list, which produces `unrecognized configuration parameter`. Note the YAML pattern is strict: every setting needs its own `- -c` line *before* it.
- **`port is already allocated`** — something else is on 5433. Change `PG_HOST_PORT` in `.env` and re-run `db-up`.
- **`Ctrl+C` during `db-logs` stopped my container** — it shouldn't (logs is read-only), but if the container is down, `.\tasks.ps1 db-up` again.

Commit:

```powershell
git add docker/ tasks.ps1 NOTES.md
git commit -m "Warehouse Postgres container, tuned for bulk load"
```

---

## Step 5 — Roles, schemas, and a migration runner

### Why

**Roles.** You're going to create three database roles instead of doing everything as `postgres`. In production, the process that loads raw data should not have permission to drop your marts — that separation is what stops a buggy script from being a catastrophe. Here it costs you ten minutes and builds the habit. It also makes permission errors *informative*: if dbt gets "permission denied for schema raw," you immediately know your grant model is wrong, rather than discovering six weeks later that everything runs as superuser.

| Role | Owns | Can do |
|---|---|---|
| `etl` | schema `raw` | Create/truncate/load `raw.*`. Read-only elsewhere. |
| `dbt` | schemas `stg`, `mart` | Read `raw.*`, create anything in `stg`/`mart`. |
| `analyst` | nothing | `SELECT` on `mart.*`. No login — a group role you'd grant to humans. |

**Migrations.** dbt will own `stg` and `mart` from Step 14 onward. But `raw` is *source-owned* DDL — dbt shouldn't manage it, and hand-typing `CREATE TABLE` into a GUI is exactly how environments drift until nobody can rebuild them. Numbered migration files plus a small runner give you: an ordered, replayable history; the ability to nuke and rebuild in under two minutes; and a diff in git for every schema change. This is what Flyway and Liquibase do — writing it once by hand is the fastest way to understand why they exist.

**Why Python instead of the container's `initdb` hook.** The `postgres` image runs scripts in `/docker-entrypoint-initdb.d` — but *only on the very first startup of an empty volume*. That's a genuinely confusing behavior when you're iterating: you edit the script, restart, and nothing happens. It also usually requires a `.sh` wrapper to get passwords in from the environment, which reintroduces the CRLF problem from Step 1. A Python runner is idempotent, re-runnable, gives real error messages, and keeps passwords out of SQL files entirely.

### Do

**5a. The migration runner.** Create `scripts/migrate.py`:

```python
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
    (os.environ.get("PG_ETL_USER", "etl"), os.environ["PG_ETL_PASSWORD"]),
    (os.environ.get("PG_DBT_USER", "dbt"), os.environ["PG_DBT_PASSWORD"]),
]


def ensure_roles(conn: psycopg.Connection) -> None:
    """Create or update login roles. Uses sql.Identifier/Literal so the role
    name and password are correctly quoted — never use f-strings for DDL."""
    for name, password in ROLES:
        exists = conn.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = %s", (name,)
        ).fetchone()
        if exists:
            conn.execute(
                sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(
                    sql.Identifier(name), sql.Literal(password)
                )
            )
            print(f"role  {name}: updated")
        else:
            conn.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(name), sql.Literal(password)
                )
            )
            print(f"role  {name}: created")

    # 'analyst' is a NOLOGIN group role — you GRANT it to people, nobody logs in as it.
    if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname = 'analyst'").fetchone():
        conn.execute("CREATE ROLE analyst NOLOGIN")
        print("role  analyst: created")


def apply(conn: psycopg.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS public.schema_migrations (
            version    text PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now()
        )
    """)
    conn.commit()

    applied = {
        r[0] for r in conn.execute("SELECT version FROM public.schema_migrations")
    }

    files = sorted(MIGRATIONS.glob("V*.sql"))
    if not files:
        print(f"WARNING: no migrations found in {MIGRATIONS}", file=sys.stderr)

    for path in files:
        version = path.name.split("__")[0]
        if version in applied:
            print(f"skip  {path.name}")
            continue
        print(f"apply {path.name}")
        # Each migration runs in its own transaction: if it fails, it rolls back
        # cleanly and the version is NOT recorded, so a fixed rerun works.
        try:
            conn.execute(path.read_text(encoding="utf-8"))
            conn.execute(
                "INSERT INTO public.schema_migrations (version) VALUES (%s)", (version,)
            )
            conn.commit()
        except Exception:
            conn.rollback()
            print(f"FAILED on {path.name}", file=sys.stderr)
            raise


def main() -> None:
    with psycopg.connect(DSN, autocommit=False) as conn:
        ensure_roles(conn)
        conn.commit()
        apply(conn)
    print("migrations: done")


if __name__ == "__main__":
    main()
```

**5b. First migration — extensions and schemas.** Create `db/migrations/V001__extensions_and_schemas.sql`:

```sql
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

-- analyst reads marts only.
GRANT USAGE ON SCHEMA mart TO analyst;
ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA mart GRANT SELECT ON TABLES TO analyst;
```

**5c. Title normalization.** Create `db/migrations/V002__norm_title.sql`:

```sql
-- ONE implementation of MediaWiki title normalization, in SQL, used by everything.
--
-- WHY SQL AND NOT PYTHON: the single hardest-to-find bug in this entire project
-- would be two implementations of this rule drifting apart. Python normalizes a
-- link target one way, SQL normalizes the page title another way, the join silently
-- drops 3% of edges, and nothing errors. Every graph statistic is then quietly wrong.
-- One implementation, in the layer where the join happens. The Python parser stays
-- dumb: it emits target_raw exactly as written and lets SQL compute target_norm.
--
-- MediaWiki title rules implemented here:
--   * underscores and spaces are equivalent      New_York = New York
--   * runs of whitespace collapse to one space
--   * leading/trailing whitespace is stripped
--   * the FIRST letter is case-insensitive       apple = Apple
--     but the rest are case-SENSITIVE            iPhone != IPhone
CREATE OR REPLACE FUNCTION public.norm_title(t text)
RETURNS text
LANGUAGE sql
IMMUTABLE PARALLEL SAFE STRICT
AS $$
    SELECT CASE
             WHEN s = '' THEN s
             ELSE upper(left(s, 1)) || substr(s, 2)
           END
    FROM (
        SELECT regexp_replace(btrim(replace(t, '_', ' ')), '\s+', ' ', 'g') AS s
    ) x;
$$;

-- KNOWN GAP: does not unescape HTML entities. '&amp;' should become '&'.
-- This affects a small but nonzero fraction of titles. Measure the count once
-- shard 0 is loaded (query in Step 16) before deciding whether to fix it.
--   TODO(V00x): decide on entity handling based on measured impact.

COMMENT ON FUNCTION public.norm_title(text) IS
  'Canonical MediaWiki title normalization. Join on this, never on the display title.';
```

**5d. The raw layer.** Create `db/migrations/V003__raw_schema.sql`:

```sql
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
```

**5e. Run it.**

```powershell
.\tasks.ps1 migrate
```

Expected output:

```
role  etl: created
role  dbt: created
role  analyst: created
apply V001__extensions_and_schemas.sql
apply V002__norm_title.sql
apply V003__raw_schema.sql
migrations: done
```

### Verify

Run it a second time — this is the idempotency proof:

```powershell
.\tasks.ps1 migrate
```

Should now print `skip` for all three files. If it tries to re-apply, the bookkeeping table isn't working.

Check the schemas exist and are owned correctly:

```powershell
docker exec -it wikigraph-warehouse psql -U postgres -d wikigraph -c "\dn+"
```

Expected — note the Owner column:

```
                        List of schemas
  Name  |  Owner   |  Access privileges  | Description
--------+----------+---------------------+-------------
 mart   | dbt      | dbt=UC/dbt         +|
        |          | analyst=U/dbt       |
 public | pg_database_owner | ...        |
 raw    | etl      | etl=UC/etl         +|
        |          | dbt=U/etl           |
 stg    | dbt      | dbt=UC/dbt          |
```

Check the raw tables:

```powershell
docker exec -it wikigraph-warehouse psql -U postgres -d wikigraph -c "\dt raw.*"
```

Should list `page` (type: partitioned table) and `ingest_manifest`.

Test the normalization function — these are the cases that matter:

```powershell
docker exec -it wikigraph-warehouse psql -U postgres -d wikigraph -c @"
SELECT t, public.norm_title(t) AS normalized FROM (VALUES
  ('New_York'), ('New York'), ('  Dog  '), ('apple'), ('iPhone'),
  ('A  b'), (''), ('Salt & Pepper')
) v(t);
"@
```

Expected:

| t | normalized |
|---|---|
| `New_York` | `New York` |
| `New York` | `New York` |
| `  Dog  ` | `Dog` |
| `apple` | `Apple` |
| `iPhone` | `IPhone` |
| `A  b` | `A b` |
| `` | `` |
| `Salt & Pepper` | `Salt & Pepper` |

The key ones: rows 1 and 2 must produce **identical** output (that's the join key working), and `iPhone` → `IPhone` is *correct* — MediaWiki genuinely uppercases only the first character. `apple` and `Apple` are the same page; `iPhone` and `IPhone` are the same page too, because only position 1 is case-insensitive.

Finally, prove you can rebuild from nothing — this is the real deliverable of Steps 4 and 5:

```powershell
.\tasks.ps1 db-nuke      # type 'yes'
.\tasks.ps1 db-up
Start-Sleep -Seconds 15
.\tasks.ps1 migrate
```

Under two minutes, from zero to a fully-structured warehouse. That reproducibility is what makes the rest of this project safe to experiment with.

### If it breaks

- **`KeyError: 'PG_ETL_PASSWORD'`** — `tasks.ps1` didn't load `.env`. Check the file exists at the repo root and has no blank-line-with-spaces weirdness. Debug with `.\tasks.ps1 migrate` after adding `Write-Host $env:PG_ETL_PASSWORD` temporarily.
- **`connection refused` on localhost:5433** — the container isn't up or is still initializing. `docker ps` should show `wikigraph-warehouse` as `healthy` (not just `running`). Give it 15 seconds after `db-up`.
- **`extension "pg_stat_statements" is not available`** — the `shared_preload_libraries` flag didn't apply. Check Step 4's verify query. If it's genuinely unavailable, comment out that one `CREATE EXTENSION` line; nothing downstream depends on it.
- **`role "etl" does not exist` during V001** — `ensure_roles` ran but didn't commit before `apply`. Confirm the `conn.commit()` between them in `main()`.
- **`permission denied for schema public`** during V003 — you're not connected as `postgres`. Check `WIKIGRAPH_ADMIN_DSN` in `tasks.ps1` uses `postgres:...`, not `etl:...`.

Commit:

```powershell
git add db/ scripts/
git commit -m "Migrations: roles, schemas, norm_title, raw layer"
```

---

## Step 6 — Measure your disk before optimizing it

### Why

You'll read advice — including in your own existing build plan — that says Windows users must move `raw_data` into the WSL2 filesystem or suffer 3–5× slower reads. That advice is **half true**, and acting on it blindly costs you a 228 GB copy and a whole second development environment to maintain.

Here's the actual situation. Docker Desktop on Windows shares `C:\` into containers through a filesystem bridge. That bridge is genuinely slow for workloads with **many small files** (think `node_modules`, or `git status` on a huge repo). For **large sequential reads** — which is exactly what streaming a 12 GB XML file is — modern Docker Desktop is much closer to native, and the parse itself is CPU-bound anyway. If lxml can only chew through XML at 60 MB/s, it does not matter whether the disk can deliver 200 MB/s or 2000 MB/s.

So: measure first. If your read throughput comfortably exceeds your parse throughput, the mount is not your bottleneck and moving to WSL2 buys you nothing. That's the actual engineering judgment, and it's a better habit than following a rule of thumb.

### Do

First, raw read throughput *through the Docker mount*. This uses a throwaway container with the same bind mount Airflow will have:

```powershell
$raw = (Get-Content .env | Select-String "^WIKIGRAPH_RAW_DATA=").ToString().Split("=",2)[1].Trim()
docker run --rm -v "${raw}:/data:ro" alpine sh -c "cd /data && ls -la | head -5 && time dd if=/data/enwiki-2026-07-01-p10p1400054.xml of=/dev/null bs=8M count=1000"
```

> Adjust the filename if your shard is at a nested path — your file tree shows shards inside same-named subdirectories, so it may be `/data/enwiki-2026-07-01-p10p1400054.xml/enwiki-2026-07-01-p10p1400054.xml`. The `ls` in that command will show you.

That reads 8 GB. Note the `real` time. Throughput = 8000 MB ÷ seconds.

Now the same read on the Windows host for comparison:

```powershell
Measure-Command { Get-Content -Path "$raw\enwiki-2026-07-01-p10p1400054.xml" -ReadCount 0 -TotalCount 0 } 
```

(That's a rough comparison; the container `dd` number is the one that matters.)

Record both in `NOTES.md`:

```markdown
## Disk throughput (Step 6)
- Docker bind mount read: ____ MB/s
- Decision: ____
```

### The decision rule

You'll get a parse throughput number in Step 8. Compare:

| Bind mount read speed | What to do |
|---|---|
| **> 150 MB/s** | Stay on `C:\`. You are CPU-bound, not I/O-bound. Do nothing. |
| **60–150 MB/s** | Stay for now. Revisit only if Step 8 shows parse throughput near your read speed. |
| **< 60 MB/s** | Worth moving. See below. |

Most modern Docker Desktop setups land in the first bucket.

**If you do need to move**, the minimal version — you do *not* need a full WSL2 dev environment, just a faster location for the data:

```powershell
wsl --install -d Ubuntu        # if you don't have a distro yet; reboot may be needed
wsl -d Ubuntu -- mkdir -p /home/$USER/wikigraph_data
# copy one shard first and re-benchmark before committing to all 19
```

Then point `WIKIGRAPH_RAW_DATA` at `\\wsl$\Ubuntu\home\<user>\wikigraph_data`. Your Python, git, and editor all stay on Windows — only the bulk data moves.

### Verify

You have a number in `NOTES.md` and a decision. That's the whole step.

### If it breaks

- **`docker run` says the path is invalid** — Windows paths in `-v` need forward slashes or doubled backslashes. The `$raw` variable from `.env` should already have forward slashes if you followed Step 1.
- **File not found inside the container** — run `docker run --rm -v "${raw}:/data:ro" alpine find /data -name "*.xml" -maxdepth 2` to see the real layout.

---

## Step 7 — The parser, as a testable library

### Why

This is the heart of the project and it's pure Python — no Airflow, no Docker, no database. You will iterate on it dozens of times, so the loop needs to be fast: edit, `pytest`, see result, in under a second.

**Two things about this parser are non-obvious and worth understanding before you copy the code.**

**(a) `elem.clear()` alone does not fully free memory.** When `iterparse` finishes a `<page>` element, calling `elem.clear()` frees its children — but the (now empty) `<page>` element itself stays attached to the root as a sibling, forever. Over millions of pages those empty stubs accumulate.

I measured this rather than repeating folklore. Parsing 200,000 synthetic pages, peak RSS:

| Strategy | 50k pages | 100k | 150k | 200k |
|---|---|---|---|---|
| No cleanup | 162 MB | 307 MB | 453 MB | **599 MB** |
| `elem.clear()` only | 23 MB | 30 MB | 36 MB | **43 MB** |
| `clear()` + delete siblings | 16 MB | 16 MB | 16 MB | **16 MB** |

Read that carefully, because it corrects a claim you'll see elsewhere: **`clear()` alone does not blow up.** It leaks about 100 bytes per page — real, linear, but at a few million pages per shard that's a few hundred MB, which is survivable, not an OOM. What it *does* do is make your memory usage a function of input size, which means it works on your test shard and gets slowly worse on the biggest one. The sibling-deletion idiom makes it genuinely flat, costs two extra lines, and removes the variable entirely. Use it — but now you know why, and you know it's about predictability rather than avoiding a crash.

*(Caveat on those numbers: measured with small 200-byte page bodies. The residue is the empty element stub, so it should be roughly independent of article length, but confirm on your real shard in Step 8.)*

**(b) Use `lxml`, not the standard library's `xml.etree`.** Roughly 2–3× faster on this workload, and critically it supports the `tag=` filter on `iterparse`, so it never builds Python objects for elements you don't care about.

**Why Parquet in the middle, instead of parsing straight into Postgres?** Three reasons: it decouples a slow CPU-bound step from a slow I/O-bound step so they can be retried independently; Parquet is columnar and compressed, so 12 GB of XML becomes a couple of GB on disk; and it's the format both Snowflake and Databricks ingest natively, so this layer is the part of your pipeline that ports without a rewrite.

### Do

Create `src/wikigraph/shards.py`:

```python
"""Discover shard files on disk and describe them."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from .config import RAW_DIR

# enwiki-2026-07-01-p10p1400054.xml
#         ^dump      ^range (this is the shard name / partition key)
SHARD_RE = re.compile(r"enwiki-(?P<dump>\d{4}-\d{2}-\d{2})-(?P<range>p\d+p\d+)\.xml$")


@dataclass(frozen=True)
class Shard:
    name: str          # 'p10p1400054' — used as the Postgres partition key
    dump_date: str     # '2026-07-01'
    path: str
    size_bytes: int

    def as_dict(self) -> dict:
        """Airflow passes task return values through XCom, which needs plain
        JSON-serializable types. Hence dicts, not dataclasses, across task boundaries."""
        return {
            "name": self.name,
            "dump_date": self.dump_date,
            "path": self.path,
            "size_bytes": self.size_bytes,
        }


def discover(raw_dir: Path | None = None) -> list[Shard]:
    """Find all shard XML files. Searches one level deep because the files may
    live in same-named subdirectories."""
    raw_dir = Path(raw_dir) if raw_dir else RAW_DIR
    found: dict[str, Shard] = {}

    for pattern in ("enwiki-*.xml", "*/enwiki-*.xml"):
        for p in sorted(raw_dir.glob(pattern)):
            if not p.is_file():
                continue
            m = SHARD_RE.search(p.name)
            if not m:
                continue
            # If a shard appears at both depths, keep the first (shallower) one.
            found.setdefault(
                m["range"],
                Shard(
                    name=m["range"],
                    dump_date=m["dump"],
                    path=str(p),
                    size_bytes=p.stat().st_size,
                ),
            )

    if not found:
        # Fail loudly. A pipeline that silently succeeds on zero input is worse
        # than one that crashes, because you find out three layers downstream.
        raise FileNotFoundError(f"no shard files matched in {raw_dir.resolve()}")

    return [found[k] for k in sorted(found)]
```

Create `src/wikigraph/parse.py`:

```python
"""Stream MediaWiki XML export shards into Parquet.

Design constraints this file satisfies:
  * flat memory regardless of shard size (see the iter_pages cleanup idiom)
  * ns=0 filtering at parse time — never write namespaces you don't want
  * counts the grain assertion (pages with >1 <revision>) for free
  * zero Airflow imports, so it's unit-testable in milliseconds
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree

from .config import MW_NS, PARQUET_ROW_GROUP


def q(tag: str) -> str:
    """Qualify a tag name with the MediaWiki namespace.
    lxml reports tags as '{http://...}page', not 'page'."""
    return f"{{{MW_NS}}}{tag}"


# Explicit schema: types are declared once, here, and flow through to Postgres.
# Letting pyarrow infer types would give you int64 for everything and silently
# double your storage.
SCHEMA = pa.schema([
    ("page_id",          pa.int32()),
    ("dump_date",        pa.date32()),
    ("shard_name",       pa.string()),
    ("title",            pa.string()),
    ("namespace",        pa.int16()),
    ("is_redirect",      pa.bool_()),
    ("redirect_target",  pa.string()),
    ("revision_id",      pa.int64()),
    ("revision_ts",      pa.timestamp("us", tz="UTC")),
    ("contributor_name", pa.string()),
    ("contributor_id",   pa.int64()),
    ("text_bytes",       pa.int32()),
    ("wikitext",         pa.string()),
])


def _ts(raw: str | None):
    if not raw:
        return None
    return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _page_to_row(elem, shard_name: str, dump_date) -> dict:
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
        "page_id":          int(elem.findtext(q("id"))),
        "dump_date":        dump_date,
        "shard_name":       shard_name,
        "title":            elem.findtext(q("title")),
        "namespace":        int(elem.findtext(q("ns"))),
        "is_redirect":      redirect is not None,
        "redirect_target":  redirect.get("title") if redirect is not None else None,
        "revision_id":      revision_id,
        "revision_ts":      revision_ts,
        "contributor_name": contributor_name,
        "contributor_id":   contributor_id,
        "text_bytes":       text_bytes,
        "wikitext":         wikitext,
    }


def iter_pages(path: str | Path, shard_name: str, dump_date) -> Iterator[tuple[dict, int]]:
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

    def flush() -> None:
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
                continue                       # never write ns != 0 to disk
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
        "bytes_out":            out_path.stat().st_size,
    }
```

> **On `multi_revision_pages`:** your design doc flags an open question — whether any shard contains pages with more than one `<revision>` block. If any do, your one-row-per-page data model is wrong and every downstream count is off. Rather than writing a separate sampling task, this counts it on every page for free. Step 12 turns a nonzero value into a hard DAG failure.

**Now the test fixture.** Create `tests/fixtures/mini_dump.xml`. Every page in it exists to pin down one specific edge case:

```xml
<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.11/" version="0.11" xml:lang="en">
  <siteinfo><sitename>Wikipedia</sitename></siteinfo>
  <page>
    <title>Dog</title>
    <ns>0</ns>
    <id>101</id>
    <revision>
      <id>9001</id>
      <timestamp>2026-06-01T12:00:00Z</timestamp>
      <contributor><username>Alice</username><id>55</id></contributor>
      <text bytes="42" xml:space="preserve">The [[domestic dog]] is a [[mammal]].</text>
    </revision>
  </page>
  <page>
    <title>Doggo</title>
    <ns>0</ns>
    <id>102</id>
    <redirect title="Dog" />
    <revision>
      <id>9002</id>
      <timestamp>2025-01-02T03:04:05Z</timestamp>
      <contributor><ip>192.0.2.7</ip></contributor>
      <text bytes="20" xml:space="preserve">#REDIRECT [[Dog]]</text>
    </revision>
  </page>
  <page>
    <title>Talk:Dog</title>
    <ns>1</ns>
    <id>103</id>
    <revision>
      <id>9003</id>
      <timestamp>2024-05-05T00:00:00Z</timestamp>
      <contributor><username>Bob</username><id>66</id></contributor>
      <text bytes="5" xml:space="preserve">hi</text>
    </revision>
  </page>
  <page>
    <title>Salt &amp; Pepper</title>
    <ns>0</ns>
    <id>104</id>
    <revision>
      <id>9004</id>
      <timestamp>2023-03-03T09:09:09Z</timestamp>
      <contributor><username>Carol</username><id>77</id></contributor>
      <text bytes="0" xml:space="preserve"></text>
    </revision>
  </page>
  <page>
    <title>Empty Page</title>
    <ns>0</ns>
    <id>105</id>
    <revision>
      <id>9005</id>
      <timestamp>2022-02-02T02:02:02Z</timestamp>
      <contributor><username>Dave</username></contributor>
    </revision>
  </page>
  <page>
    <title>Two Revisions</title>
    <ns>0</ns>
    <id>106</id>
    <revision>
      <id>9006</id>
      <timestamp>2021-01-01T01:01:01Z</timestamp>
      <contributor><username>Eve</username><id>88</id></contributor>
      <text bytes="3" xml:space="preserve">aaa</text>
    </revision>
    <revision>
      <id>9007</id>
      <timestamp>2021-02-01T01:01:01Z</timestamp>
      <contributor><username>Eve</username><id>88</id></contributor>
      <text bytes="3" xml:space="preserve">bbb</text>
    </revision>
  </page>
</mediawiki>
```

| Page | Pins down |
|---|---|
| `Dog` | the happy path |
| `Doggo` | redirect flag + `redirect_target` + anonymous IP contributor (no `<id>`) |
| `Talk:Dog` | ns≠0 must be counted in `pages_seen` but excluded from `pages_written` |
| `Salt & Pepper` | XML entity in the title; `bytes="0"` must yield `0`, not `None` |
| `Empty Page` | no `<text>` element at all — must not crash |
| `Two Revisions` | the grain assertion must fire |

Create `tests/test_parse.py`:

```python
import datetime as dt

import pyarrow.parquet as pq

from wikigraph.parse import parse_shard_to_parquet

FIXTURE = "tests/fixtures/mini_dump.xml"
DUMP = dt.date(2026, 7, 1)


def test_counts_and_filtering(tmp_path):
    stats = parse_shard_to_parquet(FIXTURE, "p1p9", DUMP, tmp_path / "x.parquet")
    assert stats["pages_seen"] == 6          # every <page>, all namespaces
    assert stats["pages_written"] == 5       # the ns=1 Talk page is dropped
    assert stats["multi_revision_pages"] == 1  # 'Two Revisions' has two


def test_row_contents(tmp_path):
    out = tmp_path / "x.parquet"
    parse_shard_to_parquet(FIXTURE, "p1p9", DUMP, out)
    rows = {r["page_id"]: r for r in pq.read_table(out).to_pylist()}

    assert all(r["namespace"] == 0 for r in rows.values())
    assert set(rows) == {101, 102, 104, 105, 106}

    # redirect
    assert rows[102]["is_redirect"] is True
    assert rows[102]["redirect_target"] == "Dog"
    # anonymous editor: IP lands in contributor_name, id is null
    assert rows[102]["contributor_name"] == "192.0.2.7"
    assert rows[102]["contributor_id"] is None

    # non-redirect has no target
    assert rows[101]["is_redirect"] is False
    assert rows[101]["redirect_target"] is None

    # XML entity is unescaped by the parser
    assert rows[104]["title"] == "Salt & Pepper"
    # bytes="0" must be 0, not None — the falsy-string trap
    assert rows[104]["text_bytes"] == 0
    assert rows[104]["wikitext"] == ""

    # page with no <text> element survives with nulls
    assert rows[105]["wikitext"] is None
    assert rows[105]["text_bytes"] is None

    # only the FIRST revision is kept
    assert rows[106]["revision_id"] == 9006
    assert rows[106]["wikitext"] == "aaa"


def test_limit_stops_early(tmp_path):
    stats = parse_shard_to_parquet(FIXTURE, "p1p9", DUMP, tmp_path / "x.parquet", limit=2)
    assert stats["pages_written"] == 2
```

Create `tests/test_shards.py`:

```python
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
```

Run them:

```powershell
.\tasks.ps1 test
```

### Verify

```
........                                                          [100%]
8 passed in 0.4s
```

Under a second. **This is the loop you will live in for the rest of the project.** Every time you change the parser, this tells you in less time than it takes to switch windows.

> I ran the parsing logic against exactly this fixture before writing it down — all six edge cases behave as the tests assert. The one that surprises people is `bytes="0"`: the naive `int(b) if b else None` works only because `"0"` is a *non-empty string* and therefore truthy. If you ever refactor that to check the integer, you'll silently turn every zero-length article into a NULL.

### If it breaks

- **`ModuleNotFoundError: wikigraph`** — venv not active, or `pip install -e .` not run.
- **`FileNotFoundError: tests/fixtures/mini_dump.xml`** — pytest resolves relative paths from the working directory. Run from the repo root.
- **Test finds 0 pages** — the XML namespace in your fixture must exactly match `MW_NS` in `config.py`, including `export-0.11`. Confirm against a real shard: `Get-Content $raw\<shard>.xml -TotalCount 3`.

Commit:

```powershell
git add src/ tests/
git commit -m "XML parser with flat-memory streaming, plus edge-case tests"
```

---

## Step 8 — Run the parser on a real shard

### Why

Tests prove correctness on six pages. This proves it on a few million, and produces the numbers you need to size everything else: how long a shard takes, how much Parquet it produces, what peak memory looks like, and — importantly — the **real redirect share**, which your design doc flags as an open question worth ~20 GB of storage estimate.

Measuring before scaling is the actual skill here. Every number you write down now is one you don't have to guess at in Step 12 when you're setting timeouts and pool sizes.

### Do

**8a. Smoke test — 50,000 pages first.** Never start with the full run.

```powershell
python -c @"
import datetime, time
from wikigraph.parse import parse_shard_to_parquet
t0 = time.time()
s = parse_shard_to_parquet(
    r'raw_data\enwiki-2026-07-01-p10p1400054.xml',
    'p10p1400054', datetime.date(2026,7,1),
    r'staging\p10p1400054.smoke.parquet', limit=50_000)
print(s)
print(f'{time.time()-t0:.1f}s')
"@
```

> Adjust the input path if your shards are nested one directory deep.

Sanity-check the output before spending an hour on the full run:

```powershell
python -c @"
import pyarrow.parquet as pq
t = pq.read_table(r'staging\p10p1400054.smoke.parquet')
print(t.num_rows, 'rows,', t.schema.names)
print(t.slice(0,3).to_pylist()[0]['title'])
"@
```

**8b. The full shard, timed and memory-profiled.**

```powershell
python -c @"
import datetime, time, os
from wikigraph.parse import parse_shard_to_parquet
t0 = time.time()
s = parse_shard_to_parquet(
    r'raw_data\enwiki-2026-07-01-p10p1400054.xml',
    'p10p1400054', datetime.date(2026,7,1),
    r'staging\p10p1400054.parquet')
el = time.time()-t0
print(s)
print(f'elapsed {el/60:.1f} min | {s[\"pages_seen\"]/el:,.0f} pages/s')
"@
```

While it runs, open Task Manager → Details → find `python.exe` and watch the **Working Set** column. It should climb to a plateau in the first minute and then stay there. If it climbs steadily for the whole run, your cleanup idiom isn't working — kill it and re-check `iter_pages`.

**8c. Answer the open questions from the design doc.**

```powershell
python -c @"
import pyarrow.parquet as pq, pyarrow.compute as pc
t = pq.read_table(r'staging\p10p1400054.parquet', columns=['is_redirect','text_bytes'])
n = t.num_rows
red = pc.sum(t['is_redirect']).as_py()
print(f'ns=0 pages : {n:,}')
print(f'redirects  : {red:,}  ({red/n:.1%})')
tb = t['text_bytes'].drop_null()
print(f'text_bytes : mean {pc.mean(tb).as_py():,.0f}  max {pc.max(tb).as_py():,}')
"@
```

**8d. Write it all down.** Append to `NOTES.md`:

```markdown
## Shard p10p1400054 parse (Step 8)
- Input XML size: ____ GB
- Wall time: ____ min
- Throughput: ____ pages/s, ____ MB/s of XML
- Peak RSS: ____ MB          <- if this grew all run, the cleanup is broken
- pages_seen: ____
- pages_written (ns=0): ____
- multi_revision_pages: ____   <- MUST be 0
- Parquet out: ____ MB  (compression ratio ____ x)
- Redirect share: ____%
  Design doc guessed 26.9% (biased head sample) vs 61% (published estimate).
  Measured answer: ____
- Extrapolated to 19 shards: ____ hours, ____ GB Parquet
```

### Verify

Three things must be true before you continue:

1. **`multi_revision_pages == 0`.** If it isn't, stop. Your entire data model assumes one row per page. Investigate before building anything on top of it.
2. **Peak memory plateaued.** Flat memory is the property that makes the remaining 18 shards a scheduling problem rather than a rewrite.
3. **`pages_written` is a plausible fraction of `pages_seen`.** Your EDA said ~88.5% of pages are ns=0. If you're getting 5% or 99%, your namespace filter is wrong.

Compare your MB/s against the disk number from Step 6. If parse throughput is well below read throughput, you're CPU-bound and the Step 6 decision stands: leave the data on `C:\`.

### If it breaks

- **Memory climbs the whole run** — the sibling-deletion `while` loop is missing or misplaced. It must be *inside* the `for` loop, after `elem.clear()`.
- **`XMLSyntaxError: Extra content at the end of the document`** — the shard is truncated (incomplete download/decompress). Check its size against the `.bz2` and re-decompress.
- **Takes dramatically longer than the smoke test extrapolates** — you're probably swapping. Check Working Set against physical RAM.
- **`OSError: [Errno 28] No space left on device`** — `staging/` filled your drive. Parquet for one shard should be low single-digit GB; check Step 3's disk numbers.

Commit `NOTES.md` (but not the Parquet — it's gitignored):

```powershell
git add NOTES.md
git commit -m "Measured facts: shard 0 parse throughput and redirect share"
```

---

## Step 9 — The loader: Parquet → Postgres

### Why

**Why binary COPY and not `INSERT` or pandas `to_sql()`.** Row-by-row `INSERT` at this volume is 100× slower — each statement is a round trip with its own parse and plan. `to_sql()` is `INSERT` in a trench coat, and it materializes the whole frame in memory first. `COPY` is Postgres's bulk-load protocol and it's the only thing that's fast enough.

**Why `FORMAT BINARY` specifically.** Text-format `COPY` requires escaping delimiters, quotes, newlines, and backslashes on the way out, and trusting Postgres to unescape them identically. Wikitext contains *every one* of those characters, constantly, including in adversarial combinations. This produces failures on maybe 0.1% of rows — which at 900k rows per shard is ~900 mystery errors, deep into a load, with unhelpful messages. Binary format sidesteps escaping entirely and is faster besides.

**Why the load is a separate step from the parse.** They fail for different reasons (CPU/XML corruption vs. disk/locks/permissions) and they have different retry costs. Splitting them means a failed load retries in minutes instead of re-parsing for an hour.

**Three production habits embedded in the code below**, worth naming because they're the ones people skip:

1. **Idempotency by partition replacement** (Rule 2). `TRUNCATE` then `COPY`, inside one transaction. Rerun produces the identical end state.
2. **Streaming always.** `iter_batches` holds 2,000 rows at a time. `pq.read_table()` would hold the entire shard — several GB — in memory.
3. **`ANALYZE` immediately after loading.** Postgres's autovacuum won't get to a freshly-loaded 900k-row partition fast enough, and the planner will use stale statistics that say the table is empty. That turns a 10-second `stg` transform into a 40-minute nested loop. This one line has probably saved more people more hours than any other in this runbook.

### Do

Create `src/wikigraph/load.py`:

```python
"""Load parsed Parquet into raw.page via binary COPY. Idempotent per shard."""
from __future__ import annotations

import re
from pathlib import Path

import psycopg
import pyarrow.parquet as pq
from psycopg import sql

# Column order here must match PG_TYPES below and the COPY statement.
COLUMNS = [
    "page_id", "dump_date", "shard_name", "title", "namespace",
    "is_redirect", "redirect_target", "revision_id", "revision_ts",
    "contributor_name", "contributor_id", "text_bytes", "wikitext",
]

# Binary COPY requires explicit types — the wire format carries no type info,
# so both ends must agree exactly. A mismatch here produces corrupt data or a
# cryptic "insufficient data left in message", not a friendly error.
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
    batch_size: int = 2_000,
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
                ).format(part=sql.Identifier(part), val=sql.Literal(shard_name))
            )

            # 2. Idempotency: replace, never append.
            cur.execute(sql.SQL("TRUNCATE raw.{}").format(sql.Identifier(part)))

            # 3. Stream Parquet straight into COPY. Copying into the PARTITION
            #    rather than the parent table skips tuple routing overhead.
            copy_stmt = sql.SQL(
                "COPY raw.{} ({}) FROM STDIN (FORMAT BINARY)"
            ).format(
                sql.Identifier(part),
                sql.SQL(", ").join(map(sql.Identifier, COLUMNS)),
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
        #    ANALYZE cannot run inside the load transaction, hence the separate block.
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("ANALYZE raw.{}").format(sql.Identifier(part)))

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
```

Run it against your parsed shard. From the repo root with the venv active:

```powershell
# Build the DSN from .env rather than pasting a password into your shell history.
Get-Content .env | Where-Object { $_ -match '^\w+=' } | ForEach-Object {
    $k,$v = $_.Split('=',2); Set-Item -Path "env:$($k.Trim())" -Value $v.Trim()
}
$env:WH_DSN = "postgresql://$($env:PG_ETL_USER):$($env:PG_ETL_PASSWORD)@localhost:$($env:PG_HOST_PORT)/$($env:PG_DB)"

python -c @"
import os, time
from wikigraph.load import load_parquet
t0 = time.time()
r = load_parquet(os.environ['WH_DSN'], r'staging\p10p1400054.parquet', 'p10p1400054')
print(r, f'{time.time()-t0:.1f}s')
"@
```

### Verify

**1. Row count matches the parse exactly.**

```powershell
docker exec -it wikigraph-warehouse psql -U etl -d wikigraph -c @"
SELECT count(*)                                     AS rows,
       count(*) FILTER (WHERE is_redirect)          AS redirects,
       min(page_id), max(page_id),
       pg_size_pretty(pg_total_relation_size('raw.page_p10p1400054')) AS size
FROM raw.page WHERE shard_name = 'p10p1400054';
"@
```

`rows` must equal `pages_written` from Step 8. Not approximately — exactly.

**2. Idempotency proof.** Run the loader a second time, then re-run the count query. **The number must be identical.** If it doubled, your `TRUNCATE` isn't firing and Rule 2 is broken — fix it now, because every layer above this will inherit the duplicates.

**3. Partition routing works.**

```powershell
docker exec -it wikigraph-warehouse psql -U etl -d wikigraph -c "\d+ raw.page"
```

Should list `raw.page_p10p1400054` under "Partitions".

**4. Spot-check real data.**

```powershell
docker exec -it wikigraph-warehouse psql -U etl -d wikigraph -c @"
SELECT page_id, title, is_redirect, redirect_target, length(wikitext) AS chars
FROM raw.page WHERE shard_name='p10p1400054' AND NOT is_redirect
ORDER BY length(wikitext) DESC NULLS LAST LIMIT 5;
"@
```

You should see recognizable Wikipedia article titles. If titles look like `&amp;lt;` or are mangled, the encoding path is wrong.

Record the load time and on-disk size in `NOTES.md`. The ratio of Postgres size to Parquet size is your TOAST compression factor, and it's what the design doc's 120 GB budget hinges on.

### If it breaks

- **`insufficient data left in message`** — `PG_TYPES` and `COLUMNS` are out of sync with the actual table, or in the wrong order. All three lists must correspond position by position.
- **`invalid byte sequence for encoding "UTF8": 0x00`** — the NUL-stripping line in `parse.py` is missing. Fix it there and re-parse; you cannot fix it at load time.
- **`permission denied for table page`** — you're connected as the wrong role. `raw` is owned by `etl`.
- **`relation "raw.page" does not exist`** — migrations didn't run, or you're connected to the wrong database. `\dt raw.*` to check.
- **Load is much slower than expected** — check for an index on `raw.page`. There shouldn't be one; indexes during bulk load cost ~5×. Indexes come after, in Step 14.

Commit:

```powershell
git add src/wikigraph/load.py NOTES.md
git commit -m "Binary COPY loader with partition-replacement idempotency"
```

---

## Step 10 — Custom Airflow image + warehouse wiring

### Why

You now have working code. Everything from here is orchestration.

**Why build a custom image instead of `_PIP_ADDITIONAL_REQUIREMENTS`.** The stock Airflow compose file offers an environment variable that pip-installs packages at container start. It's explicitly documented as dev-only, and for good reason: it re-downloads on every restart (slow), it silently picks up new versions (so your pipeline breaks on a day you changed nothing), and if PyPI is unreachable your whole stack fails to boot. Building an image once is a five-minute change that makes your environment reproducible.

**Why pin exact versions.** Beyond the general argument, there's a live trap right now: `pip install dbt-postgres` unpinned can resolve to a **dbt-core 2.0 alpha**, which then fails with *"The 'postgres' adapter is not yet supported by dbt Fusion."* dbt Labs is mid-transition to the Fusion engine, and Postgres isn't a supported Fusion adapter yet. This has been reported specifically on Windows. Pin with upper bounds and you never see it.

**Why bind-mount `src/` instead of baking it into the image.** During development you want to edit `parse.py` on Windows and have the next task run pick it up — no rebuild, no restart. The production move is the opposite (`pip install .` into the image, so code is versioned with the image), and it's worth noting that tradeoff in your README. For now, iteration speed wins.

### Do

**10a. The image.** Create `airflow-docker/docker/` and put two files in it.

`airflow-docker/docker/requirements.txt`:

```
# --- parsing / loading (mirror of pyproject.toml) ---
lxml==5.3.0
pyarrow==18.1.0
psycopg[binary]==3.2.3
mwparserfromhell==0.7.2

# --- Airflow providers ---
apache-airflow-providers-postgres==6.1.0
apache-airflow-providers-common-sql==1.21.0

# --- dbt (Steps 13-15) ---
# UPPER BOUNDS ARE LOAD-BEARING: unpinned, pip can pull a dbt-core 2.0 alpha,
# which fails with "the 'postgres' adapter is not yet supported by dbt Fusion".
dbt-core>=1.10,<2.0
dbt-postgres>=1.9,<2.0
astronomer-cosmos>=1.11,<2.0
```

> **Before you build**, confirm these still resolve — versions move. `pip index versions dbt-core` and `pip index versions apache-airflow-providers-postgres` will tell you. Adjust the exact pins; keep the `<2.0` bounds.

`airflow-docker/docker/Dockerfile`:

```dockerfile
# Match the Airflow version your existing compose file uses.
# Check with: docker compose exec airflow-scheduler airflow version
ARG AIRFLOW_BASE=apache/airflow:3.3.0
FROM ${AIRFLOW_BASE}

# lxml needs these headers if pip has to build from source.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
```

**First, check your Airflow version** so the `ARG` matches:

```powershell
cd airflow-docker
docker compose exec airflow-scheduler airflow version
cd ..
```

Airflow 3.3.0 is current (released July 2026). If you're on 2.x, note it — the DAG code in Step 12 uses Airflow 3 imports (`from airflow.sdk import dag, task`) and you'd substitute `from airflow.decorators import dag, task`. Upgrading is worth doing; the 3.x Task SDK is the stable public interface going forward.

Build:

```powershell
.\tasks.ps1 airflow-build
```

This takes a few minutes the first time.

**10b. The compose override.** Docker Compose automatically merges a file named `docker-compose.override.yml` sitting next to `docker-compose.yaml`. This means you can add your customizations **without editing the stock Airflow file** — which matters, because you'll want to pull upstream updates to it later.

Create `airflow-docker/docker-compose.override.yml`:

```yaml
# Merged automatically with docker-compose.yaml.
# Paths are relative to THIS file's directory (airflow-docker/).

x-wikigraph-extra: &wikigraph-extra
  volumes:
    # Source shards: READ-ONLY. A pipeline must not be able to corrupt its input.
    - ${WIKIGRAPH_RAW_DATA}:/opt/airflow/raw_data:ro
    # Your package, bind-mounted so edits on Windows take effect immediately.
    - ../src:/opt/airflow/src
    # dbt project (Step 13).
    - ../dbt:/opt/airflow/dbt
    # Parquet scratch: a NAMED VOLUME, not a bind mount. This is the hot write
    # path, and named volumes live in the VM's native filesystem — much faster
    # than writing back through the Windows bridge.
    - wikigraph_staging:/opt/airflow/staging
  environment:
    PYTHONPATH: /opt/airflow/src
    WIKIGRAPH_RAW_DIR: /opt/airflow/raw_data
    WIKIGRAPH_STAGING_DIR: /opt/airflow/staging
    WIKIGRAPH_DUMP_DATE: ${WIKIGRAPH_DUMP_DATE}
    # Airflow reads AIRFLOW_CONN_<ID> and registers a connection named
    # 'wikigraph_warehouse' automatically — no clicking in the UI, nothing
    # stored in the metadata DB. This is how real deployments inject secrets.
    AIRFLOW_CONN_WIKIGRAPH_WAREHOUSE: ${AIRFLOW_CONN_WIKIGRAPH_WAREHOUSE}
    PG_HOST: warehouse
    PG_DB: ${PG_DB}
    PG_DBT_USER: ${PG_DBT_USER}
    PG_DBT_PASSWORD: ${PG_DBT_PASSWORD}
  networks:
    - default          # Airflow's own network, for its metadata DB
    - wikigraph-net    # the shared network, so 'warehouse' resolves

services:
  # DELETE any service name your compose file doesn't have — an override for a
  # nonexistent service is an error.
  airflow-scheduler:     { <<: *wikigraph-extra }
  airflow-dag-processor: { <<: *wikigraph-extra }
  airflow-worker:        { <<: *wikigraph-extra }
  airflow-triggerer:     { <<: *wikigraph-extra }
  airflow-apiserver:     { <<: *wikigraph-extra }
  airflow-init:          { <<: *wikigraph-extra }

volumes:
  wikigraph_staging:

networks:
  wikigraph-net:
    external: true
```

**Check which services your compose file actually defines** and delete the rest:

```powershell
cd airflow-docker; docker compose --env-file ..\.env config --services; cd ..
```

Airflow 2.x has `airflow-webserver` instead of `airflow-apiserver` and no `airflow-dag-processor`.

Bring it up:

```powershell
.\tasks.ps1 airflow-up
```

The first start with a new image recreates every container — give it a minute.

### Verify

**1. The image is in use:**

```powershell
cd airflow-docker; docker compose --env-file ..\.env images | Select-String wikigraph; cd ..
```

**2. Your package imports inside the container:**

```powershell
cd airflow-docker; docker compose --env-file ..\.env exec airflow-scheduler python -c "import wikigraph.parse, lxml.etree, pyarrow, psycopg; print('imports ok')"; cd ..
```

**3. Shards are visible from inside Airflow:**

```powershell
cd airflow-docker; docker compose --env-file ..\.env exec airflow-scheduler ls /opt/airflow/raw_data; cd ..
```

**4. The warehouse is reachable by hostname** — this is the network wiring, and it's the most likely thing to be wrong:

```powershell
cd airflow-docker; docker compose --env-file ..\.env exec airflow-scheduler python -c "import os, psycopg; dsn=os.environ['AIRFLOW_CONN_WIKIGRAPH_WAREHOUSE'].replace('postgres://','postgresql://',1); c=psycopg.connect(dsn); print(c.execute('select current_user, current_database(), count(*) from raw.page').fetchone())"; cd ..
```

Expected: `('etl', 'wikigraph', 913452)` — your actual row count from Step 9. Seeing your own data from inside Airflow means all the wiring is correct.

**5. Airflow registered the connection:**

```powershell
.\tasks.ps1 af connections get wikigraph_warehouse
```

### If it breaks

- **`could not translate host name "warehouse"`** — the containers aren't on `wikigraph-net`. Check `docker network inspect wikigraph-net` lists both the warehouse and the Airflow containers. Most common cause: the `networks:` block in the override didn't apply because the service name doesn't exist in your compose file.
- **`ModuleNotFoundError: No module named 'wikigraph'`** inside the container — `PYTHONPATH` isn't set, or the `../src` mount didn't apply. `exec airflow-scheduler ls /opt/airflow/src` should show `wikigraph/`.
- **`service "airflow-apiserver" has no container`** — that service doesn't exist in your compose version. Delete that line from the override.
- **Password authentication failed for user "etl"** — special characters in the password broke URI parsing. Change it to alphanumerics (Step 1's warning).
- **Containers restart in a loop after the image swap** — check `AIRFLOW_IMAGE_NAME` in `.env` matches the tag you built, and that the base version in the Dockerfile matches your compose file's expectations.

Commit:

```powershell
git add airflow-docker/docker/ airflow-docker/docker-compose.override.yml
git commit -m "Custom Airflow image and warehouse wiring"
```

---

## Step 11 — Pools and a smoke DAG

### Why

**Pools** are Airflow's concurrency limiter — a named semaphore. Without one, triggering the ingest DAG would launch 19 simultaneous XML parses, each wanting a CPU core and hundreds of MB, and your laptop would become unusable while Airflow's own scheduler starves and starts marking healthy tasks as zombies. A pool of 3 says "at most three of these run at once, the rest queue."

This is genuinely how production Airflow manages shared resources — pools around database connections, around API rate limits, around anything finite.

**The smoke DAG** exists to separate "is my platform wired correctly" from "is my pipeline logic correct." When the real DAG fails in Step 12, you want to already know that connections, imports, and mounts work — so the failure must be in your logic. Debugging both at once is much harder.

### Do

**11a. Create the pools.** `tasks.ps1 af` passes anything through to the Airflow CLI inside the scheduler container:

```powershell
.\tasks.ps1 af pools set shard_parse 3 "Parallel shard XML parses"
.\tasks.ps1 af pools set warehouse_load 2 "Concurrent COPY streams into the warehouse"
```

Sizing rationale — adjust to the CPU count you noted in Step 3:

- **`shard_parse = 3`** — parsing is CPU-bound and single-threaded per task. Three leaves headroom for the scheduler and your desktop. On an 8-core machine you could try 4–5; measure before raising it.
- **`warehouse_load = 2`** — one Postgres on one SSD. More concurrent `COPY` streams mostly cause WAL contention rather than throughput.

**11b. The smoke DAG.** Create `airflow-docker/dags/wikigraph_smoke.py`:

```python
"""Platform smoke test. Proves connections, mounts, and imports work.

Deliberately contains NO pipeline logic — if this is green and the real DAG
is red, the problem is your code, not your infrastructure.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="wikigraph_smoke",
    description="Verify warehouse connectivity and shard visibility",
    schedule=None,               # manual trigger only
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,               # don't backfill from start_date
    tags=["wikigraph", "smoke"],
)
def wikigraph_smoke():

    @task
    def check_warehouse() -> str:
        # Imports go INSIDE task functions. Module-level imports run on every
        # DAG parse — every ~30 seconds, in the scheduler process. Keep the top
        # of a DAG file cheap.
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="wikigraph_warehouse")
        version = hook.get_first("SELECT version()")[0]
        rows = hook.get_first("SELECT count(*) FROM raw.page")[0]
        print(f"warehouse: {version}")
        print(f"raw.page rows: {rows:,}")
        return version

    @task
    def check_shards() -> int:
        from wikigraph.shards import discover

        shards = discover()
        for s in shards:
            print(f"{s.name:>16}  {s.size_bytes / 1e9:6.1f} GB  {s.path}")
        return len(shards)

    @task
    def check_staging_writable() -> str:
        from pathlib import Path

        from wikigraph.config import STAGING_DIR

        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        probe = Path(STAGING_DIR) / ".write_probe"
        probe.write_text("ok")
        probe.unlink()
        return f"{STAGING_DIR} is writable"

    check_warehouse()
    check_shards()
    check_staging_writable()


wikigraph_smoke()
```

**11c. Trigger it.** Open the Airflow UI at <http://localhost:8080> (default login `airflow` / `airflow` unless your compose file changed it). Find `wikigraph_smoke`, unpause it with the toggle, and press the ▶ trigger button.

Or from PowerShell:

```powershell
.\tasks.ps1 af dags trigger wikigraph_smoke
```

### Verify

All three tasks green. Click each one → **Logs** and confirm:

- `check_warehouse` prints a PostgreSQL 17 version string **and your row count from Step 9**
- `check_shards` lists all 19 shards with sizes
- `check_staging_writable` returns the staging path

Confirm the pools registered:

```powershell
.\tasks.ps1 af pools list
```

### If it breaks

- **DAG doesn't appear in the UI** — check for import errors: `.\tasks.ps1 af dags list-import-errors`. This is the single most useful Airflow command and you'll use it constantly.
- **`The conn_id 'wikigraph_warehouse' isn't defined`** — the `AIRFLOW_CONN_*` variable didn't reach the container. Verify with `exec airflow-scheduler printenv | Select-String AIRFLOW_CONN`.
- **Task stays "queued" forever** — no worker is running, or the pool has no slots. `docker ps` should show `airflow-worker` healthy.
- **`FileNotFoundError: no shard files matched`** — the raw_data mount is wrong, or the files are nested deeper than one level. `exec airflow-scheduler find /opt/airflow/raw_data -name "*.xml" | head`.

Commit:

```powershell
git add airflow-docker/dags/wikigraph_smoke.py
git commit -m "Smoke DAG: platform connectivity check"
```

---

## Step 12 — The ingest DAG

### Why

Now you wrap working, tested code in orchestration. Note the ordering: the parser and loader were both proven standalone before Airflow ever touched them. That's Rule 1 paying off.

**Airflow concepts you need here, and no more:**

| Concept | What it actually is |
|---|---|
| **DAG** | A Python file declaring tasks and their order. Re-parsed every ~30s, so it must import fast. |
| **Task** | One unit of work. Retried independently. Has its own logs. |
| **`@task`** | The TaskFlow decorator. Wraps a plain Python function. This is all you need. |
| **XCom** | Return values passed between tasks, stored in the metadata DB. **Keep them small** — paths and counts, never DataFrames. |
| **Connection** | A named credential looked up by `conn_id` instead of hardcoded. |
| **Pool** | The semaphore from Step 11. |
| **Dynamic task mapping** | `.expand()` — one task definition becomes N runtime instances. This is how you fan out over shards. |
| **Params** | Runtime arguments you pass when triggering. Lets you run one shard without editing code. |

**The single most important idea in this DAG is that the assertions raise.** A task that silently loads zero rows is a bug you discover four transformations later, with no idea where it started. Every check below fails at the earliest point the problem is detectable:

- `discover_shards` raises if the filter matched nothing
- `parse_shard` raises if `multi_revision_pages > 0` (your data model is invalid) or if zero pages were written
- `load_shard` raises if the loaded row count ≠ the parsed row count

That last one is **reconciliation between stages**, and it's what people mean by a data quality gate — not just "no exception was thrown," but "the number that came out matches the number that went in."

### Do

Create `airflow-docker/dags/wikigraph_ingest.py`:

```python
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
    """psycopg wants the 'postgresql://' scheme; Airflow connection URIs use
    'postgres://'. One place to do the translation."""
    return os.environ["AIRFLOW_CONN_WIKIGRAPH_WAREHOUSE"].replace(
        "postgres://", "postgresql://", 1
    )


@dag(
    dag_id="wikigraph_ingest",
    description="Parse MediaWiki XML shards to Parquet and COPY into raw.page",
    schedule=None,                # manual until a second dump exists
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,            # never let two ingest runs race on a partition
    default_args=DEFAULT_ARGS,
    tags=["wikigraph", "ingest"],
    params={
        "shards": Param(
            [], type="array",
            description="Shard names to process, e.g. ['p10p1400054']. Empty = all.",
        ),
        "dump_date": Param("2026-07-01", type="string", format="date"),
    },
)
def wikigraph_ingest():

    @task
    def discover_shards(**context) -> list[dict]:
        from wikigraph.shards import discover

        wanted = set(context["params"]["shards"] or [])
        shards = [s.as_dict() for s in discover() if not wanted or s.name in wanted]
        if not shards:
            raise ValueError(
                f"no shards matched {wanted or '(all)'} — check WIKIGRAPH_RAW_DIR"
            )
        print(f"selected {len(shards)} shard(s): {[s['name'] for s in shards]}")
        return shards

    @task(
        pool="shard_parse",
        execution_timeout=pendulum.duration(hours=2),
        # Makes the UI show 'p10p1400054' instead of 'parse_shard[3]'.
        # Small quality-of-life win you will be very glad of at 2am.
        map_index_template="{{ task.op_kwargs['shard']['name'] }}",
    )
    def parse_shard(shard: dict) -> dict:
        from pathlib import Path

        from wikigraph.config import STAGING_DIR
        from wikigraph.load import upsert_manifest
        from wikigraph.parse import parse_shard_to_parquet

        dsn = _warehouse_dsn()
        dump_date = dt.date.fromisoformat(shard["dump_date"])

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
                "multiple <revision> blocks — the one-row-per-page grain is invalid"
            )

        # --- GATE 2: a silent zero is worse than a crash. ---
        if stats["pages_written"] == 0:
            upsert_manifest(dsn, shard["name"], dump_date, status="failed",
                            error_detail="zero ns=0 pages written")
            raise ValueError(f"{shard['name']}: zero ns=0 pages written")

        upsert_manifest(
            dsn, shard["name"], dump_date,
            status="parsed",
            pages_seen=stats["pages_seen"],
            pages_loaded=stats["pages_written"],
            parse_ended=pendulum.now("UTC"),
        )
        # Only a small dict crosses the task boundary — this goes through XCom.
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
                            error_detail="row count mismatch parse vs load")
            raise ValueError(
                f"{stats['shard_name']}: parsed {stats['pages_written']:,} rows "
                f"but loaded {result['rows_loaded']:,}"
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
    parsed = parse_shard.expand(shard=shards)     # fan out
    loaded = load_shard.expand(stats=parsed)      # fan out, 1:1 with parse
    summarize(loaded)                             # fan in


wikigraph_ingest()
```

**Trigger it with one shard.** In the UI: **Trigger DAG w/ config**, and paste:

```json
{"shards": ["p10p1400054"], "dump_date": "2026-07-01"}
```

From PowerShell:

```powershell
.\tasks.ps1 af dags trigger wikigraph_ingest --conf '{\"shards\": [\"p10p1400054\"]}'
```

Use this params mechanism constantly during development — it's how you iterate on one shard without editing code or waiting on 19.

### Verify

**1. One-shard run is green** and `summarize` logs your expected row count.

**2. The manifest is populated** — this is the "I can prove what's in the warehouse" part:

```powershell
docker exec -it wikigraph-warehouse psql -U etl -d wikigraph -c @"
SELECT shard_name, status, pages_seen, pages_loaded,
       round(extract(epoch from (parse_ended - parse_started))/60, 1) AS parse_min
FROM raw.ingest_manifest ORDER BY shard_name;
"@
```

**3. Rerunning is safe.** Clear the `load_shard` task in the UI (select it → **Clear**) and let it re-run. The row count in `raw.page` must not change. That's Rule 2, verified through the orchestrator rather than just in isolation.

**4. Fan-out works at 19.** Trigger with `{}` (empty config) and confirm the graph view shows 19 mapped instances of `parse_shard`, with 3 running and 16 queued (the pool doing its job). **You can stop the run** — you're only confirming the fan-out and the pool, not backfilling yet. Mark the DAG run failed or clear it.

**5. The gates actually fire.** Temporarily change Gate 2 to `if stats["pages_written"] >= 0:` and re-trigger. The task should fail with your error message. Change it back. Testing that your alarms work is worth five minutes — an assertion you've never seen fire is an assertion you don't know is wired up.

### If it breaks

- **DAG has an import error** — `.\tasks.ps1 af dags list-import-errors`. Usually a typo or a module-level import that doesn't resolve in the container.
- **`parse_shard` fails with `FileNotFoundError`** on a path that exists on Windows — you're passing a host path into a container. `discover()` runs inside the container and returns container paths; make sure `WIKIGRAPH_RAW_DIR` is set (Step 10's override), not the Windows default from `config.py`.
- **Tasks stuck "queued"** — pool exhausted by a previous stuck run, or no worker. `.\tasks.ps1 af pools list` shows used vs. free slots.
- **XCom serialization error** — something in a return value isn't JSON-serializable. `dt.date` is a common culprit; note `parse_shard_to_parquet` returns `str(dump_date)` for exactly this reason.
- **`load_shard` fails on `dump_date`** — `stats["dump_date"]` is a string; `dt.date.fromisoformat` handles it. If you changed the parser's return type, this breaks.
- **Run succeeds but zero mapped instances** — `discover_shards` returned an empty list and something swallowed the raise. Check its logs.

Commit:

```powershell
git add airflow-docker/dags/wikigraph_ingest.py
git commit -m "Ingest DAG: dynamic mapping over shards with quality gates"
```

**You now have a working data pipeline.** Everything from here is transformation.

---

## Step 13 — dbt: what it is and project setup

### Why

**What dbt actually is, without the marketing.** dbt is a SQL compiler and runner. You write `SELECT` statements in `.sql` files. dbt wraps each one in `CREATE TABLE AS` or `CREATE VIEW AS`, figures out the dependency order by reading the `ref()` calls between your files, runs them in that order, and then runs the tests you declared. That's it. It does not extract. It does not load. It is the **T** in ELT.

Given you could write the same SQL and run it with `SQLExecuteQueryOperator`, why bother? Three things you'd otherwise build yourself and build worse:

1. **Dependency ordering for free.** You never write "run stg_page before dim_article." dbt reads `ref('stg_page')` inside `dim_article.sql` and infers the graph. Add a model in the middle and nothing else changes.
2. **Tests as first-class objects.** `unique`, `not_null`, `relationships`, and `accepted_values` are one line of YAML each. On this project the `unique` test on `norm_title` is doing serious work — if your normalization is wrong, two titles collapse to one key, and your link joins silently multiply rows. The test catches it loudly instead.
3. **Lineage documentation that can't go stale.** `dbt docs generate` produces a browsable dependency graph from the actual code. It's free, and it's a genuinely good portfolio artifact.

**Why `profiles.yml` reads from environment variables.** Credentials in a file that gets committed is the single most common way secrets leak. `env_var()` keeps them in `.env`, which is gitignored, and it's what you'd do in production anyway.

### Do

**13a. Project skeleton.**

```powershell
New-Item -ItemType Directory -Force -Path `
  dbt\wikigraph\models\staging, `
  dbt\wikigraph\models\marts, `
  dbt\wikigraph\macros
```

`dbt/wikigraph/dbt_project.yml`:

```yaml
name: 'wikigraph'
version: '0.1.0'
config-version: 2
profile: 'wikigraph'

model-paths: ["models"]
macro-paths: ["macros"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  wikigraph:
    staging:
      +schema: stg
      +materialized: table
    marts:
      +schema: mart
      +materialized: table
```

> **A dbt gotcha worth knowing now:** by default dbt *concatenates* the target schema and the model schema, so `schema: stg` in your profile plus `+schema: stg` here would produce `stg_stg`. The `generate_schema_name` macro below overrides that to use the model's schema verbatim, which is what you want.

`dbt/wikigraph/macros/generate_schema_name.sql`:

```sql
{#-
  Override dbt's default schema naming.
  Default behaviour: <profile_schema>_<model_schema>  (e.g. "stg_mart")
  What we want:      <model_schema>                   (e.g. "mart")
  This is the single most common source of "why is my table in the wrong schema".
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

`dbt/wikigraph/profiles.yml`:

```yaml
wikigraph:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('PG_HOST', 'warehouse') }}"
      port: 5432
      user: "{{ env_var('PG_DBT_USER') }}"
      password: "{{ env_var('PG_DBT_PASSWORD') }}"
      dbname: "{{ env_var('PG_DB', 'wikigraph') }}"
      schema: stg
      threads: 4
      keepalives_idle: 0
```

**13b. Declare the source.** This is how dbt knows `raw.page` exists without managing it. `dbt/wikigraph/models/sources.yml`:

```yaml
version: 2

sources:
  - name: raw
    description: "Landing tables owned by the ingest pipeline. dbt reads, never writes."
    schema: raw
    tables:
      - name: page
        description: "One row per ns=0 MediaWiki page. Partitioned by shard_name."
        columns:
          - name: page_id
            data_tests: [not_null]
          - name: namespace
            description: "Should always be 0 — the parser filters at parse time."
            data_tests:
              - accepted_values:
                  arguments:
                    values: [0]
          - name: title
            data_tests: [not_null]
      - name: ingest_manifest
        description: "Per-shard ingest bookkeeping."
```

**13c. Verify dbt can connect.**

```powershell
.\tasks.ps1 dbt debug
```

### Verify

`dbt debug` should end with `All checks passed!` and show a successful connection test.

### If it breaks

- **`Env var required but not provided: 'PG_DBT_USER'`** — the variable isn't in the container. Check the `environment:` block in your override and restart Airflow.
- **`Database Error: permission denied for schema raw`** — the grants from V001 didn't apply. Re-run `.\tasks.ps1 migrate` and check `\dn+` output.
- **`Could not find profile named 'wikigraph'`** — `--profiles-dir .` is missing, or `profiles.yml` isn't next to `dbt_project.yml`.
- **`dbt: command not found`** — the pip install in your image didn't include dbt-core, or it's not on `PATH`. Try `bash -lc 'which dbt'`; the usual location is `/home/airflow/.local/bin/dbt`.

Commit:

```powershell
git add dbt/
git commit -m "dbt project skeleton with env-var profile and raw source"
```

---

## Step 14 — Your first dbt models and tests

### Why

This is where `raw` becomes something you can actually query. Two models to start — resist the urge to build all of `mart` at once. Get one clean layer working, with tests, and add on top.

**Note what the models do and don't do.** They filter, they type, they normalize. They do not parse wikitext — that's Python's job (link extraction, a later phase). The dividing line: SQL does set operations, Python does string parsing. Crossing that line in either direction is how transformation layers become unmaintainable.

**The `unique` test on `norm_title` is the point of this step.** If `public.norm_title` has a bug, two distinct titles collapse to the same key. Every subsequent join on that key multiplies rows instead of matching them, and no error is raised — you just get graph statistics that are quietly, confidently wrong. The test turns a silent corruption into a loud failure.

### Do

**14a. `dbt/wikigraph/models/staging/stg_page.sql`:**

```sql
{{ config(materialized='table', schema='stg') }}

-- One row per ns=0 page, INCLUDING redirects.
-- Redirects stay here because they are the alias dictionary (design doc §4.2) —
-- 27% of pages, built by Wikipedia editors over two decades. They are excluded
-- from the graph as NODES later, in dim_article, not here.

select
    page_id,
    title,                                              -- display form
    public.norm_title(title)             as norm_title, -- the join key. Always.

    is_redirect,
    -- Redirect targets can carry a section anchor: 'Dog#Behavior'. Strip it —
    -- the anchor is a position within the target page, not a different page.
    case
        when is_redirect
        then public.norm_title(split_part(redirect_target, '#', 1))
    end                                  as redirect_to_norm,

    revision_id,
    revision_ts,
    text_bytes,
    dump_date,
    shard_name

from {{ source('raw', 'page') }}
where namespace = 0
```

**14b. `dbt/wikigraph/models/staging/schema.yml`** — the tests are the deliverable here:

```yaml
version: 2

models:
  - name: stg_page
    description: "Typed, normalized ns=0 pages including redirects."
    columns:
      - name: page_id
        description: "MediaWiki page ID. Globally unique across all shards."
        data_tests: [unique, not_null]

      - name: norm_title
        description: >
          Canonical title. THE join key for every link resolution.
          The unique test here is load-bearing: a collision means norm_title()
          is wrong, and every downstream join would silently multiply rows.
        data_tests: [unique, not_null]

      - name: is_redirect
        data_tests: [not_null]

      - name: redirect_to_norm
        description: "Normalized redirect target, anchor stripped. NULL for articles."

      - name: dump_date
        data_tests: [not_null]
```

**14c. A first mart model.** `dbt/wikigraph/models/marts/dim_article.sql`:

```sql
{{ config(materialized='table', schema='mart') }}

-- Canonical articles only. Redirects are NOT nodes in the graph — an edge
-- pointing at a redirect gets resolved to its target during link processing,
-- so a redirect never appears as a vertex.
--
-- Degree and pagerank columns are declared but NULL until link extraction
-- exists. Declaring them now keeps the contract stable for consumers.

select
    page_id                              as article_id,
    title,
    norm_title,
    text_bytes,
    revision_ts,

    -- Cheap heuristic classifications. Refine later; they're useful immediately
    -- for excluding noise from graph stats.
    title ilike '%(disambiguation)'      as is_disambig,
    title ilike 'List of %'              as is_list_page,
    coalesce(text_bytes, 0) < 1500       as is_stub,

    cast(null as integer)                as out_degree,
    cast(null as integer)                as in_degree,
    cast(null as double precision)       as pagerank,

    dump_date

from {{ ref('stg_page') }}
where not is_redirect
```

**14d. `dbt/wikigraph/models/marts/schema.yml`:**

```yaml
version: 2

models:
  - name: dim_article
    description: "One row per canonical (non-redirect) ns=0 article."
    columns:
      - name: article_id
        data_tests: [unique, not_null]
      - name: norm_title
        data_tests: [unique, not_null]
      - name: title
        data_tests: [not_null]
```

**14e. Run it.**

```powershell
.\tasks.ps1 dbt build
```

`dbt build` = `dbt run` + `dbt test`, in dependency order. Always prefer it to bare `dbt run` — running models without testing them is how you ship broken data confidently.

### Verify

Expected output shape:

```
1 of 9 START sql table model stg.stg_page ................ [RUN]
1 of 9 OK created sql table model stg.stg_page ........... [SELECT 913452 in 12.4s]
2 of 9 START test not_null_stg_page_page_id .............. [RUN]
...
Completed successfully
Done. PASS=9 WARN=0 ERROR=0 SKIP=0 TOTAL=9
```

Then query your data as a human would:

```powershell
docker exec -it wikigraph-warehouse psql -U dbt -d wikigraph -c @"
SELECT count(*) AS articles FROM mart.dim_article;
SELECT article_id, title, text_bytes FROM mart.dim_article
ORDER BY text_bytes DESC NULLS LAST LIMIT 10;
"@
```

**You should see the ten longest Wikipedia articles in shard 0, by name.** That's the milestone — raw XML on disk has become queryable analytics tables.

Now answer the design doc's open questions with real numbers:

```powershell
docker exec -it wikigraph-warehouse psql -U dbt -d wikigraph -c @"
SELECT count(*)                                                      AS ns0_pages,
       count(*) FILTER (WHERE is_redirect)                           AS redirects,
       round(100.0 * count(*) FILTER (WHERE is_redirect) / count(*), 1) AS redirect_pct
FROM stg.stg_page;

-- How many redirects point at a target that doesn't exist in this shard?
-- Expect a lot: targets legitimately live in other shards. This number should
-- drop toward zero after backfill, which makes it a good backfill progress metric.
SELECT count(*) AS dangling_redirects
FROM stg.stg_page s
WHERE s.is_redirect
  AND NOT EXISTS (SELECT 1 FROM stg.stg_page t WHERE t.norm_title = s.redirect_to_norm);

-- The HTML-entity gap flagged in V002. If this is tiny, leave the TODO.
SELECT count(*) AS titles_with_entities
FROM stg.stg_page WHERE title LIKE '%&%;%';
"@
```

Record all of these in `NOTES.md` and update the sizing table in your design doc with measured values.

### If it breaks

- **`unique` test on `norm_title` FAILS** — this is the important one, and it's a real finding, not a nuisance. Inspect the collisions:
  ```sql
  SELECT norm_title, count(*), array_agg(title)
  FROM stg.stg_page GROUP BY 1 HAVING count(*) > 1
  ORDER BY 2 DESC LIMIT 20;
  ```
  Look at the `title` arrays. If they're genuinely the same page under MediaWiki rules (e.g. `Foo bar` and `Foo_bar`), your data has a real duplicate and you should investigate the source. If they're *different* pages that your function wrongly merged, `norm_title` is too aggressive — fix it in a new migration (`V004__norm_title_fix.sql`), never by editing V002, which has already been applied.
- **`Compilation Error: model 'stg_page' depends on a source named 'raw.page' which was not found`** — `sources.yml` must be under `models/`, and the `name:`/`schema:` must both be `raw`.
- **Models land in a schema called `stg_stg` or `dbt_mart`** — the `generate_schema_name` macro isn't being picked up. Confirm it's in `macros/` and the macro name is exactly `generate_schema_name`.
- **`permission denied for schema mart`** — `mart` must be owned by `dbt`. `\dn+` to check; re-run migrations if not.
- **`function public.norm_title(text) does not exist`** — V002 didn't apply, or `dbt` lacks EXECUTE. Functions grant EXECUTE to PUBLIC by default, so this almost always means the migration didn't run.

Commit:

```powershell
git add dbt/ NOTES.md
git commit -m "First dbt models: stg_page and dim_article, with tests"
```

---

## Step 15 — Cosmos: dbt models as Airflow tasks

### Why

You can run dbt from a `BashOperator` — one task that shells out to `dbt build`. It works, and it's a perfectly reasonable place to start. But it gives you one opaque black box: if model 7 of 20 fails, Airflow shows you a single red square and you go read a log to find out which.

**Cosmos** parses your dbt project and renders **each model and each test as its own Airflow task**. That gives you per-model retries, per-model logs, per-model duration history, and a graph view that matches your actual lineage. It's about fifteen lines of code and it's what you'd run in production.

**Why a separate DAG from ingest.** Your design doc calls for this and it's right: a six-hour parse should not block you from iterating on SQL. Separate DAGs mean you can re-run transforms twenty times against already-loaded data.

### Do

Create `airflow-docker/dags/wikigraph_transform.py`:

```python
"""Transform: raw.page -> stg -> mart, via dbt, one Airflow task per model."""
from __future__ import annotations

import os

import pendulum
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig

DBT_PROJECT = "/opt/airflow/dbt/wikigraph"

profile_config = ProfileConfig(
    profile_name="wikigraph",
    target_name="dev",
    # Reuse the same profiles.yml dbt uses on the command line — one source of
    # truth, so "works in the shell but not in Airflow" can't happen.
    profiles_yml_filepath=f"{DBT_PROJECT}/profiles.yml",
)

wikigraph_transform = DbtDag(
    dag_id="wikigraph_transform",
    project_config=ProjectConfig(DBT_PROJECT),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=os.environ.get(
            "DBT_EXECUTABLE_PATH", "/home/airflow/.local/bin/dbt"
        ),
    ),
    operator_args={
        "install_deps": False,     # no packages.yml yet
    },
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["wikigraph", "transform", "dbt"],
)
```

Confirm the dbt path first — it varies by image:

```powershell
cd airflow-docker; docker compose --env-file ..\.env exec -T airflow-scheduler which dbt; cd ..
```

If it's not `/home/airflow/.local/bin/dbt`, add the real path as `DBT_EXECUTABLE_PATH` in your compose override's `environment:` block.

Trigger `wikigraph_transform` from the UI.

### Verify

The graph view should show a **separate task per model and per test**, in dependency order:

```
stg_page_run ─▶ stg_page_test ─▶ dim_article_run ─▶ dim_article_test
```

All green. Compare against Step 14's single `dbt build` output — same work, vastly better observability.

**Optional but worth doing: link the two DAGs.** Airflow 3's asset-based scheduling lets ingest declare that it produces `raw.page`, and transform declare that it consumes it — transform then fires automatically when ingest completes. The simpler version is a `TriggerDagRunOperator` at the end of ingest. Either is fine; assets are the more modern primitive and worth learning when you have a spare hour.

### If it breaks

- **`ModuleNotFoundError: No module named 'cosmos'`** — not in the image. Add `astronomer-cosmos` to `requirements.txt` and rebuild.
- **Cosmos renders zero tasks** — it couldn't parse the project. Check `ProjectConfig` points at the directory containing `dbt_project.yml`, and that `dbt parse` succeeds manually.
- **Every model task fails with a profile error** — Cosmos runs dbt from a different working directory than you did. The absolute `profiles_yml_filepath` above is what fixes this.
- **DAG import is slow / the scheduler complains** — Cosmos parses the dbt project on every DAG parse. For a project this size it's fine; at scale you'd use a pre-generated `manifest.json` via `LoadMode.DBT_MANIFEST`.

Commit:

```powershell
git add airflow-docker/dags/wikigraph_transform.py
git commit -m "Transform DAG: dbt models as individual Airflow tasks via Cosmos"
```

---

## Step 16 — End-to-end verification

### Why

Everything is built. This step proves it's *reproducible*, which is a different and stronger claim. A pipeline that works once is a script; a pipeline that rebuilds itself from nothing is infrastructure.

### Do

**16a. The full teardown-and-rebuild.** This is the real test.

```powershell
.\tasks.ps1 db-nuke          # type 'yes' — destroys the warehouse volume
.\tasks.ps1 db-up
Start-Sleep -Seconds 20
.\tasks.ps1 migrate
```

Then from the Airflow UI:

1. Trigger `wikigraph_ingest` with `{"shards": ["p10p1400054"]}` — wait for green
2. Trigger `wikigraph_transform` — wait for green

**16b. Final reconciliation.** Rows must not vanish silently between layers:

```powershell
docker exec -it wikigraph-warehouse psql -U dbt -d wikigraph -c @"
WITH layers AS (
  SELECT 'manifest.pages_loaded' AS layer, sum(pages_loaded)::bigint AS n
    FROM raw.ingest_manifest WHERE status = 'loaded'
  UNION ALL SELECT 'raw.page',        count(*) FROM raw.page
  UNION ALL SELECT 'stg.stg_page',    count(*) FROM stg.stg_page
  UNION ALL SELECT 'mart.dim_article',count(*) FROM mart.dim_article
)
SELECT * FROM layers;
"@
```

Expected relationship:

- `manifest.pages_loaded` = `raw.page` = `stg.stg_page` — **exactly equal**. Any difference is a bug.
- `mart.dim_article` = `stg.stg_page` − redirects. Should be smaller by exactly your redirect count.

**16c. Generate the lineage docs.**

```powershell
.\tasks.ps1 dbt docs generate
```

That writes `target/index.html` and `target/manifest.json` inside the dbt project — and because `dbt/` is bind-mounted from your repo, they land on your Windows filesystem where you can just open them:

```powershell
Start-Process .\dbt\wikigraph\target\index.html
```

(`dbt docs serve` would also work, but it starts a web server *inside* the container on a port you haven't published, so opening the file directly is less fuss.) The lineage graph is a genuinely useful reference once you have twenty models, and a good portfolio artifact.

**16d. Update your design doc** with everything you measured. The sizing table in `wikigraph_design.md` currently contains estimates with a stated ±40% uncertainty on link density and an unresolved 26.9%-vs-61% question on redirect share. You now have real numbers for one full shard. Replace the guesses and note which shard they came from.

### Definition of done

- [ ] `.\tasks.ps1 db-nuke` → `db-up` → `migrate` rebuilds the warehouse in under two minutes
- [ ] `wikigraph_ingest` green on one shard; rerunning changes no row counts
- [ ] `wikigraph_transform` green, one Airflow task per dbt model
- [ ] All dbt tests pass, including `unique` on `norm_title`
- [ ] `SELECT * FROM mart.dim_article LIMIT 10` returns recognizable Wikipedia articles
- [ ] Row counts reconcile across manifest → raw → stg → mart
- [ ] `NOTES.md` has measured throughput, sizes, and redirect share
- [ ] Everything is committed; `raw_data/`, `staging/`, and `.env` are not

---

## What comes next

In the order the design doc recommends — each is independently useful:

1. **Link extraction** (`src/wikigraph/links.py`) — the hardest correctness problem in the project. The six context flags (`in_parens`, `in_italics`, `in_template`, `in_table`, `in_ref`, `in_infobox`) are what make the first-link goal implementable at all; without them it isn't. Build it as a single left-to-right scanner tracking depth counters, not as post-hoc regex matching. Validate against 20 hand-checked articles before trusting it — an hour of manual checking saves a week.
2. **`fct_article_link`** with redirects resolved → export an ego network → your first graph plot. **Goal #1 done.**
3. **`article_alias` + trigram/FTS indexes.** **Goal #2 done.**
4. **`fct_first_link`** and the in-memory functional-graph walk. **Goal #3 done.**
5. **Backfill** the other 18 shards. Before you do: revisit partitioning for `stg.pagelink` (dbt's default `table` materialization produces an *unpartitioned* table, which is fine at 13M rows and painful at 245M), re-check disk headroom against your measured Parquet size, and delete each shard's Parquet once link extraction succeeds.

One deliberate simplification to revisit: **do not partition `stg.pagelink` during single-shard development.** It's ~13M rows unpartitioned, which Postgres handles fine, and the dbt-plus-partitioning interaction is a distraction from getting the link logic right. Leave a comment in the model file so future-you knows it was a choice.

---

## Pitfalls, ranked by how much time they'll cost you

| Pitfall | Symptom | Fix |
|---|---|---|
| Appending instead of replacing on rerun | Duplicate rows; no idea when it started | `TRUNCATE` partition inside the load transaction |
| Two implementations of title normalization | Joins silently drop or multiply rows — the hardest bug here to find | SQL function only; Python emits `target_raw` |
| No `ANALYZE` after bulk load | A 10-second query takes 40 minutes | `ANALYZE` in the loader, every time |
| Logic inside `@task` bodies | Every debug cycle costs a DAG re-parse | Package under `src/`, DAG just calls it |
| Text-format `COPY` with wikitext | Mystery parse errors on ~0.1% of rows, deep into a load | `FORMAT BINARY` |
| Bind-mounting PGDATA to a Windows path | Postgres won't start, or corrupts silently | Named volume, always |
| CRLF line endings in files a container runs | `bad interpreter: /bin/bash^M`, or unrelated-looking errors | `.gitattributes` with `eol=lf` |
| `elem.clear()` without deleting siblings | Memory grows with input size — works on your test shard, degrades on the biggest | The cleanup idiom in `iter_pages` |
| Big objects through XCom | Metadata DB bloats; tasks crawl | Pass paths and counts only |
| Unbounded Airflow parallelism | Laptop unusable; healthy tasks marked zombie | Pools, `max_active_runs` |
| Indexes present during bulk load | Load takes ~5× longer | Create indexes after loading |
| Unpinned dbt deps | Pulls a dbt 2.0 alpha; "postgres adapter not supported by dbt Fusion" | Pin with `<2.0` upper bounds |
| Docker disk image too small | Postgres dies mid-load, data directory unrecoverable | Raise it in Step 3, before you need it |
| dbt schema concatenation | Tables land in `stg_mart` | The `generate_schema_name` override |

---

## Appendix: command reference

```powershell
# ---- warehouse ----
.\tasks.ps1 db-up          # start
.\tasks.ps1 db-down        # stop, keep data
.\tasks.ps1 db-nuke        # stop, DELETE data
.\tasks.ps1 db-shell       # psql as etl
.\tasks.ps1 db-logs        # follow logs
.\tasks.ps1 migrate        # apply pending migrations

# ---- local dev ----
.\tasks.ps1 test           # pytest
.\tasks.ps1 lint           # ruff

# ---- airflow ----
.\tasks.ps1 airflow-build  # rebuild the custom image
.\tasks.ps1 airflow-up
.\tasks.ps1 airflow-down

# ---- airflow CLI passthrough:  .\tasks.ps1 af <any airflow command> ----
.\tasks.ps1 af dags list
.\tasks.ps1 af dags list-import-errors    # your most-used command
.\tasks.ps1 af pools list
.\tasks.ps1 af tasks list wikigraph_ingest

# ---- dbt passthrough (adds --profiles-dir . for you) ----
.\tasks.ps1 dbt build
.\tasks.ps1 dbt test
.\tasks.ps1 dbt docs generate
```

> **Why these wrap `docker compose` instead of you calling it directly:** Compose
> discovers `docker-compose.override.yml` by looking in the *current directory*.
> `--project-directory` does not change that. So any compose command must be run
> from inside `airflow-docker/` or the override silently won't load — and "my
> volume mounts disappeared" is a miserable thing to debug. `tasks.ps1` handles
> the directory change for you.

---

## Sources for version pins

- [Apache Airflow supported versions](https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html) — 3.3.0 current as of July 2026
- [dbt-core on PyPI](https://pypi.org/project/dbt-core/) — 1.12.0, July 2026
- [dbt-adapters issue #1992](https://github.com/dbt-labs/dbt-adapters/issues/1992) — the unpinned `dbt-postgres` → dbt 2.0 alpha → Fusion error, reported on Windows
- [dbt Fusion rollout plan](https://docs.getdbt.com/blog/dbt-fusion-engine-path-to-ga) — Postgres adapter not yet GA on Fusion
- [postgres on Docker Hub](https://hub.docker.com/_/postgres) — 18.4 current; note the PGDATA path change in 18+
- [Astronomer Cosmos releases](https://github.com/astronomer/astronomer-cosmos/releases) — 1.11+ targets Airflow 3
