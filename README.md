# wikigraph
for fun side project analyzing wikipedia


data from https://dumps.wikimedia.org/other/mediawiki_content_current/enwiki/2026-07-01/xml/bzip2/

Generating dbt Documentation:
.\tasks.ps1 dbt docs generate --static

Opening dbt Documentation:
Start-Process .\dbt\wikigraph\target\static_index.html


# COMMAND REFERENCE ---------------------------------------------------
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