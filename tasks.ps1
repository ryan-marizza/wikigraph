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