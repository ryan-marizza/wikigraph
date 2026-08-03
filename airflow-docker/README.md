## Overview

The official quick-start runs Airflow as a set of containers via Docker Compose: an API server (the UI), scheduler, DAG processor, triggerer, a Celery worker, plus Postgres and Redis. This is fine for learning and local development, but the compose file offers no security guarantees for production — the Airflow community recommends Kubernetes with the official Helm chart when you go to prod.

Latest release is Airflow 3.3.0, released July 6, 2026. I'll use that below.

## Step 1 — Prep Docker Desktop

Allocate at least 4 GB of memory to Docker (8 GB is better) — if it's too low the web server will restart in a loop. You also need Docker Compose v2.14.0 or newer.

In Docker Desktop: **Settings → Resources → Advanced**, set Memory to 8 GB, then **Apply & Restart**. Verify:

```bash
docker compose version
```

## Step 2 — Create the project folder

```bash
mkdir airflow-docker && cd airflow-docker
mkdir -p ./dags ./logs ./plugins ./config
```

These get mounted into the containers: `./dags` for your DAG files, `./logs` for task and scheduler logs, `./config` for custom settings, and `./plugins` for custom plugins.

## Step 3 — Fetch the compose file

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.3.0/docker-compose.yaml'
```

If that 404s (docs sometimes lag a release), grab the link from the [stable docs page](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) instead.

The file defines `airflow-scheduler`, `airflow-dag-processor`, `airflow-api-server` (the UI on port 8080), `airflow-worker`, `airflow-triggerer`, `airflow-init`, `postgres`, and `redis`. Flower is optional via `docker compose --profile flower up`.

## Step 4 — Create the `.env` file

On **macOS or Windows**, create `.env` next to the compose file:

```
AIRFLOW_UID=50000
```

On **Linux**, set it to your actual UID so files aren't created as root:

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

## Step 5 — Initialize the database

```bash
docker compose up airflow-init
```

Wait for `airflow-init-1 exited with code 0`. This creates an account with login `airflow` and password `airflow`.

Change those defaults by adding `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` to your `.env` *before* running init.

## Step 6 — Start everything

```bash
docker compose up -d
```

Check health in a second terminal (or just look at the Containers tab in Docker Desktop):

```bash
docker ps
```

All containers should reach `(healthy)`. First start takes a few minutes while the image pulls.

## Step 7 — Log in

Open **http://localhost:8080** and sign in with `airflow` / `airflow`.

## Step 8 — Add a DAG

Drop this in `./dags/hello_pipeline.py` — it'll show up in the UI within ~30 seconds:

```python
from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="hello_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example"],
)
def hello_pipeline():
    @task
    def extract() -> dict:
        return {"rows": 42}

    @task
    def transform(data: dict) -> int:
        return data["rows"] * 2

    @task
    def load(value: int) -> None:
        print(f"Loaded {value} rows")

    load(transform(extract()))


hello_pipeline()
```

Note the `airflow.sdk` import — that's the Airflow 3 Task SDK, not the old `airflow.decorators` path.

## Step 9 — Add your own Python dependencies

Don't use `_PIP_ADDITIONAL_REQUIREMENTS` beyond quick experiments; it reinstalls on every container start. Build a custom image instead.

Comment out the `image:` line and uncomment `build: .` in `docker-compose.yaml`:

```yaml
# image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.3.0}
build: .
```

Create a `Dockerfile` alongside it:

```dockerfile
FROM apache/airflow:3.3.0
COPY requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
```

Pinning apache-airflow to the same version as the base image keeps pip from silently up/downgrading Airflow while resolving your other dependencies.

Then `docker compose build` (or add `--build` to your `up` command).

## Useful commands

```bash
docker compose logs -f airflow-scheduler   # tail a service
docker compose run airflow-worker airflow info   # run CLI commands
docker compose down                        # stop, keep data
docker compose down --volumes --remove-orphans   # nuke and start clean
```

## Two things that commonly bite people

**Connecting to a database on your host machine.** Use `host.docker.internal` instead of `localhost` in your Airflow connections — on Docker Desktop this works out of the box.

**Port 8080 already in use.** Change the mapping under `airflow-apiserver` to something like `"8081:8080"`.

Once you've got this running, the natural next step for a real project is splitting your `dags/` into a proper repo structure with a `docker-compose.override.yaml` for local-only tweaks, so the base file stays close to upstream and is easy to update.