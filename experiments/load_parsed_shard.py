import os, time
from pathlib import Path
from dotenv import load_dotenv
from wikigraph.load import load_parquet

# Anchored to the file, not the cwd — otherwise this breaks the moment you run
# it from anywhere but the repo root.
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

WH_DSN = f"postgresql://{os.environ['PG_ETL_USER']}:{os.environ['PG_ETL_PASSWORD']}@localhost:{os.environ['PG_HOST_PORT']}/{os.environ['PG_DB']}"

t0 = time.time()

r = load_parquet(
    dsn=WH_DSN,
    parquet_path=r"staging/enwiki-2026-07-01-p10p1400054.parquet",
    shard_name="p10p1400054",)
print(r, f'Loaded in {time.time() - t0:.1f} seconds.')