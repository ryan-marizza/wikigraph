import datetime, time

from wikigraph.parse import parse_shard_to_parquet

t0 = time.time()

s = parse_shard_to_parquet(
    r"raw_data\enwiki-2026-07-01-p10p1400054.xml\enwiki-2026-07-01-p10p1400054.xml",
    "p10p1400054",
    datetime.date(2026, 7, 1),
    r"staging\enwiki-2026-07-01-p10p1400054.smoke.parquet",
    limit=50_000)

print(s)
print(f"took {time.time() - t0:.1f} seconds")