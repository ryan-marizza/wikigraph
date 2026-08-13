import pyarrow.parquet as pq, pyarrow.compute as pc

t = pq.read_table(r"staging\enwiki-2026-07-01-p10p1400054.parquet", columns=["is_redirect", "text_bytes"])
n = t.num_rows
red = pc.sum(t['is_redirect']).as_py()
print(f'ns=0 pages : {n:,}')
print(f'redirects  : {red:,}  ({red/n:.1%})')
tb = t['text_bytes'].drop_null()
print(f'text_bytes : mean {pc.mean(tb).as_py():,.0f}  max {pc.max(tb).as_py():,}')