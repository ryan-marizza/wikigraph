# WikiGraph — Measured Facts

## Environment
- Date: 8/4/2026
- Host: Windows, Docker Desktop 4.85.0, WSL 2.7.11.0
- Backend: WSL2
- WSL VM memory: 45.59 GB   (default = 50% of system RAM)
- WSL VM processors: 24  (default = all logical CPUs)
- Docker VHDX path: C:\Users\ryanm\AppData\Local\Docker\wsl\disk\docker_data.vhdx
- Docker VHDX current size: 6.18 GB
- Sparse VHD enabled: yes
- Free space on VHDX drive: 3072.9 GB
- Free space on raw_data drive: 3072.9 GB

## Measurements
## Disk throughput (Step 6)
- Docker bind mount read: 233.3 MB/s
- Same read on Windows host: 6,614 MB/s (likely cache-assisted)
- Decision: Stay on C:\ — I/O far exceeds expected parse throughput (~60 MB/s)


## Shard p10p1400054 parse (Step 8)
- Input XML size: 9.9 GB
- Wall time: 1.4 min
- Throughput: 10,803 pages/s, 126MB/s of XML
- Peak RSS: 3 GB        <- if this grew all run, the cleanup is broken
- pages_seen: 924050
- pages_written (ns=0): 717051
- multi_revision_pages: 0  <- MUST be 0
- Parquet out: 2970 MB  (compression ratio 3.34 x)
- Redirect share: 46%
  Design doc guessed 26.9% (biased head sample) vs 61% (published estimate).
  Measured answer: 46%
- Extrapolated to 19 shards: 0.44 hours, 55.1 GB Parquet