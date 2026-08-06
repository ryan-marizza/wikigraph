"""Discover dump shards on disk and parse their partition keys from filenames."""
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
    """One dump shard file: its partition key, dump date, path and size."""
    name: str
    dump_date: str
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
        raise FileNotFoundError(f"No shard XML files found in {raw_dir}")

    return [found[k] for k in sorted(found)]
