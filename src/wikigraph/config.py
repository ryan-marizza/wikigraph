"""Single source of truth for paths and constants.

Everything reads from environment variables with sensible local defaults,
so the same code runs on your Windows host and inside the Airflow container
without conditionals.
"""

from __future__ import annotations

import os
from pathlib import Path

RAW_DIR = Path(os.environ.get("WIKIGRAPH_RAW_DIR", "raw_data"))
STAGING_DIR = Path(os.environ.get("WIKIGRAPH_STAGING_DIR", "staging"))
DUMP_DATE = os.environ.get("WIKIGRAPH_DUMP_DATE", "2026-07-01")

# MediaWiki export schema namespace. Every XML tag is prefixed with this.
MW_NS = "http://www.mediawiki.org/xml/export-0.11/"

#Rows per Parqet row group. This is a tradeoff between memory usage and speed. Lower values use less memory but are slower to read/write.
PARQUET_ROW_GROUP = 50_000
