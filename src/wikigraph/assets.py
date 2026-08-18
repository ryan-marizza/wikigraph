"""Asset definitions shared by the ingest and transform DAGs.

An Asset is just a name Airflow uses to connect a producer to a consumer.
It does not read or check the data — Airflow trusts the producing task's
success as proof the asset was updated.
"""

from __future__ import annotations

from airflow.sdk import Asset

RAW_PAGE = Asset(
    name="raw_page",
    uri="postgres://warehouse:4342/wikigraph/raw/page"
)