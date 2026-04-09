from __future__ import annotations

from datetime import datetime

import requests
from airflow.decorators import dag, task

from common.open_data_settings import get_source_config
from common.pipeline_utils import persist_open_data_snapshot

SOURCE_KEY = "territory_catalog"


@dag(
    dag_id="ods_territory_reference_fetch",
    description=(
        "Справочник территорий (REST JSON). Параметры запроса задаются в config/*.yaml; "
        "подходит для источников в духе агрегаторов открытых данных."
    ),
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["workshop", "ods", "open-data"],
)
def ods_territory_reference_fetch() -> None:
    @task
    def fetch_and_land() -> None:
        spec = get_source_config(SOURCE_KEY)
        url = spec["url"]
        params = dict(spec.get("query_params") or {})
        r = requests.get(url, params=params, timeout=120)
        r.raise_for_status()
        body = r.json()
        persist_open_data_snapshot(SOURCE_KEY, r.url, body)

    fetch_and_land()


dag = ods_territory_reference_fetch()
