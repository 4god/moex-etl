from __future__ import annotations

from datetime import datetime

import requests
from airflow.decorators import dag, task

from common.open_data_settings import get_source_config
from common.pipeline_utils import persist_open_data_snapshot

SOURCE_KEY = "national_accounts_gdp"


@dag(
    dag_id="ods_economic_indicator_fetch",
    description=(
        "Публичные макроданные (пример: ВВП в текущих ценах) через REST. "
        "Источник в духе открытых каталогов вроде World Bank Open Data; URL и параметры — в config/*.yaml."
    ),
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["workshop", "ods", "open-data"],
)
def ods_economic_indicator_fetch() -> None:
    @task
    def fetch_and_land() -> None:
        spec = get_source_config(SOURCE_KEY)
        base = spec["base_url"].format(**spec["path_params"])
        params = dict(spec.get("query_params") or {})
        r = requests.get(base, params=params, timeout=120)
        r.raise_for_status()
        body = r.json()
        persist_open_data_snapshot(SOURCE_KEY, r.url, body)

    fetch_and_land()


dag = ods_economic_indicator_fetch()
