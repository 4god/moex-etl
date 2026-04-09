from __future__ import annotations

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

from common.dag_defaults import TAGS_ODS_OPEN_DATA, WORKSHOP_START_DATE
from common.ods_gap_fill import purge_raw_rows_for_source
from common.open_data_settings import (
    get_source_config,
    open_data_query_params,
    resolve_open_data_url,
)
from common.pipeline_utils import (
    TOPIC_ODS_TERRITORY_RAW,
    consume_raw_to_postgres,
    publish_to_kafka,
)

SOURCE_KEY = "un_population_locations"
SOURCE_LABEL = "ODS_UN_POPULATION_LOCATIONS"
# Ограничение воркшопа: не расползаемся по сотням страниц API (всего ~150).
MAX_UN_LIST_PAGES = 5


@dag(
    dag_id="ods_un_locations_fetch",
    description=(
        "ООН (Population API): пагинация через dynamic task mapping → Kafka → "
        "raw.ods_territory_payloads."
    ),
    start_date=WORKSHOP_START_DATE,
    schedule="@weekly",
    catchup=False,
    max_active_tasks=4,
    tags=[*TAGS_ODS_OPEN_DATA, "un", "task-mapping"],
)
def ods_un_locations_fetch() -> None:
    """Параллельные инстансы fetch (mapped tasks), ingest один — после всех."""

    with TaskGroup(
        group_id="un_population_ods",
        tooltip="План страниц → N параллельных GET → один consumer в Postgres",
    ):
        @task(task_id="purge_un_locations_snapshot")
        def purge_un_locations_snapshot() -> None:
            """Полная перезапись снимка: удалить прошлые страницы этого source в raw."""
            hook = PostgresHook(postgres_conn_id="dwh")
            purge_raw_rows_for_source(hook, "ods_territory_payloads", SOURCE_LABEL)

        @task(task_id="plan_page_numbers")
        def plan_page_numbers() -> list[int]:
            spec = get_source_config(SOURCE_KEY)
            url = resolve_open_data_url(spec)
            base_params = open_data_query_params(spec)
            r = requests.get(
                url,
                params={**base_params, "pageNumber": 1},
                timeout=120,
            )
            r.raise_for_status()
            meta = r.json()
            total_pages = int(meta.get("pages") or 1)
            n = min(total_pages, MAX_UN_LIST_PAGES)
            return list(range(1, n + 1))

        @task(task_id="fetch_page_to_kafka")
        def fetch_page_to_kafka(page: int) -> None:
            spec = get_source_config(SOURCE_KEY)
            url = resolve_open_data_url(spec)
            base_params = open_data_query_params(spec)
            r = requests.get(
                url,
                params={**base_params, "pageNumber": page},
                timeout=120,
            )
            r.raise_for_status()
            publish_to_kafka(
                TOPIC_ODS_TERRITORY_RAW,
                {
                    "source": SOURCE_LABEL,
                    "endpoint": r.url,
                    "payload": r.json(),
                },
            )

        @task(task_id="ingest_kafka_to_raw")
        def ingest_kafka_to_raw() -> int:
            return consume_raw_to_postgres(
                topic=TOPIC_ODS_TERRITORY_RAW,
                table="ods_territory_payloads",
                group_id="workshop-raw-ods-territory-loader",
            )

        purge = purge_un_locations_snapshot()
        pages = plan_page_numbers()
        mapped = fetch_page_to_kafka.expand(page=pages)
        purge >> pages >> mapped >> ingest_kafka_to_raw()


dag = ods_un_locations_fetch()
