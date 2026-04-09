"""ODS: Open-Meteo через deferrable HttpOperator (триггер вместо удержания worker-слота).

HTTP-запрос выполняется в :class:`~airflow.providers.http.triggers.http.HttpTrigger`;
после ответа задача резюмируется, публикуем в Kafka как у прочих ODS.
Именованные функции вместо lambda — корректнее для сериализации в deferrable-режиме.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.http.operators.http import HttpOperator
from airflow.utils.task_group import TaskGroup

from common.pipeline_utils import (
    TOPIC_ODS_PUBLIC_RAW,
    consume_raw_to_postgres,
    publish_to_kafka,
)

SOURCE_LABEL = "ODS_OPEN_METEO_HTTP_DEFERRABLE"

# Москва — демо-координаты (тот же смысл, что и в основном meteo DAG).
OPEN_METEO_LAT = 55.75
OPEN_METEO_LON = 37.62


def _open_meteo_response_capture(response: Any) -> dict[str, Any]:
    """В XCom уходит URL + JSON; не lambda — стабильнее для deferrable."""
    return {
        "endpoint": str(response.url),
        "payload": response.json(),
    }


@dag(
    dag_id="ods_open_meteo_defer_fetch",
    description=(
        "Open-Meteo (deferrable HttpOperator) → Kafka → raw.ods_public_api_payloads. "
        "Нужен connection `http_open_meteo` (см. AIRFLOW_CONN_HTTP_OPEN_METEO)."
    ),
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["workshop", "ods", "open-data", "kafka", "deferrable", "open-meteo"],
)
def ods_open_meteo_defer_fetch() -> None:
    with TaskGroup(group_id="open_meteo_defer_ods"):
        fetch_open_meteo_defer = HttpOperator(
            task_id="fetch_open_meteo_defer",
            http_conn_id="http_open_meteo",
            method="GET",
            endpoint="v1/forecast",
            data={
                "latitude": OPEN_METEO_LAT,
                "longitude": OPEN_METEO_LON,
                "current": "temperature_2m",
            },
            deferrable=True,
            response_filter=_open_meteo_response_capture,
        )

        @task(task_id="publish_to_kafka")
        def publish_to_kafka_task() -> None:
            ti = get_current_context()["ti"]
            captured = ti.xcom_pull(task_ids="open_meteo_defer_ods.fetch_open_meteo_defer")
            if not captured:
                raise ValueError("Нет XCom от HttpOperator (fetch_open_meteo_defer).")
            publish_to_kafka(
                TOPIC_ODS_PUBLIC_RAW,
                {
                    "source": SOURCE_LABEL,
                    "endpoint": captured["endpoint"],
                    "payload": captured["payload"],
                },
            )

        @task(task_id="ingest_kafka_to_raw")
        def ingest_kafka_to_raw() -> int:
            return consume_raw_to_postgres(
                topic=TOPIC_ODS_PUBLIC_RAW,
                table="ods_public_api_payloads",
                group_id="workshop-raw-ods-public-loader",
            )

        fetch_open_meteo_defer >> publish_to_kafka_task() >> ingest_kafka_to_raw()


dag = ods_open_meteo_defer_fetch()
