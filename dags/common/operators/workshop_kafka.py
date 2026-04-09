"""Операторы: типовой контракт REST JSON → Kafka → raw (как MOEX/CBR/ODS)."""

from __future__ import annotations

from typing import Any

import requests
from airflow.models import BaseOperator

from common.open_data_settings import (
    get_source_config,
    open_data_query_params,
    resolve_open_data_url,
)
from common.pipeline_utils import consume_raw_to_postgres, publish_to_kafka


class WorkshopJsonToKafkaOperator(BaseOperator):
    """
    GET по конфигу open-data (source_key) и публикация одного сообщения в Kafka
    в формате {source, endpoint, payload}.
    """

    template_fields: tuple[str, ...] = ("source_key", "source_label", "topic")

    def __init__(
        self,
        *,
        source_key: str,
        source_label: str,
        topic: str,
        timeout: int = 120,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.source_key = source_key
        self.source_label = source_label
        self.topic = topic
        self.timeout = timeout

    def execute(self, context: dict) -> None:
        spec = get_source_config(self.source_key)
        url = resolve_open_data_url(spec)
        params = open_data_query_params(spec)
        response = requests.get(url, params=params or None, timeout=self.timeout)
        response.raise_for_status()
        publish_to_kafka(
            self.topic,
            {
                "source": self.source_label,
                "endpoint": response.url,
                "payload": response.json(),
            },
        )


class WorkshopKafkaConsumeToRawOperator(BaseOperator):
    """Чтение Kafka-топика в raw-таблицу (тот же контракт, что consume_raw_to_postgres)."""

    template_fields: tuple[str, ...] = ("topic", "table", "group_id")

    def __init__(
        self,
        *,
        topic: str,
        table: str,
        group_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.topic = topic
        self.table = table
        self.group_id = group_id

    def execute(self, context: dict) -> int:
        return consume_raw_to_postgres(
            topic=self.topic,
            table=self.table,
            group_id=self.group_id,
        )
