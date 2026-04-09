"""Операторы: типовой контракт REST JSON → Kafka → raw (как MOEX/CBR/ODS)."""

from __future__ import annotations

from typing import Any

import requests
from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.ods_gap_fill import (
    eurostat_existing_time_keys,
    eurostat_months_needed_count,
    purge_raw_rows_for_source,
    required_month_keys,
    wb_existing_years,
    wb_filter_payload_by_years,
    wb_years_needed,
)
from common.open_data_settings import (
    get_backfill_floor_date,
    get_source_config,
    is_ods_backfill_enabled,
    open_data_query_params,
    resolve_ods_ingest_mode,
    resolve_open_data_url,
    utc_today,
)
from common.pipeline_utils import consume_raw_to_postgres, publish_to_kafka


def _sanitize_query_params(params: dict[str, Any] | None) -> dict[str, Any]:
    """Маскирует типичные секреты в query string (логируем только безопасное)."""
    if not params:
        return {}
    sensitive_suffix = ("_key", "_token", "_secret", "_password")
    sensitive_exact = frozenset({"api_key", "token", "password", "secret", "authorization"})
    out: dict[str, Any] = {}
    for k, v in params.items():
        lk = str(k).lower()
        if lk in sensitive_exact or lk.endswith(sensitive_suffix):
            out[k] = "***"
        else:
            out[k] = v
    return out


class WorkshopJsonToKafkaOperator(BaseOperator):
    """
    GET по конфигу open-data (source_key) и публикация одного сообщения в Kafka
    в формате {source, endpoint, payload}.

    Режимы (YAML `ods_ingest_mode` или авто): **gap_fill** (date / lastTimePeriod) — догрузка
    пробелов по датам в raw; **snapshot_replace** — DELETE по `source` в `raw_table`, затем один снимок.

    См. README: идемпотентность, backfill.
    """

    template_fields: tuple[str, ...] = ("source_key", "source_label", "topic", "raw_table")

    def __init__(
        self,
        *,
        source_key: str,
        source_label: str,
        topic: str,
        raw_table: str | None = None,
        timeout: int = 120,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.source_key = source_key
        self.source_label = source_label
        self.topic = topic
        self.raw_table = raw_table
        self.timeout = timeout

    def execute(self, context: dict) -> None:
        spec = get_source_config(self.source_key)
        url = resolve_open_data_url(spec)
        params = dict(open_data_query_params(spec))
        mode = resolve_ods_ingest_mode(spec, params)
        wb_missing_years: set[int] | None = None

        hook = PostgresHook(postgres_conn_id="dwh")

        if mode == "snapshot_replace" and self.raw_table:
            purge_raw_rows_for_source(hook, self.raw_table, self.source_label)
            self.log.info(
                "ODS snapshot_replace: DELETE raw rows source=%s table=%s",
                self.source_label,
                self.raw_table,
            )

        backfill_on = (
            is_ods_backfill_enabled() and spec.get("ods_disable_backfill") is not True
        )

        if backfill_on and mode == "gap_fill":
            floor = get_backfill_floor_date()
            ceiling = utc_today()
            if floor > ceiling:
                floor, ceiling = ceiling, floor

            if self.raw_table and "date" in params:
                existing = wb_existing_years(hook, self.raw_table, self.source_label)
                needed = wb_years_needed(floor, ceiling)
                missing = needed - existing
                if not missing:
                    raise AirflowSkipException(
                        f"ODS: годы из источника уже есть в raw для {self.source_label}"
                    )
                wb_missing_years = missing
                params["date"] = f"{min(missing)}:{max(missing)}"
            elif self.raw_table and "lastTimePeriod" in params:
                ex = eurostat_existing_time_keys(hook, self.raw_table, self.source_label)
                req = required_month_keys(floor, ceiling)
                if req.issubset(ex):
                    raise AirflowSkipException(
                        f"ODS: месяцы Eurostat уже покрыты в raw для {self.source_label}"
                    )
                params["lastTimePeriod"] = min(
                    eurostat_months_needed_count(floor, ceiling), 600
                )
            else:
                if "date" in params:
                    params["date"] = f"{floor.year}:{ceiling.year}"
                if "lastTimePeriod" in params:
                    params["lastTimePeriod"] = min(
                        eurostat_months_needed_count(floor, ceiling), 600
                    )

        self.log.info(
            "ODS HTTP GET mode=%s source=%s source_key=%s url=%s params=%s",
            mode,
            self.source_label,
            self.source_key,
            url,
            _sanitize_query_params(params),
        )
        response = requests.get(url, params=params or None, timeout=self.timeout)
        response.raise_for_status()
        self.log.info(
            "ODS HTTP response source=%s status=%s final_url=%s",
            self.source_label,
            response.status_code,
            response.url,
        )
        data = response.json()

        if wb_missing_years is not None:
            data = wb_filter_payload_by_years(data, wb_missing_years)
            if data is None:
                raise AirflowSkipException(
                    f"ODS: пустой ответ после фильтра по годам ({self.source_label})"
                )

        publish_to_kafka(
            self.topic,
            {
                "source": self.source_label,
                "endpoint": response.url,
                "payload": data,
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
