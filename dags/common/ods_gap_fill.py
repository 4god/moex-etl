"""Пробелы по датам источника в raw ODS: сравнение с датами в JSON ответов API."""

from __future__ import annotations

from datetime import date
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook

ALLOWED_RAW_PAYLOAD_TABLES = frozenset(
    {
        "ods_health_payloads",
        "ods_marketing_payloads",
        "ods_economic_payloads",
        "ods_territory_payloads",
        "ods_public_api_payloads",
    }
)


def _validate_table(table: str) -> str:
    if table not in ALLOWED_RAW_PAYLOAD_TABLES:
        raise ValueError(f"raw table not allowed for ODS gap fill: {table}")
    return table


def wb_existing_years(hook: PostgresHook, table: str, source_label: str) -> set[int]:
    """Годы наблюдений World Bank (поле date в элементах payload[1])."""
    _validate_table(table)
    sql = f"""
        SELECT DISTINCT (obs->>'date')::int AS yr
        FROM raw.{table} t,
        LATERAL jsonb_array_elements(t.payload->1) AS obs
        WHERE t.source = %s
          AND jsonb_typeof(t.payload) = 'array'
          AND jsonb_array_length(t.payload) > 1
          AND obs ? 'date'
          AND (obs->>'date') ~ '^[0-9]{{4}}$'
          AND (obs->>'value') IS NOT NULL
    """
    rows = hook.get_records(sql, parameters=(source_label,))
    out: set[int] = set()
    for r in rows:
        if r and r[0] is not None:
            out.add(int(r[0]))
    return out


def wb_years_needed(floor: date, ceiling: date) -> set[int]:
    return set(range(floor.year, ceiling.year + 1))


def wb_filter_payload_by_years(payload: Any, keep_years: set[int]) -> Any:
    """Оставить в payload[1] только наблюдения, чей год (поле date) в keep_years."""
    if not isinstance(payload, list) or len(payload) < 2:
        return payload
    block = payload[1]
    if not isinstance(block, list):
        return payload
    filtered = []
    for obs in block:
        if not isinstance(obs, dict):
            continue
        raw_d = obs.get("date")
        if raw_d is None:
            continue
        try:
            y = int(str(raw_d)[:4])
        except (TypeError, ValueError):
            continue
        if y in keep_years:
            filtered.append(obs)
    if not filtered:
        return None
    return [payload[0], filtered]


def eurostat_existing_time_keys(hook: PostgresHook, table: str, source_label: str) -> set[str]:
    """Ключи периодов из dimension.time.category.label (Eurostat SDMX JSON)."""
    _validate_table(table)
    sql = f"""
        SELECT DISTINCT key
        FROM raw.{table} t,
        LATERAL jsonb_object_keys(
            COALESCE(t.payload->'dimension'->'time'->'category'->'label', '{{}}'::jsonb)
        ) AS key
        WHERE t.source = %s
    """
    rows = hook.get_records(sql, parameters=(source_label,))
    return {str(r[0]) for r in rows if r and r[0] is not None}


def required_month_keys(floor: date, ceiling: date) -> set[str]:
    """Периоды YYYY-MM от floor до ceiling (включительно по месяцам)."""
    keys: set[str] = set()
    y, m = floor.year, floor.month
    end_y, end_m = ceiling.year, ceiling.month
    while (y, m) <= (end_y, end_m):
        keys.add(f"{y:04d}-{m:02d}")
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return keys


def eurostat_months_needed_count(floor: date, ceiling: date) -> int:
    return max(1, len(required_month_keys(floor, ceiling)))


def purge_raw_rows_for_source(hook: PostgresHook, table: str, source_label: str) -> None:
    """Полная перезапись снимка в raw: удалить все строки с данным source (перед новым GET)."""
    _validate_table(table)
    hook.run(
        f"DELETE FROM raw.{table} WHERE source = %s",
        parameters=(source_label,),
    )
