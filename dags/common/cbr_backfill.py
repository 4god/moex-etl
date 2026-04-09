"""Догрузка курсов ЦБ РФ по датам: архив https://www.cbr-xml-daily.ru/archive/YYYY/MM/DD/daily_json.js"""

from __future__ import annotations

from datetime import date, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook

CBR_SOURCE_LABEL = "CBR_DAILY"


def cbr_archive_url(d: date) -> str:
    return (
        f"https://www.cbr-xml-daily.ru/archive/"
        f"{d.year:04d}/{d.month:02d}/{d.day:02d}/daily_json.js"
    )


def _daterange_inclusive(floor: date, ceiling: date) -> list[date]:
    out: list[date] = []
    d = floor
    while d <= ceiling:
        out.append(d)
        d += timedelta(days=1)
    return out


def cbr_existing_dates(hook: PostgresHook, source_label: str = CBR_SOURCE_LABEL) -> set[date]:
    """Даты, для которых уже есть снимок в raw (поле Date в JSON)."""
    sql = """
        SELECT DISTINCT ((payload->>'Date')::timestamptz)::date AS d
        FROM raw.cbr_daily_payloads
        WHERE source = %s
          AND payload ? 'Date'
    """
    rows = hook.get_records(sql, parameters=(source_label,))
    out: set[date] = set()
    for r in rows:
        if r and r[0] is not None:
            out.add(r[0])
    return out


def cbr_missing_dates(
    hook: PostgresHook,
    floor: date,
    ceiling: date,
    source_label: str = CBR_SOURCE_LABEL,
) -> list[date]:
    needed = set(_daterange_inclusive(floor, ceiling))
    existing = cbr_existing_dates(hook, source_label)
    return sorted(needed - existing)
