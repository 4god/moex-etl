from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from common.pipeline_utils import (
    DS_STG_METEO_READY,
    DS_WEATHER_ANALYTICS_DIM_READY,
    run_pipeline_sql,
)


@dag(
    dag_id="weather_regime_dimension_build",
    description=(
        "История режимов погоды по дням в analytics: медленно меняющееся измерение "
        "(валидность от/до, текущая версия). Реализация в DV-320 SQL."
    ),
    start_date=datetime(2024, 1, 1),
    schedule=[DS_STG_METEO_READY],
    catchup=False,
    tags=["workshop", "analytics", "meteo"],
)
def weather_regime_dimension_build() -> None:
    @task(outlets=[DS_WEATHER_ANALYTICS_DIM_READY])
    def run_weather_dimension_sql() -> None:
        run_pipeline_sql("DV-320_scd2_weather")

    run_weather_dimension_sql()


dag = weather_regime_dimension_build()
