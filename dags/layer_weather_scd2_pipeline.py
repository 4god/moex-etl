from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from common.pipeline_utils import DS_STG_METEO_READY, DS_WEATHER_SCD2_READY, run_pipeline_sql


@dag(
    dag_id="layer_weather_scd2_build",
    description="Weather SCD2 layer DAG triggered by meteo STG dataset",
    start_date=datetime(2024, 1, 1),
    schedule=[DS_STG_METEO_READY],
    catchup=False,
    tags=["workshop", "layer", "scd2", "meteo"],
)
def layer_weather_scd2_build() -> None:
    """Build SCD2 weather dimension from meteo STG layer."""

    @task(outlets=[DS_WEATHER_SCD2_READY])
    def build_weather_scd2_layer() -> None:
        run_pipeline_sql("DV-320_scd2_weather")

    build_weather_scd2_layer()


dag = layer_weather_scd2_build()
