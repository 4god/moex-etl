Pipeline Overview
=================

Методология: **Data Vault 2.0**.

Оркестрация: source DAG-ы и layer DAG-ы связаны через Airflow Datasets (data assets).

Слои:

- ``api -> kafka``: получение данных из API и публикация payload в Kafka topics
- ``debezium -> kafka``: CDC-коннекторы для масштабирования на новые источники
- ``raw``: сырые JSON payloads (MOEX, CBR, Open-Meteo)
- ``stg``: нормализованные таблицы источников, включая погоду по Москве
- ``vault``: Hub/Link/Satellite
- ``datamart``: бизнес-витрины
- ``analytics (SCD2)``: историческая размерность ``dim_moscow_weather_regime_scd2``
- ``analytics``: dbt views для BI

Основные task-папки SQL:

- ``sql/pipelines/SRC-110_moex_stg_refresh``
- ``sql/pipelines/SRC-120_cbr_stg_refresh``
- ``sql/pipelines/SRC-130_meteo_stg_refresh``
- ``sql/pipelines/DV-210_vault_load``
  - ``.../moex/hubs|links|satellites``
  - ``.../cbr/hubs|links|satellites``
  - ``.../meteo/hubs|links|satellites``
- ``sql/pipelines/DV-310_datamart_publish``
- ``sql/pipelines/DV-320_scd2_weather``

Миграции:

- ``sql/migrations/001_create_databases.sql``
- ``sql/migrations/010_init_foundation.sql``
- ``sql/migrations/020_add_kafka_metadata_to_raw.sql``
- ``sql/migrations/030_add_open_meteo_source.sql``
