# Архитектура ETL-воркшопа

Выбранная методология моделирования: **Data Vault 2.0**.

**Интерактивная карта контейнеров и портов** (откройте файл в браузере): [`services-map.html`](services-map.html) — узлы перетаскиваются, по клику — ссылки на UI и подсказки по профилям `bi` / `dbt`.

## 1) Диаграмма всего проекта (инструменты и взаимодействие)

```mermaid
flowchart LR
    A[MOEX ISS API] --> C[Airflow Source DAGs]
    B[CBR Daily API] --> C
    H[Open-Meteo API] --> C
    X[Other Public APIs] --> C
    J[Other DB sources<br/>CDC] --> DBC[Debezium Connect]
    DBC --> K
    C --> K[(Kafka topics<br/>raw.moex.payloads<br/>raw.cbr.payloads<br/>raw.open_meteo.payloads<br/>raw.<new_source>.payloads<br/>+ CDC topics)]
    C2[Airflow DAGs<br/>datasets + open data] --> D
    K --> D[(Postgres raw/stg/vault/datamart)]
    E[dbt container<br/>run + test] --> D
    D --> F[Metabase]
    F --> G[Dashboard]
```

## 2) Поток данных по слоям

```mermaid
flowchart TB
    API["MOEX + CBR + Open-Meteo + Other APIs"] --> KAFKA["Kafka topics<br/>raw ingestion bus"]
    CDC["Other sources via Debezium CDC"] --> KAFKA
    KAFKA --> RAW["raw<br/>JSON payloads by source"]
    RAW --> STG["stg<br/>Нормализация в табличный вид"]
    STG --> VAULT["vault<br/>Hub/Link/Satellite"]
    VAULT --> DM["datamart<br/>Бизнес-витрины"]
    STG --> SCD["analytics<br/>SCD Type 2 dimension"]
    DM --> BI["Metabase / BI"]
```

## 3) Оркестрация

```mermaid
flowchart LR
    S1["src_moex_ingestion"] --> D1["Dataset: stg/moex"]
    S2["src_cbr_ingestion"] --> D2["Dataset: stg/cbr"]
    S3["src_meteo_ingestion"] --> D3["Dataset: stg/meteo"]
    S4["src_<new_source>_ingestion"] --> D5["Dataset: stg/<new_source>"]
    D1 --> L1["vault_batch_load"]
    D2 --> L1
    D5 --> L4["optional downstream DAG"]
    L1 --> D4["Dataset: vault/loaded"]
    D4 --> L2["marts_publish_refresh"]
    D3 --> L3["weather_regime_dimension_build"]
```

Отдельно по расписанию: DAG `ods_*` публикуют ответы публичных REST в Kafka (`raw.ods.*`) и consumer пишет в `raw.ods_economic_payloads` / `raw.ods_territory_payloads` (см. `config/open_data_sources.example.yaml`).
