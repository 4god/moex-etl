# Архитектура ETL-воркшопа

Выбранная методология моделирования: **Data Vault 2.0**.

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
    C2[Airflow Layer DAGs<br/>triggered by datasets] --> D
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
    D1 --> L1["layer_vault_load"]
    D2 --> L1
    D5 --> L4["layer_<source>_pipeline (optional)"]
    L1 --> D4["Dataset: vault/loaded"]
    D4 --> L2["layer_datamart_publish"]
    D3 --> L3["layer_weather_scd2_build"]
```
