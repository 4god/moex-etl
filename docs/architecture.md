# Архитектура ETL-воркшопа

Выбранная методология моделирования: **Data Vault 2.0**.

**Интерактивная карта контейнеров и портов** (откройте файл в браузере): [`services-map.html`](services-map.html) — узлы перетаскиваются, по клику — ссылки на UI и подсказки по профилям `bi` / `dbt`.

## 1) Диаграмма всего проекта (инструменты и взаимодействие)

**Смысл:** **Airflow** оркестрирует загрузку и вызывает **plain SQL** (`sql/tasks`) для слоёв до **`datamart`**. **dbt** — отдельный шаг: читает уже лежащие в Postgres **`stg`/`datamart`**, пишет в **`analytics`**. **Metabase** смотрит на **`datamart`** и **`analytics`**.

```mermaid
flowchart TB
    subgraph src[Источники]
        A[MOEX / CBR / Open-Meteo / др. API]
        J[OLTP и др. БД]
    end
    subgraph ingest[Загрузка]
        C[Airflow: source DAGs, ODS, CDC-обвязка]
        DBC[Debezium Connect]
    end
    K[(Kafka topics)]
    subgraph pg[PostgreSQL workshop]
        R[raw]
        STG[stg]
        V[vault]
        DM[datamart]
        AN[analytics]
    end
    DBT[dbt run / test<br/>DAG dbt_analytics_build]
    MB[Metabase]

    src --> ingest
    J --> DBC --> K
    C --> K
    C --> R
    C --> STG
    C --> V
    C --> DM
    K --> R
    STG --> DBT
    DM --> DBT
    DBT --> AN
    DM --> MB
    AN --> MB
```

Пояснение к стрелкам **Airflow → слои Postgres:** на схеме показано кратко; фактически большая часть шагов **`raw` → `stg` → `vault` → `datamart`** выполняется SQL-файлами из **`sql/tasks`**, вызываемыми из DAG (см. также раздел «Оркестрация» ниже).

## 2) Поток данных по слоям (где plain SQL, где dbt)

| Слой в Postgres | Как появляется |
|-----------------|----------------|
| `raw` … `datamart` | **Airflow** + **`sql/tasks`** (и bootstrap), без dbt |
| `analytics` | **dbt** (`dbt run`), после готовности витрин в `datamart` |

```mermaid
flowchart TB
    API["API + open data"] --> KAFKA["Kafka"]
    CDC["CDC Debezium"] --> KAFKA
    KAFKA --> RAW["raw"]
    AF["Airflow DAG + sql/tasks"] --> RAW
    AF --> STG["stg"]
    AF --> VAULT["vault"]
    AF --> DM["datamart<br/>витрины DV / SQL"]
    STG --> DBT["dbt: staging/marts модели"]
    DM --> DBT
    DBT --> AN["analytics<br/>факты/вью для BI"]
    DM --> BI["Metabase"]
    AN --> BI
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

После публикации витрин (Dataset **`datamart/published`**) в ход идёт DAG **`dbt_analytics_build`**: **`dbt run`** / **`dbt test`** → схема **`analytics`**.

Отдельно по расписанию: DAG `ods_*` публикуют ответы публичных REST в Kafka (`raw.ods.*`) и consumer пишет в `raw.ods_economic_payloads`, `raw.ods_territory_payloads` и при необходимости `raw.ods_public_api_payloads` (см. `config/open_data_sources.example.yaml`).
