# MOEX ETL Workshop (1 hour)

Учебный проект для студентов-программистов: полный цикл Data Engineering на реальных API по методологии Data Vault 2.0.

- Источники: MOEX ISS API + CBR Daily API + Open-Meteo API (Москва)
- Шина ingestion: Kafka
- CDC/мультиконнект-слой: Debezium (Kafka Connect)
- Оркестрация: Airflow
- DWH: PostgreSQL
- Трансформации и quality checks: dbt
- BI: Metabase
- Моделирование: Data Vault 2.0 (`api -> kafka -> raw -> stg -> vault -> datamart`) + SCD Type 2

Подходит для воркшопа на 60 минут и легко воспроизводится через Docker.

## Что увидят студенты

- Как забирать данные из внешнего API по расписанию.
- Как использовать Kafka как буфер и шину между extract и raw-layer.
- Как подключать несколько источников через Debezium connectors.
- Как реализовать историчность измерения через SCD Type 2.
- Почему хранить `raw` JSON отдельно полезно для отладки и повторной обработки.
- Как парсить JSON в табличный вид (`stg`) SQL-запросами.
- Как строить Data Vault (Hub/Link/Satellite) и витрины (`datamart`) поверх него.
- Как запускать и мониторить пайплайн в Airflow.

## Архитектура и модель данных

Диаграммы находятся в:
- `docs/architecture.md` (инструменты и поток всего проекта)
- `docs/data_model.md` (модель Data Vault)

## Структура проекта

```text
.
├── airflow/
│   ├── Dockerfile
│   └── requirements.txt
├── dags/
│   ├── common/
│   │   └── pipeline_utils.py
│   ├── source_moex_pipeline.py
│   ├── source_cbr_pipeline.py
│   ├── source_meteo_pipeline.py
│   ├── layer_vault_pipeline.py
│   ├── layer_datamart_pipeline.py
│   └── layer_weather_scd2_pipeline.py
├── docs/
│   ├── architecture.md
│   ├── data_model.md
│   └── sphinx/
│       ├── conf.py
│       ├── index.rst
│       └── ...
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── moex/
│   │   ├── cbr/
│   │   ├── meteo/
│   │   ├── marts/
│   │   └── sources.yml
│   └── tests/
├── debezium/
│   └── connectors/
│       ├── README.md
│       └── postgres-source-template.json
├── sql/
│   ├── migrations/
│   │   ├── 001_create_databases.sql
│   │   ├── 010_init_foundation.sql
│   │   ├── 020_add_kafka_metadata_to_raw.sql
│   │   └── 030_add_open_meteo_source.sql
│   ├── pipelines/
│   │   ├── SRC-110_moex_stg_refresh/
│   │   ├── SRC-120_cbr_stg_refresh/
│   │   ├── SRC-130_meteo_stg_refresh/
│   │   ├── DV-210_vault_load/
│   │   │   ├── moex/hubs|links|satellites
│   │   │   ├── cbr/hubs|links|satellites
│   │   │   └── meteo/hubs|links|satellites
│   │   ├── DV-310_datamart_publish/
│   │   └── DV-320_scd2_weather/
│   └── tasks/
│       ├── README.md
│       └── DE-1234_example_ad_hoc/ (пример one-off задач)
├── .env.example
├── .gitignore
├── Makefile
└── docker-compose.yml
```

## Быстрый старт (для студентов)

### Запуск на разных платформах

Ниже минимальный набор инструментов и команды для старта на macOS, Ubuntu Linux и Windows.

#### 1) Что установить заранее

Общее для всех платформ:

- Docker + Docker Compose v2
- Git
- (опционально) `curl` для проверки Debezium API

Официальные источники:

- Docker Desktop (macOS / Windows): [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/)
- Docker Engine + Compose plugin (Ubuntu): [https://docs.docker.com/engine/install/ubuntu/](https://docs.docker.com/engine/install/ubuntu/)
- Git: [https://git-scm.com/downloads](https://git-scm.com/downloads)
- Windows Terminal (удобно для PowerShell/CMD): [https://apps.microsoft.com/detail/9N0DX20HK701](https://apps.microsoft.com/detail/9N0DX20HK701)
- MSYS2 (если хочешь Unix-like shell на Windows): [https://www.msys2.org/](https://www.msys2.org/)

#### 2) Команды запуска по ОС

##### macOS (Terminal / zsh)

```bash
cp .env.example .env
docker compose build
docker compose up -d kafka kafka-connect postgres airflow-init airflow-webserver airflow-scheduler dbt-docs
docker compose ps
```

##### Linux Ubuntu (bash)

Если Docker установлен через официальный репозиторий, иногда нужен запуск с `sudo`:

```bash
cp .env.example .env
sudo docker compose build
sudo docker compose up -d kafka kafka-connect postgres airflow-init airflow-webserver airflow-scheduler dbt-docs
sudo docker compose ps
```

Чтобы запускать без `sudo`, добавь пользователя в группу `docker` (после этого перелогинься):

```bash
sudo usermod -aG docker $USER
```

##### Windows PowerShell

```powershell
Copy-Item .env.example .env
docker compose build
docker compose up -d kafka kafka-connect postgres airflow-init airflow-webserver airflow-scheduler dbt-docs
docker compose ps
```

##### Windows CMD

```cmd
copy .env.example .env
docker compose build
docker compose up -d kafka kafka-connect postgres airflow-init airflow-webserver airflow-scheduler dbt-docs
docker compose ps
```

##### Windows Git Bash / MSYS2

```bash
cp .env.example .env
docker compose build
docker compose up -d kafka kafka-connect postgres airflow-init airflow-webserver airflow-scheduler dbt-docs
docker compose ps
```

Примечание для Windows: проект лучше хранить на локальном диске (например, `C:\work\etl_demo`), а не в сетевой папке, чтобы избежать проблем с Docker volume/performance.

### 0) Требования

- Docker Desktop
- Свободные порты: `5432`, `8080`, `8081`, `3000`, `8083`, `9092`

### 1) Подготовка

```bash
cp .env.example .env
docker compose build
docker compose up -d kafka kafka-connect postgres airflow-init airflow-webserver airflow-scheduler dbt-docs
```

Проверить, что контейнеры поднялись:

```bash
docker compose ps
```

### 2) Airflow

- URL: [http://localhost:8080](http://localhost:8080)
- Логин: `admin`
- Пароль: `admin`

Открыть и триггернуть source DAG-и:

- `src_moex_ingestion`
- `src_cbr_ingestion`
- `src_meteo_ingestion`

Дальше layer DAG-и запускаются автоматически через Airflow Datasets (data assets):

- `layer_vault_load` (после обновления STG из MOEX и CBR)
- `layer_datamart_publish` (после загрузки vault)
- `layer_weather_scd2_build` (после обновления meteo STG)

После успешного прогона DAG можно отдельно запустить dbt-модели и проверки качества.

Debezium Connect API:
- URL: [http://localhost:8083](http://localhost:8083)
- Проверка статуса: `curl http://localhost:8083/connectors`

dbt Docs:
- URL: [http://localhost:8081](http://localhost:8081)
- Поднимается автоматически сервисом `dbt-docs` в общем `docker compose up -d`

### 3) Проверка слоев в PostgreSQL

```bash
docker compose exec postgres psql -U etl -d workshop -c "SELECT COUNT(*) FROM raw.moex_iss_payloads;"
docker compose exec postgres psql -U etl -d workshop -c "SELECT COUNT(*) FROM raw.cbr_daily_payloads;"
docker compose exec postgres psql -U etl -d workshop -c "SELECT COUNT(*) FROM raw.open_meteo_payloads;"
docker compose exec postgres psql -U etl -d workshop -c "SELECT COUNT(*) FROM stg.moex_securities;"
docker compose exec postgres psql -U etl -d workshop -c "SELECT COUNT(*) FROM stg.moscow_weather_daily;"
docker compose exec postgres psql -U etl -d workshop -c "SELECT COUNT(*) FROM vault.hub_security;"
docker compose exec postgres psql -U etl -d workshop -c "SELECT COUNT(*) FROM datamart.dm_security_snapshot;"
docker compose exec postgres psql -U etl -d workshop -c "SELECT city_code, version_num, weather_regime, valid_from, valid_to, is_current FROM analytics.dim_moscow_weather_regime_scd2 ORDER BY version_num;"
```

### 4) BI в Metabase

Поднять BI отдельно (опционально, когда дойдете до дашбордов):

```bash
docker compose --profile bi up -d metabase
```

- URL: [http://localhost:3000](http://localhost:3000)
- При первом входе пройти onboarding и добавить БД PostgreSQL:
  - Host: `postgres`
  - Port: `5432`
  - DB: `workshop`
  - User: `etl`
  - Password: `etl`
- Таблица для дашборда: `datamart.dm_security_snapshot`

Рекомендуемые визуализации:
- Top-20 бумаг по `value_total`
- Top-20 по росту `pct_change`
- Сравнение `last_price_rub` и `last_price_usd`

### 5) Пользователи и права в Metabase (для группы студентов)

Metabase использует **email как логин**. Для воркшопа удобно разделить доступ через группы.

Быстрый сценарий:

1. **Создай группы**  
   `Admin settings -> People -> Groups`
   - `Instructors`
   - `Students`
   - (опционально) `Viewers`

2. **Добавь пользователей**  
   `Admin settings -> People -> Invite people`  
   Для каждого укажи email, имя и группу.

3. **Выдай доступ к базе**  
   `Admin settings -> Permissions -> Data`
   - `Instructors`: `Curate` (могут создавать вопросы/дашборды)
   - `Students`: `View data` (читают данные, строят простые вопросы)
   - `Viewers`: `No self-service` или `View only` (только просмотр готовых дашбордов)

4. **Ограничь схемы**
   В правах базы `Workshop DWH` можно:
   - оставить `Students` доступ только к схеме `datamart`,
   - скрыть `raw` и `stg`, чтобы не перегружать новичков.

5. **Ограничь коллекции**
   `Admin settings -> Permissions -> Collections`
   - `Students`: write только в `Workshop / Student Sandbox`
   - `Instructors`: write в общую `Workshop / Shared`
   - `Viewers`: read-only

Рекомендуемая модель для занятия:
- студенты читают только `datamart` и `analytics`;
- редактируют дашборды только в своей песочнице;
- общую витрину и общий dashboard меняют только преподаватели.

#### Усиление безопасности на уровне PostgreSQL (опционально)

Если хочешь реально запретить доступ к `raw/stg` даже при ошибке в BI-правах, сделай отдельного read-only пользователя и подключи Metabase к нему:

```sql
CREATE USER bi_reader WITH PASSWORD 'bi_reader';
GRANT CONNECT ON DATABASE workshop TO bi_reader;
GRANT USAGE ON SCHEMA datamart TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA datamart TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA datamart GRANT SELECT ON TABLES TO bi_reader;
```

И затем в Metabase укажи:
- User: `bi_reader`
- Password: `bi_reader`
- DB: `workshop`

### 6) Как быстро сделать простой дашборд в Metabase

Пример: дашборд "Top MOEX volumes" на `analytics.fct_moex_liquidity`.

1. Открой `New -> SQL query`.
2. Выбери базу `Workshop DWH`.
3. Выполни запрос:

```sql
select
  secid,
  shortname,
  value_total,
  num_trades
from analytics.fct_moex_liquidity
order by value_total desc
limit 20;
```

4. Нажми `Visualization -> Bar`:
   - X-axis: `shortname`
   - Y-axis: `value_total`
5. Нажми `Save` и назови вопрос `Top MOEX volumes (dbt)`.
6. Нажми `Add to dashboard` и создай `MOEX Workshop Dashboard`.

Второй быстрый график (рост/падение):

```sql
select
  shortname,
  pct_change
from analytics.fct_moex_movers
order by pct_change desc
limit 20;
```

## Сценарий воркшопа на 60 минут

- `0-10 мин`: архитектура, слои и роль Airflow.
- `10-20 мин`: запуск проекта через Docker.
- `20-35 мин`: разбор DAG и raw/stg/datamart SQL.
- `35-50 мин`: построение дашборда в Metabase.
- `50-60 мин`: идеи расширения и Q&A.

## Debezium для нескольких источников

Если нужно добавить еще источники (например, PostgreSQL/MySQL с CDC), используй Debezium Connect:

1. Подними `kafka-connect` (уже есть в `docker-compose.yml`).
2. Подготовь JSON-конфиг коннектора в `debezium/connectors/`.
3. Зарегистрируй коннектор через REST API:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/connectors/postgres-source-template.json
```

4. Проверить статус:

```bash
curl http://localhost:8083/connectors/postgres-source-demo/status
```

Идея архитектуры: каждый новый источник публикует события в свои Kafka topics, а downstream-слой (raw/stg/vault/datamart) остается стабильным.

## Что именно делают DAG-и

### Source DAG-ы (разделены по источникам)

- `dags/source_moex_pipeline.py`:
  - `src_moex_ingestion`: `MOEX API -> Kafka -> raw.moex_iss_payloads -> stg.moex_*`
- `dags/source_cbr_pipeline.py`:
  - `src_cbr_ingestion`: `CBR API -> Kafka -> raw.cbr_daily_payloads -> stg.cbr_fx_rates`
- `dags/source_meteo_pipeline.py`:
  - `src_meteo_ingestion`: `Open-Meteo API -> Kafka -> raw.open_meteo_payloads -> stg.moscow_weather_daily`

### Layer DAG-ы (запуск по data assets)

- `dags/layer_vault_pipeline.py`:
  - `layer_vault_load` запускается по Dataset-триггерам после `stg`-обновлений MOEX + CBR
- `dags/layer_datamart_pipeline.py`:
  - `layer_datamart_publish` запускается после `vault`-слоя
- `dags/layer_weather_scd2_pipeline.py`:
  - `layer_weather_scd2_build` запускается после `stg.moscow_weather_daily`
  - строит `analytics.dim_moscow_weather_regime_scd2` (SCD Type 2)

Все SQL из автоматизации DAG-и читают из `sql/pipelines/*`.
Папка `sql/tasks/*` в DAG-ах не используется.

## Data Vault структура по источникам

Чтобы модель была читаемой, Vault-логика разложена по источникам:

- `sql/pipelines/DV-210_vault_load/moex/hubs|links|satellites`
- `sql/pipelines/DV-210_vault_load/cbr/hubs|links|satellites`
- `sql/pipelines/DV-210_vault_load/meteo/hubs|links|satellites`

Объединение источников происходит только на правилах Data Vault (через общие hub/link) и в витринах/datamart.

## dbt: что добавлено

- Модели:
  - `analytics.fct_moex_liquidity` (MOEX)
  - `analytics.fct_moex_movers` (MOEX)
  - `analytics.fct_cbr_fx_rates` (CBR)
  - `analytics.fct_moscow_weather_daily` (Open-Meteo)
  - `analytics.fct_moex_weather_snapshot` (объединение в витрине)
- Конфигурация моделей:
  - `materialized='incremental'` (PostgreSQL-friendly инкрементальная загрузка)
  - `pre_hook`: очистка текущего `trade_date` перед догрузкой
  - `post_hook`: создание индексов и `ANALYZE` после загрузки
- Проверки качества:
  - generic tests в `dbt/models/marts/schema.yml`
  - custom tests в `dbt/tests/*.sql`

Локальный ручной запуск dbt (если нужно отдельно от Airflow):

```bash
docker compose --profile dbt run --rm dbt dbt run --project-dir /usr/app --profiles-dir /usr/app --target dev
docker compose --profile dbt run --rm dbt dbt test --project-dir /usr/app --profiles-dir /usr/app --target dev
```

Lineage и документация моделей через dbt docs (ручной режим, если нужен отдельно):

```bash
docker compose --profile dbt run --rm dbt dbt docs generate --project-dir /usr/app --profiles-dir /usr/app --target dev
docker compose --profile dbt run --rm -p 8081:8081 dbt dbt docs serve --host 0.0.0.0 --port 8081 --project-dir /usr/app --profiles-dir /usr/app --target dev
```

Открыть: `http://localhost:8081`

## Sphinx автодокументация

Sphinx-конфиг и страницы лежат в `docs/sphinx/`.

Сборка HTML-документации:

```bash
make docs-sphinx
```

Готовая документация появится в `docs/sphinx/_build/html/index.html`.

## Полезные команды

Перезапуск только Airflow:

```bash
docker compose restart airflow-webserver airflow-scheduler
```

Показать логи scheduler:

```bash
docker compose logs -f airflow-scheduler
```

Поднять все вместе (включая BI):

```bash
docker compose --profile bi up -d
```

Поднять dbt-контейнер на время выполнения команд:

```bash
docker compose --profile dbt run --rm dbt dbt --version
```

Сгенерировать dbt lineage через Makefile:

```bash
make docs-lineage
```

Остановить проект:

```bash
docker compose down
```

Сбросить все данные и начать заново:

```bash
docker compose down -v
```

## Публикация в GitHub

В проекте уже создана локальная ветка:

- `workshop/moex-etl`

Чтобы опубликовать ее в ваш GitHub-репозиторий:

```bash
git add .
git commit -m "Add MOEX ETL workshop project"
git remote add origin <your-github-repo-url>
git push -u origin workshop/moex-etl
```

## Идеи усложнения (домашка)

- Добавить инкрементальную загрузку с watermark.
- Историзировать витрины (SCD2 / snapshots).
- Сделать quality checks (например, через Great Expectations).
- Подключить ClickHouse и сравнить производительность с PostgreSQL.
- Добавить второй источник (например, ЦБ РФ) и объединить в витрине.
