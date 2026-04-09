# ETL Workshop

Учебный проект для студентов-программистов: полный цикл Data Engineering на реальных API по методологии **Data Vault 2.0**. Воспроизводится через Docker.

**Поток данных:** внешние API и CDC → **Kafka** → в **PostgreSQL** слои **`raw` → `stg` → `vault` → `datamart`** (строятся **Airflow** и SQL из **`sql/tasks`**). После публикации витрин DAG **`dbt_analytics_build`** запускает **dbt**, который создаёт объекты в схеме **`analytics`** (модели в каталоге **`dbt/`**). **Metabase** читает и **`datamart`**, и **`analytics`**.

**Инструменты:** оркестрация и ingestion — **Airflow**; трансформации «до витрин» — **plain SQL** в **`sql/tasks`**; слой под отчёты и тесты dbt — **`dbt run` / `dbt test`**; BI — **Metabase**; CDC — **Debezium** (Kafka Connect).

Схемы и пояснения: [`docs/architecture.md`](docs/architecture.md).

---

## Студентам

Работа **на стенде преподавателя (VM):** в таблице ниже подставьте **`HOST`** = IP или домен, который дал преподаватель (порты должны быть открыты в файрволе). Обычно достаточно **браузера**; SSH на ВМ — только если так договорились.

- **Номер 01…30 и учётка:** получите свой номер у преподавателя или зафиксируйте его в [общей таблице (ФИО и номер)](https://docs.google.com/spreadsheets/d/1P1BxMey_72MALrLUw-qn5qS29AnI4akBimwo5NM5dmI/edit?usp=sharing), чтобы не пересечься с другими. По номеру **`XX`** ниже подставляются логин и пароль.

| Сервис | URL | Логин | Пароль |
|--------|-----|-------|--------|
| Airflow | `http://HOST:8080` | `workshop_student_XX` | `workshop_stuXX` |
| Metabase | `http://HOST:8085` | `workshop_student_XX@workshop.local` | `workshop_stuXX` |
| Kafka UI | `http://HOST:8090` | `workshop` | `workshop_kafka_ui` |
| PostgreSQL | `HOST:5432`, БД `workshop` или `sourcedb` | `workshop_student_XX` | `workshop_stuXX` |
| Kafka (топики) | `HOST:9092` | нет | — |

**`XX`** — ваш номер **01…30** (в пароле две цифры: `workshop_stu01` … `workshop_stu30`).

- **Metabase:** в вопросах выбирайте своё подключение **«Workshop PG (student XX)»**, не `etl`.
- **Свой код:** DAG — в [`dags/workshop_students/`](dags/workshop_students/); таблицы в Postgres — только в схеме **`stu_XX`**. Общие слои (`raw`, `stg`, …) — **только чтение**. Не трогайте чужие учётки, коннекторы Debezium и системные Connections в Airflow (`DWH`, `SOURCEDB`); на сервере не запускайте `docker compose down -v`.
- **Git:** клон репозитория, ветка от **`workshop/moex-etl`**, изменения через merge/pull request — по правилам преподавателя. После `git pull` новые или изменённые DAG в `dags/` обычно **не требуют** пересборки образов и полного рестарта стека — scheduler подхватывает файлы с диска (см. [§13](#13-команды-makefile-и-docker)).
- **Пароль:** после первого входа желательно сменить (Airflow — меню пользователя; Metabase — настройки аккаунта). Принудительного «смени сейчас» у локальных логинов нет.

**Свой ноутбук:** поднять Docker-стек локально — раздел [Быстрый старт](#3-быстрый-старт); там адреса с **`localhost`**.

---

<details>
<summary><strong>Оглавление</strong> (нажмите, чтобы развернуть)</summary>

[Студентам](#студентам) · доступ к ВМ, логины, git

1. [Документация и карта сервисов](#1-документация-и-карта-сервисов)
2. [Структура репозитория](#2-структура-репозитория)
3. [Быстрый старт](#3-быстрый-старт)
4. [Airflow, URL сервисов, DAG](#4-airflow-url-сервисов-dag)
5. [PostgreSQL: проверка данных](#5-postgresql-проверка-данных)
6. [Metabase (BI)](#6-metabase-bi)
7. [Debezium и несколько источников](#7-debezium-и-несколько-источников)
8. [Что делают DAG-и](#8-что-делают-dag-и)
9. [Data Vault по папкам](#9-data-vault-по-папкам)
10. [dbt](#10-dbt)
11. [Аналитика SQL: pg_stat_statements](#11-аналитика-sql-pg_stat_statements)
12. [Sphinx](#12-sphinx)
13. [Команды Makefile и Docker](#13-команды-makefile-и-docker)
14. [Публикация в GitHub](#14-публикация-в-github)
15. [CI: Workshop CI](#15-ci-workshop-ci-smoke--деплой)
16. [Учётки: преподаватель](#16-учётки-преподаватель)
17. [Дополнительные задания](#17-дополнительные-задания)

</details>

---

## 1. Документация и карта сервисов

| Файл | Назначение |
|------|------------|
| [`docs/architecture.md`](docs/architecture.md) | Инструменты и потоки |
| [`docs/data_model.md`](docs/data_model.md) | Модель Data Vault |
| [`docs/services-map.html`](docs/services-map.html) | Интерактивная карта контейнеров и портов (браузер на **вашем ПК**) |

<details>
<summary>Как открыть интерактивную карту <code>services-map.html</code></summary>

- Из **корня клона**: `make serve-docs` или `sh scripts/serve_docs.sh` → в браузере `http://localhost:8765/services-map.html`.
- Если `./scripts/serve_docs.sh: not found` — вы не в корне репозитория, не сделали `git pull`, или мешают CRLF: выполните `cd docs && python3 -m http.server 8765 --bind 0.0.0.0`.
- **С другого ПК по IP VM:** откройте порт **8765** в security group облака **или** используйте SSH-туннель: на VM держите сервер карт, на ПК `ssh -L 8765:127.0.0.1:8765 user@IP`, затем `http://localhost:8765/services-map.html`.
- Хост в ссылках на карте подставляется из адреса страницы автоматически.

</details>

---

## 2. Структура репозитория

<details>
<summary>Дерево каталогов (нажмите, чтобы развернуть)</summary>

```text
.
├── airflow/                 # Dockerfile, requirements.txt
├── config/                  # open_data_sources и др.
├── dags/                    # DAG Airflow (source, vault, marts, dbt, ods, …)
├── docs/                    # architecture, data_model, services-map.html, sphinx/
├── dbt/                     # dbt-проект
├── debezium/connectors/     # шаблоны коннекторов
├── scripts/                 # apply_bootstrap, reset_stack, serve_docs
├── sql/bootstrap/           # DDL при старте Postgres + bootstrap-apply
├── sql/tasks/               # SRC-*, DV-*, DE-* (SQL задачи)
├── Makefile
└── docker-compose.yml
```

</details>

---

## 3. Быстрый старт

Подключение к **чужой ВМ** (без Docker у себя): [Студентам](#студентам).

**Порты на хосте:** `5432`, `8080` (Airflow), `8081` (dbt docs), `8083` (Kafka Connect), `8085` (Metabase, profile `bi`), `8090` (Kafka UI), `9092` (Kafka).

**Подготовка и запуск (все ОС, из корня репозитория):**

```bash
cp .env.example .env
docker compose build
docker compose up -d kafka kafka-connect postgres airflow-init airflow-webserver airflow-scheduler airflow-triggerer dbt-docs
docker compose ps
```

<details>
<summary>Команды по ОС (macOS / Ubuntu / Windows)</summary>

**macOS**

```bash
cp .env.example .env
docker compose build
docker compose up -d kafka kafka-connect postgres airflow-init airflow-webserver airflow-scheduler airflow-triggerer dbt-docs
docker compose ps
```

**Ubuntu** (при необходимости `sudo` для docker; чтобы без sudo: `sudo usermod -aG docker $USER` и перелогиниться)

```bash
cp .env.example .env
sudo docker compose build
sudo docker compose up -d kafka kafka-connect postgres airflow-init airflow-webserver airflow-scheduler airflow-triggerer dbt-docs
sudo docker compose ps
```

**Windows PowerShell**

```powershell
Copy-Item .env.example .env
docker compose build
docker compose up -d kafka kafka-connect postgres airflow-init airflow-webserver airflow-scheduler airflow-triggerer dbt-docs
docker compose ps
```

**Windows CMD**

```cmd
copy .env.example .env
docker compose build
docker compose up -d kafka kafka-connect postgres airflow-init airflow-webserver airflow-scheduler airflow-triggerer dbt-docs
docker compose ps
```

**Git Bash / MSYS2** — те же команды, что для macOS.

<details>
<summary>Ошибка Docker: «failed to set up container networking: network … not found»</summary>

Часто после прерванного `docker compose up`, отмены job в CI или гонки при старте контейнеров. Попробуй:

```bash
docker compose down --remove-orphans
docker network prune -f
docker compose up -d --build --remove-orphans
```

Если ошибка повторяется, подними сеть по шагам (как в CI): сначала Kafka и Postgres, дождись `healthy`, затем **сервисы по одному** — иначе часто падает на следующем контейнере после `airflow-scheduler` / `kafka-connect`:

```bash
docker compose up -d kafka postgres
# дождись healthy у kafka и postgres, затем по очереди:
for s in kafka-connect kafka-ui bootstrap-apply airflow-init \
         airflow-webserver airflow-scheduler airflow-triggerer dbt-docs; do
  docker compose up -d --build --remove-orphans "$s"
  sleep 2
done
```

**Metabase (`bi`):** перечислять сервисы вручную не нужно. В `docker-compose.yml` у Metabase заданы `depends_on` для `airflow-webserver`, `airflow-scheduler`, `airflow-triggerer`, поэтому **`docker compose --profile bi up -d --build`** поднимает BI **после** старта Airflow, без отдельной второй команды.

Без Metabase / отдельного контейнера `dbt`: `docker compose up -d --build`.

**Пошаговый цикл `for s in …`** выше в этом блоке — только **запасной вариант**, если `docker compose up` стабильно падает с ошибкой сети.

В `docker-compose.yml` задано **явное имя сети** (`workshop_${COMPOSE_PROJECT_NAME}`), в CI задаётся уникальный `COMPOSE_PROJECT_NAME` на каждый run.

На macOS/Windows при повторении сбрось Docker (Restart Docker Desktop / «Clean / Purge data» в крайнем случае).

</details>

На Windows проект лучше держать на локальном диске (например `C:\work\etl_demo`), не в сетевой папке.

</details>

---

## 4. Airflow, URL сервисов, DAG

На **стенде преподавателя** подставьте его `HOST` вместо `localhost` (см. [Студентам](#студентам)).

| Сервис | URL | Примечание |
|--------|-----|------------|
| Airflow | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` |
| Kafka Connect (Debezium) | [http://localhost:8083](http://localhost:8083) | `curl http://localhost:8083/connectors` |
| Kafka UI | [http://localhost:8090](http://localhost:8090) | Сервис `kafka-ui` в compose |
| dbt docs | [http://localhost:8081](http://localhost:8081) | Сервис `dbt-docs` |

<details>
<summary>Airflow: Connections и Variables из Docker</summary>

Основной источник — блок `environment` у `x-airflow-common` в `docker-compose.yml` (переменные `AIRFLOW_CONN_*` и `AIRFLOW_VAR_*` попадают во все контейнеры Airflow).

**Connections** (в UI: Admin → Connections; идентификатор — нижний регистр имени после префикса):

| Conn id | Env в compose |
|---------|----------------|
| `dwh` | `AIRFLOW_CONN_DWH` |
| `sourcedb` | `AIRFLOW_CONN_SOURCEDB` |
| `http_open_meteo` | `AIRFLOW_CONN_HTTP_OPEN_METEO` |
| `postgres_browse` | `AIRFLOW_CONN_POSTGRES_BROWSE` (опционально, postgres-суперпользователь для отладки) |

**Variables** (через `Variable.get` и в UI; имя ключа — суффикс после `AIRFLOW_VAR_` в нижнем регистре с подчёркиваниями, например `KAFKA_BOOTSTRAP_SERVERS` → `kafka_bootstrap_servers`):

| Ключ (пример) | Env |
|----------------|-----|
| `kafka_bootstrap_servers` | `AIRFLOW_VAR_KAFKA_BOOTSTRAP_SERVERS` |
| `open_data_config_dir` | `AIRFLOW_VAR_OPEN_DATA_CONFIG_DIR` |
| `workshop_env` | `AIRFLOW_VAR_WORKSHOP_ENV` |
| `kafka_connect_url` | `AIRFLOW_VAR_KAFKA_CONNECT_URL` |
| `kafka_ui_url_internal` | `AIRFLOW_VAR_KAFKA_UI_URL_INTERNAL` |
| `kafka_ui_url_host` | `AIRFLOW_VAR_KAFKA_UI_URL_HOST` |
| `dbt_project_dir` | `AIRFLOW_VAR_DBT_PROJECT_DIR` |

Файл `config/airflow_variables.json` дублирует значения для ручного импорта в БД метаданных (например, если Airflow запущен без этих env):

```bash
docker compose exec airflow-webserver airflow variables import /opt/airflow/config/airflow_variables.json
```

Учти: при активных `AIRFLOW_VAR_*` приоритет у переменных окружения; импорт в БД имеет смысл для окружений без compose.

**Плагин воркшопа:** [`airflow/plugins/workshop_plugin.py`](airflow/plugins/workshop_plugin.py) регистрирует операторы `WorkshopJsonToKafkaOperator` и `WorkshopKafkaConsumeToRawOperator` (реализация в [`dags/common/operators/workshop_kafka.py`](dags/common/operators/workshop_kafka.py)). В UI: Admin → Plugins.

</details>

**Источники (запустить вручную в UI):** `src_moex_ingestion`, `src_cbr_ingestion`, `src_meteo_ingestion`.

**Дальше по Datasets:** `vault_batch_load` → `marts_publish_refresh`; `weather_regime_dimension_build` (meteo); опционально `ods_*_fetch` (в т.ч. World Bank, REST Countries, Eurostat, ООН, NASA APOD, Open-Meteo deferrable — см. `dags/ods_*.py`); после datamart — **`dbt_analytics_build`** (`dbt run` / `dbt test`).

После `git pull` или смены `airflow/requirements.txt` пересоберите Airflow:

```bash
docker compose build --no-cache airflow-webserver airflow-scheduler airflow-triggerer airflow-init
docker compose up -d airflow-init airflow-webserver airflow-scheduler airflow-triggerer
```

<details>
<summary>Нет DAG в UI — что проверить</summary>

1. `airflow-scheduler` и **`airflow-triggerer`** в статусе **Up** (`docker compose ps`). Triggerer нужен для deferrable DAG (например `ods_open_meteo_defer_fetch`). Иначе: `docker compose logs airflow-scheduler` / `docker compose logs airflow-triggerer`.
2. Compose запущен из **корня** репозитория (рядом `dags/`):

```bash
ls dags/*.py
docker compose exec airflow-webserver ls -la /opt/airflow/dags
```

3. Ошибки импорта:

```bash
docker compose exec airflow-webserver airflow dags list-import-errors
docker compose exec airflow-webserver airflow dags list
docker compose logs --tail=100 airflow-scheduler
```

</details>

---

## 5. PostgreSQL: проверка данных

<details>
<summary>Примеры <code>SELECT COUNT(*)</code> по слоям</summary>

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

</details>

---

## 6. Metabase (BI)

```bash
docker compose --profile bi up -d --build
```

Metabase стартует после Airflow (см. `depends_on` в `docker-compose.yml`).

- URL: [http://localhost:8085](http://localhost:8085) (в контейнере порт 3000).
- Учётки и подключения к БД — см. [Студентам](#студентам) и [§16](#16-учётки-преподаватель). В вопросах выберите **«Workshop PG (student XX)»**, не `etl`.
- Таблица для примеров: `datamart.dm_security_snapshot`.

<details>
<summary>Ручная настройка групп и прав (опционально)</summary>

Если нужны отдельные коллекции или ограничение видимости БД между студентами в OSS: **Admin → People / Permissions**. По умолчанию у каждого студента своё подключение PostgreSQL с ролью `workshop_student_XX` — запросы выполняются с ограничениями PG.

</details>

<details>
<summary>Read-only пользователь в PostgreSQL для BI (опционально)</summary>

```sql
CREATE USER bi_reader WITH PASSWORD 'bi_reader';
GRANT CONNECT ON DATABASE workshop TO bi_reader;
GRANT USAGE ON SCHEMA datamart TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA datamart TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA datamart GRANT SELECT ON TABLES TO bi_reader;
```

В Metabase указать `bi_reader` / `bi_reader`.

</details>

<details>
<summary>Пример SQL-запросов для дашборда (MOEX)</summary>

```sql
select secid, shortname, value_total, num_trades
from analytics.fct_moex_liquidity
order by value_total desc
limit 20;
```

```sql
select shortname, pct_change
from analytics.fct_moex_movers
order by pct_change desc
limit 20;
```

Визуализация: Bar chart, затем Save → Add to dashboard.

</details>

---

## 7. Debezium и несколько источников

1. Сервис `kafka-connect` уже в `docker-compose.yml`; Postgres поднят с `wal_level=logical` (см. `command` у `postgres`).
2. Bootstrap создаёт БД **`sourcedb`**, пользователей **`debezium`** и **`sourcedb_loader`**, OLTP-данные и справочники `ref_*` — `sql/bootstrap/070_debezium_sourcedb.sql`. Регулярный полный перегруз справочников — DAG **`sourcedb_reference_full_reload`** (connection `AIRFLOW_CONN_SOURCEDB`).
3. Конфиги в `debezium/connectors/`:
   - **`postgres-cdc-oltp.json`** — `public.customers`, `public.orders` → топики с префиксом `cdc.pg_oltp`.
   - **`postgres-cdc-inventory.json`** — `inventory.products`, `inventory.stock_movements` → `cdc.pg_inventory`.
4. Регистрация (два независимых CDC-источника на одну БД, разные replication slot):

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/connectors/postgres-cdc-oltp.json
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/connectors/postgres-cdc-inventory.json
```

5. Статус: `curl http://localhost:8083/connectors/postgres-cdc-oltp/status` (и аналогично для `postgres-cdc-inventory`).

Идея: новый источник → свои Kafka topics (`topic.prefix`) → стабильный downstream (raw/stg/vault/datamart). Подробнее — `debezium/connectors/README.md`.

---

## 8. Что делают DAG-и

| DAG | Файл | Поток |
|-----|------|--------|
| `src_moex_ingestion` | `source_moex_pipeline.py` | MOEX API → Kafka → raw → stg |
| `src_cbr_ingestion` | `source_cbr_pipeline.py` | CBR API → Kafka → raw → stg |
| `src_meteo_ingestion` | `source_meteo_pipeline.py` | Open-Meteo → Kafka → raw → stg |
| `vault_batch_load` | `vault_batch_load.py` | После STG MOEX+CBR |
| `marts_publish_refresh` | `marts_publish_refresh.py` | После vault |
| `weather_regime_dimension_build` | `weather_regime_dimension_build.py` | Meteo STG → analytics SCD2 |
| `ods_*` | `ods_*_fetch.py` | REST → Kafka → raw ODS |
| `sourcedb_reference_full_reload` | `sourcedb_reference_full_reload.py` | Полный перегруз справочников в `sourcedb` (ref_*) |
| `dbt_analytics_build` | `dbt_analytics_build.py` | dbt run/test |

SQL-пайплайны в `sql/tasks/*` (SRC-*, DV-*); one-off в `DE-*` DAG не вызывают автоматически.

---

## 9. Data Vault по папкам

- `sql/tasks/DV-210_vault_load/moex/…`
- `sql/tasks/DV-210_vault_load/cbr/…`
- `sql/tasks/DV-210_vault_load/meteo/…`

Склейка источников — по правилам DV и в витринах/datamart.

---

## 10. dbt

- **Роль в стеке:** dbt **не** грузит данные из API/Kafka — только выполняет SQL в Postgres. Источники для моделей — таблицы в **`stg`** и **`datamart`**; результат материализуется в схеме **`analytics`** (см. `dbt/profiles.yml`, `schema:`).
- **Оркестрация:** DAG `dbt_analytics_build` после Dataset datamart — `dbt run` / `dbt test` из образа Airflow (`/opt/airflow/dbt`).
- **Папки проекта:** в репозитории `models/staging/` и `models/marts/` — это **логика dbt**, не схемы Postgres; целевая схема по умолчанию — **`analytics`**.
- **Модели:** `fct_moex_liquidity`, `fct_moex_movers`, `fct_cbr_fx_rates`, `fct_moscow_weather_daily`, `fct_moex_weather_snapshot` и др.
- **Качество:** `schema.yml`, custom tests в `dbt/tests/`.

<details>
<summary>Ручной запуск dbt и dbt docs</summary>

```bash
docker compose --profile dbt run --rm dbt dbt run --project-dir /usr/app --profiles-dir /usr/app --target dev
docker compose --profile dbt run --rm dbt dbt test --project-dir /usr/app --profiles-dir /usr/app --target dev
```

Lineage (через Makefile): `make docs-lineage`

Отдельно docs serve:

```bash
docker compose --profile dbt run --rm dbt dbt docs generate --project-dir /usr/app --profiles-dir /usr/app --target dev
docker compose --profile dbt run --rm -p 8081:8081 dbt dbt docs serve --host 0.0.0.0 --port 8081 --project-dir /usr/app --profiles-dir /usr/app --target dev
```

</details>

---

## 11. Аналитика SQL: pg_stat_statements

В Postgres включён `pg_stat_statements`; в bootstrap — `050_pg_stat_statements.sql`, представление **`util.v_statement_log`**.

<details>
<summary>Примеры запросов</summary>

```sql
SELECT * FROM util.v_statement_log
WHERE query_text ILIKE '%vault%'
ORDER BY total_exec_time_ms DESC
LIMIT 20;

SELECT pg_stat_statements_reset();
```

Если том Postgres уже был, а `shared_preload_libraries` добавили позже — перезапустите Postgres, затем `docker compose up` (сервис **`bootstrap-apply`** догонит SQL). Подробнее: `sql/bootstrap/README.md`.

</details>

---

## 12. Sphinx

```bash
make docs-sphinx
```

HTML: `docs/sphinx/_build/html/index.html`.

---

## 13. Команды Makefile и Docker

**Установка `make` и `psql` на хост VM** (один раз): `sudo ./scripts/vm_install_host_tools.sh` — то же вызывает job деплоя в [Workshop CI](.github/workflows/workshop-ci.yml) после `git pull` (нужен sudo без пароля у пользователя SSH).

**На VM без `make`** (после установки пакетов) можно пользоваться и Makefile, и скриптом из корня репозитория (права на выполнение: `chmod +x scripts/workshop.sh`):

| Задача | Через make | Без make (bash) |
|--------|------------|-----------------|
| Подъём стека с BI (пересборка образов) | `make up` | `./scripts/workshop.sh up` |
| Подъём без `--build` | `make up-fast` | `./scripts/workshop.sh up-fast` |
| Остановка | `make down` | `./scripts/workshop.sh down` |
| Сброс томов и перезапуск | `make reset-db` | `./scripts/workshop.sh reset-db` |
| Повторный bootstrap SQL | `make apply-bootstrap` | `./scripts/workshop.sh apply-bootstrap` |
| Учётки студентов (Airflow + Metabase) | `make provision-students` | `./scripts/workshop.sh provision-students` |
| Рестарт Airflow (webserver + scheduler + triggerer) | `make reload-airflow` | `./scripts/workshop.sh reload-airflow` |
| Статика карты сервисов (`docs/`, порт 8765) | `make serve-docs` | `cd docs && python3 -m http.server 8765 --bind 0.0.0.0` |

Минимальный стек **без Metabase**: `STACK_PROFILES= ./scripts/workshop.sh up` (как `make up STACK_PROFILES=`).

**Что меняли → что запускать** (чтобы не гонять лишний `build` и не рестартовать весь стек без нужды):

| Изменения | Обычно достаточно |
|-----------|-------------------|
| `dags/`, `config/`, `sql/tasks/`, правки в уже смонтированных файлах | Ничего или `make up-fast`, если нужно поднять остановленный сервис. Новые DAG Airflow подхватывает scheduler с диска; полный рестарт контейнеров не обязателен. |
| «Застрял» UI или нужен жёсткий перечитать процессы Airflow | `make reload-airflow` |
| `sql/bootstrap/` (DDL, учётки, фундаментальные объекты) | `make apply-bootstrap` или полный `up` (сервис **`bootstrap-apply`** при `up` тоже догоняет новые `*.sql`) |
| `airflow/Dockerfile`, `airflow/requirements.txt`, `airflow/plugins/` (если не только volume), образы приложений | `make up` (с `--build`) или явно `docker compose build …` и перезапуск затронутых сервисов |
| `docker-compose.yml`, порты, новый сервис | `docker compose up -d` для нужных сервисов или `make up-fast` / `make up` по ситуации |

**Bootstrap:** при каждом `docker compose up` сервис **`bootstrap-apply`** прогоняет `sql/bootstrap/*.sql` (новые файлы подхватываются без ручного `psql`).

Полезные команды отладки:

```bash
docker compose logs -f airflow-scheduler airflow-triggerer
docker compose --profile dbt run --rm dbt dbt --version
```

Если при `make up` / `docker compose up` появляется **`failed to set up container networking: network … not found`** (часто у Metabase после prune или частичного перезапуска): полный останов проекта и подъём заново — `make up-clean`, или вручную `docker compose --profile bi --profile dbt down --remove-orphans`, затем снова `make up`.

---

## 14. Публикация в GitHub

Ветка по умолчанию для воркшопа: `workshop/moex-etl`.

```bash
git add .
git commit -m "Add MOEX ETL workshop project"
git remote add origin <your-github-repo-url>
git push -u origin workshop/moex-etl
```

---

## 15. CI: Workshop CI (smoke + деплой)

Файл: [`.github/workflows/workshop-ci.yml`](.github/workflows/workshop-ci.yml). В Actions: **Workshop CI**.

Кратко: при push в `workshop/moex-etl` сначала при необходимости гоняется **полный smoke** в CI (docker compose с нуля), затем **деплой на VPS** по SSH (`git pull`, `docker compose build`, `up`). Деплой **ждёт** окончания smoke, если тот запускался; повторные деплои **отменяют** предыдущие деплои, но **не** отменяют чужой smoke (разные `concurrency`-группы). Ручной запуск: **Run workflow**.

**Secrets / Variables:** `VM_HOST`, `VM_USER`, `VM_SSH_KEY`, `VM_DEPLOY_PATH` (путь к клону на сервере). На VM при деплое: [`scripts/vm_install_host_tools.sh`](scripts/vm_install_host_tools.sh) (`make` / `psql` на хосте; нужен `sudo -n` или один раз вручную `sudo ./scripts/vm_install_host_tools.sh`).

<details>
<summary>Подробности (paths-filter, отмены, SSH deploy)</summary>

1. **paths-filter** — решает, нужен ли тяжёлый reset по списку путей.
2. **reset-and-up** — только если пути затронуты. `concurrency: stack-reset-smoke-${{ github.ref }}`, `cancel-in-progress: true` — новый smoke отменяет предыдущий на той же ветке.
3. **deploy-vm** — не PR; `needs` smoke: ждёт завершения; деплой идёт и после успеха, и после провала smoke. Если smoke **skipped** — деплой всё равно. Если smoke **cancelled** — деплой этого прогона нет. `concurrency: deploy-workshop-vm-${{ github.ref }}`, `cancel-in-progress: true` — отменяются только старые деплои.

</details>

<details>
<summary>Полная настройка SSH-ключа и пользователя <code>deploy</code> на VPS</summary>

**1. Ключ на вашем ПК (только для деплоя):**

```bash
ssh-keygen -t ed25519 -f ~/.ssh/github_deploy_moex_etl -C "github-actions-deploy-moex-etl" -N ""
cat ~/.ssh/github_deploy_moex_etl.pub
```

Приватный ключ → Secret `VM_SSH_KEY` (целиком, с `BEGIN`/`END`). Публичный — в `authorized_keys` на сервере.

**2. Пользователь на VM (пример `deploy`):**

```bash
sudo adduser deploy
sudo usermod -aG docker deploy
sudo mkdir -p /home/deploy/.ssh
# вставить строку из .pub в authorized_keys
sudo chown -R deploy:deploy /home/deploy/.ssh
sudo chmod 700 /home/deploy/.ssh
sudo chmod 600 /home/deploy/.ssh/authorized_keys
```

Выдать `deploy` права на каталог клона, настроить `git` и ветку `workshop/moex-etl`. Для приватного репо — Deploy Key или доступ к `git pull`.

**Утилиты на хосте (`make`, `psql`):** при деплое из Actions выполняется [`scripts/vm_install_host_tools.sh`](scripts/vm_install_host_tools.sh). Для пользователя `deploy` добавьте sudo без пароля, например:

```bash
echo 'deploy ALL=(ALL) NOPASSWD: /usr/bin/apt-get, /usr/bin/dnf, /sbin/apk' | sudo tee /etc/sudoers.d/deploy-workshop
sudo chmod 440 /etc/sudoers.d/deploy-workshop
```

(Подставьте фактические пути к менеджеру пакетов на вашей ОС.) Вручную на уже развёрнутой VM: из корня клона `sudo ./scripts/vm_install_host_tools.sh`.

`VM_DEPLOY_PATH` = абсолютный путь к репозиторию (где лежит `docker-compose.yml`), без `/` в конце.

**3. Проверка с ПК:**

```bash
ssh -i ~/.ssh/github_deploy_moex_etl deploy@<IP_VM>
```

**4. Secrets в GitHub:** см. таблицу выше. Проверка: Actions → **Workshop CI** → Run workflow.

**Безопасность:** не коммитить приватный ключ; при утечке — новая пара и обновление Secret + `authorized_keys`.

</details>

---

## 16. Учётки: преподаватель

Студентам: таблица входов и правила — в начале: [§ Студентам](#студентам).

Учётки **01–30** создаются при `docker compose up` (Postgres — bootstrap; Airflow — `workshop-provision-airflow`; Metabase — `workshop-provision-metabase` с `--profile bi`). Повтор: `make provision-students` или `./scripts/workshop.sh provision-students`.

Права в БД: [`sql/bootstrap/080_workshop_student_sandboxes.sql`](sql/bootstrap/080_workshop_student_sandboxes.sql). Metabase: [`docs/metabase_student_setup.md`](docs/metabase_student_setup.md).

**Админы (не студенты):** Airflow `admin` / `admin`; Metabase `workshop_admin@workshop.local` / `workshop_admin`; Postgres `postgres` / `postgres`.

<details>
<summary>Локальный запуск у студента (localhost)</summary>

| Сервис | URL | Логин | Пароль |
|--------|-----|-------|--------|
| Airflow | http://localhost:8080 | `workshop_student_XX` | `workshop_stuXX` |
| Metabase | http://localhost:8085 | `workshop_student_XX@workshop.local` | `workshop_stuXX` |
| Kafka UI | http://localhost:8090 | `workshop` | `workshop_kafka_ui` |
| Postgres | localhost:5432 | `workshop_student_XX` | `workshop_stuXX` |
| Kafka Connect | http://localhost:8083 | без логина | — |

</details>

---

## 17. Дополнительные задания

<details>
<summary>Идеи для углубления</summary>

**Инфраструктура:** новый сервис в Compose (Jupyter, ClickHouse...) и настройка окружения; добавление алертинга упавших ранов в тг канал или по почте.

**Airflow:** ветвление в DAG, Datasets, DynamicTaskMapping, TriggerDagRunOperator, кастомный оператор в `airflow/plugins/`.

**Данные / SQL:** сложная витрина в `sql/tasks` или dbt; второй внешний API; watermark, SCD2, тесты данных; перенос витрин на ClickHouse с использованием его особенностей (движков например).

**Оптимизация:** партиции, индексы, разбор `EXPLAIN (ANALYZE, BUFFERS)`; идеи шардирования/read-replica.

</details>
