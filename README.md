# MOEX ETL Workshop (1 hour)

Учебный проект для студентов-программистов: полный цикл Data Engineering на реальном API.

- Источник: MOEX ISS API (JSON)
- Оркестрация: Airflow
- DWH: PostgreSQL
- BI: Metabase
- Слои: `raw -> stg -> datamart`

Подходит для воркшопа на 60 минут и легко воспроизводится через Docker.

## Что увидят студенты

- Как забирать данные из внешнего API по расписанию.
- Почему хранить `raw` JSON отдельно полезно для отладки и повторной обработки.
- Как парсить JSON в табличный вид (`stg`) SQL-запросами.
- Как строить аналитическую витрину (`datamart`) и подключать BI.
- Как запускать и мониторить пайплайн в Airflow.

## Архитектура

Диаграммы находятся в `docs/architecture.md`.

## Структура проекта

```text
.
├── airflow/
│   ├── Dockerfile
│   └── requirements.txt
├── dags/
│   └── moex_workshop_etl.py
├── docs/
│   └── architecture.md
├── sql/
│   ├── bootstrap/
│   │   ├── 00_create_databases.sql
│   │   └── 01_init_workshop.sql
│   ├── 02_transform_stg.sql
│   └── 03_build_datamarts.sql
├── .env.example
├── .gitignore
└── docker-compose.yml
```

## Быстрый старт (для студентов)

### 0) Требования

- Docker Desktop
- Свободные порты: `5432`, `8080`, `3000`

### 1) Подготовка

```bash
cp .env.example .env
docker compose build
docker compose up -d
```

Проверить, что контейнеры поднялись:

```bash
docker compose ps
```

### 2) Airflow

- URL: [http://localhost:8080](http://localhost:8080)
- Логин: `admin`
- Пароль: `admin`

Открыть DAG `moex_etl_workshop` и нажать **Trigger DAG**.

### 3) Проверка слоев в PostgreSQL

```bash
docker compose exec postgres psql -U etl -d workshop -c "SELECT COUNT(*) FROM raw.moex_iss_payloads;"
docker compose exec postgres psql -U etl -d workshop -c "SELECT COUNT(*) FROM stg.moex_securities;"
docker compose exec postgres psql -U etl -d workshop -c "SELECT COUNT(*) FROM datamart.dm_moex_share_overview;"
```

### 4) BI в Metabase

- URL: [http://localhost:3000](http://localhost:3000)
- При первом входе пройти onboarding и добавить БД PostgreSQL:
  - Host: `postgres`
  - Port: `5432`
  - DB: `workshop`
  - User: `etl`
  - Password: `etl`
- Таблица для дашборда: `datamart.dm_moex_share_overview`

Рекомендуемые визуализации:
- Top-20 бумаг по `value_total`
- Top-20 по росту `pct_change`
- Тепловая карта `boardid x avg(pct_change)`

## Сценарий воркшопа на 60 минут

- `0-10 мин`: архитектура, слои и роль Airflow.
- `10-20 мин`: запуск проекта через Docker.
- `20-35 мин`: разбор DAG и raw/stg/datamart SQL.
- `35-50 мин`: построение дашборда в Metabase.
- `50-60 мин`: идеи расширения и Q&A.

## Что именно делает DAG

`dags/moex_workshop_etl.py`:
1. `extract_moex_raw`:
   - GET к `https://iss.moex.com/iss/engines/stock/markets/shares/securities.json`
   - Сохраняет полный JSON в `raw.moex_iss_payloads`.
2. `build_stg_layer`:
   - Берет последний raw payload.
   - Парсит секции `securities` и `marketdata` в `stg` таблицы.
3. `build_datamart_layer`:
   - Строит витрину `datamart.dm_moex_share_overview` с `pct_change`.

## Полезные команды

Перезапуск только Airflow:

```bash
docker compose restart airflow-webserver airflow-scheduler
```

Показать логи scheduler:

```bash
docker compose logs -f airflow-scheduler
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
