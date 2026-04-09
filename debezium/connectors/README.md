# Debezium connectors

Эта папка содержит шаблоны конфигов для Kafka Connect (Debezium).

**Предпосылки:** подняты `postgres` (с `wal_level=logical`), `kafka`, `kafka-connect`; применён bootstrap с `070_debezium_sourcedb.sql` (БД `sourcedb`, пользователь `debezium`, таблицы под CDC).

## Быстрый цикл работы

1. Запусти сервисы:

```bash
docker compose up -d kafka kafka-connect postgres
# при необходимости дождись bootstrap-apply (создаст sourcedb)
```

2. Зарегистрируй коннекторы (два независимых источника CDC на одну БД, разные слоты и topic prefix):

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/connectors/postgres-cdc-oltp.json

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/connectors/postgres-cdc-inventory.json
```

3. Проверяй список и статус:

```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/postgres-cdc-oltp/status
curl http://localhost:8083/connectors/postgres-cdc-inventory/status
```

4. Обновление конфига (пример для OLTP):

```bash
curl -X PUT http://localhost:8083/connectors/postgres-cdc-oltp/config \
  -H "Content-Type: application/json" \
  -d @debezium/connectors/postgres-cdc-oltp.json
```

## Файлы

| Файл | Коннектор | Таблицы `sourcedb` | Префикс топиков Kafka |
|------|-----------|-------------------|------------------------|
| `postgres-cdc-oltp.json` | `postgres-cdc-oltp` | `public.orders`, `public.customers`, `public.ref_geo_region`, `public.ref_customer_segment` | `cdc.pg_oltp.*` |
| `postgres-cdc-inventory.json` | `postgres-cdc-inventory` | `inventory.products`, `inventory.stock_movements` | `cdc.pg_inventory.*` |

## Примечания

- У каждого коннектора свой `name`, `slot.name` и `topic.prefix` / `database.server.name`.
- Старый шаблон `postgres-source-template.json` (хост `source-postgres`) заменён: Kafka Connect в compose смотрит на сервис **`postgres`**.
- После добавления таблиц в `table.include.list` обновите конфиг коннектора (`PUT .../config`) и дождитесь пересоздания publication.
- В production добавь отдельные Kafka topics с retention-политиками и ACL.
