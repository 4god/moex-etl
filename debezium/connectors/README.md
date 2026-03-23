# Debezium connectors

Эта папка содержит шаблоны конфигов для Kafka Connect (Debezium).

## Быстрый цикл работы

1. Запусти сервис:

```bash
docker compose up -d kafka kafka-connect
```

2. Зарегистрируй коннектор:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/connectors/postgres-source-template.json
```

3. Проверяй список и статус:

```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/postgres-source-demo/status
```

4. Обновление конфига:

```bash
curl -X PUT http://localhost:8083/connectors/postgres-source-demo/config \
  -H "Content-Type: application/json" \
  -d @debezium/connectors/postgres-source-template.json
```

## Примечания

- Для каждого нового источника создавай отдельный JSON-файл.
- У каждого коннектора должен быть уникальный `name` и `database.server.name`.
- В production добавь отдельные Kafka topics с retention-политиками и ACL.
