# Bootstrap (первичная инициализация Postgres)

Скрипты в этой папке монтируются в контейнер Postgres как `/docker-entrypoint-initdb.d` и выполняются **один раз** при первом создании тома данных (`postgres-data`). Порядок — лексикографический по имени файла (`001_*`, затем `010_*`, …).

Для «чистого» пересоздания схемы удалите том Postgres и поднимите `docker compose` снова.

Если том уже существует и добавлен новый файл (например `040_*`), выполните его вручную один раз:  
`docker compose exec -i postgres psql -U etl -d workshop < sql/bootstrap/040_open_data_landing.sql`
