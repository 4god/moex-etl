# Bootstrap (первичная инициализация Postgres)

Скрипты в этой папке монтируются в контейнер Postgres как `/docker-entrypoint-initdb.d` и выполняются **один раз** при первом создании тома данных (`postgres-data`). Порядок — лексикографический по имени файла (`001_*`, затем `010_*`, …).

**Повторное применение без ручного `psql`:** сервис **`bootstrap-apply`** в `docker-compose.yml` после готовности Postgres прогоняет **все** `sql/bootstrap/*.sql` в том же порядке. Скрипты сделаны идемпотентными (в т.ч. `001_create_databases.sql`), поэтому новый файл в этой папке подхватывается при следующем `docker compose up` без отдельных команд.

Для «чистого» пересоздания данных: `make reset-db` или `./scripts/reset_stack.sh` (сброс томов и перезапуск).

Ручной прогон одного файла по-прежнему возможен, если нужно вне compose:  
`docker compose exec -i postgres psql -U postgres -d postgres -f /path/to/file.sql` (или смонтированный путь).

Для **`050_pg_stat_statements.sql`**: расширение требует `shared_preload_libraries=pg_stat_statements` в `command` сервиса `postgres` (уже в репозитории); после смены `command` перезапустите контейнер Postgres, затем достаточно `docker compose up` — bootstrap-apply догонит скрипт.
