# Metabase: учётки студентов и доступ к PostgreSQL

См. также SQL: `sql/bootstrap/080_workshop_student_sandboxes.sql` — роли `workshop_student_01` … `workshop_student_30`, схемы `stu_01` … `stu_30` в БД `workshop`.

## Смена пароля

В Metabase **OSS** нет режима «обязательно сменить пароль при первом входе» для локальных пользователей. После входа: **шестерёнка → Account settings → Password**. На VM без `make` повторный провижининг: `./scripts/workshop.sh provision-students`.

## Автоматизация

При `docker compose --profile bi up -d` после старта Metabase выполняется контейнер **`workshop-provision-metabase`** (скрипт `scripts/provision_metabase.py`):

1. Первичная настройка сайта (если том `metabase-data` пустой): админ `workshop_admin@workshop.local` / `workshop_admin`.
2. Создание пользователей `workshop_student_XX@workshop.local` с паролями `workshop_stuXX`.
3. Добавление подключений **«Workshop PG (student XX)»** к PostgreSQL с учётными данными соответствующей роли.

Повторный запуск: `make provision-students` (идемпотентно).

Если Metabase уже настраивали вручную с другим паролем админа, задайте переменные окружения `METABASE_ADMIN_EMAIL` / `METABASE_ADMIN_PASSWORD` для сервиса `workshop-provision-metabase` или очистите `./metabase-data`.

## Что настроено в PostgreSQL

| Объект | Права студента |
|--------|----------------|
| `raw`, `stg`, `vault`, `datamart`, `analytics` | только **SELECT** (включая новые таблицы от `etl`) |
| БД `sourcedb`, схемы `public`, `inventory` | только **SELECT** |
| Схема **`stu_XX`** (свой номер) | **полные** права владельца (CREATE/ALTER/DROP своих таблиц) |
| Чужие схемы `stu_YY` | нет доступа |

Пароли по умолчанию: `workshop_stu01` … `workshop_stu30` (двузначный номер). **В проде смените** (`ALTER ROLE … PASSWORD`).

## Metabase (открытая версия)

У Metabase **два уровня**:

1. **Пользователь Metabase** (логин в веб-интерфейсе) — email `workshop_student_XX@workshop.local`.
2. **Подключение к БД** — для каждого студента заведено отдельное подключение с ролью `workshop_student_XX`, чтобы ограничения PostgreSQL применялись при запросах.

Чтобы ограничения PostgreSQL работали, в вопросах и нативном SQL выбирайте **своё** подключение «Workshop PG (student XX)», а не учётку `etl`.

### Упрощённый вариант (один общий technical user)

Если всем выдать подключение `etl`, **ограничения только SELECT на слоях не сохранятся** (у `etl` полные права). Для воркшопа с требованием «только чтение чужих таблиц» используйте **отдельные роли на студента** в Metabase (как в автоматической схеме выше).

## Проверка из контейнера

```bash
docker compose exec postgres psql -U workshop_student_01 -d workshop -c "SELECT current_user, COUNT(*) FROM raw.moex_iss_payloads;"
docker compose exec postgres psql -U workshop_student_01 -d workshop -c "CREATE TABLE stu_01.demo (id int); DROP TABLE stu_01.demo;"
```

Ожидание: первый запрос ок; таблицы в `stu_01` — ок; `DELETE FROM raw.moex_iss_payloads` — отказ (нет прав).
