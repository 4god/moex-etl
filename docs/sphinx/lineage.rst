Data Lineage
============

Для lineage используется ``dbt docs``:

1. Сгенерировать манифест и сайт:

.. code-block:: bash

   docker compose --profile dbt run --rm dbt dbt docs generate --project-dir /usr/app --profiles-dir /usr/app --target dev

2. Поднять локальный сервер документации:

.. code-block:: bash

   docker compose --profile dbt run --rm -p 8081:8081 dbt dbt docs serve --host 0.0.0.0 --port 8081 --project-dir /usr/app --profiles-dir /usr/app --target dev

3. Открыть:

- ``http://localhost:8081``

В интерфейсе видно:

- lineage моделей;
- column-level происхождение полей (через SQL моделей);
- upstream/downstream зависимые объекты.
