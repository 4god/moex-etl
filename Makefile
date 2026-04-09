.PHONY: docs-sphinx docs-lineage dbt-run dbt-test up down reset-db apply-bootstrap serve-docs

# Профиль BI (Metabase): по умолчанию как в README. Для минимального стека: make up STACK_PROFILES=
STACK_PROFILES ?= --profile bi

up:
	docker compose $(STACK_PROFILES) up -d --build

down:
	docker compose down

# Сброс томов Postgres/Kafka и перезапуск (чистая БД + повторный bootstrap)
reset-db:
	./scripts/reset_stack.sh

# Повторно прогнать sql/bootstrap к уже запущенному Postgres (обычно не нужно: то же делает сервис bootstrap-apply при каждом up)
apply-bootstrap:
	docker compose run --rm bootstrap-apply

# Статика docs/services-map.html (порт 8765; слушает 0.0.0.0). Другой порт: cd docs && python3 -m http.server 9000 --bind 0.0.0.0
serve-docs:
	cd docs && python3 -m http.server 8765 --bind 0.0.0.0

docs-sphinx:
	python3 -m venv .venv-docs
	. .venv-docs/bin/activate && pip install -r docs/sphinx/requirements.txt
	. .venv-docs/bin/activate && sphinx-build -b html docs/sphinx docs/sphinx/_build/html

docs-lineage:
	docker compose --profile dbt run --rm dbt dbt docs generate --project-dir /usr/app --profiles-dir /usr/app --target dev

dbt-run:
	docker compose --profile dbt run --rm dbt dbt run --project-dir /usr/app --profiles-dir /usr/app --target dev

dbt-test:
	docker compose --profile dbt run --rm dbt dbt test --project-dir /usr/app --profiles-dir /usr/app --target dev
