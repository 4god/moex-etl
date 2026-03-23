.PHONY: docs-sphinx docs-lineage dbt-run dbt-test

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
