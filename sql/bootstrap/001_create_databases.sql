-- Идемпотентно: безопасно и для первого initdb.d, и при повторном прогоне (сервис bootstrap-apply).
DO $$
BEGIN
  CREATE USER airflow WITH PASSWORD 'airflow';
EXCEPTION
  WHEN duplicate_object THEN NULL;
END
$$;

DO $$
BEGIN
  CREATE USER etl WITH PASSWORD 'etl';
EXCEPTION
  WHEN duplicate_object THEN NULL;
END
$$;

SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow')\gexec

SELECT 'CREATE DATABASE workshop OWNER etl'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'workshop')\gexec
