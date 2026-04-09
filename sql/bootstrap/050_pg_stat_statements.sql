\connect workshop;

-- Агрегированная статистика по SQL (нормализованный текст, без каждого литерала).
-- Требует shared_preload_libraries=pg_stat_statements в postgresql.conf (см. docker-compose).
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

GRANT pg_read_all_stats TO etl;

CREATE SCHEMA IF NOT EXISTS util;

CREATE OR REPLACE VIEW util.v_statement_log AS
SELECT
    d.datname AS database_name,
    rol.rolname AS role_name,
    s.query AS query_text,
    s.calls,
    s.rows,
    round(s.total_exec_time::numeric, 3) AS total_exec_time_ms,
    round(s.mean_exec_time::numeric, 3) AS mean_exec_time_ms,
    round(s.stddev_exec_time::numeric, 3) AS stddev_exec_time_ms,
    s.min_exec_time AS min_exec_time_ms,
    s.max_exec_time AS max_exec_time_ms,
    s.queryid
FROM pg_stat_statements s
LEFT JOIN pg_roles rol ON rol.oid = s.userid
LEFT JOIN pg_database d ON d.oid = s.dbid;

ALTER VIEW util.v_statement_log OWNER TO etl;
GRANT SELECT ON util.v_statement_log TO etl;

COMMENT ON VIEW util.v_statement_log IS
    'Обёртка над pg_stat_statements: кто (role_name), куда (database_name), какой запрос (query_text), сколько раз и сколько времени. '
    'Агрегаты по нормализованному тексту, не каждый вызов. Построчный журнал SQL — в логах сервера (log_statement, см. docker-compose и README).';
