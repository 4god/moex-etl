-- Песочницы для студентов (30 учёток): только чтение слоёв raw/stg/vault/datamart/analytics + sourcedb;
-- полные права только в своей схеме stu_XX в workshop.
-- Пароли по умолчанию: workshop_stu01 … workshop_stu30 — смените в проде: ALTER ROLE … PASSWORD '…';

\connect workshop

DO $$
DECLARE
    i INT;
    rname TEXT;
    sname TEXT;
    sch TEXT;
BEGIN
    FOR i IN 1..30 LOOP
        rname := 'workshop_student_' || LPAD(i::text, 2, '0');
        sname := 'stu_' || LPAD(i::text, 2, '0');
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = rname) THEN
            EXECUTE format(
                'CREATE ROLE %I LOGIN PASSWORD %L',
                rname,
                'workshop_stu' || LPAD(i::text, 2, '0')
            );
        END IF;
        -- Схема-песочница: владелец — роль студента (CREATE/DROP своих таблиц).
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I AUTHORIZATION %I', sname, rname);
        EXECUTE format('ALTER SCHEMA %I OWNER TO %I', sname, rname);
    END LOOP;
END
$$;

DO $$
DECLARE
    i INT;
    rname TEXT;
BEGIN
    FOR i IN 1..30 LOOP
        rname := 'workshop_student_' || LPAD(i::text, 2, '0');
        EXECUTE format('GRANT CONNECT ON DATABASE workshop TO %I', rname);
    END LOOP;
END
$$;

-- Чтение существующих витрин и слоёв (без INSERT/UPDATE/DELETE/TRUNCATE — не выдаём).
DO $$
DECLARE
    i INT;
    rname TEXT;
    sch TEXT;
BEGIN
    FOR sch IN SELECT unnest(ARRAY['raw', 'stg', 'vault', 'datamart', 'analytics'])
    LOOP
        FOR i IN 1..30 LOOP
            rname := 'workshop_student_' || LPAD(i::text, 2, '0');
            EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', sch, rname);
            EXECUTE format(
                'GRANT SELECT ON ALL TABLES IN SCHEMA %I TO %I',
                sch,
                rname
            );
            EXECUTE format(
                'GRANT SELECT ON ALL SEQUENCES IN SCHEMA %I TO %I',
                sch,
                rname
            );
        END LOOP;
    END LOOP;
END
$$;

-- Новые таблицы, которые создаёт etl в слоях — студенты видят их автоматически.
DO $$
DECLARE
    i INT;
    rname TEXT;
    sch TEXT;
BEGIN
    FOR sch IN SELECT unnest(ARRAY['raw', 'stg', 'vault', 'datamart', 'analytics'])
    LOOP
        FOR i IN 1..30 LOOP
            rname := 'workshop_student_' || LPAD(i::text, 2, '0');
            EXECUTE format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE etl IN SCHEMA %I GRANT SELECT ON TABLES TO %I',
                sch,
                rname
            );
            EXECUTE format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE etl IN SCHEMA %I GRANT SELECT ON SEQUENCES TO %I',
                sch,
                rname
            );
        END LOOP;
    END LOOP;
END
$$;

-- Доступ к чужим песочницам не выдаём (схемы stu_* только владелец + суперпользователь).

\c sourcedb

-- Чтение CDC-данных в sourcedb (существующие и будущие таблицы debezium).
DO $$
DECLARE
    i INT;
    rname TEXT;
BEGIN
    FOR i IN 1..30 LOOP
        rname := 'workshop_student_' || LPAD(i::text, 2, '0');
        EXECUTE format('GRANT CONNECT ON DATABASE sourcedb TO %I', rname);
        EXECUTE format('GRANT USAGE ON SCHEMA public, inventory TO %I', rname);
        EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA public TO %I', rname);
        EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA inventory TO %I', rname);
        EXECUTE format('GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO %I', rname);
        EXECUTE format('GRANT SELECT ON ALL SEQUENCES IN SCHEMA inventory TO %I', rname);
    END LOOP;
END
$$;

DO $$
DECLARE
    i INT;
    rname TEXT;
BEGIN
    FOR i IN 1..30 LOOP
        rname := 'workshop_student_' || LPAD(i::text, 2, '0');
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES FOR ROLE debezium IN SCHEMA public GRANT SELECT ON TABLES TO %I',
            rname
        );
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES FOR ROLE debezium IN SCHEMA public GRANT SELECT ON SEQUENCES TO %I',
            rname
        );
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES FOR ROLE debezium IN SCHEMA inventory GRANT SELECT ON TABLES TO %I',
            rname
        );
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES FOR ROLE debezium IN SCHEMA inventory GRANT SELECT ON SEQUENCES TO %I',
            rname
        );
    END LOOP;
END
$$;
