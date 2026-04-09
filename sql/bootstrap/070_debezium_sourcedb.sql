-- OLTP-песочница для Debezium (отдельная БД, не workshop).
-- Требует PostgreSQL с wal_level=logical (см. docker-compose: command postgres -c ...).

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
        CREATE ROLE debezium WITH LOGIN PASSWORD 'debezium' REPLICATION;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sourcedb_loader') THEN
        CREATE ROLE sourcedb_loader WITH LOGIN PASSWORD 'sourcedb_loader';
    END IF;
END
$$;

SELECT format('CREATE DATABASE %I OWNER postgres', 'sourcedb')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'sourcedb')\gexec

GRANT CONNECT ON DATABASE sourcedb TO debezium;
GRANT CONNECT ON DATABASE sourcedb TO sourcedb_loader;

\c sourcedb

CREATE SCHEMA IF NOT EXISTS inventory;

-- Справочники (полный перегруз из Airflow DAG sourcedb_reference_full_reload; владелец — sourcedb_loader).
CREATE TABLE IF NOT EXISTS public.ref_geo_region (
    code TEXT PRIMARY KEY,
    name_local TEXT NOT NULL,
    name_en TEXT,
    sort_order INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS public.ref_customer_segment (
    code TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    default_discount_pct NUMERIC(5, 2) NOT NULL DEFAULT 0,
    sort_order INTEGER NOT NULL DEFAULT 0
);

ALTER TABLE public.ref_geo_region OWNER TO sourcedb_loader;
ALTER TABLE public.ref_customer_segment OWNER TO sourcedb_loader;

GRANT SELECT ON TABLE public.ref_geo_region, public.ref_customer_segment TO debezium;

CREATE TABLE IF NOT EXISTS public.customers (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    country TEXT,
    geo_code TEXT,
    segment_code TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE public.customers ADD COLUMN IF NOT EXISTS geo_code TEXT;
ALTER TABLE public.customers ADD COLUMN IF NOT EXISTS segment_code TEXT;

CREATE TABLE IF NOT EXISTS public.orders (
    id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES public.customers (id),
    order_total NUMERIC(18, 2),
    status TEXT NOT NULL DEFAULT 'new',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inventory.products (
    id BIGSERIAL PRIMARY KEY,
    sku TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    unit_price NUMERIC(18, 2)
);

CREATE TABLE IF NOT EXISTS inventory.stock_movements (
    id BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES inventory.products (id),
    delta_qty INTEGER NOT NULL,
    reason TEXT,
    moved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Владелец транзакционных таблиц — debezium (publication filtered).
ALTER TABLE public.customers OWNER TO debezium;
ALTER TABLE public.orders OWNER TO debezium;
ALTER TABLE inventory.products OWNER TO debezium;
ALTER TABLE inventory.stock_movements OWNER TO debezium;

ALTER SEQUENCE public.customers_id_seq OWNER TO debezium;
ALTER SEQUENCE public.orders_id_seq OWNER TO debezium;
ALTER SEQUENCE inventory.products_id_seq OWNER TO debezium;
ALTER SEQUENCE inventory.stock_movements_id_seq OWNER TO debezium;

GRANT USAGE ON SCHEMA public, inventory TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public, inventory TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public, inventory
    GRANT SELECT ON TABLES TO debezium;

-- Стартовое наполнение справочников (как в DAG; идемпотентно через TRUNCATE при повторе только вручную).
INSERT INTO public.ref_geo_region (code, name_local, name_en, sort_order) VALUES
    ('RU-MOW', 'Москва', 'Moscow', 10),
    ('RU-SPB', 'Санкт-Петербург', 'Saint Petersburg', 20),
    ('RU-NW', 'Северо-Запад', 'North-West', 30),
    ('RU-CFD', 'Центральный ФО', 'Central FD', 40),
    ('RU-SFD', 'Южный ФО', 'Southern FD', 50),
    ('RU-PFD', 'Приволжский ФО', 'Volga FD', 60),
    ('RU-URAL', 'Уральский ФО', 'Ural FD', 70),
    ('RU-SIB', 'Сибирский ФО', 'Siberian FD', 80),
    ('RU-DV', 'Дальневосточный ФО', 'Far Eastern FD', 90),
    ('KZ-ALA', 'Алматы', 'Almaty', 100),
    ('KZ-AST', 'Астана', 'Astana', 110),
    ('BY-MINSK', 'Минск', 'Minsk', 120),
    ('GE-TBS', 'Тбилиси', 'Tbilisi', 130),
    ('AM-YVN', 'Ереван', 'Yerevan', 140),
    ('AZ-BAK', 'Баку', 'Baku', 150)
ON CONFLICT (code) DO NOTHING;

INSERT INTO public.ref_customer_segment (code, label, default_discount_pct, sort_order) VALUES
    ('RETAIL', 'Розница', 0.0, 10),
    ('SMB', 'Малый бизнес', 3.0, 20),
    ('CORP', 'Корпоративный', 7.0, 30),
    ('WHOLESALE', 'Опт', 5.0, 40),
    ('GOV', 'Госсектор', 0.0, 50),
    ('PARTNER', 'Партнёр', 12.0, 60)
ON CONFLICT (code) DO NOTHING;

-- Демо-строки и массовое OLTP-наполнение (однократно, пока мало данных).
INSERT INTO public.customers (name, email, country, geo_code, segment_code)
SELECT 'CDC Demo Customer', 'cdc-demo@local', 'RU', 'RU-MOW', 'RETAIL'
WHERE NOT EXISTS (SELECT 1 FROM public.customers LIMIT 1);

INSERT INTO public.customers (name, email, country, geo_code, segment_code)
SELECT
    'Bulk Customer ' || g.i,
    'bulk' || g.i || '@sourcedb.demo',
    (ARRAY['RU', 'KZ', 'BY', 'GE', 'AM', 'AZ'])[1 + ((g.i - 1) % 6)],
    (ARRAY['RU-MOW', 'RU-SPB', 'KZ-ALA', 'BY-MINSK', 'RU-CFD', 'RU-PFD'])[1 + ((g.i - 1) % 6)],
    (ARRAY['RETAIL', 'SMB', 'CORP', 'WHOLESALE', 'PARTNER'])[1 + ((g.i - 1) % 5)]
FROM generate_series(1, 48) AS g(i)
WHERE (SELECT count(*)::bigint FROM public.customers) < 8;

INSERT INTO public.orders (customer_id, order_total, status)
SELECT c.id,
       (100 + (c.id % 47) * 17)::NUMERIC(18, 2),
       (ARRAY['new', 'paid', 'shipped', 'delivered', 'cancelled'])[1 + (c.id % 5)]
FROM public.customers c
WHERE NOT EXISTS (
    SELECT 1 FROM public.orders o WHERE o.customer_id = c.id
);

INSERT INTO inventory.products (sku, title, unit_price)
SELECT 'SKU-DEMO-1', 'Warehouse demo item', 49.99::NUMERIC(18, 2)
WHERE NOT EXISTS (SELECT 1 FROM inventory.products LIMIT 1);

INSERT INTO inventory.products (sku, title, unit_price)
SELECT
    'SKU-BULK-' || LPAD(g.i::text, 4, '0'),
    'Demo product ' || g.i,
    (9.99 + (g.i % 20))::NUMERIC(18, 2)
FROM generate_series(1, 24) AS g(i)
WHERE (SELECT count(*)::bigint FROM inventory.products) < 5;

INSERT INTO inventory.stock_movements (product_id, delta_qty, reason)
SELECT p.id, 10 + (p.id % 50), 'bootstrap'
FROM inventory.products p
WHERE NOT EXISTS (SELECT 1 FROM inventory.stock_movements LIMIT 1)
ORDER BY p.id
LIMIT 12;
