\connect workshop;

-- Идемпотентность raw при повторных прогонах: одинаковый (source, endpoint) не вставляется повторно (см. consume_raw_to_postgres).
-- Сначала убираем уже существующие полные дубли (оставляем строку с минимальным id).

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'moex_iss_payloads',
        'cbr_daily_payloads',
        'open_meteo_payloads',
        'ods_economic_payloads',
        'ods_territory_payloads',
        'ods_public_api_payloads',
        'ods_health_payloads',
        'ods_marketing_payloads'
    ]
    LOOP
        EXECUTE format($q$
            DELETE FROM raw.%I AS t1
            WHERE EXISTS (
                SELECT 1 FROM raw.%I AS t2
                WHERE t2.source = t1.source
                  AND t2.endpoint = t1.endpoint
                  AND t2.id < t1.id
            )
        $q$, t, t);
    END LOOP;
END
$$;

ALTER TABLE raw.moex_iss_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea
    GENERATED ALWAYS AS (digest(convert_to(COALESCE(endpoint, ''), 'UTF8'), 'sha256')) STORED;
ALTER TABLE raw.cbr_daily_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea
    GENERATED ALWAYS AS (digest(convert_to(COALESCE(endpoint, ''), 'UTF8'), 'sha256')) STORED;
ALTER TABLE raw.open_meteo_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea
    GENERATED ALWAYS AS (digest(convert_to(COALESCE(endpoint, ''), 'UTF8'), 'sha256')) STORED;
ALTER TABLE raw.ods_economic_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea
    GENERATED ALWAYS AS (digest(convert_to(COALESCE(endpoint, ''), 'UTF8'), 'sha256')) STORED;
ALTER TABLE raw.ods_territory_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea
    GENERATED ALWAYS AS (digest(convert_to(COALESCE(endpoint, ''), 'UTF8'), 'sha256')) STORED;
ALTER TABLE raw.ods_public_api_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea
    GENERATED ALWAYS AS (digest(convert_to(COALESCE(endpoint, ''), 'UTF8'), 'sha256')) STORED;
ALTER TABLE raw.ods_health_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea
    GENERATED ALWAYS AS (digest(convert_to(COALESCE(endpoint, ''), 'UTF8'), 'sha256')) STORED;
ALTER TABLE raw.ods_marketing_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea
    GENERATED ALWAYS AS (digest(convert_to(COALESCE(endpoint, ''), 'UTF8'), 'sha256')) STORED;

CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_moex_source_endpoint_hash
    ON raw.moex_iss_payloads (source, endpoint_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_cbr_source_endpoint_hash
    ON raw.cbr_daily_payloads (source, endpoint_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_meteo_source_endpoint_hash
    ON raw.open_meteo_payloads (source, endpoint_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_ods_economic_source_endpoint_hash
    ON raw.ods_economic_payloads (source, endpoint_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_ods_territory_source_endpoint_hash
    ON raw.ods_territory_payloads (source, endpoint_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_ods_public_source_endpoint_hash
    ON raw.ods_public_api_payloads (source, endpoint_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_ods_health_source_endpoint_hash
    ON raw.ods_health_payloads (source, endpoint_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_ods_marketing_source_endpoint_hash
    ON raw.ods_marketing_payloads (source, endpoint_hash);
