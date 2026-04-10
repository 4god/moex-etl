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

-- Хэш endpoint для уникального индекса (source, endpoint_hash).
-- GENERATED STORED требует immutable-выражение; надёжнее задать столбец триггером (md5 UTF-8 строки endpoint при кодировке БД UTF8).
CREATE OR REPLACE FUNCTION raw.set_endpoint_hash_from_endpoint()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.endpoint_hash := decode(md5(COALESCE(NEW.endpoint, '')), 'hex');
    RETURN NEW;
END;
$$;

ALTER TABLE raw.moex_iss_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea;
ALTER TABLE raw.cbr_daily_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea;
ALTER TABLE raw.open_meteo_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea;
ALTER TABLE raw.ods_economic_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea;
ALTER TABLE raw.ods_territory_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea;
ALTER TABLE raw.ods_public_api_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea;
ALTER TABLE raw.ods_health_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea;
ALTER TABLE raw.ods_marketing_payloads
    ADD COLUMN IF NOT EXISTS endpoint_hash bytea;

DROP TRIGGER IF EXISTS trg_raw_set_endpoint_hash ON raw.moex_iss_payloads;
CREATE TRIGGER trg_raw_set_endpoint_hash
    BEFORE INSERT OR UPDATE OF endpoint ON raw.moex_iss_payloads
    FOR EACH ROW EXECUTE FUNCTION raw.set_endpoint_hash_from_endpoint();

DROP TRIGGER IF EXISTS trg_raw_set_endpoint_hash ON raw.cbr_daily_payloads;
CREATE TRIGGER trg_raw_set_endpoint_hash
    BEFORE INSERT OR UPDATE OF endpoint ON raw.cbr_daily_payloads
    FOR EACH ROW EXECUTE FUNCTION raw.set_endpoint_hash_from_endpoint();

DROP TRIGGER IF EXISTS trg_raw_set_endpoint_hash ON raw.open_meteo_payloads;
CREATE TRIGGER trg_raw_set_endpoint_hash
    BEFORE INSERT OR UPDATE OF endpoint ON raw.open_meteo_payloads
    FOR EACH ROW EXECUTE FUNCTION raw.set_endpoint_hash_from_endpoint();

DROP TRIGGER IF EXISTS trg_raw_set_endpoint_hash ON raw.ods_economic_payloads;
CREATE TRIGGER trg_raw_set_endpoint_hash
    BEFORE INSERT OR UPDATE OF endpoint ON raw.ods_economic_payloads
    FOR EACH ROW EXECUTE FUNCTION raw.set_endpoint_hash_from_endpoint();

DROP TRIGGER IF EXISTS trg_raw_set_endpoint_hash ON raw.ods_territory_payloads;
CREATE TRIGGER trg_raw_set_endpoint_hash
    BEFORE INSERT OR UPDATE OF endpoint ON raw.ods_territory_payloads
    FOR EACH ROW EXECUTE FUNCTION raw.set_endpoint_hash_from_endpoint();

DROP TRIGGER IF EXISTS trg_raw_set_endpoint_hash ON raw.ods_public_api_payloads;
CREATE TRIGGER trg_raw_set_endpoint_hash
    BEFORE INSERT OR UPDATE OF endpoint ON raw.ods_public_api_payloads
    FOR EACH ROW EXECUTE FUNCTION raw.set_endpoint_hash_from_endpoint();

DROP TRIGGER IF EXISTS trg_raw_set_endpoint_hash ON raw.ods_health_payloads;
CREATE TRIGGER trg_raw_set_endpoint_hash
    BEFORE INSERT OR UPDATE OF endpoint ON raw.ods_health_payloads
    FOR EACH ROW EXECUTE FUNCTION raw.set_endpoint_hash_from_endpoint();

DROP TRIGGER IF EXISTS trg_raw_set_endpoint_hash ON raw.ods_marketing_payloads;
CREATE TRIGGER trg_raw_set_endpoint_hash
    BEFORE INSERT OR UPDATE OF endpoint ON raw.ods_marketing_payloads
    FOR EACH ROW EXECUTE FUNCTION raw.set_endpoint_hash_from_endpoint();

UPDATE raw.moex_iss_payloads
SET endpoint_hash = decode(md5(COALESCE(endpoint, '')), 'hex')
WHERE endpoint_hash IS NULL;
UPDATE raw.cbr_daily_payloads
SET endpoint_hash = decode(md5(COALESCE(endpoint, '')), 'hex')
WHERE endpoint_hash IS NULL;
UPDATE raw.open_meteo_payloads
SET endpoint_hash = decode(md5(COALESCE(endpoint, '')), 'hex')
WHERE endpoint_hash IS NULL;
UPDATE raw.ods_economic_payloads
SET endpoint_hash = decode(md5(COALESCE(endpoint, '')), 'hex')
WHERE endpoint_hash IS NULL;
UPDATE raw.ods_territory_payloads
SET endpoint_hash = decode(md5(COALESCE(endpoint, '')), 'hex')
WHERE endpoint_hash IS NULL;
UPDATE raw.ods_public_api_payloads
SET endpoint_hash = decode(md5(COALESCE(endpoint, '')), 'hex')
WHERE endpoint_hash IS NULL;
UPDATE raw.ods_health_payloads
SET endpoint_hash = decode(md5(COALESCE(endpoint, '')), 'hex')
WHERE endpoint_hash IS NULL;
UPDATE raw.ods_marketing_payloads
SET endpoint_hash = decode(md5(COALESCE(endpoint, '')), 'hex')
WHERE endpoint_hash IS NULL;

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
