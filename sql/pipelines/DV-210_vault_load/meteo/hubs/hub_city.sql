INSERT INTO vault.hub_city (city_hk, city_code, load_dts, record_source)
VALUES (
    md5('MOSCOW'),
    'MOSCOW',
    NOW(),
    'OPEN_METEO_STG'
)
ON CONFLICT (city_code) DO NOTHING;
