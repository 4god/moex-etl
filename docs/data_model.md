# Data Model (Data Vault 2.0)

## Концептуальная схема

```mermaid
erDiagram
    HUB_SECURITY {
        text security_hk PK
        text secid UK
        timestamptz load_dts
        text record_source
    }
    HUB_CURRENCY {
        text currency_hk PK
        text char_code UK
        timestamptz load_dts
        text record_source
    }
    HUB_CALENDAR {
        text date_hk PK
        date business_date UK
        timestamptz load_dts
        text record_source
    }
    LINK_SECURITY_CURRENCY_DATE {
        text link_hk PK
        text security_hk FK
        text currency_hk FK
        text date_hk FK
        timestamptz load_dts
        text record_source
    }
    SAT_SECURITY_PROFILE {
        text security_hk FK
        text hashdiff
        text shortname
        text boardid
        int lot_size
        timestamptz load_dts
    }
    SAT_SECURITY_MARKET {
        text security_hk FK
        text date_hk FK
        text hashdiff
        numeric last_price
        numeric pct_change
        numeric value_total
        timestamptz load_dts
    }
    SAT_CURRENCY_RATE {
        text currency_hk FK
        text date_hk FK
        text hashdiff
        int nominal
        numeric rate
        timestamptz load_dts
    }
    DM_SECURITY_SNAPSHOT {
        text secid PK
        date trade_date
        numeric last_price_rub
        numeric last_price_usd
        numeric last_price_eur
        numeric value_total
    }
    DIM_MOSCOW_WEATHER_REGIME_SCD2 {
        bigint weather_scd_id PK
        text city_code
        text weather_regime
        boolean is_precipitation_day
        date valid_from
        date valid_to
        boolean is_current
        int version_num
    }

    HUB_SECURITY ||--o{ LINK_SECURITY_CURRENCY_DATE : participates
    HUB_CURRENCY ||--o{ LINK_SECURITY_CURRENCY_DATE : participates
    HUB_CALENDAR ||--o{ LINK_SECURITY_CURRENCY_DATE : participates

    HUB_SECURITY ||--o{ SAT_SECURITY_PROFILE : describes
    HUB_SECURITY ||--o{ SAT_SECURITY_MARKET : metrics
    HUB_CALENDAR ||--o{ SAT_SECURITY_MARKET : by_date

    HUB_CURRENCY ||--o{ SAT_CURRENCY_RATE : rates
    HUB_CALENDAR ||--o{ SAT_CURRENCY_RATE : by_date
```

## Ключевая идея методологии

- **Hub** хранит бизнес-ключи (`SECID`, `CHAR_CODE`, `DATE`).
- **Link** фиксирует связи между ключами.
- **Satellite** хранит изменяемые атрибуты и метрики.
- **Datamart** строится поверх Vault для BI и аналитики.
- **SCD Type 2** в `analytics.dim_moscow_weather_regime_scd2` хранит историю режимов погоды по окнам валидности.

## Организация по источникам

Vault SQL разложен по source-папкам для прозрачности:

- `sql/pipelines/DV-210_vault_load/moex/hubs|links|satellites`
- `sql/pipelines/DV-210_vault_load/cbr/hubs|links|satellites`
- `sql/pipelines/DV-210_vault_load/meteo/hubs|links|satellites`

Для нового источника добавляется отдельная папка по тому же шаблону:

- `sql/pipelines/DV-210_vault_load/<new_source>/hubs|links|satellites`
