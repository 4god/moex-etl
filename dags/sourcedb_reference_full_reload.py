"""Полный перегруз справочников в sourcedb (OLTP-песочница для Debezium).

Таблицы ref_* владеет роль sourcedb_loader; Airflow подключается через connection ``sourcedb``.
Транзакционные customers/orders наполняются отдельно (bootstrap SQL), не трогаются этим DAG.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

CONN_ID = "sourcedb"

REF_GEO_REGIONS: list[tuple[str, str, str | None, int]] = [
    ("RU-MOW", "Москва", "Moscow", 10),
    ("RU-SPB", "Санкт-Петербург", "Saint Petersburg", 20),
    ("RU-NW", "Северо-Запад", "North-West", 30),
    ("RU-CFD", "Центральный ФО", "Central FD", 40),
    ("RU-SFD", "Южный ФО", "Southern FD", 50),
    ("RU-PFD", "Приволжский ФО", "Volga FD", 60),
    ("RU-URAL", "Уральский ФО", "Ural FD", 70),
    ("RU-SIB", "Сибирский ФО", "Siberian FD", 80),
    ("RU-DV", "Дальневосточный ФО", "Far Eastern FD", 90),
    ("KZ-ALA", "Алматы", "Almaty", 100),
    ("KZ-AST", "Астана", "Astana", 110),
    ("BY-MINSK", "Минск", "Minsk", 120),
    ("GE-TBS", "Тбилиси", "Tbilisi", 130),
    ("AM-YVN", "Ереван", "Yerevan", 140),
    ("AZ-BAK", "Баку", "Baku", 150),
]

REF_CUSTOMER_SEGMENTS: list[tuple[str, str, float, int]] = [
    ("RETAIL", "Розница", 0.0, 10),
    ("SMB", "Малый бизнес", 3.0, 20),
    ("CORP", "Корпоративный", 7.0, 30),
    ("WHOLESALE", "Опт", 5.0, 40),
    ("GOV", "Госсектор", 0.0, 50),
    ("PARTNER", "Партнёр", 12.0, 60),
]


@dag(
    dag_id="sourcedb_reference_full_reload",
    description="sourcedb: TRUNCATE + INSERT справочников ref_geo_region, ref_customer_segment.",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["workshop", "sourcedb", "cdc", "reference"],
)
def sourcedb_reference_full_reload() -> None:
    @task
    def full_reload_reference_tables() -> None:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    TRUNCATE TABLE public.ref_geo_region, public.ref_customer_segment;
                    """
                )
                execute_values(
                    cur,
                    """
                    INSERT INTO public.ref_geo_region
                        (code, name_local, name_en, sort_order)
                    VALUES %s
                    """,
                    REF_GEO_REGIONS,
                )
                execute_values(
                    cur,
                    """
                    INSERT INTO public.ref_customer_segment
                        (code, label, default_discount_pct, sort_order)
                    VALUES %s
                    """,
                    REF_CUSTOMER_SEGMENTS,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    full_reload_reference_tables()


dag = sourcedb_reference_full_reload()
