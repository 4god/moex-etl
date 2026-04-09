# Локальные настройки Airflow (см. https://airflow.apache.org/docs/apache-airflow/stable/howto/customize-ui.html)
try:
    from airflow.www.utils import UIAlert

    # Напоминание сменить пароли по умолчанию. Принудительной смены пароля при первом входе у локальных учёток FAB нет.
    DASHBOARD_UIALERTS = [
        UIAlert(
            "Пароли из README общие для воркшопа. Смените свой пароль после первого входа: "
            "меню пользователя (справа вверху) → Profile / User → задайте новый пароль.",
            category="warning",
            html=False,
        ),
    ]
except ImportError:
    DASHBOARD_UIALERTS = []
