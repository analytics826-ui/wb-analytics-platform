def build_admin_daily_kpi_summary(report_date, companies_sent_to_users, success_count, error_count, send_type="auto", admin_name="Ivan"):
    report_date_str = report_date.strftime("%d.%m.%Y") if hasattr(report_date, "strftime") else str(report_date)
    company_lines = "\n".join(companies_sent_to_users) if companies_sent_to_users else "—"
    return (
        f"{admin_name}\n"
        f"Период {report_date_str}\n\n"
        f"Компании:\n"
        f"{company_lines}\n\n"
        f"Тип: {send_type}\n"
        f"Успешно: {success_count}\n"
        f"Ошибок: {error_count}"
    )
