import argparse
import io
import json
import os
import re
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import requests

API_FILE = "data_api/data_api.xlsx"
RAW_JSON_DIR = "reports/raw_json"
CACHE_DIR = os.path.join("reports", "cache")
CACHE_REGIONS_DIR = os.path.join(CACHE_DIR, "regions")
OUTPUT_DIR = os.path.join("reports", "exports", "regions")
SELLER_ANALYTICS_BASE = "https://seller-analytics-api.wildberries.ru"
ENDPOINT = f"{SELLER_ANALYTICS_BASE}/api/v1/analytics/region-sale"
TIMEOUT_SECONDS = 90

os.makedirs(RAW_JSON_DIR, exist_ok=True)
os.makedirs(CACHE_REGIONS_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


def sanitize_filename(name: str) -> str:
    name = str(name).strip()
    name = re.sub(r"[<>:\"/\\|?*\n\r\t]", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Скачать отчет WB 'Продажи по регионам' и сохранить в Excel."
    )
    parser.add_argument("--company", required=True, help="Название компании из столбца company в data_api.xlsx")
    parser.add_argument("--date-from", dest="date_from", help="Дата начала в формате YYYY-MM-DD")
    parser.add_argument("--date-to", dest="date_to", help="Дата конца в формате YYYY-MM-DD")
    parser.add_argument("--api-file", default=API_FILE, help="Путь к data_api.xlsx")
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Игнорировать кэш и скачать отчет заново",
    )
    return parser.parse_args()


def get_default_dates() -> tuple[datetime.date, datetime.date]:
    return (datetime.now() - timedelta(days=7)).date(), (datetime.now() - timedelta(days=1)).date()


def parse_date_arg(value: str | None, default_date) -> datetime.date:
    if not value:
        return default_date
    return datetime.strptime(value, "%Y-%m-%d").date()


def validate_period(date_from, date_to) -> None:
    if date_from > date_to:
        raise ValueError("Дата начала не может быть больше даты конца.")
    delta_days = (date_to - date_from).days + 1
    if delta_days > 31:
        raise ValueError(
            f"Период {delta_days} дней. Для отчета 'Продажи по регионам' максимум 31 день за один запрос."
        )


def load_companies(api_file: str) -> pd.DataFrame:
    if not os.path.exists(api_file):
        raise FileNotFoundError(f"Не найден файл: {api_file}")
    df = pd.read_excel(api_file)
    if df.empty:
        raise ValueError("Файл data_api.xlsx пуст.")
    required_columns = {"company", "regions"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"В data_api.xlsx отсутствуют столбцы: {', '.join(sorted(missing))}")
    return df


def get_company_token(df_companies: pd.DataFrame, company_name: str) -> str:
    match = df_companies[df_companies["company"].astype(str) == str(company_name)]
    if match.empty:
        available = ", ".join(df_companies["company"].astype(str).tolist())
        raise ValueError(f"Компания '{company_name}' не найдена. Доступные компании: {available}")

    token = match.iloc[0]["regions"]
    if pd.isna(token) or str(token).strip() == "":
        raise ValueError(f"Для компании '{company_name}' пустой токен в столбце regions.")
    return str(token).strip()


def build_cache_file_path(company_name: str, date_from, date_to) -> str:
    safe_company = sanitize_filename(company_name)
    filename = f"regions_{safe_company}_{date_from.strftime('%Y-%m-%d')}_{date_to.strftime('%Y-%m-%d')}.json"
    return os.path.join(CACHE_REGIONS_DIR, filename)


def load_from_cache(path: str) -> Any:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception:
        return None


def save_to_cache(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def save_raw_json(prefix: str, company_name: str, data: Any) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_company = sanitize_filename(company_name)
    filename = f"{prefix}_{safe_company}_{ts}.json"
    path = os.path.join(RAW_JSON_DIR, filename)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)
    return path


def extract_report_rows(payload: Any) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, dict):
        report = payload.get("report")
        if isinstance(report, list):
            return report
        data = payload.get("data")
        if isinstance(data, list):
            return data
        if isinstance(report, dict):
            nested_data = report.get("data")
            if isinstance(nested_data, list):
                return nested_data
        return []
    if isinstance(payload, list):
        return payload
    return []


def fetch_region_sales_report(api_key: str, company_name: str, date_from, date_to, refresh_cache: bool = False) -> tuple[list[dict[str, Any]], str, str | None]:
    cache_path = build_cache_file_path(company_name, date_from, date_to)

    if not refresh_cache:
        cached_data = load_from_cache(cache_path)
        if cached_data is not None:
            rows = extract_report_rows(cached_data)
            return rows, f"⚡ Отчет загружен из кэша. Строк: {len(rows)}", cache_path

    headers = {"Authorization": api_key}
    params = {
        "dateFrom": date_from.strftime("%Y-%m-%d"),
        "dateTo": date_to.strftime("%Y-%m-%d"),
    }

    response = requests.get(ENDPOINT, headers=headers, params=params, timeout=TIMEOUT_SECONDS)

    if response.status_code != 200:
        raise RuntimeError(f"Ошибка API {response.status_code}: {response.text}")

    payload = response.json()
    rows = extract_report_rows(payload)

    save_to_cache(cache_path, payload)
    save_raw_json("REGIONS", company_name, payload)

    return rows, f"✅ Отчет скачан из API. Строк: {len(rows)}", cache_path


def normalize_region_sales_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    numeric_columns = [
        "nmID",
        "saleInvoiceCostPrice",
        "saleInvoiceCostPricePerc",
        "saleItemInvoiceQty",
    ]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    preferred_order = [
        "countryName",
        "foName",
        "regionName",
        "cityName",
        "nmID",
        "sa",
        "saleItemInvoiceQty",
        "saleInvoiceCostPrice",
        "saleInvoiceCostPricePerc",
    ]
    ordered_columns = [col for col in preferred_order if col in df.columns]
    remaining_columns = [col for col in df.columns if col not in ordered_columns]
    df = df[ordered_columns + remaining_columns]

    sort_columns = [col for col in ["countryName", "foName", "regionName", "cityName", "nmID"] if col in df.columns]
    if sort_columns:
        df = df.sort_values(sort_columns, kind="stable").reset_index(drop=True)

    return df


def build_meta_df(company_name: str, date_from, date_to, row_count: int, cache_path: str | None) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Параметр": "Компания", "Значение": company_name},
            {"Параметр": "Дата начала", "Значение": date_from.strftime("%Y-%m-%d")},
            {"Параметр": "Дата конца", "Значение": date_to.strftime("%Y-%m-%d")},
            {"Параметр": "Строк в отчете", "Значение": row_count},
            {"Параметр": "Сформировано", "Значение": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            {"Параметр": "Файл кэша", "Значение": cache_path or ""},
        ]
    )



def save_excel_report(df_report: pd.DataFrame, company_name: str, date_from, date_to, cache_path: str | None) -> str:
    safe_company = sanitize_filename(company_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = (
        f"region_sales_{safe_company}_{date_from.strftime('%Y-%m-%d')}_{date_to.strftime('%Y-%m-%d')}_{timestamp}.xlsx"
    )
    excel_path = os.path.join(OUTPUT_DIR, filename)

    meta_df = build_meta_df(company_name, date_from, date_to, len(df_report), cache_path)

    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        df_report.to_excel(writer, index=False, sheet_name="Region_Sales")
        meta_df.to_excel(writer, index=False, sheet_name="Meta")

        workbook = writer.book
        worksheet = writer.sheets["Region_Sales"]
        header_format = workbook.add_format({"bold": True})

        for col_idx, column in enumerate(df_report.columns):
            max_len = max(len(str(column)), df_report[column].astype(str).map(len).max() if not df_report.empty else 0)
            worksheet.set_column(col_idx, col_idx, min(max(max_len + 2, 12), 35))
            worksheet.write(0, col_idx, column, header_format)

    return excel_path


def main() -> None:
    args = parse_args()

    default_from, default_to = get_default_dates()
    date_from = parse_date_arg(args.date_from, default_from)
    date_to = parse_date_arg(args.date_to, default_to)
    validate_period(date_from, date_to)

    companies_df = load_companies(args.api_file)
    api_key = get_company_token(companies_df, args.company)

    rows, status_message, cache_path = fetch_region_sales_report(
        api_key=api_key,
        company_name=args.company,
        date_from=date_from,
        date_to=date_to,
        refresh_cache=args.refresh_cache,
    )

    df_report = normalize_region_sales_df(rows)
    excel_path = save_excel_report(df_report, args.company, date_from, date_to, cache_path)

    print(status_message)
    print(f"Excel сохранен: {excel_path}")
    print(f"Столбцы: {list(df_report.columns)}")


if __name__ == "__main__":
    main()
