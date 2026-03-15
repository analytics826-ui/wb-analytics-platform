import streamlit as st
import pandas as pd
import requests
import json
import io
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from processor import create_analysis_report
from telegram_notifier import send_admin_message, send_users_message

# --- НАСТРОЙКИ ПУТЕЙ ---
API_FILE = "data_api/data_api.xlsx"
RAW_JSON_DIR = "reports/raw_json"
CACHE_DIR = os.path.join("reports", "cache")
CACHE_FIN_DIR = os.path.join(CACHE_DIR, "fin")
CACHE_ADS_DIR = os.path.join(CACHE_DIR, "ads")
CACHE_STORAGE_DIR = os.path.join(CACHE_DIR, "storage")
CACHE_STOCKS_DIR = os.path.join(CACHE_DIR, "stocks")
CACHE_REGIONS_DIR = os.path.join(CACHE_DIR, "regions")
PRICE_SAVE_PATH = "data"
NOMENCLATURE_DIR = "data"  # nomenclature_<company>.parquet
PRICE_PARQUET_PATH = os.path.join(PRICE_SAVE_PATH, "price_list.parquet")
LAST_KPI_SEND_PATH = os.path.join(PRICE_SAVE_PATH, "last_kpi_send.json")
KPI_LOG_PATH = os.path.join(PRICE_SAVE_PATH, "kpi_log.json")

# === НАСТРОЙКА АВТООБНОВЛЕНИЯ НОМЕНКЛАТУРЫ ===
NOMENCLATURE_REFRESH_DAYS = 3

os.makedirs(RAW_JSON_DIR, exist_ok=True)
os.makedirs(PRICE_SAVE_PATH, exist_ok=True)
os.makedirs(NOMENCLATURE_DIR, exist_ok=True)
os.makedirs(CACHE_FIN_DIR, exist_ok=True)
os.makedirs(CACHE_ADS_DIR, exist_ok=True)
os.makedirs(CACHE_STORAGE_DIR, exist_ok=True)
os.makedirs(CACHE_STOCKS_DIR, exist_ok=True)
os.makedirs(CACHE_REGIONS_DIR, exist_ok=True)

st.set_page_config(page_title="WB Financial Report", layout="wide")

SELLER_ANALYTICS_BASE = "https://seller-analytics-api.wildberries.ru"


# =========================
# STATE HELPERS
# =========================
def init_state():
    defaults = {
        "selected_company": None,
        "date_from": None,
        "date_to": None,
        "loaded": False,

        "report_data": None,
        "ads_data": None,
        "storage_data": None,
        "stocks_data": None,
        "region_sales_data": None,

        "df_fin": None,
        "df_ads": None,
        "df_storage": None,
        "df_stocks": None,
        "df_region_sales": None,
        "df_region_sales_geo": None,
        "df_nom": None,
        "df_price": None,
        "df_analysis": None,
        "df_missing_cost_barcodes": None,
        "df_missing_cost_stocks": None,
        "df_stocks_by_warehouse": None,

        "excel_fin_raw": None,
        "excel_ads_raw": None,
        "excel_storage_raw": None,
        "excel_stocks_raw": None,
        "excel_region_sales": None,
        "excel_analysis": None,
        "excel_missing_cost_barcodes": None,
        "excel_missing_cost_stocks": None,
        "excel_stocks_by_warehouse": None,

        "status_msg_fin": "",
        "status_msg_ads": "",
        "status_msg_storage": "",
        "status_msg_stocks": "",
        "status_msg_regions": "",
        "status_msg_nom": "",

        "ts_loaded": None,
        "test_kpi_result": None,
        "test_kpi_error": "",
        "daily_kpi_send_result": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def clear_loaded_data():
    st.session_state.loaded = False

    st.session_state.report_data = None
    st.session_state.ads_data = None
    st.session_state.storage_data = None
    st.session_state.stocks_data = None
    st.session_state.region_sales_data = None

    st.session_state.df_fin = None
    st.session_state.df_ads = None
    st.session_state.df_storage = None
    st.session_state.df_stocks = None
    st.session_state.df_region_sales = None
    st.session_state.df_region_sales_geo = None
    st.session_state.df_nom = None
    st.session_state.df_price = None
    st.session_state.df_analysis = None
    st.session_state.df_missing_cost_barcodes = None
    st.session_state.df_missing_cost_stocks = None
    st.session_state.df_stocks_by_warehouse = None

    st.session_state.excel_fin_raw = None
    st.session_state.excel_ads_raw = None
    st.session_state.excel_storage_raw = None
    st.session_state.excel_stocks_raw = None
    st.session_state.excel_region_sales = None
    st.session_state.excel_analysis = None
    st.session_state.excel_missing_cost_barcodes = None
    st.session_state.excel_missing_cost_stocks = None
    st.session_state.excel_stocks_by_warehouse = None

    st.session_state.status_msg_fin = ""
    st.session_state.status_msg_ads = ""
    st.session_state.status_msg_storage = ""
    st.session_state.status_msg_stocks = ""
    st.session_state.status_msg_regions = ""
    st.session_state.status_msg_nom = ""

    st.session_state.ts_loaded = None


init_state()


# =========================
# UTILS
# =========================
def _get_companies_from_secrets() -> pd.DataFrame:
    try:
        if "companies" not in st.secrets:
            return pd.DataFrame()

        companies_section = st.secrets["companies"]
        rows = []
        columns = ["company", "api", "advertising_api", "storage", "content", "remaining_goods", "regions"]

        for company_name, company_cfg in companies_section.items():
            row = {"company": str(company_name).strip()}
            for col in columns[1:]:
                row[col] = str(company_cfg[col]).strip() if col in company_cfg and company_cfg[col] is not None else ""
            rows.append(row)

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows, columns=columns)
    except Exception:
        return pd.DataFrame()


@st.cache_data
def get_companies():
    df_secrets = _get_companies_from_secrets()
    if not df_secrets.empty:
        return df_secrets

    try:
        df = pd.read_excel(API_FILE)
        return df
    except Exception as e:
        st.error(f"Ошибка чтения {API_FILE}: {e}")
        return pd.DataFrame()


def sanitize_filename(name: str) -> str:
    name = str(name).strip()
    name = re.sub(r"[<>:\"/\\|?*\n\r\t]", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def to_excel(df: pd.DataFrame, sheet_name: str = "Report") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


def save_raw_json(prefix: str, company_name: str, data):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_company = sanitize_filename(company_name)
    filename = f"{prefix}_{safe_company}_{ts}.json"
    path = os.path.join(RAW_JSON_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    return path


def build_cache_file_path(report_type: str, company_name: str, date_from=None, date_to=None) -> str:
    safe_company = sanitize_filename(company_name)

    if report_type == "fin":
        filename = f"fin_{safe_company}_{date_from.strftime('%Y-%m-%d')}_{date_to.strftime('%Y-%m-%d')}.json"
        return os.path.join(CACHE_FIN_DIR, filename)

    if report_type == "ads":
        filename = f"ads_{safe_company}_{date_from.strftime('%Y-%m-%d')}_{date_to.strftime('%Y-%m-%d')}.json"
        return os.path.join(CACHE_ADS_DIR, filename)

    if report_type == "storage":
        filename = f"storage_{safe_company}_{date_from.strftime('%Y-%m-%d')}_{date_to.strftime('%Y-%m-%d')}.json"
        return os.path.join(CACHE_STORAGE_DIR, filename)

    if report_type == "stocks":
        today_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"stocks_{safe_company}_{today_str}.json"
        return os.path.join(CACHE_STOCKS_DIR, filename)

    if report_type == "regions":
        filename = f"regions_{safe_company}_{date_from.strftime('%Y-%m-%d')}_{date_to.strftime('%Y-%m-%d')}.json"
        return os.path.join(CACHE_REGIONS_DIR, filename)

    raise ValueError(f"Неизвестный тип кэша: {report_type}")


def load_from_cache(path: str):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_to_cache(path: str, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception:
        pass


def load_price_list_parquet() -> pd.DataFrame:
    """
    Загружаем price_list.parquet (если есть).
    Ожидаемые столбцы: 'Баркод' и 'Себестоимость ' (возможен пробел в конце).
    """
    if not os.path.exists(PRICE_PARQUET_PATH):
        return pd.DataFrame()
    try:
        return pd.read_parquet(PRICE_PARQUET_PATH)
    except Exception:
        return pd.DataFrame()


def get_nomenclature_paths(company_name: str):
    safe_company = sanitize_filename(company_name)
    parquet_path = os.path.join(NOMENCLATURE_DIR, f"nomenclature_{safe_company}.parquet")
    preview_xlsx_path = os.path.join(NOMENCLATURE_DIR, f"nomenclature_{safe_company}_preview.xlsx")
    return parquet_path, preview_xlsx_path


def is_file_older_than_days(path: str, days: int) -> bool:
    if not os.path.exists(path):
        return True
    try:
        modified_ts = os.path.getmtime(path)
        modified_dt = datetime.fromtimestamp(modified_ts)
        return datetime.now() - modified_dt > timedelta(days=days)
    except Exception:
        return True


def save_nomenclature_files(company_name: str, df_nom: pd.DataFrame):
    parquet_path, preview_xlsx_path = get_nomenclature_paths(company_name)

    df_nom.to_parquet(parquet_path, index=False, engine="pyarrow")

    with pd.ExcelWriter(preview_xlsx_path, engine="xlsxwriter") as writer:
        df_nom.to_excel(writer, index=False, sheet_name="Nomenclature")

    return parquet_path, preview_xlsx_path


def _normalize_id_series_local(s: pd.Series) -> pd.Series:
    s = s.copy()
    s = s.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.replace({"nan": "", "None": ""})
    return s


def _pick_fin_barcode_column_local(df_fin: pd.DataFrame) -> str | None:
    if df_fin is None or df_fin.empty:
        return None

    cols = list(df_fin.columns)

    for c in ["barcode", "Barcode", "BARCODE", "ШК", "шк", "Штрихкод", "штрихкод"]:
        if c in cols:
            return c

    for c in cols:
        name = str(c).strip().lower()
        if "barcode" in name:
            return c
        if "штрихкод" in name:
            return c
        if name in ("шк", "шк товара", "шк_товара"):
            return c

    return None


def _pick_stock_barcode_column_local(df_stocks: pd.DataFrame) -> str | None:
    if df_stocks is None or df_stocks.empty:
        return None

    cols = list(df_stocks.columns)

    for c in ["barcode", "Barcode", "BARCODE", "Баркод", "ШК", "шк", "Штрихкод", "штрихкод"]:
        if c in cols:
            return c

    for c in cols:
        name = str(c).strip().lower()
        if "barcode" in name:
            return c
        if "баркод" in name:
            return c
        if "штрихкод" in name:
            return c
        if name in ("шк", "шк товара", "шк_товара"):
            return c

    return None


def _build_missing_cost_mask_local(cost_series: pd.Series) -> pd.Series:
    return (
        cost_series.isna()
        | (cost_series.astype(str).str.strip() == "")
        | (cost_series.astype(str).str.strip().str.lower() == "nan")
        | (cost_series.astype(str).str.strip().str.lower() == "none")
    )


def get_missing_cost_barcodes(df_fin: pd.DataFrame, df_price: pd.DataFrame) -> pd.DataFrame:
    """
    Ищет баркоды, которые:
    - реально участвовали бы в расчете себестоимости
      (операция = Продажа или Возврат)
    - но в price_list.parquet имеют пустую себестоимость
    """
    if df_fin is None or df_fin.empty or df_price is None or df_price.empty:
        return pd.DataFrame(columns=["Баркод"])

    if len(df_fin.columns) <= 24:
        return pd.DataFrame(columns=["Баркод"])

    barcode_col = _pick_fin_barcode_column_local(df_fin)
    if not barcode_col:
        return pd.DataFrame(columns=["Баркод"])

    df_fin_local = df_fin.copy()
    op_col_name = df_fin_local.columns[24]
    df_fin_local["_op"] = df_fin_local[op_col_name].astype(str).str.strip()

    df_fin_used = df_fin_local[df_fin_local["_op"].isin(["Продажа", "Возврат"])].copy()
    if df_fin_used.empty:
        return pd.DataFrame(columns=["Баркод"])

    df_fin_used["_barcode_norm"] = _normalize_id_series_local(df_fin_used[barcode_col])
    df_fin_used = df_fin_used[df_fin_used["_barcode_norm"] != ""].copy()

    if df_fin_used.empty:
        return pd.DataFrame(columns=["Баркод"])

    df_p = df_price.copy()
    df_p.columns = [str(c).strip() for c in df_p.columns]

    if "Баркод" not in df_p.columns:
        return pd.DataFrame(columns=["Баркод"])

    cost_col = None
    for c in df_p.columns:
        if str(c).strip().lower() == "себестоимость":
            cost_col = c
            break

    if cost_col is None:
        return pd.DataFrame(columns=["Баркод"])

    df_p["_barcode_norm"] = _normalize_id_series_local(df_p["Баркод"])

    cost_raw = df_p[cost_col]
    mask_missing_cost = _build_missing_cost_mask_local(cost_raw)

    df_p_missing = df_p[mask_missing_cost].copy()
    if df_p_missing.empty:
        return pd.DataFrame(columns=["Баркод"])

    used_barcodes = set(df_fin_used["_barcode_norm"].tolist())
    df_p_missing = df_p_missing[df_p_missing["_barcode_norm"].isin(used_barcodes)].copy()

    if df_p_missing.empty:
        return pd.DataFrame(columns=["Баркод"])

    out = df_p_missing[["_barcode_norm"]].drop_duplicates().copy()
    out = out.rename(columns={"_barcode_norm": "Баркод"})
    out = out.sort_values("Баркод").reset_index(drop=True)

    return out


def get_missing_cost_stocks_barcodes(df_stocks: pd.DataFrame, df_price: pd.DataFrame) -> pd.DataFrame:
    """
    Ищет баркоды, которые:
    - есть в остатках
    - quantity > 0
    - но в price_list.parquet имеют пустую себестоимость
    Возвращает только один столбец: Баркод
    """
    if df_stocks is None or df_stocks.empty:
        return pd.DataFrame(columns=["Баркод"])

    barcode_col = _pick_stock_barcode_column_local(df_stocks)
    if not barcode_col:
        return pd.DataFrame(columns=["Баркод"])

    qty_col = None
    for c in ["quantity", "Quantity", "QTY", "qty", "Остаток", "остаток"]:
        if c in df_stocks.columns:
            qty_col = c
            break

    if qty_col is None:
        for c in df_stocks.columns:
            name = str(c).strip().lower()
            if name == "quantity" or "quantity" in name or "остаток" in name:
                qty_col = c
                break

    if qty_col is None:
        return pd.DataFrame(columns=["Баркод"])

    df_s = df_stocks.copy()
    df_s[qty_col] = pd.to_numeric(df_s[qty_col], errors="coerce").fillna(0)
    df_s = df_s[df_s[qty_col] > 0].copy()

    if df_s.empty:
        return pd.DataFrame(columns=["Баркод"])

    df_s["_barcode_norm"] = _normalize_id_series_local(df_s[barcode_col])
    df_s = df_s[df_s["_barcode_norm"] != ""].copy()

    if df_s.empty:
        return pd.DataFrame(columns=["Баркод"])

    if df_price is None or df_price.empty:
        out = df_s[["_barcode_norm"]].drop_duplicates().copy()
        out = out.rename(columns={"_barcode_norm": "Баркод"})
        out = out.sort_values("Баркод").reset_index(drop=True)
        return out

    df_p = df_price.copy()
    df_p.columns = [str(c).strip() for c in df_p.columns]

    if "Баркод" not in df_p.columns:
        out = df_s[["_barcode_norm"]].drop_duplicates().copy()
        out = out.rename(columns={"_barcode_norm": "Баркод"})
        out = out.sort_values("Баркод").reset_index(drop=True)
        return out

    cost_col = None
    for c in df_p.columns:
        if str(c).strip().lower() == "себестоимость":
            cost_col = c
            break

    if cost_col is None:
        out = df_s[["_barcode_norm"]].drop_duplicates().copy()
        out = out.rename(columns={"_barcode_norm": "Баркод"})
        out = out.sort_values("Баркод").reset_index(drop=True)
        return out

    df_p["_barcode_norm"] = _normalize_id_series_local(df_p["Баркод"])
    df_p = df_p[df_p["_barcode_norm"] != ""].copy()

    cost_raw = df_p[cost_col]
    mask_missing_cost = _build_missing_cost_mask_local(cost_raw)

    missing_barcodes_in_price = set(df_p.loc[mask_missing_cost, "_barcode_norm"].tolist())
    all_barcodes_in_price = set(df_p["_barcode_norm"].tolist())

    used_barcodes = set(df_s["_barcode_norm"].tolist())

    result_barcodes = set()

    for bc in used_barcodes:
        if bc not in all_barcodes_in_price:
            result_barcodes.add(bc)
        elif bc in missing_barcodes_in_price:
            result_barcodes.add(bc)

    if not result_barcodes:
        return pd.DataFrame(columns=["Баркод"])

    out = pd.DataFrame({"Баркод": sorted(result_barcodes)})
    return out.reset_index(drop=True)


def create_stocks_by_warehouse_report(df_stocks: pd.DataFrame, df_nom: pd.DataFrame) -> pd.DataFrame:
    """
    Строит отдельную таблицу:
    строки = Категория
    столбцы = реальные склады + Остальные
    справа:
    - Итого FBO
    - В пути до получателей
    - В пути возвраты на склад WB

    Все показатели только в штуках.
    """
    if df_stocks is None or df_stocks.empty:
        return pd.DataFrame()

    required_cols = {"nmId", "warehouseName", "quantity"}
    if not required_cols.issubset(set(df_stocks.columns)):
        return pd.DataFrame()

    tmp = df_stocks.copy()
    tmp["nmId"] = _normalize_id_series_local(tmp["nmId"])
    tmp["warehouseName"] = tmp["warehouseName"].astype(str).str.strip()
    tmp["quantity"] = pd.to_numeric(tmp["quantity"], errors="coerce").fillna(0.0)

    nm_to_cat = {}
    if (
        df_nom is not None
        and isinstance(df_nom, pd.DataFrame)
        and not df_nom.empty
        and "nm_id" in df_nom.columns
        and "subject" in df_nom.columns
    ):
        tmp_nom = df_nom[["nm_id", "subject"]].copy()
        tmp_nom["nm_id"] = _normalize_id_series_local(tmp_nom["nm_id"])
        tmp_nom["subject"] = tmp_nom["subject"].astype(str).str.strip()
        tmp_nom = tmp_nom[tmp_nom["nm_id"] != ""].drop_duplicates(subset=["nm_id"])
        nm_to_cat = dict(zip(tmp_nom["nm_id"], tmp_nom["subject"]))

    def map_cat(nm_id: str) -> str:
        if not nm_id:
            return "Не найдено"
        return nm_to_cat.get(nm_id, "Не найдено")

    tmp["Категория"] = tmp["nmId"].apply(map_cat)

    status_to_customer = "В пути до получателей"
    status_return_to_wb = "В пути возвраты на склад WB"
    status_total_fbo = "Всего находится на складах"

    mask_to_customer = tmp["warehouseName"] == status_to_customer
    mask_return_to_wb = tmp["warehouseName"] == status_return_to_wb
    mask_total_fbo = tmp["warehouseName"] == status_total_fbo

    mask_fbo = ~(mask_to_customer | mask_return_to_wb | mask_total_fbo)

    df_fbo = tmp[mask_fbo].copy()
    df_to_customer = tmp[mask_to_customer].copy()
    df_return_to_wb = tmp[mask_return_to_wb].copy()

    if df_fbo.empty and df_to_customer.empty and df_return_to_wb.empty:
        return pd.DataFrame()

    pivot_fbo = pd.DataFrame()
    if not df_fbo.empty:
        pivot_fbo = (
            pd.pivot_table(
                df_fbo,
                index="Категория",
                columns="warehouseName",
                values="quantity",
                aggfunc="sum",
                fill_value=0,
            )
            .reset_index()
        )

    in_transit_customer = pd.DataFrame(columns=["Категория", "В пути до получателей"])
    if not df_to_customer.empty:
        in_transit_customer = (
            df_to_customer.groupby("Категория", as_index=False)["quantity"]
            .sum()
            .rename(columns={"quantity": "В пути до получателей"})
        )

    return_to_wb = pd.DataFrame(columns=["Категория", "В пути возвраты на склад WB"])
    if not df_return_to_wb.empty:
        return_to_wb = (
            df_return_to_wb.groupby("Категория", as_index=False)["quantity"]
            .sum()
            .rename(columns={"quantity": "В пути возвраты на склад WB"})
        )

    if pivot_fbo.empty:
        base = pd.DataFrame({"Категория": sorted(set(tmp["Категория"].tolist()))})
    else:
        base = pivot_fbo.copy()

    if not pivot_fbo.empty:
        stock_cols = [c for c in pivot_fbo.columns if c != "Категория"]
        base["Итого FBO"] = base[stock_cols].sum(axis=1)
    else:
        base["Итого FBO"] = 0.0

    base = base.merge(in_transit_customer, on="Категория", how="left")
    base = base.merge(return_to_wb, on="Категория", how="left")

    if "В пути до получателей" not in base.columns:
        base["В пути до получателей"] = 0.0
    if "В пути возвраты на склад WB" not in base.columns:
        base["В пути возвраты на склад WB"] = 0.0

    base["В пути до получателей"] = pd.to_numeric(base["В пути до получателей"], errors="coerce").fillna(0.0)
    base["В пути возвраты на склад WB"] = pd.to_numeric(
        base["В пути возвраты на склад WB"], errors="coerce"
    ).fillna(0.0)

    stock_cols = [
        c for c in base.columns
        if c not in ["Категория", "Итого FBO", "В пути до получателей", "В пути возвраты на склад WB"]
    ]

    preferred_order = []
    if "Остальные" in stock_cols:
        preferred_order = [c for c in stock_cols if c != "Остальные"] + ["Остальные"]
    else:
        preferred_order = stock_cols

    final_cols = ["Категория"] + preferred_order + [
        "Итого FBO",
        "В пути до получателей",
        "В пути возвраты на склад WB",
    ]

    base = base[final_cols].copy()
    base = base.sort_values("Категория").reset_index(drop=True)

    numeric_cols = [c for c in base.columns if c != "Категория"]
    for c in numeric_cols:
        base[c] = pd.to_numeric(base[c], errors="coerce").fillna(0.0)

    total_row = {"Категория": "Итого"}
    for c in numeric_cols:
        total_row[c] = base[c].sum()

    base = pd.concat([base, pd.DataFrame([total_row])], ignore_index=True)

    return base


# =========================
# FIN REPORT
# =========================
def fetch_financial_report(api_key, company_name, date_from, date_to):
    cache_path = build_cache_file_path("fin", company_name, date_from, date_to)
    cached_data = load_from_cache(cache_path)
    if cached_data is not None:
        if not cached_data:
            return None, "ℹ Финансовый отчет загружен из кэша: нет данных за выбранный период."
        return cached_data, f"⚡ Финансовый отчет загружен из кэша. Строк: {len(cached_data)}"

    url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
    headers = {"Authorization": str(api_key).strip()}

    all_data = []
    rrdid = 0
    last_seen_rrd_id = None
    max_iterations = 200

    try:
        for _ in range(max_iterations):
            params = {
                "dateFrom": date_from.strftime("%Y-%m-%d"),
                "dateTo": date_to.strftime("%Y-%m-%d"),
                "limit": 100000,
                "period": "daily",
                "rrdid": rrdid,
            }

            response = requests.get(url, headers=headers, params=params, timeout=90)

            if response.status_code == 204:
                break

            if response.status_code != 200:
                return None, f"Ошибка API {response.status_code}: {response.text}"

            chunk = response.json()

            if not isinstance(chunk, list):
                return None, f"Ошибка: неожиданный формат финансового отчета: {type(chunk)}"

            if not chunk:
                break

            all_data.extend(chunk)

            last_row = chunk[-1]
            next_rrd_id = (
                last_row.get("rrd_id")
                or last_row.get("rrdId")
                or last_row.get("rrdID")
            )

            if next_rrd_id is None:
                break

            if str(next_rrd_id) == str(last_seen_rrd_id):
                break

            last_seen_rrd_id = next_rrd_id
            rrdid = next_rrd_id

            if len(chunk) < 100000:
                break

            time.sleep(0.2)

        save_to_cache(cache_path, all_data)
        save_raw_json("FIN", company_name, all_data)

        if not all_data:
            return None, "ℹ Финансовый отчет: нет данных за выбранный период."

        return all_data, f"✅ Финансовый отчет сохранен. Строк: {len(all_data)}"

    except Exception as e:
        return None, f"Ошибка соединения: {e}"


# =========================
# ADS REPORT
# =========================
def fetch_advertising_report(api_key, company_name, date_from, date_to):
    cache_path = build_cache_file_path("ads", company_name, date_from, date_to)
    cached_data = load_from_cache(cache_path)
    if cached_data is not None:
        return cached_data, "⚡ Отчет по рекламе загружен из кэша."

    url = "https://advert-api.wildberries.ru/adv/v1/upd"
    headers = {"Authorization": str(api_key).strip()}
    params = {
        "from": date_from.strftime("%Y-%m-%d"),
        "to": date_to.strftime("%Y-%m-%d"),
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=90)

        if response.status_code == 200:
            data = response.json()
            save_to_cache(cache_path, data)
            save_raw_json("ADS", company_name, data)
            return data, "✅ Отчет по рекламе сохранен."

        return None, f"Ошибка рекламы API {response.status_code}: {response.text}"

    except Exception as e:
        return None, f"Ошибка соединения (реклама): {e}"


def add_article_column_from_campname(df_ads: pd.DataFrame) -> pd.DataFrame:
    if df_ads is None or df_ads.empty:
        return df_ads

    df_ads = df_ads.copy()
    if "campName" not in df_ads.columns:
        df_ads["article"] = ""
        return df_ads

    def extract_article(val):
        s = "" if pd.isna(val) else str(val).strip()
        m = re.match(r"^(\d+)\s", s)
        return m.group(1) if m else ""

    df_ads["article"] = df_ads["campName"].apply(extract_article)
    return df_ads


# =========================
# NOMENCLATURE
# =========================
def load_nomenclature_for_company(company_name: str) -> pd.DataFrame:
    parquet_path, _ = get_nomenclature_paths(company_name)
    if not os.path.exists(parquet_path):
        return pd.DataFrame()
    try:
        return pd.read_parquet(parquet_path)
    except Exception:
        return pd.DataFrame()


def fetch_nomenclature_from_wb(content_token: str, company_name: str):
    """
    Скачивает номенклатуру WB по API content.
    Сохраняет:
    - nomenclature_<company>.parquet
    - nomenclature_<company>_preview.xlsx
    """

    if not content_token or str(content_token).strip() == "" or str(content_token).lower() == "nan":
        return pd.DataFrame(), "⚠ Номенклатура: content API ключ не указан в data_api.xlsx"

    headers = {"Authorization": str(content_token).strip()}
    url = "https://content-api.wildberries.ru/content/v2/get/cards/list"

    all_cards = []
    cursor = {"limit": 100}

    try:
        while True:
            payload = {
                "settings": {
                    "cursor": cursor,
                    "filter": {
                        "withPhoto": -1
                    }
                }
            }

            response = requests.post(url, headers=headers, json=payload, timeout=90)

            if response.status_code != 200:
                return pd.DataFrame(), f"⚠ Номенклатура: ошибка API {response.status_code}: {response.text}"

            resp_json = response.json()
            cards = resp_json.get("cards", [])
            cursor_resp = resp_json.get("cursor", {})

            if not cards:
                break

            for card in cards:
                nm_id = card.get("nmID")
                subject_name = card.get("subjectName", "")
                title = card.get("title", "")
                vendor_code = card.get("vendorCode", "")
                brand = card.get("brand", "")

                barcodes = []
                sizes = card.get("sizes", [])
                for size in sizes:
                    skus = size.get("skus", [])
                    if skus:
                        barcodes.extend(skus)

                if barcodes:
                    for bc in barcodes:
                        all_cards.append({
                            "nm_id": nm_id,
                            "subject": subject_name,
                            "title": title,
                            "vendorCode": vendor_code,
                            "brand": brand,
                            "Баркод": str(bc).strip()
                        })
                else:
                    all_cards.append({
                        "nm_id": nm_id,
                        "subject": subject_name,
                        "title": title,
                        "vendorCode": vendor_code,
                        "brand": brand,
                        "Баркод": ""
                    })

            updated_at = cursor_resp.get("updatedAt")
            nm_id_cursor = cursor_resp.get("nmID")

            if not updated_at or not nm_id_cursor:
                break

            cursor = {
                "limit": 100,
                "updatedAt": updated_at,
                "nmID": nm_id_cursor
            }

            if len(cards) < 100:
                break

        df_nom = pd.DataFrame(all_cards)

        if df_nom.empty:
            return pd.DataFrame(), "ℹ Номенклатура: WB вернул пустой список карточек."

        if "nm_id" in df_nom.columns:
            df_nom["nm_id"] = pd.to_numeric(df_nom["nm_id"], errors="coerce").fillna(0).astype("int64").astype(str)

        if "Баркод" in df_nom.columns:
            df_nom["Баркод"] = df_nom["Баркод"].astype(str).str.strip()

        save_nomenclature_files(company_name, df_nom)

        return df_nom, f"✅ Номенклатура обновлена. Строк: {len(df_nom)}"

    except Exception as e:
        return pd.DataFrame(), f"⚠ Номенклатура: ошибка соединения: {e}"


def ensure_nomenclature_for_company(company_name: str, content_token: str):
    """
    Логика:
    - если файла нет -> скачать
    - если файл старше NOMENCLATURE_REFRESH_DAYS -> обновить
    - иначе просто загрузить локально
    """
    parquet_path, _ = get_nomenclature_paths(company_name)

    need_refresh = False
    reason = ""

    if not os.path.exists(parquet_path):
        need_refresh = True
        reason = "Файл номенклатуры не найден"
    elif is_file_older_than_days(parquet_path, NOMENCLATURE_REFRESH_DAYS):
        need_refresh = True
        reason = f"Файл старше {NOMENCLATURE_REFRESH_DAYS} дней"

    if need_refresh:
        df_nom, msg = fetch_nomenclature_from_wb(content_token, company_name)
        if not df_nom.empty:
            return df_nom, f"✅ {reason}. Номенклатура скачана/обновлена."
        else:
            df_local = load_nomenclature_for_company(company_name)
            if not df_local.empty:
                return df_local, f"{msg} Используется ранее сохраненная локальная номенклатура."
            return pd.DataFrame(), msg

    df_nom = load_nomenclature_for_company(company_name)
    if not df_nom.empty:
        return df_nom, f"ℹ Номенклатура загружена локально. Файл свежий."
    return pd.DataFrame(), "⚠ Номенклатура: файл есть, но не удалось его прочитать."


# =========================
# PAID STORAGE REPORT (ASYNC)
# =========================
def fetch_paid_storage_report(storage_token: str, company_name: str, date_from, date_to):
    delta_days = (date_to - date_from).days + 1
    if delta_days > 8:
        return None, f"⚠ Платное хранение: период {delta_days} дней, а максимум 8 дней. Уменьши период."

    cache_path = build_cache_file_path("storage", company_name, date_from, date_to)
    cached_data = load_from_cache(cache_path)
    if cached_data is not None:
        if isinstance(cached_data, list):
            return cached_data, f"⚡ Платное хранение загружено из кэша. Строк: {len(cached_data)}"
        return cached_data, "⚡ Платное хранение загружено из кэша."

    headers = {"Authorization": str(storage_token).strip()}

    gen_url = f"{SELLER_ANALYTICS_BASE}/api/v1/paid_storage"
    params = {"dateFrom": date_from.strftime("%Y-%m-%d"), "dateTo": date_to.strftime("%Y-%m-%d")}

    try:
        r = requests.get(gen_url, headers=headers, params=params, timeout=60)
        if r.status_code != 200:
            return None, f"Ошибка API хранения (generate) {r.status_code}: {r.text}"

        gen_json = r.json()
        task_id = (
            gen_json.get("data", {}).get("taskId")
            or gen_json.get("data", {}).get("id")
            or gen_json.get("taskId")
            or gen_json.get("id")
        )
        if not task_id:
            return None, f"Ошибка: не найден taskId в ответе generate: {gen_json}"

        status_url = f"{SELLER_ANALYTICS_BASE}/api/v1/paid_storage/tasks/{task_id}/status"

        max_attempts = 24
        status_val = None
        for _ in range(max_attempts):
            sr = requests.get(status_url, headers=headers, timeout=30)
            if sr.status_code != 200:
                return None, f"Ошибка API хранения (status) {sr.status_code}: {sr.text}"
            sj = sr.json()
            status_val = sj.get("data", {}).get("status") or sj.get("status")
            if status_val == "done":
                break
            if status_val in ("failed", "error"):
                return None, f"❌ Платное хранение: генерация завершилась со статусом {status_val}"
            time.sleep(5)

        if status_val != "done":
            return None, "⚠ Платное хранение: не дождались завершения генерации (попробуй еще раз)."

        dl_url = f"{SELLER_ANALYTICS_BASE}/api/v1/paid_storage/tasks/{task_id}/download"
        dr = requests.get(dl_url, headers=headers, timeout=90)

        if dr.status_code == 204:
            data = []
            save_to_cache(cache_path, data)
            save_raw_json("STORAGE", company_name, data)
            return [], "ℹ Платное хранение: нет данных за выбранный период."
        if dr.status_code != 200:
            return None, f"Ошибка API хранения (download) {dr.status_code}: {dr.text}"

        data = dr.json()
        if not isinstance(data, list):
            data = data.get("data", data)
        if not isinstance(data, list):
            return None, f"Ошибка: неожиданный формат отчета хранения: {type(data)}"

        save_to_cache(cache_path, data)
        save_raw_json("STORAGE", company_name, data)
        return data, f"✅ Платное хранение сохранено. Строк: {len(data)}"

    except Exception as e:
        return None, f"Ошибка соединения (хранение): {e}"


# =========================
# WAREHOUSE REMAINS REPORT (ASYNC)
# =========================
def normalize_warehouse_remains_data(data) -> pd.DataFrame:
    """
    Преобразует отчет остатков в плоский DataFrame:
    одна строка = товар + склад
    """
    if data is None:
        return pd.DataFrame()

    if isinstance(data, dict):
        data = data.get("data", data)

    if not isinstance(data, list):
        return pd.DataFrame()

    rows = []

    for item in data:
        brand = item.get("brand", "")
        subject_name = item.get("subjectName", "")
        vendor_code = item.get("vendorCode", "")
        nm_id = item.get("nmId", "")
        barcode = item.get("barcode", "")
        tech_size = item.get("techSize", "")
        volume = item.get("volume", None)

        warehouses = item.get("warehouses", [])
        if isinstance(warehouses, list) and warehouses:
            for wh in warehouses:
                rows.append({
                    "brand": brand,
                    "subjectName": subject_name,
                    "vendorCode": str(vendor_code).strip(),
                    "nmId": str(nm_id).strip(),
                    "barcode": str(barcode).strip(),
                    "techSize": str(tech_size).strip(),
                    "volume": volume,
                    "warehouseName": wh.get("warehouseName", ""),
                    "quantity": wh.get("quantity", 0),
                })
        else:
            rows.append({
                "brand": brand,
                "subjectName": subject_name,
                "vendorCode": str(vendor_code).strip(),
                "nmId": str(nm_id).strip(),
                "barcode": str(barcode).strip(),
                "techSize": str(tech_size).strip(),
                "volume": volume,
                "warehouseName": "",
                "quantity": 0,
            })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0.0)

    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    if "nmId" in df.columns:
        df["nmId"] = _normalize_id_series_local(df["nmId"])

    if "barcode" in df.columns:
        df["barcode"] = _normalize_id_series_local(df["barcode"])

    if "vendorCode" in df.columns:
        df["vendorCode"] = df["vendorCode"].astype(str).str.strip()

    return df


def fetch_warehouse_remains_report(remaining_goods_token: str, company_name: str):
    """
    Отчет об остатках на складах WB.
    Асинхронная схема:
    1) generate
    2) status
    3) download
    """
    if not remaining_goods_token or str(remaining_goods_token).strip() == "" or str(remaining_goods_token).lower() == "nan":
        return None, "ℹ Остатки на складах: токен remaining_goods не указан в data_api.xlsx"

    cache_path = build_cache_file_path("stocks", company_name)
    cached_data = load_from_cache(cache_path)
    if cached_data is not None:
        if isinstance(cached_data, dict) and "data" in cached_data and isinstance(cached_data["data"], list):
            return cached_data["data"], f"⚡ Остатки на складах загружены из кэша. Строк товаров: {len(cached_data['data'])}"
        if isinstance(cached_data, list):
            return cached_data, f"⚡ Остатки на складах загружены из кэша. Строк товаров: {len(cached_data)}"

    headers = {"Authorization": str(remaining_goods_token).strip()}

    gen_url = f"{SELLER_ANALYTICS_BASE}/api/v1/warehouse_remains"
    gen_params = {
        "locale": "ru",
        "groupByNm": "true",
        "groupByBarcode": "true",
        "groupBySize": "true",
        "groupBySa": "true",
        "groupByBrand": "false",
        "groupBySubject": "false",
        "filterPics": "0",
    }

    try:
        r = requests.get(gen_url, headers=headers, params=gen_params, timeout=60)
        if r.status_code != 200:
            return None, f"Ошибка API остатков (generate) {r.status_code}: {r.text}"

        gen_json = r.json()
        task_id = (
            gen_json.get("data", {}).get("taskId")
            or gen_json.get("data", {}).get("id")
            or gen_json.get("taskId")
            or gen_json.get("id")
        )
        if not task_id:
            return None, f"Ошибка: не найден taskId в ответе остатков generate: {gen_json}"

        status_url = f"{SELLER_ANALYTICS_BASE}/api/v1/warehouse_remains/tasks/{task_id}/status"

        max_attempts = 24
        status_val = None
        for _ in range(max_attempts):
            sr = requests.get(status_url, headers=headers, timeout=30)
            if sr.status_code != 200:
                return None, f"Ошибка API остатков (status) {sr.status_code}: {sr.text}"

            sj = sr.json()
            status_val = sj.get("data", {}).get("status") or sj.get("status")

            if status_val == "done":
                break
            if status_val in ("failed", "error"):
                return None, f"❌ Остатки на складах: генерация завершилась со статусом {status_val}"

            time.sleep(5)

        if status_val != "done":
            return None, "⚠ Остатки на складах: не дождались завершения генерации (попробуй еще раз)."

        dl_url = f"{SELLER_ANALYTICS_BASE}/api/v1/warehouse_remains/tasks/{task_id}/download"
        dr = requests.get(dl_url, headers=headers, timeout=90)

        if dr.status_code == 204:
            data = []
            save_to_cache(cache_path, data)
            save_raw_json("STOCKS", company_name, data)
            return [], "ℹ Остатки на складах: нет данных."
        if dr.status_code != 200:
            return None, f"Ошибка API остатков (download) {dr.status_code}: {dr.text}"

        data = dr.json()
        save_to_cache(cache_path, data)
        save_raw_json("STOCKS", company_name, data)

        if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
            return data["data"], f"✅ Остатки на складах сохранены. Строк товаров: {len(data['data'])}"
        if isinstance(data, list):
            return data, f"✅ Остатки на складах сохранены. Строк товаров: {len(data)}"

        return None, f"Ошибка: неожиданный формат отчета остатков: {type(data)}"

    except Exception as e:
        return None, f"Ошибка соединения (остатки): {e}"


# =========================
# RUN LOAD
# =========================
def extract_region_sales_rows(payload):
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


def normalize_region_sales_df(rows) -> pd.DataFrame:
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

    text_columns = ["countryName", "foName", "regionName", "cityName", "sa"]
    for column in text_columns:
        if column in df.columns:
            df[column] = df[column].fillna("Не указано").astype(str).str.strip().replace({"": "Не указано"})

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


def fetch_region_sales_report(api_key: str, company_name: str, date_from, date_to):
    if date_from > date_to:
        return [], "❌ Продажи по регионам: дата начала больше даты конца."

    delta_days = (date_to - date_from).days + 1
    if delta_days > 31:
        return [], "⚠ Продажи по регионам: WB API позволяет период не более 31 дня за один запрос."

    if not api_key or str(api_key).strip() == "" or str(api_key).lower() == "nan":
        return [], "ℹ Продажи по регионам: токен regions не указан в data_api.xlsx"

    cache_path = build_cache_file_path("regions", company_name, date_from, date_to)
    cached_data = load_from_cache(cache_path)
    if cached_data is not None:
        rows = extract_region_sales_rows(cached_data)
        return rows, f"⚡ Продажи по регионам загружены из кэша. Строк: {len(rows)}"

    headers = {"Authorization": str(api_key).strip()}
    params = {
        "dateFrom": date_from.strftime("%Y-%m-%d"),
        "dateTo": date_to.strftime("%Y-%m-%d"),
    }
    url = f"{SELLER_ANALYTICS_BASE}/api/v1/analytics/region-sale"

    try:
        response = requests.get(url, headers=headers, params=params, timeout=90)
    except Exception as e:
        return [], f"❌ Продажи по регионам: ошибка запроса — {e}"

    if response.status_code != 200:
        return [], f"❌ Продажи по регионам API {response.status_code}: {response.text}"

    try:
        payload = response.json()
    except Exception as e:
        return [], f"❌ Продажи по регионам: не удалось прочитать JSON — {e}"

    rows = extract_region_sales_rows(payload)
    save_to_cache(cache_path, payload)
    save_raw_json("REGIONS", company_name, payload)
    return rows, f"✅ Продажи по регионам загружены из API. Строк: {len(rows)}"


def build_region_sales_geo_report(df_region_sales: pd.DataFrame) -> pd.DataFrame:
    if df_region_sales is None or df_region_sales.empty:
        return pd.DataFrame()

    for required_col in ["countryName", "foName", "regionName", "cityName"]:
        if required_col not in df_region_sales.columns:
            return pd.DataFrame()

    df = df_region_sales.copy()
    if "saleItemInvoiceQty" not in df.columns:
        df["saleItemInvoiceQty"] = 0
    if "saleInvoiceCostPrice" not in df.columns:
        df["saleInvoiceCostPrice"] = 0

    geo = (
        df.groupby(["countryName", "foName", "regionName", "cityName"], dropna=False, as_index=False)
        .agg({
            "saleItemInvoiceQty": "sum",
            "saleInvoiceCostPrice": "sum",
        })
    )

    total_sales = pd.to_numeric(geo["saleInvoiceCostPrice"], errors="coerce").fillna(0).sum()
    if total_sales != 0:
        geo["saleInvoiceCostPricePerc"] = (geo["saleInvoiceCostPrice"] / total_sales * 100).round(2)
    else:
        geo["saleInvoiceCostPricePerc"] = 0.0

    geo = geo.rename(columns={
        "countryName": "Страна",
        "foName": "Федеральный округ",
        "regionName": "Регион",
        "cityName": "Город",
        "saleItemInvoiceQty": "Продажи, шт",
        "saleInvoiceCostPrice": "Выручка, ₽",
        "saleInvoiceCostPricePerc": "Доля выручки, %",
    })

    geo = geo.sort_values(["Страна", "Федеральный округ", "Регион", "Город"], kind="stable").reset_index(drop=True)
    return geo




def enrich_region_sales_with_category(df_region_sales: pd.DataFrame, df_nom: pd.DataFrame) -> pd.DataFrame:
    if df_region_sales is None or df_region_sales.empty:
        return pd.DataFrame()

    df = df_region_sales.copy()

    if "nmID" not in df.columns:
        df["Категория"] = "Не найдено"
        return df

    df["_nm_id_norm"] = _normalize_id_series_local(df["nmID"])

    if (
        df_nom is None
        or not isinstance(df_nom, pd.DataFrame)
        or df_nom.empty
        or "nm_id" not in df_nom.columns
        or "subject" not in df_nom.columns
    ):
        df["Категория"] = "Не найдено"
        df = df.drop(columns=["_nm_id_norm"], errors="ignore")
        return df

    nom = df_nom[["nm_id", "subject"]].copy()
    nom["nm_id"] = _normalize_id_series_local(nom["nm_id"])
    nom["subject"] = nom["subject"].fillna("Не найдено").astype(str).str.strip()
    nom.loc[nom["subject"] == "", "subject"] = "Не найдено"
    nom = nom[nom["nm_id"] != ""].drop_duplicates(subset=["nm_id"])

    df = df.merge(
        nom.rename(columns={"nm_id": "_nm_id_norm", "subject": "Категория"}),
        on="_nm_id_norm",
        how="left"
    )
    df["Категория"] = df["Категория"].fillna("Не найдено").astype(str).str.strip()
    df.loc[df["Категория"] == "", "Категория"] = "Не найдено"
    df = df.drop(columns=["_nm_id_norm"], errors="ignore")
    return df

def prepare_report_bundle(
    company_name: str,
    api_fin: str,
    api_ads: str,
    api_storage: str,
    content_api: str,
    remaining_goods_api: str,
    regions_api: str,
    date_from,
    date_to
) -> dict:
    """
    Общая функция подготовки всех данных по компании и периоду.
    Используется и UI, и будущим daily-digest сценарием.
    Ничего не пишет в session_state — только возвращает готовый bundle.
    """
    df_nom, msg_nom = ensure_nomenclature_for_company(company_name, content_api)

    report_data, msg_fin = fetch_financial_report(api_fin, company_name, date_from, date_to)
    ads_data, msg_ads = fetch_advertising_report(api_ads, company_name, date_from, date_to)

    storage_data = None
    msg_storage = ""
    if api_storage and str(api_storage).strip() and str(api_storage).lower() != "nan":
        storage_data, msg_storage = fetch_paid_storage_report(api_storage, company_name, date_from, date_to)
    else:
        msg_storage = "ℹ Платное хранение: токен storage не указан в data_api.xlsx"

    stocks_data = None
    msg_stocks = ""
    if remaining_goods_api and str(remaining_goods_api).strip() and str(remaining_goods_api).lower() != "nan":
        stocks_data, msg_stocks = fetch_warehouse_remains_report(remaining_goods_api, company_name)
    else:
        msg_stocks = "ℹ Остатки на складах: токен remaining_goods не указан в data_api.xlsx"

    region_sales_data, msg_regions = fetch_region_sales_report(regions_api, company_name, date_from, date_to)

    df_fin = pd.DataFrame(report_data) if report_data else pd.DataFrame()
    df_ads = pd.DataFrame(ads_data) if ads_data else pd.DataFrame()
    df_ads = add_article_column_from_campname(df_ads)

    df_storage = pd.DataFrame(storage_data) if isinstance(storage_data, list) else pd.DataFrame()
    if df_storage.empty:
        cache_path = build_cache_file_path("storage", company_name, date_from, date_to)
        cached_storage = load_from_cache(cache_path)
        if isinstance(cached_storage, list) and len(cached_storage) > 0:
            df_storage = pd.DataFrame(cached_storage)

    df_stocks = normalize_warehouse_remains_data(stocks_data)
    df_region_sales = normalize_region_sales_df(region_sales_data)
    df_region_sales_geo = build_region_sales_geo_report(df_region_sales)
    df_price = load_price_list_parquet()

    period_label = f"{date_from} - {date_to}"

    try:
        df_analysis = create_analysis_report(
            df_raw=df_fin,
            company_name=company_name,
            period_str=period_label,
            df_ads=df_ads,
            df_storage=df_storage,
            df_stocks=df_stocks,
            df_nom=df_nom,
            df_price=df_price,
            nom_nm_col="nm_id",
            nom_cat_col="subject",
            add_total_row=True,
            total_label="Итого"
        )
    except TypeError:
        df_analysis = create_analysis_report(
            df_raw=df_fin,
            company_name=company_name,
            period_str=period_label,
            df_ads=df_ads,
            df_storage=df_storage,
            df_nom=df_nom,
            df_price=df_price,
            nom_nm_col="nm_id",
            nom_cat_col="subject",
            add_total_row=True,
            total_label="Итого"
        )

    df_missing_cost_barcodes = get_missing_cost_barcodes(df_fin, df_price)
    df_missing_cost_stocks = get_missing_cost_stocks_barcodes(df_stocks, df_price)
    df_stocks_by_warehouse = create_stocks_by_warehouse_report(df_stocks, df_nom)

    return {
        "company_name": company_name,
        "date_from": date_from,
        "date_to": date_to,
        "period_label": period_label,
        "report_data": report_data,
        "ads_data": ads_data,
        "storage_data": storage_data,
        "stocks_data": stocks_data,
        "region_sales_data": region_sales_data,
        "df_fin": df_fin,
        "df_ads": df_ads,
        "df_storage": df_storage,
        "df_stocks": df_stocks,
        "df_region_sales": df_region_sales,
        "df_region_sales_geo": df_region_sales_geo,
        "df_nom": df_nom,
        "df_price": df_price,
        "df_analysis": df_analysis,
        "df_missing_cost_barcodes": df_missing_cost_barcodes,
        "df_missing_cost_stocks": df_missing_cost_stocks,
        "df_stocks_by_warehouse": df_stocks_by_warehouse,
        "status_msg_nom": msg_nom,
        "status_msg_fin": msg_fin,
        "status_msg_ads": msg_ads,
        "status_msg_storage": msg_storage,
        "status_msg_stocks": msg_stocks,
        "status_msg_regions": msg_regions,
    }


def run_load(
    company_name: str,
    api_fin: str,
    api_ads: str,
    api_storage: str,
    content_api: str,
    remaining_goods_api: str,
    regions_api: str,
    date_from,
    date_to
):
    clear_loaded_data()

    bundle = prepare_report_bundle(
        company_name=company_name,
        api_fin=api_fin,
        api_ads=api_ads,
        api_storage=api_storage,
        content_api=content_api,
        remaining_goods_api=remaining_goods_api,
        regions_api=regions_api,
        date_from=date_from,
        date_to=date_to,
    )

    st.session_state.status_msg_nom = bundle["status_msg_nom"]
    st.session_state.status_msg_fin = bundle["status_msg_fin"]
    st.session_state.status_msg_ads = bundle["status_msg_ads"]
    st.session_state.status_msg_storage = bundle["status_msg_storage"]
    st.session_state.status_msg_stocks = bundle["status_msg_stocks"]
    st.session_state.status_msg_regions = bundle["status_msg_regions"]

    df_fin = bundle["df_fin"]
    df_ads = bundle["df_ads"]
    df_storage = bundle["df_storage"]
    df_stocks = bundle["df_stocks"]
    df_region_sales = bundle["df_region_sales"]
    df_region_sales_geo = bundle["df_region_sales_geo"]
    df_nom = bundle["df_nom"]
    df_price = bundle["df_price"]
    df_analysis = bundle["df_analysis"]
    df_missing_cost_barcodes = bundle["df_missing_cost_barcodes"]
    df_missing_cost_stocks = bundle["df_missing_cost_stocks"]
    df_stocks_by_warehouse = bundle["df_stocks_by_warehouse"]

    excel_fin_raw = to_excel(df_fin, sheet_name="Financial_Report") if not df_fin.empty else None
    excel_ads_raw = to_excel(df_ads, sheet_name="Ads_Report") if not df_ads.empty else None
    excel_storage_raw = to_excel(df_storage, sheet_name="Paid_Storage") if not df_storage.empty else None
    excel_stocks_raw = to_excel(df_stocks, sheet_name="Warehouse_Remains") if not df_stocks.empty else None
    excel_region_sales = to_excel(df_region_sales, sheet_name="Region_Sales") if not df_region_sales.empty else None
    excel_analysis = to_excel(df_analysis, sheet_name="Analysis") if df_analysis is not None else None
    excel_stocks_by_warehouse = (
        to_excel(df_stocks_by_warehouse, sheet_name="Stocks_By_Warehouse")
        if isinstance(df_stocks_by_warehouse, pd.DataFrame) and not df_stocks_by_warehouse.empty
        else None
    )

    excel_missing_cost_barcodes = (
        to_excel(df_missing_cost_barcodes, sheet_name="Missing_Cost_Barcodes")
        if isinstance(df_missing_cost_barcodes, pd.DataFrame) and not df_missing_cost_barcodes.empty
        else None
    )

    excel_missing_cost_stocks = (
        to_excel(df_missing_cost_stocks, sheet_name="Missing_Cost_Stocks")
        if isinstance(df_missing_cost_stocks, pd.DataFrame) and not df_missing_cost_stocks.empty
        else None
    )

    st.session_state.loaded = True
    st.session_state.selected_company = company_name
    st.session_state.date_from = date_from
    st.session_state.date_to = date_to

    st.session_state.report_data = bundle["report_data"]
    st.session_state.ads_data = bundle["ads_data"]
    st.session_state.storage_data = bundle["storage_data"]
    st.session_state.stocks_data = bundle["stocks_data"]
    st.session_state.region_sales_data = bundle["region_sales_data"]

    st.session_state.df_fin = df_fin
    st.session_state.df_ads = df_ads
    st.session_state.df_storage = df_storage
    st.session_state.df_stocks = df_stocks
    st.session_state.df_region_sales = df_region_sales
    st.session_state.df_region_sales_geo = df_region_sales_geo
    st.session_state.df_nom = df_nom
    st.session_state.df_price = df_price
    st.session_state.df_analysis = df_analysis
    st.session_state.df_missing_cost_barcodes = df_missing_cost_barcodes
    st.session_state.df_missing_cost_stocks = df_missing_cost_stocks
    st.session_state.df_stocks_by_warehouse = df_stocks_by_warehouse

    st.session_state.excel_fin_raw = excel_fin_raw
    st.session_state.excel_ads_raw = excel_ads_raw
    st.session_state.excel_storage_raw = excel_storage_raw
    st.session_state.excel_stocks_raw = excel_stocks_raw
    st.session_state.excel_region_sales = excel_region_sales
    st.session_state.excel_analysis = excel_analysis
    st.session_state.excel_missing_cost_barcodes = excel_missing_cost_barcodes
    st.session_state.excel_missing_cost_stocks = excel_missing_cost_stocks
    st.session_state.excel_stocks_by_warehouse = excel_stocks_by_warehouse

    st.session_state.ts_loaded = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def build_sales_summary_report(df_analysis: pd.DataFrame) -> pd.DataFrame:
    if df_analysis is None or df_analysis.empty:
        return pd.DataFrame()

    summary_columns = [
        "Кабинет",
        "Период",
        "Категория",
        "Продаж штук",
        "Себестоимость",
        "Прибыль",
        "Рентабельность",
        "Реклама",
        "Хранение",
        "Отзывы за баллы",
        "ДРР",
        "Остаток FBO, штук",
        "Остаток FBO, рублей",
    ]

    available_columns = [col for col in summary_columns if col in df_analysis.columns]
    if not available_columns:
        return pd.DataFrame()

    df = df_analysis[available_columns].copy()

    numeric_columns = [
        "Продаж штук",
        "Себестоимость",
        "Прибыль",
        "Рентабельность",
        "Реклама",
        "Хранение",
        "Отзывы за баллы",
        "ДРР",
        "Остаток FBO, штук",
        "Остаток FBO, рублей",
    ]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    if "Прибыль" in df.columns and "Себестоимость" in df.columns:
        profit = pd.to_numeric(df["Прибыль"], errors="coerce").fillna(0.0)
        cost = pd.to_numeric(df["Себестоимость"], errors="coerce").fillna(0.0)
        df["Рентабельность"] = 0.0
        non_zero_mask = cost != 0
        df.loc[non_zero_mask, "Рентабельность"] = ((profit[non_zero_mask] / cost[non_zero_mask]) * 100).round(1)

    return df


def _build_total_kpi_from_summary(df_summary: pd.DataFrame) -> dict:
    if df_summary is None or df_summary.empty:
        return {
            "Продаж штук": 0,
            "Прибыль": 0.0,
            "Рентабельность": 0.0,
            "Реклама": 0.0,
            "Хранение": 0.0,
            "Остаток FBO, ₽": 0.0,
        }

    df = df_summary.copy()
    if "Категория" in df.columns:
        total_mask = df["Категория"].astype(str).str.strip() == "Итого"
        if total_mask.any():
            total_row = df.loc[total_mask].iloc[-1]
        else:
            total_row = None
    else:
        total_row = None

    numeric_columns = [
        "Продаж штук",
        "Прибыль",
        "Себестоимость",
        "Реклама",
        "Хранение",
        "Остаток FBO, рублей",
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    if total_row is not None:
        sales_qty = float(pd.to_numeric(total_row.get("Продаж штук", 0), errors="coerce"))
        profit = float(pd.to_numeric(total_row.get("Прибыль", 0), errors="coerce"))
        cost = float(pd.to_numeric(total_row.get("Себестоимость", 0), errors="coerce"))
        ads = float(pd.to_numeric(total_row.get("Реклама", 0), errors="coerce"))
        storage = float(pd.to_numeric(total_row.get("Хранение", 0), errors="coerce"))
        stocks_rub = float(pd.to_numeric(total_row.get("Остаток FBO, рублей", 0), errors="coerce"))
    else:
        if "Категория" in df.columns:
            df = df[df["Категория"].astype(str).str.strip() != "Итого"].copy()
        sales_qty = float(df["Продаж штук"].sum()) if "Продаж штук" in df.columns else 0.0
        profit = float(df["Прибыль"].sum()) if "Прибыль" in df.columns else 0.0
        cost = float(df["Себестоимость"].sum()) if "Себестоимость" in df.columns else 0.0
        ads = float(df["Реклама"].sum()) if "Реклама" in df.columns else 0.0
        storage = float(df["Хранение"].sum()) if "Хранение" in df.columns else 0.0
        stocks_rub = float(df["Остаток FBO, рублей"].sum()) if "Остаток FBO, рублей" in df.columns else 0.0

    profitability = round((profit / cost) * 100, 1) if cost else 0.0

    return {
        "Продаж штук": int(round(sales_qty)),
        "Прибыль": round(profit, 2),
        "Рентабельность": profitability,
        "Реклама": round(ads, 2),
        "Хранение": round(storage, 2),
        "Остаток FBO, ₽": round(stocks_rub, 2),
    }


def get_company_kpi_and_missing_cost(
    company_name: str,
    api_fin: str,
    api_ads: str,
    api_storage: str,
    content_api: str,
    remaining_goods_api: str,
    regions_api: str,
    date_from,
    date_to,
) -> dict:
    """
    Возвращает KPI по компании за период + список баркодов без себестоимости.
    Это первая базовая функция для будущего daily-digest и Telegram-уведомлений.
    """
    result = {
        "status": "error",
        "error": "",
        "company_name": company_name,
        "date_from": str(date_from),
        "date_to": str(date_to),
        "kpi": {
            "Продаж штук": 0,
            "Прибыль": 0.0,
            "Рентабельность": 0.0,
            "Реклама": 0.0,
            "Хранение": 0.0,
            "Остаток FBO, ₽": 0.0,
        },
        "missing_cost_barcodes": [],
        "missing_cost_count": 0,
        "df_summary": pd.DataFrame(),
        "df_analysis": pd.DataFrame(),
    }

    try:
        bundle = prepare_report_bundle(
            company_name=company_name,
            api_fin=api_fin,
            api_ads=api_ads,
            api_storage=api_storage,
            content_api=content_api,
            remaining_goods_api=remaining_goods_api,
            regions_api=regions_api,
            date_from=date_from,
            date_to=date_to,
        )

        df_analysis = bundle.get("df_analysis", pd.DataFrame())
        df_summary = build_sales_summary_report(df_analysis)
        kpi = _build_total_kpi_from_summary(df_summary)

        df_missing = bundle.get("df_missing_cost_barcodes", pd.DataFrame())
        if isinstance(df_missing, pd.DataFrame) and not df_missing.empty and "Баркод" in df_missing.columns:
            missing_cost_barcodes = (
                df_missing["Баркод"]
                .astype(str)
                .str.strip()
                .replace({"nan": "", "None": ""})
            )
            missing_cost_barcodes = sorted([x for x in missing_cost_barcodes.tolist() if x])
        else:
            missing_cost_barcodes = []

        result["status"] = "success"
        result["kpi"] = kpi
        result["missing_cost_barcodes"] = missing_cost_barcodes
        result["missing_cost_count"] = len(missing_cost_barcodes)
        result["df_summary"] = df_summary
        result["df_analysis"] = df_analysis
        return result
    except Exception as e:
        result["error"] = str(e)
        return result


def format_metric_int(value) -> str:
    value = pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0).iloc[0]
    return f"{int(round(value)):,}".replace(",", " ")


def format_metric_money(value, decimals: int = 2) -> str:
    value = pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0).iloc[0]
    return f"{value:,.{decimals}f} ₽".replace(",", " ")


def format_metric_percent(value, decimals: int = 1) -> str:
    value = pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0).iloc[0]
    return f"{value:.{decimals}f}%"


def format_daily_kpi_message(company_name: str, report_date, kpi: dict) -> str:
    report_date_str = report_date.strftime("%d.%m.%Y") if hasattr(report_date, "strftime") else str(report_date)
    return (
        "📊 WB Analytics\n\n"
        f"Дата отчета: {report_date_str}\n"
        f"Компания: {company_name}\n\n"
        f"Продаж, штук: {format_metric_int(kpi.get('Продаж штук', 0))}\n"
        f"Прибыль: {format_metric_money(kpi.get('Прибыль', 0), 2)}\n"
        f"Рентабельность: {format_metric_percent(kpi.get('Рентабельность', 0), 1)}\n"
        f"Реклама: {format_metric_money(kpi.get('Реклама', 0), 2)}\n"
        f"Хранение: {format_metric_money(kpi.get('Хранение', 0), 2)}\n"
        f"Остаток FBO, ₽: {format_metric_money(kpi.get('Остаток FBO, ₽', 0), 2)}"
    )


def format_missing_cost_message(company_name: str, report_date, barcodes: list[str]) -> str:
    report_date_str = report_date.strftime("%d.%m.%Y") if hasattr(report_date, "strftime") else str(report_date)
    barcode_lines = "\n".join(str(bc) for bc in barcodes[:200])
    extra = ""
    if len(barcodes) > 200:
        extra = f"\n... и еще {len(barcodes) - 200} шт."
    return (
        "⚠️ Нет себестоимости\n\n"
        f"Дата отчета: {report_date_str}\n"
        f"Компания: {company_name}\n"
        f"Количество баркодов без себестоимости: {len(barcodes)}\n\n"
        "Баркоды:\n"
        f"{barcode_lines}{extra}"
    )


def send_daily_kpi_for_all_companies(companies_df: pd.DataFrame, report_date) -> dict:
    results = {
        "report_date": report_date.strftime("%d.%m.%Y") if hasattr(report_date, "strftime") else str(report_date),
        "success_companies": [],
        "admin_alert_companies": [],
        "error_companies": [],
        "logs": [],
    }

    if companies_df is None or companies_df.empty:
        results["error_companies"].append("Нет компаний для обработки")
        results["logs"].append("❌ Не найден список компаний")
        return results

    has_storage_col = "storage" in companies_df.columns
    has_content_col = "content" in companies_df.columns
    has_remaining_goods_col = "remaining_goods" in companies_df.columns
    has_regions_col = "regions" in companies_df.columns

    for _, row in companies_df.iterrows():
        company_name = str(row.get("company", "")).strip()
        if not company_name:
            continue

        try:
            result = get_company_kpi_and_missing_cost(
                company_name=company_name,
                api_fin=row.get("api", ""),
                api_ads=row.get("advertising_api", ""),
                api_storage=row.get("storage", "") if has_storage_col else "",
                content_api=row.get("content", "") if has_content_col else "",
                remaining_goods_api=row.get("remaining_goods", "") if has_remaining_goods_col else "",
                regions_api=row.get("regions", "") if has_regions_col else "",
                date_from=report_date,
                date_to=report_date,
            )

            if result.get("status") != "success":
                err = result.get("error", "Неизвестная ошибка")
                send_admin_message(
                    f"❌ Ошибка daily KPI\n\nДата отчета: {results['report_date']}\nКомпания: {company_name}\nОшибка: {err}"
                )
                results["error_companies"].append(company_name)
                results["logs"].append(f"❌ {company_name}: ошибка — {err}")
                continue

            missing_barcodes = result.get("missing_cost_barcodes", []) or []
            if missing_barcodes:
                admin_text = format_missing_cost_message(company_name, report_date, missing_barcodes)
                send_admin_message(admin_text)
                results["admin_alert_companies"].append(company_name)
                results["logs"].append(
                    f"⚠️ {company_name}: отправлено админу, нет себестоимости по {len(missing_barcodes)} баркодам"
                )
                continue

            text = format_daily_kpi_message(company_name, report_date, result.get("kpi", {}))
            send_results = send_users_message(company_name, text)

            if send_results:
                ok_count = sum(1 for x in send_results if x.get("ok"))
                results["success_companies"].append(company_name)
                results["logs"].append(
                    f"✅ {company_name}: KPI отправлен получателям ({ok_count}/{len(send_results)})"
                )
            else:
                results["logs"].append(
                    f"ℹ️ {company_name}: для компании не найдено получателей в telegram_users"
                )

        except Exception as e:
            send_admin_message(
                f"❌ Ошибка daily KPI\n\nДата отчета: {results['report_date']}\nКомпания: {company_name}\nОшибка: {e}"
            )
            results["error_companies"].append(company_name)
            results["logs"].append(f"❌ {company_name}: исключение — {e}")

    return results


def read_json_file(path: str, default):
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def write_json_file(path: str, data):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def append_kpi_log(entry: dict):
    logs = read_json_file(KPI_LOG_PATH, [])
    if not isinstance(logs, list):
        logs = []
    logs.append(entry)
    logs = logs[-200:]
    write_json_file(KPI_LOG_PATH, logs)


def auto_send_daily_kpi(companies_df: pd.DataFrame):
    """
    Автоматическая отправка KPI после 11:00 по Москве.
    Срабатывает не чаще одного раза в день.
    Для Streamlit Cloud отправка произойдет при первом открытии приложения после 11:00 МСК.

    Важно:
    Streamlit может несколько раз перезапускать скрипт при открытии страницы,
    поэтому защитная запись в LAST_KPI_SEND_PATH ставится ДО фактической отправки.
    """
    try:
        now_msk = datetime.now(ZoneInfo("Europe/Moscow"))
        today_str = now_msk.strftime("%Y-%m-%d")

        if now_msk.hour < 11:
            return None

        state = read_json_file(LAST_KPI_SEND_PATH, {})
        if isinstance(state, dict) and state.get("date") == today_str:
            return state

        report_date = (now_msk - timedelta(days=1)).date()

        # Ставим защитную метку ДО отправки, чтобы второй rerun не отправил дубли.
        pending_state = {
            "date": today_str,
            "report_date": report_date.strftime("%Y-%m-%d"),
            "sent_at_msk": now_msk.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "sending",
            "success_companies": [],
            "admin_alert_companies": [],
            "error_companies": [],
            "logs": ["ℹ️ Автоотправка KPI запущена"],
        }
        write_json_file(LAST_KPI_SEND_PATH, pending_state)

        results = send_daily_kpi_for_all_companies(companies_df, report_date)

        state_payload = {
            "date": today_str,
            "report_date": report_date.strftime("%Y-%m-%d"),
            "sent_at_msk": now_msk.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "sent",
            "success_companies": results.get("success_companies", []),
            "admin_alert_companies": results.get("admin_alert_companies", []),
            "error_companies": results.get("error_companies", []),
            "logs": results.get("logs", []),
        }
        write_json_file(LAST_KPI_SEND_PATH, state_payload)

        append_kpi_log({
            "date": today_str,
            "report_date": report_date.strftime("%Y-%m-%d"),
            "sent_at_msk": now_msk.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "sent",
            "success_count": len(results.get("success_companies", [])),
            "admin_alert_count": len(results.get("admin_alert_companies", [])),
            "error_count": len(results.get("error_companies", [])),
            "logs": results.get("logs", []),
        })

        return state_payload

    except Exception as e:
        try:
            send_admin_message(f"❌ Ошибка автоотправки KPI\n\nОшибка: {e}")
        except Exception:
            pass

        error_now = datetime.now(ZoneInfo("Europe/Moscow"))
        error_payload = {
            "date": error_now.strftime("%Y-%m-%d"),
            "report_date": "",
            "sent_at_msk": error_now.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "error",
            "success_companies": [],
            "admin_alert_companies": [],
            "error_companies": ["auto_send_daily_kpi"],
            "logs": [f"❌ Ошибка автоотправки KPI: {e}"],
        }
        write_json_file(LAST_KPI_SEND_PATH, error_payload)

        append_kpi_log({
            "date": error_payload["date"],
            "report_date": "",
            "sent_at_msk": error_payload["sent_at_msk"],
            "status": "error",
            "success_count": 0,
            "admin_alert_count": 0,
            "error_count": 1,
            "logs": error_payload["logs"],
        })
        return None


# =========================
# UI
# =========================
st.sidebar.title("Меню управления")
choice = st.sidebar.radio("Переключить вкладку:", ["📊 Отчеты", "📥 Загрузить Прайс"])

if choice == "📥 Загрузить Прайс":
    st.title("📥 Загрузка прайс-листа")
    st.info(f"Файл будет сохранен по пути: {PRICE_SAVE_PATH}")

    uploaded_file = st.file_uploader("Выберите Excel файл для конвертации в Parquet", type=["xlsx", "xls"])
    if uploaded_file:
        try:
            df_p = pd.read_excel(uploaded_file)
            parquet_file = os.path.join(PRICE_SAVE_PATH, "price_list.parquet")
            df_p.to_parquet(parquet_file, index=False, engine="pyarrow")
            st.success(f"✅ Готово! Файл перезаписан. Строк в прайсе: {len(df_p)}")
            st.dataframe(df_p.head(10), use_container_width=True)
        except Exception as e:
            st.error(f"Ошибка обработки прайса: {e}")

    if os.path.exists(PRICE_PARQUET_PATH):
        df_price_now = load_price_list_parquet()
        if not df_price_now.empty:
            st.write("### Текущий price_list.parquet (первые 10 строк)")
            st.dataframe(df_price_now.head(10), use_container_width=True)
        else:
            st.warning("price_list.parquet есть, но не удалось прочитать или он пуст.")

else:
    st.title("💰 Финансовый отчет WB (Детализация реализации)")

    companies_df = get_companies()
    if companies_df.empty:
        st.error("Файл data_api.xlsx пуст или не найден!")
        st.stop()

    has_storage_col = "storage" in companies_df.columns
    has_content_col = "content" in companies_df.columns
    has_remaining_goods_col = "remaining_goods" in companies_df.columns
    has_regions_col = "regions" in companies_df.columns

    auto_send_daily_kpi(companies_df)

    company_list = list(companies_df["company"].unique())
    default_company = st.session_state.selected_company if st.session_state.selected_company in company_list else company_list[0]
    target_company = st.sidebar.selectbox("Выберите компанию:", company_list, index=company_list.index(default_company))

    if st.session_state.selected_company is not None and target_company != st.session_state.selected_company:
        clear_loaded_data()
        st.session_state.selected_company = target_company

    row = companies_df[companies_df["company"] == target_company].iloc[0]
    raw_api = row["api"]
    advertising_api = row["advertising_api"]
    storage_api = row["storage"] if has_storage_col else ""
    content_api = row["content"] if has_content_col else ""
    remaining_goods_api = row["remaining_goods"] if has_remaining_goods_col else ""
    regions_api = row["regions"] if has_regions_col else ""

    st.sidebar.subheader("Настройка периода")
    default_from = datetime.now() - timedelta(days=7)
    default_to = datetime.now() - timedelta(days=1)

    if st.session_state.date_from is None:
        st.session_state.date_from = default_from
    if st.session_state.date_to is None:
        st.session_state.date_to = default_to

    date_from = st.sidebar.date_input("Дата начала:", st.session_state.date_from)
    date_to = st.sidebar.date_input("Дата конца:", st.session_state.date_to)

    period_days = (date_to - date_from).days + 1
    period_too_long = period_days > 7
    if period_too_long:
        st.sidebar.warning("Выберите период не больше 7 дней")

    if (st.session_state.date_from != date_from) or (st.session_state.date_to != date_to):
        clear_loaded_data()
        st.session_state.date_from = date_from
        st.session_state.date_to = date_to

    if st.sidebar.button("Получить финансовый отчет"):
        if period_too_long:
            st.sidebar.warning("Выберите период не больше 7 дней")
        else:
            with st.spinner("Загрузка данных..."):
                run_load(
                    target_company,
                    raw_api,
                    advertising_api,
                    storage_api,
                    content_api,
                    remaining_goods_api,
                    regions_api,
                    date_from,
                    date_to
                )

    with st.sidebar.expander("🧪 Тест daily KPI функции", expanded=False):
        st.caption("Проверка новой функции: KPI по компании + баркоды без себестоимости")
        if st.button("Запустить тест KPI функции", key="btn_test_kpi_function"):
            if period_too_long:
                st.warning("Для теста выберите период не больше 7 дней")
            else:
                with st.spinner("Тестируем KPI-функцию..."):
                    test_result = get_company_kpi_and_missing_cost(
                        company_name=target_company,
                        api_fin=raw_api,
                        api_ads=advertising_api,
                        api_storage=storage_api,
                        content_api=content_api,
                        remaining_goods_api=remaining_goods_api,
                        regions_api=regions_api,
                        date_from=date_from,
                        date_to=date_to,
                    )
                    st.session_state.test_kpi_result = test_result
                    st.session_state.test_kpi_error = test_result.get("error", "")

    with st.sidebar.expander("🤖 Telegram тест", expanded=False):
        st.caption("Проверка подключения Telegram-бота")
        if st.button("Отправить тестовое сообщение", key="btn_test_telegram_message"):
            try:
                ok = send_admin_message("✅ Telegram подключен к WB Analytics Platform")
                if ok:
                    st.sidebar.success("Сообщение отправлено")
                else:
                    st.sidebar.error("Ошибка отправки")
            except Exception as e:
                st.sidebar.error(f"Ошибка Telegram: {e}")

    with st.sidebar.expander("📨 Daily KPI в Telegram", expanded=False):
        st.caption("Ручной запуск KPI по всем компаниям за вчерашнюю дату")
        yesterday_report_date = (datetime.now() - timedelta(days=1)).date()
        st.write(f"Дата отчета: {yesterday_report_date.strftime('%d.%m.%Y')}")
        if st.button("Отправить KPI по всем компаниям", key="btn_send_daily_kpi_all"):
            with st.spinner("Отправляем KPI по всем компаниям..."):
                send_result = send_daily_kpi_for_all_companies(companies_df, yesterday_report_date)
                st.session_state["daily_kpi_send_result"] = send_result

    daily_kpi_send_result = st.session_state.get("daily_kpi_send_result")
    if daily_kpi_send_result:
        st.sidebar.info(
            f"Daily KPI: успешно {len(daily_kpi_send_result.get('success_companies', []))}, "
            f"алертов админу {len(daily_kpi_send_result.get('admin_alert_companies', []))}, "
            f"ошибок {len(daily_kpi_send_result.get('error_companies', []))}"
        )

    if st.session_state.status_msg_nom:
        msg = st.session_state.status_msg_nom
        if msg.startswith("✅") or msg.startswith("ℹ"):
            st.sidebar.info(msg)
        elif msg.startswith("⚠"):
            st.sidebar.warning(msg)
        else:
            st.sidebar.info(msg)

    if st.session_state.status_msg_fin:
        st.sidebar.info(st.session_state.status_msg_fin)

    if st.session_state.status_msg_ads:
        msg = st.session_state.status_msg_ads
        if msg.startswith("✅") or msg.startswith("⚡"):
            st.sidebar.info(msg)
        else:
            st.sidebar.warning(msg)

    if st.session_state.status_msg_storage:
        msg = st.session_state.status_msg_storage
        if msg.startswith("✅") or msg.startswith("ℹ") or msg.startswith("⚡"):
            st.sidebar.info(msg)
        elif msg.startswith("⚠"):
            st.sidebar.warning(msg)
        elif msg.startswith("❌"):
            st.sidebar.error(msg)
        else:
            st.sidebar.info(msg)

    if st.session_state.status_msg_stocks:
        msg = st.session_state.status_msg_stocks
        if msg.startswith("✅") or msg.startswith("ℹ") or msg.startswith("⚡"):
            st.sidebar.info(msg)
        elif msg.startswith("⚠"):
            st.sidebar.warning(msg)
        elif msg.startswith("❌"):
            st.sidebar.error(msg)
        else:
            st.sidebar.info(msg)

    if st.session_state.status_msg_regions:
        msg = st.session_state.status_msg_regions
        if msg.startswith("✅") or msg.startswith("ℹ") or msg.startswith("⚡"):
            st.sidebar.info(msg)
        elif msg.startswith("⚠"):
            st.sidebar.warning(msg)
        elif msg.startswith("❌"):
            st.sidebar.error(msg)
        else:
            st.sidebar.info(msg)

    if st.session_state.test_kpi_result is not None:
        st.markdown("## 🧪 Результат теста KPI-функции")
        test_result = st.session_state.test_kpi_result
        if test_result.get("status") != "success":
            st.error(f"Ошибка тестовой функции: {test_result.get('error', 'Неизвестная ошибка')}")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Компания", test_result.get("company_name", ""))
            c2.metric("Период с", test_result.get("date_from", ""))
            c3.metric("Период по", test_result.get("date_to", ""))

            kpi = test_result.get("kpi", {})
            k1, k2, k3 = st.columns(3)
            k1.metric("Продаж, штук", format_metric_int(kpi.get("Продаж штук", 0)))
            k2.metric("Прибыль", format_metric_money(kpi.get("Прибыль", 0)))
            k3.metric("Рентабельность", format_metric_percent(kpi.get("Рентабельность", 0)))

            k4, k5, k6 = st.columns(3)
            k4.metric("Реклама", format_metric_money(kpi.get("Реклама", 0)))
            k5.metric("Хранение", format_metric_money(kpi.get("Хранение", 0)))
            k6.metric("Остаток FBO, ₽", format_metric_money(kpi.get("Остаток FBO, ₽", 0)))

            st.write(f"**Баркодов без себестоимости:** {test_result.get('missing_cost_count', 0)}")
            missing_list = test_result.get("missing_cost_barcodes", [])
            if missing_list:
                st.code("\n".join(missing_list))
            else:
                st.success("По продажам за выбранный период все баркоды имеют себестоимость.")

    if st.session_state.loaded and isinstance(st.session_state.df_fin, pd.DataFrame) and not st.session_state.df_fin.empty:
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📌 Сводная по продажам",
            "📊 Аналитика и расчеты",
            "📦 Остатки по складам",
            "🌍 География продаж",
            "📋 Исходный отчет"
        ])

        with tab5:
            st.write(f"### 📋 Полные данные: {target_company}")
            st.dataframe(st.session_state.df_fin, use_container_width=True)

            if st.session_state.excel_fin_raw:
                st.download_button(
                    label="📥 Скачать исходный Excel",
                    data=st.session_state.excel_fin_raw,
                    file_name=f"WB_Raw_{target_company}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    key="dl_fin_raw"
                )

            if isinstance(st.session_state.df_ads, pd.DataFrame) and not st.session_state.df_ads.empty:
                st.write("### 📣 История затрат на рекламу (исходные данные)")
                st.dataframe(st.session_state.df_ads, use_container_width=True)

                if st.session_state.excel_ads_raw:
                    st.download_button(
                        label="📥 Скачать исходный файл по рекламе",
                        data=st.session_state.excel_ads_raw,
                        file_name=f"WB_Ads_{target_company}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        key="dl_ads_raw"
                    )

            if isinstance(st.session_state.df_storage, pd.DataFrame) and not st.session_state.df_storage.empty:
                st.write("### 🧊 Платное хранение (исходные данные)")
                st.dataframe(st.session_state.df_storage, use_container_width=True)

                if st.session_state.excel_storage_raw:
                    st.download_button(
                        label="📥 Скачать исходный файл по хранению",
                        data=st.session_state.excel_storage_raw,
                        file_name=f"WB_Storage_{target_company}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        key="dl_storage_raw"
                    )
            else:
                if has_storage_col and storage_api and str(storage_api).strip() and str(storage_api).lower() != "nan":
                    st.info("Платное хранение: данных нет (или период > 8 дней).")

            if isinstance(st.session_state.df_stocks, pd.DataFrame) and not st.session_state.df_stocks.empty:
                st.write("### 📦 Остатки на складах (исходные данные)")
                st.dataframe(st.session_state.df_stocks, use_container_width=True)

                if st.session_state.excel_stocks_raw:
                    st.download_button(
                        label="📥 Скачать исходный файл по остаткам",
                        data=st.session_state.excel_stocks_raw,
                        file_name=f"WB_Stocks_{target_company}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        key="dl_stocks_raw"
                    )
            else:
                if has_remaining_goods_col and remaining_goods_api and str(remaining_goods_api).strip() and str(remaining_goods_api).lower() != "nan":
                    st.info("Остатки на складах: данных нет.")

        with tab2:
            st.write("### 📈 Итоговые показатели по категориям")

            if isinstance(st.session_state.df_missing_cost_barcodes, pd.DataFrame) and not st.session_state.df_missing_cost_barcodes.empty:
                st.warning("Нет цены на товар")
                if st.session_state.excel_missing_cost_barcodes:
                    st.download_button(
                        label="📥 Скачать Excel с баркодами без цены",
                        data=st.session_state.excel_missing_cost_barcodes,
                        file_name=f"WB_Missing_Cost_Barcodes_{target_company}.xlsx",
                        key="dl_missing_cost_barcodes"
                    )

            if isinstance(st.session_state.df_missing_cost_stocks, pd.DataFrame) and not st.session_state.df_missing_cost_stocks.empty:
                st.warning("Есть остатки товаров без себестоимости")
                if st.session_state.excel_missing_cost_stocks:
                    st.download_button(
                        label="📥 Скачать Excel с баркодами остатков без себестоимости",
                        data=st.session_state.excel_missing_cost_stocks,
                        file_name=f"WB_Missing_Cost_Stocks_Barcodes_{target_company}.xlsx",
                        key="dl_missing_cost_stocks"
                    )

            if isinstance(st.session_state.df_analysis, pd.DataFrame) and not st.session_state.df_analysis.empty:
                df_all = st.session_state.df_analysis.copy()

                if "Категория" in df_all.columns:
                    df_total = df_all[df_all["Категория"].astype(str).str.strip() == "Итого"].copy()
                    df_main = df_all[df_all["Категория"].astype(str).str.strip() != "Итого"].copy()
                else:
                    df_total = pd.DataFrame()
                    df_main = df_all

                st.dataframe(df_main, use_container_width=True, height=560)

                if not df_total.empty:
                    df_total = df_total.tail(1).copy()

                    def _style_total_row(_row):
                        return [
                            "color:#b30000; font-weight:700; background-color:#ffe6e6; "
                            "border-top:3px solid #b30000"
                        ] * len(_row)

                    styled_total = (
                        df_total.style
                        .apply(_style_total_row, axis=1)
                        .format(precision=2)
                    )

                    st.markdown("**Итого (закреплено снизу):**")
                    st.dataframe(styled_total, use_container_width=True, height=90)

                if st.session_state.excel_analysis:
                    st.download_button(
                        label="📥 Скачать аналитический отчет",
                        data=st.session_state.excel_analysis,
                        file_name=f"WB_Analysis_{target_company}.xlsx",
                        key="dl_analysis"
                    )
            else:
                st.warning("Нет данных для аналитики. Проверь отчеты и/или номенклатуру.")

        with tab3:
            st.write("### 📦 Остатки по складам (штук)")

            if isinstance(st.session_state.df_stocks_by_warehouse, pd.DataFrame) and not st.session_state.df_stocks_by_warehouse.empty:
                df_sw_all = st.session_state.df_stocks_by_warehouse.copy()

                if "Категория" in df_sw_all.columns:
                    df_sw_total = df_sw_all[df_sw_all["Категория"].astype(str).str.strip() == "Итого"].copy()
                    df_sw_main = df_sw_all[df_sw_all["Категория"].astype(str).str.strip() != "Итого"].copy()
                else:
                    df_sw_total = pd.DataFrame()
                    df_sw_main = df_sw_all

                st.dataframe(df_sw_main, use_container_width=True, height=560)

                if not df_sw_total.empty:
                    df_sw_total = df_sw_total.tail(1).copy()

                    def _style_total_row_stocks(_row):
                        return [
                            "color:#b30000; font-weight:700; background-color:#ffe6e6; "
                            "border-top:3px solid #b30000"
                        ] * len(_row)

                    styled_sw_total = (
                        df_sw_total.style
                        .apply(_style_total_row_stocks, axis=1)
                        .format(precision=0)
                    )

                    st.markdown("**Итого по складам (закреплено снизу):**")
                    st.dataframe(styled_sw_total, use_container_width=True, height=90)

                if st.session_state.excel_stocks_by_warehouse:
                    st.download_button(
                        label="📥 Скачать остатки по складам",
                        data=st.session_state.excel_stocks_by_warehouse,
                        file_name=f"WB_Stocks_By_Warehouse_{target_company}.xlsx",
                        key="dl_stocks_by_warehouse"
                    )
            else:
                st.warning("Нет данных для отчета по складам. Сначала загрузите отчет остатков.")

        with tab4:
            st.write("### 🌍 География продаж")

            if isinstance(st.session_state.df_region_sales, pd.DataFrame) and not st.session_state.df_region_sales.empty:
                df_region_sales_enriched = enrich_region_sales_with_category(
                    st.session_state.df_region_sales,
                    st.session_state.df_nom
                )

                category_options = ["Все категории"]
                if "Категория" in df_region_sales_enriched.columns:
                    category_values = (
                        df_region_sales_enriched["Категория"]
                        .fillna("Не найдено")
                        .astype(str)
                        .str.strip()
                        .replace({"": "Не найдено"})
                        .unique()
                        .tolist()
                    )
                    category_options += sorted(category_values)

                selected_category = st.selectbox(
                    "Категория",
                    category_options,
                    key="geo_sales_category"
                )

                filtered_region_sales = df_region_sales_enriched.copy()
                if selected_category != "Все категории":
                    filtered_region_sales = filtered_region_sales[
                        filtered_region_sales["Категория"].astype(str) == selected_category
                    ].copy()

                df_geo = build_region_sales_geo_report(filtered_region_sales)

                if not df_geo.empty:
                    detail_level = st.radio(
                        "Уровень детализации",
                        ["Федеральный округ", "Регион", "Город"],
                        horizontal=True,
                        key="geo_sales_detail_level"
                    )

                    sort_metric = st.radio(
                        "Сортировать по",
                        ["Продажи, шт", "Выручка, ₽"],
                        horizontal=True,
                        key="geo_sales_sort_metric"
                    )

                    filtered_geo = df_geo.copy()

                    if detail_level in ["Регион", "Город"]:
                        fo_options = ["Все"] + sorted(filtered_geo["Федеральный округ"].dropna().astype(str).unique().tolist())
                        selected_fo = st.selectbox("Федеральный округ", fo_options, key="geo_sales_fo")
                        if selected_fo != "Все":
                            filtered_geo = filtered_geo[filtered_geo["Федеральный округ"].astype(str) == selected_fo].copy()

                    if detail_level == "Город":
                        region_options = ["Все"] + sorted(filtered_geo["Регион"].dropna().astype(str).unique().tolist())
                        selected_region = st.selectbox("Регион", region_options, key="geo_sales_region")
                        if selected_region != "Все":
                            filtered_geo = filtered_geo[filtered_geo["Регион"].astype(str) == selected_region].copy()

                    if detail_level == "Федеральный округ":
                        group_cols = ["Страна", "Федеральный округ"]
                    elif detail_level == "Регион":
                        group_cols = ["Страна", "Федеральный округ", "Регион"]
                    else:
                        group_cols = ["Страна", "Федеральный округ", "Регион", "Город"]

                    df_geo_view = (
                        filtered_geo.groupby(group_cols, dropna=False, as_index=False)
                        .agg({
                            "Продажи, шт": "sum",
                            "Выручка, ₽": "sum",
                        })
                    )

                    total_geo_sales = pd.to_numeric(df_geo_view["Выручка, ₽"], errors="coerce").fillna(0).sum()
                    if total_geo_sales != 0:
                        df_geo_view["Доля выручки, %"] = (df_geo_view["Выручка, ₽"] / total_geo_sales * 100).round(2)
                    else:
                        df_geo_view["Доля выручки, %"] = 0.0

                    second_sort_metric = "Выручка, ₽" if sort_metric == "Продажи, шт" else "Продажи, шт"
                    df_geo_view = df_geo_view.sort_values(
                        [sort_metric, second_sort_metric],
                        ascending=[False, False],
                        kind="stable"
                    ).reset_index(drop=True)

                    st.dataframe(df_geo_view, use_container_width=True, height=560)

                    st.markdown("### 🏆 ТОП-10 регионов")
                    df_top_regions = (
                        filtered_geo.groupby(["Регион"], dropna=False, as_index=False)
                        .agg({
                            "Продажи, шт": "sum",
                            "Выручка, ₽": "sum",
                        })
                    )
                    total_top_regions_sales = pd.to_numeric(df_top_regions["Выручка, ₽"], errors="coerce").fillna(0).sum()
                    if total_top_regions_sales != 0:
                        df_top_regions["Доля выручки, %"] = (df_top_regions["Выручка, ₽"] / total_top_regions_sales * 100).round(2)
                    else:
                        df_top_regions["Доля выручки, %"] = 0.0
                    df_top_regions = (
                        df_top_regions.sort_values([sort_metric, second_sort_metric], ascending=[False, False], kind="stable")
                        .head(10)
                        .reset_index(drop=True)
                    )
                    st.dataframe(df_top_regions, use_container_width=True, height=390)

                    st.markdown("### 🏙 ТОП-10 городов")
                    df_top_cities = (
                        filtered_geo.groupby(["Город"], dropna=False, as_index=False)
                        .agg({
                            "Продажи, шт": "sum",
                            "Выручка, ₽": "sum",
                        })
                    )
                    total_top_cities_sales = pd.to_numeric(df_top_cities["Выручка, ₽"], errors="coerce").fillna(0).sum()
                    if total_top_cities_sales != 0:
                        df_top_cities["Доля выручки, %"] = (df_top_cities["Выручка, ₽"] / total_top_cities_sales * 100).round(2)
                    else:
                        df_top_cities["Доля выручки, %"] = 0.0
                    df_top_cities = (
                        df_top_cities.sort_values([sort_metric, second_sort_metric], ascending=[False, False], kind="stable")
                        .head(10)
                        .reset_index(drop=True)
                    )
                    st.dataframe(df_top_cities, use_container_width=True, height=390)

                    with st.expander("Показать исходные строки отчета «Продажи по регионам»"):
                        st.dataframe(filtered_region_sales, use_container_width=True, height=420)

                    if st.session_state.excel_region_sales:
                        st.download_button(
                            label="📥 Скачать исходный отчет по географии продаж",
                            data=st.session_state.excel_region_sales,
                            file_name=f"WB_Region_Sales_{target_company}.xlsx",
                            key="dl_region_sales"
                        )
                else:
                    st.warning("По выбранной категории нет данных в отчете «Продажи по регионам».")
            else:
                st.warning("Нет данных для отчета «Продажи по регионам». Проверь токен regions и выбранный период.")

        with tab1:
            st.write("### 📌 Сводная по продажам")

            if isinstance(st.session_state.df_analysis, pd.DataFrame) and not st.session_state.df_analysis.empty:
                df_summary_all = build_sales_summary_report(st.session_state.df_analysis)

                if not df_summary_all.empty:
                    if "Категория" in df_summary_all.columns:
                        df_summary_total = df_summary_all[df_summary_all["Категория"].astype(str).str.strip() == "Итого"].copy()
                        df_summary_main = df_summary_all[df_summary_all["Категория"].astype(str).str.strip() != "Итого"].copy()
                    else:
                        df_summary_total = pd.DataFrame()
                        df_summary_main = df_summary_all.copy()

                    summary_category_options = ["Все категории"]
                    if "Категория" in df_summary_main.columns:
                        summary_categories = (
                            df_summary_main["Категория"]
                            .dropna()
                            .astype(str)
                            .str.strip()
                        )
                        summary_categories = sorted([c for c in summary_categories.unique().tolist() if c])
                        summary_category_options += summary_categories

                    selected_summary_category = st.selectbox(
                        "Категория",
                        summary_category_options,
                        key="sales_summary_category"
                    )

                    df_summary_view = df_summary_main.copy()
                    if selected_summary_category != "Все категории":
                        df_summary_view = df_summary_view[
                            df_summary_view["Категория"].astype(str).str.strip() == selected_summary_category
                        ].copy()

                    metric_row = None
                    if not df_summary_view.empty:
                        sales_total = pd.to_numeric(df_summary_view.get("Продаж штук", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
                        profit_total = pd.to_numeric(df_summary_view.get("Прибыль", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
                        cost_total = pd.to_numeric(df_summary_view.get("Себестоимость", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
                        ads_total = pd.to_numeric(df_summary_view.get("Реклама", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
                        storage_total = pd.to_numeric(df_summary_view.get("Хранение", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
                        fbo_total = pd.to_numeric(df_summary_view.get("Остаток FBO, рублей", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
                        profitability_total = round((profit_total / cost_total) * 100, 1) if cost_total != 0 else 0.0
                        metric_row = {
                            "Продаж штук": sales_total,
                            "Прибыль": profit_total,
                            "Себестоимость": cost_total,
                            "Реклама": ads_total,
                            "Хранение": storage_total,
                            "Рентабельность": profitability_total,
                            "Остаток FBO, рублей": fbo_total,
                        }

                    if metric_row is not None:
                        col_kpi_1, col_kpi_2, col_kpi_3, col_kpi_4, col_kpi_5, col_kpi_6, col_kpi_7 = st.columns(7)
                        with col_kpi_1:
                            st.metric("Продаж штук", format_metric_int(metric_row.get("Продаж штук", 0)))
                        with col_kpi_2:
                            st.metric("Прибыль", format_metric_money(metric_row.get("Прибыль", 0), decimals=2))
                        with col_kpi_3:
                            st.metric("Себестоимость", format_metric_money(metric_row.get("Себестоимость", 0), decimals=2))
                        with col_kpi_4:
                            st.metric("Реклама", format_metric_money(metric_row.get("Реклама", 0), decimals=2))
                        with col_kpi_5:
                            st.metric("Хранение", format_metric_money(metric_row.get("Хранение", 0), decimals=2))
                        with col_kpi_6:
                            st.metric("Рентабельность", format_metric_percent(metric_row.get("Рентабельность", 0), decimals=1))
                        with col_kpi_7:
                            st.metric("Остаток FBO, ₽", format_metric_money(metric_row.get("Остаток FBO, рублей", 0), decimals=2))

                    format_map_main = {}
                    if "Себестоимость" in df_summary_view.columns:
                        format_map_main["Себестоимость"] = "{:.2f}"
                    if "Прибыль" in df_summary_view.columns:
                        format_map_main["Прибыль"] = "{:.2f}"
                    if "Рентабельность" in df_summary_view.columns:
                        format_map_main["Рентабельность"] = "{:.1f}"
                    if "ДРР" in df_summary_view.columns:
                        format_map_main["ДРР"] = "{:.1f}"
                    if "Хранение" in df_summary_view.columns:
                        format_map_main["Хранение"] = "{:.2f}"

                    if not df_summary_view.empty:
                        styled_summary_main = df_summary_view.style.format(format_map_main)
                        st.dataframe(styled_summary_main, use_container_width=True, height=560)
                    else:
                        st.info("По выбранной категории нет данных в сводной по продажам.")

                    if selected_summary_category == "Все категории" and not df_summary_total.empty:
                        df_summary_total = df_summary_total.tail(1).copy()
                        if "Прибыль" in df_summary_total.columns and "Себестоимость" in df_summary_total.columns:
                            total_profit = pd.to_numeric(df_summary_total["Прибыль"], errors="coerce").fillna(0.0)
                            total_cost = pd.to_numeric(df_summary_total["Себестоимость"], errors="coerce").fillna(0.0)
                            df_summary_total["Рентабельность"] = 0.0
                            total_non_zero_mask = total_cost != 0
                            df_summary_total.loc[total_non_zero_mask, "Рентабельность"] = ((total_profit[total_non_zero_mask] / total_cost[total_non_zero_mask]) * 100).round(1)

                        def _style_total_row_summary(_row):
                            return [
                                "color:#b30000; font-weight:700; background-color:#ffe6e6; "
                                "border-top:3px solid #b30000"
                            ] * len(_row)

                        format_map_total = {}
                        if "Себестоимость" in df_summary_total.columns:
                            format_map_total["Себестоимость"] = "{:.2f}"
                        if "Прибыль" in df_summary_total.columns:
                            format_map_total["Прибыль"] = "{:.2f}"
                        if "Рентабельность" in df_summary_total.columns:
                            format_map_total["Рентабельность"] = "{:.1f}"
                        if "ДРР" in df_summary_total.columns:
                            format_map_total["ДРР"] = "{:.1f}"
                        if "Хранение" in df_summary_total.columns:
                            format_map_total["Хранение"] = "{:.2f}"

                        styled_summary_total = (
                            df_summary_total.style
                            .apply(_style_total_row_summary, axis=1)
                            .format(format_map_total)
                        )

                        st.markdown("**Итого (закреплено снизу):**")
                        st.dataframe(styled_summary_total, use_container_width=True, height=90)

                    excel_summary = to_excel(df_summary_view, sheet_name="Sales_Summary") if not df_summary_view.empty else None
                    if excel_summary:
                        suffix = "all" if selected_summary_category == "Все категории" else sanitize_filename(selected_summary_category)
                        st.download_button(
                            label="📥 Скачать сводную по продажам",
                            data=excel_summary,
                            file_name=f"WB_Sales_Summary_{target_company}_{suffix}.xlsx",
                            key="dl_sales_summary"
                        )
                else:
                    st.warning("Не удалось сформировать сводную по продажам из аналитики.")
            else:
                st.warning("Нет данных для сводной по продажам. Сначала загрузите аналитический отчет.")
    else:
        st.info("Выбери компанию и период, затем нажми «Получить финансовый отчет».")
