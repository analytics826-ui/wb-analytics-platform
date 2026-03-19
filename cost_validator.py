import io
from datetime import datetime

import pandas as pd


SALES_WARNING_COVERAGE_PCT = 95.0
SALES_CRITICAL_COVERAGE_PCT = 80.0
STOCKS_WARNING_COVERAGE_PCT = 95.0
STOCKS_CRITICAL_COVERAGE_PCT = 80.0


def _normalize_id_series(s: pd.Series) -> pd.Series:
    s = s.copy()
    s = s.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.replace({"nan": "", "None": "", "<NA>": ""})
    return s


def _find_first_matching_column(df: pd.DataFrame, exact_names: list[str], contains_names: list[str]) -> str | None:
    if df is None or df.empty:
        return None

    cols = list(df.columns)
    exact_map = {str(c).strip().lower(): c for c in cols}

    for name in exact_names:
        key = str(name).strip().lower()
        if key in exact_map:
            return exact_map[key]

    for c in cols:
        cname = str(c).strip().lower()
        for part in contains_names:
            if part and part.lower() in cname:
                return c

    return None


def _prepare_price_table(df_price: pd.DataFrame) -> tuple[pd.DataFrame, str, str]:
    if df_price is None or not isinstance(df_price, pd.DataFrame) or df_price.empty:
        raise ValueError("Прайс пустой или не загружен")

    df_p = df_price.copy()
    df_p.columns = [str(c).strip() for c in df_p.columns]

    barcode_col = _find_first_matching_column(df_p, ["Баркод", "barcode"], ["баркод", "barcode", "штрихкод"])
    if not barcode_col:
        raise ValueError("В прайсе не найдена колонка с баркодом")

    cost_col = _find_first_matching_column(df_p, ["Себестоимость"], ["себестоим"])
    if not cost_col:
        raise ValueError("В прайсе не найдена колонка 'Себестоимость'")

    df_p["_barcode_norm"] = _normalize_id_series(df_p[barcode_col])
    df_p = df_p[df_p["_barcode_norm"] != ""].copy()
    df_p = df_p.drop_duplicates(subset=["_barcode_norm"], keep="last")

    raw_cost = df_p[cost_col]
    cost_num = pd.to_numeric(raw_cost, errors="coerce")
    missing_mask = (
        raw_cost.isna()
        | (raw_cost.astype(str).str.strip() == "")
        | (raw_cost.astype(str).str.strip().str.lower().isin(["nan", "none"]))
        | cost_num.isna()
        | (cost_num <= 0)
    )

    df_p["_has_cost"] = ~missing_mask
    return df_p, barcode_col, cost_col


def _extract_sales_barcodes(df_fin: pd.DataFrame) -> pd.Series:
    if df_fin is None or not isinstance(df_fin, pd.DataFrame) or df_fin.empty:
        raise ValueError("Финансовый отчет пустой")

    barcode_col = _find_first_matching_column(
        df_fin,
        ["barcode", "Barcode", "ШК", "Штрихкод"],
        ["barcode", "штрихкод", "шк"],
    )
    if not barcode_col:
        raise ValueError("В финансовом отчете не найдена колонка с баркодом")

    operation_col = _find_first_matching_column(
        df_fin,
        ["supplier_oper_name", "supplierOperName"],
        ["supplier_oper", "oper_name", "операц"],
    )
    if not operation_col:
        raise ValueError("В финансовом отчете не найдена колонка с типом операции")

    tmp = df_fin.copy()
    tmp["_barcode_norm"] = _normalize_id_series(tmp[barcode_col])
    tmp["_operation"] = tmp[operation_col].astype(str).str.strip().str.lower()

    valid_ops = {"продажа", "возврат"}
    out = tmp.loc[tmp["_operation"].isin(valid_ops), "_barcode_norm"]
    out = out[out != ""].drop_duplicates().reset_index(drop=True)
    return out


def _extract_stocks_barcodes(df_stocks: pd.DataFrame) -> pd.Series:
    if df_stocks is None or not isinstance(df_stocks, pd.DataFrame) or df_stocks.empty:
        return pd.Series(dtype="object")

    barcode_col = _find_first_matching_column(
        df_stocks,
        ["barcode", "Barcode", "Баркод", "Штрихкод"],
        ["barcode", "баркод", "штрихкод", "шк"],
    )
    if not barcode_col:
        raise ValueError("В остатках не найдена колонка с баркодом")

    qty_col = _find_first_matching_column(
        df_stocks,
        ["quantity", "Quantity", "qty", "Остаток"],
        ["quantity", "qty", "остаток"],
    )
    if not qty_col:
        raise ValueError("В остатках не найдена колонка с количеством")

    tmp = df_stocks.copy()
    tmp[qty_col] = pd.to_numeric(tmp[qty_col], errors="coerce").fillna(0)
    tmp = tmp[tmp[qty_col] > 0].copy()
    if tmp.empty:
        return pd.Series(dtype="object")

    tmp["_barcode_norm"] = _normalize_id_series(tmp[barcode_col])
    out = tmp.loc[tmp["_barcode_norm"] != "", "_barcode_norm"].drop_duplicates().reset_index(drop=True)
    return out


def _series_to_missing_df(series: pd.Series) -> pd.DataFrame:
    if series is None or len(series) == 0:
        return pd.DataFrame(columns=["Баркод"])
    out = pd.DataFrame({"Баркод": sorted(set(_normalize_id_series(series).tolist()))})
    out = out[out["Баркод"] != ""].reset_index(drop=True)
    return out


def _make_union_missing_df(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    vals = []
    if isinstance(df1, pd.DataFrame) and "Баркод" in df1.columns:
        vals.extend(df1["Баркод"].astype(str).tolist())
    if isinstance(df2, pd.DataFrame) and "Баркод" in df2.columns:
        vals.extend(df2["Баркод"].astype(str).tolist())
    vals = sorted(set([x.strip() for x in vals if str(x).strip() and str(x).strip().lower() not in {"nan", "none"}]))
    return pd.DataFrame({"Баркод": vals}) if vals else pd.DataFrame(columns=["Баркод"])


def _coverage_pct(total_count: int, missing_count: int) -> float:
    if total_count <= 0:
        return 100.0
    return round(((total_count - missing_count) / total_count) * 100, 1)


def _resolve_status(sales_coverage_pct: float, stocks_coverage_pct: float, missing_total_count: int) -> str:
    if sales_coverage_pct < SALES_CRITICAL_COVERAGE_PCT or stocks_coverage_pct < STOCKS_CRITICAL_COVERAGE_PCT:
        return "critical"
    if missing_total_count > 0:
        return "warning"
    return "ok"


def validate_cost_data(
    company_name: str,
    date_from,
    date_to,
    df_fin: pd.DataFrame,
    df_stocks: pd.DataFrame,
    df_price: pd.DataFrame,
    df_missing_cost_barcodes: pd.DataFrame | None = None,
    df_missing_cost_stocks: pd.DataFrame | None = None,
) -> dict:
    result = {
        "company_name": company_name,
        "date_from": str(date_from),
        "date_to": str(date_to),
        "status": "error",
        "error": "",
        "sales_barcodes_total": 0,
        "sales_missing_count": 0,
        "sales_coverage_pct": 0.0,
        "stocks_barcodes_total": 0,
        "stocks_missing_count": 0,
        "stocks_coverage_pct": 0.0,
        "missing_total_count": 0,
        "missing_sales_df": pd.DataFrame(columns=["Баркод"]),
        "missing_stocks_df": pd.DataFrame(columns=["Баркод"]),
        "missing_all_df": pd.DataFrame(columns=["Баркод"]),
        "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    try:
        price_df, _, _ = _prepare_price_table(df_price)
        sales_barcodes = _extract_sales_barcodes(df_fin)
        stocks_barcodes = _extract_stocks_barcodes(df_stocks)

        if isinstance(df_missing_cost_barcodes, pd.DataFrame) and not df_missing_cost_barcodes.empty and "Баркод" in df_missing_cost_barcodes.columns:
            missing_sales_df = _series_to_missing_df(df_missing_cost_barcodes["Баркод"])
        else:
            known_cost_barcodes = set(price_df.loc[price_df["_has_cost"], "_barcode_norm"].tolist())
            sales_missing_series = sales_barcodes[~sales_barcodes.isin(known_cost_barcodes)]
            missing_sales_df = _series_to_missing_df(sales_missing_series)

        if isinstance(df_missing_cost_stocks, pd.DataFrame) and not df_missing_cost_stocks.empty and "Баркод" in df_missing_cost_stocks.columns:
            missing_stocks_df = _series_to_missing_df(df_missing_cost_stocks["Баркод"])
        else:
            known_cost_barcodes = set(price_df.loc[price_df["_has_cost"], "_barcode_norm"].tolist())
            stocks_missing_series = stocks_barcodes[~stocks_barcodes.isin(known_cost_barcodes)]
            missing_stocks_df = _series_to_missing_df(stocks_missing_series)

        missing_all_df = _make_union_missing_df(missing_sales_df, missing_stocks_df)

        sales_total = int(len(sales_barcodes))
        sales_missing = int(len(missing_sales_df))
        stocks_total = int(len(stocks_barcodes))
        stocks_missing = int(len(missing_stocks_df))

        sales_coverage_pct = _coverage_pct(sales_total, sales_missing)
        stocks_coverage_pct = _coverage_pct(stocks_total, stocks_missing)
        missing_total_count = int(len(missing_all_df))

        result.update({
            "status": _resolve_status(sales_coverage_pct, stocks_coverage_pct, missing_total_count),
            "sales_barcodes_total": sales_total,
            "sales_missing_count": sales_missing,
            "sales_coverage_pct": sales_coverage_pct,
            "stocks_barcodes_total": stocks_total,
            "stocks_missing_count": stocks_missing,
            "stocks_coverage_pct": stocks_coverage_pct,
            "missing_total_count": missing_total_count,
            "missing_sales_df": missing_sales_df,
            "missing_stocks_df": missing_stocks_df,
            "missing_all_df": missing_all_df,
        })
        return result
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        return result


def make_barcodes_excel_bytes(df_barcodes: pd.DataFrame) -> bytes:
    if df_barcodes is None or not isinstance(df_barcodes, pd.DataFrame) or df_barcodes.empty:
        df_export = pd.DataFrame(columns=["Баркод"])
    else:
        df_export = df_barcodes.copy()
        if "Баркод" not in df_export.columns:
            first_col = df_export.columns[0]
            df_export = df_export[[first_col]].rename(columns={first_col: "Баркод"})
        else:
            df_export = df_export[["Баркод"]].copy()

        df_export["Баркод"] = _normalize_id_series(df_export["Баркод"])
        df_export = df_export[df_export["Баркод"] != ""].drop_duplicates().copy()
        df_export["Баркод"] = pd.to_numeric(df_export["Баркод"], errors="coerce")
        df_export = df_export.dropna(subset=["Баркод"]).copy()

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_export.to_excel(writer, index=False, sheet_name="Баркоды")
        workbook = writer.book
        worksheet = writer.sheets["Баркоды"]
        number_format = workbook.add_format({"num_format": "0"})
        worksheet.set_column("A:A", 20, number_format)
    return output.getvalue()
