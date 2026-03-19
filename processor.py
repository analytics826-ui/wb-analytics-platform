import pandas as pd


def _normalize_id_series(s: pd.Series) -> pd.Series:
    """
    Приводим идентификаторы к строке без пробелов и без .0
    """
    s = s.copy()
    s = s.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.replace({"nan": "", "None": ""})
    return s


def _add_total_row(df: pd.DataFrame, label: str = "Итого") -> pd.DataFrame:
    """
    Добавляет последнюю строку "Итого":
    - суммирует все числовые столбцы
    - текстовые столбцы заполняет пусто, а "Категория" = label
    """
    if df is None or df.empty:
        return df

    df_out = df.copy()
    numeric_cols = df_out.select_dtypes(include="number").columns.tolist()
    total = {col: df_out[col].sum() for col in numeric_cols}

    for col in df_out.columns:
        if col not in total:
            total[col] = ""

    if "Категория" in df_out.columns:
        total["Категория"] = label

    df_out = pd.concat([df_out, pd.DataFrame([total])], ignore_index=True)
    return df_out


def _drop_not_found_if_all_zero(out: pd.DataFrame, label: str = "Не найдено") -> pd.DataFrame:
    """
    Удаляет строку(и) с Категория == "Не найдено" только если по ней все числовые поля == 0.
    """
    if out is None or out.empty or "Категория" not in out.columns:
        return out

    df = out.copy()
    cat_series = df["Категория"].astype(str).str.strip()

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if not numeric_cols:
        return out

    mask_nf = cat_series == str(label)
    if not mask_nf.any():
        return out

    numeric_sum_abs = df.loc[mask_nf, numeric_cols].fillna(0).abs().sum(axis=1)
    mask_drop = numeric_sum_abs == 0

    drop_idx = df.loc[mask_nf].index[mask_drop]
    if len(drop_idx) == 0:
        return out

    df = df.drop(index=drop_idx).reset_index(drop=True)
    return df


def _pick_fin_barcode_column(df_fin: pd.DataFrame) -> str | None:
    """
    Ищем колонку barcode в финансовом отчёте максимально аккуратно:
    - сначала точные варианты
    - потом по подстроке "barcode"
    """
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


def _pick_column_by_exact_or_contains(df: pd.DataFrame, exact: str, contains_any: list[str]) -> str | None:
    """
    Возвращает название колонки:
    - если есть точное совпадение по имени (с учетом регистра) -> оно
    - иначе ищет по подстрокам (case-insensitive) среди названий колонок
    """
    if df is None or df.empty:
        return None

    cols = list(df.columns)
    if exact in cols:
        return exact

    exact_norm = str(exact).strip().lower()
    for c in cols:
        if str(c).strip().lower() == exact_norm:
            return c

    for c in cols:
        cname = str(c).strip().lower()
        for part in contains_any:
            if part and part.lower() in cname:
                return c

    return None


def _build_price_map(df_price: pd.DataFrame) -> dict:
    """
    Строим словарь barcode_norm -> cost
    Поддерживаем возможный пробел в конце у 'Себестоимость '.
    """
    if df_price is None or not isinstance(df_price, pd.DataFrame) or df_price.empty:
        return {}

    col_map = {c: str(c).strip() for c in df_price.columns}
    df_p = df_price.rename(columns=col_map).copy()

    if "Баркод" not in df_p.columns:
        return {}

    cost_col = None
    for c in df_p.columns:
        if str(c).strip().lower() == "себестоимость":
            cost_col = c
            break
    if cost_col is None:
        return {}

    df_p["__barcode_norm"] = _normalize_id_series(df_p["Баркод"])
    df_p["__cost"] = pd.to_numeric(df_p[cost_col], errors="coerce").fillna(0.0)

    df_p = df_p[df_p["__barcode_norm"] != ""].copy()
    df_p = df_p.drop_duplicates(subset=["__barcode_norm"], keep="last")

    return dict(zip(df_p["__barcode_norm"], df_p["__cost"]))


def _pick_stock_nm_column(df_stocks: pd.DataFrame) -> str | None:
    if df_stocks is None or df_stocks.empty:
        return None

    for c in ["nmId", "nm_id", "NMID", "nmid"]:
        if c in df_stocks.columns:
            return c

    for c in df_stocks.columns:
        name = str(c).strip().lower()
        if name in ("nmid", "nm_id"):
            return c

    return None


def _pick_stock_barcode_column(df_stocks: pd.DataFrame) -> str | None:
    if df_stocks is None or df_stocks.empty:
        return None

    for c in ["barcode", "Barcode", "BARCODE", "Баркод", "ШК", "шк", "Штрихкод", "штрихкод"]:
        if c in df_stocks.columns:
            return c

    for c in df_stocks.columns:
        name = str(c).strip().lower()
        if "barcode" in name or "баркод" in name or "штрихкод" in name:
            return c

    return None


def _pick_stock_qty_column(df_stocks: pd.DataFrame) -> str | None:
    if df_stocks is None or df_stocks.empty:
        return None

    for c in ["quantity", "Quantity", "qty", "QTY", "Остаток", "остаток"]:
        if c in df_stocks.columns:
            return c

    for c in df_stocks.columns:
        name = str(c).strip().lower()
        if name == "quantity" or "quantity" in name or "остаток" in name:
            return c

    return None


def _pick_stock_warehouse_column(df_stocks: pd.DataFrame) -> str | None:
    if df_stocks is None or df_stocks.empty:
        return None

    for c in ["warehouseName", "WarehouseName", "warehouse_name", "Склад", "склад"]:
        if c in df_stocks.columns:
            return c

    for c in df_stocks.columns:
        name = str(c).strip().lower()
        if "warehouse" in name or "склад" in name:
            return c

    return None


def _build_stock_metrics_by_cat(df_stocks: pd.DataFrame, nm_to_cat: dict, df_price: pd.DataFrame):
    """
    Считает 6 метрик из отчета остатков:
    1) Остаток FBO, штук
    2) Остаток FBO, рублей
    3) В пути до получателей, штук
    4) В пути до получателей, рублей
    5) В пути возвраты на склад WB, штук
    6) В пути возвраты на склад WB, рублей

    Логика:
    - FBO = все строки, кроме:
      "В пути до получателей"
      "В пути возвраты на склад WB"
      "Всего находится на складах"
    - "Остальные" включаем в FBO
    """
    metrics = {
        "fbo_qty_by_cat": {},
        "fbo_value_by_cat": {},
        "in_transit_to_customer_qty_by_cat": {},
        "in_transit_to_customer_value_by_cat": {},
        "return_to_wb_qty_by_cat": {},
        "return_to_wb_value_by_cat": {},
    }

    if df_stocks is None or not isinstance(df_stocks, pd.DataFrame) or df_stocks.empty:
        return metrics

    nm_col = _pick_stock_nm_column(df_stocks)
    barcode_col = _pick_stock_barcode_column(df_stocks)
    qty_col = _pick_stock_qty_column(df_stocks)
    warehouse_col = _pick_stock_warehouse_column(df_stocks)

    if not nm_col or not qty_col or not warehouse_col:
        return metrics

    tmp = df_stocks.copy()

    tmp[nm_col] = _normalize_id_series(tmp[nm_col])
    tmp[qty_col] = pd.to_numeric(tmp[qty_col], errors="coerce").fillna(0.0)
    tmp[warehouse_col] = tmp[warehouse_col].astype(str).str.strip()

    def map_cat_from_stock(nm):
        if not nm:
            return "Не найдено"
        return nm_to_cat.get(nm, "Не найдено")

    tmp["Категория"] = tmp[nm_col].apply(map_cat_from_stock)

    price_map = _build_price_map(df_price)
    if price_map and barcode_col:
        tmp["_barcode_norm"] = _normalize_id_series(tmp[barcode_col])
        tmp["_unit_cost"] = tmp["_barcode_norm"].map(price_map).fillna(0.0)
    else:
        tmp["_unit_cost"] = 0.0

    tmp["_value"] = tmp[qty_col] * tmp["_unit_cost"]

    STATUS_IN_TRANSIT_TO_CUSTOMER = "В пути до получателей"
    STATUS_RETURN_TO_WB = "В пути возвраты на склад WB"
    STATUS_TOTAL_ON_WAREHOUSES = "Всего находится на складах"

    mask_in_transit_to_customer = tmp[warehouse_col] == STATUS_IN_TRANSIT_TO_CUSTOMER
    mask_return_to_wb = tmp[warehouse_col] == STATUS_RETURN_TO_WB
    mask_total_on_warehouses = tmp[warehouse_col] == STATUS_TOTAL_ON_WAREHOUSES

    mask_fbo = ~(mask_in_transit_to_customer | mask_return_to_wb | mask_total_on_warehouses)

    tmp_fbo = tmp[mask_fbo].copy()
    tmp_transit_customer = tmp[mask_in_transit_to_customer].copy()
    tmp_return_to_wb = tmp[mask_return_to_wb].copy()

    if not tmp_fbo.empty:
        metrics["fbo_qty_by_cat"] = tmp_fbo.groupby("Категория")[qty_col].sum().to_dict()
        metrics["fbo_value_by_cat"] = tmp_fbo.groupby("Категория")["_value"].sum().to_dict()

    if not tmp_transit_customer.empty:
        metrics["in_transit_to_customer_qty_by_cat"] = (
            tmp_transit_customer.groupby("Категория")[qty_col].sum().to_dict()
        )
        metrics["in_transit_to_customer_value_by_cat"] = (
            tmp_transit_customer.groupby("Категория")["_value"].sum().to_dict()
        )

    if not tmp_return_to_wb.empty:
        metrics["return_to_wb_qty_by_cat"] = (
            tmp_return_to_wb.groupby("Категория")[qty_col].sum().to_dict()
        )
        metrics["return_to_wb_value_by_cat"] = (
            tmp_return_to_wb.groupby("Категория")["_value"].sum().to_dict()
        )

    return metrics


def create_analysis_report(
    df_raw: pd.DataFrame,
    company_name: str,
    period_str: str,
    df_ads: pd.DataFrame = None,
    df_storage: pd.DataFrame = None,
    df_stocks: pd.DataFrame = None,
    df_nom: pd.DataFrame = None,
    df_price: pd.DataFrame = None,
    nom_nm_col: str = "nm_id",
    nom_cat_col: str = "subject",
    add_total_row: bool = False,
    total_label: str = "Итого",
):
    """
    Аналитика по категориям.

    Категории строятся по товарам из:
    - финансового отчета: df_raw["nm_id"]
    - рекламы: df_ads["article"]
    - хранения: df_storage["nmId"]
    - остатков: df_stocks["nmId"]
    Категория определяется через номенклатуру: nm_id -> df_nom[nom_cat_col].

    Реклама:
    - сумма df_ads["updSum"] по категории (через article -> категория)

    Хранение:
    - сумма df_storage["warehousePrice"] по категории (через nmId -> категория)

    Остатки:
    - Остаток FBO, штук / рублей
    - В пути до получателей, штук / рублей
    - В пути возвраты на склад WB, штук / рублей

    Себестоимость продаж:
    - берём barcode из финансового отчёта
    - матчим с price_list.parquet: 'Баркод' -> 'Себестоимость'
    - считаем по категории: Σ(cost по "Продажа") − Σ(cost по "Возврат")

    Штрафы:
    - если тип операции (iloc[:,24]) содержит "штраф"
    - берём сумму из колонки "penalty" (или похожей)

    Платная приемка:
    - если тип операции (iloc[:,24]) содержит "прием"
    - берём сумму из колонки "deduction" (или похожей)
    """

    columns_final = [
        "Кабинет",
        "Период",
        "Категория",
        "Цена розничная с учетом\nсогласованной скидки",
        "Продаж штук",
        "Возвратов штук",
        "Рентабельность",
        "Прибыль",
        "Себестоимость",
        "Итого к оплате",
        "К перечислению",
        "Всего затраты",
        "Логистика",
        "Штрафы",
        "Платная приемка",
        "Реклама",
        "Отзывы за баллы",
        "ДРР",
        "Хранение",
        "Услуги FBO",
        "Компенсация скидки\nпо программе лояльности",
        "Стоимость участия\nв программе лояльности",
        "Сумма удержанная за начисленные\nбаллы программы лояльности",
        "Остаток FBO, штук",
        "Остаток FBO, рублей",
        "В пути до получателей, штук",
        "В пути до получателей, рублей",
        "В пути возвраты на склад WB, штук",
        "В пути возвраты на склад WB, рублей",
    ]

    if df_raw is None or df_raw.empty:
        out = pd.DataFrame(columns=columns_final)
        if add_total_row:
            out = _add_total_row(out, label=total_label)
        return out

    # -------------------------
    # 1) Справочник nm_id -> category (из номенклатуры)
    # -------------------------
    nom_ok = (
        df_nom is not None
        and isinstance(df_nom, pd.DataFrame)
        and not df_nom.empty
        and (nom_nm_col in df_nom.columns)
        and (nom_cat_col in df_nom.columns)
    )

    nm_to_cat = {}
    if nom_ok:
        tmp_nom = df_nom[[nom_nm_col, nom_cat_col]].copy()
        tmp_nom[nom_nm_col] = _normalize_id_series(tmp_nom[nom_nm_col])
        tmp_nom[nom_cat_col] = tmp_nom[nom_cat_col].astype(str).str.strip()
        tmp_nom = tmp_nom[tmp_nom[nom_nm_col] != ""]
        tmp_nom = tmp_nom.drop_duplicates(subset=[nom_nm_col])
        nm_to_cat = dict(zip(tmp_nom[nom_nm_col], tmp_nom[nom_cat_col]))

    # -------------------------
    # 2) Собираем nm_id из всех источников
    # -------------------------
    finance_nm_ids = []
    if "nm_id" in df_raw.columns:
        finance_nm_ids = _normalize_id_series(df_raw["nm_id"]).tolist()

    ads_nm_ids = []
    if df_ads is not None and isinstance(df_ads, pd.DataFrame) and not df_ads.empty and "article" in df_ads.columns:
        ads_nm_ids = _normalize_id_series(df_ads["article"]).tolist()

    storage_nm_ids = []
    if df_storage is not None and isinstance(df_storage, pd.DataFrame) and not df_storage.empty and "nmId" in df_storage.columns:
        storage_nm_ids = _normalize_id_series(df_storage["nmId"]).tolist()

    stocks_nm_ids = []
    if df_stocks is not None and isinstance(df_stocks, pd.DataFrame) and not df_stocks.empty and "nmId" in df_stocks.columns:
        stocks_nm_ids = _normalize_id_series(df_stocks["nmId"]).tolist()

    all_nm_ids = set([x for x in (finance_nm_ids + ads_nm_ids + storage_nm_ids + stocks_nm_ids) if x])

    # -------------------------
    # 3) Если номенклатуры нет — fallback (старый способ)
    # -------------------------
    if not nom_ok:
        try:
            categories_slice = df_raw.iloc[:, 11]
            unique_cats = categories_slice.dropna().unique().tolist()
            unique_cats = [str(cat).strip() for cat in unique_cats if str(cat).strip() != ""]
        except Exception:
            unique_cats = []

        if not unique_cats:
            out = pd.DataFrame(columns=columns_final)
            if add_total_row:
                out = _add_total_row(out, label=total_label)
            return out

        rows = []
        for cat in unique_cats:
            df_cat = df_raw[df_raw.iloc[:, 11].astype(str).str.strip() == cat]

            sales_qty = df_cat[df_cat.iloc[:, 24] == "Продажа"].iloc[:, 18].sum()
            return_qty_sub = df_cat[df_cat.iloc[:, 24] == "Возврат"].iloc[:, 18].sum()
            total_sales_pcs = sales_qty - return_qty_sub

            total_returns_pcs = df_cat[df_cat.iloc[:, 24] == "Возврат"].iloc[:, 18].sum()

            logistics_sum = df_cat[df_cat.iloc[:, 24] == "Логистика"].iloc[:, 32].sum()
            logistics_corr = df_cat[df_cat.iloc[:, 24] == "Коррекция логистики"].iloc[:, 32].sum()
            total_logistics = logistics_sum + logistics_corr

            pay_sales = df_cat[df_cat.iloc[:, 24] == "Продажа"].iloc[:, 42].sum()
            pay_returns = df_cat[df_cat.iloc[:, 24] == "Возврат"].iloc[:, 42].sum()
            pay_comp_ret = df_cat[df_cat.iloc[:, 24] == "Добровольная компенсация при возврате"].iloc[:, 42].sum()
            pay_comp_dmg = df_cat[df_cat.iloc[:, 24] == "Компенсация ущерба"].iloc[:, 42].sum()
            total_to_pay = pay_sales - pay_returns + pay_comp_ret + pay_comp_dmg
            reviews_total = 0.0
            fbo_total = 0.0
            total_costs = (
                float(total_logistics)
                + 0.0
                + 0.0
                + 0.0
                + reviews_total
                + 0.0
                + fbo_total
                + 0.0
                + 0.0
                + 0.0
            )
            total_to_payment = float(total_to_pay) - float(total_costs)
            profit_total = float(total_to_payment)
            profitability_total = 0.0
            drr_total = 0.0

            rows.append({
                "Кабинет": company_name,
                "Период": period_str,
                "Категория": cat,
                "Цена розничная с учетом\nсогласованной скидки": 0,
                "Продаж штук": total_sales_pcs,
                "Возвратов штук": total_returns_pcs,
                "Логистика": total_logistics,
                "К перечислению": total_to_pay,
                "Реклама": 0,
                "Хранение": 0,
                "Услуги FBO": fbo_total,
                "Рентабельность": profitability_total,
                "Прибыль": profit_total,
                "Себестоимость": 0,
                "Итого к оплате": total_to_payment,
                "Всего затраты": total_costs,
                "Штрафы": 0,
                "Платная приемка": 0,
                "Отзывы за баллы": reviews_total,
                "ДРР": drr_total,
                "Компенсация скидки\nпо программе лояльности": 0,
                "Стоимость участия\nв программе лояльности": 0,
                "Сумма удержанная за начисленные\nбаллы программы лояльности": 0,
                "Остаток FBO, штук": 0,
                "Остаток FBO, рублей": 0,
                "В пути до получателей, штук": 0,
                "В пути до получателей, рублей": 0,
                "В пути возвраты на склад WB, штук": 0,
                "В пути возвраты на склад WB, рублей": 0,
            })

        out = pd.DataFrame(rows, columns=columns_final)
        if add_total_row:
            out = _add_total_row(out, label=total_label)
        return out

    # -------------------------
    # 4) Категории по номенклатуре
    # -------------------------
    categories = []
    for nm in all_nm_ids:
        cat = nm_to_cat.get(nm, "")
        categories.append(cat if cat else "Не найдено")

    unique_cats = sorted(set([c for c in categories if c]))
    if not unique_cats:
        out = pd.DataFrame(columns=columns_final)
        if add_total_row:
            out = _add_total_row(out, label=total_label)
        return out

    # -------------------------
    # 5) Реклама: category -> sum(updSum)
    # -------------------------
    ads_by_cat = {}
    if df_ads is not None and isinstance(df_ads, pd.DataFrame) and not df_ads.empty:
        if "article" in df_ads.columns and "updSum" in df_ads.columns:
            tmp_ads = df_ads.copy()
            tmp_ads["article"] = _normalize_id_series(tmp_ads["article"])
            tmp_ads["updSum"] = pd.to_numeric(tmp_ads["updSum"], errors="coerce").fillna(0.0)

            def map_cat_from_article(article):
                if not article:
                    return "Не найдено"
                return nm_to_cat.get(article, "Не найдено")

            tmp_ads["Категория"] = tmp_ads["article"].apply(map_cat_from_article)
            ads_by_cat = tmp_ads.groupby("Категория")["updSum"].sum().to_dict()

    # -------------------------
    # 6) Хранение: category -> sum(warehousePrice)
    # -------------------------
    storage_by_cat = {}
    if df_storage is not None and isinstance(df_storage, pd.DataFrame) and not df_storage.empty:
        if "nmId" in df_storage.columns and "warehousePrice" in df_storage.columns:
            tmp_st = df_storage.copy()
            tmp_st["nmId"] = _normalize_id_series(tmp_st["nmId"])
            tmp_st["warehousePrice"] = pd.to_numeric(tmp_st["warehousePrice"], errors="coerce").fillna(0.0)

            def map_cat_from_storage(nm):
                if not nm:
                    return "Не найдено"
                return nm_to_cat.get(nm, "Не найдено")

            tmp_st["Категория"] = tmp_st["nmId"].apply(map_cat_from_storage)
            storage_by_cat = tmp_st.groupby("Категория")["warehousePrice"].sum().to_dict()

    # -------------------------
    # 7) Остатки: 6 метрик по category
    # -------------------------
    stock_metrics = _build_stock_metrics_by_cat(df_stocks, nm_to_cat, df_price)

    fbo_qty_by_cat = stock_metrics["fbo_qty_by_cat"]
    fbo_value_by_cat = stock_metrics["fbo_value_by_cat"]
    in_transit_to_customer_qty_by_cat = stock_metrics["in_transit_to_customer_qty_by_cat"]
    in_transit_to_customer_value_by_cat = stock_metrics["in_transit_to_customer_value_by_cat"]
    return_to_wb_qty_by_cat = stock_metrics["return_to_wb_qty_by_cat"]
    return_to_wb_value_by_cat = stock_metrics["return_to_wb_value_by_cat"]

    # -------------------------
    # 8) Финансы: проставляем категорию строкам по nm_id + готовим себестоимость + штрафы/приемка
    # -------------------------
    df_fin = df_raw.copy()

    if "nm_id" in df_fin.columns:
        df_fin["_nm_norm"] = _normalize_id_series(df_fin["nm_id"])

        def map_fin_cat(nm):
            if not nm:
                return "Не найдено"
            return nm_to_cat.get(nm, "Не найдено")

        df_fin["_cat"] = df_fin["_nm_norm"].apply(map_fin_cat)
    else:
        df_fin["_cat"] = "Не найдено"

    op_col_name = df_fin.columns[24]
    df_fin["_op"] = df_fin[op_col_name].astype(str).str.strip()

    # --- Себестоимость: строим price_map и считаем сумму по категориям ---
    cogs_by_cat = {}

    try:
        price_map = _build_price_map(df_price)

        barcode_col = _pick_fin_barcode_column(df_fin)
        if price_map and barcode_col:
            df_fin["_barcode_norm"] = _normalize_id_series(df_fin[barcode_col])
            df_fin["_unit_cost"] = df_fin["_barcode_norm"].map(price_map).fillna(0.0)

            sales_cost = (
                df_fin[df_fin["_op"] == "Продажа"]
                .groupby("_cat")["_unit_cost"].sum()
                .to_dict()
            )
            return_cost = (
                df_fin[df_fin["_op"] == "Возврат"]
                .groupby("_cat")["_unit_cost"].sum()
                .to_dict()
            )

            cats = set(list(sales_cost.keys()) + list(return_cost.keys()))
            for c in cats:
                cogs_by_cat[c] = float(sales_cost.get(c, 0.0)) - float(return_cost.get(c, 0.0))
    except Exception:
        cogs_by_cat = {}

    # --- Штрафы: сумма penalty по категории, где операция содержит "штраф" ---
    penalty_by_cat = {}
    penalty_col = _pick_column_by_exact_or_contains(
        df_fin,
        exact="penalty",
        contains_any=["penalty", "штраф"]
    )
    if penalty_col:
        df_fin["_penalty_val"] = pd.to_numeric(df_fin[penalty_col], errors="coerce").fillna(0.0)
        penalty_by_cat = (
            df_fin[df_fin["_op"].str.contains("штраф", case=False, na=False)]
            .groupby("_cat")["_penalty_val"].sum()
            .to_dict()
        )

    # --- Платная приемка: сумма deduction по категории, где операция содержит "прием" ---
    acceptance_by_cat = {}
    deduction_col = _pick_column_by_exact_or_contains(
        df_fin,
        exact="deduction",
        contains_any=["deduction", "удерж", "вычет"]
    )
    if deduction_col:
        df_fin["_deduction_val"] = pd.to_numeric(df_fin[deduction_col], errors="coerce").fillna(0.0)
        acceptance_by_cat = (
            df_fin[df_fin["_op"].str.contains("прием", case=False, na=False)]
            .groupby("_cat")["_deduction_val"].sum()
            .to_dict()
        )

    # --- Отзывы за баллы: сумма deduction по категории, где операция = "Удержание"
    #     и bonus_type_name содержит "Списание за отзыв" ---
    reviews_by_cat = {}
    bonus_type_col = _pick_column_by_exact_or_contains(
        df_fin,
        exact="bonus_type_name",
        contains_any=["bonus_type_name", "bonus", "тип бонуса"]
    )
    if deduction_col and bonus_type_col:
        df_fin["_bonus_type_name"] = df_fin[bonus_type_col].astype(str).str.strip()
        reviews_by_cat = (
            df_fin[
                (df_fin["_op"].str.strip().str.lower() == "удержание")
                & (df_fin["_bonus_type_name"].str.contains("Списание за отзыв", case=False, na=False))
            ]
            .groupby("_cat")["_deduction_val"]
            .sum()
            .to_dict()
        )

    # --- Компенсация скидки по программе лояльности ---
    loyalty_discount_by_cat = {}
    if "cashback_discount" in df_fin.columns:
        df_fin["_cashback_discount"] = pd.to_numeric(df_fin["cashback_discount"], errors="coerce").fillna(0.0)
        loyalty_discount_by_cat = (
            df_fin[df_fin["_op"].str.contains("компенсац", case=False, na=False)]
            .groupby("_cat")["_cashback_discount"]
            .sum()
            .to_dict()
        )

    # --- Стоимость участия в программе лояльности ---
    loyalty_commission_by_cat = {}
    if "cashback_commission_change" in df_fin.columns:
        df_fin["_cashback_commission"] = pd.to_numeric(
            df_fin["cashback_commission_change"], errors="coerce"
        ).fillna(0.0)
        loyalty_commission_by_cat = (
            df_fin[
                df_fin["_op"].str.contains(
                    "стоимость участия в программе лояльности", case=False, na=False
                )
            ]
            .groupby("_cat")["_cashback_commission"]
            .sum()
            .to_dict()
        )

    # --- Сумма удержанная за начисленные баллы программы лояльности ---
    loyalty_points_by_cat = {}
    if "cashback_amount" in df_fin.columns:
        df_fin["_cashback_amount"] = pd.to_numeric(df_fin["cashback_amount"], errors="coerce").fillna(0.0)
        loyalty_points_by_cat = (
            df_fin[
                df_fin["_op"].str.contains(
                    "сумма удержанная за начисленные баллы программы лояльности",
                    case=False,
                    na=False
                )
            ]
            .groupby("_cat")["_cashback_amount"]
            .sum()
            .to_dict()
        )

    # --- Цена розничная с учетом согласованной скидки: Продажа - Возврат ---
    retail_price_by_cat = {}
    if "retail_price_withdisc_rub" in df_fin.columns:
        df_fin["_retail_price_withdisc"] = pd.to_numeric(
            df_fin["retail_price_withdisc_rub"], errors="coerce"
        ).fillna(0.0)

        retail_sales = (
            df_fin[df_fin["_op"] == "Продажа"]
            .groupby("_cat")["_retail_price_withdisc"]
            .sum()
            .to_dict()
        )
        retail_returns = (
            df_fin[df_fin["_op"] == "Возврат"]
            .groupby("_cat")["_retail_price_withdisc"]
            .sum()
            .to_dict()
        )

        retail_cats = set(list(retail_sales.keys()) + list(retail_returns.keys()))
        for c in retail_cats:
            retail_price_by_cat[c] = float(retail_sales.get(c, 0.0)) - float(retail_returns.get(c, 0.0))

    # -------------------------
    # 9) Итог по категориям
    # -------------------------
    rows = []
    for cat in unique_cats:
        df_cat = df_fin[df_fin["_cat"] == cat]

        sales_qty = df_cat[df_cat.iloc[:, 24] == "Продажа"].iloc[:, 18].sum() if not df_cat.empty else 0
        return_qty_sub = df_cat[df_cat.iloc[:, 24] == "Возврат"].iloc[:, 18].sum() if not df_cat.empty else 0
        total_sales_pcs = sales_qty - return_qty_sub

        total_returns_pcs = df_cat[df_cat.iloc[:, 24] == "Возврат"].iloc[:, 18].sum() if not df_cat.empty else 0

        logistics_sum = df_cat[df_cat.iloc[:, 24] == "Логистика"].iloc[:, 32].sum() if not df_cat.empty else 0
        logistics_corr = df_cat[df_cat.iloc[:, 24] == "Коррекция логистики"].iloc[:, 32].sum() if not df_cat.empty else 0
        total_logistics = logistics_sum + logistics_corr

        pay_sales = df_cat[df_cat.iloc[:, 24] == "Продажа"].iloc[:, 42].sum() if not df_cat.empty else 0
        pay_returns = df_cat[df_cat.iloc[:, 24] == "Возврат"].iloc[:, 42].sum() if not df_cat.empty else 0
        pay_comp_ret = df_cat[df_cat.iloc[:, 24] == "Добровольная компенсация при возврате"].iloc[:, 42].sum() if not df_cat.empty else 0
        pay_comp_dmg = df_cat[df_cat.iloc[:, 24] == "Компенсация ущерба"].iloc[:, 42].sum() if not df_cat.empty else 0
        total_to_pay = pay_sales - pay_returns + pay_comp_ret + pay_comp_dmg

        ads_total = float(ads_by_cat.get(cat, 0.0))
        storage_total = float(storage_by_cat.get(cat, 0.0))
        cogs_total = float(cogs_by_cat.get(cat, 0.0))

        fbo_qty_total = float(fbo_qty_by_cat.get(cat, 0.0))
        fbo_value_total = float(fbo_value_by_cat.get(cat, 0.0))
        in_transit_to_customer_qty_total = float(in_transit_to_customer_qty_by_cat.get(cat, 0.0))
        in_transit_to_customer_value_total = float(in_transit_to_customer_value_by_cat.get(cat, 0.0))
        return_to_wb_qty_total = float(return_to_wb_qty_by_cat.get(cat, 0.0))
        return_to_wb_value_total = float(return_to_wb_value_by_cat.get(cat, 0.0))

        штрафы_total = float(penalty_by_cat.get(cat, 0.0))
        приемка_total = float(acceptance_by_cat.get(cat, 0.0))

        loyalty_discount_total = float(loyalty_discount_by_cat.get(cat, 0.0))
        loyalty_commission_total = float(loyalty_commission_by_cat.get(cat, 0.0))
        loyalty_points_total = float(loyalty_points_by_cat.get(cat, 0.0))
        retail_price_total = float(retail_price_by_cat.get(cat, 0.0))
        reviews_total = float(reviews_by_cat.get(cat, 0.0))
        fbo_total = 0.0
        subscription_total = 0.0

        total_costs = (
            float(total_logistics)
            + float(штрафы_total)
            + float(приемка_total)
            + float(ads_total)
            + float(reviews_total)
            + float(storage_total)
            + float(fbo_total)
            + float(subscription_total)
            + float(loyalty_commission_total)
            + float(loyalty_points_total)
        )
        total_to_payment = float(total_to_pay) - float(total_costs)
        profit_total = float(total_to_payment) - float(cogs_total)
        profitability_total = round((profit_total / float(cogs_total)) * 100, 1) if float(cogs_total) != 0 else 0.0
        drr_total = round(((float(reviews_total) + float(ads_total)) / float(retail_price_total)) * 100, 1) if float(retail_price_total) != 0 else 0.0

        rows.append({
            "Кабинет": company_name,
            "Период": period_str,
            "Категория": cat,
            "Цена розничная с учетом\nсогласованной скидки": retail_price_total,
            "Продаж штук": total_sales_pcs,
            "Возвратов штук": total_returns_pcs,
            "Логистика": total_logistics,
            "К перечислению": total_to_pay,
            "Реклама": ads_total,
            "Хранение": storage_total,
            "Услуги FBO": fbo_total,
            "Себестоимость": cogs_total,
            "Штрафы": штрафы_total,
            "Платная приемка": приемка_total,
            "Рентабельность": profitability_total,
            "Прибыль": profit_total,
            "Итого к оплате": total_to_payment,
            "Всего затраты": total_costs,
            "Отзывы за баллы": reviews_total,
            "ДРР": drr_total,
            "Компенсация скидки\nпо программе лояльности": loyalty_discount_total,
            "Стоимость участия\nв программе лояльности": loyalty_commission_total,
            "Сумма удержанная за начисленные\nбаллы программы лояльности": loyalty_points_total,
            "Остаток FBO, штук": fbo_qty_total,
            "Остаток FBO, рублей": fbo_value_total,
            "В пути до получателей, штук": in_transit_to_customer_qty_total,
            "В пути до получателей, рублей": in_transit_to_customer_value_total,
            "В пути возвраты на склад WB, штук": return_to_wb_qty_total,
            "В пути возвраты на склад WB, рублей": return_to_wb_value_total,
        })

    out = pd.DataFrame(rows, columns=columns_final)

    out = _drop_not_found_if_all_zero(out, label="Не найдено")

    if add_total_row:
        out = _add_total_row(out, label=total_label)

    return out


def _pick_date_column(df: pd.DataFrame, candidates: list[str], contains_any: list[str]) -> str | None:
    if df is None or df.empty:
        return None

    cols = list(df.columns)
    for c in candidates:
        if c in cols:
            return c

    lowered = {str(c).strip().lower(): c for c in cols}
    for c in candidates:
        if str(c).strip().lower() in lowered:
            return lowered[str(c).strip().lower()]

    for c in cols:
        cname = str(c).strip().lower()
        for part in contains_any:
            if part and part.lower() in cname:
                return c

    return None



def _normalize_to_date_series(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    return dt.dt.date



def create_period_comparison_report(
    df_raw: pd.DataFrame,
    company_name: str,
    date_from,
    date_to,
    df_ads: pd.DataFrame = None,
    df_storage: pd.DataFrame = None,
    df_nom: pd.DataFrame = None,
    df_price: pd.DataFrame = None,
    nom_nm_col: str = "nm_id",
    nom_cat_col: str = "subject",
):
    """
    Строит таблицу сравнения периодов по дням и категориям.

    Итоговая структура:
    - Дата
    - Категория
    - Продаж штук
    - Прибыль
    - Рентабельность
    - Итого к оплате
    - Реклама
    - Хранение

    Логика максимально безопасная:
    - используем уже существующую create_analysis_report()
    - для каждого дня выбранного периода фильтруем df_fin / df_ads / df_storage
    - считаем дневной отчет тем же процессором, без изменения текущей аналитики
    """
    final_columns = [
        "Дата",
        "Категория",
        "Продаж штук",
        "Прибыль",
        "Рентабельность",
        "Итого к оплате",
        "Реклама",
        "Хранение",
    ]

    if df_raw is None or not isinstance(df_raw, pd.DataFrame) or df_raw.empty:
        return pd.DataFrame(columns=final_columns)

    fin_date_col = _pick_date_column(
        df_raw,
        candidates=["rr_dt", "RR_DT", "sale_dt", "saleDt", "create_dt", "date", "Date"],
        contains_any=["rr_dt", "sale_dt", "create_dt", "дата", "date"],
    )

    if not fin_date_col:
        return pd.DataFrame(columns=final_columns)

    df_fin_all = df_raw.copy()
    df_fin_all["__cmp_date"] = _normalize_to_date_series(df_fin_all[fin_date_col])
    df_fin_all = df_fin_all[df_fin_all["__cmp_date"].notna()].copy()

    if df_fin_all.empty:
        return pd.DataFrame(columns=final_columns)

    ads_date_col = _pick_date_column(
        df_ads,
        candidates=["updTime", "upd_time", "date", "Date", "day", "Day"],
        contains_any=["updtime", "upd_time", "дата", "date", "day"],
    ) if isinstance(df_ads, pd.DataFrame) and not df_ads.empty else None

    storage_date_col = _pick_date_column(
        df_storage,
        candidates=["date", "Date", "calcDate", "storageDate", "dt", "day"],
        contains_any=["date", "дата", "day", "calc"],
    ) if isinstance(df_storage, pd.DataFrame) and not df_storage.empty else None

    df_ads_all = df_ads.copy() if isinstance(df_ads, pd.DataFrame) else pd.DataFrame()
    if ads_date_col and not df_ads_all.empty:
        df_ads_all["__cmp_date"] = _normalize_to_date_series(df_ads_all[ads_date_col])

    df_storage_all = df_storage.copy() if isinstance(df_storage, pd.DataFrame) else pd.DataFrame()
    if storage_date_col and not df_storage_all.empty:
        df_storage_all["__cmp_date"] = _normalize_to_date_series(df_storage_all[storage_date_col])

    start_date = pd.to_datetime(date_from).date()
    end_date = pd.to_datetime(date_to).date()
    days = pd.date_range(start=start_date, end=end_date, freq="D")

    results = []

    for day_ts in days:
        day = day_ts.date()

        df_fin_day = df_fin_all[df_fin_all["__cmp_date"] == day].copy()
        if df_fin_day.empty:
            continue
        df_fin_day = df_fin_day.drop(columns=["__cmp_date"], errors="ignore")

        if not df_ads_all.empty and ads_date_col and "__cmp_date" in df_ads_all.columns:
            df_ads_day = df_ads_all[df_ads_all["__cmp_date"] == day].copy().drop(columns=["__cmp_date"], errors="ignore")
        else:
            df_ads_day = pd.DataFrame(columns=df_ads_all.columns if not df_ads_all.empty else [])

        if not df_storage_all.empty and storage_date_col and "__cmp_date" in df_storage_all.columns:
            df_storage_day = df_storage_all[df_storage_all["__cmp_date"] == day].copy().drop(columns=["__cmp_date"], errors="ignore")
        else:
            df_storage_day = pd.DataFrame(columns=df_storage_all.columns if not df_storage_all.empty else [])

        day_report = create_analysis_report(
            df_raw=df_fin_day,
            company_name=company_name,
            period_str=day.strftime("%d.%m.%Y"),
            df_ads=df_ads_day,
            df_storage=df_storage_day,
            df_stocks=None,
            df_nom=df_nom,
            df_price=df_price,
            nom_nm_col=nom_nm_col,
            nom_cat_col=nom_cat_col,
            add_total_row=False,
            total_label="Итого",
        )

        if day_report is None or day_report.empty:
            continue

        day_report = day_report.copy()
        day_report.insert(0, "Дата", day.strftime("%d.%m.%Y"))

        keep_cols = [c for c in final_columns if c in day_report.columns]
        day_report = day_report[keep_cols].copy()
        results.append(day_report)

    if not results:
        return pd.DataFrame(columns=final_columns)

    out = pd.concat(results, ignore_index=True)
    for col in ["Продаж штук", "Прибыль", "Рентабельность", "Итого к оплате", "Реклама", "Хранение"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)

    out = out.sort_values(["Дата", "Категория"], kind="stable").reset_index(drop=True)
    return out

