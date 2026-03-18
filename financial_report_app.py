
# --- LOG VIEW BLOCK ADDED ---
# Этот файл — версия с выводом логов Telegram в интерфейсе

import os
import pandas as pd
import streamlit as st

# ====== ТВОЙ ИСХОДНЫЙ КОД ОСТАЕТСЯ ВЫШЕ ======
# (ничего не ломаем)

# ====== БЛОК ЛОГОВ ВНИЗУ ======

st.markdown("## 📋 Лог отправки Telegram")

log_path = "data/telegram_send_log.csv"

if os.path.exists(log_path):
    try:
        df_log = pd.read_csv(log_path)
        st.dataframe(df_log.tail(50), use_container_width=True)
    except Exception as e:
        st.error(f"Ошибка чтения лога: {e}")
else:
    st.info("Лог пока не создан — отправь KPI, чтобы появился")
