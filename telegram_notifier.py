import requests
import streamlit as st
from typing import Any


def _secrets_to_plain(obj: Any):
    """Аккуратно превращает st.secrets в обычные dict/list, если возможно."""
    if isinstance(obj, dict):
        return {k: _secrets_to_plain(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_secrets_to_plain(x) for x in obj]

    # Streamlit secrets sections иногда похожи на маппинг, но не dict.
    try:
        if hasattr(obj, "items"):
            return {k: _secrets_to_plain(v) for k, v in obj.items()}
    except Exception:
        pass

    return obj


def _find_key_recursive(data: Any, target_key: str):
    """Ищет ключ в secrets даже если он случайно оказался во вложенном разделе."""
    if isinstance(data, dict):
        if target_key in data:
            return data[target_key]
        for value in data.values():
            found = _find_key_recursive(value, target_key)
            if found is not None:
                return found
    elif isinstance(data, list):
        for item in data:
            found = _find_key_recursive(item, target_key)
            if found is not None:
                return found
    return None


def get_secret_value(key: str, default=None):
    """
    Безопасно читает секрет:
    1) сначала как верхнеуровневый ключ
    2) потом рекурсивно по всем sections
    """
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass

    try:
        plain = _secrets_to_plain(st.secrets)
        found = _find_key_recursive(plain, key)
        if found is not None:
            return found
    except Exception:
        pass

    return default


def get_telegram_debug_info() -> dict:
    """Служебная диагностика: что реально видит приложение в st.secrets."""
    info = {
        "has_telegram_bot_token": False,
        "has_telegram_admin_id": False,
        "has_telegram_users": False,
        "top_level_keys": [],
    }

    try:
        info["top_level_keys"] = list(st.secrets.keys())
    except Exception:
        info["top_level_keys"] = []

    bot_token = get_secret_value("telegram_bot_token")
    admin_id = get_secret_value("telegram_admin_id")
    users = get_secret_value("telegram_users")

    info["has_telegram_bot_token"] = bot_token is not None and str(bot_token).strip() != ""
    info["has_telegram_admin_id"] = admin_id is not None and str(admin_id).strip() != ""
    info["has_telegram_users"] = users is not None

    return info


def send_telegram_message(chat_id: int, text: str) -> bool:
    token = get_secret_value("telegram_bot_token")
    if token is None or str(token).strip() == "":
        raise KeyError("В secrets не найден telegram_bot_token")

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }

    response = requests.post(url, json=payload, timeout=20)
    return response.ok


def send_admin_message(text: str) -> bool:
    admin_id = get_secret_value("telegram_admin_id")
    if admin_id is None or str(admin_id).strip() == "":
        debug = get_telegram_debug_info()
        raise KeyError(
            "В secrets не найден telegram_admin_id. "
            f"Верхние ключи secrets: {debug['top_level_keys']}"
        )

    return send_telegram_message(int(admin_id), text)


def send_users_message(company: str, text: str) -> list[dict]:
    users = get_secret_value("telegram_users", [])
    results = []

    if users is None:
        users = []

    for user in users:
        try:
            companies = user.get("companies", [])
            if company in companies:
                ok = send_telegram_message(int(user["chat_id"]), text)
                results.append(
                    {
                        "name": user.get("name", ""),
                        "chat_id": user["chat_id"],
                        "ok": ok,
                    }
                )
        except Exception as e:
            results.append(
                {
                    "name": user.get("name", "") if isinstance(user, dict) else "",
                    "chat_id": user.get("chat_id", "") if isinstance(user, dict) else "",
                    "ok": False,
                    "error": str(e),
                }
            )

    return results
