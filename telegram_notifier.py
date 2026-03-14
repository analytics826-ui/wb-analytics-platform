import requests
import streamlit as st


def send_telegram_message(chat_id: int, text: str) -> bool:
    token = st.secrets["telegram_bot_token"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }

    response = requests.post(url, json=payload, timeout=20)
    return response.ok


def send_admin_message(text: str) -> bool:
    admin_id = st.secrets["telegram_admin_id"]
    return send_telegram_message(admin_id, text)


def send_users_message(company: str, text: str) -> list[dict]:
    users = st.secrets["telegram_users"]
    results = []

    for user in users:
        companies = user.get("companies", [])
        if company in companies:
            ok = send_telegram_message(user["chat_id"], text)
            results.append(
                {
                    "name": user.get("name", ""),
                    "chat_id": user["chat_id"],
                    "ok": ok,
                }
            )

    return results