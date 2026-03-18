import csv
import os
from datetime import datetime
from typing import Optional

LOG_DIR = "data"
LOG_FILE = os.path.join(LOG_DIR, "telegram_send_log.csv")

CSV_HEADERS = [
    "datetime",
    "send_type",
    "company",
    "user",
    "chat_id",
    "status",
    "error",
]


def _ensure_log_file() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)

    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADERS)


def log_telegram_send(
    send_type: str,
    company: str,
    user: str,
    chat_id: int | str,
    status: str,
    error: Optional[str] = "",
) -> None:
    """
    Логирует попытку отправки Telegram-сообщения в CSV.

    send_type:
        manual / auto / test
    status:
        OK / ERROR
    """
    try:
        _ensure_log_file()

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(LOG_FILE, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    timestamp,
                    str(send_type).strip(),
                    str(company).strip(),
                    str(user).strip(),
                    str(chat_id).strip(),
                    str(status).strip(),
                    str(error).strip(),
                ]
            )
    except Exception:
        # Логгер не должен ломать рабочую отправку сообщений
        pass