from pathlib import Path

import pytz

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_MORNING = "OFF"
DEFAULT_DAY = "OFF"
DEFAULT_EVENING = "OFF"
MSK_TZ = pytz.timezone("Europe/Moscow")
DB_NAME = str(BASE_DIR / "db" / "bp_tracker.db")

WELCOME_TEXT = (
    "👋 <b>Привет!</b> Я помогу отслеживать давление и приём лекарств.\n\n"
    "📝 <b>Как записать давление:</b>\n"
    "Просто отправьте сообщение:\n"
    "<code>120/80</code> или <code>120/80 65</code> (с пульсом)\n\n"
    "После каждого замера покажу статус и спрошу о самочувствии.\n"
    "Каждые 15 замеров предложу обновить рабочую норму.\n\n"
    "📅 <i>По воскресеньям в 20:00 — еженедельный отчёт.</i>"
)

SETTINGS_TEXT = "Настройки напоминаний (МСК) и нормы давления:"
CANCEL_INPUTS = {"отмена", "cancel", "/cancel"}
VALID_SCHEDULE_FIELDS = {"morning", "day", "evening"}
