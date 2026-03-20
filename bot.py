import os
import re
import logging
from datetime import datetime, timedelta
from functools import partial
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler,
)
from config import (
    DEFAULT_MORNING,
    DEFAULT_DAY,
    DEFAULT_EVENING,
    MSK_TZ,
    WELCOME_TEXT,
    SETTINGS_TEXT,
    CANCEL_INPUTS,
    VALID_SCHEDULE_FIELDS,
)
from database import connect_db, init_db
from jobs import schedule_user_jobs
from services import (
    build_history_csv,
    get_user_baseline_info,
    extract_user_baseline_info,
    calculate_median_baseline,
    classify_bp,
)

# --- ЛОГИРОВАНИЕ ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.WARNING
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

load_dotenv()


def build_delete_callback(target: str, row_id: int, label: str) -> str:
    return f"delete_{target}_{row_id}_{label}"


def parse_delete_callback(data: str):
    parts = data.split("_", 3)
    if len(parts) != 4 or parts[0] != "delete":
        return None
    _, target, row_id, label = parts
    if target not in {"bp", "med", "cancel"}:
        return None
    if target == "cancel":
        return target, None, ""
    if not row_id.isdigit():
        return None
    return target, int(row_id), label


async def safe_edit_or_reply(query, text: str, **kwargs):
    try:
        await query.edit_message_text(text, **kwargs)
    except Exception:
        logger.warning("Не удалось отредактировать сообщение, отправляю новый ответ", exc_info=True)
        if query.message:
            await query.message.reply_text(text, **kwargs)


async def send_history_csv(chat_id: int, bot):
    async with connect_db() as db:
        async with db.execute(
            "SELECT timestamp, measurement FROM records WHERE chat_id=? ORDER BY timestamp ASC",
            (chat_id,),
        ) as cursor:
            bp_records = await cursor.fetchall()

        async with db.execute(
            """SELECT i.timestamp, m.name, m.dosage
            FROM med_intake i
            JOIN medications m ON i.med_id = m.id
            WHERE i.chat_id=?
            ORDER BY i.timestamp ASC""",
            (chat_id,),
        ) as cursor:
            med_records = await cursor.fetchall()

    await bot.send_document(
        chat_id=chat_id,
        document=build_history_csv(bp_records, med_records),
        filename="history.csv",
    )


async def handle_cancel_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> bool:
    wait_mode = context.user_data.get("waiting_for")
    if not wait_mode or text.lower() not in CANCEL_INPUTS:
        return False

    context.user_data.pop("waiting_for", None)
    context.user_data.pop("med_name", None)
    context.user_data.pop("med_dose", None)
    await update.effective_message.reply_text("✅ Текущее действие отменено.")
    return True


async def handle_waiting_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
) -> bool:
    wait_mode = context.user_data.get("waiting_for")

    if wait_mode == "baseline":
        match = re.match(r"^(\d{2,3})[^\d]+(\d{2,3})$", text)
        if match:
            sys_val, dia_val = map(int, match.groups())
            async with connect_db() as db:
                await db.execute(
                    """INSERT OR REPLACE INTO users_profile
                    (chat_id, working_sys, working_dia, is_auto_baseline, baseline_updated_at)
                    VALUES (?, ?, ?, ?, ?)""",
                    (chat_id, sys_val, dia_val, 0, datetime.now(MSK_TZ).strftime("%Y-%m-%d %H:%M")),
                )
                await db.commit()
            context.user_data.pop("waiting_for")
            await update.effective_message.reply_text(
                f"✅ Ваша норма {sys_val}/{dia_val} сохранена (вручную)."
            )
        else:
            await update.effective_message.reply_text("❌ Формат: 120/80.")
        return True

    if wait_mode in ["morning", "day", "evening"]:
        if wait_mode not in VALID_SCHEDULE_FIELDS:
            context.user_data.pop("waiting_for", None)
            return True
        try:
            datetime.strptime(text, "%H:%M")
            async with connect_db() as db:
                await db.execute(
                    f"UPDATE schedule SET {wait_mode}=? WHERE chat_id=?",
                    (text, chat_id),
                )
                await db.commit()
            await schedule_user_jobs(chat_id, context)
            context.user_data.pop("waiting_for")
            await update.effective_message.reply_text(f"✅ Напоминание установлено на {text}.")
        except ValueError:
            await update.effective_message.reply_text("❌ Формат: ЧЧ:ММ.")
        return True

    if wait_mode == "med_name":
        if len(text) < 2:
            await update.effective_message.reply_text(
                "❌ Название слишком короткое. Введите название лекарства или напишите «отмена»."
            )
            return True
        context.user_data["med_name"] = text
        context.user_data["waiting_for"] = "med_dose"
        await update.effective_message.reply_text("Введите дозировку или напишите «отмена»:")
        return True

    if wait_mode == "med_dose":
        if len(text) < 1:
            await update.effective_message.reply_text(
                "❌ Введите дозировку или напишите «отмена»."
            )
            return True
        context.user_data["med_dose"] = text
        context.user_data["waiting_for"] = "med_time"
        await update.effective_message.reply_text(
            "Введите время напоминания (ЧЧ:ММ) или напишите «отмена»:"
        )
        return True

    if wait_mode == "med_time":
        try:
            datetime.strptime(text, "%H:%M")
            med_name = context.user_data.pop("med_name")
            med_dose = context.user_data.pop("med_dose")
            async with connect_db() as db:
                await db.execute(
                    "INSERT INTO medications (chat_id, name, dosage, reminder_time) VALUES (?, ?, ?, ?)",
                    (chat_id, med_name, med_dose, text),
                )
                await db.commit()
            await schedule_user_jobs(chat_id, context)
            context.user_data.pop("waiting_for")
            await update.effective_message.reply_text("✅ Лекарство добавлено.")
        except ValueError:
            await update.effective_message.reply_text(
                "❌ Ошибка времени. Используйте формат ЧЧ:ММ или напишите «отмена»."
            )
        return True

    return False


# --- ОБРАБОТЧИКИ КОМАНД ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    async with connect_db() as db:
        await db.execute(
            "INSERT OR IGNORE INTO schedule VALUES (?, ?, ?, ?)",
            (chat_id, DEFAULT_MORNING, DEFAULT_DAY, DEFAULT_EVENING),
        )
        await db.commit()

    await schedule_user_jobs(chat_id, context)

    await update.effective_message.reply_text(WELCOME_TEXT, parse_mode="HTML")


async def show_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    async with connect_db() as db:
        async with db.execute(
            "SELECT morning, day, evening FROM schedule WHERE chat_id=?",
            (chat_id,),
        ) as cursor:
            schedule_row = await cursor.fetchone()

    morning, day, evening = schedule_row or (DEFAULT_MORNING, DEFAULT_DAY, DEFAULT_EVENING)
    schedule_text = (
        "\n\n🕒 <b>Текущее расписание:</b>\n"
        f"• Утро: {morning}\n"
        f"• День: {day}\n"
        f"• Вечер: {evening}"
    )

    keyboard = [
        [
            InlineKeyboardButton("🌅 Утро", callback_data="set_morning"),
            InlineKeyboardButton("☀️ День", callback_data="set_day"),
            InlineKeyboardButton("🌙 Вечер", callback_data="set_evening"),
        ],
        [
            InlineKeyboardButton("❌ Откл. Утро", callback_data="off_morning"),
            InlineKeyboardButton("❌ Откл. День", callback_data="off_day"),
            InlineKeyboardButton("❌ Откл. Вечер", callback_data="off_evening"),
        ],
        [InlineKeyboardButton("🎯 Установить норму давления", callback_data="set_baseline")],
    ]
    await update.effective_message.reply_text(
        SETTINGS_TEXT + schedule_text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def med_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["waiting_for"] = "med_name"
    await update.effective_message.reply_text("Введите название лекарства:")


async def med_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    async with connect_db() as db:
        async with db.execute(
            "SELECT id, name, dosage, reminder_time FROM medications WHERE chat_id=?",
            (chat_id,),
        ) as cursor:
            meds = await cursor.fetchall()

    if not meds:
        await update.effective_message.reply_text("Список лекарств пуст.")
        return

    text = "💊 <b>Ваши лекарства:</b>\n\n"
    keyboard = []

    for med_id, med_name, med_dosage, reminder_time in meds:
        text += f"• {med_name} ({med_dosage}) — {reminder_time} МСК\n"
        keyboard.append(
            [InlineKeyboardButton(f"❌ Удалить {med_name}", callback_data=f"del_med_{med_id}")]
        )

    await update.effective_message.reply_text(
        text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def universal_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    chat_id = update.effective_chat.id

    try:
        if data.startswith("set_"):
            mode = data.split("_")[1]
            context.user_data["waiting_for"] = mode
            if mode == "baseline":
                await query.edit_message_text("Введите норму (120/80):")
            else:
                await query.edit_message_text("Введите время (ЧЧ:ММ):")

        elif data.startswith("off_"):
            field = data.split("_")[1]
            if field not in VALID_SCHEDULE_FIELDS:
                await query.edit_message_text("❌ Ошибка: неверное поле.")
                return
            async with connect_db() as db:
                await db.execute(
                    f"UPDATE schedule SET {field}=? WHERE chat_id=?", ("OFF", chat_id)
                )
                await db.commit()
            await schedule_user_jobs(chat_id, context)
            await query.edit_message_text("✅ Отключено.")

        elif data.startswith("take_"):
            med_id = data.split("_")[1]
            async with connect_db() as db:
                cursor = await db.execute(
                    """INSERT INTO med_intake (chat_id, med_id, timestamp)
                    SELECT ?, id, ? FROM medications WHERE id=? AND chat_id=?""",
                    (chat_id, datetime.now(MSK_TZ).strftime("%Y-%m-%d %H:%M"), med_id, chat_id),
                )
                inserted = cursor.rowcount
                await db.commit()
            if inserted:
                await query.edit_message_text("✅ Отметка о приеме сохранена.")
            else:
                await query.edit_message_text("❌ Лекарство не найдено.")

        elif data.startswith("del_med_"):
            med_id = data.split("_")[2]
            async with connect_db() as db:
                cursor = await db.execute(
                    "DELETE FROM medications WHERE id=? AND chat_id=?", (med_id, chat_id)
                )
                deleted = cursor.rowcount
                await db.commit()
            if deleted:
                await schedule_user_jobs(chat_id, context)
                await query.edit_message_text("✅ Удалено.")
            else:
                await query.edit_message_text("❌ Лекарство не найдено.")

        elif data.startswith("apply_base_"):
            parts = data.split("_")
            new_sys = parts[2]
            new_dia = parts[3]
            async with connect_db() as db:
                await db.execute(
                    """INSERT OR REPLACE INTO users_profile
                    (chat_id, working_sys, working_dia, is_auto_baseline, baseline_updated_at)
                    VALUES (?, ?, ?, ?, ?)""",
                    (chat_id, new_sys, new_dia, 1, datetime.now(MSK_TZ).strftime("%Y-%m-%d %H:%M")),
                )
                await db.commit()
            await query.edit_message_text(f"✅ Норма обновлена до {new_sys}/{new_dia}.")

        elif data.startswith("feel_"):
            parts = data.split("_")
            feeling = parts[1]
            rowid = parts[2] if len(parts) > 2 else None

            feeling_map = {"good": "😊", "ok": "😐", "bad": "☹️"}
            feeling_emoji = feeling_map.get(feeling, "")

            if rowid:
                async with connect_db() as db:
                    await db.execute(
                        "UPDATE records SET wellbeing=? WHERE id=? AND chat_id=?", (feeling, rowid, chat_id)
                    )
                    await db.commit()

            original_text = query.message.text_html or query.message.text or ""
            if "💬 Как вы себя чувствуете?" in original_text:
                new_text = original_text.replace(
                    "\n\n💬 Как вы себя чувствуете?", f" {feeling_emoji}"
                )
            else:
                new_text = original_text + f" {feeling_emoji}"

            try:
                await query.edit_message_text(new_text, parse_mode="HTML")
            except Exception:
                logger.warning("Не удалось обновить сообщение с самочувствием")

            await query.answer(f"Записано: {feeling_emoji}")

        elif data.startswith("delete_"):
            parsed = parse_delete_callback(data)
            if not parsed:
                await query.edit_message_text("❌ Некорректная команда удаления.")
                return

            target, rowid, label = parsed
            if target == "cancel":
                await query.edit_message_text("❌ Отменено.")
                return

            if target == "bp":
                async with connect_db() as db:
                    cursor = await db.execute(
                        "DELETE FROM records WHERE id=? AND chat_id=?", (rowid, chat_id)
                    )
                    await db.commit()
                if cursor.rowcount:
                    await query.edit_message_text(f"🗑 Удалено: {label}")
                else:
                    await query.edit_message_text("❌ Запись не найдена или уже удалена.")
                return

            if target == "med":
                async with connect_db() as db:
                    cursor = await db.execute(
                        "DELETE FROM med_intake WHERE id=? AND chat_id=?", (rowid, chat_id)
                    )
                    await db.commit()
                if cursor.rowcount:
                    await query.edit_message_text(f"🗑 Удалён приём: {label}")
                else:
                    await query.edit_message_text("❌ Запись не найдена или уже удалена.")
                return

        elif data == "export_csv":
            await query.edit_message_text("📥 Подготовка файла...")

            await send_history_csv(chat_id, context.bot)
            await query.edit_message_text("✅ Файл отправлен.")

    except Exception:
        logger.exception("Ошибка в callback обработчике")
        await safe_edit_or_reply(query, "❌ Произошла ошибка.")


async def log_measurement(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    if await handle_cancel_input(update, context, text):
        return

    if await handle_waiting_input(update, context, chat_id, text):
        return

    match = re.match(r"^(\d{2,3})[\s/-]+(\d{2,3})(?:[\s/-]+(\d{2,3}))?$", text)
    if not match:
        return

    sys_val, dia_val, pulse = map(int, match.groups(default=0))

    if not (50 <= sys_val <= 250 and 30 <= dia_val <= 150):
        await update.effective_message.reply_text("⚠️ Цифры кажутся нереалистичными. Проверьте ввод.")
        return

    now_msk = datetime.now(MSK_TZ)
    timestamp = now_msk.strftime("%Y-%m-%d %H:%M")

    base_sys, base_dia, is_auto = await get_user_baseline_info(chat_id)
    status = classify_bp(sys_val, dia_val, base_sys, base_dia)

    measurement_str = f"{sys_val}/{dia_val}"
    if pulse:
        measurement_str += f" {pulse}"

    keyboard = []

    async with connect_db() as db:
        await db.execute(
            "INSERT INTO records (chat_id, timestamp, measurement, wellbeing) VALUES (?, ?, ?, NULL)",
            (chat_id, timestamp, measurement_str),
        )
        async with db.execute("SELECT last_insert_rowid()") as cursor:
            row = await cursor.fetchone()
            last_rowid = row[0] if row else None

        async with db.execute(
            "SELECT COUNT(*) FROM records WHERE chat_id=?", (chat_id,)
        ) as count_cursor:
            count_row = await count_cursor.fetchone()
            if count_row[0] % 15 == 0:
                new_baseline = await calculate_median_baseline(chat_id)
                if new_baseline:
                    new_base_sys, new_base_dia = new_baseline
                    safe_base_sys = base_sys if base_sys > 0 else 120
                    if abs(new_base_sys - safe_base_sys) / safe_base_sys > 0.05:
                        status += f"\n\n🤖 <b>Совет:</b> Медиана за 15 замеров: {new_base_sys}/{new_base_dia}. Обновим вашу рабочую норму давления?"
                        keyboard.append(
                            [InlineKeyboardButton(
                                f"🔄 Обновить до {new_base_sys}/{new_base_dia}",
                                callback_data=f"apply_base_{new_base_sys}_{new_base_dia}",
                            )]
                        )

        await db.commit()

    wellbeing_keyboard = [
        [
            InlineKeyboardButton("😊 Хорошо", callback_data=f"feel_good_{last_rowid}"),
            InlineKeyboardButton("😐 Нормально", callback_data=f"feel_ok_{last_rowid}"),
            InlineKeyboardButton("☹️ Плохо", callback_data=f"feel_bad_{last_rowid}"),
        ]
    ]

    async with connect_db() as db:
        async with db.execute(
            "SELECT id, name FROM medications WHERE chat_id=?", (chat_id,)
        ) as med_cursor:
            async for med_id, med_name in med_cursor:
                wellbeing_keyboard.append(
                    [InlineKeyboardButton(f"💊 Принял {med_name}", callback_data=f"take_{med_id}")]
                )

    wellbeing_keyboard.extend(keyboard)

    await update.effective_message.reply_text(
        f"✅ <b>Записано:</b> {sys_val}/{dia_val}\n📊 <b>Статус:</b> {status}\n\n💬 Как вы себя чувствуете?",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(wellbeing_keyboard),
    )


async def get_stats(update: Update, context: ContextTypes.DEFAULT_TYPE, days: int):
    chat_id = update.effective_chat.id
    start_dt = (datetime.now(MSK_TZ) - timedelta(days=days)).strftime("%Y-%m-%d 00:00")

    feeling_map = {"good": "😊", "ok": "😐", "bad": "☹️"}

    async with connect_db() as db:
        async with db.execute(
            "SELECT timestamp, measurement, wellbeing FROM records WHERE chat_id=? AND timestamp >= ? ORDER BY timestamp ASC",
            (chat_id, start_dt),
        ) as cursor:
            bp_records = await cursor.fetchall()

        async with db.execute(
            """SELECT i.timestamp, m.name || ' (' || m.dosage || ')'
            FROM med_intake i
            JOIN medications m ON i.med_id = m.id
            WHERE i.chat_id=? AND i.timestamp >= ?
            ORDER BY i.timestamp ASC""",
            (chat_id, start_dt),
        ) as cursor:
            med_records = await cursor.fetchall()

        base_sys, base_dia, is_auto = await extract_user_baseline_info(db, chat_id)

    if not bp_records and not med_records:
        await update.effective_message.reply_text("Нет данных.")
        return

    events = []
    for timestamp, value, wellbeing in bp_records:
        feeling_emoji = feeling_map.get(wellbeing, "") if wellbeing else ""
        events.append((timestamp, f"🔹 {timestamp[5:16]} — <b>{value}</b> {feeling_emoji}"))
    for timestamp, value in med_records:
        events.append((timestamp, f"💊 {timestamp[5:16]} — {value}"))
    events.sort(key=lambda x: x[0])

    baseline_type = "авто" if is_auto else "ручная"
    result = (
        f"📊 <b>Статистика за {days} дн.</b>\n🎯 Норма: {base_sys}/{base_dia} ({baseline_type})\n"
        + "—" * 15 + "\n"
    )
    result += "\n".join([event[1] for event in events])

    await update.effective_message.reply_text(result, parse_mode="HTML")


async def export_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await send_history_csv(chat_id, context.bot)


async def delete_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    async with connect_db() as db:
        async with db.execute(
            "SELECT id, measurement FROM records WHERE chat_id=? ORDER BY timestamp DESC LIMIT 1",
            (chat_id,),
        ) as cursor:
            bp_row = await cursor.fetchone()

        async with db.execute(
            """SELECT mi.id, m.name, m.dosage
            FROM med_intake mi
            JOIN medications m ON mi.med_id = m.id
            WHERE mi.chat_id=?
            ORDER BY mi.timestamp DESC LIMIT 1""",
            (chat_id,),
        ) as cursor:
            med_row = await cursor.fetchone()

    has_bp = bp_row is not None
    has_med = med_row is not None

    if not has_bp and not has_med:
        await update.effective_message.reply_text("Нечего удалять.")
        return

    keyboard = []

    if has_bp:
        bp_callback = build_delete_callback("bp", bp_row[0], bp_row[1])

    if has_med:
        med_label = f"{med_row[1]} ({med_row[2]})"
        med_callback = build_delete_callback("med", med_row[0], med_label)

    if has_bp and has_med:
        keyboard = [
            [
                InlineKeyboardButton(f"📊 {bp_row[1]}", callback_data=bp_callback),
                InlineKeyboardButton(f"💊 {med_row[1]}", callback_data=med_callback),
            ],
            [InlineKeyboardButton("❌ Отмена", callback_data="delete_cancel_0_")],
        ]
        message_text = "🗑 <b>Что удалить?</b>"
    elif has_bp:
        keyboard = [
            [InlineKeyboardButton(f"🗑 Удалить {bp_row[1]}", callback_data=bp_callback)],
            [InlineKeyboardButton("❌ Отмена", callback_data="delete_cancel_0_")],
        ]
        message_text = f"🗑 <b>Удалить последнюю запись?</b>\n\n📊 {bp_row[1]}"
    else:
        keyboard = [
            [InlineKeyboardButton(f"🗑 Удалить {med_row[1]}", callback_data=med_callback)],
            [InlineKeyboardButton("❌ Отмена", callback_data="delete_cancel_0_")],
        ]
        message_text = f"🗑 <b>Удалить последнюю запись?</b>\n\n💊 {med_row[1]} ({med_row[2]})"

    await update.effective_message.reply_text(
        message_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(msg="Exception while handling an update:", exc_info=context.error)


async def post_init(application: Application):
    await init_db()
    async with connect_db() as db:
        async with db.execute("SELECT chat_id FROM schedule") as cursor:
            async for (chat_id,) in cursor:
                await schedule_user_jobs(chat_id, application)


if __name__ == "__main__":
    token = os.getenv("TG_TOKEN")
    if not token:
        raise RuntimeError("Не задан TG_TOKEN в переменных окружения.")

    application = Application.builder().token(token).post_init(post_init).build()

    application.add_error_handler(error_handler)

    commands = [
        ("start", start),
        ("settings", show_settings),
        ("med_add", med_add),
        ("med_list", med_list),
        ("delete_last", delete_last),
        ("export", export_data),
    ]

    for cmd_name, cmd_handler in commands:
        application.add_handler(CommandHandler(cmd_name, cmd_handler))

    application.add_handler(CommandHandler("stats_3", partial(get_stats, days=3)))
    application.add_handler(CommandHandler("stats_7", partial(get_stats, days=7)))

    application.add_handler(CallbackQueryHandler(universal_callback))

    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, log_measurement)
    )

    application.run_polling()
