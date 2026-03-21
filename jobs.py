import logging
from datetime import datetime, timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, ContextTypes

from config import MSK_TZ
from database import connect_db

logger = logging.getLogger(__name__)


def _job_time(value: str):
    localized_dt = MSK_TZ.localize(datetime.strptime(value, "%H:%M"))
    return localized_dt.timetz()


def _safe_job_time(value: str, *, chat_id: int, source: str):
    try:
        return _job_time(value)
    except ValueError:
        logger.warning(
            "Пропущено некорректное время '%s' для chat_id=%s, source=%s",
            value,
            chat_id,
            source,
        )
        return None


async def schedule_user_jobs(chat_id: int, context: ContextTypes.DEFAULT_TYPE | Application):
    job_queue = context.job_queue
    for job in job_queue.get_jobs_by_name(f"user_{chat_id}"):
        job.schedule_removal()
    for job in job_queue.get_jobs_by_name(f"weekly_{chat_id}"):
        job.schedule_removal()

    async with connect_db() as db:
        async with db.execute(
            "SELECT morning, day, evening FROM schedule WHERE chat_id=?", (chat_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                for period_idx, period_name in enumerate(["morning", "day", "evening"]):
                    if row[period_idx] != "OFF":
                        time_obj = _safe_job_time(
                            row[period_idx], chat_id=chat_id, source=f"schedule.{period_name}"
                        )
                        if time_obj is None:
                            continue
                        job_queue.run_daily(
                            send_reminder,
                            time_obj,
                            chat_id=chat_id,
                            name=f"user_{chat_id}",
                            data={"type": "bp", "period": period_name},
                        )

        async with db.execute(
            "SELECT id, name, dosage, reminder_time FROM medications WHERE chat_id=?",
            (chat_id,),
        ) as cursor:
            async for med_id, med_name, med_dosage, reminder_time in cursor:
                time_obj = _safe_job_time(
                    reminder_time, chat_id=chat_id, source=f"medications.{med_id}"
                )
                if time_obj is None:
                    continue
                job_queue.run_daily(
                    send_med_reminder,
                    time_obj,
                    chat_id=chat_id,
                    name=f"user_{chat_id}",
                    data={"id": med_id, "name": med_name, "dose": med_dosage},
                )

    sunday_time = _job_time("20:00")
    job_queue.run_daily(
        send_weekly_report,
        sunday_time,
        days=(6,),
        chat_id=chat_id,
        name=f"weekly_{chat_id}",
        data={"type": "weekly_report"},
    )


async def send_med_reminder(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data
    chat_id = context.job.chat_id
    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"💊 Пора принять: <b>{job_data['name']}</b> ({job_data['dose']})",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("✅ Принял", callback_data=f"take_{job_data['id']}")]]
            ),
        )
    except Exception as e:
        logger.exception(f"Ошибка отправки напоминания о лекарстве для chat_id={chat_id}: {e}")
        return


async def send_reminder(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    period = context.job.data.get("period", "")
    async with connect_db() as db:
        one_hour_ago = (datetime.now(MSK_TZ) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M")
        async with db.execute(
            "SELECT timestamp FROM records WHERE chat_id=? AND timestamp > ? LIMIT 1",
            (chat_id, one_hour_ago),
        ) as cursor:
            if await cursor.fetchone():
                return

    period_names = {"morning": "утреннего", "day": "дневного", "evening": "вечернего"}
    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"⏰ Время для {period_names.get(period, '')} замера давления!",
        )
    except Exception as e:
        logger.exception(f"Ошибка отправки напоминания о замере для chat_id={chat_id}: {e}")
        return


async def send_weekly_report(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    now_msk = datetime.now(MSK_TZ)
    week_ago = (now_msk - timedelta(days=7)).strftime("%Y-%m-%d 00:00")

    async with connect_db() as db:
        async with db.execute(
            "SELECT COUNT(*) FROM records WHERE chat_id=? AND timestamp >= ?",
            (chat_id, week_ago),
        ) as cursor:
            bp_count_row = await cursor.fetchone()
            bp_count = bp_count_row[0] if bp_count_row else 0

        async with db.execute(
            "SELECT COUNT(*) FROM med_intake WHERE chat_id=? AND timestamp >= ?",
            (chat_id, week_ago),
        ) as cursor:
            med_count_row = await cursor.fetchone()
            med_count = med_count_row[0] if med_count_row else 0

        async with db.execute(
            "SELECT wellbeing, COUNT(*) FROM records WHERE chat_id=? AND timestamp >= ? AND wellbeing IS NOT NULL GROUP BY wellbeing",
            (chat_id, week_ago),
        ) as cursor:
            wellbeing_rows = await cursor.fetchall()

    report_lines = [
        "📊 <b>Еженедельный отчёт</b>\n",
        f"• Замеров за неделю: {bp_count}",
        f"• Принято лекарств: {med_count}",
    ]

    if wellbeing_rows:
        feeling_map = {"good": "😊", "ok": "😐", "bad": "☹️"}
        feeling_names = {"good": "Хорошо", "ok": "Нормально", "bad": "Плохо"}
        total_feelings = sum(count for _, count in wellbeing_rows)
        report_lines.append("\n📈 <b>Самочувствие за неделю:</b>")
        for feeling, count in sorted(wellbeing_rows, key=lambda x: x[1], reverse=True):
            emoji = feeling_map.get(feeling, "")
            name = feeling_names.get(feeling, feeling)
            percent = round(count / total_feelings * 100) if total_feelings > 0 else 0
            report_lines.append(f"  {emoji} {name}: {count} раз ({percent}%)")

    keyboard = [[InlineKeyboardButton("📥 Скачать отчёт (PDF)", callback_data="export_pdf")]]
    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text="\n".join(report_lines),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.exception(f"Ошибка отправки weekly report для chat_id={chat_id}: {e}")
        return
