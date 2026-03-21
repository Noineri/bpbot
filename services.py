import csv
import io
import re
import statistics

from database import connect_db


def build_history_csv(bp_records, med_records):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["--- ЗАМЕРЫ ---"])
    writer.writerow(["Дата и время", "Показания", "Пульс"])
    writer.writerows(bp_records)
    writer.writerow([])
    writer.writerow(["--- ЛЕКАРСТВА ---"])
    writer.writerow(["Дата и время", "Название", "Доза"])
    writer.writerows(med_records)
    output.seek(0)
    return io.BytesIO(output.getvalue().encode("utf-8-sig"))


async def extract_user_baseline_info(db, chat_id: int):
    async with db.execute(
        "SELECT working_sys, working_dia, is_auto_baseline FROM users_profile WHERE chat_id=?",
        (chat_id,),
    ) as cursor:
        row = await cursor.fetchone()
        if row and row[0]:
            return row[0], row[1], row[2]
    return 120, 80, 1


async def get_user_baseline_info(chat_id: int):
    async with connect_db() as db:
        return await extract_user_baseline_info(db, chat_id)


async def calculate_median_baseline(chat_id: int):
    async with connect_db() as db:
        async with db.execute(
            "SELECT measurement FROM records WHERE chat_id=? ORDER BY timestamp DESC LIMIT 15",
            (chat_id,),
        ) as cursor:
            rows = await cursor.fetchall()

    if len(rows) < 10:
        return None

    sys_vals = []
    dia_vals = []

    for (measurement,) in rows:
        match = re.match(r"(\d{2,3})/(\d{2,3})", measurement)
        if match:
            sys_vals.append(int(match.group(1)))
            dia_vals.append(int(match.group(2)))

    if not sys_vals or not dia_vals:
        return None

    return int(statistics.median(sys_vals)), int(statistics.median(dia_vals))


def classify_bp(sys_val: int, dia_val: int, base_sys: int, base_dia: int) -> str:
    if base_sys <= 0:
        base_sys = 120
    sys_diff = (sys_val - base_sys) / base_sys

    if sys_val >= 160 or dia_val >= 100:
        return "🔴 Крит. высокая"
    if sys_val >= 140 or dia_val >= 90:
        return "🟠 Высокое"
    if sys_diff > 0.15:
        return "🟡 Повышенное"
    if sys_diff < -0.15:
        return "🔵 Пониженное"
    return "🟢 В норме"
