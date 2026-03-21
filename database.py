import logging
from pathlib import Path
from contextlib import asynccontextmanager

import aiosqlite

from config import DB_NAME

logger = logging.getLogger(__name__)


@asynccontextmanager
async def connect_db():
    Path(DB_NAME).parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(DB_NAME)
    try:
        await db.execute("PRAGMA foreign_keys = ON")
        yield db
    finally:
        await db.close()


async def _get_table_columns(db, table_name: str) -> list[str]:
    async with db.execute(f"PRAGMA table_info({table_name})") as cursor:
        rows = await cursor.fetchall()
    return [row[1] for row in rows]


async def _migrate_legacy_table(db, table_name: str, create_sql: str, columns: list[str]):
    """Миграция старой таблицы без id в новый формат с AUTOINCREMENT."""
    existing = await _get_table_columns(db, table_name)
    if not existing or "id" in existing:
        return

    required = {c for c in columns if c != "id"}
    if not required.issubset(existing):
        logger.warning("Таблица %s: не хватает колонок для миграции, пропускаю", table_name)
        return

    backup = f"{table_name}_old"
    await db.execute(f"ALTER TABLE {table_name} RENAME TO {backup}")
    await db.execute(create_sql)

    select_cols = [c if c in existing else "NULL" for c in columns if c != "id"]
    insert_cols = [c for c in columns if c != "id"]
    await db.execute(
        f"INSERT INTO {table_name} ({', '.join(insert_cols)}) "
        f"SELECT {', '.join(select_cols)} FROM {backup}"
    )
    await db.execute(f"DROP TABLE {backup}")
    logger.info("Таблица %s мигрирована в новый формат", table_name)


async def init_db():
    async with connect_db() as db:
        await db.execute(
            """CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            timestamp DATETIME NOT NULL,
            measurement TEXT NOT NULL,
            pulse INTEGER,
            wellbeing TEXT
        )"""
        )
        await db.execute(
            """CREATE TABLE IF NOT EXISTS schedule (
            chat_id INTEGER PRIMARY KEY,
            morning TEXT NOT NULL,
            day TEXT NOT NULL,
            evening TEXT NOT NULL
        )"""
        )
        await db.execute(
            """CREATE TABLE IF NOT EXISTS users_profile (
            chat_id INTEGER PRIMARY KEY,
            working_sys INTEGER,
            working_dia INTEGER,
            is_auto_baseline BOOLEAN NOT NULL DEFAULT 1,
            baseline_updated_at DATETIME
        )"""
        )
        await db.execute(
            """CREATE TABLE IF NOT EXISTS medications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            dosage TEXT NOT NULL,
            reminder_time TEXT NOT NULL
        )"""
        )
        await db.execute(
            """CREATE TABLE IF NOT EXISTS med_intake (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            med_id INTEGER NOT NULL,
            timestamp DATETIME NOT NULL,
            FOREIGN KEY (med_id) REFERENCES medications(id) ON DELETE CASCADE
        )"""
        )

        # Миграция: добавляем колонку pulse, если её нет
        records_cols = await _get_table_columns(db, "records")
        if records_cols and "pulse" not in records_cols:
            await db.execute("ALTER TABLE records ADD COLUMN pulse INTEGER")
            # Извлекаем пульс из measurement (формат "120/80 65") в отдельную колонку
            await db.execute(
                """UPDATE records SET
                pulse = CAST(SUBSTR(measurement, INSTR(measurement, ' ') + 1) AS INTEGER)
                WHERE measurement LIKE '% %'"""
            )
            # Убираем пульс из measurement
            await db.execute(
                """UPDATE records SET
                measurement = SUBSTR(measurement, 1, INSTR(measurement, ' ') - 1)
                WHERE measurement LIKE '% %'"""
            )
            logger.info("Колонка pulse добавлена и данные мигрированы")

        # Миграция старых таблиц (без id) — только для records и med_intake,
        # у остальных таблиц PRIMARY KEY — chat_id, а не AUTOINCREMENT id.
        await _migrate_legacy_table(
            db, "records",
            """CREATE TABLE records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                timestamp DATETIME NOT NULL,
                measurement TEXT NOT NULL,
                pulse INTEGER,
                wellbeing TEXT
            )""",
            ["id", "chat_id", "timestamp", "measurement", "pulse", "wellbeing"],
        )
        await _migrate_legacy_table(
            db, "med_intake",
            """CREATE TABLE med_intake (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                med_id INTEGER NOT NULL,
                timestamp DATETIME NOT NULL,
                FOREIGN KEY (med_id) REFERENCES medications(id) ON DELETE CASCADE
            )""",
            ["id", "chat_id", "med_id", "timestamp"],
        )

        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_records_chat_timestamp ON records(chat_id, timestamp)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_medications_chat_id ON medications(chat_id)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_med_intake_chat_timestamp ON med_intake(chat_id, timestamp)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_med_intake_med_id ON med_intake(med_id)"
        )
        await db.commit()
