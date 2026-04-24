import csv
import hashlib
import json
import logging
import os
import random
import sqlite3
import threading
import time
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional

import schedule
import yadisk

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
YANDEX_TOKEN = os.getenv("YANDEX_TOKEN")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")

CURATOR_NAME = os.getenv("CURATOR_NAME", "Оля")
CURATOR_USERNAME = os.getenv("CURATOR_USERNAME", "username_куратора")

YANDEX_FOLDER = "/SomaSpace"
DB_PATH = "/app/data/somaspace.db"

BASE_DIR = Path(__file__).resolve().parent
QUESTIONS_FILE = BASE_DIR / "questions.json"
WELCOME_FILE = BASE_DIR / "bot_welcome_messages.json"

WAITING_CODE = 0

with open(QUESTIONS_FILE, encoding="utf-8") as f:
    CONFIG = json.load(f)

with open(WELCOME_FILE, encoding="utf-8") as f:
    WELCOME = json.load(f)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def render_text(text: str) -> str:
    return (
        text
        .replace("[ИМЯ КУРАТОРА]", CURATOR_NAME)
        .replace("@[username_куратора]", f"@{CURATOR_USERNAME}")
        .replace("[username_куратора]", CURATOR_USERNAME)
    )


def get_conn():
    os.makedirs("/app/data", exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            code TEXT PRIMARY KEY,
            name TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS participants (
            anon_id TEXT PRIMARY KEY,
            company_code TEXT NOT NULL,
            telegram_id INTEGER UNIQUE NOT NULL,
            registered TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS survey_answers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            company_code TEXT NOT NULL,
            anon_id TEXT NOT NULL,
            question_id TEXT NOT NULL,
            category TEXT,
            value TEXT NOT NULL,
            score INTEGER,
            weekday TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS survey_sessions (
            telegram_id INTEGER PRIMARY KEY,
            company_code TEXT NOT NULL,
            question_ids_json TEXT NOT NULL,
            current_index INTEGER DEFAULT 0,
            started_at TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        INSERT OR IGNORE INTO companies (code, name)
        VALUES ('TEST', 'Тестовая компания')
    """)

    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")


def anonymize(telegram_id: int) -> str:
    raw = str(telegram_id).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def is_admin(user_id: int) -> bool:
    return ADMIN_TELEGRAM_ID is not None and str(user_id) == str(ADMIN_TELEGRAM_ID)


def get_all_companies() -> list[str]:
    conn = get_conn()
    rows = conn.execute("SELECT code FROM companies ORDER BY code").fetchall()
    conn.close()
    return [row[0] for row in rows]


def get_company_name(code: str) -> Optional[str]:
    conn = get_conn()
    row = conn.execute(
        "SELECT name FROM companies WHERE code = ?",
        (code.upper(),)
    ).fetchone()
    conn.close()
    return row[0] if row else None


def is_valid_code(code: str) -> bool:
    return code.upper() in get_all_companies()


def register_user(telegram_id: int, company_code: str) -> bool:
    anon_id = anonymize(telegram_id)
    try:
        conn = get_conn()
        conn.execute("""
            INSERT OR IGNORE INTO participants
            (anon_id, company_code, telegram_id)
            VALUES (?, ?, ?)
        """, (anon_id, company_code.upper(), telegram_id))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка регистрации: {e}")
        return False


def get_user_company(telegram_id: int) -> Optional[str]:
    conn = get_conn()
    row = conn.execute(
        "SELECT company_code FROM participants WHERE telegram_id = ?",
        (telegram_id,)
    ).fetchone()
    conn.close()
    return row[0] if row else None


def get_all_participants() -> list[tuple[int, str]]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT telegram_id, company_code FROM participants"
    ).fetchall()
    conn.close()
    return rows


def get_questions_for_date(dt: datetime) -> Optional[list[str]]:
    weekday = dt.strftime("%A").lower()
    week_num = (dt.day - 1) // 7 + 1

    if week_num > 4:
        week_num = 4

    week_key = f"week_{week_num}"
    day_config = CONFIG["schedule_by_week"].get(week_key, {}).get(weekday)

    if not day_config:
        return None

    question_ids = day_config["questions"].copy()

    if day_config.get("add_monthly_deep"):
        question_ids.append("MONTHLY_DEEP")

    return question_ids


def get_test_questions_for_current_week() -> list[str]:
    dt = datetime.now()
    week_num = (dt.day - 1) // 7 + 1

    if week_num > 4:
        week_num = 4

    week_key = f"week_{week_num}"
    week_cfg = CONFIG["schedule_by_week"].get(week_key, {})

    day_config = week_cfg.get("thursday") or week_cfg.get("tuesday")

    if not day_config:
        return ["mood", "workload", "leave_marker"]

    question_ids = day_config["questions"].copy()

    if day_config.get("add_monthly_deep"):
        question_ids.append("MONTHLY_DEEP")

    return question_ids


def get_question(question_id: str) -> Optional[dict]:
    if question_id == "MONTHLY_DEEP":
        return CONFIG["monthly_deep_question"]

    if question_id in CONFIG["core_questions"]:
        return CONFIG["core_questions"][question_id]

    if question_id in CONFIG["rotation_questions"]:
        return CONFIG["rotation_questions"][question_id]

    return None


def get_greeting_for_date(dt: datetime) -> str:
    weekday = dt.strftime("%A").lower()
    if weekday == "tuesday":
        return random.choice(CONFIG["greetings"]["tuesday_morning"])
    return random.choice(CONFIG["greetings"]["thursday_afternoon"])


def get_test_greeting() -> str:
    return "Привет 👋 Это тестовый запуск опроса.\nПройдём его сейчас."


def get_closing() -> str:
    return random.choice(CONFIG["closings"]["after_survey"])


def get_button_text(question_id: str, value: str) -> str:
    question = get_question(question_id)
    if not question:
        return value

    for btn in question["buttons"]:
        if btn["value"] == value:
            return btn["text"]

    return value


def start_survey_session(telegram_id: int, company_code: str, question_ids: list[str]):
    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO survey_sessions
        (telegram_id, company_code, question_ids_json, current_index, started_at)
        VALUES (?, ?, ?, 0, datetime('now'))
    """, (
        telegram_id,
        company_code.upper(),
        json.dumps(question_ids, ensure_ascii=False),
    ))
    conn.commit()
    conn.close()


def get_survey_session(telegram_id: int) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("""
        SELECT company_code, question_ids_json, current_index
        FROM survey_sessions
        WHERE telegram_id = ?
    """, (telegram_id,)).fetchone()
    conn.close()

    if not row:
        return None

    company_code, question_ids_json, current_index = row
    return {
        "company_code": company_code,
        "question_ids": json.loads(question_ids_json),
        "current_index": current_index
    }


def update_survey_session_index(telegram_id: int, new_index: int):
    conn = get_conn()
    conn.execute("""
        UPDATE survey_sessions
        SET current_index = ?
        WHERE telegram_id = ?
    """, (new_index, telegram_id))
    conn.commit()
    conn.close()


def clear_survey_session(telegram_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM survey_sessions WHERE telegram_id = ?", (telegram_id,))
    conn.commit()
    conn.close()


def save_single_answer(telegram_id: int, company: str, question_id: str, value: str):
    question = get_question(question_id)
    if not question:
        return

    score = None
    category = question.get("category", "")

    for btn in question["buttons"]:
        if btn["value"] == value:
            score = btn.get("score")
            break

    now = datetime.now()
    conn = get_conn()
    conn.execute("""
        INSERT INTO survey_answers
        (date, time, company_code, anon_id, question_id, category, value, score, weekday)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        now.strftime("%Y-%m-%d"),
        now.strftime("%H:%M"),
        company.upper(),
        anonymize(telegram_id),
        question_id,
        category,
        value,
        score,
        now.strftime("%A")
    ))
    conn.commit()
    conn.close()
    logger.info(f"Сохранён ответ: {company} / {question_id} / {value}")


def export_to_csv(company_code: Optional[str] = None) -> str:
    filename = f"/app/data/somaspace_answers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    conn = get_conn()

    if company_code:
        rows = conn.execute("""
            SELECT date, time, company_code, anon_id, question_id, category, value, score, weekday
            FROM survey_answers
            WHERE company_code = ?
            ORDER BY date DESC, time DESC
        """, (company_code.upper(),)).fetchall()
    else:
        rows = conn.execute("""
            SELECT date, time, company_code, anon_id, question_id, category, value, score, weekday
            FROM survey_answers
            ORDER BY date DESC, time DESC
        """).fetchall()

    conn.close()

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Дата", "Время", "Компания", "ID_анонимный",
            "ID_вопроса", "Категория", "Ответ", "Score", "День_недели"
        ])
        writer.writerows(rows)

    return filename


def upload_to_yandex_disk(local_file: str):
    if not YANDEX_TOKEN:
        logger.warning("YANDEX_TOKEN не задан, загрузка на Яндекс.Диск пропущена")
        return

    try:
        y = yadisk.YaDisk(token=YANDEX_TOKEN)

        if not y.exists(YANDEX_FOLDER):
            y.mkdir(YANDEX_FOLDER)

        remote_path = f"{YANDEX_FOLDER}/{Path(local_file).name}"
        y.upload(local_file, remote_path, overwrite=True)
        logger.info(f"Файл загружен на Яндекс.Диск: {remote_path}")

        os.remove(local_file)

    except Exception as e:
        logger.error(f"Ошибка загрузки на Яндекс.Диск: {e}")


def daily_backup():
    logger.info("Начинаем ежедневный бэкап...")
    csv_file = export_to_csv()
    upload_to_yandex_disk(csv_file)


def format_distribution(question_id: str, rows: list[tuple[str, int]], total: int) -> str:
    if total == 0 or not rows:
        return "Нет данных."

    lines = []
    for value, count in rows:
        pct = round(count / total * 100)
        label = get_button_text(question_id, value)
        lines.append(f"{label}: {count} ({pct}%)")

    return "\n".join(lines)


def get_company_stats_text(company_code: str) -> str:
    code = company_code.upper()
    company_name = get_company_name(code) or "Неизвестная компания"

    conn = get_conn()

    total_answers = conn.execute("""
        SELECT COUNT(*) FROM survey_answers WHERE company_code = ?
    """, (code,)).fetchone()[0]

    unique_people = conn.execute("""
        SELECT COUNT(DISTINCT anon_id) FROM survey_answers WHERE company_code = ?
    """, (code,)).fetchone()[0]

    distinct_questions = conn.execute("""
        SELECT DISTINCT question_id
        FROM survey_answers
        WHERE company_code = ?
        ORDER BY question_id
    """, (code,)).fetchall()

    conn.close()

    if total_answers == 0:
        return (
            f"📊 *Статистика по {code}*\n"
            f"{company_name}\n\n"
            "Пока нет ответов по этой компании."
        )

    parts = [
        f"📊 *Статистика по {code}*",
        company_name,
        "",
        f"Всего ответов: *{total_answers}*",
        f"Уникальных людей: *{unique_people}*",
        "",
    ]

    conn = get_conn()

    for (question_id,) in distinct_questions:
        question = get_question(question_id)
        if not question:
            continue

        question_total = conn.execute("""
            SELECT COUNT(*)
            FROM survey_answers
            WHERE company_code = ? AND question_id = ?
        """, (code, question_id)).fetchone()[0]

        rows = conn.execute("""
            SELECT value, COUNT(*)
            FROM survey_answers
            WHERE company_code = ? AND question_id = ?
            GROUP BY value
            ORDER BY COUNT(*) DESC
        """, (code, question_id)).fetchall()

        parts.append(f"*{question.get('category', question_id)}*")
        parts.append(format_distribution(question_id, rows, question_total))
        parts.append("")

    conn.close()
    return "\n".join(parts).strip()


def build_keyboard_for_question(question_id: str) -> InlineKeyboardMarkup:
    question = get_question(question_id)

    if not question:
        return InlineKeyboardMarkup([])

    keyboard = [
        [
            InlineKeyboardButton(
                btn["text"],
                callback_data=f"{question_id}:{btn['value']}"
            )
        ]
        for btn in question["buttons"]
    ]

    return InlineKeyboardMarkup(keyboard)


def build_welcome_keyboard(step_key: str) -> InlineKeyboardMarkup:
    step = WELCOME["welcome_flow"].get(step_key, {})
    buttons = step.get("buttons", [])

    keyboard = [
        [
            InlineKeyboardButton(
                btn["text"],
                callback_data=f"welcome:{btn['value']}"
            )
        ]
        for btn in buttons
    ]

    return InlineKeyboardMarkup(keyboard)


async def send_welcome_step(
    bot,
    chat_id: int,
    step_key: str,
    use_delay: bool = True
):
    step = WELCOME["welcome_flow"][step_key]

    delay = step.get("delay_seconds", 0)
    if use_delay and delay:
        await asyncio.sleep(delay)

    await bot.send_message(
        chat_id=chat_id,
        text=render_text(step["text"]),
        reply_markup=build_welcome_keyboard(step_key),
        parse_mode="Markdown"
    )


async def send_question(bot, chat_id: int, question_id: str, current_num: int, total_num: int):
    question = get_question(question_id)

    if not question:
        return

    text = question["text"].replace("{total}", str(total_num))

    await bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=build_keyboard_for_question(question_id),
        parse_mode="Markdown"
    )


async def launch_survey_for_user(bot, telegram_id: int, company_code: str, question_ids: list[str], greeting: str):
    if not question_ids:
        return

    start_survey_session(telegram_id, company_code, question_ids)

    await bot.send_message(
        chat_id=telegram_id,
        text=greeting
    )

    await send_question(
        bot=bot,
        chat_id=telegram_id,
        question_id=question_ids[0],
        current_num=1,
        total_num=len(question_ids)
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    clear_survey_session(update.effective_user.id)

    await send_welcome_step(
        bot=context.bot,
        chat_id=update.effective_chat.id,
        step_key="step_1_greeting",
        use_delay=False
    )

    return WAITING_CODE


async def handle_welcome_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    value = query.data.replace("welcome:", "")

    if value == "greeting_ack":
        await send_welcome_step(context.bot, query.message.chat_id, "step_2_what_is_it")

    elif value == "what_ack":
        await send_welcome_step(context.bot, query.message.chat_id, "step_3_confidentiality")

    elif value == "conf_understood":
        await send_welcome_step(context.bot, query.message.chat_id, "step_4_no_retaliation")

    elif value == "no_retaliation_ack":
        await send_welcome_step(context.bot, query.message.chat_id, "step_5_consent")

    elif value == "consent_yes":
        step = WELCOME["welcome_flow"]["step_6_after_consent_yes"]
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=render_text(step["text"]),
            parse_mode="Markdown"
        )

    elif value == "consent_questions":
        step = WELCOME["welcome_flow"]["step_6_after_consent_questions"]
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=render_text(step["text"]),
            parse_mode="Markdown"
        )

    elif value == "consent_no":
        step = WELCOME["welcome_flow"]["step_6_after_consent_no"]
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=render_text(step["text"]),
            parse_mode="Markdown"
        )


async def receive_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    code = update.message.text.strip().upper()
    user_id = update.effective_user.id

    if not is_valid_code(code):
        await update.message.reply_text(
            "❌ Код не найден. Попробуй ещё раз или уточни у HR."
        )
        return WAITING_CODE

    register_user(user_id, code)

    await update.message.reply_text(
        "✅ Отлично, ты зарегистрирован!\n\n"
        "Сейчас запущу тестовый опрос, чтобы можно было всё проверить."
    )

    question_ids = get_test_questions_for_current_week()
    greeting = get_test_greeting()

    await launch_survey_for_user(
        bot=context.bot,
        telegram_id=user_id,
        company_code=code,
        question_ids=question_ids,
        greeting=greeting
    )

    return ConversationHandler.END


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = WELCOME["contact_commands"]["help"]["text"]

    await update.message.reply_text(
        render_text(text),
        parse_mode="Markdown"
    )


async def contact_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = WELCOME["contact_commands"]["contact"]["text"]

    await update.message.reply_text(
        render_text(text),
        parse_mode="Markdown"
    )


async def privacy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = WELCOME["contact_commands"]["privacy"]["text"]

    await update.message.reply_text(
        render_text(text),
        parse_mode="Markdown"
    )


async def myid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Твой Telegram ID: `{update.effective_user.id}`",
        parse_mode="Markdown"
    )


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_survey_session(update.effective_user.id)

    await update.message.reply_text(
        "Опрос остановлен. Чтобы вернуться — /start"
    )


async def testsurvey_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    company_code = get_user_company(user_id)

    if not company_code:
        await update.message.reply_text(
            "Сначала зарегистрируйся через /start и введи код компании."
        )
        return

    clear_survey_session(user_id)

    question_ids = get_test_questions_for_current_week()
    greeting = get_test_greeting()

    await launch_survey_for_user(
        bot=context.bot,
        telegram_id=user_id,
        company_code=company_code,
        question_ids=question_ids,
        greeting=greeting
    )


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("У тебя нет доступа к статистике.")
        return

    if not context.args:
        await update.message.reply_text(
            "Напиши код компании так:\n`/stats TEST`",
            parse_mode="Markdown"
        )
        return

    company_code = context.args[0].strip().upper()

    if not is_valid_code(company_code):
        await update.message.reply_text("Такой код компании не найден.")
        return

    text = get_company_stats_text(company_code)
    await update.message.reply_text(text, parse_mode="Markdown")


async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("У тебя нет доступа к экспорту.")
        return

    company_code = None

    if context.args:
        company_code = context.args[0].strip().upper()

    try:
        filename = export_to_csv(company_code)

        caption = "Вот экспорт ответов в CSV."
        if company_code:
            caption = f"Вот экспорт ответов по компании {company_code}."

        with open(filename, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=os.path.basename(filename),
                caption=caption
            )

    except Exception as e:
        logger.error(f"Ошибка отправки CSV: {e}")
        await update.message.reply_text("Не получилось отправить CSV-файл.")


async def handle_survey_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    data = query.data

    if ":" not in data:
        await query.edit_message_text("Ошибка формата ответа.")
        return

    question_id, value = data.split(":", 1)

    session = get_survey_session(user_id)

    if not session:
        await query.edit_message_text(
            "Эта сессия уже неактуальна. Нажми /start или /testsurvey и начни заново."
        )
        return

    company_code = session["company_code"]
    question_ids = session["question_ids"]
    current_index = session["current_index"]

    if current_index >= len(question_ids):
        clear_survey_session(user_id)
        await query.edit_message_text("Опрос уже завершён. Нажми /start, чтобы начать заново.")
        return

    expected_question_id = question_ids[current_index]

    if question_id != expected_question_id:
        clear_survey_session(user_id)
        await query.edit_message_text(
            "Похоже, это старая кнопка из предыдущего опроса. Нажми /start или /testsurvey."
        )
        return

    save_single_answer(user_id, company_code, question_id, value)

    next_index = current_index + 1

    if next_index >= len(question_ids):
        clear_survey_session(user_id)
        await query.edit_message_text(get_closing())
        return

    update_survey_session_index(user_id, next_index)

    next_question_id = question_ids[next_index]
    next_question = get_question(next_question_id)
    total_num = len(question_ids)

    text = next_question["text"].replace("{total}", str(total_num))

    await query.edit_message_text(
        text=text,
        reply_markup=build_keyboard_for_question(next_question_id),
        parse_mode="Markdown"
    )


async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = update.callback_query.data

    if data.startswith("welcome:"):
        await handle_welcome_callback(update, context)
    else:
        await handle_survey_button(update, context)


async def send_survey(application: Application):
    now = datetime.now()
    question_ids = get_questions_for_date(now)

    if not question_ids:
        logger.info("Сегодня не день опроса по расписанию")
        return

    greeting = get_greeting_for_date(now)
    participants = get_all_participants()
    sent = 0

    logger.info(f"Старт рассылки. Вопросов: {len(question_ids)}")

    for telegram_id, company_code in participants:
        try:
            clear_survey_session(telegram_id)

            await launch_survey_for_user(
                bot=application.bot,
                telegram_id=telegram_id,
                company_code=company_code,
                question_ids=question_ids,
                greeting=greeting
            )

            sent += 1

        except Exception as e:
            logger.warning(f"Не удалось отправить {telegram_id}: {e}")

    logger.info(f"Рассылка завершена: отправлено {sent} из {len(participants)}")


def run_scheduler(application: Application):
    schedule.clear()

    meta_schedule = CONFIG.get("_meta", {}).get("schedule", {})

    for weekday, cfg in meta_schedule.items():
        time_str = cfg["time"]

        if weekday == "tuesday":
            schedule.every().tuesday.at(time_str).do(
                lambda: application.create_task(send_survey(application))
            )
        elif weekday == "thursday":
            schedule.every().thursday.at(time_str).do(
                lambda: application.create_task(send_survey(application))
            )
        elif weekday == "monday":
            schedule.every().monday.at(time_str).do(
                lambda: application.create_task(send_survey(application))
            )
        elif weekday == "wednesday":
            schedule.every().wednesday.at(time_str).do(
                lambda: application.create_task(send_survey(application))
            )
        elif weekday == "friday":
            schedule.every().friday.at(time_str).do(
                lambda: application.create_task(send_survey(application))
            )

    schedule.every().day.at("23:00").do(daily_backup)

    logger.info(f"Расписание загружено: {meta_schedule}")

    while True:
        try:
            schedule.run_pending()
            time.sleep(30)
        except Exception as e:
            logger.error(f"Ошибка планировщика: {e}")
            time.sleep(30)


def main():
    if not BOT_TOKEN:
        raise ValueError("Не задан BOT_TOKEN")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    reg = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            WAITING_CODE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_code)
            ]
        },
        fallbacks=[CommandHandler("start", start)],
    )

    app.add_handler(reg)
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("contact", contact_cmd))
    app.add_handler(CommandHandler("privacy", privacy_cmd))
    app.add_handler(CommandHandler("myid", myid_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("testsurvey", testsurvey_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(CallbackQueryHandler(callback_router))

    threading.Thread(target=run_scheduler, args=(app,), daemon=True).start()

    logger.info("Бот запущен!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
