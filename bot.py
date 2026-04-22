import hashlib
import logging
import os
import csv
import sqlite3
import asyncio
import threading
from datetime import datetime
from pathlib import Path

import schedule
import yadisk

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler
)

# НАСТРОЙКИ

BOT_TOKEN = os.getenv("BOT_TOKEN")
YANDEX_TOKEN = os.getenv("YANDEX_TOKEN")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")

YANDEX_FOLDER = "/SomaSpace"
DB_PATH = "/app/data/somaspace.db"

SURVEY_DAYS = ["monday", "wednesday", "friday"]
SURVEY_HOUR = 10
SURVEY_MINUTE = 0

WAITING_CODE = 0

QUESTIONS = {
    "q1": {
        "text": "*Вопрос 1 из 3*\nКак ты себя чувствуешь прямо сейчас?",
        "buttons": [
            [("🔥 В ресурсе", "resurs"), ("😐 Нормально", "normalno")],
            [("🌧 Устал(а)", "ustal"), ("💤 На пределе", "predel")]
        ]
    },
    "q2": {
        "text": "*Вопрос 2 из 3*\nКакая у тебя нагрузка на этой неделе?",
        "buttons": [
            [("Лёгкая", "legkaya"), ("Нормальная", "normalnaya")],
            [("Высокая", "vysokaya"), ("Не справляюсь", "ne_spravlyayus")]
        ]
    },
    "q3": {
        "text": "*Вопрос 3 из 3*\nДумал(а) на этой неделе о смене работы?",
        "buttons": [
            [("Нет", "net")],
            [("Промелькнула мысль", "mysl")],
            [("Да, серьёзно", "da_serezno")]
        ]
    }
}

Q1_LABELS = {
    "resurs": "🔥 В ресурсе",
    "normalno": "😐 Нормально",
    "ustal": "🌧 Устал(а)",
    "predel": "💤 На пределе"
}

Q2_LABELS = {
    "legkaya": "Лёгкая",
    "normalnaya": "Нормальная",
    "vysokaya": "Высокая",
    "ne_spravlyayus": "Не справляюсь"
}

Q3_LABELS = {
    "net": "Нет",
    "mysl": "Промелькнула мысль",
    "da_serezno": "Да, серьёзно"
}

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# БАЗА

def init_db():
    os.makedirs("/app/data", exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
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
            company_code TEXT,
            telegram_id INTEGER,
            registered TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS answers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            time TEXT,
            company_code TEXT,
            anon_id TEXT,
            q1_state TEXT,
            q2_load TEXT,
            q3_leave TEXT,
            weekday TEXT
        )
    """)

    cur.execute("""
        INSERT OR IGNORE INTO companies (code, name)
        VALUES ('TEST', 'Тестовая компания')
    """)

    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")

def get_all_companies() -> list:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT code FROM companies ORDER BY code").fetchall()
    conn.close()
    return [row[0] for row in rows]

def get_all_participants() -> list:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT telegram_id, company_code FROM participants"
    ).fetchall()
    conn.close()
    return rows

def anonymize(telegram_id: int) -> str:
    raw = str(telegram_id).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]

def is_valid_code(code: str) -> bool:
    return code.upper() in get_all_companies()

def register_user(telegram_id: int, company_code: str) -> bool:
    anon_id = anonymize(telegram_id)
    try:
        conn = sqlite3.connect(DB_PATH)
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

def save_answer(telegram_id: int, company: str, q1: str, q2: str, q3: str):
    now = datetime.now()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO answers
        (date, time, company_code, anon_id, q1_state, q2_load, q3_leave, weekday)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        now.strftime("%Y-%m-%d"),
        now.strftime("%H:%M"),
        company.upper(),
        anonymize(telegram_id),
        q1, q2, q3,
        now.strftime("%A")
    ))
    conn.commit()
    conn.close()
    logger.info(f"Ответ сохранён: {company}")

# CSV оставляем, но можно не использовать

def export_to_csv(company_code: str = None) -> str:
    filename = f"/app/data/somaspace_answers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    conn = sqlite3.connect(DB_PATH)
    if company_code:
        rows = conn.execute(
            "SELECT date, time, company_code, anon_id, q1_state, q2_load, q3_leave, weekday "
            "FROM answers WHERE company_code = ? ORDER BY date DESC, time DESC",
            (company_code.upper(),)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT date, time, company_code, anon_id, q1_state, q2_load, q3_leave, weekday "
            "FROM answers ORDER BY date DESC, time DESC"
        ).fetchall()
    conn.close()

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Дата", "Время", "Компания", "ID_анонимный",
            "Состояние", "Нагрузка", "Мысли_об_уходе", "День_недели"
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

# СТАТИСТИКА

def _format_distribution(rows, total, labels_map):
    if total == 0:
        return "Нет данных."

    lines = []
    for key, count in rows:
        pct = round(count / total * 100)
        label = labels_map.get(key, key)
        bar = "█" * max(1, pct // 10) if count > 0 else ""
        lines.append(f"{label}: {count} ({pct}%)")
    return "\n".join(lines) if lines else "Нет данных."

def get_company_stats_text(company_code: str) -> str:
    code = company_code.upper()

    conn = sqlite3.connect(DB_PATH)

    company = conn.execute(
        "SELECT name FROM companies WHERE code = ?",
        (code,)
    ).fetchone()

    total = conn.execute(
        "SELECT COUNT(*) FROM answers WHERE company_code = ?",
        (code,)
    ).fetchone()[0]

    unique_people = conn.execute(
        "SELECT COUNT(DISTINCT anon_id) FROM answers WHERE company_code = ?",
        (code,)
    ).fetchone()[0]

    q1_rows = conn.execute("""
        SELECT q1_state, COUNT(*)
        FROM answers
        WHERE company_code = ?
        GROUP BY q1_state
        ORDER BY COUNT(*) DESC
    """, (code,)).fetchall()

    q2_rows = conn.execute("""
        SELECT q2_load, COUNT(*)
        FROM answers
        WHERE company_code = ?
        GROUP BY q2_load
        ORDER BY COUNT(*) DESC
    """, (code,)).fetchall()

    q3_rows = conn.execute("""
        SELECT q3_leave, COUNT(*)
        FROM answers
        WHERE company_code = ?
        GROUP BY q3_leave
        ORDER BY COUNT(*) DESC
    """, (code,)).fetchall()

    conn.close()

    company_name = company[0] if company else "Неизвестная компания"

    if total == 0:
        return (
            f"📊 *Статистика по {code}*\n"
            f"{company_name}\n\n"
            "Пока нет ответов по этой компании."
        )

    text = (
        f"📊 *Статистика по {code}*\n"
        f"{company_name}\n\n"
        f"Всего ответов: *{total}*\n"
        f"Уникальных людей: *{unique_people}*\n\n"
        f"*Состояние*\n{_format_distribution(q1_rows, total, Q1_LABELS)}\n\n"
        f"*Нагрузка*\n{_format_distribution(q2_rows, total, Q2_LABELS)}\n\n"
        f"*Мысли об уходе*\n{_format_distribution(q3_rows, total, Q3_LABELS)}"
    )

    return text

def make_keyboard(buttons_config: list) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(text, callback_data=cb) for text, cb in row]
        for row in buttons_config
    ]
    return InlineKeyboardMarkup(keyboard)

def is_admin(user_id: int) -> bool:
    return ADMIN_TELEGRAM_ID is not None and str(user_id) == str(ADMIN_TELEGRAM_ID)

# ОБРАБОТЧИКИ

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Привет 👋\n\n"
        "Я бот SõmaSpace — помогаю командам следить за своим состоянием.\n\n"
        "Три раза в неделю буду присылать 3 коротких вопроса. Меньше минуты.\n\n"
        "🔒 *Твои ответы анонимны.* Руководитель видит только общую картину команды — никаких личных данных.\n\n"
        "Введи *код своей компании* — его дал тебе HR:",
        parse_mode="Markdown"
    )
    return WAITING_CODE

async def receive_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    code = update.message.text.strip().upper()
    user_id = update.effective_user.id

    if not is_valid_code(code):
        await update.message.reply_text(
            "❌ Код не найден. Попробуй ещё раз или уточни у HR."
        )
        return WAITING_CODE

    register_user(user_id, code)
    context.user_data["company_code"] = code
    context.user_data["survey_state"] = "q1"

    await update.message.reply_text(
        "✅ Отлично, ты зарегистрирован!\n\n"
        "Давай начнём небольшой опрос 👇"
    )

    await update.message.reply_text(
        QUESTIONS["q1"]["text"],
        reply_markup=make_keyboard(QUESTIONS["q1"]["buttons"]),
        parse_mode="Markdown"
    )

    return ConversationHandler.END

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *SõmaSpace Pulse Bot*\n\n"
        "Проверка бота:\n"
        "1. Нажми /start\n"
        "2. Введи код `TEST`\n"
        "3. Пройди 3 вопроса\n\n"
        "Команды:\n"
        "/start — регистрация\n"
        "/stop — отписаться\n"
        "/help — эта справка\n"
        "/myid — показать твой Telegram ID\n"
        "/stats TEST — статистика по компании\n"
        "/export TEST — выгрузить CSV (только для администратора)",
        parse_mode="Markdown"
    )

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Хорошо, больше не буду беспокоить.\n"
        "Если захочешь вернуться — /start"
    )

async def myid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Твой Telegram ID: `{update.effective_user.id}`",
        parse_mode="Markdown"
    )

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not is_admin(user_id):
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
    user_id = update.effective_user.id

    if not is_admin(user_id):
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

async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    answer = query.data
    state = context.user_data.get("survey_state")
    user_id = update.effective_user.id

    if state == "q1":
        context.user_data["q1"] = answer
        context.user_data["survey_state"] = "q2"
        await query.edit_message_text(
            QUESTIONS["q2"]["text"],
            reply_markup=make_keyboard(QUESTIONS["q2"]["buttons"]),
            parse_mode="Markdown"
        )

    elif state == "q2":
        context.user_data["q2"] = answer
        context.user_data["survey_state"] = "q3"
        await query.edit_message_text(
            QUESTIONS["q3"]["text"],
            reply_markup=make_keyboard(QUESTIONS["q3"]["buttons"]),
            parse_mode="Markdown"
        )

    elif state == "q3":
        company = context.user_data.get("company_code", "UNKNOWN")
        save_answer(
            user_id,
            company,
            context.user_data.get("q1", ""),
            context.user_data.get("q2", ""),
            answer
        )

        for key in ["survey_state", "q1", "q2"]:
            context.user_data.pop(key, None)

        await query.edit_message_text("Спасибо, записала! 🙏\nДо следующего раза.")

# РАССЫЛКА

async def send_survey(application: Application):
    logger.info("Начинаем рассылку...")
    participants = get_all_participants()
    sent = 0

    for telegram_id, company_code in participants:
        try:
            if telegram_id not in application.user_data:
                application.user_data[telegram_id] = {}

            application.user_data[telegram_id]["survey_state"] = "q1"
            application.user_data[telegram_id]["company_code"] = company_code

            await application.bot.send_message(
                chat_id=telegram_id,
                text="Привет 👋 Время короткого опроса!\n\n" + QUESTIONS["q1"]["text"],
                reply_markup=make_keyboard(QUESTIONS["q1"]["buttons"]),
                parse_mode="Markdown"
            )
            sent += 1

        except Exception as e:
            logger.warning(f"Не удалось отправить {telegram_id}: {e}")

    logger.info(f"Рассылка завершена: отправлено {sent} из {len(participants)}")

def run_scheduler(application: Application):
    def survey_job():
        asyncio.run(send_survey(application))

    time_str = f"{SURVEY_HOUR:02d}:{SURVEY_MINUTE:02d}"
    for day in SURVEY_DAYS:
        getattr(schedule.every(), day).at(time_str).do(survey_job)

    schedule.every().day.at("23:00").do(daily_backup)

    logger.info(f"Расписание: {SURVEY_DAYS} в {time_str}, бэкап в 23:00")

    import time
    while True:
        schedule.run_pending()
        time.sleep(60)

# ЗАПУСК

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
        fallbacks=[CommandHandler("start", start)]
    )

    app.add_handler(reg)
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("myid", myid_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(CallbackQueryHandler(handle_button))

    threading.Thread(target=run_scheduler, args=(app,), daemon=True).start()

    logger.info("Бот запущен!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
